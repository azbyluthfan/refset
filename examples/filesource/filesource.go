// Package filesource is an example refset.Source over plain files: a JSON
// snapshot plus an append-only change log in JSON Lines. It shows that refset
// isn't tied to databases.
//
// Layout in the directory:
//
//	snapshot.json   {"version": 12, "entries": [{"key": ..., "value": ...}]}
//	changes.jsonl   {"version": 13, "ops": [{"key": ..., "value": ..., "delete": false}]}
//
// Writes go through Store.Apply, which appends one line per version and
// syncs it. Compact folds the log into a new snapshot. A Store serializes its
// own writers and readers with a mutex, so one process may write while its
// datasets read. Coordinating several writing processes would need file
// locking, which this example leaves out.
//
// There is no push notification: Watch returns a channel that never sends,
// and datasets rely on Options.PollInterval, which the Source contract allows.
package filesource

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"sync"

	"github.com/azbyluthfan/refset"
)

const (
	snapshotFile = "snapshot.json"
	changesFile  = "changes.jsonl"
)

// Store is a file-backed dataset and its refset.Source.
type Store[K comparable, V any] struct {
	name string
	dir  string
	mu   sync.RWMutex
}

type entry[K comparable, V any] struct {
	Key   K `json:"key"`
	Value V `json:"value"`
}

type snapshot[K comparable, V any] struct {
	Version uint64        `json:"version"`
	Entries []entry[K, V] `json:"entries"`
}

type op[K comparable, V any] struct {
	Key    K    `json:"key"`
	Value  V    `json:"value,omitzero"`
	Delete bool `json:"delete,omitempty"`
}

type line[K comparable, V any] struct {
	Version uint64     `json:"version"`
	Ops     []op[K, V] `json:"ops"`
}

// Open returns the store in dir, creating an empty one if needed.
func Open[K comparable, V any](name, dir string) (*Store[K, V], error) {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, err
	}
	s := &Store[K, V]{name: name, dir: dir}
	if _, err := os.Stat(s.path(snapshotFile)); errors.Is(err, fs.ErrNotExist) {
		if err := writeAtomic(s.path(snapshotFile), snapshot[K, V]{}); err != nil {
			return nil, err
		}
	}
	return s, nil
}

func (s *Store[K, V]) path(f string) string { return filepath.Join(s.dir, f) }

// Name implements refset.Source.
func (s *Store[K, V]) Name() string { return s.name }

// Apply commits ops as one new version, appended and synced to the log.
func (s *Store[K, V]) Apply(ops ...refset.Op[K, V]) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	snap, lines, err := s.read()
	if err != nil {
		return 0, err
	}
	l := line[K, V]{Version: head(snap, lines) + 1}
	for _, o := range ops {
		l.Ops = append(l.Ops, op[K, V]{Key: o.Key, Value: o.Value, Delete: o.Delete})
	}
	b, err := json.Marshal(l)
	if err != nil {
		return 0, err
	}
	f, err := os.OpenFile(s.path(changesFile), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return 0, err
	}
	if _, err := f.Write(append(b, '\n')); err != nil {
		_ = f.Close()
		return 0, err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return 0, err
	}
	return l.Version, f.Close()
}

// Compact folds the change log into a new snapshot and empties the log.
// Datasets behind the new snapshot get refset.ErrSnapshotRequired and reload.
func (s *Store[K, V]) Compact() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	snap, lines, err := s.read()
	if err != nil {
		return err
	}
	state := replay(snap, lines)
	next := snapshot[K, V]{Version: head(snap, lines)}
	for k, v := range state {
		next.Entries = append(next.Entries, entry[K, V]{k, v})
	}
	if err := writeAtomic(s.path(snapshotFile), next); err != nil {
		return err
	}
	return os.Remove(s.path(changesFile))
}

// LoadSnapshot implements refset.Source.
func (s *Store[K, V]) LoadSnapshot(ctx context.Context, add func(K, V)) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	s.mu.RLock()
	snap, lines, err := s.read()
	s.mu.RUnlock()
	if err != nil {
		return 0, err
	}
	for k, v := range replay(snap, lines) {
		add(k, v)
	}
	return head(snap, lines), nil
}

// Changes implements refset.Source: one delta per logged version.
func (s *Store[K, V]) Changes(ctx context.Context, after uint64) (uint64, []refset.Delta[K, V], error) {
	if err := ctx.Err(); err != nil {
		return 0, nil, err
	}
	s.mu.RLock()
	snap, lines, err := s.read()
	s.mu.RUnlock()
	if err != nil {
		return 0, nil, err
	}
	h := head(snap, lines)
	if after < snap.Version || after > h {
		return 0, nil, refset.ErrSnapshotRequired
	}
	var out []refset.Delta[K, V]
	for _, l := range lines {
		if l.Version <= after {
			continue
		}
		d := refset.Delta[K, V]{Version: l.Version}
		for _, o := range l.Ops {
			d.Ops = append(d.Ops, refset.Op[K, V]{Key: o.Key, Value: o.Value, Delete: o.Delete})
		}
		out = append(out, d)
	}
	return h, out, nil
}

// Watch implements refset.Source. Files have no push notifications, so the
// channel never sends; it closes when ctx ends.
func (s *Store[K, V]) Watch(ctx context.Context) (<-chan uint64, error) {
	ch := make(chan uint64)
	go func() {
		<-ctx.Done()
		close(ch)
	}()
	return ch, nil
}

// read loads the snapshot and the complete lines of the log after it. A
// trailing partial line (a write in progress or torn by a crash) is ignored.
func (s *Store[K, V]) read() (snapshot[K, V], []line[K, V], error) {
	var snap snapshot[K, V]
	b, err := os.ReadFile(s.path(snapshotFile))
	if err != nil {
		return snap, nil, err
	}
	if err := json.Unmarshal(b, &snap); err != nil {
		return snap, nil, fmt.Errorf("filesource: %s: %w", snapshotFile, err)
	}
	data, err := os.ReadFile(s.path(changesFile))
	if errors.Is(err, fs.ErrNotExist) {
		return snap, nil, nil
	}
	if err != nil {
		return snap, nil, err
	}
	if i := bytes.LastIndexByte(data, '\n'); i >= 0 {
		data = data[:i+1]
	} else {
		data = nil
	}
	var lines []line[K, V]
	sc := bufio.NewScanner(bytes.NewReader(data))
	sc.Buffer(make([]byte, 64*1024), 16<<20)
	next := snap.Version + 1
	for sc.Scan() {
		var l line[K, V]
		if err := json.Unmarshal(sc.Bytes(), &l); err != nil {
			return snap, nil, fmt.Errorf("filesource: %s: %w", changesFile, err)
		}
		if l.Version != next {
			return snap, nil, fmt.Errorf("filesource: %s: version %d where %d was expected", changesFile, l.Version, next)
		}
		next++
		lines = append(lines, l)
	}
	return snap, lines, sc.Err()
}

func head[K comparable, V any](snap snapshot[K, V], lines []line[K, V]) uint64 {
	if len(lines) > 0 {
		return lines[len(lines)-1].Version
	}
	return snap.Version
}

func replay[K comparable, V any](snap snapshot[K, V], lines []line[K, V]) map[K]V {
	m := make(map[K]V, len(snap.Entries))
	for _, e := range snap.Entries {
		m[e.Key] = e.Value
	}
	for _, l := range lines {
		for _, o := range l.Ops {
			if o.Delete {
				delete(m, o.Key)
			} else {
				m[o.Key] = o.Value
			}
		}
	}
	return maps.Clone(m)
}

func writeAtomic(path string, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	f, err := os.Create(tmp) //nolint:gosec // a temporary file in the store's own directory
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
