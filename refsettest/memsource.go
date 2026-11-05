// Package refsettest provides test helpers for refset: an in-memory Source
// with controllable faults, per-replica views of it, a source wrapper that adds
// a KeyLoader, a fake clock whose monotonic and wall readings move
// independently, and RunConformance, which checks any Source against the
// Source contract.
package refsettest

import (
	"context"
	"errors"
	"maps"
	"sync"
	"sync/atomic"

	"github.com/azbyluthfan/refset"
)

// ErrPartitioned is returned by a partitioned Replica.
var ErrPartitioned = errors.New("refsettest: replica partitioned from source")

// MemSource is an in-memory refset.Source. Writes are versioned and gap-free;
// each Apply call is one atomic version. It satisfies the Source contract and
// offers hooks to inject faults. For several replicas with different faults,
// give each its own view with NewReplica. MemSource is safe for concurrent use.
type MemSource[K comparable, V any] struct {
	name string

	mu        sync.Mutex
	head      uint64
	state     map[K]V              // state at head
	compacted uint64               // changes at or below this version are gone
	log       []refset.Delta[K, V] // versions compacted+1 .. head, ascending
	watchers  map[chan uint64]struct{}
	replicas  []*Replica[K, V]

	dropNotify  bool
	changesErr  error
	loadErr     error
	changesGate chan struct{}

	changesCalls atomic.Int64
	loadCalls    atomic.Int64
}

// NewMemSource returns an empty source at version 0.
func NewMemSource[K comparable, V any](name string) *MemSource[K, V] {
	return &MemSource[K, V]{
		name:     name,
		state:    make(map[K]V),
		watchers: make(map[chan uint64]struct{}),
	}
}

// Name implements refset.Source.
func (s *MemSource[K, V]) Name() string { return s.name }

// Upsert sets key to value as one new version and returns it.
func (s *MemSource[K, V]) Upsert(key K, value V) uint64 {
	return s.Apply(refset.Op[K, V]{Key: key, Value: value})
}

// Delete removes key as one new version and returns it.
func (s *MemSource[K, V]) Delete(key K) uint64 {
	return s.Apply(refset.Op[K, V]{Key: key, Delete: true})
}

// Apply commits ops atomically as one new version and returns it.
func (s *MemSource[K, V]) Apply(ops ...refset.Op[K, V]) uint64 {
	s.mu.Lock()
	s.head++
	v := s.head
	applyOps(s.state, ops)
	s.log = append(s.log, refset.Delta[K, V]{Version: v, Ops: append([]refset.Op[K, V](nil), ops...)})
	if !s.dropNotify {
		for ch := range s.watchers {
			select {
			case ch <- v:
			default: // notifications are best effort
			}
		}
	}
	replicas := s.replicas
	s.mu.Unlock()

	for _, r := range replicas {
		r.notify(v)
	}
	return v
}

func applyOps[K comparable, V any](m map[K]V, ops []refset.Op[K, V]) {
	for _, op := range ops {
		if op.Delete {
			delete(m, op.Key)
		} else {
			m[op.Key] = op.Value
		}
	}
}

// Head returns the latest committed version.
func (s *MemSource[K, V]) Head() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.head
}

// State returns a copy of the current contents.
func (s *MemSource[K, V]) State() map[K]V {
	s.mu.Lock()
	defer s.mu.Unlock()
	return maps.Clone(s.state)
}

// changesLocked returns the retained deltas in (after, upTo].
func (s *MemSource[K, V]) changesLocked(after, upTo uint64) []refset.Delta[K, V] {
	var out []refset.Delta[K, V]
	for _, d := range s.log {
		if d.Version > upTo {
			break
		}
		if d.Version > after {
			out = append(out, d)
		}
	}
	return out
}

// LoadSnapshot implements refset.Source.
func (s *MemSource[K, V]) LoadSnapshot(ctx context.Context, add func(K, V)) (uint64, error) {
	s.loadCalls.Add(1)
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.loadErr != nil {
		return 0, s.loadErr
	}
	for k, v := range s.state {
		add(k, v)
	}
	return s.head, nil
}

// Changes implements refset.Source. It returns each retained version as its
// own delta. If a gate is set (SetChangesGate), it reads the result first and
// then blocks until the gate is closed or ctx ends, simulating a slow response.
func (s *MemSource[K, V]) Changes(ctx context.Context, after uint64) (uint64, []refset.Delta[K, V], error) {
	s.changesCalls.Add(1)
	s.mu.Lock()
	if s.changesErr != nil {
		err := s.changesErr
		s.mu.Unlock()
		return 0, nil, err
	}
	if after < s.compacted || after > s.head {
		s.mu.Unlock()
		return 0, nil, refset.ErrSnapshotRequired
	}
	head := s.head
	out := s.changesLocked(after, head)
	gate := s.changesGate
	s.mu.Unlock()

	if gate != nil {
		select {
		case <-gate:
		case <-ctx.Done():
			return 0, nil, ctx.Err()
		}
	}
	return head, out, nil
}

// Watch implements refset.Source.
func (s *MemSource[K, V]) Watch(ctx context.Context) (<-chan uint64, error) {
	ch := make(chan uint64, 64)
	s.mu.Lock()
	s.watchers[ch] = struct{}{}
	s.mu.Unlock()
	go func() {
		<-ctx.Done()
		s.mu.Lock()
		delete(s.watchers, ch)
		close(ch)
		s.mu.Unlock()
	}()
	return ch, nil
}

func (s *MemSource[K, V]) loadCurrent(_ context.Context, key K) (V, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.state[key]
	return v, ok, nil
}

// SetDropNotifications makes Watch silently drop every notification while on.
func (s *MemSource[K, V]) SetDropNotifications(drop bool) {
	s.mu.Lock()
	s.dropNotify = drop
	s.mu.Unlock()
}

// SetChangesError makes Changes fail with err until it is set back to nil.
func (s *MemSource[K, V]) SetChangesError(err error) {
	s.mu.Lock()
	s.changesErr = err
	s.mu.Unlock()
}

// SetLoadError makes LoadSnapshot fail with err until it is set back to nil.
func (s *MemSource[K, V]) SetLoadError(err error) {
	s.mu.Lock()
	s.loadErr = err
	s.mu.Unlock()
}

// SetChangesGate makes later Changes calls block on gate after reading their
// result. Close the gate to release them; nil removes the gate.
func (s *MemSource[K, V]) SetChangesGate(gate chan struct{}) {
	s.mu.Lock()
	s.changesGate = gate
	s.mu.Unlock()
}

// Compact discards all retained changes, so Changes from any earlier version
// returns refset.ErrSnapshotRequired.
func (s *MemSource[K, V]) Compact() {
	s.mu.Lock()
	s.compacted = s.head
	s.log = nil
	s.mu.Unlock()
}

// Restore replaces the source's contents and resets its head to version, as a
// database restored from a backup would. If version is below what a replica
// has applied, its next Changes call returns refset.ErrSnapshotRequired.
func (s *MemSource[K, V]) Restore(version uint64, state map[K]V) {
	s.mu.Lock()
	s.head = version
	s.state = maps.Clone(state)
	if s.state == nil {
		s.state = make(map[K]V)
	}
	s.log = nil
	s.compacted = version
	s.mu.Unlock()
}

// ChangesCalls returns how many times Changes was called.
func (s *MemSource[K, V]) ChangesCalls() int64 { return s.changesCalls.Load() }

// LoadCalls returns how many times LoadSnapshot was called.
func (s *MemSource[K, V]) LoadCalls() int64 { return s.loadCalls.Load() }

// currentReader is a source that can read its latest committed value for a
// key, which WithKeyLoader exposes as a refset.KeyLoader.
type currentReader[K comparable, V any] interface {
	refset.Source[K, V]
	loadCurrent(ctx context.Context, key K) (V, bool, error)
}

// LoaderSource wraps a MemSource or Replica and adds refset.KeyLoader, so a
// dataset can fall back to it.
type LoaderSource[K comparable, V any] struct {
	refset.Source[K, V]
	reader currentReader[K, V]

	gateMu sync.Mutex
	gate   chan struct{}
	calls  atomic.Int64
}

// WithKeyLoader returns s with a KeyLoader that reads the latest committed
// value (from the primary state, bypassing any replica delay, as a fallback
// query to the primary database would).
func WithKeyLoader[K comparable, V any](s currentReader[K, V]) *LoaderSource[K, V] {
	return &LoaderSource[K, V]{Source: s, reader: s}
}

// LoadKey implements refset.KeyLoader.
func (l *LoaderSource[K, V]) LoadKey(ctx context.Context, key K) (V, bool, error) {
	l.calls.Add(1)
	l.gateMu.Lock()
	gate := l.gate
	l.gateMu.Unlock()
	if gate != nil {
		select {
		case <-gate:
		case <-ctx.Done():
			var zero V
			return zero, false, ctx.Err()
		}
	}
	return l.reader.loadCurrent(ctx, key)
}

// SetLoadKeyGate makes later LoadKey calls block until gate is closed.
func (l *LoaderSource[K, V]) SetLoadKeyGate(gate chan struct{}) {
	l.gateMu.Lock()
	l.gate = gate
	l.gateMu.Unlock()
}

// LoadKeyCalls returns how many times LoadKey was called.
func (l *LoaderSource[K, V]) LoadKeyCalls() int64 { return l.calls.Load() }
