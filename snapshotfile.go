package refset

import (
	"bufio"
	"context"
	"encoding"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"hash"
	"hash/crc64"
	"hash/fnv"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"time"
)

// SnapshotFile wraps a Source with a snapshot saved on local disk, so a
// restarting process can serve reads before its source has sent a full
// snapshot. Open loads the file, then catches up through Source.Changes from
// the file's version, or reloads from the source if those changes are no
// longer available.
//
//	src := &refset.SnapshotFile[string, Plan]{Source: plansSource, Path: "/var/lib/app/plans.snap"}
//	plans, err := refset.Open(ctx, src, refset.Options{MaxStaleness: 5 * time.Second})
//	...
//	defer src.Save(plans.Snapshot()) // and periodically, for crashes
//
// Until the first successful catch-up the dataset has not confirmed it is
// current, so GetFresh fails with ErrTooStale whenever MaxStaleness or
// MaxLagVersions is set; Get and GetAtLeast serve the file's data (GetAtLeast
// still waits for the version a request asks for).
//
// The file is used only if it is intact (checksum), was saved for the same
// dataset name and the same key and value types, and is no older than MaxAge;
// otherwise Open loads from the source as usual and logs why. Entries are
// encoded with encoding/gob, so K and V must be gob-encodable; Save refuses
// types with unexported struct fields, which gob would silently drop.
//
// The file holds the dataset's data: it is created with mode 0600, and anyone
// who can write it can change what a restarted replica serves until it
// catches up. Keep it in a directory only the process can write.
type SnapshotFile[K comparable, V any] struct {
	Source[K, V]

	// Path is the file to load at Open and write in Save.
	Path string

	// MaxAge, if set, ignores a file saved longer ago than this.
	MaxAge time.Duration
}

// snapshotCache is implemented by SnapshotFile; Open uses it before the
// source's own LoadSnapshot.
type snapshotCache[K comparable, V any] interface {
	loadCached(ctx context.Context, add func(K, V)) (version uint64, err error)
}

// wrapped is implemented by sources that wrap another one, so Open can find
// the wrapped source's optional capabilities (KeyLoader, ReloadSource.Equal).
type wrapped[K comparable, V any] interface {
	unwrap() Source[K, V]
}

func (f *SnapshotFile[K, V]) unwrap() Source[K, V] { return f.Source }

const (
	snapshotMagic  = "refset-snapshot\n" // 16 bytes
	snapshotFormat = 1
	// Fixed prefix: magic, CRC-64 of the body, body length.
	snapshotPrefixLen = len(snapshotMagic) + 8 + 8
)

var crcTable = crc64.MakeTable(crc64.ECMA)

// snapshotHeader starts the gob-encoded body.
type snapshotHeader struct {
	Format   int
	Dataset  string
	Version  uint64
	Count    int
	Types    uint64 // fingerprint of K and V; a changed type ignores the file
	SavedAt  time.Time
	TypeName string // for error messages
}

type snapshotEntry[K comparable, V any] struct {
	K K
	V V
}

// Save writes s to Path atomically: to a temporary file in the same
// directory, flushed to disk, then renamed over Path. Snapshots are immutable,
// so Save can run while the dataset keeps changing.
func (f *SnapshotFile[K, V]) Save(s Snapshot[K, V]) error {
	if err := gobSafe(reflect.TypeFor[K]()); err != nil {
		return fmt.Errorf("refset: SnapshotFile key type: %w", err)
	}
	if err := gobSafe(reflect.TypeFor[V]()); err != nil {
		return fmt.Errorf("refset: SnapshotFile value type: %w", err)
	}
	dir := filepath.Dir(f.Path)
	tmp, err := os.CreateTemp(dir, filepath.Base(f.Path)+".tmp-*")
	if err != nil {
		return fmt.Errorf("refset: SnapshotFile save: %w", err)
	}
	defer func() { _ = os.Remove(tmp.Name()) }() // no-op after the rename
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("refset: SnapshotFile save: %w", err)
	}
	if err := f.write(tmp, s); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("refset: SnapshotFile save: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("refset: SnapshotFile save: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("refset: SnapshotFile save: %w", err)
	}
	if err := os.Rename(tmp.Name(), f.Path); err != nil {
		return fmt.Errorf("refset: SnapshotFile save: %w", err)
	}
	// Make the rename itself durable. Not every platform can sync a
	// directory; the file's contents are already on disk.
	if d, err := os.Open(dir); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}
	return nil
}

func (f *SnapshotFile[K, V]) write(w io.WriteSeeker, s Snapshot[K, V]) error {
	if _, err := w.Write(make([]byte, snapshotPrefixLen)); err != nil { // patched below
		return err
	}
	crc := crc64.New(crcTable)
	counter := &countWriter{}
	bw := bufio.NewWriterSize(io.MultiWriter(w, crc, counter), 1<<16)
	enc := gob.NewEncoder(bw)
	h := snapshotHeader{
		Format: snapshotFormat, Dataset: f.Name(), Version: s.Version(), Count: s.Len(),
		Types: typesFingerprint[K, V](), SavedAt: time.Now(), TypeName: typeNames[K, V](),
	}
	if err := enc.Encode(h); err != nil {
		return err
	}
	for k, v := range s.All() {
		if err := enc.Encode(snapshotEntry[K, V]{k, v}); err != nil {
			return err
		}
	}
	if err := bw.Flush(); err != nil {
		return err
	}
	prefix := make([]byte, 0, snapshotPrefixLen)
	prefix = append(prefix, snapshotMagic...)
	prefix = binary.BigEndian.AppendUint64(prefix, crc.Sum64())
	prefix = binary.BigEndian.AppendUint64(prefix, counter.n)
	if _, err := w.Seek(0, io.SeekStart); err != nil {
		return err
	}
	_, err := w.Write(prefix)
	return err
}

// loadCached checks the file completely (checksum, then header) before
// passing any entry to add, so a damaged or mismatched file adds nothing.
func (f *SnapshotFile[K, V]) loadCached(_ context.Context, add func(K, V)) (uint64, error) {
	file, err := os.Open(f.Path)
	if err != nil {
		return 0, err
	}
	defer func() { _ = file.Close() }()
	prefix := make([]byte, snapshotPrefixLen)
	if _, err := io.ReadFull(file, prefix); err != nil {
		return 0, fmt.Errorf("reading header: %w", err)
	}
	if string(prefix[:len(snapshotMagic)]) != snapshotMagic {
		return 0, errors.New("not a refset snapshot file")
	}
	wantCRC := binary.BigEndian.Uint64(prefix[len(snapshotMagic):])
	size := binary.BigEndian.Uint64(prefix[len(snapshotMagic)+8:])

	// Pass 1: the checksum over the whole body.
	crc := crc64.New(crcTable)
	if n, err := io.Copy(crc, io.LimitReader(file, int64(size))); err != nil {
		return 0, fmt.Errorf("reading body: %w", err)
	} else if uint64(n) != size {
		return 0, fmt.Errorf("file truncated (%d of %d bytes)", n, size)
	}
	if crc.Sum64() != wantCRC {
		return 0, errors.New("checksum mismatch: file is damaged")
	}

	// Pass 2: decode. The header is checked before any entry is added.
	if _, err := file.Seek(int64(snapshotPrefixLen), io.SeekStart); err != nil {
		return 0, err
	}
	dec := gob.NewDecoder(bufio.NewReaderSize(io.LimitReader(file, int64(size)), 1<<16))
	var h snapshotHeader
	if err := dec.Decode(&h); err != nil {
		return 0, fmt.Errorf("decoding header: %w", err)
	}
	switch {
	case h.Format != snapshotFormat:
		return 0, fmt.Errorf("format %d, want %d", h.Format, snapshotFormat)
	case h.Dataset != f.Name():
		return 0, fmt.Errorf("saved for dataset %q, not %q", h.Dataset, f.Name())
	case h.Types != typesFingerprint[K, V]():
		return 0, fmt.Errorf("saved for types %s, not %s", h.TypeName, typeNames[K, V]())
	case f.MaxAge > 0 && time.Since(h.SavedAt) > f.MaxAge:
		return 0, fmt.Errorf("saved %v ago, older than MaxAge %v", time.Since(h.SavedAt).Round(time.Second), f.MaxAge)
	}
	for range h.Count {
		var e snapshotEntry[K, V]
		if err := dec.Decode(&e); err != nil {
			return 0, fmt.Errorf("decoding entry: %w", err)
		}
		add(e.K, e.V)
	}
	return h.Version, nil
}

func encodesItself(t reflect.Type) bool {
	for _, i := range []reflect.Type{
		reflect.TypeFor[gob.GobEncoder](), reflect.TypeFor[encoding.BinaryMarshaler](), reflect.TypeFor[encoding.TextMarshaler](),
	} {
		if t.Implements(i) || reflect.PointerTo(t).Implements(i) {
			return true
		}
	}
	return false
}

type countWriter struct{ n uint64 }

func (c *countWriter) Write(p []byte) (int, error) { c.n += uint64(len(p)); return len(p), nil }

func typeNames[K comparable, V any]() string {
	return reflect.TypeFor[K]().String() + " → " + reflect.TypeFor[V]().String()
}

// typesFingerprint hashes the structure of K and V (kinds, field names and
// types, recursively), so a file saved for a different shape is ignored.
func typesFingerprint[K comparable, V any]() uint64 {
	h := fnv.New64a()
	writeType(h, reflect.TypeFor[K](), map[reflect.Type]bool{})
	_, _ = h.Write([]byte{0})
	writeType(h, reflect.TypeFor[V](), map[reflect.Type]bool{})
	return h.Sum64()
}

func writeType(h hash.Hash64, t reflect.Type, seen map[reflect.Type]bool) {
	_, _ = fmt.Fprintf(h, "%s.%s:%s|", t.PkgPath(), t.Name(), t.Kind())
	if seen[t] {
		return // recursive type
	}
	seen[t] = true
	switch t.Kind() {
	case reflect.Pointer, reflect.Slice, reflect.Array:
		if t.Kind() == reflect.Array {
			_, _ = fmt.Fprintf(h, "[%d]", t.Len())
		}
		writeType(h, t.Elem(), seen)
	case reflect.Map:
		writeType(h, t.Key(), seen)
		writeType(h, t.Elem(), seen)
	case reflect.Struct:
		for i := range t.NumField() {
			fl := t.Field(i)
			_, _ = fmt.Fprintf(h, "%s=", fl.Name)
			writeType(h, fl.Type, seen)
		}
	}
}

// gobSafe reports types gob can't round-trip faithfully: struct fields it
// would silently drop (unexported), and kinds it can't encode.
func gobSafe(t reflect.Type) error {
	return gobSafeSeen(t, map[reflect.Type]bool{}, t.String())
}

func gobSafeSeen(t reflect.Type, seen map[reflect.Type]bool, path string) error {
	if seen[t] {
		return nil
	}
	seen[t] = true
	switch t.Kind() {
	case reflect.Chan, reflect.Func, reflect.UnsafePointer, reflect.Uintptr:
		return fmt.Errorf("%s: gob can't encode a %s", path, t.Kind())
	case reflect.Pointer, reflect.Slice, reflect.Array:
		return gobSafeSeen(t.Elem(), seen, path)
	case reflect.Map:
		if err := gobSafeSeen(t.Key(), seen, path); err != nil {
			return err
		}
		return gobSafeSeen(t.Elem(), seen, path)
	case reflect.Struct:
		if encodesItself(t) {
			return nil // GobEncoder, BinaryMarshaler or TextMarshaler, e.g. time.Time
		}
		var unexported []string
		for i := range t.NumField() {
			fl := t.Field(i)
			if !fl.IsExported() {
				unexported = append(unexported, fl.Name)
				continue
			}
			if err := gobSafeSeen(fl.Type, seen, path+"."+fl.Name); err != nil {
				return err
			}
		}
		if len(unexported) > 0 {
			return fmt.Errorf("%s has unexported fields (%s) that gob would silently drop", path, strings.Join(unexported, ", "))
		}
	}
	return nil
}
