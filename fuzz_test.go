package refset

import (
	"context"
	"encoding/binary"
	"errors"
	"hash/crc64"
	"maps"
	"os"
	"path/filepath"
	"testing"
)

// scripted is a Source whose Changes and LoadSnapshot answers are set by the
// fuzzer, valid or not.
type scripted struct {
	head     uint64
	deltas   []Delta[int, int]
	snapshot []Op[int, int]
	failNext bool
}

func (s *scripted) Name() string { return "fuzz" }
func (s *scripted) LoadSnapshot(_ context.Context, add func(int, int)) (uint64, error) {
	if s.failNext {
		return 0, errors.New("load failed")
	}
	for _, e := range s.snapshot {
		add(e.Key, e.Value)
	}
	return s.head, nil
}
func (s *scripted) Changes(context.Context, uint64) (uint64, []Delta[int, int], error) {
	if s.failNext {
		return 0, nil, errors.New("changes failed")
	}
	return s.head, s.deltas, nil
}
func (s *scripted) Watch(context.Context) (<-chan uint64, error) { return nil, errors.New("no watch") }

// checkRoot verifies a root's internal consistency: every key in the shard
// its hash selects, and n equal to the number of entries.
func checkRoot(t *testing.T, r *root[int, int]) {
	t.Helper()
	n := 0
	for i, m := range r.shards {
		for k := range m {
			if shardOf(r.l, k) != i {
				t.Fatalf("key %d in shard %d, belongs in %d", k, i, shardOf(r.l, k))
			}
			n++
		}
	}
	if n != r.n {
		t.Fatalf("n = %d, but %d entries", r.n, n)
	}
}

// FuzzDatasetRefresh drives a Dataset with arbitrary Changes answers and
// snapshots, valid or not. Whatever the source says, the dataset must not
// panic, must stay internally consistent, and must hold exactly what a model
// gets by applying only the answers the dataset accepted.
func FuzzDatasetRefresh(f *testing.F) {
	f.Add([]byte{0x01, 0x02, 0x03, 0x11, 0x22, 0x80, 0x05, 0x41, 0x07})
	f.Add([]byte{0x81, 0x03, 0x00, 0x09, 0x02, 0xc0, 0x01, 0x02, 0x03})
	f.Fuzz(func(t *testing.T, data []byte) {
		src := &scripted{head: 1, snapshot: []Op[int, int]{{Key: 1, Value: 1}}}
		d, err := Open(context.Background(), Source[int, int](src), Options{Shards: 4, PollInterval: 1 << 62})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = d.Close() }()
		model := map[int]int{1: 1}
		next := func() byte {
			if len(data) == 0 {
				return 0
			}
			b := data[0]
			data = data[1:]
			return b
		}
		for len(data) > 0 {
			op := next()
			cur := d.root.Load()
			src.failNext = op&0x40 != 0
			if op&0x80 != 0 { // full snapshot, possibly with duplicate keys
				src.head = uint64(next() % 16)
				src.snapshot = nil
				for range op & 0x07 {
					src.snapshot = append(src.snapshot, Op[int, int]{Key: int(next() % 8), Value: int(next())})
				}
				if ok := d.refresh(true); ok {
					model = map[int]int{}
					for _, e := range src.snapshot {
						model[e.Key] = e.Value
					}
				} else if !src.failNext {
					t.Fatal("a valid snapshot was rejected")
				}
			} else { // changes: head and deltas chosen freely, valid or not
				src.head = cur.version + uint64(next()%4) - 1 // may be below applied
				src.deltas = nil
				for range op & 0x03 {
					dl := Delta[int, int]{Version: cur.version + uint64(next()%5) - 1}
					for range next() % 3 {
						b := next()
						dl.Ops = append(dl.Ops, Op[int, int]{Key: int(b % 8), Value: int(b), Delete: b&0x80 != 0})
					}
					src.deltas = append(src.deltas, dl)
				}
				valid := !src.failNext && checkChanges("fuzz", cur.version, src.head, src.deltas) == nil
				ok := d.refresh(false)
				switch {
				case ok != valid:
					t.Fatalf("refresh = %v for an answer whose validity is %v", ok, valid)
				case ok && src.head > cur.version:
					for _, dl := range src.deltas {
						if dl.Version <= cur.version {
							continue // already applied: skipped
						}
						for _, o := range dl.Ops {
							if o.Delete {
								delete(model, o.Key)
							} else {
								model[o.Key] = o.Value
							}
						}
					}
				}
			}
			r := d.root.Load()
			checkRoot(t, r)
			if got := rootContents(r); !maps.Equal(got, model) {
				t.Fatalf("contents %v, model %v", got, model)
			}
		}
	})
}

// FuzzLoadDiff checks diff-on-reload against a plain rebuild for arbitrary
// old and new snapshots, including repeated keys.
func FuzzLoadDiff(f *testing.F) {
	f.Add([]byte{1, 2, 3, 4}, []byte{1, 2, 1, 2, 5, 6})
	f.Fuzz(func(t *testing.T, before, after []byte) {
		pairs := func(b []byte) []Op[int, int] {
			var out []Op[int, int]
			for i := 0; i+1 < len(b); i += 2 {
				out = append(out, Op[int, int]{Key: int(b[i] % 32), Value: int(b[i+1] % 4)})
			}
			return out
		}
		src := &listSource[int, int]{version: 1, entries: pairs(before)}
		d := openList(t, src, 4)
		src.version, src.entries = 2, pairs(after)
		_, next, _ := reload(t, d)
		want := map[int]int{}
		for _, e := range src.entries {
			want[e.Key] = e.Value
		}
		checkRoot(t, next)
		if got := rootContents(next); !maps.Equal(got, want) {
			t.Fatalf("diffed reload %v, rebuild %v", got, want)
		}
	})
}

// FuzzSnapshotFileDecode feeds arbitrary bodies to the snapshot file reader,
// wrapped with a valid checksum so they reach the decoder: CRC-64 detects
// damage, not tampering. The reader must return an error or valid data,
// never panic.
func FuzzSnapshotFileDecode(f *testing.F) {
	f.Add([]byte("not gob"))
	f.Add([]byte{0x0c, 0xff, 0x81, 0x03, 0x01, 0x01})
	f.Fuzz(func(t *testing.T, body []byte) {
		path := filepath.Join(t.TempDir(), "f.snap")
		file := make([]byte, 0, snapshotPrefixLen+len(body))
		file = append(file, snapshotMagic...)
		file = binary.BigEndian.AppendUint64(file, crc64.Checksum(body, crcTable))
		file = binary.BigEndian.AppendUint64(file, uint64(len(body)))
		file = append(file, body...)
		if err := os.WriteFile(path, file, 0o600); err != nil {
			t.Fatal(err)
		}
		sf := &SnapshotFile[int, int]{Source: &scripted{}, Path: path}
		n := 0
		_, _ = sf.loadCached(context.Background(), func(int, int) { n++ })
		_ = n
	})
}
