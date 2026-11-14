package refset_test

import (
	"context"
	"errors"
	"maps"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/azbyluthfan/refset"
	"github.com/azbyluthfan/refset/refsettest"
)

func seededSrc(n int) *refsettest.MemSource[string, int] {
	src := newSrc()
	ops := make([]refset.Op[string, int], n)
	for i := range ops {
		ops[i] = refset.Op[string, int]{Key: "k" + strconv.Itoa(i), Value: i}
	}
	src.Apply(ops...)
	return src
}

// saveFrom opens a dataset on src, saves it to path and closes it.
func saveFrom(t *testing.T, src refset.Source[string, int], path string) {
	t.Helper()
	f := &refset.SnapshotFile[string, int]{Source: src, Path: path}
	d, err := refset.Open(context.Background(), f, quiet(refset.Options{}))
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Save(d.Snapshot()); err != nil {
		t.Fatal(err)
	}
	_ = d.Close()
}

func contentsOf(d *refset.Dataset[string, int]) map[string]int {
	return maps.Collect(d.Snapshot().All())
}

func TestSnapshotFileRestartCatchesUp(t *testing.T) {
	path := filepath.Join(t.TempDir(), "plans.snap")
	src := seededSrc(1000)
	saveFrom(t, src, path)
	saved := src.Head()

	// Changes after the save, logged as usual.
	src.Upsert("k1", -1)
	src.Delete("k2")
	src.Upsert("new", 7)

	loads := src.LoadCalls()
	d := open(t, &refset.SnapshotFile[string, int]{Source: src, Path: path}, quiet(refset.Options{}))
	if src.LoadCalls() != loads {
		t.Fatal("Open loaded a full snapshot from the source instead of the file")
	}
	eventually(t, "caught up", func() bool { return d.Version() == src.Head() })
	if got := contentsOf(d); !maps.Equal(got, src.State()) {
		t.Fatalf("contents differ from the source after catching up (file was at %d)", saved)
	}
	if src.LoadCalls() != loads {
		t.Fatal("catching up from the file's version reloaded everything")
	}
}

// Until the first successful catch-up, data from the file has not been
// confirmed current: GetFresh must fail closed, Get must serve it.
func TestSnapshotFileFailsClosedUntilCaughtUp(t *testing.T) {
	path := filepath.Join(t.TempDir(), "plans.snap")
	src := seededSrc(10)
	saveFrom(t, src, path)

	gate := make(chan struct{})
	src.SetChangesGate(gate) // the catch-up poll blocks
	clock := refsettest.NewFakeClock(time.Date(2026, 11, 14, 9, 0, 0, 0, time.UTC))
	d := open(t, &refset.SnapshotFile[string, int]{Source: src, Path: path},
		refset.Options{Clock: clock, PollInterval: time.Second, MaxStaleness: 2 * time.Second})

	if v, ok := d.Get("k3"); !ok || v != 3 {
		t.Fatalf("Get from the file = %d, %v", v, ok)
	}
	if _, _, err := d.GetFresh("k3"); !errors.Is(err, refset.ErrTooStale) {
		t.Fatalf("GetFresh before catching up: err = %v, want ErrTooStale", err)
	}
	close(gate)
	eventually(t, "caught up and fresh", func() bool { _, _, err := d.GetFresh("k3"); return err == nil })
}

func TestSnapshotFileIgnoredWhenUnusable(t *testing.T) {
	dir := t.TempDir()
	good := filepath.Join(dir, "good.snap")
	src := seededSrc(500)
	saveFrom(t, src, good)
	raw, err := os.ReadFile(good)
	if err != nil {
		t.Fatal(err)
	}
	write := func(name string, b []byte) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, b, 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	flipped := append([]byte(nil), raw...)
	flipped[len(flipped)/2] ^= 0xff
	other := refsettest.NewMemSource[string, int]("other-dataset")
	other.Upsert("x", 1)
	otherPath := filepath.Join(dir, "other.snap")
	saveFrom(t, other, otherPath)
	otherRaw, _ := os.ReadFile(otherPath)

	cases := map[string]struct {
		path   string
		maxAge time.Duration
	}{
		"damaged":       {path: write("damaged.snap", flipped)},
		"truncated":     {path: write("truncated.snap", raw[:len(raw)-100])},
		"not a file":    {path: write("garbage.snap", []byte("hello"))},
		"other dataset": {path: write("other-copy.snap", otherRaw)},
		"missing":       {path: filepath.Join(dir, "missing.snap")},
		"too old":       {path: good, maxAge: time.Nanosecond},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			loads := src.LoadCalls()
			d := open(t, &refset.SnapshotFile[string, int]{Source: src, Path: c.path, MaxAge: c.maxAge}, quiet(refset.Options{}))
			if src.LoadCalls() != loads+1 {
				t.Fatal("expected a full load from the source")
			}
			if !maps.Equal(contentsOf(d), src.State()) {
				t.Fatal("contents differ from the source")
			}
		})
	}
}

type planV1 struct{ Seats int }

type planV2 struct{ Seats, Projects int } // a field added since the file was saved

// gob would decode a planV1 file into planV2 without complaint, leaving
// Projects zero: the type fingerprint must reject the file.
func TestSnapshotFileIgnoredWhenValueTypeChanged(t *testing.T) {
	path := filepath.Join(t.TempDir(), "plans.snap")
	v1 := refsettest.NewMemSource[string, planV1]("plans")
	v1.Upsert("a", planV1{Seats: 5})
	f1 := &refset.SnapshotFile[string, planV1]{Source: v1, Path: path}
	d1, err := refset.Open(context.Background(), f1, quiet(refset.Options{}))
	if err != nil {
		t.Fatal(err)
	}
	if err := f1.Save(d1.Snapshot()); err != nil {
		t.Fatal(err)
	}
	_ = d1.Close()

	v2 := refsettest.NewMemSource[string, planV2]("plans") // same name, same version
	v2.Upsert("a", planV2{Seats: 5, Projects: 3})
	d := open(t, &refset.SnapshotFile[string, planV2]{Source: v2, Path: path}, quiet(refset.Options{}))
	if v2.LoadCalls() != 1 {
		t.Fatal("a file saved for another value type was used")
	}
	if v, _ := d.Get("a"); v != (planV2{Seats: 5, Projects: 3}) {
		t.Fatalf("a = %+v", v)
	}
}

func TestSnapshotFileSourceMovedOn(t *testing.T) {
	t.Run("compacted past the file", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "plans.snap")
		src := seededSrc(100)
		saveFrom(t, src, path)
		src.Upsert("k5", -5)
		src.Compact() // the changes since the file are gone
		d := open(t, &refset.SnapshotFile[string, int]{Source: src, Path: path}, quiet(refset.Options{}))
		eventually(t, "reloaded", func() bool { return d.Version() == src.Head() })
		if !maps.Equal(contentsOf(d), src.State()) {
			t.Fatal("contents differ after the reload")
		}
	})
	t.Run("history behind the file, source breaks C4", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "plans.snap")
		src := seededSrc(100)
		src.Upsert("k1", 11) // version 2
		saveFrom(t, src, path)
		src.Restore(1, map[string]int{"only": 1})
		// A source that reports a head below the file's version without
		// ErrSnapshotRequired, as the contract asks: the dataset must still
		// reload instead of failing every poll.
		d := open(t, &refset.SnapshotFile[string, int]{Source: lowHead{src}, Path: path}, quiet(refset.Options{}))
		eventually(t, "reloaded at the older version", func() bool { return d.Version() == 1 })
		if got := contentsOf(d); !maps.Equal(got, map[string]int{"only": 1}) {
			t.Fatalf("contents %v", got)
		}
	})
	t.Run("history behind the file", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "plans.snap")
		src := seededSrc(100)
		src.Upsert("k1", 11)
		src.Upsert("k1", 12) // version 3
		saveFrom(t, src, path)
		src.Restore(1, map[string]int{"only": 1}) // e.g. restored from an older backup
		d := open(t, &refset.SnapshotFile[string, int]{Source: src, Path: path}, quiet(refset.Options{}))
		eventually(t, "reloaded at the older version", func() bool { return d.Version() == 1 })
		if got := contentsOf(d); !maps.Equal(got, map[string]int{"only": 1}) {
			t.Fatalf("contents %v", got)
		}
	})
}

type hidden struct {
	Shown  int
	secret int
}

func TestSnapshotFileRefusesFieldsGobWouldDrop(t *testing.T) {
	path := filepath.Join(t.TempDir(), "h.snap")
	src := refsettest.NewMemSource[string, hidden]("h")
	src.Upsert("a", hidden{1, 2})
	f := &refset.SnapshotFile[string, hidden]{Source: src, Path: path}
	d := open(t, f, quiet(refset.Options{}))
	if err := f.Save(d.Snapshot()); err == nil {
		t.Fatal("Save accepted a value type with an unexported field")
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("a file was written anyway")
	}
	_ = hidden{}.secret
}

func TestSnapshotFileIsPrivate(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Unix permissions")
	}
	path := filepath.Join(t.TempDir(), "plans.snap")
	saveFrom(t, seededSrc(3), path)
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0o600 {
		t.Fatalf("mode %v, want 0600", fi.Mode().Perm())
	}
}

// The wrapper must not hide the wrapped source's KeyLoader.
func TestSnapshotFileKeepsKeyLoader(t *testing.T) {
	path := filepath.Join(t.TempDir(), "plans.snap")
	src := seededSrc(10)
	saveFrom(t, src, path)
	d := open(t, &refset.SnapshotFile[string, int]{Source: refsettest.WithKeyLoader(src), Path: path},
		quiet(refset.Options{WaitBound: 20 * time.Millisecond}))
	eventually(t, "caught up", func() bool { _, _, err := d.GetFresh("k1"); return err == nil })
	src.SetChangesGate(make(chan struct{})) // stall polls so the read times out
	v := src.Upsert("k1", 100)
	got, _, err := d.GetAtLeast(context.Background(), "k1", v)
	if err != nil || got != 100 || d.Stats().Fallbacks != 1 {
		t.Fatalf("GetAtLeast = %d, %v; fallbacks %d (want the KeyLoader's answer)", got, err, d.Stats().Fallbacks)
	}
}

// lowHead answers Changes from beyond its head with that head and no error,
// instead of ErrSnapshotRequired (a source that breaks contract rule C4).
type lowHead struct {
	*refsettest.MemSource[string, int]
}

func (s lowHead) Changes(ctx context.Context, after uint64) (uint64, []refset.Delta[string, int], error) {
	if h := s.Head(); after > h {
		return h, nil, nil
	}
	return s.MemSource.Changes(ctx, after)
}

// BenchmarkSnapshotFile measures Save and an Open from the file at 1M
// entries, next to an Open from an in-process source (the floor for any
// real source, which adds its own read time).
func BenchmarkSnapshotFile(b *testing.B) {
	const n = 1_000_000
	src := seededSrc(n)
	path := filepath.Join(b.TempDir(), "bench.snap")
	f := &refset.SnapshotFile[string, int]{Source: src, Path: path}
	d, err := refset.Open(context.Background(), f, quiet(refset.Options{}))
	if err != nil {
		b.Fatal(err)
	}
	snap := d.Snapshot()
	_ = d.Close()
	b.Run("save", func(b *testing.B) {
		for b.Loop() {
			if err := f.Save(snap); err != nil {
				b.Fatal(err)
			}
		}
		fi, _ := os.Stat(path)
		b.ReportMetric(float64(fi.Size())/n, "file-B/entry")
	})
	for _, c := range []struct {
		name string
		src  refset.Source[string, int]
	}{{"open-from-file", f}, {"open-from-source", src}} {
		b.Run(c.name, func(b *testing.B) {
			for b.Loop() {
				d, err := refset.Open(context.Background(), c.src, quiet(refset.Options{}))
				if err != nil {
					b.Fatal(err)
				}
				_ = d.Close()
			}
		})
	}
}
