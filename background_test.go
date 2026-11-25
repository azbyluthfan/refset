package refset_test

import (
	"context"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/azbyluthfan/refset"
	"github.com/azbyluthfan/refset/refsettest"
)

var errDown = errors.New("source down")

func openBackground[K comparable, V any](t *testing.T, src refset.Source[K, V], opts refset.Options) *refset.Dataset[K, V] {
	t.Helper()
	d, err := refset.OpenBackground(src, opts)
	if err != nil {
		t.Fatalf("OpenBackground: %v", err)
	}
	t.Cleanup(func() { _ = d.Close() })
	return d
}

func isReady[K comparable, V any](d *refset.Dataset[K, V]) bool {
	select {
	case <-d.Ready():
		return true
	default:
		return false
	}
}

func newClock() *refsettest.FakeClock {
	return refsettest.NewFakeClock(time.Date(2026, 11, 17, 9, 0, 0, 0, time.UTC))
}

// With the source up, OpenBackground loads at once and then behaves like
// Open: the dataset follows changes.
func TestOpenBackgroundSourceUp(t *testing.T) {
	src := newSrc()
	src.Upsert("a", 1)
	d := openBackground(t, refset.Source[string, int](src), refset.Options{PollInterval: 5 * time.Millisecond})
	select {
	case <-d.Ready():
	case <-time.After(5 * time.Second):
		t.Fatal("not ready")
	}
	if v, ok := d.Get("a"); !ok || v != 1 {
		t.Fatalf("a = %d, %v", v, ok)
	}
	if st := d.Stats(); !st.Loaded || st.AppliedVersion != 1 {
		t.Fatalf("Loaded %v, AppliedVersion %d", st.Loaded, st.AppliedVersion)
	}
	if _, _, err := d.GetFresh("a"); err != nil {
		t.Fatalf("GetFresh after the load: %v", err)
	}
	src.Upsert("a", 2)
	eventually(t, "change applied", func() bool { v, _ := d.Get("a"); return v == 2 })
}

// Open's datasets are ready when Open returns.
func TestOpenIsReady(t *testing.T) {
	src := newSrc()
	d := open(t, refset.Source[string, int](src), quiet(refset.Options{}))
	if !isReady(d) || !d.Stats().Loaded {
		t.Fatal("Open returned a dataset that isn't ready")
	}
}

// With the source down: the dataset starts empty and says so on every read
// that can, retries at growing intervals, and serves normally once a load
// succeeds.
func TestOpenBackgroundRetriesUntilTheSourceIsUp(t *testing.T) {
	src := newSrc()
	src.Upsert("a", 1)
	src.SetLoadError(errDown)
	clock := newClock()
	d := openBackground(t, refset.Source[string, int](src), refset.Options{Clock: clock, PollInterval: time.Hour, MaxStaleness: 2 * time.Hour})

	eventually(t, "first attempt failed and a retry is scheduled", func() bool { return clock.Timers() == 1 })
	if isReady(d) {
		t.Fatal("ready although the load failed")
	}
	st := d.Stats()
	if st.Loaded || st.PollErrors != 1 || st.AppliedVersion != 0 || st.Entries != 0 {
		t.Fatalf("before the load: %+v", st)
	}
	if _, ok := d.Get("a"); ok {
		t.Fatal("Get found a key before the load")
	}
	_, _, err := d.GetFresh("a")
	if !errors.Is(err, refset.ErrTooStale) || !errors.Is(err, refset.ErrNotLoaded) {
		t.Fatalf("GetFresh before the load: %v; want ErrTooStale and ErrNotLoaded", err)
	}
	ctx := refset.ContextWithMinVersion(context.Background(), "plans", 1)
	for name, read := range map[string]func() error{
		"GetForRequest":                func() error { _, _, err := d.GetForRequest(context.Background(), "a"); return err },
		"GetForRequest with a minimum": func() error { _, _, err := d.GetForRequest(ctx, "a"); return err },
		"GetAtLeast":                   func() error { _, _, err := d.GetAtLeast(context.Background(), "a", 1); return err },
	} {
		if err := read(); !errors.Is(err, refset.ErrNotLoaded) {
			t.Errorf("%s before the load: %v; want ErrNotLoaded at once", name, err)
		}
	}

	// Retries back off: 1s, then 2s (±20% jitter each).
	clock.Advance(1200 * time.Millisecond)
	eventually(t, "second attempt failed", func() bool { return d.Stats().PollErrors == 2 && clock.Timers() == 1 })
	clock.Advance(1500 * time.Millisecond) // under the 2s retry's earliest (1.6s)
	if got := d.Stats().PollErrors; got != 2 {
		t.Fatalf("retried after %v; want a growing interval", 1500*time.Millisecond)
	}

	src.SetLoadError(nil)
	clock.Advance(900 * time.Millisecond) // 2.4s since the second attempt: past the latest (2.4s)
	eventually(t, "loaded", func() bool { return isReady(d) })
	if v, ok := d.Get("a"); !ok || v != 1 {
		t.Fatalf("after the load: a = %d, %v", v, ok)
	}
	if st := d.Stats(); !st.Loaded || st.AppliedVersion != 1 {
		t.Fatalf("after the load: Loaded %v, AppliedVersion %d", st.Loaded, st.AppliedVersion)
	}
	if _, _, err := d.GetFresh("a"); err != nil {
		t.Fatalf("GetFresh after the load: %v", err)
	}
	if _, _, err := d.GetForRequest(ctx, "a"); err != nil {
		t.Fatalf("GetForRequest after the load: %v", err)
	}

	// The normal loop runs: a notified change is applied.
	src.Upsert("a", 2)
	eventually(t, "change applied after the background load", func() bool { v, _ := d.Get("a"); return v == 2 })
}

func TestOpenBackgroundRetryIntervalIsCapped(t *testing.T) {
	src := newSrc()
	src.SetLoadError(errDown)
	clock := newClock()
	d := openBackground(t, refset.Source[string, int](src), refset.Options{Clock: clock, PollInterval: time.Hour})
	for attempt := uint64(1); attempt <= 8; attempt++ {
		eventually(t, "attempt failed and retry scheduled", func() bool { return d.Stats().PollErrors == attempt && clock.Timers() == 1 })
		clock.Advance(36 * time.Second) // 30s cap + 20% jitter
	}
	eventually(t, "ninth attempt", func() bool { return d.Stats().PollErrors == 9 })
}

// keyed is a MemSource with a KeyLoader that answers while the snapshot load
// fails.
type keyed struct {
	*refsettest.MemSource[string, int]
	calls atomic.Int64
}

func (k *keyed) LoadKey(_ context.Context, key string) (int, bool, error) {
	k.calls.Add(1)
	v, ok := k.State()[key]
	return v, ok, nil
}

// Before the load, version-aware reads go straight to the KeyLoader.
func TestOpenBackgroundFallsBackBeforeTheLoad(t *testing.T) {
	mem := newSrc()
	mem.Upsert("a", 7)
	mem.SetLoadError(errDown)
	src := &keyed{MemSource: mem}
	clock := newClock()
	d := openBackground(t, refset.Source[string, int](src), refset.Options{Clock: clock, PollInterval: time.Hour, WaitBound: time.Hour})
	eventually(t, "first attempt failed", func() bool { return clock.Timers() == 1 })

	v, ok, err := d.GetForRequest(context.Background(), "a")
	if err != nil || !ok || v != 7 {
		t.Fatalf("GetForRequest: %d, %v, %v; want 7 from the KeyLoader", v, ok, err)
	}
	// Doesn't wait out the hour-long WaitBound.
	v, ok, err = d.GetAtLeast(context.Background(), "a", 5)
	if err != nil || !ok || v != 7 {
		t.Fatalf("GetAtLeast: %d, %v, %v; want 7 from the KeyLoader", v, ok, err)
	}
	if got := d.Stats().Fallbacks; got != 2 || src.calls.Load() != 2 {
		t.Fatalf("Fallbacks %d, LoadKey calls %d; want 2, 2", got, src.calls.Load())
	}
	// GetFresh stays fail-closed: no fallback.
	if _, _, err := d.GetFresh("a"); !errors.Is(err, refset.ErrNotLoaded) {
		t.Fatalf("GetFresh: %v", err)
	}
}

// Close during the retries returns promptly; Ready never closes.
func TestOpenBackgroundCloseWhileRetrying(t *testing.T) {
	src := newSrc()
	src.SetLoadError(errDown)
	clock := newClock()
	d, err := refset.OpenBackground(refset.Source[string, int](src), refset.Options{Clock: clock, PollInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	eventually(t, "first attempt failed", func() bool { return clock.Timers() == 1 })
	done := make(chan error, 1)
	go func() { done <- d.Close() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close hung while the load was being retried")
	}
	if isReady(d) {
		t.Fatal("ready after Close without a load")
	}
	if _, _, err := d.GetForRequest(context.Background(), "a"); !errors.Is(err, refset.ErrNotLoaded) {
		t.Fatalf("read after Close: %v", err)
	}
}

// A panic in the source during the background load is recovered, counted
// and retried like an error.
func TestOpenBackgroundPanicIsRetried(t *testing.T) {
	mem := newSrc()
	mem.Upsert("a", 1)
	src := &panicInFirstLoad{MemSource: mem}
	src.panics.Store(true)
	clock := newClock()
	d := openBackground(t, refset.Source[string, int](src), refset.Options{Clock: clock, PollInterval: time.Hour})
	eventually(t, "panic recovered", func() bool { return d.Stats().Panics == 1 && clock.Timers() == 1 })
	src.panics.Store(false)
	clock.Advance(1200 * time.Millisecond)
	eventually(t, "loaded after the panic", func() bool { return isReady(d) })
}

type panicInFirstLoad struct {
	*refsettest.MemSource[string, int]
	panics atomic.Bool
}

func (p *panicInFirstLoad) LoadSnapshot(ctx context.Context, add func(string, int)) (uint64, error) {
	if p.panics.Load() {
		panic("source bug in LoadSnapshot")
	}
	return p.MemSource.LoadSnapshot(ctx, add)
}

// With a usable snapshot file and the source down, OpenBackground starts
// from the file: ready at once, fail-closed until the source confirms it.
func TestOpenBackgroundStartsFromSnapshotFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "plans.snap")
	src := newSrc()
	src.Upsert("a", 1)
	first := open(t, refset.Source[string, int](src), quiet(refset.Options{}))
	if err := (&refset.SnapshotFile[string, int]{Source: src, Path: path}).Save(first.Snapshot()); err != nil {
		t.Fatal(err)
	}

	src.SetLoadError(errDown)
	src.SetChangesError(errDown)
	d := openBackground(t, refset.Source[string, int](&refset.SnapshotFile[string, int]{Source: src, Path: path}),
		quiet(refset.Options{MaxStaleness: 2 * time.Hour}))
	if !isReady(d) || !d.Stats().Loaded {
		t.Fatal("not ready although the snapshot file is usable")
	}
	if v, ok := d.Get("a"); !ok || v != 1 {
		t.Fatalf("a = %d, %v from the file", v, ok)
	}
	if _, _, err := d.GetFresh("a"); !errors.Is(err, refset.ErrTooStale) || errors.Is(err, refset.ErrNotLoaded) {
		t.Fatalf("GetFresh on unconfirmed file data: %v; want ErrTooStale only", err)
	}
}

// Saving before the load would replace a good file with an empty one.
func TestSaveRefusesUnloadedDataset(t *testing.T) {
	path := filepath.Join(t.TempDir(), "plans.snap")
	src := newSrc()
	src.SetLoadError(errDown)
	f := &refset.SnapshotFile[string, int]{Source: src, Path: path}
	d := openBackground(t, refset.Source[string, int](f), refset.Options{Clock: newClock(), PollInterval: time.Hour})
	if err := f.Save(d.Snapshot()); !errors.Is(err, refset.ErrNotLoaded) {
		t.Fatalf("Save before the load: %v; want ErrNotLoaded", err)
	}
	if matches, _ := filepath.Glob(path + "*"); len(matches) != 0 {
		t.Fatalf("Save left files: %v", matches)
	}
}

func TestOpenBackgroundRejectsBadArguments(t *testing.T) {
	if _, err := refset.OpenBackground[string, int](nil, refset.Options{}); err == nil {
		t.Error("nil source accepted")
	}
	if _, err := refset.OpenBackground(refset.Source[string, int](newSrc()), refset.Options{MaxStaleness: -1}); err == nil {
		t.Error("invalid options accepted")
	}
}

// ReloadSource.LoadKey is the fallback, with no wrapper type.
func TestReloadSourceLoadKeyField(t *testing.T) {
	var calls atomic.Int64
	src := &refset.ReloadSource[string, int]{
		Dataset: "plans",
		Load:    func(context.Context) (uint64, []int, error) { return 0, nil, errDown },
		Key:     func(v int) string { return "k" },
		LoadKey: func(_ context.Context, key string) (int, bool, error) {
			calls.Add(1)
			return 7, key == "a", nil
		},
	}
	clock := newClock()
	d := openBackground(t, refset.Source[string, int](src), refset.Options{Clock: clock, PollInterval: time.Hour})
	eventually(t, "first attempt failed", func() bool { return clock.Timers() == 1 })
	v, ok, err := d.GetForRequest(context.Background(), "a")
	if err != nil || !ok || v != 7 || calls.Load() != 1 {
		t.Fatalf("GetForRequest: %d, %v, %v after %d LoadKey calls; want 7 from LoadKey", v, ok, err, calls.Load())
	}

	// Without LoadKey there is no fallback.
	src2 := &refset.ReloadSource[string, int]{Dataset: "plans", Key: src.Key, Load: src.Load}
	clock2 := newClock()
	d2 := openBackground(t, refset.Source[string, int](src2), refset.Options{Clock: clock2, PollInterval: time.Hour})
	eventually(t, "first attempt failed", func() bool { return clock2.Timers() == 1 })
	if _, _, err := d2.GetForRequest(context.Background(), "a"); !errors.Is(err, refset.ErrNotLoaded) {
		t.Fatalf("without LoadKey: %v, want ErrNotLoaded", err)
	}
}

// A type embedding a ReloadSource overrides its LoadKey field with a method,
// as it did before the field existed.
type reloadWithLoadKeyMethod struct {
	*refset.ReloadSource[string, int]
}

func (reloadWithLoadKeyMethod) LoadKey(context.Context, string) (int, bool, error) {
	return 9, true, nil
}

func TestLoadKeyMethodOverridesReloadSourceField(t *testing.T) {
	src := reloadWithLoadKeyMethod{&refset.ReloadSource[string, int]{
		Dataset: "plans",
		Load:    func(context.Context) (uint64, []int, error) { return 0, nil, errDown },
		Key:     func(int) string { return "k" },
		LoadKey: func(context.Context, string) (int, bool, error) { return 1, true, nil },
	}}
	clock := newClock()
	d := openBackground(t, refset.Source[string, int](src), refset.Options{Clock: clock, PollInterval: time.Hour})
	eventually(t, "first attempt failed", func() bool { return clock.Timers() == 1 })
	if v, _, err := d.GetForRequest(context.Background(), "a"); err != nil || v != 9 {
		t.Fatalf("got %d, %v; want 9 from the method", v, err)
	}
}
