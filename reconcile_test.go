package refset_test

import (
	"context"
	"maps"
	"sync"
	"testing"
	"time"

	"github.com/azbyluthfan/refset"
	"github.com/azbyluthfan/refset/refsettest"
)

const reloadEvery = 10 * time.Minute

// reconcileFixture opens a dataset on a fake clock that reloads every
// reloadEvery with Reconcile on, and returns a function that triggers the
// next periodic reload and waits for its reconcile to finish.
func reconcileFixture[V any](t *testing.T, src refset.Source[string, V]) (*refset.Dataset[string, V], func()) {
	t.Helper()
	clock := refsettest.NewFakeClock(time.Date(2026, 11, 14, 9, 0, 0, 0, time.UTC))
	d := open(t, src, refset.Options{Clock: clock, PollInterval: 24 * time.Hour, ReloadInterval: reloadEvery, Reconcile: true})
	eventually(t, "poll and reload timers armed", func() bool { return clock.Timers() == 2 })
	reload := func() {
		t.Helper()
		before := d.Stats().Reloads
		clock.Advance(reloadEvery)
		eventually(t, "periodic reload", func() bool { return d.Stats().Reloads > before })
		eventually(t, "reload timer re-armed", func() bool { return clock.Timers() == 2 })
	}
	return d, reload
}

func TestReconcileCountsChangesTheFeedMissed(t *testing.T) {
	src := newSrc()
	src.Upsert("a", 1)
	src.Upsert("b", 2)
	src.Upsert("c", 3)
	d, reload := reconcileFixture(t, src)

	reload() // nothing changed
	if st := d.Stats(); st.Reconciles != 1 || st.Divergences != 0 {
		t.Fatalf("clean reload: Reconciles %d, Divergences %d", st.Reconciles, st.Divergences)
	}

	// Change the source behind the feed's back: same version, different data
	// (a changed, b removed, d added).
	state := src.State()
	state["a"] = 100
	delete(state, "b")
	state["d"] = 4
	src.Restore(src.Head(), state)
	reload()
	st := d.Stats()
	if st.Reconciles != 2 || st.Divergences != 3 {
		t.Fatalf("after an unlogged change: Reconciles %d, Divergences %d; want 2, 3", st.Reconciles, st.Divergences)
	}
	if v, _ := d.Get("a"); v != 100 {
		t.Fatalf("reloaded data not installed: a = %d", v)
	}
	if _, ok := d.Get("b"); ok {
		t.Fatal("b should be gone after the reload")
	}
}

// Changes the feed reported, but the replica hasn't applied yet when the
// periodic reload runs, are expected differences, not divergences.
func TestReconcileIgnoresReportedChanges(t *testing.T) {
	src := newSrc()
	src.Upsert("a", 1)
	d, reload := reconcileFixture(t, src)
	src.SetDropNotifications(true) // keep the replica behind until the reload
	src.Upsert("a", 2)
	src.Upsert("b", 3)
	src.Delete("a")
	reload()
	if st := d.Stats(); st.Reconciles != 1 || st.Divergences != 0 {
		t.Fatalf("Reconciles %d, Divergences %d; want 1, 0", st.Reconciles, st.Divergences)
	}
	if got := maps.Collect(d.Snapshot().All()); !maps.Equal(got, map[string]int{"b": 3}) {
		t.Fatalf("contents %v", got)
	}
}

// A ReloadSource whose application changed data without bumping the version
// is the ReloadSource form of a missed change.
func TestReconcileCatchesUnbumpedReloadSourceVersion(t *testing.T) {
	var mu sync.Mutex
	version := uint64(1)
	rows := []withTags{{"a", []string{"x"}}, {"b", []string{"y"}}}
	src := &refset.ReloadSource[string, withTags]{
		Dataset: "tags",
		Load: func(context.Context) (uint64, []withTags, error) {
			mu.Lock()
			defer mu.Unlock()
			out := make([]withTags, len(rows))
			copy(out, rows)
			return version, out, nil
		},
		Version: func(context.Context) (uint64, error) {
			mu.Lock()
			defer mu.Unlock()
			return version, nil
		},
		Key: func(r withTags) string { return r.ID },
	}
	d, reload := reconcileFixture(t, refset.Source[string, withTags](src))

	mu.Lock()
	rows[1] = withTags{"b", []string{"changed"}} // forgot version++
	mu.Unlock()
	reload()
	// No Equal and a slice in the value: compared with reflect.DeepEqual.
	if st := d.Stats(); st.Reconciles != 1 || st.Divergences != 1 {
		t.Fatalf("Reconciles %d, Divergences %d; want 1, 1", st.Reconciles, st.Divergences)
	}
}

type withTags struct {
	ID   string
	Tags []string
}

func TestReconcileNeedsReloadInterval(t *testing.T) {
	if _, err := refset.Open(context.Background(), newSrc(), refset.Options{Reconcile: true}); err == nil {
		t.Fatal("Reconcile without ReloadInterval should be rejected")
	}
}
