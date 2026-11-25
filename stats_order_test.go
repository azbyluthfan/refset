package refset

import (
	"context"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

// pauseAfterFetch widens the window between a refresh's fetch and its result
// becoming visible, where a counter recorded too early would be seen ahead of
// what it counts. The flaky tests of earlier releases hit it by chance.
func pauseAfterFetch(t *testing.T) {
	t.Helper()
	testHookAfterFetch = func() { time.Sleep(20 * time.Millisecond) }
	t.Cleanup(func() { testHookAfterFetch = nil }) // registered first, so it runs after Close
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		runtime.Gosched()
	}
}

// When Reloads moves, the reloaded version is already served and confirmed.
func TestReloadCounterFollowsTheData(t *testing.T) {
	pauseAfterFetch(t)
	src := &listSource[string, int]{}
	src.set(map[string]int{"a": 1})
	d := openList(t, src, 0)
	src.set(map[string]int{"a": 2})
	// Each refresh stores a new Reading. Compare the pointer, not the time:
	// on a coarse clock (Windows) Open and this refresh can read the same
	// instant.
	confirmedBefore := d.lastConfirmed.Load()
	d.kick <- struct{}{}
	waitFor(t, "reload counted", func() bool { return d.Stats().Reloads == 1 })
	if v, _ := d.Get("a"); v != 2 || d.Version() != src.version {
		t.Fatalf("Reloads moved before the data: a = %d at version %d", v, d.Version())
	}
	if d.lastConfirmed.Load() == confirmedBefore {
		t.Fatal("Reloads moved before the confirmed time")
	}
}

// panicLoad panics in LoadSnapshot while told to.
type panicLoad struct {
	*listSource[string, int]
	panics atomic.Bool
}

func (p *panicLoad) LoadSnapshot(ctx context.Context, add func(string, int)) (uint64, error) {
	if p.panics.Load() {
		panic("source bug")
	}
	return p.listSource.LoadSnapshot(ctx, add)
}

// When Panics moves, the failed poll is already counted.
func TestPanicCounterFollowsPollErrors(t *testing.T) {
	pauseAfterFetch(t)
	inner := &listSource[string, int]{}
	inner.set(map[string]int{"a": 1})
	src := &panicLoad{listSource: inner}
	d, err := Open(context.Background(), Source[string, int](src), Options{PollInterval: 1 << 62})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	src.panics.Store(true)
	d.kick <- struct{}{}
	waitFor(t, "panic counted", func() bool { return d.Stats().Panics == 1 })
	if st := d.Stats(); st.PollErrors != 1 {
		t.Fatalf("Panics moved before PollErrors: PollErrors %d", st.PollErrors)
	}
}
