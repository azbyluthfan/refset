package refset_test

import (
	"context"
	"errors"
	"maps"
	"sync"
	"testing"
	"time"

	"github.com/azbyluthfan/refset"
	"github.com/azbyluthfan/refset/refsettest"
)

func TestOpenLoadsSnapshot(t *testing.T) {
	src := newSrc()
	src.Upsert("free", 0)
	src.Upsert("pro", 20)
	d := open(t, src, refset.Options{})

	if d.Name() != "plans" || d.Version() != 2 {
		t.Fatalf("name %q version %d", d.Name(), d.Version())
	}
	if v, ok := d.Get("pro"); !ok || v != 20 {
		t.Fatalf("Get(pro) = %d, %v", v, ok)
	}
	if _, ok := d.Get("enterprise"); ok {
		t.Fatal("unexpected key")
	}
	s := d.Snapshot()
	got := maps.Collect(s.All())
	if s.Version() != 2 || s.Len() != 2 || !maps.Equal(got, map[string]int{"free": 0, "pro": 20}) {
		t.Fatalf("snapshot v%d len %d %v", s.Version(), s.Len(), got)
	}
	st := d.Stats()
	if st.Name != "plans" || st.AppliedVersion != 2 || st.HeadVersion != 2 || st.Entries != 2 || st.Polls != 0 {
		t.Fatalf("stats %+v", st)
	}
}

func TestOpenErrors(t *testing.T) {
	if _, err := refset.Open[string, int](context.Background(), nil, refset.Options{}); err == nil {
		t.Fatal("nil source accepted")
	}
	if _, err := refset.Open(context.Background(), newSrc(), refset.Options{Shards: 3}); err == nil {
		t.Fatal("invalid options accepted")
	}
	src := newSrc()
	boom := errors.New("source down")
	src.SetLoadError(boom)
	if _, err := refset.Open(context.Background(), src, refset.Options{}); !errors.Is(err, boom) {
		t.Fatalf("load error: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := refset.Open(ctx, newSrc(), refset.Options{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled ctx: %v", err)
	}
}

// Changes reported through Watch are applied without any read asking.
func TestWatchDrivesUpdates(t *testing.T) {
	src := newSrc()
	d := open(t, src, quiet(refset.Options{}))
	src.Upsert("a", 1)
	src.Upsert("b", 2)
	src.Delete("a")
	eventually(t, "version 3", func() bool { return d.Version() == 3 })
	if _, ok := d.Get("a"); ok {
		t.Fatal("deleted key still present")
	}
	if v, _ := d.Get("b"); v != 2 {
		t.Fatalf("b = %d", v)
	}
}

// With notifications lost, the poll ticker still delivers changes (G3).
func TestPollingDeliversWithoutNotifications(t *testing.T) {
	src := newSrc()
	src.SetDropNotifications(true)
	d := open(t, src, refset.Options{PollInterval: 10 * time.Millisecond})
	for i := range 20 {
		src.Upsert("k", i)
	}
	eventually(t, "version 20", func() bool { return d.Version() == 20 })
}

// Watch failing at Open is not fatal; polling takes over.
type noWatch struct {
	*refsettest.MemSource[string, int]
}

func (noWatch) Watch(context.Context) (<-chan uint64, error) { return nil, errors.New("no push") }

func TestWatchErrorFallsBackToPolling(t *testing.T) {
	src := newSrc()
	d := open[string, int](t, noWatch{src}, refset.Options{PollInterval: 10 * time.Millisecond})
	src.Upsert("a", 1)
	eventually(t, "version 1", func() bool { return d.Version() == 1 })
}

// G4, headline case: the replica hasn't heard of the version (notification
// lost); the read kicks a poll and answers with fresh data well before the
// ticker would.
func TestGetAtLeastKicksPoll(t *testing.T) {
	src := newSrc()
	src.Upsert("u1", 0) // free
	d := open(t, src, quiet(refset.Options{WaitBound: 5 * time.Second}))
	src.SetDropNotifications(true)
	v := src.Upsert("u1", 1) // pro

	start := time.Now()
	got, ok, err := d.GetAtLeast(context.Background(), "u1", v)
	if err != nil || !ok || got != 1 {
		t.Fatalf("GetAtLeast = %d, %v, %v", got, ok, err)
	}
	if time.Since(start) > time.Second {
		t.Fatalf("took %v; should not wait for the ticker", time.Since(start))
	}
	if d.Version() < v {
		t.Fatal("version not applied")
	}
}

func TestGetAtLeastAlreadyApplied(t *testing.T) {
	src := newSrc()
	src.Upsert("a", 1)
	d := open(t, src, quiet(refset.Options{}))
	calls := src.ChangesCalls()
	if v, ok, err := d.GetAtLeast(context.Background(), "a", 1); err != nil || !ok || v != 1 {
		t.Fatalf("got %d %v %v", v, ok, err)
	}
	if src.ChangesCalls() != calls {
		t.Fatal("satisfied read called the source")
	}
}

// N concurrent requests for an unknown version share polls: at
// most one in flight plus one queued.
func TestConcurrentRequestsCoalescePolls(t *testing.T) {
	src := newSrc()
	d := open(t, src, quiet(refset.Options{WaitBound: 10 * time.Second}))
	src.SetDropNotifications(true)
	gate := make(chan struct{})
	src.SetChangesGate(gate)
	v := src.Upsert("a", 7)

	const n = 500
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got, ok, err := d.GetAtLeast(context.Background(), "a", v)
			if err == nil && (!ok || got != 7) {
				err = errors.New("wrong value")
			}
			errs <- err
		}()
	}
	eventually(t, "all requests waiting", func() bool { return d.Stats().Waiters == n })
	eventually(t, "first poll in flight", func() bool { return src.ChangesCalls() == 1 })
	close(gate)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(20 * time.Millisecond) // let a queued follow-up poll run
	if c := src.ChangesCalls(); c > 2 {
		t.Fatalf("%d requests caused %d Changes calls, want <= 2", n, c)
	}
}

// ErrVersionAhead only after a poll that started after the request
// arrived. A poll already in flight doesn't count.
func TestVersionAheadNeedsPollStartedAfterRequest(t *testing.T) {
	src := newSrc()
	d := open(t, src, quiet(refset.Options{WaitBound: 10 * time.Second}))
	src.SetDropNotifications(true)
	gate := make(chan struct{})
	src.SetChangesGate(gate)

	// A asks for version 1 while the source is at 0: a poll starts, reads
	// head 0 and stalls.
	a := goGet(context.Background(), d, "k", 1)
	eventually(t, "poll 1 in flight", func() bool { return src.ChangesCalls() == 1 })

	// The write commits, then B arrives asking for it.
	src.Upsert("k", 42)
	b := goGet(context.Background(), d, "k", 1)
	eventually(t, "both waiting", func() bool { return d.Stats().Waiters == 2 })
	close(gate)

	if r := recv(t, a); !errors.Is(r.err, refset.ErrVersionAhead) {
		t.Fatalf("A: %+v, want ErrVersionAhead", r)
	}
	if r := recv(t, b); r.err != nil || !r.ok || r.v != 42 {
		t.Fatalf("B: %+v, want 42 (the in-flight poll must not count for B)", r)
	}
}

// A forged far-ahead version costs one shared poll and no fallback.
func TestVersionAheadSkipsFallback(t *testing.T) {
	mem := newSrc()
	mem.Upsert("a", 1)
	src := refsettest.WithKeyLoader(mem)
	d := open[string, int](t, src, quiet(refset.Options{WaitBound: 10 * time.Second}))

	start := time.Now()
	_, _, err := d.GetAtLeast(context.Background(), "a", 999_999_999)
	if !errors.Is(err, refset.ErrVersionAhead) {
		t.Fatalf("err = %v", err)
	}
	if time.Since(start) > time.Second || src.LoadKeyCalls() != 0 {
		t.Fatalf("took %v, %d fallback calls", time.Since(start), src.LoadKeyCalls())
	}
}

// stall makes the replica unable to catch up: notifications dropped and every
// poll blocked until the test ends.
func stall(t *testing.T, src *refsettest.MemSource[string, int]) {
	src.SetDropNotifications(true)
	gate := make(chan struct{})
	src.SetChangesGate(gate)
	t.Cleanup(func() { close(gate) })
}

func TestTimeoutFallsBackToKeyLoader(t *testing.T) {
	mem := newSrc()
	mem.Upsert("u1", 0)
	src := refsettest.WithKeyLoader(mem)
	d := open[string, int](t, src, quiet(refset.Options{WaitBound: 30 * time.Millisecond}))
	stall(t, mem)
	v := mem.Upsert("u1", 1)

	got, ok, err := d.GetAtLeast(context.Background(), "u1", v)
	if err != nil || !ok || got != 1 {
		t.Fatalf("fallback = %d, %v, %v", got, ok, err)
	}
	if src.LoadKeyCalls() != 1 || d.Stats().Fallbacks != 1 {
		t.Fatalf("LoadKey calls %d, stats %+v", src.LoadKeyCalls(), d.Stats())
	}
	if d.Version() >= v {
		t.Fatal("fallback answer must not advance the replica")
	}
	if d.Stats().Waiters != 0 {
		t.Fatal("waiter leaked after timeout")
	}
}

// G4: without a KeyLoader, a timeout is ErrNotCaughtUp, never stale data.
func TestTimeoutWithoutKeyLoader(t *testing.T) {
	src := newSrc()
	src.Upsert("u1", 0)
	d := open(t, src, quiet(refset.Options{WaitBound: 30 * time.Millisecond}))
	stall(t, src)
	v := src.Upsert("u1", 1)

	if _, _, err := d.GetAtLeast(context.Background(), "u1", v); !errors.Is(err, refset.ErrNotCaughtUp) {
		t.Fatalf("err = %v, want ErrNotCaughtUp", err)
	}
}

func TestWaitBoundNeverWaits(t *testing.T) {
	src := newSrc()
	d := open(t, src, quiet(refset.Options{WaitBound: -1}))
	start := time.Now()
	if _, _, err := d.GetAtLeast(context.Background(), "a", 5); !errors.Is(err, refset.ErrNotCaughtUp) {
		t.Fatalf("err = %v", err)
	}
	if time.Since(start) > 100*time.Millisecond || src.ChangesCalls() != 0 {
		t.Fatal("WaitBound -1 waited or polled")
	}
}

func TestFallbacksCollapsePerKey(t *testing.T) {
	mem := newSrc()
	src := refsettest.WithKeyLoader(mem)
	d := open[string, int](t, src, quiet(refset.Options{WaitBound: 10 * time.Millisecond}))
	stall(t, mem)
	v := mem.Upsert("hot", 5)
	loadGate := make(chan struct{})
	src.SetLoadKeyGate(loadGate)

	const n = 50
	results := make([]<-chan result, n)
	for i := range n {
		results[i] = goGet(context.Background(), d, "hot", v)
	}
	eventually(t, "all requests in fallback", func() bool { return d.Stats().Fallbacks == n })
	close(loadGate)
	for _, ch := range results {
		if r := recv(t, ch); r.err != nil || r.v != 5 {
			t.Fatalf("result %+v", r)
		}
	}
	if c := src.LoadKeyCalls(); c != 1 {
		t.Fatalf("LoadKey called %d times, want 1", c)
	}
}

func TestMaxConcurrentFallbacks(t *testing.T) {
	mem := newSrc()
	src := refsettest.WithKeyLoader(mem)
	d := open[string, int](t, src, quiet(refset.Options{WaitBound: 10 * time.Millisecond, MaxConcurrentFallbacks: 1}))
	stall(t, mem)
	v := mem.Upsert("a", 1)
	mem.Upsert("b", 2)
	loadGate := make(chan struct{})
	src.SetLoadKeyGate(loadGate)

	a := goGet(context.Background(), d, "a", v)
	eventually(t, "fallback for a in flight", func() bool { return src.LoadKeyCalls() == 1 })
	if _, _, err := d.GetAtLeast(context.Background(), "b", v); !errors.Is(err, refset.ErrNotCaughtUp) {
		t.Fatalf("b over the cap: %v", err)
	}
	close(loadGate)
	if r := recv(t, a); r.err != nil || r.v != 1 {
		t.Fatalf("a: %+v", r)
	}
}

func TestContextCancelStopsWait(t *testing.T) {
	src := newSrc()
	d := open(t, src, quiet(refset.Options{WaitBound: 10 * time.Second}))
	stall(t, src)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, _, err := d.GetAtLeast(ctx, "a", src.Upsert("a", 1)); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v", err)
	}
	if d.Stats().Waiters != 0 {
		t.Fatal("waiter leaked after cancel")
	}
}

func TestGetForRequestUsesContextVersion(t *testing.T) {
	src := newSrc()
	src.Upsert("u1", 0)
	d := open(t, src, quiet(refset.Options{WaitBound: 5 * time.Second}))
	src.SetDropNotifications(true)
	v := src.Upsert("u1", 1)

	// No version in context: plain local read.
	if got, _, err := d.GetForRequest(context.Background(), "u1"); err != nil || got != 0 {
		t.Fatalf("no version: %d %v", got, err)
	}
	// Another dataset's version is ignored.
	other := refset.ContextWithMinVersion(context.Background(), "perms", 99)
	if got, _, err := d.GetForRequest(other, "u1"); err != nil || got != 0 {
		t.Fatalf("other dataset: %d %v", got, err)
	}
	ctx := refset.ContextWithMinVersion(context.Background(), "plans", v)
	if got, ok, err := d.GetForRequest(ctx, "u1"); err != nil || !ok || got != 1 {
		t.Fatalf("with version: %d %v %v", got, ok, err)
	}
}

func TestCloseReleasesWaiters(t *testing.T) {
	src := newSrc()
	d, err := refset.Open(context.Background(), src, quiet(refset.Options{WaitBound: 10 * time.Second}))
	if err != nil {
		t.Fatal(err)
	}
	stall(t, src)
	r := goGet(context.Background(), d, "a", src.Upsert("a", 1))
	eventually(t, "waiting", func() bool { return d.Stats().Waiters == 1 })
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	if res := recv(t, r); !errors.Is(res.err, refset.ErrClosed) {
		t.Fatalf("waiter: %+v", res)
	}
	if _, _, err := d.GetAtLeast(context.Background(), "a", 100); !errors.Is(err, refset.ErrClosed) {
		t.Fatalf("after close: %v", err)
	}
	_ = d.Close() // idempotent
	d.Get("a")    // still served
}

// G3: a replica whose changes were compacted away reloads a full snapshot.
func TestReloadWhenChangesCompacted(t *testing.T) {
	src := newSrc()
	src.Upsert("a", 1)
	d := open(t, src, quiet(refset.Options{}))
	src.SetDropNotifications(true)
	src.Upsert("b", 2)
	src.Delete("a")
	src.Compact()

	if v, ok, err := d.GetAtLeast(context.Background(), "b", 3); err != nil || !ok || v != 2 {
		t.Fatalf("after reload: %d %v %v", v, ok, err)
	}
	if _, ok := d.Get("a"); ok {
		t.Fatal("deleted key survived reload")
	}
	if st := d.Stats(); st.Reloads != 1 || st.Entries != 1 {
		t.Fatalf("stats %+v", st)
	}
}

func TestReloadInterval(t *testing.T) {
	src := newSrc()
	src.Upsert("a", 1)
	d := open(t, src, quiet(refset.Options{ReloadInterval: 10 * time.Millisecond}))
	eventually(t, "periodic reloads", func() bool { return d.Stats().Reloads >= 2 })
	if v, _ := d.Get("a"); v != 1 {
		t.Fatal("data lost across reloads")
	}
}

// A failing source doesn't break reads, counts errors, and recovers.
func TestPollErrorsAndRecovery(t *testing.T) {
	src := newSrc()
	src.Upsert("a", 1)
	d := open(t, src, quiet(refset.Options{WaitBound: 50 * time.Millisecond}))
	src.SetDropNotifications(true)
	src.SetChangesError(errors.New("connection refused"))
	v := src.Upsert("a", 2)

	if _, _, err := d.GetAtLeast(context.Background(), "a", v); !errors.Is(err, refset.ErrNotCaughtUp) {
		t.Fatalf("while failing: %v", err)
	}
	if got, _ := d.Get("a"); got != 1 || d.Stats().PollErrors == 0 {
		t.Fatalf("Get = %d, stats %+v", got, d.Stats())
	}
	src.SetChangesError(nil)
	time.Sleep(60 * time.Millisecond) // past the kick backoff
	if got, _, err := d.GetAtLeast(context.Background(), "a", v); err != nil || got != 2 {
		t.Fatalf("after recovery: %d %v", got, err)
	}
}

// A source that breaks the contract is treated as a failed poll, not applied.
type badSource struct {
	*refsettest.MemSource[string, int]
}

func (badSource) Changes(context.Context, uint64) (uint64, []refset.Delta[string, int], error) {
	return 1, []refset.Delta[string, int]{{Version: 5, Ops: []refset.Op[string, int]{{Key: "x", Value: 1}}}}, nil
}

func TestContractViolationIsAnError(t *testing.T) {
	d := open[string, int](t, badSource{newSrc()}, refset.Options{PollInterval: 5 * time.Millisecond})
	eventually(t, "poll errors", func() bool { return d.Stats().PollErrors >= 2 })
	if _, ok := d.Get("x"); ok || d.Version() != 0 {
		t.Fatal("invalid changes were applied")
	}
}

// History reset: a reload may move the version backwards; it is logged, not fatal.
func TestHistoryResetReloads(t *testing.T) {
	src := newSrc()
	for i := range 5 {
		src.Upsert("k", i)
	}
	d := open(t, src, refset.Options{PollInterval: 5 * time.Millisecond})
	src.Restore(1, map[string]int{"k": 100}) // a backup from version 1
	eventually(t, "reload after reset", func() bool { return d.Stats().Reloads >= 1 })
	if v, _ := d.Get("k"); v != 100 || d.Version() != 1 {
		t.Fatalf("after reset: k=%d v%d", v, d.Version())
	}
}

// GetCtx is kept for one release as a deprecated alias of GetForRequest.
func TestGetCtxStillWorks(t *testing.T) {
	src := newSrc()
	v := src.Upsert("u1", 1)
	d := open(t, src, quiet(refset.Options{}))
	ctx := refset.ContextWithMinVersion(context.Background(), d.Name(), v)
	if got, ok, err := d.GetCtx(ctx, "u1"); err != nil || !ok || got != 1 {
		t.Fatalf("GetCtx = %d %v %v", got, ok, err)
	}
}
