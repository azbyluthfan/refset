package refset_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/azbyluthfan/refset"
	"github.com/azbyluthfan/refset/refsettest"
)

// panicky wraps a MemSource and panics in Changes or LoadKey while told to.
type panicky struct {
	*refsettest.MemSource[string, int]
	changes atomic.Bool
	loadKey atomic.Bool
	gate    chan struct{} // LoadKey waits here, so followers join the flight
}

func (p *panicky) Changes(ctx context.Context, after uint64) (uint64, []refset.Delta[string, int], error) {
	if p.changes.Load() {
		panic("source bug in Changes")
	}
	return p.MemSource.Changes(ctx, after)
}

func (p *panicky) LoadKey(context.Context, string) (int, bool, error) {
	if p.gate != nil {
		<-p.gate
	}
	if p.loadKey.Load() {
		panic("source bug in LoadKey")
	}
	return 42, true, nil
}

// A panic in the background loop fails that poll: the dataset keeps serving
// its last version, counts the panic, fails closed, and recovers when the
// source behaves again. The process doesn't crash.
func TestPanicInChangesFailsThePoll(t *testing.T) {
	mem := newSrc()
	mem.Upsert("a", 1)
	src := &panicky{MemSource: mem}
	clock := refsettest.NewFakeClock(time.Date(2026, 11, 14, 9, 0, 0, 0, time.UTC))
	d := open(t, refset.Source[string, int](src), refset.Options{Clock: clock, PollInterval: time.Second, MaxStaleness: 2 * time.Second})
	eventually(t, "poll ticker armed", func() bool { return clock.Timers() == 1 })

	src.changes.Store(true)
	mem.Upsert("a", 2)
	eventually(t, "panic recovered", func() bool { return d.Stats().Panics == 1 })
	if v, _ := d.Get("a"); v != 1 {
		t.Fatalf("a = %d after a failed poll, want the last version's 1", v)
	}
	if st := d.Stats(); st.PollErrors == 0 {
		t.Fatal("the panic wasn't counted as a poll error")
	}
	clock.Advance(3 * time.Second)
	eventually(t, "fails closed while the source panics", func() bool {
		_, _, err := d.GetFresh("a")
		return errors.Is(err, refset.ErrTooStale)
	})

	src.changes.Store(false)
	// Fake timers fire only if armed when the clock moves: wait for the loop
	// to re-arm its poll timer after the failed poll.
	eventually(t, "poll timer re-armed", func() bool { return clock.Timers() == 1 })
	clock.Advance(time.Second)
	eventually(t, "recovered", func() bool { v, _ := d.Get("a"); return v == 2 })
	if _, _, err := d.GetFresh("a"); err != nil {
		t.Fatalf("GetFresh after recovery: %v", err)
	}
}

// Several reads waiting on one fallback call must all get an error when it
// panics, never a "no such key" answer.
func TestPanicInFallbackReachesEveryWaiter(t *testing.T) {
	mem := newSrc()
	src := &panicky{MemSource: mem, gate: make(chan struct{})}
	src.loadKey.Store(true)
	d := open(t, refset.Source[string, int](src), quiet(refset.Options{WaitBound: time.Millisecond}))
	mem.SetChangesGate(make(chan struct{})) // the dataset can't catch up: reads fall back
	v := mem.Upsert("a", 1)

	const readers = 8
	var wg sync.WaitGroup
	results := make(chan error, readers)
	for range readers {
		wg.Go(func() {
			_, ok, err := d.GetAtLeast(context.Background(), "a", v)
			if err == nil && !ok {
				err = errors.New("answered 'no such key' without an error")
			}
			results <- err
		})
	}
	time.Sleep(50 * time.Millisecond) // let readers join the one flight
	close(src.gate)
	wg.Wait()
	close(results)
	for err := range results {
		var pe *refset.PanicError
		if !errors.As(err, &pe) {
			t.Fatalf("a reader got %v, want a *refset.PanicError", err)
		}
	}
}

type panicOnLoad struct {
	*refsettest.MemSource[string, int]
}

func (panicOnLoad) LoadSnapshot(context.Context, func(string, int)) (uint64, error) {
	panic("source bug in LoadSnapshot")
}

func TestPanicInOpenIsAnError(t *testing.T) {
	_, err := refset.Open(context.Background(), refset.Source[string, int](panicOnLoad{newSrc()}), refset.Options{})
	var pe *refset.PanicError
	if !errors.As(err, &pe) || len(pe.Stack) == 0 {
		t.Fatalf("Open: %v, want a *refset.PanicError with a stack", err)
	}
}

// stuck ignores context cancellation in Changes.
type stuck struct {
	*refsettest.MemSource[string, int]
	entered chan struct{}
	release chan struct{}
}

func (s stuck) Changes(context.Context, uint64) (uint64, []refset.Delta[string, int], error) {
	s.entered <- struct{}{}
	<-s.release
	return 0, nil, errors.New("released")
}

func TestCloseDoesntHangOnAStuckSource(t *testing.T) {
	restore := refset.SetCloseWaitForTest(100 * time.Millisecond)
	defer restore()
	src := stuck{newSrc(), make(chan struct{}, 1), make(chan struct{})}
	d, err := refset.Open(context.Background(), refset.Source[string, int](src), refset.Options{PollInterval: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	<-src.entered
	start := time.Now()
	err = d.Close()
	if err == nil || time.Since(start) > 2*time.Second {
		t.Fatalf("Close = %v after %v; want an error after about 100ms", err, time.Since(start))
	}
	close(src.release) // let the stuck call finish
}
