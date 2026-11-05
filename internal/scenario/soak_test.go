package scenario_test

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/azbyluthfan/refset"
	"github.com/azbyluthfan/refset/refsettest"
)

// TestSoak is design §8.6, run manually before a release:
//
//	REFSET_SOAK=1h REFSET_SOAK_LOG=soak.log go test -run TestSoak -timeout 2h ./internal/scenario/
//
// Three replicas follow one source while a writer commits at a steady rate
// and a chaos loop randomly partitions replicas, drops their notifications,
// slows them, compacts the source and restarts replicas. Every check
// interval (default 1m) writes pause, faults heal, and every replica must
// reach the source's version with identical contents. It also watches for
// goroutine and heap growth across checks.
func TestSoak(t *testing.T) {
	total, err := time.ParseDuration(os.Getenv("REFSET_SOAK"))
	if err != nil {
		t.Skip("set REFSET_SOAK to a duration, e.g. REFSET_SOAK=1h, to run the soak test")
	}
	every := time.Minute
	if v, err := time.ParseDuration(os.Getenv("REFSET_SOAK_CHECK")); err == nil {
		every = v
	}
	logf := func(format string, args ...any) {
		line := fmt.Sprintf("%s "+format, append([]any{time.Now().Format(time.TimeOnly)}, args...)...)
		t.Log(line)
		if p := os.Getenv("REFSET_SOAK_LOG"); p != "" {
			if f, err := os.OpenFile(p, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644); err == nil {
				_, _ = fmt.Fprintln(f, line)
				_ = f.Close()
			}
		}
	}

	src := refsettest.NewMemSource[string, plan](plansDataset)
	opts := refset.Options{PollInterval: 100 * time.Millisecond, BatchWindow: 2 * time.Millisecond, Shards: 64}
	type node struct {
		mu  sync.Mutex
		rep *refsettest.Replica[string, plan]
		ds  *refset.Dataset[string, plan]
	}
	nodes := make([]*node, 3)
	for i := range nodes {
		r := src.NewReplica()
		d, err := refset.Open(context.Background(), r, opts)
		if err != nil {
			t.Fatal(err)
		}
		nodes[i] = &node{rep: r, ds: d}
	}
	defer func() {
		for _, n := range nodes {
			_ = n.ds.Close()
		}
	}()

	tiers := []string{"free", "pro", "team", "enterprise"}
	// gate fences writes and chaos actions: each holds it for reading while
	// it acts; a check takes it for writing, so nothing is half-done or starts
	// while replicas are compared.
	var gate sync.RWMutex
	stop := make(chan struct{})
	var wg sync.WaitGroup
	var writes, restarts, compactions atomic.Int64

	// Writer: about 200 versions per second over 2,000 keys.
	wg.Add(1)
	go func() {
		defer wg.Done()
		rng := rand.New(rand.NewPCG(1, 1))
		tick := time.NewTicker(5 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-stop:
				return
			case <-tick.C:
			}
			gate.RLock()
			key := fmt.Sprintf("user-%04d", rng.IntN(2000))
			src.Apply(refset.Op[string, plan]{Key: key, Value: plan{tiers[rng.IntN(4)]}, Delete: rng.IntN(10) == 0})
			writes.Add(1)
			gate.RUnlock()
		}
	}()

	// Chaos: one random fault every 500ms.
	wg.Add(1)
	go func() {
		defer wg.Done()
		rng := rand.New(rand.NewPCG(2, 2))
		tick := time.NewTicker(500 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-stop:
				return
			case <-tick.C:
			}
			gate.RLock()
			n := nodes[rng.IntN(len(nodes))]
			switch rng.IntN(6) {
			case 0:
				n.rep.SetPartitioned(rng.IntN(2) == 0)
			case 1:
				n.rep.SetDropNotifications(rng.IntN(2) == 0)
			case 2:
				n.rep.SetDelay(time.Duration(rng.IntN(20)) * time.Millisecond)
			case 3:
				src.Compact()
				compactions.Add(1)
			case 4: // restart the replica process
				n.mu.Lock()
				_ = n.ds.Close()
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				if d, err := refset.Open(ctx, n.rep, opts); err == nil {
					n.ds = d
					restarts.Add(1)
				} else {
					// Partitioned at restart: Open fails, as it should. Retry healed.
					n.rep.SetPartitioned(false)
					if d, err := refset.Open(context.Background(), n.rep, opts); err == nil {
						n.ds = d
						restarts.Add(1)
					}
				}
				cancel()
				n.mu.Unlock()
			default: // heal
				n.rep.SetPartitioned(false)
				n.rep.SetDropNotifications(false)
				n.rep.SetDelay(0)
			}
			gate.RUnlock()
		}
	}()

	str := func(p plan) string { return p.Tier }
	var baseGoroutines int
	var baseHeap uint64
	deadline := time.Now().Add(total)
	checks := 0
	for time.Now().Before(deadline) {
		time.Sleep(min(every, time.Until(deadline)))
		gate.Lock()
		for _, n := range nodes {
			n.rep.SetPartitioned(false)
			n.rep.SetDropNotifications(false)
			n.rep.SetDelay(0)
		}
		head := src.Head()
		want := contentHash(src.State(), str)
		for i, n := range nodes {
			n.mu.Lock()
			d := n.ds
			n.mu.Unlock()
			waitStart := time.Now()
			for d.Version() != head {
				if time.Since(waitStart) > 30*time.Second {
					t.Fatalf("check %d: replica %d stuck at version %d, head %d", checks, i, d.Version(), head)
				}
				time.Sleep(5 * time.Millisecond)
			}
			got := make(map[string]plan)
			for k, v := range d.Snapshot().All() {
				got[k] = v
			}
			if h := contentHash(got, str); h != want {
				t.Fatalf("check %d: replica %d content hash %x, source %x at version %d", checks, i, h, want, head)
			}
		}
		runtime.GC()
		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)
		g := runtime.NumGoroutine()
		checks++
		if checks == 1 {
			baseGoroutines, baseHeap = g, ms.HeapAlloc
		}
		logf("check %d ok: version %d, writes %d, restarts %d, compactions %d, goroutines %d, heap %.1f MiB",
			checks, head, writes.Load(), restarts.Load(), compactions.Load(), g, float64(ms.HeapAlloc)/(1<<20))
		gate.Unlock()
	}
	close(stop)
	wg.Wait()

	// Growth checks against the first check (after warm-up). AfterFunc
	// notification goroutines come and go, so allow some slack.
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	if g := runtime.NumGoroutine(); checks > 1 && g > baseGoroutines+20 {
		t.Errorf("goroutines grew from %d to %d", baseGoroutines, g)
	}
	if checks > 1 && ms.HeapAlloc > 2*baseHeap+(16<<20) {
		t.Errorf("heap grew from %.1f MiB to %.1f MiB", float64(baseHeap)/(1<<20), float64(ms.HeapAlloc)/(1<<20))
	}
	logf("done: %d checks, %d writes, %d restarts, %d compactions; goroutines %d (first check %d), heap %.1f MiB (first check %.1f MiB)",
		checks, writes.Load(), restarts.Load(), compactions.Load(), runtime.NumGoroutine(), baseGoroutines,
		float64(ms.HeapAlloc)/(1<<20), float64(baseHeap)/(1<<20))
}
