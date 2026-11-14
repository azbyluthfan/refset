package refset_test

import (
	"context"
	"fmt"
	"math/rand/v2"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/azbyluthfan/refset"
)

// G2: each version moves value between two keys, keeping their sum constant.
// 64 readers taking snapshots must always see the constant sum, under -race.
func TestConcurrentReadersSeeAtomicBatches(t *testing.T) {
	if runtime.GOARCH == "wasm" {
		t.Skip("needs goroutines running in parallel; WebAssembly runs Go on one thread without preemption")
	}
	const total = 1000
	src := newSrc()
	src.Apply(
		refset.Op[string, int]{Key: "a", Value: total / 2},
		refset.Op[string, int]{Key: "b", Value: total / 2},
	)
	for i := range 200 { // unrelated keys spread over shards
		src.Upsert(string(rune('A'+i%26))+string(rune('0'+i/26)), i)
	}
	d := open(t, src, refset.Options{Shards: 16, BatchWindow: time.Millisecond, PollInterval: 2 * time.Millisecond, WaitBound: 10 * time.Second})

	stop := make(chan struct{})
	var wg sync.WaitGroup
	var reads, violations atomic.Int64
	for range 64 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				s := d.Snapshot()
				a, _ := s.Get("a")
				b, _ := s.Get("b")
				if a+b != total {
					violations.Add(1)
				}
				d.Get("a")
				reads.Add(1)
			}
		}()
	}

	rng := rand.New(rand.NewPCG(1, 2))
	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		st := src.State()
		amt := rng.IntN(50) - 25
		src.Apply(
			refset.Op[string, int]{Key: "a", Value: st["a"] - amt},
			refset.Op[string, int]{Key: "b", Value: st["b"] + amt},
		)
	}
	head := src.Head()
	if _, _, err := d.GetAtLeast(context.Background(), "a", head); err != nil {
		t.Fatal(err)
	}
	close(stop)
	wg.Wait()

	if v := violations.Load(); v != 0 {
		t.Fatalf("%d of %d reads saw a partial batch", v, reads.Load())
	}
	if d.Version() != head {
		t.Fatalf("version %d, head %d", d.Version(), head)
	}
	// Iteration over a snapshot is stable and sums correctly.
	s := d.Snapshot()
	sum := 0
	for k, v := range s.All() {
		if k == "a" || k == "b" {
			sum += v
		}
	}
	if sum != total {
		t.Fatalf("iteration sum %d", sum)
	}
}

// G4: many concurrent waiters for different versions are all released with
// data at least as new as they asked for, and none leak.
func TestManyWaitersReleasedInOrder(t *testing.T) {
	baseline := runtime.NumGoroutine()
	src := newSrc()
	d, err := refset.Open(context.Background(), src, refset.Options{WaitBound: 5 * time.Second, BatchWindow: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}

	// Readers ask for versions that are committed at the source (as a client
	// echoing a writer's version would) while the replica is still catching up.
	const writes, readers = 300, 200
	src.Upsert("k", 1) // value == version
	var wg sync.WaitGroup
	errs := make(chan error, readers)
	for i := range readers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			minV := 1 + uint64(rand.IntN(int(src.Head())))
			v, ok, err := d.GetAtLeast(context.Background(), "k", minV)
			switch {
			case err != nil:
				errs <- fmt.Errorf("reader %d (min %d): %w", i, minV, err)
			case !ok || uint64(v) < minV:
				errs <- fmt.Errorf("reader %d: got version %d, asked for %d", i, v, minV)
			}
		}()
	}
	for i := 2; i <= writes; i++ {
		src.Upsert("k", i)
		runtime.Gosched()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	eventually(t, "goroutines back to baseline", func() bool { return runtime.NumGoroutine() <= baseline })
}
