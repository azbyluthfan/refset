package refset_test

import (
	"context"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/azbyluthfan/refset"
)

func benchDataset(b *testing.B, n int, opts refset.Options) (*refset.Dataset[string, int], []string) {
	b.Helper()
	src := newSrc()
	keys := make([]string, n)
	ops := make([]refset.Op[string, int], n)
	for i := range n {
		keys[i] = "key-" + strconv.Itoa(i)
		ops[i] = refset.Op[string, int]{Key: keys[i], Value: i}
	}
	src.Apply(ops...)
	d, err := refset.Open(context.Background(), src, opts)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = d.Close() })
	return d, keys
}

func parallelGets(b *testing.B, d *refset.Dataset[string, int], keys []string) {
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			d.Get(keys[i%len(keys)])
			i += 7919 // a prime stride spreads reads over shards
		}
	})
}

// BenchmarkGetParallelIdle is the baseline: no background writes.
func BenchmarkGetParallelIdle(b *testing.B) {
	d, keys := benchDataset(b, 10_000, refset.Options{PollInterval: time.Hour})
	parallelGets(b, d, keys)
}

// BenchmarkGetParallelWhilePolling measures reads while the background loop
// polls continuously, writing its counters, timestamps and waiter state on
// every poll (rule R2: those writes must not invalidate the cache lines Get
// reads). A separate goroutine keeps asking for a version beyond the head,
// which forces a poll per request: the worst case of a forged-header flood.
// Readers use all but two cores so the loop and the kicker aren't starved.
func BenchmarkGetParallelWhilePolling(b *testing.B) { readersWithLoop(b, true) }

// BenchmarkGetParallelQuiet is the same harness with the loop idle, for
// comparison with BenchmarkGetParallelWhilePolling.
func BenchmarkGetParallelQuiet(b *testing.B) { readersWithLoop(b, false) }

func readersWithLoop(b *testing.B, kick bool) {
	d, keys := benchDataset(b, 10_000, refset.Options{PollInterval: time.Hour, WaitBound: time.Second})
	stop := make(chan struct{})
	kicked := make(chan struct{})
	go func() {
		defer close(kicked)
		for kick {
			select {
			case <-stop:
				return
			default:
				_, _, _ = d.GetAtLeast(context.Background(), keys[0], 1<<60)
			}
		}
	}()

	readers := max(runtime.GOMAXPROCS(0)-2, 1)
	polls0 := d.Stats().Polls
	b.ReportAllocs()
	b.ResetTimer()
	var wg sync.WaitGroup
	per := b.N/readers + 1
	for r := range readers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			i := r * 1013
			for range per {
				d.Get(keys[i%len(keys)])
				i += 7919
			}
		}()
	}
	wg.Wait()
	b.StopTimer()
	close(stop)
	<-kicked
	b.ReportMetric(float64(d.Stats().Polls-polls0)/b.Elapsed().Seconds(), "polls/s")
}

// BenchmarkOpen measures startup: a full snapshot load into shards, which
// every restarted replica pays (design §4.4).
func BenchmarkOpen(b *testing.B) {
	for _, n := range []int{10_000, 100_000, 1_000_000} {
		b.Run("entries="+strconv.Itoa(n), func(b *testing.B) {
			src := newSrc()
			ops := make([]refset.Op[string, int], n)
			for i := range n {
				ops[i] = refset.Op[string, int]{Key: "key-" + strconv.Itoa(i), Value: i}
			}
			src.Apply(ops...)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				d, err := refset.Open(context.Background(), src, refset.Options{PollInterval: time.Hour})
				if err != nil {
					b.Fatal(err)
				}
				b.StopTimer()
				_ = d.Close()
				b.StartTimer()
			}
		})
	}
}
