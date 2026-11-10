package main

import (
	"context"
	"fmt"
	"math/rand/v2"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/azbyluthfan/refset"
)

// load is the write pattern applied while something is measured.
type load struct {
	name   string
	rate   int  // writes per second
	reload bool // every write is a full reload
}

const readBatch = 16

// tailSection measures read latency percentiles. Each sample is the time for
// readBatch random reads, so the p50 divided by readBatch is the typical cost
// of one read, and a stall anywhere in the batch (a GC pause, a cache flush
// after a swap, lock contention) shows in full in the tail.
func tailSection(cfg config) {
	fmt.Printf("\n## Read latency under load\n\n")
	fmt.Printf("%d reader goroutines, each timing batches of %d random reads for %v per cell. Percentiles are per batch.\n\n",
		cfg.readers, readBatch, cfg.dur)
	fmt.Println("| Entries | Load | Store | p50 | p99 | p99.9 | max | per read (p50) |")
	fmt.Println("|---|---|---|---|---|---|---|---|")
	loads := []load{
		{name: "idle"},
		{name: "1,000 changes/s", rate: 1000},
		{name: "full reload every 1s", rate: 1, reload: true},
	}
	for _, n := range cfg.sizes {
		keys, vals := dataOf(n)
		order := make([]int32, n)
		for i := range order {
			order[i] = int32(i)
		}
		rand.New(rand.NewPCG(1, 2)).Shuffle(n, func(i, j int) { order[i], order[j] = order[j], order[i] })

		for _, l := range loads {
			for _, store := range []string{"refset", "RWMutex map"} {
				var t target
				if store == "refset" {
					t = newRefset(keys, vals, l.reload, refsetOpts(cfg))
				} else {
					t = newMap(keys, vals, l.reload)
				}
				h := measureReads(cfg, t, keys, order, l)
				t.close()
				fmt.Printf("| %s | %s | %s | %s | %s | %s | %s | %s |\n", fmtN(n), l.name, store,
					fmtDur(h.quantile(0.5)), fmtDur(h.quantile(0.99)), fmtDur(h.quantile(0.999)),
					fmtDur(h.quantile(1)), fmtDur(h.quantile(0.5)/readBatch))
			}
		}
	}
}

func measureReads(cfg config, t target, keys []string, order []int32, l load) *hist {
	runtime.GC()
	stop := make(chan struct{})
	var wg sync.WaitGroup
	hs := make([]*hist, cfg.readers)
	for r := range cfg.readers {
		hs[r] = new(hist)
		wg.Go(func() {
			h := hs[r]
			i := r * len(order) / cfg.readers
			for {
				select {
				case <-stop:
					return
				default:
				}
				start := time.Now()
				for range readBatch {
					t.get(keys[order[i]])
					if i++; i == len(order) {
						i = 0
					}
				}
				h.record(int64(time.Since(start)))
			}
		})
	}
	wg.Go(func() {
		pace(l.rate, stop, func(i int) { t.write(keyFor(i, len(keys)), planOf(i)) })
	})
	time.Sleep(cfg.dur)
	close(stop)
	wg.Wait()
	total := new(hist)
	for _, h := range hs {
		total.merge(h)
	}
	return total
}

// propSection measures commit-to-visible latency on a replica: the writer
// commits a change at the source, then waits until the dataset serves it.
func propSection(cfg config) {
	const n = 100_000
	fmt.Printf("\n## Propagation latency\n\n")
	fmt.Printf("%s entries, %d changes per case, one at a time with a gap between them. Time from the source committing a change to the replica serving it.\n\n", fmtN(n), cfg.samples)
	fmt.Println("| Path | p50 | p99 | max |")
	fmt.Println("|---|---|---|---|")
	keys, vals := dataOf(n)
	opts := refsetOpts(cfg)

	type pcase struct {
		name   string
		reload bool
		drop   bool
		poll   time.Duration
		ryw    bool          // read with GetAtLeast(version) instead of watching Version
		gap    time.Duration // plus a random 0..gap, so changes do not phase-lock with polls
	}
	cases := []pcase{
		{name: "notification (default BatchWindow 5ms)", gap: 20 * time.Millisecond},
		{name: "read-your-writes: GetAtLeast(version)", ryw: true, gap: 20 * time.Millisecond},
		{name: "polling only, PollInterval 100ms", drop: true, poll: 100 * time.Millisecond, gap: 100 * time.Millisecond},
		{name: "notification, full reload per change", reload: true, gap: 20 * time.Millisecond},
	}
	for _, c := range cases {
		o := opts
		if c.poll > 0 {
			o.PollInterval = c.poll
		}
		t := newRefset(keys, vals, c.reload, o)
		t.f.drop.Store(c.drop)
		h := new(hist)
		for i := range cfg.samples {
			k := keyFor(i, n)
			start := time.Now()
			v := t.f.upsert(k, planOf(i+1))
			if c.ryw {
				if _, _, err := t.d.GetAtLeast(context.Background(), keys[k], v); err != nil {
					panic(err)
				}
			} else {
				for t.d.Version() < v {
					runtime.Gosched()
				}
			}
			h.record(int64(time.Since(start)))
			time.Sleep(c.gap + rand.N(c.gap))
		}
		t.close()
		fmt.Printf("| %s | %s | %s | %s |\n", c.name, fmtDur(h.quantile(0.5)), fmtDur(h.quantile(0.99)), fmtDur(h.quantile(1)))
	}
}

// gcSection measures what the garbage collector does while changes are
// applied, without readers: the cost the write path puts on the process.
func gcSection(cfg config) {
	fmt.Printf("\n## Garbage collection under writes\n\n")
	fmt.Printf("No readers; each cell runs %v. GC CPU is the runtime's estimate excluding idle-time marking, as a share of one core. Pause p99/max are bucket upper bounds.\n\n", cfg.gcDur)
	fmt.Println("| Entries | Writes | Store | GC CPU | GC cycles/s | Allocated/s | Mark assist | Pause p99 | Pause max |")
	fmt.Println("|---|---|---|---|---|---|---|---|---|")
	loads := []load{
		{name: "0/s"},
		{name: "10/s", rate: 10},
		{name: "100/s", rate: 100},
		{name: "1,000/s", rate: 1000},
		{name: "1/s, full reload each", rate: 1, reload: true},
	}
	for _, n := range cfg.sizes {
		if n < 100_000 && len(cfg.sizes) > 1 {
			continue // small datasets cost the GC little; keep the table short
		}
		keys, vals := dataOf(n)
		for _, l := range loads {
			for _, store := range []string{"refset", "RWMutex map"} {
				var t target
				if store == "refset" {
					t = newRefset(keys, vals, l.reload, refsetOpts(cfg))
				} else {
					t = newMap(keys, vals, l.reload)
				}
				runtime.GC()
				runtime.GC()
				stop := make(chan struct{})
				var wg sync.WaitGroup
				wg.Go(func() {
					pace(l.rate, stop, func(i int) { t.write(keyFor(i, n), planOf(i)) })
				})
				a := readGC()
				time.Sleep(cfg.gcDur)
				close(stop)
				wg.Wait()
				d := diffGC(a, readGC())
				t.close()
				fmt.Printf("| %s | %s | %s | %.1f%% | %.1f | %s | %s | %s | %s |\n", fmtN(n), l.name, store,
					100*d.gcCores, float64(d.cycles)/d.wall.Seconds(), fmtBytes(d.allocRate),
					fmtDur(d.assist), fmtDur(d.pauseP99), fmtDur(d.pauseMax))
			}
		}
	}

	// Holding a dataset also costs every GC cycle the application's own
	// allocations trigger: the collector marks the whole live heap, dataset
	// included. No dataset writes here; the "app" allocates steadily.
	fmt.Printf("\nHolding a dataset while the application allocates %s/s (no dataset writes, %v per cell). The difference from \"nothing held\" is the GC cost of keeping the dataset in memory.\n\n",
		fmtBytes(appAllocRate), cfg.gcDur)
	fmt.Println("| Entries | Held | GC CPU | GC cycles/s | Mark assist | Pause p99 |")
	fmt.Println("|---|---|---|---|---|---|")
	for _, n := range cfg.sizes {
		if n < 100_000 && len(cfg.sizes) > 1 {
			continue
		}
		for _, store := range []string{"nothing", "refset", "RWMutex map"} {
			var t target
			switch store {
			case "refset":
				keys, vals := dataOf(n)
				t = newRefset(keys, vals, false, refsetOpts(cfg))
			case "RWMutex map":
				keys, vals := dataOf(n)
				t = newMap(keys, vals, false)
			}
			runtime.GC()
			runtime.GC()
			stop := make(chan struct{})
			var wg sync.WaitGroup
			wg.Go(func() { pace(appAllocs, stop, func(int) { appSink = make([]byte, appAllocRate/appAllocs) }) })
			a := readGC()
			time.Sleep(cfg.gcDur)
			close(stop)
			wg.Wait()
			d := diffGC(a, readGC())
			if t != nil {
				t.close()
			}
			runtime.KeepAlive(t)
			fmt.Printf("| %s | %s | %.1f%% | %.1f | %s | %s |\n", fmtN(n), store,
				100*d.gcCores, float64(d.cycles)/d.wall.Seconds(), fmtDur(d.assist), fmtDur(d.pauseP99))
		}
	}
}

const (
	appAllocRate = 100 << 20 // bytes per second the simulated application allocates
	appAllocs    = 1000      // allocations per second
)

// appSink keeps simulated application allocations from being optimised away.
var appSink []byte

// memSection measures steady memory per entry and the heap peak during a
// full reload.
func memSection(cfg config) {
	fmt.Printf("\n## Memory\n\n")
	fmt.Println("Bytes per entry: live heap after GC, dataset storage only (keys and pointed-to values are shared with the source).")
	fmt.Println()
	fmt.Println("| Entries | Key, value | refset | map | refset ÷ map |")
	fmt.Println("|---|---|---|---|---|")
	for _, n := range cfg.sizes {
		r, m := perEntry(cfg, n, func(i int) string { return "key-" + fmt.Sprint(i) }, planOf)
		fmt.Printf("| %s | string, 24-byte struct | %.0f B | %.0f B | %.2f |\n", fmtN(n), r, m, r/m)
		r, m = perEntry(cfg, n, func(i int) int64 { return int64(i) }, func(i int) int64 { return int64(i) })
		fmt.Printf("| %s | int64, int64 | %.0f B | %.0f B | %.2f |\n", fmtN(n), r, m, r/m)
		r, m = perEntry(cfg, n, func(i int) string { return "key-" + fmt.Sprint(i) }, func(i int) *plan { p := planOf(i); return &p })
		fmt.Printf("| %s | string, pointer to struct | %.0f B | %.0f B | %.2f |\n", fmtN(n), r, m, r/m)
	}

	fmt.Println()
	fmt.Println("Heap during one full reload (string key, 24-byte struct): steady is the whole process after GC; peak is the highest heap sampled every millisecond from the start of the reload until it is served, including garbage not yet collected.")
	fmt.Println()
	fmt.Println("| Entries | Steady heap | Peak during reload | Peak ÷ steady | Reload time |")
	fmt.Println("|---|---|---|---|---|")
	for _, n := range cfg.sizes {
		keys, vals := dataOf(n)
		t := newRefset(keys, vals, true, refsetOpts(cfg))
		runtime.GC()
		runtime.GC()
		steady := heapBytes()
		var peak atomic.Uint64
		stop := make(chan struct{})
		var wg sync.WaitGroup
		wg.Go(func() {
			tick := time.NewTicker(time.Millisecond)
			defer tick.Stop()
			for {
				peak.Store(max(peak.Load(), heapBytes()))
				select {
				case <-stop:
					return
				case <-tick.C:
				}
			}
		})
		start := time.Now()
		v := t.f.upsert(0, planOf(1))
		for t.d.Version() < v {
			time.Sleep(100 * time.Microsecond)
		}
		took := time.Since(start)
		close(stop)
		wg.Wait()
		t.close()
		p := peak.Load()
		fmt.Printf("| %s | %s | %s | %.2f | %s |\n", fmtN(n), fmtBytes(float64(steady)), fmtBytes(float64(p)),
			float64(p)/float64(steady), fmtDur(took))
	}
}

// perEntry returns bytes per entry of a refset Dataset and of a plain map
// holding the same n entries.
func perEntry[K comparable, V any](cfg config, n int, key func(int) K, val func(int) V) (refsetB, mapB float64) {
	keys := make([]K, n)
	vals := make([]V, n)
	for i := range n {
		keys[i], vals[i] = key(i), val(i)
	}
	f := newFeed(keys, vals, false)
	base := settledHeap()

	d, err := refset.Open(context.Background(), f, refsetOpts(cfg))
	if err != nil {
		panic(err)
	}
	refsetB = float64(settledHeap()-base) / float64(n)
	_ = d.Close()
	runtime.KeepAlive(d)
	d = nil

	base = settledHeap()
	m := make(map[K]V, n)
	for i, k := range keys {
		m[k] = vals[i]
	}
	mapB = float64(settledHeap()-base) / float64(n)
	// Everything that existed at a base reading must still be alive at the
	// matching end reading, or the GC frees it in between and the store
	// looks smaller than it is (vals is otherwise dead after the loop).
	runtime.KeepAlive(m)
	runtime.KeepAlive(f)
	runtime.KeepAlive(keys)
	runtime.KeepAlive(vals)
	return refsetB, mapB
}

func settledHeap() uint64 {
	runtime.GC()
	runtime.GC()
	return heapBytes()
}
