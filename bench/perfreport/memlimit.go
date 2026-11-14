package main

import (
	"context"
	"fmt"
	"math"
	"runtime"
	"runtime/debug"
	"runtime/metrics"
	"time"

	"github.com/azbyluthfan/refset"
)

// memlimitSection sweeps GOMEMLIMIT down towards the dataset's size, with
// GOGC at 100 and off, and measures what the GC does while the application
// allocates, while the dataset receives changes, and during a full reload.
// It answers how close to the dataset a memory limit can be set.
func memlimitSection(cfg config) {
	fmt.Printf("\n## Memory limits\n\n")
	fmt.Printf("`GOMEMLIMIT` as a multiple of the process's steady Go memory (total minus memory returned to the OS, what the limit counts), with `GOGC` 100 and off. Each GC cell runs %v. Holding: the application allocates %s/s and the dataset receives no writes. Reload: one full reload changing one entry; the peak is the highest Go memory sampled during it, as a multiple of steady.\n\n",
		cfg.memlimitDur, fmtBytes(appAllocRate))
	fmt.Println("| Entries | Values | GOGC | Limit | Holding: GC CPU, collections/s | 1,000 changes/s: GC CPU, collections/s | Reload: time, peak |")
	fmt.Println("|---|---|---|---|---|---|---|")
	for _, n := range cfg.memlimitSizes {
		if cfg.memlimitKinds["comparable"] {
			sweep(cfg, n, "comparable (reloads keep shards)", planOf)
		}
		if cfg.memlimitKinds["uncomparable"] {
			sweep(cfg, n, "not comparable (reloads rebuild)", func(i int) taggedPlan { return taggedPlan{P: planOf(i)} })
		}
	}
}

// taggedPlan is plan plus a slice, so == can't compare it and every reload
// rebuilds the whole dataset.
type taggedPlan struct {
	P    plan
	Tags []string
}

func sweep[V any](cfg config, n int, kind string, mk func(int) V) {
	keys := make([]string, n)
	vals := make([]V, n)
	for i := range n {
		keys[i] = "key-" + fmt.Sprint(i)
		vals[i] = mk(i)
	}
	f := newFeed(keys, vals, false)
	d, err := refset.Open(context.Background(), f, refsetOpts(cfg))
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = d.Close()
		debug.SetGCPercent(100)
		debug.SetMemoryLimit(math.MaxInt64)
	}()
	settledHeap()
	steady := goMemory()

	type setting struct {
		gogc  int
		ratio float64 // 0: no limit
	}
	var settings []setting
	for _, g := range []int{100, -1} {
		for _, r := range []float64{0, 4, 2, 1.5, 1.25} {
			if g < 0 && r == 0 {
				continue // GOGC off without a limit never collects
			}
			settings = append(settings, setting{g, r})
		}
	}
	gen := 0
	for _, s := range settings {
		debug.SetGCPercent(s.gogc)
		limit := int64(math.MaxInt64)
		if s.ratio > 0 {
			limit = int64(s.ratio * float64(steady))
		}
		debug.SetMemoryLimit(limit)
		runtime.GC()

		hold := gcFor(cfg.memlimitDur, appAllocator)
		churn := gcFor(cfg.memlimitDur, func(stop <-chan struct{}) {
			pace(1000, stop, func(i int) { gen++; f.upsert(keyFor(i, n), mk(i+gen)) })
		})

		f.mu.Lock()
		f.reload = true
		f.mu.Unlock()
		peak := peakGoMemory(func() {
			gen++
			v := f.upsert(0, mk(gen))
			for d.Version() < v {
				time.Sleep(100 * time.Microsecond)
			}
		})
		f.mu.Lock()
		f.reload = false
		f.mu.Unlock()

		gogc, lim := "100", "none"
		if s.gogc < 0 {
			gogc = "off"
		}
		if s.ratio > 0 {
			lim = fmt.Sprintf("%.3g× (%s)", s.ratio, fmtBytes(float64(limit)))
		}
		fmt.Printf("| %s | %s | %s | %s | %.1f%%, %.2f | %.1f%%, %.2f | %s, %.2f× |\n", fmtN(n), kind, gogc, lim,
			100*hold.gcCores, float64(hold.cycles)/hold.wall.Seconds(),
			100*churn.gcCores, float64(churn.cycles)/churn.wall.Seconds(),
			fmtDur(peak.took), float64(peak.max)/float64(steady))

		debug.SetGCPercent(100)
		debug.SetMemoryLimit(math.MaxInt64)
	}
	runtime.KeepAlive(keys)
	runtime.KeepAlive(vals)
}

// goMemory is the memory GOMEMLIMIT counts: everything the Go runtime has
// mapped, minus heap memory already returned to the OS.
func goMemory() uint64 {
	s := []metrics.Sample{{Name: "/memory/classes/total:bytes"}, {Name: "/memory/classes/heap/released:bytes"}}
	metrics.Read(s)
	return s[0].Value.Uint64() - s[1].Value.Uint64()
}

type peakResult struct {
	took time.Duration
	max  uint64
}

// peakGoMemory runs fn while sampling goMemory every millisecond.
func peakGoMemory(fn func()) peakResult {
	stop := make(chan struct{})
	done := make(chan uint64)
	go func() {
		var m uint64
		tick := time.NewTicker(time.Millisecond)
		defer tick.Stop()
		for {
			m = max(m, goMemory())
			select {
			case <-stop:
				done <- m
				return
			case <-tick.C:
			}
		}
	}()
	start := time.Now()
	fn()
	took := time.Since(start)
	close(stop)
	return peakResult{took: took, max: <-done}
}
