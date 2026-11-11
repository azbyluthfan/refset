package main

import (
	"context"
	"fmt"
	"maps"
	"math/rand/v2"
	"runtime"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/azbyluthfan/refset"
)

// scaleSection measures one large dataset of flat values (string key, 24-byte
// struct): Open, memory, reads, write and holding GC cost, and a full reload
// that changes one entry. refset only: a second copy in a locked map would not
// fit comfortably next to it on a small machine, and the 1M-entry tables
// already compare the two.
func scaleSection(cfg config) {
	n := cfg.scaleN
	fmt.Printf("\n## Large dataset: %s flat entries\n\n", fmtN(n))
	keys, vals := dataOf(n)
	base := settledHeap()

	start := time.Now()
	t := newRefset(keys, vals, false, refsetOpts(cfg))
	openTime := time.Since(start)
	steady := settledHeap()
	st := t.d.Stats()
	fmt.Println("| Measure | Value |")
	fmt.Println("|---|---|")
	fmt.Printf("| Open (in-process source) | %s |\n", fmtDur(openTime))
	fmt.Printf("| Shards chosen | %d |\n", st.Shards)
	fmt.Printf("| Dataset storage per entry | %.0f B |\n", float64(steady-base)/float64(n))
	fmt.Printf("| Whole heap after GC (keys, source copy, dataset) | %s |\n", fmtBytes(float64(steady)))

	order := randomOrder(n)
	for _, l := range []load{{name: "idle"}, {name: "1,000 changes/s", rate: 1000}} {
		h := measureReads(cfg, t, keys, order, l)
		fmt.Printf("| Reads, %s: p50 / p99 / p99.9 per %d reads (p50 per read) | %s / %s / %s (%s) |\n", l.name, readBatch,
			fmtDur(h.quantile(0.5)), fmtDur(h.quantile(0.99)), fmtDur(h.quantile(0.999)), fmtDur(h.quantile(0.5)/readBatch))
	}

	d := gcWhile(cfg, func(stop <-chan struct{}) {
		pace(1000, stop, func(i int) { t.write(keyFor(i, n), planOf(i)) })
	})
	fmt.Printf("| 1,000 changes/s: allocated, GC CPU, cycles/s, CPU per cycle | %s/s, %.1f%%, %.2f, %s |\n",
		fmtBytes(d.allocRate), 100*d.gcCores, float64(d.cycles)/d.wall.Seconds(), perCycle(d))
	d = gcWhile(cfg, appAllocator)
	fmt.Printf("| Holding it while the app allocates %s/s: GC CPU, cycles/s, CPU per cycle | %.1f%%, %.2f, %s |\n",
		fmtBytes(appAllocRate), 100*d.gcCores, float64(d.cycles)/d.wall.Seconds(), perCycle(d))

	// One full reload that changes a single entry, as ReloadSource would do:
	// values are comparable, so unchanged shards are kept.
	t.f.mu.Lock()
	t.f.reload = true
	t.f.mu.Unlock()
	r := measureReload(func() uint64 { return t.f.upsert(0, planOf(7)) }, t.d.Version)
	fmt.Printf("| Full reload changing one entry: time, allocated, heap peak ÷ steady | %s, %s, %.2f |\n",
		fmtDur(r.took), fmtBytes(float64(r.allocated)), r.peakRatio)
	t.close()
	runtime.KeepAlive(keys)
	runtime.KeepAlive(vals)
}

// flatRow is a flat, comparable value that carries its key, for ReloadSource.
type flatRow struct {
	ID string
	P  plan
}

func flatRowOf(i, gen int) flatRow { return flatRow{ID: "acct-" + strconv.Itoa(i), P: planOf(i + gen)} }

// account is a deeply nested value: slices, a map, slices of structs holding
// slices, and a pointer into them. Every Account is its own allocation graph.
type account struct {
	ID      string
	Plan    plan
	Tags    []string
	Limits  map[string]int
	Members []member
	Owner   *member
}

type member struct {
	Name  string
	Role  string
	Perms []string
}

var (
	roles = []string{"owner", "admin", "member", "viewer"}
	perms = []string{"read", "write", "billing", "invite", "export"}
)

func accountOf(i, gen int) account {
	a := account{
		ID:     "acct-" + strconv.Itoa(i),
		Plan:   planOf(i + gen),
		Tags:   []string{tiers[i%3], roles[i%4], perms[i%5]},
		Limits: map[string]int{"seats": 5 + i%50, "projects": 10, "api": 1000 + gen, "storage": 50},
	}
	a.Members = make([]member, 5)
	for m := range a.Members {
		a.Members[m] = member{
			Name:  "user-" + strconv.Itoa(i*5+m),
			Role:  roles[m%4],
			Perms: []string{perms[m%5], perms[(m+1)%5]},
		}
	}
	a.Owner = &a.Members[0]
	return a
}

func accountsEqual(a, b account) bool {
	if a.ID != b.ID || a.Plan != b.Plan || !slices.Equal(a.Tags, b.Tags) || !maps.Equal(a.Limits, b.Limits) ||
		len(a.Members) != len(b.Members) || (a.Owner == nil) != (b.Owner == nil) {
		return false
	}
	for i := range a.Members {
		x, y := a.Members[i], b.Members[i]
		if x.Name != y.Name || x.Role != y.Role || !slices.Equal(x.Perms, y.Perms) {
			return false
		}
	}
	return a.Owner == nil || a.Owner.Name == b.Owner.Name
}

// nestedSection compares flat values with deeply nested ones at the same
// sizes: memory, reads, GC cost of holding and changing them, and reloads
// through ReloadSource with and without an Equal function.
func nestedSection(cfg config) {
	fmt.Printf("\n## Nested values\n\n")
	fmt.Println("`account`: an ID, a 24-byte struct, a 3-string slice, a 4-entry map, 5 members each with a 2-string slice, and a pointer to the first member, about 20 heap objects per value. The flat value is the 24-byte struct used elsewhere. Nested data is shared between the source and the stores, as values loaded once would be.")
	fmt.Println()
	fmt.Printf("GC columns run %v each. Holding: the application allocates %s/s and the data receives no writes.\n\n", cfg.gcDurLarge, fmtBytes(appAllocRate))
	fmt.Println("| Entries | Value | Heap per entry, whole process | Read p50 per read | Holding: GC CPU, cycles/s, CPU per cycle | 1,000 changes/s: GC CPU, allocated |")
	fmt.Println("|---|---|---|---|---|---|")
	for _, n := range cfg.nestedSizes {
		keys := make([]string, n)
		for i := range keys {
			keys[i] = "acct-" + strconv.Itoa(i)
		}
		order := randomOrder(n)

		// Flat values.
		empty := settledHeap()
		flat := make([]plan, n)
		for i := range flat {
			flat[i] = planOf(i)
		}
		ft := newFeed(keys, flat, false)
		fd, err := refset.Open(context.Background(), ft, refsetOpts(cfg))
		if err != nil {
			panic(err)
		}
		nestedRow(cfg, n, "flat 24-byte struct", float64(settledHeap()-empty)/float64(n), keys, order,
			func(k string) bool { _, ok := fd.Get(k); return ok },
			func(i int) { ft.upsert(keyFor(i, n), planOf(i)) })
		_ = fd.Close()
		runtime.KeepAlive(ft)
		ft, fd, flat = nil, nil, nil

		// Nested values.
		empty = settledHeap()
		nested := make([]account, n)
		for i := range nested {
			nested[i] = accountOf(i, 0)
		}
		nf := newFeed(keys, nested, false)
		nd, err := refset.Open(context.Background(), nf, refsetOpts(cfg))
		if err != nil {
			panic(err)
		}
		nestedRow(cfg, n, "nested account", float64(settledHeap()-empty)/float64(n), keys, order,
			func(k string) bool { _, ok := nd.Get(k); return ok },
			func(i int) { nf.upsert(keyFor(i, n), accountOf(keyFor(i, n), i)) })
		_ = nd.Close()
		runtime.KeepAlive(nf)
		runtime.KeepAlive(nested)
	}

	fmt.Println()
	fmt.Println("Full reloads through `ReloadSource` changing one entry per reload (median of 3). `Load` returns a copy of the value slice each time, as an application's own load function would.")
	fmt.Println()
	fmt.Println("| Entries | Value | Equal | Reload time | Allocated per reload | Shards kept | Heap peak ÷ steady |")
	fmt.Println("|---|---|---|---|---|---|---|")
	for _, n := range cfg.nestedSizes {
		reloadRow(cfg, n, "flat struct with ID", "== (automatic)", flatRowOf, nil, func(r flatRow) string { return r.ID })
		reloadRow(cfg, n, "nested account", "none", accountOf, nil, func(a account) string { return a.ID })
		reloadRow(cfg, n, "nested account", "hand-written", accountOf, accountsEqual, func(a account) string { return a.ID })
	}
}

func nestedRow(cfg config, n int, name string, perEntry float64, keys []string, order []int32, get func(string) bool, write func(int)) {
	h := readLatency(cfg, keys, order, get)
	hold := gcWhile(cfg, appAllocator)
	churn := gcWhile(cfg, func(stop <-chan struct{}) { pace(1000, stop, write) })
	fmt.Printf("| %s | %s | %.0f B | %s | %.1f%%, %.2f, %s | %.1f%%, %s/s |\n", fmtN(n), name, perEntry,
		fmtDur(h.quantile(0.5)/readBatch), 100*hold.gcCores, float64(hold.cycles)/hold.wall.Seconds(), perCycle(hold),
		100*churn.gcCores, fmtBytes(churn.allocRate))
}

// reloadRow opens a ReloadSource-backed dataset of n values made by mk and
// times reloads that each change one entry.
func reloadRow[V any](cfg config, n int, name, eqName string, mk func(i, gen int) V, eq func(a, b V) bool, id func(V) string) {
	s := &appStore[V]{vals: make([]V, n), notify: make(chan uint64, 1)}
	for i := range s.vals {
		s.vals[i] = mk(i, 0)
	}
	src := &refset.ReloadSource[string, V]{
		Dataset: "perf",
		Load:    s.load,
		Version: s.version,
		Notify:  func(context.Context) (<-chan uint64, error) { return s.notify, nil },
		Key:     id,
		Equal:   eq,
	}
	opts := refsetOpts(cfg)
	d, err := refset.Open(context.Background(), src, opts)
	if err != nil {
		panic(err)
	}
	var results []reloadResult
	for gen := 1; gen <= 3; gen++ {
		i := (gen * 7919) % n
		results = append(results, measureReload(func() uint64 { return s.set(i, mk(i, gen)) }, d.Version))
	}
	kept := d.Stats().ShardsKept
	_ = d.Close()
	slices.SortFunc(results, func(a, b reloadResult) int { return int(a.took - b.took) })
	r := results[1]
	fmt.Printf("| %s | %s | %s | %s | %s | %d of %d per reload | %.2f |\n", fmtN(n), name, eqName, fmtDur(r.took),
		fmtBytes(float64(r.allocated)), kept/3, d.Stats().Shards, r.peakRatio)
}

// appStore is an application's own data for ReloadSource: values in a slice
// and a version, under one lock.
type appStore[V any] struct {
	mu     sync.Mutex
	ver    uint64
	vals   []V
	notify chan uint64
}

func (s *appStore[V]) load(context.Context) (uint64, []V, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ver, slices.Clone(s.vals), nil
}

func (s *appStore[V]) version(context.Context) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ver, nil
}

func (s *appStore[V]) set(i int, v V) uint64 {
	s.mu.Lock()
	s.vals[i] = v
	s.ver++
	ver := s.ver
	s.mu.Unlock()
	select {
	case s.notify <- ver:
	default:
	}
	return ver
}

type reloadResult struct {
	took      time.Duration
	allocated uint64
	peakRatio float64
}

// measureReload makes one change with change, waits until version reports
// it, and returns the time taken, bytes allocated meanwhile and the heap peak
// relative to the settled heap before the change.
func measureReload(change func() uint64, version func() uint64) reloadResult {
	steady := settledHeap()
	// An isolated change: let any batch window from the previous poll pass,
	// so this measures the reload, not batching.
	time.Sleep(20 * time.Millisecond)
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
	a := readGC()
	start := time.Now()
	v := change()
	for version() < v {
		time.Sleep(50 * time.Microsecond)
	}
	took := time.Since(start)
	b := readGC()
	close(stop)
	wg.Wait()
	return reloadResult{took: took, allocated: b.allocated - a.allocated, peakRatio: float64(peak.Load()) / float64(steady)}
}

// gcWhile runs work for cfg.gcDur and reports what the GC did meanwhile.
func gcWhile(cfg config, work func(stop <-chan struct{})) gcDelta {
	return gcFor(cfg.gcDurLarge, work)
}

// gcFor runs work for dur and reports what the GC did meanwhile.
func gcFor(dur time.Duration, work func(stop <-chan struct{})) gcDelta {
	runtime.GC()
	runtime.GC()
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() { work(stop) })
	a := readGC()
	time.Sleep(dur)
	close(stop)
	wg.Wait()
	return diffGC(a, readGC())
}

// perCycle is GC CPU per completed cycle, or "-" without one.
func perCycle(d gcDelta) string {
	if d.cycles == 0 {
		return "no cycle"
	}
	return fmtDur(time.Duration(d.gcCores * float64(d.wall) / float64(d.cycles)))
}

func appAllocator(stop <-chan struct{}) {
	pace(appAllocs, stop, func(int) { appSink = make([]byte, appAllocRate/appAllocs) })
	runtime.KeepAlive(appSink)
}

// readLatency times batches of reads from cfg.readers goroutines, idle.
func readLatency(cfg config, keys []string, order []int32, get func(string) bool) *hist {
	runtime.GC()
	stop := make(chan struct{})
	var wg sync.WaitGroup
	hs := make([]*hist, cfg.readers)
	for r := range cfg.readers {
		hs[r] = new(hist)
		wg.Go(func() {
			i := r * len(order) / cfg.readers
			for {
				select {
				case <-stop:
					return
				default:
				}
				start := time.Now()
				for range readBatch {
					get(keys[order[i]])
					if i++; i == len(order) {
						i = 0
					}
				}
				hs[r].record(int64(time.Since(start)))
			}
		})
	}
	time.Sleep(cfg.dur)
	close(stop)
	wg.Wait()
	total := new(hist)
	for _, h := range hs {
		total.merge(h)
	}
	return total
}

func randomOrder(n int) []int32 {
	order := make([]int32, n)
	for i := range order {
		order[i] = int32(i)
	}
	rand.New(rand.NewPCG(1, 2)).Shuffle(n, func(i, j int) { order[i], order[j] = order[j], order[i] })
	return order
}
