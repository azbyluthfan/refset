// Command memreport measures refset's memory per entry and garbage-collector
// pause times at 1M entries, next to a plain map with the same contents
// (design §8.7). Run: cd bench && go run ./memreport
package main

import (
	"context"
	"fmt"
	"runtime"
	"runtime/debug"
	"slices"
	"strconv"
	"time"

	"github.com/azbyluthfan/refset"
	"github.com/azbyluthfan/refset/refsettest"
)

type plan struct {
	Tier  string
	Seats int32
}

const n = 1_000_000

func heap() uint64 {
	runtime.GC()
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return ms.HeapAlloc
}

// pauses forces GCs with the structure live and returns their stop-the-world
// pause times.
func pauses(keep any) []time.Duration {
	var before debug.GCStats
	debug.ReadGCStats(&before)
	for range 20 {
		runtime.GC()
	}
	var after debug.GCStats
	debug.ReadGCStats(&after)
	runtime.KeepAlive(keep)
	got := after.Pause[:after.NumGC-before.NumGC]
	slices.Sort(got)
	return got
}

func main() {
	keys := make([]string, n)
	vals := make([]plan, n)
	for i := range n {
		keys[i] = "user-" + strconv.Itoa(i)
		vals[i] = plan{Tier: []string{"free", "pro", "team"}[i%3], Seats: int32(i % 50)}
	}
	base := heap()

	m := make(map[string]plan, n)
	for i := range n {
		m[keys[i]] = vals[i]
	}
	mapBytes := heap() - base
	mapPauses := pauses(m)
	clear(m) // drop the map's contents before measuring refset

	src := refsettest.NewMemSource[string, plan]("plans")
	ops := make([]refset.Op[string, plan], n)
	for i := range n {
		ops[i] = refset.Op[string, plan]{Key: keys[i], Value: vals[i]}
	}
	src.Apply(ops...)
	clear(ops)
	withSource := heap()
	start := time.Now()
	d, err := refset.Open(context.Background(), src, refset.Options{PollInterval: time.Hour})
	if err != nil {
		panic(err)
	}
	openTime := time.Since(start)
	datasetBytes := heap() - withSource
	dsPauses := pauses(d)

	fmt.Printf("Go %s, %s/%s, GOMAXPROCS %d, %d entries (string key, 24-byte value)\n",
		runtime.Version(), runtime.GOOS, runtime.GOARCH, runtime.GOMAXPROCS(0), n)
	fmt.Printf("%-22s %10s %14s %14s %14s\n", "", "bytes/entry", "GC pause p50", "GC pause max", "")
	fmt.Printf("%-22s %10.0f %14v %14v\n", "map[string]plan", float64(mapBytes)/n, mapPauses[len(mapPauses)/2], mapPauses[len(mapPauses)-1])
	fmt.Printf("%-22s %10.0f %14v %14v\n", "refset (256 shards)", float64(datasetBytes)/n, dsPauses[len(dsPauses)/2], dsPauses[len(dsPauses)-1])
	fmt.Printf("refset Open (full load of %d entries): %v\n", n, openTime.Round(time.Millisecond))
	fmt.Println("Keys are shared with the source in this measurement; bytes/entry counts map storage only.")
	_ = d.Close()
}
