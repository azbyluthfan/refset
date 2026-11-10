// Command perfreport measures refset on the three axes the roadmap tracks,
// next to a map behind a sync.RWMutex fed the same writes:
//
//   - tail: read latency percentiles while idle, while changes are applied and
//     while full reloads swap in;
//   - prop: how long a committed change takes to become visible on a replica;
//   - gc: garbage-collector CPU, cycles, allocation and pauses at several
//     write rates;
//   - mem: bytes per entry for several key and value types, and the heap peak
//     during a full reload.
//
// Output is Markdown, ready for docs/benchmarks.md. Run from the bench module:
//
//	go run ./perfreport                       # everything, about 10 minutes
//	go run ./perfreport -sections gc -sizes 1000000
//	go run ./perfreport -quick                # smoke test, seconds
package main

import (
	"flag"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"
)

type config struct {
	sections map[string]bool
	sizes    []int
	dur      time.Duration // per tail cell
	gcDur    time.Duration // per gc cell
	samples  int           // per propagation case
	readers  int
	shards   int
}

func main() {
	sections := flag.String("sections", "tail,prop,gc,mem", "comma-separated sections to run")
	sizes := flag.String("sizes", "10000,100000,1000000", "comma-separated dataset sizes")
	dur := flag.Duration("dur", 3*time.Second, "duration of each read-latency cell")
	gcDur := flag.Duration("gcdur", 5*time.Second, "duration of each GC cell")
	samples := flag.Int("samples", 200, "changes timed per propagation case")
	readers := flag.Int("readers", max(1, runtime.GOMAXPROCS(0)/2), "reader goroutines in the latency section")
	shards := flag.Int("shards", 0, "Options.Shards for refset (0 = the library default)")
	quick := flag.Bool("quick", false, "tiny sizes and durations, to check the tool runs")
	flag.Parse()

	cfg := config{
		sections: map[string]bool{},
		dur:      *dur,
		gcDur:    *gcDur,
		samples:  *samples,
		readers:  *readers,
		shards:   *shards,
	}
	for _, s := range strings.Split(*sections, ",") {
		cfg.sections[strings.TrimSpace(s)] = true
	}
	for _, s := range strings.Split(*sizes, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(s))
		if err != nil || n < 1 {
			fmt.Fprintf(os.Stderr, "bad size %q\n", s)
			os.Exit(2)
		}
		cfg.sizes = append(cfg.sizes, n)
	}
	if *quick {
		cfg.sizes = []int{1000}
		cfg.dur, cfg.gcDur, cfg.samples = 200*time.Millisecond, 300*time.Millisecond, 10
	}

	shardNote := "library default"
	if cfg.shards > 0 {
		shardNote = strconv.Itoa(cfg.shards)
	}
	fmt.Printf("# refset perfreport\n\nGo %s, %s/%s, %d CPUs, GOMAXPROCS %d, GOGC %s, shards: %s. %s\n",
		runtime.Version(), runtime.GOOS, runtime.GOARCH, runtime.NumCPU(), runtime.GOMAXPROCS(0),
		envOr("GOGC", "100"), shardNote, time.Now().Format(time.DateOnly))

	if cfg.sections["tail"] {
		tailSection(cfg)
	}
	if cfg.sections["prop"] {
		propSection(cfg)
	}
	if cfg.sections["gc"] {
		gcSection(cfg)
	}
	if cfg.sections["mem"] {
		memSection(cfg)
	}
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// plan is the value used everywhere: a string and a small int, 24 bytes.
type plan struct {
	Tier  string
	Seats int32
}

var tiers = []string{"free", "pro", "team"}

func planOf(x int) plan { return plan{Tier: tiers[x%3], Seats: int32(x % 50)} }

func dataOf(n int) ([]string, []plan) {
	keys := make([]string, n)
	vals := make([]plan, n)
	for i := range n {
		keys[i] = "key-" + strconv.Itoa(i)
		vals[i] = planOf(i)
	}
	return keys, vals
}

// keyFor spreads writes over the keyspace.
func keyFor(i, n int) int { return int((uint64(i) * 7919) % uint64(n)) }

func fmtN(n int) string {
	switch {
	case n >= 1_000_000 && n%1_000_000 == 0:
		return strconv.Itoa(n/1_000_000) + "M"
	case n >= 1000 && n%1000 == 0:
		return strconv.Itoa(n/1000) + "k"
	}
	return strconv.Itoa(n)
}
