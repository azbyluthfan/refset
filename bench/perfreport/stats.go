package main

import (
	"fmt"
	"math"
	"math/bits"
	"runtime/metrics"
	"time"
)

// hist is a log-linear histogram of durations in nanoseconds: 32 buckets per
// power of two, so any percentile is within about 3% of the true value.
// Recording never allocates.
type hist struct {
	counts [64 * subBuckets]uint64
	n      uint64
	max    int64
}

const subBits = 5
const subBuckets = 1 << subBits

func bucketOf(ns int64) int {
	if ns < subBuckets {
		return int(max(ns, 0))
	}
	exp := bits.Len64(uint64(ns)) - 1 // ns in [2^exp, 2^(exp+1))
	sub := int(uint64(ns)>>(exp-subBits)) & (subBuckets - 1)
	return (exp-subBits+1)*subBuckets + sub
}

// lowerBound is the smallest value in bucket b.
func lowerBound(b int) int64 {
	if b < subBuckets {
		return int64(b)
	}
	exp := b/subBuckets + subBits - 1
	sub := b % subBuckets
	return int64(subBuckets+sub) << (exp - subBits)
}

func (h *hist) record(ns int64) {
	h.counts[bucketOf(ns)]++
	h.n++
	if ns > h.max {
		h.max = ns
	}
}

func (h *hist) merge(o *hist) {
	for i, c := range o.counts {
		h.counts[i] += c
	}
	h.n += o.n
	h.max = max(h.max, o.max)
}

// quantile returns the q-quantile (0..1); the maximum is exact.
func (h *hist) quantile(q float64) time.Duration {
	if h.n == 0 {
		return 0
	}
	if q >= 1 {
		return time.Duration(h.max)
	}
	rank := uint64(math.Ceil(q * float64(h.n)))
	var seen uint64
	for b, c := range h.counts {
		seen += c
		if seen >= rank {
			return time.Duration(min(lowerBound(b), h.max))
		}
	}
	return time.Duration(h.max)
}

// gcSample is a snapshot of the runtime's GC counters. CPU figures are
// complete up to the last finished GC cycle.
type gcSample struct {
	at        time.Time
	cycles    uint64
	allocated uint64        // bytes allocated since start
	gcCPU     time.Duration // GC CPU excluding idle-time marking
	assist    time.Duration // part of gcCPU spent by allocating goroutines
	pauses    *metrics.Float64Histogram
}

var gcMetricNames = []string{
	"/gc/cycles/total:gc-cycles",
	"/gc/heap/allocs:bytes",
	"/cpu/classes/gc/total:cpu-seconds",
	"/cpu/classes/gc/mark/idle:cpu-seconds",
	"/cpu/classes/gc/mark/assist:cpu-seconds",
	"/sched/pauses/total/gc:seconds",
}

func readGC() gcSample {
	s := make([]metrics.Sample, len(gcMetricNames))
	for i, n := range gcMetricNames {
		s[i].Name = n
	}
	metrics.Read(s)
	secs := func(i int) time.Duration { return time.Duration(s[i].Value.Float64() * float64(time.Second)) }
	return gcSample{
		at:        time.Now(),
		cycles:    s[0].Value.Uint64(),
		allocated: s[1].Value.Uint64(),
		gcCPU:     secs(2) - secs(3),
		assist:    secs(4),
		pauses:    s[5].Value.Float64Histogram(),
	}
}

// gcDelta is what the GC did between two samples.
type gcDelta struct {
	wall       time.Duration
	cycles     uint64
	allocRate  float64       // bytes per second
	gcCores    float64       // GC CPU seconds per wall second
	assist     time.Duration // total mark assist
	pauseP99   time.Duration
	pauseMax   time.Duration // upper bound of the highest bucket hit
	pauseCount uint64
}

func diffGC(a, b gcSample) gcDelta {
	d := gcDelta{
		wall:   b.at.Sub(a.at),
		cycles: b.cycles - a.cycles,
		assist: b.assist - a.assist,
	}
	sec := d.wall.Seconds()
	d.allocRate = float64(b.allocated-a.allocated) / sec
	d.gcCores = (b.gcCPU - a.gcCPU).Seconds() / sec

	counts := make([]uint64, len(b.pauses.Counts))
	for i := range counts {
		counts[i] = b.pauses.Counts[i] - a.pauses.Counts[i]
		d.pauseCount += counts[i]
	}
	if d.pauseCount > 0 {
		rank := uint64(math.Ceil(0.99 * float64(d.pauseCount)))
		var seen uint64
		for i, c := range counts {
			if c == 0 {
				continue
			}
			upper := b.pauses.Buckets[i+1]
			if math.IsInf(upper, 1) {
				upper = b.pauses.Buckets[i]
			}
			seen += c
			if seen >= rank && d.pauseP99 == 0 {
				d.pauseP99 = time.Duration(upper * float64(time.Second))
			}
			d.pauseMax = time.Duration(upper * float64(time.Second))
		}
	}
	return d
}

// heapBytes reads the bytes in live and not-yet-swept heap objects.
func heapBytes() uint64 {
	s := []metrics.Sample{{Name: "/memory/classes/heap/objects:bytes"}}
	metrics.Read(s)
	return s[0].Value.Uint64()
}

// pace calls fn rate times per second until stop is closed. It schedules
// against the start time, so a slow call doesn't lower the average rate.
func pace(rate int, stop <-chan struct{}, fn func(i int)) {
	if rate <= 0 {
		<-stop
		return
	}
	interval := time.Second / time.Duration(rate)
	start := time.Now()
	// One reusable timer: time.After would allocate on every call and show
	// up as garbage in the GC section for both stores.
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	for i := 0; ; i++ {
		if wait := time.Until(start.Add(time.Duration(i) * interval)); wait > 0 {
			timer.Reset(wait)
			select {
			case <-stop:
				return
			case <-timer.C:
			}
		} else {
			select {
			case <-stop:
				return
			default:
			}
		}
		fn(i)
	}
}

func fmtDur(d time.Duration) string {
	switch {
	case d == 0:
		return "0"
	case d < time.Microsecond:
		return fmt.Sprintf("%d ns", d.Nanoseconds())
	case d < time.Millisecond:
		return fmt.Sprintf("%.1f µs", float64(d)/float64(time.Microsecond))
	case d < time.Second:
		return fmt.Sprintf("%.1f ms", float64(d)/float64(time.Millisecond))
	}
	return fmt.Sprintf("%.2f s", d.Seconds())
}

func fmtBytes(b float64) string {
	switch {
	case b < 1<<10:
		return fmt.Sprintf("%.0f B", b)
	case b < 1<<20:
		return fmt.Sprintf("%.0f KB", b/(1<<10))
	case b < 1<<30:
		return fmt.Sprintf("%.1f MB", b/(1<<20))
	}
	return fmt.Sprintf("%.2f GB", b/(1<<30))
}
