package refset_test

import (
	"context"
	"errors"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/azbyluthfan/refset"
	"github.com/azbyluthfan/refset/refsettest"
)

// slowSource adds a fixed latency to every Changes call, like a database
// round trip, and counts the calls.
type slowSource struct {
	*refsettest.MemSource[string, int]
	latency time.Duration
	calls   atomic.Int64
}

func (s *slowSource) Changes(ctx context.Context, after uint64) (uint64, []refset.Delta[string, int], error) {
	s.calls.Add(1)
	time.Sleep(s.latency)
	return s.MemSource.Changes(ctx, after)
}

type floodResult struct {
	pollsPerSec  float64
	forgedPerSec float64
	rywP50       time.Duration
	rywMax       time.Duration
}

// flood runs attackers asking for a version far beyond the head while a
// writer commits every 5 ms and a reader reads its writes, and reports the
// source calls per second and the reader's latency.
func flood(t *testing.T, attackers int, dur time.Duration) floodResult {
	t.Helper()
	src := &slowSource{MemSource: newSrc(), latency: 200 * time.Microsecond}
	src.Upsert("k", 0)
	d := open(t, refset.Source[string, int](src), refset.Options{PollInterval: time.Second})
	ctx, cancel := context.WithTimeout(context.Background(), dur)
	defer cancel()

	var forged atomic.Int64
	var wg sync.WaitGroup
	for range attackers {
		wg.Go(func() {
			for ctx.Err() == nil {
				if _, _, err := d.GetAtLeast(context.Background(), "k", 1<<60); errors.Is(err, refset.ErrVersionAhead) {
					forged.Add(1)
				}
			}
		})
	}
	var lat []time.Duration
	wg.Go(func() {
		for i := 1; ctx.Err() == nil; i++ {
			v := src.Upsert("k", i)
			start := time.Now()
			if _, _, err := d.GetAtLeast(context.Background(), "k", v); err == nil {
				lat = append(lat, time.Since(start))
			}
			time.Sleep(5 * time.Millisecond)
		}
	})
	before := src.calls.Load()
	start := time.Now()
	<-ctx.Done()
	wg.Wait()
	sec := time.Since(start).Seconds()
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	r := floodResult{pollsPerSec: float64(src.calls.Load()-before) / sec, forgedPerSec: float64(forged.Load()) / sec}
	if len(lat) > 0 {
		r.rywP50, r.rywMax = lat[len(lat)/2], lat[len(lat)-1]
	}
	return r
}

// TestForgedVersionFlood measures how much source load a flood of forged
// versions causes. Run with REFSET_FLOOD=1 to print the numbers.
func TestForgedVersionFlood(t *testing.T) {
	if os.Getenv("REFSET_FLOOD") == "" {
		t.Skip("set REFSET_FLOOD=1 to run the flood measurement")
	}
	for _, attackers := range []int{0, 1, 64} {
		r := flood(t, attackers, 2*time.Second)
		t.Logf("attackers %2d: source calls %6.0f/s, forged answers %7.0f/s, read-your-writes p50 %v max %v",
			attackers, r.pollsPerSec, r.forgedPerSec, r.rywP50.Round(time.Microsecond), r.rywMax.Round(time.Microsecond))
	}
}

// Forged versions must not drive the source at its own speed. Before
// rate limiting, one attacker caused about 3,800 calls a second here; the
// limit allows about 200 (one per BatchWindow) plus normal traffic.
func TestForgedVersionsAreRateLimited(t *testing.T) {
	r := flood(t, 8, 500*time.Millisecond)
	if r.pollsPerSec > 1000 {
		t.Fatalf("a forged-version flood caused %.0f source calls a second; want rate-limited (under 1,000)", r.pollsPerSec)
	}
	if r.forgedPerSec == 0 {
		t.Fatal("the attackers got no ErrVersionAhead answers: the measurement didn't run")
	}
}
