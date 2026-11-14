package refset_test

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/azbyluthfan/refset"
	"github.com/azbyluthfan/refset/refsettest"
)

// slowLoader is a source whose KeyLoader takes 5 ms and records how many
// calls run at once.
type slowLoader struct {
	*refsettest.MemSource[string, int]
	running, peak atomic.Int64
}

func (s *slowLoader) LoadKey(context.Context, string) (int, bool, error) {
	n := s.running.Add(1)
	for p := s.peak.Load(); n > p && !s.peak.CompareAndSwap(p, n); p = s.peak.Load() {
	}
	time.Sleep(5 * time.Millisecond)
	s.running.Add(-1)
	return 1, true, nil
}

// herd sends n concurrent reads for distinct keys at a version the replica
// can't reach, so every read falls back. It returns the peak number of
// concurrent fallback calls and how many reads got ErrNotCaughtUp.
func herd(t *testing.T, n int, opts refset.Options) (peak int64, refused int) {
	t.Helper()
	src := &slowLoader{MemSource: newSrc()}
	opts.WaitBound = time.Millisecond
	d := open(t, refset.Source[string, int](src), quiet(opts))
	src.SetChangesGate(make(chan struct{})) // the replica can't catch up
	v := src.Upsert("x", 1)
	var wg sync.WaitGroup
	var nRefused atomic.Int64
	for i := range n {
		wg.Go(func() {
			_, _, err := d.GetAtLeast(context.Background(), "k"+strconv.Itoa(i), v)
			if errors.Is(err, refset.ErrNotCaughtUp) {
				nRefused.Add(1)
			}
		})
	}
	wg.Wait()
	return src.peak.Load(), int(nRefused.Load())
}

func TestFallbacksAreCappedByDefault(t *testing.T) {
	peak, refused := herd(t, 500, refset.Options{})
	t.Logf("default: peak %d concurrent fallback calls, %d of 500 reads refused", peak, refused)
	if peak > 32 || refused == 0 {
		t.Fatalf("default cap: peak %d concurrent calls (want at most 32), %d refused", peak, refused)
	}
	peak, refused = herd(t, 500, refset.Options{MaxConcurrentFallbacks: -1})
	t.Logf("unlimited: peak %d concurrent fallback calls, %d refused", peak, refused)
	if peak <= 32 || refused != 0 {
		t.Fatalf("MaxConcurrentFallbacks -1 should be unlimited: peak %d, refused %d", peak, refused)
	}
}
