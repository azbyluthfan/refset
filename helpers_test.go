package refset_test

import (
	"context"
	"testing"
	"time"

	"github.com/azbyluthfan/refset"
	"github.com/azbyluthfan/refset/refsettest"
)

// eventually fails the test if cond doesn't become true within 5 seconds.
func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

func open[K comparable, V any](t *testing.T, src refset.Source[K, V], opts refset.Options) *refset.Dataset[K, V] {
	t.Helper()
	d, err := refset.Open(context.Background(), src, opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = d.Close() })
	return d
}

// quiet options: only explicit kicks and notifications drive polling.
func quiet(o refset.Options) refset.Options {
	if o.PollInterval == 0 {
		o.PollInterval = time.Hour
	}
	return o
}

// result of a read run in a goroutine.
type result struct {
	v   int
	ok  bool
	err error
}

func goGet(ctx context.Context, d *refset.Dataset[string, int], key string, minVersion uint64) <-chan result {
	ch := make(chan result, 1)
	go func() {
		v, ok, err := d.GetAtLeast(ctx, key, minVersion)
		ch <- result{v, ok, err}
	}()
	return ch
}

func recv(t *testing.T, ch <-chan result) result {
	t.Helper()
	select {
	case r := <-ch:
		return r
	case <-time.After(5 * time.Second):
		t.Fatal("read did not return")
		return result{}
	}
}

func newSrc() *refsettest.MemSource[string, int] {
	return refsettest.NewMemSource[string, int]("plans")
}
