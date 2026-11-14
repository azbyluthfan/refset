package refset

import (
	"context"
	"sync"
)

// flights collapses concurrent fallback loads for the same key into one call,
// and optionally caps how many distinct loads run at once.
type flights[K comparable, V any] struct {
	mu  sync.Mutex
	m   map[K]*flight[V]
	sem chan struct{} // nil means unlimited
}

type flight[V any] struct {
	done chan struct{}
	v    V
	ok   bool
	err  error
}

func newFlights[K comparable, V any](limit int) *flights[K, V] {
	f := &flights[K, V]{m: make(map[K]*flight[V])}
	if limit > 0 {
		f.sem = make(chan struct{}, limit)
	}
	return f
}

// do runs fn for key unless a call for key is already in flight, in which case
// it waits for that call's result. Followers stop waiting when their ctx ends.
// If the concurrency cap is reached, it returns ErrNotCaughtUp without calling
// fn.
func (g *flights[K, V]) do(ctx context.Context, key K, fn func() (V, bool, error)) (V, bool, error) {
	g.mu.Lock()
	if f, ok := g.m[key]; ok {
		g.mu.Unlock()
		select {
		case <-f.done:
			return f.v, f.ok, f.err
		case <-ctx.Done():
			var zero V
			return zero, false, ctx.Err()
		}
	}
	if g.sem != nil {
		select {
		case g.sem <- struct{}{}:
		default:
			g.mu.Unlock()
			var zero V
			return zero, false, ErrNotCaughtUp
		}
	}
	f := &flight[V]{done: make(chan struct{})}
	g.m[key] = f
	g.mu.Unlock()

	defer func() {
		g.mu.Lock()
		delete(g.m, key)
		g.mu.Unlock()
		if g.sem != nil {
			<-g.sem
		}
		close(f.done)
	}()
	f.v, f.ok, f.err = call(fn)
	return f.v, f.ok, f.err
}

// call runs fn, returning a panic as a *PanicError. Without this, a panic
// would unwind past the flight with its zero result recorded, and every
// follower would read "no such key" with no error.
func call[V any](fn func() (V, bool, error)) (v V, ok bool, err error) {
	defer func() {
		if p := recovered(recover()); p != nil {
			var zero V
			v, ok, err = zero, false, p
		}
	}()
	return fn()
}
