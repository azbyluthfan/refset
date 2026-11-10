package main

import (
	"context"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/azbyluthfan/refset"
)

// feed is a minimal in-memory Source for one Dataset. Unlike
// refsettest.MemSource it keeps no history beyond what the dataset hasn't
// fetched yet and stores its state in two flat slices, so its own memory and
// garbage stay small and the measurements show the dataset's costs.
//
// In reload mode it behaves like refset.ReloadSource: Changes never returns
// per-key changes, so every change makes the dataset reload everything.
type feed[K comparable, V any] struct {
	reload bool
	drop   atomic.Bool // drop notifications (polling only)
	watch  chan uint64

	mu      sync.Mutex
	keys    []K
	vals    []V
	version uint64
	pending []refset.Delta[K, V]
}

func newFeed[K comparable, V any](keys []K, vals []V, reload bool) *feed[K, V] {
	return &feed[K, V]{
		reload:  reload,
		watch:   make(chan uint64, 1),
		keys:    keys,
		vals:    slices.Clone(vals),
		version: 1,
	}
}

func (f *feed[K, V]) Name() string { return "perf" }

// upsert sets entry i to v as one new version and returns that version.
func (f *feed[K, V]) upsert(i int, v V) uint64 {
	f.mu.Lock()
	f.version++
	ver := f.version
	f.vals[i] = v
	if !f.reload {
		f.pending = append(f.pending, refset.Delta[K, V]{
			Version: ver,
			Ops:     []refset.Op[K, V]{{Key: f.keys[i], Value: v}},
		})
	}
	f.mu.Unlock()
	if !f.drop.Load() {
		select {
		case f.watch <- ver:
		default:
			// A notification is already queued; the dataset fetches
			// everything up to the head when it handles it.
		}
	}
	return ver
}

func (f *feed[K, V]) LoadSnapshot(_ context.Context, add func(K, V)) (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i, k := range f.keys {
		add(k, f.vals[i])
	}
	f.pending = f.pending[:0]
	return f.version, nil
}

func (f *feed[K, V]) Changes(_ context.Context, after uint64) (uint64, []refset.Delta[K, V], error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.reload {
		if f.version == after {
			return after, nil, nil
		}
		return 0, nil, refset.ErrSnapshotRequired
	}
	// One consumer: pending holds exactly the versions after the last head
	// returned, which the dataset has applied (Changes is only called by its
	// loop, with after = its applied version).
	if len(f.pending) > 0 && f.pending[0].Version != after+1 {
		return 0, nil, refset.ErrSnapshotRequired
	}
	out := slices.Clone(f.pending)
	f.pending = f.pending[:0]
	return f.version, out, nil
}

func (f *feed[K, V]) Watch(context.Context) (<-chan uint64, error) { return f.watch, nil }
