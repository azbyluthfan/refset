package refset

import (
	"hash/maphash"
	"iter"
	"math"
)

// layout maps keys to shards. The seed is fixed for the life of a Dataset; the
// shard count belongs to each root, so a full reload can change it.
type layout struct {
	seed maphash.Seed
	mask uint64
}

func newLayout(shards int) layout {
	return layout{seed: maphash.MakeSeed(), mask: uint64(shards - 1)} //nolint:gosec // shards is validated: a power of two from 1 to maxShards
}

// withShards returns the same seed with a different shard count.
func (l layout) withShards(shards int) layout {
	return layout{seed: l.seed, mask: uint64(shards - 1)} //nolint:gosec // shards is validated: a power of two from 1 to maxShards
}

func (l layout) shards() int { return int(l.mask) + 1 } //nolint:gosec // mask is below maxShards (65,536)

func shardOf[K comparable](l layout, key K) int {
	return int(maphash.Comparable(l.seed, key) & l.mask) //nolint:gosec // masked to below maxShards (65,536)
}

// Automatic shard count bounds. Below minAutoShards, the per-shard copy is
// already tiny; above maxShards the shard slice no longer fits the L1 cache
// (performance rule R3).
const minAutoShards = 16

// Below smallDataset entries a dataset largely fits in the CPU caches, and
// more shard maps make reads measurably slower (1024 shards at 100k entries:
// +7.5% per read; 512: within noise). Above it reads are bound by main memory
// and the shard count doesn't affect them. See docs/benchmarks.md.
const (
	smallDataset     = 200_000
	smallDatasetCap  = 512
	autoShardsFactor = 2.5
)

// autoShards picks a shard count for n entries: about 2.5 × √n, rounded up to
// a power of two, which balances the two copies a change makes (the shard
// slice, 8 bytes per shard, and each touched shard, n ÷ shards entries). For
// datasets small enough to stay in cache it is capped, to keep reads fast.
func autoShards(n int) int {
	target := autoShardsFactor * math.Sqrt(float64(max(n, 0)))
	s := minAutoShards
	for float64(s) < target && s < maxShards {
		s <<= 1
	}
	if n < smallDataset {
		s = min(s, smallDatasetCap)
	}
	return s
}

// root is one immutable version of the dataset. Shard maps are never modified
// after the root is published; a new version copies only the shards it
// changes and shares the rest.
type root[K comparable, V any] struct {
	version uint64
	l       layout
	shards  []map[K]V
	n       int
}

func (r *root[K, V]) get(key K) (V, bool) {
	v, ok := r.shards[shardOf(r.l, key)][key]
	return v, ok
}

func emptyShards[K comparable, V any](n int) []map[K]V {
	s := make([]map[K]V, n)
	for i := range s {
		s[i] = make(map[K]V)
	}
	return s
}

// Snapshot is an immutable view of a [Dataset] at one version. It stays valid
// and unchanged while newer versions are applied.
//
// Values are shared with every other reader. Treat them as read-only: if V
// contains a map, slice or pointer, copy before modifying.
type Snapshot[K comparable, V any] struct {
	r *root[K, V]
}

// Version returns the version this snapshot reflects.
func (s Snapshot[K, V]) Version() uint64 {
	if s.r == nil {
		return 0
	}
	return s.r.version
}

// Get returns the value for key in this snapshot.
func (s Snapshot[K, V]) Get(key K) (V, bool) {
	if s.r == nil {
		var zero V
		return zero, false
	}
	return s.r.get(key)
}

// Len returns the number of entries in this snapshot.
func (s Snapshot[K, V]) Len() int {
	if s.r == nil {
		return 0
	}
	return s.r.n
}

// All iterates over every entry in this snapshot, in no particular order.
func (s Snapshot[K, V]) All() iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {
		if s.r == nil {
			return
		}
		for _, m := range s.r.shards {
			for k, v := range m {
				if !yield(k, v) {
					return
				}
			}
		}
	}
}
