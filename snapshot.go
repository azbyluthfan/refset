package refset

import (
	"hash/maphash"
	"iter"
)

// layout maps keys to shards. It is fixed for the life of a Dataset.
type layout struct {
	seed maphash.Seed
	mask uint64
}

func newLayout(shards int) layout {
	return layout{seed: maphash.MakeSeed(), mask: uint64(shards - 1)}
}

func shardOf[K comparable](l layout, key K) int {
	return int(maphash.Comparable(l.seed, key) & l.mask)
}

// root is one immutable version of the dataset. Shard maps are never modified
// after the root is published; a new version copies only the shards it
// changes and shares the rest.
type root[K comparable, V any] struct {
	version uint64
	shards  []map[K]V
	n       int
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
	l layout
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
	v, ok := s.r.shards[shardOf(s.l, key)][key]
	return v, ok
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
