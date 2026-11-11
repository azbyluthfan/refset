package refset

import "hash/maphash"

// table is an open-addressing hash table with linear probing, an
// alternative shard storage to a Go map (roadmap P2, "single hash per read").
//
// It takes the key's hash from the caller, so a read hashes the key once:
// the low bits pick the shard, the high bits pick the slot. Each slot holds
// the hash, key and value together, so a lookup usually touches one slot and
// probing continues into the adjacent one, which the CPU prefetches.
//
// Like a shard map, a table is never modified after it is published; a
// change clones it first.
type table[K comparable, V any] struct {
	slots []slot[K, V] // any length: slots are chosen by multiply-shift, not a mask
	n     int
}

// slot is empty when h is 0. Stored hashes have the low bit set (h | 1), so
// a real hash of 0 can't look empty.
type slot[K comparable, V any] struct {
	h uint64
	k K
	v V
}

const minTableSlots = 8

// hashOf is the one hash a table-backed read computes: its low bits pick the
// shard (as shardOf does), its high bits the slot.
func hashOf[K comparable](l layout, key K) uint64 {
	return maphash.Comparable(l.seed, key)
}

// Tables are filled to at most maxLoadNum/maxLoadDen of their slots. Sizes
// aren't rounded to powers of two, so a table built for n entries wastes no
// more than that headroom.
const maxLoadNum, maxLoadDen = 17, 20 // 85%

// newTable returns a table sized for n entries.
func newTable[K comparable, V any](n int) *table[K, V] {
	return &table[K, V]{slots: make([]slot[K, V], slotsFor(n))}
}

func slotsFor(n int) int { return max(minTableSlots, n*maxLoadDen/maxLoadNum+1) }

// home is the first slot probed for hash h. The low bits of h choose the
// shard, so the slot comes from the high 32 bits, scaled to the table size
// (multiply-shift instead of a modulo).
func (t *table[K, V]) home(h uint64) int {
	return int((h >> 32) * uint64(len(t.slots)) >> 32)
}

// next is the slot after i, wrapping at the end.
func (t *table[K, V]) next(i int) int {
	if i++; i == len(t.slots) {
		return 0
	}
	return i
}

func (t *table[K, V]) get(h uint64, key K) (V, bool) {
	fh := h | 1
	for i := t.home(h); ; i = t.next(i) {
		s := &t.slots[i]
		if s.h == fh && s.k == key {
			return s.v, true
		}
		if s.h == 0 {
			var zero V
			return zero, false
		}
	}
}

// put sets key to v and reports whether the key was new. The table must be
// private to the caller (a clone, or one being built).
func (t *table[K, V]) put(h uint64, key K, v V) bool {
	if (t.n+1)*maxLoadDen > len(t.slots)*maxLoadNum {
		t.resize(slotsFor(t.n + 1 + t.n/2))
	}
	fh := h | 1
	for i := t.home(h); ; i = t.next(i) {
		s := &t.slots[i]
		if s.h == 0 {
			*s = slot[K, V]{h: fh, k: key, v: v}
			t.n++
			return true
		}
		if s.h == fh && s.k == key {
			s.v = v
			return false
		}
	}
}

// del removes key and reports whether it was present, shifting later
// entries of the probe run back so lookups never stop early (no tombstones).
func (t *table[K, V]) del(h uint64, key K) bool {
	fh := h | 1
	i := t.home(h)
	for {
		s := &t.slots[i]
		if s.h == 0 {
			return false
		}
		if s.h == fh && s.k == key {
			break
		}
		i = t.next(i)
	}
	c := len(t.slots)
	dist := func(from, to int) int { return (to - from + c) % c } // forward steps
	for j := t.next(i); t.slots[j].h != 0; j = t.next(j) {
		// Slot j can fill the hole at i if the hole lies on its probe path,
		// from its home k up to j; otherwise it would become unreachable.
		if k := t.home(t.slots[j].h); dist(k, i) <= dist(k, j) {
			t.slots[i] = t.slots[j]
			i = j
		}
	}
	t.slots[i] = slot[K, V]{}
	t.n--
	return true
}

func (t *table[K, V]) resize(c int) {
	old := t.slots
	t.slots = make([]slot[K, V], c)
	for _, s := range old {
		if s.h == 0 {
			continue
		}
		for i := t.home(s.h); ; i = t.next(i) {
			if t.slots[i].h == 0 {
				t.slots[i] = s
				break
			}
		}
	}
}

// clone returns a private copy for copy-on-write.
func (t *table[K, V]) clone() *table[K, V] {
	return &table[K, V]{slots: append([]slot[K, V](nil), t.slots...), n: t.n}
}

// all calls yield for every entry until it returns false.
func (t *table[K, V]) all(yield func(K, V) bool) bool {
	for i := range t.slots {
		if s := &t.slots[i]; s.h != 0 && !yield(s.k, s.v) {
			return false
		}
	}
	return true
}
