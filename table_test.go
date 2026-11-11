package refset

import (
	"hash/maphash"
	"math/rand/v2"
	"testing"
)

// checkTable verifies t against model: same size, every model entry found,
// every slot reachable from its home without crossing an empty slot.
func checkTable(t *testing.T, tb *table[int, int], seed maphash.Seed, model map[int]int) {
	t.Helper()
	if tb.n != len(model) {
		t.Fatalf("n = %d, model has %d", tb.n, len(model))
	}
	for k, v := range model {
		if got, ok := tb.get(maphash.Comparable(seed, k), k); !ok || got != v {
			t.Fatalf("get(%d) = %d, %v; want %d", k, got, ok, v)
		}
	}
	seen := 0
	for i, s := range tb.slots {
		if s.h == 0 {
			continue
		}
		seen++
		for j := tb.home(s.h); j != i; j = tb.next(j) {
			if tb.slots[j].h == 0 {
				t.Fatalf("slot %d (key %d) unreachable: empty slot %d on its probe path", i, s.k, j)
			}
		}
	}
	if seen != len(model) {
		t.Fatalf("%d occupied slots, model has %d", seen, len(model))
	}
}

func TestTableMatchesMap(t *testing.T) {
	seed := maphash.MakeSeed()
	rng := rand.New(rand.NewPCG(1, 2))
	for _, keyspace := range []int{4, 50, 2000} {
		tb := newTable[int, int](0)
		model := map[int]int{}
		for op := range 20_000 {
			k := rng.IntN(keyspace)
			h := maphash.Comparable(seed, k)
			if rng.IntN(3) == 0 {
				_, had := model[k]
				if got := tb.del(h, k); got != had {
					t.Fatalf("del(%d) = %v, want %v", k, got, had)
				}
				delete(model, k)
			} else {
				_, had := model[k]
				if added := tb.put(h, k, op); added == had {
					t.Fatalf("put(%d) added = %v, key existed = %v", k, added, had)
				}
				model[k] = op
			}
			if op%997 == 0 {
				checkTable(t, tb, seed, model)
			}
		}
		checkTable(t, tb, seed, model)
	}
}

// Deleting from long probe runs that wrap around the end of the slot array
// is where backward-shift deletion goes wrong. A table of 8 slots and keys
// forced to the last and first homes exercise it directly.
func TestTableDeleteInCollidingRuns(t *testing.T) {
	seed := maphash.MakeSeed()
	rng := rand.New(rand.NewPCG(3, 4))
	for round := range 2000 {
		tb := newTable[int, int](0)
		model := map[int]int{}
		// Find keys whose home is one of slots 6, 7, 0 (runs that wrap).
		var keys []int
		for k := 0; len(keys) < 5; k++ {
			if home := tb.home(maphash.Comparable(seed, k)); home == 6 || home == 7 || home == 0 {
				keys = append(keys, k)
			}
		}
		for _, k := range keys {
			tb.put(maphash.Comparable(seed, k), k, k)
			model[k] = k
		}
		rng.Shuffle(len(keys), func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })
		for _, k := range keys[:3] {
			tb.del(maphash.Comparable(seed, k), k)
			delete(model, k)
			checkTable(t, tb, seed, model)
		}
		_ = round
	}
}

func TestTableCloneIsIndependent(t *testing.T) {
	seed := maphash.MakeSeed()
	a := newTable[int, int](10)
	for k := range 10 {
		a.put(maphash.Comparable(seed, k), k, k)
	}
	b := a.clone()
	b.put(maphash.Comparable(seed, 3), 3, 300)
	b.del(maphash.Comparable(seed, 4), 4)
	b.put(maphash.Comparable(seed, 99), 99, 99)
	if v, _ := a.get(maphash.Comparable(seed, 3), 3); v != 3 {
		t.Fatal("clone wrote through to the original")
	}
	if _, ok := a.get(maphash.Comparable(seed, 4), 4); !ok {
		t.Fatal("delete in the clone removed from the original")
	}
	if a.n != 10 || b.n != 10 {
		t.Fatalf("n: original %d, clone %d", a.n, b.n)
	}
}

func FuzzTable(f *testing.F) {
	f.Add([]byte{1, 2, 3, 0x81, 0x82, 4})
	f.Fuzz(func(t *testing.T, ops []byte) {
		seed := maphash.MakeSeed()
		tb := newTable[int, int](0)
		model := map[int]int{}
		for i, b := range ops {
			k := int(b & 0x3f)
			h := maphash.Comparable(seed, k)
			if b&0x80 != 0 {
				tb.del(h, k)
				delete(model, k)
			} else {
				tb.put(h, k, i)
				model[k] = i
			}
		}
		checkTable(t, tb, seed, model)
	})
}
