package bench

import (
	"context"
	"math/rand/v2"
	"strconv"
	"sync"
	"testing"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"

	"github.com/azbyluthfan/refset"
	"github.com/azbyluthfan/refset/refsettest"
)

var sizes = []int{10_000, 100_000, 1_000_000}

func keys(n int) []string {
	ks := make([]string, n)
	for i := range n {
		ks[i] = "key-" + strconv.Itoa(i)
	}
	return ks
}

// order is a random permutation of key indexes, so reads jump around memory
// like real requests and hardware prefetching can't help.
func order(n int) []int32 {
	o := make([]int32, n)
	for i := range o {
		o[i] = int32(i)
	}
	rand.New(rand.NewPCG(1, 1)).Shuffle(n, func(i, j int) { o[i], o[j] = o[j], o[i] })
	return o
}

func openDataset[V any](b *testing.B, ks []string, val func(int) V) *refset.Dataset[string, V] {
	src := refsettest.NewMemSource[string, V]("bench")
	ops := make([]refset.Op[string, V], len(ks))
	for i, k := range ks {
		ops[i] = refset.Op[string, V]{Key: k, Value: val(i)}
	}
	src.Apply(ops...)
	d, err := refset.Open(context.Background(), src, refset.Options{PollInterval: time.Hour})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = d.Close() })
	return d
}

// run reads keys in random order from get, serially or from every core.
func run(b *testing.B, ks []string, get func(string) bool) {
	o := order(len(ks))
	b.Run("serial", func(b *testing.B) {
		b.ReportAllocs()
		i := 0
		for b.Loop() {
			get(ks[o[i]])
			if i++; i == len(o) {
				i = 0
			}
		}
	})
	b.Run("parallel", func(b *testing.B) {
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			i := rand.IntN(len(o))
			for pb.Next() {
				get(ks[o[i]])
				if i++; i == len(o) {
					i = 0
				}
			}
		})
	})
}

func BenchmarkGet(b *testing.B) {
	for _, n := range sizes {
		ks := keys(n)
		b.Run("entries="+strconv.Itoa(n), func(b *testing.B) {
			b.Run("refset", func(b *testing.B) {
				d := openDataset(b, ks, func(i int) int { return i })
				run(b, ks, func(k string) bool { _, ok := d.Get(k); return ok })
			})
			b.Run("golang-lru", func(b *testing.B) {
				c, _ := lru.New[string, int](n)
				for i, k := range ks {
					c.Add(k, i)
				}
				run(b, ks, func(k string) bool { _, ok := c.Get(k); return ok })
			})
			b.Run("rwmutex-map", func(b *testing.B) {
				var mu sync.RWMutex
				m := make(map[string]int, n)
				for i, k := range ks {
					m[k] = i
				}
				run(b, ks, func(k string) bool {
					mu.RLock()
					_, ok := m[k]
					mu.RUnlock()
					return ok
				})
			})
			b.Run("plain-map-no-lock", func(b *testing.B) {
				m := make(map[string]int, n)
				for i, k := range ks {
					m[k] = i
				}
				run(b, ks, func(k string) bool { _, ok := m[k]; return ok })
			})
		})
	}
}

type small struct{ A, B int64 } // 16 bytes

type large struct { // 256 bytes
	A [32]int64
}

// BenchmarkGetValueSize: maps store values inline, so a large value spans more
// cache lines per lookup; storing a pointer keeps map slots small but adds a
// hop (rule R5).
func BenchmarkGetValueSize(b *testing.B) {
	const n = 100_000
	ks := keys(n)
	b.Run("small-16B", func(b *testing.B) {
		d := openDataset(b, ks, func(i int) small { return small{int64(i), 0} })
		run(b, ks, func(k string) bool { v, _ := d.Get(k); return v.A >= 0 })
	})
	b.Run("large-256B", func(b *testing.B) {
		d := openDataset(b, ks, func(i int) large { var l large; l.A[0] = int64(i); return l })
		run(b, ks, func(k string) bool { v, _ := d.Get(k); return v.A[0] >= 0 })
	})
	b.Run("pointer-to-large", func(b *testing.B) {
		d := openDataset(b, ks, func(i int) *large { l := &large{}; l.A[0] = int64(i); return l })
		run(b, ks, func(k string) bool { v, _ := d.Get(k); return v.A[0] >= 0 })
	})
}
