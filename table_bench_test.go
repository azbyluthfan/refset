package refset

import (
	"fmt"
	"maps"
	"math/rand/v2"
	"runtime"
	"slices"
	"strconv"
	"testing"
)

// BenchmarkShardStorage compares Go maps with tables as shard storage, with
// the same layout and automatic shard count (roadmap P2): reads on one core
// and on all cores, a one-key copy-on-write change, building from scratch,
// and bytes per entry.
func BenchmarkShardStorage(b *testing.B) {
	for _, n := range []int{10_000, 100_000, 1_000_000} {
		keys := make([]string, n)
		for i := range keys {
			keys[i] = "key-" + strconv.Itoa(i)
		}
		order := rand.New(rand.NewPCG(1, 1)).Perm(n)
		l := newLayout(autoShards(n))
		shards := l.shards()

		buildMaps := func() []map[string]int {
			s := sizedShards[string, int](shards, n)
			for i, k := range keys {
				s[shardOf(l, k)][k] = i
			}
			return s
		}
		buildTables := func() []*table[string, int] {
			s := make([]*table[string, int], shards)
			for i := range s {
				s[i] = newTable[string, int](n/shards + n/(16*shards) + 1)
			}
			for i, k := range keys {
				h := hashOf(l, k)
				s[h&l.mask].put(h, k, i)
			}
			return s
		}
		ms, ts := buildMaps(), buildTables()
		mapGet := func(k string) (int, bool) { v, ok := ms[shardOf(l, k)][k]; return v, ok }
		tableGet := func(k string) (int, bool) { h := hashOf(l, k); return ts[h&l.mask].get(h, k) }

		for _, impl := range []struct {
			name  string
			get   func(string) (int, bool)
			apply func(i int)
			build func() any
		}{
			{"map", mapGet, func(i int) {
				k := keys[i%n]
				next := slices.Clone(ms)
				s := shardOf(l, k)
				next[s] = maps.Clone(ms[s])
				next[s][k] = i
			}, func() any { return buildMaps() }},
			{"table", tableGet, func(i int) {
				k := keys[i%n]
				next := slices.Clone(ts)
				h := hashOf(l, k)
				s := h & l.mask
				next[s] = ts[s].clone()
				next[s].put(h, k, i)
			}, func() any { return buildTables() }},
		} {
			prefix := fmt.Sprintf("n=%d/impl=%s", n, impl.name)
			b.Run(prefix+"/get-serial", func(b *testing.B) {
				i := 0
				for b.Loop() {
					impl.get(keys[order[i]])
					if i++; i == n {
						i = 0
					}
				}
			})
			b.Run(prefix+"/get-parallel", func(b *testing.B) {
				b.RunParallel(func(pb *testing.PB) {
					i := rand.IntN(n)
					for pb.Next() {
						impl.get(keys[order[i]])
						if i++; i == n {
							i = 0
						}
					}
				})
			})
			b.Run(prefix+"/apply-1-key", func(b *testing.B) {
				b.ReportAllocs()
				i := 0
				for b.Loop() {
					impl.apply(i * 7919)
					i++
				}
			})
			b.Run(prefix+"/build", func(b *testing.B) {
				b.ReportAllocs()
				var sink any
				for b.Loop() {
					sink = impl.build()
				}
				runtime.GC()
				var before, after runtime.MemStats
				runtime.ReadMemStats(&before)
				kept := impl.build()
				runtime.GC()
				runtime.ReadMemStats(&after)
				b.ReportMetric(float64(after.HeapAlloc-before.HeapAlloc)/float64(n), "B/entry")
				runtime.KeepAlive(kept)
				runtime.KeepAlive(sink)
			})
		}
		runtime.KeepAlive(ms)
		runtime.KeepAlive(ts)
	}
}
