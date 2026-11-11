package refset

import (
	"fmt"
	"strconv"
	"testing"
)

// BenchmarkApplyDeltas measures copy-on-write cost (design §4.11, rule R6):
// applying a batch copies every shard it touches, so cost depends on dataset
// size, shard count and how many keys the batch changes.
func BenchmarkApplyDeltas(b *testing.B) {
	for _, size := range []int{10_000, 100_000, 1_000_000} {
		for _, shards := range []int{16, 256, 4096} {
			for _, batch := range []int{1, 100} {
				name := fmt.Sprintf("entries=%d/shards=%d/keys=%d", size, shards, batch)
				b.Run(name, func(b *testing.B) {
					l := newLayout(shards)
					cur := &root[string, int]{l: l, shards: emptyShards[string, int](shards)}
					keys := make([]string, size)
					for i := range size {
						keys[i] = "key-" + strconv.Itoa(i)
						cur.shards[shardOf(l, keys[i])][keys[i]] = i
					}
					cur.n = size
					ops := make([]Op[string, int], batch)
					b.ReportAllocs()
					b.ResetTimer()
					for i := range b.N {
						for j := range ops {
							ops[j] = Op[string, int]{Key: keys[(i*batch+j*7919)%size], Value: i}
						}
						v := cur.version + 1
						cur, _ = applyDeltas(cur, v, []Delta[string, int]{{Version: v, Ops: ops}})
					}
				})
			}
		}
	}
}
