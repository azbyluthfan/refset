package refset

import (
	"context"
	"errors"
	"maps"
	"math"
	"math/rand/v2"
	"reflect"
	"slices"
	"testing"
)

// listSource is a Source whose snapshot is a list of entries, returned in
// order (duplicates allowed: the later one wins). Changes always asks for a
// full reload, so tests drive reloads by calling load directly.
type listSource[K comparable, V any] struct {
	version uint64
	entries []Op[K, V]
	eq      func(a, b V) bool
}

func (s *listSource[K, V]) Name() string { return "list" }
func (s *listSource[K, V]) LoadSnapshot(_ context.Context, add func(K, V)) (uint64, error) {
	for _, e := range s.entries {
		add(e.Key, e.Value)
	}
	return s.version, nil
}
func (s *listSource[K, V]) Changes(context.Context, uint64) (uint64, []Delta[K, V], error) {
	return 0, nil, ErrSnapshotRequired
}
func (s *listSource[K, V]) Watch(context.Context) (<-chan uint64, error) {
	return nil, errors.New("no watch")
}
func (s *listSource[K, V]) equalFunc() func(a, b V) bool { return s.eq }

func (s *listSource[K, V]) set(m map[K]V) {
	s.version++
	s.entries = s.entries[:0]
	for k, v := range m {
		s.entries = append(s.entries, Op[K, V]{Key: k, Value: v})
	}
}

func openList[K comparable, V any](t *testing.T, src *listSource[K, V], shards int) *Dataset[K, V] {
	t.Helper()
	d, err := Open(context.Background(), Source[K, V](src), Options{Shards: shards, PollInterval: 1 << 62})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	return d
}

// reload runs one full reload the way the loop does and installs it.
func reload[K comparable, V any](t *testing.T, d *Dataset[K, V]) (*root[K, V], *root[K, V], copyStats) {
	t.Helper()
	prev := d.root.Load()
	next, st, err := d.load(context.Background(), prev)
	if err != nil {
		t.Fatal(err)
	}
	d.root.Store(next)
	return prev, next, st
}

func rootContents[K comparable, V any](r *root[K, V]) map[K]V {
	out := make(map[K]V)
	for _, m := range r.shards {
		maps.Copy(out, m)
	}
	return out
}

func samePtr(a, b any) bool { return reflect.ValueOf(a).Pointer() == reflect.ValueOf(b).Pointer() }

func TestAutoShards(t *testing.T) {
	for _, c := range []struct{ n, want int }{
		{0, 16}, {10, 16}, {1000, 128}, {10_000, 256}, {100_000, 512}, {199_999, 512}, {200_000, 2048}, {1_000_000, 4096}, {math.MaxInt, maxShards},
	} {
		if got := autoShards(c.n); got != c.want {
			t.Errorf("autoShards(%d) = %d, want %d", c.n, got, c.want)
		}
	}
}

func intEntries(n int) map[int]int {
	m := make(map[int]int, n)
	for i := range n {
		m[i] = i
	}
	return m
}

func TestOpenChoosesShardCount(t *testing.T) {
	src := &listSource[int, int]{}
	src.set(intEntries(100_000))
	if got := openList(t, src, 0).Stats().Shards; got != 512 {
		t.Errorf("automatic: %d shards for 100k entries, want 512", got)
	}
	if got := openList(t, src, 64).Stats().Shards; got != 64 {
		t.Errorf("Options.Shards 64: got %d", got)
	}
}

func TestReloadReshardsWhenSizeMovesALot(t *testing.T) {
	src := &listSource[int, int]{}
	src.set(intEntries(1000))
	d := openList(t, src, 0)
	if got := d.Stats().Shards; got != 128 {
		t.Fatalf("1k entries: %d shards, want 128", got)
	}
	src.set(intEntries(4000)) // 4× the entries: 2× the shards, not worth a reshard
	if _, next, _ := reload(t, d); len(next.shards) != 128 || next.n != 4000 {
		t.Fatalf("4k entries: %d shards, n %d; want 128 kept", len(next.shards), next.n)
	}
	src.set(intEntries(1_000_000)) // from 256 shards (chosen at 4k) to 4096
	_, next, _ := reload(t, d)
	if len(next.shards) != 4096 || next.n != 1_000_000 || !maps.Equal(rootContents(next), intEntries(1_000_000)) {
		t.Fatalf("1M entries: %d shards, n %d; want 4096 and full contents", len(next.shards), next.n)
	}
	src.set(intEntries(10))
	if _, next, _ := reload(t, d); len(next.shards) != 16 || next.n != 10 {
		t.Fatalf("shrunk to 10 entries: %d shards, n %d; want 16", len(next.shards), next.n)
	}
}

func TestReloadKeepsUnchangedShards(t *testing.T) {
	src := &listSource[int, int]{}
	src.set(intEntries(10_000))
	d := openList(t, src, 0)
	src.version++ // same contents, new version
	prev, next, st := reload(t, d)
	if st.shardsKept != len(prev.shards) || st.shardsCopied != 0 {
		t.Fatalf("unchanged reload: kept %d, copied %d of %d shards", st.shardsKept, st.shardsCopied, len(prev.shards))
	}
	for i := range prev.shards {
		if !samePtr(prev.shards[i], next.shards[i]) {
			t.Fatalf("shard %d was rebuilt", i)
		}
	}
	if next.version != src.version || next.n != 10_000 {
		t.Fatalf("version %d n %d", next.version, next.n)
	}

	// One changed value rebuilds exactly its shard.
	m := intEntries(10_000)
	m[42] = -1
	src.set(m)
	prev, next, st = reload(t, d)
	if st.shardsCopied != 1 || st.shardsKept != len(prev.shards)-1 {
		t.Fatalf("one change: copied %d, kept %d", st.shardsCopied, st.shardsKept)
	}
	if v, _ := next.get(42); v != -1 {
		t.Fatalf("key 42 = %d", v)
	}
	if v, _ := prev.get(42); v != 42 {
		t.Fatal("previous version was modified")
	}
}

// A source may return a key twice. If it also leaves out another key of the
// same shard, the count of matching keys equals the old shard's size, and the
// shard must still not be kept.
func TestReloadDuplicateKeyHidingDeletion(t *testing.T) {
	src := &listSource[string, int]{version: 1, entries: []Op[string, int]{{Key: "a", Value: 1}, {Key: "b", Value: 2}}}
	d := openList(t, src, 1) // one shard: a and b share it
	src.version = 2
	src.entries = []Op[string, int]{{Key: "a", Value: 1}, {Key: "a", Value: 1}} // b deleted, a twice
	_, next, st := reload(t, d)
	if got := rootContents(next); !maps.Equal(got, map[string]int{"a": 1}) || next.n != 1 || st.shardsKept != 0 {
		t.Fatalf("got %v n=%d kept=%d; want only a", got, next.n, st.shardsKept)
	}
}

// Randomized: after every reload the dataset equals the source exactly, and a
// shard is kept only if none of its keys changed.
func TestReloadDiffMatchesModel(t *testing.T) {
	for _, shards := range []int{0, 1, 16} {
		rng := rand.New(rand.NewPCG(uint64(shards), 7))
		model := intEntries(500)
		src := &listSource[int, int]{}
		src.set(model)
		d := openList(t, src, shards)
		for round := range 300 {
			changed := map[int]bool{}
			for range rng.IntN(6) {
				k := rng.IntN(700)
				changed[k] = true
				switch rng.IntN(3) {
				case 0:
					delete(model, k)
				default:
					model[k] = rng.IntN(5) // often the same value again
				}
			}
			src.set(model)
			if rng.IntN(4) == 0 && len(src.entries) > 0 { // repeat an entry
				src.entries = append(src.entries, src.entries[rng.IntN(len(src.entries))])
			}
			rng.Shuffle(len(src.entries), func(i, j int) { src.entries[i], src.entries[j] = src.entries[j], src.entries[i] })
			prev, next, _ := reload(t, d)
			if got := rootContents(next); !maps.Equal(got, model) || next.n != len(model) {
				t.Fatalf("shards=%d round %d: contents differ (n=%d, want %d)", shards, round, next.n, len(model))
			}
			if len(prev.shards) != len(next.shards) {
				continue
			}
			for i := range next.shards {
				if samePtr(prev.shards[i], next.shards[i]) {
					for k := range changed {
						pv, pok := prev.shards[i][k]
						nv, nok := model[k]
						if shardOf(next.l, k) == i && (pok != nok || pv != nv) {
							t.Fatalf("shards=%d round %d: shard %d kept although key %d changed", shards, round, i, k)
						}
					}
				}
			}
		}
	}
}

type withSlice struct {
	ID   string
	Tags []string
}

type withIface struct {
	ID string
	X  any
}

func TestReloadDiffNeedsAWayToCompare(t *testing.T) {
	// Not comparable, no Equal: every shard is rebuilt.
	s1 := &listSource[string, withSlice]{version: 1, entries: []Op[string, withSlice]{{Key: "a", Value: withSlice{"a", []string{"x"}}}}}
	d1 := openList(t, s1, 4)
	if d1.eq != nil {
		t.Fatal("eq set for an uncomparable type")
	}
	s1.version++
	if _, _, st := reload(t, d1); st.shardsKept != 0 || st.shardsCopied != 4 {
		t.Fatalf("no Equal: kept %d copied %d", st.shardsKept, st.shardsCopied)
	}

	// Same type with an Equal function: unchanged shards are kept.
	s2 := &listSource[string, withSlice]{version: 1, entries: s1.entries,
		eq: func(a, b withSlice) bool { return a.ID == b.ID && slices.Equal(a.Tags, b.Tags) }}
	d2 := openList(t, s2, 4)
	s2.version++
	if _, _, st := reload(t, d2); st.shardsKept != 4 {
		t.Fatalf("with Equal: kept %d of 4", st.shardsKept)
	}

	// Comparable by the compiler's rules, but == could panic on what the
	// interface holds: no diffing, and no panic.
	s3 := &listSource[string, withIface]{version: 1, entries: []Op[string, withIface]{{Key: "a", Value: withIface{"a", []int{1}}}}}
	d3 := openList(t, s3, 4)
	if d3.eq != nil {
		t.Fatal("eq set for a type holding an interface")
	}
	s3.version++
	reload(t, d3)
}

func TestSafelyComparable(t *testing.T) {
	type inner struct{ A [2]string }
	type ptr struct{ P *withSlice }
	for _, c := range []struct {
		t    reflect.Type
		want bool
	}{
		{reflect.TypeFor[int](), true},
		{reflect.TypeFor[string](), true},
		{reflect.TypeFor[inner](), true},
		{reflect.TypeFor[ptr](), true}, // pointers compare by address
		{reflect.TypeFor[*withSlice](), true},
		{reflect.TypeFor[withSlice](), false},
		{reflect.TypeFor[withIface](), false},
		{reflect.TypeFor[any](), false},
		{reflect.TypeFor[[1]any](), false},
		{reflect.TypeFor[[]int](), false},
	} {
		if got := safelyComparable(c.t); got != c.want {
			t.Errorf("%v: got %v, want %v", c.t, got, c.want)
		}
	}
}
