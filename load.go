package refset

import (
	"context"
	"hash/maphash"
	"reflect"
)

// load reads a full snapshot from the source. prev is the version being
// replaced, or nil at Open.
//
// The shard count is Options.Shards when set. Otherwise it is chosen from the
// dataset's size: at Open the entries are staged first so the size is known;
// on a reload it comes from prev, and the result is re-sharded only if the
// size moved by more than a factor of 16 (a factor of 4 in shard count).
//
// On a reload with an unchanged shard count and a way to compare values (see
// equalFor), shards whose entries all came back unchanged are kept from prev
// instead of rebuilt: a reload that changes little allocates little and needs
// little more memory than the current version.
func (d *Dataset[K, V]) load(ctx context.Context, prev *root[K, V]) (*root[K, V], copyStats, error) {
	fixed := d.cfg.shards > 0
	if !fixed && prev == nil {
		return d.loadStaged(ctx)
	}
	shards := d.cfg.shards
	if !fixed {
		shards = autoShards(prev.n)
	}
	l := d.base.withShards(shards)

	var r *root[K, V]
	var st copyStats
	var err error
	if prev != nil && d.eq != nil && prev.l == l {
		r, st, err = d.loadDiff(ctx, prev)
	} else {
		hint := 0
		if prev != nil {
			hint = prev.n
		}
		r, err = d.loadInto(ctx, l, hint)
		st.shardsCopied = shards
	}
	if err != nil {
		return nil, copyStats{}, err
	}
	if want := autoShards(r.n); !fixed && (want >= 4*shards || 4*want <= shards) {
		r = reshard(r, d.base.withShards(want))
		st = copyStats{shardsCopied: want}
	}
	return r, st, nil
}

// loadInto loads every entry into new shards with layout l, sized for about
// hint entries.
func (d *Dataset[K, V]) loadInto(ctx context.Context, l layout, hint int) (*root[K, V], error) {
	shards := sizedShards[K, V](l.shards(), hint)
	version, err := d.src.LoadSnapshot(ctx, func(k K, v V) {
		shards[shardOf(l, k)][k] = v
	})
	if err != nil {
		return nil, err
	}
	return newRoot(version, l, shards), nil
}

// loadStaged collects the entries before building shards, so the shard count
// can be chosen from the dataset's size and every shard map created at its
// final size. Staging costs one slice of entries, smaller than the maps.
func (d *Dataset[K, V]) loadStaged(ctx context.Context) (*root[K, V], copyStats, error) {
	type entry struct {
		k K
		v V
	}
	// Chunks instead of one growing slice: appending would copy everything
	// staged so far each time it grows.
	const maxChunk = 1 << 14
	var chunks [][]entry
	var cur []entry
	total := 0
	version, err := d.src.LoadSnapshot(ctx, func(k K, v V) {
		if len(cur) == cap(cur) {
			if cur != nil {
				chunks = append(chunks, cur)
			}
			cur = make([]entry, 0, min(maxChunk, max(64, total)))
		}
		cur = append(cur, entry{k, v})
		total++
	})
	if err != nil {
		return nil, copyStats{}, err
	}
	chunks = append(chunks, cur)
	n := autoShards(total)
	l := d.base.withShards(n)
	shards := sizedShards[K, V](n, total)
	for i, c := range chunks {
		for _, e := range c {
			shards[shardOf(l, e.k)][e.k] = e.v
		}
		chunks[i] = nil // let the GC take each chunk once it is copied
	}
	return newRoot(version, l, shards), copyStats{shardsCopied: n}, nil
}

// loadDiff loads a full snapshot on top of prev, which has the same layout.
//
// While a shard's entries keep matching prev, only their keys are recorded.
// The first entry that differs turns the shard into a new map, seeded with the
// keys seen so far (their values equal prev's). At the end, a shard that never
// differed is kept from prev if the load returned exactly prev's keys for it;
// otherwise it is rebuilt from the recorded keys, which drops deleted entries.
func (d *Dataset[K, V]) loadDiff(ctx context.Context, prev *root[K, V]) (*root[K, V], copyStats, error) {
	l := prev.l
	n := len(prev.shards)
	fresh := make([]map[K]V, n) // nil while the shard matches prev
	kept := make([][]K, n)      // keys that matched prev, while fresh is nil
	version, err := d.src.LoadSnapshot(ctx, func(k K, v V) {
		i := shardOf(l, k)
		if m := fresh[i]; m != nil {
			m[k] = v
			return
		}
		old := prev.shards[i]
		if ov, ok := old[k]; ok && d.eq(ov, v) {
			if kept[i] == nil {
				kept[i] = make([]K, 0, len(old))
			}
			kept[i] = append(kept[i], k)
			return
		}
		m := make(map[K]V, len(old)+1)
		for _, kk := range kept[i] {
			m[kk] = old[kk]
		}
		kept[i] = nil
		m[k] = v
		fresh[i] = m
	})
	if err != nil {
		return nil, copyStats{}, err
	}

	var st copyStats
	shards := make([]map[K]V, n)
	seed := maphash.MakeSeed()
	for i, old := range prev.shards {
		switch {
		case fresh[i] != nil:
			shards[i] = fresh[i]
			st.shardsCopied++
		case len(kept[i]) == len(old) && sameKeys(seed, kept[i], old):
			shards[i] = old
			st.shardsKept++
		default: // entries were deleted (or a key was returned twice)
			m := make(map[K]V, len(kept[i]))
			for _, k := range kept[i] {
				m[k] = old[k]
			}
			shards[i] = m
			st.shardsCopied++
		}
		kept[i] = nil
	}
	return newRoot(version, l, shards), st, nil
}

// sameKeys reports whether keys, every one of which is in m and with
// len(keys) == len(m), holds each of m's keys exactly once. That fails only
// if the source returned a key twice and left another out. It compares sums
// of seeded 64-bit hashes, so a false match needs a hash collision.
func sameKeys[K comparable, V any](seed maphash.Seed, keys []K, m map[K]V) bool {
	var a, b uint64
	for _, k := range keys {
		a += maphash.Comparable(seed, k)
	}
	for k := range m {
		b += maphash.Comparable(seed, k)
	}
	return a == b
}

// reshard copies r into shards with layout l.
func reshard[K comparable, V any](r *root[K, V], l layout) *root[K, V] {
	shards := sizedShards[K, V](l.shards(), r.n)
	for _, m := range r.shards {
		for k, v := range m {
			shards[shardOf(l, k)][k] = v
		}
	}
	return newRoot(r.version, l, shards)
}

// sizedShards returns n empty shard maps, each sized for its share of about
// total entries, so loading doesn't grow them step by step.
func sizedShards[K comparable, V any](n, total int) []map[K]V {
	per := 0
	if total > 0 {
		per = total/n + total/(8*n) + 1 // a little headroom for uneven hashing
	}
	s := make([]map[K]V, n)
	for i := range s {
		s[i] = make(map[K]V, per)
	}
	return s
}

func newRoot[K comparable, V any](version uint64, l layout, shards []map[K]V) *root[K, V] {
	n := 0
	for _, m := range shards {
		n += len(m)
	}
	return &root[K, V]{version: version, l: l, shards: shards, n: n}
}

// equalFor returns how a reload compares values to keep unchanged shards:
// the source's own function if it offers one (ReloadSource.Equal), else ==
// when V is comparable and contains no interface (== on an interface holding
// an uncomparable value panics), else nil, which turns diffing off.
func equalFor[K comparable, V any](src Source[K, V]) func(a, b V) bool {
	if s, ok := src.(interface{ equalFunc() func(a, b V) bool }); ok {
		if f := s.equalFunc(); f != nil {
			return f
		}
	}
	if !safelyComparable(reflect.TypeFor[V]()) {
		return nil
	}
	// The conversions don't escape, so this doesn't allocate.
	return func(a, b V) bool { return any(a) == any(b) }
}

// safelyComparable reports whether == on values of t is defined and can't
// panic.
func safelyComparable(t reflect.Type) bool {
	if !t.Comparable() {
		return false
	}
	switch t.Kind() {
	case reflect.Interface:
		return false
	case reflect.Array:
		return safelyComparable(t.Elem())
	case reflect.Struct:
		for i := range t.NumField() {
			if !safelyComparable(t.Field(i).Type) {
				return false
			}
		}
	}
	return true
}
