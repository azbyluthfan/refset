package refset

import "reflect"

// reconcile compares a periodic full reload (next) with the version it
// replaces (cur) and returns how many entries differ although the source
// reported no change to them: entries the change feed missed (design §4.10).
// ok is false when the comparison isn't possible: the source can't list the
// changes since cur, or its history went backwards.
//
// Keys the source did report changing since cur are expected to differ and
// are skipped, so with a source that meets the contract the count is zero.
func (d *Dataset[K, V]) reconcile(cur, next *root[K, V]) (divergent int, ok bool) {
	if next.version < cur.version {
		return 0, false
	}
	// Asked after the load, so the range covers every change up to and
	// beyond the loaded version: a change racing with the load is reported
	// here and skipped, never counted as a divergence.
	_, deltas, err := d.src.Changes(d.ctx, cur.version)
	if err != nil {
		d.log.Info("refset: reconcile skipped: source can't list changes since the replaced version",
			"applied", cur.version, "err", err)
		return 0, false
	}
	touched := make(map[K]struct{})
	for _, dl := range deltas {
		if dl.Version > cur.version {
			for _, op := range dl.Ops {
				touched[op.Key] = struct{}{}
			}
		}
	}
	eq := d.eq
	if eq == nil {
		eq = func(a, b V) bool { return reflect.DeepEqual(a, b) }
	}
	sameLayout := cur.l == next.l
	for i, m := range next.shards {
		if sameLayout && sameMap(m, cur.shards[i]) {
			continue // kept by the reload: identical by construction
		}
		for k, v := range m {
			if _, ok := touched[k]; ok {
				continue
			}
			if ov, ok := cur.get(k); !ok || !eq(ov, v) {
				divergent++ // added or changed without a reported change
			}
		}
	}
	for i, m := range cur.shards {
		if sameLayout && sameMap(m, next.shards[i]) {
			continue
		}
		for k := range m {
			if _, ok := touched[k]; ok {
				continue
			}
			if _, ok := next.get(k); !ok {
				divergent++ // removed without a reported change
			}
		}
	}
	return divergent, true
}

// sameMap reports whether a and b are the same map, not just equal.
func sameMap[K comparable, V any](a, b map[K]V) bool {
	return reflect.ValueOf(a).Pointer() == reflect.ValueOf(b).Pointer()
}
