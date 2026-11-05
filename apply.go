package refset

import (
	"fmt"
	"maps"
	"slices"
)

// applyDeltas returns a new root at version head with deltas applied on top of
// cur. Only shards touched by the deltas are copied; the rest are shared with
// cur. Deltas at or below cur.version are skipped (duplicate delivery). cur is
// never modified.
func applyDeltas[K comparable, V any](l layout, cur *root[K, V], head uint64, deltas []Delta[K, V]) *root[K, V] {
	next := &root[K, V]{version: head, shards: slices.Clone(cur.shards), n: cur.n}
	var copied []bool
	for _, d := range deltas {
		if d.Version <= cur.version {
			continue
		}
		for _, op := range d.Ops {
			i := shardOf(l, op.Key)
			if copied == nil {
				copied = make([]bool, len(next.shards))
			}
			if !copied[i] {
				next.shards[i] = maps.Clone(cur.shards[i])
				copied[i] = true
			}
			m := next.shards[i]
			_, existed := m[op.Key]
			if op.Delete {
				if existed {
					delete(m, op.Key)
					next.n--
				}
				continue
			}
			if !existed {
				next.n++
			}
			m[op.Key] = op.Value
		}
	}
	return next
}

// checkChanges validates a Changes result against the contract before it is
// applied.
func checkChanges[K comparable, V any](name string, applied, head uint64, deltas []Delta[K, V]) error {
	var prev uint64
	for _, d := range deltas {
		if d.Version > head {
			return fmt.Errorf("refset: source %q returned a delta at version %d above its head %d", name, d.Version, head)
		}
		if d.Version < prev {
			return fmt.Errorf("refset: source %q returned deltas out of order (%d after %d)", name, d.Version, prev)
		}
		prev = d.Version
	}
	if head < applied {
		return fmt.Errorf("refset: source %q reported head %d below applied version %d without ErrSnapshotRequired",
			name, head, applied)
	}
	return nil
}
