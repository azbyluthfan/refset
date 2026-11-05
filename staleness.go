package refset

import (
	"fmt"
	"time"
)

// staleness is the time since the replica last confirmed it was caught up with
// its source, using the larger of the monotonic and wall-clock elapsed times.
func (d *Dataset[K, V]) staleness() time.Duration {
	return elapsed(*d.lastConfirmed.Load(), d.cfg.clock.Now())
}

func (d *Dataset[K, V]) lag() uint64 {
	applied := d.root.Load().version
	head := d.knownHead.Load()
	if head <= applied {
		return 0
	}
	return head - applied
}

func (d *Dataset[K, V]) checkFresh() error {
	if d.cfg.maxLag > 0 {
		if lag := d.lag(); lag > d.cfg.maxLag {
			return fmt.Errorf("%w: %d versions behind (limit %d)", ErrTooStale, lag, d.cfg.maxLag)
		}
	}
	if d.cfg.maxStaleness > 0 {
		if s := d.staleness(); s > d.cfg.maxStaleness {
			return fmt.Errorf("%w: last confirmed current %v ago (limit %v)", ErrTooStale, s, d.cfg.maxStaleness)
		}
	}
	return nil
}
