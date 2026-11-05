package refset

import "time"

// Stats describes a dataset's state, for metrics and debugging.
type Stats struct {
	Name           string
	AppliedVersion uint64        // version served by Get
	HeadVersion    uint64        // latest version the source has reported
	LagVersions    uint64        // HeadVersion - AppliedVersion
	Staleness      time.Duration // time since the replica last confirmed it was caught up
	Entries        int
	LastApply      time.Time // wall time the served version was installed
	Waiters        int       // reads currently waiting for a version
	Polls          uint64    // refreshes attempted (changes or full reloads)
	PollErrors     uint64
	Reloads        uint64 // full snapshot loads after Open
	Fallbacks      uint64 // reads that timed out and went to KeyLoader
}

// Stats returns a point-in-time description of the dataset.
func (d *Dataset[K, V]) Stats() Stats {
	r := d.root.Load()
	d.mu.Lock()
	waiters := d.waiters.Len()
	d.mu.Unlock()
	return Stats{
		Name:           d.name,
		AppliedVersion: r.version,
		HeadVersion:    max(d.knownHead.Load(), r.version),
		LagVersions:    d.lag(),
		Staleness:      d.staleness(),
		Entries:        r.n,
		LastApply:      time.Unix(0, d.lastApply.Load()),
		Waiters:        waiters,
		Polls:          d.polls.Load(),
		PollErrors:     d.pollErrors.Load(),
		Reloads:        d.reloads.Load(),
		Fallbacks:      d.fallbacks.Load(),
	}
}
