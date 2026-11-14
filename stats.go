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
	Shards         int       // shards in the served version (see Options.Shards)
	LastApply      time.Time // wall time the served version was installed
	Waiters        int       // reads currently waiting for a version
	Polls          uint64    // refreshes attempted (changes or full reloads)
	PollErrors     uint64
	Reloads        uint64 // full snapshot loads after Open
	Panics         uint64 // panics recovered in source code (see PanicError)
	Fallbacks      uint64 // reads that timed out and went to KeyLoader

	// Copy-on-write work, to judge the shard count (docs/performance.md).
	// Each applied batch copies the shards it touches; each reload builds
	// new shards except those it keeps unchanged.
	Applies       uint64 // batches of changes applied
	ShardsCopied  uint64 // shards copied by applies or rebuilt by reloads
	EntriesCopied uint64 // entries in the shards applies copied
	ShardsKept    uint64 // shards reloads kept because nothing in them changed

	// Reconcile check (Options.Reconcile).
	Reconciles  uint64 // periodic reloads compared with the version they replaced
	Divergences uint64 // entries found different without a reported change, in total
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
		Shards:         len(r.shards),
		LastApply:      time.Unix(0, d.lastApply.Load()),
		Waiters:        waiters,
		Polls:          d.polls.Load(),
		PollErrors:     d.pollErrors.Load(),
		Reloads:        d.reloads.Load(),
		Panics:         d.panics.Load(),
		Fallbacks:      d.fallbacks.Load(),
		Applies:        d.applies.Load(),
		ShardsCopied:   d.shardsCopied.Load(),
		EntriesCopied:  d.entriesCopied.Load(),
		ShardsKept:     d.shardsKept.Load(),
		Reconciles:     d.reconciles.Load(),
		Divergences:    d.divergences.Load(),
	}
}
