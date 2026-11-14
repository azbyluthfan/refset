package refset

import (
	"errors"
	"fmt"
	"log/slog"
	"time"
)

// Options configures a [Dataset]. The zero value of every field means "use the
// default"; refset.Options{} is a valid configuration.
type Options struct {
	// Shards is the number of snapshot shards; a batch copies only the shards
	// it touches. 0 chooses it from the dataset's size, about 2.5 × √entries
	// rounded up to a power of two (256 at 10k entries, 4096 at 1M), at Open
	// and again when a full reload finds the size moved a lot. A non-zero
	// value fixes it; it must be a power of two. See docs/performance.md.
	Shards int

	// BatchWindow is the shortest time between polls started by Watch
	// notifications, so changes arriving close together are applied as one
	// batch. A notification after a quiet period polls at once; one that
	// arrives within BatchWindow of the last poll waits for the rest of the
	// window. A read waiting for a version (GetAtLeast, GetCtx) always polls
	// at once. 0 means 5ms.
	BatchWindow time.Duration

	// WaitBound is how long GetAtLeast and GetCtx wait for a version before
	// falling back. 0 means 50ms; -1 means never wait.
	WaitBound time.Duration

	// PollInterval is how often the dataset polls the source without a
	// notification. 0 means 1s.
	PollInterval time.Duration

	// MaxStaleness makes GetFresh fail with ErrTooStale when the replica has
	// not confirmed it is caught up for longer than this. 0 means unchecked.
	// Must be greater than PollInterval.
	MaxStaleness time.Duration

	// MaxLagVersions makes GetFresh fail with ErrTooStale when the replica is
	// more than this many versions behind the known head. 0 means unchecked.
	MaxLagVersions uint64

	// MaxConcurrentFallbacks caps KeyLoader calls in flight (after collapsing
	// concurrent calls for the same key). Over the cap, reads get
	// ErrNotCaughtUp at once instead of adding to the source's load at the
	// moment it is likely struggling. 0 means 32; -1 means unlimited.
	MaxConcurrentFallbacks int

	// ReloadInterval reloads the full snapshot periodically in the background,
	// bounding how long a change the source failed to report can leave the
	// replica diverged. 0 means never.
	ReloadInterval time.Duration

	// Reconcile compares each periodic reload (ReloadInterval) with the
	// version it replaces, and counts entries that differ although the source
	// reported no change to them: changes the source's feed missed, such as
	// an unlogged write or a ReloadSource change made without bumping the
	// version. They are counted in Stats().Divergences and logged as a
	// warning (the count, not the keys); the reloaded data is installed
	// either way. Costs one comparison of the dataset per interval, less
	// when the reload kept unchanged shards. Needs ReloadInterval.
	Reconcile bool

	// Clock supplies time. nil means the system clocks.
	Clock Clock

	// Logger receives warnings about source errors and reloads. nil discards.
	Logger *slog.Logger
}

const (
	defaultBatchWindow  = 5 * time.Millisecond
	defaultMaxFallbacks = 32
	defaultWaitBound    = 50 * time.Millisecond
	defaultPoll         = time.Second
	maxShards           = 1 << 16
)

type config struct {
	shards         int // 0: chosen from the dataset's size
	batchWindow    time.Duration
	waitBound      time.Duration // < 0 means never wait
	pollInterval   time.Duration
	maxStaleness   time.Duration
	maxLag         uint64
	maxFallbacks   int
	reloadInterval time.Duration
	reconcile      bool
	clock          Clock
	log            *slog.Logger
}

func (o Options) resolve() (config, error) {
	c := config{
		shards:         o.Shards,
		batchWindow:    o.BatchWindow,
		waitBound:      o.WaitBound,
		pollInterval:   o.PollInterval,
		maxStaleness:   o.MaxStaleness,
		maxLag:         o.MaxLagVersions,
		maxFallbacks:   o.MaxConcurrentFallbacks,
		reloadInterval: o.ReloadInterval,
		reconcile:      o.Reconcile,
		clock:          o.Clock,
		log:            o.Logger,
	}
	var errs []error
	bad := func(format string, args ...any) { errs = append(errs, fmt.Errorf(format, args...)) }

	if c.shards < 0 || c.shards > maxShards || c.shards&(c.shards-1) != 0 { // 0: automatic
		bad("Shards must be 0 (automatic) or a power of two between 1 and %d, got %d", maxShards, c.shards)
	}
	switch {
	case c.batchWindow == 0:
		c.batchWindow = defaultBatchWindow
	case c.batchWindow < 0:
		bad("BatchWindow must not be negative, got %v", c.batchWindow)
	}
	switch {
	case c.waitBound == 0:
		c.waitBound = defaultWaitBound
	case c.waitBound == -1:
	case c.waitBound < 0:
		bad("WaitBound must be positive, 0 (default) or -1 (never wait), got %v", c.waitBound)
	}
	switch {
	case c.pollInterval == 0:
		c.pollInterval = defaultPoll
	case c.pollInterval < 0:
		bad("PollInterval must not be negative, got %v", c.pollInterval)
	}
	switch {
	case c.maxStaleness < 0:
		bad("MaxStaleness must not be negative, got %v", c.maxStaleness)
	case c.maxStaleness > 0 && c.maxStaleness <= c.pollInterval:
		bad("MaxStaleness (%v) must be greater than PollInterval (%v), or a healthy replica fails between polls",
			c.maxStaleness, c.pollInterval)
	}
	switch {
	case c.maxFallbacks == 0:
		c.maxFallbacks = defaultMaxFallbacks
	case c.maxFallbacks == -1:
		c.maxFallbacks = 0 // unlimited
	case c.maxFallbacks < 0:
		bad("MaxConcurrentFallbacks must be positive, 0 (default 32) or -1 (unlimited), got %d", c.maxFallbacks)
	}
	if c.reloadInterval < 0 {
		bad("ReloadInterval must not be negative, got %v", c.reloadInterval)
	}
	if c.reconcile && c.reloadInterval == 0 {
		bad("Reconcile needs ReloadInterval: it compares each periodic reload")
	}
	if c.clock == nil {
		c.clock = systemClock{}
	}
	if c.log == nil {
		c.log = slog.New(slog.DiscardHandler)
	}
	if len(errs) > 0 {
		return config{}, fmt.Errorf("refset: invalid options: %w", errors.Join(errs...))
	}
	return c, nil
}
