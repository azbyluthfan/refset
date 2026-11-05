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
	// it touches. 0 means 256, which suits up to about 100k entries; use about
	// 1024 at 100k and 4096 at 1M (roughly 2.5 × √entries). Must be a power
	// of two. See docs/performance.md.
	Shards int

	// BatchWindow delays the poll after a Watch notification so that changes
	// arriving close together are applied as one batch. 0 means 5ms.
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
	// ErrNotCaughtUp. 0 means unlimited.
	MaxConcurrentFallbacks int

	// ReloadInterval reloads the full snapshot periodically in the background,
	// bounding how long a change the source failed to report can leave the
	// replica diverged. 0 means never.
	ReloadInterval time.Duration

	// Clock supplies time. nil means the system clocks.
	Clock Clock

	// Logger receives warnings about source errors and reloads. nil discards.
	Logger *slog.Logger
}

const (
	defaultShards      = 256
	defaultBatchWindow = 5 * time.Millisecond
	defaultWaitBound   = 50 * time.Millisecond
	defaultPoll        = time.Second
	maxShards          = 1 << 16
)

type config struct {
	shards         int
	batchWindow    time.Duration
	waitBound      time.Duration // < 0 means never wait
	pollInterval   time.Duration
	maxStaleness   time.Duration
	maxLag         uint64
	maxFallbacks   int
	reloadInterval time.Duration
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
		clock:          o.Clock,
		log:            o.Logger,
	}
	var errs []error
	bad := func(format string, args ...any) { errs = append(errs, fmt.Errorf(format, args...)) }

	switch {
	case c.shards == 0:
		c.shards = defaultShards
	case c.shards < 0 || c.shards > maxShards || c.shards&(c.shards-1) != 0:
		bad("Shards must be a power of two between 1 and %d, got %d", maxShards, c.shards)
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
	if c.maxFallbacks < 0 {
		bad("MaxConcurrentFallbacks must not be negative, got %d", c.maxFallbacks)
	}
	if c.reloadInterval < 0 {
		bad("ReloadInterval must not be negative, got %v", c.reloadInterval)
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
