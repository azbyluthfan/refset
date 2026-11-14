package refset

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// kickBackoff pauses request-triggered polls after a failed poll, so a failing
// source isn't hammered by every waiting request. Ticker polls continue.
const kickBackoff = 50 * time.Millisecond

// Dataset is an in-memory replica of one reference dataset, kept current by a
// [Source]. Reads are local; Get is lock-free and never blocks.
//
// Values returned by Dataset methods are shared with every other reader. Treat
// them as read-only: if V contains a map, slice or pointer, copy before
// modifying, or every reader sees the change.
//
// A Dataset is safe for concurrent use. Close it to stop the background loop.
type Dataset[K comparable, V any] struct {
	// Fields are grouped by who writes them, with cache-line padding between
	// groups (design §4.11, rule R2), so the loop's per-poll writes and
	// waiting requests' writes never invalidate the lines every Get reads.

	// Read-hot: read by every Get. Set at Open; root changes only when a new
	// version is applied, which readers must observe anyway.
	root     atomic.Pointer[root[K, V]]
	base     layout            // hash seed; each root carries its own shard count
	eq       func(a, b V) bool // compares values on reloads; nil turns diffing off
	cfg      config
	src      Source[K, V]
	loader   KeyLoader[K, V] // nil if src doesn't implement KeyLoader
	name     string
	log      *slog.Logger
	kick     chan struct{}
	flights  *flights[K, V]
	ctx      context.Context
	cancel   context.CancelFunc
	loopDone chan struct{}
	_        cacheLinePad

	// Written by the background loop on every poll (and fallbacks by slow-path
	// reads). Read by GetFresh, waiting reads and Stats.
	knownHead     atomic.Uint64
	lastConfirmed atomic.Pointer[Reading]
	lastApply     atomic.Int64 // wall ns of the last change applied
	polls         atomic.Uint64
	pollErrors    atomic.Uint64
	reloads       atomic.Uint64
	fallbacks     atomic.Uint64
	applies       atomic.Uint64
	shardsCopied  atomic.Uint64
	entriesCopied atomic.Uint64
	shardsKept    atomic.Uint64
	reconciles    atomic.Uint64
	divergences   atomic.Uint64
	fromFile      atomic.Bool // started from a snapshot file and not yet caught up
	_             cacheLinePad

	// Written by waiting reads and the loop under mu.
	mu           sync.Mutex // guards waiters, pollsStarted, closed
	waiters      waiterHeap
	pollsStarted uint64
	closed       bool
	close        sync.Once
}

// Open loads the full dataset from src and starts keeping it current in the
// background. It blocks until the snapshot is in memory, so reads are never
// served from a partial or empty dataset. If the snapshot cannot be loaded
// before ctx ends, Open returns an error. ctx bounds only the initial load;
// call Close to stop the dataset.
func Open[K comparable, V any](ctx context.Context, src Source[K, V], opts Options) (*Dataset[K, V], error) {
	if src == nil {
		return nil, errors.New("refset: Open with nil Source")
	}
	cfg, err := opts.resolve()
	if err != nil {
		return nil, err
	}
	// Optional capabilities belong to the source itself, even when it is
	// wrapped (SnapshotFile).
	caps := src
	if w, ok := src.(wrapped[K, V]); ok {
		caps = w.unwrap()
	}
	d := &Dataset[K, V]{
		src:      src,
		name:     src.Name(),
		cfg:      cfg,
		base:     newLayout(1),
		eq:       equalFor(caps),
		kick:     make(chan struct{}, 1),
		flights:  newFlights[K, V](cfg.maxFallbacks),
		loopDone: make(chan struct{}),
	}
	d.log = cfg.log.With("dataset", d.name)
	if kl, ok := caps.(KeyLoader[K, V]); ok {
		d.loader = kl
	}
	start := cfg.clock.Now()
	r, fromFile := d.loadFromFile(ctx)
	if r == nil {
		if r, _, err = d.load(ctx, nil); err != nil {
			return nil, fmt.Errorf("refset: open dataset %q: %w", d.name, err)
		}
	}
	d.root.Store(r)
	d.knownHead.Store(r.version)
	if fromFile {
		// Data from disk has never been confirmed current: GetFresh fails
		// closed until the first successful refresh confirms it.
		never := Reading{MonoNanos: start.MonoNanos - 1<<62, WallNanos: 0}
		d.lastConfirmed.Store(&never)
		d.fromFile.Store(true)
	} else {
		d.lastConfirmed.Store(&start)
	}
	d.lastApply.Store(start.WallNanos)

	d.ctx, d.cancel = context.WithCancel(context.Background())
	watch, err := src.Watch(d.ctx)
	if err != nil {
		d.log.Warn("refset: watch unavailable; relying on polling", "err", err)
		watch = nil
	}
	go d.loop(watch)
	if fromFile {
		d.kick <- struct{}{} // catch up with the source at once
	}
	return d, nil
}

// loadFromFile loads the snapshot file if the source has one and it is
// usable. Any problem is logged and Open loads from the source instead.
func (d *Dataset[K, V]) loadFromFile(ctx context.Context) (*root[K, V], bool) {
	c, ok := d.src.(snapshotCache[K, V])
	if !ok {
		return nil, false
	}
	r, _, err := d.loadStaged(ctx, c.loadCached)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		d.log.Info("refset: no snapshot file yet; loading from the source")
		return nil, false
	case err != nil:
		d.log.Warn("refset: snapshot file not used; loading from the source", "err", err)
		return nil, false
	}
	d.log.Info("refset: started from snapshot file; catching up with the source", "version", r.version, "entries", r.n)
	return r, true
}

// Name returns the dataset's name, as reported by its Source.
func (d *Dataset[K, V]) Name() string { return d.name }

// Version returns the version currently served by Get.
func (d *Dataset[K, V]) Version() uint64 { return d.root.Load().version }

// Snapshot returns an immutable view of the current version.
func (d *Dataset[K, V]) Snapshot() Snapshot[K, V] {
	return Snapshot[K, V]{r: d.root.Load()}
}

// Read path (design §4.11, rule R1): Get, Snapshot, GetFresh, and GetAtLeast /
// GetCtx when the version is already applied must not write shared memory or
// allocate. A write here would invalidate the cache line on every core that is
// reading; read_path_test.go checks the allocation half of this rule.

// Get returns the value for key from the current version. It never waits and
// never fails; use GetAtLeast, GetCtx or GetFresh when freshness matters.
func (d *Dataset[K, V]) Get(key K) (V, bool) {
	return d.root.Load().get(key)
}

// GetAtLeast returns the value for key at version minVersion or newer.
//
// If the replica already has that version, it answers immediately. Otherwise
// it waits up to Options.WaitBound for the version to be applied, asking the
// background loop to poll the source at once if the version isn't known yet.
// On timeout it calls the source's KeyLoader if there is one, and returns
// ErrNotCaughtUp otherwise. If a poll that started after this call shows the
// source's head is below minVersion, it returns ErrVersionAhead without a
// fallback. If ctx ends first, it returns ctx.Err().
func (d *Dataset[K, V]) GetAtLeast(ctx context.Context, key K, minVersion uint64) (V, bool, error) {
	if d.root.Load().version >= minVersion {
		v, ok := d.Get(key)
		return v, ok, nil
	}
	if d.cfg.waitBound < 0 {
		return d.fallback(ctx, key)
	}
	w, err := d.register(minVersion)
	if err != nil {
		var zero V
		return zero, false, err
	}
	if w == nil { // applied while registering
		v, ok := d.Get(key)
		return v, ok, nil
	}

	t := d.cfg.clock.NewTimer(d.cfg.waitBound)
	defer t.Stop()
	select {
	case err := <-w.ch:
		return d.afterWait(key, err)
	case <-t.C():
		if !d.unregister(w) {
			return d.afterWait(key, <-w.ch)
		}
		return d.fallback(ctx, key)
	case <-ctx.Done():
		if !d.unregister(w) {
			return d.afterWait(key, <-w.ch)
		}
		var zero V
		return zero, false, ctx.Err()
	}
}

func (d *Dataset[K, V]) afterWait(key K, err error) (V, bool, error) {
	if err != nil {
		var zero V
		return zero, false, err
	}
	v, ok := d.Get(key)
	return v, ok, nil
}

// GetCtx is GetAtLeast with the minimum version taken from ctx (see
// ContextWithMinVersion) for this dataset's name. Without a minimum in ctx, it
// behaves like Get.
func (d *Dataset[K, V]) GetCtx(ctx context.Context, key K) (V, bool, error) {
	if minVersion, ok := MinVersion(ctx, d.name); ok {
		return d.GetAtLeast(ctx, key, minVersion)
	}
	v, ok := d.Get(key)
	return v, ok, nil
}

// GetFresh returns the value for key only if the replica can confirm it is
// current within Options.MaxStaleness and Options.MaxLagVersions; otherwise it
// returns an error wrapping ErrTooStale. Use it for fail-closed decisions such
// as permission checks.
//
// "Current" means caught up with every change the source has reported. A change
// the source never reported (see the Source contract) cannot be detected.
func (d *Dataset[K, V]) GetFresh(key K) (V, bool, error) {
	if err := d.checkFresh(); err != nil {
		var zero V
		return zero, false, err
	}
	v, ok := d.Get(key)
	return v, ok, nil
}

// Close stops the background loop and fails any waiting reads with ErrClosed.
// Get and Snapshot keep serving the last version.
func (d *Dataset[K, V]) Close() error {
	d.close.Do(func() {
		d.cancel()
		<-d.loopDone
		d.mu.Lock()
		d.closed = true
		d.waiters.releaseAll(ErrClosed)
		d.mu.Unlock()
	})
	return nil
}

// register adds a waiter for minVersion. It returns nil, nil if the version was
// applied in the meantime.
func (d *Dataset[K, V]) register(minVersion uint64) (*waiter, error) {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return nil, ErrClosed
	}
	if d.root.Load().version >= minVersion {
		d.mu.Unlock()
		return nil, nil
	}
	w := &waiter{min: minVersion, ch: make(chan error, 1)}
	if minVersion > d.knownHead.Load() {
		// Unknown version: ErrVersionAhead needs a poll that starts after
		// this request (design §4.5).
		w.needSeq = d.pollsStarted + 1
	}
	heap.Push(&d.waiters, w)
	d.mu.Unlock()

	// Ask for a poll now even if a notification already announced the
	// version, rather than wait for a batched poll.
	select {
	case d.kick <- struct{}{}:
	default: // a poll is already queued; it serves this request too
	}
	return w, nil
}

func (d *Dataset[K, V]) unregister(w *waiter) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.waiters.remove(w)
}

func (d *Dataset[K, V]) fallback(ctx context.Context, key K) (V, bool, error) {
	if d.loader == nil {
		var zero V
		return zero, false, ErrNotCaughtUp
	}
	d.fallbacks.Add(1)
	return d.flights.do(ctx, key, func() (V, bool, error) {
		return d.loader.LoadKey(ctx, key)
	})
}

// loop is the only goroutine that calls Changes and LoadSnapshot after Open,
// and the only writer of d.root.
func (d *Dataset[K, V]) loop(watch <-chan uint64) {
	defer close(d.loopDone)
	clock := d.cfg.clock

	// lastStart is the monotonic start time of the last refresh, if any;
	// failed says whether it failed.
	var refreshed, failed bool
	var lastStart int64
	refresh := func(full bool) {
		start := clock.Now().MonoNanos
		failed = !d.refresh(full)
		refreshed, lastStart = true, start
	}

	tick := clock.NewTimer(d.cfg.pollInterval)
	var batch, reload Timer
	var batchC, reloadC <-chan time.Time
	if d.cfg.reloadInterval > 0 {
		reload = clock.NewTimer(d.cfg.reloadInterval)
		reloadC = reload.C()
	}
	stopBatch := func() {
		if batchC != nil {
			batch.Stop()
			batchC = nil
		}
	}
	defer func() {
		tick.Stop()
		if batch != nil {
			batch.Stop()
		}
		if reload != nil {
			reload.Stop()
		}
	}()

	for {
		select {
		case <-d.ctx.Done():
			return
		case v, ok := <-watch:
			if !ok {
				watch = nil
				if d.ctx.Err() == nil {
					d.log.Warn("refset: watch closed; relying on polling")
				}
				continue
			}
			d.raiseHead(v)
			if v <= d.root.Load().version || batchC != nil {
				continue // already applied, or a batched poll is pending
			}
			// Adaptive batching: poll at once unless the last poll was less
			// than BatchWindow ago; then wait out the rest of the window, so
			// a burst of changes is applied in batches but a change after a
			// quiet period isn't delayed.
			since := time.Duration(clock.Now().MonoNanos - lastStart)
			if !refreshed || since >= d.cfg.batchWindow {
				refresh(false)
				continue
			}
			batch = clock.NewTimer(d.cfg.batchWindow - since)
			batchC = batch.C()
		case <-batchC:
			batchC = nil
			refresh(false)
		case <-tick.C():
			refresh(false)
			tick = clock.NewTimer(d.cfg.pollInterval)
		case <-d.kick:
			if failed && time.Duration(clock.Now().MonoNanos-lastStart) < kickBackoff {
				continue
			}
			stopBatch() // this poll covers any batched one
			refresh(false)
		case <-reloadC:
			refresh(true)
			reload = clock.NewTimer(d.cfg.reloadInterval)
			reloadC = reload.C()
		}
	}
}

// refresh brings the replica up to the source's head, by changes or, when full
// is set or the source requires it, by a full snapshot. Every refresh has a
// sequence number; waiters use it to know whether a poll started after they
// arrived. It reports whether the refresh succeeded.
func (d *Dataset[K, V]) refresh(full bool) bool {
	d.mu.Lock()
	d.pollsStarted++
	seq := d.pollsStarted
	d.mu.Unlock()

	start := d.cfg.clock.Now()
	d.polls.Add(1)
	cur := d.root.Load()
	next, head, reloaded, err := d.fetch(cur, full)
	if err != nil {
		if d.ctx.Err() == nil {
			d.pollErrors.Add(1)
			d.log.Warn("refset: refresh failed", "err", err, "applied", cur.version)
		}
		d.finish(seq, 0, false)
		return false
	}
	if next != cur {
		d.root.Store(next)
		d.lastApply.Store(d.cfg.clock.Now().WallNanos)
	}
	if reloaded {
		d.knownHead.Store(head) // a reload may legitimately lower it (history reset)
	} else {
		d.raiseHead(head)
	}
	d.lastConfirmed.Store(&start)
	d.fromFile.Store(false)
	d.finish(seq, head, true)
	return true
}

func (d *Dataset[K, V]) fetch(cur *root[K, V], full bool) (next *root[K, V], head uint64, reloaded bool, err error) {
	if !full {
		var deltas []Delta[K, V]
		head, deltas, err = d.src.Changes(d.ctx, cur.version)
		if err == nil && head < cur.version && d.fromFile.Load() {
			// The snapshot file is ahead of the source (its history was
			// reset or restored): reload rather than fail every poll.
			err = ErrSnapshotRequired
		}
		switch {
		case errors.Is(err, ErrSnapshotRequired):
			d.log.Info("refset: source requires a full snapshot", "applied", cur.version)
		case err != nil:
			return nil, 0, false, err
		default:
			if err := checkChanges(d.name, cur.version, head, deltas); err != nil {
				return nil, 0, false, err
			}
			if head == cur.version {
				return cur, head, false, nil
			}
			next, st := applyDeltas(cur, head, deltas)
			d.applies.Add(1)
			d.recordCopies(st)
			return next, head, false, nil
		}
	}
	next, st, err := d.load(d.ctx, cur)
	if err != nil {
		return nil, 0, false, err
	}
	d.reloads.Add(1)
	d.recordCopies(st)
	if full && d.cfg.reconcile {
		if n, ok := d.reconcile(cur, next); ok {
			d.reconciles.Add(1)
			d.divergences.Add(uint64(n))
			if n > 0 {
				d.log.Warn("refset: reconcile found entries that differ from the source without a reported change; the source's change feed missed them",
					"count", n, "applied", cur.version, "reloaded", next.version)
			}
		}
	}
	if next.version < cur.version {
		d.log.Warn("refset: source history went backwards; reloaded at an older version",
			"applied", cur.version, "reloaded", next.version)
	}
	return next, next.version, true, nil
}

func (d *Dataset[K, V]) recordCopies(st copyStats) {
	d.shardsCopied.Add(uint64(st.shardsCopied))
	d.entriesCopied.Add(uint64(st.entriesCopied))
	d.shardsKept.Add(uint64(st.shardsKept))
}

// finish records a completed poll and releases waiters it satisfies.
func (d *Dataset[K, V]) finish(seq, head uint64, ok bool) {
	applied := d.root.Load().version
	d.mu.Lock()
	defer d.mu.Unlock()
	d.waiters.releaseApplied(applied)
	if ok {
		d.waiters.releaseAhead(seq, head)
	}
}

func (d *Dataset[K, V]) raiseHead(v uint64) {
	for {
		cur := d.knownHead.Load()
		if v <= cur || d.knownHead.CompareAndSwap(cur, v) {
			return
		}
	}
}
