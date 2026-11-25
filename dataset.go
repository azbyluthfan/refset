package refset

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"math/rand/v2"
	"reflect"
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
	kick     chan struct{} // a read waits for a version the source has announced
	kickNew  chan struct{} // a read waits for a version beyond any announced
	flights  *flights[K, V]
	ctx      context.Context
	cancel   context.CancelFunc
	ready    chan struct{} // closed when the first version is installed
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
	fromFile      atomic.Bool  // started from a snapshot file and not yet caught up
	lastAhead     atomic.Int64 // monotonic time a poll last answered ErrVersionAhead; 0 never
	panics        atomic.Uint64
	afterFetch    atomic.Pointer[func()] // tests only: runs between a refresh's fetch and its result
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
//
// To start without waiting for the source, see OpenBackground.
func Open[K comparable, V any](ctx context.Context, src Source[K, V], opts Options) (*Dataset[K, V], error) {
	d, err := newDataset(src, opts)
	if err != nil {
		return nil, err
	}
	start := d.cfg.clock.Now()
	r, fromFile, err := d.openLoad(ctx)
	if err != nil {
		return nil, fmt.Errorf("refset: open dataset %q: %w", d.name, err)
	}
	d.install(r, fromFile, start)
	d.ctx, d.cancel = context.WithCancel(context.Background()) //nolint:gosec // cancel is kept in d.cancel and called by Close
	d.run(fromFile)
	return d, nil
}

// OpenBackground returns a dataset at once and loads it in the background,
// for applications that would rather start while the source is unreachable
// than wait for it. It fails only on invalid arguments.
//
// If src is a SnapshotFile with a usable file, the dataset starts from it,
// as Open does, and is loaded at once. Otherwise the dataset is empty until
// its first full load succeeds. Failed loads are retried, at growing
// intervals from 1 second to 30 seconds, until one succeeds or the dataset
// is closed; each failure counts in Stats().PollErrors and is logged.
//
// Until it is loaded:
//
//   - Ready's channel is open, and Stats().Loaded is false.
//   - Get returns the zero value and false for every key, and Snapshot is
//     empty. Check Ready or Loaded first, or use the reads below.
//   - GetForRequest and GetAtLeast don't wait: they call the source's
//     KeyLoader, or return ErrNotLoaded if it has none. A ReloadSource gets
//     one by embedding (see ReloadSource).
//   - GetFresh returns an error wrapping both ErrTooStale and ErrNotLoaded.
//   - SnapshotFile.Save refuses the empty snapshot, so it can't replace a
//     good file.
//
// To wait a bounded time for the first load, so a normal start serves
// local data from its first request, select on Ready with a timer:
//
//	d, err := refset.OpenBackground(src, opts)
//	if err != nil {
//		return err
//	}
//	select {
//	case <-d.Ready():
//	case <-time.After(5 * time.Second): // carry on; lookups use the KeyLoader
//	}
func OpenBackground[K comparable, V any](src Source[K, V], opts Options) (*Dataset[K, V], error) {
	d, err := newDataset(src, opts)
	if err != nil {
		return nil, err
	}
	start := d.cfg.clock.Now()
	d.ctx, d.cancel = context.WithCancel(context.Background()) //nolint:gosec // cancel is kept in d.cancel and called by Close
	if r := d.safeLoadFromFile(d.ctx); r != nil {
		d.install(r, true, start)
		d.run(true)
		return d, nil
	}
	d.root.Store(&root[K, V]{l: d.base, shards: emptyShards[K, V](1), pending: true})
	never := Reading{MonoNanos: start.MonoNanos - 1<<62, WallNanos: 0}
	d.lastConfirmed.Store(&never)
	go d.loadInBackground()
	return d, nil
}

func newDataset[K comparable, V any](src Source[K, V], opts Options) (*Dataset[K, V], error) {
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
		kickNew:  make(chan struct{}, 1),
		flights:  newFlights[K, V](cfg.maxFallbacks),
		ready:    make(chan struct{}),
		loopDone: make(chan struct{}),
	}
	d.log = cfg.log.With("dataset", d.name)
	if cfg.reconcile && d.eq == nil && holdsKind(reflect.TypeFor[V](), isIndirect) {
		d.log.Warn("refset: Reconcile compares values that hold pointers with reflect.DeepEqual, which also compares unexported internal state; " +
			"for values such as protobuf messages, set ReloadSource.Equal (proto.Equal) or Reconcile may report divergences that aren't there")
	}
	if kl, ok := caps.(KeyLoader[K, V]); ok {
		d.loader = kl
	}
	return d, nil
}

// install serves r, the dataset's first version, loaded at start.
func (d *Dataset[K, V]) install(r *root[K, V], fromFile bool, start Reading) {
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
	close(d.ready)
}

// run starts the background loop on a loaded dataset. catchUp polls the
// source at once, for data that came from a snapshot file.
func (d *Dataset[K, V]) run(catchUp bool) {
	watch, err := d.watch()
	if err != nil {
		d.log.Warn("refset: watch unavailable; relying on polling", "err", err)
		watch = nil
	}
	go d.loop(watch)
	if catchUp {
		d.kick <- struct{}{} // catch up with the source at once
	}
}

// Background load retry intervals (OpenBackground).
const (
	firstRetryMin = time.Second
	firstRetryMax = 30 * time.Second
)

// loadInBackground is OpenBackground's first load: it retries until a full
// load succeeds or the dataset is closed, then runs the loop.
func (d *Dataset[K, V]) loadInBackground() {
	delay := firstRetryMin
	for attempt := 1; ; attempt++ {
		d.mu.Lock()
		d.pollsStarted++
		seq := d.pollsStarted
		d.mu.Unlock()
		start := d.cfg.clock.Now()
		d.polls.Add(1)
		r, err := d.firstLoad(d.ctx)
		if err == nil {
			d.install(r, false, start)
			d.finish(seq, r.version, true)
			d.log.Info("refset: loaded in the background", "version", r.version, "entries", r.n, "attempts", attempt)
			d.run(false)
			return
		}
		if d.ctx.Err() != nil {
			close(d.loopDone)
			return
		}
		d.pollErrors.Add(1)
		var p *PanicError
		if errors.As(err, &p) {
			d.panics.Add(1)
		}
		wait := jitter(delay)
		d.log.Warn("refset: background load failed; serving nothing locally until it succeeds",
			"attempt", attempt, "retry_in", wait, "err", err)
		t := d.cfg.clock.NewTimer(wait)
		select {
		case <-d.ctx.Done():
			t.Stop()
			close(d.loopDone)
			return
		case <-t.C():
		}
		delay = min(2*delay, firstRetryMax)
	}
}

// jitter spreads d by ±20%, so replicas that failed together don't all
// retry against a recovering source at the same moment.
func jitter(d time.Duration) time.Duration {
	return d + time.Duration((rand.Float64()*0.4-0.2)*float64(d)) //nolint:gosec // timing jitter, not security
}

// openLoad is Open's first load, from the snapshot file or the source, with a
// panic in either returned as a *PanicError.
func (d *Dataset[K, V]) openLoad(ctx context.Context) (r *root[K, V], fromFile bool, err error) {
	defer func() {
		if p := recovered(recover()); p != nil {
			d.panics.Add(1)
			r, fromFile, err = nil, false, p
		}
	}()
	if r, fromFile = d.loadFromFile(ctx); r != nil {
		return r, fromFile, nil
	}
	r, _, err = d.load(ctx, nil)
	return r, false, err
}

// firstLoad is a full load from the source with a panic returned as a
// *PanicError.
func (d *Dataset[K, V]) firstLoad(ctx context.Context) (r *root[K, V], err error) {
	defer func() {
		if p := recovered(recover()); p != nil {
			d.log.Error("refset: panic in source code during the background load",
				"panic", p.Value, "stack", string(p.Stack))
			r, err = nil, p // counted by loadInBackground, after PollErrors
		}
	}()
	r, _, err = d.load(ctx, nil)
	return r, err
}

// safeLoadFromFile is loadFromFile with a panic treated as an unusable file.
func (d *Dataset[K, V]) safeLoadFromFile(ctx context.Context) (r *root[K, V]) {
	defer func() {
		if p := recovered(recover()); p != nil {
			d.panics.Add(1)
			d.log.Warn("refset: snapshot file not used; loading from the source", "err", p)
			r = nil
		}
	}()
	r, _ = d.loadFromFile(ctx)
	return r
}

// watch calls Source.Watch, with a panic returned as a *PanicError.
func (d *Dataset[K, V]) watch() (ch <-chan uint64, err error) {
	defer func() {
		if p := recovered(recover()); p != nil {
			d.panics.Add(1)
			ch, err = nil, p
		}
	}()
	return d.src.Watch(d.ctx)
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
// GetForRequest when the version is already applied must not write shared memory or
// allocate. A write here would invalidate the cache line on every core that is
// reading; read_path_test.go checks the allocation half of this rule.

// Get returns the value for key from the current version. It never waits and
// never fails; use GetAtLeast, GetForRequest or GetFresh when freshness matters.
// Before an OpenBackground dataset is loaded, it finds nothing.
func (d *Dataset[K, V]) Get(key K) (V, bool) {
	return d.root.Load().get(key)
}

// Ready returns a channel that is closed once the dataset has data to serve:
// before Open returns, or when OpenBackground's first load (or snapshot file)
// is in. It stays open if the dataset is closed before that.
func (d *Dataset[K, V]) Ready() <-chan struct{} { return d.ready }

// notLoaded answers a version-aware read before OpenBackground's first load:
// from the KeyLoader if there is one, without waiting for the load.
func (d *Dataset[K, V]) notLoaded(ctx context.Context, key K) (V, bool, error) {
	if d.loader == nil {
		var zero V
		return zero, false, ErrNotLoaded
	}
	return d.fallback(ctx, key)
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
//
// Before an OpenBackground dataset is loaded, it doesn't wait: it calls the
// KeyLoader, or returns ErrNotLoaded if there is none.
func (d *Dataset[K, V]) GetAtLeast(ctx context.Context, key K, minVersion uint64) (V, bool, error) {
	r := d.root.Load()
	if r.pending {
		return d.notLoaded(ctx, key)
	}
	if r.version >= minVersion {
		v, ok := r.get(key)
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

// GetForRequest reads key at the version the request needs: the minimum
// version for this dataset carried in ctx (see ContextWithMinVersion, usually
// set by refsethttp's middleware from the client's version header). With a
// minimum it behaves like GetAtLeast, waiting for that version; without one,
// like Get. Before an OpenBackground dataset is loaded, it calls the
// KeyLoader, or returns ErrNotLoaded if there is none.
func (d *Dataset[K, V]) GetForRequest(ctx context.Context, key K) (V, bool, error) {
	if minVersion, ok := MinVersion(ctx, d.name); ok {
		return d.GetAtLeast(ctx, key, minVersion)
	}
	r := d.root.Load()
	if r.pending {
		return d.notLoaded(ctx, key)
	}
	v, ok := r.get(key)
	return v, ok, nil
}

// GetFresh returns the value for key only if the replica can confirm it is
// current within Options.MaxStaleness and Options.MaxLagVersions; otherwise it
// returns an error wrapping ErrTooStale. Use it for fail-closed decisions such
// as permission checks.
//
// Before an OpenBackground dataset is loaded, the error wraps both ErrTooStale
// and ErrNotLoaded.
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
//
// Close waits for the loop to finish its current call to the source. If the
// source ignores context cancellation and is still busy after 10 seconds,
// Close returns an error saying so and leaves that call running.
func (d *Dataset[K, V]) Close() error {
	var err error
	d.close.Do(func() {
		d.cancel()
		select {
		case <-d.loopDone:
		case <-time.After(closeWait):
			err = fmt.Errorf("refset: close dataset %q: the source did not return within %v of cancellation; it should honor its context", d.name, closeWait)
		}
		d.mu.Lock()
		d.closed = true
		d.waiters.releaseAll(ErrClosed)
		d.mu.Unlock()
	})
	return err
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
	unknown := minVersion > d.knownHead.Load()
	if unknown {
		// Unknown version: ErrVersionAhead needs a poll that starts after
		// this request (design §4.5).
		w.needSeq = d.pollsStarted + 1
	}
	heap.Push(&d.waiters, w)
	d.mu.Unlock()

	// Ask for a poll now, even if a notification already announced the
	// version, rather than wait for a batched poll. A version beyond any
	// announced, which is what a forged version always is, goes through the
	// rate-limited kick instead (see loop).
	kick := d.kick
	if unknown {
		kick = d.kickNew
	}
	select {
	case kick <- struct{}{}:
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
		case <-d.kickNew:
			// A read for a version beyond any the source has announced polls
			// at once, unless a recent poll found such a version doesn't exist
			// (ErrVersionAhead): someone is sending forged versions, so these
			// polls run at most once per BatchWindow until that stops.
			// Coalescing already bounds a flood to one poll at a time; this
			// also bounds the rate, so forged versions can't make every replica
			// poll its source as fast as it answers. Legitimate versions are
			// found, so without forgeries nothing is delayed.
			if failed && time.Duration(clock.Now().MonoNanos-lastStart) < kickBackoff {
				continue
			}
			now := clock.Now().MonoNanos
			since := time.Duration(now - lastStart)
			ahead := d.lastAhead.Load()
			underAttack := ahead != 0 && time.Duration(now-ahead) < forgeryCooldown
			if !underAttack || !refreshed || since >= d.cfg.batchWindow {
				stopBatch()
				refresh(false)
			} else if batchC == nil {
				batch = clock.NewTimer(d.cfg.batchWindow - since)
				batchC = batch.C()
			}
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
	next, head, c, err := d.safeFetch(cur, full)
	if f := d.afterFetch.Load(); f != nil {
		(*f)()
	}
	// Counters are recorded after what they describe is visible, so code
	// that sees one move (a test, or an application watching Stats) also
	// sees its result: the new version, or the failure.
	if err != nil {
		if d.ctx.Err() == nil {
			d.pollErrors.Add(1)
			d.log.Warn("refset: refresh failed", "err", err, "applied", cur.version)
		}
		var p *PanicError
		if errors.As(err, &p) {
			d.panics.Add(1) // after PollErrors: a panic is also a failed poll
		}
		d.finish(seq, 0, false)
		return false
	}
	if next != cur {
		d.root.Store(next)
		d.lastApply.Store(d.cfg.clock.Now().WallNanos)
	}
	if c.reloaded {
		d.knownHead.Store(head) // a reload may legitimately lower it (history reset)
	} else {
		d.raiseHead(head)
	}
	d.lastConfirmed.Store(&start)
	d.fromFile.Store(false)
	d.record(c)
	d.finish(seq, head, true)
	return true
}

// safeFetch is fetch with a panic in source code (Changes, LoadSnapshot,
// Equal, a ReloadSource's functions) recovered and returned as a
// *PanicError: the poll fails and the dataset keeps its last version.
func (d *Dataset[K, V]) safeFetch(cur *root[K, V], full bool) (next *root[K, V], head uint64, c pollCounts, err error) {
	defer func() {
		if p := recovered(recover()); p != nil {
			d.log.Error("refset: panic in source code; the poll failed and the dataset keeps its last version",
				"panic", p.Value, "stack", string(p.Stack))
			next, head, c, err = nil, 0, pollCounts{}, p // counted by refresh
		}
	}()
	return d.fetch(cur, full)
}

// pollCounts are the Stats a successful refresh adds. fetch returns them and
// refresh records them once the new version is installed.
type pollCounts struct {
	applied    bool // a batch of changes was applied
	reloaded   bool // a full snapshot was loaded
	reconciled bool
	divergent  int
	copies     copyStats
}

func (d *Dataset[K, V]) record(c pollCounts) {
	if c.applied {
		d.applies.Add(1)
	}
	if c.reloaded {
		d.reloads.Add(1)
	}
	d.recordCopies(c.copies)
	if c.reconciled {
		d.divergences.Add(uint64(c.divergent)) //nolint:gosec // a count, never negative
		d.reconciles.Add(1)                    // after Divergences, which waiting on it should see
	}
}

func (d *Dataset[K, V]) fetch(cur *root[K, V], full bool) (next *root[K, V], head uint64, c pollCounts, err error) {
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
			return nil, 0, c, err
		default:
			if err := checkChanges(d.name, cur.version, head, deltas); err != nil {
				return nil, 0, c, err
			}
			if head == cur.version {
				return cur, head, c, nil
			}
			next, st := applyDeltas(cur, head, deltas)
			return next, head, pollCounts{applied: true, copies: st}, nil
		}
	}
	next, st, err := d.load(d.ctx, cur)
	if err != nil {
		return nil, 0, c, err
	}
	c = pollCounts{reloaded: true, copies: st}
	if full && d.cfg.reconcile {
		if n, ok := d.reconcile(cur, next); ok {
			c.reconciled, c.divergent = true, n
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
	return next, next.version, c, nil
}

func (d *Dataset[K, V]) recordCopies(st copyStats) {
	d.shardsCopied.Add(uint64(st.shardsCopied))   //nolint:gosec // a count, never negative
	d.entriesCopied.Add(uint64(st.entriesCopied)) //nolint:gosec // a count, never negative
	d.shardsKept.Add(uint64(st.shardsKept))       //nolint:gosec // a count, never negative
}

// finish records a completed poll and releases waiters it satisfies.
func (d *Dataset[K, V]) finish(seq, head uint64, ok bool) {
	applied := d.root.Load().version
	d.mu.Lock()
	defer d.mu.Unlock()
	d.waiters.releaseApplied(applied)
	if ok && d.waiters.releaseAhead(seq, head) > 0 {
		d.lastAhead.Store(d.cfg.clock.Now().MonoNanos)
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
