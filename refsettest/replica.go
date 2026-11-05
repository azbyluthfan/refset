package refsettest

import (
	"context"
	"maps"
	"sync"
	"sync/atomic"
	"time"

	"github.com/azbyluthfan/refset"
)

// Replica is one replica's view of a MemSource, with its own faults: a slow
// connection, dropped notifications, a partition, injected errors or an
// unresponsive source. Give each Dataset in a multi-replica test its own
// Replica. With no faults set, a Replica behaves exactly like its MemSource.
//
// A delay models a slow path to the source (network or query latency): every
// Changes and LoadSnapshot call reads the source's latest committed state and
// then takes the delay to return, and notifications arrive the delay late.
// The replica never sees an older head than the source has committed, which
// the Source contract requires (C3); a source that reads from a lagging copy
// would break read-your-writes.
type Replica[K comparable, V any] struct {
	src *MemSource[K, V]

	mu           sync.Mutex
	delay        time.Duration
	drop         bool
	partitioned  bool
	unresponsive bool
	changesErr   error
	watchers     map[chan uint64]struct{}

	changesCalls atomic.Int64
	loadCalls    atomic.Int64
}

// NewReplica returns a new view of s with no faults.
func (s *MemSource[K, V]) NewReplica() *Replica[K, V] {
	r := &Replica[K, V]{src: s, watchers: make(map[chan uint64]struct{})}
	s.mu.Lock()
	s.replicas = append(s.replicas, r)
	s.mu.Unlock()
	return r
}

// Name implements refset.Source.
func (r *Replica[K, V]) Name() string { return r.src.name }

// fault returns the error to fail a call with, blocking while unresponsive.
func (r *Replica[K, V]) fault(ctx context.Context, withChangesErr bool) error {
	r.mu.Lock()
	partitioned, unresponsive, err := r.partitioned, r.unresponsive, r.changesErr
	r.mu.Unlock()
	switch {
	case unresponsive:
		<-ctx.Done()
		return ctx.Err()
	case partitioned:
		return ErrPartitioned
	case withChangesErr:
		return err
	default:
		return nil
	}
}

// slow waits for the replica's delay, or until ctx ends.
func (r *Replica[K, V]) slow(ctx context.Context) error {
	r.mu.Lock()
	d := r.delay
	r.mu.Unlock()
	if d <= 0 {
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// LoadSnapshot implements refset.Source.
func (r *Replica[K, V]) LoadSnapshot(ctx context.Context, add func(K, V)) (uint64, error) {
	r.loadCalls.Add(1)
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if err := r.fault(ctx, false); err != nil {
		return 0, err
	}
	s := r.src
	s.mu.Lock()
	if s.loadErr != nil {
		err := s.loadErr
		s.mu.Unlock()
		return 0, err
	}
	v, state := s.head, maps.Clone(s.state)
	s.mu.Unlock()
	if err := r.slow(ctx); err != nil {
		return 0, err
	}
	for k, val := range state {
		add(k, val)
	}
	return v, nil
}

// Changes implements refset.Source.
func (r *Replica[K, V]) Changes(ctx context.Context, after uint64) (uint64, []refset.Delta[K, V], error) {
	r.changesCalls.Add(1)
	if err := r.fault(ctx, true); err != nil {
		return 0, nil, err
	}
	s := r.src
	s.mu.Lock()
	if after < s.compacted || after > s.head {
		s.mu.Unlock()
		return 0, nil, refset.ErrSnapshotRequired
	}
	head := s.head
	out := s.changesLocked(after, head)
	s.mu.Unlock()
	if err := r.slow(ctx); err != nil {
		return 0, nil, err
	}
	return head, out, nil
}

// Watch implements refset.Source.
func (r *Replica[K, V]) Watch(ctx context.Context) (<-chan uint64, error) {
	ch := make(chan uint64, 64)
	r.mu.Lock()
	r.watchers[ch] = struct{}{}
	r.mu.Unlock()
	go func() {
		<-ctx.Done()
		r.mu.Lock()
		delete(r.watchers, ch)
		close(ch)
		r.mu.Unlock()
	}()
	return ch, nil
}

// notify delivers a new head to this replica's watchers, after its delay.
func (r *Replica[K, V]) notify(v uint64) {
	r.mu.Lock()
	delay := r.delay
	r.mu.Unlock()
	send := func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		if r.drop || r.partitioned || r.unresponsive {
			return
		}
		for ch := range r.watchers {
			select {
			case ch <- v:
			default:
			}
		}
	}
	if delay > 0 {
		time.AfterFunc(delay, send)
		return
	}
	send()
}

// loadCurrent reads the primary's latest value directly, without the
// replica's delay: the fallback path is a direct query to the primary.
func (r *Replica[K, V]) loadCurrent(ctx context.Context, key K) (V, bool, error) {
	if err := r.fault(ctx, false); err != nil {
		var zero V
		return zero, false, err
	}
	return r.src.loadCurrent(ctx, key)
}

// SetDelay makes this replica's Changes and LoadSnapshot calls take d to
// return, and its notifications arrive d late.
func (r *Replica[K, V]) SetDelay(d time.Duration) {
	r.mu.Lock()
	r.delay = d
	r.mu.Unlock()
}

// SetDropNotifications drops every notification to this replica while on.
func (r *Replica[K, V]) SetDropNotifications(drop bool) {
	r.mu.Lock()
	r.drop = drop
	r.mu.Unlock()
}

// SetPartitioned cuts this replica off: every call, including LoadKey, fails
// with ErrPartitioned and notifications are dropped.
func (r *Replica[K, V]) SetPartitioned(p bool) {
	r.mu.Lock()
	r.partitioned = p
	r.mu.Unlock()
}

// SetUnresponsive makes every call block until its context ends, like a
// database that accepts connections but never answers.
func (r *Replica[K, V]) SetUnresponsive(u bool) {
	r.mu.Lock()
	r.unresponsive = u
	r.mu.Unlock()
}

// SetChangesError makes Changes fail with err until it is set back to nil.
func (r *Replica[K, V]) SetChangesError(err error) {
	r.mu.Lock()
	r.changesErr = err
	r.mu.Unlock()
}

// ChangesCalls returns how many times Changes was called on this replica.
func (r *Replica[K, V]) ChangesCalls() int64 { return r.changesCalls.Load() }

// LoadCalls returns how many times LoadSnapshot was called on this replica.
func (r *Replica[K, V]) LoadCalls() int64 { return r.loadCalls.Load() }
