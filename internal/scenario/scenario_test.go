// Package scenario_test runs the design's headline multi-replica scenarios
// (docs/design.md §8.5) in-process: several Datasets, one per replica, share
// one MemSource, each through its own Replica view with its own faults.
package scenario_test

import (
	"context"
	"errors"
	"hash/fnv"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/azbyluthfan/refset"
	"github.com/azbyluthfan/refset/refsettest"
)

const (
	plansDataset = "plans"
	permsDataset = "perms"
)

type plan struct{ Tier string }

// cluster is one source with n replicas, each running its own Dataset.
type cluster[V any] struct {
	src  *refsettest.MemSource[string, V]
	reps []*refsettest.Replica[string, V]
	ds   []*refset.Dataset[string, V]
}

type clusterOpts struct {
	replicas  int
	keyLoader bool
	opts      refset.Options
}

func newCluster[V any](t *testing.T, name string, seed map[string]V, c clusterOpts) *cluster[V] {
	t.Helper()
	cl := &cluster[V]{src: refsettest.NewMemSource[string, V](name)}
	for k, v := range seed {
		cl.src.Upsert(k, v)
	}
	for range c.replicas {
		r := cl.src.NewReplica()
		var src refset.Source[string, V] = r
		if c.keyLoader {
			src = refsettest.WithKeyLoader(r)
		}
		d, err := refset.Open(context.Background(), src, c.opts)
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		t.Cleanup(func() { _ = d.Close() })
		cl.reps = append(cl.reps, r)
		cl.ds = append(cl.ds, d)
	}
	return cl
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

const (
	a, b, c = 0, 1, 2
	user    = "user-1"
)

func planCluster(t *testing.T, keyLoader bool, opts refset.Options) *cluster[plan] {
	return newCluster(t, plansDataset, map[string]plan{user: {"free"}},
		clusterOpts{replicas: 3, keyLoader: keyLoader, opts: opts})
}

// Without a version header, a replica whose changes are delayed answers with
// the old plan. This is the problem refset exists to solve.
func TestPlanUpgradeWithoutVersionHeaderIsStale(t *testing.T) {
	cl := planCluster(t, false, refset.Options{})
	cl.reps[b].SetDelay(200 * time.Millisecond)
	cl.src.Upsert(user, plan{"pro"})

	eventually(t, "replica A applies the upgrade", func() bool { p, _ := cl.ds[a].Get(user); return p.Tier == "pro" })
	if p, _ := cl.ds[b].Get(user); p.Tier != "free" {
		t.Fatalf("replica B answered %q; the delayed replica should still say free", p.Tier)
	}
}

// With the version, B waits for the delayed change and answers "pro" (G4).
func TestPlanUpgradeWaitsForVersion(t *testing.T) {
	cl := planCluster(t, false, refset.Options{WaitBound: 500 * time.Millisecond})
	cl.reps[b].SetDelay(20 * time.Millisecond)
	v := cl.src.Upsert(user, plan{"pro"})

	start := time.Now()
	p, ok, err := cl.ds[b].GetAtLeast(context.Background(), user, v)
	took := time.Since(start)
	if err != nil || !ok || p.Tier != "pro" {
		t.Fatalf("B = %q, %v, %v; want pro", p.Tier, ok, err)
	}
	if took < 15*time.Millisecond || took > 300*time.Millisecond {
		t.Fatalf("took %v; expected roughly the 20ms delay", took)
	}
	if st := cl.ds[b].Stats(); st.Fallbacks != 0 {
		t.Fatalf("used the fallback: %+v", st)
	}
}

// A request for a version B hasn't heard of (notification lost) makes B poll
// at once instead of waiting for its ticker.
func TestPlanUpgradeWithLostNotification(t *testing.T) {
	cl := planCluster(t, false, refset.Options{PollInterval: time.Hour, WaitBound: time.Second})
	cl.reps[b].SetDropNotifications(true)
	v := cl.src.Upsert(user, plan{"pro"})

	start := time.Now()
	p, _, err := cl.ds[b].GetAtLeast(context.Background(), user, v)
	if err != nil || p.Tier != "pro" {
		t.Fatalf("B = %q, %v", p.Tier, err)
	}
	if took := time.Since(start); took > 500*time.Millisecond {
		t.Fatalf("took %v; should not wait for the hourly ticker", took)
	}
}

// When B can't catch up within the wait bound, the fallback answers "pro"
// from the primary, within about the wait bound.
func TestPlanUpgradeFallsBackWhenTooSlow(t *testing.T) {
	cl := planCluster(t, true, refset.Options{WaitBound: 50 * time.Millisecond})
	cl.reps[b].SetDelay(200 * time.Millisecond)
	v := cl.src.Upsert(user, plan{"pro"})

	start := time.Now()
	p, ok, err := cl.ds[b].GetAtLeast(context.Background(), user, v)
	took := time.Since(start)
	if err != nil || !ok || p.Tier != "pro" {
		t.Fatalf("B = %q, %v, %v; want pro via fallback", p.Tier, ok, err)
	}
	if took < 45*time.Millisecond || took > 180*time.Millisecond {
		t.Fatalf("took %v; expected about the 50ms wait bound", took)
	}
	if st := cl.ds[b].Stats(); st.Fallbacks != 1 {
		t.Fatalf("fallbacks = %d", st.Fallbacks)
	}
}

// Without a fallback, B reports it isn't caught up; it never answers "free".
func TestPlanUpgradeNeverStaleWithoutFallback(t *testing.T) {
	cl := planCluster(t, false, refset.Options{WaitBound: 50 * time.Millisecond})
	cl.reps[b].SetDelay(200 * time.Millisecond)
	v := cl.src.Upsert(user, plan{"pro"})

	p, _, err := cl.ds[b].GetAtLeast(context.Background(), user, v)
	if !errors.Is(err, refset.ErrNotCaughtUp) {
		t.Fatalf("B = %q, %v; want ErrNotCaughtUp", p.Tier, err)
	}
	if p.Tier == "free" {
		t.Fatal("returned the stale plan alongside the error")
	}
}

// Revocation (G5): B is partitioned from its source and keeps serving the old
// permission until MaxStaleness, then denies. Healthy replicas deny as soon as
// they apply the change. Uses a fake clock so the 2s bound is exact.
func TestRevocationFailsClosedWhenPartitioned(t *testing.T) {
	clock := refsettest.NewFakeClock(time.Date(2026, 11, 5, 9, 0, 0, 0, time.UTC))
	cl := newCluster(t, permsDataset, map[string]bool{user: true}, clusterOpts{
		replicas: 3,
		opts:     refset.Options{PollInterval: time.Second, MaxStaleness: 2 * time.Second, Clock: clock},
	})
	allowed := func(d *refset.Dataset[string, bool]) (bool, error) {
		ok, _, err := d.GetFresh(user)
		if errors.Is(err, refset.ErrTooStale) {
			return false, err // fail closed
		}
		return ok, err
	}
	polls := func() uint64 { return cl.ds[a].Stats().Polls + cl.ds[c].Stats().Polls }
	advance := func(d time.Duration) {
		before := polls()
		clock.Advance(d)
		eventually(t, "healthy replicas poll", func() bool { return polls() >= before+2 })
	}

	// Fake timers only fire on Advance, so wait until each loop has armed the
	// timer it needs before moving the clock: 3 poll tickers, plus a batch
	// timer on A and C once they receive the revocation notification.
	timers := func(n int) {
		eventually(t, "timers armed", func() bool { return clock.Timers() == n })
	}
	timers(3)
	cl.reps[b].SetPartitioned(true)
	revoked := cl.src.Upsert(user, false) // revoke
	timers(5)
	advance(5 * time.Millisecond) // batch window: A and C apply the notified change
	for _, i := range []int{a, c} {
		eventually(t, "healthy replica applies the revocation", func() bool { return cl.ds[i].Version() == revoked })
	}

	for _, i := range []int{a, c} {
		if ok, err := allowed(cl.ds[i]); ok || err != nil {
			t.Fatalf("replica %d after applying the revocation: allowed=%v err=%v", i, ok, err)
		}
	}
	if ok, err := allowed(cl.ds[b]); !ok || err != nil {
		t.Fatalf("B inside the staleness bound should still serve its old data: allowed=%v err=%v", ok, err)
	}

	advance(1990 * time.Millisecond) // t = 1.995s
	if ok, err := allowed(cl.ds[b]); !ok || err != nil {
		t.Fatalf("B at 1.995s: allowed=%v err=%v", ok, err)
	}
	clock.Advance(10 * time.Millisecond) // t = 2.005s
	if ok, err := allowed(cl.ds[b]); ok || !errors.Is(err, refset.ErrTooStale) {
		t.Fatalf("B past 2s must fail closed: allowed=%v err=%v", ok, err)
	}
	if _, err := allowed(cl.ds[a]); err != nil {
		t.Fatalf("healthy replica A reported stale: %v", err)
	}
	if v, _ := cl.ds[b].Get(user); !v {
		t.Fatal("Get should keep serving B's last version during the partition")
	}

	// Heal: B catches up at its next poll and is fresh again.
	cl.reps[b].SetPartitioned(false)
	timers(3) // every poll ticker re-armed after the last round
	before := cl.ds[b].Stats().Polls
	clock.Advance(time.Second)
	eventually(t, "B polls after healing", func() bool { return cl.ds[b].Stats().Polls > before })
	eventually(t, "B applies the revocation", func() bool { v, _ := cl.ds[b].Get(user); return !v })
	if ok, err := allowed(cl.ds[b]); ok || err != nil {
		t.Fatalf("B after healing: allowed=%v err=%v", ok, err)
	}
}

func contentHash[V any](entries map[string]V, str func(V) string) uint64 {
	var sum uint64
	for k, v := range entries {
		h := fnv.New64a()
		h.Write([]byte(k))
		h.Write([]byte{0})
		h.Write([]byte(str(v)))
		sum += h.Sum64() // order-independent
	}
	return sum
}

// Convergence (G6): after 10,000 random writes under random faults (delays,
// lost notifications, partitions, compactions forcing reloads), once faults
// heal and writes stop, every replica reaches the source's version with
// identical contents.
func TestConvergenceAfterRandomWritesAndFaults(t *testing.T) {
	cl := newCluster[plan](t, plansDataset, nil, clusterOpts{
		replicas: 3,
		opts:     refset.Options{PollInterval: 20 * time.Millisecond, BatchWindow: time.Millisecond, Shards: 16},
	})
	rng := rand.New(rand.NewPCG(7, 7))
	tiers := []string{"free", "pro", "team", "enterprise"}
	key := func() string { return "user-" + string(rune('A'+rng.IntN(26))) + string(rune('a'+rng.IntN(20))) }

	for i := range 10_000 {
		ops := []refset.Op[string, plan]{{Key: key(), Value: plan{tiers[rng.IntN(4)]}, Delete: rng.IntN(5) == 0}}
		if rng.IntN(10) == 0 {
			ops = append(ops, refset.Op[string, plan]{Key: key(), Value: plan{tiers[rng.IntN(4)]}})
		}
		cl.src.Apply(ops...)
		if i%500 == 0 {
			r := cl.reps[rng.IntN(3)]
			r.SetDropNotifications(rng.IntN(2) == 0)
			r.SetPartitioned(rng.IntN(4) == 0)
			r.SetDelay(time.Duration(rng.IntN(5)) * time.Millisecond)
		}
		if i%2500 == 1249 {
			cl.src.Compact()
		}
		if i%100 == 0 {
			time.Sleep(time.Millisecond) // let replicas make progress mid-stream
		}
	}
	for _, r := range cl.reps {
		r.SetPartitioned(false)
		r.SetDropNotifications(false)
		r.SetDelay(0)
	}

	head := cl.src.Head()
	str := func(p plan) string { return p.Tier }
	want := contentHash(cl.src.State(), str)
	for i, d := range cl.ds {
		eventually(t, "replica reaches head", func() bool { return d.Version() == head })
		s := d.Snapshot()
		got := make(map[string]plan, s.Len())
		for k, v := range s.All() {
			got[k] = v
		}
		if h := contentHash(got, str); h != want {
			t.Fatalf("replica %d at version %d: content hash %x, source %x", i, d.Version(), h, want)
		}
	}
	if cl.ds[0].Stats().Reloads+cl.ds[1].Stats().Reloads+cl.ds[2].Stats().Reloads == 0 {
		t.Log("note: no replica needed a reload in this run")
	}
}

// A source that never answers makes Open fail at the ctx deadline, rather than
// start with empty data.
func TestSourceUnresponsiveAtOpen(t *testing.T) {
	src := refsettest.NewMemSource[string, plan](plansDataset)
	r := src.NewReplica()
	r.SetUnresponsive(true)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := refset.Open(ctx, r, refset.Options{})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Open = %v, want DeadlineExceeded", err)
	}
	if took := time.Since(start); took > time.Second {
		t.Fatalf("Open took %v", took)
	}

	r.SetUnresponsive(false)
	r.SetPartitioned(true)
	if _, err := refset.Open(context.Background(), r, refset.Options{}); !errors.Is(err, refsettest.ErrPartitioned) {
		t.Fatalf("Open on a partitioned source = %v", err)
	}
}

// While the source is down, reads keep working from memory; GetFresh fails
// closed after the bound and recovers when the source returns.
func TestSourceDownWhileRunning(t *testing.T) {
	cl := planCluster(t, false, refset.Options{PollInterval: 20 * time.Millisecond, MaxStaleness: 100 * time.Millisecond})
	d := cl.ds[a]
	cl.reps[a].SetPartitioned(true)

	eventually(t, "GetFresh fails closed", func() bool {
		_, _, err := d.GetFresh(user)
		return errors.Is(err, refset.ErrTooStale)
	})
	if p, ok := d.Get(user); !ok || p.Tier != "free" {
		t.Fatalf("Get during outage = %q, %v", p.Tier, ok)
	}
	if d.Stats().PollErrors == 0 {
		t.Fatal("expected poll errors")
	}
	cl.reps[a].SetPartitioned(false)
	eventually(t, "fresh again", func() bool { _, _, err := d.GetFresh(user); return err == nil })
}
