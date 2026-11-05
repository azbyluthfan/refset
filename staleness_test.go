package refset_test

import (
	"errors"
	"testing"
	"time"

	"github.com/azbyluthfan/refset"
	"github.com/azbyluthfan/refset/refsettest"
)

// freshFixture opens a dataset on a fake clock with PollInterval 1s and
// MaxStaleness 2s. Polls fail until the test allows them.
type freshFixture struct {
	t     *testing.T
	src   *refsettest.MemSource[string, int]
	clock *refsettest.FakeClock
	d     *refset.Dataset[string, int]
}

func newFreshFixture(t *testing.T, opts refset.Options) *freshFixture {
	src := newSrc()
	src.Upsert("u1", 1)
	clock := refsettest.NewFakeClock(time.Date(2026, 11, 5, 9, 0, 0, 0, time.UTC))
	opts.Clock = clock
	if opts.PollInterval == 0 {
		opts.PollInterval = time.Second
	}
	if opts.MaxStaleness == 0 {
		opts.MaxStaleness = 2 * time.Second
	}
	f := &freshFixture{t: t, src: src, clock: clock, d: open(t, src, opts)}
	eventually(t, "loop timer armed", func() bool { return clock.Timers() >= 1 })
	src.SetChangesError(errors.New("partitioned"))
	return f
}

func (f *freshFixture) fresh() bool {
	f.t.Helper()
	_, _, err := f.d.GetFresh("u1")
	if err != nil && !errors.Is(err, refset.ErrTooStale) {
		f.t.Fatalf("unexpected error %v", err)
	}
	return err == nil
}

// tick advances the fake clock by one PollInterval (1s) and waits for the
// poll the ticker fires.
func (f *freshFixture) tick() {
	f.t.Helper()
	before := f.d.Stats().Polls
	f.clock.Advance(time.Second)
	eventually(f.t, "poll", func() bool { return f.d.Stats().Polls > before })
	eventually(f.t, "ticker re-armed", func() bool { return f.clock.Timers() >= 1 })
}

// G5: boundary just under and just over MaxStaleness.
func TestGetFreshBoundary(t *testing.T) {
	f := newFreshFixture(t, refset.Options{})
	f.clock.Advance(1999 * time.Millisecond)
	if !f.fresh() {
		t.Fatal("stale before the bound")
	}
	f.clock.Advance(2 * time.Millisecond)
	if f.fresh() {
		t.Fatal("fresh after the bound with every poll failing")
	}
}

// G5 / revocation: once polls succeed again, the replica is fresh again.
func TestGetFreshRecoversAfterSuccessfulPoll(t *testing.T) {
	f := newFreshFixture(t, refset.Options{})
	f.tick() // failed poll
	f.tick() // failed poll; staleness 2s
	f.clock.Advance(time.Millisecond)
	if f.fresh() {
		t.Fatal("should be stale")
	}
	f.src.SetChangesError(nil)
	f.tick()
	if !f.fresh() {
		t.Fatal("should be fresh after a successful poll")
	}
}

// §4.6: machine suspended; monotonic stops, wall jumps. Must fail closed.
func TestGetFreshSuspend(t *testing.T) {
	f := newFreshFixture(t, refset.Options{})
	f.clock.Suspend(time.Hour)
	if f.fresh() {
		t.Fatal("fresh after an hour of suspend")
	}
	f.src.SetChangesError(nil)
	f.tick()
	if !f.fresh() {
		t.Fatal("should recover after a successful poll")
	}
}

// §4.6: wall clock stepped backwards; the monotonic clock still enforces the bound.
func TestGetFreshWallSteppedBack(t *testing.T) {
	f := newFreshFixture(t, refset.Options{})
	f.clock.StepWall(-10 * time.Minute)
	f.clock.Advance(1500 * time.Millisecond)
	if !f.fresh() {
		t.Fatal("stale before the bound")
	}
	f.clock.Advance(600 * time.Millisecond)
	if f.fresh() {
		t.Fatal("wall stepped back hid staleness")
	}
}

// §4.6: wall clock stepped forwards; spuriously stale (safe) until the next poll.
func TestGetFreshWallSteppedForward(t *testing.T) {
	f := newFreshFixture(t, refset.Options{})
	f.src.SetChangesError(nil)
	f.clock.StepWall(5 * time.Second)
	if f.fresh() {
		t.Fatal("forward step should fail closed")
	}
	f.tick()
	if !f.fresh() {
		t.Fatal("should recover at the next successful poll")
	}
}

// G5: MaxLagVersions counts versions the source reported but the replica
// hasn't applied.
func TestGetFreshMaxLag(t *testing.T) {
	f := newFreshFixture(t, refset.Options{MaxLagVersions: 2})
	for i := range 3 {
		f.src.Upsert("u1", 10+i)
	}
	eventually(t, "head from notifications", func() bool { return f.d.Stats().HeadVersion == 4 })
	if st := f.d.Stats(); st.LagVersions != 3 {
		t.Fatalf("lag %d", st.LagVersions)
	}
	if f.fresh() {
		t.Fatal("3 versions behind with limit 2")
	}
}

func TestGetFreshUncheckedByDefault(t *testing.T) {
	src := newSrc()
	clock := refsettest.NewFakeClock(time.Now())
	d := open(t, src, refset.Options{Clock: clock})
	src.SetChangesError(errors.New("down"))
	clock.Suspend(24 * time.Hour)
	if _, _, err := d.GetFresh("x"); err != nil {
		t.Fatalf("no bounds configured: %v", err)
	}
	if s := d.Stats().Staleness; s < 24*time.Hour {
		t.Fatalf("Stats.Staleness = %v", s)
	}
}
