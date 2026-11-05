package refsettest

import (
	"sync"
	"time"

	"github.com/azbyluthfan/refset"
)

// FakeClock is a refset.Clock for tests. Its monotonic and wall readings can
// be moved independently to simulate suspend, NTP steps and similar events.
// Timers follow the monotonic reading, like Go's timers.
type FakeClock struct {
	mu     sync.Mutex
	mono   int64
	wall   int64
	timers map[*fakeTimer]struct{}
}

// NewFakeClock returns a clock whose wall reading starts at start.
func NewFakeClock(start time.Time) *FakeClock {
	return &FakeClock{wall: start.UnixNano(), timers: make(map[*fakeTimer]struct{})}
}

// Now implements refset.Clock.
func (c *FakeClock) Now() refset.Reading {
	c.mu.Lock()
	defer c.mu.Unlock()
	return refset.Reading{MonoNanos: c.mono, WallNanos: c.wall}
}

// NewTimer implements refset.Clock.
func (c *FakeClock) NewTimer(d time.Duration) refset.Timer {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := &fakeTimer{c: c, ch: make(chan time.Time, 1), deadline: c.mono + int64(d)}
	if d <= 0 {
		t.ch <- time.Unix(0, c.wall)
		return t
	}
	c.timers[t] = struct{}{}
	return t
}

// Advance moves both readings forward by d, as normal time passing does, and
// fires timers that come due.
func (c *FakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.mono += int64(d)
	c.wall += int64(d)
	for t := range c.timers {
		if t.deadline <= c.mono {
			delete(c.timers, t)
			select {
			case t.ch <- time.Unix(0, c.wall):
			default:
			}
		}
	}
}

// StepWall moves only the wall reading, by d (which may be negative), as an
// NTP step or manual clock change does. Timers are unaffected.
func (c *FakeClock) StepWall(d time.Duration) {
	c.mu.Lock()
	c.wall += int64(d)
	c.mu.Unlock()
}

// Suspend simulates the machine sleeping for d: the wall reading moves forward
// and the monotonic reading does not. Timers are unaffected.
func (c *FakeClock) Suspend(d time.Duration) { c.StepWall(d) }

// Timers returns the number of pending timers.
func (c *FakeClock) Timers() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.timers)
}

type fakeTimer struct {
	c        *FakeClock
	ch       chan time.Time
	deadline int64
}

func (t *fakeTimer) C() <-chan time.Time { return t.ch }

func (t *fakeTimer) Stop() bool {
	t.c.mu.Lock()
	defer t.c.mu.Unlock()
	_, pending := t.c.timers[t]
	delete(t.c.timers, t)
	return pending
}
