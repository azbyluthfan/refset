package refset

import "time"

// Clock supplies time to a [Dataset]. The default reads the system clocks;
// tests use refsettest.FakeClock to move them independently.
type Clock interface {
	// Now returns a monotonic and a wall-clock reading taken together.
	Now() Reading
	// NewTimer returns a timer that fires once after d on the monotonic clock.
	NewTimer(d time.Duration) Timer
}

// Reading is a pair of raw clock readings in nanoseconds. Only differences
// between readings are meaningful.
//
// The monotonic reading never goes backwards and is unaffected by wall-clock
// changes, but on most systems it stops while the machine is suspended. The
// wall reading is Unix time and can jump in either direction. Staleness uses
// the larger of the two elapsed times, so every clock error fails closed.
type Reading struct {
	MonoNanos int64
	WallNanos int64
}

// Timer is a one-shot timer created by a [Clock].
type Timer interface {
	C() <-chan time.Time
	Stop() bool
}

// processEpoch carries a monotonic reading; durations since it are monotonic.
var processEpoch = time.Now()

type systemClock struct{}

func (systemClock) Now() Reading {
	t := time.Now()
	return Reading{MonoNanos: int64(t.Sub(processEpoch)), WallNanos: t.UnixNano()}
}

func (systemClock) NewTimer(d time.Duration) Timer { return sysTimer{time.NewTimer(d)} }

type sysTimer struct{ t *time.Timer }

func (s sysTimer) C() <-chan time.Time { return s.t.C }
func (s sysTimer) Stop() bool          { return s.t.Stop() }

// elapsed returns the time from a to b as the larger of the monotonic and
// wall-clock differences, with a negative wall difference counted as zero.
func elapsed(a, b Reading) time.Duration {
	mono := b.MonoNanos - a.MonoNanos
	wall := b.WallNanos - a.WallNanos
	return time.Duration(max(mono, wall, 0))
}
