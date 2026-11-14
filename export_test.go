package refset

import "time"

// SetCloseWaitForTest shortens how long Close waits for a stuck loop.
func SetCloseWaitForTest(d time.Duration) (restore func()) {
	old := closeWait
	closeWait = d
	return func() { closeWait = old }
}
