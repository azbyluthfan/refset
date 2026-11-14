package refset

import (
	"fmt"
	"runtime/debug"
	"time"
)

// PanicError is returned in place of a panic in code refset calls on the
// application's behalf: a Source, a KeyLoader, or a ReloadSource's functions.
// refset recovers such panics so one bad call can't crash the process or
// leave other readers with a wrong answer; the dataset keeps its last version
// and, while its source keeps failing, GetFresh fails closed as for any other
// source error.
type PanicError struct {
	Value any    // the value passed to panic
	Stack []byte // the panicking goroutine's stack
}

func (e *PanicError) Error() string { return fmt.Sprintf("refset: panic in source code: %v", e.Value) }

// recovered converts a recovered panic value into a *PanicError, or nil.
func recovered(r any) *PanicError {
	if r == nil {
		return nil
	}
	return &PanicError{Value: r, Stack: debug.Stack()}
}

// closeWait bounds how long Close waits for the background loop, which is
// blocked only if the source ignores context cancellation.
var closeWait = 10 * time.Second
