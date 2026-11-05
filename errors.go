package refset

import "errors"

var (
	// ErrNotCaughtUp is returned by a version-aware read when the replica did
	// not reach the requested version within the wait bound and the source
	// has no [KeyLoader] (or the fallback limit was reached).
	ErrNotCaughtUp = errors.New("refset: not caught up to requested version")

	// ErrVersionAhead is returned when a request asks for a version beyond the
	// source's head, confirmed by a poll that started after the request
	// arrived. It usually means a forged or corrupted version header.
	ErrVersionAhead = errors.New("refset: requested version is beyond the source head")

	// ErrTooStale is returned by GetFresh when the replica cannot confirm it
	// is current within the configured bounds.
	ErrTooStale = errors.New("refset: replica is too stale")

	// ErrSnapshotRequired is returned by Source.Changes when the requested
	// changes cannot be expressed as per-key changes. The dataset then
	// reloads a full snapshot.
	ErrSnapshotRequired = errors.New("refset: changes unavailable, full snapshot required")

	// ErrClosed is returned by reads that need to wait after the dataset was
	// closed.
	ErrClosed = errors.New("refset: dataset closed")
)
