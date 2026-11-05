package refset

import "context"

// Source connects a [Dataset] to the system that holds the data: a database,
// files, an HTTP service. refset's guarantees hold only if the source meets the
// Source contract (docs/source-contract.md in the repository).
//
// The dataset calls Name, LoadSnapshot, Changes and Watch from a single
// goroutine. They are never called concurrently with each other for one
// dataset.
type Source[K comparable, V any] interface {
	// Name identifies the dataset. It must be stable for the life of the source
	// and match the name writers use in version headers.
	Name() string

	// LoadSnapshot streams the full dataset by calling add once per entry and
	// returns the version the entries reflect. That version must be the
	// source's latest committed version at some moment during the call. If it
	// returns an error, everything passed to add is discarded. add must not be
	// called after LoadSnapshot returns.
	LoadSnapshot(ctx context.Context, add func(key K, value V)) (version uint64, err error)

	// Changes returns every change after version after, up to and including
	// head, where head is the source's latest committed version at some moment
	// during the call. Deltas must be in ascending version order. A nil error
	// promises the result is complete; if the source is not sure, it must
	// return an error. If the changes are no longer available as per-key
	// changes, it returns ErrSnapshotRequired.
	Changes(ctx context.Context, after uint64) (head uint64, deltas []Delta[K, V], err error)

	// Watch returns a channel of head versions used only to wake the dataset
	// early. Notifications may be dropped, duplicated or delayed. The channel
	// must be closed when ctx is canceled.
	Watch(ctx context.Context) (<-chan uint64, error)
}

// KeyLoader is an optional interface for a [Source]. If the source implements
// it, a version wait that times out falls back to LoadKey instead of returning
// [ErrNotCaughtUp].
//
// LoadKey must return the source's current value for key, at a version at
// least as new as any version the source has reported. It is called from
// request goroutines and must be safe for concurrent use.
type KeyLoader[K comparable, V any] interface {
	LoadKey(ctx context.Context, key K) (value V, ok bool, err error)
}

// Delta is one or more committed changes, tagged with the version they bring
// the dataset to.
type Delta[K comparable, V any] struct {
	Version uint64
	Ops     []Op[K, V]
}

// Op is a single upsert or delete of one key.
type Op[K comparable, V any] struct {
	Key    K
	Value  V // ignored when Delete is true
	Delete bool
}
