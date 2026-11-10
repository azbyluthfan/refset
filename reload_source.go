package refset

import (
	"context"
	"errors"
	"fmt"
)

// ReloadSource is the simplest way to feed your own data to a Dataset. You give
// it a function that loads every value (your existing code that reads rows,
// calls an API or parses a file into structs) and a function that reports the
// current version. Whenever the version moves, the dataset loads everything
// again and swaps it in.
//
//	src := &refset.ReloadSource[string, Currency]{
//		Dataset: "currencies",
//		Load:    loadCurrencies, // func(ctx) (version uint64, []Currency, error)
//		Version: currencyVersion, // func(ctx) (uint64, error), cheap
//		Key:     func(c Currency) string { return c.Code },
//	}
//	currencies, err := refset.Open(ctx, src, refset.Options{})
//
// Every change reloads the whole dataset, which suits most reference data
// (thousands of rows, changing a few times an hour). For large datasets that
// change often, implement Source.Changes with per-key changes instead.
//
// Your functions must keep two promises (the Source contract, C1 and C3):
//
//   - Load returns values and the version they reflect, read together: the
//     values are exactly the state at that version.
//   - Version returns the latest committed version, and it only goes up. Bump
//     it on every change, in the same transaction or under the same lock as
//     the change itself. Read it from the primary copy of your data, never
//     from a lagging replica or cache, or read-your-writes breaks.
type ReloadSource[K comparable, V any] struct {
	// Dataset is the dataset's name, shared with writers and version headers.
	Dataset string

	// Load returns every value and the version they reflect. If two values
	// have the same key, the later one wins.
	Load func(ctx context.Context) (version uint64, values []V, err error)

	// Key returns a value's key.
	Key func(V) K

	// Version returns the current version cheaply, for example one row from a
	// version table or an in-memory counter. Optional: when nil, every poll
	// calls Load to learn the version, so a change costs two loads.
	Version func(ctx context.Context) (uint64, error)

	// Notify, if set, returns a channel of new versions so the dataset reloads
	// promptly instead of on its next poll. Optional; it has the same rules as
	// Source.Watch (best effort, closed when ctx ends).
	Notify func(ctx context.Context) (<-chan uint64, error)

	// Equal, if set, reports whether two values are the same. A reload keeps
	// the shards whose values are all unchanged instead of rebuilding them, so
	// a reload that changes few values allocates little and needs little more
	// memory than the current version. Optional: when V is comparable with ==
	// (and holds no interfaces), == is used without it. Set it for values
	// that can't be compared with ==, such as structs with slices or maps. A
	// value Equal reports as unchanged keeps its previous copy.
	Equal func(a, b V) bool
}

func (s *ReloadSource[K, V]) equalFunc() func(a, b V) bool {
	if s == nil {
		return nil
	}
	return s.Equal
}

var errNoWatch = errors.New("refset: ReloadSource has no Notify function; relying on polling")

func (s *ReloadSource[K, V]) check() error {
	switch {
	case s == nil:
		return errors.New("refset: nil ReloadSource")
	case s.Dataset == "" || s.Load == nil || s.Key == nil:
		return errors.New("refset: ReloadSource needs Dataset, Load and Key")
	}
	return nil
}

// Name implements Source.
func (s *ReloadSource[K, V]) Name() string {
	if s == nil {
		return ""
	}
	return s.Dataset
}

// LoadSnapshot implements Source by calling Load.
func (s *ReloadSource[K, V]) LoadSnapshot(ctx context.Context, add func(K, V)) (uint64, error) {
	if err := s.check(); err != nil {
		return 0, err
	}
	version, values, err := s.Load(ctx)
	if err != nil {
		return 0, err
	}
	for _, v := range values {
		add(s.Key(v), v)
	}
	return version, nil
}

// Changes implements Source. It never returns per-key changes: if the version
// moved, it returns ErrSnapshotRequired and the dataset reloads.
func (s *ReloadSource[K, V]) Changes(ctx context.Context, after uint64) (uint64, []Delta[K, V], error) {
	if err := s.check(); err != nil {
		return 0, nil, err
	}
	var head uint64
	var err error
	if s.Version != nil {
		head, err = s.Version(ctx)
	} else {
		head, _, err = s.Load(ctx)
	}
	switch {
	case err != nil:
		return 0, nil, fmt.Errorf("refset: dataset %q: reading version: %w", s.Dataset, err)
	case head == after:
		return head, nil, nil
	default: // newer, or history went backwards: reload either way
		return 0, nil, ErrSnapshotRequired
	}
}

// Watch implements Source with the optional Notify function.
func (s *ReloadSource[K, V]) Watch(ctx context.Context) (<-chan uint64, error) {
	if s.Notify == nil {
		return nil, errNoWatch
	}
	return s.Notify(ctx)
}
