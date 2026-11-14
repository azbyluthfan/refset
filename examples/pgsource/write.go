package pgsource

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Versions maps dataset names to the versions a write produced. It is a plain
// map, so it can be passed to refsethttp.SetVersions.
type Versions map[string]uint64

// Get returns the version for dataset, or 0 if the write didn't change it.
func (v Versions) Get(dataset string) uint64 { return v[dataset] }

// ErrUndeclared is returned when a write logs a dataset it didn't declare.
var ErrUndeclared = errors.New("pgsource: logged a dataset not declared in Write")

// Tx is the transaction passed to Write's callback. Use it like a pgx.Tx for
// your SQL, and log every changed key of a replicated dataset.
type Tx struct {
	pgx.Tx
	declared map[string]bool
	keys     map[string]map[string]struct{}
	reload   map[string]bool
	err      error
}

// LogUpsert records that keys of dataset were inserted or updated. Keys are
// in their text form, as Table.Codec encodes them (for string and integer
// keys, the usual decimal or string form).
func (t *Tx) LogUpsert(dataset string, keys ...string) error { return t.log(dataset, keys) }

// LogDelete records that keys of dataset were deleted. The changelog doesn't
// distinguish deletes from upserts: replicas re-read the key and find it gone.
func (t *Tx) LogDelete(dataset string, keys ...string) error { return t.log(dataset, keys) }

// LogReloadAll records that the whole dataset may have changed, for example
// after a change to a table it joins. Replicas reload a full snapshot.
func (t *Tx) LogReloadAll(dataset string) error {
	if err := t.check(dataset); err != nil {
		return err
	}
	t.reload[dataset] = true
	return nil
}

func (t *Tx) log(dataset string, keys []string) error {
	if err := t.check(dataset); err != nil {
		return err
	}
	set := t.keys[dataset]
	if set == nil {
		set = make(map[string]struct{})
		t.keys[dataset] = set
	}
	for _, k := range keys {
		set[k] = struct{}{}
	}
	return nil
}

func (t *Tx) check(dataset string) error {
	if !t.declared[dataset] {
		err := fmt.Errorf("%w: %q", ErrUndeclared, dataset)
		t.err = errors.Join(t.err, err)
		return err
	}
	return nil
}

// Write runs fn in a transaction that may change the declared datasets, and
// returns the new version of each dataset fn logged changes to.
//
// It first locks every declared dataset's version row, in sorted order, so
// concurrent writers serialize per dataset and can never deadlock. Datasets
// declared but not logged keep their version. Logging an undeclared dataset
// fails the write. If fn returns an error, the transaction rolls back and no
// version is consumed.
//
// Keep transactions short: declared datasets stay locked until commit.
func Write(ctx context.Context, pool *pgxpool.Pool, fn func(*Tx) error, datasets ...string) (Versions, error) {
	if len(datasets) == 0 {
		return nil, errors.New("pgsource: Write needs at least one dataset")
	}
	sorted := slices.Compact(slices.Sorted(slices.Values(datasets)))
	var out Versions
	err := pgx.BeginFunc(ctx, pool, func(ptx pgx.Tx) error {
		for _, ds := range sorted {
			if _, err := ptx.Exec(ctx,
				`INSERT INTO refset_versions (dataset) VALUES ($1) ON CONFLICT (dataset) DO NOTHING`, ds); err != nil {
				return err
			}
			if _, err := ptx.Exec(ctx, `SELECT 1 FROM refset_versions WHERE dataset = $1 FOR UPDATE`, ds); err != nil {
				return err
			}
		}
		tx := &Tx{
			Tx:       ptx,
			declared: make(map[string]bool, len(sorted)),
			keys:     make(map[string]map[string]struct{}),
			reload:   make(map[string]bool),
		}
		for _, ds := range sorted {
			tx.declared[ds] = true
		}
		if err := fn(tx); err != nil {
			return err
		}
		if tx.err != nil {
			return tx.err
		}

		out = make(Versions)
		for _, ds := range sorted {
			keys, reload := tx.keys[ds], tx.reload[ds]
			if len(keys) == 0 && !reload {
				continue
			}
			var v int64
			if err := ptx.QueryRow(ctx,
				`UPDATE refset_versions SET version = version + 1 WHERE dataset = $1 RETURNING version`, ds).Scan(&v); err != nil {
				return err
			}
			if len(keys) > 0 {
				if _, err := ptx.Exec(ctx, `
					INSERT INTO refset_changelog (dataset, version, key)
					SELECT $1, $2, k FROM unnest($3::text[]) AS k`,
					ds, v, slices.Sorted(maps.Keys(keys))); err != nil {
					return err
				}
			}
			if reload {
				if _, err := ptx.Exec(ctx,
					`INSERT INTO refset_changelog (dataset, version, key, reload) VALUES ($1, $2, '', true)`, ds, v); err != nil {
					return err
				}
			}
			if _, err := ptx.Exec(ctx, `SELECT pg_notify($1, $2)`, Channel, fmt.Sprintf("%s:%d", ds, v)); err != nil {
				return err
			}
			out[ds] = uint64(v) //nolint:gosec // RETURNING version; the column is CHECK (version >= 0)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// Compact deletes changelog rows older than retention, for every dataset.
// Replicas that fall behind the deleted history reload a full snapshot. Run
// it from a cron job or a single leader; replicas never delete.
func Compact(ctx context.Context, pool *pgxpool.Pool, retention time.Duration) error {
	_, err := pool.Exec(ctx, `
		WITH deleted AS (
			DELETE FROM refset_changelog
			WHERE committed_at < now() - make_interval(secs => $1)
			RETURNING dataset, version
		)
		UPDATE refset_versions v
		SET compacted_through = greatest(v.compacted_through, d.through)
		FROM (SELECT dataset, max(version) AS through FROM deleted GROUP BY dataset) d
		WHERE v.dataset = d.dataset`, retention.Seconds())
	return err
}

// CompactThrough deletes one dataset's changelog rows up to and including
// version through.
func CompactThrough(ctx context.Context, pool *pgxpool.Pool, dataset string, through uint64) error {
	return pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx,
			`DELETE FROM refset_changelog WHERE dataset = $1 AND version <= $2`, dataset, int64(through)); err != nil { //nolint:gosec // a version the caller read from the bigint column
			return err
		}
		_, err := tx.Exec(ctx,
			`UPDATE refset_versions SET compacted_through = greatest(compacted_through, $2) WHERE dataset = $1`,
			dataset, int64(through)) //nolint:gosec // a version the caller read from the bigint column
		return err
	})
}
