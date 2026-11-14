// Package pgsource is the reference refset.Source for PostgreSQL (design §6.3).
// It lives in the examples module: copy and adapt it, or wait for it to be
// promoted to an official refsetpg module.
//
// How it works:
//
//   - Every replicated dataset has a row in refset_versions. Writers go through
//     Write, which locks the declared datasets' rows in sorted order, so
//     commits on one dataset happen in version order and writers never
//     deadlock. Write appends the changed keys to refset_changelog and sends
//     NOTIFY refset, '<dataset>:<version>' in the same transaction.
//   - The changelog holds keys only. Replicas re-read changed rows with the
//     dataset's own query, inside one REPEATABLE READ transaction with the
//     head, so values always come from the table at exactly the head version.
//   - NOTIFY is only a wake-up; replicas also poll, so a lost notification
//     only delays a change.
//
// Requirements:
//
//   - The pool must point at the primary. Every read checks
//     pg_is_in_recovery() and fails with ErrStandby on a standby, because a
//     lagging copy breaks read-your-writes (Source contract C3, C6). Full
//     snapshots alone may come from a replica: see Table.SnapshotPool.
//   - Every change to a replicated table must be logged, through Write or
//     through InstallTriggers. Unlogged changes leave replicas diverged until
//     their next reload (design §4.10).
//   - Datasets built from joins must capture changes to joined tables too:
//     the writer logs the affected keys, InstallKeyMappingTrigger maps
//     changed rows to keys with a query, or LogReloadAll /
//     InstallReloadTrigger reload the whole dataset.
package pgsource

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Channel is the NOTIFY channel; payloads are "<dataset>:<version>".
const Channel = "refset"

// ErrStandby is returned when the pool is connected to a standby. The source
// must read from the primary.
var ErrStandby = errors.New("pgsource: connected to a standby (pg_is_in_recovery); the source must read from the primary")

const schemaSQL = `
CREATE TABLE IF NOT EXISTS refset_versions (
	dataset           text PRIMARY KEY,
	version           bigint NOT NULL DEFAULT 0 CHECK (version >= 0),
	compacted_through bigint NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS refset_changelog (
	dataset      text        NOT NULL,
	version      bigint      NOT NULL,
	key          text        NOT NULL,          -- '' for a reload marker
	reload       boolean     NOT NULL DEFAULT false,
	committed_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS refset_changelog_dataset_version
	ON refset_changelog (dataset, version);
`

// Migrate creates refset's tables if they don't exist. It is safe to run
// concurrently from several processes.
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	return pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext('refset_migrate'))`); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, schemaSQL); err != nil {
			return fmt.Errorf("pgsource: migrate: %w", err)
		}
		return nil
	})
}

// querier is a pool, connection or transaction.
type querier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// headOf reads a dataset's version and compaction watermark and checks that
// the connection is on the primary, in one round trip.
func headOf(ctx context.Context, q querier, dataset string) (head, compacted uint64, err error) {
	var h, c int64
	var standby bool
	err = q.QueryRow(ctx, `
		SELECT coalesce(v.version, 0), coalesce(v.compacted_through, 0), pg_is_in_recovery()
		FROM (SELECT 1) one
		LEFT JOIN refset_versions v ON v.dataset = $1`, dataset).Scan(&h, &c, &standby)
	switch {
	case err != nil:
		return 0, 0, err
	case standby:
		return 0, 0, ErrStandby
	case h < 0 || c < 0:
		return 0, 0, fmt.Errorf("pgsource: dataset %q has a negative version", dataset)
	}
	return uint64(h), uint64(c), nil
}

// CurrentVersions reads the current version of each dataset, for writers
// using triggers instead of Write. With InstallTriggers (immediate mode) call
// it in the same transaction, after the writes. With InstallDeferredTriggers
// the version is assigned at commit, so call it on the pool after the commit;
// read inside the transaction it would return the version before the write.
func CurrentVersions(ctx context.Context, q querier, datasets ...string) (Versions, error) {
	out := make(Versions, len(datasets))
	for _, ds := range datasets {
		h, _, err := headOf(ctx, q, ds)
		if err != nil {
			return nil, err
		}
		out[ds] = h
	}
	return out, nil
}
