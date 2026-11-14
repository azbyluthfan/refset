package pgsource

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Deferred trigger mode assigns versions at commit instead of at a dataset's
// first statement. The statement triggers only record changed keys in
// refset_pending; a deferred constraint trigger, fired once per transaction at
// commit, locks the touched datasets' version rows in sorted order, bumps
// them, writes the changelog and notifies. Every transaction takes those locks
// at the same point and in the same order, so trigger-mode writers can't
// deadlock on them, whatever order their statements touch tables in.
//
// Versions still become visible in commit order: a dataset's version row stays
// locked from its bump until the transaction commits.
const deferredSchemaSQL = `
CREATE TABLE IF NOT EXISTS refset_pending (
	txid    xid8    NOT NULL DEFAULT pg_current_xact_id(),
	dataset text    NOT NULL,
	key     text    NOT NULL,          -- '' for a reload marker
	reload  boolean NOT NULL DEFAULT false
);
CREATE INDEX IF NOT EXISTS refset_pending_txid ON refset_pending (txid, dataset);
-- One row per writing transaction; its deferred trigger runs the commit step.
CREATE TABLE IF NOT EXISTS refset_pending_tx (
	txid xid8 PRIMARY KEY
);

CREATE OR REPLACE FUNCTION refset_capture_deferred() RETURNS trigger LANGUAGE plpgsql AS $fn$
DECLARE
	ds     text := TG_ARGV[0];
	keycol text := TG_ARGV[1];
	n      bigint := 0;
BEGIN
	IF TG_OP IN ('INSERT', 'UPDATE') THEN
		EXECUTE 'SELECT count(*) FROM refset_new_rows' INTO n;
	END IF;
	IF n = 0 AND TG_OP IN ('UPDATE', 'DELETE') THEN
		EXECUTE 'SELECT count(*) FROM refset_old_rows' INTO n;
	END IF;
	IF n = 0 THEN
		RETURN NULL; -- no rows touched: no version
	END IF;

	IF keycol IS NULL THEN
		INSERT INTO refset_pending (dataset, key, reload) VALUES (ds, '', true);
	ELSE
		IF TG_OP IN ('INSERT', 'UPDATE') THEN
			EXECUTE format('INSERT INTO refset_pending (dataset, key)
				SELECT DISTINCT $1, (%I)::text FROM refset_new_rows', keycol) USING ds;
		END IF;
		IF TG_OP IN ('UPDATE', 'DELETE') THEN
			EXECUTE format('INSERT INTO refset_pending (dataset, key)
				SELECT DISTINCT $1, (%I)::text FROM refset_old_rows', keycol) USING ds;
		END IF;
	END IF;
	INSERT INTO refset_pending_tx VALUES (pg_current_xact_id()) ON CONFLICT DO NOTHING;
	RETURN NULL;
END
$fn$;

CREATE OR REPLACE FUNCTION refset_commit_pending() RETURNS trigger LANGUAGE plpgsql AS $fn$
DECLARE
	tx xid8 := NEW.txid;
	ds text;
	v  bigint;
BEGIN
	-- Sorted: every transaction locks version rows in the same order.
	FOR ds IN SELECT DISTINCT dataset FROM refset_pending WHERE txid = tx ORDER BY dataset LOOP
		INSERT INTO refset_versions (dataset) VALUES (ds) ON CONFLICT (dataset) DO NOTHING;
		UPDATE refset_versions SET version = version + 1 WHERE dataset = ds RETURNING version INTO v;
		INSERT INTO refset_changelog (dataset, version, key, reload)
			SELECT DISTINCT ds, v, key, reload FROM refset_pending WHERE txid = tx AND dataset = ds;
		PERFORM pg_notify('refset', ds || ':' || v);
	END LOOP;
	DELETE FROM refset_pending WHERE txid = tx;
	DELETE FROM refset_pending_tx WHERE txid = tx;
	RETURN NULL;
END
$fn$;

DO $do$
BEGIN
	IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'refset_commit_pending'
	               AND tgrelid = 'refset_pending_tx'::regclass) THEN
		CREATE CONSTRAINT TRIGGER refset_commit_pending AFTER INSERT ON refset_pending_tx
			DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION refset_commit_pending();
	END IF;
END
$do$;
`

// InstallDeferredTriggers is InstallTriggers in deferred mode: versions are
// assigned at commit, in sorted dataset order, so trigger-mode writers can't
// deadlock on refset's version rows (they can still deadlock on their own
// tables, as any writers can).
//
// Because the version is assigned at commit, a writer reads it after the
// commit, with CurrentVersions on the pool. That returns its own version or a
// later one, which is what read-your-writes needs. Read inside the
// transaction, it would return the version before the write.
//
// Needs PostgreSQL 13 or newer. Do not mix with Write, or with InstallTriggers
// (immediate mode), on the same dataset. A transaction that runs SET
// CONSTRAINTS ALL IMMEDIATE moves the commit step to that statement.
func InstallDeferredTriggers(ctx context.Context, pool *pgxpool.Pool, dataset, table, keyColumn string) error {
	if keyColumn == "" {
		return errNoKeyColumn
	}
	return install(ctx, pool, dataset, table, keyColumn, true)
}

// InstallDeferredReloadTrigger is InstallReloadTrigger in deferred mode.
func InstallDeferredReloadTrigger(ctx context.Context, pool *pgxpool.Pool, dataset, table string) error {
	return install(ctx, pool, dataset, table, "", true)
}
