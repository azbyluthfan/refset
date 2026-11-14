package pgsource

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// TriggerMode selects when a trigger assigns versions: Immediate locks a
// dataset's version row at the first statement (as InstallTriggers),
// Deferred at commit in sorted order (as InstallDeferredTriggers).
type TriggerMode int

const (
	Immediate TriggerMode = iota
	Deferred
)

// mapKeysFunctionSQL runs a key-mapping query over the rows a statement
// changed and logs the dataset keys it returns, immediately or for the
// commit step. A statement whose rows map to no keys consumes no version.
const mapKeysFunctionSQL = `
CREATE OR REPLACE FUNCTION refset_map_keys() RETURNS trigger LANGUAGE plpgsql AS $fn$
DECLARE
	ds       text    := TG_ARGV[0];
	keys_sql text    := TG_ARGV[1];
	deferred boolean := TG_ARGV[2] = 'deferred';
	changed  text;
	ks       text[];
	v        bigint;
BEGIN
	changed := CASE TG_OP
		WHEN 'INSERT' THEN 'SELECT * FROM refset_new_rows'
		WHEN 'DELETE' THEN 'SELECT * FROM refset_old_rows'
		ELSE 'SELECT * FROM refset_new_rows UNION ALL SELECT * FROM refset_old_rows'
	END;
	EXECUTE format('WITH refset_changed AS (%s) SELECT array_agg(DISTINCT k::text) FROM (%s) AS refset_keys(k)',
		changed, keys_sql) INTO ks;
	IF ks IS NULL THEN
		RETURN NULL;
	END IF;

	IF deferred THEN
		INSERT INTO refset_pending (dataset, key) SELECT ds, unnest(ks);
		INSERT INTO refset_pending_tx VALUES (pg_current_xact_id()) ON CONFLICT DO NOTHING;
	ELSE
		INSERT INTO refset_versions (dataset) VALUES (ds) ON CONFLICT (dataset) DO NOTHING;
		UPDATE refset_versions SET version = version + 1 WHERE dataset = ds RETURNING version INTO v;
		INSERT INTO refset_changelog (dataset, version, key) SELECT ds, v, unnest(ks);
		PERFORM pg_notify('refset', ds || ':' || v);
	END IF;
	RETURN NULL;
END
$fn$;
`

// InstallKeyMappingTrigger logs the dataset keys affected by changes to
// table, a table the dataset joins, so replicas re-read just those keys
// instead of reloading the whole dataset (as InstallReloadTrigger does).
//
// keysSQL is a query returning one column, the dataset's keys (cast to
// text, as KeyCodec encodes them), and may read the changed rows of table as
// refset_changed: the new rows of an INSERT, the old ones of a DELETE, and
// both of an UPDATE. For a dataset of user plans that joins tiers:
//
//	pgsource.InstallKeyMappingTrigger(ctx, pool, pgsource.Deferred, "plans", "tiers",
//		`SELECT p.user_id FROM user_plans p JOIN refset_changed c ON c.id = p.tier_id`)
//
// The query runs inside the writing transaction, so it sees that
// transaction's own changes; keep it indexed and cheap. It is checked when
// the trigger is installed. Use the same mode as the dataset's other
// triggers. keysSQL comes from the application, not from end users: it is
// executed as written.
func InstallKeyMappingTrigger(ctx context.Context, pool *pgxpool.Pool, mode TriggerMode, dataset, table, keysSQL string) error {
	if strings.TrimSpace(keysSQL) == "" {
		return errors.New("pgsource: InstallKeyMappingTrigger needs a key query")
	}
	modeArg := "immediate"
	if mode == Deferred {
		modeArg = "deferred"
	}
	tbl := pgx.Identifier(strings.Split(table, ".")).Sanitize()
	return installTriggers(ctx, pool, triggerSpec{
		dataset:  dataset,
		table:    table,
		function: "refset_map_keys",
		args:     quoteLiteral(dataset) + ", " + quoteLiteral(keysSQL) + ", " + quoteLiteral(modeArg),
		deferred: mode == Deferred,
		check: func(ctx context.Context, tx pgx.Tx) error {
			// Plan the query against an empty refset_changed of the right
			// shape: catches syntax errors, unknown columns and a wrong
			// column count now rather than at the first write.
			q := fmt.Sprintf(`EXPLAIN WITH refset_changed AS (SELECT * FROM %s WHERE false)
				SELECT k::text FROM (%s) AS refset_keys(k)`, tbl, keysSQL)
			if _, err := tx.Exec(ctx, q); err != nil {
				return fmt.Errorf("pgsource: key query for dataset %q on %s: %w", dataset, table, err)
			}
			return nil
		},
	})
}
