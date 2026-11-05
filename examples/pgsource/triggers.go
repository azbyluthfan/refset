package pgsource

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// captureFunctionSQL is one trigger function for both modes. With a key column
// argument it logs the changed keys; without one it logs a reload marker. It
// skips statements that touched no rows, so they consume no version.
const captureFunctionSQL = `
CREATE OR REPLACE FUNCTION refset_capture() RETURNS trigger LANGUAGE plpgsql AS $fn$
DECLARE
	ds     text := TG_ARGV[0];
	keycol text := TG_ARGV[1];
	n      bigint := 0;
	v      bigint;
BEGIN
	IF TG_OP IN ('INSERT', 'UPDATE') THEN
		EXECUTE 'SELECT count(*) FROM refset_new_rows' INTO n;
	END IF;
	IF n = 0 AND TG_OP IN ('UPDATE', 'DELETE') THEN
		EXECUTE 'SELECT count(*) FROM refset_old_rows' INTO n;
	END IF;
	IF n = 0 THEN
		RETURN NULL;
	END IF;

	INSERT INTO refset_versions (dataset) VALUES (ds) ON CONFLICT (dataset) DO NOTHING;
	UPDATE refset_versions SET version = version + 1 WHERE dataset = ds RETURNING version INTO v;

	IF keycol IS NULL THEN
		INSERT INTO refset_changelog (dataset, version, key, reload) VALUES (ds, v, '', true);
	ELSE
		IF TG_OP IN ('INSERT', 'UPDATE') THEN
			EXECUTE format('INSERT INTO refset_changelog (dataset, version, key)
				SELECT DISTINCT $1, $2, (%I)::text FROM refset_new_rows', keycol) USING ds, v;
		END IF;
		IF TG_OP IN ('UPDATE', 'DELETE') THEN
			EXECUTE format('INSERT INTO refset_changelog (dataset, version, key)
				SELECT DISTINCT $1, $2, (%I)::text FROM refset_old_rows', keycol) USING ds, v;
		END IF;
	END IF;

	PERFORM pg_notify('refset', ds || ':' || v);
	RETURN NULL;
END
$fn$;
`

// InstallTriggers makes every INSERT, UPDATE and DELETE on table log the
// changed keys of dataset (keyColumn, cast to text), so writes that don't go
// through Write (manual SQL, migrations, other services) are still captured.
// Writers then read versions with CurrentVersions instead of using Write; do
// not use both on the same dataset.
//
// Unlike Write, triggers lock a dataset's version row when a statement first
// touches its table, in whatever order the SQL runs. Two transactions writing
// tables A then B and B then A can deadlock; Postgres aborts one with
// SQLSTATE 40P01, which the writer must retry (IsDeadlock). Nothing is
// corrupted. Keep such transactions short and touch tables in a consistent
// order.
func InstallTriggers(ctx context.Context, pool *pgxpool.Pool, dataset, table, keyColumn string) error {
	if keyColumn == "" {
		return errors.New("pgsource: InstallTriggers needs a key column")
	}
	return install(ctx, pool, dataset, table, keyColumn)
}

// InstallReloadTrigger makes any change to table (for example a lookup table
// a dataset joins) record that the whole dataset changed, so replicas reload
// it.
func InstallReloadTrigger(ctx context.Context, pool *pgxpool.Pool, dataset, table string) error {
	return install(ctx, pool, dataset, table, "")
}

func install(ctx context.Context, pool *pgxpool.Pool, dataset, table, keyColumn string) error {
	tbl := pgx.Identifier(strings.Split(table, ".")).Sanitize()
	args := quoteLiteral(dataset)
	if keyColumn != "" {
		args += ", " + quoteLiteral(keyColumn)
	}
	prefix := "refset_" + strings.NewReplacer(".", "_", `"`, "_").Replace(table) + "_"
	return pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext('refset_triggers'))`); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, captureFunctionSQL); err != nil {
			return fmt.Errorf("pgsource: create trigger function: %w", err)
		}
		triggers := []struct{ name, event, referencing string }{
			{"ins", "INSERT", "NEW TABLE AS refset_new_rows"},
			{"upd", "UPDATE", "OLD TABLE AS refset_old_rows NEW TABLE AS refset_new_rows"},
			{"del", "DELETE", "OLD TABLE AS refset_old_rows"},
		}
		for _, tr := range triggers {
			name := pgx.Identifier{prefix + tr.name}.Sanitize()
			stmt := fmt.Sprintf(`
				CREATE OR REPLACE TRIGGER %s AFTER %s ON %s
				REFERENCING %s
				FOR EACH STATEMENT EXECUTE FUNCTION refset_capture(%s)`,
				name, tr.event, tbl, tr.referencing, args)
			if _, err := tx.Exec(ctx, stmt); err != nil {
				return fmt.Errorf("pgsource: create trigger on %s: %w", table, err)
			}
		}
		return nil
	})
}

// IsDeadlock reports whether err is a Postgres deadlock (SQLSTATE 40P01),
// which a writer should retry.
func IsDeadlock(err error) bool {
	var pg interface{ SQLState() string }
	return errors.As(err, &pg) && pg.SQLState() == "40P01"
}

func quoteLiteral(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }
