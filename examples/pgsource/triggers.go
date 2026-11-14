package pgsource

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
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
// corrupted. InstallDeferredTriggers avoids this by assigning versions at
// commit in sorted order; prefer it for new installations.
func InstallTriggers(ctx context.Context, pool *pgxpool.Pool, dataset, table, keyColumn string) error {
	if keyColumn == "" {
		return errNoKeyColumn
	}
	return install(ctx, pool, dataset, table, keyColumn, false)
}

var errNoKeyColumn = errors.New("pgsource: installing triggers needs a key column")

// InstallReloadTrigger makes any change to table (for example a lookup table
// a dataset joins) record that the whole dataset changed, so replicas reload
// it.
func InstallReloadTrigger(ctx context.Context, pool *pgxpool.Pool, dataset, table string) error {
	return install(ctx, pool, dataset, table, "", false)
}

func install(ctx context.Context, pool *pgxpool.Pool, dataset, table, keyColumn string, deferred bool) error {
	args := quoteLiteral(dataset)
	if keyColumn != "" {
		args += ", " + quoteLiteral(keyColumn)
	}
	function := "refset_capture"
	if deferred {
		function = "refset_capture_deferred"
	}
	return installTriggers(ctx, pool, triggerSpec{
		dataset: dataset, table: table, function: function, args: args, deferred: deferred,
	})
}

// triggerSpec is one dataset's set of statement triggers on one table.
type triggerSpec struct {
	dataset, table string
	function, args string // trigger function and its literal arguments
	deferred       bool   // also create the deferred-mode objects
	// check, if set, runs inside the installing transaction before the
	// triggers are created, to reject a bad configuration early.
	check func(ctx context.Context, tx pgx.Tx) error
}

func installTriggers(ctx context.Context, pool *pgxpool.Pool, spec triggerSpec) error {
	tbl := pgx.Identifier(strings.Split(spec.table, ".")).Sanitize()
	return pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext('refset_triggers'))`); err != nil {
			return err
		}
		for _, sql := range []string{captureFunctionSQL, mapKeysFunctionSQL} {
			if _, err := tx.Exec(ctx, sql); err != nil {
				return fmt.Errorf("pgsource: create trigger function: %w", err)
			}
		}
		if spec.deferred {
			if _, err := tx.Exec(ctx, deferredSchemaSQL); err != nil {
				return fmt.Errorf("pgsource: create deferred trigger objects: %w", err)
			}
		}
		if spec.check != nil {
			if err := spec.check(ctx, tx); err != nil {
				return err
			}
		}
		if err := dropLegacyTriggers(ctx, tx, spec.table, spec.dataset); err != nil {
			return err
		}
		for _, tr := range []struct{ op, event, referencing string }{
			{"ins", "INSERT", "NEW TABLE AS refset_new_rows"},
			{"upd", "UPDATE", "OLD TABLE AS refset_old_rows NEW TABLE AS refset_new_rows"},
			{"del", "DELETE", "OLD TABLE AS refset_old_rows"},
		} {
			stmt := fmt.Sprintf(`
				CREATE OR REPLACE TRIGGER %s AFTER %s ON %s
				REFERENCING %s
				FOR EACH STATEMENT EXECUTE FUNCTION %s(%s)`,
				pgx.Identifier{triggerName(spec.table, spec.dataset, tr.op)}.Sanitize(),
				tr.event, tbl, tr.referencing, spec.function, spec.args)
			if _, err := tx.Exec(ctx, stmt); err != nil {
				return fmt.Errorf("pgsource: create trigger on %s: %w", spec.table, err)
			}
		}
		return nil
	})
}

// triggerName is unique per table, dataset and operation, so datasets that
// share a table (a lookup table several datasets join, say) each keep their
// own triggers. It stays within Postgres's 63-byte identifier limit.
func triggerName(table, dataset, op string) string {
	h := fnv.New32a()
	_, _ = h.Write([]byte(table + "\x00" + dataset))
	readable := strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_' {
			return r
		}
		return '_'
	}, dataset)
	if len(readable) > 40 {
		readable = readable[:40]
	}
	return fmt.Sprintf("refset_%s_%08x_%s", readable, h.Sum32(), op)
}

// dropLegacyTriggers removes this dataset's triggers named by earlier
// versions (refset_<table>_<op>, one set per table, so a second dataset on
// the same table replaced the first's). Another dataset's legacy trigger on
// the same table is left alone.
func dropLegacyTriggers(ctx context.Context, tx pgx.Tx, table, dataset string) error {
	legacy := "refset_" + strings.NewReplacer(".", "_", `"`, "_").Replace(table) + "_"
	rows, err := tx.Query(ctx, `
		SELECT tgname FROM pg_trigger
		WHERE tgrelid = $1::regclass AND tgname = ANY($2)
		  AND tgnargs > 0 AND split_part(encode(tgargs, 'escape'), '\000', 1) = $3`,
		table, []string{legacy + "ins", legacy + "upd", legacy + "del"}, dataset)
	if err != nil {
		return fmt.Errorf("pgsource: look for older triggers on %s: %w", table, err)
	}
	names, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return err
	}
	tbl := pgx.Identifier(strings.Split(table, ".")).Sanitize()
	for _, n := range names {
		if _, err := tx.Exec(ctx, "DROP TRIGGER "+pgx.Identifier{n}.Sanitize()+" ON "+tbl); err != nil {
			return fmt.Errorf("pgsource: drop older trigger %s: %w", n, err)
		}
	}
	return nil
}

// IsDeadlock reports whether err is a Postgres deadlock (SQLSTATE 40P01),
// which a writer should retry.
func IsDeadlock(err error) bool {
	var pg interface{ SQLState() string }
	return errors.As(err, &pg) && pg.SQLState() == "40P01"
}

// quoteLiteral quotes s as a Postgres string literal for DDL, where values
// can't be passed as parameters (trigger arguments). Doubling quotes alone is
// only safe while standard_conforming_strings is on; with it off, a backslash
// could end the literal early. So backslashes are doubled too and the literal
// is written as an escape string (E'...'), which reads the same under either
// setting.
func quoteLiteral(s string) string {
	q := strings.ReplaceAll(s, "'", "''")
	if strings.Contains(q, `\`) {
		return ` E'` + strings.ReplaceAll(q, `\`, `\\`) + `'`
	}
	return "'" + q + "'"
}
