package pgsource_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/azbyluthfan/refset"
	"github.com/azbyluthfan/refset/examples/pgsource"
)

// Two datasets with triggers on the same table each keep theirs. Earlier
// versions named triggers after the table only, so the second install
// replaced the first and that dataset stopped seeing changes.
func TestTriggersForTwoDatasetsOnOneTable(t *testing.T) {
	p := testPool(t)
	ctx := context.Background()
	tbl, dsA, _ := itemTable(t, p, nil)
	dsB := unique("dsb")
	if err := pgsource.InstallReloadTrigger(ctx, p, dsA, tbl); err != nil {
		t.Fatal(err)
	}
	if err := pgsource.InstallReloadTrigger(ctx, p, dsB, tbl); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Exec(ctx, "INSERT INTO "+tbl+" VALUES ('x', 1)"); err != nil {
		t.Fatal(err)
	}
	v, err := pgsource.CurrentVersions(ctx, p, dsA, dsB)
	if err != nil {
		t.Fatal(err)
	}
	if v.Get(dsA) != 1 || v.Get(dsB) != 1 {
		t.Fatalf("versions after one insert: %s=%d %s=%d, want 1 each", dsA, v.Get(dsA), dsB, v.Get(dsB))
	}
}

// Reinstalling replaces a dataset's triggers that earlier versions named
// refset_<table>_<op>, so a change isn't captured twice; another dataset's
// trigger with an old name on the same table is left alone.
func TestReinstallDropsOldTriggerNames(t *testing.T) {
	p := testPool(t)
	ctx := context.Background()
	tbl, ds, _ := itemTable(t, p, nil)
	other := unique("other")
	if err := pgsource.InstallTriggers(ctx, p, ds, tbl, "k"); err != nil { // creates the trigger function
		t.Fatal(err)
	}
	// Recreate the old layout: this dataset's triggers under the old names,
	// and another dataset's insert trigger under the same old name pattern
	// on a second table.
	legacy := func(table, dataset, op, event, ref string) {
		name := pgx.Identifier{"refset_" + table + "_" + op}.Sanitize()
		if _, err := p.Exec(ctx, fmt.Sprintf(
			`CREATE OR REPLACE TRIGGER %s AFTER %s ON %s REFERENCING %s FOR EACH STATEMENT EXECUTE FUNCTION refset_capture('%s', 'k')`,
			name, event, table, ref, dataset)); err != nil {
			t.Fatal(err)
		}
	}
	legacy(tbl, ds, "ins", "INSERT", "NEW TABLE AS refset_new_rows")
	tbl2, _, _ := itemTable(t, p, nil)
	legacy(tbl2, other, "ins", "INSERT", "NEW TABLE AS refset_new_rows")

	if err := pgsource.InstallTriggers(ctx, p, ds, tbl, "k"); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Exec(ctx, "INSERT INTO "+tbl+" VALUES ('a', 1)"); err != nil {
		t.Fatal(err)
	}
	if v, _ := pgsource.CurrentVersions(ctx, p, ds); v.Get(ds) != 1 {
		t.Fatalf("one insert produced version %d: the old trigger still fires", v.Get(ds))
	}
	var n int
	if err := p.QueryRow(ctx, "SELECT count(*) FROM pg_trigger WHERE tgrelid = $1::regclass AND tgname = $2",
		tbl2, "refset_"+tbl2+"_ins").Scan(&n); err != nil || n != 1 {
		t.Fatalf("another dataset's old-named trigger was removed (%d, %v)", n, err)
	}
}

type userPlan struct {
	UserID string `db:"user_id"`
	Tier   string `db:"tier"`
}

// A dataset of users' plans joins tiers. Renaming a tier must update exactly
// the users on it, through the key-mapping trigger, without a full reload.
func TestKeyMappingTrigger(t *testing.T) {
	for _, mode := range []pgsource.TriggerMode{pgsource.Immediate, pgsource.Deferred} {
		t.Run(map[pgsource.TriggerMode]string{pgsource.Immediate: "immediate", pgsource.Deferred: "deferred"}[mode], func(t *testing.T) {
			p := testPool(t)
			ctx := context.Background()
			tiers, plans, ds := unique("tiers"), unique("plans"), unique("ds")
			for _, sql := range []string{
				fmt.Sprintf(`CREATE TABLE %s (id int PRIMARY KEY, name text NOT NULL)`, tiers),
				fmt.Sprintf(`CREATE TABLE %s (user_id text PRIMARY KEY, tier_id int NOT NULL REFERENCES %s)`, plans, tiers),
				fmt.Sprintf(`INSERT INTO %s VALUES (1, 'free'), (2, 'pro'), (3, 'enterprise')`, tiers),
				fmt.Sprintf(`INSERT INTO %s VALUES ('u1', 1), ('u2', 2), ('u3', 2)`, plans),
			} {
				if _, err := p.Exec(ctx, sql); err != nil {
					t.Fatal(err)
				}
			}
			t.Cleanup(func() { _, _ = p.Exec(context.Background(), "DROP TABLE IF EXISTS "+plans+", "+tiers) })

			if mode == pgsource.Deferred {
				if err := pgsource.InstallDeferredTriggers(ctx, p, ds, plans, "user_id"); err != nil {
					t.Fatal(err)
				}
			} else if err := pgsource.InstallTriggers(ctx, p, ds, plans, "user_id"); err != nil {
				t.Fatal(err)
			}
			if err := pgsource.InstallKeyMappingTrigger(ctx, p, mode, ds, tiers,
				fmt.Sprintf(`SELECT p.user_id FROM %s p JOIN refset_changed c ON c.id = p.tier_id`, plans)); err != nil {
				t.Fatal(err)
			}

			join := fmt.Sprintf(`SELECT p.user_id, t.name AS tier FROM %s p JOIN %s t ON t.id = p.tier_id`, plans, tiers)
			src, err := pgsource.NewSource(p, nil, pgsource.Table[string, userPlan]{
				Dataset: ds, Query: join, ByKeysQuery: join + " WHERE p.user_id = ANY($1)",
				Key: func(u userPlan) string { return u.UserID },
			})
			if err != nil {
				t.Fatal(err)
			}
			d := openDataset2(t, src)
			tierOf := func(u string) string { v, _ := d.Get(u); return v.Tier }

			// Rename a tier with raw SQL: only its users change.
			if _, err := p.Exec(ctx, "UPDATE "+tiers+" SET name = 'pro-2026' WHERE id = 2"); err != nil {
				t.Fatal(err)
			}
			v, err := pgsource.CurrentVersions(ctx, p, ds)
			if err != nil {
				t.Fatal(err)
			}
			if _, _, err := d.GetAtLeast(ctx, "u2", v.Get(ds)); err != nil {
				t.Fatal(err)
			}
			if tierOf("u1") != "free" || tierOf("u2") != "pro-2026" || tierOf("u3") != "pro-2026" {
				t.Fatalf("u1=%s u2=%s u3=%s", tierOf("u1"), tierOf("u2"), tierOf("u3"))
			}
			if st := d.Stats(); st.Reloads != 0 {
				t.Fatalf("a tier rename reloaded the whole dataset (%d reloads)", st.Reloads)
			}

			// A tier nobody is on maps to no keys: no version.
			before := v.Get(ds)
			if _, err := p.Exec(ctx, "UPDATE "+tiers+" SET name = 'ent' WHERE id = 3"); err != nil {
				t.Fatal(err)
			}
			if v, _ := pgsource.CurrentVersions(ctx, p, ds); v.Get(ds) != before {
				t.Fatalf("a change affecting no users produced version %d (was %d)", v.Get(ds), before)
			}
			if mode == pgsource.Deferred {
				assertNoPending(t, p)
			}
		})
	}
}

func TestKeyMappingTriggerRejectsBadQuery(t *testing.T) {
	p := testPool(t)
	ctx := context.Background()
	tbl, ds, _ := itemTable(t, p, nil)
	err := pgsource.InstallKeyMappingTrigger(ctx, p, pgsource.Immediate, ds, tbl, "SELECT no_such_column FROM refset_changed")
	if err == nil || !strings.Contains(err.Error(), "key query") {
		t.Fatalf("err = %v, want a key query error", err)
	}
	var n int
	if err := p.QueryRow(ctx, "SELECT count(*) FROM pg_trigger WHERE tgrelid = $1::regclass AND tgname LIKE 'refset_%'", tbl).Scan(&n); err != nil || n != 0 {
		t.Fatalf("%d triggers created despite the error (%v)", n, err)
	}
}

func openDataset2(t *testing.T, src refset.Source[string, userPlan]) *refset.Dataset[string, userPlan] {
	t.Helper()
	d, err := refset.Open(context.Background(), src, refset.Options{PollInterval: time.Hour, WaitBound: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	return d
}
