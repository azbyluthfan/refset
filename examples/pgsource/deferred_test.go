package pgsource_test

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/azbyluthfan/refset"
	"github.com/azbyluthfan/refset/examples/pgsource"
)

// The scenario that deadlocks in immediate mode (TestTriggerModeDeadlockIsRetryable):
// two transactions write tables A and B in opposite orders. In deferred mode
// both commit, and replicas see both.
func TestDeferredTriggersDontDeadlock(t *testing.T) {
	p := testPool(t)
	ctx := context.Background()
	ta, dsA, srcA := itemTable(t, p, nil)
	tb, dsB, srcB := itemTable(t, p, nil)
	for _, x := range [][2]string{{dsA, ta}, {dsB, tb}} {
		if err := pgsource.InstallDeferredTriggers(ctx, p, x[0], x[1], "k"); err != nil {
			t.Fatal(err)
		}
	}
	dA := openDataset(t, srcA, refset.Options{PollInterval: 20 * time.Millisecond})
	dB := openDataset(t, srcB, refset.Options{PollInterval: 20 * time.Millisecond})

	tx1, _ := p.Begin(ctx)
	tx2, _ := p.Begin(ctx)
	defer func() { _ = tx1.Rollback(ctx) }()
	defer func() { _ = tx2.Rollback(ctx) }()
	mustExec(t, tx1, "INSERT INTO "+ta+" VALUES ('x', 1)")
	mustExec(t, tx2, "INSERT INTO "+tb+" VALUES ('y', 2)")
	mustExec(t, tx1, "INSERT INTO "+tb+" VALUES ('x', 1)") // didn't block: no version row locked yet
	mustExec(t, tx2, "INSERT INTO "+ta+" VALUES ('y', 2)")

	errs := make(chan error, 2)
	go func() { errs <- tx1.Commit(ctx) }()
	go func() { errs <- tx2.Commit(ctx) }()
	for range 2 {
		if err := <-errs; err != nil {
			t.Fatalf("commit failed: %v", err)
		}
	}
	for _, d := range []*refset.Dataset[string, item]{dA, dB} {
		eventually(t, "replica has both rows", func() bool {
			_, x := d.Get("x")
			_, y := d.Get("y")
			return x && y
		})
	}
	// Each transaction produced one version per dataset.
	v, err := pgsource.CurrentVersions(ctx, p, dsA, dsB)
	if err != nil {
		t.Fatal(err)
	}
	if v.Get(dsA) != 2 || v.Get(dsB) != 2 {
		t.Fatalf("versions %v, want 2 each", v)
	}
	assertNoPending(t, p)
}

// Many writers touching four tables in random orders: none is aborted for a
// deadlock, and every replica converges on its table.
func TestDeferredTriggersUnderConcurrency(t *testing.T) {
	p := testPool(t)
	ctx := context.Background()
	type ds struct {
		table, name string
		d           *refset.Dataset[string, item]
	}
	var sets []ds
	for range 4 {
		tbl, name, src := itemTable(t, p, nil)
		if err := pgsource.InstallDeferredTriggers(ctx, p, name, tbl, "k"); err != nil {
			t.Fatal(err)
		}
		sets = append(sets, ds{tbl, name, openDataset(t, src, refset.Options{PollInterval: 20 * time.Millisecond})})
	}
	const writers, rounds = 20, 10
	var wg sync.WaitGroup
	errs := make(chan error, writers*rounds)
	for w := range writers {
		wg.Go(func() {
			rng := rand.New(rand.NewPCG(uint64(w), 9))
			for r := range rounds {
				order := rng.Perm(len(sets))
				err := pgx.BeginFunc(ctx, p, func(tx pgx.Tx) error {
					for _, i := range order {
						if _, err := tx.Exec(ctx, fmt.Sprintf(
							"INSERT INTO %s VALUES ($1, $2) ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v", sets[i].table),
							fmt.Sprintf("w%d", w), r); err != nil {
							return err
						}
						time.Sleep(time.Millisecond) // widen the window for lock-order conflicts
					}
					return nil
				})
				if err != nil {
					errs <- err
				}
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("writer failed (deadlock = %v): %v", pgsource.IsDeadlock(err), err)
	}
	for _, s := range sets {
		eventually(t, "replica converged", func() bool {
			if s.d.Snapshot().Len() != writers {
				return false
			}
			for w := range writers {
				if v, ok := s.d.Get(fmt.Sprintf("w%d", w)); !ok || v.V != rounds-1 {
					return false
				}
			}
			return true
		})
	}
	assertNoPending(t, p)
}

// In deferred mode the version is assigned at commit: read inside the
// transaction it is still the old one, read after the commit it covers the
// write, which is what a writer must send to clients.
func TestDeferredTriggersVersionAfterCommit(t *testing.T) {
	p := testPool(t)
	ctx := context.Background()
	tbl, name, src := itemTable(t, p, nil)
	if err := pgsource.InstallDeferredTriggers(ctx, p, name, tbl, "k"); err != nil {
		t.Fatal(err)
	}
	d := openDataset(t, src, refset.Options{PollInterval: time.Hour, WaitBound: 5 * time.Second})

	var inside pgsource.Versions
	if err := pgx.BeginFunc(ctx, p, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, "INSERT INTO "+tbl+" VALUES ('a', 1)"); err != nil {
			return err
		}
		var err error
		inside, err = pgsource.CurrentVersions(ctx, tx, name)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	after, err := pgsource.CurrentVersions(ctx, p, name)
	if err != nil {
		t.Fatal(err)
	}
	if inside.Get(name) != 0 || after.Get(name) != 1 {
		t.Fatalf("version inside the transaction %d (want 0), after commit %d (want 1)", inside.Get(name), after.Get(name))
	}
	got, ok, err := d.GetAtLeast(ctx, "a", after.Get(name))
	if err != nil || !ok || got.V != 1 {
		t.Fatalf("read-your-writes with the version read after commit: %v %v %v", got, ok, err)
	}
}

// Rolled-back transactions and statements that touch no rows consume no
// version and leave nothing pending.
func TestDeferredTriggersRollbackAndNoOps(t *testing.T) {
	p := testPool(t)
	ctx := context.Background()
	tbl, name, _ := itemTable(t, p, nil)
	if err := pgsource.InstallDeferredTriggers(ctx, p, name, tbl, "k"); err != nil {
		t.Fatal(err)
	}
	tx, _ := p.Begin(ctx)
	mustExec(t, tx, "INSERT INTO "+tbl+" VALUES ('a', 1)")
	_ = tx.Rollback(ctx)
	if _, err := p.Exec(ctx, "UPDATE "+tbl+" SET v = 2 WHERE k = 'missing'"); err != nil {
		t.Fatal(err)
	}
	v, err := pgsource.CurrentVersions(ctx, p, name)
	if err != nil {
		t.Fatal(err)
	}
	if v.Get(name) != 0 {
		t.Fatalf("version %d after a rollback and a no-op, want 0", v.Get(name))
	}
	assertNoPending(t, p)
}

func mustExec(t *testing.T, tx pgx.Tx, sql string) {
	t.Helper()
	if _, err := tx.Exec(context.Background(), sql); err != nil {
		t.Fatal(err)
	}
}

// assertNoPending checks the commit step cleaned up after itself.
func assertNoPending(t *testing.T, p interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}) {
	t.Helper()
	var n int
	if err := p.QueryRow(context.Background(),
		"SELECT (SELECT count(*) FROM refset_pending) + (SELECT count(*) FROM refset_pending_tx)").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("%d pending rows left after commit", n)
	}
}
