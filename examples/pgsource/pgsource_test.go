package pgsource_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/azbyluthfan/refset"
	"github.com/azbyluthfan/refset/examples/pgsource"
	"github.com/azbyluthfan/refset/refsettest"
)

// Tests need a Postgres: docker compose -f examples/docker-compose.yml up -d
// and REFSET_PG_URL=postgres://refset:refset@localhost:55432/refset?sslmode=disable

var (
	poolOnce sync.Once
	pool     *pgxpool.Pool
	poolErr  error
	seq      atomic.Int64
)

func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	url := os.Getenv("REFSET_PG_URL")
	if url == "" {
		t.Skip("REFSET_PG_URL not set; start examples/docker-compose.yml")
	}
	poolOnce.Do(func() {
		ctx := context.Background()
		pool, poolErr = pgxpool.New(ctx, url)
		if poolErr == nil {
			poolErr = pgsource.Migrate(ctx, pool)
		}
	})
	if poolErr != nil {
		t.Fatal(poolErr)
	}
	return pool
}

// unique returns a fresh name for a test's dataset or table.
func unique(prefix string) string {
	return fmt.Sprintf("%s_%d_%d", prefix, time.Now().UnixNano()%1_000_000, seq.Add(1))
}

type item struct {
	K string `db:"k"`
	V int    `db:"v"`
}

// itemTable creates a table and a source over it.
func itemTable(t *testing.T, p *pgxpool.Pool, l *pgsource.Listener, opts ...func(*pgsource.Table[string, item])) (string, string, refset.Source[string, item]) {
	t.Helper()
	ctx := context.Background()
	tbl, ds := unique("items"), unique("ds")
	if _, err := p.Exec(ctx, fmt.Sprintf(`CREATE TABLE %s (k text PRIMARY KEY, v int NOT NULL)`, tbl)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = p.Exec(context.Background(), "DROP TABLE IF EXISTS "+tbl) })
	table := pgsource.Table[string, item]{
		Dataset:     ds,
		Query:       "SELECT k, v FROM " + tbl,
		ByKeysQuery: "SELECT k, v FROM " + tbl + " WHERE k = ANY($1)",
		ByKeyQuery:  "SELECT k, v FROM " + tbl + " WHERE k = $1",
		Key:         func(i item) string { return i.K },
	}
	for _, o := range opts {
		o(&table)
	}
	src, err := pgsource.NewSource(p, l, table)
	if err != nil {
		t.Fatal(err)
	}
	return tbl, ds, src
}

// apply writes ops to tbl through Write, logging every key.
func apply(ctx context.Context, p *pgxpool.Pool, tbl, ds string, ops ...refset.Op[string, item]) (uint64, error) {
	vs, err := pgsource.Write(ctx, p, func(tx *pgsource.Tx) error {
		for _, op := range ops {
			var err error
			if op.Delete {
				_, err = tx.Exec(ctx, "DELETE FROM "+tbl+" WHERE k = $1", op.Key)
			} else {
				_, err = tx.Exec(ctx, "INSERT INTO "+tbl+" (k, v) VALUES ($1, $2) ON CONFLICT (k) DO UPDATE SET v = $2",
					op.Key, op.Value.V)
			}
			if err != nil {
				return err
			}
			if err := tx.LogUpsert(ds, op.Key); err != nil {
				return err
			}
		}
		return nil
	}, ds)
	return vs.Get(ds), err
}

func newListener(t *testing.T, p *pgxpool.Pool) *pgsource.Listener {
	l := pgsource.NewListener(p, nil)
	t.Cleanup(l.Close)
	return l
}

func TestConformance(t *testing.T) {
	p := testPool(t)
	l := newListener(t, p)
	refsettest.RunConformance(t, func(t *testing.T) refsettest.Harness[string, item] {
		tbl, ds, src := itemTable(t, p, l)
		return refsettest.Harness[string, item]{
			Source: src,
			Apply: func(ctx context.Context, ops ...refset.Op[string, item]) (uint64, error) {
				return apply(ctx, p, tbl, ds, ops...)
			},
			Compact: func(ctx context.Context) error {
				v, err := pgsource.CurrentVersions(ctx, p, ds)
				if err != nil {
					return err
				}
				return pgsource.CompactThrough(ctx, p, ds, v.Get(ds))
			},
			Key: func(i int) string { return "k" + strconv.Itoa(i) },
			Value: func(i int) item {
				return item{V: i}
			},
			// The key is part of the stored row; compare values only.
			Equal: func(a, b item) bool { return a.V == b.V },
		}
	})
}

func openDataset(t *testing.T, src refset.Source[string, item], o refset.Options) *refset.Dataset[string, item] {
	t.Helper()
	d, err := refset.Open(context.Background(), src, o)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	return d
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// Read-your-writes end to end: Write returns the version, any replica waits for it.
func TestWriteThenGetAtLeast(t *testing.T) {
	p := testPool(t)
	ctx := context.Background()
	tbl, ds, src := itemTable(t, p, newListener(t, p))
	if _, err := apply(ctx, p, tbl, ds, refset.Op[string, item]{Key: "user-1", Value: item{V: 0}}); err != nil {
		t.Fatal(err)
	}
	d := openDataset(t, src, refset.Options{WaitBound: 2 * time.Second})
	v, err := apply(ctx, p, tbl, ds, refset.Op[string, item]{Key: "user-1", Value: item{V: 1}})
	if err != nil {
		t.Fatal(err)
	}
	got, ok, err := d.GetAtLeast(ctx, "user-1", v)
	if err != nil || !ok || got.V != 1 {
		t.Fatalf("GetAtLeast = %+v, %v, %v", got, ok, err)
	}
	eventually(t, "notification applied", func() bool { return d.Version() == v })
}

// G3: kill the LISTEN connection (and the pool's idle connections), write 100
// changes; the replica catches up, through the listener's reconnect or, if a
// poll hits a dead connection, through the poll ticker. Notifications only
// shorten the delay; polling is what guarantees delivery.
func TestListenerReconnects(t *testing.T) {
	testPool(t) // migrated, and skips without REFSET_PG_URL
	ctx := context.Background()
	// A dedicated, tagged pool, so terminating its connections can't affect
	// other tests.
	cfg, _ := pgxpool.ParseConfig(os.Getenv("REFSET_PG_URL"))
	cfg.ConnConfig.RuntimeParams["application_name"] = "refset_reconnect_test"
	p, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	tbl, ds, src := itemTable(t, p, newListener(t, p))
	d := openDataset(t, src, refset.Options{PollInterval: 200 * time.Millisecond})
	v1, _ := apply(ctx, p, tbl, ds, refset.Op[string, item]{Key: "a", Value: item{V: 1}})
	eventually(t, "listener working", func() bool { return d.Version() == v1 })

	// Terminate this pool's other connections: the listener's dedicated one
	// and the idle ones the dataset polls with.
	if _, err := p.Exec(ctx, `
		SELECT pg_terminate_backend(pid) FROM pg_stat_activity
		WHERE application_name = 'refset_reconnect_test' AND pid <> pg_backend_pid()`); err != nil {
		t.Fatal(err)
	}
	var head uint64
	for i := range 100 {
		var err error
		if head, err = apply(ctx, p, tbl, ds, refset.Op[string, item]{Key: "k" + strconv.Itoa(i%10), Value: item{V: i}}); err != nil {
			t.Fatal(err)
		}
	}
	eventually(t, "replica catches up after reconnect", func() bool { return d.Version() == head })
}

// G3: a replica behind compacted history reloads a full snapshot.
func TestCompactionForcesReload(t *testing.T) {
	p := testPool(t)
	ctx := context.Background()
	tbl, ds, src := itemTable(t, p, nil)
	d := openDataset(t, src, refset.Options{PollInterval: time.Hour, WaitBound: 2 * time.Second})
	var head uint64
	for i := range 5 {
		head, _ = apply(ctx, p, tbl, ds, refset.Op[string, item]{Key: "k" + strconv.Itoa(i), Value: item{V: i}})
	}
	if err := pgsource.CompactThrough(ctx, p, ds, head); err != nil {
		t.Fatal(err)
	}
	if _, _, err := d.GetAtLeast(ctx, "k4", head); err != nil {
		t.Fatal(err)
	}
	if st := d.Stats(); st.Reloads != 1 || st.Entries != 5 {
		t.Fatalf("stats %+v", st)
	}
}

func TestBacklogLimitForcesReload(t *testing.T) {
	p := testPool(t)
	ctx := context.Background()
	tbl, ds, src := itemTable(t, p, nil, func(tb *pgsource.Table[string, item]) { tb.BacklogLimit = 3 })
	d := openDataset(t, src, refset.Options{PollInterval: time.Hour, WaitBound: 2 * time.Second})
	var head uint64
	for i := range 5 {
		head, _ = apply(ctx, p, tbl, ds, refset.Op[string, item]{Key: "k" + strconv.Itoa(i), Value: item{V: i}})
	}
	if _, _, err := d.GetAtLeast(ctx, "k4", head); err != nil {
		t.Fatal(err)
	}
	if st := d.Stats(); st.Reloads != 1 {
		t.Fatalf("expected a reload for 5 keys over a limit of 3: %+v", st)
	}
}

func TestRollbackConsumesNoVersion(t *testing.T) {
	p := testPool(t)
	ctx := context.Background()
	tbl, ds, _ := itemTable(t, p, nil)
	v1, _ := apply(ctx, p, tbl, ds, refset.Op[string, item]{Key: "a", Value: item{V: 1}})
	boom := errors.New("business rule failed")
	_, err := pgsource.Write(ctx, p, func(tx *pgsource.Tx) error {
		if _, err := tx.Exec(ctx, "UPDATE "+tbl+" SET v = 99"); err != nil {
			return err
		}
		_ = tx.LogUpsert(ds, "a")
		return boom
	}, ds)
	if !errors.Is(err, boom) {
		t.Fatalf("Write = %v", err)
	}
	vs, _ := pgsource.CurrentVersions(ctx, p, ds)
	var rows int
	_ = p.QueryRow(ctx, `SELECT count(*) FROM refset_changelog WHERE dataset = $1`, ds).Scan(&rows)
	if vs.Get(ds) != v1 || rows != 1 {
		t.Fatalf("after rollback: version %d (want %d), changelog rows %d (want 1)", vs.Get(ds), v1, rows)
	}
}

func TestUndeclaredDatasetRollsBack(t *testing.T) {
	p := testPool(t)
	ctx := context.Background()
	tbl, ds, _ := itemTable(t, p, nil)
	other := unique("ds")
	_, err := pgsource.Write(ctx, p, func(tx *pgsource.Tx) error {
		_, _ = tx.Exec(ctx, "INSERT INTO "+tbl+" VALUES ('a', 1)")
		_ = tx.LogUpsert(ds, "a")
		_ = tx.LogUpsert(other, "x") // ignored error must still fail the write
		return nil
	}, ds)
	if !errors.Is(err, pgsource.ErrUndeclared) {
		t.Fatalf("Write = %v", err)
	}
	if vs, _ := pgsource.CurrentVersions(ctx, p, ds); vs.Get(ds) != 0 {
		t.Fatal("version consumed by a failed write")
	}
}

// Writers declaring overlapping datasets in different orders never
// deadlock; declared but untouched datasets keep their version.
func TestMultiDatasetWrites(t *testing.T) {
	p := testPool(t)
	ctx := context.Background()
	ta, a, _ := itemTable(t, p, nil)
	tb, b, _ := itemTable(t, p, nil)

	const writers = 20
	var wg sync.WaitGroup
	errs := make(chan error, writers)
	for i := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			order := []string{a, b}
			if i%2 == 1 {
				order = []string{b, a}
			}
			_, err := pgsource.Write(ctx, p, func(tx *pgsource.Tx) error {
				for _, ds := range order {
					tbl := ta
					if ds == b {
						tbl = tb
					}
					if _, err := tx.Exec(ctx, "INSERT INTO "+tbl+" VALUES ($1, $2)", "k"+strconv.Itoa(i), i); err != nil {
						return err
					}
					if err := tx.LogUpsert(ds, "k"+strconv.Itoa(i)); err != nil {
						return err
					}
				}
				return nil
			}, order...)
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("write failed (deadlock?): %v", err)
		}
	}
	vs, _ := pgsource.CurrentVersions(ctx, p, a, b)
	if vs.Get(a) != writers || vs.Get(b) != writers {
		t.Fatalf("versions %v, want %d each", vs, writers)
	}

	got, err := pgsource.Write(ctx, p, func(tx *pgsource.Tx) error {
		_, _ = tx.Exec(ctx, "INSERT INTO "+ta+" VALUES ('only-a', 1)")
		return tx.LogUpsert(a, "only-a")
	}, a, b)
	if err != nil {
		t.Fatal(err)
	}
	if _, touched := got[b]; touched || got.Get(a) != writers+1 {
		t.Fatalf("Versions %v: b declared but untouched must not appear", got)
	}
	if vs, _ := pgsource.CurrentVersions(ctx, p, b); vs.Get(b) != writers {
		t.Fatal("untouched dataset's version changed")
	}
}

// An idle poll is a single statement: no transaction opened.
func TestIdlePollIsOneStatement(t *testing.T) {
	testPool(t)
	cfg, _ := pgxpool.ParseConfig(os.Getenv("REFSET_PG_URL"))
	var queries atomic.Int64
	cfg.ConnConfig.Tracer = tracer{&queries}
	p, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	ctx := context.Background()
	tbl, ds, src := itemTable(t, p, nil)
	head, _ := apply(ctx, p, tbl, ds, refset.Op[string, item]{Key: "a", Value: item{V: 1}})

	queries.Store(0)
	if h, deltas, err := src.Changes(ctx, head); err != nil || h != head || len(deltas) != 0 {
		t.Fatalf("Changes = %d, %v, %v", h, deltas, err)
	}
	if n := queries.Load(); n != 1 {
		t.Fatalf("idle poll ran %d statements, want 1", n)
	}
}

type tracer struct{ n *atomic.Int64 }

func (t tracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryStartData) context.Context {
	t.n.Add(1)
	return ctx
}
func (tracer) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

// A dataset built from a join stays correct when the joined
// table changes, by writer-logged keys, by LogReloadAll, or by a reload trigger.
func TestJoinDerivedDataset(t *testing.T) {
	p := testPool(t)
	ctx := context.Background()
	tiers, plans, ds := unique("tiers"), unique("plans"), unique("ds")
	for _, sql := range []string{
		fmt.Sprintf(`CREATE TABLE %s (id int PRIMARY KEY, name text NOT NULL)`, tiers),
		fmt.Sprintf(`CREATE TABLE %s (user_id text PRIMARY KEY, tier_id int NOT NULL REFERENCES %s)`, plans, tiers),
		fmt.Sprintf(`INSERT INTO %s VALUES (1, 'free'), (2, 'pro')`, tiers),
		fmt.Sprintf(`INSERT INTO %s VALUES ('u1', 1), ('u2', 2), ('u3', 2)`, plans),
	} {
		if _, err := p.Exec(ctx, sql); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() { _, _ = p.Exec(context.Background(), "DROP TABLE IF EXISTS "+plans+", "+tiers) })

	type userPlan struct {
		UserID string `db:"user_id"`
		Tier   string `db:"tier"`
	}
	join := fmt.Sprintf(`SELECT p.user_id, t.name AS tier FROM %s p JOIN %s t ON t.id = p.tier_id`, plans, tiers)
	src, err := pgsource.NewSource(p, nil, pgsource.Table[string, userPlan]{
		Dataset:     ds,
		Query:       join,
		ByKeysQuery: join + " WHERE p.user_id = ANY($1)",
		Key:         func(u userPlan) string { return u.UserID },
	})
	if err != nil {
		t.Fatal(err)
	}
	d, err := refset.Open(ctx, src, refset.Options{PollInterval: time.Hour, WaitBound: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = d.Close() }()
	tierOf := func(u string) string { v, _ := d.Get(u); return v.Tier }

	// 1. Writer computes and logs the affected keys.
	vs, err := pgsource.Write(ctx, p, func(tx *pgsource.Tx) error {
		if _, err := tx.Exec(ctx, "UPDATE "+tiers+" SET name = 'pro-2026' WHERE id = 2"); err != nil {
			return err
		}
		rows, _ := tx.Query(ctx, "SELECT user_id FROM "+plans+" WHERE tier_id = 2")
		users, err := pgx.CollectRows(rows, pgx.RowTo[string])
		if err != nil {
			return err
		}
		return tx.LogUpsert(ds, users...)
	}, ds)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := d.GetAtLeast(ctx, "u2", vs.Get(ds)); err != nil {
		t.Fatal(err)
	}
	if tierOf("u2") != "pro-2026" || tierOf("u3") != "pro-2026" || tierOf("u1") != "free" {
		t.Fatalf("logged keys: u1=%s u2=%s u3=%s", tierOf("u1"), tierOf("u2"), tierOf("u3"))
	}

	// 2. Writer marks the whole dataset changed.
	vs, err = pgsource.Write(ctx, p, func(tx *pgsource.Tx) error {
		if _, err := tx.Exec(ctx, "UPDATE "+tiers+" SET name = 'basic' WHERE id = 1"); err != nil {
			return err
		}
		return tx.LogReloadAll(ds)
	}, ds)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := d.GetAtLeast(ctx, "u1", vs.Get(ds)); err != nil {
		t.Fatal(err)
	}
	if tierOf("u1") != "basic" || d.Stats().Reloads != 1 {
		t.Fatalf("reload marker: u1=%s stats %+v", tierOf("u1"), d.Stats())
	}

	// 3. A reload trigger on the joined table catches a raw UPDATE.
	if err := pgsource.InstallReloadTrigger(ctx, p, ds, tiers); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Exec(ctx, "UPDATE "+tiers+" SET name = 'starter' WHERE id = 1"); err != nil {
		t.Fatal(err)
	}
	v, _ := pgsource.CurrentVersions(ctx, p, ds)
	if _, _, err := d.GetAtLeast(ctx, "u1", v.Get(ds)); err != nil {
		t.Fatal(err)
	}
	if tierOf("u1") != "starter" {
		t.Fatalf("reload trigger: u1=%s", tierOf("u1"))
	}
}

// Triggers capture writes that bypass Write.
func TestTriggerCapture(t *testing.T) {
	p := testPool(t)
	ctx := context.Background()
	tbl, ds, src := itemTable(t, p, newListener(t, p))
	if err := pgsource.InstallTriggers(ctx, p, ds, tbl, "k"); err != nil {
		t.Fatal(err)
	}
	d := openDataset(t, src, refset.Options{PollInterval: time.Hour, WaitBound: 2 * time.Second})

	for _, sql := range []string{
		"INSERT INTO " + tbl + " VALUES ('a', 1), ('b', 2), ('c', 3)",
		"UPDATE " + tbl + " SET v = v * 10 WHERE k <> 'c'",
		"DELETE FROM " + tbl + " WHERE k = 'c'",
		"UPDATE " + tbl + " SET v = 0 WHERE k = 'nobody'", // touches no rows: no version
	} {
		if _, err := p.Exec(ctx, sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	vs, _ := pgsource.CurrentVersions(ctx, p, ds)
	if vs.Get(ds) != 3 {
		t.Fatalf("version %d after 3 effective statements", vs.Get(ds))
	}
	eventually(t, "replica applies trigger-captured changes", func() bool { return d.Version() == 3 })
	a, _ := d.Get("a")
	b, _ := d.Get("b")
	_, hasC := d.Get("c")
	if a.V != 10 || b.V != 20 || hasC {
		t.Fatalf("replica: a=%d b=%d c present=%v", a.V, b.V, hasC)
	}
}

// Trigger mode can deadlock; Postgres aborts one writer
// with 40P01, the retry succeeds, and replicas stay correct.
func TestTriggerModeDeadlockIsRetryable(t *testing.T) {
	p := testPool(t)
	ctx := context.Background()
	ta, dsA, srcA := itemTable(t, p, nil)
	tb, dsB, _ := itemTable(t, p, nil)
	for _, x := range [][2]string{{dsA, ta}, {dsB, tb}} {
		if err := pgsource.InstallTriggers(ctx, p, x[0], x[1], "k"); err != nil {
			t.Fatal(err)
		}
	}
	dA := openDataset(t, srcA, refset.Options{PollInterval: 20 * time.Millisecond})

	tx1, _ := p.Begin(ctx)
	tx2, _ := p.Begin(ctx)
	defer func() { _ = tx1.Rollback(ctx) }()
	defer func() { _ = tx2.Rollback(ctx) }()
	if _, err := tx1.Exec(ctx, "INSERT INTO "+ta+" VALUES ('x', 1)"); err != nil {
		t.Fatal(err)
	}
	if _, err := tx2.Exec(ctx, "INSERT INTO "+tb+" VALUES ('y', 2)"); err != nil {
		t.Fatal(err)
	}
	errs := make(chan error, 2)
	go func() { _, err := tx1.Exec(ctx, "INSERT INTO "+tb+" VALUES ('x', 1)"); errs <- err }()
	time.Sleep(100 * time.Millisecond)
	go func() { _, err := tx2.Exec(ctx, "INSERT INTO "+ta+" VALUES ('y', 2)"); errs <- err }()
	e1, e2 := <-errs, <-errs
	deadlocks := 0
	for _, e := range []error{e1, e2} {
		if pgsource.IsDeadlock(e) {
			deadlocks++
		} else if e != nil {
			t.Fatalf("unexpected error: %v", e)
		}
	}
	if deadlocks != 1 {
		t.Fatalf("want exactly one deadlock abort, got errors %v / %v", e1, e2)
	}
	// Commit the survivor; the aborted one fails to commit and is retried.
	err1, err2 := tx1.Commit(ctx), tx2.Commit(ctx)
	if (err1 == nil) == (err2 == nil) {
		t.Fatalf("exactly one commit should succeed: %v / %v", err1, err2)
	}
	retryRow := "x"
	if err2 != nil {
		retryRow = "y"
	}
	if err := pgx.BeginFunc(ctx, p, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, "INSERT INTO "+ta+" VALUES ($1, 1) ON CONFLICT DO NOTHING", retryRow); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, "INSERT INTO "+tb+" VALUES ($1, 1) ON CONFLICT DO NOTHING", retryRow)
		return err
	}); err != nil {
		t.Fatalf("retry: %v", err)
	}
	eventually(t, "replica A has both rows", func() bool {
		_, x := dA.Get("x")
		_, y := dA.Get("y")
		return x && y
	})
}

// Design §4.10: a write that skips logging leaves the replica diverged while
// GetFresh still reports fresh; ReloadInterval bounds how long that lasts.
func TestUnloggedWriteDivergesUntilReload(t *testing.T) {
	p := testPool(t)
	ctx := context.Background()
	tbl, ds, src := itemTable(t, p, nil)
	v, _ := apply(ctx, p, tbl, ds, refset.Op[string, item]{Key: "a", Value: item{V: 1}})
	d := openDataset(t, src, refset.Options{
		PollInterval: 20 * time.Millisecond, MaxStaleness: time.Second, ReloadInterval: 300 * time.Millisecond,
	})
	if _, err := p.Exec(ctx, "UPDATE "+tbl+" SET v = 2 WHERE k = 'a'"); err != nil { // no LogUpsert
		t.Fatal(err)
	}
	time.Sleep(60 * time.Millisecond) // a couple of polls
	got, _, err := d.GetFresh("a")
	if err != nil || got.V != 1 || d.Version() != v {
		t.Fatalf("expected undetected divergence (fresh, old value): %+v, %v", got, err)
	}
	eventually(t, "periodic reload repairs the divergence", func() bool { g, _ := d.Get("a"); return g.V == 2 })
}

func TestKeyLoaderFallback(t *testing.T) {
	p := testPool(t)
	ctx := context.Background()
	tbl, ds, src := itemTable(t, p, nil)
	if _, ok := src.(refset.KeyLoader[string, item]); !ok {
		t.Fatal("ByKeyQuery set but no KeyLoader")
	}
	_, _, plain := itemTable(t, p, nil, func(tb *pgsource.Table[string, item]) { tb.ByKeyQuery = "" })
	if _, ok := plain.(refset.KeyLoader[string, item]); ok {
		t.Fatal("KeyLoader without ByKeyQuery")
	}
	d := openDataset(t, src, refset.Options{PollInterval: time.Hour, WaitBound: -1})
	v, _ := apply(ctx, p, tbl, ds, refset.Op[string, item]{Key: "a", Value: item{V: 7}})
	got, ok, err := d.GetAtLeast(ctx, "a", v)
	if err != nil || !ok || got.V != 7 || d.Stats().Fallbacks != 1 {
		t.Fatalf("fallback = %+v %v %v, stats %+v", got, ok, err, d.Stats())
	}
}

func TestNewSourceValidation(t *testing.T) {
	p := testPool(t)
	if _, err := pgsource.NewSource(p, nil, pgsource.Table[string, item]{Dataset: "x"}); err == nil {
		t.Fatal("incomplete Table accepted")
	}
	type key struct{ A, B int }
	_, err := pgsource.NewSource(p, nil, pgsource.Table[key, item]{
		Dataset: "x", Query: "q", ByKeysQuery: "q", Key: func(item) key { return key{} },
	})
	if err == nil {
		t.Fatal("struct key without Codec accepted")
	}
}

// Random writes from several goroutines; a replica converges to the table.
func TestConvergenceAgainstTable(t *testing.T) {
	p := testPool(t)
	ctx := context.Background()
	tbl, ds, src := itemTable(t, p, newListener(t, p))
	d := openDataset(t, src, refset.Options{PollInterval: 50 * time.Millisecond})
	var wg sync.WaitGroup
	for w := range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(uint64(w), 1))
			for range 100 {
				k := "k" + strconv.Itoa(rng.IntN(30))
				_, _ = apply(ctx, p, tbl, ds, refset.Op[string, item]{Key: k, Value: item{V: rng.IntN(1000)}, Delete: rng.IntN(5) == 0})
			}
		}()
	}
	wg.Wait()
	vs, _ := pgsource.CurrentVersions(ctx, p, ds)
	eventually(t, "replica at head", func() bool { return d.Version() == vs.Get(ds) })
	rows, _ := p.Query(ctx, "SELECT k, v FROM "+tbl)
	want, err := pgx.CollectRows(rows, pgx.RowToStructByName[item])
	if err != nil {
		t.Fatal(err)
	}
	s := d.Snapshot()
	if s.Len() != len(want) {
		t.Fatalf("replica has %d rows, table %d", s.Len(), len(want))
	}
	for _, w := range want {
		if g, ok := s.Get(w.K); !ok || g.V != w.V {
			t.Fatalf("key %s: replica %+v, table %+v", w.K, g, w)
		}
	}
}
