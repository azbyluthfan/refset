package pgsource_test

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/azbyluthfan/refset"
	"github.com/azbyluthfan/refset/examples/pgsource"
)

var (
	replicaOnce sync.Once
	replicaPool *pgxpool.Pool
	replicaErr  error
)

// simulatedReplica is a second database on the test server standing in for
// a lagging read replica: tests copy a table into it as it was at an older
// version. (A real hot standby can't be lagged on demand in CI.)
func simulatedReplica(t *testing.T) *pgxpool.Pool {
	t.Helper()
	p := testPool(t)
	replicaOnce.Do(func() {
		ctx := context.Background()
		_, _ = p.Exec(ctx, "CREATE DATABASE refset_replica_sim") // exists after the first run
		u, err := url.Parse(os.Getenv("REFSET_PG_URL"))
		if err != nil {
			replicaErr = err
			return
		}
		u.Path = "/refset_replica_sim"
		if replicaPool, replicaErr = pgxpool.New(ctx, u.String()); replicaErr == nil {
			replicaErr = pgsource.Migrate(ctx, replicaPool)
		}
	})
	if replicaErr != nil {
		t.Fatal(replicaErr)
	}
	return replicaPool
}

// lagFixture writes five versions of a dataset on the primary and puts its
// state at version 3 on the simulated replica.
func lagFixture(t *testing.T) (primary, replica *pgxpool.Pool, table, ds string) {
	t.Helper()
	primary, replica = testPool(t), simulatedReplica(t)
	ctx := context.Background()
	table, ds = unique("items"), unique("ds")
	for _, p := range []*pgxpool.Pool{primary, replica} {
		if _, err := p.Exec(ctx, fmt.Sprintf(`CREATE TABLE %s (k text PRIMARY KEY, v int NOT NULL)`, table)); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		for _, p := range []*pgxpool.Pool{primary, replica} {
			_, _ = p.Exec(context.Background(), "DROP TABLE IF EXISTS "+table)
		}
	})
	for i := 1; i <= 5; i++ {
		writeItem(t, primary, table, ds, "k"+strconv.Itoa(i), i)
		if i == 3 {
			if _, err := replica.Exec(ctx, fmt.Sprintf(`INSERT INTO %s SELECT k, v FROM (VALUES ('k1',1),('k2',2),('k3',3)) AS r(k, v)`, table)); err != nil {
				t.Fatal(err)
			}
			if _, err := replica.Exec(ctx, `INSERT INTO refset_versions (dataset, version) VALUES ($1, 3)`, ds); err != nil {
				t.Fatal(err)
			}
		}
	}
	return primary, replica, table, ds
}

func writeItem(t *testing.T, p *pgxpool.Pool, table, ds, k string, v int) {
	t.Helper()
	if _, err := pgsource.Write(context.Background(), p, func(tx *pgsource.Tx) error {
		if _, err := tx.Exec(context.Background(), "INSERT INTO "+table+" VALUES ($1, $2) ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v", k, v); err != nil {
			return err
		}
		return tx.LogUpsert(ds, k)
	}, ds); err != nil {
		t.Fatal(err)
	}
}

func replicaSource(t *testing.T, primary, snapshots *pgxpool.Pool, table, ds string, backlog int) refset.Source[string, item] {
	t.Helper()
	src, err := pgsource.NewSource(primary, nil, pgsource.Table[string, item]{
		Dataset: ds, Query: "SELECT k, v FROM " + table, ByKeysQuery: "SELECT k, v FROM " + table + " WHERE k = ANY($1)",
		Key: func(i item) string { return i.K }, SnapshotPool: snapshots, BacklogLimit: backlog,
	})
	if err != nil {
		t.Fatal(err)
	}
	return src
}

// openedAt opens a dataset without polling, returns the version Open loaded
// (which shows where the snapshot came from), then checks it catches up to
// the primary's state.
func openedAt(t *testing.T, src refset.Source[string, item]) uint64 {
	t.Helper()
	d := openDataset(t, src, refset.Options{PollInterval: time.Hour, WaitBound: 5 * time.Second})
	loaded := d.Version()
	if _, _, err := d.GetAtLeast(context.Background(), "k5", 5); err != nil {
		t.Fatalf("catching up to version 5: %v", err)
	}
	for i := 1; i <= 5; i++ {
		if v, ok := d.Get("k" + strconv.Itoa(i)); !ok || v.V != i {
			t.Fatalf("after catching up, k%d = %v %v", i, v, ok)
		}
	}
	return loaded
}

func TestSnapshotFromReplica(t *testing.T) {
	primary, replica, table, ds := lagFixture(t)
	if got := openedAt(t, replicaSource(t, primary, replica, table, ds, 0)); got != 3 {
		t.Fatalf("Open loaded version %d; want 3, the replica's", got)
	}
}

func TestSnapshotFromReplicaFallsBackToPrimary(t *testing.T) {
	t.Run("compacted past the replica", func(t *testing.T) {
		primary, replica, table, ds := lagFixture(t)
		if err := pgsource.CompactThrough(context.Background(), primary, ds, 4); err != nil {
			t.Fatal(err)
		}
		if got := openedAt(t, replicaSource(t, primary, replica, table, ds, 0)); got != 5 {
			t.Fatalf("Open loaded version %d; want 5, the primary's", got)
		}
	})
	t.Run("reload marker after the replica", func(t *testing.T) {
		primary, replica, table, ds := lagFixture(t)
		if _, err := pgsource.Write(context.Background(), primary, func(tx *pgsource.Tx) error {
			return tx.LogReloadAll(ds)
		}, ds); err != nil {
			t.Fatal(err)
		}
		if got := openedAt(t, replicaSource(t, primary, replica, table, ds, 0)); got != 6 {
			t.Fatalf("Open loaded version %d; want 6, the primary's", got)
		}
	})
	t.Run("backlog over the limit", func(t *testing.T) {
		primary, replica, table, ds := lagFixture(t)
		// k4 and k5 changed after the replica's version: 2 keys > limit 1.
		if got := openedAt(t, replicaSource(t, primary, replica, table, ds, 1)); got != 5 {
			t.Fatalf("Open loaded version %d; want 5, the primary's", got)
		}
	})
	t.Run("replica unreachable", func(t *testing.T) {
		primary, _, table, ds := lagFixture(t)
		u, _ := url.Parse(os.Getenv("REFSET_PG_URL"))
		u.Host = "127.0.0.1:1" // nothing listens here
		dead, err := pgxpool.New(context.Background(), u.String())
		if err != nil {
			t.Fatal(err)
		}
		defer dead.Close()
		if got := openedAt(t, replicaSource(t, primary, dead, table, ds, 0)); got != 5 {
			t.Fatalf("Open loaded version %d; want 5, the primary's", got)
		}
	})
}
