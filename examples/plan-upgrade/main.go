// Command plan-upgrade is design §6.2 as a runnable story: a customer upgrades
// from Free to Pro, and their next request lands on a replica that hasn't
// caught up. It starts a billing service and two API replicas over real HTTP
// and Postgres, then plays the requests and prints what each replica answers.
//
//	make demo
//
// (which runs docker compose for Postgres and then this program).
package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/azbyluthfan/refset"
	"github.com/azbyluthfan/refset/examples/pgsource"
	"github.com/azbyluthfan/refset/refsethttp"
)

// PlansDataset is shared by billing (the writer) and the replicas.
const PlansDataset = "plans"

type Plan struct {
	UserID string `db:"user_id"`
	Tier   string `db:"tier"`
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, envOr("REFSET_PG_URL", "postgres://refset:refset@localhost:55432/refset?sslmode=disable"))
	if err != nil {
		return err
	}
	defer pool.Close()
	if err := setup(ctx, pool); err != nil {
		return err
	}

	billing := httptest.NewServer(billingHandler(pool))
	defer billing.Close()
	// Replica B: no change notifications, a 2s poll and a 40ms path to the
	// database, so it is behind when the customer's next request arrives.
	b := newReplica(ctx, pool, "B", 40*time.Millisecond, false)
	defer b.Close()
	// Replica C: a very slow path (400ms) but a fallback query to the primary.
	c := newReplica(ctx, pool, "C", 400*time.Millisecond, true)
	defer c.Close()

	client := billing.Client()
	step := func(n int, s string) { fmt.Printf("\n%d. %s\n", n, s) }

	step(1, "user-1 upgrades from Free to Pro (POST billing/upgrade)")
	version := upgrade(client, billing.URL, "user-1")
	fmt.Printf("   billing commits and returns   Refset-Version: %s\n", version)

	step(2, "next request lands on replica B WITHOUT the version (what a TTL cache does)")
	get(client, b.URL, "user-1", "")

	step(3, "same request WITH Refset-Min-Version: B waits for that version")
	get(client, b.URL, "user-1", version)

	step(4, "user-2 upgrades; replica C is far too slow, so it falls back to the primary")
	version = upgrade(client, billing.URL, "user-2")
	get(client, c.URL, "user-2", version)

	step(5, "a client forges a version that doesn't exist")
	get(client, b.URL, "user-1", "plans=999999999")

	fmt.Println("\nWithout the version the replica answers from stale memory; with it, every replica answers correctly.")
	return nil
}

func billingHandler(pool *pgxpool.Pool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user := r.URL.Query().Get("user")
		versions, err := pgsource.Write(r.Context(), pool, func(tx *pgsource.Tx) error {
			if _, err := tx.Exec(r.Context(), `UPDATE user_plans SET tier = 'pro' WHERE user_id = $1`, user); err != nil {
				return err
			}
			return tx.LogUpsert(PlansDataset, user)
		}, PlansDataset)
		if err != nil {
			http.Error(w, "upgrade failed", http.StatusInternalServerError)
			return
		}
		refsethttp.SetVersions(w, versions) // Refset-Version: plans=N
		w.WriteHeader(http.StatusNoContent)
	})
}

type replica struct {
	*httptest.Server
	plans *refset.Dataset[string, Plan]
}

func (r *replica) Close() {
	r.Server.Close()
	_ = r.plans.Close()
}

func newReplica(ctx context.Context, pool *pgxpool.Pool, name string, delay time.Duration, fallback bool) *replica {
	table := pgsource.Table[string, Plan]{
		Dataset:     PlansDataset,
		Query:       "SELECT user_id, tier FROM user_plans",
		ByKeysQuery: "SELECT user_id, tier FROM user_plans WHERE user_id = ANY($1)",
		Key:         func(p Plan) string { return p.UserID },
	}
	if fallback {
		table.ByKeyQuery = "SELECT user_id, tier FROM user_plans WHERE user_id = $1"
	}
	src, err := pgsource.NewSource(pool, nil, table)
	if err != nil {
		log.Fatal(err)
	}
	plans, err := refset.Open(ctx, slow(src, delay), refset.Options{PollInterval: 2 * time.Second})
	if err != nil {
		log.Fatal(err)
	}
	srv := httptest.NewServer(refsethttp.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		p, ok, err := plans.GetCtx(r.Context(), r.URL.Query().Get("user"))
		took := time.Since(start).Round(time.Millisecond)
		w.Header().Set("X-Took", took.String())
		w.Header().Set("X-Replica", name)
		switch {
		case err != nil || !ok:
			http.Error(w, fmt.Sprintf("plan unavailable (%v)", err), http.StatusServiceUnavailable)
		case p.Tier != "pro":
			http.Error(w, "upgrade required (plan: "+p.Tier+")", http.StatusForbidden)
		default:
			_, _ = io.WriteString(w, "advanced report")
		}
	})))
	return &replica{Server: srv, plans: plans}
}

func upgrade(c *http.Client, base, user string) string {
	resp, err := c.Post(base+"/upgrade?user="+user, "", nil)
	if err != nil {
		log.Fatal(err)
	}
	_ = resp.Body.Close()
	return resp.Header.Get("Refset-Version")
}

func get(c *http.Client, base, user, minVersion string) {
	req, _ := http.NewRequest(http.MethodGet, base+"/reports/advanced?user="+user, nil)
	if minVersion != "" {
		req.Header.Set("Refset-Min-Version", minVersion)
	}
	resp, err := c.Do(req)
	if err != nil {
		log.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	sent := "(no version)"
	if minVersion != "" {
		sent = "Refset-Min-Version: " + minVersion
	}
	fmt.Printf("   replica %s  %-34s -> %d %-34s in %s\n",
		resp.Header.Get("X-Replica"), sent, resp.StatusCode, strings.TrimSpace(string(body)), resp.Header.Get("X-Took"))
}

func setup(ctx context.Context, pool *pgxpool.Pool) error {
	if err := pgsource.Migrate(ctx, pool); err != nil {
		return err
	}
	if _, err := pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS user_plans (user_id text PRIMARY KEY, tier text NOT NULL)`); err != nil {
		return err
	}
	_, err := pgsource.Write(ctx, pool, func(tx *pgsource.Tx) error {
		if _, err := tx.Exec(ctx, `INSERT INTO user_plans VALUES ('user-1', 'free'), ('user-2', 'free')
			ON CONFLICT (user_id) DO UPDATE SET tier = 'free'`); err != nil {
			return err
		}
		return tx.LogUpsert(PlansDataset, "user-1", "user-2")
	}, PlansDataset)
	return err
}

// slow wraps a source so every call to it takes delay longer, as a slow
// network or overloaded database would. It keeps the KeyLoader fallback fast:
// that is a direct query to the primary.
func slow[K comparable, V any](src refset.Source[K, V], delay time.Duration) refset.Source[K, V] {
	s := slowSource[K, V]{Source: src, delay: delay}
	if kl, ok := src.(refset.KeyLoader[K, V]); ok {
		return slowKeyed[K, V]{s, kl}
	}
	return s
}

type slowSource[K comparable, V any] struct {
	refset.Source[K, V]
	delay time.Duration
}

func (s slowSource[K, V]) Changes(ctx context.Context, after uint64) (uint64, []refset.Delta[K, V], error) {
	head, deltas, err := s.Source.Changes(ctx, after)
	select {
	case <-time.After(s.delay):
	case <-ctx.Done():
		return 0, nil, ctx.Err()
	}
	return head, deltas, err
}

type slowKeyed[K comparable, V any] struct {
	slowSource[K, V]
	refset.KeyLoader[K, V]
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
