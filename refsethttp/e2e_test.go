package refsethttp_test

import (
	"context"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/azbyluthfan/refset"
	"github.com/azbyluthfan/refset/refsethttp"
	"github.com/azbyluthfan/refset/refsettest"
)

const plansDataset = "plans"

// planApp is the design's §6.2 plan-upgrade story over real HTTP: a billing
// service that writes, and an API replica whose source is slow to deliver.
type planApp struct {
	src     *refsettest.MemSource[string, string]
	billing *httptest.Server
	replica *httptest.Server
}

func newPlanApp(t *testing.T, h *refsethttp.Handler, replicaDelay time.Duration) *planApp {
	t.Helper()
	src := refsettest.NewMemSource[string, string](plansDataset)
	src.Upsert("user-1", "free")

	view := src.NewReplica()
	view.SetDropNotifications(true) // B hasn't heard about the upgrade
	view.SetDelay(replicaDelay)
	plans, err := refset.Open(context.Background(), view, refset.Options{PollInterval: time.Hour, WaitBound: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = plans.Close() })

	billing := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		v := src.Upsert("user-1", "pro")
		h.SetVersion(w, plansDataset, v)
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(billing.Close)

	replica := httptest.NewServer(h.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tier, ok, err := plans.GetCtx(r.Context(), "user-1")
		switch {
		case err != nil || !ok:
			http.Error(w, "plan unavailable", http.StatusServiceUnavailable)
		case tier != "pro":
			http.Error(w, "upgrade required", http.StatusForbidden)
		default:
			_, _ = io.WriteString(w, "advanced report")
		}
	})))
	t.Cleanup(replica.Close)
	return &planApp{src: src, billing: billing, replica: replica}
}

func status(t *testing.T, c *http.Client, req *http.Request) int {
	t.Helper()
	resp, err := c.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode
}

func upgrade(t *testing.T, c *http.Client, app *planApp) *http.Response {
	t.Helper()
	resp, err := c.Post(app.billing.URL, "text/plain", nil)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	return resp
}

// The headline story, over HTTP: after upgrading, a client that echoes the
// version gets "pro" from a replica that hadn't heard of the change; one that
// doesn't gets the stale "free".
func TestPlanUpgradeOverHTTP(t *testing.T) {
	h := mustNew(t, refsethttp.Config{})
	app := newPlanApp(t, h, 20*time.Millisecond)
	c := app.billing.Client()

	resp := upgrade(t, c, app)
	version := resp.Header.Get("Refset-Version")
	if version == "" {
		t.Fatal("billing returned no version")
	}

	without, _ := http.NewRequest(http.MethodGet, app.replica.URL, nil)
	if got := status(t, c, without); got != http.StatusForbidden {
		t.Fatalf("without version: %d, want 403 (stale free plan)", got)
	}
	with, _ := http.NewRequest(http.MethodGet, app.replica.URL, nil)
	with.Header.Set("Refset-Min-Version", version)
	if got := status(t, c, with); got != http.StatusOK {
		t.Fatalf("with version: %d, want 200 (pro)", got)
	}
}

// Browsers: the cookie set by billing is sent to the replica automatically.
func TestPlanUpgradeOverHTTPWithCookies(t *testing.T) {
	h := mustNew(t, refsethttp.Config{CookiePrefix: "refset_", CookieInsecure: true, SigningKey: key1})
	app := newPlanApp(t, h, 20*time.Millisecond)
	jar, _ := cookiejar.New(nil)
	c := &http.Client{Jar: jar}

	upgrade(t, c, app)
	// httptest servers share a host (127.0.0.1), so the jar sends billing's
	// cookie to the replica, as a browser would for one site.
	req, _ := http.NewRequest(http.MethodGet, app.replica.URL, nil)
	if got := status(t, c, req); got != http.StatusOK {
		t.Fatalf("with cookie: %d, want 200 (pro)", got)
	}
}

// With signing on, a forged version is ignored: the request is served from
// memory at once instead of making the replica poll or wait.
func TestForgedVersionIgnoredWithSigning(t *testing.T) {
	h := mustNew(t, refsethttp.Config{SigningKey: key1})
	app := newPlanApp(t, h, 500*time.Millisecond)
	c := app.billing.Client()

	req, _ := http.NewRequest(http.MethodGet, app.replica.URL, nil)
	req.Header.Set("Refset-Min-Version", "plans=999999999")
	start := time.Now()
	if got := status(t, c, req); got != http.StatusForbidden {
		t.Fatalf("forged version: %d, want 403 from memory", got)
	}
	if took := time.Since(start); took > 300*time.Millisecond {
		t.Fatalf("forged version made the replica wait %v", took)
	}
}
