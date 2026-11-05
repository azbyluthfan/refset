package refsethttp_test

import (
	"context"
	"maps"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/azbyluthfan/refset"
	"github.com/azbyluthfan/refset/refsethttp"
)

func mustNew(t *testing.T, cfg refsethttp.Config) *refsethttp.Handler {
	t.Helper()
	h, err := refsethttp.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return h
}

// captured runs a request through h.Middleware and returns the minimum
// versions the handler saw in its context.
func captured(h *refsethttp.Handler, r *http.Request, datasets ...string) map[string]uint64 {
	got := make(map[string]uint64)
	h.Middleware(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		for _, ds := range datasets {
			if v, ok := refset.MinVersion(r.Context(), ds); ok {
				got[ds] = v
			}
		}
	})).ServeHTTP(httptest.NewRecorder(), r)
	return got
}

// echo copies a writer response's version header into a new request's
// min-version header, as a client would.
func echo(resp *httptest.ResponseRecorder, versionHeader, minHeader string) *http.Request {
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	if v := resp.Header().Get(versionHeader); v != "" {
		r.Header.Set(minHeader, v)
	}
	for _, c := range resp.Result().Cookies() {
		r.AddCookie(c)
	}
	return r
}

func TestRoundTripDefaults(t *testing.T) {
	w := httptest.NewRecorder()
	refsethttp.SetVersion(w, "plans", 51)
	refsethttp.SetVersion(w, "perms", 9)
	refsethttp.SetVersion(w, "plans", 40) // lower: keeps 51
	if got := w.Header().Get("Refset-Version"); got != "perms=9,plans=51" {
		t.Fatalf("header %q", got)
	}
	if len(w.Result().Cookies()) != 0 {
		t.Fatal("cookies set without CookiePrefix")
	}

	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.Header.Set("Refset-Min-Version", w.Header().Get("Refset-Version"))
	var got map[string]uint64
	refsethttp.Middleware(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		got = make(map[string]uint64)
		for _, ds := range []string{"plans", "perms", "flags"} {
			if v, ok := refset.MinVersion(r.Context(), ds); ok {
				got[ds] = v
			}
		}
	})).ServeHTTP(httptest.NewRecorder(), r)
	if want := map[string]uint64{"plans": 51, "perms": 9}; !maps.Equal(got, want) {
		t.Fatalf("context versions %v, want %v", got, want)
	}
}

func TestSetVersions(t *testing.T) {
	w := httptest.NewRecorder()
	refsethttp.SetVersions(w, map[string]uint64{"plans": 51, "perms": 9})
	if got := w.Header().Get("Refset-Version"); got != "perms=9,plans=51" {
		t.Fatalf("header %q", got)
	}
}

func TestSetVersionPanicsOnInvalidDataset(t *testing.T) {
	for _, ds := range []string{"", "a,b", "a=b", "has space", strings.Repeat("x", 65)} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("SetVersion(%q) did not panic", ds)
				}
			}()
			refsethttp.SetVersion(httptest.NewRecorder(), ds, 1)
		}()
	}
}

func TestParseVersions(t *testing.T) {
	tests := []struct {
		in      string
		want    map[string]uint64
		wantErr bool
	}{
		{"", map[string]uint64{}, false},
		{"plans=51", map[string]uint64{"plans": 51}, false},
		{" plans = 51 , perms=9 ,", map[string]uint64{"plans": 51, "perms": 9}, false},
		{"plans=51,plans=60,plans=7", map[string]uint64{"plans": 60}, false},
		{"plans=18446744073709551615", map[string]uint64{"plans": 1<<64 - 1}, false},
		// Malformed entries are skipped; valid ones survive.
		{"plans=51,garbage,perms=9", map[string]uint64{"plans": 51, "perms": 9}, true},
		{"plans=-1", map[string]uint64{}, true},
		{"plans=+5", map[string]uint64{}, true},
		{"plans=0x10", map[string]uint64{}, true},
		{"plans=18446744073709551616", map[string]uint64{}, true},
		{"plans=", map[string]uint64{}, true},
		{"=5", map[string]uint64{}, true},
		{"pla ns=5", map[string]uint64{}, true},
		{"plans=5~sig", map[string]uint64{"plans": 5}, false}, // no keys: signature not checked
	}
	for _, tt := range tests {
		got, err := refsethttp.ParseVersions(tt.in)
		if !maps.Equal(got, tt.want) || (err != nil) != tt.wantErr {
			t.Errorf("ParseVersions(%q) = %v, %v; want %v, err=%v", tt.in, got, err, tt.want, tt.wantErr)
		}
	}
}

func TestOversizedHeadersIgnored(t *testing.T) {
	long := "plans=1," + strings.Repeat("x", 5000) + "=1"
	if got, err := refsethttp.ParseVersions(long); len(got) != 0 || err == nil {
		t.Fatalf("oversized header: %v, %v", got, err)
	}
	var many []string
	for i := range 40 {
		many = append(many, "ds"+string(rune('a'+i%26))+string(rune('a'+i/26))+"=1")
	}
	got, err := refsethttp.ParseVersions(strings.Join(many, ","))
	if len(got) != 32 || err == nil {
		t.Fatalf("too many entries: kept %d, err %v", len(got), err)
	}
}

// Malformed input never fails the request: the handler always runs.
func TestMiddlewareNeverRejects(t *testing.T) {
	for _, hv := range []string{"%%%", "plans=abc", strings.Repeat("a=1,", 2000), "plans=1~bad"} {
		r := httptest.NewRequest(http.MethodGet, "/", nil)
		r.Header.Set("Refset-Min-Version", hv)
		w := httptest.NewRecorder()
		ran := false
		refsethttp.Middleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			ran = true
			w.WriteHeader(http.StatusTeapot)
		})).ServeHTTP(w, r)
		if !ran || w.Code != http.StatusTeapot {
			t.Fatalf("header %.20q: handler ran=%v code=%d", hv, ran, w.Code)
		}
	}
}

func TestMultipleHeaderLines(t *testing.T) {
	h := mustNew(t, refsethttp.Config{})
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.Header.Add("Refset-Min-Version", "plans=51")
	r.Header.Add("Refset-Min-Version", "perms=9")
	if got := captured(h, r, "plans", "perms"); !maps.Equal(got, map[string]uint64{"plans": 51, "perms": 9}) {
		t.Fatalf("got %v", got)
	}
}

func TestCustomHeaderNames(t *testing.T) {
	h := mustNew(t, refsethttp.Config{VersionHeader: "X-Data-Version", MinVersionHeader: "X-Data-Min"})
	w := httptest.NewRecorder()
	h.SetVersion(w, "plans", 3)
	if w.Header().Get("X-Data-Version") != "plans=3" || w.Header().Get("Refset-Version") != "" {
		t.Fatalf("headers %v", w.Header())
	}
	r := echo(w, "X-Data-Version", "X-Data-Min")
	if got := captured(h, r, "plans"); got["plans"] != 3 {
		t.Fatalf("got %v", got)
	}
}

func TestCookies(t *testing.T) {
	h := mustNew(t, refsethttp.Config{CookiePrefix: "refset_", CookieMaxAge: time.Minute})
	w := httptest.NewRecorder()
	h.SetVersion(w, "plans", 51)
	h.SetVersion(w, "perms", 9)

	cookies := w.Result().Cookies()
	byName := make(map[string]*http.Cookie)
	for _, c := range cookies {
		byName[c.Name] = c
	}
	c := byName["refset_plans"]
	if c == nil || c.Value != "51" || !c.HttpOnly || !c.Secure || c.SameSite != http.SameSiteLaxMode || c.MaxAge != 60 || c.Path != "/" {
		t.Fatalf("plans cookie %+v", c)
	}
	if byName["refset_perms"] == nil {
		t.Fatal("no perms cookie")
	}

	// A browser sends the cookies back with no header at all.
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	for _, c := range cookies {
		r.AddCookie(c)
	}
	r.AddCookie(&http.Cookie{Name: "refset_flags", Value: "junk"}) // ignored
	r.AddCookie(&http.Cookie{Name: "session", Value: "abc"})       // not ours
	if got := captured(h, r, "plans", "perms", "flags"); !maps.Equal(got, map[string]uint64{"plans": 51, "perms": 9}) {
		t.Fatalf("got %v", got)
	}
	// Header and cookie for the same dataset: the higher wins.
	r.Header.Set("Refset-Min-Version", "plans=60")
	if got := captured(h, r, "plans"); got["plans"] != 60 {
		t.Fatalf("got %v", got)
	}

	insecure := mustNew(t, refsethttp.Config{CookiePrefix: "v_", CookieInsecure: true})
	w = httptest.NewRecorder()
	insecure.SetVersion(w, "plans", 1)
	if w.Result().Cookies()[0].Secure {
		t.Fatal("CookieInsecure ignored")
	}
}

var key1 = []byte("0123456789abcdef0123456789abcdef")
var key2 = []byte("fedcba9876543210fedcba9876543210")

func TestSigning(t *testing.T) {
	h := mustNew(t, refsethttp.Config{SigningKey: key1})
	w := httptest.NewRecorder()
	h.SetVersion(w, "plans", 51)
	header := w.Header().Get("Refset-Version")
	if !strings.HasPrefix(header, "plans=51~") {
		t.Fatalf("not signed: %q", header)
	}

	check := func(name, value string, want map[string]uint64) {
		t.Helper()
		r := httptest.NewRequest(http.MethodGet, "/", nil)
		r.Header.Set("Refset-Min-Version", value)
		if got := captured(h, r, "plans", "perms"); !maps.Equal(got, want) {
			t.Errorf("%s: got %v, want %v", name, got, want)
		}
	}
	sig := header[strings.Index(header, "~")+1:]
	check("genuine", header, map[string]uint64{"plans": 51})
	check("unsigned forged", "plans=999999999", map[string]uint64{})
	check("signature moved to another number", "plans=52~"+sig, map[string]uint64{})
	check("signature moved to another dataset", "perms=51~"+sig, map[string]uint64{})
	check("garbage signature", "plans=51~AAAA", map[string]uint64{})

	other := mustNew(t, refsethttp.Config{SigningKey: key2})
	w2 := httptest.NewRecorder()
	other.SetVersion(w2, "plans", 51)
	check("signed with an unknown key", w2.Header().Get("Refset-Version"), map[string]uint64{})

	// Merging into an existing signed header re-signs everything.
	h.SetVersion(w, "perms", 9)
	check("merged header", w.Header().Get("Refset-Version"), map[string]uint64{"plans": 51, "perms": 9})
}

func TestKeyRotation(t *testing.T) {
	old := mustNew(t, refsethttp.Config{SigningKey: key1})
	rotated := mustNew(t, refsethttp.Config{SigningKey: key2, VerifyKeys: [][]byte{key1}})
	w := httptest.NewRecorder()
	old.SetVersion(w, "plans", 7)
	r := echo(w, "Refset-Version", "Refset-Min-Version")
	if got := captured(rotated, r, "plans"); got["plans"] != 7 {
		t.Fatalf("old key not accepted during rotation: %v", got)
	}
}

func TestSignedCookies(t *testing.T) {
	h := mustNew(t, refsethttp.Config{SigningKey: key1, CookiePrefix: "refset_"})
	w := httptest.NewRecorder()
	h.SetVersion(w, "plans", 51)
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	for _, c := range w.Result().Cookies() {
		r.AddCookie(c)
	}
	r.AddCookie(&http.Cookie{Name: "refset_perms", Value: "999"}) // forged, unsigned
	got, err := h.MinVersions(r)
	if !maps.Equal(got, map[string]uint64{"plans": 51}) || err == nil {
		t.Fatalf("got %v, err %v", got, err)
	}
}

func TestNewRejectsInvalidConfig(t *testing.T) {
	bad := []refsethttp.Config{
		{VersionHeader: "Bad Header"},
		{MinVersionHeader: "x:y"},
		{CookiePrefix: "a;b"},
		{CookieMaxAge: time.Millisecond},
		{SigningKey: []byte("short")},
		{SigningKey: key1, VerifyKeys: [][]byte{[]byte("short")}},
		{VerifyKeys: [][]byte{key1}},
	}
	for _, cfg := range bad {
		if _, err := refsethttp.New(cfg); err == nil {
			t.Errorf("New(%+v) accepted", cfg)
		}
	}
}

// The parser must never panic and must only return valid, bounded results.
func FuzzParse(f *testing.F) {
	for _, s := range []string{"plans=51", "a=1,b=2", "plans=51~abc", " , = ,", "x=18446744073709551616"} {
		f.Add(s)
	}
	h, _ := refsethttp.New(refsethttp.Config{SigningKey: key1})
	f.Fuzz(func(t *testing.T, s string) {
		for _, p := range []*refsethttp.Handler{nil, h} {
			var got map[string]uint64
			if p == nil {
				got, _ = refsethttp.ParseVersions(s)
			} else {
				got, _ = p.Parse(s)
			}
			if len(got) > 32 {
				t.Fatalf("%d entries", len(got))
			}
			for ds := range got {
				if ds == "" || len(ds) > 64 || strings.ContainsAny(ds, " ,=~\t") {
					t.Fatalf("invalid dataset name %q", ds)
				}
			}
		}
	})
}

func TestContextIsolation(t *testing.T) {
	// Versions from one request never leak into another's context.
	h := mustNew(t, refsethttp.Config{})
	r1 := httptest.NewRequest(http.MethodGet, "/", nil)
	r1.Header.Set("Refset-Min-Version", "plans=5")
	_ = captured(h, r1, "plans")
	if _, ok := refset.MinVersion(context.Background(), "plans"); ok {
		t.Fatal("leaked into background context")
	}
	r2 := httptest.NewRequest(http.MethodGet, "/", nil)
	if got := captured(h, r2, "plans"); len(got) != 0 {
		t.Fatalf("second request saw %v", got)
	}
}
