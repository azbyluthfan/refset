package refsetexpvar_test

import (
	"context"
	"encoding/json"
	"expvar"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/azbyluthfan/refset"
	"github.com/azbyluthfan/refset/refsetexpvar"
	"github.com/azbyluthfan/refset/refsettest"
)

func openPlans(t *testing.T, name string) (*refset.Dataset[string, int], *refsettest.MemSource[string, int]) {
	t.Helper()
	src := refsettest.NewMemSource[string, int](name)
	src.Upsert("a", 1)
	d, err := refset.Open(context.Background(), src, refset.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	return d, src
}

// published decodes the "refset" map as /debug/vars serves it.
func published(t *testing.T) map[string]refset.Stats {
	t.Helper()
	rec := httptest.NewRecorder()
	expvar.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/debug/vars", nil))
	var all map[string]json.RawMessage
	if err := json.Unmarshal(rec.Body.Bytes(), &all); err != nil {
		t.Fatal(err)
	}
	out := map[string]refset.Stats{}
	if raw, ok := all[refsetexpvar.MapName]; ok {
		if err := json.Unmarshal(raw, &out); err != nil {
			t.Fatal(err)
		}
	}
	return out
}

func TestPublish(t *testing.T) {
	d, src := openPlans(t, "plans")
	remove := refsetexpvar.Publish(d)
	st, ok := published(t)["plans"]
	if !ok || st.AppliedVersion != 1 || st.Entries != 1 || st.Name != "plans" {
		t.Fatalf("published stats %+v (present %v)", st, ok)
	}

	// Values are read when served, not when published.
	v := src.Upsert("b", 2)
	if _, _, err := d.GetAtLeast(context.Background(), "b", v); err != nil {
		t.Fatal(err)
	}
	if st := published(t)["plans"]; st.AppliedVersion != 2 || st.Entries != 2 {
		t.Fatalf("stats not current: %+v", st)
	}

	// Publishing the same name again replaces it instead of panicking.
	d2, _ := openPlans(t, "plans")
	remove2 := refsetexpvar.Publish(d2)
	if st := published(t)["plans"]; st.AppliedVersion != 1 {
		t.Fatalf("republished dataset not shown: %+v", st)
	}
	remove2()
	remove()
	if _, ok := published(t)["plans"]; ok {
		t.Fatal("removed dataset still published")
	}
}

func ExamplePublish() {
	src := refsettest.NewMemSource[string, int]("currencies")
	src.Upsert("USD", 2)
	d, err := refset.Open(context.Background(), src, refset.Options{})
	if err != nil {
		panic(err)
	}
	defer func() { _ = d.Close() }()

	remove := refsetexpvar.Publish(d)
	defer remove()

	// Served at /debug/vars with the rest of expvar, e.g.:
	//   "refset": {"currencies": {"Name": "currencies", "AppliedVersion": 1, ...}}
	var st refset.Stats
	_ = json.Unmarshal([]byte(expvar.Get(refsetexpvar.MapName).(*expvar.Map).Get("currencies").String()), &st)
	fmt.Println(st.Name, st.AppliedVersion, st.Entries)
	// Output: currencies 1 1
}
