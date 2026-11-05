package refsethttp_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"

	"github.com/azbyluthfan/refset"
	"github.com/azbyluthfan/refset/refsethttp"
)

// A writer reports the version it produced; the client echoes it; Middleware
// on the replica puts it in the request context for Dataset.GetCtx.
func Example() {
	// Writer side (billing service), after committing version 51 of "plans".
	w := httptest.NewRecorder()
	refsethttp.SetVersion(w, "plans", 51)
	fmt.Println(w.Header().Get("Refset-Version"))

	// Reader side (any replica): the client sent the version back.
	r := httptest.NewRequest(http.MethodGet, "/reports", nil)
	r.Header.Set("Refset-Min-Version", "plans=51")
	refsethttp.Middleware(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		v, _ := refset.MinVersion(r.Context(), "plans")
		fmt.Println("handler must see plans at version", v, "or newer")
		// plan, ok, err := plans.GetCtx(r.Context(), userID)
	})).ServeHTTP(httptest.NewRecorder(), r)

	// Output:
	// plans=51
	// handler must see plans at version 51 or newer
}

// Signing stops clients from forging versions that would make replicas wait.
func ExampleNew() {
	h, err := refsethttp.New(refsethttp.Config{
		SigningKey:   []byte("a random secret of at least 16 bytes"),
		CookiePrefix: "refset_", // browsers carry versions without client code
	})
	if err != nil {
		panic(err)
	}
	mux := http.NewServeMux()
	mux.Handle("/reports", h.Middleware(http.NotFoundHandler()))
	fmt.Println(h.Format(map[string]uint64{"plans": 51})[:9])
	// Output: plans=51~
}
