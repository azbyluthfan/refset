package refsettest_test

import (
	"context"
	"strings"
	"testing"

	"github.com/azbyluthfan/refset"
	"github.com/azbyluthfan/refset/refsettest"
)

type account struct {
	Name    string
	Tags    []string
	Limits  map[string]int
	owner   *member // unexported, behind a pointer
	members []member
}

type member struct{ Role string }

// recorder is a refsettest.TB that keeps errors instead of failing.
type recorder struct {
	errs     []string
	cleanups []func()
}

func (r *recorder) Helper()                        {}
func (r *recorder) Errorf(format string, a ...any) { r.errs = append(r.errs, format) }
func (r *recorder) Cleanup(f func())               { r.cleanups = append(r.cleanups, f) }
func (r *recorder) finish() {
	for _, f := range r.cleanups {
		f()
	}
}
func (r *recorder) failed() bool { return len(r.errs) > 0 }

func openAccounts(t *testing.T) *refset.Dataset[string, account] {
	t.Helper()
	src := refsettest.NewMemSource[string, account]("accounts")
	src.Upsert("a", account{Name: "A", Tags: []string{"x"}, Limits: map[string]int{"seats": 5},
		owner: &member{"owner"}, members: []member{{"admin"}}})
	src.Upsert("b", account{Name: "B"})
	d, err := refset.Open(context.Background(), src, refset.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	return d
}

func TestDetectValueMutation(t *testing.T) {
	for name, mutate := range map[string]func(a account){
		"slice element":      func(a account) { a.Tags[0] = "changed" },
		"map entry":          func(a account) { a.Limits["seats"] = 99 },
		"unexported pointer": func(a account) { a.owner.Role = "nobody" },
		"slice of structs":   func(a account) { a.members[0].Role = "viewer" },
		"map entry added":    func(a account) { a.Limits["new"] = 1 },
	} {
		t.Run(name, func(t *testing.T) {
			d := openAccounts(t)
			r := &recorder{}
			refsettest.DetectValueMutation(r, d)
			a, _ := d.Get("a")
			mutate(a)
			r.finish()
			if !r.failed() || !strings.Contains(r.errs[0], "were modified") {
				t.Fatalf("mutation not reported: %v", r.errs)
			}
		})
	}

	t.Run("read-only use and copies are fine", func(t *testing.T) {
		d := openAccounts(t)
		r := &recorder{}
		refsettest.DetectValueMutation(r, d)
		a, _ := d.Get("a")
		_ = len(a.Tags) + a.Limits["seats"]
		a.Name = "local copy of the top-level struct" // Get returned a copy of the struct
		tags := append([]string(nil), a.Tags...)
		tags[0] = "changed copy"
		r.finish()
		if r.failed() {
			t.Fatalf("false positive: %v", r.errs)
		}
	})
}
