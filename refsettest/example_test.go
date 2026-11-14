package refsettest_test

import (
	"context"
	"fmt"

	"github.com/azbyluthfan/refset"
	"github.com/azbyluthfan/refset/refsettest"
)

// printTB stands in for *testing.T so the example can show the failure.
type printTB struct{ cleanups []func() }

func (*printTB) Helper()                        {}
func (*printTB) Errorf(format string, a ...any) { fmt.Println("test failed: values were modified") }
func (p *printTB) Cleanup(f func())             { p.cleanups = append(p.cleanups, f) }

// In a test, DetectValueMutation fails the test if the code under test
// changes a value it read: values are shared by every reader.
func ExampleDetectValueMutation() {
	type Plan struct{ Features []string }
	src := refsettest.NewMemSource[string, Plan]("plans")
	src.Upsert("pro", Plan{Features: []string{"reports"}})
	plans, err := refset.Open(context.Background(), src, refset.Options{})
	if err != nil {
		panic(err)
	}
	defer func() { _ = plans.Close() }()

	t := &printTB{} // in a real test: refsettest.DetectValueMutation(t, plans) with your *testing.T
	refsettest.DetectValueMutation(t, plans)

	// Code under test: a bug that edits a shared value in place.
	p, _ := plans.Get("pro")
	p.Features[0] = "everything"

	for _, f := range t.cleanups { // the testing package runs these at the end
		f()
	}
	// Output: test failed: values were modified
}
