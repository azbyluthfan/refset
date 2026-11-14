package refset_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"github.com/azbyluthfan/refset"
	"github.com/azbyluthfan/refset/refsettest"
)

type Plan struct {
	Tier string
}

const PlansDataset = "plans"

// Open a dataset and read from it. Here the source is in memory; in production
// it would read from a database or service (see the Source contract).
func Example() {
	src := refsettest.NewMemSource[string, Plan](PlansDataset)
	src.Upsert("user-1", Plan{Tier: "free"})

	plans, err := refset.Open(context.Background(), src, refset.Options{})
	if err != nil {
		panic(err) // source unreachable: don't start with empty data
	}
	defer func() { _ = plans.Close() }()

	p, ok := plans.Get("user-1") // local, lock-free
	fmt.Println(p.Tier, ok, plans.Version())
	// Output: free true 1
}

// Read-your-writes: a writer returns the new version to the client, and the
// next request asks for at least that version on whichever replica it lands.
func ExampleDataset_GetAtLeast() {
	src := refsettest.NewMemSource[string, Plan](PlansDataset)
	src.Upsert("user-1", Plan{Tier: "free"})
	plans, _ := refset.Open(context.Background(), src, refset.Options{WaitBound: time.Second})
	defer func() { _ = plans.Close() }()

	version := src.Upsert("user-1", Plan{Tier: "pro"}) // the billing write

	p, _, err := plans.GetAtLeast(context.Background(), "user-1", version)
	fmt.Println(p.Tier, err)
	// Output: pro <nil>
}

// Fail closed: GetFresh refuses to answer when the replica cannot confirm it
// is current, for example during a network partition.
func ExampleDataset_GetFresh() {
	src := refsettest.NewMemSource[string, Plan](PlansDataset)
	src.Upsert("user-1", Plan{Tier: "pro"})
	clock := refsettest.NewFakeClock(time.Now())
	plans, _ := refset.Open(context.Background(), src, refset.Options{
		PollInterval: time.Second,
		MaxStaleness: 2 * time.Second,
		Clock:        clock,
	})
	defer func() { _ = plans.Close() }()

	src.SetChangesError(errors.New("partitioned"))
	clock.Advance(3 * time.Second)

	_, _, err := plans.GetFresh("user-1")
	fmt.Println(errors.Is(err, refset.ErrTooStale))
	// Output: true
}

// GetForRequest takes the minimum version from the request context, usually set by
// HTTP middleware from a Refset-Min-Version header.
func ExampleContextWithMinVersion() {
	src := refsettest.NewMemSource[string, Plan](PlansDataset)
	plans, _ := refset.Open(context.Background(), src, refset.Options{WaitBound: time.Second})
	defer func() { _ = plans.Close() }()
	version := src.Upsert("user-1", Plan{Tier: "pro"})

	ctx := refset.ContextWithMinVersion(context.Background(), PlansDataset, version)
	p, _, err := plans.GetForRequest(ctx, "user-1")
	fmt.Println(p.Tier, err)
	// Output: pro <nil>
}

// Feed your own structs to refset: give it a function that loads every value
// (your existing code that reads rows, calls an API or parses a file) and a
// function that reports the current version. When the version moves, the
// dataset reloads everything.
func ExampleReloadSource() {
	type Currency struct {
		Code    string
		RateUSD float64
	}

	// Your application's data: structs plus a version you bump on every change.
	var mu sync.Mutex
	version := uint64(1)
	rows := []Currency{{"IDR", 0.000063}, {"EUR", 1.08}}

	currencies, err := refset.Open(context.Background(), &refset.ReloadSource[string, Currency]{
		Dataset: "currencies",
		Load: func(context.Context) (uint64, []Currency, error) {
			mu.Lock()
			defer mu.Unlock()
			return version, slices.Clone(rows), nil
		},
		Version: func(context.Context) (uint64, error) {
			mu.Lock()
			defer mu.Unlock()
			return version, nil
		},
		Key: func(c Currency) string { return c.Code },
	}, refset.Options{})
	if err != nil {
		panic(err)
	}
	defer func() { _ = currencies.Close() }()

	c, _ := currencies.Get("IDR")
	fmt.Println(c.RateUSD)

	// An admin changes a rate: update the structs and bump the version.
	mu.Lock()
	rows[0].RateUSD = 0.000061
	version++
	v := version
	mu.Unlock()

	c, _, _ = currencies.GetAtLeast(context.Background(), "IDR", v)
	fmt.Println(c.RateUSD)
	// Output:
	// 6.3e-05
	// 6.1e-05
}

// Restart from a snapshot saved on local disk: Open serves the file's data at
// once and catches up with the source in the background. GetFresh refuses
// until it has, so fail-closed checks never act on old data.
func ExampleSnapshotFile() {
	dir, err := os.MkdirTemp("", "refset-example-")
	if err != nil {
		panic(err)
	}
	defer func() { _ = os.RemoveAll(dir) }()

	src := refsettest.NewMemSource[string, Plan](PlansDataset)
	src.Upsert("user-1", Plan{Tier: "pro"})
	file := &refset.SnapshotFile[string, Plan]{Source: src, Path: filepath.Join(dir, "plans.snap")}

	// First run: load from the source, save before shutting down.
	plans, err := refset.Open(context.Background(), file, refset.Options{})
	if err != nil {
		panic(err)
	}
	if err := file.Save(plans.Snapshot()); err != nil {
		panic(err)
	}
	_ = plans.Close()

	// Restart: the dataset starts from the file, then catches up.
	plans, err = refset.Open(context.Background(), file, refset.Options{})
	if err != nil {
		panic(err)
	}
	defer func() { _ = plans.Close() }()
	p, _ := plans.Get("user-1")
	fmt.Println(p.Tier, plans.Version(), src.LoadCalls())
	// Output: pro 1 1
}

// Detect changes the source's feed missed: compare each periodic reload with
// the version it replaces, and count the entries that differ although the
// source reported no change. Alert on Stats().Divergences.
func ExampleOptions_reconcile() {
	src := refsettest.NewMemSource[string, Plan](PlansDataset)
	src.Upsert("user-1", Plan{Tier: "free"})
	clock := refsettest.NewFakeClock(time.Date(2026, 11, 14, 9, 0, 0, 0, time.UTC))
	plans, err := refset.Open(context.Background(), src, refset.Options{
		Clock:          clock,
		ReloadInterval: 10 * time.Minute,
		Reconcile:      true,
	})
	if err != nil {
		panic(err)
	}
	defer func() { _ = plans.Close() }()

	// Someone edits the data without the change being logged: same version,
	// different content.
	state := src.State()
	state["user-1"] = Plan{Tier: "pro"}
	src.Restore(src.Head(), state)

	for clock.Timers() < 2 { // the poll and reload timers are armed
		time.Sleep(time.Millisecond)
	}
	clock.Advance(10 * time.Minute) // the periodic reload runs
	for plans.Stats().Reconciles == 0 {
		time.Sleep(time.Millisecond)
	}
	p, _ := plans.Get("user-1")
	fmt.Println(plans.Stats().Divergences, p.Tier)
	// Output: 1 pro
}
