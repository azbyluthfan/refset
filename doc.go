// Package refset keeps a whole reference dataset (plans, permissions, prices,
// feature flags) in the memory of every replica, coherent with its source of
// truth, with dataset-level versions that let a request demand "at least
// version N".
//
// Reads are local and lock-free. Changes arrive in the background through a
// [Source] that the application implements; refset itself has no knowledge of
// any database, cache, broker or network protocol, and depends only on the Go
// standard library.
//
// # Getting started
//
// The simplest source is [ReloadSource]: your existing code that loads
// structs, plus a cheap function that returns a version bumped on every
// change. The dataset reloads whenever the version moves:
//
//	plans, err := refset.Open(ctx, &refset.ReloadSource[string, Plan]{
//		Dataset: "plans",
//		Load:    store.LoadPlans, // func(ctx) (version uint64, []Plan, error)
//		Version: store.Version,   // func(ctx) (uint64, error)
//		Key:     func(p Plan) string { return p.ID },
//	}, refset.Options{})
//	if err != nil {
//		return err // source unreachable: don't start with empty data
//	}
//	defer plans.Close()
//
//	plan, ok := plans.Get(userID)                     // local, never waits
//	plan, ok, err = plans.GetAtLeast(ctx, userID, v)  // waits for version v
//	plan, ok, err = plans.GetFresh(userID)            // fails closed when stale
//
// For large datasets that change often, implement [Source] with per-key
// changes. Package refsethttp carries versions between services for
// read-your-writes, package refsettest checks a Source against the contract
// and helps test code that reads datasets, and package refsetexpvar
// publishes Stats at /debug/vars.
//
// The API is experimental until v1.0: a minor release may change it.
// Documentation: the README (https://github.com/azbyluthfan/refset), the
// guarantees (https://github.com/azbyluthfan/refset/blob/main/docs/guarantees.md)
// and the Source contract
// (https://github.com/azbyluthfan/refset/blob/main/docs/source-contract.md).
package refset
