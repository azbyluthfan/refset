# refset

[![CI](https://github.com/azbyluthfan/refset/actions/workflows/ci.yml/badge.svg)](https://github.com/azbyluthfan/refset/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/azbyluthfan/refset.svg)](https://pkg.go.dev/github.com/azbyluthfan/refset)
[![Status: experimental](https://img.shields.io/badge/status-experimental-orange)](#status)
[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue)](LICENSE)

**Versioned, replicated reference data for Go.** refset keeps a whole reference dataset (plans, permissions, prices, feature flags) in the memory of every replica, coherent with its source of truth, with dataset-level versions that let a request demand "at least version N."

- **In-process, zero dependencies.** The library uses only the Go standard library. Reads are local and lock-free, and scale across cores: on an M1, ~25 ns per read on one core for 10k entries, and 8 cores together read every ~5 ns, where a `sync.RWMutex` map stays near 87 ns ([benchmarks](docs/benchmarks.md)).
- **Bring your own source.** Changes arrive through a small `Source` interface over any store with a change log. A Postgres implementation is included as an example.
- **Read-your-writes across replicas.** A writer hands the client the new version; whichever replica gets the next request waits for that version, or falls back to the source, instead of answering with stale data.
- **Fails closed when cut off.** `GetFresh` refuses to answer once a replica can't confirm it is current, for permission checks and revocations.

## The problem

A customer upgrades from Free to Pro. Billing writes the new plan and redirects them. Their next request lands on API replica B, which caches plans in memory and hasn't caught up yet.

| | Replica B answers |
|---|---|
| TTL cache (golang-lru, otter, ...) | **"Free"** until the entry expires. A long TTL means long staleness; a short one means database load |
| refset, request without a version | "Free", same as the cache |
| refset, request carrying `Refset-Min-Version: plans=51` | **"Pro"**: B waits a few milliseconds for version 51, or asks the database once |

See it for yourself:

```bash
make demo-lru   # the TTL cache vs refset, no database needed
make demo       # billing + two API replicas over real HTTP and Postgres (needs Docker)
```

```
1. user-1 upgrades from Free to Pro (POST billing/upgrade)
   billing commits and returns   Refset-Version: plans=2
2. next request lands on replica B WITHOUT the version (what a TTL cache does)
   replica B  (no version)                    -> 403 upgrade required (plan: free)  in 0s
3. same request WITH Refset-Min-Version: B waits for that version
   replica B  Refset-Min-Version: plans=2     -> 200 advanced report               in 46ms
4. user-2 upgrades; replica C is far too slow, so it falls back to the primary
   replica C  Refset-Min-Version: plans=3     -> 200 advanced report               in 58ms
```

More examples, and which one to read for your situation: [examples/README.md](examples/README.md).

## Usage

Requires Go 1.26 or newer. Install the latest release:

```bash
go get github.com/azbyluthfan/refset@latest
```

| Package | Import path | For |
|---|---|---|
| `refset` | `github.com/azbyluthfan/refset` | datasets, sources, version-aware reads |
| `refsethttp` | `github.com/azbyluthfan/refset/refsethttp` | carrying versions over HTTP (headers, cookies, signing) |
| `refsettest` | `github.com/azbyluthfan/refset/refsettest` | tests: in-memory source with faults, fake clock, `RunConformance` |

All three are in one module with no dependencies. While refset is at v0.x, a minor release may change the API; the [CHANGELOG](CHANGELOG.md) says what to change.

**Quick start.** A complete program: your own code holds plans and a version, and refset serves them from memory.

```go
package main

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/azbyluthfan/refset"
)

type Plan struct{ ID, Name string }

// store is your own code: plans plus a version bumped on every change.
type store struct {
	mu      sync.Mutex
	version uint64
	plans   []Plan
}

func (s *store) Load(context.Context) (uint64, []Plan, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.version, append([]Plan(nil), s.plans...), nil
}

func (s *store) Version(context.Context) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.version, nil
}

func (s *store) Set(p Plan) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.plans {
		if s.plans[i].ID == p.ID {
			s.plans[i] = p
		}
	}
	s.version++
	return s.version
}

func main() {
	ctx := context.Background()
	s := &store{version: 1, plans: []Plan{{"u1", "Free"}}}

	plans, err := refset.Open(ctx, &refset.ReloadSource[string, Plan]{
		Dataset: "plans",
		Load:    s.Load,
		Version: s.Version,
		Key:     func(p Plan) string { return p.ID },
	}, refset.Options{})
	if err != nil {
		log.Fatal(err) // source unreachable: don't start with empty data
	}
	defer plans.Close()

	p, _ := plans.Get("u1")
	fmt.Println("before:", p.Name) // Free

	v := s.Set(Plan{"u1", "Pro"})
	// A request that carries the writer's version waits for it.
	p, _, err = plans.GetAtLeast(ctx, "u1", v)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("after:", p.Name) // Pro
}
```

The sections below show each part in a real service.

**Reading** (every API replica):

```go
plans, err := refset.Open(ctx, plansSource, refset.Options{})
if err != nil {
    log.Fatal(err) // source unreachable: don't start with empty data
}
defer plans.Close()

plan, ok := plans.Get(userID)                    // local, lock-free, never waits
plan, ok, err = plans.GetForRequest(r.Context(), userID) // waits for the version the request carries
perms, ok, err := permissions.GetFresh(userID)    // ErrTooStale if this replica can't confirm it's current
```

**Carrying versions over HTTP** (`refsethttp`, standard library only):

```go
import "github.com/azbyluthfan/refset/refsethttp"

// Writer (billing), after committing:
refsethttp.SetVersion(w, "plans", version) // Refset-Version: plans=51

// Every replica:
mux.Handle("/reports", refsethttp.Middleware(reportsHandler))
```

Browsers can carry versions in cookies instead, and versions can be HMAC-signed so clients can't forge them: see `refsethttp.Config`.

**Your own data.** refset doesn't talk to databases itself. You tell it where the data comes from. The simplest way is `refset.ReloadSource`: give it your existing code that loads structs, plus a cheap "current version" function, and it reloads the dataset whenever the version moves:

```go
plans, err := refset.Open(ctx, &refset.ReloadSource[string, Plan]{
    Dataset: "plans",
    Load:    store.LoadPlans,  // func(ctx) (version uint64, []Plan, error): your query, API call or file
    Version: store.Version,    // func(ctx) (uint64, error): bumped on every change
    Key:     func(p Plan) string { return p.ID },
}, refset.Options{})
```

Bump the version in the same transaction (or under the same lock) as each change.

Every change reloads the whole dataset, which suits most reference data. For large datasets that change often, implement the full `Source` interface (`Name`, `LoadSnapshot`, `Changes`, `Watch`, optional `LoadKey`) with per-key changes, as the Postgres example does. What any source must guarantee is the [Source contract](docs/source-contract.md); `refsettest.RunConformance` checks an implementation.

**Examples:** [examples/README.md](examples/README.md) says which one to read for your situation: structs in memory, your own SQL, another service's API, Postgres with per-key changes, or writing your own source. The examples on `main` track the latest code; to read them as they were in a release, browse that tag (for example [v0.1.0](https://github.com/azbyluthfan/refset/tree/v0.1.0/examples)).

## How it works

- The dataset is split into shards held behind one atomic pointer. A read loads the pointer and looks up one map: no locks, no allocation.
- A background loop asks the source for changes when notified, on a poll interval, or at once when a request needs a version it hasn't seen. Concurrent requests share one poll. Each batch copies only the shards it touches and swaps the pointer, so readers see a whole batch or none of it.
- Staleness is measured on the replica's own clocks (the larger of monotonic and wall-clock time), so it works when the replica is cut off, and every clock problem fails closed.

The full design is in [docs/design.md](docs/design.md).

## When not to use refset

refset fits data that is read on most requests, changed by people or admin tools, and the same for every user: catalogs, prices, plans, permissions, feature flags, tax rules, seat maps.

It does not fit carts, stock counts, seat holds, reservations or sessions: data that is written constantly, belongs to one user, or needs an atomic check-and-set at the database. In the same product, keep those in the database and put the reference data in refset.

## Operating refset

- The dataset lives in memory: a restarted process reloads it in `Open` and reports ready only after that. Roll restarts gradually. To restart from local disk instead, wrap the source in `refset.SnapshotFile` and `Save` on shutdown ([restart example](examples/restart/main.go)).
- A reload holds two copies of the dataset while it swaps.
- Values are shared between readers: treat them as read-only.
- Keep a real `PollInterval`; set `MaxStaleness` to at least 2× it for fail-closed reads.
- Keep values small or store pointers to large ones. The shard count follows the dataset's size; see [performance](docs/performance.md).
- Every change to the source must be recorded in its change feed. Changes it never reports go unnoticed until the next reload (`Options.ReloadInterval` bounds that).

Details: [guarantees](docs/guarantees.md) · [performance and memory](docs/performance.md) · [benchmarks](docs/benchmarks.md).

## Status

Experimental. The latest release is v0.2.0; the API may change before v1.0. See the [CHANGELOG](CHANGELOG.md) and the [roadmap](docs/roadmap.md).

## Documentation

- [Design](docs/design.md): goals, architecture, API and test plan
- [Guarantees](docs/guarantees.md): what is and isn't promised, staleness and memory behavior
- [Source contract](docs/source-contract.md): what a `Source` must guarantee
- [Performance and memory](docs/performance.md) and [benchmarks](docs/benchmarks.md)
- [Security model](docs/security.md): trust boundaries, threats and how they're handled
- [Roadmap](docs/roadmap.md): what comes after v0.1.0

## Contributing

Contributions are welcome. Please read [CONTRIBUTING.md](CONTRIBUTING.md); commits must be signed off under the [Developer Certificate of Origin](https://developercertificate.org/) (`git commit -s`).

## License

[Apache License 2.0](LICENSE)
