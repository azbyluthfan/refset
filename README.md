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

## Usage

```bash
go get github.com/azbyluthfan/refset
```

**Reading** (every API replica):

```go
plans, err := refset.Open(ctx, plansSource, refset.Options{})
if err != nil {
    log.Fatal(err) // source unreachable: don't start with empty data
}
defer plans.Close()

plan, ok := plans.Get(userID)                    // local, lock-free, never waits
plan, ok, err = plans.GetCtx(r.Context(), userID) // waits for the version the request carries
perms, ok, err := permissions.GetFresh(userID)    // ErrTooStale if this replica can't confirm it's current
```

**Carrying versions over HTTP** (`refsethttp`, standard library only):

```go
// Writer (billing), after committing:
refsethttp.SetVersion(w, "plans", version) // Refset-Version: plans=51

// Every replica:
mux.Handle("/reports", refsethttp.Middleware(reportsHandler))
```

Browsers can carry versions in cookies instead, and versions can be HMAC-signed so clients can't forge them: see `refsethttp.Config`.

**Sources.** A source implements four methods: `Name`, `LoadSnapshot`, `Changes` and `Watch` (plus optional `LoadKey` for the fallback). What they must guarantee is the [Source contract](docs/source-contract.md); `refsettest.RunConformance` checks any implementation. Two examples ship in [examples/](examples):

- [`pgsource`](examples/pgsource): PostgreSQL. Writers use `pgsource.Write`, which versions and logs each change in the same transaction; replicas re-read changed rows at exactly the logged version; `NOTIFY` wakes them up. Optional triggers capture writes made with plain SQL.
- [`filesource`](examples/filesource): a JSON snapshot and an append-only change file.

## How it works

- The dataset is split into shards held behind one atomic pointer. A read loads the pointer and looks up one map: no locks, no allocation.
- A background loop asks the source for changes when notified, on a poll interval, or at once when a request needs a version it hasn't seen. Concurrent requests share one poll. Each batch copies only the shards it touches and swaps the pointer, so readers see a whole batch or none of it.
- Staleness is measured on the replica's own clocks (the larger of monotonic and wall-clock time), so it works when the replica is cut off, and every clock problem fails closed.

The full design is in [docs/design.md](docs/design.md).

## When not to use refset

refset fits data that is read on most requests, changed by people or admin tools, and the same for every user: catalogs, prices, plans, permissions, feature flags, tax rules, seat maps.

It does not fit carts, stock counts, seat holds, reservations or sessions: data that is written constantly, belongs to one user, or needs an atomic check-and-set at the database. In the same product, keep those in the database and put the reference data in refset.

## Operating refset

- The dataset lives in memory: a restarted process reloads it in `Open` and reports ready only after that. Roll restarts gradually.
- A reload holds two copies of the dataset while it swaps.
- Values are shared between readers: treat them as read-only.
- Keep a real `PollInterval`; set `MaxStaleness` to at least 2× it for fail-closed reads.
- Keep values small or store pointers to large ones, and raise `Options.Shards` above ~100k entries (about 1024 at 100k, 4096 at 1M).
- Every change to the source must be recorded in its change feed. Changes it never reports go unnoticed until the next reload (`Options.ReloadInterval` bounds that).

Details: [guarantees](docs/guarantees.md) · [performance and memory](docs/performance.md) · [benchmarks](docs/benchmarks.md).

## Status

Experimental, heading for v0.1.0. The API may change before v1.0. See the [CHANGELOG](CHANGELOG.md).

## Documentation

- [Design](docs/design.md): goals, architecture, API and test plan
- [Guarantees](docs/guarantees.md): what is and isn't promised, staleness and memory behavior
- [Source contract](docs/source-contract.md): what a `Source` must guarantee
- [Performance and memory](docs/performance.md) and [benchmarks](docs/benchmarks.md)

## Contributing

Contributions are welcome. Please read [CONTRIBUTING.md](CONTRIBUTING.md); commits must be signed off under the [Developer Certificate of Origin](https://developercertificate.org/) (`git commit -s`).

## License

[Apache License 2.0](LICENSE)
