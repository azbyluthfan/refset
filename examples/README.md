# refset examples

Runnable programs showing how to feed refset from different kinds of data, and the problem it solves. This folder is its own Go module, so the dependencies used here (pgx, golang-lru) never reach the refset library.

## Which example should I read?

| If you want to... | Read |
|---|---|
| See the problem refset solves | [`baseline-lru`](baseline-lru/main.go), then [`plan-upgrade`](plan-upgrade/main.go) |
| Use data your app already has as structs | [`structs`](structs/main.go) if it's in memory, [`dbrows`](dbrows/main.go) if you read it with your own SQL, [`httpjson`](httpjson/main.go) if it comes from another service |
| Keep a large Postgres dataset current with per-key changes | [`pgsource`](pgsource/), used by [`currencies`](currencies/main.go) and [`plan-upgrade`](plan-upgrade/main.go) |
| Write your own full `Source` | [`filesource`](filesource/filesource.go) (small and readable), then [`pgsource`](pgsource/) |

**Most applications should start with `structs` or `dbrows`.** They use `refset.ReloadSource`: you hand refset your existing code that loads structs plus a cheap "current version" function, and it reloads the dataset whenever the version moves. That fits most reference data. Reach for a full source like `pgsource` only when a dataset is large and changes often, so that reloading everything on every change costs too much.

## All examples

| Example | What it shows | Needs | Run (from `examples/`) |
|---|---|---|---|
| [`baseline-lru`](baseline-lru/main.go) | A TTL cache keeps serving a stale plan after an upgrade; refset with a version answers correctly | Nothing | `go run ./baseline-lru` |
| [`structs`](structs/main.go) | `ReloadSource` over plans the app holds in a slice; a change is visible at once to a request that asks for its version; `Equal` lets a reload keep the plans that didn't change, as `Stats` shows | Nothing | `go run ./structs` |
| [`httpjson`](httpjson/main.go) | `ReloadSource` over another service's JSON API with a version endpoint; replicas pick up a flag rollout change on their next poll | Nothing | `go run ./httpjson` |
| [`dbrows`](dbrows/main.go) | `ReloadSource` over the app's own SQL that scans rows into structs, plus a one-row version counter the writer bumps in the same transaction; no refset tables or adapter | Postgres | `go run ./dbrows` |
| [`currencies`](currencies/main.go) | A currency table served from memory with `pgsource`; an admin update reaches the replica through `NOTIFY` | Postgres | `go run ./currencies` |
| [`plan-upgrade`](plan-upgrade/main.go) | The full story over real HTTP: billing and two API replicas, with and without the version header, a slow replica falling back to the primary, and a forged version refused | Postgres | `go run ./plan-upgrade` |
| [`pgsource`](pgsource/) | A reusable package: the reference Postgres `Source` with per-key changes, `Write`, a shared listener, compaction, and optional triggers | Postgres (for its tests) | `go test ./pgsource` |
| [`filesource`](filesource/filesource.go) | A reusable package: a full `Source` over a JSON snapshot and an append-only change file, in under 300 lines | Nothing | `go test ./filesource` |

Every program starts with a comment explaining what it does, and prints what happens step by step.

## Running the Postgres examples

They need a Postgres to connect to. From the repository root:

```bash
make pg-up      # starts Postgres 17 in Docker (examples/docker-compose.yml)
```

Then, from `examples/`:

```bash
export REFSET_PG_URL='postgres://refset:refset@localhost:55432/refset?sslmode=disable'
go run ./dbrows
```

The programs use that URL by default, so the `export` is only needed for the tests (`go test ./...`), which skip the Postgres tests when it isn't set. Stop the database with `make pg-down`.

Shortcuts from the repository root:

| Command | Runs |
|---|---|
| `make demo-lru` | `baseline-lru` |
| `make demo` | starts Postgres, then `plan-upgrade` |
| `make examples` | starts Postgres, then vets and tests the whole examples module |

## Using an example in your project

The examples aren't published as a library: `pgsource` and `filesource` are references to copy and adapt. In this repository they build against the library next to them (a `replace` in `go.mod`), so the examples on `main` may use changes that aren't released yet. To work from a release, take them from its tag, for example [v0.1.0](https://github.com/azbyluthfan/refset/tree/v0.1.0/examples).

To use one in your own module:

1. Add refset, and the example's own dependencies (only `pgsource` and the Postgres programs need pgx):

   ```bash
   go get github.com/azbyluthfan/refset@latest
   go get github.com/jackc/pgx/v5
   ```

2. Copy the package directory (for example `pgsource/`) into your module, and change its import path wherever you use it.
3. Keep its tests: they are the quickest way to see your copy still meets the contract after you change it.

Before relying on your own source, check it with the conformance suite:

```go
refsettest.RunConformance(t, func(t *testing.T) refsettest.Harness[string, Plan] { ... })
```

See the [Source contract](../docs/source-contract.md) for what a source must guarantee, and [guarantees](../docs/guarantees.md) for what refset promises in return.
