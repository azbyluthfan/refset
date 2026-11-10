# Roadmap

Where refset is going after the first release. The reasoning behind the design is in [design.md](design.md). Measurements referred to here are in [benchmarks.md](benchmarks.md).

Nothing here is a promise of dates. Items move up when users ask for them; open an issue if one matters to you.

## Where things stand (2026-11-11)

| Area | State |
|---|---|
| Repository, CI, Source contract | Done |
| Core library (`Dataset`, copy-on-write shards, waiters, staleness) | Done |
| Test kit (`refsettest`, conformance suite, in-process scenarios) | Done |
| HTTP propagation (`refsethttp`) | Done |
| Examples (`pgsource`, `filesource`, demos) | Done |
| Docs, benchmarks, 1-hour soak | Done |
| `ReloadSource` and the `structs` / `dbrows` / `httpjson` examples | Done |
| Pipelines: CI, examples, weekly (soak, fuzzing, govulncheck, newest Go and Postgres), release | Done, green on GitHub |
| **v0.1.0 release** | **Next:** tag, announce |

## v0.1.0: first release

- ~~Confirm `ci.yml` and `examples.yml` are green on GitHub for `main`.~~ Done 2026-11-11, along with a first run of `weekly.yml`.
- Tag `v0.1.0`; the release workflow checks the tag, runs the full suite, publishes the CHANGELOG section and the module proxy picks it up ([releasing.md](releasing.md)).
- Announce on the Go forum, r/golang and Gophers Slack.

## Performance track

refset trades along three axes, and an improvement on one often costs another:

- **Latency:** how long a read takes (mean and tail), and how long a committed change takes to become visible on a replica.
- **GC time:** the CPU the garbage collector spends because of refset, both scanning the live dataset and collecting the shards each change throws away. Pause times are only part of this.
- **Memory footprint:** bytes per entry in steady state, and the peak during a reload (two full copies today).

### Known today

| Axis | Measured | Gap |
|---|---|---|
| Read latency | 24 ns serial / 4.8 ns parallel at 10k entries; 136 ns serial at 1M | Each read hashes the key twice: 8–36 ns slower than a locked map on one core. Only means are measured, no p99 |
| Propagation latency | Not measured | Every notification waits `BatchWindow` (5 ms) before polling, even when nothing else is arriving |
| GC time | Pause p50 23 µs, max 44 µs at 1M entries, idle | GC CPU share, mark work and assist time are not measured, and nothing is measured while changes are applied or during a reload |
| Garbage per change | 221 KB for 1 key at 1M entries with the default 256 shards (50 KB with 4096) | Default shard count is about 10× too low at 1M entries |
| Memory | 101 B per entry, same as a plain map | A reload holds two full copies. `ReloadSource` reloads everything on *every* change, so each change costs a full copy of garbage and a 2× peak |

### Phase P0: measure all three axes (before changing anything)

The current benchmarks report means and idle GC pauses. Optimising GC time or tail latency needs numbers for them first.

- **Tail read latency:** p50 / p99 / p99.9 of `Get` while the loop applies changes and while a reload swaps in, at 10k, 100k and 1M entries.
- **Propagation latency:** commit → visible on a replica, with notifications and with polling only.
- **GC cost under churn:** GC CPU share (from `runtime/metrics`), GC cycles per second, mark assist time and pauses, at 0, 10, 100 and 1,000 changes per second, and during a reload. Compared with a locked map receiving the same writes.
- **Memory:** steady live heap, peak heap during a reload, bytes per entry for a few key and value types.
- Extend `bench/memreport` into a `bench/perfreport` that prints all of the above. Library code stays stdlib-only.
- Add a before-release comparison against the last release's numbers (benchstat in the `bench/` module), so regressions are visible.

**Exit:** `docs/benchmarks.md` has a table per axis, and every later item below states its effect in those tables.

### Phase P1: quick wins (v0.2)

| Item | Latency | GC time | Memory | Notes |
|---|---|---|---|---|
| **Automatic shard count** at `Open`: ≈ 2.5 × √entries, rounded to a power of two; `Options.Shards` stays as an override | apply 10× faster at 1M | 14× less garbage per change at 1M | 0 | Already measured (100-key batch at 1M: 2.6 ms → 263 µs, 18 MB → 1.3 MB). Re-size on reload if the dataset has grown a lot |
| **Adaptive batch window**: apply the first notification at once, batch only when changes keep arriving | up to −5 ms propagation | slightly more applies under bursts | 0 | Keeps batching where it saves work, removes the fixed delay where it doesn't |
| **Diff-on-reload** for `ReloadSource` and full reloads: compare loaded entries with the current snapshot and apply only what changed, reusing untouched shards | same | far less garbage per reload | peak drops from 2× toward 1× + changed shards | Needs `V` comparable or an `Equal` func; falls back to today's full swap otherwise. Also the basis of the reconcile check below |
| **Apply counters** in `Stats()`: shards copied and bytes copied per apply | 0 | 0 | 0 | Lets users see whether their shard count fits |
| **GC tuning guide** in `docs/performance.md`: `GOGC`, `GOMEMLIMIT` and how a large long-lived dataset changes when GC runs | 0 | user-side | user-side | Docs only |

### Phase P2: storage layout (v0.3, opt-in, gated on P0 numbers)

These change how shards store data, so they are done together, behind an option, and only if P0 shows GC time or single-core latency matters at realistic sizes.

| Item | Latency | GC time | Memory | Cost |
|---|---|---|---|---|
| **Single hash per read**: a per-shard open-addressing table that takes the hash computed for shard selection, instead of a Go map that hashes again | −8 to −36 ns serial | 0 | ± | Own hash table to maintain and fuzz |
| **Pointer-free shards**: keys copied into a per-shard byte arena and referenced by offset, values kept pointer-free where `V` allows, so the GC never scans the dataset | small + (one more indirection for string keys) | mark work for the dataset drops to near zero | likely lower (no per-key string headers) | Only pays off for large datasets; values containing pointers still need scanning |
| **Flat, array-based layout** for very large read-mostly datasets (rule R8) | more predictable at 1M+ (prefetch-friendly) | as above | lower | Changes cost more to apply; for datasets that change rarely |

**Exit for each item:** better on its target axis in the P0 tables, no worse than 5% on the others at 10k–100k entries, read path still allocation-free (rule R1).

### Not planned

- Persistent tree structures (HAMT and similar) instead of copy-on-write shards. They copy less per change but make every read slower and chase more pointers; refset optimises for reads. Revisit only if P0 shows apply cost dominating for real users after P1.

## Correctness and operations track

- **Reconcile check** (opt-in): a periodic reload that compares untouched keys before swapping, logs differences and counts them in `Stats().Divergences`, to detect changes the source never reported. Shares code with diff-on-reload.
- **Local snapshot file** (opt-in) for faster restarts: start from disk, catch up through `Changes`; `GetFresh` fails closed until the first catch-up.
- **pgsource:** deferred, sorted trigger variant (removes trigger-mode deadlocks); key-mapping triggers for joined tables; snapshot loading from a read replica.

## Ecosystem track

- Metrics adapters built on `Stats()` (OpenTelemetry, Prometheus), as separate modules.
- Promote `examples/pgsource` to an official `refsetpg` module once people run it in production.
- More example sources (MySQL, NATS KV, HTTP snapshot + changes); community sources listed in the README.
- gRPC metadata helpers, as a separate module.

## v1.0

When the API has external users and one minor release has gone out without breaking changes. The core module stays at zero dependencies.
