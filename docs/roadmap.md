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
| **v0.1.0** | **Released 2026-11-11** ([release](https://github.com/azbyluthfan/refset/releases/tag/v0.1.0), [pkg.go.dev](https://pkg.go.dev/github.com/azbyluthfan/refset)) |
| Performance P0: measure latency, GC and memory | Done ([benchmarks](benchmarks.md#latency-gc-and-memory-under-load)) |
| **Performance P1** | **Next** |

## Performance track

refset trades along three axes, and an improvement on one often costs another:

- **Latency:** how long a read takes (mean and tail), and how long a committed change takes to become visible on a replica.
- **GC time:** the CPU the garbage collector spends because of refset, both scanning the live dataset and collecting the shards each change throws away. Pause times are only part of this.
- **Memory footprint:** bytes per entry in steady state, and the peak during a reload (two full copies today).

### Known today (measured in P0)

Apple M1, default options, string keys and a 24-byte value. Full tables in [benchmarks.md](benchmarks.md#latency-gc-and-memory-under-load).

| Axis | Measured | Gap |
|---|---|---|
| Read latency | 26–36 ns per read up to 100k entries, ~185 ns at 1M (memory-bound); p99.9 per 16 reads stays at 3–7 µs up to 100k under writes and reloads, where a locked map's doubles | None that matters at these sizes. Single-core reads still hash each key twice (8–36 ns slower than a locked map) |
| Propagation latency | Notification: 5.0 ms p50, nearly all of it the fixed `BatchWindow`. Read-your-writes: 76 µs p50, but p99 ~1 ms and max 6.4 ms | The batch window delays isolated changes, and read-your-writes waits for it when the notification arrives first |
| GC, write path | At 1M entries, ~370 KB of garbage per one-key change with 256 shards: 11% of a core at 1,000 changes/s (1% at 100/s). Pauses < 0.2 ms | Default shard count is too low for large datasets |
| GC, reloads | A full reload at 1M allocates 190 MB, 1.7× a presized map | Shards grow from empty on every load |
| GC, holding data | 1–5% of a core at 100 MB/s of application allocation, the same as a map | Small; pointer-free storage could save at most this |
| Memory | Within 10% of a plain map per entry (100 B at 1M); a reload peaks at 1.9× the steady heap | The reload peak, every time for `ReloadSource` |

### Phase P0: measure all three axes (done 2026-11-11)

- `bench/perfreport` (`make perfreport`): read latency percentiles while idle, applying changes and reloading; commit-to-visible latency by notification, read-your-writes, polling and full reload; GC CPU, cycles, allocation and pauses at 0–1,000 changes/s, on reloads, and while the application allocates; bytes per entry for three key/value types and the heap peak during a reload. Each next to a locked map receiving the same writes.
- `scripts/bench-compare.sh` (`make bench-compare`): core benchmarks of the latest release against the working tree with benchstat, alternating runs; part of the release checklist.
- CI builds the `bench` module and smoke-runs perfreport, so the tool keeps working.

### Phase P1: quick wins (v0.2)

Ordered by measured effect. Each item is re-measured with perfreport against the P0 tables.

| Item | Latency | GC time | Memory | Target from P0 |
|---|---|---|---|---|
| **Automatic shard count** at `Open` and on reload: ≈ 2.5 × √entries, rounded to a power of two; `Options.Shards` stays as an override | faster applies at 1M | far less garbage per change | 0 | 1M, 1,000 changes/s: 370 MB/s and 11% of a core → a few MB/s and ~1% |
| **Adaptive batch window**: apply the first notification at once, batch only when changes keep arriving | −5 ms propagation | slightly more applies under bursts | 0 | Notification p50 5.0 ms → under 0.5 ms; read-your-writes max 6.4 ms → under 1 ms |
| **Pre-size shards on reload** from the current entry count | faster reloads | less garbage per reload | 0 | 1M reload: 190 MB → about a presized map's 113 MB |
| **Diff-on-reload** for `ReloadSource` and full reloads: keep the current shard where its contents didn't change | same | far less garbage when little changed | peak drops toward 1× + changed shards | 1M reload peak 1.9× steady → close to 1× when few entries changed. Needs `V` comparable or an `Equal` func; falls back to a full swap otherwise. Basis of the reconcile check below |
| **Apply counters** in `Stats()`: shards and bytes copied | 0 | 0 | 0 | Lets users see whether their shard count fits |
| **GC tuning guide** in `docs/performance.md`: `GOGC`, `GOMEMLIMIT`, and how a large long-lived dataset changes when GC runs | 0 | user-side | user-side | Docs only |

### Phase P2: storage layout (v0.3, opt-in, gated on P0 numbers)

These change how shards store data, so they are done together, behind an option, and only if a real workload shows they matter. P0 says they don't yet: reads are already flat under load, and the GC cost of holding a dataset was 1–5% of a core, the same as a map's.

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
