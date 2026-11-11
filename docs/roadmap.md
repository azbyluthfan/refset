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
| Performance P1: automatic shards, adaptive batching, diffed reloads | Done, unreleased ([results](benchmarks.md#after-the-p1-changes)) |
| Large datasets (10M) and nested values | Measured ([results](benchmarks.md#large-datasets-and-nested-values)) |
| Performance P2: storage layout | Investigated, not adopted ([results](benchmarks.md#shard-storage-experiment-p2)) |
| **v0.2.0** | **Next:** release P0 and P1 ([releasing.md](releasing.md)) |

## Performance track

refset trades along three axes, and an improvement on one often costs another:

- **Latency:** how long a read takes (mean and tail), and how long a committed change takes to become visible on a replica.
- **GC time:** the CPU the garbage collector spends because of refset, both scanning the live dataset and collecting the shards each change throws away. Pause times are only part of this.
- **Memory footprint:** bytes per entry in steady state, and the peak during a reload (two full copies today).

### Known today (after P1)

Apple M1, default options, string keys and a 24-byte value. Full tables in [benchmarks.md](benchmarks.md#latency-gc-and-memory-under-load), before/after in [After the P1 changes](benchmarks.md#after-the-p1-changes).

| Axis | Measured | Remaining gap |
|---|---|---|
| Read latency | 28–39 ns per read up to 100k entries, ~185 ns at 1M (memory-bound); p99.9 per 16 reads stays at 3–6 µs up to 100k under writes and reloads, where a locked map's doubles | Single-core reads hash each key twice (8–36 ns slower than a locked map); a single-hash table was tried in P2 and not adopted |
| Propagation latency | Notification: 70 µs p50, 179 µs max. Read-your-writes: 88 µs p50, 338 µs max | None at these sizes; a full reload per change costs the load time (11 ms at 100k) |
| GC, write path | 1M entries at 1,000 changes/s: 28 MB/s of garbage, 0.8% of a core (was 370 MB/s, 11%) | None measured |
| GC, reloads | A reload changing one entry at 1M allocates 17 MB (was 190 MB) | Values that can't be compared need `ReloadSource.Equal` to benefit |
| GC, holding data | 1–5% of a core at 100 MB/s of application allocation, the same as a map | Small; pointer-free storage could save at most this |
| Memory | Within 10% of a plain map per entry (90 B at 1M); a reload peaks at 1.12× the steady heap (was 1.9×) | Without a way to compare values, a reload still holds two copies |
| Large datasets | 10M flat entries: `Open` 2.05 s, 105 B per entry, reads 220 ns (memory-bound), a collection every ~14 s at ~0.4–0.5 s of CPU each, a one-entry reload 1.9 s at 1.10× peak ([details](benchmarks.md#large-datasets-and-nested-values)) | Beyond 10M not measured (8 GB machine) |
| Nested values | Read 2.3–3.8× slower than flat (the value copy); each collection marks ~6× longer but runs ~10× less often; reloads rebuild everything without `ReloadSource.Equal` (356 MB at 1M), and with it allocate 3× less but take 2.7× longer | Documented (store `*V`, set `Equal`); pointer-heavy values are the case where pointer-free storage can't help, because the values themselves hold the pointers |

### Phase P0: measure all three axes (done 2026-11-11)

- `bench/perfreport` (`make perfreport`): read latency percentiles while idle, applying changes and reloading; commit-to-visible latency by notification, read-your-writes, polling and full reload; GC CPU, cycles, allocation and pauses at 0–1,000 changes/s, on reloads, and while the application allocates; bytes per entry for three key/value types and the heap peak during a reload. Each next to a locked map receiving the same writes.
- `scripts/bench-compare.sh` (`make bench-compare`): core benchmarks of the latest release against the working tree with benchstat, alternating runs; part of the release checklist.
- CI builds the `bench` module and smoke-runs perfreport, so the tool keeps working.

### Phase P1: quick wins (done 2026-11-11, ships in v0.2)

Each item was re-measured with perfreport against the P0 tables; results in [benchmarks.md](benchmarks.md#after-the-p1-changes). The automatic shard count is capped at 512 below 200k entries: without the cap, 1024 shards at 100k made reads 7.5% slower.

| Item | Target from P0 | Result |
|---|---|---|
| Automatic shard count | 1M, 1,000 changes/s: 370 MB/s, 11% → a few MB/s, ~1% | 28 MB/s, 0.8% |
| Adaptive batch window | Notification p50 5.0 ms → < 0.5 ms; read-your-writes max 6.4 ms → < 1 ms | 70 µs; 338 µs |
| Pre-size shards on reload | 1M reload 190 MB → ~113 MB | 17 MB with diffing; `Open` 38–40% faster |
| Diff-on-reload | Peak 1.9× → toward 1× | 1.12× |
| Apply counters in `Stats()` | — | `Shards`, `Applies`, `ShardsCopied`, `EntriesCopied`, `ShardsKept` |
| GC tuning guide | — | [performance.md](performance.md#tuning-the-gc-for-a-large-dataset) |

The plan as written before the work:

| Item | Latency | GC time | Memory | Target from P0 |
|---|---|---|---|---|
| **Automatic shard count** at `Open` and on reload: ≈ 2.5 × √entries, rounded to a power of two; `Options.Shards` stays as an override | faster applies at 1M | far less garbage per change | 0 | 1M, 1,000 changes/s: 370 MB/s and 11% of a core → a few MB/s and ~1% |
| **Adaptive batch window**: apply the first notification at once, batch only when changes keep arriving | −5 ms propagation | slightly more applies under bursts | 0 | Notification p50 5.0 ms → under 0.5 ms; read-your-writes max 6.4 ms → under 1 ms |
| **Pre-size shards on reload** from the current entry count | faster reloads | less garbage per reload | 0 | 1M reload: 190 MB → about a presized map's 113 MB |
| **Diff-on-reload** for `ReloadSource` and full reloads: keep the current shard where its contents didn't change | same | far less garbage when little changed | peak drops toward 1× + changed shards | 1M reload peak 1.9× steady → close to 1× when few entries changed. Needs `V` comparable or an `Equal` func; falls back to a full swap otherwise. Basis of the reconcile check below |
| **Apply counters** in `Stats()`: shards and bytes copied | 0 | 0 | 0 | Lets users see whether their shard count fits |
| **GC tuning guide** in `docs/performance.md`: `GOGC`, `GOMEMLIMIT`, and how a large long-lived dataset changes when GC runs | 0 | user-side | user-side | Docs only |

### Phase P2: storage layout (investigated 2026-11-11, not adopted)

| Item | Outcome |
|---|---|
| **Single hash per read**, in a flat open-addressing table per shard (also covers the **flat, array-based layout**) | Built and measured against Go maps in two variants ([results](benchmarks.md#shard-storage-experiment-p2)). One made 1M-entry reads 16–25% faster but used 35–174% more memory; the other matched memory but read 14–23% slower at 10k–100k. Neither met the exit criteria (no more than 5% worse on any axis at 10k–100k). Go's Swiss-table maps already set a high bar |
| **Pointer-free shards** (keys in a byte arena) | Not built. Holding a dataset cost the GC 2–5% of a core at 100 MB/s of application allocation, the same as a plain map, and at most half of a flat entry's pointers are in its key. Nested values keep their pointers in the values, which an arena can't remove ([measurements](benchmarks.md#large-datasets-and-nested-values)) |

Revisit if a real workload shows reads or GC time from refset's own storage dominating, or if Go's maps change.

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
