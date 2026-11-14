# Roadmap

Where refset is going after the first release. The reasoning behind the design is in [design.md](design.md). Measurements referred to here are in [benchmarks.md](benchmarks.md).

Nothing here is a promise of dates. Items move up when users ask for them; open an issue if one matters to you.

## Where things stand (2026-11-14)

| Area | State |
|---|---|
| Core library, test kit, HTTP propagation, examples, docs | Done |
| **v0.1.0** | Released 2026-11-11 ([release](https://github.com/azbyluthfan/refset/releases/tag/v0.1.0)) |
| **v0.2.0**: performance P0 and P1 (automatic shards, adaptive batching, diffed reloads) | Released 2026-11-11 ([release](https://github.com/azbyluthfan/refset/releases/tag/v0.2.0)) |
| Performance P2: storage layout | Investigated, not adopted ([results](benchmarks.md#shard-storage-experiment-p2)) |
| Large datasets (10M) and nested values; memory limits (P3a) | Measured ([large data](benchmarks.md#large-datasets-and-nested-values), [memory limits](benchmarks.md#memory-limits)) |
| Correctness and operations: reconcile check, `SnapshotFile`, `pgsource` deferred and key-mapping triggers, replica snapshots | Done, ships in v0.3.0 |
| Security (P3c): panic recovery, fallback cap, forged-version rate limit, fuzzing, static analysis, threat model | Done, ships in v0.3.0 |
| Platforms (P3b): Linux x86-64 and ARM64, Windows, macOS, 32-bit, big-endian, WebAssembly, 1–2 CPUs in CI | Done, ships in v0.3.0 |
| `refsetexpvar`, `GetForRequest`, runnable examples | Done, ships in v0.3.0 |
| **v0.3.0** | **Releasing** ([CHANGELOG](../CHANGELOG.md)) |
| **Next** | The rest of P3 (real-Postgres measurement, dedicated-machine benchmarks, suspend tests), change notification, ecosystem items on demand |

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

### Phase P3: exploration (in progress)

Questions the docs make claims about without having tested them, or where real deployments differ from the one machine (Apple M1, macOS) used so far. Each item answers one question and changes the docs it names; none is expected to change the library unless the answer surprises. Three groups: measurement, platforms, security.

#### P3a: measurement

| Item | Question | How | Changes |
|---|---|---|---|
| **Tight memory limits** (done 2026-11-14) | How does GC CPU grow as `GOMEMLIMIT` approaches the dataset's size, and where should it sit? | perfreport `-sections memlimit`: limits from none to 1.25× the steady Go memory, `GOGC` 100 and off, 1M and 10M entries ([results](benchmarks.md#memory-limits)) | Measured: 2× or more costs nothing; 1.5× is fine for large datasets; 1.25× multiplies collections several times over and makes rebuilding reloads overshoot. Rule in [performance.md](performance.md#tuning-the-gc-for-a-large-dataset) |
| **The source side, with Postgres** | How long does `pgsource` take to load 1M and 10M rows, and what do N replicas do to the database? Every `Open` and reload number so far is in-process and leaves out the source's read time | Load and reload timings for `pgsource` and a `ReloadSource` over `dbrows`; 1–50 replicas polling and listening on one Postgres, measuring connections, idle-poll query rate and `NOTIFY` fan-out | Startup, reload and replica-count guidance in docs/performance.md and the examples README |

#### P3b: machines, CPUs and operating systems

| Item | Question | How | Changes |
|---|---|---|---|
| **x86 and arm64 servers** (in progress) | Do the read curve and the 200k-entry shard cap hold on x86 (64-byte cache lines, smaller per-core L2, large L3) and on arm64 servers? | The weekly run uploads perfreport from x86-64 and ARM64 GitHub runners, for trends. Still to do: once each on a dedicated Intel, AMD and Graviton machine, for numbers to publish | A table per machine in docs/benchmarks.md; the shard cap adjusted if they disagree |
| **Operating systems** (done 2026-11-14) | Does everything pass on Windows and Linux as well as macOS, with their different timer resolutions and file semantics? | The test suite with the race detector on Linux (x86-64, ARM64), Windows and macOS on every push | Passed on the first run on every platform, including `SnapshotFile`'s atomic rename on Windows |
| **Clocks across suspend and migration** | Does `GetFresh` fail closed as guaranteed (G5) on each OS when a machine sleeps, a VM is migrated, or NTP steps the clock? The fake-clock tests cover the logic, but the real monotonic clock stops during suspend on some systems and not others | Suspend/resume and clock-step runs on macOS, Linux and Windows, checking `Stats().Staleness` and `GetFresh` against wall time | docs/guarantees.md states the behaviour per OS, with tests where the platform allows |
| **32-bit, big-endian and WebAssembly** (done 2026-11-14) | Does refset build and pass on 32-bit, big-endian and WebAssembly targets? | 32-bit x86 natively on every push; s390x and 32-bit ARM under QEMU and `wasip1` under wasmtime weekly; vetted for FreeBSD and RISC-V | Cross-compiling found that the tests didn't build on 32-bit (a test constant overflowed `int`); fixed. The library built everywhere. Supported platforms listed in the README |
| **Container CPU limits** (done 2026-11-14) | With 1–2 CPUs, does the background loop keep up? | The full test suite, race detector on, with `GOMAXPROCS` 1 and 2 on every push | Passes. A measured latency table under a CPU quota is left for the dedicated-machine runs |

#### P3c: security (done 2026-11-14)

| Item | Question | How | Changes |
|---|---|---|---|
| **Threat model** (done) | What does refset trust, and what can an attacker control? | Written down | [docs/security.md](security.md), linked from SECURITY.md |
| **Memory safety and injection** (reviewed 2026-11-11 for v0.2.0) | Can anything in refset corrupt memory or be injected into? | Reviewed: no `unsafe`, cgo, assembly or `linkname` in the library outside tests; `reflect` only checks a type's comparability; shared state uses atomics and mutexes, with the race detector in CI, a 64-reader race test and a 1-hour soak under `-race`; slices and strings are bounds-checked, so overflows panic instead of corrupting; the version header is length- and entry-limited, parsed with `ParseUint`, and fuzzed; dataset names are restricted to `[A-Za-z0-9._-]`, so headers and cookies can't be injected into; HMAC signatures are compared in constant time; cookies are `HttpOnly`, `SameSite=Lax`, `Secure` by default; `pgsource` sanitizes identifiers, passes values as parameters, and now quotes DDL literals safely whatever `standard_conforming_strings` is; govulncheck finds nothing in any module | Recorded in docs/security.md |
| **Shared values in user code** (done) | Can applications find code that changes values every reader shares? | `refsettest.DetectValueMutation` | Fails a test when code under test changes a value it read |
| **Forged-version amplification** (done) | How much load can one client cause with forged versions? | Flood test: one client made a replica poll its source ~3,860 times a second | Polls for unannounced versions are rate-limited to one per `BatchWindow` once a poll has found a forged one: ~180 a second, legitimate reads undelayed without forgeries. Signing recommended for untrusted clients |
| **Fallback limits** (done) | Can slow replicas plus many requests multiply load on the source? | 500 concurrent reads for distinct keys made 500 concurrent source calls | `MaxConcurrentFallbacks` defaults to 32 (`-1` for unlimited) |
| **Misbehaving user code and sources** (done) | What happens when source code panics or ignores cancellation? | Tests injecting each failure | Panics recovered as `*PanicError` (`Stats().Panics`); a panic in a shared fallback no longer gives waiters a wrong "no such key"; `Close` returns an error after 10 s instead of hanging. Oversized snapshots and wrong data stay the source's responsibility |
| **Broader fuzzing** (done) | Is the apply path safe against any sequence of source answers, and diff-on-reload and snapshot files against any input? | `FuzzDatasetRefresh`, `FuzzLoadDiff`, `FuzzSnapshotFileDecode` (a planted bug found in 0.07 s) | Run weekly with the other fuzz targets |
| **Supply chain and static analysis** (done) | What else is cheap and worth having? | gosec over shipped code (two hardening changes, the rest annotated), CodeQL security-extended queries in CI | OpenSSF Scorecard left out: its standard setup publishes results to a public service, a decision for the maintainer |

### Not planned

- Persistent tree structures (HAMT and similar) instead of copy-on-write shards. They copy less per change but make every read slower and chase more pointers; refset optimises for reads. Revisit only if P0 shows apply cost dominating for real users after P1.

## Correctness and operations track

- ~~**Reconcile check**~~ **Done (v0.3.0):** `Options.Reconcile` compares each periodic reload with the version it replaces, skipping keys the source reported changing, and counts the rest in `Stats().Divergences` with a warning in the log.
- ~~**Local snapshot file**~~ **Done (v0.3.0):** `refset.SnapshotFile` wraps a source; `Open` starts from the saved file (checksum, dataset name, type fingerprint and age checked; the source is used otherwise) and catches up through `Changes`; `GetFresh` fails closed until it has. 1M entries: `Save` 170 ms, 18 MB file, `Open` from the file 260 ms. How much faster that is than a real source's full load is part of the P3a Postgres measurement.
- **pgsource:**
  - ~~Deferred, sorted trigger variant~~ **Done (v0.3.0):** `InstallDeferredTriggers` assigns versions at commit in sorted order. Under 20 concurrent writers in random table orders, immediate mode aborted 194 of 200 transactions for deadlocks, deferred mode none. Writers read their version after the commit.
  - ~~Key-mapping triggers for joined tables~~ **Done (v0.3.0):** `InstallKeyMappingTrigger` runs an application query over a joined table's changed rows and logs only the affected dataset keys, in immediate or deferred mode, instead of reloading the whole dataset.
  - **Fixed:** triggers were named after their table only, so a second dataset with triggers on the same table silently replaced the first's. Names now include the dataset, and reinstalling removes that dataset's old-named triggers.
  - ~~Snapshot loading from a read replica~~ **Done (v0.3.0):** `Table.SnapshotPool` reads full snapshots on a replica when catching up from its version will work, and on the primary otherwise. Checked against a real hot standby with replay paused.

## Change notification (proposed)

Applications that keep something derived from a dataset (a compiled rule set, a search index, a cache of rendered pages) can only notice changes today by polling `Version()`. A way to be told when a new version is applied would let them rebuild exactly when needed. Open questions before building it: a callback or a channel; what happens when a consumer is slow (drop to the latest version, or block the loop, which must never happen); whether consumers get the changed keys or only the new version; and how full reloads are reported. To be designed on its own rather than added to a release in passing.

## Ecosystem track

- Metrics adapters built on `Stats()` (OpenTelemetry, Prometheus), as separate modules. `refsetexpvar` (standard library only) already publishes `Stats` at `/debug/vars`.
- Promote `examples/pgsource` to an official `refsetpg` module once people run it in production.
- More example sources (MySQL, NATS KV, HTTP snapshot + changes); community sources listed in the README.
- gRPC metadata helpers, as a separate module.

## v1.0

When the API has external users and one minor release has gone out without breaking changes. The core module stays at zero dependencies.
