# Changelog

All notable changes to this project are documented here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses [Semantic Versioning](https://semver.org/). Until v1.0.0, minor versions may contain breaking changes.

## [Unreleased]

### Added

- **`Options.Reconcile`**: compares each periodic reload (`ReloadInterval`) with the version it replaces and counts entries that differ although the source reported no change to them, in the new `Stats().Divergences` (and `Stats().Reconciles`), with a warning in the log. It detects changes a source's feed missed: unlogged writes, manual edits, or a `ReloadSource` change made without bumping the version.
- `perfreport -sections memlimit`: sweeps `GOMEMLIMIT` from no limit to 1.25× of steady memory with `GOGC` 100 and off.

### Documentation

- docs/performance.md: a measured rule for `GOMEMLIMIT` and container sizing (2× steady memory or more costs nothing; avoid 1.25×), replacing an unmeasured one.

## [0.2.0] - 2026-11-11

Performance release: changes propagate in microseconds instead of after a fixed 5 ms window, large datasets produce about 13× less garbage per change, and reloads that change little no longer double memory. Reads are unchanged. Measurements in [docs/benchmarks.md](docs/benchmarks.md#after-the-p1-changes).

### Changed

- **`Options.Shards: 0` now chooses the shard count from the dataset's size** (it was a fixed 256): about 2.5 × √entries, rounded up to a power of two, at most 512 below 200,000 entries where more shards would slow reads. It is chosen at `Open` and again when a full reload finds the size has moved a lot. Datasets up to about 100k entries get 512 or fewer; 1M entries get 4096, which cut garbage per change from ~370 KB to ~28 KB. `Stats().Shards` reports the count; set `Shards` to fix it.
- **Adaptive batch window.** A notification after a quiet period is applied at once instead of after `BatchWindow`; only notifications within `BatchWindow` of the last poll are batched. Notification to visible went from 5 ms to about 70 µs.
- **Read-your-writes polls at once.** `GetAtLeast` and `GetCtx` start a poll even when a notification already announced the version, instead of waiting for a batched poll. The worst case went from 6.4 ms to about 0.3 ms.
- **Full reloads create shard maps at their final size**, and `Open` stages entries first so it can: `Open` is 38–40% faster than in v0.1.0 and allocates up to 28% less.

### Added

- **Reloads keep unchanged shards.** When values can be compared (`==` for comparable types without interfaces, or the new `ReloadSource.Equal`), a shard whose entries all came back unchanged is kept instead of rebuilt. A 1M-entry reload changing one entry allocates 17 MB instead of 190 MB and peaks at 1.12× the heap instead of 1.9×. This is every reload with `ReloadSource`.
- **`ReloadSource.Equal`**, for values `==` can't compare (structs with slices or maps). The `structs` example shows it.
- **`Stats` fields** `Shards`, `Applies`, `ShardsCopied`, `EntriesCopied` and `ShardsKept`, to see what each change and reload copies.
- **`bench/perfreport`** (`make perfreport`): read latency percentiles under writes and reloads, commit-to-visible latency, GC CPU and allocation under writes and while the application allocates, memory per entry and the reload peak, each next to a locked map; `-sections scale,nested` adds a large flat dataset and deeply nested values.
- **`scripts/bench-compare.sh`** (`make bench-compare`): core benchmarks of the latest release against the working tree, with benchstat.

### Fixed

- `examples/pgsource`: trigger DDL now quotes the dataset name and key column as escape strings when they contain a backslash, so they stay literals even on a server with `standard_conforming_strings` off. Values come from the application, not end users; this closes the gap regardless.

### Documentation

- README: Go version requirement, the three import paths, and a complete quick-start program; package documentation with a getting-started example.
- docs/performance.md: the automatic shard count, a GC tuning guide (`GOGC`, `GOMEMLIMIT`, containers), and guidance for large datasets and nested values.
- docs/benchmarks.md: latency, GC and memory under load; results up to 10M flat entries and 1M nested values; the storage experiment that was tried and not adopted.
- docs/roadmap.md: the performance track's results, the next exploration phase (memory limits, real-source costs, other machines and operating systems, security), and a memory-safety and injection review.

### Upgrading

No API was removed or renamed. If you relied on exactly 256 shards (for example, in a benchmark), set `Options.Shards: 256`. If you set `BatchWindow`, it now bounds how often notifications trigger polls rather than delaying every one.

## [0.1.0] - 2026-11-11

First release. Experimental: the API may change before v1.0.

### Added

- Repository scaffolding, CI, design document and source contract.
- Core `refset` package: `Dataset` with `Open`, `Get`, `GetAtLeast`, `GetCtx`, `GetFresh`, `Snapshot`, `Stats`; `Source` contract; staleness from the larger of monotonic and wall-clock time; coalesced request-triggered polls; per-key fallback collapsing.
- `ReloadSource`: feed a dataset from your own code with a load function and a version function; the dataset reloads whenever the version moves. No adapter, changelog or database required.
- `refsettest`: in-memory source, per-replica fault injection, fake clock, and `RunConformance` for checking any `Source`.
- `refsethttp`: version headers, per-dataset cookies, optional HMAC signing, and middleware for read-your-writes over HTTP.
- Documentation: guarantees, performance and memory, benchmarks, soak results, source contract, design, roadmap.
- `bench/` module comparing reads with golang-lru and locked maps, plus a memory and GC report.
- Release workflow: tagging `vX.Y.Z` checks the tag and CHANGELOG, runs the full suite on every supported Go and Postgres version, publishes the CHANGELOG section as a GitHub release and fetches the version through the Go module proxy ([docs/releasing.md](docs/releasing.md)).
- Weekly workflow: soak, fuzzing, govulncheck on all modules, and the newest Go and Postgres; a failure opens an issue. `make fuzz`, `make soak`.
- `examples/structs`, `examples/dbrows`, `examples/httpjson`: `ReloadSource` over in-memory structs, an app's own SQL, and a JSON API.
- `examples/` module: `pgsource` (reference Postgres source with `Write`, listener, compaction and triggers), `filesource`, and the `currencies`, `plan-upgrade` and `baseline-lru` demos (`make demo`, `make demo-lru`).
