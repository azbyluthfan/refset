# Changelog

All notable changes to this project are documented here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses [Semantic Versioning](https://semver.org/). Until v1.0.0, minor versions may contain breaking changes.

## [Unreleased]

### Added

- Repository scaffolding, CI, design document, source contract and landscape notes.
- Core `refset` package: `Dataset` with `Open`, `Get`, `GetAtLeast`, `GetCtx`, `GetFresh`, `Snapshot`, `Stats`; `Source` contract; staleness from the larger of monotonic and wall-clock time; coalesced request-triggered polls; per-key fallback collapsing.
- `ReloadSource`: feed a dataset from your own code with a load function and a version function; the dataset reloads whenever the version moves. No adapter, changelog or database required.
- `refsettest`: in-memory source, per-replica fault injection, fake clock, and `RunConformance` for checking any `Source`.
- `refsethttp`: version headers, per-dataset cookies, optional HMAC signing, and middleware for read-your-writes over HTTP.
- Documentation: guarantees, performance and memory, benchmarks, soak results, source contract, design.
- `bench/` module comparing reads with golang-lru and locked maps, plus a memory and GC report.
- Release workflow: tagging `vX.Y.Z` runs the full suite and publishes the CHANGELOG section as a GitHub release.
- `examples/structs`, `examples/dbrows`, `examples/httpjson`: `ReloadSource` over in-memory structs, an app's own SQL, and a JSON API.
- `examples/` module: `pgsource` (reference Postgres source with `Write`, listener, compaction and triggers), `filesource`, and the `currencies`, `plan-upgrade` and `baseline-lru` demos (`make demo`, `make demo-lru`).
