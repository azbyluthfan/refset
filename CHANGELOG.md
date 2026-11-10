# Changelog

All notable changes to this project are documented here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses [Semantic Versioning](https://semver.org/). Until v1.0.0, minor versions may contain breaking changes.

## [Unreleased]

### Changed

- Package documentation: a getting-started example, and links that work on pkg.go.dev.
- README: Go version requirement, import paths of the three packages, and a complete quick-start program.
- The examples and bench modules require v0.1.0 (they still build against the code in the repository); the examples README explains how to use an example in your own module.

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
