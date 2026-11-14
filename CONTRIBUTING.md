# Contributing to refset

Thanks for your interest. refset is early and experimental, so issues that question the design are as welcome as code.

## Ground rules

- **The library module stays dependency-free.** The root `go.mod` must have no `require` lines, including test-only dependencies. Use the standard library (`testing`, `testing/quick`, fuzzing). Code that needs third-party packages belongs in `examples/` or `bench/`, which are separate modules. CI enforces this (`make check-deps`).
- **Every change states which guarantee it affects.** The guarantees G1–G6 are defined in [docs/design.md §8](docs/design.md#8-test-plan). A change to behavior should come with a test that maps to one of them.
- **Sources follow the contract.** A new example source must pass `refsettest.RunConformance` (see [docs/source-contract.md](docs/source-contract.md)).

## Developer Certificate of Origin

All commits must be signed off, certifying the [Developer Certificate of Origin](https://developercertificate.org/):

```bash
git commit -s -m "Describe the change"
```

This adds a `Signed-off-by: Your Name <you@example.com>` line. CI checks every commit in a pull request. To fix a commit you forgot to sign:

```bash
git commit --amend -s --no-edit
```

There is no CLA.

## Development

Requirements: Go 1.26 or newer. Docker is needed only for the Postgres example's tests.

```bash
make test        # unit tests
make race        # tests with the race detector
make lint        # golangci-lint (install from https://golangci-lint.run)
make check-deps  # fails if the library module gained a dependency
make ci          # everything CI runs for the library
make fuzz        # every fuzz target, 30s each (FUZZTIME=5m for longer)
make soak        # replicas under random faults, 10 minutes (SOAK_DURATION=1h)
make vulncheck   # govulncheck on the library, examples and bench modules
make perfreport  # latency, GC and memory under load, as Markdown (about 10 minutes)
make bench-compare  # core benchmarks: latest release vs your working tree
```

`go.work` ties the library and the example modules together for local development. CI checks the library on its own with `GOWORK=off`.

### Pipelines

| Workflow | When | What |
|---|---|---|
| `ci.yml` | every push to `main` and every pull request | lint (with gosec), zero-dependency check, tests with the race detector on Go 1.26 and 1.27 and on Linux ARM64, Windows and macOS, tests as 32-bit x86 and with 1–2 CPUs, workflow and script checks, DCO |
| `examples.yml` | every push to `main` and every pull request | the examples module against Postgres 15 and 17, and every demo |
| `weekly.yml` | Sundays, or by hand from the Actions tab | 45-minute soak, 5 minutes per fuzz target, govulncheck on all modules, newest Go and Postgres, s390x and 32-bit ARM under QEMU, WebAssembly under wasmtime, perfreport on x86-64 and ARM64 for trends; a failure opens an issue |
| `codeql.yml` | pushes to `main`, pull requests, weekly | CodeQL security-extended queries on all modules |
| `release.yml` | a `vX.Y.Z` tag | the steps in [docs/releasing.md](docs/releasing.md) |

Actions are pinned by commit SHA and updated by Dependabot.

## Pull requests

- Keep each pull request to one change.
- Fill in the pull request template, including the guarantee section.
- Run `make ci` before pushing.

## Reporting security issues

Please don't open a public issue. See [SECURITY.md](SECURITY.md).
