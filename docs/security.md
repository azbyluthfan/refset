# Security model

What refset trusts, what an attacker can control, and how each risk is handled. To report a vulnerability, see [SECURITY.md](../SECURITY.md).

## What refset is, in security terms

A library inside your process. It holds a copy of reference data in memory, keeps it current from a `Source` your application provides, and answers reads. It opens no ports and has no dependencies outside the Go standard library. `refsethttp` reads version numbers from request headers and cookies; `SnapshotFile` reads and writes one local file.

## Trust boundaries

| Party | Trusted? | Why |
|---|---|---|
| Your application code, and the `Source`, `KeyLoader`, `ReloadSource` functions and `Equal` it supplies | Trusted to be correct, not to be bug-free | It is your code. refset recovers its panics so a bug can't crash the process or produce a wrong answer (below), but can't protect against a source that returns wrong data |
| The source of truth (your database or service) | Trusted | refset replicates what it says. Its guarantees assume the [Source contract](source-contract.md) holds |
| Clients sending requests | **Untrusted** | They control the version headers and cookies `refsethttp` reads |
| The local disk under `SnapshotFile.Path` | Trusted, protected by you | Whoever can write that file can change what a restarted replica serves until it catches up |
| Other processes on the host | Out of scope | refset doesn't defend against code running with your process's privileges |

## Assets

- **Correct answers.** A read must never return data the source didn't commit, or report data as fresh when the replica can't confirm it is (guarantees G1–G6 in [guarantees.md](guarantees.md)).
- **The source's capacity.** Clients must not be able to make refset overload the source.
- **The dataset's contents.** They may be sensitive (user plans, permissions); refset doesn't log keys or values and writes snapshot files readable only by their owner.

## Threats and what handles them

### Forged version headers

A client can send any `Refset-Min-Version`. refset never trusts it for correctness, only for how long to wait:

- **A version the source has reached** is safe to honour: the reply is at that version or newer.
- **A version beyond the source's head** gets `ErrVersionAhead` after one poll that started after the request arrived, with no fallback to the source.
- **Load from forged versions is bounded.** Concurrent requests share polls (one in flight, one queued), and once a poll has found a requested version doesn't exist, polls for unannounced versions run at most once per `BatchWindow` (5 ms) for the next second. Measured: one client flooding forged versions made a replica poll its source about 3,860 times a second; with the limit, about 180, the same as normal traffic. Legitimate read-your-writes reads are not delayed when nobody is forging versions, and by up to ~5 ms while someone is.
- **Signing removes the problem.** With `refsethttp.Config.SigningKey`, versions carry an HMAC-SHA256 tag (compared in constant time, with key rotation through `VerifyKeys`), and unsigned or badly signed versions are ignored before any poll. **Set a signing key on any endpoint reachable by untrusted clients.**

### Header and cookie injection

Dataset names are restricted to letters, digits, `.`, `_` and `-`, so names can't break a header or cookie. Incoming version headers are limited to 4 KB and 32 entries, parsed with `strconv.ParseUint`, and fuzzed. Cookies are `HttpOnly` and `SameSite=Lax`, and `Secure` unless `Config.CookieInsecure` is set for local development.

### Load on the source through fallbacks

When a replica can't reach a requested version within `WaitBound`, it can read the key from the source (`KeyLoader`). Concurrent fallbacks for the same key share one call, and at most `MaxConcurrentFallbacks` (default 32) run at once per dataset; beyond that, reads get `ErrNotCaughtUp` immediately. Without the cap, 500 concurrent reads for distinct keys made 500 concurrent source calls, at the moment the source was likely already slow.

### Bugs in code refset calls

A panic in a `Source`, `KeyLoader` or `ReloadSource` function is recovered and returned as a `*PanicError` (with its stack). On the background loop the poll fails and the dataset keeps its last version, logged and counted in `Stats().Panics`; `GetFresh` fails closed while the source keeps failing. Reads sharing a fallback call all get the error; a panic during `Open` is returned by `Open`. A source that ignores context cancellation can't make `Close` hang: it returns an error after 10 seconds.

Not handled: a source that returns wrong data, blocks forever on a poll (the replica goes stale and `GetFresh` fails closed, which is the intended outcome), or returns a snapshot too large for memory. Bound those in your source.

### Changes the source never reports

If data changes without the source's feed recording it, replicas diverge while `GetFresh` reports them fresh. `Options.Reconcile` detects this on each periodic reload and counts it in `Stats().Divergences`; alert on it. See [guarantees.md](guarantees.md).

### Mutation of shared values

Values returned by reads are shared with every reader. Code that changes a slice, map or pointed-to struct inside one changes it for everyone, and doing so concurrently is a data race, which can corrupt memory. Treat values as read-only. In tests, `refsettest.DetectValueMutation` fails the test when code under test changes a value it read, and `go test -race` catches concurrent changes.

### Snapshot files

`SnapshotFile` writes atomically, with mode 0600. On load it checks a CRC-64 checksum (against damage, not tampering), the dataset name, a fingerprint of the key and value types and an optional `MaxAge`, and bounds the body length by the file's size; any mismatch falls back to the source. The decoder is fuzzed with bodies that pass the checksum. The file is not authenticated: protect its directory.

### SQL in the Postgres example

`examples/pgsource` passes values as query parameters and quotes identifiers with `pgx.Identifier`. Trigger DDL, which can't take parameters, quotes literals as escape strings so they stay literals whatever `standard_conforming_strings` is set to; a test checks this against Postgres with both settings, including injection attempts. Queries in `Table` and key-mapping triggers come from the application and run as written.

## Memory safety

The library uses no `unsafe`, cgo, assembly or `linkname`. Go bounds-checks every slice and string access, so an out-of-range access panics instead of reading or writing other memory. The remaining way to corrupt memory in Go is a data race: refset's shared state uses atomics and mutexes, published versions are immutable, and readers never write shared memory. CI runs every test with the race detector, including a 64-reader test and a one-hour soak test.

## How this is checked

- `go test -race` on every push; fuzz targets for the header parser, change application, refreshes with arbitrary source answers, diffed reloads and snapshot file decoding, run weekly.
- gosec (through golangci-lint) on all shipped code, with each suppressed finding annotated with its reason; CodeQL's security-extended queries on every push, pull request and weekly.
- govulncheck on all modules on every push and weekly; actions pinned by commit SHA and updated by Dependabot; the library has no dependencies.
- Mutation checks during development: each safety check above was broken on purpose to confirm a test fails.
