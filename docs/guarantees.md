# What refset Guarantees

This page states exactly what refset promises, under which conditions, and what it doesn't. Design reasoning is in [design.md](design.md); what a source must do is in [source-contract.md](source-contract.md).

**Every guarantee below assumes the source meets the [Source contract](source-contract.md).** Check a source with `refsettest.RunConformance`.

## The guarantees

| | Guarantee | Tested by |
|---|---|---|
| **G1 Consistency** | Every version a replica serves equals the source's state after exactly the changes up to that version. A replica may skip intermediate versions (it applies changes in batches ending at the source's head), but it never serves a version the source didn't commit. | Unit, property and fuzz tests; conformance suite |
| **G2 Atomic batches** | Readers never see part of a batch. A `Snapshot` is one version for as long as you hold it. | 64-reader race test with a constant-sum invariant |
| **G3 No lost changes** | Every change the source reports is eventually applied, despite lost notifications, restarts, reconnects or compacted history. | Polling and reconnect tests; compaction and restart scenarios; 1-hour soak |
| **G4 Read-your-writes** | `GetAtLeast(ctx, key, v)` (and `GetCtx` with a version in the context) never returns data older than version `v`. It returns it, or a fallback answer from the source's `KeyLoader`, or an error. | Plan-upgrade scenarios, in-process and over HTTP and Postgres |
| **G5 Bounded staleness** | `GetFresh` never returns data when the replica can't confirm it is current within `MaxStaleness`, or is more than `MaxLagVersions` behind. | Fake-clock staleness tests; revocation scenario |
| **G6 Convergence** | Once writes stop and faults heal, every replica reaches the same version with identical contents. | 10,000-write convergence scenario; 1-hour soak |

## Staleness, in detail

So that nobody is surprised by `GetFresh`:

- **The guarantee in one line:** a change committed at time *t* is refused by `GetFresh` on every replica by *t* + `MaxStaleness`, up to the replica clock's rate error (negligible: well under 1 ms per second).
- **What "stale" means:** the time since the replica last *confirmed* with its source that it was caught up. It is not the age of the data. A replica holding month-old data that it confirmed a second ago is fresh.
- **Why it works when the replica is cut off:** a replica that can't reach its source can't learn about new changes, but it does know how long it has been since it last heard from the source. That is what it measures.
- **Clocks:** staleness is the larger of the elapsed monotonic time and the elapsed wall-clock time, both on the replica. Every clock error therefore fails closed:

  | Event | Effect on `GetFresh` |
  |---|---|
  | Normal operation | Correct |
  | Machine or VM suspended and resumed | Stale until the next successful poll (the wall clock jumped) |
  | Wall clock stepped backwards (NTP) | Still correct (the monotonic clock enforces the bound) |
  | Wall clock stepped forwards (NTP, VM migration) | Spuriously `ErrTooStale` for up to one `PollInterval`, then correct |
  | Long GC pause, CPU throttling, container freeze | Stale if the pause exceeds the bound |

- **Clock skew never matters:** not between replicas, and not between a replica and its source. No timestamps cross machines.
- **Choosing values:** `MaxStaleness` must be greater than `PollInterval` (`Open` rejects anything else). Use at least 2× `PollInterval`, so one slow or failed poll doesn't trip it.
- **Only `GetFresh` enforces the bound.** `Get`, `GetAtLeast` and `GetCtx` never return `ErrTooStale`.

## What is not guaranteed

- **Changes the source never reported.** If the backing store changes and the source's change feed doesn't record it (a writer that skips logging, logs the wrong dataset, a change to a joined table, a manual edit without triggers), replicas silently diverge from the source until their next full reload, **and `GetFresh` still reports them as fresh**: "fresh" means caught up with everything the source reported. Mitigations: capture writes with triggers (the Postgres example's `InstallTriggers`), set `Options.ReloadInterval` to bound how long a divergence can last, and set `Options.Reconcile` to detect it: each periodic reload is compared with the version it replaces, and entries that differ although the source reported no change to them are counted in `Stats().Divergences` and logged as a warning. Alert on that counter: a non-zero value means some writer, trigger or version bump is missing.
- **Ordering across datasets.** Each dataset has its own version sequence and is applied independently. Even if two datasets change in one source transaction, a replica can briefly have the new version of one and the old version of the other. To read your own write across both, send both versions (`plans=51,perms=9`); each `GetCtx` waits for its own. Better still, avoid the need: derive values at read time, or merge datasets that always change together (see [design §4.9](design.md#49-modelling-guidance)).
- **Fallback answers are not cached.** A `KeyLoader` answer is returned to that one caller. It doesn't update the replica or advance its version.
- **Requests for versions that don't exist.** A version above the source's head, confirmed by a poll that started after the request arrived, gets `ErrVersionAhead` and no fallback. Header signing in `refsethttp` lets you ignore forged versions entirely.
- **Write throughput.** refset has no write path; how fast a dataset can be written depends on the source. The Postgres example serializes writers per dataset.
- **Persistence.** By default nothing survives a restart; `SnapshotFile` keeps an optional copy on local disk (next section).

## Memory-only behavior

The dataset lives in process memory.

- **A restarted process starts empty** unless the source is wrapped in a `SnapshotFile`. `Open` loads the full dataset from the source and blocks until it is in memory, so reads are never served from a partial or empty dataset. If the source is unreachable, `Open` returns an error when its context ends.
- **With `SnapshotFile`, a restart starts from the last saved file** and catches up with the source in the background. Until that first catch-up succeeds the data has not been confirmed current: `GetFresh` fails with `ErrTooStale` (when `MaxStaleness` or `MaxLagVersions` is set), `GetAtLeast` and `GetCtx` wait for the version a request asks for as usual, and `Get` serves the file's data, as any replica that is behind would. A file is used only if its checksum, dataset name and key and value types match (and it is younger than `MaxAge`, if set); otherwise `Open` loads from the source. If the source has compacted past the file's version, or its history is now behind it, the dataset reloads in full. Call `Save` on shutdown and periodically; it writes atomically.
- **Report ready only after `Open` returns** (for example, a Kubernetes readiness probe).
- **Startup time grows with dataset size** (see [benchmarks.md](benchmarks.md)).
- **Roll restarts gradually.** Restarting every replica at once means every replica loads the full dataset from the source at the same moment.
- **A reload may hold two copies.** Reloads (after compacted history, a large backlog, a history reset or `ReloadInterval`) build the new version beside the old one and swap. When values can be compared (`==`, or `ReloadSource.Equal`), unchanged shards are kept and a reload that changes little needs little extra memory; otherwise the dataset must fit in memory twice.
- **Values are shared and read-only.** `Get` returns the value stored in the snapshot, shared with every other reader. If the value contains a map, slice or pointer, copy before modifying it, or every reader sees the change.

## Operating checklist

- Keep a real `PollInterval` (the default 1s). Notifications only shorten delivery; polling guarantees it.
- Set `MaxStaleness` to at least 2× `PollInterval` on datasets used for fail-closed decisions, and read them with `GetFresh`.
- Give sources a `KeyLoader` if a slow replica should fall back to the source rather than return `ErrNotCaughtUp`; consider `MaxConcurrentFallbacks` to cap the load.
- Read from the primary. A source that reads from a lagging copy (a database read replica, a change stream consumed with lag) breaks read-your-writes: legitimate versions look like `ErrVersionAhead`.
- Sign version headers (`refsethttp.Config.SigningKey`) when clients are on the public internet.
- Watch `Stats()`: `Staleness`, `LagVersions`, `PollErrors`, `Reloads`, `Fallbacks`.
