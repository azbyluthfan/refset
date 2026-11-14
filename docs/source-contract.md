# The Source Contract

A `Source` connects a refset `Dataset` to wherever the data actually lives: a database, files, an HTTP service, a message stream. refset's guarantees (G1–G6, [design §8](design.md#8-test-plan)) hold **only if the source meets this contract**. `refsettest.RunConformance` checks it automatically; every source in this repository runs it, and third-party sources should too.

The words MUST, MUST NOT, SHOULD and MAY are used in their usual specification sense.

## Interface

```go
package refset

type Source[K comparable, V any] interface {
    // Name identifies the dataset. It is used to look up the request's minimum
    // version (e.g. "plans" in "Refset-Min-Version: plans=51") and to label
    // stats and logs.
    Name() string

    // LoadSnapshot streams the full dataset as of one version. It calls add
    // once per entry and returns the version that the entries reflect.
    LoadSnapshot(ctx context.Context, add func(key K, value V)) (version uint64, err error)

    // Changes returns every change after version `after`, up to and including
    // the source's current head, and that head.
    Changes(ctx context.Context, after uint64) (head uint64, deltas []Delta[K, V], err error)

    // Watch returns a channel of head versions, used only to wake the
    // dataset's update loop early.
    Watch(ctx context.Context) (<-chan uint64, error)
}

// Optional. If the source implements it, the dataset uses it as the fallback
// when a version wait times out.
type KeyLoader[K comparable, V any] interface {
    LoadKey(ctx context.Context, key K) (value V, ok bool, err error)
}

type Delta[K comparable, V any] struct {
    Version uint64
    Ops     []Op[K, V]
}

type Op[K comparable, V any] struct {
    Key    K
    Value  V    // ignored when Delete is true
    Delete bool
}

var ErrSnapshotRequired = errors.New("refset: changes unavailable, full snapshot required")
```

`LoadSnapshot` takes a callback instead of returning an iterator so that a source can stream a large table and still report an error that happens part-way through (an `iter.Seq2` cannot carry one). If it returns an error, the dataset discards everything `add` received.

## Versions

- A version is a `uint64` assigned by the source. Version 0 means "nothing committed yet"; the first committed change has version 1 or higher.
- Versions belong to one dataset. Different datasets have independent sequences.

## Requirements

### C1: Consistent snapshot

The entries passed to `add` MUST be exactly the dataset's state at the returned `version`: every change up to and including `version` applied, nothing after. Each key MUST be passed at most once. `version` MUST be the source's latest committed version at some moment during the call (the dataset treats a successful load as confirming it is caught up, for staleness).

*Example: Postgres reads the version row and the table in one `REPEATABLE READ` transaction. A file source writes the snapshot file and its version atomically (write to a temporary file, then rename).*

Supports G1.

### C2: Ordered, gap-free versions

Every committed change MUST have exactly one version, and versions MUST become visible to `Changes` only in commit order: once version N is visible, every version below N is visible too. A source MUST NOT later reveal a change with a version at or below one it has already reported.

*Counter-example: assigning versions from a database sequence at insert time. Transactions commit in a different order than they draw numbers, so a reader can see 52 before 51 commits and skip 51 forever. The Postgres example avoids this by serializing writers on a per-dataset version row.*

Supports G1, G3.

### C3: Complete changes up to the head

A call to `Changes(ctx, after)` that returns a nil error MUST:

- return `head`, the latest **committed** version at some moment during the call. So `head` includes every version committed before the call started;
- return deltas that cover **every** change in `(after, head]`, in ascending version order, each with `after < Version ≤ head`;
- give values that, applied in order onto the state at `after`, produce exactly the state at `head`.

A source MAY collapse many changes into fewer deltas, including a single delta at `head` holding the current value of every changed key; intermediate versions are then never materialized, which the dataset accepts. A key MAY appear in more than one delta; the later one wins. If nothing changed, return `head == after` and no deltas.

A nil error is a promise: the dataset records "caught up as of this call" and uses it for staleness (design §4.6). If the source is not sure it saw everything (network error, timeout, partial read), it MUST return an error instead.

*Note on lagging copies:* a source that reads from a copy that trails the primary (a database read replica, a change stream consumed with lag) breaks this requirement. The symptom is read-your-writes failing: a client holding version 51 asks a replica whose source still reports head 50, and gets `ErrVersionAhead` instead of a short wait. Read `head` and changes from the primary (design §6.3). Slowness is fine: a source may take a long time to answer, as long as the head it reports was current when it read it.

*Note on head: `head` reported below `after` means the source's history went backwards (see "History resets" below). Return `ErrSnapshotRequired` in that case.*

Supports G1, G3, G5.

### C4: Snapshot required

If the changes after `after` cannot be returned as per-key changes, `Changes` MUST return `ErrSnapshotRequired` (or an error wrapping it). The dataset then reloads with `LoadSnapshot` while continuing to serve the old snapshot. Typical reasons:

- the change history before some version was deleted (compaction) and `after` is older than that;
- so many keys changed that a full reload is cheaper (a threshold chosen by the source);
- the source recorded a whole-dataset change (for example a change to a joined table, design §6.3).

Supports G3.

### C5: Best-effort Watch

`Watch` MAY drop, duplicate, delay or reorder notifications, and MAY send versions the dataset has already applied. The dataset's correctness never depends on it; it only shortens the time until the next `Changes` call. The channel MUST be closed when `ctx` is cancelled. A source with no push mechanism MAY return a channel that never sends and is closed on cancellation; the dataset then relies on polling.

If the underlying subscription breaks, the source SHOULD reconnect on its own and SHOULD send its current head after reconnecting, since notifications may have been lost meanwhile.

Supports G3 (indirectly: latency only).

### C6: KeyLoader returns current data

If implemented, `LoadKey` MUST return the source's value for the key at a version greater than or equal to every version the source has already reported from `Changes` or `Watch`. In practice: read the latest committed value from the primary copy of the data, never from a lagging replica or a cache.

`ok == false` means the key does not exist at that version.

Supports G4.

### C7: Stable name

`Name` MUST return the same value for the life of the source, and it MUST match the dataset name writers put in version headers. Declare it once as a constant shared by the writer and the source.

Supports G4.

## Failures, cancellation and panics

These aren't contract requirements; they describe how a dataset treats a source that misbehaves.

- **Errors** from `Changes` or `LoadSnapshot` fail that poll: the dataset keeps its last version, counts the error in `Stats().PollErrors`, retries on its next poll, and `GetFresh` fails closed once the replica has gone `MaxStaleness` without confirming it is current.
- **Panics** in any source method, a `KeyLoader`, or a `ReloadSource` function are recovered and returned as a `*refset.PanicError` with the stack: on the background loop they fail the poll like an error (and are counted in `Stats().Panics`); in a fallback, every read sharing that call gets the error; during `Open`, `Open` returns it. Don't rely on this: a panic is still a bug.
- **Cancellation.** Methods must return promptly when their context ends (see Concurrency). If one doesn't, `Close`, which cancels the background loop's context, waits up to 10 seconds for the current call, then returns an error and leaves the call running.

## Concurrency

- The dataset calls `LoadSnapshot`, `Changes` and `Watch` from its single update loop; they are never called concurrently with each other for one dataset.
- `LoadKey` is called from request goroutines and MUST be safe for concurrent use. The dataset collapses concurrent calls for the same key, but different keys are loaded in parallel.
- `Name` MUST be safe to call from any goroutine.
- All methods MUST respect `ctx` cancellation promptly.

## History resets

If a source's history is reset (database restored from a backup, tables recreated) its versions can go backwards. This is outside the contract and outside refset's guarantees. A source that can detect it (for example `head < after`) MUST return `ErrSnapshotRequired`; the dataset reloads and logs a warning, and its applied version may decrease. Writers and clients holding old version numbers may see `ErrVersionAhead` until the source's versions pass them again.

## The simplest source

`refset.ReloadSource` meets this contract from two functions you write: `Load` (every value and the version it reflects, read together: C1) and `Version` (the latest committed version: C3). Its `Changes` answers `ErrSnapshotRequired` whenever the version moved (C4), so the dataset reloads everything. You keep two promises: bump the version with every change, in the same transaction or under the same lock, and read it from the primary copy of the data.

## Checking a source

```go
func TestConformance(t *testing.T) {
    refsettest.RunConformance(t, func(t *testing.T) refsettest.Harness[string, Plan] {
        src := newMySource(t) // a fresh, empty source per subtest
        return refsettest.Harness[string, Plan]{
            Source:  src,
            Apply:   src.Write,   // commit ops as one version, return it; concurrency-safe
            Compact: src.Compact, // optional: discard history up to the head
            Key:     func(i int) string { return "user-" + strconv.Itoa(i) },
            Value:   func(i int) Plan { return Plan{Tier: tiers[i%len(tiers)]} },
        }
    })
}
```

The suite checks, each as a named subtest:

| Subtest | Requirement |
|---|---|
| `C7/name` | `Name` is non-empty and stable |
| `C1_C3/sequential` | Versions are contiguous; `Changes` from every earlier version reports the right head and, applied, produces the model's state; `LoadSnapshot` matches the model |
| `C2/concurrent_writers` | 8 concurrent writers get unique, contiguous versions |
| `C1_C3/during_writes` | Snapshots and changes taken while writes continue match the model at the versions they report |
| `C4/compaction` | After `Compact`, changes from before it return `ErrSnapshotRequired` (skipped if `Compact` is nil) |
| `C4/history_reset` | `Changes` after a version beyond the head returns `ErrSnapshotRequired` |
| `C5/watch` | Notifications never exceed the head; the channel closes on cancellation (skipped if `Watch` returns an error) |
| `C6/key_loader` | `LoadKey` returns the latest value, reports deletes, and is safe for concurrent use (skipped without `KeyLoader`) |

A source may answer `Changes` with `ErrSnapshotRequired` instead of per-key changes (as `ReloadSource` always does); the suite then checks that the full snapshot it asks for is exactly the state at the current head.

The suite is itself tested against deliberately broken sources (one that loses a delta, one that drops a key from its snapshot, one that ignores a history reset) to make sure it fails them.
