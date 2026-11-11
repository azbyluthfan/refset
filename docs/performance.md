# Performance and Memory

How refset uses memory and CPU caches, what that means for your data, and the rules maintainers keep. Measurements are in [benchmarks.md](benchmarks.md); the reasoning is in [design §4.11](design.md#411-memory-and-cpu-caches).

## The short version

- Reads are local and lock-free: about 25 ns on one core for a 10k-entry dataset, and they scale across cores (8 cores: one read every ~5 ns in aggregate). A map behind a `sync.RWMutex` stops scaling at about 87 ns per read.
- Large datasets are bound by main memory: at 1M entries a random read costs ~135 ns on one core.
- Keep values small, or store pointers to large ones.
- The shard count follows the dataset's size; leave `Options.Shards` at 0 unless you have measured a reason.
- A reload that changes little needs little extra memory when values can be compared (`==`, or `ReloadSource.Equal`). Otherwise the dataset must fit in memory twice, because a reload builds a new copy before swapping.

## How the CPU caches your data

Code can't choose what the CPU caches. The hardware loads whole cache lines (64 bytes on x86, 128 on Apple Silicon) when memory is used, keeps them per core (L1, usually L2) or shared (L3), evicts the least recently used within small address-determined sets, prefetches only predictable access patterns, and invalidates a line in every other core's cache when one core writes to it. What code controls is layout and access patterns. refset follows these rules:

| # | Rule | What it means for you |
|---|---|---|
| R1 | The read path never writes shared memory and never allocates | Nothing to do; tests enforce it. Reads don't slow each other down |
| R2 | Fields readers use are on different cache lines from fields the background loop writes | Nothing to do; a layout test enforces it. Measured: no read slowdown while the loop polls 363k times a second |
| R3 | The fixed read-path overhead stays small (the shard slice is 8 bytes per shard: 2 KB at 256, 32 KB at 4096) | Nothing to do |
| R4 | Datasets larger than the CPU's L2/L3 cache are read from main memory | Expect ~25–30 ns per read up to ~100k entries and ~135 ns at 1M (on an M1). Still far faster than any network call |
| R5 | Go maps store values inline, and `Get` copies the value | **Keep values small.** A 256-byte value read 7.5× slower than a 16-byte one; storing `*V` for large values was 2.6× faster. Pointers add a hop per field they reach, so flat structs are best |
| R6 | Each change copies every shard it touches | Nothing to do by default: the shard count is chosen from the dataset's size (below). At 1M entries it is 4096, which made a one-key change produce 13× less garbage than 256 |
| R7 | A reload builds the new version beside the old one | With comparable values (or `ReloadSource.Equal`), unchanged shards are kept, and a reload that changes little peaked at 1.12× the steady heap. Without a way to compare, **budget memory for two copies**: at about 100 bytes per entry, 1M entries is ~100 MB, so ~200 MB during a reload |
| R8 | Hash maps defeat prefetching and cost GC scanning | A flat, pointer-light layout is a post-v0.1 option for very large datasets. GC pauses at 1M entries were already only tens of microseconds |

## Garbage collection

refset adds GC work in two ways ([measurements](benchmarks.md#garbage-collection)):

- **Changes.** Each change copies the shards it touches, and the old copies become garbage. With the automatic shard count at 1M entries (4096 shards), that is ~28 KB per change: under 1% of a core at 1,000 changes a second. A fixed 256 shards made it 370 KB and 11%.
- **Reloads.** A reload that keeps unchanged shards allocates only the shards that changed (17 MB for a 1M-entry reload changing one entry). Without a way to compare values it allocates a whole new copy, created at its final size.
- **Holding the data.** Every GC cycle marks the whole live heap, dataset included. With the application allocating 100 MB/s, holding 100k–1M entries cost 1–5% of a core, the same as holding them in a plain map. Go sizes its heap goal from the live heap, so a larger dataset means fewer, larger collections.

Pauses stayed under 1 ms in every measurement.

### Tuning the GC for a large dataset

Go starts a collection when the heap has grown by `GOGC` percent (default 100) over the live heap of the last one. A dataset is long-lived, so it raises the live heap and with it the point where the next collection starts:

- **Without a memory limit**, a 150 MB dataset lets the heap grow to about 300 MB before collecting. Collections are rarer, but the process uses about twice the dataset's size. That is usually fine and needs no tuning.
- **In a container with a memory limit**, set `GOMEMLIMIT` to about 90% of the container's limit. The GC then works harder only when the heap approaches it, instead of letting the process be killed. Keep the limit well above twice the dataset (or 1.2× with diffed reloads), or the GC will run continuously.
- **If GC CPU is high and memory is plentiful**, raise `GOGC` (for example 200). Each collection still marks the whole dataset, but they come half as often.
- **Don't lower `GOGC` to save memory** with a large dataset: every collection marks it, so collections become frequent and expensive. Use `GOMEMLIMIT` instead.

Measure before tuning: `GODEBUG=gctrace=1` prints each collection, and `runtime/metrics` reports GC CPU (`/cpu/classes/gc/total:cpu-seconds`). `Stats()` shows refset's side: `ShardsCopied`, `EntriesCopied` and `ShardsKept` say how much each change and reload copies.

## Choosing `Options.Shards`

Leave it at 0. refset then chooses about 2.5 × √entries, rounded up to a power of two, and at most 512 below 200,000 entries:

| Entries | Automatic `Shards` |
|---|---|
| 1,000 | 128 |
| 10,000 | 256 |
| 100,000 | 512 |
| 200,000 | 2,048 |
| 1,000,000 | 4,096 |
| 16,000,000 and up | 16,384 – 65,536 |

It is chosen at `Open` from the loaded size, and again on a full reload when the size has moved enough to want 4× more or fewer shards. `Stats().Shards` reports the current count.

Why the rule: fewer shards make each change copy more data; more shards make the per-change copy of the shard slice (8 bytes per shard) larger. 2.5 × √entries balances the two. The cap applies while a dataset largely fits in the CPU caches: there, more shard maps slow reads (at 100k entries, 1024 shards read 7.5% slower than 256, 512 within noise), while the larger copies cost little. From about 250k entries reads come from main memory and the shard count no longer affects them.

Set `Shards` only to fix the count, for example to compare configurations.

## Memory budget

- **Steady state:** about the size of a plain Go map with the same contents. At 1M entries with string keys and a 24-byte value, 101 bytes per entry for both.
- **During a reload:** with comparable values (or `ReloadSource.Equal`), the current version plus the shards that changed, and briefly the keys of unchanged ones (a reload changing one entry peaked at 1.12× the steady heap at 1M entries). Otherwise two copies, briefly.
- **Per change:** the touched shards are copied and the old ones become garbage once no reader holds them. At 1M entries (4096 automatic shards), a one-key change produced about 28 KB of garbage.
- **Snapshots you hold** keep their version's shards alive. Don't keep `Snapshot` values around longer than a request.

## Maintainer rules

When changing the core:

1. Nothing on the read path (`Get`, `Snapshot`, `GetFresh`, satisfied `GetAtLeast`/`GetCtx`) may write to shared memory or allocate. `TestReadPathDoesNotAllocate` checks the allocation half; reviewers check the other.
2. New fields in `Dataset` go in the group that matches who writes them (read-hot, loop-written, waiter state). `TestDatasetFieldGroupsOnSeparateCacheLines` keeps the groups 128 bytes apart.
3. Re-run `BenchmarkGetParallelWhilePolling` and the comparisons in `bench/` before a release, and update [benchmarks.md](benchmarks.md) if anything moves.
