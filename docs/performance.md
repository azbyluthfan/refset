# Performance and Memory

How refset uses memory and CPU caches, what that means for your data, and the rules maintainers keep. Measurements are in [benchmarks.md](benchmarks.md); the reasoning is in [design §4.11](design.md#411-memory-and-cpu-caches).

## The short version

- Reads are local and lock-free: about 25 ns on one core for a 10k-entry dataset, and they scale across cores (8 cores: one read every ~5 ns in aggregate). A map behind a `sync.RWMutex` stops scaling at about 87 ns per read.
- Large datasets are bound by main memory: at 1M entries a random read costs ~135 ns on one core.
- Keep values small, or store pointers to large ones.
- For datasets over ~100k entries, raise `Options.Shards`.
- The dataset must fit in memory twice, because a reload builds a new copy before swapping.

## How the CPU caches your data

Code can't choose what the CPU caches. The hardware loads whole cache lines (64 bytes on x86, 128 on Apple Silicon) when memory is used, keeps them per core (L1, usually L2) or shared (L3), evicts the least recently used within small address-determined sets, prefetches only predictable access patterns, and invalidates a line in every other core's cache when one core writes to it. What code controls is layout and access patterns. refset follows these rules:

| # | Rule | What it means for you |
|---|---|---|
| R1 | The read path never writes shared memory and never allocates | Nothing to do; tests enforce it. Reads don't slow each other down |
| R2 | Fields readers use are on different cache lines from fields the background loop writes | Nothing to do; a layout test enforces it. Measured: no read slowdown while the loop polls 363k times a second |
| R3 | The fixed read-path overhead stays small (the shard slice is 8 bytes per shard: 2 KB at 256, 32 KB at 4096) | Nothing to do |
| R4 | Datasets larger than the CPU's L2/L3 cache are read from main memory | Expect ~25–30 ns per read up to ~100k entries and ~135 ns at 1M (on an M1). Still far faster than any network call |
| R5 | Go maps store values inline, and `Get` copies the value | **Keep values small.** A 256-byte value read 7.5× slower than a 16-byte one; storing `*V` for large values was 2.6× faster. Pointers add a hop per field they reach, so flat structs are best |
| R6 | Each change copies every shard it touches | **Size shards to the dataset.** The default 256 suits 10k–100k entries; use about 1024 at 100k and 4096 at 1M (≈ 2.5 × √entries). At 1M entries, 4096 shards made a 100-key update 10× faster and 14× less garbage than 256 |
| R7 | A reload builds the new copy beside the old one | **Budget memory for two copies.** At about 100 bytes per entry (string key, small value), 1M entries is ~100 MB, so ~200 MB during a reload |
| R8 | Hash maps defeat prefetching and cost GC scanning | A flat, pointer-light layout is a post-v0.1 option for very large datasets. GC pauses at 1M entries were already only tens of microseconds |

## Choosing `Options.Shards`

| Entries | Suggested `Shards` |
|---|---|
| up to ~50k | 256 (default) |
| ~100k | 1024 |
| ~1M | 4096 |
| more | 2.5 × √entries, rounded up to a power of two (max 65,536) |

Fewer shards make each change copy more data. More shards make the per-change copy of the shard slice larger, which matters for tiny datasets. Automatic sizing from the snapshot at `Open` is planned.

## Memory budget

- **Steady state:** about the size of a plain Go map with the same contents. At 1M entries with string keys and a 24-byte value, 101 bytes per entry for both.
- **During a reload:** two copies, briefly.
- **Per change:** the touched shards are copied and the old ones become garbage once no reader holds them. At 1M entries and 4096 shards, a one-key change produced about 50 KB of garbage.
- **Snapshots you hold** keep their version's shards alive. Don't keep `Snapshot` values around longer than a request.

## Maintainer rules

When changing the core:

1. Nothing on the read path (`Get`, `Snapshot`, `GetFresh`, satisfied `GetAtLeast`/`GetCtx`) may write to shared memory or allocate. `TestReadPathDoesNotAllocate` checks the allocation half; reviewers check the other.
2. New fields in `Dataset` go in the group that matches who writes them (read-hot, loop-written, waiter state). `TestDatasetFieldGroupsOnSeparateCacheLines` keeps the groups 128 bytes apart.
3. Re-run `BenchmarkGetParallelWhilePolling` and the comparisons in `bench/` before a release, and update [benchmarks.md](benchmarks.md) if anything moves.
