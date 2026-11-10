# Benchmarks

Sanity checks, not competitive claims (design §8.7). Measured on 2026-11-05.

**Machine:** Apple M1 (4 performance + 4 efficiency cores), 8 GB RAM, macOS, Go 1.27.3, `GOMAXPROCS=8`. Cache sizes: 128-byte lines, 128 KB L1 data cache per performance core, 12 MB L2 shared by the performance cores, 16 KB pages.

**Reproduce:**

```bash
cd bench && go test -run '^$' -bench . -count 3     # Get comparisons, value size
go test -run '^$' -bench 'ApplyDeltas|Open|GetParallel' .   # in the repo root
cd bench && go run ./memreport                      # memory per entry, GC pauses
make perfreport                                     # latency, GC and memory under load (about 10 minutes)
make bench-compare                                  # core benchmarks: latest release vs working tree
```

Reads use string keys (`"key-123"`) visited in a random order, so hardware prefetching doesn't flatter any implementation. "Serial" is one goroutine; "parallel" is all 8 cores (`b.RunParallel`), and its ns/op is aggregate throughput (total time ÷ total reads), not per-read latency.

## Reads compared with alternatives

Mean of 3 runs, ns per read. 0 allocations everywhere.

| Entries | Mode | refset | `map` + `sync.RWMutex` | golang-lru | plain `map`, no lock (unsafe with writers) |
|---|---|---|---|---|---|
| 10,000 | serial | 24.0 | 15.9 | 29.9 | 13.5 |
| 10,000 | parallel | **4.8** | 86.9 | 167.8 | 2.7 |
| 100,000 | serial | 29.2 | 19.4 | 57.0 | 18.0 |
| 100,000 | parallel | **6.1** | 85.5 | 216.5 | 3.9 |
| 1,000,000 | serial | 135.6 | 99.5 | 199.6 | 92.6 |
| 1,000,000 | parallel | **26.1** | 89.3 | 348.9 | 19.4 |

What this shows:

- **Under concurrency, refset scales and lock-based maps don't.** With 8 cores reading, refset is 3–18× faster than a map behind a `sync.RWMutex`, whose shared reader counter becomes a contended cache line, and 13–35× faster than golang-lru. It stays within about 1.8× of an unsynchronized map, which isn't safe once anything writes.
- **Single-threaded, refset costs about 8–36 ns more than a locked map.** Each read hashes the key twice (once to pick a shard, once inside the shard's map) and follows one more pointer. That is the price of copy-on-write shards; removing the double hash is a post-v0.1 idea.
- **At 1M entries, reads are memory-bound** (rule R4): 93–200 ns single-threaded for every implementation, because random keys miss the 12 MB L2 and go to main memory. 10k and 100k entries stay largely in cache.

## Reads while the background loop is busy

`BenchmarkGetParallelWhilePolling`: 6 reader goroutines while the loop is forced to poll about 363,000 times a second (a flood of requests for a version beyond the head).

| Loop | ns per read |
|---|---|
| Idle | 5.48 |
| Polling ~363k/s | 5.57 |

No measurable interference. The `Dataset` fields are padded so the loop's writes stay off the cache lines readers use (rule R2), and a test keeps it that way.

## Value size

100,000 entries, ns per read, mean of 3 runs.

| Value type | serial | parallel |
|---|---|---|
| 16-byte struct | 30.6 | 6.4 |
| 256-byte struct, stored inline | 232.2 | 32.2 |
| pointer to a 256-byte struct | 88.4 | 13.3 |

Go maps store values inline, and `Get` returns a copy of the value. A 256-byte value spreads each map slot over several cache lines and copies 256 bytes per read: 7.5× slower than a small value. Storing a pointer instead is 2.6× faster (rule R5). **Keep values small, or store pointers to large ones** (and treat what they point to as read-only).

## Applying changes (copy-on-write)

Time and garbage to apply one batch, by dataset size, shard count and keys changed (`BenchmarkApplyDeltas`, rule R6).

| Entries | Shards | 1 key: time | 1 key: garbage | 100 keys: time | 100 keys: garbage |
|---|---|---|---|---|---|
| 10,000 | 16 | 4.9 µs | 27 KB | 87 µs | 438 KB |
| 10,000 | **256** (default) | **1.0 µs** | 4 KB | 40 µs | 157 KB |
| 10,000 | 4096 | 9.1 µs | 37 KB | 23 µs | 62 KB |
| 100,000 | 16 | 38 µs | 219 KB | 602 µs | 3.5 MB |
| 100,000 | 256 (default) | 3.5 µs | 16 KB | 236 µs | 1.1 MB |
| 100,000 | 4096 | 8.6 µs | 38 KB | 56 µs | 157 KB |
| 1,000,000 | 16 | 475 µs | 3.5 MB | 7.7 ms | 56 MB |
| 1,000,000 | 256 (default) | 32 µs | 221 KB | 2.6 ms | 18 MB |
| 1,000,000 | **4096** | **8.5 µs** | 50 KB | **263 µs** | 1.3 MB |

A batch copies the shard slice (8 bytes per shard) plus every shard it touches (entries ÷ shards each). Too few shards make each copy big; too many make the slice copy dominate small batches. **A fixed 256 is right around 10k–100k entries but about 10× too few at 1M entries.** These measurements led to the automatic shard count (`Options.Shards` 0): about 2.5 × √entries rounded up to a power of two, capped at 512 below 200k entries to keep reads fast, so 4096 at 1M ([performance](performance.md#choosing-optionsshards)).

## Startup (full load)

`BenchmarkOpen`, string keys and int values, from an in-memory source.

| Entries | `Open` time | Allocated |
|---|---|---|
| 10,000 | 0.8 ms | 0.9 MB |
| 100,000 | 7.7 ms | 7.0 MB |
| 1,000,000 | 159 ms | 112 MB |

This is the in-process cost only. A real source adds its own read time: transferring and scanning 1M rows from Postgres takes longer than building the maps.

## Memory and GC at 1M entries

`go run ./memreport`: 1,000,000 entries with string keys and a 24-byte value, keys shared with the source.

| | Bytes per entry (map storage) | GC pause p50 | GC pause max |
|---|---|---|---|
| `map[string]plan` | 101 | 13.0 µs | 64.5 µs |
| refset, 256 shards | 101 | 22.7 µs | 44.0 µs |

Sharding adds no measurable memory per entry, and GC pauses stay in the tens of microseconds. Remember that a reload briefly holds two copies (rule R7).

## Latency, GC and memory under load

`make perfreport` (`bench/perfreport`) measures the three axes the [roadmap](roadmap.md) tracks, next to a map behind a `sync.RWMutex` that receives the same writes. Measured on 2026-11-11, same machine, default options (256 shards, 5 ms `BatchWindow`), string keys and a 24-byte value. Writes go through a minimal in-memory source, so the numbers show refset's costs rather than the source's.

### Read latency

4 reader goroutines time batches of 16 random reads for 3 s per cell; percentiles are per batch, so p50 ÷ 16 is the typical cost of one read and any stall (GC pause, lock wait, cache flush after a swap) shows in full in the tail. "1,000 changes/s" writes one key per change; "full reload every 1s" makes every change a full reload, as `ReloadSource` does.

| Entries | Load | Store | p50 | p99 | p99.9 | per read (p50) |
|---|---|---|---|---|---|---|
| 10k | idle | refset | 416 ns | 528 ns | 2.7 µs | 26 ns |
| 10k | idle | RWMutex map | 3.2 µs | 7.4 µs | 15.9 µs | 200 ns |
| 10k | 1,000 changes/s | refset | 456 ns | 1.2 µs | 2.9 µs | 28 ns |
| 10k | 1,000 changes/s | RWMutex map | 3.3 µs | 7.7 µs | 30.2 µs | 204 ns |
| 10k | full reload every 1s | refset | 416 ns | 1.3 µs | 2.9 µs | 26 ns |
| 10k | full reload every 1s | RWMutex map | 3.3 µs | 6.4 µs | 15.6 µs | 204 ns |
| 100k | idle | refset | 528 ns | 3.9 µs | 6.9 µs | 33 ns |
| 100k | idle | RWMutex map | 3.1 µs | 7.3 µs | 16.9 µs | 196 ns |
| 100k | 1,000 changes/s | refset | 576 ns | 3.4 µs | 5.6 µs | 36 ns |
| 100k | 1,000 changes/s | RWMutex map | 3.2 µs | 12.3 µs | 58.4 µs | 200 ns |
| 100k | full reload every 1s | refset | 576 ns | 3.5 µs | 7.2 µs | 36 ns |
| 100k | full reload every 1s | RWMutex map | 3.2 µs | 8.4 µs | 27.1 µs | 200 ns |
| 1M | idle | refset | 3.0 µs | 7.3 µs | 27.6 µs | 188 ns |
| 1M | idle | RWMutex map | 3.3 µs | 7.4 µs | 16.9 µs | 204 ns |
| 1M | 1,000 changes/s | refset | 2.9 µs | 7.0 µs | 15.9 µs | 184 ns |
| 1M | 1,000 changes/s | RWMutex map | 3.3 µs | 9.7 µs | 32.8 µs | 208 ns |
| 1M | full reload every 1s | refset | 2.9 µs | 6.9 µs | 15.9 µs | 184 ns |
| 1M | full reload every 1s | RWMutex map | 3.4 µs | 9.5 µs | 16.9 µs | 212 ns |

- **Writes and reloads don't move refset's read tail.** Up to 100k entries, refset's p99.9 stays at 3–7 µs per 16 reads whatever the load. The locked map's p99.9 doubles or more under writes (16.9 → 58.4 µs at 100k), because readers queue behind each write lock.
- **With 4 readers, the locked map costs ~200 ns per read at every size**: the shared reader count is contended (the [comparison above](#reads-compared-with-alternatives) shows the same with 8 cores). refset reads cost 26–36 ns up to 100k entries.
- **At 1M entries both are memory-bound** (~185–210 ns per read), and refset no longer has a clear edge on the median.
- The maximum of a cell is a single sample over 3 s and comes mostly from OS scheduling; reruns of the same cell ranged from 0.2 to 80 ms for both stores, so it is left out.

### Propagation latency

100k entries, 200 changes per case, each followed by a random gap so changes don't line up with polls. Time from the source committing a change until the replica serves it.

| Path | p50 | p99 | max |
|---|---|---|---|
| Notification (default `BatchWindow` 5 ms) | 5.0 ms | 5.4 ms | 7.0 ms |
| Read-your-writes: `GetAtLeast(version)` | 76 µs | 983 µs | 6.4 ms |
| Polling only, `PollInterval` 100 ms | 42 ms | 99 ms | 100 ms |
| Notification, full reload per change | 15.5 ms | 17.8 ms | 19.6 ms |

- **A notified change waits the whole batch window.** The poll itself takes well under 0.1 ms, so the fixed 5 ms `BatchWindow` is nearly all of the delay when changes arrive one at a time.
- **Read-your-writes is fast, with a tail from the same window.** A request asking for a version above the known head starts a poll at once (76 µs). When the notification has already raised the known head, the request doesn't start a poll and waits for the batch timer: that is the p99 and max.
- **Polling alone** takes half a poll interval on average, as expected.
- **A full reload per change** at 100k entries costs the batch window plus about 10 ms of loading.

### Garbage collection

No readers; each cell runs 5 s. GC CPU is the runtime's estimate (`runtime/metrics`) excluding idle-time marking, as a share of one core; pauses are histogram bucket upper bounds.

| Entries | Writes | Store | GC CPU | GC cycles/s | Allocated/s | Pause p99 |
|---|---|---|---|---|---|---|
| 100k | 10/s | refset | 0.0% | 0.0 | 225 KB | 0 |
| 100k | 100/s | refset | 0.2% | 0.2 | 2.3 MB | 115 µs |
| 100k | 1,000/s | refset | 1.8% | 1.8 | 21.1 MB | 131 µs |
| 100k | 1/s, full reload each | refset | 0.4% | 0.6 | 10.6 MB | 82 µs |
| 100k | 1/s, full reload each | RWMutex map | 0.4% | 0.4 | 7.2 MB | 82 µs |
| 1M | 10/s | refset | 0.0% | 0.0 | 3.7 MB | 0 |
| 1M | 100/s | refset | 1.0% | 0.2 | 37.7 MB | 131 µs |
| 1M | 1,000/s | refset | 11.3% | 2.4 | 370.5 MB | 197 µs |
| 1M | 1/s, full reload each | refset | 3.9% | 1.0 | 190.1 MB | 82 µs |
| 1M | 1/s, full reload each | RWMutex map | 2.6% | 0.6 | 113.4 MB | 82 µs |

The locked map updated in place allocates nothing at any write rate, so it has no GC cost (rows omitted), and with no writes neither store allocates.

- **Each change costs a copy of the shards it touches.** With the default 256 shards, a one-key change at 1M entries copies a ~4k-entry shard (~370 KB of garbage). At 1,000 changes/s that is 370 MB/s and 11% of a core in GC; at 100k entries, 21 MB/s and 1.8%. Pauses stay under 0.2 ms.
- **A full reload allocates 1.7× more than building one map** (190 vs 113 MB per reload at 1M): refset grows 256 shard maps from empty, while the rebuilt map is created at its final size.

Holding a dataset also costs the GC when the application allocates: every cycle the application's garbage triggers marks the whole live heap, the dataset included. Here the application allocates 100 MB/s and the dataset receives no writes:

| Entries | Held | GC CPU | GC cycles/s | Pause p99 |
|---|---|---|---|---|
| 100k | nothing | 2.0% | 29.4 | 164 µs |
| 100k | refset | 6.8% | 9.0 | 229 µs |
| 100k | RWMutex map | 7.1% | 8.4 | 262 µs |
| 1M | nothing | 1.9% | 29.4 | 197 µs |
| 1M | refset | 3.8% | 0.8 | 393 µs |
| 1M | RWMutex map | 2.8% | 0.6 | 98 µs |

- **Holding the data costs 1–5% of a core** at this allocation rate, the same for refset and a map. Go sizes its heap goal from the live heap, so a larger dataset means fewer, bigger collections rather than more work per second.

### Memory

Bytes per entry are the live heap after GC, counting dataset storage only (keys and pointed-to values are shared with the source).

| Entries | Key, value | refset | map | refset ÷ map |
|---|---|---|---|---|
| 10k | string, 24-byte struct | 72 B | 79 B | 0.91 |
| 10k | int64, int64 | 32 B | 30 B | 1.09 |
| 10k | string, pointer to struct | 47 B | 44 B | 1.09 |
| 100k | string, 24-byte struct | 56 B | 63 B | 0.89 |
| 100k | int64, int64 | 25 B | 24 B | 1.04 |
| 100k | string, pointer to struct | 35 B | 35 B | 1.00 |
| 1M | string, 24-byte struct | 100 B | 101 B | 1.00 |
| 1M | int64, int64 | 38 B | 38 B | 1.00 |
| 1M | string, pointer to struct | 56 B | 56 B | 1.00 |

Heap during one full reload: steady is the whole process after GC; peak is the highest heap sampled every millisecond until the new version is served, garbage included. Reload time includes the 5 ms batch window.

| Entries | Steady heap | Peak during reload | Peak ÷ steady | Reload time |
|---|---|---|---|---|
| 100k | 10.9 MB | 21.0 MB | 1.92 | 13.4 ms |
| 1M | 149.5 MB | 282.7 MB | 1.89 | 203.4 ms |

- **Sharding costs no memory**: refset's storage is within 10% of a plain map at every size, sometimes smaller (smaller maps leave less unused space when they grow).
- **A reload nearly doubles the heap** until the new version is served (rule R7). At 10k entries the reload finishes between samples, so it isn't listed.

### What this means for the roadmap

| Axis | Finding | Next step |
|---|---|---|
| Read latency | Already flat under writes and reloads; memory-bound at 1M | No change needed for latency alone |
| Propagation | The fixed 5 ms batch window is almost all of the delay, and adds a tail to read-your-writes | Adaptive batch window |
| GC, write path | At 1M entries the default shard count makes each change ~370 KB of garbage: 11% of a core at 1,000 changes/s | Automatic shard count |
| GC, reloads | Reloads allocate 1.7× a presized map and peak at 1.9× the heap | Pre-size shards on reload; diff-on-reload |
| GC, holding data | 1–5% of a core at 100 MB/s of application allocation, same as a map | Pointer-free storage would save at most this; keep it gated |
| Memory | Same as a map; reload peak is the only overhead | Diff-on-reload |

## After the P1 changes

The same `make perfreport` run on the P1 code (automatic shard count, adaptive batch window, pre-sized and diffed reloads), 2026-11-11, same machine and defaults. Only rows that changed are shown; the tables above are the baseline.

| Measure | Before (P0) | After (P1) |
|---|---|---|
| Notification → visible, p50 / max | 5.0 ms / 7.0 ms | 70 µs / 179 µs |
| Read-your-writes `GetAtLeast`, p50 / max | 76 µs / 6.4 ms | 88 µs / 338 µs |
| Full reload per change, p50 (100k) | 15.5 ms | 11.0 ms |
| 1M entries, 1,000 changes/s: allocated, GC CPU | 370 MB/s, 11.3% | 27.9 MB/s, 0.8% |
| 100k entries, 1,000 changes/s: allocated, GC CPU | 21.1 MB/s, 1.8% | 11.8 MB/s, 1.0% |
| 1M entries, full reload (one entry changed): allocated, GC CPU | 190 MB, 3.9% | 16.8 MB, 0.0% |
| 100k entries, full reload (one entry changed): allocated | 10.6 MB | 1.6 MB |
| Heap peak during a reload, ÷ steady (1M / 100k) | 1.89 / 1.92 | 1.12 / 1.14 |
| Bytes per entry, 1M, string key + 24-byte struct | 100 B | 90 B |
| Read p50 per read, 10k / 100k / 1M | 26 / 33–36 / 184–188 ns | 28 / 36–39 / 184 ns |

- **Propagation** no longer waits for the batch window when changes arrive one at a time, and read-your-writes no longer waits for it in the tail.
- **Write garbage** at 1M entries fell 13× because the automatic shard count is 4096 there. At 100k it is capped at 512, which halves the garbage while keeping reads as fast as with 256.
- **Reloads that change little** keep every unchanged shard. What remains (16.8 MB at 1M) is mostly the list of matching keys kept while loading, freed when the reload finishes.
- **Reads** are unchanged within noise. An earlier rule without the cap (1024 shards at 100k) made 100k-entry reads 7.5% slower:

| 100k entries, one core | 256 shards | 512 | 1024 | 4096 |
|---|---|---|---|---|
| ns per read | 32.0 | 32.4 | 34.2 | 39.9 |

At 250k and 500k entries, 256, 1024 and 2048 shards read within noise of each other (97–98 ns and 127–130 ns): reads there come from main memory.

`Open` also got faster: `make bench-compare` against v0.1.0 shows 38–40% less time and 10–28% fewer bytes allocated at 100k and 1M entries, because shards are created at their final size.

## Soak test

`REFSET_SOAK=1h go test -race -run TestSoak -timeout 2h ./internal/scenario/`: three replicas follow one source for an hour while a writer commits about 200 versions a second and a chaos loop partitions replicas, drops their notifications, slows them, compacts the source and restarts replicas. Every minute, faults heal and each replica must match the source exactly.

Result on 2026-11-05 (same machine, race detector on): **60 of 60 checks matched**, over 718,508 versions, 1,177 replica restarts and 1,186 forced reloads. Goroutines stayed at 10 and the live heap at 0.8–1.0 MiB, with no growth trend.
