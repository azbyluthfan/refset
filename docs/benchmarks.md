# Benchmarks

Sanity checks, not competitive claims (design §8.7). Measured on 2026-11-05.

**Machine:** Apple M1 (4 performance + 4 efficiency cores), 8 GB RAM, macOS, Go 1.27.3, `GOMAXPROCS=8`. Cache sizes: 128-byte lines, 128 KB L1 data cache per performance core, 12 MB L2 shared by the performance cores, 16 KB pages.

**Reproduce:**

```bash
cd bench && go test -run '^$' -bench . -count 3     # Get comparisons, value size
go test -run '^$' -bench 'ApplyDeltas|Open|GetParallel' .   # in the repo root
cd bench && go run ./memreport                      # memory per entry, GC pauses
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

A batch copies the shard slice (8 bytes per shard) plus every shard it touches (entries ÷ shards each). Too few shards make each copy big; too many make the slice copy dominate small batches. **The default of 256 is right around 10k–100k entries but about 10× too few at 1M entries.** For large datasets, set `Options.Shards`: roughly 1024 at 100k entries and 4096 at 1M (about 2.5 × √entries, rounded up to a power of two). Choosing it automatically from the dataset size is planned.

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

## Soak test

`REFSET_SOAK=1h go test -race -run TestSoak -timeout 2h ./internal/scenario/`: three replicas follow one source for an hour while a writer commits about 200 versions a second and a chaos loop partitions replicas, drops their notifications, slows them, compacts the source and restarts replicas. Every minute, faults heal and each replica must match the source exactly.

Result on 2026-11-05 (same machine, race detector on): **60 of 60 checks matched**, over 718,508 versions, 1,177 replica restarts and 1,186 forced reloads. Goroutines stayed at 10 and the live heap at 0.8–1.0 MiB, with no growth trend.
