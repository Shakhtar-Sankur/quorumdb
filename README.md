# quorumdb

[![ci](https://github.com/Shakhtar-Sankur/quorumdb/actions/workflows/ci.yml/badge.svg)](https://github.com/Shakhtar-Sankur/quorumdb/actions/workflows/ci.yml)

A distributed SQL database, built one proven layer at a time, in Rust, with
zero dependencies.

The goal is Spanner-class architecture: a crash-safe storage engine, Raft
replication, automatic sharding, distributed transactions, and a SQL layer
that speaks the Postgres wire protocol. Every layer is tested the way
FoundationDB is tested: deterministic simulation, where a single seed
reproduces an entire run of random work, random crashes and random disk
failures, exactly.

**Status: milestones 1 and 2 of 8 are complete: a crash-safe, leveled LSM
storage engine with block-indexed tables and bloom filters.**

## The number that matters

```
$ quorumdb sim --seeds 10000 --steps 2000
ok: 10000 seeds x 2000 steps in 175.9s
  14001068 writes, 799255 crashes (117234 torn writes), 1189891 flushes
  791267 compactions (304768 trivial moves), 1833137 tombstones dropped, tables reached level 6
  957589 unsynced writes lost as allowed, 0 durable writes lost
```

Every seed picks its own tiny block, table and level sizes, so a run of two
thousand steps pushes tables through up to six levels of compaction while
the power fails around it. Not one acknowledged durable write was lost,
reordered, corrupted, or brought back from the dead.

## A simulator that has never failed proves nothing

So the test suite plants four real bugs and requires the simulator to catch
every one:

| Planted bug | What the simulator found |
|---|---|
| Recovery does not truncate a torn log tail | The next crash loses durable keys hidden behind the garbage |
| The directory is never synced after a rename | After a crash, every durable key is gone |
| A table is not synced before the manifest names it | Recovery finds a table that fails its checksum |
| Compaction drops a deletion while a deeper level still holds the key | Deleted keys come back from the dead |

```
$ cargo test --release --test simulation every_planted_bug_is_caught -- --nocapture
```

## Benchmarks

One million keys with 100-byte values, default options, on a 4-core cloud
container. Numbers are from `quorumdb bench`, which CI also runs on every
push.

```
$ quorumdb bench ./bench-db
fill, random order         152957 ops/s  (6.5s)
read, present keys         109391 ops/s  (p50 7.8µs, p99 27.9µs)
read, absent keys          609844 ops/s  (bloom filters skipped 99.2% of table probes)
full scan                 2503521 ops/s
write + fsync                4251 ops/s  (235.2µs each)
write amplification           4.9x  (31 flushes, 45 compactions, 12 trivial moves)
L0: 3 tables, 12197091 bytes
L1: 5 tables, 9829324 bytes
L2: 49 tables, 104011674 bytes
```

- **Point reads** check the memtable, then level 0, then at most one table
  per deeper level, and read exactly one 4 KB block from each table they
  cannot rule out.
- **Bloom filters** at 10 bits per key let a lookup skip a table without
  touching disk. Absent keys are chosen inside every table's key range, so
  only the filters can rule tables out.
- **Write amplification** counts every byte flushed and every byte
  rewritten by compaction, per byte the user wrote.

## What is inside

- **Write-ahead log.** Every mutation is appended, with a CRC-32, before it
  is applied or acknowledged. Recovery replays it and stops at the first
  torn or corrupt record.
- **Block-indexed tables.** Immutable sorted files made of checksummed
  4 KB blocks, a block index, a bloom filter, and a checksummed footer.
  Opening a table reads only its footer, index and filter; a lookup reads
  one block. A test flips every bit of a table, one at a time, and every
  flip is detected.
- **Leveled compaction,** as in LevelDB and RocksDB. Level 0 holds fresh,
  overlapping flushes; each deeper level is one sorted run of disjoint
  tables, ten times larger than the level above. Compaction merges a table
  with the tables it overlaps one level down, sweeping the key space
  round-robin, and moves a table down without rewriting it when nothing
  overlaps.
- **Streaming k-way merge.** Scans and compactions hold one block per table
  in memory, never a whole table.
- **Atomic manifest.** The set of live files and their levels changes only
  through write-temporary, sync, rename, sync-directory.
- **Two durability modes.** `Always` syncs every write; `Manual` syncs on
  request and may lose unsynced writes in a crash, but only from the end,
  never out of order.
- **A simulated disk.** `SimFs` implements the same interface as the real
  filesystem. On a crash it reverts every name to its last directory sync and
  every file to its last sync, keeps a random prefix of unsynced appends, and
  may flip a bit in the last byte that survives.

## The durability contract

1. A write is appended to the log before it is applied or acknowledged.
2. A table is synced before any manifest names it.
3. A new manifest becomes current only through write-temp, sync, rename,
   sync-dir. Old logs and tables are removed only after that.
4. Recovery truncates a torn log tail before anything new is appended.
5. Compaction drops a deletion only when no deeper level could hold an
   older value for its key.

The simulator checks, after every crash, that the recovered state equals
the model after some prefix of the acknowledged writes, and that the prefix
includes everything the engine promised was durable. It checks every read
against the model too.

## Run it

```
cargo test --release                     # unit, simulation and real-disk tests
cargo run --release -- sim --seeds 1000  # the crash simulator
cargo run --release -- bench ./bench-db  # benchmarks on a real, empty directory
cargo run --release -- shell ./data      # an interactive shell: put, get, del, scan, levels
```

A failing seed prints the command that replays it exactly.

## Roadmap

| # | Milestone | Status |
|---|---|---|
| 1 | Crash-safe single-node storage engine and deterministic simulator | done |
| 2 | Block-indexed tables read from disk, bloom filters, leveled compaction, CI, benchmarks | done |
| 3 | Raft replication, with network partitions and clock skew in the simulator | next |
| 4 | Range sharding with automatic splits and rebalancing | |
| 5 | Distributed transactions: snapshot isolation, then serializable | |
| 6 | SQL: parser, planner, executor, Postgres wire protocol | |
| 7 | TLA+ model of the transaction protocol; TPC-C benchmarks | |
| 8 | A time-travel debugger: replay any seed and step through the cluster | |

## Honest limits today

- Single node only. Replication starts at milestone 3.
- Compaction runs inline on the writing thread, so a write that triggers it
  waits for it. There is no block cache yet; reads rely on the OS page cache.
- The simulator models power loss and torn writes. It does not yet model
  disks that lie about syncs, or silent corruption of already-synced data.

## License

Apache-2.0. See [LICENSE](LICENSE).
