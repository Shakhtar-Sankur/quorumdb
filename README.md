# quorumdb

A distributed SQL database, built one proven layer at a time, in Rust, with
zero dependencies.

The goal is Spanner-class architecture: a crash-safe storage engine, Raft
replication, automatic sharding, distributed transactions, and a SQL layer
that speaks the Postgres wire protocol. Every layer is tested the way
FoundationDB is tested: deterministic simulation, where a single seed
reproduces an entire run of random work, random crashes and random disk
failures, exactly.

**Status: milestone 1 of 8, the single-node storage engine, is complete.**

## The number that matters

```
$ quorumdb sim --seeds 10000 --steps 2000
ok: 10000 seeds x 2000 steps in 70.3s
  14001004 writes, 799332 crashes (111342 torn writes), 1301528 flushes, 590782 compactions
  926783 unsynced writes lost as allowed, 0 durable writes lost
```

Fourteen million writes and eight hundred thousand simulated power losses,
a seventh of them tearing the last write in half. Not one acknowledged
durable write was lost, reordered, or corrupted.

## A simulator that has never failed proves nothing

So the test suite plants three real durability bugs and requires the
simulator to catch every one:

| Planted bug | What the simulator found |
|---|---|
| Recovery does not truncate a torn log tail | The next crash loses durable keys hidden behind the garbage |
| The directory is never synced after a rename | After a crash, every durable key is gone |
| A table is not synced before the manifest names it | Recovery finds a table that fails its checksum |

```
$ cargo test --release --test simulation every_planted_bug_is_caught -- --nocapture
```

## What milestone 1 contains

- **Write-ahead log.** Every mutation is appended, with a CRC-32, before it
  is applied or acknowledged. Recovery replays it and stops at the first
  torn or corrupt record.
- **Memtable and SSTables.** Writes land in a sorted in-memory table that is
  flushed to immutable, checksummed, sorted files, which compaction merges.
- **Atomic manifest.** The set of live files changes only through
  write-temporary, sync, rename, sync-directory.
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
   sync-dir. The old log is removed only after that.
4. Recovery truncates a torn log tail before anything new is appended.

The simulator checks, after every crash, that the recovered state equals
the model after some prefix of the acknowledged writes, and that the prefix
includes everything the engine promised was durable.

## Run it

```
cargo test --release                     # unit, simulation and real-disk tests
cargo run --release -- sim --seeds 1000  # the crash simulator
cargo run --release -- shell ./data      # an interactive shell on a real directory
```

A failing seed prints the command that replays it exactly.

## Roadmap

| # | Milestone | Status |
|---|---|---|
| 1 | Crash-safe single-node storage engine and deterministic simulator | done |
| 2 | Block-indexed tables read from disk, bloom filters, leveled compaction | next |
| 3 | Raft replication, with network partitions and clock skew in the simulator | |
| 4 | Range sharding with automatic splits and rebalancing | |
| 5 | Distributed transactions: snapshot isolation, then serializable | |
| 6 | SQL: parser, planner, executor, Postgres wire protocol | |
| 7 | TLA+ model of the transaction protocol; TPC-C benchmarks | |
| 8 | A time-travel debugger: replay any seed and step through the cluster | |

## Honest limits today

- Single node only. Replication starts at milestone 3.
- Tables are loaded fully into memory; reading blocks from disk is milestone 2.
- The simulator models power loss and torn writes. It does not yet model
  disks that lie about syncs, or silent corruption of already-synced data.
