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

**Status: milestones 1 to 4 of 8 are complete: a crash-safe LSM storage
engine, Raft consensus, and a sharded multi-Raft key-value store that splits
and rebalances itself, with every history checked for linearizability.**

## The numbers that matter

The sharded key-value store: 1,500 simulated clusters, each with 3 to 5
nodes on crashing disks, 10 seconds of simulated time plus recovery:

```
$ quorumdb kv-sim --from 300 --seeds 1500
ok: 1500 clusters x 10s simulated in 236.3s
  84358249 messages (2777240 lost), 11939 crashes, 10018 partitions
  13453 ranges after splits, 21402 replica moves
  1651803 writes, 1645962 reads, 21661 with unknown outcome; 102027 key histories linearizable
```

Raft, 10,000 simulated clusters, each run for 10 seconds of simulated time:

```
$ quorumdb raft-sim --seeds 10000
ok: 10000 clusters x 10s simulated in 886.5s
  198745186 messages (22893424 lost), 610064 crashes, 144354 partitions, 118136 leaders elected
  53962 membership changes, 37094 leadership transfers, 282963 snapshots installed
  14289650 writes acknowledged, 5844923 linearizable reads, 0 safety violations
```

The storage engine, 10,000 seeds of random work and power loss:

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

## Raft, and three bugs the simulator found in it

The Raft core (`src/raft`) is a pure state machine in the style of etcd's:
it never touches a disk, a network or a clock. The caller feeds it ticks
and messages and drains a `Ready`: state to persist, messages to send,
entries to apply. So the simulator can run whole clusters in one thread,
deterministically, from a seed.

It implements leader election with **pre-vote** and **check-quorum**, log
replication with fast backtracking by conflicting term, the Figure 8 commit
rule, **ReadIndex** linearizable reads, **snapshots and log compaction**,
**single-server membership changes**, and **leadership transfer**.

Each simulated cluster has 3 or 5 voters and spare nodes to add. Each seed
picks a level of chaos. Messages are delayed, dropped, duplicated and
reordered; the network splits into random partitions; nodes crash and
restart from what they persisted; each node's clock runs at its own speed,
up to 2x apart; voters are added and removed; leadership is handed around.
Clients write and read throughout. The simulator checks after every event:

- **Election safety:** never two leaders in one term.
- **State machine safety:** never two different entries applied at one index.
- **Durability:** no acknowledged write is ever lost.
- **Linearizable reads:** a read sees every write acknowledged before it began.
- **Liveness:** 20 seconds after the chaos stops, one leader leads and
  every voter has converged.

Before the simulator ever passed, it found three real bugs, each a
liveness failure that unit tests had missed:

| What the simulator found | Cause and fix |
|---|---|
| A removed node, never told it was removed, kept calling elections and deposed every new leader | Followers now ignore vote requests while they hear from a live leader, whether or not check-quorum is on (Raft dissertation, 4.2.3) |
| A candidate with a stale log and a fast clock kept resetting the timers of everyone who rejected it, starving the one node that could win | Rejecting a vote no longer resets the election timer: only a live leader, or granting a vote, does |
| A candidate that fell behind in term sent its requests to a node with a higher term, which ignored them silently, so it never caught up | A stale vote request is answered with a rejection carrying the current term, as the paper specifies |

It also showed why pre-vote matters. With pre-vote off, a candidate that
cannot win but whose clock runs fast raises the term forever, and the node
that could win always asks one term too late. Pre-vote is always on.

And it catches four planted consensus bugs:

| Planted bug | What the simulator found |
|---|---|
| Grant votes without checking the candidate's log is up to date | Two nodes applied different entries at the same index |
| Commit an entry from an earlier term by counting replicas (Figure 8) | Two nodes applied different entries at the same index |
| Accept entries without checking the previous entry matches | Two nodes applied different entries at the same index |
| Serve a ReadIndex read without confirming leadership with a quorum | A stale read missed an acknowledged write |

```
$ cargo test --release --test raft every_planted_raft_bug_is_caught -- --nocapture
```

## A sharded, self-balancing key-value store

`src/kv` turns the pieces into a distributed system, in the shape of
CockroachDB and TiKV:

- **Ranges.** The key space is split into ranges; each is replicated by its
  own Raft group, and each node hosts replicas of many ranges.
- **One engine per node.** A node keeps every replica's Raft log, hard
  state, applied index and data in its one storage engine. All replicas'
  Raft state is persisted in one batch and **one sync** per round (group
  commit), before any message leaves. A command's data and the applied
  index are written in one atomic batch, so after a crash the state machine
  is always exactly the log applied up to that index.
- **Splits.** A split is a Raft command: when applied, the range shrinks and
  the right half becomes a new range whose replicas start from a shared
  synthetic snapshot, on every member, at the same log index.
- **Replica ids.** A node's membership in a range has an id that is never
  reused, and every descriptor carries a `next_incarnation` (like
  CockroachDB's NextReplicaID). Removed replicas leave a tombstone.
  Garbage collection is therefore safe even on stale information.
- **Placement driver.** Splits ranges past a size threshold, restores the
  replication factor, replaces replicas on dead nodes, moves replicas from
  the most to the least loaded node, and spreads leadership. It holds only
  soft state and acts only through ordinary requests, which Raft orders and
  checks, so a stale decision can delay the cluster but not corrupt it.

The cluster simulator runs real stores on simulated disks that lose unsynced
writes when they crash, over a lossy, partitioned network with clock skew,
while the placement driver splits and moves ranges and clients read and
write through it all. It then checks that:

- **Every key's history is linearizable.** A checker in the style of
  Knossos and Porcupine (Wing and Gong's algorithm with Lowe's memoization)
  must find, for each key, one order of all reads and writes that respects
  real time. Writes whose outcome the client never learned may take effect
  late, or never. This one property rules out lost writes, stale reads, and
  values flipping back.
- **Replicas agree exactly.** Every replica of a range at the same applied
  index holds byte-identical data.
- **The cluster recovers.** Once faults stop, every key is readable.

Before the simulator passed, it found two more real bugs:

| What the simulator found | Cause and fix |
|---|---|
| A read returned a value that had been overwritten three seconds earlier | The placement driver garbage-collected a replica on stale information, after the range had re-added the node: the replica had already acknowledged log entries as a voter. Fixed with never-reused replica ids and tombstones, as in CockroachDB |
| A range lost its quorum for good | A replica being added received a snapshot taken before its own addition, did not find itself in it, concluded it had been removed, and deleted itself. Fixed with `next_incarnation`: a replica is removed only if it was added and then dropped |

It catches three planted bugs:

| Planted bug | What the simulator found |
|---|---|
| Send Raft messages without first syncing the state they promise | After crashes, a range could no longer serve reads |
| Serve reads from local state without ReadIndex | A history that is not linearizable |
| Treat a snapshot that does not list you as your removal (the real bug above) | A range lost its quorum for good |

```
$ cargo test --release --test cluster every_planted_cluster_bug_is_caught -- --nocapture
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
- **Atomic write batches and range scans.** A batch is one log record,
  recovered entirely or not at all; scans seek past every table and block
  that ends before the start key.
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
cargo run --release -- sim --seeds 1000  # the storage crash simulator
cargo run --release -- raft-sim --seeds 1000  # the Raft cluster simulator
cargo run --release -- kv-sim --seeds 200     # the sharded cluster simulator
cargo run --release -- bench ./bench-db  # benchmarks on a real, empty directory
cargo run --release -- shell ./data      # an interactive shell: put, get, del, scan, levels
```

A failing seed prints the command that replays it exactly.

## Roadmap

| # | Milestone | Status |
|---|---|---|
| 1 | Crash-safe single-node storage engine and deterministic simulator | done |
| 2 | Block-indexed tables read from disk, bloom filters, leveled compaction, CI, benchmarks | done |
| 3 | Raft consensus, with partitions, crashes, clock skew and membership changes in the simulator | done |
| 4 | Multi-Raft: replicas on the storage engine, range sharding, automatic splits and rebalancing | done |
| 5 | Distributed transactions: snapshot isolation, then serializable | next |
| 6 | SQL: parser, planner, executor, Postgres wire protocol | |
| 7 | TLA+ model of the transaction protocol; TPC-C benchmarks | |
| 8 | A time-travel debugger: replay any seed and step through the cluster | |

## Honest limits today

- The cluster runs in the simulator and in tests; there is no network
  server yet (it arrives with the SQL layer in milestone 6).
- The placement driver is one soft-state process, not yet replicated, and
  ranges split but never merge.
- Snapshots are sent as one message, which suits the simulator's small
  ranges but not multi-gigabyte ones.
- Compaction runs inline on the writing thread, so a write that triggers it
  waits for it. There is no block cache yet; reads rely on the OS page cache.
- The simulator models power loss and torn writes. It does not yet model
  disks that lie about syncs, or silent corruption of already-synced data.

## License

Apache-2.0. See [LICENSE](LICENSE).
