---------------------------- MODULE Percolator ----------------------------
(***************************************************************************)
(* quorumdb's distributed transaction protocol (src/txn), as TLC checks it. *)
(*                                                                         *)
(* Percolator-style two-phase commit over MVCC, with serializable          *)
(* isolation by write-snapshot validation (Yabandeh and Gomez Ferro):      *)
(*                                                                         *)
(*  - Begin takes a start timestamp from the oracle.                       *)
(*  - Reads see the newest version committed at or below the start         *)
(*    timestamp. A lock that could hide such a version blocks the read     *)
(*    until it is resolved.                                                *)
(*  - Prewrite locks each written key, refusing if anyone committed it     *)
(*    at or after our start (first committer wins), or if a rollback       *)
(*    fence for our start is there.                                        *)
(*  - After every prewrite: a commit timestamp from the oracle, then       *)
(*    validation of every key read: no other commit in                     *)
(*    (start, commit], no other lock that could commit there.              *)
(*  - Committing the primary key is the single commit point; the other     *)
(*    keys are rolled forward (or back) by whoever meets their locks.      *)
(*  - Anyone may roll back a transaction that has not committed its        *)
(*    primary, as a lock whose time-to-live expired lets them: it removes  *)
(*    the primary lock and leaves a rollback record, a fence against a     *)
(*    late prewrite.                                                       *)
(*                                                                         *)
(* Every step is atomic at one key, as a Raft-replicated range makes it.   *)
(* The oracle is modelled as a correct counter; that it stays monotonic    *)
(* across leader crashes is checked by the simulator, not here. Point      *)
(* reads only: range scans and phantom validation are the simulator's.     *)
(*                                                                         *)
(* FAULT plants the same bugs the simulator plants, to show TLC finds      *)
(* them: "skip_write_conflict", "read_ignores_locks",                      *)
(* "skip_read_validation". "none" is the real protocol.                    *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS Keys, Txns, FAULT

VARIABLES
    clock,       \* the oracle's next timestamp
    lock,        \* lock[k]: [txn, start], or NoLock
    writes,      \* writes[k]: write records [ts, start, kind, txn]
    phase,       \* phase[t]
    start,       \* start[t]: start timestamp (0 before Begin)
    commit,      \* commit[t]: commit timestamp (0 before it has one)
    rs, ws,      \* keys t reads and writes, chosen at Begin
    readVal,     \* readVal[t]: pairs <<k, the txn whose write t saw, or "init">>
    prewritten,  \* keys t has locked
    validated    \* keys t has validated

vars == <<clock, lock, writes, phase, start, commit, rs, ws, readVal, prewritten, validated>>

NoLock == [txn |-> "none", start |-> 0]
Phases == {"idle", "reading", "prewriting", "validating", "committing", "committed", "aborted"}

Primary(t) == CHOOSE k \in ws[t] : TRUE

\* A transaction's fate is decided at its primary key, once and for all.
PrimaryCommitted(t) ==
    start[t] # 0 /\ \E r \in writes[Primary(t)] : r.start = start[t] /\ r.kind = "put"
PrimaryRolledBack(t) ==
    start[t] # 0 /\ \E r \in writes[Primary(t)] : r.start = start[t] /\ r.kind = "rollback"

\* The txn whose committed write of k a read at ts sees, or "init".
Visible(k, ts) ==
    LET puts == {r \in writes[k] : r.kind = "put" /\ r.ts <= ts}
    IN IF puts = {} THEN "init"
       ELSE (CHOOSE r \in puts : \A o \in puts : o.ts <= r.ts).txn

Init ==
    /\ clock = 1
    /\ lock = [k \in Keys |-> NoLock]
    /\ writes = [k \in Keys |-> {}]
    /\ phase = [t \in Txns |-> "idle"]
    /\ start = [t \in Txns |-> 0]
    /\ commit = [t \in Txns |-> 0]
    /\ rs = [t \in Txns |-> {}]
    /\ ws = [t \in Txns |-> {}]
    /\ readVal = [t \in Txns |-> {}]
    /\ prewritten = [t \in Txns |-> {}]
    /\ validated = [t \in Txns |-> {}]

Begin(t) ==
    /\ phase[t] = "idle"
    /\ \E R \in SUBSET Keys, W \in (SUBSET Keys) \ {{}} :
        /\ rs' = [rs EXCEPT ![t] = R]
        /\ ws' = [ws EXCEPT ![t] = W]
    /\ start' = [start EXCEPT ![t] = clock]
    /\ clock' = clock + 1
    /\ phase' = [phase EXCEPT ![t] = "reading"]
    /\ UNCHANGED <<lock, writes, commit, readVal, prewritten, validated>>

Read(t, k) ==
    /\ phase[t] = "reading"
    /\ k \in rs[t]
    /\ \A p \in readVal[t] : p[1] # k
    \* A lock from a transaction that started at or below our snapshot may
    \* be about to commit below it: wait until it is resolved.
    /\ \/ FAULT = "read_ignores_locks"
       \/ lock[k].txn = "none"
       \/ lock[k].start > start[t]
    /\ readVal' = [readVal EXCEPT ![t] = @ \cup {<<k, Visible(k, start[t])>>}]
    /\ UNCHANGED <<clock, lock, writes, phase, start, commit, rs, ws, prewritten, validated>>

DoneReading(t) ==
    /\ phase[t] = "reading"
    /\ {p[1] : p \in readVal[t]} = rs[t]
    /\ phase' = [phase EXCEPT ![t] = "prewriting"]
    /\ UNCHANGED <<clock, lock, writes, start, commit, rs, ws, readVal, prewritten, validated>>

Prewrite(t, k) ==
    /\ phase[t] = "prewriting"
    /\ k \in ws[t] \ prewritten[t]
    /\ lock[k].txn = "none"             \* another's lock: wait, or resolve it
    /\ LET fenced == \E r \in writes[k] : r.start = start[t] /\ r.kind = "rollback"
           conflict == \E r \in writes[k] :
                          r.kind = "put" /\ r.ts >= start[t] /\ r.start # start[t]
       IN IF fenced \/ (conflict /\ FAULT # "skip_write_conflict")
          THEN /\ phase' = [phase EXCEPT ![t] = "aborted"]
               /\ UNCHANGED <<lock, prewritten>>
          ELSE /\ lock' = [lock EXCEPT ![k] = [txn |-> t, start |-> start[t]]]
               /\ prewritten' = [prewritten EXCEPT ![t] = @ \cup {k}]
               /\ UNCHANGED phase
    /\ UNCHANGED <<clock, writes, start, commit, rs, ws, readVal, validated>>

GetCommitTs(t) ==
    /\ phase[t] = "prewriting"
    /\ prewritten[t] = ws[t]
    /\ commit' = [commit EXCEPT ![t] = clock]
    /\ clock' = clock + 1
    /\ phase' = [phase EXCEPT ![t] = "validating"]
    /\ UNCHANGED <<lock, writes, start, rs, ws, readVal, prewritten, validated>>

Validate(t, k) ==
    /\ phase[t] = "validating"
    /\ FAULT # "skip_read_validation"
    /\ k \in rs[t] \ validated[t]
    \* Another's lock that could commit at or below our commit: resolve first.
    /\ ~(lock[k].txn \notin {"none", t} /\ lock[k].start < commit[t])
    /\ IF \E r \in writes[k] :
             r.kind = "put" /\ r.ts > start[t] /\ r.ts <= commit[t] /\ r.start # start[t]
       THEN /\ phase' = [phase EXCEPT ![t] = "aborted"]
            /\ UNCHANGED validated
       ELSE /\ validated' = [validated EXCEPT ![t] = @ \cup {k}]
            /\ UNCHANGED phase
    /\ UNCHANGED <<clock, lock, writes, start, commit, rs, ws, readVal, prewritten>>

DoneValidating(t) ==
    /\ phase[t] = "validating"
    /\ validated[t] = rs[t] \/ FAULT = "skip_read_validation"
    /\ phase' = [phase EXCEPT ![t] = "committing"]
    /\ UNCHANGED <<clock, lock, writes, start, commit, rs, ws, readVal, prewritten, validated>>

\* The commit point: only while our primary lock is still there.
CommitPrimary(t) ==
    /\ phase[t] = "committing"
    /\ LET p == Primary(t) IN
       IF lock[p].txn = t
       THEN /\ writes' = [writes EXCEPT ![p] = @ \cup
                  {[ts |-> commit[t], start |-> start[t], kind |-> "put", txn |-> t]}]
            /\ lock' = [lock EXCEPT ![p] = NoLock]
            /\ phase' = [phase EXCEPT ![t] = "committed"]
       ELSE /\ phase' = [phase EXCEPT ![t] = "aborted"]   \* rolled back under us
            /\ UNCHANGED <<writes, lock>>
    /\ UNCHANGED <<clock, start, commit, rs, ws, readVal, prewritten, validated>>

ClientAbort(t) ==
    /\ phase[t] \in {"reading", "prewriting", "validating"}
    /\ phase' = [phase EXCEPT ![t] = "aborted"]
    /\ UNCHANGED <<clock, lock, writes, start, commit, rs, ws, readVal, prewritten, validated>>

\* Roll back t at its primary: by t itself after aborting, or by anyone
\* once t's lock time-to-live has expired (t may merely be slow).
RollbackPrimary(t) ==
    /\ phase[t] \in {"prewriting", "validating", "committing", "aborted"}
    /\ ~PrimaryCommitted(t)
    /\ ~PrimaryRolledBack(t)
    /\ LET p == Primary(t) IN
       /\ writes' = [writes EXCEPT ![p] = @ \cup
              {[ts |-> start[t], start |-> start[t], kind |-> "rollback", txn |-> t]}]
       /\ lock' = IF lock[p].txn = t THEN [lock EXCEPT ![p] = NoLock] ELSE lock
    /\ UNCHANGED <<clock, phase, start, commit, rs, ws, readVal, prewritten, validated>>

\* Whoever meets a secondary lock finishes it as its primary decided.
ResolveSecondary(k) ==
    LET u == lock[k].txn IN
    /\ u # "none"
    /\ k # Primary(u)
    /\ \/ /\ PrimaryCommitted(u)
          /\ writes' = [writes EXCEPT ![k] = @ \cup
                 {[ts |-> commit[u], start |-> start[u], kind |-> "put", txn |-> u]}]
       \/ /\ PrimaryRolledBack(u)
          /\ writes' = [writes EXCEPT ![k] = @ \cup
                 {[ts |-> start[u], start |-> start[u], kind |-> "rollback", txn |-> u]}]
    /\ lock' = [lock EXCEPT ![k] = NoLock]
    /\ UNCHANGED <<clock, phase, start, commit, rs, ws, readVal, prewritten, validated>>

Next ==
    \/ \E t \in Txns :
        \/ Begin(t) \/ DoneReading(t) \/ GetCommitTs(t) \/ DoneValidating(t)
        \/ CommitPrimary(t) \/ ClientAbort(t) \/ RollbackPrimary(t)
        \/ \E k \in Keys : Read(t, k) \/ Prewrite(t, k) \/ Validate(t, k)
    \/ \E k \in Keys : ResolveSecondary(k)

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* Properties. A transaction is committed once its primary is.             *)

Committed(t) == PrimaryCommitted(t)

TypeOK ==
    /\ phase \in [Txns -> Phases]
    /\ \A k \in Keys : lock[k] = NoLock \/ lock[k].txn \in Txns

\* All or nothing: no transaction both committed and rolled back, no
\* write of a transaction that did not commit, and no committed
\* transaction's key rolled back.
Atomicity ==
    \A k \in Keys : \A r \in writes[k] :
        /\ r.kind = "put" => Committed(r.txn)
        /\ r.kind = "rollback" => ~Committed(r.txn)

\* No lost updates: two committed transactions that wrote the same key
\* never overlapped in time.
FirstCommitterWins ==
    \A t, u \in Txns :
        (t # u /\ Committed(t) /\ Committed(u) /\ ws[t] \cap ws[u] # {})
            => (commit[t] < start[u] \/ commit[u] < start[t])

\* The last committed writer of k at or below ts (excluding t), or "init".
LastWriter(k, ts, t) ==
    LET W == {u \in Txns \ {t} : Committed(u) /\ k \in ws[u] /\ commit[u] <= ts}
    IN IF W = {} THEN "init" ELSE CHOOSE u \in W : \A o \in W : commit[o] <= commit[u]

\* Snapshot reads: a committed transaction saw exactly the committed state
\* as of its start timestamp.
SnapshotReads ==
    \A t \in Txns : Committed(t) =>
        \A p \in readVal[t] : p[2] = LastWriter(p[1], start[t], t)

\* Serializable, in commit timestamp order: every committed transaction's
\* reads are what a serial execution in that order would have returned.
Serializable ==
    \A t \in Txns : Committed(t) =>
        \A p \in readVal[t] : p[2] = LastWriter(p[1], commit[t] - 1, t)
=============================================================================
