---- MODULE Percolator_TTrace_1790742068 ----
EXTENDS Sequences, Percolator, TLCExt, Toolbox, Percolator_TEConstants, Naturals, TLC

_expression ==
    LET Percolator_TEExpression == INSTANCE Percolator_TEExpression
    IN Percolator_TEExpression!expression
----

_trace ==
    LET Percolator_TETrace == INSTANCE Percolator_TETrace
    IN Percolator_TETrace!trace
----

_inv ==
    ~(
        TLCGet("level") = Len(_TETrace)
        /\
        phase = ((t1 :> "committed" @@ t2 :> "committed"))
        /\
        rs = ((t1 :> {} @@ t2 :> {k1}))
        /\
        validated = ((t1 :> {} @@ t2 :> {k1}))
        /\
        commit = ((t1 :> 2 @@ t2 :> 4))
        /\
        start = ((t1 :> 1 @@ t2 :> 3))
        /\
        prewritten = ((t1 :> {k1} @@ t2 :> {k1}))
        /\
        lock = ((k1 :> [start |-> 0, txn |-> "none"] @@ k2 :> [start |-> 0, txn |-> "none"]))
        /\
        clock = (5)
        /\
        writes = ((k1 :> {[start |-> 1, txn |-> t1, kind |-> "put", ts |-> 2], [start |-> 3, txn |-> t2, kind |-> "put", ts |-> 4]} @@ k2 :> {}))
        /\
        ws = ((t1 :> {k1} @@ t2 :> {k1}))
        /\
        readVal = ((t1 :> {} @@ t2 :> {<<k1, "init">>}))
    )
----

_init ==
    /\ prewritten = _TETrace[1].prewritten
    /\ rs = _TETrace[1].rs
    /\ validated = _TETrace[1].validated
    /\ readVal = _TETrace[1].readVal
    /\ lock = _TETrace[1].lock
    /\ phase = _TETrace[1].phase
    /\ commit = _TETrace[1].commit
    /\ start = _TETrace[1].start
    /\ writes = _TETrace[1].writes
    /\ ws = _TETrace[1].ws
    /\ clock = _TETrace[1].clock
----

_next ==
    /\ \E i,j \in DOMAIN _TETrace:
        /\ \/ /\ j = i + 1
              /\ i = TLCGet("level")
        /\ prewritten  = _TETrace[i].prewritten
        /\ prewritten' = _TETrace[j].prewritten
        /\ rs  = _TETrace[i].rs
        /\ rs' = _TETrace[j].rs
        /\ validated  = _TETrace[i].validated
        /\ validated' = _TETrace[j].validated
        /\ readVal  = _TETrace[i].readVal
        /\ readVal' = _TETrace[j].readVal
        /\ lock  = _TETrace[i].lock
        /\ lock' = _TETrace[j].lock
        /\ phase  = _TETrace[i].phase
        /\ phase' = _TETrace[j].phase
        /\ commit  = _TETrace[i].commit
        /\ commit' = _TETrace[j].commit
        /\ start  = _TETrace[i].start
        /\ start' = _TETrace[j].start
        /\ writes  = _TETrace[i].writes
        /\ writes' = _TETrace[j].writes
        /\ ws  = _TETrace[i].ws
        /\ ws' = _TETrace[j].ws
        /\ clock  = _TETrace[i].clock
        /\ clock' = _TETrace[j].clock

\* Uncomment the ASSUME below to write the states of the error trace
\* to the given file in Json format. Note that you can pass any tuple
\* to `JsonSerialize`. For example, a sub-sequence of _TETrace.
    \* ASSUME
    \*     LET J == INSTANCE Json
    \*         IN J!JsonSerialize("Percolator_TTrace_1790742068.json", _TETrace)

=============================================================================

 Note that you can extract this module `Percolator_TEExpression`
  to a dedicated file to reuse `expression` (the module in the 
  dedicated `Percolator_TEExpression.tla` file takes precedence 
  over the module `Percolator_TEExpression` below).

---- MODULE Percolator_TEExpression ----
EXTENDS Sequences, Percolator, TLCExt, Toolbox, Percolator_TEConstants, Naturals, TLC

expression == 
    [
        \* To hide variables of the `Percolator` spec from the error trace,
        \* remove the variables below.  The trace will be written in the order
        \* of the fields of this record.
        prewritten |-> prewritten
        ,rs |-> rs
        ,validated |-> validated
        ,readVal |-> readVal
        ,lock |-> lock
        ,phase |-> phase
        ,commit |-> commit
        ,start |-> start
        ,writes |-> writes
        ,ws |-> ws
        ,clock |-> clock
        
        \* Put additional constant-, state-, and action-level expressions here:
        \* ,_stateNumber |-> _TEPosition
        \* ,_prewrittenUnchanged |-> prewritten = prewritten'
        
        \* Format the `prewritten` variable as Json value.
        \* ,_prewrittenJson |->
        \*     LET J == INSTANCE Json
        \*     IN J!ToJson(prewritten)
        
        \* Lastly, you may build expressions over arbitrary sets of states by
        \* leveraging the _TETrace operator.  For example, this is how to
        \* count the number of times a spec variable changed up to the current
        \* state in the trace.
        \* ,_prewrittenModCount |->
        \*     LET F[s \in DOMAIN _TETrace] ==
        \*         IF s = 1 THEN 0
        \*         ELSE IF _TETrace[s].prewritten # _TETrace[s-1].prewritten
        \*             THEN 1 + F[s-1] ELSE F[s-1]
        \*     IN F[_TEPosition - 1]
    ]

=============================================================================



Parsing and semantic processing can take forever if the trace below is long.
 In this case, it is advised to uncomment the module below to deserialize the
 trace from a generated binary file.

\*
\*---- MODULE Percolator_TETrace ----
\*EXTENDS IOUtils, Percolator, Percolator_TEConstants, TLC
\*
\*trace == IODeserialize("Percolator_TTrace_1790742068.bin", TRUE)
\*
\*=============================================================================
\*

---- MODULE Percolator_TETrace ----
EXTENDS Percolator, Percolator_TEConstants, TLC

trace == 
    <<
    ([phase |-> (t1 :> "idle" @@ t2 :> "idle"),rs |-> (t1 :> {} @@ t2 :> {}),validated |-> (t1 :> {} @@ t2 :> {}),commit |-> (t1 :> 0 @@ t2 :> 0),start |-> (t1 :> 0 @@ t2 :> 0),prewritten |-> (t1 :> {} @@ t2 :> {}),lock |-> (k1 :> [start |-> 0, txn |-> "none"] @@ k2 :> [start |-> 0, txn |-> "none"]),clock |-> 1,writes |-> (k1 :> {} @@ k2 :> {}),ws |-> (t1 :> {} @@ t2 :> {}),readVal |-> (t1 :> {} @@ t2 :> {})]),
    ([phase |-> (t1 :> "reading" @@ t2 :> "idle"),rs |-> (t1 :> {} @@ t2 :> {}),validated |-> (t1 :> {} @@ t2 :> {}),commit |-> (t1 :> 0 @@ t2 :> 0),start |-> (t1 :> 1 @@ t2 :> 0),prewritten |-> (t1 :> {} @@ t2 :> {}),lock |-> (k1 :> [start |-> 0, txn |-> "none"] @@ k2 :> [start |-> 0, txn |-> "none"]),clock |-> 2,writes |-> (k1 :> {} @@ k2 :> {}),ws |-> (t1 :> {k1} @@ t2 :> {}),readVal |-> (t1 :> {} @@ t2 :> {})]),
    ([phase |-> (t1 :> "prewriting" @@ t2 :> "idle"),rs |-> (t1 :> {} @@ t2 :> {}),validated |-> (t1 :> {} @@ t2 :> {}),commit |-> (t1 :> 0 @@ t2 :> 0),start |-> (t1 :> 1 @@ t2 :> 0),prewritten |-> (t1 :> {} @@ t2 :> {}),lock |-> (k1 :> [start |-> 0, txn |-> "none"] @@ k2 :> [start |-> 0, txn |-> "none"]),clock |-> 2,writes |-> (k1 :> {} @@ k2 :> {}),ws |-> (t1 :> {k1} @@ t2 :> {}),readVal |-> (t1 :> {} @@ t2 :> {})]),
    ([phase |-> (t1 :> "prewriting" @@ t2 :> "idle"),rs |-> (t1 :> {} @@ t2 :> {}),validated |-> (t1 :> {} @@ t2 :> {}),commit |-> (t1 :> 0 @@ t2 :> 0),start |-> (t1 :> 1 @@ t2 :> 0),prewritten |-> (t1 :> {k1} @@ t2 :> {}),lock |-> (k1 :> [start |-> 1, txn |-> t1] @@ k2 :> [start |-> 0, txn |-> "none"]),clock |-> 2,writes |-> (k1 :> {} @@ k2 :> {}),ws |-> (t1 :> {k1} @@ t2 :> {}),readVal |-> (t1 :> {} @@ t2 :> {})]),
    ([phase |-> (t1 :> "validating" @@ t2 :> "idle"),rs |-> (t1 :> {} @@ t2 :> {}),validated |-> (t1 :> {} @@ t2 :> {}),commit |-> (t1 :> 2 @@ t2 :> 0),start |-> (t1 :> 1 @@ t2 :> 0),prewritten |-> (t1 :> {k1} @@ t2 :> {}),lock |-> (k1 :> [start |-> 1, txn |-> t1] @@ k2 :> [start |-> 0, txn |-> "none"]),clock |-> 3,writes |-> (k1 :> {} @@ k2 :> {}),ws |-> (t1 :> {k1} @@ t2 :> {}),readVal |-> (t1 :> {} @@ t2 :> {})]),
    ([phase |-> (t1 :> "committing" @@ t2 :> "idle"),rs |-> (t1 :> {} @@ t2 :> {}),validated |-> (t1 :> {} @@ t2 :> {}),commit |-> (t1 :> 2 @@ t2 :> 0),start |-> (t1 :> 1 @@ t2 :> 0),prewritten |-> (t1 :> {k1} @@ t2 :> {}),lock |-> (k1 :> [start |-> 1, txn |-> t1] @@ k2 :> [start |-> 0, txn |-> "none"]),clock |-> 3,writes |-> (k1 :> {} @@ k2 :> {}),ws |-> (t1 :> {k1} @@ t2 :> {}),readVal |-> (t1 :> {} @@ t2 :> {})]),
    ([phase |-> (t1 :> "committing" @@ t2 :> "reading"),rs |-> (t1 :> {} @@ t2 :> {k1}),validated |-> (t1 :> {} @@ t2 :> {}),commit |-> (t1 :> 2 @@ t2 :> 0),start |-> (t1 :> 1 @@ t2 :> 3),prewritten |-> (t1 :> {k1} @@ t2 :> {}),lock |-> (k1 :> [start |-> 1, txn |-> t1] @@ k2 :> [start |-> 0, txn |-> "none"]),clock |-> 4,writes |-> (k1 :> {} @@ k2 :> {}),ws |-> (t1 :> {k1} @@ t2 :> {k1}),readVal |-> (t1 :> {} @@ t2 :> {})]),
    ([phase |-> (t1 :> "committing" @@ t2 :> "reading"),rs |-> (t1 :> {} @@ t2 :> {k1}),validated |-> (t1 :> {} @@ t2 :> {}),commit |-> (t1 :> 2 @@ t2 :> 0),start |-> (t1 :> 1 @@ t2 :> 3),prewritten |-> (t1 :> {k1} @@ t2 :> {}),lock |-> (k1 :> [start |-> 1, txn |-> t1] @@ k2 :> [start |-> 0, txn |-> "none"]),clock |-> 4,writes |-> (k1 :> {} @@ k2 :> {}),ws |-> (t1 :> {k1} @@ t2 :> {k1}),readVal |-> (t1 :> {} @@ t2 :> {<<k1, "init">>})]),
    ([phase |-> (t1 :> "committed" @@ t2 :> "reading"),rs |-> (t1 :> {} @@ t2 :> {k1}),validated |-> (t1 :> {} @@ t2 :> {}),commit |-> (t1 :> 2 @@ t2 :> 0),start |-> (t1 :> 1 @@ t2 :> 3),prewritten |-> (t1 :> {k1} @@ t2 :> {}),lock |-> (k1 :> [start |-> 0, txn |-> "none"] @@ k2 :> [start |-> 0, txn |-> "none"]),clock |-> 4,writes |-> (k1 :> {[start |-> 1, txn |-> t1, kind |-> "put", ts |-> 2]} @@ k2 :> {}),ws |-> (t1 :> {k1} @@ t2 :> {k1}),readVal |-> (t1 :> {} @@ t2 :> {<<k1, "init">>})]),
    ([phase |-> (t1 :> "committed" @@ t2 :> "prewriting"),rs |-> (t1 :> {} @@ t2 :> {k1}),validated |-> (t1 :> {} @@ t2 :> {}),commit |-> (t1 :> 2 @@ t2 :> 0),start |-> (t1 :> 1 @@ t2 :> 3),prewritten |-> (t1 :> {k1} @@ t2 :> {}),lock |-> (k1 :> [start |-> 0, txn |-> "none"] @@ k2 :> [start |-> 0, txn |-> "none"]),clock |-> 4,writes |-> (k1 :> {[start |-> 1, txn |-> t1, kind |-> "put", ts |-> 2]} @@ k2 :> {}),ws |-> (t1 :> {k1} @@ t2 :> {k1}),readVal |-> (t1 :> {} @@ t2 :> {<<k1, "init">>})]),
    ([phase |-> (t1 :> "committed" @@ t2 :> "prewriting"),rs |-> (t1 :> {} @@ t2 :> {k1}),validated |-> (t1 :> {} @@ t2 :> {}),commit |-> (t1 :> 2 @@ t2 :> 0),start |-> (t1 :> 1 @@ t2 :> 3),prewritten |-> (t1 :> {k1} @@ t2 :> {k1}),lock |-> (k1 :> [start |-> 3, txn |-> t2] @@ k2 :> [start |-> 0, txn |-> "none"]),clock |-> 4,writes |-> (k1 :> {[start |-> 1, txn |-> t1, kind |-> "put", ts |-> 2]} @@ k2 :> {}),ws |-> (t1 :> {k1} @@ t2 :> {k1}),readVal |-> (t1 :> {} @@ t2 :> {<<k1, "init">>})]),
    ([phase |-> (t1 :> "committed" @@ t2 :> "validating"),rs |-> (t1 :> {} @@ t2 :> {k1}),validated |-> (t1 :> {} @@ t2 :> {}),commit |-> (t1 :> 2 @@ t2 :> 4),start |-> (t1 :> 1 @@ t2 :> 3),prewritten |-> (t1 :> {k1} @@ t2 :> {k1}),lock |-> (k1 :> [start |-> 3, txn |-> t2] @@ k2 :> [start |-> 0, txn |-> "none"]),clock |-> 5,writes |-> (k1 :> {[start |-> 1, txn |-> t1, kind |-> "put", ts |-> 2]} @@ k2 :> {}),ws |-> (t1 :> {k1} @@ t2 :> {k1}),readVal |-> (t1 :> {} @@ t2 :> {<<k1, "init">>})]),
    ([phase |-> (t1 :> "committed" @@ t2 :> "validating"),rs |-> (t1 :> {} @@ t2 :> {k1}),validated |-> (t1 :> {} @@ t2 :> {k1}),commit |-> (t1 :> 2 @@ t2 :> 4),start |-> (t1 :> 1 @@ t2 :> 3),prewritten |-> (t1 :> {k1} @@ t2 :> {k1}),lock |-> (k1 :> [start |-> 3, txn |-> t2] @@ k2 :> [start |-> 0, txn |-> "none"]),clock |-> 5,writes |-> (k1 :> {[start |-> 1, txn |-> t1, kind |-> "put", ts |-> 2]} @@ k2 :> {}),ws |-> (t1 :> {k1} @@ t2 :> {k1}),readVal |-> (t1 :> {} @@ t2 :> {<<k1, "init">>})]),
    ([phase |-> (t1 :> "committed" @@ t2 :> "committing"),rs |-> (t1 :> {} @@ t2 :> {k1}),validated |-> (t1 :> {} @@ t2 :> {k1}),commit |-> (t1 :> 2 @@ t2 :> 4),start |-> (t1 :> 1 @@ t2 :> 3),prewritten |-> (t1 :> {k1} @@ t2 :> {k1}),lock |-> (k1 :> [start |-> 3, txn |-> t2] @@ k2 :> [start |-> 0, txn |-> "none"]),clock |-> 5,writes |-> (k1 :> {[start |-> 1, txn |-> t1, kind |-> "put", ts |-> 2]} @@ k2 :> {}),ws |-> (t1 :> {k1} @@ t2 :> {k1}),readVal |-> (t1 :> {} @@ t2 :> {<<k1, "init">>})]),
    ([phase |-> (t1 :> "committed" @@ t2 :> "committed"),rs |-> (t1 :> {} @@ t2 :> {k1}),validated |-> (t1 :> {} @@ t2 :> {k1}),commit |-> (t1 :> 2 @@ t2 :> 4),start |-> (t1 :> 1 @@ t2 :> 3),prewritten |-> (t1 :> {k1} @@ t2 :> {k1}),lock |-> (k1 :> [start |-> 0, txn |-> "none"] @@ k2 :> [start |-> 0, txn |-> "none"]),clock |-> 5,writes |-> (k1 :> {[start |-> 1, txn |-> t1, kind |-> "put", ts |-> 2], [start |-> 3, txn |-> t2, kind |-> "put", ts |-> 4]} @@ k2 :> {}),ws |-> (t1 :> {k1} @@ t2 :> {k1}),readVal |-> (t1 :> {} @@ t2 :> {<<k1, "init">>})])
    >>
----


=============================================================================

---- MODULE Percolator_TEConstants ----
EXTENDS Percolator

CONSTANTS k1, k2, t1, t2

=============================================================================

---- CONFIG Percolator_TTrace_1790742068 ----
CONSTANTS
    Keys = { k1 , k2 }
    Txns = { t1 , t2 }
    FAULT = "read_ignores_locks"
    t1 = t1
    t2 = t2
    k1 = k1
    k2 = k2

INVARIANT
    _inv

CHECK_DEADLOCK
    \* CHECK_DEADLOCK off because of PROPERTY or INVARIANT above.
    FALSE

INIT
    _init

NEXT
    _next

CONSTANT
    _TETrace <- _trace

ALIAS
    _expression
=============================================================================
\* Generated on Wed Sep 30 04:21:11 UTC 2026