#!/bin/sh
# Model-check the transaction protocol with TLC: the real protocol must
# satisfy every invariant, and each planted bug must violate the invariant
# that guards against it. Needs Java and tla2tools.jar (TLA2TOOLS or ./).
set -eu
cd "$(dirname "$0")"
jar="${TLA2TOOLS:-tla2tools.jar}"
[ -f "$jar" ] || { echo "tla2tools.jar not found: set TLA2TOOLS" >&2; exit 2; }

run() { # config, expected invariant violation ("" for none)
    out=$(java -XX:+UseParallelGC -cp "$jar" tlc2.TLC -workers auto -deadlock -config "$1" Percolator.tla 2>&1 || true)
    states=$(echo "$out" | grep -o '[0-9]* distinct states found' | tail -1)
    if [ -z "$2" ]; then
        if echo "$out" | grep -q "Model checking completed. No error has been found."; then
            echo "ok   $1: every invariant holds ($states)"
        else
            echo "FAIL $1: expected no violation"; echo "$out" | tail -40; exit 1
        fi
    else
        if echo "$out" | grep -q "Invariant $2 is violated"; then
            echo "ok   $1: TLC found the planted bug, $2 violated ($states)"
        else
            echo "FAIL $1: expected $2 to be violated"; echo "$out" | tail -40; exit 1
        fi
    fi
}

run Percolator.cfg ""
run BugSkipWriteConflict.cfg FirstCommitterWins
run BugReadIgnoresLocks.cfg SnapshotReads
run BugSkipReadValidation.cfg Serializable
