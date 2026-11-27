// Package export copies the run state off the state root's disk: the ledger
// through SQLite's online backup, so the copy is consistent while the daemon
// writes, and each run's directory and the reports repository as plain files
// beside it. The copy is laid out as a state root is, a runs.db file and runs/
// and reports/ trees, so a restore is a copy back onto a clean directory.
//
// An export that is refused, for a destination missing or not writable all
// the way down, replaces nothing. One that fails part way, a share that fills
// up for instance, can leave the copy partly updated: each file in it is
// whole, either the previous one or the new one, but the set can mix the two
// until the next export completes. The copy never deletes; a file pruned from
// the state root stays in it.
package export
