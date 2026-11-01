// Package reap owns §5's restart pass: process control over the rows a dead
// daemon left in flight. Each row is checked against its recorded pid, the
// processes proven to be the run's own are killed by process group, and the row
// is then marked unknown — the daemon never adopts a run it did not start. The
// rows left in flight are the ones whose kill failed and the ones the pass could
// not resolve, which is what keeps the gate from starting a second run beside
// processes that are still alive.
//
// Only darwin can establish a pid's identity today (kern.proc.pid,
// kern.procargs2 and kern.proc.pgrp); elsewhere the lookups error, which
// establishes neither that the pid is the run's nor that it is not. So nothing
// is signalled there, and every row whose pid gets as far as those lookups — a
// live one leading its own group — is left in flight as unresolved, as is every
// dead leader's, whose group cannot be listed either: fail closed for the
// overlap guard as much as for the signalling. Only the checks that do not need
// the kernel's identity still close a row there, the pid not leading its group
// and the ledger data that establishes nothing. The tests over the kill path
// therefore run on darwin only —
// nowhere else can a pid be established, so nowhere else is anything signalled.
//
// Some of the package's tests cannot run inside srt, which is where a hosted
// coding segment runs this suite: the containment tests that stand in for a
// model process with a holder under its own seatbelt profile start a nested
// /usr/bin/sandbox-exec, and a process already in a sandbox cannot apply
// another, so it exits 71 and the test fails with "start detached child: exit
// status 71". They are TestContainKillsDetachedSandboxedHolders,
// TestContainLeavesProcessesWithoutTheGrantAlone,
// TestContainFailsClosedOnAMissingControl,
// TestContainRecreatesACanaryTheHolderTookAway,
// TestContainNeverReportsEmptyWhileAHolderChurnsItsCanary,
// TestContainFailsClosedWhenALookupErrors,
// TestContainReleasesAPidHandedOnBeforeTheStop and
// TestRestartContainsADetachedHolderBeforeTheRowCloses. Such a failure there is
// the sandbox, not the change under test; they run outside it, in the anchors.
// Listing processes is not in itself the obstacle: srt's base policy lets a
// sandboxed process read kern.proc.all, kern.proc.pgrp and kern.proc.pid for
// processes in its own sandbox, so the other tests here run inside it. Only
// kern.proc.uid, the listing Contain's sweep starts from, is outside that
// policy and refused, which those same tests would reach next.
//
// It is its own package because neither neighbour is the right home: process
// control does not belong in the ledger, which owns rows and not signals, and
// this is not the scheduler's slot maths.
package reap
