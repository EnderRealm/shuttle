# Landing lifecycle

The state of a hosted weft-service/v2 run on a host that serves `landing`, and every
transition that changes it. `internal/ledger` and `internal/weftremote` implement this
table; a change to either that moves a transition changes the table first. Spec references
are to weft's `docs/service-v2-contract.md` (§) and `docs/shuttle-contract.md` (v1 §).

## 1. State

A run's landing state is the tuple **(phase, outcome, settlement, stop)**. Nothing else
decides an acknowledgement or resumability.

| Column | Values | Meaning |
|---|---|---|
| `phase` | queued, running, parked, settling, ended | whether a segment is (or is about to be) live, and what for |
| `outcome` | null, finished, halted, failed, stopped | the grade, or the host's closure of an ungraded segment |
| `settlement` | not_applicable, pending, in_progress, reconciling, landed, not_landed(step, reason) | §7.1 |
| `stop` | 0 / 1 | an operator stopped **the current attempt**. An attempt starts at a resume/answer claim or a reconcile retry |

There is one stop flag. `stopped` lives for one attempt: no grade or `ended` clears it,
and only a resume/answer claim or a reconcile retry does. Resumability reads `outcome`
(§4), never the flag. The flag's API meaning is unchanged — `stopped: true` is "the
operator stopped this run" — so a finished run whose landing was stopped reads
`outcome: finished, stopped: true, settlement: not_landed/stopped`.

Evidence is per segment and is not part of the state. `E(s, step)` is true when segment
`s` itself reported `land_step_result done` for the step, or `s` is a reconcile that
observed the step `done`. Another segment's record is history and never evidence.

### Named states

| # | State | phase | outcome | settlement | Live segment |
|---|---|---|---|---|---|
| A | before grade | queued / running / parked | null | pending (from `--land` hello) | pipeline or none (parked) |
| A′ | stopping before grade | **ended** (stop is recorded at once, v1 §3.7) | stopped | pending | pipeline, still live |
| B | graded, landing | settling | finished | pending / in_progress(step) | the landing segment |
| C | reconciling | settling | (as recorded) | reconciling | reconcile, claimed or live |
| T1 | landed | ended | finished | landed | — |
| T2 | not landed | ended | finished | not_landed(step, reason) | — after reap: `action_required = landing_unsettled`, repository paused |
| T2u | unreconciled, grade unknown | ended | stopped if stop=1, else failed (the lost segment's class kept, unknown if none) | not_landed(first step not done, unreconciled) | — as T2: held and paused at reap |
| T3 | over, nothing lands | ended | halted / failed / stopped | not_applicable | — |
| T2r | retry queued | ended | as T2 / T2u | not_landed | none; reconcile entry waiting |

## 2. Invariants

1. **I-ack:** `ack_land_intent` is written to stdin only after the `weft_land_steps` row with
   `acked_at` has committed (§7.2).
2. **I-resume:** a run is resumable only if `settlement = not_applicable` and it has no
   `weft_land_steps` rows. So `outcome = finished` is never resumable (v1 §3.8: a grade is
   not answered by resume).
3. **I-rows:** `weft_land_steps` rows are written only for a run graded `finished`. A landing
   segment's intent and result rows exist only after a finished grade. A reconcile's observed
   rows are written only when it observed `outcome = finished`. Together with I-resume, a new
   pipeline segment can never meet a previous landing's rows.
4. **I-one:** at most one segment that can take a step (a landing segment past its grade, or
   a reconcile) is live per run. This is the reservation row (`run` → `settlement` /
   `maintenance`).
5. **I-pause:** a repository is paused only at reap, by a run reaching T2. Only `continue`
   lifts the pause, and continue is refused while any run there is in C.
6. **I-release:** no terminal event releases a reservation. Only reap does.

## 3. The ack rule (one function: `RecordWeftLandIntent`)

For segment `s` intending step `k` (index `i`), check in order:

| # | Check | On failure |
|---|---|---|
| 1 | `k` is a landing step, commit = the host-recorded graded head and base = the run's base, both non-empty, no intent of `s` awaiting its result | protocol error → segment failed |
| 2 | `s` is a landing segment: `land = 1`, `s` itself graded the run `finished`, settlement ∈ {pending, in_progress} | protocol error |
| 2′ | `s` is a reconcile: it has observed, it observed `outcome = finished`, the host recorded a finished grade with a head and passed it as `--commit`, settlement = reconciling. A reconcile launched without `--commit` observes and takes no step (§7.4) | protocol error |
| 3 | **every** earlier step `j < i` has `E(s, j)`. | protocol error |
| 4 | not `E(s, k)`: never intend a step this segment already did or saw done | protocol error |
| 5 | reconcile only: `s` observed `k` as exactly `not_done`. `unknown` or absent is never retaken | protocol error |
| 6 | `stop = 0` | **record the intent with no ack**. The worker blocks; the stop on stdin makes it take no step and end `not_landed stopped` |

Only check 6 is a non-failure refusal. Checks 1–5 are the worker breaking §7.2.

## 4. Resumability (one function: `weftResumableTx`, used by `AdmitResume` and `ClaimWeftResume`)

A run is resumable if and only if all of the following hold:

- `origin ≠ archive` and `disposition ≠ archived`;
- `phase = ended`;
- `action_required ∉ {stale_base, landing_unsettled}`;
- `settlement = not_applicable` and the run has no land-step rows (I-resume);
- `outcome = stopped`, **or** `outcome = failed` and the last pipeline segment's failure is a
  host class (v1 §3.8) **and that segment has no grade: it sent no `graded`, and no reconcile
  read one for it from `runs.jsonl`**. `graded` is the grading half of
  v1's `ended`, so a `failed` the worker graded is never resumable, even when its segment is
  then lost before `ended` (row 13t).

The rule reads `outcome = stopped`, not the `stopped` flag: v1's rule verbatim ("every
`ended` reads off `outcome` and `failure`").

## 5. Transitions

"Stop" carries over unless the row says otherwise. Ack and Resumable describe the state
after the transition.

| # | From | Event | Guard | To / effect | Ack? | Resumable after? |
|---|---|---|---|---|---|---|
| 1 | A | hello with `--land` | — | settlement pending; segment `land = 1` | — | — |
| 2 | A, stop=0 | graded finished | — | **B**: phase settling, outcome finished, head = landCommit | ack per §3 | no |
| 3 | A′ (stop=1) | graded finished (buffered behind the stop) | — | **B with stop=1**: phase settling, outcome finished. The grade stands; the stop stops the landing | no (check 6) | no |
| 4 | A / A′ | graded halted / failed / stopped | — | **T3**: settlement not_applicable "graded X" | — | per §4 (halted and failed with a null failure: no) |
| 5 | A | operator stop | live segment | **A′**: phase ended, outcome stopped, stop=1; stop on stdin | — | yes once reaped (T3) |
| 6 | A (parked) | operator stop | no process | **T3**: question withdrawn, waiting answer/resume invalidated, settlement not_applicable | — | yes |
| 7 | B | land_intent k | §3 | settlement in_progress(k); row written, acked_at if acked | §3 | no |
| 8 | B | land_step_result k | k is the one intent, it was acked | row result + result_segment | — | no |
| 9 | B | operator stop | — | stop=1, outcome unchanged; stop on stdin. The worker finishes the step in flight and takes no other (§6.6) | no further | no |
| 10 | B | ended landed | E(s, every step) | **T1** | — | no |
| 11 | B | ended not_landed(k, r) | — | **T2** (pause at reap) | — | no |
| 11n | B | ended not_applicable | — | protocol error → segment failed, then row 12: a finished grade ends only landed or not_landed | — | no |
| 12 | A, A′, B (settlement pending / in_progress) | worker exits / killed with no terminal event | segment `land = 1` | **C**: row `run` → `settlement`; reconcile entry dispatched in the same tx | — | no |
| 12l | as 12 / 13, the run at its segment limit | lost landing, no reconcile can be claimed | — | held as row 22 at once: not_landed unreconciled (T2, or T2u with no grade), paused at reap | — | no |
| 13 | A, A′, B (settlement pending / in_progress) | daemon restart, segment not established gone | §12 step 2 | as 12 | — | no |
| 13t | T3 (graded non-finished, `ended` not yet read) | worker exits / killed, or daemon restart | segment `land = 1` | stays **T3**: the segment's class is recorded, its run row and slot released at reap; no reconcile, no pause | — | per §4 |
| 13p | B | `parked` | — | protocol error → segment failed, then row 12: a graded segment never parks | — | no |
| 14 | B → T1/T2 | reap | terminal recorded | release `run` row and slot; T2 → action landing_unsettled + pause | — | no |
| 15 | C | observed | first event | segment observed_* recorded; land-step rows only if observed `finished` (I-rows) | — | — |
| 15g | C, outcome finished recorded | observed with an outcome other than `finished` | — | protocol error → reconcile failed, then row 22 (**T2** unreconciled): the host's recorded grade stands against the worker's reading | — | no |
| 16 | C | land_intent k (reconcile) | §3 incl. 2′, 5 | settlement stays reconciling | §3 | no |
| 17 | C | operator stop | reconcile live | stop=1; stop on stdin | no | no |
| 18 | C | reconciled, observed none | — | **T3**: outcome stopped if stop=1, else failed with class kept (unknown if none); not_applicable | — | **yes** (§7.4: v1 interrupted) |
| 19 | C | reconciled, observed halted / failed / stopped | — | **T3** with that outcome; the lost pipeline segment is recorded as graded (the reading is its grade) | — | per §4 |
| 20 | C | reconciled, observed finished, landed | E(s, every step) | **T1**; landing_unsettled cleared at reap, pause stands | — | no |
| 21 | C | reconciled, observed finished, not landed | — | **T2** | — | no |
| 21n | C, observed finished | reconciled not_applicable | — | protocol error → reconcile failed, then row 22: as row 11n, a finished grade ends only landed or not_landed | — | no |
| 22 | C, outcome finished | reconcile lost (exit or restart) | no `reconciled` | **T2** unreconciled at the first step not done | — | no |
| 22u | C, no grade recorded | reconcile lost (exit or restart) | no `reconciled` | if the lost reconcile observed `finished`, it is adopted (with the segment's graded mark) and the run is **T2** at the first step not done; if it observed halted / failed / stopped, the run settles as row 19 (**T3**, not_applicable, no pause); otherwise **T2u**: the host cannot tell whether the lost segment graded, so nothing is resumable, and with no graded head no retry is admitted (row 23): fresh work is the way forward | — | no |
| 23 | T2, T2u | reconcile retry admitted | reason ∈ {failed, stopped, unreconciled}; not archived or superseded (checked again at dispatch; a fresh archive invalidates a waiting retry); the host recorded a graded head — outcome finished with head and base — else `not_reconcilable`, as a retry could only observe | **T2r**; **stop := 0** (a new attempt) | — | no |
| 24 | T2r | retry dispatched | ignores the pause | **C** under a `maintenance` row | — | no |
| 25 | T3 | resume admitted / claimed | §4 | **A**: new pipeline segment, outcome null, **stop := 0**, settlement pending at hello | — | — |
| 26 | T1, T2, T2u | resume | — | refused `run_not_resumable` | — | no |
| 27 | T2, T2u (reaped) | continue | no run in C in that repository | pause lifted; the run stays T2 | — | no |
| 28 | T1, T2, T3 | dismiss | requireIdle: not in A/B/C, nothing queued | cleanup entry; the run's state is unchanged | — | unchanged |
| 29 | T2, T3 (fresh options) | fresh, archive done | requireIdle; the archive head is the run's recorded head, or for a run with none (`branch_exists`, §6.7) the confirmation's `authorized_head`, which the archive worker verifies against the branch | old run: disposition archived, outcome stopped (§6.7 — the archived run is never resumable), stop=1; new run in A | — | old run never |
| 30 | any ended | operator stop | — | refused `run_over` | — | — |

## 6. Settled calls

- A stop before the grade followed by a finished grade (row 3) lands nothing and ends
  `not_landed stopped`, which pauses the repository like every not_landed (§7.5). The host
  never contradicts the worker's `ended`.
- Dismiss is accepted on a reaped not_landed run (T2): `settlement_unsettled` refuses only
  in_progress and reconciling. The branch is kept and the pause is unaffected.
- Fresh on a run with no recorded head (`branch_exists`) archives the branch at the confirmation's
  `authorized_head` (§6.7); the archive worker refuses if the branch does not name it.
- A dismiss or fresh against a repository a terminal session holds waits behind that
  reservation rather than being refused (§6.2, §6.8; `dismiss-response.json`, `fresh-response.json`).
- An archive segment that fails before it spawns invalidates its fresh entry `archive_refused`:
  nothing can have been written.
- A weft-archive/v1 run with no repository binds to its project's repository on this host;
  a project with none is refused 404 `project_not_found`.
