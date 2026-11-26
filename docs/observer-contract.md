# Observer contract

Status: written 2026-09-09 alongside `internal/observer`, which is the executable
half of this document. Where the two disagree the code is right and this file is
stale. The contract package contains validation, a reference notebook, two
domains' fixtures, the language-neutral conformance suite, the subprocess
notebook client, the notebook Shuttle holds and the door its authors reach it
through. `internal/runner` wires both
notebooks and the collector boundary to the same manual and scheduled pipeline
used by the daily agent.
`observer/v1` is unreleased on those terms — no records exist under it and no
domain adapter reads it — so a correction to the shape is still made in place; a
version pins a shape once there are records written under it to read. That is
why `observer/v1` itself carries the three observations that do not come from a
run — a person's observation, a correction and a carried-over reading (§1) —
rather than a new version.

An observer cannot judge a change consistently if every domain invents its own
meaning for a window, a baseline, missing evidence or a retired question. This
document is the part of that agreement the types cannot hold: what is fact and
what is a reading of it, who owns which store, what is kept and for how long, and
how an experiment is registered without inheriting the observer's context.

## 1. What travels

Six payloads, all JSON, all carrying `contract_version` (`observer/v1` today);
the Assessment has four kinds, listed as rows of their own.
A payload with no version, or one this binary does not read, is refused naming
`contract_version` and listing what it does read — the notebook outlives the
binary, so a record has to say which rules it was written under rather than be
guessed at from its shape.

| Payload | Is | Written by | Direction |
|---|---|---|---|
| Question | a standing question about a subject: what would answer it, what answering it would change, how fast its inputs move | a person, a worker or the observer, recorded in `author` | into the notebook from its author |
| Experiment | a change's declared effect, its activation, the window and opportunity it needs, and the readings and baseline it will be judged by | a person, a worker or the observer, recorded in `author` | into the notebook from its author |
| Bundle | recorded fact: readings over an explicit window, from named sources, out of a named snapshot, with what could not be read | the observer | written into the notebook |
| Assessment | one outcome and one prose interpretation, carrying the bundle it was drawn from; an unprompted assessment also carries the finding that opens its entry | the observer, by a run (§10) | written into the notebook |
| Person's observation | an outcome and the facts as a person states them, with the time, and no bundle | a person, `author.kind` `human`, calling as the operator's uid | into the notebook through the door (§12) |
| Correction | a person's observation that also names, in `corrects`, the observation it corrects and what was wrong with it | a person, `author.kind` `human`, calling as the operator's uid | into the notebook through the door |
| Carried-over reading | a reading first recorded elsewhere, with the source it came from in `carried_over` and its original date in `at` | any declared author | into the notebook through the door |
| Action | ticket-facing feedback under a stable id: a `ticket-note` on the entry's ticket, a `ticket-reopen` or `ticket-release` of it, or a `ticket-open` carrying the title of a ticket to create | the observer | out toward the ticket store, as a proposal (§11) |
| Receipt | what became of an action, under that same id | the observer, recording what the far store did | back from that store: `applied`, `refused` or `failed`, with its reference |

`author` is a kind from a fixed vocabulary — `human`, `worker`, `observer` — and an
optional free `ref` holding whatever identifies that author: a person's name, a
ticket id, a run id. It is required on a question, an experiment and every
assessment but the observer's own, because the three carry different authority,
and a reader who cannot tell them apart reads a model's proposal as a person's
decision. An assessment with no `author` is the observer's. A ticket reference
on a question or an experiment is provenance and is optional on both: many
questions and experiments are written by hand with no ticket behind them.

The three rows after the assessment are assessments too — one row each of an
entry's history, under the same `question_id`, outcome vocabulary and request
rules — and the row's kind is read from the fields it carries, never declared
beside them. No `author`, or the observer's, with neither `corrects` nor
`carried_over`, is the observer's own and must carry its bundle and no `facts`.
`author.kind` `human` is a person's observation and carries `facts`, one
statement each, its `at` as a timestamp, and no bundle: a bundle is what the observer read, and a
person's statement dressed as one would look checkable in a way it is not.
`corrects` makes it a correction, `{"observation": <n>, "wrong": "..."}`, with
`n` the 1-based position of the row corrected; positions are stable because the
history is append-only. `carried_over`, `{"source": "..."}`, makes it a
carried-over reading, which carries its reading in `interpretation`, may carry
`facts` or a bundle, and names an author of any kind — whoever first wrote it.
Its `at` is the date it was written where it came from, a calendar date or a
timestamp as written there, since a time of day the source never recorded is
not the contract's to add. None of the three opens a finding, and a worker
writes only a carried-over reading. Everything else is refused naming the
field.

Who writes each is the point of the split. A run writes only the observer's own
assessment; one claiming a person, a correction or a carried source is refused
(§10). A person's observation and a correction are a person's, and reach the
notebook only through the door (§12), as a carried-over reading does — Mead's
dated readings moving in from its file with their dates is the case it exists
for. A declared `human` is not enough to make a row a person's: a person's
`effect-observed` releases a ticket (§11), and the door is reachable by every
hosted agent user, so the door records an observation or a correction only from
the operator's own uid, which the kernel attests. A carried-over reading
authorizes nothing and keeps the door's rule for everything else: any caller,
its uid a record.

A Question pins the subject of a standing entry. A Finding does the same for an
entry opened by an unprompted assessment: it declares its own id, subject, question
and consequence in that first payload. Every later assessment recorded under
either entry shape must carry evidence about that declared subject; a different
kind or instance is refused rather than appended to the wrong entry.

The direction that carries nothing at all is the one back to the subject: the
notebook is never a prompt input for the agent being observed. Watching is one
direction, and §4 says what that means for whoever registers an experiment.

Every entry and action id is qualified `<domain>/<observer>/<name>`. That is the
whole of the collision story: the Wren observer's `days-repeating` and the
harness observer's `days-repeating` are different entries, neither one's receipts
are reachable from the other's id, and an id qualified for somebody else is
refused rather than merged. Both domains' fixtures live in
`internal/observer/testdata` and are driven through the same validators and the
same notebook, which is what keeps the contract from quietly becoming a Wren
schema with a general name.

## 2. Evidence versus generated interpretation

A Bundle is what was read. An Assessment is what a model made of it. They are
separate payloads, stored as separate records, and the assessment carries its
bundle rather than replacing it.

The reason is that the two decay differently. A reading of `repeat_rate` over a
named window, out of a named snapshot, taken by a named method revision, is
checkable years later by anyone who still has the snapshot. "Her days have
narrowed" is a sentence a model produced on a particular day from a particular
prompt, and it is worth exactly what the reading under it is worth. Stored in one
field, no later reader can tell which of the two they are quoting, and the
sentence outlives the number that justified it.

So: a bundle names the observer, the domain, the subject instance, the method
revision, an explicit window with an IANA zone, the collection and snapshot it
was read out of, its source references, its sample coverage, and an explicit
reason for every source it could not read. A missing reading carries a reason and
no value, because an absence compared against a baseline as zero is a fabricated
fall. An assessment names an outcome from a fixed vocabulary and an
interpretation in prose, and it may not sign another observer's bundle as its
evidence.

An incompatible baseline is refused rather than compared. A delta across a
changed method revision, a different subject instance or another collection is a
number about the measurement, and the only honest thing to do with it is decline
to compute it.

## 3. Question state and assessment outcome

Question states are `observing`, `answered` and `retired`. Assessment outcomes
are `effect-observed`, `effect-absent-with-sufficient-opportunity` and
`insufficient-evidence`. They are different vocabularies on purpose: the
assessment records what the evidence showed and never sets a state, and the
transition rules — which shuttle owns, not the domain — say what that does to
the question. `insufficient-evidence` closes nothing, ever.

Time expiring is not evidence. A window that passed says only that nobody looked,
so no deadline retires a question and no deadline reopens one. Retirement is an
explicit decision with a reason; a reopen takes a named condition
(`re-activated`, `method-revised`, `contradicted`) and applies only to an
answered question. A retired question does not come back: it was taken off the
list deliberately, and a new question is the honest way back on.

A person's observation goes through the same transition rules as the
observer's assessment: their `effect-observed` or absence answers an observing
question, and their `insufficient-evidence` closes nothing. The sufficiency
rule below is a rule about a bundle and does not apply to a statement that
carries none; the person is its authority. A correction and a carried-over
reading are recorded and move no state. The first says what was wrong with a
row the rules already applied, and re-applying them from it would rewrite what
the notebook did on the day; the second was applied, if at all, by the store it
came from.

`effect-absent-with-sufficient-opportunity` is the outcome the rules guard
hardest, because it is the one that says a change did not work. It requires a
live activation, evidence whose window opens at or after that activation and
covers the declared duration, and coverage of at least the declared number of
opportunities. Short of any of those it is refused, since what it is describing
is `insufficient-evidence` wearing a label it did not earn.

## 4. How an experiment is registered

A ticket reference on a standing question is provenance and nothing more: it says
where the question came from. Feedback on a change is a different object, and
what makes it one is four declarations — not who wrote them. A worker registers
an experiment for the change it just landed; a person declares one for a change
they made by hand, often with no ticket at all. Either way the author is
recorded in `author` rather than assumed from whoever transported the payload,
and the same four declarations are required:

1. **The expected effect** — what should become true, in the subject's own terms.
   Without it there is nothing to observe and an absence means nothing.
2. **An activation** — the deployment or restart that made the change live for
   the named instance. A commit time is provenance and is explicitly not an
   activation: merged code is not running code, and dating the window from the
   merge counts opportunity the subject never had. A registration carrying only a
   commit time is `pending`, which is a legitimate state and not a failure. A
   registration naming nothing at all — no event, no commit — is refused: pending
   has a floor, and an absent declaration is not a state.
3. **A window and an opportunity requirement** — how long after activation, and
   how many chances the subject must actually have had. Elapsed time alone is not
   opportunity.
4. **Evidence requirements and a baseline** — the readings that must be present,
   and a baseline carrying the method revision, subject instance and scope they
   will be measured against.

Anything missing from that list is refused naming the field, or leaves the
experiment pending. Nothing is inferred and nothing is defaulted: a harness that
fills in a window would be reporting a measurement nobody declared, and one that
stamped an unstated author with the observer's identity would be reporting a
proposal as a decision.

The registration is a payload through the adapter and that is the entire
interface. A worker does not read the notebook, does not see the observer's
other questions, and does not learn what has already been concluded about the
subject; it declares what its own change should do and hands that over. The
notebook is likewise never a prompt input for the observed subject — watching is
one direction. That is what "without inheriting the observer's context" means
here: two stores, one payload between them, and no path back. For a notebook
Shuttle holds, the payload goes through the observer door (§12).

## 5. Domain ownership and the adapter protocol

The notebook is the domain's. A domain persists entries however it already stores
things — a TOML file, a table, a directory of markdown — and shuttle supplies the
wire format, the validation and the state transitions it applies on the way in.
`Notebook` in `adapter.go` is that protocol and `Memory` is the reference
implementation both fixture domains are exercised against, so an adapter has
something to match rather than a paragraph to interpret.

A domain may instead leave the notebook to Shuttle. `Held` in `held.go` is a
notebook Shuttle holds itself, with `Memory`'s rules rather than a copy of them:
every operation loads the notebook into a `Memory`, applies the operation there,
and writes back only what an accepted mutation changed. It is one git repository
per observer identity, at `<state root>/notebooks/<domain>/<observer>` (by default
under `~/.shuttle`), and it is never rebuilt from anywhere else:

- **One file per entry.** `entries/` holds each entry with the actions claimed
  against it and their receipts; `requests/` holds each applied request id with
  the outcome a replay returns; `notebook.json` holds the revision and the
  owning identity. A file is named by the digest of its id, since ids carry `/`
  and the volume may fold case; the id is inside the file and in the commit
  subject. Identities differing only in case share one directory on such a
  volume, so every operation, a push included, refuses a repository recorded
  for another identity, naming the notebook and both identities.
- **One commit per accepted mutation.** The subject names the operation and the
  entry or action, the body the request and the revisions. A refusal or a replay
  writes and commits nothing, so the history is the notebook's revisions one for
  one.
- **Serialized across processes.** Every operation, reads included, holds an
  exclusive lock on the sibling `<observer>.lock` for the whole load, apply and
  commit. That is what keeps the daemon and `shuttle reconcile` from losing each
  other's writes, and it is what orders a claim against a retirement across
  processes the way `Memory`'s mutex does within one. A writer that dies between
  writing its files and committing them leaves a `<observer>.pending` marker,
  and the next operation rolls the tree back to the last commit: that mutation
  was never acknowledged, and sweeping it into a later commit would break the
  one-for-one history.
- **Its own copy off the machine.** Nothing else under the state root has a copy
  off the Studio, so the repository's `origin`, when the operator adds one, is
  pushed by the end of every observer run that reaches its model, of every
  reconciliation that writes to it, and of every write the door applies
  between runs (§12). A `local-only` registration's notebook is never pushed,
  whatever remote it has. A failed push is recorded in
  `observer-result.json` as `notebook_push: failed` with the git error, and
  logged; it does not fail the run, since the commits are durable locally and
  the next push carries them.
- **Denied to the model.** The notebooks root is denied to every observer model
  run by name, beside the rest of `~/.shuttle`, so a state root kept elsewhere
  does not open it.

The protocol in one place:

- **Stable, qualified ids.** Assigned by the observer, never reassigned.
- **Revision preconditions.** Every mutation carries the notebook revision it was
  decided against. A stale one is refused and writes nothing, so a request
  composed before a decision it could not see never replaces that decision.
- **Append-only observations.** Recording adds a row. A second reading of the
  same window is another row, never a correction of the first, and callers get a
  copy of the history rather than a handle on it. A correction is a row of its
  own naming an earlier one, which must be on the entry and must not itself be a
  correction: the row that was believed is the one corrected, so every
  correction of it names that row. An entry's `corrected` lists, ascending, the
  positions some correction names. It is derived from the rows every time the
  entry is read — never stored, so marking a row corrected never edits it — and
  shuttle derives it from an adapter's response rather than taking it, so an
  adapter need not store it either.
- **Duplicate request ids.** A repeat with the same content returns the original
  outcome; the same id with changed content is refused. The request id is checked
  before the revision, because a replay after an uncertain response necessarily
  carries the revision the caller last saw. Request ids are scoped to the
  identity that issued them — an adapter keys its request log by observer and id
  together — so an id need only be unique to its own observer.
- **Explicit retirement**, with a reason.
- **Replay after uncertainty.** A caller that never saw its response repeats the
  request under the same id. A claim whose ticket write may or may not have
  landed is reconciled by settling the same action id.

## 6. Retirement and action ordering

The notebook serializes claim and retirement, and durably records both. That
makes their order decidable, and it is admission that is being ordered — not a
transaction across the notebook and the ticket store, which are two stores with
two failure modes and no shared commit.

- **Retirement first:** no later claim is admitted, and nothing is in flight.
- **Claim first:** the action id is in flight. The retirement then refuses every
  further claim and *returns* the in-flight ids rather than swallowing them,
  because it cannot promise anything about a ticket write already on the wire.
  That action may still settle, or be reconciled by receipt under the same id.
  Settling records its outcome and does not reactivate the retired question.

Anything stronger would be a lie: the write is somebody else's store, and the
only thing this side can honestly do is stop admitting new ones and say which
ones were already out.

## 7. Retention

The notebook is a record of what was believed and when, so what it keeps is not a
cache and is not rebuilt.

- **Observations are kept indefinitely and are never rewritten.** A number that
  moved the day after we shipped something is a different fact from one that
  moved on its own, and only the dated, append-only record tells them apart. An
  edited observation destroys exactly that distinction.
- **A corrected observation is kept.** A reading that was wrong is corrected by
  appending a correction that names it, not by editing it: the record has to
  show what was believed on each day, and the day it stopped being believed is
  the correction's own row. The earlier row stays exactly as recorded and is
  marked corrected when read (§5).
- **Entries are kept after retirement**, with the retirement's reason and the
  observations under it. A retired question is closed, not deleted; deleting it
  would remove the evidence for a decision somebody took.
- **Receipts are kept for the life of the entry.** They are the only record that
  an action was proposed under an id, which is what makes a replay after an
  uncertain response reconcilable rather than a second write.
- **Bundles are kept with the assessment that cites them.** A bundle references a
  snapshot it does not contain, so a domain that prunes the underlying store
  keeps the bundle anyway: what was read stays checkable in shape and provenance
  even when the source is gone, and that is different from a reading nobody ever
  recorded.
- **What a domain may prune** is its own upstream source data on its own policy.
  The notebook holds no secrets and no transcript text; it holds readings,
  references and the observer's prose.

## 8. Domain-command transport

A registration names its notebook one of two ways. `"held_notebook": true` names
the notebook Shuttle holds for the definition's identity (§5); Shuttle calls it
in-process through the same interface, with no command, no wire and no exit
codes, and a registration that also carries a `notebook` command is refused.
Otherwise the notebook is a domain command, as below. The command transport stays
supported while a domain's notebook is still its own.

An adapter is a spawned domain command, configured by the operator. Shuttle
starts it directly rather than through a shell, writes exactly one JSON request
to stdin, closes stdin, and reads exactly one JSON response from stdout. Each
invocation is one operation against the domain-owned notebook; persistence and
serialization across invocations belong to the adapter. Diagnostics go to
stderr and never share stdout with the response.

Validation stays on the shuttle side of the boundary in both directions.
Shuttle validates a payload before placing it in a request and validates a
successful response before using it. An adapter parses JSON and implements the
notebook behaviour, but does not reproduce `checkQualified`, `checkDay` or any
other payload validation. There is deliberately no JSON Schema or second
validator for an adapter to keep in sync. Mead's validation of its own TOML
store is separate from this wire-payload validation.

Every request is an object with an `operation` string. The operation names and
their additional fields are:

| Operation | Request fields | Successful stdout |
|---|---|---|
| `revision` | none | `{"revision":<integer>}` |
| `entry` | `entry_id` | the Entry payload |
| `list` | none | `{"entries":[<Entry>,...]}`, every entry, retired and answered ones included, ordered by id |
| `register` | `request`, `payload` (Question) | the Entry payload |
| `activate` | `request`, `payload` (Experiment) | the Entry payload |
| `record` | `request`, `payload` (Assessment) | the Entry payload |
| `claim` | `request`, `payload` (Action) | the Receipt payload |
| `settle` | `request`, `payload` (Receipt) | the Receipt payload |
| `retire` | `request`, `entry_id`, `reason` | a Retirement object |
| `reopen` | `request`, `entry_id`, `condition` | the Entry payload |

`request` has `identity`, `revision` and `request_id`, using the same JSON names
as the conformance data. `payload` is the already validated `observer/v1`
object, not a path; paths belong only to the conformance files that load those
objects. Retirement has the fields `entry_id`, `reason`, `at`, `in_flight` and
`revision`. Each entry in a `list` response is validated as an `entry`
response is, and the listing is refused unless its ids ascend bytewise with
none repeated. An adapter that does not implement `list` fails every run's
snapshot as `notebook-failed` (§10) until it does.

Exit status carries the sentinel refusal class. On a refusal stdout is empty and
stderr may contain a diagnostic:

| Exit | Refusal |
|---:|---|
| 10 | stale revision (`ErrStaleRevision`) |
| 11 | retired entry (`ErrRetired`) |
| 12 | missing entry (`ErrNoEntry`) |
| 13 | action was not claimed (`ErrNotClaimed`) |
| 14 | request id reused with changed content (`ErrRequestConflict`) |
| 15 | action id already held (`ErrActionHeld`) — a claim under an id already in flight or settled |

The observer door reports a refusal by this table too, and `shuttle observer`
exits with it (§12).

Exit 0 means stdout holds the successful response. Exit 1 is an adapter or
storage failure, not a contract refusal. A signal, timeout, malformed stdout,
extra stdout or a payload that fails shuttle's response validation is likewise
a runtime failure and is never translated into one of the sentinel exits.

## 9. Adapter conformance

The published suite is under
`internal/observer/testdata/conformance`. `manifest.json` is the complete,
ordered list of scenario files under `scenarios/`; the Go runner fails if the
manifest names a missing file or the directory contains an unlisted one.
Payload paths in a scenario are relative to its file and resolve under
`internal/observer/testdata`, so another language reads the same JSON fixtures
that the Go runner gives to `Memory`.

A scenario has a name and ordered steps:

```json
{
  "name": "retirement refuses a later claim",
  "steps": [
    {
      "operation": "register",
      "request": {
        "identity": {"observer": "mead", "domain": "wren"},
        "revision": 0,
        "request_id": "register"
      },
      "payload": "../../wren/question.json",
      "expect": {
        "status": "ok",
        "revision": 1,
        "entry": {
          "id": "wren/mead/days-repeating",
          "state": "observing",
          "observations": [],
          "in_flight": []
        }
      }
    }
  ]
}
```

Each step names one of the transport operations and the outcome it must
produce. `status` is `ok` or `refused`; a refusal also names one of
`stale-revision`, `retired`, `no-entry`, `not-claimed`, `request-conflict`
or `action-held`. `revision` is the notebook revision after the operation.
Entry expectations project the stable protocol facts used by these scenarios:
`id`, `state`, optional `experiment_status`, ordered observations as `at` and
`outcome`, and ordered `in_flight` ids. A `list` step expects `entries`, those
projections in id order. Receipt and retirement expectations use
their corresponding fields; `"at":"*"` accepts any non-empty generated
timestamp in a direct expectation. `{"same_as":3}` means the complete outcome,
including such a generated timestamp, must equal the third step's actual
outcome exactly; step numbers are one-based.

An adapter demonstrates conformance by running every manifested scenario
against a fresh notebook and comparing every step in order. It must report a
difference with the scenario name, one-based step index, operation and expected
and actual outcomes. Passing the payload validators alone is not conformance:
the scenarios are what exercise stale-write refusal, idempotent replay,
request-id scoping, append-only observations and claim/retirement ordering.

## 10. Shuttle observer runs

An observer agent selects a named operator registration with
`"observer":{"config":"<name>"}`. The registration lives in `config.json`, not
in model-authored output or the agent prompt:

```json
{
  "observers": {
    "mead": {
      "definition": "/absolute/path/to/observer.json",
      "collector": {
        "argv": ["/absolute/path/to/collector", "collect"],
        "working_dir": "/absolute/domain/root"
      },
      "notebook": {
        "argv": ["/absolute/path/to/notebook-adapter"],
        "working_dir": "/absolute/domain/root"
      },
      "data_policy": "cloud",
      "deny_read": ["/absolute/domain/store"],
      "max_model_input_bytes": 65536,
      "timeout_seconds": 60,
      "tickets": {
        "projects": ["shuttle"],
        "discovery_project": "shuttle",
        "max_discoveries": 1
      }
    }
  }
}
```

`tickets` is the observer's ticket write scope and is optional. `projects` names
the tk projects its notes, reopens and releases may land in; `discovery_project`, one of
them, is where a discovery is created; `max_discoveries` caps the tickets one
run may create, and `0` refuses discoveries outright. When the block is present
`paths.tk_config` must be an absolute path to a loadable tk config, since the
store is opened from its `central_root` the way the tk providers open it. A
registration without the block applies nothing: every action is retained and
refused with the reason `ticket output is not configured for this observer`.

Both commands — the collector alone, for a held notebook — are executed directly
as argv, never through a shell, from their declared working directories and with
a minimal environment. A registration marked `local-only` is refused before a
Claude run or auth probe; adding a local runtime is separate work. Every
registration names at least one absolute domain store root in `deny_read`; every
observer run denies the union across all registrations, so a cloud observer
cannot read another observer's local-only store. A denied root inside the run
directory is refused because the run-directory allowance would reopen it. The
observer definition is strict `observer-run/v1` JSON with `revision`,
`identity`, `questions` and `instructions`. `questions` is still validated —
each id qualified for the identity, none twice, an empty list allowed — but it
no longer selects what a run reads: a question registered after the
definition was written, by a person, a book or a worker whose ticket landed,
is read like any other.

Before model execution Shuttle takes a stable notebook snapshot by reading the
revision, the notebook's `list` and the revision again. It retries a moving
snapshot three times. The snapshot is every listed entry of the definition's
identity that is `observing`. A retired entry is not read. An answered one is
not read either until a reopen returns it to `observing`, which is the only way
back §3 allows. The collector receives one `collect` request containing
the run, window, definition identity/revision and that snapshot. Its successful
response is:

```json
{"status":"observed|empty|unavailable","evidence":[],"exploration":"..."}
```

Every evidence item is validated as a Bundle and must carry the configured
identity. `empty` carries no bundle. `unavailable` carries a bundle that names
an unavailable source or reading. The complete definition, notebook snapshot
and collector response remain under `observer/`; `inputs/observer.json` is a
separate bounded presentation. Bounding drops globally oldest history, then
evidence from the end, then truncates exploratory prose, and records those
losses rather than truncating JSON. A dropped observation is presented as
`null` in its place rather than spliced out: a row's position is what a
correction and an entry's `corrected` name it by (§5), so every row left keeps
the position it has in the notebook, and a `null` row is omitted history. Observer-run Read, Glob and Grep
permissions cover only `inputs/`. Full definition, notebook and evidence
artifacts remain in a sibling staging directory covered by the sandbox's
`~/.shuttle` denial while the model runs, then move atomically under
`observer/` after the model process exits. The model therefore cannot bypass
the bounded presentation through its cwd even if its tool runtime grants
default reads there.

The model may write only `out/observer.json` and an optional `out/report.md`.
The first carries required `silent`, plus Assessment and proposed Action
arrays; a non-silent run configured for email also carries a printable
`subject` of at most 120 characters. An assessment is accepted only when it is
the observer's own — no author other than the observer, no `corrects`, no
`carried_over` (§1) — and its evidence is byte-equivalent after canonical JSON
encoding to a collector bundle. An action's evidence references must come from the same collection;
what becomes of the action after the model exits is §11.
Delivery retry reads the original output capabilities retained in `run.json`;
turning email on in a later agent definition cannot promote a historical
file-only report into a message it was never validated to send.

Validated assessments are recorded through the notebook adapter with request
ids derived from the immutable run id and assessment position. The same request
and revision are replayed once after an uncertain adapter failure. Stale
revisions, retired entries and changed content under a request id are surfaced;
none is rewritten against a newer revision. `observer-result.json`,
`observer/validated.json` and `applied.json` retain the collection outcome,
validated output, request ids, revisions, the per-action apply record and
optional report application. This is why an empty collection, unavailable
source, invalid payload, collection failure and deliberate model silence
remain different facts after the run.

## 11. Ticket actions

After the assessments are recorded, shuttle applies the validated actions
through tk's conditional action protocol — `ApplyAction` under a stable action
id held against a precondition read from the ticket, and `CreateDiscovery`
under a finding key — never through ordinary note or status calls, which
cannot promise a lost response is not a second write. Authorization is
decided by shuttle from the retained snapshot and this run's assessments
alone; nothing in source prose, ticket text or a writer's `source` label can
widen it, target another ticket, or stand in for a person's decision.

- **Authorization.** Exactly one assessment in the run must be about the
  action's entry, and `insufficient-evidence` authorizes nothing. A note,
  reopen or release must name a standing question's own ticket — the
  experiment's, else the question's — in a configured project; a finding
  opened this run has no ticket and takes no note. A note follows a
  confirmation or an absence and never changes status. A reopen needs
  `effect-absent-with-sufficient-opportunity`, a live experiment, and the §3
  sufficiency rule re-run by shuttle against the snapshot entry; a pending
  activation, an incompatible baseline, a short window or too few
  opportunities refuses it. A release needs `effect-observed` — the event the
  ticket was parked on occurred — and nothing else from the assessment. On a
  parked ticket an absence never releases and never closes: however long it is
  sustained, it only adds a note, and the ticket stays for a person to decide. A
  discovery needs a finding, a title, and room under `max_discoveries`. An
  action is applied only after its authorizing assessment's record receipt —
  the request id the notebook admitted it under — is in `applied.json`;
  without one the action is held as failed, never written.
- **Order.** Authorize; read the store once (a reopen needs its ticket `done` —
  `closed` is a decision and is not reopened; a release needs its ticket
  `backlog` with a non-blank `waiting_on`, and not an epic, whose status is
  derived from its children; a ticket already holding a receipt under the
  reopen or release id is left to tk, which replays an identical write and
  refuses a different one; a discovery looks for the ticket already holding
  its finding key, else a related one); claim in the notebook; write to tk;
  settle the receipt. What was read — the ticket precondition, or
  the prior or related ticket — is persisted with the claim's request id before
  the claim is attempted, and every retry of the claim and the write is held
  against exactly that value, never a re-read, so a ticket edited after the
  read is refused by tk as changed even when the claim's response was lost.
  `ErrRetired` on the claim refuses the action with no tk write, and so does
  `ErrActionHeld`: an action id the notebook already holds, from an earlier
  run's proposal on the same question, is refused rather than claimed again,
  and a new proposal needs its own id. A mechanical failure ahead of the claim
  (an unreadable ticket, a notebook that moved twice) records `failed` and
  leaves the action at its stage for reconciliation, as does a claim the
  adapter still fails after its replay; the remaining actions are driven
  before the run fails. Every stage is recorded in `applied.json` with the
  reason for a refusal or failure, and `observer-result.json` counts applied,
  refused and failed.
- **The reopen cap.** Every reopen is written under the fixed tk action id
  `observer-reopen`. tk keeps one receipt per id in the ticket file, so a
  second proposal carrying a different note is refused by the ticket itself,
  across questions and runs, and the refusal is recorded. The cap is durable in
  the ticket and replicates with it.
- **The release cap.** Every release is written under the fixed tk action id
  `observer-release`, through tk's `release` transition: it clears
  `waiting_on`, moves the `backlog` ticket to `ready`, and lands the note and
  the receipt in the same write. The cap is the reopen cap's: a second release
  proposal on the ticket carries a different note and is refused by the
  ticket's own receipt, even after a person parks it again, and the refusal is
  recorded.
- **Discovery.** A finding key already claimed in the project is answered by
  the key — the same content replays, different content is refused. Otherwise
  the discovery project is searched for a related ticket: one whose title
  shares at least two whole-word terms of four or more characters with the
  proposal's title and the finding's question (a heuristic, not an
  exactly-once guarantee; body and note text do not count). One found receives
  the evidence as a note under the action id instead of a new ticket. A created
  ticket is `backlog`, tagged `observer`, and carries the finding's question and
  the proposal as its description, its consequence as an unverifiable criterion,
  and an evidence section. Its title is one printable line of at most 120
  characters, refused at validation otherwise. Capacity under
  `max_discoveries` is reserved at the decision, not the write: a discovery
  counts from the moment its persisted decision is a creation — no prior
  ticket holding the key, no related ticket — whether its claim is still
  unresolved or its write has landed, so a later discovery in the same run is
  refused while an earlier one is unresolved; a related note and an earlier
  run's ticket do not count.
- **Notes are the whole write surface.** Every note is composed by shuttle from
  validated fields — identity, entry, outcome, run, the proposal's prose and
  the evidence references. No path edits a description, acceptance criteria or
  design, on any ticket. A line of the prose that tk's format would read as
  structure — a `## ` heading, or a `**<RFC3339>**` note delimiter — is
  escaped with a backslash before it is written, so it reads back as text and
  the evidence and criteria composed after it survive the next write; line
  endings are normalised the way tk's scanner reads them (`\r\n` and `\r` to
  `\n`) before those lines are found. The discovery's criterion and every
  evidence value — source kind and reference, window bounds, timezone,
  snapshot — are written as one literal line with every line break collapsed
  to a space, so none of them can open a heading, a delimiter, a further
  criterion or a `verify:` command.
- **Reconciliation.** Claim and settle carry request ids derived from the run
  id and action position; tk writes carry the action id or finding key. A run
  that lost a response, or died between stages, is resumed from the recorded
  stage by `ReconcileObserverActions`: a claim or settle replays under its id,
  a tk write that landed is answered `Replayed`, and a conflict — a moved
  precondition, a different note under the reopen or release id — is recorded
  as refused rather than written over. A release reconciles on the same terms:
  a lost claim or settle, or a run that died after the claim or after the
  release landed, resumes from its stage and leaves the ticket released once
  under `observer-release`. The destination is bound at the decision: the
  tk store root and, for a discovery, the project are persisted with the
  record before the claim, and a resume whose registration now names another
  store or `discovery_project` does not redirect the write; it records the
  action `failed` with both destinations and leaves it for a person. An
  assessment without a record receipt is replayed first under its original
  request id; one the notebook still refuses is reported as the failure it is
  and its action stays unadmitted. The operator entry point is
  `shuttle reconcile <run-id>`. It takes only a failed run and refuses any
  other before a notebook call. A failed run with nothing left to drive — every
  assessment holds its receipt and every action is settled or refused — makes
  no notebook call, so the retained `notebook_revision` is not rewritten, and
  goes straight to the finish. A reconciliation that converges finishes the
  run as a live one would: the report is prepared, `observer-result.json` is
  rewritten as `applied` with the reconciled counts over the retained
  collection, the row moves to `completed`, `result.json` follows it, and the
  report is delivered. The failure streak, its incident and any pause it
  caused stand. A report already filed for the run is not filed again. One
  that does not converge rewrites `observer-result.json` with its counts as
  `conflict-or-failure`; one whose finish fails rewrites it as `applied` with
  the failure as its detail. Either leaves the row `failed`, and running the
  reconciliation again resumes from there.
- **Retirement.** §6's ordering holds at the apply layer: a retirement ahead of
  the claim refuses it, and one behind it reports the action in flight while
  the write lands and settles under its own id, leaving the question retired.
- **A person's observed event.** A person who saw the event a ticket is parked
  on says so with an `observe` carrying `effect-observed` (§12), and that
  authorizes the release a run's `effect-observed` would. tk has no finish
  transition, so "release or finish" is the release today. The door applies it
  right after the observation is recorded, on the run's terms: the standing
  question's own ticket, the experiment's else the question's, in the
  registration's configured `projects`; a `backlog` non-epic ticket with a
  non-blank `waiting_on`, read once and held against that precondition; tk's
  `release` under `observer-release`, so the release cap is the same cap. The
  observation is the authority in place of a run's assessment, so there is no
  claim or receipt in the notebook; the observation's own record is the
  admission. The note is composed by shuttle from the validated observation —
  author, entry, outcome, its `at`, the door request id and each fact as one
  literal line — and reads no clock, so a repeat of the same door call replays
  identically at tk, and that repeat is how a release that failed is tried
  again. Another person's observation composes another note and is refused by
  the ticket's receipt. A refused or failed release never undoes the
  observation. Any other outcome from a person, every correction and every
  carried-over reading authorize nothing: no ticket is read or written for
  them.

## 12. The observer door

A question's author — a person, a `/work` run, a worker in a hosted slot —
reaches a notebook Shuttle holds through the running daemon, never through the
notebook's files, which a hosted agent user cannot open. `Door` in `door.go` is
the server and `shuttle observer` its client:

```
shuttle observer register [question.json] --request-id ID
shuttle observer activate [experiment.json] --request-id ID
shuttle observer retire <entry-id> --reason TEXT --request-id ID
shuttle observer reopen <entry-id> --condition NAME --request-id ID
shuttle observer observe [observation.json] --request-id ID
shuttle observer correct [correction.json] --request-id ID
shuttle observer carry [reading.json] --request-id ID
shuttle observer list <name>
```

- **Transport.** A unix socket the daemon serves whenever a registration names
  `held_notebook`: `/Users/Shared/weft/observer/door.sock`, or
  `$SHUTTLE_OBSERVER_SOCKET` for the daemon and the command alike. The daemon
  creates the directory, refuses one that is not the operator's or is a
  symlink, and sets it 0750 and the socket 0660. Both take the shared root's
  group, `weftagents` (docs/agent-users.md), so a hosted agent user connects
  without traversing the operator's home or holding the weft bearer token, and
  no other user connects. A socket it cannot take ends the daemon's start, so
  a machine with a held notebook needs the shared root `setup.sh` makes. The
  door is not part of the weft service and reaches nothing of it.
- **Seven operations.** `register` takes a Question payload, `activate` an
  Experiment, `retire` an `entry_id` and `reason`, `reopen` an `entry_id` and
  `condition`. `observe`, `correct` and `carry` each take an Assessment payload
  of one kind (§1) and record it: `observe` a person's observation, `correct` a
  correction, `carry` a carried-over reading. `observe` and `correct` are
  taken only from the operator's uid (Attribution, below). The kind is the
  operation's before anything else in the payload is judged, so a payload of another kind is refused
  naming the field that makes it one — `corrects`, `carried_over`, or
  `author.kind` for an `observe` not by a person. The observer's own assessment
  has no operation: it is a run's alone. Nothing else in the notebook is
  reachable: no entry, no observation, no revision is ever read out through the
  door.
- **The one read is local.** `shuttle observer list <name>` does not use the
  door. It is the operator's: it loads config.json, takes the registration
  named `<name>`, loads its definition and builds the notebook exactly as a run
  does — the one held under the state root, or the §8 command adapter with a
  run's minimal environment — and prints every entry of the definition's
  identity as a JSON array of `id`, `state`, `author` and `latest` (the outcome
  and `at` of the last observation, or null). A question's author is the one
  it declares; an entry a finding opened is authored by the observer. A hosted
  agent user cannot open the held notebook's files, so this read stays on the
  operator's side of §4's one direction.
- **Wire.** One JSON object per connection, then the write side closed, and
  one object back:
  `{"operation":"register","request":{"request_id":"..."},"payload":{...}}`.
  Unknown fields are refused, so `request.revision` and `request.identity` are
  refused, and so is a field the operation does not take.
- **Which notebook.** The one held for the identity the payload is by, or the
  entry id is qualified for. Only an identity named by the definition of a
  `held_notebook` registration in the config.json the daemon started with is
  accepted; anything else is refused naming `identity` or `entry_id`.
- **Shuttle supplies the revision.** The payload is validated by the same
  validators as everywhere else, so a missing `author` or an activation naming
  nothing is refused naming the field, and a commit-only activation is
  `pending`. The daemon then reads the notebook's revision and applies the
  operation at it. A concurrent writer landing between the two is a stale
  revision the caller never chose, so it is read again, up to three times;
  that is safe because the request id is checked before the revision. A
  replay answers what the first attempt did, and changed content under the id
  is refused.
- **The answer.** `{"status":"ok","answer":{...}}`. A register, reopen,
  correct or carry answers `entry_id` and `state`; an activate adds
  `experiment_status`; an observe adds `release`, what became of the release
  §11 lets a person's `effect-observed` authorize: `{"status":"applied"}`,
  `refused` with the rule's `reason`, or `failed` with the detail withheld to
  the daemon log, each with the `ticket` it was decided on, or
  `{"status":"none"}` for any other outcome. A retire answers
  `entry_id`, `reason` and `in_flight`, the actions already claimed (§6).
  Nothing else from the notebook travels back: watching is one direction (§4).
  The release's ticket is the one exception, and it is the caller's own
  question's provenance.
- **Pushed once written.** A write the door applies — a replay of one
  included — is pushed to the notebook's `origin` after the answer is written,
  as the end of a run pushes it (§5), so a change made between runs does not
  sit only on this machine until the next one. A notebook with no `origin`,
  and a `local-only` registration's whatever its remote, is not pushed. A
  failed push is logged and changes nothing: the write is committed and
  answered, and the next push — the door's next write or the next run's end —
  carries it. Nothing about the push travels back to the caller.
- **Refusals.** `refused` carries §8's `exit` for its class and the `field`,
  and `shuttle observer` exits with it: 10 stale revision, 11 retired, 12 no
  entry, 14 request conflict. `invalid` is a call or payload the validators
  refused, naming `field`; `failed` is the door or the notebook's storage.
  Both exit 1. A refusal of what the caller sent travels back whole. One the
  notebook made carries only its field and class, never the notebook's own
  text: that text can quote a stored subject, an entry's state or the
  revision, which the door does not read out. The daemon log carries it.
- **Attribution.** The daemon reads the caller's uid from the kernel
  (`LOCAL_PEERCRED`) and logs one line per call to `daemon.log`: uid,
  operation, request id, entry id and outcome, with exit, field and error for
  anything not ok. An observe logs a line beside it with the request id, entry
  id and the release's status, ticket and reason, and an applied write one
  with the request id, entry id and the push's outcome — `pushed`,
  `nothing-to-push`, `no-remote`, `local-only`, or `failed` with the error. A
  call whose peer cannot be read is not applied, so off darwin the door
  applies nothing. For `register`, `activate`, `retire`, `reopen` and `carry`
  the uid is a record, not an authority: the author is what the payload
  declares (§1). For `observe` and `correct` it is the authority. A person's
  observation carries the release authority of §11, and the door's socket is
  open to the `weftagents` group, so a hosted agent user could otherwise
  declare itself `human` and release a ticket. Those two are applied only when
  the peer uid is the daemon's own — the operator's — and any other peer is
  refused as `invalid` naming `author.kind`, applying and releasing nothing.
