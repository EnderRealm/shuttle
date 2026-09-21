# Observer contract

Status: written 2026-09-09 alongside `internal/observer`, which is the executable
half of this document. Where the two disagree the code is right and this file is
stale. The contract package contains validation, a reference notebook, two
domains' fixtures, the language-neutral conformance suite and the subprocess
notebook client. `internal/runner` wires that client and the collector boundary
to the same manual and scheduled pipeline used by the daily agent.
`observer/v1` is unreleased on those terms — no records exist under it and no
domain adapter reads it — so a correction to the shape is still made in place; a
version pins a shape once there are records written under it to read.

An observer cannot judge a change consistently if every domain invents its own
meaning for a window, a baseline, missing evidence or a retired question. This
document is the part of that agreement the types cannot hold: what is fact and
what is a reading of it, who owns which store, what is kept and for how long, and
how an experiment is registered without inheriting the observer's context.

## 1. What travels

Six payloads, all JSON, all carrying `contract_version` (`observer/v1` today).
A payload with no version, or one this binary does not read, is refused naming
`contract_version` and listing what it does read — the notebook outlives the
binary, so a record has to say which rules it was written under rather than be
guessed at from its shape.

| Payload | Is | Written by | Direction |
|---|---|---|---|
| Question | a standing question about a subject: what would answer it, what answering it would change, how fast its inputs move | a person, a worker or the observer, recorded in `author` | into the notebook from its author |
| Experiment | a change's declared effect, its activation, the window and opportunity it needs, and the readings and baseline it will be judged by | a person, a worker or the observer, recorded in `author` | into the notebook from its author |
| Bundle | recorded fact: readings over an explicit window, from named sources, out of a named snapshot, with what could not be read | the observer | written into the notebook |
| Assessment | one outcome and one prose interpretation, carrying the bundle it was drawn from; an unprompted assessment also carries the finding that opens its entry | the observer | written into the notebook |
| Action | ticket-facing feedback under a stable id: a `ticket-note` on the entry's ticket, a `ticket-reopen` of it, or a `ticket-open` carrying the title of a ticket to create | the observer | out toward the ticket store, as a proposal (§11) |
| Receipt | what became of an action, under that same id | the observer, recording what the far store did | back from that store: `applied`, `refused` or `failed`, with its reference |

`author` is a kind from a fixed vocabulary — `human`, `worker`, `observer` — and an
optional free `ref` holding whatever identifies that author: a person's name, a
ticket id, a run id. It is required on a question and an experiment because the
three carry different authority, and a reader who cannot tell them apart reads a
model's proposal as a person's decision. A ticket reference on either is
provenance and is optional on both: many questions and experiments are written by
hand with no ticket behind them.

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
here: two stores, one payload between them, and no path back.

## 5. Domain ownership and the adapter protocol

The notebook is the domain's. A domain persists entries however it already stores
things — a TOML file, a table, a directory of markdown — and shuttle supplies the
wire format, the validation and the state transitions it applies on the way in.
`Notebook` in `adapter.go` is that protocol and `Memory` is the reference
implementation both fixture domains are exercised against, so an adapter has
something to match rather than a paragraph to interpret.

The protocol in one place:

- **Stable, qualified ids.** Assigned by the observer, never reassigned.
- **Revision preconditions.** Every mutation carries the notebook revision it was
  decided against. A stale one is refused and writes nothing, so a request
  composed before a decision it could not see never replaces that decision.
- **Append-only observations.** Recording adds a row. A second reading of the
  same window is another row, never a correction of the first, and callers get a
  copy of the history rather than a handle on it.
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
`revision`.

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
`outcome`, and ordered `in_flight` ids. Receipt and retirement expectations use
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
the tk projects its notes and reopens may land in; `discovery_project`, one of
them, is where a discovery is created; `max_discoveries` caps the tickets one
run may create, and `0` refuses discoveries outright. When the block is present
`paths.tk_config` must be an absolute path to a loadable tk config, since the
store is opened from its `central_root` the way the tk providers open it. A
registration without the block applies nothing: every action is retained and
refused with the reason `ticket output is not configured for this observer`.

Both commands are executed directly as argv, never through a shell, from their
declared working directories and with a minimal environment. A registration
marked `local-only` is refused before a Claude run or auth probe; adding a local
runtime is separate work. Every registration names at least one absolute domain
store root in `deny_read`; every observer run denies the union across all
registrations, so a cloud observer cannot read another observer's local-only
store. A denied root inside the run directory is refused because the
run-directory allowance would reopen it. The observer definition is strict
`observer-run/v1` JSON with `revision`, `identity`, `questions` and
`instructions`. Question ids select notebook entries; an empty list is valid
for an exploratory observer.

Before model execution Shuttle takes a stable notebook snapshot by reading the
revision, each configured entry and the revision again. It retries a moving
snapshot three times. The collector receives one `collect` request containing
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
losses rather than truncating JSON. Observer-run Read, Glob and Grep
permissions cover only `inputs/`. Full definition, notebook and evidence
artifacts remain in a sibling staging directory covered by the sandbox's
`~/.shuttle` denial while the model runs, then move atomically under
`observer/` after the model process exits. The model therefore cannot bypass
the bounded presentation through its cwd even if its tool runtime grants
default reads there.

The model may write only `out/observer.json` and an optional `out/report.md`.
The first carries required `silent`, plus Assessment and proposed Action
arrays; a non-silent run configured for email also carries a printable
`subject` of at most 120 characters. An assessment is accepted only when its
evidence is byte-equivalent after canonical JSON encoding to a collector
bundle. An action's evidence references must come from the same collection;
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
  action's entry, and `insufficient-evidence` authorizes nothing. A note or
  reopen must name a standing question's own ticket — the experiment's, else
  the question's — in a configured project; a finding opened this run has no
  ticket and takes no note. A note follows a confirmation or an absence and
  never changes status. A reopen needs `effect-absent-with-sufficient-opportunity`,
  a live experiment, and the §3 sufficiency rule re-run by shuttle against the
  snapshot entry; a pending activation, an incompatible baseline, a short
  window or too few opportunities refuses it. A discovery needs a finding, a
  title, and room under `max_discoveries`. An action is applied only after
  its authorizing assessment's record receipt — the request id the notebook
  admitted it under — is in `applied.json`; without one the action is held
  as failed, never written.
- **Order.** Authorize; read the store once (a reopen needs its ticket `done` —
  `closed` is a decision and is not reopened; a discovery looks for the ticket
  already holding its finding key, else a related one); claim in the notebook;
  write to tk; settle the receipt. What was read — the ticket precondition, or
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
  precondition, a different note under the reopen id — is recorded as refused
  rather than written over. The destination is bound at the decision: the
  tk store root and, for a discovery, the project are persisted with the
  record before the claim, and a resume whose registration now names another
  store or `discovery_project` does not redirect the write; it records the
  action `failed` with both destinations and leaves it for a person. An
  assessment without a record receipt is replayed first under its original
  request id; one the notebook still refuses is reported as the failure it is
  and its action stays unadmitted.
- **Retirement.** §6's ordering holds at the apply layer: a retirement ahead of
  the claim refuses it, and one behind it reports the action in flight while
  the write lands and settles under its own id, leaving the question retired.
