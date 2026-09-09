# Observer contract

Status: written 2026-09-09 alongside `internal/observer`, which is the executable
half of this document. Where the two disagree the code is right and this file is
stale. Nothing here is wired to a runner, a schedule or a CLI: the package is the
contract, its validation, a reference notebook and two domains' fixtures.

An observer cannot judge a change consistently if every domain invents its own
meaning for a window, a baseline, missing evidence or a retired question. This
document is the part of that agreement the types cannot hold: what is fact and
what is a reading of it, who owns which store, what is kept and for how long, and
how a worker registers an experiment without inheriting the observer's context.

## 1. What travels

Six payloads, all JSON, all carrying `contract_version` (`observer/v1` today).
A payload with no version, or one this binary does not read, is refused naming
`contract_version` and listing what it does read — the notebook outlives the
binary, so a record has to say which rules it was written under rather than be
guessed at from its shape.

| Payload | Is | Written by |
|---|---|---|
| Question | a standing question about a subject: what would answer it, what answering it would change, how fast its inputs move | the observer |
| Experiment | a ticket's declared effect, its activation, the window and opportunity it needs, and the readings and baseline it will be judged by | the worker that made the change |
| Bundle | recorded fact: readings over an explicit window, from named sources, out of a named snapshot, with what could not be read | the observer |
| Assessment | one outcome and one prose interpretation, carrying the bundle it was drawn from | the observer |
| Action | ticket-facing feedback under a stable id | the observer |
| Receipt | what became of an action, under that same id | the observer |

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

## 4. How a worker registers an experiment

A ticket reference on a standing question is provenance and nothing more: it says
where the question came from. Feedback on a ticket is a different object, and it
takes four declarations, all from the worker that made the change:

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
fills in a window would be reporting a measurement nobody declared.

The registration is a payload through the adapter and that is the entire
interface. The worker does not read the notebook, does not see the observer's
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
