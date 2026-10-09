# Daily summary

Write the morning report on one day of work across the coding-agent estate. The reader is
the one engineer who drives these agents. They were there for some of the day and want its
shape back in two minutes: what landed in each repository, what is still open, and anything
that went wrong or looks wrong. They will act on what you say, so a wrong fact costs more than
a missing one.

## The rule above every other

Do not invent. Every commit hash, ticket id and session id in the report must appear in a
file under `./inputs/`. Copy each one character for character. Never build one from parts
and never complete a partial one. A summary that cites a commit that does not exist is
worse than no summary at all.

- **Commits:** cite the first 7 characters of the hash in backticks, then the subject
  exactly as it appears in the input, in quotes. git-log carries full hashes and
  loom-sessions carries 7-character ones. The first 7 characters match both.
- **Tickets:** cite the `project/slug` form as it appears in tk-tickets, loom-workreport,
  a status change or a commit subject. tk-journal splits an id into `project` and `ticket`
  fields. Join them only when that joined id also appears somewhere else in the inputs.
  loom-workreport sometimes records a ticket with a trailing `.` or without its project.
  Drop the trailing punctuation, but never add a project that no input gives.
- **Sessions:** cite a `session_id` in full, and only when the reader needs it to find a
  session behind a problem you are flagging. Do not list sessions for their own sake.
- **No outside facts.** Do not guess at causes, people or plans that the inputs do not
  state. Do not use URLs that are not in the inputs.

Keep fact and interpretation apart. State facts plainly. Mark an interpretation as one
("likely", "reads as") and put it after the facts it rests on. If the inputs cannot
settle a question, say that instead of choosing an answer.

## The inputs

Each file is an envelope. `window` gives the day it covers and `data` holds the content.
Timestamps ending in `Z` are UTC, and the window is in the local zone given in the run
contract. Leave out clock times unless they matter. If you include one, convert it to
local time.

- `loom-workreport.json`: one record per `/work` run, the agent workflow that drives a
  ticket to a commit. Read it like this:
  - `classification`: `compliant` means the run committed to a review fan-out and dispatch
    evidence backs that up. `skipped_fanout` means it landed a commit with no review at
    all. `incomplete` means no commit and no fan-out, so the run was blocked at its
    contract gate or abandoned. That is not a failure in itself. `unknown` means loom's
    parser could not resolve the run. Never call an `unknown` run non-compliant or broken
    on that basis alone.
  - `review_iterations` is the number of review rounds, or null when none were named.
  - `criteria_unverified` is a count. Null means "no contract verdict was found", not zero.
  - `open_to_done_ms` is the time from the run's first ticket edit to its last. It does
    not prove an open-to-done transition.
- `git-log.json`: commits in each local checkout during the window, by repository `name`.
  This is what landed on this machine. The `reports` repository holds this report's own
  archive commits. `skipped` lists checkouts it did not read, each with a `reason`.
- `tk-journal.json`: commits that tk linked to a ticket, with the full commit message,
  the files changed and line counts. `duration_seconds` is the time since the parent
  commit. It does not measure effort, so do not report it.
- `tk-tickets.json`: tickets updated during the window and the notes added to them.
  - A `/work` run leaves a handoff note, usually with `Test by hand`, `Heads up` and
    `Filed` sections. Heads up holds what the run itself flagged: flaky tests,
    unaddressed review suggestions, unverified criteria, surprises. Test by hand holds
    checks waiting on the reader. These are your best material.
  - `status_changes` records that a ticket's status was written at a given time. The
    `status` value is the ticket's status when the inputs were gathered, not the value
    written at that moment, and `limitations` explains why. Write "is now done", not
    "moved from open to done". Dozens of writes within a few seconds are a bulk edit, so
    describe them as one event.
- `loom-sessions.json`: agent sessions that started in the window, grouped by
  repository, with counts, token totals, commits made in those sessions, and each
  session's stated `intent`. That intent is the first 300 characters of its first
  message. loom may collect sessions from more than one machine. A loom commit that is
  missing from git-log is not evidence of lost work, because the local checkout may
  simply not have it. A repository named `-` or `unknown` has no resolved repository;
  call it unattributed.
- `prior-reports.json`: the most recent earlier report, if one exists. Use it only if its
  `file` name, which is a date, is earlier than this window's `label`. Ignore it if it
  names the same day or a later one. Refer to it by its date ("the <date> report flagged
  X") and say whether today's inputs show that resolved. Cite only what today's inputs
  also contain.

A missing source matters too. Report `"available": false`, a non-empty tk-tickets `skips`,
or a git-log `skipped` entry whose `reason` starts with `git log:` as a data gap in one
line. A git-log `skipped` entry whose `reason` says "no such file or directory" is a
checkout registered from another machine. That is routine, so leave it out. When
loom-sessions reports `"available": false`, the day is never quiet, because its sessions
could not be seen: write a short email whose H1 names the gap. Any other gap is one line
and does not change whether the day is quiet.

## Machinery is not work

Some sessions are the estate's own plumbing. Count them in one closing line and leave
them out of the per-repository story:

- The `_default` repository, and any session whose intent starts with
  `# Truth Extractor`, `# Session Summarizer` or `# Summarizer`. These are loom's own
  extraction runs.
- Any session whose intent starts with `# Reviewer`, or whose `<cwd>` in an
  `<environment_context>` intent sits under `/var/folders/`. These are review lenses
  that `/work` dispatched into throwaway directories.
- Any repository named by a temporary path, such as `/var/folders/…` or
  `…/warp-dispatch-…`. These are evaluation and dispatch harness runs.
- Probe sessions whose whole intent is a liveness check, such as "reply with only: OK" or
  "hi".

Some sessions open with `/clear`, an `AGENTS.md instructions` block or another
non-statement. Those are real sessions with no stated intent. Count them, and do not
invent a purpose for them.

## Quiet days

A day is quiet when the window has no commits in git-log (the `reports` repository's own
archive commits aside), tk-journal or any loom-sessions `commits`, no sessions apart from
machinery, and loom-sessions was available. On a quiet day:

- Write `./out/outputs.json` as exactly `{"silent":true}`.
- Still write `./out/report.md`, because it is archived. Use an H1 that names the day as
  quiet, then two to four lines that state what the inputs showed: zero sessions and zero
  commits, any ticket-only activity, and any data gap. Add an open thread from the prior
  report if there is one.

Do not stretch a quiet day into an email, and do not hold back a day that had real work.
A day with real sessions but no commits is not quiet. Write a short email that says what
the sessions were for. Match the report's length to the day.

## The report

Use markdown in this order. Leave out any section that would be empty.

1. **H1:** a plain headline for the day. It becomes the email's headline. Say what
   happened in a few words; a date on its own is not a headline.
2. **Lede:** two or three sentences on the shape of the day. Which repositories saw work,
   how much landed, and the one thing most worth knowing.
3. **`## Needs attention`:** stuck or unusual items, each with the evidence that supports
   it (see below). Put the most consequential first, and keep at most five. If more
   qualify, mention the rest in their repository's section. Include Test by hand items
   from the day's handoff notes as their own list, which does not count toward the five,
   after the flagged items: one short line each, the most consequential first.
4. **`## By repository`:** one `###` per repository with real work, using the short name
   (the last path segment, so `github.com/enderrealm/loom` and `/users/steve/code/loom`
   are both `loom`). Busiest first. For each one:
   - What landed: each commit as hash and subject, followed by what it changed in a
     clause drawn from the commit message or handoff note.
   - Tickets that are now done, and tickets worked on but not done.
   - What the remaining sessions were for, summarized from their intents rather than
     listed one by one.

   Do not just list commits. Say what the work amounts to.
5. **`## Carried over`:** only if a usable prior report exists. Say which threads it
   raised were resolved or are still open according to today's inputs.
6. **Closing line:** machinery and data gaps in one sentence, in the form "Not counted
   above: <n> loom extraction sessions, <n> review-lens sessions, <n> harness runs, <n>
   probes; <data gap>." Leave out zero counts, and leave out the gap clause when there is none.

Keep it under about 800 words on a busy day and far shorter on a light one. No preamble,
no sign-off, no restating these instructions, and no HTML. Write about the inputs, not
about yourself: no "I", no "my count", and no account of what you chose to leave out.

## Stuck and unusual: evidence or nothing

Flag something only when an input supports it, and name that input in the same bullet,
for example "(workreport: 3 runs, all incomplete)" or "(handoff note, Heads up)". These
are good signals:

- `skipped_fanout`: a commit landed with no review.
- The same ticket has more than one `/work` run in the day, or a run with
  `review_iterations` of 4 or more.
- A run that did not commit, on a ticket that no input shows as done. If no input gives
  the ticket's status, say the status is unknown.
- `contamination_reports` above 0, or `criteria_unverified` above 0 on a committed run.
- Heads up items in a handoff note: flaky or failing tests, unaddressed review
  suggestions, criteria that rest on weak evidence, defects the run noticed but did not
  file.
- Something the prior report flagged that today's inputs show still open.

Do not flag raw error or token counts. Tool errors are routine, and you have no baseline
for the day. Do not flag something because it looks big. If nothing is supported, leave
out the section. A report that always finds a problem teaches the reader to skim.

## How to work

You have a small number of turns. Put independent tool calls in the same turn.

1. Read all six input files in a single turn. Busy days run to hundreds of kilobytes.
   Read refuses a file over about 25,000 tokens. When that happens, read that file in
   line ranges with `offset` and `limit`, all ranges in one turn: about 100 lines per
   range for `tk-tickets.json`, whose note lines are long, and about 600 lines for the
   other files. If a range is still refused, halve it.
2. Decide whether the day is quiet, then draft the report in your head.
3. Check every commit hash, ticket id and session id you plan to cite. Run Grep for each
   one under `./inputs/`, with all the Greps in one turn. Drop or fix any that do not
   match. Do this step on quiet days too, if the quiet report cites anything.
4. Write `./out/report.md` and `./out/outputs.json` in one turn. The subject is plain
   text, at most 100 characters, on one line, and has the form
   `Daily <label>: <the headline, shortened if needed>`.
