package ledger

// schemaVersion is the on-disk schema this binary reads and writes. Bumping it
// means writing a migration: unlike loom's summary DB, runs.db is the only
// record that a run happened and what it cost, so it is never rebuilt.
//
// schemaVersion 1: the §6 tables — runs, agents and incidents.
// schemaVersion 2: the weft-remote/v1 durable host records.
// schemaVersion 3: weft-service/v2's host records (weft-service/v2 §6.9, §14.1).
// schemaVersion 4: run visibility — output high-water marks, liveness, worker
// contracts and allowance (weft-service/v2 §5.3, §8).
// schemaVersion 5: landing — each project's repository binding and whether a
// segment was launched to land (weft-service/v2 §6.2, §7).
// schemaVersion 6: terminal sessions — what a session resource carries beyond
// §6.9's columns, and the process identity the restart pass checks (§9).
// schemaVersion 7: the catalog's host digest — what a ticket's eligibility
// reads from the host, so catalog_revision moves when it does (§5.2).
// schemaVersion 8: the agent user each hosted pipeline segment runs as.
// schemaVersion 9: the host cleanup hold of each run (weft-service/v2 §6.8).
// schemaVersion 10: whether each queue entry was dismissed (weft-service/v2 §6.3).
const schemaVersion = 10

// schemaSQL is the schema-2 DDL: §6 plus weft-remote/v1. IF NOT EXISTS
// throughout because migrate applies it to a fresh database and to a schema-2
// one alike, ahead of weftV2SQL; a schema-3 database never runs it again,
// since weftV2SQL drops weft_repository_locks.
const schemaSQL = `
CREATE TABLE IF NOT EXISTS schema_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);

CREATE TABLE IF NOT EXISTS runs (
  id              TEXT PRIMARY KEY,      -- <agent>-<YYYYMMDD-HHMMSS>-<4 hex>
  agent           TEXT NOT NULL,
  trigger_kind    TEXT NOT NULL,         -- cron | manual
  scheduled_for   TEXT NOT NULL,         -- the slot, RFC3339 with offset
  claimed_at      TEXT NOT NULL,
  started_at      TEXT,
  ended_at        TEXT,
  status          TEXT NOT NULL,         -- claimed | running | completed | failed | unknown | skipped
  skip_reason     TEXT,                  -- missed | overlap | paused | limit | refused
  failure_class   TEXT,                  -- §7.6
  error_signature TEXT,                  -- normalised first line, for incidents
  pid             INTEGER,
  run_dir         TEXT,
  session_id      TEXT,
  num_turns       INTEGER,
  cost_usd        REAL,
  input_tokens    INTEGER,
  output_tokens   INTEGER,
  silent          INTEGER NOT NULL DEFAULT 0,
  outputs_applied TEXT,                  -- JSON: what §9 applied, with paths and ids
  delivery_status TEXT,                  -- pending | delivered | failed | skipped
  delivery_error  TEXT,
  delivered_at    TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS runs_agent_slot ON runs(agent, scheduled_for, trigger_kind);
CREATE INDEX IF NOT EXISTS runs_agent_started ON runs(agent, started_at);

CREATE TABLE IF NOT EXISTS agents (
  name             TEXT PRIMARY KEY,
  last_fired_slot  TEXT,
  last_success_at  TEXT,
  failure_streak   INTEGER NOT NULL DEFAULT 0,
  paused           INTEGER NOT NULL DEFAULT 0,
  paused_reason    TEXT,
  paused_at        TEXT
);

CREATE TABLE IF NOT EXISTS incidents (
  id              INTEGER PRIMARY KEY,
  agent           TEXT NOT NULL,
  signature       TEXT NOT NULL,
  first_seen      TEXT NOT NULL,
  last_seen       TEXT NOT NULL,
  count           INTEGER NOT NULL,
  last_run_id     TEXT,
  acknowledged_at TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS incidents_open ON incidents(agent, signature) WHERE acknowledged_at IS NULL;

CREATE TABLE IF NOT EXISTS weft_runs (
  id              TEXT PRIMARY KEY,
  ticket          TEXT NOT NULL,
  project         TEXT NOT NULL,
  repo            TEXT NOT NULL,
  branch          TEXT NOT NULL,
  worktree        TEXT NOT NULL,
  base            TEXT,
  criteria        TEXT,
  phase           TEXT NOT NULL,
  outcome         TEXT,
  halt_stage      TEXT,
  halt_reason     TEXT,
  halt_detail     TEXT,
  head            TEXT,
  stopped         INTEGER NOT NULL DEFAULT 0,
  summary         TEXT,
  created_at      TEXT NOT NULL,
  updated_at      TEXT NOT NULL,
  ended_at        TEXT
);
CREATE INDEX IF NOT EXISTS weft_runs_ticket ON weft_runs(ticket, created_at);

CREATE TABLE IF NOT EXISTS weft_segments (
  run_id          TEXT NOT NULL REFERENCES weft_runs(id),
  segment         INTEGER NOT NULL,
  kind            TEXT NOT NULL,
  ledger_run_id   TEXT NOT NULL REFERENCES runs(id),
  worker_run_id   TEXT,
  answer_id       TEXT,
  terminal        TEXT,
  failure         TEXT,
  PRIMARY KEY (run_id, segment)
);

CREATE TABLE IF NOT EXISTS weft_stages (
  run_id          TEXT NOT NULL,
  segment         INTEGER NOT NULL,
  stage           TEXT NOT NULL,
  occurrence      INTEGER NOT NULL,
  attempt         INTEGER NOT NULL,
  backend         TEXT,
  state           TEXT NOT NULL,
  session         TEXT,
  detail          TEXT,
  exit_code       INTEGER,
  turns           INTEGER,
  spend           TEXT,
  processing_ms   INTEGER,
  tokens          TEXT,
  at              TEXT NOT NULL,
  PRIMARY KEY (run_id, segment, stage, occurrence, attempt)
);

CREATE TABLE IF NOT EXISTS weft_questions (
  id              TEXT PRIMARY KEY,
  run_id          TEXT NOT NULL REFERENCES weft_runs(id),
  segment         INTEGER NOT NULL,
  stage           TEXT NOT NULL,
  occurrence      INTEGER NOT NULL,
  attempt         INTEGER NOT NULL,
  text            TEXT NOT NULL,
  asked_at        TEXT NOT NULL,
  state           TEXT NOT NULL,
  answer_id       TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS weft_questions_pending ON weft_questions(run_id) WHERE state = 'pending';

CREATE TABLE IF NOT EXISTS weft_answers (
  id              TEXT PRIMARY KEY,
  question_id     TEXT NOT NULL UNIQUE REFERENCES weft_questions(id),
  run_id          TEXT NOT NULL,
  text            TEXT NOT NULL,
  submitted_at    TEXT NOT NULL,
  segment         INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS weft_events (
  seq             INTEGER PRIMARY KEY AUTOINCREMENT,
  ts              TEXT NOT NULL,
  run_id          TEXT NOT NULL,
  type            TEXT NOT NULL,
  payload         TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS weft_requests (
  token_id        TEXT NOT NULL,
  key             TEXT NOT NULL,
  method          TEXT NOT NULL,
  path            TEXT NOT NULL,
  body_sha256     TEXT NOT NULL,
  status          INTEGER NOT NULL,
  response        TEXT NOT NULL,
  created_at      TEXT NOT NULL,
  PRIMARY KEY (token_id, key)
);

CREATE TABLE IF NOT EXISTS weft_repository_locks (
  repository      TEXT PRIMARY KEY,
  held_by_run     TEXT NOT NULL REFERENCES weft_runs(id),
  acquired_at     TEXT NOT NULL
);
`

// weftV2SQL is the forward half of the §14.1 migration that is plain DDL: the
// tables weft-service/v2 §6.9 adds, the columns it adds to the v1 tables, and
// weft_catalog, the host's record of each project's catalog_revision and the
// digest of the tickets it was last computed over (§5.2). The data half —
// moving the repository locks, labelling the v1 runs, minting host_id — is
// migrateWeftV2, in the same transaction.
const weftV2SQL = `
CREATE TABLE weft_host (host_id TEXT PRIMARY KEY, created_at TEXT NOT NULL);
CREATE TABLE weft_settings (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE weft_entries (
  id TEXT PRIMARY KEY, kind TEXT NOT NULL, ticket TEXT NOT NULL, project TEXT NOT NULL,
  repository_id TEXT NOT NULL, repository TEXT NOT NULL, selection_id TEXT,
  position INTEGER NOT NULL UNIQUE, state TEXT NOT NULL,
  waiting_reason TEXT, holder_kind TEXT, holder_id TEXT,
  invalidated_reason TEXT, invalidated_detail TEXT,
  run_id TEXT, supersedes TEXT, answer_id TEXT REFERENCES weft_answers(id),
  checkout TEXT, tk_store_root TEXT, work_evidence_dir TEXT,
  token_id TEXT NOT NULL, request_key TEXT NOT NULL,
  created_at TEXT NOT NULL, updated_at TEXT NOT NULL, revision INTEGER NOT NULL
);
CREATE TABLE weft_reservations (
  repository_id TEXT PRIMARY KEY, repository TEXT NOT NULL,
  holder_kind TEXT NOT NULL, holder_id TEXT NOT NULL, acquired_at TEXT NOT NULL
);
CREATE TABLE weft_repositories (
  repository_id TEXT PRIMARY KEY, repository TEXT NOT NULL, dispatch TEXT NOT NULL,
  paused_run TEXT, paused_reason TEXT, paused_detail TEXT, paused_at TEXT,
  revision INTEGER NOT NULL
);
CREATE TABLE weft_land_steps (
  run_id TEXT NOT NULL, step TEXT NOT NULL, intent_at TEXT NOT NULL, acked_at TEXT,
  result TEXT, result_at TEXT, observed TEXT, detail TEXT, PRIMARY KEY (run_id, step)
);
CREATE TABLE weft_terminals (
  id TEXT PRIMARY KEY, kind TEXT NOT NULL, target_kind TEXT NOT NULL, target_id TEXT NOT NULL,
  path TEXT NOT NULL, repository_id TEXT, state TEXT NOT NULL, pid INTEGER, pgid INTEGER,
  exit_code INTEGER, closed_by TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
  revision INTEGER NOT NULL
);
CREATE TABLE weft_output (
  run_id TEXT NOT NULL, cursor INTEGER NOT NULL, segment INTEGER NOT NULL, stage TEXT NOT NULL,
  occurrence INTEGER NOT NULL, attempt INTEGER NOT NULL, chunk INTEGER NOT NULL,
  at TEXT NOT NULL, text TEXT NOT NULL, PRIMARY KEY (run_id, cursor)
);
CREATE TABLE weft_archives (
  id TEXT PRIMARY KEY, sha256 TEXT NOT NULL, origin TEXT NOT NULL, imported_at TEXT NOT NULL
);
CREATE TABLE weft_catalog (
  project TEXT PRIMARY KEY, revision INTEGER NOT NULL, digest TEXT NOT NULL
);

ALTER TABLE weft_runs ADD COLUMN repository_id TEXT;
ALTER TABLE weft_runs ADD COLUMN entry_id TEXT;
ALTER TABLE weft_runs ADD COLUMN origin TEXT;
ALTER TABLE weft_runs ADD COLUMN recorded_under TEXT;
ALTER TABLE weft_runs ADD COLUMN archive_id TEXT;
ALTER TABLE weft_runs ADD COLUMN settlement_state TEXT;
ALTER TABLE weft_runs ADD COLUMN settlement_step TEXT;
ALTER TABLE weft_runs ADD COLUMN settlement_reason TEXT;
ALTER TABLE weft_runs ADD COLUMN settlement_detail TEXT;
ALTER TABLE weft_runs ADD COLUMN settlement_branch TEXT;
ALTER TABLE weft_runs ADD COLUMN settlement_commit TEXT;
ALTER TABLE weft_runs ADD COLUMN action_required TEXT;
ALTER TABLE weft_runs ADD COLUMN action_detail TEXT;
ALTER TABLE weft_runs ADD COLUMN disposition TEXT;
ALTER TABLE weft_runs ADD COLUMN archive_ref TEXT;
ALTER TABLE weft_runs ADD COLUMN supersedes TEXT;
ALTER TABLE weft_runs ADD COLUMN superseded_by TEXT;
ALTER TABLE weft_runs ADD COLUMN revision INTEGER NOT NULL DEFAULT 1;
ALTER TABLE weft_answers ADD COLUMN entry_id TEXT;
ALTER TABLE weft_answers ADD COLUMN consumed_segment INTEGER;
`

// weftVisibilitySQL is schema 4, run visibility (§5.3, §8): weft_allowance,
// the newest observation per provider; weft_output_attempts, the last chunk
// and cursor each attempt wrote, kept after retention drops the text so a
// read can say output is missing rather than never written;
// weft_runs.output_cursor, the last output cursor the run allocated, so a
// cursor is never reissued after retention empties weft_output;
// weft_runs.liveness, the latest liveness report as its JSON resource; and
// weft_segments.worker_contract, the contract the segment was launched under
// (NULL for one launched before it was recorded, which was weft-remote/v1).
const weftVisibilitySQL = `
CREATE INDEX weft_output_attempt ON weft_output(run_id, segment, stage, occurrence, attempt);
CREATE TABLE weft_output_attempts (
  run_id TEXT NOT NULL, segment INTEGER NOT NULL, stage TEXT NOT NULL, occurrence INTEGER NOT NULL,
  attempt INTEGER NOT NULL, last_chunk INTEGER NOT NULL, last_cursor INTEGER NOT NULL,
  PRIMARY KEY (run_id, segment, stage, occurrence, attempt)
);
CREATE TABLE weft_allowance (
  provider TEXT PRIMARY KEY, source TEXT NOT NULL, observed_at TEXT NOT NULL, windows TEXT NOT NULL
);
ALTER TABLE weft_runs ADD COLUMN output_cursor INTEGER NOT NULL DEFAULT 0;
ALTER TABLE weft_runs ADD COLUMN liveness TEXT;
ALTER TABLE weft_segments ADD COLUMN worker_contract TEXT;
`

// weftLandingSQL is schema 5, landing (§6.2, §7): weft_catalog keeps the
// repository each project was last bound to, so a repository resource names
// its projects in the transaction that changes it; weft_segments.land is set
// when a segment that was launched with --land says hello, which is what
// makes it a landing segment (§7.4); weft_segments.graded that the segment
// sent graded, so a failed grade is never resumable (§4); weft_segments.observed,
// observed_outcome and the observed state of each step are a reconcile's
// reading, and weft_land_steps' result_segment and observed_segment the
// segment that reported each result and observation — a step is
// acknowledged only on its own segment's evidence; weft_entries.archive_head
// the head a fresh entry archives its run at (§6.7). A retained run has none of them: its segments were never
// launched to land.
const weftLandingSQL = `
ALTER TABLE weft_catalog ADD COLUMN repository_id TEXT;
ALTER TABLE weft_catalog ADD COLUMN repository TEXT;
ALTER TABLE weft_segments ADD COLUMN land INTEGER NOT NULL DEFAULT 0;
ALTER TABLE weft_segments ADD COLUMN graded INTEGER NOT NULL DEFAULT 0;
ALTER TABLE weft_segments ADD COLUMN observed INTEGER NOT NULL DEFAULT 0;
ALTER TABLE weft_segments ADD COLUMN observed_outcome TEXT;
ALTER TABLE weft_segments ADD COLUMN observed_fast_forward TEXT;
ALTER TABLE weft_segments ADD COLUMN observed_push TEXT;
ALTER TABLE weft_segments ADD COLUMN observed_mark_done TEXT;
ALTER TABLE weft_land_steps ADD COLUMN result_segment INTEGER;
ALTER TABLE weft_land_steps ADD COLUMN observed_segment INTEGER;
ALTER TABLE weft_entries ADD COLUMN archive_head TEXT;
`

// weftTerminalSQL is schema 6, terminal sessions (§9): the project, size,
// title, owner, attachment count and output offsets a session resource
// carries, kept so a closed or interrupted session reads as it last was;
// process_start, the kernel's start time of the session's shell, which is
// what establishes a recorded pid as that shell's rather than the same pid
// reused; and group_unresolved, why the restart pass could not establish an
// interrupted session's process group gone — while it is set the session
// keeps its share of the repository's terminal reservation.
const weftTerminalSQL = `
ALTER TABLE weft_terminals ADD COLUMN project TEXT;
ALTER TABLE weft_terminals ADD COLUMN width INTEGER NOT NULL DEFAULT 0;
ALTER TABLE weft_terminals ADD COLUMN height INTEGER NOT NULL DEFAULT 0;
ALTER TABLE weft_terminals ADD COLUMN title TEXT NOT NULL DEFAULT '';
ALTER TABLE weft_terminals ADD COLUMN owner TEXT;
ALTER TABLE weft_terminals ADD COLUMN attachments INTEGER NOT NULL DEFAULT 0;
ALTER TABLE weft_terminals ADD COLUMN output_offset INTEGER NOT NULL DEFAULT 0;
ALTER TABLE weft_terminals ADD COLUMN retained_from INTEGER NOT NULL DEFAULT 0;
ALTER TABLE weft_terminals ADD COLUMN process_start TEXT;
ALTER TABLE weft_terminals ADD COLUMN group_unresolved TEXT;
`

// weftCatalogHostSQL is schema 7: weft_catalog.host_digest is the digest of
// the host-held inputs to each project's ticket eligibility — the runs in
// flight, the retained hosted runs and the pending entries — as the
// project's catalog_revision was last raised over (§5.2). The migration
// digests every recorded project in its own transaction; NULL is only ever a
// row SyncWeftCatalog has just inserted, digested before that commits.
const weftCatalogHostSQL = `
ALTER TABLE weft_catalog ADD COLUMN host_digest TEXT;
`

// weftAgentUserSQL is schema 8: weft_segments.agent_user, the agent user a
// pipeline segment was claimed with (docs/agent-users.md). It is held while
// the segment's phase-1 row is claimed or running, and only one such segment
// holds a user at a time. NULL is a maintenance segment, which runs as the
// operator, or a segment claimed before hosted segments ran as agent users.
const weftAgentUserSQL = `
ALTER TABLE weft_segments ADD COLUMN agent_user TEXT;
`

// weftHostCleanupSQL is schema 9: weft_runs.host_cleanup_hold, set when a
// cleanup entry of the run is refused or cancelled, so the host never queues
// a cleanup of that run itself again (§6.8). It is host-internal: no payload
// carries it, and nothing lifts it.
const weftHostCleanupSQL = `
ALTER TABLE weft_runs ADD COLUMN host_cleanup_hold INTEGER NOT NULL DEFAULT 0;
`

// weftQueueDismissSQL is schema 10: weft_entries.dismissed, set when a human
// dismisses a settled entry (§6.3). GET /v2/queue leaves a dismissed entry
// out; a read by id, and the idempotency replay of whatever returned it,
// still find it.
const weftQueueDismissSQL = `
ALTER TABLE weft_entries ADD COLUMN dismissed INTEGER NOT NULL DEFAULT 0;
`
