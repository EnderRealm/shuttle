package ledger

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"
)

// Run statuses, §6.1. claimed → running → completed | failed, plus unknown
// (the daemon restarted over it) and skipped (it was never launched).
const (
	StatusClaimed   = "claimed"
	StatusRunning   = "running"
	StatusCompleted = "completed"
	StatusFailed    = "failed"
	StatusUnknown   = "unknown"
	StatusSkipped   = "skipped"
)

// Trigger kinds, §5.
const (
	TriggerCron   = "cron"
	TriggerManual = "manual"
)

// Skip reasons, §5.
const (
	SkipMissed  = "missed"
	SkipOverlap = "overlap"
	SkipPaused  = "paused"
	SkipLimit   = "limit"
)

// Delivery statuses. A separate column from the run status on purpose (§6.1):
// a run whose report was produced but whose email failed is completed with a
// failed delivery, and retrying delivery must not re-run the model.
const (
	DeliveryPending   = "pending"
	DeliveryDelivered = "delivered"
	DeliveryFailed    = "failed"
	DeliverySkipped   = "skipped"
)

// Failure classes, §7.6. Nothing is ever classified as "flake".
const (
	ClassPreflight = "preflight"
	ClassAuth      = "auth"
	ClassProvider  = "provider"
	ClassDied      = "died"
	ClassNoResult  = "no_result"
	ClassStuck     = "stuck"
	ClassBudget    = "budget"
	ClassOutput    = "output"
	ClassSandbox   = "sandbox"
)

// runColumns is the select list every read shares, in scanRun's order.
const runColumns = `id, agent, trigger_kind, scheduled_for, claimed_at, started_at, ended_at,
	status, skip_reason, failure_class, error_signature, pid, run_dir, session_id,
	num_turns, cost_usd, input_tokens, output_tokens, silent, outputs_applied,
	delivery_status, delivery_error, delivered_at`

// Run is one row of the runs table. The optional columns are nullable rather
// than zero-valued: §12 turns on "reported no cost" being distinguishable from
// a cost of zero.
type Run struct {
	ID             string
	Agent          string
	TriggerKind    string
	ScheduledFor   string
	ClaimedAt      string
	StartedAt      sql.NullString
	EndedAt        sql.NullString
	Status         string
	SkipReason     sql.NullString
	FailureClass   sql.NullString
	ErrorSignature sql.NullString
	PID            sql.NullInt64
	RunDir         sql.NullString
	SessionID      sql.NullString
	NumTurns       sql.NullInt64
	CostUSD        sql.NullFloat64
	InputTokens    sql.NullInt64
	OutputTokens   sql.NullInt64
	Silent         bool
	OutputsApplied sql.NullString
	DeliveryStatus sql.NullString
	DeliveryError  sql.NullString
	DeliveredAt    sql.NullString
}

// Result is what the runtime reported, recorded on a terminal run row. A
// failed run may still carry cost: the budget stop and error_max_turns both
// report total_cost_usd (§7.4).
type Result struct {
	SessionID    string
	NumTurns     sql.NullInt64
	CostUSD      sql.NullFloat64
	InputTokens  sql.NullInt64
	OutputTokens sql.NullInt64
	Silent       bool
}

// Claim inserts the claimed row for a slot and, for a cron trigger, advances
// the agent's last_fired_slot. A second claim of the same (agent,
// scheduled_for, trigger_kind) returns claimed=false and writes nothing — the
// unique index is the arbiter, deliberately not a SELECT-then-INSERT that two
// ticks could race.
func (s *Store) Claim(agent string, slot time.Time, triggerKind string) (*Run, bool, error) {
	return s.insertSlot(agent, slot, triggerKind, StatusClaimed, "")
}

// Skip records a slot that was never launched (§5): missed, overlapped, paused
// or over a limit. The row is terminal on insert, and it is subject to the same
// unique index as Claim, so a repeat is the same no-op.
func (s *Store) Skip(agent string, slot time.Time, triggerKind, reason string) (*Run, bool, error) {
	return s.insertSlot(agent, slot, triggerKind, StatusSkipped, reason)
}

func (s *Store) insertSlot(agent string, slot time.Time, triggerKind, status, skipReason string) (*Run, bool, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return nil, false, err
	}
	defer tx.Rollback()
	run, ok, err := s.insertSlotTx(tx, agent, slot, triggerKind, status, skipReason)
	if err != nil || !ok {
		return nil, ok, err
	}
	if err := tx.Commit(); err != nil {
		return nil, false, err
	}
	return run, true, nil
}

// insertSlotTx writes one slot row inside the caller's transaction, so a gate
// and its claim can share one (ClaimGated).
func (s *Store) insertSlotTx(tx *sql.Tx, agent string, slot time.Time, triggerKind, status, skipReason string) (*Run, bool, error) {
	id, err := NewRunID(agent, slot)
	if err != nil {
		return nil, false, err
	}
	run := &Run{
		ID:           id,
		Agent:        agent,
		TriggerKind:  triggerKind,
		ScheduledFor: slot.Format(time.RFC3339),
		ClaimedAt:    s.timestamp(),
		Status:       status,
		SkipReason:   nullString(skipReason),
	}
	_, err = tx.Exec(
		`INSERT INTO runs(id, agent, trigger_kind, scheduled_for, claimed_at, status, skip_reason)
			VALUES (?, ?, ?, ?, ?, ?, ?)`,
		run.ID, run.Agent, run.TriggerKind, run.ScheduledFor, run.ClaimedAt, run.Status, run.SkipReason,
	)
	if isSlotTaken(err) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("claim %s slot %s: %w", agent, run.ScheduledFor, err)
	}
	// §5 step 7: last_fired_slot advances only on a successful claim, and only
	// on a cron one. It is the scheduler's catch-up cursor, so a skipped slot
	// leaves it where it was — and a manual run bypasses the schedule (§5), so
	// it must not move the schedule's memory either way.
	if status == StatusClaimed && triggerKind == TriggerCron {
		if _, err := tx.Exec(
			`INSERT INTO agents(name, last_fired_slot) VALUES (?, ?)
				ON CONFLICT(name) DO UPDATE SET last_fired_slot = excluded.last_fired_slot`,
			agent, run.ScheduledFor,
		); err != nil {
			return nil, false, err
		}
	}
	return run, true, nil
}

// Limits is what the §5 gate measures a slot against: the §4.2 daily caps and
// the day boundary they are counted from. A non-positive max disables that
// check.
type Limits struct {
	MaxRunsPerDay int
	MaxSpendUSD   float64
	DayStart      time.Time
}

// ClaimGated applies the §5 checks and records the slot in one transaction.
// Three outcomes: the claimed run with an empty reason; a nil run and the skip
// reason the gate refused on, whose skipped row has been written; or a nil run
// and an empty reason, meaning the slot was already in the ledger and nothing
// was written.
//
// The gate is inside the transaction because a check made outside it is a
// check-then-act race: §12 documents `shuttle daemon` and `shuttle run` as
// concurrent operator actions, SetMaxOpenConns(1) serialises only within one
// process, and the unique index refuses a repeat of the same slot but not two
// processes claiming two different slots for one agent.
func (s *Store) ClaimGated(agent string, slot time.Time, triggerKind string, limits Limits) (*Run, string, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return nil, "", err
	}
	defer tx.Rollback()
	reason, err := gate(tx, agent, limits)
	if err != nil {
		return nil, "", err
	}
	status := StatusClaimed
	if reason != "" {
		status = StatusSkipped
	}
	run, ok, err := s.insertSlotTx(tx, agent, slot, triggerKind, status, reason)
	if err != nil {
		return nil, "", err
	}
	if !ok {
		return nil, "", nil
	}
	if err := tx.Commit(); err != nil {
		return nil, "", err
	}
	if reason != "" {
		return nil, reason, nil
	}
	return run, "", nil
}

// gate returns the skip reason a slot fails on, or "" when it may fire. The
// order is §5's: the overlap check, then the pause, then the daily limits.
func gate(tx *sql.Tx, agent string, limits Limits) (string, error) {
	// One run per agent at a time, no queue (§5 step 4).
	var inFlight int
	if err := tx.QueryRow(
		`SELECT count(*) FROM runs WHERE agent = ? AND status IN (?, ?)`,
		agent, StatusClaimed, StatusRunning,
	).Scan(&inFlight); err != nil {
		return "", err
	}
	if inFlight > 0 {
		return SkipOverlap, nil
	}
	var paused bool
	err := tx.QueryRow(`SELECT paused FROM agents WHERE name = ?`, agent).Scan(&paused)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return "", err
	}
	if paused {
		return SkipPaused, nil
	}
	if limits.MaxRunsPerDay > 0 {
		// Skipped rows are excluded: a slot that was never launched cost
		// nothing, so counting it would let one sleep-through spend the day's
		// cap on rows.
		var n int
		if err := tx.QueryRow(
			`SELECT count(*) FROM runs WHERE agent = ? AND claimed_at >= ? AND status != ?`,
			agent, sinceText(limits.DayStart), StatusSkipped,
		).Scan(&n); err != nil {
			return "", err
		}
		if n >= limits.MaxRunsPerDay {
			return SkipLimit, nil
		}
	}
	if limits.MaxSpendUSD > 0 {
		// The spend cap is estate-wide (§4.2), so this sums every agent.
		var spent sql.NullFloat64
		if err := tx.QueryRow(
			`SELECT sum(cost_usd) FROM runs WHERE claimed_at >= ?`, sinceText(limits.DayStart),
		).Scan(&spent); err != nil {
			return "", err
		}
		if spent.Float64 >= limits.MaxSpendUSD {
			return SkipLimit, nil
		}
	}
	return "", nil
}

// NewRunID builds the §7.2 id: <agent>-<YYYYMMDD-HHMMSS>-<4 hex>. The
// timestamp is the slot rendered in the slot's own location, so the id reads
// as the wall-clock time the operator scheduled. Exported so a dry run, which
// claims no row, names its directory with the same id shape.
func NewRunID(agent string, slot time.Time) (string, error) {
	var b [2]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return fmt.Sprintf("%s-%s-%s", agent, slot.Format("20060102-150405"), hex.EncodeToString(b[:])), nil
}

// MarkRunning records the launched process against a claimed run.
func (s *Store) MarkRunning(id string, pid int, runDir string) error {
	return s.withTx(func(tx *sql.Tx) error {
		if _, err := requireStatus(tx, id, StatusRunning, StatusClaimed); err != nil {
			return err
		}
		_, err := tx.Exec(
			`UPDATE runs SET status = ?, started_at = ?, pid = ?, run_dir = ? WHERE id = ?`,
			StatusRunning, s.timestamp(), pid, runDir, id,
		)
		return err
	})
}

// SetRunDir attaches the retained artifact directory while a row is still
// claimed. Preflight and provider failures happen before a model process
// exists, so MarkRunning cannot be the first place this path is recorded.
func (s *Store) SetRunDir(id, runDir string) error {
	return s.updateOne(
		`UPDATE runs SET run_dir = ? WHERE id = ? AND status = ?`,
		id, runDir, id, StatusClaimed,
	)
}

// Complete closes a run as successful: it resets the agent's failure streak
// and stamps last_success_at (§6.2).
func (s *Store) Complete(id string, res Result) error {
	_, err := s.CompleteEvent(id, res)
	return err
}

// Fail closes a run as failed: it advances the streak, applies the §6.3
// auto-pause rules and upserts the incident for the normalised error.
func (s *Store) Fail(id, class, errText string, res Result) error {
	_, err := s.FailEvent(id, class, errText, res)
	return err
}

func finishRun(tx *sql.Tx, id, status, now string, res Result, class, signature string) error {
	_, err := tx.Exec(
		`UPDATE runs SET status = ?, ended_at = ?, failure_class = ?, error_signature = ?,
			session_id = ?, num_turns = ?, cost_usd = ?, input_tokens = ?, output_tokens = ?, silent = ?
			WHERE id = ?`,
		status, now, nullString(class), nullString(signature),
		nullString(res.SessionID), res.NumTurns, res.CostUSD, res.InputTokens, res.OutputTokens, res.Silent,
		id,
	)
	return err
}

// InFlight lists every run still claimed or running: the rows a daemon start
// finds left behind by its predecessor (§5). The order is deterministic so the
// restart pass reports them in the order they were claimed.
func (s *Store) InFlight() ([]*Run, error) {
	return s.queryRuns(
		`SELECT `+runColumns+` FROM runs WHERE status IN (?, ?) ORDER BY claimed_at, id`,
		StatusClaimed, StatusRunning,
	)
}

// MarkUnknown closes an in-flight run the restart pass could not account for
// (§5). It never auto-retries: a run that may have spent model quota is not
// re-fired automatically, only by an operator's `shuttle run`.
func (s *Store) MarkUnknown(id string) error {
	return s.withTx(func(tx *sql.Tx) error {
		if _, err := requireStatus(tx, id, StatusUnknown, StatusClaimed, StatusRunning); err != nil {
			return err
		}
		_, err := tx.Exec(
			`UPDATE runs SET status = ?, ended_at = ? WHERE id = ?`,
			StatusUnknown, s.timestamp(), id,
		)
		return err
	})
}

// SetDelivery records the delivery outcome independently of the run status
// (§6.1). delivered_at is stamped only when the message actually left.
func (s *Store) SetDelivery(id, status, deliveryErr string) error {
	var deliveredAt sql.NullString
	if status == DeliveryDelivered {
		deliveredAt = nullString(s.timestamp())
	}
	return s.updateOne(
		`UPDATE runs SET delivery_status = ?, delivery_error = ?, delivered_at = ? WHERE id = ?`,
		id, status, nullString(deliveryErr), deliveredAt, id,
	)
}

// SetOutputsApplied records what §9 applied, with paths and ids.
func (s *Store) SetOutputsApplied(id string, applied json.RawMessage) error {
	return s.updateOne(
		`UPDATE runs SET outputs_applied = ? WHERE id = ?`,
		id, string(applied), id,
	)
}

// Get reads one run by id.
func (s *Store) Get(id string) (*Run, error) {
	row := s.db.QueryRow(`SELECT `+runColumns+` FROM runs WHERE id = ?`, id)
	run, err := scanRun(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("run %s: not in the ledger", id)
	}
	return run, err
}

// Runs lists ledger rows most recent first. An empty agent lists every agent;
// a limit of zero or less is unbounded.
func (s *Store) Runs(agent string, limit int) ([]*Run, error) {
	query := `SELECT ` + runColumns + ` FROM runs`
	var args []any
	if agent != "" {
		query += ` WHERE agent = ?`
		args = append(args, agent)
	}
	query += ` ORDER BY claimed_at DESC, id DESC`
	if limit > 0 {
		query += ` LIMIT ?`
		args = append(args, limit)
	}
	return s.queryRuns(query, args...)
}

// queryRuns runs a runColumns select and scans every row it returns.
func (s *Store) queryRuns(query string, args ...any) ([]*Run, error) {
	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var runs []*Run
	for rows.Next() {
		run, err := scanRun(rows)
		if err != nil {
			return nil, err
		}
		runs = append(runs, run)
	}
	return runs, rows.Err()
}

// scanner is satisfied by both *sql.Row and *sql.Rows.
type scanner interface {
	Scan(dest ...any) error
}

func scanRun(sc scanner) (*Run, error) {
	var r Run
	err := sc.Scan(
		&r.ID, &r.Agent, &r.TriggerKind, &r.ScheduledFor, &r.ClaimedAt, &r.StartedAt, &r.EndedAt,
		&r.Status, &r.SkipReason, &r.FailureClass, &r.ErrorSignature, &r.PID, &r.RunDir, &r.SessionID,
		&r.NumTurns, &r.CostUSD, &r.InputTokens, &r.OutputTokens, &r.Silent, &r.OutputsApplied,
		&r.DeliveryStatus, &r.DeliveryError, &r.DeliveredAt,
	)
	if err != nil {
		return nil, err
	}
	return &r, nil
}

// requireStatus enforces the §6.1 state machine inside the transaction that is
// about to write: only the listed statuses may move to next, so a terminal row
// never moves again. It returns the run's agent for the streak and incident
// updates that follow.
func requireStatus(tx *sql.Tx, id, next string, from ...string) (string, error) {
	var agent, status string
	err := tx.QueryRow(`SELECT agent, status FROM runs WHERE id = ?`, id).Scan(&agent, &status)
	if errors.Is(err, sql.ErrNoRows) {
		return "", fmt.Errorf("run %s: not in the ledger", id)
	}
	if err != nil {
		return "", err
	}
	if !slices.Contains(from, status) {
		return "", fmt.Errorf("run %s: cannot move from %s to %s", id, status, next)
	}
	return agent, nil
}

func (s *Store) withTx(fn func(*sql.Tx) error) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}

// updateOne runs a single-row update and reports a missing id rather than
// succeeding silently over nothing.
func (s *Store) updateOne(query, id string, args ...any) error {
	res, err := s.db.Exec(query, args...)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return fmt.Errorf("run %s: not in the ledger", id)
	}
	return nil
}

// LastRecordedSlot is the scheduler's catch-up cursor: the scheduled_for of
// the newest cron row for the agent, or "" when it has none. Manual rows are
// excluded because a manual run bypasses the schedule (§5).
//
// Both the catch-up loop and the claim write a tick's rows in ascending slot
// order, so the newest row written is the newest slot. That is why this orders
// by claimed_at rather than by MAX(scheduled_for): scheduled_for is RFC3339
// with the slot's own offset (§5), so a lexicographic max is wrong across a
// DST offset change.
func (s *Store) LastRecordedSlot(agent string) (string, error) {
	var slot string
	err := s.db.QueryRow(
		`SELECT scheduled_for FROM runs WHERE agent = ? AND trigger_kind = ?
			ORDER BY claimed_at DESC, id DESC LIMIT 1`,
		agent, TriggerCron,
	).Scan(&slot)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return slot, err
}

// SpendSince sums cost_usd across every agent from since, for `shuttle
// status`. The sum is nullable on purpose (§12): sum ignores NULL and a window
// whose rows all reported no cost sums to NULL, which is unpriced rather than
// free.
func (s *Store) SpendSince(since time.Time) (sql.NullFloat64, error) {
	var total sql.NullFloat64
	err := s.db.QueryRow(
		`SELECT sum(cost_usd) FROM runs WHERE claimed_at >= ?`, sinceText(since),
	).Scan(&total)
	return total, err
}

// sinceText renders a window start in the format claimed_at is written in
// (Store.timestamp): RFC3339 in UTC, which sorts lexicographically, so the
// comparison is done in SQL rather than by scanning every row.
func sinceText(since time.Time) string {
	return since.UTC().Format(time.RFC3339)
}
