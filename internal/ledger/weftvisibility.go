package ledger

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// WeftOutputRetainBytes is weft-service/v2 §8.1's retention: the newest
// 256 KiB of text of each stage attempt. WeftOutputRetainChunks bounds the
// rows as well, since chunks with little or no text cost rows the byte count
// does not see.
const (
	WeftOutputRetainBytes  = 256 << 10
	WeftOutputRetainChunks = 2048
)

// WeftAttempt names one stage attempt of a run (§8.1).
type WeftAttempt struct {
	Segment    int    `json:"segment"`
	Stage      string `json:"stage"`
	Occurrence int    `json:"occurrence"`
	Attempt    int    `json:"attempt"`
}

// WeftOutputChunk is one chunk of an attempt's output as the host retains
// it: the worker numbers chunks within the attempt, the host assigns the
// run-wide cursor.
type WeftOutputChunk struct {
	RunID  string
	Cursor int64
	WeftAttempt
	Chunk int
	At    string
	Text  string
}

// RecordWeftOutput assigns the chunk the run's next cursor, stores it, raises
// the attempt's high-water mark, and drops the attempt's oldest chunks past
// WeftOutputRetainBytes or WeftOutputRetainChunks; the chunk just written is
// always kept. Output never enters weft_events (§6.9).
func (s *Store) RecordWeftOutput(c WeftOutputChunk) (int64, error) {
	var cursor int64
	err := s.withTx(func(tx *sql.Tx) error {
		if err := tx.QueryRow(`UPDATE weft_runs SET output_cursor=output_cursor+1 WHERE id=? RETURNING output_cursor`,
			c.RunID).Scan(&cursor); err != nil {
			return fmt.Errorf("allocate output cursor of %s: %w", c.RunID, err)
		}
		if _, err := tx.Exec(`INSERT INTO weft_output(run_id,cursor,segment,stage,occurrence,attempt,chunk,at,text)
			VALUES(?,?,?,?,?,?,?,?,?)`, c.RunID, cursor, c.Segment, c.Stage, c.Occurrence, c.Attempt,
			c.Chunk, c.At, c.Text); err != nil {
			return err
		}
		if _, err := tx.Exec(`INSERT INTO weft_output_attempts(run_id,segment,stage,occurrence,attempt,last_chunk,last_cursor)
			VALUES(?,?,?,?,?,?,?) ON CONFLICT(run_id,segment,stage,occurrence,attempt) DO UPDATE SET
			last_chunk=excluded.last_chunk,last_cursor=excluded.last_cursor`,
			c.RunID, c.Segment, c.Stage, c.Occurrence, c.Attempt, c.Chunk, cursor); err != nil {
			return err
		}
		_, err := tx.Exec(`DELETE FROM weft_output WHERE run_id=?1 AND cursor IN (
			SELECT cursor FROM (
				SELECT cursor, SUM(length(CAST(text AS BLOB))) OVER (ORDER BY cursor DESC) AS newer,
					ROW_NUMBER() OVER (ORDER BY cursor DESC) AS rank
				FROM weft_output WHERE run_id=?1 AND segment=?2 AND stage=?3 AND occurrence=?4 AND attempt=?5)
			WHERE (newer>?6 OR rank>?8) AND cursor<?7)`,
			c.RunID, c.Segment, c.Stage, c.Occurrence, c.Attempt, WeftOutputRetainBytes, cursor, WeftOutputRetainChunks)
		return err
	})
	return cursor, err
}

// WeftOutputBounds is the run's retained output window: oldest is the first
// cursor still retained, or newest+1 when none is; newest is the last cursor
// the run allocated.
func (s *Store) WeftOutputBounds(runID string) (oldest, newest int64, err error) {
	err = s.db.QueryRow(`SELECT output_cursor FROM weft_runs WHERE id=?`, runID).Scan(&newest)
	if err != nil {
		return 0, 0, err
	}
	err = s.db.QueryRow(`SELECT COALESCE(MIN(cursor),?) FROM weft_output WHERE run_id=?`, newest+1, runID).Scan(&oldest)
	return oldest, newest, err
}

// WeftOutputAfter reads at most limit of the run's retained chunks past
// cursor, in cursor order.
func (s *Store) WeftOutputAfter(runID string, cursor int64, limit int) ([]WeftOutputChunk, error) {
	return queryWeftOutput(s.db, `SELECT run_id,cursor,segment,stage,occurrence,attempt,chunk,at,text
		FROM weft_output WHERE run_id=? AND cursor>? ORDER BY cursor LIMIT ?`, runID, cursor, limit)
}

// WeftAttemptOutput reads every retained chunk of one attempt, in order.
func (s *Store) WeftAttemptOutput(runID string, a WeftAttempt) ([]WeftOutputChunk, error) {
	return queryWeftOutput(s.db, `SELECT run_id,cursor,segment,stage,occurrence,attempt,chunk,at,text
		FROM weft_output WHERE run_id=? AND segment=? AND stage=? AND occurrence=? AND attempt=? ORDER BY cursor`,
		runID, a.Segment, a.Stage, a.Occurrence, a.Attempt)
}

func queryWeftOutput(q querier, query string, args ...any) ([]WeftOutputChunk, error) {
	rows, err := q.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []WeftOutputChunk
	for rows.Next() {
		var c WeftOutputChunk
		if err := rows.Scan(&c.RunID, &c.Cursor, &c.Segment, &c.Stage, &c.Occurrence, &c.Attempt,
			&c.Chunk, &c.At, &c.Text); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// WeftOutputHighWater is the last chunk an attempt wrote and its cursor,
// whether or not retention still holds it; zero when it wrote none.
func (s *Store) WeftOutputHighWater(runID string, a WeftAttempt) (chunk int, cursor int64, err error) {
	err = s.db.QueryRow(`SELECT last_chunk,last_cursor FROM weft_output_attempts WHERE run_id=? AND segment=?
		AND stage=? AND occurrence=? AND attempt=?`, runID, a.Segment, a.Stage, a.Occurrence, a.Attempt).Scan(&chunk, &cursor)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, 0, nil
	}
	return chunk, cursor, err
}

// WeftLatestAttempt is the attempt a run's output read defaults to: the one
// that wrote the newest chunk, else the newest stage row. sql.ErrNoRows means
// the run has reported no attempt.
func (s *Store) WeftLatestAttempt(runID string) (WeftAttempt, error) {
	var a WeftAttempt
	err := s.db.QueryRow(`SELECT segment,stage,occurrence,attempt FROM weft_output_attempts WHERE run_id=?
		ORDER BY last_cursor DESC LIMIT 1`, runID).Scan(&a.Segment, &a.Stage, &a.Occurrence, &a.Attempt)
	if errors.Is(err, sql.ErrNoRows) {
		err = s.db.QueryRow(`SELECT segment,stage,occurrence,attempt FROM weft_stages WHERE run_id=?
			ORDER BY rowid DESC LIMIT 1`, runID).Scan(&a.Segment, &a.Stage, &a.Occurrence, &a.Attempt)
	}
	return a, err
}

// WeftAttemptKnown reports whether the run has a stage row for the attempt
// or the attempt wrote output.
func (s *Store) WeftAttemptKnown(runID string, a WeftAttempt) (bool, error) {
	var known bool
	err := s.db.QueryRow(`SELECT EXISTS (SELECT 1 FROM weft_stages WHERE run_id=?1 AND segment=?2 AND stage=?3
		AND occurrence=?4 AND attempt=?5) OR EXISTS (SELECT 1 FROM weft_output_attempts WHERE run_id=?1 AND segment=?2
		AND stage=?3 AND occurrence=?4 AND attempt=?5)`,
		runID, a.Segment, a.Stage, a.Occurrence, a.Attempt).Scan(&known)
	return known, err
}

// WeftLiveness is an attempt's liveness as the run carries it (§8.3).
type WeftLiveness struct {
	WeftAttempt
	Stalled     bool   `json:"stalled"`
	LastEventAt string `json:"last_event_at"`
	ObservedAt  string `json:"observed_at"`
}

// RecordWeftLiveness records the worker's liveness report on the run and
// relays it as a liveness state event in the same transaction. It is not a
// run transition: the revision a command carries is not raised by a stall.
func (s *Store) RecordWeftLiveness(runID string, l WeftLiveness) error {
	doc, err := json.Marshal(l)
	if err != nil {
		return err
	}
	return s.withTx(func(tx *sql.Tx) error {
		if _, err := tx.Exec(`UPDATE weft_runs SET liveness=? WHERE id=?`, string(doc), runID); err != nil {
			return err
		}
		return appendWeftEventTx(tx, runID, "liveness", l.ObservedAt, func(seq int64) any {
			return livenessEventPayload{eventBase: newEventBase(seq, l.ObservedAt, runID), Liveness: doc}
		})
	})
}

type livenessEventPayload struct {
	eventBase
	Liveness json.RawMessage `json:"liveness"`
}

// WeftAllowance is a provider's newest observation (§5.3): its source, when
// it was observed and the provider's own windows, verbatim.
type WeftAllowance struct {
	Provider   string
	Source     string
	ObservedAt string
	Windows    json.RawMessage
}

// Tile is the observation as a dashboard tile and an allowance event carry it.
func (a WeftAllowance) Tile() map[string]any {
	return map[string]any{
		"provider": a.Provider, "state": "observed", "source": a.Source,
		"observed_at": a.ObservedAt, "windows": a.Windows, "reason": nil,
	}
}

// RecordWeftAllowance keeps the observation when it is newer than the one
// held for its provider, and relays it as an allowance event in the same
// transaction. An older or equal one changes nothing: newer wins (§5.3).
func (s *Store) RecordWeftAllowance(a WeftAllowance) (bool, error) {
	observed, err := time.Parse(time.RFC3339Nano, a.ObservedAt)
	if err != nil {
		return false, fmt.Errorf("allowance observed_at is not an RFC 3339 timestamp")
	}
	recorded := false
	err = s.withTx(func(tx *sql.Tx) error {
		var held string
		err := tx.QueryRow(`SELECT observed_at FROM weft_allowance WHERE provider=?`, a.Provider).Scan(&held)
		switch {
		case errors.Is(err, sql.ErrNoRows):
		case err != nil:
			return err
		default:
			prior, err := time.Parse(time.RFC3339Nano, held)
			if err == nil && !observed.After(prior) {
				return nil
			}
		}
		if _, err := tx.Exec(`INSERT INTO weft_allowance(provider,source,observed_at,windows) VALUES(?,?,?,?)
			ON CONFLICT(provider) DO UPDATE SET source=excluded.source,observed_at=excluded.observed_at,
			windows=excluded.windows`, a.Provider, a.Source, a.ObservedAt, string(a.Windows)); err != nil {
			return err
		}
		recorded = true
		stamp := weftTimestamp(s.now())
		return appendWeftEventTx(tx, "", "allowance", stamp, func(seq int64) any {
			return allowanceEventPayload{ContractVersion: WeftServiceVersion, Seq: seq, TS: stamp, Tile: a.Tile()}
		})
	})
	return recorded, err
}

type allowanceEventPayload struct {
	ContractVersion string         `json:"contract_version"`
	Seq             int64          `json:"seq"`
	TS              string         `json:"ts"`
	Tile            map[string]any `json:"tile"`
}

// WeftAllowances is every provider's held observation, by provider.
func (s *Store) WeftAllowances() (map[string]WeftAllowance, error) {
	rows, err := s.db.Query(`SELECT provider,source,observed_at,windows FROM weft_allowance`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]WeftAllowance{}
	for rows.Next() {
		var a WeftAllowance
		var windows string
		if err := rows.Scan(&a.Provider, &a.Source, &a.ObservedAt, &windows); err != nil {
			return nil, err
		}
		a.Windows = json.RawMessage(windows)
		out[a.Provider] = a
	}
	return out, rows.Err()
}
