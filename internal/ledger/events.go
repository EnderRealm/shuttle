package ledger

import "database/sql"

// Event is the committed transition relevant to notifications. It is computed
// inside the terminal write's transaction, so concurrent writers cannot claim
// the same new incident or recovery. Delivery happens only after commit.
type Event struct {
	Agent        string
	RunID        string
	Class        string
	Signature    string
	IncidentID   int64
	NewIncident  bool
	PausedReason string
	Recovered    bool
}

// CompleteEvent is Complete with the transition needed by the notifier.
func (s *Store) CompleteEvent(id string, res Result) (Event, error) {
	var event Event
	err := s.withTx(func(tx *sql.Tx) error {
		agent, err := requireStatus(tx, id, StatusCompleted, StatusClaimed, StatusRunning)
		if err != nil {
			return err
		}
		event = Event{Agent: agent, RunID: id}
		// Resume clears the counter but does not turn a failed run into a
		// success. Consult the terminal history so recovery survives resume.
		var last sql.NullString
		if err := tx.QueryRow(`SELECT (SELECT status FROM runs WHERE agent = ? AND status IN (?, ?) ORDER BY ended_at DESC, rowid DESC LIMIT 1)`, agent, StatusCompleted, StatusFailed).Scan(&last); err != nil {
			return err
		}
		event.Recovered = last.String == StatusFailed
		now := s.timestamp()
		if err := finishRun(tx, id, StatusCompleted, now, res, "", ""); err != nil {
			return err
		}
		return recordSuccess(tx, agent, now)
	})
	if err != nil {
		return Event{}, err
	}
	if s.OnTerminal != nil {
		s.OnTerminal(event)
	}
	return event, nil
}

// FailEvent is Fail with new-incident and newly-paused information.
func (s *Store) FailEvent(id, class, errText string, res Result) (Event, error) {
	var event Event
	err := s.withTx(func(tx *sql.Tx) error {
		agent, err := requireStatus(tx, id, StatusFailed, StatusClaimed, StatusRunning)
		if err != nil {
			return err
		}
		event = Event{Agent: agent, RunID: id, Class: class, Signature: Signature(class, errText)}
		var paused bool
		if err := tx.QueryRow(`SELECT COALESCE((SELECT paused FROM agents WHERE name = ?), 0)`, agent).Scan(&paused); err != nil {
			return err
		}
		now := s.timestamp()
		if err := finishRun(tx, id, StatusFailed, now, res, class, event.Signature); err != nil {
			return err
		}
		if err := recordFailure(tx, agent, class, now); err != nil {
			return err
		}
		if err := upsertIncident(tx, agent, event.Signature, id, now); err != nil {
			return err
		}
		var count int
		if err := tx.QueryRow(`SELECT id, count FROM incidents WHERE agent = ? AND signature = ? AND acknowledged_at IS NULL`, agent, event.Signature).Scan(&event.IncidentID, &count); err != nil {
			return err
		}
		event.NewIncident = count == 1
		if !paused {
			var reason sql.NullString
			if err := tx.QueryRow(`SELECT paused_reason FROM agents WHERE name = ?`, agent).Scan(&reason); err != nil {
				return err
			}
			event.PausedReason = reason.String
		}
		return nil
	})
	if err != nil {
		return Event{}, err
	}
	if s.OnTerminal != nil {
		s.OnTerminal(event)
	}
	return event, nil
}
