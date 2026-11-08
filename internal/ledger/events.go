package ledger

import (
	"database/sql"
	"errors"
	"fmt"
)

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
		// A reconciled run is completed but keeps its failure class (see
		// Reconciled); it did not recover the agent, so it reads as failed.
		var last sql.NullString
		if err := tx.QueryRow(`SELECT (SELECT CASE WHEN failure_class IS NULL THEN status ELSE ? END FROM runs WHERE agent = ? AND status IN (?, ?) ORDER BY ended_at DESC, rowid DESC LIMIT 1)`, StatusFailed, agent, StatusCompleted, StatusFailed).Scan(&last); err != nil {
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
		now := s.timestamp()
		if err := finishRun(tx, id, StatusFailed, now, res, class, event.Signature); err != nil {
			return err
		}
		if event.PausedReason, err = recordFailure(tx, agent, class, now); err != nil {
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

// IncidentDeliveryAuth prefixes the signature of an incident opened because
// the email provider rejected the delivery credential (§10.1). It is not a
// §7.6 failure class: the run completed and only its email failed.
const IncidentDeliveryAuth = "delivery_auth"

// DeliveryAuthIncident opens, or counts a repeat of, the incident for a run
// whose delivery was refused on its credential. The run's status, the agent's
// streak and its pause are untouched: the model did its work, and the fix is
// the token rather than the agent (§6.1).
func (s *Store) DeliveryAuthIncident(id, errText string) (Event, error) {
	var event Event
	err := s.withTx(func(tx *sql.Tx) error {
		var agent string
		err := tx.QueryRow(`SELECT agent FROM runs WHERE id = ?`, id).Scan(&agent)
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("run %s: not in the ledger", id)
		}
		if err != nil {
			return err
		}
		event = Event{Agent: agent, RunID: id, Class: IncidentDeliveryAuth, Signature: Signature(IncidentDeliveryAuth, errText)}
		if err := upsertIncident(tx, agent, event.Signature, id, s.timestamp()); err != nil {
			return err
		}
		var count int
		if err := tx.QueryRow(`SELECT id, count FROM incidents WHERE agent = ? AND signature = ? AND acknowledged_at IS NULL`, agent, event.Signature).Scan(&event.IncidentID, &count); err != nil {
			return err
		}
		event.NewIncident = count == 1
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
