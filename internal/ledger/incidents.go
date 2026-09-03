package ledger

import (
	"database/sql"
	"fmt"
	"regexp"
	"strings"
)

// signatureMax is the §6.2 cap. A signature is an identity, not a message: the
// full error text belongs in the run directory.
const signatureMax = 120

var (
	// A token containing a slash is a path; stripped before the digits below,
	// because a path carries digits that would otherwise leave per-run
	// fragments behind.
	signaturePath   = regexp.MustCompile(`\S*/\S*`)
	signatureDigits = regexp.MustCompile(`\d+`)
	signatureSpace  = regexp.MustCompile(`\s+`)
)

// Incident is one row of the incidents table: a failure signature seen at
// least once and not yet acknowledged.
type Incident struct {
	ID             int64
	Agent          string
	Signature      string
	FirstSeen      string
	LastSeen       string
	Count          int
	LastRunID      sql.NullString
	AcknowledgedAt sql.NullString
}

// Signature normalises a failure into its incident key (§6.2): the failure
// class plus the first line of the error, lowercased, digits and paths
// stripped, capped at 120 characters. Two runs that failed the same way must
// produce the same string, or every night opens a fresh incident.
func Signature(class, errText string) string {
	line, _, _ := strings.Cut(errText, "\n")
	line = strings.ToLower(line)
	line = signaturePath.ReplaceAllString(line, "")
	line = signatureDigits.ReplaceAllString(line, "")
	line = strings.TrimSpace(signatureSpace.ReplaceAllString(line, " "))
	sig := class
	if line != "" {
		sig += ": " + line
	}
	if r := []rune(sig); len(r) > signatureMax {
		sig = string(r[:signatureMax])
	}
	return sig
}

// Ack closes an open incident. The next occurrence of the same signature opens
// a new one, which is what the partial unique index on the unacknowledged rows
// is for (§6.2).
func (s *Store) Ack(id int64) error {
	res, err := s.db.Exec(
		`UPDATE incidents SET acknowledged_at = ? WHERE id = ? AND acknowledged_at IS NULL`,
		s.timestamp(), id,
	)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return fmt.Errorf("incident %d: not open", id)
	}
	return nil
}

// OpenIncidents lists the unacknowledged incidents, most recently seen first.
func (s *Store) OpenIncidents() ([]*Incident, error) {
	rows, err := s.db.Query(
		`SELECT id, agent, signature, first_seen, last_seen, count, last_run_id, acknowledged_at
			FROM incidents WHERE acknowledged_at IS NULL ORDER BY last_seen DESC, id DESC`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var incidents []*Incident
	for rows.Next() {
		var i Incident
		if err := rows.Scan(&i.ID, &i.Agent, &i.Signature, &i.FirstSeen, &i.LastSeen,
			&i.Count, &i.LastRunID, &i.AcknowledgedAt); err != nil {
			return nil, err
		}
		incidents = append(incidents, &i)
	}
	return incidents, rows.Err()
}

// upsertIncident counts a repeat rather than duplicating it. The conflict
// target is the partial index, so an acknowledged row does not absorb the next
// occurrence: that one inserts a new incident.
func upsertIncident(tx *sql.Tx, agent, signature, runID, now string) error {
	_, err := tx.Exec(
		`INSERT INTO incidents(agent, signature, first_seen, last_seen, count, last_run_id)
			VALUES (?, ?, ?, ?, 1, ?)
			ON CONFLICT(agent, signature) WHERE acknowledged_at IS NULL
			DO UPDATE SET count = count + 1, last_seen = excluded.last_seen, last_run_id = excluded.last_run_id`,
		agent, signature, now, now, runID,
	)
	return err
}
