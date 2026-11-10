package ledger

import (
	"encoding/json"
	"path/filepath"
	"testing"
)

// A ledger stamped 10 gains weft_questions.questions and
// weft_answers.answers, its questions and answers reading as the legacy
// form they were recorded in.
func TestSchemaTenMigratesToEleven(t *testing.T) {
	path := filepath.Join(t.TempDir(), "runs.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	run := claimVisible(t, s, "demo/migrated-0011")
	if err := s.RecordWeftHello(run.ID, 1, "20261001T000000Z-11111111", "base", "criteria"); err != nil {
		t.Fatal(err)
	}
	qid, err := s.RecordWeftParked(run.ID, 1, "work", 1, 1, "Which shape?", nil, "head", json.RawMessage(`{"state":"rendered"}`))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.CompleteWeftSegment(run.ID, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ClaimWeftAnswer(run.ID, qid, "The existing one.", filepath.Join(t.TempDir(), "runs")); err != nil {
		t.Fatal(err)
	}
	for _, stmt := range []string{`ALTER TABLE weft_questions DROP COLUMN questions`,
		`ALTER TABLE weft_answers DROP COLUMN answers`,
		`UPDATE schema_meta SET value='10' WHERE key='schema_version'`} {
		if _, err := s.db.Exec(stmt); err != nil {
			t.Fatal(err)
		}
	}
	s.Close()
	if s, err = Open(path); err != nil {
		t.Fatalf("migrate 10 to 11: %v", err)
	}
	defer s.Close()
	var version string
	if err := s.db.QueryRow(`SELECT value FROM schema_meta WHERE key='schema_version'`).Scan(&version); err != nil || version != "11" {
		t.Fatalf("version = %q %v", version, err)
	}
	q, err := s.WeftQuestion(qid)
	if err != nil {
		t.Fatal(err)
	}
	if q.Text != "Which shape?" || q.Questions != nil || q.Answer == nil || q.Answer.Text != "The existing one." || q.Answer.Answers != nil {
		t.Fatalf("migrated question = %+v answer = %+v", q, q.Answer)
	}
}
