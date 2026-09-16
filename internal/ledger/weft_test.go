package ledger

import (
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"
)

func TestOpenMigratesSchemaOneToWeftSchema(t *testing.T) {
	path := filepath.Join(t.TempDir(), "runs.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	run := claim(t, s, "daily", "2026-09-02T04:30:00-07:00")
	for _, table := range []string{
		"weft_repository_locks", "weft_requests", "weft_events", "weft_answers",
		"weft_questions", "weft_stages", "weft_segments", "weft_runs",
	} {
		if _, err := s.db.Exec("DROP TABLE " + table); err != nil {
			t.Fatalf("drop %s: %v", table, err)
		}
	}
	if _, err := s.db.Exec(`UPDATE schema_meta SET value='1' WHERE key='schema_version'`); err != nil {
		t.Fatal(err)
	}
	s.Close()

	s, err = Open(path)
	if err != nil {
		t.Fatalf("migrate: %v", err)
	}
	defer s.Close()
	if _, err := s.Get(run.ID); err != nil {
		t.Fatalf("phase-1 row was not preserved: %v", err)
	}
	for _, table := range []string{
		"weft_runs", "weft_segments", "weft_stages", "weft_questions",
		"weft_answers", "weft_events", "weft_requests", "weft_repository_locks",
	} {
		var name string
		if err := s.db.QueryRow(`SELECT name FROM sqlite_master WHERE type='table' AND name=?`, table).Scan(&name); err != nil {
			t.Errorf("%s was not migrated: %v", table, err)
		}
	}
}

func TestWeftRunParksAnswersAndResumesDurably(t *testing.T) {
	s := newStore(t)
	root := filepath.Join(t.TempDir(), "runs")
	run, err := s.ClaimWeft(WeftLaunch{
		Ticket: "demo/work-1234", Project: "demo", Repo: "/tmp/demo",
		Branch: "demo/work-1234", Worktree: "/tmp/weft/work-1234", RunDirRoot: root,
	})
	if err != nil {
		t.Fatal(err)
	}
	if run.Phase != WeftPhaseQueued || len(run.Segments) != 1 {
		t.Fatalf("claimed run = %+v", run)
	}
	row, err := s.Get(run.Segments[0].LedgerRunID)
	if err != nil {
		t.Fatal(err)
	}
	if row.Agent != WeftAgent || row.TriggerKind != TriggerWeft || row.Status != StatusClaimed {
		t.Fatalf("segment ledger row = %+v", row)
	}
	if err := s.RecordWeftHello(run.ID, 1, "20260902T043012Z-abcd1234", "base", "criteria"); err != nil {
		t.Fatal(err)
	}
	stage := WeftStage{
		RunID: run.ID, Segment: 1, Stage: "work", Occurrence: 1, Attempt: 1,
		State: "needs_input", At: "2026-09-02T04:30:12.000Z",
		Turns:  sql.NullInt64{Int64: 2, Valid: true},
		Spend:  json.RawMessage(`{"usd":0,"priced":0,"unpriced":1}`),
		Tokens: json.RawMessage(`{"input":3,"output":4}`),
	}
	if err := s.RecordWeftStage(stage); err != nil {
		t.Fatal(err)
	}
	qid, err := s.RecordWeftParked(run.ID, 1, "work", 1, 1, "Which shape?", "head", json.RawMessage(`{"state":"rendered"}`))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.CompleteWeftSegment(run.ID, 1); err != nil {
		t.Fatal(err)
	}
	parked, err := s.GetWeftRun(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if parked.Phase != WeftPhaseParked || parked.Question == nil || parked.Question.ID != qid {
		t.Fatalf("parked run = %+v", parked)
	}
	row, err = s.Get(parked.Segments[0].LedgerRunID)
	if err != nil {
		t.Fatal(err)
	}
	if row.NumTurns.Int64 != 2 || row.InputTokens.Int64 != 3 || row.OutputTokens.Int64 != 4 || row.CostUSD.Valid {
		t.Fatalf("segment totals = turns %v tokens %v/%v cost %v", row.NumTurns, row.InputTokens, row.OutputTokens, row.CostUSD)
	}
	answer, err := s.ClaimWeftAnswer(run.ID, qid, "Use the existing shape.", root, "")
	if err != nil {
		t.Fatal(err)
	}
	if answer.Phase != WeftPhaseQueued || len(answer.Segments) != 2 || !answer.Segments[1].AnswerID.Valid {
		t.Fatalf("answer run = %+v", answer)
	}
	question, err := s.WeftQuestion(qid)
	if err != nil {
		t.Fatal(err)
	}
	if question.State != "answered" || question.Answer == nil || question.Answer.Text != "Use the existing shape." {
		t.Fatalf("question = %+v", question)
	}
	events, err := s.WeftEventsAfter(0)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) < 6 {
		t.Fatalf("events = %d, want launch, hello, stage, question, park and answer", len(events))
	}
	for _, event := range events {
		if !strings.HasPrefix(string(event.Payload), `{"contract_version":"weft-remote/v1","seq":`) {
			t.Fatalf("event payload order = %s", event.Payload)
		}
	}
}

func TestWeftRepositoryReservationIsAtomicAndReleasedExplicitly(t *testing.T) {
	s := newStore(t)
	root := filepath.Join(t.TempDir(), "runs")
	first, err := s.ClaimWeft(WeftLaunch{
		Ticket: "demo/one", Project: "demo", Repo: "/tmp/demo",
		Branch: "demo/one", Worktree: "/tmp/weft/one", RunDirRoot: root,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.ClaimWeft(WeftLaunch{
		Ticket: "demo/two", Project: "demo", Repo: "/tmp/demo",
		Branch: "demo/two", Worktree: "/tmp/weft/two", RunDirRoot: root,
	})
	var busy *RepositoryBusyError
	if !errors.As(err, &busy) || busy.HeldBy != first.ID {
		t.Fatalf("second claim = %v, want repository held by %s", err, first.ID)
	}
	if err := s.ReleaseWeftRepository(first.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ClaimWeft(WeftLaunch{
		Ticket: "demo/two", Project: "demo", Repo: "/tmp/demo",
		Branch: "demo/two", Worktree: "/tmp/weft/two", RunDirRoot: root,
	}); err != nil {
		t.Fatalf("claim after release: %v", err)
	}
}

func TestWeftIdempotencyRecordRoundTrips(t *testing.T) {
	s := newStore(t)
	want := WeftRequest{
		TokenID: "token", Key: "key", Method: "POST", Path: "/v1/runs",
		BodySHA: "body", Status: 201, Response: []byte(`{"ok":true}`),
		CreatedAt: "2026-09-02T04:30:12.000Z",
	}
	if err := s.PutWeftRequest(want); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetWeftRequest("token", "key")
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != want.Status || string(got.Response) != string(want.Response) {
		t.Fatalf("record = %+v", got)
	}
	if _, err := s.GetWeftRequest("token", "missing"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("missing request = %v", err)
	}
}

func TestReconcileWeftUnknownClosesAPIStateAndReleasesRepository(t *testing.T) {
	s := newStore(t)
	root := filepath.Join(t.TempDir(), "runs")
	run, err := s.ClaimWeft(WeftLaunch{
		Ticket: "demo/restart", Project: "demo", Repo: "/tmp/restart",
		Branch: "demo/restart", Worktree: "/tmp/restart-worktree", RunDirRoot: root,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.MarkWeftRunning(run.ID, 1, 12345); err != nil {
		t.Fatal(err)
	}
	if err := s.RecordWeftHello(run.ID, 1, "20260902T043012Z-abcd1234", "base", "criteria"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RecordWeftParked(run.ID, 1, "work", 1, 1, "Still there?", "head", json.RawMessage(`{"state":"rendered"}`)); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkUnknown(run.Segments[0].LedgerRunID); err != nil {
		t.Fatal(err)
	}
	if err := s.ReconcileWeftUnknown(run.Segments[0].LedgerRunID); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetWeftRun(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Phase != WeftPhaseEnded || got.Outcome.String != "failed" ||
		got.Segments[0].Failure.String != ClassUnknown || got.Question != nil {
		t.Fatalf("reconciled run = %+v", got)
	}
	if _, err := s.ClaimWeft(WeftLaunch{
		Ticket: "demo/next", Project: "demo", Repo: "/tmp/restart",
		Branch: "demo/next", Worktree: "/tmp/next-worktree", RunDirRoot: root,
	}); err != nil {
		t.Fatalf("repository remained locked: %v", err)
	}
}
