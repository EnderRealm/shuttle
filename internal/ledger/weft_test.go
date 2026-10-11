package ledger

import (
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// schemaTwo writes a weft-remote/v1 ledger as a schema-2 binary left it: a
// run that parked, was answered and finished; a run still holding its
// repository lock; three v1 events; and a v1 idempotency record.
func schemaTwo(t *testing.T, path string, extraLocks string) {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	stmts := []string{
		`CREATE TABLE schema_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)`,
		schemaSQL,
		`INSERT INTO schema_meta(key,value) VALUES('schema_version','2')`,
		`INSERT INTO runs(id,agent,trigger_kind,scheduled_for,claimed_at,status,run_dir) VALUES
			('weft-worker-20260913-142233-aaaa','weft-worker','weft','2026-09-13T14:22:33Z','2026-09-13T14:22:33Z','completed','/runs/a1'),
			('weft-worker-20260913-150000-bbbb','weft-worker','weft','2026-09-13T15:00:00Z','2026-09-13T15:00:00Z','completed','/runs/a2'),
			('weft-worker-20260914-090000-cccc','weft-worker','weft','2026-09-14T09:00:00Z','2026-09-14T09:00:00Z','running','/runs/b1')`,
		`INSERT INTO weft_runs(id,ticket,project,repo,branch,worktree,base,phase,outcome,stopped,created_at,updated_at,ended_at) VALUES
			('r-20260913T142233Z-6b2f0c1a','weft/example-ticket-0a1b','weft','/Users/steve/code/weft','weft/example-ticket-0a1b','/w/a','base','ended','finished',0,'2026-09-13T14:22:33.412Z','2026-09-13T16:49:03.556Z','2026-09-13T16:49:03.556Z'),
			('r-20260914T090000Z-0c1d2e3f','loom/example-cache-5e6f','loom','/Users/steve/code/loom','loom/example-cache-5e6f','/w/b','base','running',NULL,0,'2026-09-14T09:00:00.000Z','2026-09-14T09:00:00.000Z',NULL)`,
		`INSERT INTO weft_segments(run_id,segment,kind,ledger_run_id,worker_run_id,answer_id,terminal) VALUES
			('r-20260913T142233Z-6b2f0c1a',1,'launch','weft-worker-20260913-142233-aaaa','20260913T142235Z-11111111',NULL,'parked'),
			('r-20260913T142233Z-6b2f0c1a',2,'answer','weft-worker-20260913-150000-bbbb','20260913T150002Z-22222222','a-20260913T150000Z-33333333','ended'),
			('r-20260914T090000Z-0c1d2e3f',1,'launch','weft-worker-20260914-090000-cccc',NULL,NULL,NULL)`,
		`INSERT INTO weft_questions(id,run_id,segment,stage,occurrence,attempt,text,asked_at,state,answer_id) VALUES
			('q-20260913T145900Z-44444444','r-20260913T142233Z-6b2f0c1a',1,'work',1,1,'Which shape?','2026-09-13T14:59:00.000Z','answered','a-20260913T150000Z-33333333')`,
		`INSERT INTO weft_answers(id,question_id,run_id,text,submitted_at,segment) VALUES
			('a-20260913T150000Z-33333333','q-20260913T145900Z-44444444','r-20260913T142233Z-6b2f0c1a','The existing one.','2026-09-13T15:00:00.000Z',2)`,
		`INSERT INTO weft_events(ts,run_id,type,payload) VALUES
			('2026-09-13T14:22:33.412Z','r-20260913T142233Z-6b2f0c1a','run','{"contract_version":"weft-remote/v1"}'),
			('2026-09-13T16:49:03.556Z','r-20260913T142233Z-6b2f0c1a','ended','{"contract_version":"weft-remote/v1"}'),
			('2026-09-14T09:00:00.000Z','r-20260914T090000Z-0c1d2e3f','run','{"contract_version":"weft-remote/v1"}')`,
		`INSERT INTO weft_requests(token_id,key,method,path,body_sha256,status,response,created_at) VALUES
			('token','launch-1','POST','/v1/runs','body',201,'{"run":{}}','2026-09-14T09:00:00.000Z')`,
		`INSERT INTO weft_repository_locks(repository,held_by_run,acquired_at) VALUES
			('darwin:16777232:4242','r-20260914T090000Z-0c1d2e3f','2026-09-14T09:00:00.000Z')` + extraLocks,
	}
	for _, stmt := range stmts {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
}

func TestOpenMigratesSchemaTwoToWeftServiceV2(t *testing.T) {
	path := filepath.Join(t.TempDir(), "runs.db")
	schemaTwo(t, path, "")
	s, err := Open(path)
	if err != nil {
		t.Fatalf("migrate: %v", err)
	}
	hostID, err := s.WeftHostID()
	if err != nil {
		t.Fatal(err)
	}
	if !regexp.MustCompile(`^h-[a-z0-9-]{1,40}-[0-9a-f]{8}$`).MatchString(hostID) {
		t.Fatalf("host_id = %q", hostID)
	}
	finished, err := s.GetWeftRun("r-20260913T142233Z-6b2f0c1a")
	if err != nil {
		t.Fatal(err)
	}
	if finished.RecordedUnder != "weft-remote/v1" || finished.Origin != "hosted" ||
		finished.Disposition != "retained" || finished.SettlementState != "not_applicable" ||
		finished.SettlementDetail.String != "recorded under weft-remote/v1, which never lands" ||
		finished.RepositoryID != "weft-47327fe5" || finished.Revision != 1 || len(finished.Segments) != 2 {
		t.Fatalf("migrated v1 run = %+v", finished)
	}
	question, err := s.WeftQuestion("q-20260913T145900Z-44444444")
	if err != nil {
		t.Fatal(err)
	}
	if question.Answer == nil || question.Answer.ConsumedSegment.Int64 != 2 || question.Answer.EntryID.Valid {
		t.Fatalf("migrated answer = %+v", question.Answer)
	}
	if req, err := s.GetWeftRequest("token", "launch-1"); err != nil || req.Path != "/v1/runs" || req.Status != 201 {
		t.Fatalf("v1 idempotency record = %+v, %v", req, err)
	}
	var lockTable int
	if err := s.db.QueryRow(`SELECT count(*) FROM sqlite_master WHERE name='weft_repository_locks'`).Scan(&lockTable); err != nil || lockTable != 0 {
		t.Fatalf("weft_repository_locks survived the migration: %d, %v", lockTable, err)
	}
	var repositoryID, repository, kind, holder string
	if err := s.db.QueryRow(`SELECT repository_id,repository,holder_kind,holder_id FROM weft_reservations`).
		Scan(&repositoryID, &repository, &kind, &holder); err != nil {
		t.Fatal(err)
	}
	if repositoryID != "loom-caa7eb66" || repository != "/Users/steve/code/loom" || kind != "run" ||
		holder != "r-20260914T090000Z-0c1d2e3f" {
		t.Fatalf("moved reservation = %s %s %s %s", repositoryID, repository, kind, holder)
	}
	root := filepath.Join(t.TempDir(), "runs")
	_, err = s.ClaimWeft(WeftLaunch{Ticket: "loom/next", Project: "loom", Repo: "/Users/steve/code/loom",
		Branch: "loom/next", Worktree: "/w/next", RunDirRoot: root})
	var busy *RepositoryBusyError
	if !errors.As(err, &busy) || busy.HeldBy != "r-20260914T090000Z-0c1d2e3f" {
		t.Fatalf("claim over the moved reservation = %v", err)
	}
	if floor, err := s.WeftV1EventFloor(); err != nil || floor != 3 {
		t.Fatalf("v1 event floor = %d, %v", floor, err)
	}
	floor, oldest, newest, err := s.WeftStreamBounds()
	if err != nil || floor != 3 || oldest != 4 || newest != 3 {
		t.Fatalf("stream bounds = floor %d, %d..%d, %v; want no v2 event retained after seq 3", floor, oldest, newest, err)
	}
	s.Close()

	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	again, err := s.WeftHostID()
	if err != nil || again != hostID {
		t.Fatalf("host_id after reopen = %q, %v; minted %q", again, err, hostID)
	}
}

func TestSchemaTwoMigrationIsAllOrNothing(t *testing.T) {
	path := filepath.Join(t.TempDir(), "runs.db")
	// A second lock on the same checkout under another identity key: the
	// migration refuses to pick a holder.
	schemaTwo(t, path, `,('/Users/steve/code/loom','r-20260913T142233Z-6b2f0c1a','2026-09-14T09:00:01.000Z')`)
	db, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`UPDATE weft_runs SET repo='/Users/steve/code/loom' WHERE id='r-20260913T142233Z-6b2f0c1a'`); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(path); err == nil || !strings.Contains(err.Error(), "repository lock") {
		t.Fatalf("migration over two locks on one repository = %v", err)
	}
	var version string
	var locks int
	if err := db.QueryRow(`SELECT value FROM schema_meta WHERE key='schema_version'`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT count(*) FROM weft_repository_locks`).Scan(&locks); err != nil {
		t.Fatalf("schema-2 tables did not survive the failed migration: %v", err)
	}
	if version != "2" || locks != 2 {
		t.Fatalf("after a failed migration: version %s, %d locks", version, locks)
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
	answer, err := s.ClaimWeftAnswer(run.ID, qid, "Use the existing shape.", root)
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
	if len(events) != 7 {
		t.Fatalf("events = %d, want launch, hello, stage, question, park, the answered question and the requeue", len(events))
	}
	for _, event := range events {
		if !strings.HasPrefix(string(event.Payload), `{"contract_version":"weft-service/v2","seq":`) {
			t.Fatalf("event payload order = %s", event.Payload)
		}
	}
	if answered := events[5]; answered.Type != "question" || !strings.Contains(string(answered.Payload), `"state":"answered"`) ||
		!strings.Contains(string(answered.Payload), `"entry_id":null`) {
		t.Fatalf("answer recorded as %s %s", answered.Type, answered.Payload)
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
