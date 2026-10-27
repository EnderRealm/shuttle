package ledger

import (
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"

	"shuttle/internal/sandbox"
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
	var kinds []string
	for _, event := range events {
		kinds = append(kinds, event.Type)
	}
	// The repository events are its reservation taken by the launch, released
	// at reap and taken again by the answer.
	if want := []string{"repository", "run", "run", "stage", "question", "run", "repository", "capacity",
		"repository", "question", "run"}; !reflect.DeepEqual(kinds, want) {
		t.Fatalf("events = %v, want %v", kinds, want)
	}
	if reaped := events[7]; reaped.Type != "capacity" || !strings.Contains(string(reaped.Payload), `"capacity":{"cap":3,"running":0,"waiting":0}`) {
		t.Fatalf("reap recorded as %s %s", reaped.Type, reaped.Payload)
	}
	for _, event := range events {
		if !strings.HasPrefix(string(event.Payload), `{"contract_version":"weft-service/v2","seq":`) {
			t.Fatalf("event payload order = %s", event.Payload)
		}
	}
	if answered := events[9]; answered.Type != "question" || !strings.Contains(string(answered.Payload), `"state":"answered"`) ||
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
	if err := s.MarkWeftRunning(run.ID, 1, 12345, "weft-remote/v1"); err != nil {
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

// §12 steps 4 and 5: an archiving entry whose worker is gone is settled by
// the archive ref it was to write — at the superseded run's head, the
// maintenance row becomes the new run's in one transaction and its slot is
// carried; absent, the row and slot are released and the entry waits again.
// A maintenance row whose entry is no longer archiving, with nothing in
// flight, is a gone holder.
func TestRestartSettlesArchivingEntriesByTheirArchiveRef(t *testing.T) {
	s := newStore(t)
	root := t.TempDir()
	runs := filepath.Join(root, "runs")
	retained := func(ticket, repo string) *WeftRun {
		t.Helper()
		run, err := s.ClaimWeft(WeftLaunch{Ticket: ticket, Project: "weft", Repo: repo, Branch: ticket,
			Worktree: filepath.Join(root, "worktrees", strings.ReplaceAll(ticket, "/", "-")), RunDirRoot: runs})
		if err != nil {
			t.Fatal(err)
		}
		if err := s.RecordWeftEnded(run.ID, 1, "halted", false, "review", "rejected", "", "", "", "head-"+run.ID, nil); err != nil {
			t.Fatal(err)
		}
		if err := s.CompleteWeftSegment(run.ID, 1); err != nil {
			t.Fatal(err)
		}
		return run
	}
	archived := filepath.Join(root, "archived")
	refused := filepath.Join(root, "refused")
	cleaned := filepath.Join(root, "cleaned")
	for _, dir := range []string{archived, refused, cleaned} {
		if err := mkdir(dir); err != nil {
			t.Fatal(err)
		}
	}
	first := retained("weft/archived-0001", archived)
	second := retained("weft/refused-0002", refused)
	for i, e := range []struct{ id, ticket, repo, supersedes, state string }{
		{"e-20261011T120000Z-00000001", first.Ticket, archived, first.ID, WeftEntryArchiving},
		{"e-20261011T120000Z-00000002", second.Ticket, refused, second.ID, WeftEntryArchiving},
		{"e-20261011T120000Z-00000003", "weft/cleaned-0003", cleaned, first.ID, WeftEntryCancelled},
	} {
		if _, err := s.db.Exec(`INSERT INTO weft_entries(id,kind,ticket,project,repository_id,repository,position,state,
			supersedes,archive_head,token_id,request_key,created_at,updated_at,revision)
			VALUES(?,'fresh',?,'weft',?,?,?,?,?,?,'token','key','2026-10-11T12:00:00.000Z','2026-10-11T12:00:00.000Z',2)`,
			e.id, e.ticket, sandboxID(e.repo), e.repo, i+1, e.state, e.supersedes, "head-"+e.supersedes); err != nil {
			t.Fatal(err)
		}
		if _, err := s.db.Exec(`INSERT INTO weft_reservations(repository_id,repository,holder_kind,holder_id,acquired_at)
			VALUES(?,?,'maintenance',?,'2026-10-11T12:00:00.000Z')`, sandboxID(e.repo), e.repo, e.id); err != nil {
			t.Fatal(err)
		}
	}
	before, err := s.WeftAsOf()
	if err != nil {
		t.Fatal(err)
	}
	if c, err := s.WeftCapacity(); err != nil || c.Running != 2 {
		t.Fatalf("archiving entries hold %+v slots, %v", c, err)
	}
	var asked []string
	released, _, spawn, err := s.RestartWeftQueue(WeftDispatch{WeftRoot: filepath.Join(root, "weft"), RunDirRoot: runs},
		func(repo, ref string) (string, error) {
			asked = append(asked, ref)
			if ref == "refs/weft/archive/"+first.ID {
				return "head-" + first.ID, nil
			}
			return "", nil
		})
	if err != nil {
		t.Fatal(err)
	}
	if len(released) != 1 || released[0].HolderID != "e-20261011T120000Z-00000003" {
		t.Fatalf("released = %+v", released)
	}
	if len(asked) != 2 || len(spawn) != 1 {
		t.Fatalf("asked %v, spawn %v", asked, spawn)
	}
	fresh, err := s.GetWeftRun(spawn[0])
	if err != nil {
		t.Fatal(err)
	}
	old, err := s.GetWeftRun(first.ID)
	if err != nil {
		t.Fatal(err)
	}
	if fresh.Supersedes.String != first.ID || fresh.Phase != WeftPhaseQueued || fresh.EntryID.String != "e-20261011T120000Z-00000001" ||
		old.Disposition != WeftDispositionArchived || old.SupersededBy.String != fresh.ID ||
		old.ArchiveRef.String != "refs/weft/archive/"+first.ID || old.Outcome.String != "stopped" {
		t.Fatalf("fresh %+v old %+v", fresh, old)
	}
	var holderKind, holderID string
	if err := s.db.QueryRow(`SELECT holder_kind,holder_id FROM weft_reservations WHERE repository_id=?`, sandboxID(archived)).
		Scan(&holderKind, &holderID); err != nil || holderKind != "run" || holderID != fresh.ID {
		t.Fatalf("archived repository held by %s %s, %v", holderKind, holderID, err)
	}
	var n int
	if err := s.db.QueryRow(`SELECT count(*) FROM weft_reservations WHERE repository_id IN (?,?)`, sandboxID(refused), sandboxID(cleaned)).
		Scan(&n); err != nil || n != 0 {
		t.Fatalf("refused and cleaned repositories still reserved: %d %v", n, err)
	}
	q, err := s.WeftQueue(s.now().Add(-time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	states := map[string]string{}
	for _, e := range q.Entries {
		states[e.ID] = e.State + " " + e.RunID.String
	}
	if states["e-20261011T120000Z-00000001"] != "dispatched "+fresh.ID || states["e-20261011T120000Z-00000002"] != "waiting " {
		t.Fatalf("entries = %v", states)
	}
	if q.Capacity.Running != 1 || q.Capacity.Waiting != 1 {
		t.Fatalf("capacity = %+v", q.Capacity)
	}
	events, err := s.WeftEventsAfter(before)
	if err != nil {
		t.Fatal(err)
	}
	var kinds []string
	for _, e := range events {
		kinds = append(kinds, e.Type)
	}
	// Step 4 releases the cleaned entry's row; step 5 rewrites the archived
	// repository's row to its new run with one repository event and carries
	// the slot, and releases the refused one's row and slot.
	if !reflect.DeepEqual(kinds, []string{"repository", "repository", "queue", "run", "run", "repository", "queue", "capacity"}) {
		t.Fatalf("events = %v", kinds)
	}
}

// reportedStages records a segment's work stage done and its review stage
// running, as a worker reports them before it is gone mid-review.
func reportedStages(t *testing.T, s *Store, runID string) []WeftStage {
	t.Helper()
	stages := []WeftStage{
		{RunID: runID, Segment: 1, Stage: "work", Occurrence: 1, Attempt: 1, State: "done",
			Detail: sql.NullString{String: "work done", Valid: true}, At: "2026-09-02T04:00:00.000Z",
			Turns: sql.NullInt64{Int64: 2, Valid: true}, Tokens: json.RawMessage(`{"input":3,"output":4}`)},
		{RunID: runID, Segment: 1, Stage: "review", Occurrence: 1, Attempt: 1, State: "running",
			Backend: sql.NullString{String: "claude", Valid: true}, At: "2026-09-02T04:10:00.000Z"},
	}
	for _, stage := range stages {
		if err := s.RecordWeftStage(stage); err != nil {
			t.Fatal(err)
		}
	}
	return stages
}

func stageEvents(t *testing.T, s *Store, runID string) []string {
	t.Helper()
	events, err := s.WeftEventsAfter(0)
	if err != nil {
		t.Fatal(err)
	}
	var payloads []string
	for _, event := range events {
		if event.Type == "stage" && event.RunID == runID {
			payloads = append(payloads, string(event.Payload))
		}
	}
	return payloads
}

// A segment settled without its terminal event — died, or closed by the
// restart pass — fails the stage its worker left running, with the segment's
// failure detail and a stage event, in the settling transaction; a stage the
// worker finished keeps its report.
func TestDeadSegmentClosesRunningStages(t *testing.T) {
	s := newStore(t)
	const died = "worker exited -1 with no terminal event"
	for _, tt := range []struct {
		name, detail  string
		land, restart bool
	}{
		{"pipeline died", died, false, false},
		{"landing died", died, true, false},
		{"daemon restart", "daemon restarted before the segment settled", false, true},
	} {
		ticket := "demo/" + strings.ReplaceAll(tt.name, " ", "-")
		var run *WeftRun
		if tt.land {
			run = landingRun(t, s, ticket)
		} else {
			var err error
			if run, err = s.ClaimWeft(WeftLaunch{Ticket: ticket, Project: "demo", Repo: "/tmp/" + ticket,
				Branch: ticket, Worktree: "/tmp/weft/" + ticket, RunDirRoot: filepath.Join(t.TempDir(), "runs")}); err != nil {
				t.Fatal(err)
			}
			if err := s.MarkWeftRunning(run.ID, 1, 12345, "weft-worker/v2"); err != nil {
				t.Fatal(err)
			}
			if err := s.RecordWeftHello(run.ID, 1, "20260902T043012Z-abcd1234", "base", "criteria"); err != nil {
				t.Fatal(err)
			}
		}
		reported := reportedStages(t, s, run.ID)
		if tt.restart {
			if err := s.MarkUnknown(run.Segments[0].LedgerRunID); err != nil {
				t.Fatal(err)
			}
			if err := s.ReconcileWeftUnknown(run.Segments[0].LedgerRunID); err != nil {
				t.Fatal(err)
			}
		} else if _, err := s.FailWeftSegment(run.ID, 1, ClassDied, tt.detail); err != nil {
			t.Fatal(err)
		}
		got, err := s.GetWeftRun(run.ID)
		if err != nil {
			t.Fatal(err)
		}
		if got.Phase == WeftPhaseRunning || len(got.Stages) != 2 {
			t.Fatalf("%s: settled run = %+v", tt.name, got)
		}
		if !reflect.DeepEqual(got.Stages[0], reported[0]) {
			t.Fatalf("%s: finished stage = %+v, want %+v", tt.name, got.Stages[0], reported[0])
		}
		closed := reported[1]
		closed.State, closed.Detail, closed.At = "failed", sql.NullString{String: tt.detail, Valid: true}, weftTimestamp(testClock)
		if !reflect.DeepEqual(got.Stages[1], closed) {
			t.Fatalf("%s: running stage settled as %+v, want %+v", tt.name, got.Stages[1], closed)
		}
		events := stageEvents(t, s, run.ID)
		if len(events) != 3 || !strings.Contains(events[2], `"stage":"review"`) ||
			!strings.Contains(events[2], `"state":"failed"`) || !strings.Contains(events[2], `"detail":"`+tt.detail+`"`) {
			t.Fatalf("%s: stage events = %v", tt.name, events)
		}
	}
}

// A segment whose terminal event was recorded keeps its stages exactly as its
// worker reported them, however its process then settles.
func TestTerminalSegmentStagesUnchanged(t *testing.T) {
	s := newStore(t)
	for _, tt := range []struct {
		name     string
		terminal func(runID string) error
		failed   bool
	}{
		{"ended", func(runID string) error {
			return s.RecordWeftEnded(runID, 1, "failed", false, "review", "error", "", "", "review failed", "head", json.RawMessage(`{"state":"rendered"}`))
		}, false},
		{"ended then died", func(runID string) error {
			return s.RecordWeftEnded(runID, 1, "failed", false, "review", "error", "", "", "review failed", "head", json.RawMessage(`{"state":"rendered"}`))
		}, true},
		{"parked", func(runID string) error {
			_, err := s.RecordWeftParked(runID, 1, "review", 1, 1, "Which shape?", "head", json.RawMessage(`{"state":"rendered"}`))
			return err
		}, false},
	} {
		ticket := "demo/" + strings.ReplaceAll(tt.name, " ", "-")
		run, err := s.ClaimWeft(WeftLaunch{Ticket: ticket, Project: "demo", Repo: "/tmp/" + ticket,
			Branch: ticket, Worktree: "/tmp/weft/" + ticket, RunDirRoot: filepath.Join(t.TempDir(), "runs")})
		if err != nil {
			t.Fatal(err)
		}
		if err := s.MarkWeftRunning(run.ID, 1, 12345, "weft-worker/v2"); err != nil {
			t.Fatal(err)
		}
		if err := s.RecordWeftHello(run.ID, 1, "20260902T043012Z-abcd1234", "base", "criteria"); err != nil {
			t.Fatal(err)
		}
		reported := reportedStages(t, s, run.ID)
		if err := tt.terminal(run.ID); err != nil {
			t.Fatal(err)
		}
		if tt.failed {
			if _, err := s.FailWeftSegment(run.ID, 1, ClassDied, "worker exited 1 after a terminal event"); err != nil {
				t.Fatal(err)
			}
		} else if err := s.CompleteWeftSegment(run.ID, 1); err != nil {
			t.Fatal(err)
		}
		got, err := s.GetWeftRun(run.ID)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got.Stages, reported) {
			t.Fatalf("%s: stages = %+v, want %+v", tt.name, got.Stages, reported)
		}
		if events := stageEvents(t, s, run.ID); len(events) != 2 {
			t.Fatalf("%s: stage events = %v", tt.name, events)
		}
	}
}

func mkdir(path string) error { return os.MkdirAll(path, 0o755) }

func sandboxID(path string) string { return sandbox.RepositoryID(path) }
