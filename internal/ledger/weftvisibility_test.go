package ledger

import (
	"database/sql"
	"encoding/json"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func claimVisible(t *testing.T, s *Store, ticket string) *WeftRun {
	t.Helper()
	run, err := s.ClaimWeft(WeftLaunch{
		Ticket: ticket, Project: "demo", Repo: "/tmp/" + strings.ReplaceAll(ticket, "/", "-"),
		Branch: ticket, Worktree: "/tmp/weft/" + ticket, RunDirRoot: filepath.Join(t.TempDir(), "runs"),
	})
	if err != nil {
		t.Fatal(err)
	}
	return run
}

// counts is every authoritative weft table's row count, and the events.
func counts(t *testing.T, s *Store) map[string]int {
	t.Helper()
	out := map[string]int{}
	for _, table := range []string{"weft_runs", "weft_segments", "weft_stages", "weft_questions", "weft_answers",
		"weft_events", "weft_output", "runs"} {
		var n int
		if err := s.db.QueryRow(`SELECT count(*) FROM ` + table).Scan(&n); err != nil {
			t.Fatal(err)
		}
		out[table] = n
	}
	return out
}

func TestWeftOutputKeepsTheNewest256KiBOfEachAttempt(t *testing.T) {
	s := newStore(t)
	run := claimVisible(t, s, "demo/output-0001")
	work := WeftAttempt{Segment: 1, Stage: "work", Occurrence: 1, Attempt: 1}
	review := WeftAttempt{Segment: 1, Stage: "review", Occurrence: 1, Attempt: 1}
	chunk := strings.Repeat("x", 8<<10)
	before := counts(t, s)
	var last int64
	for i := 1; i <= 40; i++ {
		cursor, err := s.RecordWeftOutput(WeftOutputChunk{RunID: run.ID, WeftAttempt: work, Chunk: i, At: "2026-10-02T10:00:00.000Z", Text: chunk})
		if err != nil {
			t.Fatal(err)
		}
		if cursor != last+1 {
			t.Fatalf("chunk %d cursor %d, want %d", i, cursor, last+1)
		}
		last = cursor
		if i == 20 {
			if _, err := s.RecordWeftOutput(WeftOutputChunk{RunID: run.ID, WeftAttempt: review, Chunk: 1, At: "2026-10-02T10:00:00.000Z", Text: "review"}); err != nil {
				t.Fatal(err)
			}
			last++
		}
	}
	kept, err := s.WeftAttemptOutput(run.ID, work)
	if err != nil {
		t.Fatal(err)
	}
	total := 0
	for i, c := range kept {
		total += len(c.Text)
		if c.Chunk != kept[0].Chunk+i {
			t.Fatalf("retained chunks are not contiguous: %d at %d", c.Chunk, i)
		}
	}
	if total > WeftOutputRetainBytes || total+len(chunk) <= WeftOutputRetainBytes || kept[len(kept)-1].Chunk != 40 {
		t.Fatalf("retained %d bytes in chunks %d..%d", total, kept[0].Chunk, kept[len(kept)-1].Chunk)
	}
	if other, err := s.WeftAttemptOutput(run.ID, review); err != nil || len(other) != 1 {
		t.Fatalf("the review attempt lost output to work's retention: %v %v", other, err)
	}
	oldest, newest, err := s.WeftOutputBounds(run.ID)
	if err != nil || newest != last || oldest != kept[0].Cursor {
		t.Fatalf("bounds = %d..%d %v, want %d..%d", oldest, newest, err, kept[0].Cursor, last)
	}
	// One chunk over the cap on its own is still kept: the newest is never
	// dropped.
	big := strings.Repeat("y", WeftOutputRetainBytes+1)
	if _, err := s.RecordWeftOutput(WeftOutputChunk{RunID: run.ID, WeftAttempt: work, Chunk: 41, At: "2026-10-02T10:00:00.000Z", Text: big}); err != nil {
		t.Fatal(err)
	}
	if kept, _ = s.WeftAttemptOutput(run.ID, work); len(kept) != 1 || kept[0].Chunk != 41 {
		t.Fatalf("after an oversized chunk = %d chunks", len(kept))
	}
	// Retention touched weft_output alone; output never became an event.
	after := counts(t, s)
	for table, n := range before {
		if table != "weft_output" && after[table] != n {
			t.Errorf("%s: %d rows, was %d", table, after[table], n)
		}
	}
}

func TestWeftOutputPruneSparesAuthoritativeState(t *testing.T) {
	s := newStore(t)
	old := time.Date(2026, 9, 20, 9, 0, 0, 0, time.UTC)
	now := time.Date(2026, 10, 2, 9, 0, 0, 0, time.UTC)
	s.now = func() time.Time { return old }
	ended := claimVisible(t, s, "demo/ended-0001")
	if err := s.RecordWeftHello(ended.ID, 1, "20260920T090000Z-11111111", "base", "criteria"); err != nil {
		t.Fatal(err)
	}
	if err := s.RecordWeftStage(WeftStage{RunID: ended.ID, Segment: 1, Stage: "work", Occurrence: 1, Attempt: 1,
		State: "needs_input", At: "2026-09-20T09:00:00.000Z"}); err != nil {
		t.Fatal(err)
	}
	attempt := WeftAttempt{Segment: 1, Stage: "work", Occurrence: 1, Attempt: 1}
	if _, err := s.RecordWeftOutput(WeftOutputChunk{RunID: ended.ID, WeftAttempt: attempt, Chunk: 1, At: "2026-09-20T09:00:00.000Z", Text: "old"}); err != nil {
		t.Fatal(err)
	}
	qid, err := s.RecordWeftParked(ended.ID, 1, "work", 1, 1, "Which shape?", "head", json.RawMessage(`{"state":"rendered"}`))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.CompleteWeftSegment(ended.ID, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ClaimWeftAnswer(ended.ID, qid, "The existing one.", filepath.Join(t.TempDir(), "runs")); err != nil {
		t.Fatal(err)
	}
	if err := s.RecordWeftEnded(ended.ID, 2, "finished", false, "", "", "", "", "", "head", json.RawMessage(`{"state":"rendered"}`)); err != nil {
		t.Fatal(err)
	}
	s.now = func() time.Time { return now }
	live := claimVisible(t, s, "demo/live-0002")
	if _, err := s.RecordWeftOutput(WeftOutputChunk{RunID: live.ID, WeftAttempt: attempt, Chunk: 1, At: "2026-10-02T09:00:00.000Z", Text: "new"}); err != nil {
		t.Fatal(err)
	}
	before := counts(t, s)
	var recent int
	if err := s.db.QueryRow(`SELECT count(*) FROM weft_events WHERE ts>=?`, weftTimestamp(now.Add(-7*24*time.Hour))).Scan(&recent); err != nil {
		t.Fatal(err)
	}
	if err := s.PruneWeftRetention(now.Add(-7 * 24 * time.Hour)); err != nil {
		t.Fatal(err)
	}
	after := counts(t, s)
	for _, table := range []string{"weft_runs", "weft_segments", "weft_stages", "weft_questions", "weft_answers", "runs"} {
		if after[table] != before[table] {
			t.Errorf("%s: %d rows after the prune, %d before", table, after[table], before[table])
		}
	}
	// Events go by their own seven-day rule, never with output.
	if after["weft_events"] != recent || after["weft_output"] != 1 {
		t.Fatalf("after the prune: %d events (%d recent), %d output rows", after["weft_events"], recent, after["weft_output"])
	}
	if gone, _ := s.WeftAttemptOutput(ended.ID, attempt); len(gone) != 0 {
		t.Fatalf("the ended run kept output %v", gone)
	}
	if chunk, cursor, err := s.WeftOutputHighWater(ended.ID, attempt); err != nil || chunk != 1 || cursor != 1 {
		t.Fatalf("high-water after the prune = chunk %d cursor %d %v", chunk, cursor, err)
	}
	// Its cursor is never reissued: the next chunk continues past it.
	if oldest, newest, err := s.WeftOutputBounds(ended.ID); err != nil || oldest != 2 || newest != 1 {
		t.Fatalf("pruned bounds = %d..%d %v", oldest, newest, err)
	}
	run, err := s.GetWeftRun(ended.ID)
	if err != nil || run.Outcome.String != "finished" || len(run.Stages) != 1 || len(run.Segments) != 2 {
		t.Fatalf("ended run after the prune = %+v %v", run, err)
	}
}

func TestWeftAllowanceNewerWins(t *testing.T) {
	s := newStore(t)
	windows := json.RawMessage(`[{"id":"five_hour","utilization":0.38,"resets_at":"2026-10-02T13:00:00.000Z"}]`)
	record := func(at string, want bool) {
		t.Helper()
		got, err := s.RecordWeftAllowance(WeftAllowance{Provider: "claude", Source: "claude_stream", ObservedAt: at, Windows: windows})
		if err != nil || got != want {
			t.Fatalf("record %s = %v %v, want %v", at, got, err, want)
		}
	}
	record("2026-10-02T09:16:40.003Z", true)
	record("2026-10-02T09:16:40.003Z", false)
	record("2026-10-02T09:00:00Z", false)
	record("2026-10-02T09:58:41.306Z", true)
	held, err := s.WeftAllowances()
	if err != nil || held["claude"].ObservedAt != "2026-10-02T09:58:41.306Z" || len(held) != 1 {
		t.Fatalf("held = %+v %v", held, err)
	}
	events, err := s.WeftEventsAfter(0)
	if err != nil || len(events) != 2 {
		t.Fatalf("allowance events = %d %v, want one per newer observation", len(events), err)
	}
	var payload map[string]any
	if err := json.Unmarshal(events[1].Payload, &payload); err != nil {
		t.Fatal(err)
	}
	tile := payload["tile"].(map[string]any)
	if events[1].Type != "allowance" || tile["state"] != "observed" || tile["observed_at"] != "2026-10-02T09:58:41.306Z" ||
		tile["reason"] != nil || len(tile["windows"].([]any)) != 1 {
		t.Fatalf("allowance event = %s %s", events[1].Type, events[1].Payload)
	}
}

// What history reads — stage attempt identity and metrics, the contract each
// segment ran under, settlement and provenance, liveness and retained output
// — survives a reopen of the ledger, as across a daemon restart.
func TestWeftRunVisibilitySurvivesReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "runs.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	s.now = func() time.Time { return testClock }
	run := claimVisible(t, s, "demo/reopen-0001")
	if err := s.MarkWeftRunning(run.ID, 1, 4242, "weft-worker/v2"); err != nil {
		t.Fatal(err)
	}
	if err := s.RecordWeftHello(run.ID, 1, "20260902T043012Z-abcd1234", "base", "criteria"); err != nil {
		t.Fatal(err)
	}
	stage := WeftStage{RunID: run.ID, Segment: 1, Stage: "work", Occurrence: 2, Attempt: 3,
		Backend: sql.NullString{String: "claude", Valid: true}, State: "done", At: "2026-10-02T10:03:11.452Z",
		Turns: sql.NullInt64{Int64: 52, Valid: true}, Processing: sql.NullInt64{Int64: 2911004, Valid: true},
		Spend:  json.RawMessage(`{"usd":0,"priced":0,"unpriced":1}`),
		Tokens: json.RawMessage(`{"input":3920,"output":27711,"cache_read":2210934,"cache_write":140215,"reported":52,"unreported":0}`)}
	if err := s.RecordWeftStage(stage); err != nil {
		t.Fatal(err)
	}
	attempt := WeftAttempt{Segment: 1, Stage: "work", Occurrence: 2, Attempt: 3}
	if err := s.RecordWeftLiveness(run.ID, WeftLiveness{WeftAttempt: attempt, Stalled: true,
		LastEventAt: "2026-10-02T09:31:02.551Z", ObservedAt: "2026-10-02T09:36:02.600Z"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RecordWeftOutput(WeftOutputChunk{RunID: run.ID, WeftAttempt: attempt, Chunk: 1, At: "2026-10-02T09:15:09.402Z", Text: "Reading.\n"}); err != nil {
		t.Fatal(err)
	}
	before, err := s.GetWeftRun(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	s.Close()
	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	after, err := s.GetWeftRun(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("run after reopen = %+v\nbefore = %+v", after, before)
	}
	if got := after.Stages[0]; got.Occurrence != 2 || got.Attempt != 3 || got.Turns.Int64 != 52 || got.Processing.Int64 != 2911004 ||
		string(got.Tokens) != string(stage.Tokens) || string(got.Spend) != string(stage.Spend) {
		t.Fatalf("stage after reopen = %+v", got)
	}
	if after.Segments[0].WorkerContract.String != "weft-worker/v2" || after.SettlementState != WeftSettlementNone ||
		after.Origin != WeftOriginHosted || after.RecordedUnder != WeftRecordedV1 || after.ArchiveID.Valid ||
		!strings.Contains(string(after.Liveness), `"stalled":true`) {
		t.Fatalf("run after reopen = %+v", after)
	}
	cursor, err := s.RecordWeftOutput(WeftOutputChunk{RunID: run.ID, WeftAttempt: attempt, Chunk: 2, At: "2026-10-02T09:16:00.000Z", Text: "More.\n"})
	if err != nil || cursor != 2 {
		t.Fatalf("cursor after reopen = %d %v, want 2", cursor, err)
	}
}

// A v1 ledger migrated forward keeps its stage rows' attempt identity and
// metrics, reads its segments as weft-remote/v1's, and has no liveness or
// output to invent.
func TestSchemaTwoMigrationKeepsStageAttemptsAndProvenance(t *testing.T) {
	path := filepath.Join(t.TempDir(), "runs.db")
	schemaTwo(t, path, "")
	db, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO weft_stages(run_id,segment,stage,occurrence,attempt,backend,state,turns,spend,processing_ms,tokens,at) VALUES
		('r-20260913T142233Z-6b2f0c1a',1,'work',1,1,'claude','needs_input',9,'{"usd":0,"priced":0,"unpriced":1}',61000,'{"input":10,"output":20,"cache_read":0,"cache_write":0,"reported":9,"unreported":0}','2026-09-13T14:59:00.000Z'),
		('r-20260913T142233Z-6b2f0c1a',2,'review',2,3,'claude','done',4,NULL,12000,NULL,'2026-09-13T16:40:00.000Z')`); err != nil {
		t.Fatal(err)
	}
	db.Close()
	for range 2 {
		s, err := Open(path)
		if err != nil {
			t.Fatal(err)
		}
		run, err := s.GetWeftRun("r-20260913T142233Z-6b2f0c1a")
		if err != nil {
			t.Fatal(err)
		}
		if len(run.Stages) != 2 || run.Stages[1].Segment != 2 || run.Stages[1].Occurrence != 2 || run.Stages[1].Attempt != 3 ||
			run.Stages[0].Turns.Int64 != 9 || run.Stages[0].Processing.Int64 != 61000 || !strings.Contains(string(run.Stages[0].Tokens), `"reported":9`) {
			t.Fatalf("migrated stages = %+v", run.Stages)
		}
		for _, seg := range run.Segments {
			if seg.WorkerContract.Valid {
				t.Fatalf("migrated segment %d records contract %q", seg.Segment, seg.WorkerContract.String)
			}
		}
		if run.Liveness != nil || run.RecordedUnder != WeftRecordedV1 || run.Origin != WeftOriginHosted ||
			run.SettlementState != WeftSettlementNone || run.ArchiveID.Valid || run.Outcome.String != "finished" {
			t.Fatalf("migrated run = %+v", run)
		}
		if oldest, newest, err := s.WeftOutputBounds(run.ID); err != nil || oldest != 1 || newest != 0 {
			t.Fatalf("migrated output bounds = %d..%d %v", oldest, newest, err)
		}
		s.Close()
	}
}

// A ledger stamped 3 by the build before run visibility gains its tables and
// columns, and every later schema's, and its runs read.
func TestSchemaThreeMigratesToFour(t *testing.T) {
	path := filepath.Join(t.TempDir(), "runs.db")
	schemaTwo(t, path, "")
	db, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if err := migrateWeftV2(tx, testClock); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`UPDATE schema_meta SET value='3' WHERE key='schema_version'`); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	var columns int
	if err := db.QueryRow(`SELECT count(*) FROM pragma_table_info('weft_runs') WHERE name IN ('liveness','output_cursor')`).Scan(&columns); err != nil || columns != 0 {
		t.Fatalf("schema-3 fixture already has %d visibility columns, %v", columns, err)
	}
	db.Close()
	s, err := Open(path)
	if err != nil {
		t.Fatalf("migrate 3 to 4: %v", err)
	}
	defer s.Close()
	var version string
	if err := s.db.QueryRow(`SELECT value FROM schema_meta WHERE key='schema_version'`).Scan(&version); err != nil || version != "6" {
		t.Fatalf("version = %q %v", version, err)
	}
	run, err := s.GetWeftRun("r-20260913T142233Z-6b2f0c1a")
	if err != nil || run.Liveness != nil || len(run.Segments) != 2 || run.Segments[0].WorkerContract.Valid ||
		run.RecordedUnder != WeftRecordedV1 {
		t.Fatalf("run after 3 to 4 = %+v %v", run, err)
	}
	attempt := WeftAttempt{Segment: 2, Stage: "work", Occurrence: 1, Attempt: 1}
	if cursor, err := s.RecordWeftOutput(WeftOutputChunk{RunID: run.ID, WeftAttempt: attempt, Chunk: 1, At: "2026-10-02T09:00:00.000Z", Text: "x"}); err != nil || cursor != 1 {
		t.Fatalf("output after 3 to 4 = %d %v", cursor, err)
	}
	if _, err := s.WeftAllowances(); err != nil {
		t.Fatal(err)
	}
}

// Chunks with no text cost rows the byte cap does not see: the row cap keeps
// the newest WeftOutputRetainChunks, contiguous, under the same high-water.
func TestWeftOutputBoundsEmptyChunks(t *testing.T) {
	s := newStore(t)
	run := claimVisible(t, s, "demo/empty-0001")
	attempt := WeftAttempt{Segment: 1, Stage: "work", Occurrence: 1, Attempt: 1}
	total := WeftOutputRetainChunks + 300
	for i := 1; i <= total; i++ {
		if _, err := s.RecordWeftOutput(WeftOutputChunk{RunID: run.ID, WeftAttempt: attempt, Chunk: i, At: "2026-10-02T10:00:00.000Z"}); err != nil {
			t.Fatal(err)
		}
	}
	kept, err := s.WeftAttemptOutput(run.ID, attempt)
	if err != nil {
		t.Fatal(err)
	}
	if len(kept) != WeftOutputRetainChunks || kept[0].Chunk != 301 || kept[len(kept)-1].Chunk != total {
		t.Fatalf("retained %d chunks, %d..%d", len(kept), kept[0].Chunk, kept[len(kept)-1].Chunk)
	}
	for i, c := range kept {
		if c.Chunk != 301+i || c.Cursor != int64(301+i) {
			t.Fatalf("retained chunk %d at %d: chunk %d cursor %d", i, i, c.Chunk, c.Cursor)
		}
	}
	if chunk, cursor, err := s.WeftOutputHighWater(run.ID, attempt); err != nil || chunk != total || cursor != int64(total) {
		t.Fatalf("high-water = %d %d %v", chunk, cursor, err)
	}
	if oldest, newest, err := s.WeftOutputBounds(run.ID); err != nil || oldest != 301 || newest != int64(total) {
		t.Fatalf("bounds = %d..%d %v", oldest, newest, err)
	}
}
