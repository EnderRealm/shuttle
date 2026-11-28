package cmd

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"shuttle/internal/config"
	"shuttle/internal/failure"
	"shuttle/internal/launchd"
	"shuttle/internal/ledger"
	"shuttle/internal/sandbox"
	"shuttle/internal/schedule"
)

// workerVersionDoc is a weft-worker/v2 version document; its worker_version
// carries an escape sequence the row must not pass to the terminal.
const workerVersionDoc = `{"contract_version":"weft-worker/v2","event":"version",` +
	`"worker_version":"weft-worker 0.2.0 (weft@237566d)\u001b[2J",` +
	`"contracts":["weft-remote/v1","weft-worker/v2"],"capabilities":["segment","land"]}`

// fakeWorker writes a worker_bin whose body answers `version --json`.
func fakeWorker(t *testing.T, body string) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "weft-worker")
	if err := os.WriteFile(bin, []byte("#!/bin/sh\n"+body+"\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	return bin
}

func weftConfig(t *testing.T, bin string) {
	t.Helper()
	writeConfig(t, fmt.Sprintf(`{"weft": {"listen": "100.64.0.1:7420", "worker_bin": %q}}`, bin))
}

// weftStatusLine runs `shuttle status` and returns its weft-worker row.
func weftStatusLine(t *testing.T) string {
	t.Helper()
	out, err := execStdout(t, "status")
	if err != nil {
		t.Fatalf("status: %v\n%s", err, out)
	}
	for _, line := range strings.Split(out, "\n") {
		if strings.HasPrefix(line, ledger.WeftAgent+"\t") {
			return line
		}
	}
	t.Fatalf("status printed no %s row:\n%s", ledger.WeftAgent, out)
	return ""
}

// ledgerDB opens the ledger file directly, for the rows no store method
// writes outside a dispatch pass.
func ledgerDB(t *testing.T) *sql.DB {
	t.Helper()
	path, err := config.LedgerPath()
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func TestStatusWeftWorkerPause(t *testing.T) {
	agents := shuttleEnv(t)
	writeAgentDir(t, agents, "daily", dailyManifest)
	weftConfig(t, fakeWorker(t, `echo '`+workerVersionDoc+`'`))
	store, err := openLedger()
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	if line := weftStatusLine(t); !strings.Contains(line, "\tactive\t") {
		t.Fatalf("before pause: %q, want active", line)
	}
	if _, err := execStdout(t, "pause", ledger.WeftAgent); err != nil {
		t.Fatalf("pause: %v", err)
	}
	agent, err := store.Agent(ledger.WeftAgent)
	if err != nil || !agent.Paused || !agent.PausedAt.Valid {
		t.Fatalf("agent after pause = %+v, %v", agent, err)
	}
	want := "\tpaused (" + ledger.PauseManual + ") since " + agent.PausedAt.String + "\t"
	if line := weftStatusLine(t); !strings.Contains(line, want) {
		t.Fatalf("after pause: %q, want %q", line, want)
	}
	if _, err := execStdout(t, "resume", ledger.WeftAgent); err != nil {
		t.Fatalf("resume: %v", err)
	}
	if line := weftStatusLine(t); !strings.Contains(line, "\tactive\t") {
		t.Fatalf("after resume: %q, want active", line)
	}
}

func TestStatusWeftWorkerCounts(t *testing.T) {
	shuttleEnv(t)
	weftConfig(t, fakeWorker(t, `echo '`+workerVersionDoc+`'`))
	store, err := openLedger()
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	root := t.TempDir()
	// One claimed launch segment holds a slot.
	if _, err := store.ClaimWeft(ledger.WeftLaunch{
		Ticket: "demo/running-0001", Project: "demo", Repo: filepath.Join(root, "demo"), Branch: "running",
		Worktree: filepath.Join(root, "worktree"), RunDirRoot: filepath.Join(root, "runs"),
	}); err != nil {
		t.Fatal(err)
	}
	db := ledgerDB(t)
	if _, err := db.Exec(`INSERT INTO weft_settings(key,value) VALUES('capacity','3')`); err != nil {
		t.Fatal(err)
	}
	repo := filepath.Join(root, "other")
	for i, ticket := range []string{"other/waiting-0002", "other/waiting-0003"} {
		if _, err := db.Exec(`INSERT INTO weft_entries(id,kind,ticket,project,repository_id,repository,position,state,
			waiting_reason,token_id,request_key,created_at,updated_at,revision)
			VALUES(?,'launch',?,'other',?,?,?,'waiting','capacity','token',?,
			'2026-10-25T09:00:00.000Z','2026-10-25T09:00:00.000Z',1)`,
			fmt.Sprintf("e-%d", i), ticket, sandbox.RepositoryID(repo), repo, i+1, ticket); err != nil {
			t.Fatal(err)
		}
	}
	counts, err := store.WeftDashboardCounts()
	if err != nil {
		t.Fatal(err)
	}
	if counts.Running != 1 || counts.Waiting != 2 || counts.Capacity != 3 {
		t.Fatalf("seeded counts = %+v, want 1 running and 2 waiting against 3", counts)
	}
	want := fmt.Sprintf("\trunning %d waiting %d cap %d\t", counts.Running, counts.Waiting, counts.Capacity)
	if line := weftStatusLine(t); !strings.Contains(line, want) {
		t.Fatalf("status: %q, want the dashboard's %q", line, want)
	}
}

func TestStatusWeftWorkerProbe(t *testing.T) {
	shuttleEnv(t)
	weftConfig(t, fakeWorker(t, `case "$1" in version) echo '`+workerVersionDoc+`'; exit 0 ;; esac; exit 64`))
	want := `worker (probed now) weft-worker 0.2.0 (weft@237566d)\x1b[2J contracts [weft-remote/v1 weft-worker/v2] capabilities [segment land]`
	if line := weftStatusLine(t); !strings.HasSuffix(line, "\t"+want) {
		t.Fatalf("v2 worker: %q, want suffix %q", line, want)
	}

	// The pre-v2 worker: an unknown subcommand exits 64. That is the worker's
	// state, reported in the row, not a failure of status.
	old := fakeWorker(t, `exit 64`)
	weftConfig(t, old)
	want = "worker (probed now) " + old + ": exited 64"
	if line := weftStatusLine(t); !strings.HasSuffix(line, "\t"+want) {
		t.Fatalf("pre-v2 worker: %q, want suffix %q", line, want)
	}

	missing := filepath.Join(t.TempDir(), "absent")
	weftConfig(t, missing)
	if line := weftStatusLine(t); !strings.Contains(line, "worker (probed now) "+missing+": could not start") {
		t.Fatalf("missing worker: %q, want the path and the start failure", line)
	}
}

func TestStatusWithoutWeft(t *testing.T) {
	agents := shuttleEnv(t)
	writeAgentDir(t, agents, "daily", dailyManifest)
	// The rows status printed before the weft row existed: the daemon, one per
	// definition, then the spend and the incidents.
	assertRows := func(out string) {
		t.Helper()
		lines := strings.Split(strings.TrimSuffix(out, "\n"), "\n")
		if len(lines) != 4 || lines[0] != "daemon\tnot running" || !strings.HasPrefix(lines[1], "daily\tnext ") ||
			!strings.HasPrefix(lines[2], "today's spend\t") || lines[3] != "open incidents\t0" {
			t.Fatalf("status printed:\n%s\nwant the daemon and daily rows, the spend and the incidents only", out)
		}
	}
	for _, body := range []string{"", `{"weft": {"worker_bin": "/nonexistent/weft-worker"}}`} {
		if body != "" {
			writeConfig(t, body)
		}
		out, err := execStdout(t, "status")
		if err != nil {
			t.Fatalf("status with config %q: %v", body, err)
		}
		assertRows(out)
	}

	// A weft row that cannot be read is reported and fails the command, and
	// the scheduled agents' rows still print.
	weftConfig(t, fakeWorker(t, `echo '`+workerVersionDoc+`'`))
	if _, err := ledgerDB(t).Exec(`INSERT INTO weft_settings(key,value) VALUES('capacity','three')`); err != nil {
		t.Fatal(err)
	}
	out, err := execStdout(t, "status")
	if err == nil {
		t.Fatalf("status with an unreadable weft row succeeded:\n%s", out)
	}
	assertRows(out)
}

func TestStatusReportsADaemonThatCannotLoadItsConfig(t *testing.T) {
	agents := shuttleEnv(t)
	writeAgentDir(t, agents, "daily", dailyManifest)
	writeConfig(t, `{"weft": {"allow_write": ["/Users/Shared/weft"]}}`)
	const remedy = "weft.allow_write: Shuttle no longer reads this key; delete it"

	out, err := execStdout(t, "status")
	if err == nil || !strings.Contains(err.Error(), remedy) {
		t.Fatalf("status error = %v, want the loader's naming %q", err, remedy)
	}
	if out != "daemon\tnot running\n" {
		t.Fatalf("status printed:\n%s\nwant the daemon not running", out)
	}

	// The daemon's own start on that config records the failure, and the
	// daemon row carries it.
	daemonCmd.SetErr(&bytes.Buffer{})
	t.Cleanup(func() { daemonCmd.SetErr(nil) })
	if err := daemonCmd.RunE(daemonCmd, nil); err == nil {
		t.Fatal("daemon started on config.json it should refuse")
	}
	out, err = execStdout(t, "status")
	if err == nil || !strings.Contains(err.Error(), remedy) {
		t.Fatalf("status error = %v, want the loader's naming %q", err, remedy)
	}
	if !strings.HasPrefix(out, "daemon\tnot running\tstart failed: ") || !strings.Contains(out, remedy) ||
		strings.Count(out, "\n") != 1 {
		t.Fatalf("status printed:\n%s\nwant the daemon not running on the recorded failure", out)
	}

	// A daemon still running on the config it loaded before the edit is
	// reported as it is, with the error its next start will hit.
	statusProcess = func(string) (launchd.Process, bool, error) { return launchd.Process{PID: 42}, true, nil }
	out, err = execStdout(t, "status")
	if err == nil || !strings.HasPrefix(out, "daemon\trunning (pid 42)\t") {
		t.Fatalf("status = (%q, %v), want the running daemon and the loader's error", out, err)
	}
}

// statusDoc is the object `shuttle status --json` prints, as a consumer reads
// it: a pointer is nil where the object holds null.
type statusDoc struct {
	Daemon struct {
		Running           *bool
		PID               *int
		Error             *string
		StartFailure      *string `json:"start_failure"`
		StartFailureError *string `json:"start_failure_error"`
	}
	Agents      []statusDocAgent
	AgentsError *string `json:"agents_error"`
	WeftWorker  struct {
		Hosted      *bool
		HostedError *string `json:"hosted_error"`
		statusDocRow
		Counts *struct {
			Running, Waiting, Capacity int
		}
		CountsError *string `json:"counts_error"`
		Probe       *struct {
			WorkerBin    string `json:"worker_bin"`
			OK           bool
			Failure      *string
			Version      *string
			Contracts    []string
			Capabilities []string
		}
	} `json:"weft_worker"`
	SpendTodayUSD   *float64 `json:"spend_today_usd"`
	SpendTodayError *string  `json:"spend_today_error"`
	OpenIncidents   []struct {
		ID        int64
		Agent     string
		Signature string
		FirstSeen string `json:"first_seen"`
		LastSeen  string `json:"last_seen"`
		Count     int
	} `json:"open_incidents"`
	OpenIncidentsError *string `json:"open_incidents_error"`
}

type statusDocRow struct {
	Paused        *bool
	PausedReason  *string `json:"paused_reason"`
	PausedAt      *string `json:"paused_at"`
	FailureStreak *int    `json:"failure_streak"`
	RowError      *string `json:"row_error"`
}

type statusDocAgent struct {
	Name string
	statusDocRow
	NextSlot      *time.Time `json:"next_slot"`
	FiresAt       *time.Time `json:"fires_at"`
	NextSlotError *string    `json:"next_slot_error"`
	LastRun       *struct {
		Status       string
		SkipReason   *string  `json:"skip_reason"`
		ScheduledFor string   `json:"scheduled_for"`
		CostUSD      *float64 `json:"cost_usd"`
	} `json:"last_run"`
	LastRunError *string `json:"last_run_error"`
}

// agent returns the object's entry for name.
func (d statusDoc) agent(t *testing.T, name string) statusDocAgent {
	t.Helper()
	for _, a := range d.Agents {
		if a.Name == name {
			return a
		}
	}
	t.Fatalf("agents %+v name no %s", d.Agents, name)
	return statusDocAgent{}
}

// execStatusJSON runs `shuttle status --json`, requires stdout to be exactly
// one JSON object, and returns it with the command's error.
func execStatusJSON(t *testing.T) (statusDoc, error) {
	t.Helper()
	// pflag keeps a parsed value across in-process executions.
	t.Cleanup(func() { statusJSON = false })
	out, runErr := execStdout(t, "status", "--json")
	statusJSON = false
	var doc statusDoc
	if err := json.Unmarshal([]byte(out), &doc); err != nil || !strings.HasPrefix(out, "{") {
		t.Fatalf("status --json printed (%v):\n%s\nwant one JSON object", err, out)
	}
	return doc, runErr
}

// saysWhy reports whether an error field is set and names want.
func saysWhy(field *string, want string) bool {
	return field != nil && *field != "" && strings.Contains(*field, want)
}

func TestStatusJSON(t *testing.T) {
	agents := shuttleEnv(t)
	writeAgentDir(t, agents, "daily", dailyManifest)
	writeAgentDir(t, agents, "fresh", strings.Replace(dailyManifest, `"daily"`, `"fresh"`, 1))
	writeAgentDir(t, agents, "held", strings.Replace(dailyManifest, `"daily"`, `"held"`, 1))

	// Nothing has run and no weft is hosted: empty readings, none of them
	// failed, and an unpriced day that is not a free one.
	doc, err := execStatusJSON(t)
	if err != nil {
		t.Fatalf("status --json: %v", err)
	}
	if d := doc.Daemon; d.Running == nil || *d.Running || d.PID != nil || d.Error != nil ||
		d.StartFailure != nil || d.StartFailureError != nil {
		t.Fatalf("daemon = %+v, want not running", d)
	}
	if w := doc.WeftWorker; w.Hosted == nil || *w.Hosted || w.HostedError != nil || w.Paused != nil || w.Probe != nil {
		t.Fatalf("weft_worker = %+v, want not hosted", w)
	}
	if doc.OpenIncidents == nil || len(doc.OpenIncidents) != 0 || doc.OpenIncidentsError != nil {
		t.Fatalf("open_incidents = %v (%v), want a list read as empty", doc.OpenIncidents, doc.OpenIncidentsError)
	}
	if doc.SpendTodayUSD != nil || doc.SpendTodayError != nil {
		t.Fatalf("spend = %v (%v), want unpriced", doc.SpendTodayUSD, doc.SpendTodayError)
	}

	store, err := openLedger()
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	run, ok, err := store.Claim("daily", time.Now().Add(-time.Hour), ledger.TriggerCron)
	if err != nil || !ok {
		t.Fatalf("claim: %v", err)
	}
	if err := store.Fail(run.ID, failure.Died, "exit 1", ledger.Result{}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Skip("held", time.Now(), ledger.TriggerCron, ledger.SkipPaused); err != nil {
		t.Fatal(err)
	}
	if err := store.Pause("daily"); err != nil {
		t.Fatal(err)
	}
	if err := store.Pause(ledger.WeftAgent); err != nil {
		t.Fatal(err)
	}
	const signature = "died: \x1b[2Jraw"
	if _, err := ledgerDB(t).Exec(`INSERT INTO incidents(agent,signature,first_seen,last_seen,count)
		VALUES('fresh',?,'2026-10-25T09:00:00Z','2026-10-25T09:00:00Z',2)`, signature); err != nil {
		t.Fatal(err)
	}
	incidents, err := store.OpenIncidents()
	if err != nil || len(incidents) != 2 {
		t.Fatalf("seeded incidents = %v, %v, want 2", incidents, err)
	}
	bin := fakeWorker(t, `echo '`+workerVersionDoc+`'`)
	weftConfig(t, bin)
	statusProcess = func(string) (launchd.Process, bool, error) { return launchd.Process{PID: 42}, true, nil }
	const startText = "config.json:\trefused"
	path, err := startFailurePath()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(startText), 0o600); err != nil {
		t.Fatal(err)
	}

	doc, err = execStatusJSON(t)
	if err != nil {
		t.Fatalf("status --json: %v", err)
	}
	if d := doc.Daemon; d.Running == nil || !*d.Running || d.PID == nil || *d.PID != 42 ||
		d.StartFailure == nil || *d.StartFailure != startText {
		t.Fatalf("daemon = %+v, want pid 42 and the recorded start failure", d)
	}
	if len(doc.Agents) != 3 || doc.AgentsError != nil {
		t.Fatalf("agents = %+v (%v), want daily, fresh and held", doc.Agents, doc.AgentsError)
	}
	daily := doc.agent(t, "daily")
	if daily.Paused == nil || !*daily.Paused || daily.PausedReason == nil || *daily.PausedReason != ledger.PauseManual ||
		daily.FailureStreak == nil || *daily.FailureStreak != 1 || daily.RowError != nil {
		t.Fatalf("daily = %+v, want paused manually on a streak of 1", daily)
	}
	if daily.NextSlot == nil || !daily.NextSlot.After(time.Now()) || daily.FiresAt == nil ||
		!daily.FiresAt.Equal(daily.NextSlot.Add(schedule.Jitter("daily"))) {
		t.Fatalf("daily next = %v fires %v, want the slot and its jittered minute", daily.NextSlot, daily.FiresAt)
	}
	if last := daily.LastRun; last == nil || last.Status != ledger.StatusFailed || last.ScheduledFor != run.ScheduledFor ||
		last.SkipReason != nil || last.CostUSD != nil {
		t.Fatalf("daily last_run = %+v, want the failed unpriced run", last)
	}
	fresh := doc.agent(t, "fresh")
	if fresh.Paused == nil || *fresh.Paused || fresh.PausedReason != nil || fresh.LastRun != nil || fresh.LastRunError != nil {
		t.Fatalf("fresh = %+v, want active and never run", fresh)
	}
	if last := doc.agent(t, "held").LastRun; last == nil || last.Status != ledger.StatusSkipped ||
		last.SkipReason == nil || *last.SkipReason != ledger.SkipPaused {
		t.Fatalf("held last_run = %+v, want the paused skip", last)
	}
	if len(doc.OpenIncidents) != len(incidents) {
		t.Fatalf("open_incidents = %+v, want the ledger's %d", doc.OpenIncidents, len(incidents))
	}
	for i, want := range incidents {
		if got := doc.OpenIncidents[i]; got.ID != want.ID || got.Agent != want.Agent || got.Signature != want.Signature {
			t.Fatalf("open_incidents[%d] = %+v, want %+v", i, got, want)
		}
	}
	w := doc.WeftWorker
	weftAgent, err := store.Agent(ledger.WeftAgent)
	if err != nil {
		t.Fatal(err)
	}
	if w.Hosted == nil || !*w.Hosted || w.Paused == nil || !*w.Paused || w.PausedReason == nil ||
		*w.PausedReason != ledger.PauseManual || w.PausedAt == nil || *w.PausedAt != weftAgent.PausedAt.String {
		t.Fatalf("weft_worker = %+v, want hosted and paused since %s", w, weftAgent.PausedAt.String)
	}
	counts, err := store.WeftDashboardCounts()
	if err != nil {
		t.Fatal(err)
	}
	if w.Counts == nil || w.Counts.Running != counts.Running || w.Counts.Waiting != counts.Waiting ||
		w.Counts.Capacity != counts.Capacity {
		t.Fatalf("weft_worker counts = %+v, want the dashboard's %+v", w.Counts, counts)
	}
	if p := w.Probe; p == nil || !p.OK || p.Failure != nil || p.WorkerBin != bin || p.Version == nil ||
		*p.Version != "weft-worker 0.2.0 (weft@237566d)\x1b[2J" ||
		strings.Join(p.Contracts, " ") != "weft-remote/v1 weft-worker/v2" ||
		strings.Join(p.Capabilities, " ") != "segment land" {
		t.Fatalf("weft_worker probe = %+v, want the worker's document, raw", p)
	}

	// A priced run is the day's spend and its own cost.
	run, ok, err = store.Claim("fresh", time.Now().Add(-time.Hour), ledger.TriggerCron)
	if err != nil || !ok {
		t.Fatalf("claim: %v", err)
	}
	cost := sql.NullFloat64{Float64: 0.25, Valid: true}
	if err := store.Complete(run.ID, ledger.Result{CostUSD: cost}); err != nil {
		t.Fatal(err)
	}
	doc, err = execStatusJSON(t)
	if err != nil {
		t.Fatalf("status --json: %v", err)
	}
	if doc.SpendTodayUSD == nil || *doc.SpendTodayUSD != cost.Float64 {
		t.Fatalf("spend = %v, want %v", doc.SpendTodayUSD, cost.Float64)
	}
	if last := doc.agent(t, "fresh").LastRun; last == nil || last.CostUSD == nil || *last.CostUSD != cost.Float64 {
		t.Fatalf("fresh last_run = %+v, want its cost", last)
	}
}

func TestStatusJSONIncidentDates(t *testing.T) {
	agents := shuttleEnv(t)
	writeAgentDir(t, agents, "daily", dailyManifest)
	store, err := openLedger()
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	// One incident through the ledger's own write, one seeded old and repeated,
	// and an acknowledged one that is not open.
	run, ok, err := store.Claim("daily", time.Now().Add(-time.Hour), ledger.TriggerCron)
	if err != nil || !ok {
		t.Fatalf("claim: %v", err)
	}
	if err := store.Fail(run.ID, failure.Died, "exit 1", ledger.Result{}); err != nil {
		t.Fatal(err)
	}
	if _, err := ledgerDB(t).Exec(`INSERT INTO incidents(agent,signature,first_seen,last_seen,count,acknowledged_at) VALUES
		('daily','stale','2026-09-22T08:15:00Z','2026-11-01T04:30:00-07:00',4,NULL),
		('daily','acked','2026-09-01T00:00:00Z','2026-09-02T00:00:00Z',9,'2026-09-03T00:00:00Z')`); err != nil {
		t.Fatal(err)
	}
	incidents, err := store.OpenIncidents()
	if err != nil || len(incidents) != 2 {
		t.Fatalf("seeded incidents = %v, %v, want 2 open", incidents, err)
	}

	doc, err := execStatusJSON(t)
	if err != nil {
		t.Fatalf("status --json: %v", err)
	}
	if len(doc.OpenIncidents) != len(incidents) || doc.OpenIncidentsError != nil {
		t.Fatalf("open_incidents = %+v (%v), want the ledger's %d", doc.OpenIncidents, doc.OpenIncidentsError, len(incidents))
	}
	for i, want := range incidents {
		got := doc.OpenIncidents[i]
		if got.ID != want.ID || got.Count != want.Count {
			t.Fatalf("open_incidents[%d] = %+v, want %+v", i, got, want)
		}
		for _, seen := range []struct{ name, got, want string }{
			{"first_seen", got.FirstSeen, want.FirstSeen},
			{"last_seen", got.LastSeen, want.LastSeen},
		} {
			at, err := time.Parse(time.RFC3339, seen.got)
			if err != nil {
				t.Fatalf("open_incidents[%d] %s = %q, want RFC 3339: %v", i, seen.name, seen.got, err)
			}
			if w, _ := time.Parse(time.RFC3339, seen.want); !at.Equal(w) {
				t.Fatalf("open_incidents[%d] %s = %s, want the ledger's %s", i, seen.name, seen.got, seen.want)
			}
		}
	}
	if stale := doc.OpenIncidents[1]; stale.Signature != "stale" || stale.Count != 4 ||
		stale.FirstSeen == stale.LastSeen {
		t.Fatalf("open_incidents[1] = %+v, want the stale incident, seen four times across two dates", stale)
	}
	if fresh := doc.OpenIncidents[0]; fresh.Count != 1 || fresh.FirstSeen != fresh.LastSeen {
		t.Fatalf("open_incidents[0] = %+v, want the run's incident, seen once", fresh)
	}
}

func TestStatusJSONUnreadable(t *testing.T) {
	agents := shuttleEnv(t)
	writeAgentDir(t, agents, "daily", dailyManifest)
	writeAgentDir(t, agents, "fresh", strings.Replace(dailyManifest, `"daily"`, `"fresh"`, 1))
	worker := fakeWorker(t, `echo '`+workerVersionDoc+`'`)
	weftConfig(t, worker)
	store, err := openLedger()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Pause(ledger.WeftAgent); err != nil {
		t.Fatal(err)
	}
	store.Close()
	healthy := func() {
		t.Helper()
		doc, err := execStatusJSON(t)
		if err != nil {
			t.Fatalf("status --json: %v\n%+v", err, doc)
		}
	}
	healthy()
	// Each reading is broken alone and restored.
	db := ledgerDB(t)
	damage := func(query string) {
		t.Helper()
		if _, err := db.Exec(query); err != nil {
			t.Fatal(err)
		}
	}

	// A daemon process launchd will not answer for.
	processless := statusProcess
	statusProcess = func(string) (launchd.Process, bool, error) {
		return launchd.Process{}, false, errors.New("launchctl refused")
	}
	doc, err := execStatusJSON(t)
	if err == nil || doc.Daemon.Running != nil || !saysWhy(doc.Daemon.Error, "launchctl refused") {
		t.Fatalf("unreadable daemon = (%+v, %v), want running null and why", doc.Daemon, err)
	}
	if len(doc.Agents) != 2 || doc.OpenIncidents == nil {
		t.Fatalf("an unreadable daemon hid the rest: %+v", doc)
	}
	statusProcess = processless
	healthy()

	// A start-failure record that stands but cannot be read.
	path, err := startFailurePath()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(path, 0o700); err != nil {
		t.Fatal(err)
	}
	doc, _ = execStatusJSON(t)
	if doc.Daemon.StartFailure != nil || !saysWhy(doc.Daemon.StartFailureError, path) {
		t.Fatalf("unreadable start failure = %+v, want null and why", doc.Daemon)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	healthy()

	// A failing worker probe is the worker's state: carried, and no failure of
	// the command.
	old := fakeWorker(t, `exit 64`)
	weftConfig(t, old)
	doc, err = execStatusJSON(t)
	if err != nil {
		t.Fatalf("status --json with a failing probe: %v", err)
	}
	if p := doc.WeftWorker.Probe; p == nil || p.OK || !saysWhy(p.Failure, "exited 64") || p.WorkerBin != old || p.Version != nil {
		t.Fatalf("probe = %+v, want the failure", p)
	}
	weftConfig(t, worker)
	healthy()

	// A weft queue that cannot be counted leaves the pause and the probe read.
	damage(`INSERT INTO weft_settings(key,value) VALUES('capacity','three')`)
	doc, err = execStatusJSON(t)
	if w := doc.WeftWorker; err == nil || w.Counts != nil || !saysWhy(w.CountsError, "three") ||
		w.Paused == nil || !*w.Paused || w.Probe == nil || !w.Probe.OK {
		t.Fatalf("uncounted weft = (%+v, %v), want counts null and why", w, err)
	}
	damage(`DELETE FROM weft_settings WHERE key='capacity'`)
	healthy()

	// A weft-worker row that cannot be read is not an unpaused worker.
	damage(`UPDATE agents SET failure_streak='many' WHERE name='` + ledger.WeftAgent + `'`)
	doc, err = execStatusJSON(t)
	if w := doc.WeftWorker; err == nil || w.Hosted == nil || !*w.Hosted || w.Paused != nil || !saysWhy(w.RowError, "many") {
		t.Fatalf("unreadable weft row = (%+v, %v), want paused null and why", w, err)
	}
	damage(`UPDATE agents SET failure_streak=0`)
	healthy()

	// Nor is an agent's; the agents after it are still read.
	damage(`INSERT INTO agents(name,failure_streak) VALUES('daily','many')`)
	doc, err = execStatusJSON(t)
	if daily := doc.agent(t, "daily"); err == nil || daily.Paused != nil || daily.FailureStreak != nil || !saysWhy(daily.RowError, "many") {
		t.Fatalf("unreadable agent row = (%+v, %v), want paused null and why", daily, err)
	}
	if fresh := doc.agent(t, "fresh"); fresh.Paused == nil || *fresh.Paused || fresh.RowError != nil {
		t.Fatalf("fresh = %+v, want it read", fresh)
	}
	damage(`DELETE FROM agents WHERE name='daily'`)
	healthy()

	// A run that cannot be read is not an agent that never ran.
	damage(`INSERT INTO runs(id,agent,trigger_kind,scheduled_for,claimed_at,status,num_turns)
		VALUES('daily-1','daily','cron','2026-10-25T04:30:00-07:00','2026-10-25T11:30:00Z','completed','many')`)
	doc, err = execStatusJSON(t)
	if daily := doc.agent(t, "daily"); err == nil || daily.LastRun != nil || !saysWhy(daily.LastRunError, "many") {
		t.Fatalf("unreadable run = (%+v, %v), want last_run null and why", daily, err)
	}
	damage(`DELETE FROM runs`)
	healthy()

	// Incidents that cannot be read are not none.
	damage(`INSERT INTO incidents(agent,signature,first_seen,last_seen,count)
		VALUES('daily','died','2026-10-25T09:00:00Z','2026-10-25T09:00:00Z','many')`)
	doc, err = execStatusJSON(t)
	if err == nil || doc.OpenIncidents != nil || !saysWhy(doc.OpenIncidentsError, "many") {
		t.Fatalf("unreadable incidents = (%v, %v, %v), want null and why", doc.OpenIncidents, doc.OpenIncidentsError, err)
	}
	damage(`DELETE FROM incidents`)
	healthy()

	// Triggers that resolve to no slot; the agent's pause is still read.
	writeAgentDir(t, agents, "daily", strings.Replace(dailyManifest, "30 4 * * *", "30 4 31 2 *", 1))
	doc, err = execStatusJSON(t)
	if daily := doc.agent(t, "daily"); err == nil || daily.NextSlot != nil || !saysWhy(daily.NextSlotError, "triggers") || daily.Paused == nil {
		t.Fatalf("unresolvable triggers = (%+v, %v), want next_slot null and why", daily, err)
	}
	writeAgentDir(t, agents, "daily", dailyManifest)
	healthy()

	// A definition that will not load is named beside the ones that did, and
	// an agents directory that cannot be read is not an empty one. The exit
	// status is the rows'.
	writeAgentDir(t, agents, "broken", `{"name": "broken"`)
	doc, _ = execStatusJSON(t)
	if len(doc.Agents) != 2 || !saysWhy(doc.AgentsError, "broken") {
		t.Fatalf("broken definition = %+v (%v), want it named", doc.Agents, doc.AgentsError)
	}
	t.Setenv("SHUTTLE_AGENTS_DIR", filepath.Join(agents, "absent"))
	doc, _ = execStatusJSON(t)
	if doc.Agents != nil || !saysWhy(doc.AgentsError, "absent") {
		t.Fatalf("unreadable agents dir = %+v (%v), want null and why", doc.Agents, doc.AgentsError)
	}
	t.Setenv("SHUTTLE_AGENTS_DIR", agents)

	// config.json the loader refuses: whether weft is hosted is unknown rather
	// than no, and the incidents, which need no config, are still read.
	writeConfig(t, `{"weft": {"allow_write": ["/Users/Shared/weft"]}}`)
	const refused = "weft.allow_write"
	doc, err = execStatusJSON(t)
	if err == nil || !strings.Contains(err.Error(), refused) {
		t.Fatalf("status --json error = %v, want the loader's", err)
	}
	if doc.Daemon.Running == nil || *doc.Daemon.Running || doc.Agents != nil || !saysWhy(doc.AgentsError, refused) ||
		doc.WeftWorker.Hosted != nil || !saysWhy(doc.WeftWorker.HostedError, refused) ||
		doc.SpendTodayUSD != nil || !saysWhy(doc.SpendTodayError, refused) {
		t.Fatalf("refused config = %+v, want its readings null and why", doc)
	}
	if doc.OpenIncidents == nil || doc.OpenIncidentsError != nil {
		t.Fatalf("open_incidents = %v (%v), want them read", doc.OpenIncidents, doc.OpenIncidentsError)
	}

	// A ledger that is not a database: nothing it holds is read as healthy or
	// empty, and the readings it does not hold are still taken.
	weftConfig(t, worker)
	db.Close()
	ledgerPath, err := config.LedgerPath()
	if err != nil {
		t.Fatal(err)
	}
	for _, sidecar := range []string{"-wal", "-shm"} {
		if err := os.Remove(ledgerPath + sidecar); err != nil && !os.IsNotExist(err) {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(ledgerPath, []byte(strings.Repeat("not a database\n", 100)), 0o600); err != nil {
		t.Fatal(err)
	}
	doc, err = execStatusJSON(t)
	if err == nil {
		t.Fatalf("status --json on an unreadable ledger succeeded: %+v", doc)
	}
	w := doc.WeftWorker
	if doc.Daemon.Running == nil || w.Hosted == nil || !*w.Hosted || w.Probe == nil || !w.Probe.OK {
		t.Fatalf("unreadable ledger = %+v, want the daemon and the probe read", doc)
	}
	const notDB = "not a database"
	if doc.Agents != nil || !saysWhy(doc.AgentsError, notDB) ||
		w.Paused != nil || !saysWhy(w.RowError, notDB) || w.Counts != nil || !saysWhy(w.CountsError, notDB) ||
		doc.SpendTodayUSD != nil || !saysWhy(doc.SpendTodayError, notDB) ||
		doc.OpenIncidents != nil || !saysWhy(doc.OpenIncidentsError, notDB) {
		t.Fatalf("unreadable ledger = %+v, want its readings null and why", doc)
	}
}
