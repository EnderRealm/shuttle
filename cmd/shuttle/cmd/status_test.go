package cmd

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"shuttle/internal/config"
	"shuttle/internal/ledger"
	"shuttle/internal/sandbox"
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
	// The rows status printed before the weft row existed: one per definition,
	// then the spend and the incidents.
	assertRows := func(out string) {
		t.Helper()
		lines := strings.Split(strings.TrimSuffix(out, "\n"), "\n")
		if len(lines) != 3 || !strings.HasPrefix(lines[0], "daily\tnext ") ||
			!strings.HasPrefix(lines[1], "today's spend\t") || lines[2] != "open incidents\t0" {
			t.Fatalf("status printed:\n%s\nwant the daily row, the spend and the incidents only", out)
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
