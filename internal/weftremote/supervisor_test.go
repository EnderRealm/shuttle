package weftremote

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"shuttle/internal/config"
	"shuttle/internal/ledger"
	"shuttle/internal/sandbox"
)

func TestSupervisorRunsFixtureWorkerToParkedState(t *testing.T) {
	f := newSupervisorFixture(t, false)
	run := f.claim(t, "demo/worker")
	if err := f.supervisor.run(run.ID); err != nil {
		t.Fatal(err)
	}
	got, err := f.store.GetWeftRun(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Phase != ledger.WeftPhaseParked || got.Question == nil || got.Question.Text != "Which shape?" {
		t.Fatalf("run = %+v", got)
	}
	if len(got.Stages) != 1 || got.Stages[0].State != "needs_input" {
		t.Fatalf("stages = %+v", got.Stages)
	}
	row, err := f.store.Get(got.Segments[0].LedgerRunID)
	if err != nil {
		t.Fatal(err)
	}
	if row.Status != ledger.StatusCompleted || !row.PID.Valid {
		t.Fatalf("segment row = %+v", row)
	}
	for _, name := range []string{"run.json", "srt.json", "events.ndjson", "stderr.log", "result.json"} {
		if _, err := os.Stat(filepath.Join(got.Segments[0].RunDir.String, name)); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
	events, _ := os.ReadFile(filepath.Join(got.Segments[0].RunDir.String, "events.ndjson"))
	if !strings.Contains(string(events), `"event":"parked"`) {
		t.Fatalf("events = %s", events)
	}
}

func TestSupervisorHandsSegmentTheOAuthTokenByEnvironmentOnly(t *testing.T) {
	f := newSupervisorFixture(t, false)
	run := f.claim(t, "demo/worker")
	seen := filepath.Join(f.root, "oauth-seen")
	worktree, err := sandbox.WorktreePath(f.cfg.Weft.Root, f.repo, "demo/worker")
	if err != nil {
		t.Fatal(err)
	}
	script := "#!/bin/sh\n" + v1WorkerGuard + "if [ \"$CLAUDE_CODE_OAUTH_TOKEN\" = '" + fixtureOAuthToken +
		"' ]; then printf match > " + seen + "; else printf mismatch > " + seen + "; fi\n" +
		strings.TrimPrefix(fixtureWorkerScript(worktree), "#!/bin/sh\n")
	if err := os.WriteFile(f.cfg.Weft.WorkerBin, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := f.supervisor.run(run.ID); err != nil {
		t.Fatal(err)
	}
	if data, err := os.ReadFile(seen); err != nil || string(data) != "match" {
		t.Fatalf("worker environment carried the wrong CLAUDE_CODE_OAUTH_TOKEN: %q, %v", data, err)
	}
	runDir := run.Segments[0].RunDir.String
	record, err := os.ReadFile(filepath.Join(runDir, "run.json"))
	if err != nil {
		t.Fatal(err)
	}
	var recorded struct {
		Environment []string `json:"environment"`
	}
	if err := json.Unmarshal(record, &recorded); err != nil {
		t.Fatal(err)
	}
	if !oneOf("CLAUDE_CODE_OAUTH_TOKEN", recorded.Environment...) {
		t.Fatalf("run.json environment = %v", recorded.Environment)
	}
	err = filepath.WalkDir(runDir, func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if strings.Contains(string(data), fixtureOAuthToken) {
			t.Errorf("%s contains the OAuth token value", path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestSupervisorRefusesLaunchWhenOAuthItemIsMissing(t *testing.T) {
	f := newSupervisorFixture(t, false)
	f.cfg.Weft.OAuthKeychainItem = "missing-oauth-item"
	run := f.claim(t, "demo/oauth-missing")
	err := f.supervisor.run(run.ID)
	if err == nil || !strings.Contains(err.Error(), "missing-oauth-item") {
		t.Fatalf("run error = %v, want the item name", err)
	}
	got, err := f.store.GetWeftRun(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Phase != ledger.WeftPhaseEnded || got.Outcome.String != "failed" ||
		got.Segments[0].Failure.String != ledger.ClassPreflight {
		t.Fatalf("run = %+v", got)
	}
	row, err := f.store.Get(got.Segments[0].LedgerRunID)
	if err != nil {
		t.Fatal(err)
	}
	if row.PID.Valid || !strings.Contains(row.ErrorSignature.String, "missing-oauth-item") {
		t.Fatalf("segment row = %+v", row)
	}
	for _, name := range []string{"run.json", "events.ndjson"} {
		if _, err := os.Stat(filepath.Join(got.Segments[0].RunDir.String, name)); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("%s exists: worker launch was reached: %v", name, err)
		}
	}
	if _, err := f.store.ClaimWeft(ledger.WeftLaunch{
		Ticket: "demo/after-oauth", Project: "demo", Repo: got.Repo,
		Branch: "demo/after-oauth", Worktree: filepath.Join(f.root, "other"),
		RunDirRoot: filepath.Join(f.root, "runs", ledger.WeftAgent),
	}); err != nil {
		t.Fatalf("oauth refusal did not release repository: %v", err)
	}
}

func TestSupervisorRefusesLaunchWhenSandboxCannotBeEstablished(t *testing.T) {
	f := newSupervisorFixture(t, true)
	run := f.claim(t, "demo/sandbox-failure")
	if err := f.supervisor.run(run.ID); err == nil {
		t.Fatal("run succeeded with a refusing sandbox")
	}
	got, err := f.store.GetWeftRun(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Phase != ledger.WeftPhaseEnded || got.Outcome.String != "failed" ||
		got.Segments[0].Failure.String != ledger.ClassSandbox {
		t.Fatalf("run = %+v", got)
	}
	if _, err := f.store.ClaimWeft(ledger.WeftLaunch{
		Ticket: "demo/after-failure", Project: "demo", Repo: got.Repo,
		Branch: "demo/after-failure", Worktree: filepath.Join(f.root, "other"),
		RunDirRoot: filepath.Join(f.root, "runs", ledger.WeftAgent),
	}); err != nil {
		t.Fatalf("sandbox failure did not release repository: %v", err)
	}
}

func TestSupervisorClassifiesWorkerProcessFailures(t *testing.T) {
	tests := []struct {
		name    string
		script  string
		class   string
		timeout int
	}{
		{name: "zero without terminal", script: "#!/bin/sh\n" + v1WorkerGuard + "exit 0\n", class: ledger.ClassNoResult},
		{name: "nonzero without terminal", script: "#!/bin/sh\n" + v1WorkerGuard + "exit 3\n", class: ledger.ClassDied},
		{name: "protocol", script: "#!/bin/sh\n" + v1WorkerGuard + "printf 'not-json\\n'\n", class: ledger.ClassProtocol},
		{name: "timeout", script: "#!/bin/sh\n" + v1WorkerGuard + "sleep 300\n", class: ledger.ClassStuck, timeout: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newSupervisorFixture(t, false)
			if tt.timeout > 0 {
				f.cfg.Weft.SegmentTimeoutSeconds = tt.timeout
			}
			if err := os.WriteFile(f.cfg.Weft.WorkerBin, []byte(tt.script), 0o700); err != nil {
				t.Fatal(err)
			}
			run := f.claim(t, "demo/failure-"+strings.ReplaceAll(tt.name, " ", "-"))
			if err := f.supervisor.run(run.ID); err != nil {
				t.Fatal(err)
			}
			got, err := f.store.GetWeftRun(run.ID)
			if err != nil {
				t.Fatal(err)
			}
			if got.Phase != ledger.WeftPhaseEnded || got.Outcome.String != "failed" ||
				got.Segments[0].Failure.String != tt.class {
				t.Fatalf("run = %+v, want %s", got, tt.class)
			}
		})
	}
}

func TestSupervisorHoldsRepositoryUntilTerminalWorkerExits(t *testing.T) {
	f := newSupervisorFixture(t, false)
	worktree, err := sandbox.WorktreePath(f.cfg.Weft.Root, f.repo, "demo/worker")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(f.cfg.Weft.WorkerBin, []byte(fixtureWorkerScript(worktree)+"sleep 1\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	run := f.claim(t, "demo/worker")
	done := make(chan error, 1)
	go func() { done <- f.supervisor.run(run.ID) }()
	waitFor(t, func() bool {
		got, err := f.store.GetWeftRun(run.ID)
		return err == nil && got.Phase == ledger.WeftPhaseParked
	})
	_, err = f.store.ClaimWeft(ledger.WeftLaunch{
		Ticket: "demo/second", Project: "demo", Repo: f.repo,
		Branch: "demo/second", Worktree: filepath.Join(f.root, "second"),
		RunDirRoot: filepath.Join(f.root, "runs", ledger.WeftAgent),
	})
	var busy *ledger.RepositoryBusyError
	if !errors.As(err, &busy) || busy.HeldBy != run.ID {
		t.Fatalf("claim before exit = %v", err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.ClaimWeft(ledger.WeftLaunch{
		Ticket: "demo/second", Project: "demo", Repo: f.repo,
		Branch: "demo/second", Worktree: filepath.Join(f.root, "second"),
		RunDirRoot: filepath.Join(f.root, "runs", ledger.WeftAgent),
	}); err != nil {
		t.Fatalf("claim after exit: %v", err)
	}
}

func TestSupervisorKillsDescendantsBeforeReleasingRepository(t *testing.T) {
	f := newSupervisorFixture(t, false)
	run := f.claim(t, "demo/worker")
	pidFile := filepath.Join(run.Segments[0].RunDir.String, "descendant.pid")
	worktree, err := sandbox.WorktreePath(f.cfg.Weft.Root, f.repo, "demo/worker")
	if err != nil {
		t.Fatal(err)
	}
	script := fixtureWorkerScript(worktree) +
		"(trap '' TERM; while :; do /bin/sleep 1; done) </dev/null >/dev/null 2>&1 &\n" +
		"printf '%s' \"$!\" > " + pidFile + "\n"
	if err := os.WriteFile(f.cfg.Weft.WorkerBin, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	priorGrace := segmentKillGrace
	segmentKillGrace = 500 * time.Millisecond
	t.Cleanup(func() { segmentKillGrace = priorGrace })
	if err := f.supervisor.run(run.ID); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(pidFile)
	if err != nil {
		t.Fatal(err)
	}
	pid, err := strconv.Atoi(string(data))
	if err != nil {
		t.Fatal(err)
	}
	if err := syscall.Kill(pid, 0); !errors.Is(err, syscall.ESRCH) {
		t.Fatalf("descendant %d remains after repository release: %v", pid, err)
	}
	if _, err := f.store.ClaimWeft(ledger.WeftLaunch{
		Ticket: "demo/after-descendant", Project: "demo", Repo: f.repo,
		Branch: "demo/after-descendant", Worktree: filepath.Join(f.root, "after-descendant"),
		RunDirRoot: filepath.Join(f.root, "runs", ledger.WeftAgent),
	}); err != nil {
		t.Fatalf("repository was not released after group exit: %v", err)
	}
}

func TestWorkerCannotRunBeforePIDIsDurable(t *testing.T) {
	f := newSupervisorFixture(t, false)
	run := f.claim(t, "demo/worker")
	marker := filepath.Join(run.Segments[0].RunDir.String, "worker-started")
	worktree, err := sandbox.WorktreePath(f.cfg.Weft.Root, f.repo, "demo/worker")
	if err != nil {
		t.Fatal(err)
	}
	script := "#!/bin/sh\n" + v1WorkerGuard + "printf started > " + marker + "\n" +
		strings.TrimPrefix(fixtureWorkerScript(worktree), "#!/bin/sh\n")
	if err := os.WriteFile(f.cfg.Weft.WorkerBin, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	spawned := make(chan struct{})
	release := make(chan struct{})
	priorHook := afterWorkerSpawn
	afterWorkerSpawn = func() {
		close(spawned)
		<-release
	}
	t.Cleanup(func() { afterWorkerSpawn = priorHook })
	done := make(chan error, 1)
	go func() { done <- f.supervisor.run(run.ID) }()
	<-spawned
	if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("worker ran before PID persistence: %v", err)
	}
	row, err := f.store.Get(run.Segments[0].LedgerRunID)
	if err != nil {
		t.Fatal(err)
	}
	if row.Status != ledger.StatusClaimed || row.PID.Valid {
		t.Fatalf("pre-release row = %+v", row)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("worker did not run after gate release: %v", err)
	}
}

const (
	fixtureOAuthItem  = "fixture-oauth-item"
	fixtureOAuthToken = "sk-ant-oat01-fixture-token-value"
)

type supervisorFixture struct {
	root       string
	store      *ledger.Store
	cfg        *config.Config
	supervisor *Supervisor
	repo       string
}

func newSupervisorFixture(t *testing.T, refuseSandbox bool) *supervisorFixture {
	t.Helper()
	root, err := os.MkdirTemp("/tmp", "sw-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(root) })
	repo := filepath.Join(root, "repo")
	central := filepath.Join(root, "ticket-store")
	tkHome := filepath.Join(root, ".ticket")
	for _, dir := range []string{
		filepath.Join(repo, ".git", "worktrees"),
		filepath.Join(central, "tickets", "demo"),
		tkHome,
	} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	tkConfig := filepath.Join(tkHome, "config.yaml")
	if err := os.WriteFile(tkConfig, []byte(
		"central_root: "+central+"\nprojects:\n  demo:\n    path: "+repo+"\n",
	), 0o600); err != nil {
		t.Fatal(err)
	}
	srt := filepath.Join(root, "srt")
	exit := "0"
	if refuseSandbox {
		exit = "1"
	}
	if err := os.WriteFile(srt, []byte("#!/bin/sh\nexit "+exit+"\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	worker := filepath.Join(root, "weft-worker")
	cfg := &config.Config{
		Timezone: "UTC",
		Paths: config.Paths{
			TKConfig: tkConfig, SRTBin: srt, LoomHome: filepath.Join(root, "loom"),
		},
		Weft: config.Weft{
			WorkerBin: worker, Root: filepath.Join(root, "weft"),
			OAuthKeychainItem:     fixtureOAuthItem,
			SegmentTimeoutSeconds: 10,
			Sandbox:               config.WeftSandbox{AllowedDomains: []string{"github.com"}},
		},
	}
	worktree, err := sandbox.WorktreePath(cfg.Weft.Root, repo, "demo/worker")
	if err != nil {
		t.Fatal(err)
	}
	script := fixtureWorkerScript(worktree)
	if err := os.WriteFile(worker, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	store, err := ledger.Open(filepath.Join(root, "runs.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	supervisor := NewSupervisor(cfg, store, root, log.New(io.Discard, "", 0))
	supervisor.findKey = func(item string) (string, error) {
		if item != fixtureOAuthItem {
			return "", fmt.Errorf("keychain item %q: not found", item)
		}
		return fixtureOAuthToken, nil
	}
	return &supervisorFixture{root: root, store: store, cfg: cfg, supervisor: supervisor, repo: repo}
}

func (f *supervisorFixture) claim(t *testing.T, id string) *ledger.WeftRun {
	t.Helper()
	worktree, err := sandbox.WorktreePath(f.cfg.Weft.Root, f.repo, id)
	if err != nil {
		t.Fatal(err)
	}
	run, err := f.store.ClaimWeft(ledger.WeftLaunch{
		Ticket: id, Project: "demo", Repo: f.repo, Branch: id, Worktree: worktree,
		RunDirRoot: filepath.Join(f.root, "runs", ledger.WeftAgent),
	})
	if err != nil {
		t.Fatal(err)
	}
	return run
}

// v1WorkerGuard is how a weft-remote/v1 worker answers the subcommands
// weft-worker/v2 adds: exit 64, no event (§2.6).
const v1WorkerGuard = "case \"$1\" in version|allowance-probe) exit 64 ;; esac\n"

func fixtureWorkerScript(worktree string) string {
	quoted, _ := json.Marshal(worktree)
	return "#!/bin/sh\n" + v1WorkerGuard + `run=
segment=
ticket=
while [ "$#" -gt 0 ]; do
  case "$1" in
    --run) run="$2"; shift 2 ;;
    --segment) segment="$2"; shift 2 ;;
    --ticket) ticket="$2"; shift 2 ;;
    *) shift ;;
  esac
done
printf '{"contract_version":"weft-remote/v1","run_id":"%s","worker_run_id":"20260916T120000Z-12345678","segment":%s,"seq":1,"ts":"2026-09-16T12:00:00.000Z","event":"hello","worker_version":"fixture","ticket":"%s","project":"demo","branch":"%s","worktree":%s,"base":"base","criteria":"criteria","resumed":null,"inherited":[],"answer":null,"stages":[]}\n' "$run" "$segment" "$ticket" "$ticket" '` + string(quoted) + `'
printf '{"contract_version":"weft-remote/v1","run_id":"%s","worker_run_id":"20260916T120000Z-12345678","segment":%s,"seq":2,"ts":"2026-09-16T12:01:00.000Z","event":"stage","stage":"work","backend":"claude","occurrence":1,"attempt":1,"state":"needs_input","session":null,"detail":null,"exit_code":null,"turns":1,"spend":null,"processing_ms":100,"tokens":null}\n' "$run" "$segment"
printf '{"contract_version":"weft-remote/v1","run_id":"%s","worker_run_id":"20260916T120000Z-12345678","segment":%s,"seq":3,"ts":"2026-09-16T12:01:01.000Z","event":"parked","stage":"work","occurrence":1,"attempt":1,"worker_outcome":"blocked","question":"Which shape?","head":"head","summary":{"state":"rendered"}}\n' "$run" "$segment"
`
}

// A failed version probe stands for workerReprobeAfter, then an unchanged
// worker_bin is probed again; the same failure is logged once.
func TestWorkerProbeRetriesAFailureAfterItsBackOff(t *testing.T) {
	f := newSupervisorFixture(t, false)
	logs := &syncBuffer{}
	f.supervisor.log = log.New(logs, "", 0)
	version := filepath.Join(f.root, "version.json")
	script := "#!/bin/sh\ncase \"$1\" in version) cat " + version + " 2>/dev/null || exit 64; exit 0 ;; esac\nexit 3\n"
	if err := os.WriteFile(f.cfg.Weft.WorkerBin, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	prior := workerReprobeAfter
	t.Cleanup(func() { workerReprobeAfter = prior })
	workerReprobeAfter = 0
	for range 2 {
		if w := f.supervisor.refreshWorker(); w.Failure != "exited 64" {
			t.Fatalf("probe = %+v", w)
		}
	}
	if n := strings.Count(logs.String(), "failed: exited 64"); n != 1 {
		t.Fatalf("the same failure was logged %d times:\n%s", n, logs.String())
	}
	doc, err := os.ReadFile(filepath.Join(contractDir(t), "worker-version.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(version, doc, 0o600); err != nil {
		t.Fatal(err)
	}
	workerReprobeAfter = time.Hour
	if w := f.supervisor.refreshWorker(); w.Failure != "exited 64" {
		t.Fatalf("a failure was re-probed inside its back-off: %+v", w)
	}
	workerReprobeAfter = 0
	if w := f.supervisor.refreshWorker(); !w.speaks(WorkerContractV2) || !w.has("output") {
		t.Fatalf("after the back-off = %+v", w)
	}
	if !strings.Contains(logs.String(), "is weft-worker 0.2.0 (weft@dev)") {
		t.Fatalf("log lacks the recovered probe:\n%s", logs.String())
	}
}
