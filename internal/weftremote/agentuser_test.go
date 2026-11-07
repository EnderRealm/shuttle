package weftremote

import (
	"encoding/json"
	"errors"
	"log"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/EnderRealm/ticket/v8/pkg/ticket"

	"shuttle/internal/ledger"
	"shuttle/internal/reap"
)

// fixtureAgentUsers are a fixture host's weft.agent_users. No process is
// ever killed as one: every fixture kills against a userTable.
var fixtureAgentUsers = []string{"weftagent1", "weftagent2", "weftagent3"}

// userTable is the agent users' process table as a fixture host sees it. A
// kill as a user removes every process of that user but the stubborn ones,
// which stand for a process pkill cannot reach.
type userTable struct {
	mu       sync.Mutex
	procs    map[string][]reap.UserProcess
	stubborn map[int]bool
	// calls is every kill, in order, as the user it ran as.
	calls []string
	// onKill runs at each kill, before it, for a test to look at the host
	// mid-containment.
	onKill func(user string)
}

func newUserTable() *userTable {
	return &userTable{procs: map[string][]reap.UserProcess{}, stubborn: map[int]bool{}}
}

func (u *userTable) killer() reap.UserKiller {
	return reap.UserKiller{Kill: u.kill, List: u.list, Grace: 300 * time.Millisecond}
}

func (u *userTable) kill(user string) error {
	u.mu.Lock()
	hook := u.onKill
	u.mu.Unlock()
	if hook != nil {
		hook(user)
	}
	u.mu.Lock()
	defer u.mu.Unlock()
	u.calls = append(u.calls, user)
	u.procs[user] = slices.DeleteFunc(u.procs[user], func(p reap.UserProcess) bool { return !u.stubborn[p.PID] })
	return nil
}

func (u *userTable) list(user string) ([]reap.UserProcess, error) {
	u.mu.Lock()
	defer u.mu.Unlock()
	return slices.Clone(u.procs[user]), nil
}

// run starts p as user.
func (u *userTable) run(user string, p reap.UserProcess, stubborn bool) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.procs[user] = append(u.procs[user], p)
	u.stubborn[p.PID] = stubborn
}

func (u *userTable) unstick(pid int) {
	u.mu.Lock()
	defer u.mu.Unlock()
	delete(u.stubborn, pid)
}

func (u *userTable) left(user string) []reap.UserProcess {
	u.mu.Lock()
	defer u.mu.Unlock()
	return slices.Clone(u.procs[user])
}

func (u *userTable) kills(user string) int {
	u.mu.Lock()
	defer u.mu.Unlock()
	n := 0
	for _, c := range u.calls {
		if c == user {
			n++
		}
	}
	return n
}

// claimSecond is a second run claiming the fixture's repository.
func (f *supervisorFixture) claimSecond(ticket string) error {
	_, err := f.store.ClaimWeft(ledger.WeftLaunch{
		Ticket: ticket, Project: "demo", Repo: f.repo, Branch: ticket,
		Worktree:   filepath.Join(f.cfg.Weft.Root, strings.ReplaceAll(ticket, "/", "-")),
		RunDirRoot: filepath.Join(f.root, "runs", ledger.WeftAgent),
	})
	return err
}

// requireBusy fails unless err is the repository held by runID.
func requireBusy(t *testing.T, err error, runID string) {
	t.Helper()
	var busy *ledger.RepositoryBusyError
	if !errors.As(err, &busy) || busy.HeldBy != runID {
		t.Errorf("second claim = %v, want the repository held by %s", err, runID)
	}
}

// detached is an agent's model process that called setsid: its parent is
// launchd and it shares neither the worker's process group nor its session.
var detached = reap.UserProcess{PID: 4701, PPID: 1, Path: "/opt/homebrew/bin/claude", Comm: "claude"}

// recordedArgv is the argv run.json records for a segment.
func recordedArgv(t *testing.T, runDir string) []string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(runDir, "run.json"))
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Argv []string `json:"argv"`
	}
	if err := json.Unmarshal(data, &doc); err != nil {
		t.Fatal(err)
	}
	return doc.Argv
}

// argvValue is the value following flag in argv, "" when flag is absent.
func argvValue(argv []string, flag string) string {
	if i := slices.Index(argv, flag); i >= 0 && i+1 < len(argv) {
		return argv[i+1]
	}
	return ""
}

// A pipeline segment's worker is launched as its agent user with the hosted
// worktree root, never with a sandbox profile, and its argv carries no
// credential: the token travels in the environment alone.
func TestSegmentLaunchesAsItsAgentUser(t *testing.T) {
	f := newSupervisorFixture(t)
	run := f.claim(t, "demo/worker")
	if err := f.supervisor.run(run.ID); err != nil {
		t.Fatal(err)
	}
	argv := recordedArgv(t, run.Segments[0].RunDir.String)
	if got := argvValue(argv, "--agent-user"); got != "weftagent1" {
		t.Fatalf("argv = %q, want --agent-user weftagent1", argv)
	}
	if got := argvValue(argv, "--worktree-root"); got != filepath.Join(f.supervisor.sharedRoot, "worktrees") {
		t.Fatalf("argv = %q, want --worktree-root under the shared root", argv)
	}
	if slices.Contains(argv, "--sandbox-profile") {
		t.Fatalf("argv = %q carries --sandbox-profile", argv)
	}
	if strings.Contains(strings.Join(argv, "\x00"), fixtureOAuthToken) {
		t.Fatalf("argv = %q carries the OAuth token", argv)
	}
	if _, err := os.Stat(filepath.Join(run.Segments[0].RunDir.String, "srt.json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("a sandbox profile was written: %v", err)
	}
	if f.users.kills("weftagent1") < 1 {
		t.Fatal("the segment settled with no kill as its agent user")
	}
}

// A segment's agent user may have left a process behind however the segment
// ended — the worker exiting after its terminal event, an operator stop, the
// segment timeout, a protocol failure. Each settlement kills every process of
// the user, a setsid'd one included, while the repository is still reserved,
// and only then is the repository free for a second run.
func TestSettlementKillsTheAgentUserBeforeReleasingRepository(t *testing.T) {
	tests := []struct {
		name    string
		script  func(worktree string) string
		stop    bool
		timeout int
	}{
		{name: "exit", script: fixtureWorkerScript},
		{name: "stop", stop: true, script: func(string) string { return "#!/bin/sh\n" + workerGuard + "read -r _\nexit 0\n" }},
		{name: "timeout", timeout: 1, script: func(string) string { return "#!/bin/sh\n" + workerGuard + "sleep 300\n" }},
		{name: "protocol", script: func(string) string { return "#!/bin/sh\n" + workerGuard + "printf 'not-json\\n'\nsleep 300\n" }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newSupervisorFixture(t)
			logs := &syncBuffer{}
			f.supervisor.log = log.New(logs, "", 0)
			if tt.timeout > 0 {
				f.cfg.Weft.SegmentTimeoutSeconds = tt.timeout
			}
			ticket := "demo/contain-" + tt.name
			if err := os.WriteFile(f.cfg.Weft.WorkerBin, []byte(tt.script(f.worktree(t, ticket))), 0o700); err != nil {
				t.Fatal(err)
			}
			run := f.claim(t, ticket)
			f.users.run("weftagent1", detached, false)
			f.users.onKill = func(string) { requireBusy(t, f.claimSecond("demo/during-contain"), run.ID) }
			done := make(chan error, 1)
			go func() { done <- f.supervisor.run(run.ID) }()
			if tt.stop {
				waitFor(t, func() bool {
					got, err := f.store.GetWeftRun(run.ID)
					return err == nil && got.Segments[0].StartedAt.Valid
				})
				f.supervisor.Stop(run.ID)
			}
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			if left := f.users.left("weftagent1"); len(left) != 0 {
				t.Fatalf("weftagent1 still runs %v after settlement", left)
			}
			if !strings.Contains(logs.String(), "agent user weftagent1: killed 1 process(es) 4701(/opt/homebrew/bin/claude)") {
				t.Fatalf("log does not name the killed process:\n%s", logs.String())
			}
			f.users.onKill = nil
			if err := f.claimSecond("demo/after-contain"); err != nil {
				t.Fatalf("repository was not released after containment: %v", err)
			}
		})
	}
}

// Concurrent segments run as distinct agent users. A user whose process
// outlives the kill — a setsid'd descendant pkill cannot reach — keeps its
// segment unsettled: the segment's slot, repository and user stay held, and
// the next entry waits. The user returns to the pool only through a
// settlement whose kill left nothing, and the next segment is claimed with
// the user that did.
func TestAgentUsersReturnToThePoolOnlyOnceTheirProcessesAreGone(t *testing.T) {
	h := newQueueHost(t)
	h.cfg.Weft.AgentUsers = fixtureAgentUsers[:2]
	if err := h.store.ConfigureWeftAgentUsers(2); err != nil {
		t.Fatal(err)
	}
	ids := []string{"loom/first-0001", "ticket/second-0002", "warp/third-0003"}
	for _, id := range ids {
		h.ticket(id, ticket.StatusReady, 2)
		h.mode(id, "hold")
	}
	var runs []string
	for _, id := range ids[:2] {
		runs = append(runs, h.waitDispatched(entryOf(h.launch(id, "launch-"+id))["entry_id"].(string)))
	}
	third := entryOf(h.launch(ids[2], "launch-"+ids[2]))["entry_id"].(string)
	users := map[string]string{}
	for _, runID := range runs {
		waitFor(t, func() bool { return h.run(runID).Segments[0].StartedAt.Valid })
		run := h.run(runID)
		user := run.Segments[0].AgentUser.String
		if got := argvValue(recordedArgv(t, run.Segments[0].RunDir.String), "--agent-user"); got != user {
			t.Fatalf("run %s holds %q and was launched with --agent-user %q", runID, user, got)
		}
		users[runID] = user
	}
	if users[runs[0]] == users[runs[1]] || !slices.Contains(fixtureAgentUsers[:2], users[runs[0]]) ||
		!slices.Contains(fixtureAgentUsers[:2], users[runs[1]]) {
		t.Fatalf("concurrent segments hold %v, want weftagent1 and weftagent2 once each", users)
	}
	if e := h.entry(third); e["state"] != ledger.WeftEntryWaiting || e["waiting"].(map[string]any)["reason"] != ledger.WeftWaitCapacity {
		t.Fatalf("third entry = %v, want it waiting on capacity", e)
	}

	// The first segment's user keeps a process its kill cannot reach.
	stuck := users[runs[0]]
	h.users.run(stuck, detached, true)
	h.release(ids[0])
	waitFor(t, func() bool {
		return strings.Contains(h.logs.String(), "outlived pkill -KILL -U") && strings.Contains(h.logs.String(), "reservation retained")
	})
	first := h.run(runs[0])
	if row, err := h.store.Get(first.Segments[0].LedgerRunID); err != nil || row.Status != ledger.StatusRunning || h.reaped(first) {
		t.Fatalf("the first segment settled beside a live process of %s: %+v %v", stuck, row, err)
	}
	_, err := h.store.ClaimWeft(ledger.WeftLaunch{Ticket: "loom/during-0009", Project: "loom", Repo: first.Repo, Branch: "loom/during-0009",
		Worktree: first.Worktree + "-during", RunDirRoot: filepath.Join(h.root, "runs", ledger.WeftAgent)})
	requireBusy(t, err, runs[0])
	time.Sleep(200 * time.Millisecond)
	if e := h.entry(third); e["state"] != ledger.WeftEntryWaiting || len(h.invocations(ids[2])) != 0 {
		t.Fatalf("third entry = %v with %v, want it still waiting while %s has a process", e, h.invocations(ids[2]), stuck)
	}

	// The second segment's kill leaves nothing, so its user is the third's.
	free := users[runs[1]]
	h.release(ids[1])
	runs = append(runs, h.waitDispatched(third))
	third3 := h.run(runs[2])
	if got := third3.Segments[0].AgentUser.String; got != free {
		t.Fatalf("third run holds %q, want %s, freed by the second's settlement and not %s, still held", got, free, stuck)
	}
	if h.users.kills(free) < 2 {
		t.Fatalf("%s was reassigned after %d kill(s), want its settlement's", free, h.users.kills(free))
	}
	if left := h.users.left(stuck); len(left) != 1 {
		t.Fatalf("%s runs %v", stuck, left)
	}

	// Torn down by hand: the first segment's user is let go and its segment
	// settled, so the host's teardown finds nothing running.
	t.Cleanup(func() {
		h.users.unstick(detached.PID)
		h.release(ids[2])
		run := h.run(runs[0])
		if _, err := h.service.segments.settle(run, run.Segments[0], "", "", nil); err != nil {
			t.Error(err)
		}
	})
}

// A segment claimed before hosted segments ran as agent users, a run whose
// worktree agent users cannot reach, and a worker that cannot launch as an
// agent user are each refused before spawn, and the repository is released.
func TestPipelineSegmentsAreRefusedWithoutAnAgentUserLaunch(t *testing.T) {
	tests := []struct {
		name  string
		claim func(t *testing.T, f *supervisorFixture) (*ledger.WeftRun, error)
		setup func(t *testing.T, f *supervisorFixture)
		want  string
	}{
		{name: "no agent user", want: "holds no agent user", claim: func(t *testing.T, f *supervisorFixture) (*ledger.WeftRun, error) {
			return f.store.ClaimWeft(ledger.WeftLaunch{Ticket: "demo/old", Project: "demo", Repo: f.repo, Branch: "demo/old",
				Worktree: f.worktree(t, "demo/old"), RunDirRoot: filepath.Join(f.root, "runs", ledger.WeftAgent)})
		}},
		{name: "old worktree", want: "is not under " + "WORKTREES", claim: func(t *testing.T, f *supervisorFixture) (*ledger.WeftRun, error) {
			return f.store.ClaimWeft(ledger.WeftLaunch{Ticket: "demo/old", Project: "demo", Repo: f.repo, Branch: "demo/old",
				Worktree:   filepath.Join(f.cfg.Weft.Root, "worktrees", "repo-1", "demo-old"),
				RunDirRoot: filepath.Join(f.root, "runs", ledger.WeftAgent), AgentUser: "weftagent1"})
		}},
		{name: "v1 worker", want: "does not speak weft-worker/v2 with the agent_user capability", setup: func(t *testing.T, f *supervisorFixture) {
			script := "#!/bin/sh\ncase \"$1\" in version|allowance-probe) exit 64 ;; esac\nexit 0\n"
			if err := os.WriteFile(f.cfg.Weft.WorkerBin, []byte(script), 0o700); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "no capability", want: "does not speak weft-worker/v2 with the agent_user capability", setup: func(t *testing.T, f *supervisorFixture) {
			if err := os.WriteFile(filepath.Join(f.root, "version.json"), fixtureVersion(t, []string{"segment", "output"}), 0o600); err != nil {
				t.Fatal(err)
			}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newSupervisorFixture(t)
			if tt.setup != nil {
				tt.setup(t, f)
			}
			var run *ledger.WeftRun
			var err error
			if tt.claim != nil {
				run, err = tt.claim(t, f)
			} else {
				run, err = f.store.ClaimWeft(ledger.WeftLaunch{Ticket: "demo/old", Project: "demo", Repo: f.repo, Branch: "demo/old",
					Worktree: f.worktree(t, "demo/old"), RunDirRoot: filepath.Join(f.root, "runs", ledger.WeftAgent), AgentUser: "weftagent1"})
			}
			if err != nil {
				t.Fatal(err)
			}
			want := strings.Replace(tt.want, "WORKTREES", filepath.Join(f.supervisor.sharedRoot, "worktrees"), 1)
			if err := f.supervisor.run(run.ID); err == nil || !strings.Contains(err.Error(), want) {
				t.Fatalf("run = %v, want %q", err, want)
			}
			got, err := f.store.GetWeftRun(run.ID)
			if err != nil {
				t.Fatal(err)
			}
			if got.Phase != ledger.WeftPhaseEnded || got.Segments[0].Failure.String != ledger.ClassPreflight {
				t.Fatalf("run = %+v, want it failed %s", got, ledger.ClassPreflight)
			}
			if _, err := os.Stat(filepath.Join(got.Segments[0].RunDir.String, "run.json")); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("the worker launch was reached: %v", err)
			}
			if err := f.claimSecond("demo/after-refusal"); err != nil {
				t.Fatalf("the refusal did not release the repository: %v", err)
			}
		})
	}
}

// The cap is bounded by the agent users: an unset cap is at most their
// number, the capacity route refuses one above it, and a host whose ledger
// stores one above it, or that names no agent user, refuses to start.
func TestCapacityIsBoundedByTheAgentUsers(t *testing.T) {
	h := newQueueHost(t)
	if got := h.queue()["capacity"].(map[string]any)["cap"]; got != float64(3) {
		t.Fatalf("cap = %v, want 3", got)
	}
	r := h.send("PUT", "/v2/queue/capacity", "cap-4", `{"contract_version":"weft-service/v2","cap":4,"revision":`+
		jsonNumber(h.queue()["revision"])+`}`)
	if r.code != 400 || !strings.Contains(r.raw, `"malformed"`) || !strings.Contains(r.raw, "3 agent user(s) in weft.agent_users") {
		t.Fatalf("PUT cap 4 = %d %s", r.code, r.raw)
	}

	cfg := *h.cfg
	cfg.Weft.AgentUsers = fixtureAgentUsers[:2]
	cfg.Weft.Listen = ""
	if _, err := New(&cfg, h.store, "secret-token", h.root, log.New(&syncBuffer{}, "", 0)); err != nil {
		t.Fatalf("an unset cap over two agent users: %v", err)
	}
	if c, err := h.store.WeftCapacity(); err != nil || c.Cap != 2 {
		t.Fatalf("unset cap over two agent users = %+v %v, want 2", c, err)
	}
	r = h.send("PUT", "/v2/queue/capacity", "cap-2", `{"contract_version":"weft-service/v2","cap":2,"revision":`+
		jsonNumber(h.queue()["revision"])+`}`)
	if r.code != 200 {
		t.Fatalf("PUT cap 2 = %d %s", r.code, r.raw)
	}
	cfg.Weft.AgentUsers = fixtureAgentUsers[:1]
	if _, err := New(&cfg, h.store, "secret-token", h.root, log.New(&syncBuffer{}, "", 0)); err == nil ||
		!strings.Contains(err.Error(), "capacity stored in the ledger is 2") || !strings.Contains(err.Error(), "1 agent user(s) in weft.agent_users") {
		t.Fatalf("a stored cap of 2 over one agent user = %v", err)
	}
	cfg.Weft.AgentUsers = nil
	if _, err := New(&cfg, h.store, "secret-token", h.root, log.New(&syncBuffer{}, "", 0)); err == nil ||
		!strings.Contains(err.Error(), "weft.agent_users names no agent user") {
		t.Fatalf("no agent users = %v", err)
	}
}

func jsonNumber(v any) string {
	doc, _ := json.Marshal(v)
	return string(doc)
}
