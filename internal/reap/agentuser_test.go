package reap

import (
	"errors"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeTable is a process table by agent user. A kill as a user removes every
// process of that user but the stubborn ones, which stand for a process the
// kill cannot reach, and launchd's services, which launchd puts back.
type fakeTable struct {
	mu       sync.Mutex
	procs    map[string][]UserProcess
	stubborn map[int]bool
	// calls is every kill and list, in order, as "kill <user>" or "list <user>".
	calls   []string
	killErr error
	listErr error
}

func newFakeTable(procs map[string][]UserProcess) *fakeTable {
	return &fakeTable{procs: procs, stubborn: map[int]bool{}}
}

func (f *fakeTable) kill(user string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, "kill "+user)
	f.procs[user] = slices.DeleteFunc(f.procs[user], func(p UserProcess) bool { return !f.stubborn[p.PID] && !exempt(p) })
	return f.killErr
}

func (f *fakeTable) list(user string) ([]UserProcess, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, "list "+user)
	if f.listErr != nil {
		return nil, f.listErr
	}
	return slices.Clone(f.procs[user]), nil
}

func (f *fakeTable) killer() UserKiller {
	return UserKiller{Kill: f.kill, List: f.list, Grace: 200 * time.Millisecond}
}

func (f *fakeTable) left(user string) []UserProcess {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.procs[user])
}

var (
	agentChild   = UserProcess{PID: 701, PPID: 700, Path: "/opt/homebrew/bin/claude", Comm: "claude"}
	setsidOrphan = UserProcess{PID: 702, PPID: 1, Path: "/bin/sleep", Comm: "sleep"}
	distnoted    = UserProcess{PID: 703, PPID: 1, Path: "/usr/sbin/distnoted", Comm: "distnoted"}
	cfprefsd     = UserProcess{PID: 704, PPID: 1, Path: "/usr/sbin/cfprefsd", Comm: "cfprefsd"}
	lsd          = UserProcess{PID: 705, PPID: 1, Path: "/usr/libexec/lsd", Comm: "lsd"}
	secd         = UserProcess{PID: 706, PPID: 1, Path: "/usr/libexec/secd", Comm: "secd"}
)

// Every process of the user is killed, a setsid'd orphan among them, and the
// kill is done once none is left but the services launchd respawns in every
// user's session. Another agent user's processes are never touched.
func TestKillUserKillsEveryProcessOfTheUserButLaunchdServices(t *testing.T) {
	other := UserProcess{PID: 801, PPID: 800, Path: "/bin/sleep", Comm: "sleep"}
	f := newFakeTable(map[string][]UserProcess{
		"weftagent1": {agentChild, setsidOrphan, distnoted, cfprefsd, lsd, secd},
		"weftagent2": {other},
	})
	found, err := f.killer().KillUser("weftagent1")
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(found, []UserProcess{agentChild, setsidOrphan}) {
		t.Fatalf("found = %v, want the agent's two processes and no service", found)
	}
	if left := f.left("weftagent1"); !slices.Equal(left, []UserProcess{distnoted, cfprefsd, lsd, secd}) {
		t.Fatalf("weftagent1 still runs %v, want only launchd's services", left)
	}
	if left := f.left("weftagent2"); !slices.Equal(left, []UserProcess{other}) {
		t.Fatalf("weftagent2 runs %v, want it untouched", left)
	}
	for _, call := range f.calls {
		if strings.HasSuffix(call, "weftagent2") {
			t.Fatalf("calls = %v, want none for weftagent2", f.calls)
		}
	}
	if !slices.Contains(f.calls, "kill weftagent1") {
		t.Fatalf("calls = %v, want a kill as weftagent1", f.calls)
	}
}

// A user with nothing running is still killed once before it is reported
// empty: the table is what decides, and the kill is what it decides after.
func TestKillUserKillsOnceWhenNothingRuns(t *testing.T) {
	f := newFakeTable(map[string][]UserProcess{"weftagent1": {distnoted}})
	found, err := f.killer().KillUser("weftagent1")
	if err != nil || len(found) != 0 {
		t.Fatalf("KillUser = %v %v, want nothing found", found, err)
	}
	if !slices.Equal(f.calls, []string{"list weftagent1", "kill weftagent1", "list weftagent1"}) {
		t.Fatalf("calls = %v", f.calls)
	}
}

// A process that outlives the kill fails it after the grace, naming each
// survivor by pid and exec path. A service is exempt by its exec path and its
// launchd parent together: one named like a service by its short command name
// alone, or at a service's path under another parent, is a survivor.
func TestKillUserFailsWhileAProcessSurvives(t *testing.T) {
	spoofed := UserProcess{PID: 710, PPID: 1, Path: "/Users/Shared/weft/worktrees/x/distnoted", Comm: "distnoted"}
	reparented := UserProcess{PID: 711, PPID: 702, Path: "/usr/sbin/distnoted", Comm: "distnoted"}
	unread := UserProcess{PID: 712, PPID: 1, Comm: "secd"}
	f := newFakeTable(map[string][]UserProcess{"weftagent1": {setsidOrphan, spoofed, reparented, unread, distnoted}})
	for _, p := range []UserProcess{setsidOrphan, spoofed, reparented, unread} {
		f.stubborn[p.PID] = true
	}
	start := time.Now()
	_, err := f.killer().KillUser("weftagent1")
	if err == nil {
		t.Fatal("KillUser succeeded with four processes left")
	}
	if time.Since(start) < 200*time.Millisecond {
		t.Fatalf("gave up after %s, before its grace", time.Since(start))
	}
	for _, want := range []string{"4 process(es) of agent user weftagent1", "702(/bin/sleep)",
		"710(/Users/Shared/weft/worktrees/x/distnoted)", "711(/usr/sbin/distnoted)", "712(secd)"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not name %q", err, want)
		}
	}
	if strings.Contains(err.Error(), "703(") {
		t.Errorf("error %q names launchd's distnoted", err)
	}
	kills := 0
	for _, call := range f.calls {
		if call == "kill weftagent1" {
			kills++
		}
	}
	if kills < 2 {
		t.Fatalf("killed %d time(s), want the kill repeated until the grace ran out", kills)
	}
}

// The table, not the kill's exit status, decides: sudo refusing exits 1 as
// pkill matching nothing does. A kill that errors over an emptied table is
// done; one that errors with a survivor reports both.
func TestKillUserJudgesByTheTableNotTheKill(t *testing.T) {
	f := newFakeTable(map[string][]UserProcess{"weftagent1": {agentChild}})
	f.killErr = errors.New("sudo: a password is required")
	if _, err := f.killer().KillUser("weftagent1"); err != nil {
		t.Fatalf("an emptied table with a failing kill = %v", err)
	}
	f = newFakeTable(map[string][]UserProcess{"weftagent1": {agentChild}})
	f.stubborn[agentChild.PID] = true
	f.killErr = errors.New("sudo: a password is required")
	_, err := f.killer().KillUser("weftagent1")
	if err == nil || !strings.Contains(err.Error(), "701(/opt/homebrew/bin/claude)") || !strings.Contains(err.Error(), "a password is required") {
		t.Fatalf("a survivor with a failing kill = %v", err)
	}
}

// A table that cannot be read establishes nothing, and a name sudo could read
// as a flag or a uid is never handed to it.
func TestKillUserFailsClosed(t *testing.T) {
	f := newFakeTable(map[string][]UserProcess{})
	f.listErr = errors.New("kern.proc.ruid: boom")
	if _, err := f.killer().KillUser("weftagent1"); err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("an unreadable table = %v", err)
	}
	for _, name := range []string{"", "-x", "#501", "weft agent"} {
		f := newFakeTable(map[string][]UserProcess{})
		if _, err := f.killer().KillUser(name); err == nil || len(f.calls) != 0 {
			t.Fatalf("KillUser(%q) = %v with calls %v, want refused before any", name, err, f.calls)
		}
	}
}

func TestUserProcessNamesItselfByPath(t *testing.T) {
	if got := setsidOrphan.String(); got != strconv.Itoa(setsidOrphan.PID)+"(/bin/sleep)" {
		t.Fatalf("String = %q", got)
	}
	if got := (UserProcess{PID: 9, Comm: "secd"}).String(); got != "9(secd)" {
		t.Fatalf("String without a path = %q", got)
	}
}
