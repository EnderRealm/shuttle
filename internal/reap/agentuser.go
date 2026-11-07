package reap

import (
	"errors"
	"fmt"
	"os/exec"
	"os/user"
	"strconv"
	"strings"
	"time"

	"shuttle/internal/config"
)

// UserProcess is one live process whose real uid is an agent user's, as the
// process table answered for it.
type UserProcess struct {
	PID, PPID int
	Start     time.Time
	// Path is the process's exec path, "" where it could not be read; Comm
	// the kernel's short command name, which names it in a log line and
	// identifies nothing.
	Path, Comm string
}

func (p UserProcess) String() string {
	name := p.Path
	if name == "" {
		name = p.Comm
	}
	return fmt.Sprintf("%d(%s)", p.PID, name)
}

// launchdUserServices are the per-user system services launchd starts in an
// agent user's background session and respawns after any kill (measured on
// the Studio: each ppid 1). They run Apple's binaries, not the agent's, and
// are never a segment's.
var launchdUserServices = map[string]bool{
	"/usr/sbin/distnoted": true,
	"/usr/sbin/cfprefsd":  true,
	"/usr/libexec/lsd":    true,
	"/usr/libexec/secd":   true,
}

// exempt reports whether p is one of launchdUserServices as launchd runs it:
// by full exec path, never the short command name an agent can set to
// anything, and with launchd as its parent. An agent's orphan also has ppid
// 1, and is exempt only by exec'ing one of those binaries, which then runs
// Apple's code and not the agent's.
func exempt(p UserProcess) bool {
	return p.PPID == 1 && launchdUserServices[p.Path]
}

// UserKiller kills every process of an agent user. Kill runs pkill as the
// user, List reads the process table for the user, and Grace bounds the
// whole; a nil or zero field is the real one.
type UserKiller struct {
	Kill  func(user string) error
	List  func(user string) ([]UserProcess, error)
	Grace time.Duration
}

// KillUser is UserKiller's with the real kill command and process table.
func KillUser(name string) ([]UserProcess, error) { return UserKiller{}.KillUser(name) }

// KillUser kills every process whose real uid is name's: the containment
// domain of a segment that runs as that agent user (docs/agent-users.md).
// The user belongs to exactly one unsettled segment, so nothing of its can be
// an unrelated process, setsid'd or not.
//
// It repeats `sudo -n -u <name> pkill -KILL -U <name>` until the process
// table holds no process of the user but launchdUserServices: pkill lists
// and then signals, so a child forked in between escapes one pass. The
// table, not pkill's exit status, decides: sudo failing exits 1 as pkill
// matching nothing does. A table that cannot be read fails at once, and a
// process still there after the grace fails naming each survivor; either
// way the caller keeps the segment unsettled and its user held.
//
// It answers the processes of the user the table held before the first kill.
func (k UserKiller) KillUser(name string) ([]UserProcess, error) {
	if !config.WellFormedAgentUser(name) {
		return nil, fmt.Errorf("agent user %q is not an account name", name)
	}
	kill, list, grace := k.Kill, k.List, k.Grace
	if kill == nil {
		kill = pkillAs
	}
	if list == nil {
		list = listAgentUser
	}
	if grace == 0 {
		grace = killGrace
	}
	found, err := list(name)
	if err != nil {
		return nil, err
	}
	found = live(found)
	deadline := time.Now().Add(grace)
	for {
		killErr := kill(name)
		procs, err := list(name)
		if err != nil {
			return found, err
		}
		left := live(procs)
		if len(left) == 0 {
			return found, nil
		}
		if time.Now().After(deadline) {
			names := make([]string, len(left))
			for i, p := range left {
				names[i] = p.String()
			}
			detail := ""
			if killErr != nil {
				detail = "; the last kill: " + killErr.Error()
			}
			return found, fmt.Errorf("%d process(es) of agent user %s outlived pkill -KILL -U for %s: %s%s",
				len(left), name, grace, strings.Join(names, " "), detail)
		}
		time.Sleep(killPoll)
	}
}

// live is procs less the exempt services.
func live(procs []UserProcess) []UserProcess {
	var out []UserProcess
	for _, p := range procs {
		if !exempt(p) {
			out = append(out, p)
		}
	}
	return out
}

// pkillAs sends SIGKILL to every process whose real uid is name's, as name:
// the operator may signal no other uid's processes, and sudoers grants it
// every agent user (docs/agent-users.md). pkill never signals itself.
func pkillAs(name string) error {
	out, err := exec.Command("/usr/bin/sudo", "-n", "-u", name, "/usr/bin/pkill", "-KILL", "-U", name).CombinedOutput()
	var exit *exec.ExitError
	if errors.As(err, &exit) && exit.ExitCode() == 1 && len(out) == 0 {
		// pkill matched nothing.
		return nil
	}
	if err != nil {
		return fmt.Errorf("sudo -n -u %s pkill -KILL -U %s: %v: %s", name, name, err, strings.TrimSpace(string(out)))
	}
	return nil
}

// listAgentUser is every live process whose real uid is name's.
func listAgentUser(name string) ([]UserProcess, error) {
	u, err := user.Lookup(name)
	if err != nil {
		return nil, fmt.Errorf("agent user %s: %w", name, err)
	}
	uid, err := strconv.Atoi(u.Uid)
	if err != nil {
		return nil, fmt.Errorf("agent user %s: uid %q: %w", name, u.Uid, err)
	}
	return uidProcesses(uid)
}
