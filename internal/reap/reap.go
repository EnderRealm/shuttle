package reap

import (
	"errors"
	"fmt"
	"path/filepath"
	"syscall"
	"time"

	"golang.org/x/sys/unix"

	"shuttle/internal/ledger"
)

// killGrace is the pause between SIGTERM and SIGKILL, and again between the
// SIGKILL and giving up on the group. A var so the tests do not sit through
// two full grace periods.
var killGrace = 5 * time.Second

// killPoll is how often the group is listed while a grace period runs. The
// processes are not this daemon's children, so a listing that no longer holds
// them is the only evidence they are gone — there is no wait to sit on.
const killPoll = 50 * time.Millisecond

// kill is the process control Restart applies to a row it established. A var so
// a test can drive the failure path, which no signal this daemon may send can
// produce on demand.
var kill = killGroup

// sendSignal is the syscall signalGroup sends through. A var so a test can
// record what the pass actually sent; identify's kill 0 deliberately does not
// go through it, being a probe rather than a signal.
var sendSignal = syscall.Kill

// listGroup is the kernel's answer for who is in a process group. A var so a
// test can substitute a listing, which is the only way to drive the group id
// being reused between two probes.
var listGroup = groupMembers

// lookupStart and lookupIsRun are the kernel identity lookups identify runs
// against a live pid. Vars so a test can drive the lookup-error path, which no
// process this daemon can start produces on demand.
var (
	lookupStart = processStart
	lookupIsRun = processIsRun
)

// The runtime argument pair that carries the run directory into the launched
// argv (§7.4): --settings <run_dir>/settings.json. It is the run's identity,
// because the run id is in that path.
const (
	settingsFlag = "--settings"
	settingsFile = "settings.json"
)

// lookupFailed prefixes the detail of a lookup that errored on a live pid, so
// the log line is telling apart from an established-foreign one: this row was
// held because nothing could be read, not closed because something was.
const lookupFailed = "identity lookup failed: "

// startSlack widens the window a process must have started in to be the run's
// own. started_at is stamped by MarkRunning after the launch and truncated to
// the second, so the process can have started up to a second plus the runner's
// latency before it; claimed_at is the other end, before which the run did not
// exist. A process outside the widened window took the pid after the run's own
// died, or held it before the run was claimed.
const startSlack = 2 * time.Second

// member is one process in a group, with the identity that distinguishes it
// from the same pid reused: the kernel's start time for it.
type member struct {
	PID   int
	Start time.Time
}

// Action is what the pass did with one row's recorded pid.
type Action string

const (
	ActionNoPID  Action = "no_pid" // the row was claimed with no pid recorded; nothing to check
	ActionDead   Action = "dead"   // the pid is gone and the group it led holds nothing; no signal sent
	ActionKilled Action = "killed" // the pid was established as the run's own, the group was signalled and is now empty
	// ActionForeign is a pid established as not the run's own — another user's
	// process, one that does not lead its group, one that started outside the
	// window, one whose argv does not carry the run's settings argument — plus
	// the rows whose own ledger data can establish nothing. Never signalled, and
	// closed per Restart's rule.
	ActionForeign Action = "foreign"
	// ActionKillFailed is signals that were sent and did not resolve the group:
	// the run's own processes outlived the SIGKILL, or the kernel refused a
	// signal. The row stays in flight per Restart's rule.
	ActionKillFailed Action = "kill_failed"
	// ActionUnresolved is a row nothing could be established about, with nothing
	// signalled: a group holding live processes the pass cannot authenticate as
	// the run's own, a dead leader whose group still holds live members, or an
	// identity lookup that errored on a live pid. The row stays in flight per
	// Restart's rule.
	ActionUnresolved Action = "unresolved"
)

// actionOwned is identify's answer for a pid established as the run's own, the
// one case that may be signalled. Restart resolves it to what killGroup made of
// the group, so it is never stored in an Outcome and never reaches the log.
const actionOwned Action = "owned"

// Outcome is what the pass did with one in-flight row, for the daemon log.
type Outcome struct {
	RunID  string
	Agent  string
	PID    int // 0 when the row had none
	Action Action
	// Detail is the reason a pid was judged foreign or its group left
	// unresolved, or the text of a lookup or kill error; empty otherwise.
	Detail string
}

// Ledger is the subset of the run ledger the pass uses, declared here on the
// consumer side.
type Ledger interface {
	InFlight() ([]*ledger.Run, error)
	MarkUnknown(id string) error
}

var _ Ledger = (*ledger.Store)(nil)

// Restart is §5's restart pass: every row a previous daemon left in flight is
// checked against its recorded pid, the processes proven to be the run's own
// are killed by process group, and the row is then marked unknown — the daemon
// never adopts or resumes one of these runs, and closing the row is what stops
// every later slot for the agent being an overlap skip.
//
// A row is closed only once the pass has established that no process of the run
// remains, or that the recorded pid belongs to something that is not the run.
// Short of that it stays in flight — the kill that failed, the group left
// holding members the pass cannot authenticate, the dead leader whose group
// still holds live members, the identity lookup that errored on a live pid: the
// ledger asserts only what the pass checked, and closing a row whose processes
// may still be alive would drop the overlap guard and let the next tick claim a
// second run beside them. The exception is a row whose own ledger data
// establishes nothing — no run_dir, or one that is not absolute — since no later
// pass could establish it either and holding it would wedge the agent forever;
// those close, unsignalled.
//
// The pass does not stop over a row it could not resolve — one process it cannot
// account for must not halt the estate — and the next daemon start re-checks
// that pid.
//
// It returns one Outcome per row, in ledger order. A MarkUnknown failure stops
// the pass and is returned with the outcomes recorded so far: the daemon must
// not go on to tick over a row it could not close.
func Restart(l Ledger) ([]Outcome, error) {
	runs, err := l.InFlight()
	if err != nil {
		return nil, err
	}
	outcomes := make([]Outcome, 0, len(runs))
	for _, run := range runs {
		out := Outcome{RunID: run.ID, Agent: run.Agent, PID: int(run.PID.Int64)}
		action, leader, detail := identify(run)
		if action == actionOwned {
			out.Action, out.Detail = kill(leader)
		} else {
			out.Action, out.Detail = action, detail
		}
		outcomes = append(outcomes, out)
		if out.Action == ActionKillFailed || out.Action == ActionUnresolved {
			continue
		}
		if err := l.MarkUnknown(run.ID); err != nil {
			return outcomes, fmt.Errorf("run %s: %w", run.ID, err)
		}
	}
	return outcomes, nil
}

// identify answers actionOwned when the row's recorded pid is established as
// the run's own process, which is the only case that may be signalled, and
// returns it as the member the caller signals: pid alone would let the kill
// path re-derive the group from an id the kernel may have handed on, so the
// kernel start time that established it travels with it. The caller sends the
// signals and resolves the outcome by what they did. When it is not established
// the returned Action is the outcome to record and the string says why.
//
// Every case that is not established is never signalled: the kernel may have
// reused the recorded pid for an unrelated process by the time the daemon came
// back, and a group SIGKILL aimed at that is unrecoverable. A lookup that errors
// on a live pid establishes nothing either way, so it answers ActionUnresolved
// and the row is held rather than closed (Restart).
func identify(run *ledger.Run) (Action, member, string) {
	if !run.PID.Valid || run.PID.Int64 <= 0 {
		return ActionNoPID, member{}, ""
	}
	pid := int(run.PID.Int64)
	if err := syscall.Kill(pid, 0); err != nil {
		if errors.Is(err, syscall.ESRCH) {
			return leaderGone(pid)
		}
		// EPERM is a live process belonging to another user, which this
		// daemon's run never is.
		return ActionForeign, member{}, "kill 0: " + err.Error()
	}
	// Checked once the pid is known to be live, so a dead one is resolved as
	// dead rather than on the row's contents. Without a run_dir there is nothing
	// to establish the live pid against and no later pass could do better, so
	// the row is closed rather than held forever.
	if !run.RunDir.Valid || run.RunDir.String == "" {
		return ActionForeign, member{}, "no run_dir recorded"
	}
	dir := run.RunDir.String
	if !filepath.IsAbs(dir) {
		return ActionForeign, member{}, fmt.Sprintf("run_dir %q is not an absolute path", dir)
	}
	// The runtime launches with Setpgid (§7.4), so a run's own process leads
	// its group; a pid the kernel handed to something else rarely does.
	pgid, err := unix.Getpgid(pid)
	if err != nil {
		return ActionUnresolved, member{}, lookupFailed + "getpgid: " + err.Error()
	}
	if pgid != pid {
		return ActionForeign, member{}, "pid is not its process-group leader"
	}
	started, err := lookupStart(pid)
	if err != nil {
		return ActionUnresolved, member{}, lookupFailed + err.Error()
	}
	from, to, err := startWindow(run)
	if err != nil {
		// The row's own timestamps, not a lookup: nothing about the process can
		// be established from them by this pass or any later one.
		return ActionForeign, member{}, err.Error()
	}
	if started.Before(from) || started.After(to) {
		return ActionForeign, member{}, fmt.Sprintf("process started %s, outside %s..%s",
			started.UTC().Format(time.RFC3339), from.Format(time.RFC3339), to.Format(time.RFC3339))
	}
	// The identity condition, as against the heuristics above it: the window
	// only narrows which reused pid is plausible, while the settings argument
	// carries the run id, which no unrelated process can be carrying.
	isRun, err := lookupIsRun(pid, dir)
	if err != nil {
		return ActionUnresolved, member{}, lookupFailed + err.Error()
	}
	if !isRun {
		return ActionForeign, member{}, fmt.Sprintf("process argv does not carry %s %s",
			settingsFlag, filepath.Join(dir, settingsFile))
	}
	return actionOwned, member{PID: pid, Start: started}, ""
}

// leaderGone is the answer for a recorded pid the kernel no longer knows. The
// leader being gone is not the run being gone: the group it led can still hold
// live members — a tool child that outlived the CLI, or the survivor a previous
// pass's SIGTERM handler forked — and while any member remains the pgid stays
// reserved, so nothing unrelated can have taken it. Those members cannot be
// authenticated as the run's own either, so they are never signalled and the row
// is held per Restart's rule; a listing that cannot be read establishes nothing
// about the group at all, so it is held the same way.
func leaderGone(pid int) (Action, member, string) {
	members, err := listGroup(pid)
	if err != nil {
		return ActionUnresolved, member{}, err.Error()
	}
	if len(members) == 0 {
		return ActionDead, member{}, ""
	}
	return ActionUnresolved, member{}, fmt.Sprintf(
		"leader gone; process group %d holds %d live member(s) the pass cannot authenticate", pid, len(members))
}

// startWindow is the interval the recorded pid's process must have started in
// to be this run's own, in UTC: the ledger's claimed_at and started_at, each
// widened by startSlack.
func startWindow(run *ledger.Run) (time.Time, time.Time, error) {
	claimed, err := time.Parse(time.RFC3339, run.ClaimedAt)
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("parse claimed_at: %w", err)
	}
	// A claimed row carrying a pid should not exist — MarkRunning writes both
	// in one statement — so the claim instant bounds both ends of the window.
	started := claimed
	if run.StartedAt.Valid {
		if started, err = time.Parse(time.RFC3339, run.StartedAt.String); err != nil {
			return time.Time{}, time.Time{}, fmt.Errorf("parse started_at: %w", err)
		}
	}
	return claimed.UTC().Add(-startSlack), started.UTC().Add(startSlack), nil
}

// killGroup stops the group the run's process leads, in the runtime's two
// stages (§7.4) but without its wait, since these processes are not our
// children. It returns the outcome the row is recorded with and its detail:
// ActionKilled once the group is empty, ActionDead when it emptied before
// anything was signalled, ActionUnresolved for a group left holding members the
// pass cannot authenticate, and ActionKillFailed for the run's own processes
// outliving the SIGKILL or a signal the kernel refused.
//
// Every stage is decided on the identity of the members the group started
// with — pid and kernel start time — rather than on the group id still
// answering: these are orphans, init reaps the last member the instant it
// exits, and the id is free for reuse from then on, so a group that answers a
// later probe may be an unrelated one that took the id. Never having seen the
// group empty establishes nothing.
//
// The check-to-signal gap cannot be closed on macOS: there is no pidfd-style
// handle to hold a process group of orphans open, so every stage can only list
// and then signal. The residual is precisely the window between the final
// listing and the kill(-pgid) syscall, and landing on an unrelated group inside
// it needs the last original member to exit and the kernel to hand the same
// pgid to a new group leader in that gap. Nothing narrower exists without a
// kernel primitive; the window is not closed.
func killGroup(leader member) (Action, string) {
	pid := leader.PID
	// Snapshotted before the SIGTERM, so every later probe is against members
	// that were the run's own. The snapshot is trusted only because it still
	// holds the process identify established: a listing that cannot be read, or
	// one that does not, fails closed.
	original, err := listGroup(pid)
	if err != nil {
		return ActionUnresolved, err.Error()
	}
	if len(original) == 0 {
		// The run's processes went between identify's probe and this listing,
		// so there was never anything to signal and the row closes.
		return ActionDead, "group emptied before signalling"
	}
	if !holds(original, leader) {
		// The group is not empty but the leader identify established is not in
		// it: the run's own members went and the id was handed on, or something
		// else already held it. Nothing here can be shown to be the run's, so
		// nothing is signalled.
		return ActionUnresolved, fmt.Sprintf(
			"process group %d holds %d member(s) the pass cannot authenticate; nothing signalled", pid, len(original))
	}
	signalErr := signalGroup(pid, syscall.SIGTERM)
	survived, strangers, err := originalSurvives(pid, original)
	if err != nil {
		return ActionKillFailed, err.Error()
	}
	// The SIGKILL goes only to a group one of the original members is still in
	// — a descendant that ignored the SIGTERM keeps it alive after the leader
	// is gone.
	if !survived {
		return unrecognised(pid, strangers, syscall.SIGTERM)
	}
	if e := signalGroup(pid, syscall.SIGKILL); e != nil {
		signalErr = e
	}
	survived, strangers, err = originalSurvives(pid, original)
	if err != nil {
		return ActionKillFailed, err.Error()
	}
	if !survived {
		return unrecognised(pid, strangers, syscall.SIGKILL)
	}
	// A refused signal explains the survival, so it is reported over the
	// generic text when there is one.
	if signalErr != nil {
		return ActionKillFailed, signalErr.Error()
	}
	return ActionKillFailed, fmt.Sprintf("process group %d outlived SIGKILL", pid)
}

// unrecognised resolves the group a signal stage left without any of the run's
// own members in it. Empty is the run gone and the only clean answer; anything
// else is members the pass cannot authenticate — a replacement group that took
// the id, or survivors the run forked out of a signal handler, and from here the
// two are indistinguishable. Neither may be signalled — one is not ours, the
// other cannot be shown to be — so the row is held per Restart's rule. The stage
// is named because a group left unrecognised after the SIGTERM and one left
// after the SIGKILL are different states to debug.
func unrecognised(pid, strangers int, sig syscall.Signal) (Action, string) {
	if strangers == 0 {
		return ActionKilled, ""
	}
	return ActionUnresolved, fmt.Sprintf("process group %d holds %d unrecognised member(s) after %s",
		pid, strangers, unix.SignalName(sig))
}

// signalGroup signals the whole group. ESRCH is not a failure: the group
// emptied between the check and the signal.
func signalGroup(pid int, sig syscall.Signal) error {
	err := sendSignal(-pid, sig)
	if err == nil || errors.Is(err, syscall.ESRCH) {
		return nil
	}
	return fmt.Errorf("kill group %d with %s: %w", pid, sig, err)
}

// originalSurvives polls the group for up to killGrace and reports whether any
// of the members it held before the signals is still there, along with how many
// members the deciding listing held. The group and not the leader: a descendant
// that ignored the SIGTERM keeps it populated after the leader is gone, and it
// is the one the SIGKILL is for.
//
// The poll stops at the first listing holding no original member, whether it is
// empty or not. Waiting longer decides nothing: neither a replacement group that
// took the id nor a survivor the run forked will disappear, so the count is what
// the caller needs to tell "the run is gone" from "something is left that cannot
// be authenticated".
//
// A listing that fails ends the poll with the error: an unreadable listing
// cannot establish that the run's own processes are gone, and the caller keeps
// the row in flight rather than assuming either way.
func originalSurvives(pid int, original []member) (bool, int, error) {
	deadline := time.Now().Add(killGrace)
	for {
		now, err := listGroup(pid)
		if err != nil {
			return false, 0, err
		}
		if !holdsAny(now, original) {
			return false, len(now), nil
		}
		if time.Now().After(deadline) {
			return true, len(now), nil
		}
		time.Sleep(killPoll)
	}
}

// holdsAny reports whether the listing still holds one of the original members.
func holdsAny(now, original []member) bool {
	for _, o := range original {
		if holds(now, o) {
			return true
		}
	}
	return false
}

// holds reports whether the listing holds this exact process. A process carrying
// the member's pid with another start time is not that member: it is what took
// the pid after it died.
func holds(now []member, m member) bool {
	for _, n := range now {
		if n.PID == m.PID && n.Start.Equal(m.Start) {
			return true
		}
	}
	return false
}
