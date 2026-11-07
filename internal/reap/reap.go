package reap

import (
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
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

// lookupStart and lookupArgv are the kernel identity lookups identify runs
// against a live pid. Vars so a test can drive the lookup-error path, which no
// process this daemon can start produces on demand.
var (
	lookupStart = processStart
	lookupArgv  = processArgv
)

// The runtime argument pair that carries the run directory into the launched
// argv (§7.4): --settings <run_dir>/settings.json. It is the run's identity,
// because the run id is in that path.
const (
	settingsFlag       = "--settings"
	settingsFile       = "settings.json"
	sandboxProfileFlag = "--sandbox-profile"
	sandboxProfileFile = "srt.json"
)

// The worker argument pairs that name a hosted segment (weft-service/v2
// §11.1): a segment launched as an agent user carries no sandbox profile, and
// these two together are its identity.
const (
	weftRunFlag     = "--run"
	weftSegmentFlag = "--segment"
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
	// Comm is the kernel's short command name, where the listing carried it;
	// it names the process in a log line and identifies nothing.
	Comm string
}

// Action is what the pass did with one row's recorded pid.
type Action string

const (
	ActionNoPID Action = "no_pid" // the row was claimed with no pid recorded; nothing to check
	// ActionDead is the run gone with nothing having received a signal: the pid
	// is gone and the group it led holds nothing, or the kernel found no member
	// of the group to deliver to.
	ActionDead   Action = "dead"
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
	// signalled: a group whose pre-signal snapshot does not hold the process
	// identify established, a dead leader whose group still holds live members,
	// or an identity lookup that errored on a live pid. The row stays in flight
	// per Restart's rule.
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
	// unresolved, the text of a lookup or kill error, or — on a kill that
	// succeeded through the SIGKILL — the note that it rested on continuity
	// rather than on a member of the pre-SIGTERM listing; empty otherwise. A
	// kill that failed reports why it failed instead, which is the more useful
	// of the two on a row that is being held.
	Detail string
}

// Ledger is the subset of the run ledger the pass uses, declared here on the
// consumer side.
type Ledger interface {
	InFlight() ([]*ledger.Run, error)
	MarkUnknown(id string) error
	WeftCodingWorktree(ledgerRunID string) (string, bool, error)
	WeftSegmentAgent(ledgerRunID string) (ledger.WeftAgentSegment, bool, error)
}

// contain is the sweep Restart runs for a hosted coding segment. A var so a
// test can drive a boundary that will not empty.
var contain = Contain

// killUser is the kill Restart runs for a hosted segment's agent user. A var
// so a test can substitute the kill command and process table.
var killUser = KillUser

var _ Ledger = (*ledger.Store)(nil)

// Restart is §5's restart pass: every row a previous daemon left in flight is
// checked against its recorded pid, the processes proven to be the run's own
// are killed by process group, and the row is then marked unknown — the daemon
// never adopts or resumes one of these runs, and closing the row is what stops
// every later slot for the agent being an overlap skip.
//
// A row is closed only once the pass has established that no process of the run
// remains, or that the recorded pid belongs to something that is not the run.
// Short of that it stays in flight — the kill that failed, the group whose
// pre-signal snapshot could not be authenticated, the dead leader whose group
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
// A hosted coding segment's row is held the same way until containRow's sweep
// establishes that no detached descendant still holds the segment's sandbox
// grant: the group being empty is not the segment being gone. A hosted
// segment that runs as an agent user is held until every process of that
// user is gone, killed before its worker's group and again after it.
//
// It returns one Outcome per row, in ledger order. A MarkUnknown failure, or a
// ledger read that cannot say whether a row is a coding segment or which
// agent user it holds, stops the pass and is returned with the outcomes
// recorded so far: the daemon must not go on to tick over a row it could not
// close.
func Restart(l Ledger) ([]Outcome, error) {
	runs, err := l.InFlight()
	if err != nil {
		return nil, err
	}
	outcomes := make([]Outcome, 0, len(runs))
	for _, run := range runs {
		out := Outcome{RunID: run.ID, Agent: run.Agent, PID: int(run.PID.Int64)}
		var agent *ledger.WeftAgentSegment
		a, holds, err := l.WeftSegmentAgent(run.ID)
		if err != nil {
			out.Action, out.Detail = ActionUnresolved, "agent user: "+err.Error()
			return append(outcomes, out), fmt.Errorf("run %s: %w", run.ID, err)
		}
		if holds {
			agent = &a
		}
		swept := ""
		if agent != nil && run.PID.Valid && run.PID.Int64 > 0 {
			// The agent's processes share the worker's process group and
			// session, where the operator's signals do not reach them, so they
			// go first: the group kill below would otherwise outlast its
			// SIGKILL. The user is this row's alone, so nothing of it is
			// foreign. A row with no pid never released its worker's start
			// gate, and nothing ran as its user.
			found, err := killUser(agent.AgentUser)
			if err != nil {
				out.Action, out.Detail = ActionKillFailed, "agent user: "+err.Error()
				outcomes = append(outcomes, out)
				continue
			}
			swept = killedDetail(agent.AgentUser, found)
		}
		action, leader, detail := identify(run, agent)
		if action == actionOwned {
			out.Action, out.Detail = kill(leader)
		} else {
			out.Action, out.Detail = action, detail
		}
		if out.Action != ActionKillFailed && out.Action != ActionUnresolved && out.Action != ActionNoPID {
			if out.Action, out.Detail, err = containRow(l, run, agent, out.Action, out.Detail); err != nil {
				// No sweep ran, so the row is held: an outcome left closing would
				// let the daemon's reconcile release the repository unswept.
				out.Action, out.Detail = ActionUnresolved, "containment: "+err.Error()
				return append(outcomes, out), fmt.Errorf("run %s: %w", run.ID, err)
			}
		}
		out.Detail = joinDetail(out.Detail, swept)
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

// containRow finishes a hosted segment's containment once its process group
// is accounted for. A segment that runs as an agent user has every process of
// that user killed, which reaches a descendant that called setsid, and the
// row may close only once none is left; a failure holds the row kill_failed.
// A coding segment launched before agent users has its sandbox boundary
// swept: a descendant that called setsid left the group with the segment's
// write grant, and the row may close only once no process holds it. A sweep
// that fails holds the row in flight — kill_failed when it stopped a holder,
// unresolved otherwise — so its repository stays reserved. A row with no pid
// never released its worker's start gate, so nothing of it ever ran, and the
// caller skips it.
func containRow(l Ledger, run *ledger.Run, agent *ledger.WeftAgentSegment, action Action, detail string) (Action, string, error) {
	if agent != nil {
		found, err := killUser(agent.AgentUser)
		if err != nil {
			return ActionKillFailed, joinDetail(detail, "agent user: "+err.Error()), nil
		}
		return action, joinDetail(detail, killedDetail(agent.AgentUser, found)), nil
	}
	_, coding, err := l.WeftCodingWorktree(run.ID)
	if err != nil || !coding {
		return action, detail, err
	}
	c, err := contain(SegmentBoundary(run.RunDir.String))
	switch {
	case err != nil && len(c.Stopped) > 0:
		return ActionKillFailed, fmt.Sprintf("containment %s: %v", c, err), nil
	case err != nil:
		return ActionUnresolved, "containment: " + err.Error(), nil
	case c.Found():
		return action, joinDetail(detail, "containment "+c.String()), nil
	}
	return action, detail, nil
}

// killedDetail names what a kill of user found, "" when it found nothing.
func killedDetail(user string, found []UserProcess) string {
	if len(found) == 0 {
		return ""
	}
	names := make([]string, len(found))
	for i, p := range found {
		names[i] = p.String()
	}
	return fmt.Sprintf("agent user %s: killed %d process(es) %s", user, len(found), strings.Join(names, " "))
}

func joinDetail(detail, more string) string {
	switch {
	case more == "":
		return detail
	case detail == "":
		return more
	}
	return detail + "; " + more
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
// and the row is held rather than closed (Restart). agent is the hosted
// segment the row is when it runs as an agent user, whose worker carries no
// sandbox profile and is established by its run and segment instead.
func identify(run *ledger.Run, agent *ledger.WeftAgentSegment) (Action, member, string) {
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
	argv, err := lookupArgv(pid)
	if err != nil {
		return ActionUnresolved, member{}, lookupFailed + err.Error()
	}
	if agent != nil {
		segment := strconv.Itoa(agent.Segment)
		if !carries(argv, weftRunFlag, agent.RunID) || !carries(argv, weftSegmentFlag, segment) {
			return ActionForeign, member{}, fmt.Sprintf("process argv does not carry %s %s and %s %s",
				weftRunFlag, agent.RunID, weftSegmentFlag, segment)
		}
		return actionOwned, member{PID: pid, Start: started}, ""
	}
	if !runArgv(argv, dir) {
		return ActionForeign, member{}, fmt.Sprintf("process argv does not carry %s %s or %s %s",
			settingsFlag, filepath.Join(dir, settingsFile),
			sandboxProfileFlag, filepath.Join(dir, sandboxProfileFile))
	}
	return actionOwned, member{PID: pid, Start: started}, ""
}

// runArgv reports whether argv carries the settings argument or sandbox
// profile beneath dir.
func runArgv(argv []string, dir string) bool {
	return carries(argv, settingsFlag, filepath.Join(dir, settingsFile)) ||
		carries(argv, sandboxProfileFlag, filepath.Join(dir, sandboxProfileFile))
}

// carries reports whether argv holds flag immediately followed by value. The
// two elements must be adjacent and the value must match exactly — a process
// that merely mentions it, in its exec path, in some other argument or in its
// environment, is not the run and must never be signalled.
func carries(argv []string, flag, value string) bool {
	for i := 0; i+1 < len(argv); i++ {
		if argv[i] == flag && argv[i+1] == value {
			return true
		}
	}
	return false
}

// leaderGone is the answer for a recorded pid the kernel no longer knows. The
// leader being gone is not the run being gone: the group it led can still hold
// live members — a tool child that outlived the CLI. They are never signalled
// all the same. killGroup's continuity runs from a snapshot that held the process
// identify established, and there is no such observation here — the leader died
// before this pass ever listed the group, so the group may have emptied and the
// id been handed on at any point since. The row is held per Restart's rule; a
// listing that cannot be read establishes nothing about the group at all, so it
// is held the same way.
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
// anything could be delivered, ActionUnresolved for a snapshot that cannot be
// authenticated as the run's, and ActionKillFailed for a group that outlived the
// SIGKILL or a signal the kernel refused. A kill that succeeded through the
// SIGKILL carries a detail when it rested on continuity rather than on a member
// of the pre-SIGTERM listing, since the action alone does not say which, and
// only one of the two is established on an observed member.
//
// The group is authenticated once, before anything is signalled: the snapshot
// has to hold the process identify established, or nothing is sent. Past that
// point every member the group holds is the run's by continuity. A process group
// id is the pid of the process that created it, so a replacement group with this
// id needs a new process allocated this pid, and the kernel will not allocate a
// pid that is in use as a pgid — XNU's alloc_pid skips it via pgfind_internal,
// Linux keeps the leader's struct pid alive while a PIDTYPE_PGID reference
// exists. While the group holds any member its id cannot have been handed on, so
// a group never observed empty is still the same group, and a descendant that
// appeared after the snapshot — the child a leader forked microseconds later,
// the process a SIGTERM handler forked — is the run's to kill. That is the rule
// the snapshot's non-leader members are already killed under, since only the
// leader is ever authenticated individually; continuity widens it from members
// at the snapshot instant to members at any instant while the group stayed
// non-empty.
//
// The converse is load-bearing and retained: a group that has been observed
// empty is never signalled again. These are orphans, init reaps the last member
// the instant it exits and the id is free for reuse from then on, so the SIGKILL
// is conditional on a listing that still holds members.
//
// The residual is a group that empties and is entirely replaced inside one
// killPoll gap: the same coincidence the listing-to-signal window already
// carries, now as wide as the poll interval. It still needs the pid allocator to
// wrap back around onto this pgid inside that gap. There is no pidfd-style handle
// to hold a group of orphans open on macOS, so every stage can only list and then
// signal; the window is not closed.
func killGroup(leader member) (Action, string) {
	pid := leader.PID
	// Snapshotted before the SIGTERM. It is trusted only because it still holds
	// the process identify established: a listing that cannot be read, or one
	// that does not hold it, fails closed. This is the only authentication the
	// group gets, so nothing is signalled until it passes.
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
	var signalErr error
	if err := signalGroup(pid, syscall.SIGTERM); err != nil {
		if errors.Is(err, errGroupGone) {
			// The group emptied between the snapshot and the send, and the
			// kernel said so rather than delivering anything. Polling from here
			// decides nothing: the id is already free, so a member a listing
			// answers with is one the pass has no claim on.
			return ActionDead, "group emptied before the SIGTERM was delivered"
		}
		signalErr = err
	}
	empty, last, err := groupEmpties(pid)
	if err != nil {
		return ActionKillFailed, err.Error()
	}
	if empty {
		return ActionKilled, ""
	}
	// Still holding members, which are the run's by continuity — a descendant
	// that ignored the SIGTERM, or one forked after the snapshot was taken.
	//
	// Which of those the SIGKILL went to is recorded, because the action alone
	// cannot be read after the fact and the two are not equally established. A
	// group still holding a member of the snapshot cannot have been handed on at
	// all while that member is in it, so the signal is proven to reach the run's
	// own. A group holding none of them is the residual above: still the run's
	// on the continuity argument, but no longer on an observed member, and an
	// operator has no other way to see that the accepted window was the one this
	// kill relied on.
	continuity := ""
	if !holdsAny(last, original) {
		continuity = fmt.Sprintf(
			"process group %d held no member of the pre-SIGTERM listing when the SIGKILL was sent", pid)
	}
	if e := signalGroup(pid, syscall.SIGKILL); e != nil {
		if errors.Is(e, errGroupGone) {
			// Empty at the send, so the SIGTERM had already carried the group
			// off. The continuity detail still stands: it records what the
			// escalation rested on, not what the last signal did.
			return ActionKilled, continuity
		}
		signalErr = e
	}
	empty, _, err = groupEmpties(pid)
	if err != nil {
		return ActionKillFailed, err.Error()
	}
	if empty {
		return ActionKilled, continuity
	}
	// A refused signal explains the survival, so it is reported over the
	// generic text when there is one.
	if signalErr != nil {
		return ActionKillFailed, signalErr.Error()
	}
	return ActionKillFailed, fmt.Sprintf("process group %d outlived SIGKILL", pid)
}

// errGroupGone is signalGroup's answer for ESRCH, which is the kernel reporting
// that the group has no members: not a failed signal but the group observed
// empty, by the one probe that cannot race the send it is part of. It is
// distinct from nil because a group seen empty may not be signalled again — its
// id is free for reuse from that instant, so a listing taken afterwards can be
// answering for a group this pass never authenticated.
var errGroupGone = errors.New("process group holds no members")

// signalGroup signals the whole group, answering errGroupGone when the kernel
// found nothing in it.
func signalGroup(pid int, sig syscall.Signal) error {
	err := sendSignal(-pid, sig)
	if err == nil {
		return nil
	}
	if errors.Is(err, syscall.ESRCH) {
		return errGroupGone
	}
	return fmt.Errorf("kill group %d with %s: %w", pid, sig, err)
}

// groupEmpties polls the group for up to killGrace and reports whether it became
// empty, along with the last listing it read when it did not. Empty is the only
// evidence the run's processes are gone: these are not this daemon's children,
// so there is no wait to sit on, and while the group holds anything at all it is
// still the group the snapshot authenticated. The listing comes back so the
// caller can say how established the members it is about to signal are, which
// decides nothing but is the difference between the two ways a kill can land.
//
// A listing that fails ends the poll with the error: an unreadable listing
// cannot establish that the group emptied, and the caller keeps the row in
// flight rather than assuming either way.
func groupEmpties(pid int) (bool, []member, error) {
	deadline := time.Now().Add(killGrace)
	for {
		now, err := listGroup(pid)
		if err != nil {
			return false, nil, err
		}
		if len(now) == 0 {
			return true, nil, nil
		}
		if time.Now().After(deadline) {
			return false, now, nil
		}
		time.Sleep(killPoll)
	}
}

// holdsAny reports whether the listing still holds one of the members the group
// was snapshotted with.
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
