package weftremote

import (
	"bytes"
	"errors"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/EnderRealm/ticket/v8/pkg/ticket"

	"shuttle/internal/gitenv"
	"shuttle/internal/ledger"
)

// cleanupWorkerScript is the fixture worker with a cleanup that does what
// weft-worker's does to git: `git worktree remove` without force on the
// worktree recorded for the ticket, done when git removed it, refused with
// git's own reason when it would not. Every other subcommand is the fixture
// worker's.
const cleanupWorkerScript = `#!/bin/sh
control=CONTROL
if [ "$1" != cleanup ]; then exec INNER "$@"; fi
printf '%s\n' "$*" >> "$control/argv.log"
run= segment= ticket= repo=
while [ "$#" -gt 0 ]; do
  case "$1" in
    --run) run="$2"; shift 2 ;;
    --segment) segment="$2"; shift 2 ;;
    --ticket) ticket="$2"; shift 2 ;;
    --repo) repo="$2"; shift 2 ;;
    *) shift ;;
  esac
done
slug=$(printf '%s' "$ticket" | tr / -)
worktree=$(cat "$control/$slug.worktree")
if out=$(git -C "$repo" worktree remove "$worktree" 2>&1); then
  result=done removed=true detail=null
else
  result=refused removed=false
  detail="\"$(printf '%s' "$out" | tr '\n' ' ' | sed -e 's/\\/\\\\/g' -e 's/"/\\"/g')\""
fi
printf '{"contract_version":"weft-worker/v2","run_id":"%s","worker_run_id":null,"segment":%s,"seq":1,"ts":"2026-11-07T12:00:00.000Z","event":"maintained","op":"cleanup","result":"%s","worktree_removed":%s,"archive_ref":null,"head":null,"detail":%s}\n' "$run" "$segment" "$result" "$removed" "$detail"
`

// newHostCleanupHost is a landing host started as the daemon starts it,
// whose projects are real git checkouts and whose worker's cleanup runs git.
func newHostCleanupHost(t *testing.T) *queueHost {
	t.Helper()
	h := newLandingHost(t)
	for _, name := range keysOf(h.projects) {
		dir := h.projects[name]
		if err := os.RemoveAll(filepath.Join(dir, ".git")); err != nil {
			t.Fatal(err)
		}
		h.git(dir, "init", "-q", "-b", "main")
		h.git(dir, "commit", "-q", "--allow-empty", "-m", "base")
	}
	inner := filepath.Join(h.root, "weft-worker-fixture")
	if err := os.WriteFile(inner, []byte(strings.Replace(v2WorkerScript, "CONTROL", h.control, 1)), 0o700); err != nil {
		t.Fatal(err)
	}
	h.worker(strings.NewReplacer("CONTROL", h.control, "INNER", inner).Replace(cleanupWorkerScript))
	if err := h.service.Restart(); err != nil {
		t.Fatal(err)
	}
	h.service.probes.Wait()
	return h
}

// git runs git in dir, isolated from the operator's configuration.
func (h *queueHost) git(dir string, args ...string) string {
	h.t.Helper()
	out, err := h.gitResult(dir, args...)
	if err != nil {
		h.t.Fatalf("git %s: %v: %s", strings.Join(args, " "), err, out)
	}
	return out
}

func (h *queueHost) gitResult(dir string, args ...string) (string, error) {
	cmd := exec.Command("git", append([]string{"-C", dir, "-c", "user.name=shuttle", "-c", "user.email=shuttle@example.com",
		"-c", "commit.gpgsign=false"}, args...)...)
	cmd.Env = append(gitenv.Env(), "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_NOSYSTEM=1")
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	err := cmd.Run()
	return out.String(), err
}

// addWorktree gives the run the worktree and branch its worker would have
// left, and records it where the fixture worker's cleanup finds it.
func (h *queueHost) addWorktree(run *ledger.WeftRun) {
	h.t.Helper()
	h.git(run.Repo, "worktree", "add", "-q", "-b", run.Branch, run.Worktree)
	h.writeControl(slugOf(run.Ticket)+".worktree", []byte(run.Worktree))
}

// registered is whether git still lists the run's worktree.
func (h *queueHost) registered(run *ledger.WeftRun) bool {
	h.t.Helper()
	return strings.Contains(h.git(run.Repo, "worktree", "list", "--porcelain"), "/"+slugOf(run.Ticket)+"\n")
}

func (h *queueHost) branchKept(run *ledger.WeftRun) bool {
	h.t.Helper()
	_, err := h.gitResult(run.Repo, "rev-parse", "--verify", "--quiet", "refs/heads/"+run.Branch)
	return err == nil
}

// hostEntry is one cleanup entry of the run as the ledger holds it, with its
// provenance.
type hostEntry struct {
	id, token, key, state, reason, detail string
}

func (h *queueHost) cleanupEntries(runID string) []hostEntry {
	h.t.Helper()
	rows, err := h.db().Query(`SELECT id,token_id,request_key,state,COALESCE(invalidated_reason,''),
		COALESCE(invalidated_detail,'') FROM weft_entries WHERE run_id=? AND kind='cleanup' ORDER BY position`, runID)
	if err != nil {
		h.t.Fatal(err)
	}
	defer rows.Close()
	var out []hostEntry
	for rows.Next() {
		var e hostEntry
		if err := rows.Scan(&e.id, &e.token, &e.key, &e.state, &e.reason, &e.detail); err != nil {
			h.t.Fatal(err)
		}
		out = append(out, e)
	}
	return out
}

func (h *queueHost) hold(runID string) bool {
	h.t.Helper()
	var hold bool
	if err := h.db().QueryRow(`SELECT host_cleanup_hold FROM weft_runs WHERE id=?`, runID).Scan(&hold); err != nil {
		h.t.Fatal(err)
	}
	return hold
}

// finish writes the ticket's status in tk as a hand edit outside Shuttle
// would, and runs the host's watch of tk once.
func (h *queueHost) finish(id string, status ticket.Status) {
	h.t.Helper()
	h.ticket(id, status, 2)
	if _, err := h.service.syncCatalog(); err != nil {
		h.t.Fatal(err)
	}
}

// restart is the daemon starting again over the same ledger: §12's pass,
// which evaluates every retained run of every project.
func (h *queueHost) restart() {
	h.t.Helper()
	restarted, err := New(h.service.cfg, h.store, "secret-token", h.root, h.service.log)
	if err != nil {
		h.t.Fatal(err)
	}
	restarted.segments.findKey = h.service.segments.findKey
	restarted.segments.sharedRoot = h.service.segments.sharedRoot
	restarted.segments.killUser = h.service.segments.killUser
	h.service = restarted
	if err := restarted.Restart(); err != nil {
		h.t.Fatal(err)
	}
	restarted.probes.Wait()
}

func (h *queueHost) parkedRun(id string) *ledger.WeftRun {
	h.t.Helper()
	parked := decode(h.t, fixtureBytes(h.t, "worker-parked.json")).(map[string]any)
	parked["worker_run_id"] = fixtureWorkerRun
	parked["summary"].(map[string]any)["report"].(map[string]any)["ticket"] = id
	h.script(id, 1, workerLines{h.landingSegment(id, "worker-segment.ndjson")[0], parked})
	return h.waitPhase(h.waitDispatched(entryOf(h.launch(id, "parks-"+slugOf(id)))["entry_id"].(string)), ledger.WeftPhaseParked)
}

func (h *queueHost) haltedRun(id string) *ledger.WeftRun {
	h.t.Helper()
	h.script(id, 1, h.halted(id))
	run := h.waitReaped(h.waitDispatched(entryOf(h.launch(id, "halts-"+slugOf(id)))["entry_id"].(string)))
	if run.Outcome.String != "halted" || run.Disposition != ledger.WeftDispositionRetained || run.SettlementState != ledger.WeftSettlementNone {
		h.t.Fatalf("halted run = %+v", run)
	}
	return run
}

// A retained run whose ticket is finished outside Shuttle is cleaned by the
// host with no request (§6.8): the catalog change admits the cleanup entry a
// dismiss would, dispatched with it, and its worker removes the worktree
// without force and keeps the branch. A parked run so cleaned is ended as a
// stop ends it, its question withdrawn; a run whose landing finishes its
// ticket is cleaned in the commit that reaps it.
func TestHostCleansRetainedRunOnceTicketFinished(t *testing.T) {
	h := newHostCleanupHost(t)
	halted, parked, lands := "weft/example-second-4c2d", "loom/example-parked-1a2b", "warp/example-lands-3c4d"
	for _, id := range []string{halted, parked, lands} {
		h.ticket(id, ticket.StatusReady, 2)
	}

	run := h.haltedRun(halted)
	h.addWorktree(run)
	if entries := h.cleanupEntries(run.ID); len(entries) != 0 {
		t.Fatalf("a run whose ticket is ready was queued for cleanup: %+v", entries)
	}
	before := asOf(t, h.store)
	h.finish(halted, ticket.StatusDone)
	var cleaned *ledger.WeftRun
	waitFor(t, func() bool {
		cleaned = h.run(run.ID)
		return cleaned.Disposition == ledger.WeftDispositionCleaned && h.reaped(cleaned)
	})
	if _, err := os.Stat(run.Worktree); !errors.Is(err, os.ErrNotExist) || h.registered(run) {
		t.Fatalf("the worktree is still there: %v\n%s", err, h.git(run.Repo, "worktree", "list"))
	}
	if !h.branchKept(run) {
		t.Fatalf("the cleanup removed branch %s", run.Branch)
	}
	if cleaned.Outcome.String != "halted" || cleaned.Phase != ledger.WeftPhaseEnded || len(cleaned.Segments) != 2 ||
		cleaned.Segments[1].Kind != ledger.WeftSegmentCleanup || len(cleaned.Stages) != len(run.Stages) {
		t.Fatalf("cleaned run = %+v", cleaned)
	}
	entries := h.cleanupEntries(run.ID)
	if len(entries) != 1 || entries[0].token != ledger.WeftHostToken || entries[0].key != entries[0].id || entries[0].state != "dispatched" {
		t.Fatalf("host-started entry = %+v", entries)
	}
	entry := h.entry(entries[0].id)
	entryShape(t).check(t, "entry", entry)
	if entry["kind"] != "cleanup" || entry["run_id"] != run.ID || entry["revision"] != float64(1) {
		t.Fatalf("host-started entry = %v", entry)
	}
	var requests int
	if err := h.db().QueryRow(`SELECT count(*) FROM weft_requests WHERE path LIKE '%/dismiss'`).Scan(&requests); err != nil || requests != 0 {
		t.Fatalf("dismiss requests = %d %v", requests, err)
	}
	// The stream says what events-host-cleanup.sse does, in the shapes the
	// stream fixtures carry: the catalog change, the entry dispatched at
	// revision 1 under its maintenance row, then the row released and the
	// run cleaned.
	samples := stateSamples(t)
	for name, more := range fixtureEvents(t, "events-host-cleanup.sse") {
		samples[name] = append(samples[name], more...)
	}
	seen := map[string]bool{}
	for _, ev := range h.eventsSince(before) {
		doc := decode(t, ev.Payload).(map[string]any)
		learn(samples[ev.Type]...).check(t, ev.Type, doc)
		switch {
		case ev.Type == "catalog" && doc["project"] == "weft":
			seen["catalog"] = true
		case ev.Type == "queue" && doc["entry"].(map[string]any)["entry_id"] == entries[0].id:
			seen["queue "+doc["entry"].(map[string]any)["state"].(string)] = true
		case ev.Type == "repository" && holderOf(doc["repository"].(map[string]any)) == "maintenance "+entries[0].id:
			seen["reserved"] = true
		case ev.Type == "repository" && seen["reserved"] && doc["repository"].(map[string]any)["reservation"] == nil:
			seen["released"] = true
		case ev.Type == "run" && doc["run_id"] == run.ID && doc["disposition"] == "cleaned":
			seen["cleaned"] = true
		}
	}
	for _, want := range []string{"catalog", "queue dispatched", "reserved", "released", "cleaned"} {
		if !seen[want] {
			t.Errorf("events after the ticket finished lack %s: %v", want, seen)
		}
	}
	if strings.Count(h.argv(), "cleanup --contract weft-worker/v2 --run "+run.ID) != 1 || strings.Contains(h.argv(), "force") {
		t.Fatalf("argv = %s", h.argv())
	}
	if !strings.Contains(h.logs.String(), "entry="+entries[0].id+" run="+run.ID+": admitted cleanup "+halted+" (host-started") {
		t.Errorf("log lacks the host-started admission:\n%s", h.logs.String())
	}

	// A parked run qualifies: it has no process, and its pending settlement
	// has no worker to intend a step. Cleaned, it is ended stopped and its
	// question withdrawn.
	park := h.parkedRun(parked)
	h.addWorktree(park)
	question, err := h.store.PendingWeftQuestion(park.ID)
	if err != nil {
		t.Fatal(err)
	}
	before = asOf(t, h.store)
	h.finish(parked, ticket.StatusClosed)
	waitFor(t, func() bool {
		cleaned = h.run(park.ID)
		return cleaned.Disposition == ledger.WeftDispositionCleaned && h.reaped(cleaned)
	})
	if cleaned.Phase != ledger.WeftPhaseEnded || cleaned.Outcome.String != "stopped" || h.registered(park) || !h.branchKept(park) {
		t.Fatalf("host-cleaned parked run = %+v", cleaned)
	}
	if q, err := h.store.WeftQuestion(question.ID); err != nil || q.State != "withdrawn" {
		t.Fatalf("question of the cleaned parked run = %+v %v", q, err)
	}
	withdrawn := false
	for _, ev := range h.eventsSince(before) {
		if ev.Type == "question" && strings.Contains(string(ev.Payload), question.ID) && strings.Contains(string(ev.Payload), `"withdrawn"`) {
			withdrawn = true
		}
	}
	if !withdrawn {
		t.Fatal("no question event withdrew the parked run's question")
	}

	// A landing finishes its own ticket before it settles, so no catalog
	// evaluation sees it qualify: it has a process and its landing is in
	// motion. The commit that reaps it cleans it.
	lines := h.landingSegment(lands, "worker-segment.ndjson")
	h.script(lands, 1, splice(lines, indexOf(lines, "ended"), nil))
	landing := h.waitDispatched(entryOf(h.launch(lands, "lands"))["entry_id"].(string))
	waitFor(t, func() bool {
		r := h.run(landing)
		return r.Phase == ledger.WeftPhaseSettling && len(r.Steps) == 3 && r.Steps[2].Result.String == "done"
	})
	h.addWorktree(h.run(landing))
	h.finish(lands, ticket.StatusDone)
	if entries := h.cleanupEntries(landing); len(entries) != 0 {
		t.Fatalf("a landing run with its worker live was queued for cleanup: %+v", entries)
	}
	h.release(lands)
	waitFor(t, func() bool {
		cleaned = h.run(landing)
		return cleaned.Disposition == ledger.WeftDispositionCleaned && h.reaped(cleaned)
	})
	if cleaned.SettlementState != ledger.WeftSettlementLanded || h.registered(cleaned) || !h.branchKept(cleaned) {
		t.Fatalf("landed run = %+v", cleaned)
	}
	if entries := h.cleanupEntries(landing); len(entries) != 1 || entries[0].token != ledger.WeftHostToken {
		t.Fatalf("landed run's cleanup = %+v", entries)
	}

	// A ticket finished while the daemon was down is seen by the restart's
	// pass over every retained run, before dispatch starts.
	down := "ticket/example-down-5e6f"
	h.ticket(down, ticket.StatusReady, 2)
	missed := h.haltedRun(down)
	h.addWorktree(missed)
	h.ticket(down, ticket.StatusDone, 2)
	h.restart()
	waitFor(t, func() bool {
		cleaned = h.run(missed.ID)
		return cleaned.Disposition == ledger.WeftDispositionCleaned && h.reaped(cleaned)
	})
	if h.registered(missed) || !h.branchKept(missed) {
		t.Fatalf("run cleaned at restart = %+v", cleaned)
	}
	h.settle()
}

// A retained run whose ticket is still open or ready, or whose landing did
// not land, is left retained: no catalog change and no restart queues a
// cleanup of it (§6.8).
func TestHostLeavesRetainedRunWhileTicketUnfinished(t *testing.T) {
	h := newHostCleanupHost(t)
	ready, open, unlanded := "loom/example-ready-a0b1", "warp/example-open-5e6f", "weft/example-lens-2b3c"
	for _, id := range []string{ready, open, unlanded} {
		h.ticket(id, ticket.StatusReady, 2)
	}
	runs := []*ledger.WeftRun{h.haltedRun(ready), h.haltedRun(open)}
	notLanded, _ := h.unlandedRun(unlanded)
	runs = append(runs, notLanded)
	for _, run := range runs {
		h.addWorktree(run)
	}

	revision, _ := h.eligibilityCodes(ready)
	// A catalog change for each project: another ticket of the ready run's,
	// the open run's own ticket opened, and the unlanded run's ticket done.
	h.ticket("loom/example-other-7a8b", ticket.StatusReady, 2)
	h.finish(open, ticket.StatusOpen)
	h.finish(unlanded, ticket.StatusDone)
	if after, _ := h.eligibilityCodes(ready); after == revision {
		t.Fatalf("loom's catalog_revision did not move: %d", after)
	}
	h.restart()

	for _, run := range runs {
		after := h.run(run.ID)
		if entries := h.cleanupEntries(run.ID); len(entries) != 0 {
			t.Errorf("run %s (%s, landing %s) was queued for cleanup: %+v", run.ID, run.Ticket, after.SettlementState, entries)
		}
		if after.Disposition != ledger.WeftDispositionRetained || !h.registered(run) || h.hold(run.ID) {
			t.Errorf("run %s after evaluation = %+v", run.ID, after)
		}
	}
	if h.run(notLanded.ID).SettlementState != ledger.WeftSettlementNotLanded {
		t.Fatalf("unlanded run = %+v", h.run(notLanded.ID))
	}
	if strings.Contains(h.argv(), "cleanup --contract") || strings.Contains(h.logs.String(), "host-started") {
		t.Fatalf("a cleanup was started:\n%s\n%s", h.argv(), h.logs.String())
	}
}

// A host-started cleanup of a dirty worktree is refused with git's reason:
// the entry is invalidated cleanup_refused, the run, its tree and its branch
// are as they were — a parked run still parked with its question pending —
// and the hold keeps any later catalog change or restart from queueing it
// again (§6.8).
func TestHostCleanupRefusedForDirtyTree(t *testing.T) {
	h := newHostCleanupHost(t)
	halted, parked := "weft/example-dirty-1a2b", "loom/example-parked-3c4d"
	for _, id := range []string{halted, parked} {
		h.ticket(id, ticket.StatusReady, 2)
	}
	runs := []*ledger.WeftRun{h.haltedRun(halted), h.parkedRun(parked)}
	for _, run := range runs {
		h.addWorktree(run)
		if err := os.WriteFile(filepath.Join(run.Worktree, "Sources.swift"), []byte("work in progress\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	h.finish(halted, ticket.StatusDone)
	h.finish(parked, ticket.StatusDone)

	for _, run := range runs {
		var entries []hostEntry
		waitFor(t, func() bool {
			entries = h.cleanupEntries(run.ID)
			return len(entries) == 1 && entries[0].state == "invalidated" && h.reaped(h.run(run.ID))
		})
		if e := entries[0]; e.token != ledger.WeftHostToken || e.reason != ledger.WeftInvalidCleanup ||
			!strings.Contains(e.detail, "contains modified or untracked files") {
			t.Fatalf("refused host cleanup of %s = %+v", run.Ticket, e)
		}
		after := h.run(run.ID)
		if after.Disposition != ledger.WeftDispositionRetained || after.Phase != run.Phase || after.Revision != run.Revision {
			t.Fatalf("a refused cleanup changed run %s: %+v", run.Ticket, after)
		}
		if data, err := os.ReadFile(filepath.Join(run.Worktree, "Sources.swift")); err != nil || string(data) != "work in progress\n" ||
			!h.registered(run) || !h.branchKept(run) {
			t.Fatalf("the refused cleanup touched %s's tree: %v", run.Ticket, err)
		}
		if !h.hold(run.ID) {
			t.Fatalf("run %s has no host cleanup hold", run.Ticket)
		}
	}
	if q, err := h.store.PendingWeftQuestion(runs[1].ID); err != nil || q.State != "pending" {
		t.Fatalf("parked run's question after the refusal = %+v %v", q, err)
	}
	if !strings.Contains(h.logs.String(), "host cleanup hold set") {
		t.Errorf("log lacks the hold:\n%s", h.logs.String())
	}

	// Not retried: the tickets changing again, and a restart, queue nothing.
	h.ticket(halted, ticket.StatusDone, 1)
	h.ticket(parked, ticket.StatusClosed, 2)
	if _, err := h.service.syncCatalog(); err != nil {
		t.Fatal(err)
	}
	h.restart()
	for _, run := range runs {
		if entries := h.cleanupEntries(run.ID); len(entries) != 1 {
			t.Fatalf("a refused run %s was queued again: %+v", run.Ticket, entries)
		}
	}
	if n := strings.Count(h.argv(), "cleanup --contract"); n != 2 {
		t.Fatalf("%d cleanups launched, want the 2 refused:\n%s", n, h.argv())
	}
	// Dismiss stays the operator's path, refused the same way while the
	// tree is dirty.
	run := h.run(runs[0].ID)
	dismissed := h.send(http.MethodPost, "/v2/runs/"+run.ID+"/dismiss", "d-dirty", dismissBody(run.Revision, run.Worktree))
	dismissed.expect(t, http.StatusAccepted, "dismiss-response.json")
	waitFor(t, func() bool {
		return h.entry(entryOf(dismissed)["entry_id"].(string))["state"] == "invalidated" && h.reaped(h.run(run.ID))
	})
	h.settle()
}

// A host-started cleanup waits like any entry on a reserved repository. A
// ticket reopened while it waits invalidates it ticket_ineligible and holds
// nothing, so finishing the ticket again queues another; a client cancelling
// it is a human's refusal, and the host never queues one again (§6.8).
func TestHostCleanupWaitingIsRevalidatedOrCancelled(t *testing.T) {
	h := newHostCleanupHost(t)
	id, holder := "weft/example-second-4c2d", "weft/example-holder-2b3c"
	for _, ticketID := range []string{id, holder} {
		h.ticket(ticketID, ticket.StatusReady, 2)
	}
	run := h.haltedRun(id)
	h.addWorktree(run)
	h.script(holder, 1, splice(h.halted(holder), 1, nil))
	holderRun := h.waitDispatched(entryOf(h.launch(holder, "holds"))["entry_id"].(string))

	h.finish(id, ticket.StatusDone)
	entries := h.cleanupEntries(run.ID)
	if len(entries) != 1 || entries[0].state != "waiting" {
		t.Fatalf("host cleanup behind a held repository = %+v", entries)
	}
	if reason, held := waiting(h.entry(entries[0].id)); reason != "repository_reserved" || held["id"] != holderRun {
		t.Fatalf("host cleanup waits on %s %v", reason, held)
	}
	h.finish(id, ticket.StatusOpen)
	entries = h.cleanupEntries(run.ID)
	if len(entries) != 1 || entries[0].state != "invalidated" || entries[0].reason != ledger.WeftInvalidTicket || h.hold(run.ID) {
		t.Fatalf("host cleanup of a reopened ticket = %+v, hold %v", entries, h.hold(run.ID))
	}

	h.finish(id, ticket.StatusDone)
	entries = h.cleanupEntries(run.ID)
	if len(entries) != 2 || entries[1].state != "waiting" {
		t.Fatalf("host cleanup once the ticket finished again = %+v", entries)
	}
	cancelled := h.send(http.MethodPost, "/v2/queue/"+entries[1].id+"/cancel", "cancel",
		revisionBody(h.entry(entries[1].id)["revision"]))
	cancelled.expect(t, http.StatusOK, "queue-cancel-response.json")
	if !h.hold(run.ID) {
		t.Fatal("a cancelled host cleanup set no hold")
	}
	h.finish(id, ticket.StatusOpen)
	h.finish(id, ticket.StatusClosed)
	h.release(holder)
	h.waitReaped(holderRun)
	if entries = h.cleanupEntries(run.ID); len(entries) != 2 || strings.Contains(h.argv(), "cleanup --contract") || !h.registered(run) {
		t.Fatalf("a cancelled run was queued again: %+v\n%s", entries, h.argv())
	}
	if !strings.Contains(h.logs.String(), "host cleanup hold set") {
		t.Errorf("log lacks the hold:\n%s", h.logs.String())
	}
}

// A re-probe that turns cleanup served evaluates every retained run, however
// often it turns: a ticket finished while a re-probe had cleanup not served
// is cleaned once a later one serves it again (§6.8).
func TestHostCleanupReevaluatedWhenServedAgain(t *testing.T) {
	h := newHostCleanupHost(t)
	id := "weft/example-second-4c2d"
	h.ticket(id, ticket.StatusReady, 2)
	run := h.haltedRun(id)
	h.addWorktree(run)
	// worker_bin changed, so dispatch re-probes it.
	reprobe := func(at time.Time, without ...string) {
		h.writeVersion(without...)
		if err := os.Chtimes(h.service.cfg.Weft.WorkerBin, at, at); err != nil {
			t.Fatal(err)
		}
		h.service.dispatch()
	}

	reprobe(time.Now().Add(-time.Hour), "cleanup")
	if h.service.served("cleanup") {
		t.Fatal("cleanup is still served by a worker without the cleanup capability")
	}
	h.finish(id, ticket.StatusDone)
	if entries := h.cleanupEntries(run.ID); len(entries) != 0 {
		t.Fatalf("a host not serving cleanup queued one: %+v", entries)
	}

	reprobe(time.Now().Add(time.Hour))
	if !h.service.served("cleanup") {
		t.Fatal("cleanup is not served again")
	}
	waitFor(t, func() bool {
		cleaned := h.run(run.ID)
		return cleaned.Disposition == ledger.WeftDispositionCleaned && h.reaped(cleaned)
	})
	if h.registered(run) || !h.branchKept(run) {
		t.Fatalf("run cleaned on the re-probe = %+v", h.run(run.ID))
	}
	h.settle()
}

// A restart whose tk read fails evaluates nothing, and owes the pass: the
// first dispatch that reads tk runs it, so a run retained with its ticket
// finished is not left until the next restart (§6.8, §12).
func TestHostCleanupRestartPassSurvivesUnreadableTK(t *testing.T) {
	h := newHostCleanupHost(t)
	id := "weft/example-second-4c2d"
	h.ticket(id, ticket.StatusReady, 2)
	run := h.haltedRun(id)
	h.addWorktree(run)
	h.ticket(id, ticket.StatusDone, 2)

	// A registered name no project id can be makes every tk read incomplete.
	h.projects["Not_A_Project"] = filepath.Join(h.code, "weft")
	h.writeConfig()
	h.restart()
	if entries := h.cleanupEntries(run.ID); len(entries) != 0 {
		t.Fatalf("a restart that could not read tk queued a cleanup: %+v", entries)
	}
	if !strings.Contains(h.logs.String(), "restart evaluated no run for host cleanup, tk could not be read") {
		t.Fatalf("log lacks the unread restart pass:\n%s", h.logs.String())
	}

	delete(h.projects, "Not_A_Project")
	h.writeConfig()
	h.service.dispatch()
	waitFor(t, func() bool {
		cleaned := h.run(run.ID)
		return cleaned.Disposition == ledger.WeftDispositionCleaned && h.reaped(cleaned)
	})
	if h.registered(run) || !h.branchKept(run) {
		t.Fatalf("run cleaned after tk read again = %+v", h.run(run.ID))
	}
	h.settle()
}
