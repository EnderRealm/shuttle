package weftremote

import (
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/EnderRealm/ticket/v8/pkg/ticket"

	"shuttle/internal/ledger"
)

// Markers the fixture worker acts on: read one command from stdin, or exit
// with no terminal event.
var (
	ackLine = map[string]any{"@": "ACK"}
	dieLine = map[string]any{"@": "DIE"}
)

// fixtureHead is the graded head worker-segment.ndjson lands on fixtureBase.
const fixtureHead = "90e485d97c77c8503459ec274a8fb41f13a1e625"

// newLandingHost is a queue host whose worker is worker-version.json as
// declared, probed as the daemon probes it at start: landing and cleanup are
// served.
func newLandingHost(t *testing.T) *queueHost {
	t.Helper()
	h := newWorkerHost(t)
	h.service.segments.refreshWorker()
	return h
}

func fixtureLines(t *testing.T, name string) workerLines {
	t.Helper()
	var out workerLines
	for _, line := range strings.Split(strings.TrimSpace(string(fixtureBytes(t, name))), "\n") {
		out = append(out, decode(t, []byte(line)).(map[string]any))
	}
	return out
}

// landingSegment is a landing transcript — worker-segment.ndjson, or
// worker-land-failed.ndjson — as this host launches it with --land: its
// worktree and ticket, the probe's capabilities, and the worker reading the
// acknowledgement after each intent.
func (h *queueHost) landingSegment(ticketID, fixture string) workerLines {
	h.t.Helper()
	var out workerLines
	for _, ev := range fixtureLines(h.t, fixture) {
		switch ev["event"] {
		case "hello":
			ev["worktree"], ev["ticket"], ev["branch"], ev["capabilities"] = h.worktree(ticketID), ticketID, ticketID, h.capabilities
			ev["project"], _, _ = strings.Cut(ticketID, "/")
		case "graded":
			ev["summary"].(map[string]any)["report"].(map[string]any)["ticket"] = ticketID
		}
		out = append(out, ev)
		if ev["event"] == "land_intent" {
			out = append(out, ackLine)
		}
	}
	return out
}

// through is lines up to and including the first of event — for step, the
// one for that step — and the acknowledgement that follows it.
func through(t *testing.T, lines workerLines, event, step string) workerLines {
	t.Helper()
	for i, ev := range lines {
		if ev != nil && ev["event"] == event && (step == "" || ev["step"] == step) {
			if i+1 < len(lines) && reflect.DeepEqual(lines[i+1], ackLine) {
				i++
			}
			return append(workerLines{}, lines[:i+1]...)
		}
	}
	t.Fatalf("no %s %s", event, step)
	return nil
}

// reconcileLines is a reconcile transcript in worker-reconcile-crash.ndjson's
// shapes: observed with each step as given, fresh intents for the steps
// taken, each acknowledged and done, and reconciled landed — for head on base.
func reconcileLines(t *testing.T, observed [3]string, head, base string, take ...string) workerLines {
	t.Helper()
	templates := fixtureLines(t, "worker-reconcile-crash.ndjson")
	o := maps.Clone(templates[0])
	o["fast_forward"], o["push"], o["mark_done"], o["commit"] = observed[0], observed[1], observed[2], head
	lines := workerLines{o}
	for _, step := range take {
		intent, result := maps.Clone(templates[1]), maps.Clone(templates[2])
		intent["step"], intent["commit"], intent["base"] = step, head, base
		result["step"], result["commit"] = step, head
		lines = append(lines, intent, ackLine, result)
	}
	reconciled := maps.Clone(templates[3])
	reconciled["landing"] = map[string]any{"state": "landed", "step": nil, "reason": nil, "detail": nil, "branch": "main", "commit": head}
	return append(lines, reconciled)
}

// commands are the stdin commands the fixture worker read in a segment.
func (h *queueHost) commands(ticketID string, segment int) []any {
	h.t.Helper()
	data, err := os.ReadFile(filepath.Join(h.control, fmt.Sprintf("%s-%d.commands", slugOf(ticketID), segment)))
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		h.t.Fatal(err)
	}
	var out []any
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		out = append(out, decode(h.t, []byte(line)))
	}
	return out
}

// acks are worker-commands.ndjson's acknowledgements for steps, for head.
func acks(t *testing.T, head string, steps ...string) []any {
	t.Helper()
	var out []any
	for _, ev := range fixtureLines(t, "worker-commands.ndjson") {
		if ev["command"] == "ack_land_intent" && slices.Contains(steps, ev["step"].(string)) {
			ev["commit"] = head
			out = append(out, ev)
		}
	}
	return out
}

func (h *queueHost) repositoryOf(project string) map[string]any {
	h.t.Helper()
	doc := h.get("/v2/repositories")
	learn(decode(h.t, fixtureBytes(h.t, "repositories.json"))).check(h.t, "repositories", doc)
	for _, r := range doc["repositories"].([]any) {
		if r := r.(map[string]any); r["repository_id"] == h.repositoryID(project) {
			return r
		}
	}
	h.t.Fatalf("repository of %s is not listed", project)
	return nil
}

func holderOf(r map[string]any) string {
	if h, ok := r["reservation"].(map[string]any); ok {
		return h["kind"].(string) + " " + h["id"].(string)
	}
	return ""
}

func (h *queueHost) argv() string {
	h.t.Helper()
	data, err := os.ReadFile(filepath.Join(h.control, "argv.log"))
	if err != nil {
		h.t.Fatal(err)
	}
	return string(data)
}

// eventsSince are the state events written after seq, each held to the
// shapes the stream fixtures show for its name.
func (h *queueHost) eventsSince(seq int64) []ledger.WeftEvent {
	h.t.Helper()
	events, err := h.store.WeftEventsAfter(seq)
	if err != nil {
		h.t.Fatal(err)
	}
	samples := stateSamples(h.t)
	for _, ev := range events {
		learn(samples[ev.Type]...).check(h.t, ev.Type, decode(h.t, ev.Payload))
	}
	return events
}

func asOf(t *testing.T, store *ledger.Store) int64 {
	t.Helper()
	seq, err := store.WeftAsOf()
	if err != nil {
		t.Fatal(err)
	}
	return seq
}

// A landing segment lands through intents the host recorded durably and then
// acknowledged, and reads graded-finished, then landed with the reviewed
// commit. Its terminal event alone releases nothing: the reservation and slot
// stay the live worker's until it is reaped.
func TestLandingSegmentLandsThroughAcknowledgedIntents(t *testing.T) {
	h := newLandingHost(t)
	id := "weft/example-second-4c2d"
	h.ticket(id, ticket.StatusReady, 2)
	graded := indexOf(h.landingSegment(id, "worker-segment.ndjson"), "graded")
	lines := splice(h.landingSegment(id, "worker-segment.ndjson"), graded+1, nil)
	h.script(id, 1, append(lines, nil))
	before := asOf(t, h.store)

	entry := entryOf(h.launch(id, "land-1"))
	runID := h.waitDispatched(entry["entry_id"].(string))
	if run := h.run(runID); run.SettlementState != ledger.WeftSettlementPending {
		t.Fatalf("a run minted while landing is served = %s", run.SettlementState)
	}
	// Held after graded: the grade is finished and nothing has landed.
	waitFor(t, func() bool { return h.run(runID).Phase == ledger.WeftPhaseSettling })
	held := h.get("/v2/runs/" + runID)["run"].(map[string]any)
	if held["outcome"] != "finished" || held["phase"] != "settling" || held["state"] != "active" ||
		held["settlement"].(map[string]any)["state"] != "pending" || len(held["settlement"].(map[string]any)["steps"].([]any)) != 0 {
		t.Fatalf("graded run = %v", held)
	}
	h.launch(id, "land-again").refused(t, http.StatusConflict, "ticket_in_flight")
	h.release(id)

	// Held after ended: landed, and still the live worker's.
	waitFor(t, func() bool { return h.run(runID).SettlementState == ledger.WeftSettlementLanded })
	if run := h.run(runID); h.reaped(run) || holderOf(h.repositoryOf("weft")) != "run "+runID ||
		h.queue()["capacity"].(map[string]any)["running"] != float64(1) {
		t.Fatalf("a terminal event released the live worker's repository or slot: %+v", run)
	}
	h.release(id)
	h.waitReaped(runID)

	doc := h.get("/v2/runs/" + runID)
	runShapeOf(t).check(t, "run", doc)
	run := doc["run"].(map[string]any)
	settlement := run["settlement"].(map[string]any)
	if run["outcome"] != "finished" || run["action_required"] != nil || settlement["state"] != "landed" ||
		settlement["commit"] != fixtureHead || settlement["branch"] != "main" || settlement["reason"] != nil {
		t.Fatalf("landed run = %v", run)
	}
	for i, step := range settlement["steps"].([]any) {
		step := step.(map[string]any)
		if step["step"] != ledger.WeftLandSteps[i] || step["result"] != "done" || step["acked_at"] == nil || step["observed"] != nil {
			t.Fatalf("step %d = %v", i, step)
		}
	}
	learn(decode(t, fixtureBytes(t, "run-result.json"))).check(t, "result", h.get("/v2/runs/"+runID+"/result"))
	if got, want := h.commands(id, 1), acks(t, fixtureHead, ledger.WeftLandSteps...); !reflect.DeepEqual(got, want) {
		t.Fatalf("acknowledgements = %v, want worker-commands.ndjson's %v", got, want)
	}
	if repo := h.repositoryOf("weft"); repo["reservation"] != nil || repo["dispatch"] != "open" {
		t.Fatalf("repository after reap = %v", repo)
	}
	h.refusesResume(runID, "resume-landed")
	if !strings.Contains(h.argv(), "segment --contract weft-worker/v2 --run "+runID) || !strings.Contains(h.argv(), "--land") {
		t.Fatalf("argv = %s", h.argv())
	}
	// Each intent was durable before it was acknowledged: one settlement
	// event per intent carries its acknowledgement, then one per result and
	// the landing; the repository was taken and released with an event each.
	var kinds []string
	for _, ev := range h.eventsSince(before) {
		switch ev.Type {
		case "settlement", "repository":
			kinds = append(kinds, ev.Type)
		}
	}
	if want := []string{"repository", "settlement", "settlement", "settlement", "settlement", "settlement", "settlement",
		"settlement", "repository"}; !reflect.DeepEqual(kinds, want) {
		t.Fatalf("settlement and repository events = %v, want %v", kinds, want)
	}
	ids := fmt.Sprintf("host=%s run=%s", h.service.hostID, runID)
	for _, line := range []string{"launched launch segment to land", "fast_forward intended and acknowledged", "mark_done done", "landing " + ids} {
		if !strings.Contains(h.logs.String(), line) {
			t.Errorf("log lacks %q:\n%s", line, h.logs.String())
		}
	}
}

// A landing segment killed at any step is never replayed: its run goes
// reconciling under a settlement holder, and the reconcile reads what was
// done and takes only what is absent, under fresh intents. No stage reruns,
// and nothing reads landed until the reconcile says so (§7.4, §13).
func TestLostLandingReconcilesAtEveryStep(t *testing.T) {
	for _, tt := range []struct {
		name, event, step string
		observed          [3]string
		take              []string
		acked             []string
		// unread is a worker gone after the host made its intent durable
		// and before it read the acknowledgement.
		unread bool
	}{
		{"before fast_forward", "graded", "", [3]string{"not_done", "not_done", "not_done"}, ledger.WeftLandSteps, nil, false},
		{"after a durable fast_forward intent before its ack", "land_intent", "fast_forward",
			[3]string{"not_done", "not_done", "not_done"}, ledger.WeftLandSteps, nil, true},
		{"after fast_forward before push", "land_step_result", "fast_forward", [3]string{"done", "not_done", "not_done"},
			[]string{"push", "mark_done"}, []string{"fast_forward"}, false},
		{"after push before mark_done", "land_step_result", "push", [3]string{"done", "done", "not_done"},
			[]string{"mark_done"}, []string{"fast_forward", "push"}, false},
		{"after mark_done before its result", "land_intent", "mark_done", [3]string{"done", "done", "done"},
			nil, ledger.WeftLandSteps, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			h := newLandingHost(t)
			id := "weft/example-ready-a0b1"
			h.ticket(id, ticket.StatusReady, 2)
			lost := through(t, h.landingSegment(id, "worker-segment.ndjson"), tt.event, tt.step)
			if tt.unread {
				lost = lost[:len(lost)-1]
			}
			h.script(id, 1, append(lost, dieLine))
			h.script(id, 2, append(workerLines{nil}, reconcileLines(t, tt.observed, fixtureHead, fixtureBase, tt.take...)...))
			runID := h.waitDispatched(entryOf(h.launch(id, "lost"))["entry_id"].(string))

			// The reconcile, held before it observes: the run is reconciling
			// under a settlement holder, its slot free, nothing landed.
			var run *ledger.WeftRun
			waitFor(t, func() bool {
				run = h.run(runID)
				return len(run.Segments) == 2 && run.SettlementState == ledger.WeftSettlementReconciling
			})
			if run.Phase != ledger.WeftPhaseSettling || run.Segments[0].Failure.String != ledger.ClassDied ||
				run.Segments[1].Kind != ledger.WeftSegmentReconcile || holderOf(h.repositoryOf("weft")) != "settlement "+runID {
				t.Fatalf("lost landing = %+v", run)
			}
			for _, step := range run.Steps {
				if step.Observed.Valid {
					t.Fatalf("observed before the reconcile observed: %+v", run.Steps)
				}
			}
			if c := h.queue()["capacity"].(map[string]any); c["running"] != float64(0) {
				t.Fatalf("a reconcile holds a slot: %v", c)
			}
			entries := h.queue()["entries"].([]any)
			if last := entries[len(entries)-1].(map[string]any); last["kind"] != "reconcile" || last["state"] != "dispatched" || last["run_id"] != runID {
				t.Fatalf("reconcile entry = %v", last)
			}
			if got, want := h.commands(id, 1), acks(t, fixtureHead, tt.acked...); !reflect.DeepEqual(got, want) {
				t.Fatalf("the lost segment was acknowledged %v, want %v", got, want)
			}
			h.release(id)

			waitFor(t, func() bool {
				run = h.run(runID)
				return run.SettlementState == ledger.WeftSettlementLanded && h.reaped(run)
			})
			run = h.run(runID)
			doc := h.get("/v2/runs/" + runID)
			runShapeOf(t).check(t, "run", doc)
			if run.Phase != ledger.WeftPhaseEnded || run.ActionRequired.Valid || run.Outcome.String != "finished" || len(run.Segments) != 2 {
				t.Fatalf("reconciled run = %+v", run)
			}
			for _, stage := range run.Stages {
				if stage.Segment != 1 {
					t.Fatalf("a stage ran outside the landing segment: %+v", stage)
				}
			}
			for _, step := range run.Steps {
				if !step.Done() {
					t.Fatalf("landed with %s not done: %+v", step.Step, run.Steps)
				}
				if tt.observed[slices.Index(ledger.WeftLandSteps, step.Step)] == "done" && step.Observed.String != "done" {
					t.Fatalf("%s observed done is not recorded: %+v", step.Step, step)
				}
			}
			if got, want := h.commands(id, 2), acks(t, fixtureHead, tt.take...); !reflect.DeepEqual(got, want) {
				t.Fatalf("the reconcile was acknowledged %v, want %v", got, want)
			}
			// A step the lost segment intended is taken again only under
			// the reconcile's own acknowledged intent.
			for _, step := range run.Steps {
				if slices.Contains(tt.take, step.Step) && (step.Result.String != "done" || !step.AckedAt.Valid ||
					step.IntentAt == "2026-10-02T10:31:12.004Z") {
					t.Fatalf("%s was not retaken under a fresh intent: %+v", step.Step, step)
				}
			}
			if strings.Count(h.argv(), "segment --contract") != 1 ||
				!strings.Contains(h.argv(), "reconcile --contract weft-worker/v2 --run "+runID+" --segment 2 --ticket "+id) ||
				!strings.Contains(h.argv(), "--worker-run "+fixtureWorkerRun+" --commit "+fixtureHead+" --base "+fixtureBase) {
				t.Fatalf("argv = %s", h.argv())
			}
			if repo := h.repositoryOf("weft"); repo["reservation"] != nil || repo["dispatch"] != "open" {
				t.Fatalf("repository after a landed reconcile = %v", repo)
			}
		})
	}
}

// §12 step 2: a daemon killed mid-landing comes back with the landing
// segment closed, its run reconciling under a settlement holder and the
// reconcile claimed under that row; Restart starts it before dispatch, and
// it lands what the lost segment had not, with no stage rerun.
func TestRestartReconcilesALandingSegmentItLost(t *testing.T) {
	h := newLandingHost(t)
	id := "weft/example-ready-a0b1"
	h.ticket(id, ticket.StatusReady, 2)
	if err := h.store.Pause(ledger.WeftAgent); err != nil {
		t.Fatal(err)
	}
	entry := entryOf(h.launch(id, "restart"))
	if err := h.store.Resume(ledger.WeftAgent); err != nil {
		t.Fatal(err)
	}
	c, err := h.service.readCatalog()
	if err != nil {
		t.Fatal(err)
	}
	spawn, _, err := h.store.DispatchWeft(h.service.dispatchConfig(c))
	if err != nil || len(spawn) != 1 {
		t.Fatalf("dispatch = %v %v", spawn, err)
	}
	runID := spawn[0]
	// What the lost worker reported before the daemon died: the push was
	// done and acknowledged, mark_done not yet intended.
	steps := []func() error{
		func() error { return h.store.MarkWeftRunning(runID, 1, 999999, WorkerContractV2) },
		func() error {
			return h.store.RecordWeftLandingHello(runID, 1, fixtureWorkerRun, fixtureBase, "criteria", true)
		},
		func() error {
			return h.store.RecordWeftGraded(runID, 1, "finished", false, "", "", "", "", fixtureHead, json.RawMessage(`{"state":"unavailable","reason":"fixture"}`))
		},
		func() error {
			_, err := h.store.RecordWeftLandIntent(runID, 1, "fast_forward", "main", fixtureHead, fixtureBase, "2026-10-03T08:20:00.000Z")
			return err
		},
		func() error {
			return h.store.RecordWeftLandResult(runID, 1, "fast_forward", "done", "", "2026-10-03T08:20:01.000Z")
		},
		func() error {
			_, err := h.store.RecordWeftLandIntent(runID, 1, "push", "main", fixtureHead, fixtureBase, "2026-10-03T08:20:02.000Z")
			return err
		},
	}
	for _, step := range steps {
		if err := step(); err != nil {
			t.Fatal(err)
		}
	}
	ledgerRun := h.run(runID).Segments[0].LedgerRunID
	if err := h.store.MarkUnknown(ledgerRun); err != nil {
		t.Fatal(err)
	}
	if err := h.store.ReconcileWeftUnknown(ledgerRun); err != nil {
		t.Fatal(err)
	}
	run := h.run(runID)
	if run.SettlementState != ledger.WeftSettlementReconciling || len(run.Segments) != 2 ||
		run.Segments[1].Kind != ledger.WeftSegmentReconcile || holderOf(h.repositoryOf("weft")) != "settlement "+runID {
		t.Fatalf("after the restart pass = %+v", run)
	}
	h.script(id, 2, reconcileLines(t, [3]string{"done", "done", "not_done"}, fixtureHead, fixtureBase, "mark_done"))
	restarted, err := New(h.service.cfg, h.store, "secret-token", h.root, h.service.log)
	if err != nil {
		t.Fatal(err)
	}
	restarted.segments.findKey = h.service.segments.findKey
	h.service = restarted
	if err := restarted.Restart(); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		run = h.run(runID)
		return run.SettlementState == ledger.WeftSettlementLanded && h.reaped(run)
	})
	run = h.run(runID)
	push := run.Steps[1]
	if push.Result.Valid || push.Observed.String != "done" || run.Steps[2].Result.String != "done" || len(run.Stages) != 0 {
		t.Fatalf("restarted landing = %+v", run)
	}
	if strings.Contains(h.argv(), "segment --contract") {
		t.Fatalf("restart relaunched the landing segment: %s", h.argv())
	}
	if e := h.entry(entry["entry_id"].(string)); e["state"] != "dispatched" || e["run_id"] != runID {
		t.Fatalf("launch entry = %v", e)
	}
	h.settle()
}

// A failed push reads not_landed at push, holds the run for the operator
// and pauses that repository only: once reaped the reservation is free for
// repair, other repositories dispatch, a launch there waits paused, and a
// retried reconcile ignores the pause. Only continue lifts it, and not while
// the reconcile runs (§7.5).
func TestUnlandedSettlementPausesOnlyItsRepository(t *testing.T) {
	h := newLandingHost(t)
	id, waits, elsewhere := "weft/example-lens-2b3c", "weft/example-later-3d4e", "loom/example-cache-5e6f"
	for _, ticketID := range []string{id, waits, elsewhere} {
		h.ticket(ticketID, ticket.StatusReady, 2)
	}
	failed := h.landingSegment(id, "worker-land-failed.ndjson")
	h.script(id, 1, failed)
	head, base := failed[indexOf(failed, "graded")]["head"].(string), failed[0]["base"].(string)
	runID := h.waitDispatched(entryOf(h.launch(id, "fails"))["entry_id"].(string))
	run := h.waitReaped(runID)
	doc := h.get("/v2/runs/" + runID)
	runShapeOf(t).check(t, "run", doc)
	r := doc["run"].(map[string]any)
	settlement := r["settlement"].(map[string]any)
	action, _ := r["action_required"].(map[string]any)
	if settlement["state"] != "not_landed" || settlement["step"] != "push" || settlement["reason"] != "failed" || settlement["commit"] != head ||
		!strings.Contains(settlement["detail"].(string), "the push to origin failed") || r["outcome"] != "finished" ||
		action["reason"] != "landing_unsettled" || !reflect.DeepEqual(action["options"], []any{"reconcile", "terminal", "continue"}) {
		t.Fatalf("unlanded run = %v", r)
	}
	repo := h.repositoryOf("weft")
	paused := repo["paused"].(map[string]any)
	if repo["dispatch"] != "paused" || repo["reservation"] != nil || paused["run_id"] != runID || paused["reason"] != "not_landed" {
		t.Fatalf("repository after reap = %v", repo)
	}
	if other := h.repositoryOf("loom"); other["dispatch"] != "open" {
		t.Fatalf("another repository = %v", other)
	}
	h.refusesResume(runID, "resume-held")

	held := entryOf(h.launch(waits, "waits"))
	if reason, _ := waiting(held); reason != "repository_paused" {
		t.Fatalf("launch into the paused repository = %v", held)
	}
	h.script(elsewhere, 1, h.landingSegment(elsewhere, "worker-segment.ndjson"))
	other := h.waitDispatched(entryOf(h.launch(elsewhere, "elsewhere"))["entry_id"].(string))

	// Continue needs the repository's revision.
	h.send(http.MethodPost, "/v2/repositories/"+h.repositoryID("weft")+"/continue", "c-stale", revisionBody(int64(repo["revision"].(float64))-1)).
		refused(t, http.StatusConflict, "revision_conflict")
	h.send(http.MethodPost, "/v2/repositories/nowhere-00000000/continue", "c-none", revisionBody(1)).
		refused(t, http.StatusNotFound, "repository_not_found")
	h.send(http.MethodPost, "/v2/repositories/"+h.repositoryID("loom")+"/continue", "c-open", revisionBody(h.repositoryOf("loom")["revision"])).
		refused(t, http.StatusConflict, "repository_not_paused")

	// The retried reconcile dispatches past the pause under a maintenance row.
	h.script(id, 2, append(workerLines{nil}, reconcileLines(t, [3]string{"done", "not_done", "not_done"}, head, base, "push", "mark_done")...))
	h.send(http.MethodPost, "/v2/runs/"+runID+"/reconcile", "rc-stale", revisionBody(run.Revision-1)).refused(t, http.StatusConflict, "revision_conflict")
	retry := h.send(http.MethodPost, "/v2/runs/"+runID+"/reconcile", "rc", revisionBody(h.run(runID).Revision))
	retry.expect(t, http.StatusAccepted, "reconcile-response.json")
	entryShape(t).check(t, "entry", entryOf(retry))
	if e := entryOf(retry); e["kind"] != "reconcile" || e["state"] != "dispatched" || e["run_id"] != runID {
		t.Fatalf("reconcile entry = %v", e)
	}
	waitFor(t, func() bool { return h.run(runID).Phase == ledger.WeftPhaseSettling })
	if holder := holderOf(h.repositoryOf("weft")); holder != "maintenance "+entryOf(retry)["entry_id"].(string) {
		t.Fatalf("reconcile holder = %s", holder)
	}
	h.send(http.MethodPost, "/v2/repositories/"+h.repositoryID("weft")+"/continue", "c-early", revisionBody(h.repositoryOf("weft")["revision"])).
		refused(t, http.StatusConflict, "settlement_unsettled")
	h.release(id)
	waitFor(t, func() bool {
		run = h.run(runID)
		return run.SettlementState == ledger.WeftSettlementLanded && h.reaped(run)
	})
	run = h.run(runID)
	if run.ActionRequired.Valid || run.Steps[1].Observed.String != "not_done" || run.Steps[1].Result.String != "done" {
		t.Fatalf("retried landing = %+v", run)
	}
	// The pause stands until continue; nothing dispatched into it meanwhile.
	if reason, _ := waiting(h.entry(held["entry_id"].(string))); reason != "repository_paused" {
		t.Fatalf("the paused repository dispatched before continue: %v", h.entry(held["entry_id"].(string)))
	}
	h.send(http.MethodPost, "/v2/runs/"+runID+"/reconcile", "rc-again", revisionBody(run.Revision)).refused(t, http.StatusConflict, "not_reconcilable")
	h.script(waits, 1, h.landingSegment(waits, "worker-segment.ndjson"))
	continued := h.send(http.MethodPost, "/v2/repositories/"+h.repositoryID("weft")+"/continue", "c-ok", revisionBody(h.repositoryOf("weft")["revision"]))
	// The continue dispatched the waiting launch in its transaction, so the
	// repository it answers is held, as repositories.json shows a held one.
	occupied := decode(t, fixtureBytes(t, "repository-continued.json")).(map[string]any)
	occupied["repository"] = decode(t, fixtureBytes(t, "repositories.json")).(map[string]any)["repositories"].([]any)[0]
	learn(decode(t, fixtureBytes(t, "repository-continued.json")), occupied).check(t, "repository-continued.json", continued.body)
	continued.expect(t, http.StatusOK)
	if repo := continued.body["repository"].(map[string]any); repo["dispatch"] != "open" || repo["paused"] != nil {
		t.Fatalf("continued = %v", repo)
	}
	h.waitDispatched(held["entry_id"].(string))
	h.waitReaped(other)
	h.settle()
}

// A reconcile that breaks the step order fails protocol before any step is
// acknowledged; the landing is then unreconciled and stays paused.
func TestReconcileRefusesMarkDoneBeforePush(t *testing.T) {
	h := newLandingHost(t)
	id := "weft/example-lens-2b3c"
	h.ticket(id, ticket.StatusReady, 2)
	// The negative transcript is the retry of worker-land-failed.ndjson's
	// landing: the same commit on the same base.
	h.script(id, 1, h.landingSegment(id, "worker-land-failed.ndjson"))
	runID := h.waitDispatched(entryOf(h.launch(id, "fails"))["entry_id"].(string))
	run := h.waitReaped(runID)
	h.script(id, 2, fixtureLines(t, "negative/worker-mark-done-before-push.ndjson"))
	h.send(http.MethodPost, "/v2/runs/"+runID+"/reconcile", "rc", revisionBody(run.Revision)).expect(t, http.StatusAccepted, "reconcile-response.json")
	waitFor(t, func() bool {
		run = h.run(runID)
		return len(run.Segments) == 2 && h.reaped(run) && run.Phase == ledger.WeftPhaseEnded
	})
	run = h.run(runID)
	if run.SettlementState != ledger.WeftSettlementNotLanded || run.SettlementReason.String != ledger.WeftLandUnreconciled ||
		run.SettlementStep.String != "push" || run.Segments[1].Failure.String != ledger.ClassProtocol ||
		run.ActionRequired.String != ledger.WeftActionLandingUnsettled || h.repositoryOf("weft")["dispatch"] != "paused" {
		t.Fatalf("protocol-failed reconcile = %+v", run)
	}
	if got := h.commands(id, 2); len(got) != 0 {
		t.Fatalf("a step out of order was acknowledged: %v", got)
	}
	if !strings.Contains(h.logs.String(), "land_intent mark_done before push was done") {
		t.Fatalf("log lacks the refusal:\n%s", h.logs.String())
	}
}

// halted is a landing transcript graded halted: it lands nothing, and its
// run is retained with the graded head for cleanup or fresh work.
func (h *queueHost) halted(ticketID string) workerLines {
	h.t.Helper()
	var out workerLines
	for _, ev := range h.landingSegment(ticketID, "worker-segment.ndjson") {
		switch ev["event"] {
		case "land_intent", "land_step_result":
			continue
		case "graded":
			ev["outcome"], ev["halt"] = "halted", map[string]any{"stage": "review", "reason": "rejected", "detail": "two findings stand"}
		case "ended":
			ev["outcome"] = "halted"
			ev["landing"] = map[string]any{"state": "not_applicable", "step": nil, "reason": nil,
				"detail": "the run halted", "branch": nil, "commit": nil}
		}
		if ev["@"] == nil {
			out = append(out, ev)
		}
	}
	return out
}

func maintainedLine(t *testing.T, fixture string, fields map[string]any) workerLines {
	t.Helper()
	ev := decode(t, fixtureBytes(t, fixture)).(map[string]any)
	maps.Copy(ev, fields)
	return workerLines{ev}
}

func dismissBody(revision int64, worktree string) string {
	return fmt.Sprintf(`{"contract_version":"weft-service/v2","revision":%d,"authorized_worktree":%q}`, revision, worktree)
}

// Dismiss revalidates the run and its worktree, queues a cleanup that takes
// the repository and no slot, and its worker removes the worktree without
// force — or refuses, and the run is unchanged (§6.8).
func TestDismissCleansOrIsRefused(t *testing.T) {
	h := newLandingHost(t)
	cleaned, dirty, running := "weft/example-second-4c2d", "loom/example-dirty-1a2b", "warp/example-busy-3c4d"
	for _, id := range []string{cleaned, dirty, running} {
		h.ticket(id, ticket.StatusReady, 2)
	}
	h.script(cleaned, 1, h.landingSegment(cleaned, "worker-segment.ndjson"))
	h.script(dirty, 1, h.halted(dirty))
	h.script(running, 1, splice(h.halted(running), 1, nil))
	landed := h.waitReaped(h.waitDispatched(entryOf(h.launch(cleaned, "l1"))["entry_id"].(string)))
	refusedRun := h.waitReaped(h.waitDispatched(entryOf(h.launch(dirty, "l2"))["entry_id"].(string)))
	busy := h.waitDispatched(entryOf(h.launch(running, "l3"))["entry_id"].(string))
	waitFor(t, func() bool { return h.run(busy).Phase == ledger.WeftPhaseRunning })
	// The worktrees the fixture worker left, as a worker would have.
	for _, id := range []string{cleaned, dirty} {
		if err := os.MkdirAll(h.worktree(id), 0o755); err != nil {
			t.Fatal(err)
		}
	}

	dismiss := func(runID, key, worktree string, revision int64) response {
		return h.send(http.MethodPost, "/v2/runs/"+runID+"/dismiss", key, dismissBody(revision, worktree))
	}
	dismiss(busy, "d-busy", h.worktree(running), h.run(busy).Revision).refused(t, http.StatusConflict, "run_active")
	h.release(running)
	h.waitReaped(busy)
	dismiss(landed.ID, "d-stale", landed.Worktree, landed.Revision-1).refused(t, http.StatusConflict, "revision_conflict")
	dismiss(landed.ID, "d-other", "/tmp/elsewhere", landed.Revision).refused(t, http.StatusConflict, "revision_conflict")
	dismiss("r-20200101T000000Z-00000000", "d-none", "/tmp/x", 1).refused(t, http.StatusNotFound, "run_not_found")
	h.send(http.MethodPost, "/v2/runs/"+landed.ID+"/dismiss", "d-head", fmt.Sprintf(
		`{"contract_version":"weft-service/v2","revision":%d,"authorized_worktree":%q,"authorized_head":%q}`, landed.Revision, landed.Worktree, fixtureHead)).
		refused(t, http.StatusBadRequest, "malformed")

	h.script(cleaned, 2, maintainedLine(t, "worker-maintained-cleanup.json", nil))
	before := asOf(t, h.store)
	accepted := dismiss(landed.ID, "d-ok", landed.Worktree, landed.Revision)
	accepted.expect(t, http.StatusAccepted, "dismiss-response.json")
	entryShape(t).check(t, "entry", entryOf(accepted))
	if e := entryOf(accepted); e["kind"] != "cleanup" || e["state"] != "dispatched" || e["run_id"] != landed.ID {
		t.Fatalf("cleanup entry = %v", e)
	}
	var run *ledger.WeftRun
	waitFor(t, func() bool {
		run = h.run(landed.ID)
		return run.Disposition == ledger.WeftDispositionCleaned && h.reaped(run)
	})
	run = h.run(landed.ID)
	doc := h.get("/v2/runs/" + landed.ID)
	runShapeOf(t).check(t, "run", doc)
	if run.SettlementState != ledger.WeftSettlementLanded || run.Segments[1].Kind != ledger.WeftSegmentCleanup ||
		len(run.Stages) != len(landed.Stages) || doc["run"].(map[string]any)["failure"] != nil {
		t.Fatalf("cleaned run = %+v", run)
	}
	for _, ev := range h.eventsSince(before) {
		if ev.Type == "capacity" && !strings.Contains(string(ev.Payload), `"running":0`) {
			t.Fatalf("a cleanup took a slot: %s", ev.Payload)
		}
	}
	if repo := h.repositoryOf("weft"); repo["reservation"] != nil {
		t.Fatalf("repository after cleanup = %v", repo)
	}
	argv := h.argv()
	if !strings.Contains(argv, "cleanup --contract weft-worker/v2 --run "+landed.ID+" --segment 2 --ticket "+cleaned) || strings.Contains(argv, "force") {
		t.Fatalf("argv = %s", argv)
	}
	dismiss(landed.ID, "d-again", landed.Worktree, run.Revision).refused(t, http.StatusConflict, "worktree_missing")

	h.script(dirty, 2, maintainedLine(t, "worker-maintained-cleanup.json", map[string]any{
		"result": "refused", "worktree_removed": false, "detail": "the worktree has uncommitted changes: Sources/App.swift"}))
	refusal := entryOf(dismiss(refusedRun.ID, "d-dirty", refusedRun.Worktree, refusedRun.Revision))
	waitFor(t, func() bool {
		return h.reaped(h.run(refusedRun.ID)) && h.entry(refusal["entry_id"].(string))["state"] == "invalidated"
	})
	if e := h.entry(refusal["entry_id"].(string)); e["invalidated"].(map[string]any)["reason"] != "cleanup_refused" ||
		!strings.Contains(e["invalidated"].(map[string]any)["detail"].(string), "uncommitted changes") {
		t.Fatalf("refused cleanup = %v", e)
	}
	if run := h.run(refusedRun.ID); run.Disposition != ledger.WeftDispositionRetained || run.Revision != refusedRun.Revision {
		t.Fatalf("a refused cleanup changed the run: %+v", run)
	}
	if err := os.RemoveAll(h.worktree(dirty)); err != nil {
		t.Fatal(err)
	}
	dismiss(refusedRun.ID, "d-gone", refusedRun.Worktree, refusedRun.Revision).refused(t, http.StatusConflict, "worktree_missing")
	h.settle()
}

func freshBody(revision int64, worktree, head string) string {
	return fmt.Sprintf(`{"contract_version":"weft-service/v2","revision":%d,"authorized_worktree":%q,"authorized_head":%q}`,
		revision, worktree, head)
}

// Fresh archives the retained run at the head the confirmation showed under
// a maintenance row and a slot, then in one transaction mints the new run,
// rewrites the row to it and carries the slot; the new run launches with no
// resume of the old one. A refused archive invalidates the entry and frees
// its row and slot, and changes nothing else (§6.7).
func TestFreshArchivesThenStartsANewRun(t *testing.T) {
	h := newLandingHost(t)
	id, refusedID := "weft/example-fourth-6e4f", "loom/example-moved-5e6f"
	for _, ticketID := range []string{id, refusedID} {
		h.ticket(ticketID, ticket.StatusReady, 2)
		h.script(ticketID, 1, h.halted(ticketID))
	}
	old := h.waitReaped(h.waitDispatched(entryOf(h.launch(id, "l1"))["entry_id"].(string)))
	if old.Head.String != fixtureHead || old.SettlementState != ledger.WeftSettlementNone {
		t.Fatalf("halted run = %+v", old)
	}
	h.launch(id, "l1-again").refused(t, http.StatusConflict, "prior_run")
	fresh := func(runID, key, worktree, head string, revision int64) response {
		return h.send(http.MethodPost, "/v2/runs/"+runID+"/fresh", key, freshBody(revision, worktree, head))
	}
	fresh(old.ID, "f-head", old.Worktree, fixtureBase, old.Revision).refused(t, http.StatusConflict, "revision_conflict")
	fresh(old.ID, "f-stale", old.Worktree, fixtureHead, old.Revision+1).refused(t, http.StatusConflict, "revision_conflict")
	h.send(http.MethodPost, "/v2/runs/"+old.ID+"/fresh", "f-short", dismissBody(old.Revision, old.Worktree)).refused(t, http.StatusBadRequest, "malformed")

	h.script(id, 2, append(workerLines{nil}, maintainedLine(t, "worker-maintained-archive.json", map[string]any{
		"archive_ref": ledger.WeftArchiveRef(old.ID), "head": fixtureHead})...))
	accepted := fresh(old.ID, "f-ok", old.Worktree, fixtureHead, old.Revision)
	accepted.expect(t, http.StatusAccepted, "fresh-response.json")
	entry := entryOf(accepted)
	if entry["kind"] != "fresh" || entry["supersedes"] != old.ID || entry["state"] != "archiving" || entry["run_id"] != nil {
		t.Fatalf("fresh entry = %v", entry)
	}
	q := h.queue()
	if !reflect.DeepEqual(q["capacity"], map[string]any{"cap": float64(3), "running": float64(1), "waiting": float64(0)}) ||
		holderOf(h.repositoryOf("weft")) != "maintenance "+entry["entry_id"].(string) {
		t.Fatalf("archiving = %v, repository %v", q, h.repositoryOf("weft"))
	}
	fresh(old.ID, "f-twice", old.Worktree, fixtureHead, h.run(old.ID).Revision).refused(t, http.StatusConflict, "run_active")
	h.launch(id, "l-during").refused(t, http.StatusConflict, "ticket_in_flight")

	before := asOf(t, h.store)
	h.release(id)
	newRun := h.waitDispatched(entry["entry_id"].(string))
	h.waitReaped(newRun)
	archived := h.run(old.ID)
	if archived.Disposition != ledger.WeftDispositionArchived || archived.SupersededBy.String != newRun ||
		archived.ArchiveRef.String != ledger.WeftArchiveRef(old.ID) || archived.Segments[1].Kind != ledger.WeftSegmentArchive {
		t.Fatalf("archived run = %+v", archived)
	}
	if started := h.run(newRun); started.Supersedes.String != old.ID || started.EntryID.String != entry["entry_id"] ||
		started.Segments[0].Kind != "launch" {
		t.Fatalf("new run = %+v", started)
	}
	// The archive's settlement is one transaction: the row rewritten to the
	// new run, the entry dispatched, both runs — and no capacity event, the
	// slot being carried.
	events := h.eventsSince(before)
	var settled []string
	for i, ev := range events {
		if ev.Type == "repository" && strings.Contains(string(ev.Payload), `"id":"`+newRun+`","kind":"run"`) {
			for _, next := range events[i : i+4] {
				settled = append(settled, next.Type)
			}
			break
		}
	}
	if !reflect.DeepEqual(settled, []string{"repository", "queue", "run", "run"}) {
		t.Fatalf("the archive settled as %v", settled)
	}
	argv := h.argv()
	if !strings.Contains(argv, "archive --contract weft-worker/v2 --run "+old.ID+" --segment 2 --ticket "+id+" --repo "+old.Repo+
		" --weft-root "+h.service.cfg.Weft.Root+" --archive-ref "+ledger.WeftArchiveRef(old.ID)+" --head "+fixtureHead) ||
		!strings.Contains(argv, "segment --contract weft-worker/v2 --run "+newRun) || strings.Contains(argv, "--resume") {
		t.Fatalf("argv = %s", argv)
	}
	h.send(http.MethodPost, "/v2/runs/"+old.ID+"/resume", "r-old", revisionBody(archived.Revision)).refused(t, http.StatusConflict, "run_not_resumable")
	fresh(old.ID, "f-archived", old.Worktree, fixtureHead, archived.Revision).refused(t, http.StatusConflict, "run_over")

	other := h.waitReaped(h.waitDispatched(entryOf(h.launch(refusedID, "l2"))["entry_id"].(string)))
	h.script(refusedID, 2, maintainedLine(t, "worker-maintained-archive.json", map[string]any{
		"result": "refused", "worktree_removed": false, "archive_ref": nil, "head": nil,
		"detail": "branch loom/example-moved-5e6f moved past the head the confirmation showed"}))
	refusal := entryOf(fresh(other.ID, "f-refused", other.Worktree, fixtureHead, other.Revision))
	waitFor(t, func() bool {
		return h.entry(refusal["entry_id"].(string))["state"] == "invalidated" && h.reaped(h.run(other.ID))
	})
	if e := h.entry(refusal["entry_id"].(string)); e["invalidated"].(map[string]any)["reason"] != "archive_refused" ||
		!strings.Contains(e["invalidated"].(map[string]any)["detail"].(string), "moved past") {
		t.Fatalf("refused archive = %v", e)
	}
	if run := h.run(other.ID); run.Disposition != ledger.WeftDispositionRetained || run.SupersededBy.Valid {
		t.Fatalf("a refused archive changed the run: %+v", run)
	}
	if c := h.queue()["capacity"].(map[string]any); c["running"] != float64(0) || h.repositoryOf("loom")["reservation"] != nil {
		t.Fatalf("a refused archive kept its slot or row: %v %v", c, h.repositoryOf("loom"))
	}
	h.settle()
}

// A legacy archive imports as history only: archived runs labelled with the
// archive, readable, never live — every command but a read refuses them —
// and a second import of the same archive changes nothing (§14.3).
func TestLegacyArchiveImportIsHistoryOnly(t *testing.T) {
	h := newLandingHost(t)
	body := string(fixtureBytes(t, "archive.json"))
	first := h.send(http.MethodPost, "/v2/archives", "import-1", body)
	first.expect(t, http.StatusCreated, "archive-import-response.json")
	imp := first.body["import"].(map[string]any)
	want := decode(t, fixtureBytes(t, "archive-import-response.json")).(map[string]any)["import"].(map[string]any)
	for _, key := range []string{"archive_id", "origin", "runs_imported", "runs_existing", "run_ids"} {
		if !reflect.DeepEqual(imp[key], want[key]) {
			t.Fatalf("import.%s = %v, archive-import-response.json has %v", key, imp[key], want[key])
		}
	}
	if replay := h.send(http.MethodPost, "/v2/archives", "import-1", body); !replay.replayed || replay.raw != first.raw {
		t.Fatalf("replay = %v %s", replay.replayed, replay.raw)
	}
	again := h.send(http.MethodPost, "/v2/archives", "import-2", body)
	again.expect(t, http.StatusCreated, "archive-import-response.json")
	if !reflect.DeepEqual(again.body["import"], first.body["import"]) {
		t.Fatalf("a second import = %v, the first %v", again.body["import"], first.body["import"])
	}
	changed := strings.Replace(body, `"skipped_lines": 1`, `"skipped_lines": 2`, 1)
	if message := h.send(http.MethodPost, "/v2/archives", "import-3", changed).refused(t, http.StatusConflict, "archive_conflict"); !strings.Contains(message, "ar-20261001T200000Z-5c6d7e8f") {
		t.Fatalf("archive_conflict = %s", message)
	}
	h.send(http.MethodPost, "/v2/archives", "import-4", string(fixtureBytes(t, "negative/archive-unsupported-version.json"))).
		refused(t, http.StatusUpgradeRequired, "upgrade_required")
	overlap := strings.Replace(body, "ar-20261001T200000Z-5c6d7e8f", "ar-20261002T200000Z-6d7e8f90", 1)
	if o := h.send(http.MethodPost, "/v2/archives", "import-5", overlap).body["import"].(map[string]any); o["runs_imported"] != float64(0) || o["runs_existing"] != float64(2) {
		t.Fatalf("an archive of runs already imported = %v", o)
	}

	runs := h.get("/v2/runs?origin=archive")
	learn(decode(t, fixtureBytes(t, "runs.json"))).check(t, "runs", runs)
	listed := runs["runs"].([]any)
	if len(listed) != 2 {
		t.Fatalf("archived runs = %v", listed)
	}
	byID := map[string]map[string]any{}
	for _, r := range listed {
		byID[r.(map[string]any)["run_id"].(string)] = r.(map[string]any)
	}
	// A legacy run that recorded no repo is bound to its project's repository
	// on this host.
	repos := map[string]any{}
	for _, r := range decode(t, fixtureBytes(t, "archive.json")).(map[string]any)["runs"].([]any) {
		r := r.(map[string]any)
		if r["repo"] == nil {
			repos[ledger.WeftArchivedRunID(r["legacy_run_id"].(string))] = h.repositoryID(r["project"].(string))
		}
	}
	for _, r := range decode(t, fixtureBytes(t, "runs.json")).(map[string]any)["runs"].([]any) {
		r := r.(map[string]any)
		got := byID[r["run_id"].(string)]
		if r["origin"] != "archive" {
			continue
		}
		if bound, ok := repos[r["run_id"].(string)]; ok {
			r["repository_id"] = bound
		}
		for _, key := range []string{"ticket", "project", "repository_id", "origin", "recorded_under", "archive_id", "state",
			"phase", "outcome", "failed", "stopped", "action_required", "landing", "disposition", "segment", "created_at", "ended_at"} {
			if !reflect.DeepEqual(got[key], r[key]) {
				t.Errorf("%s.%s = %v, runs.json has %v", r["run_id"], key, got[key], r[key])
			}
		}
	}
	landedID := "x-20260911T101500Z-aa11bb22"
	doc := h.get("/v2/runs/" + landedID)
	runShapeOf(t).check(t, "run", doc)
	run := doc["run"].(map[string]any)
	if run["worktree"] != "/Users/steve/Library/Application Support/weft/worktrees/weft-47327fe5/weft-example-legacy-1c2d" ||
		run["base"] != "344dd4400e2972e4075b8838a0ab460456c05fc1" || run["summary"].(map[string]any)["state"] != "rendered" ||
		len(run["stages"].([]any)) != 2 || len(run["segments"].([]any)) != 0 || run["settlement"].(map[string]any)["commit"] != "30627f175a6c864cb28dcdd9e9e482be38600db6" {
		t.Fatalf("archived run = %v", run)
	}
	revision := int64(run["revision"].(float64))
	for _, refusal := range []struct{ command, body, code string }{
		{"stop", revisionBody(revision), "run_over"},
		{"resume", revisionBody(revision), "run_not_resumable"},
		{"reconcile", revisionBody(revision), "not_reconcilable"},
		{"dismiss", dismissBody(revision, run["worktree"].(string)), "run_over"},
		{"fresh", freshBody(revision, run["worktree"].(string), "30627f175a6c864cb28dcdd9e9e482be38600db6"), "run_over"},
		{"answer", `{"contract_version":"weft-service/v2","question_id":"q-20260911T101500Z-aa11bb22","answer":"Yes."}`, "run_over"},
	} {
		h.send(http.MethodPost, "/v2/runs/"+landedID+"/"+refusal.command, "x-"+refusal.command, refusal.body).
			refused(t, http.StatusConflict, refusal.code)
	}
	if after := h.run(landedID); after.Revision != int64(revision) || len(after.Segments) != 0 {
		t.Fatalf("a refused command changed the archived run: %+v", after)
	}
}

// Stop reaches a landing run: the intent it is waiting on is recorded and
// never acknowledged, the worker takes no step and ends not_landed stopped,
// and its repository pauses (§6.6). The grade it landed under stands.
func TestStopDuringLandingTakesNoStep(t *testing.T) {
	h := newLandingHost(t)
	id := "weft/example-second-4c2d"
	h.ticket(id, ticket.StatusReady, 2)
	lines := h.landingSegment(id, "worker-segment.ndjson")
	ended := maps.Clone(lines[len(lines)-1])
	ended["landing"] = map[string]any{"state": "not_landed", "step": "fast_forward", "reason": "stopped",
		"detail": "stopped while waiting to fast-forward main", "branch": "main", "commit": fixtureHead}
	stopped := append(through(t, lines, "graded", ""), nil)
	stopped = append(stopped, through(t, lines, "land_intent", "fast_forward")[len(stopped)-1:]...)
	h.script(id, 1, append(stopped, ended))
	runID := h.waitDispatched(entryOf(h.launch(id, "stop-landing"))["entry_id"].(string))
	waitFor(t, func() bool { return h.run(runID).Phase == ledger.WeftPhaseSettling })

	stop := h.send(http.MethodPost, "/v2/runs/"+runID+"/stop", "stop", revisionBody(h.run(runID).Revision))
	stop.expect(t, http.StatusOK)
	if r := stop.body["run"].(map[string]any); r["phase"] != "settling" || r["outcome"] != "finished" {
		t.Fatalf("stopped landing run = %v", r)
	}
	waitFor(t, func() bool { return h.service.segments.isStopping(runID) })
	h.release(id)
	run := h.waitReaped(runID)
	if run.SettlementState != ledger.WeftSettlementNotLanded || run.SettlementReason.String != ledger.WeftLandStopped ||
		run.SettlementStep.String != "fast_forward" || run.Outcome.String != "finished" || len(run.Steps) != 1 ||
		run.Steps[0].AckedAt.Valid || run.ActionRequired.String != ledger.WeftActionLandingUnsettled {
		t.Fatalf("stopped landing = %+v", run)
	}
	if got := h.commands(id, 1); !reflect.DeepEqual(got, []any{map[string]any{"contract_version": WorkerContractV2, "command": "stop"}}) {
		t.Fatalf("the worker read %v, want only the stop", got)
	}
	if repo := h.repositoryOf("weft"); repo["dispatch"] != "paused" || repo["reservation"] != nil {
		t.Fatalf("repository after a stopped landing = %v", repo)
	}
	if !strings.Contains(h.logs.String(), "fast_forward intended and not acknowledged: the run is stopping") {
		t.Fatalf("log lacks the withheld acknowledgement:\n%s", h.logs.String())
	}
}

// A reconcile must observe before it intends: a push intended first, after a
// segment lost between the push and its result, is a protocol failure that
// is never acknowledged — the landing stays unresolved and paused rather
// than repeating an external write on the worker's word.
func TestReconcileIntentBeforeObservedIsRefused(t *testing.T) {
	h := newLandingHost(t)
	id := "weft/example-ready-a0b1"
	h.ticket(id, ticket.StatusReady, 2)
	h.script(id, 1, append(through(t, h.landingSegment(id, "worker-segment.ndjson"), "land_intent", "push"), dieLine))
	intent := maps.Clone(fixtureLines(t, "worker-reconcile-crash.ndjson")[1])
	intent["seq"], intent["step"], intent["commit"], intent["base"] = 1, "push", fixtureHead, fixtureBase
	h.script(id, 2, workerLines{intent, ackLine})
	runID := h.waitDispatched(entryOf(h.launch(id, "early"))["entry_id"].(string))
	var run *ledger.WeftRun
	waitFor(t, func() bool {
		run = h.run(runID)
		return len(run.Segments) == 2 && h.reaped(run) && run.Phase == ledger.WeftPhaseEnded
	})
	run = h.run(runID)
	if run.SettlementState != ledger.WeftSettlementNotLanded || run.SettlementReason.String != ledger.WeftLandUnreconciled ||
		run.SettlementStep.String != "push" || run.Steps[1].Result.Valid || run.Segments[1].Failure.String != ledger.ClassProtocol ||
		h.repositoryOf("weft")["dispatch"] != "paused" {
		t.Fatalf("a reconcile that intended before observing = %+v", run)
	}
	if got := h.commands(id, 2); len(got) != 0 {
		t.Fatalf("an intent before observed was acknowledged: %v", got)
	}
	if !strings.Contains(h.logs.String(), "land_intent before the reconcile observed what was done") {
		t.Fatalf("log lacks the refusal:\n%s", h.logs.String())
	}
}

// A refused precondition lands nothing: not_landed at fast_forward, the run
// held and its repository paused after reap, and no reconcile retried — it
// would refuse again (§7.4, §13).
func TestRefusedLandingPausesAndIsNotReconciled(t *testing.T) {
	h := newLandingHost(t)
	id := "weft/example-second-4c2d"
	h.ticket(id, ticket.StatusReady, 2)
	lines := h.landingSegment(id, "worker-segment.ndjson")
	refused := maps.Clone(lines[indexOf(lines, "land_step_result")])
	refused["result"], refused["detail"] = "refused", "main in the checkout has uncommitted changes"
	ended := maps.Clone(lines[len(lines)-1])
	ended["landing"] = map[string]any{"state": "not_landed", "step": "fast_forward", "reason": "refused",
		"detail": "main in the checkout has uncommitted changes; nothing was changed", "branch": "main", "commit": nil}
	h.script(id, 1, append(through(t, lines, "land_intent", "fast_forward"), refused, ended))
	runID := h.waitDispatched(entryOf(h.launch(id, "refused"))["entry_id"].(string))
	run := h.waitReaped(runID)
	doc := h.get("/v2/runs/" + runID)
	runShapeOf(t).check(t, "run", doc)
	// The worker named no commit: the settlement names the reviewed head.
	if commit := doc["run"].(map[string]any)["settlement"].(map[string]any)["commit"]; commit != fixtureHead {
		t.Fatalf("refused landing names commit %v, want the graded head", commit)
	}
	if run.SettlementState != ledger.WeftSettlementNotLanded || run.SettlementStep.String != "fast_forward" ||
		run.SettlementReason.String != ledger.WeftLandRefused || run.Steps[0].Result.String != "refused" || len(run.Steps) != 1 ||
		run.ActionRequired.String != ledger.WeftActionLandingUnsettled {
		t.Fatalf("refused landing = %+v", run)
	}
	if repo := h.repositoryOf("weft"); repo["dispatch"] != "paused" || repo["reservation"] != nil ||
		repo["paused"].(map[string]any)["run_id"] != runID {
		t.Fatalf("repository after a refused landing = %v", repo)
	}
	h.send(http.MethodPost, "/v2/runs/"+runID+"/reconcile", "rc", revisionBody(run.Revision)).
		refused(t, http.StatusConflict, "not_reconcilable")
}

// A worker's refusal on a landing host keeps its class: the segment and the
// run read the failure the worker named, and the run lands nothing.
func TestLandingHostRefusalKeepsItsFailure(t *testing.T) {
	h := newLandingHost(t)
	id := "weft/example-refused-6f70"
	h.ticket(id, ticket.StatusReady, 2)
	refusal := decode(t, fixtureBytes(t, "worker-stale-base.json")).(map[string]any)
	refusal["failure"], refusal["detail"] = "launch_refused", "the ticket's acceptance criteria are empty"
	h.script(id, 1, workerLines{refusal})
	runID := h.waitDispatched(entryOf(h.launch(id, "refused"))["entry_id"].(string))
	h.waitReaped(runID)
	run := h.get("/v2/runs/" + runID)["run"].(map[string]any)
	segment := run["segments"].([]any)[0].(map[string]any)
	if run["failure"] != "launch_refused" || segment["failure"] != "launch_refused" || segment["terminal"] != "ended" ||
		run["settlement"].(map[string]any)["state"] != "not_applicable" {
		t.Fatalf("refused landing segment = %v", run)
	}
}

// A run minted pending its landing that ends before any grade — its
// preflight failed, or it was stopped before its worker was spawned — lands
// nothing and does not read pending.
func TestUngradedLandingRunsDoNotStayPending(t *testing.T) {
	h := newLandingHost(t)
	failing, stopped := "weft/example-preflight-7a8b", "loom/example-stopped-8b9c"
	for _, id := range []string{failing, stopped} {
		h.ticket(id, ticket.StatusReady, 2)
	}
	// A worktree with no gitfile fails the sandbox preflight.
	if err := os.MkdirAll(h.worktree(failing), 0o755); err != nil {
		t.Fatal(err)
	}
	failed := h.waitReaped(h.waitDispatched(entryOf(h.launch(failing, "preflight"))["entry_id"].(string)))
	if failed.Segments[0].Failure.String != ledger.ClassSandbox || failed.SettlementState != ledger.WeftSettlementNone ||
		failed.SettlementDetail.String != "the run ended before it was graded, so nothing lands" {
		t.Fatalf("preflight-failed run = %+v", failed)
	}

	if err := h.store.Pause(ledger.WeftAgent); err != nil {
		t.Fatal(err)
	}
	h.launch(stopped, "stopped")
	if err := h.store.Resume(ledger.WeftAgent); err != nil {
		t.Fatal(err)
	}
	c, err := h.service.readCatalog()
	if err != nil {
		t.Fatal(err)
	}
	spawn, _, err := h.store.DispatchWeft(h.service.dispatchConfig(c))
	if err != nil || len(spawn) != 1 || h.run(spawn[0]).SettlementState != ledger.WeftSettlementPending {
		t.Fatalf("dispatch = %v %v", spawn, err)
	}
	runID := spawn[0]
	h.send(http.MethodPost, "/v2/runs/"+runID+"/stop", "stop", revisionBody(h.run(runID).Revision)).expect(t, http.StatusOK)
	h.service.segments.Start(runID)
	run := h.run(runID)
	if !h.reaped(run) || run.Outcome.String != "stopped" || run.SettlementState != ledger.WeftSettlementNone ||
		run.SettlementDetail.String != "the run ended before it was graded, so nothing lands" {
		t.Fatalf("run stopped before spawn = %+v", run)
	}
	h.settle()
}

// A cleanup or archive acts only on the worktree the operator authorized: a
// run retained under one weft_root, dismissed or freshed after the daemon
// restarts under another, is refused with nothing launched.
func TestMaintenanceRefusedWhenWeftRootMoved(t *testing.T) {
	h := newLandingHost(t)
	id := "weft/example-moved-9c0d"
	h.ticket(id, ticket.StatusReady, 2)
	h.script(id, 1, h.halted(id))
	run := h.waitReaped(h.waitDispatched(entryOf(h.launch(id, "l1"))["entry_id"].(string)))
	if err := os.MkdirAll(run.Worktree, 0o755); err != nil {
		t.Fatal(err)
	}
	cfg := *h.service.cfg
	cfg.Weft.Root = filepath.Join(h.root, "weft-moved")
	restarted, err := New(&cfg, h.store, "secret-token", h.root, h.service.log)
	if err != nil {
		t.Fatal(err)
	}
	restarted.segments.findKey = h.service.segments.findKey
	restarted.segments.refreshWorker()
	h.service = restarted

	dismissed := entryOf(h.send(http.MethodPost, "/v2/runs/"+run.ID+"/dismiss", "d", dismissBody(run.Revision, run.Worktree)))
	waitFor(t, func() bool {
		return h.entry(dismissed["entry_id"].(string))["state"] == "invalidated" && h.reaped(h.run(run.ID))
	})
	if e := h.entry(dismissed["entry_id"].(string))["invalidated"].(map[string]any); e["reason"] != "cleanup_refused" ||
		!strings.Contains(e["detail"].(string), "weft_root "+cfg.Weft.Root) {
		t.Fatalf("cleanup under a moved weft_root = %v", e)
	}
	run = h.run(run.ID)
	freshed := entryOf(h.send(http.MethodPost, "/v2/runs/"+run.ID+"/fresh", "f", freshBody(run.Revision, run.Worktree, fixtureHead)))
	waitFor(t, func() bool {
		return h.entry(freshed["entry_id"].(string))["state"] == "invalidated" && h.reaped(h.run(run.ID))
	})
	if e := h.entry(freshed["entry_id"].(string))["invalidated"].(map[string]any); e["reason"] != "archive_refused" ||
		!strings.Contains(e["detail"].(string), "weft_root "+cfg.Weft.Root) {
		t.Fatalf("archive under a moved weft_root = %v", e)
	}
	if after := h.run(run.ID); after.Disposition != ledger.WeftDispositionRetained || after.SupersededBy.Valid {
		t.Fatalf("a refused maintenance changed the run: %+v", after)
	}
	if argv := h.argv(); strings.Contains(argv, "cleanup --contract") || strings.Contains(argv, "archive --contract") {
		t.Fatalf("a maintenance worker was launched: %s", argv)
	}
	if c := h.queue()["capacity"].(map[string]any); c["running"] != float64(0) || h.repositoryOf("weft")["reservation"] != nil {
		t.Fatalf("a refused maintenance kept its slot or row: %v", c)
	}

	// A refusal is not host trouble: six in a row, past the failure streak
	// that pauses the agent, leave the agent unpaused with no streak and no
	// incident, each segment's row closed skipped.
	for i := range 6 {
		run = h.run(run.ID)
		e := entryOf(h.send(http.MethodPost, "/v2/runs/"+run.ID+"/dismiss", fmt.Sprintf("d-%d", i), dismissBody(run.Revision, run.Worktree)))
		waitFor(t, func() bool {
			return h.entry(e["entry_id"].(string))["state"] == "invalidated" && h.reaped(h.run(run.ID))
		})
	}
	agent, err := h.store.Agent(ledger.WeftAgent)
	if err != nil {
		t.Fatal(err)
	}
	incidents, err := h.store.OpenIncidents()
	if err != nil {
		t.Fatal(err)
	}
	if agent.Paused || agent.FailureStreak != 0 || len(incidents) != 0 {
		t.Fatalf("refusals counted as failures: agent %+v, incidents %d", agent, len(incidents))
	}
	for _, seg := range h.run(run.ID).Segments[1:] {
		row, err := h.store.Get(seg.LedgerRunID)
		if err != nil {
			t.Fatal(err)
		}
		if row.Status != ledger.StatusSkipped || row.SkipReason.String != ledger.SkipRefused || seg.Failure.Valid {
			t.Fatalf("refused %s segment %d = %+v, failure %v", seg.Kind, seg.Segment, row, seg.Failure)
		}
	}
	if repo := h.repositoryOf("weft"); repo["dispatch"] != "open" || repo["reservation"] != nil {
		t.Fatalf("repository after refusals = %v", repo)
	}
}

// stoppedReconcile is a reconcile of a landing an operator stopped: it
// observes, intends the first step absent, reads the stop the host sends in
// place of an acknowledgement, and reconciles not_landed stopped at it.
func stoppedReconcile(t *testing.T, observed [3]string, step string) workerLines {
	t.Helper()
	lines := reconcileLines(t, observed, fixtureHead, fixtureBase, step)
	reconciled := maps.Clone(lines[len(lines)-1])
	reconciled["landing"] = map[string]any{"state": "not_landed", "step": step, "reason": "stopped",
		"detail": "the landing was stopped before " + step, "branch": "main", "commit": fixtureHead}
	return append(lines[:3:3], reconciled)
}

func stopCommand() []any {
	return []any{map[string]any{"contract_version": WorkerContractV2, "command": "stop"}}
}

// holdsStoppedLanding checks a stopped landing after reap: not landed,
// stopped at step, the step never acknowledged, the run held and its
// repository paused.
func (h *queueHost) holdsStoppedLanding(runID, step string) *ledger.WeftRun {
	h.t.Helper()
	run := h.run(runID)
	at := slices.Index(ledger.WeftLandSteps, step)
	if run.SettlementState != ledger.WeftSettlementNotLanded || run.SettlementReason.String != ledger.WeftLandStopped ||
		run.SettlementStep.String != step || len(run.Steps) <= at || run.Steps[at].AckedAt.Valid ||
		run.ActionRequired.String != ledger.WeftActionLandingUnsettled || !run.Stopped || run.SettlementCommit.String != fixtureHead {
		h.t.Fatalf("stopped landing = %+v", run)
	}
	if repo := h.repositoryOf("weft"); repo["dispatch"] != "paused" || repo["reservation"] != nil {
		h.t.Fatalf("repository after a stopped landing = %v", repo)
	}
	return run
}

// retryLands is the operator's retry of a stopped landing: it lifts the
// stop, and the reconcile lands what was left, each step acknowledged.
func (h *queueHost) retryLands(id, runID string, segment int, observed [3]string, take ...string) {
	h.t.Helper()
	h.script(id, segment, reconcileLines(h.t, observed, fixtureHead, fixtureBase, take...))
	h.send(http.MethodPost, "/v2/runs/"+runID+"/reconcile", "retry", revisionBody(h.run(runID).Revision)).
		expect(h.t, http.StatusAccepted, "reconcile-response.json")
	var run *ledger.WeftRun
	waitFor(h.t, func() bool {
		run = h.run(runID)
		return len(run.Segments) == segment && run.SettlementState == ledger.WeftSettlementLanded && h.reaped(run)
	})
	if got, want := h.commands(id, segment), acks(h.t, fixtureHead, take...); !reflect.DeepEqual(got, want) || h.run(runID).Stopped {
		h.t.Fatalf("the retry was acknowledged %v, want %v", got, want)
	}
}

// An operator's stop of a landing survives the landing segment being killed
// before its ended: the reconcile of it observes and records, is
// acknowledged no step, and ends not_landed stopped with the repository
// paused. Only the operator's retry lands it (§6.6).
func TestStoppedLandingStaysStoppedThroughItsReconcile(t *testing.T) {
	h := newLandingHost(t)
	id := "weft/example-second-4c2d"
	h.ticket(id, ticket.StatusReady, 2)
	lines := h.landingSegment(id, "worker-segment.ndjson")
	stopped := append(through(t, lines, "graded", ""), nil)
	stopped = append(stopped, through(t, lines, "land_intent", "fast_forward")[len(stopped)-1:]...)
	h.script(id, 1, append(stopped, dieLine))
	h.script(id, 2, stoppedReconcile(t, [3]string{"not_done", "not_done", "not_done"}, "fast_forward"))
	runID := h.waitDispatched(entryOf(h.launch(id, "stop-kill"))["entry_id"].(string))
	waitFor(t, func() bool { return h.run(runID).Phase == ledger.WeftPhaseSettling })
	h.send(http.MethodPost, "/v2/runs/"+runID+"/stop", "stop", revisionBody(h.run(runID).Revision)).expect(t, http.StatusOK)
	h.release(id)
	waitFor(t, func() bool {
		run := h.run(runID)
		return len(run.Segments) == 2 && run.Phase == ledger.WeftPhaseEnded && h.reaped(run)
	})
	run := h.holdsStoppedLanding(runID, "fast_forward")
	if run.Segments[0].Failure.String != ledger.ClassDied || run.Segments[1].Kind != ledger.WeftSegmentReconcile {
		t.Fatalf("segments = %+v", run.Segments)
	}
	for segment := 1; segment <= 2; segment++ {
		if got := h.commands(id, segment); !reflect.DeepEqual(got, stopCommand()) {
			t.Fatalf("segment %d read %v, want only the stop", segment, got)
		}
	}
	h.retryLands(id, runID, 3, [3]string{"not_done", "not_done", "not_done"}, ledger.WeftLandSteps...)
}

// An operator's stop of a landing survives a daemon restart: the reconcile
// the restart starts is acknowledged no step and ends not_landed stopped,
// and only the operator's retry lands it.
func TestStoppedLandingStaysStoppedThroughARestart(t *testing.T) {
	h := newLandingHost(t)
	id := "weft/example-ready-a0b1"
	h.ticket(id, ticket.StatusReady, 2)
	if err := h.store.Pause(ledger.WeftAgent); err != nil {
		t.Fatal(err)
	}
	h.launch(id, "restart")
	if err := h.store.Resume(ledger.WeftAgent); err != nil {
		t.Fatal(err)
	}
	c, err := h.service.readCatalog()
	if err != nil {
		t.Fatal(err)
	}
	spawn, _, err := h.store.DispatchWeft(h.service.dispatchConfig(c))
	if err != nil || len(spawn) != 1 {
		t.Fatalf("dispatch = %v %v", spawn, err)
	}
	runID := spawn[0]
	for _, step := range []func() error{
		func() error { return h.store.MarkWeftRunning(runID, 1, 999999, WorkerContractV2) },
		func() error {
			return h.store.RecordWeftLandingHello(runID, 1, fixtureWorkerRun, fixtureBase, "criteria", true)
		},
		func() error {
			return h.store.RecordWeftGraded(runID, 1, "finished", false, "", "", "", "", fixtureHead, json.RawMessage(`{"state":"unavailable","reason":"fixture"}`))
		},
		func() error {
			_, err := h.store.RecordWeftLandIntent(runID, 1, "fast_forward", "main", fixtureHead, fixtureBase, "2026-10-03T08:20:00.000Z")
			return err
		},
		func() error {
			return h.store.RecordWeftLandResult(runID, 1, "fast_forward", "done", "", "2026-10-03T08:20:01.000Z")
		},
	} {
		if err := step(); err != nil {
			t.Fatal(err)
		}
	}
	h.send(http.MethodPost, "/v2/runs/"+runID+"/stop", "stop", revisionBody(h.run(runID).Revision)).expect(t, http.StatusOK)
	ledgerRun := h.run(runID).Segments[0].LedgerRunID
	if err := h.store.MarkUnknown(ledgerRun); err != nil {
		t.Fatal(err)
	}
	if err := h.store.ReconcileWeftUnknown(ledgerRun); err != nil {
		t.Fatal(err)
	}
	h.script(id, 2, stoppedReconcile(t, [3]string{"done", "not_done", "not_done"}, "push"))
	restarted, err := New(h.service.cfg, h.store, "secret-token", h.root, h.service.log)
	if err != nil {
		t.Fatal(err)
	}
	restarted.segments.findKey = h.service.segments.findKey
	h.service = restarted
	if err := restarted.Restart(); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		run := h.run(runID)
		return len(run.Segments) == 2 && run.Phase == ledger.WeftPhaseEnded && h.reaped(run)
	})
	h.holdsStoppedLanding(runID, "push")
	if got := h.commands(id, 2); !reflect.DeepEqual(got, stopCommand()) {
		t.Fatalf("the restarted reconcile read %v, want only the stop", got)
	}
	h.retryLands(id, runID, 3, [3]string{"done", "not_done", "not_done"}, "push", "mark_done")
}

// A cleanup waiting on its repository and a resume of the same run never
// both proceed: the resume is refused while the cleanup is queued, and a
// cleanup that finds a resume queued at dispatch is invalidated rather than
// remove the worktree under it.
func TestQueuedCleanupAndResumeExcludeEachOther(t *testing.T) {
	h := newLandingHost(t)
	id, holder := "weft/example-parked-1a2b", "weft/example-holder-2b3c"
	for _, ticketID := range []string{id, holder} {
		h.ticket(ticketID, ticket.StatusReady, 2)
	}
	parked := decode(t, fixtureBytes(t, "worker-parked.json")).(map[string]any)
	parked["worker_run_id"] = fixtureWorkerRun
	parked["summary"].(map[string]any)["report"].(map[string]any)["ticket"] = id
	h.script(id, 1, workerLines{h.landingSegment(id, "worker-segment.ndjson")[0], parked})
	runID := h.waitDispatched(entryOf(h.launch(id, "parks"))["entry_id"].(string))
	h.waitPhase(runID, ledger.WeftPhaseParked)
	h.send(http.MethodPost, "/v2/runs/"+runID+"/stop", "stop", revisionBody(h.run(runID).Revision)).expect(t, http.StatusOK)
	run := h.run(runID)
	if err := os.MkdirAll(run.Worktree, 0o755); err != nil {
		t.Fatal(err)
	}
	h.script(holder, 1, splice(h.halted(holder), 1, nil))
	holderRun := h.waitDispatched(entryOf(h.launch(holder, "holds"))["entry_id"].(string))

	cleanup := entryOf(h.send(http.MethodPost, "/v2/runs/"+runID+"/dismiss", "dismiss", dismissBody(run.Revision, run.Worktree)))
	if reason, held := waiting(cleanup); reason != "repository_reserved" || held["id"] != holderRun {
		t.Fatalf("cleanup = %v", cleanup)
	}
	if message := h.send(http.MethodPost, "/v2/runs/"+runID+"/resume", "resume", revisionBody(run.Revision)).
		refused(t, http.StatusConflict, "run_active"); !strings.Contains(message, cleanup["entry_id"].(string)) {
		t.Fatalf("resume under a queued cleanup = %s", message)
	}

	// A resume queued behind the cleanup's back — as one admitted before
	// this guard was — invalidates the cleanup at dispatch.
	resume := "e-20261012T120000Z-0000abcd"
	h.exec(`INSERT INTO weft_entries(id,kind,ticket,project,repository_id,repository,position,state,run_id,
		token_id,request_key,created_at,updated_at,revision)
		VALUES(?,'resume',?,'weft',?,?,(SELECT MAX(position)+1 FROM weft_entries),'waiting',?,'token','k',
		'2026-10-12T12:00:00.000Z','2026-10-12T12:00:00.000Z',1)`, resume, id, h.repositoryID("weft"), h.repository("weft"), runID)
	h.script(id, 2, workerLines{decode(t, fixtureBytes(t, "worker-stale-base.json")).(map[string]any)})
	h.release(holder)
	waitFor(t, func() bool {
		return h.entry(cleanup["entry_id"].(string))["state"] == "invalidated" && h.entry(resume)["state"] == "dispatched" &&
			h.reaped(h.run(runID))
	})
	if e := h.entry(cleanup["entry_id"].(string))["invalidated"].(map[string]any); e["reason"] != "run_changed" ||
		!strings.Contains(e["detail"].(string), resume) {
		t.Fatalf("cleanup with a resume queued = %v", e)
	}
	if _, err := os.Stat(run.Worktree); err != nil || strings.Contains(h.argv(), "cleanup --contract") {
		t.Fatalf("the worktree was cleaned under a resume: %v\n%s", err, h.argv())
	}
	if after := h.run(runID); after.Disposition != ledger.WeftDispositionRetained || after.Segments[1].Kind != "resume" {
		t.Fatalf("run after the resume = %+v", after)
	}
	h.waitReaped(holderRun)
}

// A stop accepted while the pipeline ran survives a finished grade the
// worker had already buffered: the grade does not clear it, the landing it
// would start is stopped, and when the worker dies before its ended the
// reconcile is acknowledged no step. Only the operator's retry lands it.
func TestStopBeforeGradeStopsTheLanding(t *testing.T) {
	h := newLandingHost(t)
	id := "weft/example-second-4c2d"
	h.ticket(id, ticket.StatusReady, 2)
	lines := h.landingSegment(id, "worker-segment.ndjson")
	graded := indexOf(lines, "graded")
	h.script(id, 1, append(append(append(workerLines{}, lines[:graded]...), nil, lines[graded]), dieLine))
	h.script(id, 2, stoppedReconcile(t, [3]string{"not_done", "not_done", "not_done"}, "fast_forward"))
	runID := h.waitDispatched(entryOf(h.launch(id, "stop-graded"))["entry_id"].(string))
	waitFor(t, func() bool { return len(h.run(runID).Stages) > 0 && h.run(runID).Phase == ledger.WeftPhaseRunning })
	h.send(http.MethodPost, "/v2/runs/"+runID+"/stop", "stop", revisionBody(h.run(runID).Revision)).expect(t, http.StatusOK)
	h.release(id)
	waitFor(t, func() bool {
		run := h.run(runID)
		return len(run.Segments) == 2 && run.Phase == ledger.WeftPhaseEnded && h.reaped(run)
	})
	run := h.holdsStoppedLanding(runID, "fast_forward")
	if !run.Stopped || run.Outcome.String != "finished" || run.Head.String != fixtureHead {
		t.Fatalf("the grade cleared the stop: %+v", run)
	}
	if got := h.commands(id, 2); !reflect.DeepEqual(got, stopCommand()) {
		t.Fatalf("the reconcile read %v, want only the stop", got)
	}
	h.refusesResume(runID, "resume-held")
	h.retryLands(id, runID, 3, [3]string{"not_done", "not_done", "not_done"}, ledger.WeftLandSteps...)
	h.refusesResume(runID, "resume-landed")
}

// A stop accepted while the pipeline ran, then a finished grade the worker
// had buffered and its landing: the grade stands, the stop stops the landing
// — its intent is never acknowledged, the worker reads the stop in its place
// and ends not_landed stopped, which holds the run and pauses its
// repository. A resume never answers the grade; the operator's retry is a new
// attempt, which lifts the stop and lands.
func TestStopBeforeFinishedGradeEndsNotLandedStopped(t *testing.T) {
	h := newLandingHost(t)
	id := "weft/example-second-4c2d"
	h.ticket(id, ticket.StatusReady, 2)
	lines := h.landingSegment(id, "worker-segment.ndjson")
	graded := indexOf(lines, "graded")
	ended := maps.Clone(lines[len(lines)-1])
	ended["landing"] = map[string]any{"state": "not_landed", "step": "fast_forward", "reason": "stopped",
		"detail": "stopped while waiting to fast-forward main", "branch": "main", "commit": fixtureHead}
	script := append(append(workerLines{}, lines[:graded]...), nil)
	script = append(script, through(t, lines, "land_intent", "fast_forward")[graded:]...)
	h.script(id, 1, append(script, ended))
	runID := h.waitDispatched(entryOf(h.launch(id, "stop-finished"))["entry_id"].(string))
	waitFor(t, func() bool { return len(h.run(runID).Stages) > 0 && h.run(runID).Phase == ledger.WeftPhaseRunning })
	h.send(http.MethodPost, "/v2/runs/"+runID+"/stop", "stop", revisionBody(h.run(runID).Revision)).expect(t, http.StatusOK)
	h.release(id)
	h.waitReaped(runID)
	run := h.holdsStoppedLanding(runID, "fast_forward")
	if run.Outcome.String != "finished" || run.Head.String != fixtureHead || len(run.Segments) != 1 {
		t.Fatalf("the stop overrode the finished grade: %+v", run)
	}
	if got := h.commands(id, 1); !reflect.DeepEqual(got, stopCommand()) {
		t.Fatalf("the worker read %v, want only the stop", got)
	}
	h.refusesResume(runID, "resume-held")
	h.retryLands(id, runID, 2, [3]string{"not_done", "not_done", "not_done"}, ledger.WeftLandSteps...)
	h.refusesResume(runID, "resume-landed")
}

// refusesResume checks a resume of the run is refused: a run with a landing
// is never resumed, whether it landed or is held (§4).
func (h *queueHost) refusesResume(runID, key string) {
	h.t.Helper()
	h.send(http.MethodPost, "/v2/runs/"+runID+"/resume", key, revisionBody(h.run(runID).Revision)).
		refused(h.t, http.StatusConflict, "run_not_resumable")
}

// A reconcile's intent needs every earlier step done on its own evidence,
// not only the one before it: one that observed the fast-forward not done
// and the push done is refused mark_done unacknowledged, and the landing
// ends unreconciled at the fast-forward with the repository paused.
func TestReconcileIntentNeedsEveryEarlierStep(t *testing.T) {
	h := newLandingHost(t)
	id := "weft/example-ready-a0b1"
	h.ticket(id, ticket.StatusReady, 2)
	h.script(id, 1, append(through(t, h.landingSegment(id, "worker-segment.ndjson"), "land_step_result", "push"), dieLine))
	h.script(id, 2, reconcileLines(t, [3]string{"not_done", "done", "not_done"}, fixtureHead, fixtureBase, "mark_done")[:3])
	runID := h.waitDispatched(entryOf(h.launch(id, "gap"))["entry_id"].(string))
	var run *ledger.WeftRun
	waitFor(t, func() bool {
		run = h.run(runID)
		return len(run.Segments) == 2 && run.Phase == ledger.WeftPhaseEnded && h.reaped(run)
	})
	run = h.run(runID)
	if got := h.commands(id, 2); len(got) != 0 {
		t.Fatalf("mark_done was acknowledged with the fast-forward not done: %v", got)
	}
	if run.SettlementState != ledger.WeftSettlementNotLanded || run.SettlementReason.String != ledger.WeftLandUnreconciled ||
		run.SettlementStep.String != ledger.WeftStepFastForward || run.Segments[1].Failure.String != ledger.ClassProtocol ||
		len(run.Steps) != 2 || run.ActionRequired.String != ledger.WeftActionLandingUnsettled ||
		h.repositoryOf("weft")["dispatch"] != "paused" {
		t.Fatalf("mark_done before the fast-forward = %+v", run)
	}
	if !strings.Contains(h.logs.String(), "land_intent mark_done before fast_forward was done") {
		t.Fatalf("log lacks the refusal:\n%s", h.logs.String())
	}
}

// A reconcile of a landing segment lost before its grade observes no
// outcome: what it saw of the steps is no landing, so it records no step,
// and the run is closed as v1's interrupted segment, resumable. The resume
// meets no earlier landing's record: its segment's landing ends
// not_applicable and the run is over.
func TestUngradedReconcileLeavesNoStepsInTheResumesWay(t *testing.T) {
	h := newLandingHost(t)
	id := "weft/example-second-4c2d"
	h.ticket(id, ticket.StatusReady, 2)
	h.script(id, 1, append(through(t, h.landingSegment(id, "worker-segment.ndjson"), "stage", ""), dieLine))
	reconcile := reconcileLines(t, [3]string{"done", "not_done", "not_done"}, fixtureHead, fixtureBase)
	reconcile[0]["outcome"], reconcile[0]["branch"], reconcile[0]["commit"] = nil, nil, nil
	reconcile[1]["landing"] = map[string]any{"state": "not_applicable", "step": nil, "reason": nil,
		"detail": "the run was never graded", "branch": nil, "commit": nil}
	h.script(id, 2, reconcile)
	const resumed = "20261002T101500Z-3d4e5f60"
	var resume workerLines
	for _, ev := range h.halted(id) {
		ev = maps.Clone(ev)
		ev["worker_run_id"] = resumed
		if ev["event"] == "hello" {
			ev["resumed"] = fixtureWorkerRun
		}
		resume = append(resume, ev)
	}
	h.script(id, 3, resume)
	runID := h.waitDispatched(entryOf(h.launch(id, "ungraded"))["entry_id"].(string))
	var run *ledger.WeftRun
	waitFor(t, func() bool {
		run = h.run(runID)
		return len(run.Segments) == 2 && run.Phase == ledger.WeftPhaseEnded && h.reaped(run)
	})
	run = h.run(runID)
	if run.Outcome.String != "failed" || run.SettlementState != ledger.WeftSettlementNone || len(run.Steps) != 0 ||
		run.Segments[0].Failure.String != ledger.ClassDied || run.ActionRequired.Valid {
		t.Fatalf("ungraded reconcile = %+v", run)
	}
	// The worktree the lost segment left, as a worker leaves it.
	admin := filepath.Join(h.repository("weft"), ".git", "worktrees", slugOf(id))
	if err := os.MkdirAll(admin, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(run.Worktree, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(run.Worktree, ".git"), []byte("gitdir: "+admin+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	h.send(http.MethodPost, "/v2/runs/"+runID+"/resume", "resume", revisionBody(run.Revision)).
		expect(t, http.StatusAccepted, "resume-response.json", "launch-response.json")
	waitFor(t, func() bool {
		run = h.run(runID)
		return len(run.Segments) == 3 && run.Phase == ledger.WeftPhaseEnded && h.reaped(run)
	})
	run = h.run(runID)
	if run.Outcome.String != "halted" || run.SettlementState != ledger.WeftSettlementNone || len(run.Steps) != 0 ||
		run.Segments[2].Kind != "resume" || run.Segments[2].Failure.Valid || run.Segments[2].Terminal.String != "ended" {
		t.Fatalf("resumed run = %+v", run)
	}
}

// A reconcile acts on its own evidence, never on what the lost segment
// reported: after a segment that reported its push done, a reconcile that
// observes the push not done or unknown has the push to take — mark_done is
// refused unacknowledged until the reconcile itself establishes the push.
func TestReconcileObservationOverridesAnEarlierResult(t *testing.T) {
	for _, tt := range []struct {
		name, push string
		take       []string
	}{
		{"mark_done on an unknown push", "unknown", nil},
		{"push retaken", "not_done", []string{"push", "mark_done"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			h := newLandingHost(t)
			id := "weft/example-ready-a0b1"
			h.ticket(id, ticket.StatusReady, 2)
			h.script(id, 1, append(through(t, h.landingSegment(id, "worker-segment.ndjson"), "land_step_result", "push"), dieLine))
			observed := [3]string{"done", tt.push, "not_done"}
			if tt.take != nil {
				h.script(id, 2, reconcileLines(t, observed, fixtureHead, fixtureBase, tt.take...))
			} else {
				lines := reconcileLines(t, observed, fixtureHead, fixtureBase, "mark_done")
				h.script(id, 2, lines[:3])
			}
			runID := h.waitDispatched(entryOf(h.launch(id, "observed"))["entry_id"].(string))
			var run *ledger.WeftRun
			waitFor(t, func() bool {
				run = h.run(runID)
				return len(run.Segments) == 2 && run.Phase == ledger.WeftPhaseEnded && h.reaped(run)
			})
			run = h.run(runID)
			if got, want := h.commands(id, 2), acks(t, fixtureHead, tt.take...); !reflect.DeepEqual(got, want) {
				t.Fatalf("the reconcile was acknowledged %v, want %v", got, want)
			}
			if tt.take != nil {
				if run.SettlementState != ledger.WeftSettlementLanded || run.Steps[1].Result.String != "done" ||
					run.Steps[1].Observed.String != "not_done" || !run.Steps[1].AckedAt.Valid {
					t.Fatalf("retaken push = %+v", run)
				}
				return
			}
			if run.SettlementState != ledger.WeftSettlementNotLanded || run.SettlementReason.String != ledger.WeftLandUnreconciled ||
				run.SettlementStep.String != "push" || run.Segments[1].Failure.String != ledger.ClassProtocol ||
				len(run.Steps) != 2 || run.Steps[1].Observed.String != "unknown" || h.repositoryOf("weft")["dispatch"] != "paused" {
				t.Fatalf("mark_done on an unknown push = %+v", run)
			}
			if !strings.Contains(h.logs.String(), "land_intent mark_done before push was done") {
				t.Fatalf("log lacks the refusal:\n%s", h.logs.String())
			}
		})
	}
}

// A reconcile takes only a step whose effect it observed absent: a step it
// observed unknown is never acknowledged, and the landing ends unreconciled
// at it with the repository paused (§7.4).
func TestReconcileNeverTakesAnUnknownStep(t *testing.T) {
	for _, tt := range []struct {
		name, event, step string
		observed          [3]string
		intend            string
	}{
		{"push unknown", "land_intent", "push", [3]string{"done", "unknown", "not_done"}, "push"},
		{"mark_done unknown", "land_step_result", "push", [3]string{"done", "done", "unknown"}, "mark_done"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			h := newLandingHost(t)
			id := "weft/example-ready-a0b1"
			h.ticket(id, ticket.StatusReady, 2)
			h.script(id, 1, append(through(t, h.landingSegment(id, "worker-segment.ndjson"), tt.event, tt.step), dieLine))
			h.script(id, 2, reconcileLines(t, tt.observed, fixtureHead, fixtureBase, tt.intend)[:3])
			runID := h.waitDispatched(entryOf(h.launch(id, "unknown"))["entry_id"].(string))
			var run *ledger.WeftRun
			waitFor(t, func() bool {
				run = h.run(runID)
				return len(run.Segments) == 2 && run.Phase == ledger.WeftPhaseEnded && h.reaped(run)
			})
			run = h.run(runID)
			if got := h.commands(id, 2); len(got) != 0 {
				t.Fatalf("a step observed unknown was acknowledged: %v", got)
			}
			if run.SettlementState != ledger.WeftSettlementNotLanded || run.SettlementReason.String != ledger.WeftLandUnreconciled ||
				run.SettlementStep.String != tt.intend || run.Segments[1].Failure.String != ledger.ClassProtocol ||
				run.ActionRequired.String != ledger.WeftActionLandingUnsettled || h.repositoryOf("weft")["dispatch"] != "paused" {
				t.Fatalf("%s = %+v", tt.name, run)
			}
			if !strings.Contains(h.logs.String(), "land_intent "+tt.intend+", which the reconcile observed unknown") {
				t.Fatalf("log lacks the refusal:\n%s", h.logs.String())
			}
		})
	}
}

// A stop accepted while the pipeline ran, followed by a grade that is not
// finished, starts no landing and stops none: the run is resumable, and its
// resume segment is sent no stop and runs.
func TestStopBeforeUnfinishedGradeLeavesTheRunResumable(t *testing.T) {
	h := newLandingHost(t)
	id := "weft/example-second-4c2d"
	h.ticket(id, ticket.StatusReady, 2)
	lines := h.landingSegment(id, "worker-segment.ndjson")
	graded := maps.Clone(lines[indexOf(lines, "graded")])
	graded["outcome"], graded["stopped"] = "stopped", true
	ended := maps.Clone(lines[len(lines)-1])
	ended["outcome"] = "stopped"
	ended["landing"] = map[string]any{"state": "not_applicable", "step": nil, "reason": nil,
		"detail": "the run was stopped before it finished", "branch": nil, "commit": nil}
	h.script(id, 1, workerLines{lines[0], nil, graded, ended})
	const resumed = "20261002T101500Z-3d4e5f60"
	var resume workerLines
	for _, ev := range h.halted(id) {
		ev = maps.Clone(ev)
		ev["worker_run_id"] = resumed
		if ev["event"] == "hello" {
			ev["resumed"] = fixtureWorkerRun
		}
		resume = append(resume, ev)
	}
	h.script(id, 2, resume)
	runID := h.waitDispatched(entryOf(h.launch(id, "stop-running"))["entry_id"].(string))
	waitFor(t, func() bool { return h.run(runID).Phase == ledger.WeftPhaseRunning })
	h.send(http.MethodPost, "/v2/runs/"+runID+"/stop", "stop", revisionBody(h.run(runID).Revision)).expect(t, http.StatusOK)
	h.release(id)
	run := h.waitReaped(runID)
	if run.Outcome.String != "stopped" || !run.Stopped || run.SettlementState != ledger.WeftSettlementNone {
		t.Fatalf("stopped run = %+v", run)
	}
	// The worktree the stopped segment left, as a worker leaves it.
	admin := filepath.Join(h.repository("weft"), ".git", "worktrees", slugOf(id))
	if err := os.MkdirAll(admin, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(run.Worktree, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(run.Worktree, ".git"), []byte("gitdir: "+admin+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	h.send(http.MethodPost, "/v2/runs/"+runID+"/resume", "resume", revisionBody(run.Revision)).
		expect(t, http.StatusAccepted, "resume-response.json", "launch-response.json")
	waitFor(t, func() bool {
		run = h.run(runID)
		return len(run.Segments) == 2 && run.Phase == ledger.WeftPhaseEnded && h.reaped(run)
	})
	run = h.run(runID)
	if got := h.commands(id, 2); len(got) != 0 {
		t.Fatalf("the resume segment was sent %v", got)
	}
	if run.Outcome.String != "halted" || run.Segments[1].Kind != "resume" || run.Segments[1].WorkerRunID.String != resumed ||
		run.Segments[1].Failure.Valid {
		t.Fatalf("resumed run = %+v", run)
	}
}

// A reconcile of a landing segment lost before the host recorded its grade
// is launched without --commit: it observes and takes no step. One that
// observes a finished grade and intends the fast-forward with its commit and
// base omitted, null or empty fails as the protocol error it is, nothing is
// acknowledged, and the run — taking the finished grade the lost reconcile
// read (row 22u) — is held unreconciled at the fast-forward with its
// repository paused (§3 checks 1 and 2′).
func TestReconcileWithoutCommitTakesNoStep(t *testing.T) {
	for _, tt := range []struct {
		name string
		set  func(intent map[string]any)
	}{
		{"omitted", func(intent map[string]any) { delete(intent, "commit"); delete(intent, "base") }},
		{"null", func(intent map[string]any) { intent["commit"], intent["base"] = nil, nil }},
		{"empty", func(intent map[string]any) { intent["commit"], intent["base"] = "", "" }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			h := newLandingHost(t)
			id := "weft/example-ready-a0b1"
			h.ticket(id, ticket.StatusReady, 2)
			h.script(id, 1, append(through(t, h.landingSegment(id, "worker-segment.ndjson"), "stage", ""), dieLine))
			lines := reconcileLines(t, [3]string{"not_done", "not_done", "not_done"}, fixtureHead, fixtureBase, "fast_forward")[:3]
			tt.set(lines[1])
			h.script(id, 2, lines)
			runID := h.waitDispatched(entryOf(h.launch(id, "missed-grade"))["entry_id"].(string))
			var run *ledger.WeftRun
			waitFor(t, func() bool {
				run = h.run(runID)
				return len(run.Segments) == 2 && run.Phase == ledger.WeftPhaseEnded && h.reaped(run)
			})
			run = h.run(runID)
			if strings.Contains(h.argv(), "--commit") {
				t.Fatalf("a reconcile with no recorded grade was given a commit: %s", h.argv())
			}
			if got := h.commands(id, 2); len(got) != 0 {
				t.Fatalf("an intent with no graded head was acknowledged: %v", got)
			}
			if run.Outcome.String != "finished" || run.SettlementState != ledger.WeftSettlementNotLanded ||
				run.SettlementReason.String != ledger.WeftLandUnreconciled || run.SettlementStep.String != ledger.WeftStepFastForward ||
				run.Segments[0].Failure.String != ledger.ClassDied || run.Segments[1].Failure.String != ledger.ClassProtocol ||
				len(run.Steps) != 0 || run.ActionRequired.String != ledger.WeftActionLandingUnsettled ||
				h.repositoryOf("weft")["dispatch"] != "paused" {
				t.Fatalf("reconcile without a commit = %+v", run)
			}
			runShapeOf(t).check(t, "run", h.get("/v2/runs/"+runID))
			h.refusesResume(runID, "resume-unreconciled")
		})
	}
}

// A graded segment never parks (row 13p): a parked after the fast-forward
// was done records no question and no terminal event, fails the segment as
// the protocol error it is, and its landing goes to reconciliation — the
// repository stays reserved for it, and no answer restarts the pipeline over
// the step already taken.
func TestParkedAfterAGradeIsReconciled(t *testing.T) {
	h := newLandingHost(t)
	id := "weft/example-ready-a0b1"
	h.ticket(id, ticket.StatusReady, 2)
	parked := decode(t, fixtureBytes(t, "worker-parked.json")).(map[string]any)
	parked["worker_run_id"] = fixtureWorkerRun
	parked["summary"].(map[string]any)["report"].(map[string]any)["ticket"] = id
	h.script(id, 1, append(through(t, h.landingSegment(id, "worker-segment.ndjson"), "land_step_result", "fast_forward"), parked))
	h.script(id, 2, append(workerLines{nil},
		reconcileLines(t, [3]string{"done", "not_done", "not_done"}, fixtureHead, fixtureBase, "push", "mark_done")...))
	runID := h.waitDispatched(entryOf(h.launch(id, "parks-graded"))["entry_id"].(string))

	var run *ledger.WeftRun
	waitFor(t, func() bool {
		run = h.run(runID)
		return len(run.Segments) == 2 && run.SettlementState == ledger.WeftSettlementReconciling && run.Segments[0].Failure.Valid
	})
	if run.Phase != ledger.WeftPhaseSettling || run.Question != nil || run.Segments[0].Terminal.Valid ||
		run.Segments[0].Failure.String != ledger.ClassProtocol || run.Segments[1].Kind != ledger.WeftSegmentReconcile ||
		holderOf(h.repositoryOf("weft")) != "settlement "+runID {
		t.Fatalf("parked after a grade = %+v", run)
	}
	h.send(http.MethodPost, "/v2/runs/"+runID+"/answer", "answer", fmt.Sprintf(
		`{"contract_version":"weft-service/v2","question_id":%q,"answer":"Use it."}`, "q-20261002T103000Z-0000abcd")).
		refused(t, http.StatusNotFound, "question_not_found")
	if !strings.Contains(h.logs.String(), "parked after graded") {
		t.Fatalf("log lacks the refusal:\n%s", h.logs.String())
	}

	h.release(id)
	waitFor(t, func() bool {
		run = h.run(runID)
		return run.SettlementState == ledger.WeftSettlementLanded && h.reaped(run)
	})
	run = h.run(runID)
	if run.Outcome.String != "finished" || run.ActionRequired.Valid || len(run.Segments) != 2 || run.Question != nil {
		t.Fatalf("reconciled run = %+v", run)
	}
	if got, want := h.commands(id, 1), acks(t, fixtureHead, "fast_forward"); !reflect.DeepEqual(got, want) {
		t.Fatalf("the parked segment was acknowledged %v, want %v", got, want)
	}
	if got, want := h.commands(id, 2), acks(t, fixtureHead, "push", "mark_done"); !reflect.DeepEqual(got, want) {
		t.Fatalf("the reconcile was acknowledged %v, want %v", got, want)
	}
	if strings.Count(h.argv(), "segment --contract") != 1 {
		t.Fatalf("argv = %s", h.argv())
	}
	if repo := h.repositoryOf("weft"); repo["reservation"] != nil || repo["dispatch"] != "open" {
		t.Fatalf("repository after the reconcile = %v", repo)
	}
	h.refusesResume(runID, "resume-landed")
}

// A reconcile's reading never overrides a recorded finished grade (row
// 15g): one that observes halted after the fast-forward was done fails as a
// protocol error, records no observation, and the landing is held
// unreconciled at the push with its repository paused — never read as a run
// that landed nothing.
func TestReconcileObservingAnotherGradeIsUnreconciled(t *testing.T) {
	h := newLandingHost(t)
	id := "weft/example-ready-a0b1"
	h.ticket(id, ticket.StatusReady, 2)
	h.script(id, 1, append(through(t, h.landingSegment(id, "worker-segment.ndjson"), "land_step_result", "fast_forward"), dieLine))
	reconcile := reconcileLines(t, [3]string{"done", "not_done", "not_done"}, fixtureHead, fixtureBase)
	reconcile[0]["outcome"] = "halted"
	reconcile[1]["landing"] = map[string]any{"state": "not_applicable", "step": nil, "reason": nil,
		"detail": "the run halted", "branch": nil, "commit": nil}
	h.script(id, 2, reconcile)
	runID := h.waitDispatched(entryOf(h.launch(id, "regraded"))["entry_id"].(string))
	var run *ledger.WeftRun
	waitFor(t, func() bool {
		run = h.run(runID)
		return len(run.Segments) == 2 && run.Phase == ledger.WeftPhaseEnded && h.reaped(run)
	})
	run = h.run(runID)
	if run.Outcome.String != "finished" || run.SettlementState != ledger.WeftSettlementNotLanded ||
		run.SettlementReason.String != ledger.WeftLandUnreconciled || run.SettlementStep.String != ledger.WeftStepPush ||
		run.Segments[1].Failure.String != ledger.ClassProtocol || run.Segments[1].Terminal.Valid || len(run.Steps) != 1 ||
		run.Steps[0].Result.String != "done" || run.Steps[0].Observed.Valid || run.SettlementCommit.String != fixtureHead ||
		run.ActionRequired.String != ledger.WeftActionLandingUnsettled || h.repositoryOf("weft")["dispatch"] != "paused" {
		t.Fatalf("reconcile observing halted = %+v", run)
	}
	if !strings.Contains(h.logs.String(), "observed an outcome other than the finished grade the host recorded") {
		t.Fatalf("log lacks the refusal:\n%s", h.logs.String())
	}
	h.refusesResume(runID, "resume-regraded")
}

// A finished grade's landing ends only landed or not landed (row 11n): an
// ended reporting not_applicable after it fails the segment as a protocol
// error, and the landing goes to reconciliation under a settlement holder
// rather than end finished with nothing landed and nothing signalled.
func TestFinishedGradeEndedNotApplicableIsReconciled(t *testing.T) {
	h := newLandingHost(t)
	id := "weft/example-ready-a0b1"
	h.ticket(id, ticket.StatusReady, 2)
	lines := h.landingSegment(id, "worker-segment.ndjson")
	ended := maps.Clone(lines[len(lines)-1])
	ended["landing"] = map[string]any{"state": "not_applicable", "step": nil, "reason": nil,
		"detail": "nothing to land", "branch": nil, "commit": nil}
	h.script(id, 1, append(append(workerLines{}, lines[:indexOf(lines, "graded")+1]...), ended))
	h.script(id, 2, append(workerLines{nil},
		reconcileLines(t, [3]string{"not_done", "not_done", "not_done"}, fixtureHead, fixtureBase, ledger.WeftLandSteps...)...))
	runID := h.waitDispatched(entryOf(h.launch(id, "lands-nothing"))["entry_id"].(string))

	var run *ledger.WeftRun
	waitFor(t, func() bool {
		run = h.run(runID)
		return len(run.Segments) == 2 && run.SettlementState == ledger.WeftSettlementReconciling && run.Segments[0].Failure.Valid
	})
	if run.Phase != ledger.WeftPhaseSettling || run.Outcome.String != "finished" || run.Segments[0].Terminal.Valid ||
		run.Segments[0].Failure.String != ledger.ClassProtocol || run.Segments[1].Kind != ledger.WeftSegmentReconcile ||
		holderOf(h.repositoryOf("weft")) != "settlement "+runID {
		t.Fatalf("finished grade ended not_applicable = %+v", run)
	}
	if !strings.Contains(h.logs.String(), "ended reports no landing after a finished grade") {
		t.Fatalf("log lacks the refusal:\n%s", h.logs.String())
	}

	h.release(id)
	waitFor(t, func() bool {
		run = h.run(runID)
		return run.SettlementState == ledger.WeftSettlementLanded && h.reaped(run)
	})
	run = h.run(runID)
	if run.Outcome.String != "finished" || run.ActionRequired.Valid || len(run.Segments) != 2 {
		t.Fatalf("reconciled run = %+v", run)
	}
	if got, want := h.commands(id, 2), acks(t, fixtureHead, ledger.WeftLandSteps...); !reflect.DeepEqual(got, want) {
		t.Fatalf("the reconcile was acknowledged %v, want %v", got, want)
	}
	if repo := h.repositoryOf("weft"); repo["reservation"] != nil || repo["dispatch"] != "open" {
		t.Fatalf("repository after the reconcile = %v", repo)
	}
}

// A reconcile that observed a finished grade ends landed or not landed
// (row 21n): after a landing segment died between its finished grade and
// its first intent, a reconciled not_applicable fails the reconcile as a
// protocol error, the landing is held unreconciled at the fast-forward with
// its repository paused, and the operator's retry is admitted and lands.
func TestReconcileOfAFinishedGradeReportingNoLandingIsUnreconciled(t *testing.T) {
	h := newLandingHost(t)
	id := "weft/example-ready-a0b1"
	h.ticket(id, ticket.StatusReady, 2)
	h.script(id, 1, append(through(t, h.landingSegment(id, "worker-segment.ndjson"), "graded", ""), dieLine))
	reconcile := reconcileLines(t, [3]string{"not_done", "not_done", "not_done"}, fixtureHead, fixtureBase)
	reconcile[1]["landing"] = map[string]any{"state": "not_applicable", "step": nil, "reason": nil,
		"detail": "nothing to land", "branch": nil, "commit": nil}
	h.script(id, 2, reconcile)
	runID := h.waitDispatched(entryOf(h.launch(id, "graded-died"))["entry_id"].(string))
	var run *ledger.WeftRun
	waitFor(t, func() bool {
		run = h.run(runID)
		return len(run.Segments) == 2 && run.Phase == ledger.WeftPhaseEnded && h.reaped(run)
	})
	run = h.run(runID)
	if run.Outcome.String != "finished" || run.SettlementState != ledger.WeftSettlementNotLanded ||
		run.SettlementReason.String != ledger.WeftLandUnreconciled || run.SettlementStep.String != ledger.WeftStepFastForward ||
		run.Segments[0].Failure.String != ledger.ClassDied || run.Segments[1].Failure.String != ledger.ClassProtocol ||
		run.Segments[1].Terminal.Valid || run.ActionRequired.String != ledger.WeftActionLandingUnsettled ||
		run.SettlementCommit.String != fixtureHead || h.repositoryOf("weft")["dispatch"] != "paused" {
		t.Fatalf("reconciled not_applicable after a finished grade = %+v", run)
	}
	if !strings.Contains(h.logs.String(), "reconciled reports no landing after observing a finished grade") {
		t.Fatalf("log lacks the refusal:\n%s", h.logs.String())
	}
	h.refusesResume(runID, "resume-unreconciled")
	h.retryLands(id, runID, 3, [3]string{"not_done", "not_done", "not_done"}, ledger.WeftLandSteps...)
}

// unlandedRun is a run of worker-land-failed.ndjson reaped not_landed at
// the push, its repository paused, and the graded head it did not land.
func (h *queueHost) unlandedRun(id string) (*ledger.WeftRun, string) {
	h.t.Helper()
	failed := h.landingSegment(id, "worker-land-failed.ndjson")
	h.script(id, 1, failed)
	run := h.waitReaped(h.waitDispatched(entryOf(h.launch(id, "fails-"+slugOf(id)))["entry_id"].(string)))
	if run.SettlementState != ledger.WeftSettlementNotLanded || h.repositoryOf("weft")["dispatch"] != "paused" {
		h.t.Fatalf("unlanded run = %+v", run)
	}
	return run, failed[indexOf(failed, "graded")]["head"].(string)
}

// continueWeft lifts the weft repository's pause, which dispatches what
// waited on it.
func (h *queueHost) continueWeft(key string) {
	h.t.Helper()
	h.send(http.MethodPost, "/v2/repositories/"+h.repositoryID("weft")+"/continue", key,
		revisionBody(h.repositoryOf("weft")["revision"])).expect(h.t, http.StatusOK)
}

// A run fresh work archived is history: its landing is never reconciled.
// A retry requested after the archive is refused at admission; one queued
// while the archive worker ran is invalidated when the archive settles; and
// one found waiting at dispatch is invalidated there. None launches a
// reconcile, so the old head is never acknowledged (row 23).
func TestArchivedRunIsNeverReconciled(t *testing.T) {
	for _, during := range []bool{false, true} {
		t.Run(fmt.Sprintf("queued during the archive %v", during), func(t *testing.T) {
			h := newLandingHost(t)
			id := "weft/example-lens-2b3c"
			h.ticket(id, ticket.StatusReady, 2)
			old, head := h.unlandedRun(id)
			lines := maintainedLine(t, "worker-maintained-archive.json", map[string]any{
				"archive_ref": ledger.WeftArchiveRef(old.ID), "head": head})
			if during {
				lines = append(workerLines{nil}, lines...)
			}
			h.script(id, 2, lines)
			h.script(id, 1, h.halted(id))
			fresh := entryOf(h.send(http.MethodPost, "/v2/runs/"+old.ID+"/fresh", "fresh", freshBody(old.Revision, old.Worktree, head)))
			if reason, _ := waiting(fresh); reason != "repository_paused" {
				t.Fatalf("fresh over a paused repository = %v", fresh)
			}
			h.continueWeft("continue")
			var retry map[string]any
			if during {
				waitFor(t, func() bool { return h.entry(fresh["entry_id"].(string))["state"] == "archiving" })
				accepted := h.send(http.MethodPost, "/v2/runs/"+old.ID+"/reconcile", "retry", revisionBody(h.run(old.ID).Revision))
				accepted.expect(t, http.StatusAccepted, "reconcile-response.json")
				retry = entryOf(accepted)
				if reason, holder := waiting(retry); reason != "repository_reserved" || holder["id"] != fresh["entry_id"] {
					t.Fatalf("retry during the archive = %v", retry)
				}
				h.release(id)
			}
			newRun := h.waitDispatched(fresh["entry_id"].(string))
			h.waitReaped(newRun)
			archived := h.run(old.ID)
			if archived.Disposition != ledger.WeftDispositionArchived || archived.SupersededBy.String != newRun {
				t.Fatalf("archived run = %+v", archived)
			}
			if during {
				e := h.entry(retry["entry_id"].(string))
				if invalid := e["invalidated"].(map[string]any); e["state"] != "invalidated" || invalid["reason"] != "run_changed" ||
					!strings.Contains(invalid["detail"].(string), "archived") {
					t.Fatalf("retry queued during the archive = %v", e)
				}
			}
			if message := h.send(http.MethodPost, "/v2/runs/"+old.ID+"/reconcile", "retry-after", revisionBody(archived.Revision)).
				refused(t, http.StatusConflict, "not_reconcilable"); !strings.Contains(message, "archived") {
				t.Fatalf("retry after the archive = %s", message)
			}

			// A retry admitted before this guard, waiting at dispatch.
			queued := "e-20261012T120000Z-0000beef"
			h.exec(`INSERT INTO weft_entries(id,kind,ticket,project,repository_id,repository,position,state,run_id,
				token_id,request_key,created_at,updated_at,revision)
				VALUES(?,'reconcile',?,'weft',?,?,(SELECT MAX(position)+1 FROM weft_entries),'waiting',?,'token','k',
				'2026-10-12T12:00:00.000Z','2026-10-12T12:00:00.000Z',1)`, queued, id, h.repositoryID("weft"), h.repository("weft"), old.ID)
			h.service.dispatch()
			waitFor(t, func() bool { return h.entry(queued)["state"] == "invalidated" })
			if invalid := h.entry(queued)["invalidated"].(map[string]any); invalid["reason"] != "run_changed" ||
				!strings.Contains(invalid["detail"].(string), "archived") {
				t.Fatalf("retry at dispatch = %v", invalid)
			}
			if strings.Contains(h.argv(), "reconcile --contract") || len(h.run(old.ID).Segments) != 2 {
				t.Fatalf("an archived run was reconciled: %s", h.argv())
			}
			h.settle()
		})
	}
}

// A dismiss or fresh against a repository a terminal session holds waits
// behind that reservation rather than being refused — dismiss-response.json
// and fresh-response.json show both waiting on a terminal holder (§6.2,
// §6.8) — launches no maintenance worker meanwhile, and dispatches once the
// session's row is released.
func TestMaintenanceWaitsBehindATerminal(t *testing.T) {
	h := newLandingHost(t)
	dismissed, freshed := "weft/example-second-4c2d", "weft/example-fourth-6e4f"
	runs := map[string]*ledger.WeftRun{}
	for _, id := range []string{dismissed, freshed} {
		h.ticket(id, ticket.StatusReady, 2)
		h.script(id, 1, h.halted(id))
		runs[id] = h.waitReaped(h.waitDispatched(entryOf(h.launch(id, "l-"+slugOf(id)))["entry_id"].(string)))
		if err := os.MkdirAll(runs[id].Worktree, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	const terminal = "t-20261011T120000Z-1b0a9c8d"
	h.exec(`INSERT INTO weft_reservations(repository_id,repository,holder_kind,holder_id,acquired_at)
		VALUES(?,?,'terminal',?,'2026-10-11T12:00:00.000Z')`, h.repositoryID("weft"), h.repository("weft"), terminal)
	h.script(dismissed, 2, maintainedLine(t, "worker-maintained-cleanup.json", nil))
	h.script(freshed, 2, maintainedLine(t, "worker-maintained-archive.json", map[string]any{
		"archive_ref": ledger.WeftArchiveRef(runs[freshed].ID), "head": fixtureHead}))

	cleanup := h.send(http.MethodPost, "/v2/runs/"+runs[dismissed].ID+"/dismiss", "dismiss",
		dismissBody(runs[dismissed].Revision, runs[dismissed].Worktree))
	cleanup.expect(t, http.StatusAccepted, "dismiss-response.json")
	fresh := h.send(http.MethodPost, "/v2/runs/"+runs[freshed].ID+"/fresh", "fresh",
		freshBody(runs[freshed].Revision, runs[freshed].Worktree, fixtureHead))
	fresh.expect(t, http.StatusAccepted, "fresh-response.json")
	for _, e := range []map[string]any{entryOf(cleanup), entryOf(fresh)} {
		if reason, holder := waiting(e); reason != "repository_reserved" || holder["kind"] != "terminal" || holder["id"] != terminal ||
			e["waiting"].(map[string]any)["detail"] != "terminal "+terminal+" holds "+h.repository("weft") {
			t.Fatalf("%s entry behind a terminal = %v", e["kind"], e)
		}
	}
	if argv := h.argv(); strings.Contains(argv, "cleanup --contract") || strings.Contains(argv, "archive --contract") {
		t.Fatalf("a maintenance worker launched under a terminal: %s", argv)
	}

	h.exec(`DELETE FROM weft_reservations WHERE holder_kind='terminal'`)
	h.service.dispatch()
	waitFor(t, func() bool {
		run := h.run(runs[dismissed].ID)
		return run.Disposition == ledger.WeftDispositionCleaned && h.reaped(run)
	})
	newRun := h.waitDispatched(entryOf(fresh)["entry_id"].(string))
	h.waitReaped(newRun)
	if archived := h.run(runs[freshed].ID); archived.Disposition != ledger.WeftDispositionArchived || archived.SupersededBy.String != newRun {
		t.Fatalf("archived run = %+v", archived)
	}
	h.settle()
}

// An archive that fails before its worker spawns wrote nothing: its fresh
// entry is invalidated archive_refused with its row and slot released, and
// is not queued again to fail again (§6 settled calls).
func TestArchiveFailingBeforeSpawnIsRefused(t *testing.T) {
	h := newLandingHost(t)
	id := "weft/example-fourth-6e4f"
	h.ticket(id, ticket.StatusReady, 2)
	h.script(id, 1, h.halted(id))
	old := h.waitReaped(h.waitDispatched(entryOf(h.launch(id, "l1"))["entry_id"].(string)))
	// The run directory root cannot hold the archive's run directory.
	root := filepath.Dir(old.Segments[0].RunDir.String)
	if err := os.Rename(root, root+".moved"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(root, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		os.Remove(root)
		os.Rename(root+".moved", root)
	})
	fresh := entryOf(h.send(http.MethodPost, "/v2/runs/"+old.ID+"/fresh", "fresh", freshBody(old.Revision, old.Worktree, fixtureHead)))
	waitFor(t, func() bool {
		return h.entry(fresh["entry_id"].(string))["state"] == "invalidated" && h.reaped(h.run(old.ID))
	})
	e := h.entry(fresh["entry_id"].(string))
	if invalid := e["invalidated"].(map[string]any); invalid["reason"] != "archive_refused" || invalid["detail"] == "" {
		t.Fatalf("archive failed before spawn = %v", e)
	}
	run := h.run(old.ID)
	if run.Disposition != ledger.WeftDispositionRetained || len(run.Segments) != 2 ||
		run.Segments[1].Failure.String != ledger.ClassPreflight || strings.Contains(h.argv(), "archive --contract") {
		t.Fatalf("run after an archive failed before spawn = %+v\n%s", run, h.argv())
	}
	if c := h.queue()["capacity"].(map[string]any); c["running"] != float64(0) || h.repositoryOf("weft")["reservation"] != nil {
		t.Fatalf("a refused archive kept its slot or row: %v %v", c, h.repositoryOf("weft"))
	}
}

// A maintenance entry for a run that has claimed all its segments takes
// nothing at dispatch: it is invalidated run_changed naming the limit, with
// no segment claimed and no row taken.
func TestMaintenanceAtTheSegmentLimitIsInvalidated(t *testing.T) {
	h := newLandingHost(t)
	id := "weft/example-lens-2b3c"
	h.ticket(id, ticket.StatusReady, 2)
	old, _ := h.unlandedRun(id)
	h.exec(`UPDATE weft_segments SET segment=? WHERE run_id=? AND segment=1`, ledger.WeftSegmentLimit, old.ID)
	accepted := h.send(http.MethodPost, "/v2/runs/"+old.ID+"/reconcile", "retry", revisionBody(h.run(old.ID).Revision))
	e := entryOf(accepted)
	invalid, _ := e["invalidated"].(map[string]any)
	if accepted.code != http.StatusAccepted {
		t.Fatalf("retry at the segment limit = %d %s", accepted.code, accepted.raw)
	}
	if e["state"] != "invalidated" || invalid["reason"] != "run_changed" ||
		!strings.Contains(invalid["detail"].(string), fmt.Sprintf("claimed all %d segments", ledger.WeftSegmentLimit)) {
		t.Fatalf("retry at the segment limit = %v", e)
	}
	run := h.run(old.ID)
	if len(run.Segments) != 1 || run.SettlementState != ledger.WeftSettlementNotLanded || strings.Contains(h.argv(), "reconcile --contract") {
		t.Fatalf("run after a retry at the limit = %+v", run)
	}
	if repo := h.repositoryOf("weft"); repo["reservation"] != nil || repo["dispatch"] != "paused" {
		t.Fatalf("repository after a retry at the limit = %v", repo)
	}
}

// A reconcile lost after it read the run's grade keeps that reading (row
// 22u): a landing segment lost before the host recorded its grade, whose
// reconcile observed finished with the fast-forward done and was then lost
// before reconciled, is held finished and unreconciled at the push with its
// repository paused — never closed failed over a done step — and a retry is
// admitted.
func TestLostReconcileKeepsItsReading(t *testing.T) {
	h := newLandingHost(t)
	id := "weft/example-ready-a0b1"
	h.ticket(id, ticket.StatusReady, 2)
	h.script(id, 1, append(through(t, h.landingSegment(id, "worker-segment.ndjson"), "stage", ""), dieLine))
	h.script(id, 2, append(reconcileLines(t, [3]string{"done", "not_done", "not_done"}, fixtureHead, fixtureBase)[:1], dieLine))
	runID := h.waitDispatched(entryOf(h.launch(id, "read-then-lost"))["entry_id"].(string))
	var run *ledger.WeftRun
	waitFor(t, func() bool {
		run = h.run(runID)
		return len(run.Segments) == 2 && run.Phase == ledger.WeftPhaseEnded && h.reaped(run)
	})
	run = h.run(runID)
	if run.Outcome.String != "finished" || run.SettlementState != ledger.WeftSettlementNotLanded ||
		run.SettlementReason.String != ledger.WeftLandUnreconciled || run.SettlementStep.String != ledger.WeftStepPush ||
		len(run.Steps) != 1 || run.Steps[0].Observed.String != "done" || run.Segments[1].Failure.String != ledger.ClassDied ||
		run.SettlementCommit.Valid || run.ActionRequired.String != ledger.WeftActionLandingUnsettled ||
		h.repositoryOf("weft")["dispatch"] != "paused" {
		t.Fatalf("lost reconcile after reading finished = %+v", run)
	}
	h.refusesResume(runID, "resume-read")
	retry := reconcileLines(t, [3]string{"done", "not_done", "not_done"}, fixtureHead, fixtureBase)
	retry[1]["landing"] = map[string]any{"state": "not_landed", "step": "push", "reason": "unreconciled",
		"detail": "no graded head to land", "branch": nil, "commit": nil}
	h.script(id, 3, retry)
	accepted := h.send(http.MethodPost, "/v2/runs/"+runID+"/reconcile", "retry", revisionBody(run.Revision))
	accepted.expect(t, http.StatusAccepted, "reconcile-response.json")
	if e := entryOf(accepted); e["kind"] != "reconcile" || e["run_id"] != runID {
		t.Fatalf("retry = %v", e)
	}
	waitFor(t, func() bool {
		run = h.run(runID)
		return len(run.Segments) == 3 && run.Phase == ledger.WeftPhaseEnded && h.reaped(run)
	})
	if run = h.run(runID); run.Outcome.String != "finished" || run.SettlementStep.String != ledger.WeftStepPush {
		t.Fatalf("retried run = %+v", run)
	}
}

// Fresh over a run with no recorded head — its first segment refused
// branch_exists — archives at the confirmation's authorized_head, which must
// be a commit, passes it to the archive worker as --head, holds the
// archive's done to it, and starts a new run (row 29, §6 settled calls).
func TestFreshArchivesAHeadlessRunAtTheAuthorizedHead(t *testing.T) {
	h := newLandingHost(t)
	id := "weft/example-fourth-6e4f"
	h.ticket(id, ticket.StatusReady, 2)
	refusal := decode(t, fixtureBytes(t, "worker-stale-base.json")).(map[string]any)
	refusal["failure"], refusal["detail"] = "branch_exists", "branch "+id+" already exists"
	h.script(id, 1, workerLines{refusal})
	old := h.waitReaped(h.waitDispatched(entryOf(h.launch(id, "l1"))["entry_id"].(string)))
	if old.Head.Valid || old.Segments[0].Failure.String != "branch_exists" {
		t.Fatalf("branch_exists run = %+v", old)
	}
	fresh := func(key, head string) response {
		return h.send(http.MethodPost, "/v2/runs/"+old.ID+"/fresh", key, freshBody(old.Revision, old.Worktree, head))
	}
	for _, head := range []string{"", "head", strings.ToUpper(fixtureHead), fixtureHead[:39]} {
		if message := fresh("f-"+head, head).refused(t, http.StatusBadRequest, "malformed"); !strings.Contains(message, "authorized_head") {
			t.Fatalf("fresh at %q = %s", head, message)
		}
	}
	h.send(http.MethodPost, "/v2/runs/"+old.ID+"/fresh", "f-null", fmt.Sprintf(
		`{"contract_version":"weft-service/v2","revision":%d,"authorized_worktree":%q,"authorized_head":null}`, old.Revision, old.Worktree)).
		refused(t, http.StatusBadRequest, "malformed")

	h.script(id, 2, maintainedLine(t, "worker-maintained-archive.json", map[string]any{
		"archive_ref": ledger.WeftArchiveRef(old.ID), "head": fixtureHead}))
	h.script(id, 1, h.halted(id))
	accepted := fresh("f-ok", fixtureHead)
	accepted.expect(t, http.StatusAccepted, "fresh-response.json")
	newRun := h.waitDispatched(entryOf(accepted)["entry_id"].(string))
	h.waitReaped(newRun)
	archived := h.run(old.ID)
	if archived.Disposition != ledger.WeftDispositionArchived || archived.SupersededBy.String != newRun ||
		archived.ArchiveRef.String != ledger.WeftArchiveRef(old.ID) {
		t.Fatalf("archived run = %+v", archived)
	}
	if started := h.run(newRun); started.Supersedes.String != old.ID || started.Segments[0].Kind != "launch" {
		t.Fatalf("new run = %+v", started)
	}
	if !strings.Contains(h.argv(), "--archive-ref "+ledger.WeftArchiveRef(old.ID)+" --head "+fixtureHead) {
		t.Fatalf("argv = %s", h.argv())
	}
}

// A not_landed names the reviewed head, never another commit (AC1): an
// ended that does not land a commit other than the graded head fails the
// segment as the protocol error it is, and its landing goes to a reconcile,
// which settles naming the graded head.
func TestNotLandedOfAnotherCommitIsRefused(t *testing.T) {
	h := newLandingHost(t)
	id := "weft/example-lens-2b3c"
	h.ticket(id, ticket.StatusReady, 2)
	failed := h.landingSegment(id, "worker-land-failed.ndjson")
	head, base := failed[indexOf(failed, "graded")]["head"].(string), failed[0]["base"].(string)
	ended := maps.Clone(failed[len(failed)-1])
	landing := maps.Clone(ended["landing"].(map[string]any))
	landing["commit"] = base
	ended["landing"] = landing
	h.script(id, 1, append(append(workerLines{}, failed[:len(failed)-1]...), ended))
	h.script(id, 2, reconcileLines(t, [3]string{"done", "not_done", "not_done"}, head, base, "push", "mark_done"))
	runID := h.waitDispatched(entryOf(h.launch(id, "other-commit"))["entry_id"].(string))
	var run *ledger.WeftRun
	waitFor(t, func() bool {
		run = h.run(runID)
		return len(run.Segments) == 2 && run.SettlementState == ledger.WeftSettlementLanded && h.reaped(run)
	})
	run = h.run(runID)
	if run.Segments[0].Failure.String != ledger.ClassProtocol || run.Segments[0].Terminal.Valid || run.SettlementCommit.String != head {
		t.Fatalf("not_landed of another commit = %+v", run)
	}
	if !strings.Contains(h.logs.String(), "ended reports a landing not landed of a commit other than the graded head") {
		t.Fatalf("log lacks the refusal:\n%s", h.logs.String())
	}
}
