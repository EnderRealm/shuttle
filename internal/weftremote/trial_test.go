package weftremote

import (
	"bufio"
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/EnderRealm/ticket/v8/pkg/ticket"

	"shuttle/internal/failure"
	"shuttle/internal/ledger"
	"shuttle/internal/sandbox"
)

// The handles launch-request-trial.json names, as folder.json lists them.
const (
	fixtureCheckoutHandle = "hd-5b1e8a3c6f0d4e27"
	fixtureStoreHandle    = "hd-7c2f9b4d0e1a5f38"
	fixtureEvidenceHandle = "hd-8d3a0c5e1f2b6a49"
	fixtureTrialTicket    = "evalcell/fix-parser-1a2b"
)

// trialCell is one evaluation cell as folder.json shows one: a checkout no
// project is bound to, a tk store of its own and an evidence directory, each
// canonical, with the handles the host's folder picker minted for them.
type trialCell struct {
	checkout, store, evidence string
	handles                   map[string]string
}

// trialCell makes the cell under the host's root, writes each ticket ready
// into the cell's store alone, and browses the cell for its handles.
func (h *queueHost) trialCell(name string, tickets ...string) trialCell {
	h.t.Helper()
	dir := filepath.Join(h.root, "eval", name)
	for _, sub := range []string{filepath.Join("checkout", ".git", "worktrees"), "tickets", "evidence"} {
		if err := os.MkdirAll(filepath.Join(dir, sub), 0o755); err != nil {
			h.t.Fatal(err)
		}
	}
	for _, id := range tickets {
		project, slug, _ := strings.Cut(id, "/")
		writeTicketIn(h.t, filepath.Join(dir, "tickets"), project, &ticket.Ticket{
			ID: slug, Status: ticket.StatusReady, Type: ticket.TypeFeature, Priority: 2, Title: "Trial " + slug, Body: criterionBody,
			Created: time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC), Updated: time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC),
		})
	}
	listing := h.get("/v2/folders?path=" + dir)
	learn(decode(h.t, fixtureBytes(h.t, "folder.json"))).check(h.t, "folder", listing)
	c := trialCell{handles: map[string]string{}}
	for _, e := range listing["folder"].(map[string]any)["entries"].([]any) {
		e := e.(map[string]any)
		if e["repository_id"] != nil || e["project"] != nil {
			h.t.Fatalf("cell entry %v is in a registered repository", e)
		}
		c.handles[e["name"].(string)] = e["handle"].(string)
		switch e["name"] {
		case "checkout":
			c.checkout = e["path"].(string)
		case "tickets":
			c.store = e["path"].(string)
		case "evidence":
			c.evidence = e["path"].(string)
		}
	}
	if c.checkout == "" || c.store == "" || c.evidence == "" {
		h.t.Fatalf("cell listing = %v", listing)
	}
	return c
}

// request is launch-request-trial.json for ticketID with the cell's handles.
func (c trialCell) request(t *testing.T, ticketID string) string {
	t.Helper()
	return strings.NewReplacer(
		fixtureCheckoutHandle, c.handles["checkout"], fixtureStoreHandle, c.handles["tickets"],
		fixtureEvidenceHandle, c.handles["evidence"], fixtureTrialTicket, ticketID,
	).Replace(string(fixtureBytes(t, "launch-request-trial.json")))
}

func (c trialCell) repositoryID() string { return sandbox.RepositoryID(c.checkout) }

// probed is the host with its worker probed, which serves trial_launch while
// the worker lists trial_environment.
func probed(t *testing.T) *queueHost {
	h := newQueueHost(t)
	h.service.dispatch()
	return h
}

func (h *queueHost) capability(id string) map[string]any {
	h.t.Helper()
	for _, c := range h.get("/v2/service")["service"].(map[string]any)["capabilities"].([]any) {
		if c := c.(map[string]any); c["id"] == id {
			return c
		}
	}
	h.t.Fatalf("no %s capability", id)
	return nil
}

// trialColumns is the entry's weft_entries row as the trial columns hold it.
func (h *queueHost) trialColumns(entryID string) []string {
	h.t.Helper()
	var checkout, store, evidence, repository, repositoryID sql.NullString
	if err := h.db().QueryRow(`SELECT checkout,tk_store_root,work_evidence_dir,repository,repository_id FROM weft_entries WHERE id=?`, entryID).
		Scan(&checkout, &store, &evidence, &repository, &repositoryID); err != nil {
		h.t.Fatal(err)
	}
	return []string{checkout.String, store.String, evidence.String, repository.String, repositoryID.String}
}

// A trial launch is admitted against its own store and checkout (§11.7): the
// ticket is found in tk_store_root's store and nowhere in the host catalog,
// the entry reserves the repository the checkout's canonical path derives,
// and its row records the three canonical paths.
func TestTrialLaunchAdmitted(t *testing.T) {
	h := probed(t)
	if c := h.capability("trial_launch"); c["state"] != "served" || c["reason"] != nil {
		t.Fatalf("trial_launch under a worker listing trial_environment = %v", c)
	}
	const second = "evalcell/second-3c4d"
	cell := h.trialCell("c-0042", fixtureTrialTicket, second)
	h.mode(fixtureTrialTicket, "hold")

	r := h.send(http.MethodPost, "/v2/launches", "trial-1", cell.request(t, fixtureTrialTicket))
	r.expect(t, http.StatusAccepted, "launch-response-trial.json")
	entryShape(t).check(t, "entry", entryOf(r))
	entry := entryOf(r)
	var want map[string]any
	fixtureJSON(t, "launch-response-trial.json", &want)
	for _, key := range []string{"kind", "ticket", "project", "state", "selection_id", "waiting", "invalidated", "supersedes", "answer_id", "revision"} {
		if !reflect.DeepEqual(entry[key], want["entry"].(map[string]any)[key]) {
			t.Errorf("entry.%s = %v, launch-response-trial.json has %v", key, entry[key], want["entry"].(map[string]any)[key])
		}
	}
	if entry["repository_id"] != cell.repositoryID() || !strings.HasPrefix(cell.repositoryID(), "checkout-") {
		t.Fatalf("entry repository_id = %v, want %s", entry["repository_id"], cell.repositoryID())
	}
	entryID, runID := entry["entry_id"].(string), entry["run_id"].(string)
	if got, recorded := h.trialColumns(entryID), []string{cell.checkout, cell.store, cell.evidence, cell.checkout, cell.repositoryID()}; !reflect.DeepEqual(got, recorded) {
		t.Fatalf("weft_entries row = %q, want %q", got, recorded)
	}
	var holderKind, holderID, reserved string
	if err := h.db().QueryRow(`SELECT holder_kind,holder_id,repository FROM weft_reservations WHERE repository_id=?`, cell.repositoryID()).
		Scan(&holderKind, &holderID, &reserved); err != nil || holderKind != ledger.WeftHolderRun || holderID != runID || reserved != cell.checkout {
		t.Fatalf("reservation = %s %s %s (%v), want run %s on %s", holderKind, holderID, reserved, err, runID, cell.checkout)
	}
	run := h.run(runID)
	if run.Repo != cell.checkout || run.RepositoryID != cell.repositoryID() || run.Project != "evalcell" || run.EntryID.String != entryID {
		t.Fatalf("minted run = %+v", run)
	}

	// A second launch into the checkout waits on the run holding it, as one
	// into a registered repository does.
	held := entryOf(h.send(http.MethodPost, "/v2/launches", "trial-2", cell.request(t, second)))
	if reason, holder := waiting(held); reason != "repository_reserved" || !reflect.DeepEqual(holder, map[string]any{"kind": "run", "id": runID}) ||
		held["repository_id"] != cell.repositoryID() {
		t.Fatalf("second trial entry = %v", held)
	}
	h.release(fixtureTrialTicket)
	h.waitPhase(runID, ledger.WeftPhaseParked)
	h.waitPhase(h.waitDispatched(held["entry_id"].(string)), ledger.WeftPhaseParked)

	// A ticket the host catalog holds and the cell's store does not is not
	// found: the host catalog is never consulted for a trial ticket.
	h.ticket("weft/host-only-0001", ticket.StatusReady, 2)
	message := h.send(http.MethodPost, "/v2/launches", "trial-3", cell.request(t, "weft/host-only-0001")).
		refused(t, http.StatusNotFound, "ticket_not_found")
	if !strings.Contains(message, cell.store) {
		t.Fatalf("ticket_not_found = %s, want it to name the store %s", message, cell.store)
	}
}

// A checkout launch must name tk_store_root and must not name repository_id;
// any trial launch is not served while the probed worker lacks
// trial_environment, and trial_launch is served only while it lists it.
func TestTrialLaunchRefused(t *testing.T) {
	h := probed(t)
	cell := h.trialCell("c-0043", fixtureTrialTicket)
	body := func(edit func(doc map[string]any)) string {
		var doc map[string]any
		if err := json.Unmarshal([]byte(cell.request(t, fixtureTrialTicket)), &doc); err != nil {
			t.Fatal(err)
		}
		edit(doc)
		out, err := json.Marshal(doc)
		if err != nil {
			t.Fatal(err)
		}
		return string(out)
	}
	noStore := body(func(doc map[string]any) { doc["environment"].(map[string]any)["tk_store_root"] = nil })
	if message := h.send(http.MethodPost, "/v2/launches", "r1", noStore).refused(t, http.StatusBadRequest, "malformed"); !strings.Contains(message, "tk_store_root") {
		t.Fatalf("checkout launch without tk_store_root = %s", message)
	}
	noEnvironment := body(func(doc map[string]any) { doc["environment"] = nil })
	if message := h.send(http.MethodPost, "/v2/launches", "r2", noEnvironment).refused(t, http.StatusBadRequest, "malformed"); !strings.Contains(message, "tk_store_root") {
		t.Fatalf("checkout launch without environment = %s", message)
	}
	both := string(fixtureBytes(t, "negative/launch-request-checkout-and-repository.json"))
	if message := h.send(http.MethodPost, "/v2/launches", "r3", both).refused(t, http.StatusBadRequest, "malformed"); !strings.Contains(message, "not both") {
		t.Fatalf("repository_id with checkout = %s", message)
	}
	noCheckout := body(func(doc map[string]any) { doc["checkout"], doc["repository_id"] = nil, h.repositoryID("weft") })
	h.send(http.MethodPost, "/v2/launches", "r4", noCheckout).refused(t, http.StatusBadRequest, "malformed")
	if queue := h.queue(); len(queue["entries"].([]any)) != 0 {
		t.Fatalf("a refused trial launch admitted %v", queue["entries"])
	}

	// A worker that lacks trial_environment serves no trial launch, whatever
	// the request's shape.
	capabilities, err := json.Marshal(h.writeVersion("land", "reconcile", "trial_environment"))
	if err != nil {
		t.Fatal(err)
	}
	h.worker(strings.Replace(strings.Replace(queueWorkerScript, "CONTROL", h.control, 1), "CAPABILITIES", string(capabilities), 1))
	h.service.dispatch()
	if c := h.capability("trial_launch"); c["state"] != "not_served" || !strings.Contains(c["reason"].(string), "lacks trial_environment") {
		t.Fatalf("trial_launch under a worker without trial_environment = %v", c)
	}
	for i, request := range []string{cell.request(t, fixtureTrialTicket), noStore, both} {
		message := h.send(http.MethodPost, "/v2/launches", fmt.Sprintf("n%d", i), request).refused(t, http.StatusNotImplemented, "capability_not_served")
		if !strings.HasPrefix(message, "trial_launch is not served by host "+h.service.hostID) {
			t.Fatalf("not-served trial launch = %s", message)
		}
	}
	// A registered launch is not a trial launch and is still served.
	h.ticket("weft/registered-0001", ticket.StatusReady, 2)
	h.waitPhase(entryOf(h.launch("weft/registered-0001", "registered"))["run_id"].(string), ledger.WeftPhaseParked)
}

// A trial segment's argv carries the store and evidence paths dispatch
// revalidated, on every segment of the run; a path that no longer
// canonicalizes to what was admitted invalidates the entry before anything
// launches. hello must report exactly those paths and registered false on a
// trial segment, and no trial environment on any other.
func TestTrialLaunchDispatch(t *testing.T) {
	h := probed(t)
	const (
		held   = "evalcell/held-2b3c"
		behind = "evalcell/behind-3c4d"
	)
	cell := h.trialCell("c-0044", fixtureTrialTicket, held, behind)
	trialArgv := func(argv []string) {
		t.Helper()
		if argvValue(argv, "--tk-store-root") != cell.store || argvValue(argv, "--work-evidence-dir") != cell.evidence ||
			argvValue(argv, "--repo") != cell.checkout {
			t.Fatalf("trial segment argv = %q", argv)
		}
	}

	r := h.send(http.MethodPost, "/v2/launches", "d1", cell.request(t, fixtureTrialTicket))
	r.expect(t, http.StatusAccepted, "launch-response-trial.json")
	runID := entryOf(r)["run_id"].(string)
	run := h.waitPhase(runID, ledger.WeftPhaseParked)
	trialArgv(recordedArgv(t, run.Segments[0].RunDir.String))
	hello := firstEvent(t, run.Segments[0].RunDir.String)
	learn(decode(t, fixtureBytes(t, "worker-hello-trial.json"))).check(t, "hello", hello)
	if hello["registered"] != false || !reflect.DeepEqual(hello["environment"], map[string]any{"tk_store_root": cell.store, "work_evidence_dir": cell.evidence}) {
		t.Fatalf("accepted hello = %v", hello)
	}

	// The answer's segment is the same trial's: its entry is held to the
	// cell, and it launches with the cell's paths.
	answered := entryOf(h.send(http.MethodPost, "/v2/runs/"+runID+"/answer", "d2", fmt.Sprintf(
		`{"contract_version":"weft-service/v2","question_id":%q,"answer":"The flat one."}`, run.Question.ID)))
	if got := h.trialColumns(answered["entry_id"].(string)); !reflect.DeepEqual(got[:3], []string{cell.checkout, cell.store, cell.evidence}) {
		t.Fatalf("answer entry's trial columns = %q", got)
	}
	waitFor(t, func() bool {
		run = h.run(runID)
		return len(run.Segments) == 2 && run.Phase == ledger.WeftPhaseParked && h.reaped(run)
	})
	trialArgv(recordedArgv(t, run.Segments[1].RunDir.String))

	// An entry waits on the checkout while its evidence directory is replaced
	// by a link elsewhere: dispatch invalidates it rather than launch a path
	// other than the one admitted.
	h.mode(held, "hold")
	heldRun := entryOf(h.send(http.MethodPost, "/v2/launches", "d3", cell.request(t, held)))["run_id"].(string)
	h.waitPhase(heldRun, ledger.WeftPhaseRunning)
	queued := entryOf(h.send(http.MethodPost, "/v2/launches", "d4", cell.request(t, behind)))
	if reason, _ := waiting(queued); reason != "repository_reserved" {
		t.Fatalf("entry behind the held run = %v", queued)
	}
	moved := cell.evidence + "-moved"
	if err := os.Rename(cell.evidence, moved); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(moved, cell.evidence); err != nil {
		t.Fatal(err)
	}
	h.release(held)
	h.waitPhase(heldRun, ledger.WeftPhaseParked)
	waitFor(t, func() bool { return h.entry(queued["entry_id"].(string))["state"] == "invalidated" })
	invalidated := h.entry(queued["entry_id"].(string))["invalidated"].(map[string]any)
	if invalidated["reason"] != ledger.WeftInvalidBinding || !strings.Contains(invalidated["detail"].(string), cell.evidence+", which "+queued["entry_id"].(string)+" was admitted for, now resolves to "+moved) {
		t.Fatalf("invalidated = %v", invalidated)
	}
	if len(h.invocations(behind)) != 0 {
		t.Fatalf("the invalidated entry launched: %v", h.invocations(behind))
	}

	// hello is a protocol failure when it reports another environment or a
	// registered repository on a trial segment, or a trial environment on a
	// registered one.
	var fixtureHello struct {
		Environment json.RawMessage `json:"environment"`
	}
	fixtureJSON(t, "worker-hello-trial.json", &fixtureHello)
	var compact bytes.Buffer
	if err := json.Compact(&compact, fixtureHello.Environment); err != nil {
		t.Fatal(err)
	}
	fixtureEnvironment := `"environment":` + compact.String() + `,"registered":false`
	other := h.trialCell("c-0045", "evalcell/elsewhere-4d5e", "evalcell/registered-5e6f")
	h.ticket("weft/trial-hello-0001", ticket.StatusReady, 2)
	for _, c := range []struct {
		ticket, environment string
		launch              func(key string) response
	}{
		{"evalcell/elsewhere-4d5e", fixtureEnvironment,
			func(key string) response {
				return h.send(http.MethodPost, "/v2/launches", key, other.request(t, "evalcell/elsewhere-4d5e"))
			}},
		{"evalcell/registered-5e6f", `"environment":{"tk_store_root":null,"work_evidence_dir":null},"registered":true`,
			func(key string) response {
				return h.send(http.MethodPost, "/v2/launches", key, other.request(t, "evalcell/registered-5e6f"))
			}},
		{"weft/trial-hello-0001", fixtureEnvironment,
			func(key string) response { return h.launch("weft/trial-hello-0001", key) }},
	} {
		h.writeControl(slugOf(c.ticket)+".environment", []byte(c.environment))
		failed := h.waitReaped(entryOf(c.launch("hello-" + c.ticket))["run_id"].(string))
		if got := failed.Segments[0].Failure.String; got != failure.Protocol {
			t.Fatalf("%s with hello %s: segment failure %q, want %s", c.ticket, c.environment, got, failure.Protocol)
		}
	}
	if n := strings.Count(h.logs.String(), "hello reports a registered repository or a trial environment other than the one the segment was launched with"); n != 2 {
		t.Errorf("%d trial hello refusals logged, want 2:\n%s", n, h.logs.String())
	}
	if !strings.Contains(h.logs.String(), "hello reports a trial environment or unregistered checkout no launch named") {
		t.Errorf("log lacks the registered segment's hello refusal:\n%s", h.logs.String())
	}
}

// firstEvent is the first line a segment's worker wrote, as recorded.
func firstEvent(t *testing.T, runDir string) map[string]any {
	t.Helper()
	f, err := os.Open(filepath.Join(runDir, "events.ndjson"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	if !scanner.Scan() {
		t.Fatalf("%s has no event", runDir)
	}
	return decode(t, scanner.Bytes()).(map[string]any)
}

// trialLanding is worker-segment.ndjson as a trial run's landing segment
// reports it: the cell's worktree, its paths in hello and an unregistered
// checkout.
func (h *queueHost) trialLanding(c trialCell, ticketID string) workerLines {
	h.t.Helper()
	worktree, err := sandbox.WorktreePath(h.service.segments.sharedRoot, c.checkout, ticketID)
	if err != nil {
		h.t.Fatal(err)
	}
	lines := h.landingSegmentAt(ticketID, worktree, "worker-segment.ndjson")
	for _, ev := range lines {
		if ev != nil && ev["event"] == "hello" {
			ev["environment"] = map[string]any{"tk_store_root": c.store, "work_evidence_dir": c.evidence}
			ev["registered"] = false
		}
	}
	return lines
}

// reconcileArgv is the argv of the run's reconcile worker as the fixture
// worker logged it, or "" when none was launched.
func (h *queueHost) reconcileArgv(runID string) string {
	h.t.Helper()
	for _, line := range strings.Split(h.argv(), "\n") {
		if strings.HasPrefix(line, "reconcile --contract weft-worker/v2 --run "+runID+" ") {
			return line
		}
	}
	return ""
}

// A trial run's lost landing is reconciled in its own store: the reconcile
// is launched with --tk-store-root, held to the path admitted at its
// preflight, since no dispatch revalidates it and its worker writes that
// store as the operator. A store moved under a link to another store is
// refused before any worker launches, and the landing stays unreconciled.
func TestTrialLaunchDispatchReconcile(t *testing.T) {
	h := newLandingHost(t)
	const (
		landed     = "evalcell/landed-6f70"
		redirected = "evalcell/redirected-7081"
	)
	cell := h.trialCell("c-0046", landed)
	h.script(landed, 1, append(through(t, h.trialLanding(cell, landed), "graded", ""), dieLine))
	h.script(landed, 2, reconcileLines(t, [3]string{"not_done", "not_done", "not_done"}, fixtureHead, fixtureBase, ledger.WeftLandSteps...))
	runID := entryOf(h.send(http.MethodPost, "/v2/launches", "landed", cell.request(t, landed)))["run_id"].(string)
	waitFor(t, func() bool {
		run := h.run(runID)
		return run.SettlementState == ledger.WeftSettlementLanded && h.reaped(run)
	})
	if argv := h.reconcileArgv(runID); !strings.HasSuffix(argv, " --tk-store-root "+cell.store) {
		t.Fatalf("trial reconcile argv = %q, want --tk-store-root %s\n%s", argv, cell.store, h.argv())
	}

	moved := h.trialCell("c-0047", redirected)
	h.script(redirected, 1, append(append(through(t, h.trialLanding(moved, redirected), "graded", ""), nil), dieLine))
	h.script(redirected, 2, reconcileLines(t, [3]string{"not_done", "not_done", "not_done"}, fixtureHead, fixtureBase, ledger.WeftLandSteps...))
	runID = entryOf(h.send(http.MethodPost, "/v2/launches", "redirected", moved.request(t, redirected)))["run_id"].(string)
	h.waitPhase(runID, ledger.WeftPhaseRunning)
	if err := os.Rename(moved.store, moved.store+"-moved"); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(cell.store, moved.store); err != nil {
		t.Fatal(err)
	}
	h.release(redirected)
	var run *ledger.WeftRun
	// The segment's row reads settled a moment before the run's settlement
	// does: GetWeftRun is not one read.
	waitFor(t, func() bool {
		run = h.run(runID)
		return len(run.Segments) == 2 && h.reaped(run) && run.SettlementState != ledger.WeftSettlementReconciling
	})
	if seg := run.Segments[1]; seg.Kind != ledger.WeftSegmentReconcile || seg.Failure.String != failure.Preflight ||
		run.SettlementState != ledger.WeftSettlementNotLanded {
		t.Fatalf("reconcile over a redirected store = %+v", run)
	}
	if argv := h.reconcileArgv(runID); argv != "" {
		t.Fatalf("a reconcile was launched over a redirected store: %s", argv)
	}
	if want := "the tk store " + moved.store + " run " + runID + " was admitted for now resolves to " + cell.store; !strings.Contains(h.logs.String(), want) {
		t.Errorf("log lacks %q:\n%s", want, h.logs.String())
	}
}

// A trial run's ticket is read from its own store and never from the host
// catalog: with the same qualified id in both, the host's ticket finishing
// neither ends the trial's parked run nor queues a host-started cleanup of
// it — on the catalog change, once the run is stopped, or at a restart —
// while the trial's own ticket is still ready.
func TestTrialLaunchHostCatalogIsolated(t *testing.T) {
	h := probed(t)
	if c := h.capability("cleanup"); c["state"] != "served" {
		t.Fatalf("cleanup = %v; host cleanup must be served for this test to mean anything", c)
	}
	const shared = "weft/shared-0001"
	h.ticket(shared, ticket.StatusReady, 2)
	cell := h.trialCell("c-0048", shared)
	runID := entryOf(h.send(http.MethodPost, "/v2/launches", "isolated", cell.request(t, shared)))["run_id"].(string)
	run := h.waitPhase(runID, ledger.WeftPhaseParked)
	question := run.Question.ID
	cleanups := func() int {
		t.Helper()
		var n int
		if err := h.db().QueryRow(`SELECT count(*) FROM weft_entries WHERE run_id=? AND kind=?`, runID, ledger.WeftEntryCleanup).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}

	h.ticket(shared, ticket.StatusDone, 2)
	if _, err := h.service.syncCatalog(); err != nil {
		t.Fatal(err)
	}
	h.service.dispatch()
	run = h.run(runID)
	if run.Phase != ledger.WeftPhaseParked || run.Question == nil || run.Question.ID != question || run.Question.State != "pending" || cleanups() != 0 {
		t.Fatalf("the host ticket finishing reached the trial run: %+v, %d cleanup entries", run, cleanups())
	}

	h.send(http.MethodPost, "/v2/runs/"+runID+"/stop", "stop", revisionBody(run.Revision)).expect(t, http.StatusOK)
	if err := h.service.Restart(); err != nil {
		t.Fatal(err)
	}
	h.service.dispatch()
	h.settle()
	run = h.run(runID)
	if run.Phase != ledger.WeftPhaseEnded || run.Disposition != ledger.WeftDispositionRetained || cleanups() != 0 {
		t.Fatalf("the stopped trial run was cleaned for the host's ticket: %+v, %d cleanup entries", run, cleanups())
	}
	if _, err := os.Stat(run.Worktree); err != nil {
		t.Fatalf("the trial worktree is gone: %v", err)
	}
}

// Two cells may run one qualified ticket id at once (§6.2, §11.7): the
// per-ticket guard and prior_run hold a ticket within the store it is read
// from, so each cell's launch is admitted and dispatches on its own, and the
// host catalog's ticket of that id and a trial's never refuse each other,
// in flight or retained. Within one store both still hold.
func TestTrialLaunchSameTicketTwoCells(t *testing.T) {
	h := probed(t)
	const (
		shared   = "weft/twin-0001"
		hostHeld = "weft/host-held-0002"
	)
	launched := func(r response) *ledger.WeftRun {
		t.Helper()
		r.expect(t, http.StatusAccepted)
		return h.waitPhase(h.waitDispatched(entryOf(r)["entry_id"].(string)), ledger.WeftPhaseParked)
	}
	stop := func(run *ledger.WeftRun) {
		t.Helper()
		h.send(http.MethodPost, "/v2/runs/"+run.ID+"/stop", "stop-"+run.ID, revisionBody(h.run(run.ID).Revision)).expect(t, http.StatusOK)
		h.waitPhase(run.ID, ledger.WeftPhaseEnded)
	}
	inCell := func(run *ledger.WeftRun, c trialCell) {
		t.Helper()
		if run.Repo != c.checkout || run.RepositoryID != c.repositoryID() || argvValue(recordedArgv(t, run.Segments[0].RunDir.String), "--tk-store-root") != c.store {
			t.Fatalf("run %s = repo %s (%s), argv %q; want the cell at %s with store %s", run.ID, run.Repo, run.RepositoryID,
				recordedArgv(t, run.Segments[0].RunDir.String), c.checkout, c.store)
		}
	}

	first, second := h.trialCell("c-0050", shared), h.trialCell("c-0051", shared)
	a := launched(h.send(http.MethodPost, "/v2/launches", "twin-a", first.request(t, shared)))
	b := launched(h.send(http.MethodPost, "/v2/launches", "twin-b", second.request(t, shared)))
	if a.ID == b.ID {
		t.Fatalf("both cells' launches dispatched run %s", a.ID)
	}
	inCell(a, first)
	inCell(b, second)
	if h.run(a.ID).Phase != ledger.WeftPhaseParked {
		t.Fatalf("the second cell's run reached the first's: %+v", h.run(a.ID))
	}

	// Within one store the guard holds: a parked run of the ticket refuses
	// another launch of it, and once retained it is prior_run.
	if message := h.send(http.MethodPost, "/v2/launches", "twin-a-again", first.request(t, shared)).
		refused(t, http.StatusConflict, "ticket_in_flight"); !strings.Contains(message, a.ID) {
		t.Fatalf("ticket_in_flight = %s, want it to name %s", message, a.ID)
	}
	stop(a)
	if message := h.send(http.MethodPost, "/v2/launches", "twin-a-prior", first.request(t, shared)).
		refused(t, http.StatusConflict, "prior_run"); !strings.Contains(message, a.ID) {
		t.Fatalf("prior_run = %s, want it to name %s", message, a.ID)
	}

	// The host catalog's ticket of the id reads eligible with no run, and
	// launches over a retained trial run and a parked one.
	h.ticket(shared, ticket.StatusReady, 2)
	detail := h.get("/v2/tickets/" + shared)["ticket"].(map[string]any)
	if detail["eligibility"].(map[string]any)["state"] != "eligible" || detail["run"] != nil || len(detail["runs"].([]any)) != 0 {
		t.Fatalf("host catalog ticket over the trials' runs = %v", detail)
	}
	host := launched(h.launch(shared, "twin-host"))
	if host.Repo != h.repository("weft") {
		t.Fatalf("host run = %+v", host)
	}

	// A trial launches over the host's run of the id, parked or retained,
	// while the host's own guard still holds for the host's ticket.
	h.ticket(hostHeld, ticket.StatusReady, 2)
	retained := launched(h.launch(hostHeld, "held-host"))
	stop(retained)
	third := h.trialCell("c-0052", shared, hostHeld)
	inCell(launched(h.send(http.MethodPost, "/v2/launches", "twin-third", third.request(t, shared))), third)
	inCell(launched(h.send(http.MethodPost, "/v2/launches", "held-third", third.request(t, hostHeld))), third)
	if message := h.launch(hostHeld, "held-host-again").refused(t, http.StatusConflict, "prior_run"); !strings.Contains(message, retained.ID) {
		t.Fatalf("host prior_run = %s, want it to name %s", message, retained.ID)
	}
	if message := h.launch(shared, "twin-host-again").refused(t, http.StatusConflict, "ticket_in_flight"); !strings.Contains(message, host.ID) {
		t.Fatalf("host ticket_in_flight = %s, want it to name %s", message, host.ID)
	}
}

// gitCheckout makes the cell's checkout a real git repository with one
// commit, so a cleanup worker's `git worktree remove` runs in it.
func (h *queueHost) gitCheckout(c trialCell) {
	h.t.Helper()
	if err := os.RemoveAll(filepath.Join(c.checkout, ".git")); err != nil {
		h.t.Fatal(err)
	}
	h.git(c.checkout, "init", "-q", "-b", "main")
	h.git(c.checkout, "commit", "-q", "--allow-empty", "-m", "base")
}

// A trial run whose ticket is done in its own store is host-cleaned as a
// registered run is (§6.5, §6.8, §11.7): its finished state is read from the
// store it was admitted against. The host catalog's ticket of the same id
// finishing neither ends nor cleans it. Once its own store reads it done —
// the host's ticket ready again, so nothing but that store can say so — the
// restart's pass ends the parked run, its question withdrawn, and admits the
// host-started cleanup, which carries the run's environment and removes the
// worktree from the cell's checkout, keeping the branch.
func TestTrialLaunchCleanedFromOwnStore(t *testing.T) {
	h := newHostCleanupHost(t)
	const shared = "weft/own-store-0001"
	h.ticket(shared, ticket.StatusReady, 2)
	cell := h.trialCell("c-0060", shared)
	h.gitCheckout(cell)
	parked := decode(t, fixtureBytes(t, "worker-parked.json")).(map[string]any)
	parked["worker_run_id"] = fixtureWorkerRun
	parked["summary"].(map[string]any)["report"].(map[string]any)["ticket"] = shared
	h.script(shared, 1, workerLines{h.trialLanding(cell, shared)[0], parked})
	runID := h.waitDispatched(entryOf(h.send(http.MethodPost, "/v2/launches", "own-store", cell.request(t, shared)))["entry_id"].(string))
	run := h.waitPhase(runID, ledger.WeftPhaseParked)
	if run.Repo != cell.checkout {
		t.Fatalf("trial run = %+v", run)
	}
	h.addWorktree(run)
	question := run.Question.ID

	h.finish(shared, ticket.StatusDone)
	h.service.dispatch()
	h.settle()
	if got := h.run(runID); got.Phase != ledger.WeftPhaseParked || got.Question == nil || got.Question.State != "pending" || len(h.cleanupEntries(runID)) != 0 {
		t.Fatalf("the host ticket finishing reached the trial run: %+v, cleanups %+v", got, h.cleanupEntries(runID))
	}

	h.ticket(shared, ticket.StatusReady, 2)
	writeTicketIn(t, cell.store, "weft", &ticket.Ticket{
		ID: "own-store-0001", Status: ticket.StatusDone, Type: ticket.TypeFeature, Priority: 2, Title: "Trial own-store-0001", Body: criterionBody,
		Created: time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC), Updated: time.Date(2026, 10, 2, 0, 0, 0, 0, time.UTC),
	})
	h.restart()
	var cleaned *ledger.WeftRun
	waitFor(t, func() bool {
		cleaned = h.run(runID)
		return cleaned.Disposition == ledger.WeftDispositionCleaned && h.reaped(cleaned)
	})
	if cleaned.Phase != ledger.WeftPhaseEnded || cleaned.Outcome.String != "stopped" {
		t.Fatalf("host-cleaned trial run = %+v", cleaned)
	}
	if _, err := os.Stat(run.Worktree); !errors.Is(err, os.ErrNotExist) || h.registered(run) {
		t.Fatalf("the trial worktree is still there: %v\n%s", err, h.git(run.Repo, "worktree", "list"))
	}
	if !h.branchKept(run) {
		t.Fatalf("the cleanup removed branch %s", run.Branch)
	}
	if q, err := h.store.WeftQuestion(question); err != nil || q.State != "withdrawn" {
		t.Fatalf("question of the cleaned trial run = %+v %v", q, err)
	}
	entries := h.cleanupEntries(runID)
	if len(entries) != 1 || entries[0].token != ledger.WeftHostToken || entries[0].state != "dispatched" {
		t.Fatalf("host-started cleanup = %+v", entries)
	}
	if got := h.trialColumns(entries[0].id); !reflect.DeepEqual(got[:3], []string{cell.checkout, cell.store, cell.evidence}) {
		t.Fatalf("cleanup entry's trial columns = %q", got)
	}
	if !strings.Contains(h.argv(), "cleanup --contract weft-worker/v2 --run "+runID+" ") || !strings.Contains(h.argv(), "--repo "+cell.checkout+" ") {
		t.Fatalf("argv = %s", h.argv())
	}
	h.settle()
}

// A trial run whose landing leaves its ticket in testing in its own store is
// cleaned in the commit that reaps it, as a registered run's is (§6.8,
// §11.7): no restart and no tk watch pass, which never reads a trial store, is
// needed. The ticket is in no host project, so only the cell's store can say
// it is in testing.
func TestTrialLaunchCleanedOnSettle(t *testing.T) {
	h := newHostCleanupHost(t)
	const lands = "evalcell/lands-8192"
	cell := h.trialCell("c-0061", lands)
	h.gitCheckout(cell)
	lines := h.trialLanding(cell, lands)
	h.script(lands, 1, splice(lines, indexOf(lines, "ended"), nil))
	runID := h.waitDispatched(entryOf(h.send(http.MethodPost, "/v2/launches", "lands", cell.request(t, lands)))["entry_id"].(string))
	waitFor(t, func() bool {
		r := h.run(runID)
		return r.Phase == ledger.WeftPhaseSettling && len(r.Steps) == 3 && r.Steps[2].Result.String == "done"
	})
	run := h.run(runID)
	h.addWorktree(run)
	writeTicketIn(t, cell.store, "evalcell", &ticket.Ticket{
		ID: "lands-8192", Status: ticket.StatusTesting, Type: ticket.TypeFeature, Priority: 2, Title: "Trial lands-8192", Body: criterionBody,
		Created: time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC), Updated: time.Date(2026, 10, 2, 0, 0, 0, 0, time.UTC),
	})
	if entries := h.cleanupEntries(runID); len(entries) != 0 {
		t.Fatalf("a landing trial run with its worker live was queued for cleanup: %+v", entries)
	}
	h.release(lands)
	var cleaned *ledger.WeftRun
	waitFor(t, func() bool {
		cleaned = h.run(runID)
		return cleaned.Disposition == ledger.WeftDispositionCleaned && h.reaped(cleaned)
	})
	if cleaned.SettlementState != ledger.WeftSettlementLanded || h.registered(run) || !h.branchKept(run) {
		t.Fatalf("landed trial run = %+v", cleaned)
	}
	entries := h.cleanupEntries(runID)
	if len(entries) != 1 || entries[0].token != ledger.WeftHostToken {
		t.Fatalf("landed trial run's cleanup = %+v", entries)
	}
	if got := h.trialColumns(entries[0].id); !reflect.DeepEqual(got[:3], []string{cell.checkout, cell.store, cell.evidence}) {
		t.Fatalf("cleanup entry's trial columns = %q", got)
	}
	h.settle()
}

// A landed trial run's cleanup waiting on the host's pause keeps its place
// once its ticket is failed in its own store, as a registered run's does
// (§6.8, §11.7): only that store can say it failed.
func TestTrialLandedCleanupSurvivesFail(t *testing.T) {
	h := newHostCleanupHost(t)
	const lands = "evalcell/lands-8192"
	cell := h.trialCell("c-0062", lands)
	h.gitCheckout(cell)
	lines := h.trialLanding(cell, lands)
	h.script(lands, 1, splice(lines, indexOf(lines, "ended"), nil))
	runID := h.waitDispatched(entryOf(h.send(http.MethodPost, "/v2/launches", "lands", cell.request(t, lands)))["entry_id"].(string))
	waitFor(t, func() bool { return len(h.run(runID).Steps) == 3 })
	h.addWorktree(h.run(runID))
	store := func(status ticket.Status, tests ...ticket.TestRow) {
		writeTicketIn(t, cell.store, "evalcell", &ticket.Ticket{
			ID: "lands-8192", Status: status, Type: ticket.TypeFeature, Priority: 2, Title: "Trial lands-8192", Body: criterionBody, Tests: tests,
			Created: time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC), Updated: time.Date(2026, 10, 2, 0, 0, 0, 0, time.UTC),
		})
	}
	store(ticket.StatusTesting)
	if err := h.store.Pause(ledger.WeftAgent); err != nil {
		t.Fatal(err)
	}
	h.release(lands)
	h.waitReaped(runID)
	if entries := h.cleanupEntries(runID); len(entries) != 1 || entries[0].state != "waiting" {
		t.Fatalf("cleanup while the host is paused = %+v", entries)
	}

	store(ticket.StatusReady, ticket.TestRow{Result: ticket.TestFailed, Checks: []ticket.TestCheck{{Index: 1, Text: "It works."}},
		Note: "It overflows.", At: "2026-10-02T10:00:00Z", By: "human", Commit: fixtureHead})
	if err := h.store.Resume(ledger.WeftAgent); err != nil {
		t.Fatal(err)
	}
	h.service.dispatch()
	if entries := h.cleanupEntries(runID); len(entries) != 1 || entries[0].state != "dispatched" {
		t.Fatalf("cleanup of a landed trial run whose ticket failed = %+v", entries)
	}
	waitFor(t, func() bool {
		run := h.run(runID)
		return run.Disposition == ledger.WeftDispositionCleaned && h.reaped(run)
	})
	h.settle()
}

// Two stores' launches of one ticket id naming the same checkout are one
// ticket to the guard (§6.2, §11.7): the worktree and branch a run takes
// derive from the repository and ticket id alone, so the second is refused
// ticket_in_flight while the first's run is in flight and prior_run while it
// is retained, and never runs in or cleans the first's worktree.
func TestTrialLaunchSameCheckoutTwoStores(t *testing.T) {
	h := probed(t)
	const shared = "weft/one-checkout-0001"
	first, second := h.trialCell("c-0070", shared), h.trialCell("c-0071", shared)
	// second's store and evidence directory, first's checkout.
	sharing := strings.Replace(second.request(t, shared), second.handles["checkout"], first.handles["checkout"], 1)
	if sharing == second.request(t, shared) {
		t.Fatal("the request still names the second cell's checkout")
	}
	r := h.send(http.MethodPost, "/v2/launches", "first", first.request(t, shared))
	r.expect(t, http.StatusAccepted)
	run := h.waitPhase(h.waitDispatched(entryOf(r)["entry_id"].(string)), ledger.WeftPhaseParked)

	if message := h.send(http.MethodPost, "/v2/launches", "sharing-in-flight", sharing).
		refused(t, http.StatusConflict, "ticket_in_flight"); !strings.Contains(message, run.ID) {
		t.Fatalf("ticket_in_flight = %s, want it to name %s", message, run.ID)
	}
	h.send(http.MethodPost, "/v2/runs/"+run.ID+"/stop", "stop", revisionBody(h.run(run.ID).Revision)).expect(t, http.StatusOK)
	h.waitPhase(run.ID, ledger.WeftPhaseEnded)
	if message := h.send(http.MethodPost, "/v2/launches", "sharing-retained", sharing).
		refused(t, http.StatusConflict, "prior_run"); !strings.Contains(message, run.ID) {
		t.Fatalf("prior_run = %s, want it to name %s", message, run.ID)
	}
	var runs, entries int
	if err := h.db().QueryRow(`SELECT count(*) FROM weft_runs WHERE ticket=?`, shared).Scan(&runs); err != nil {
		t.Fatal(err)
	}
	if err := h.db().QueryRow(`SELECT count(*) FROM weft_entries WHERE ticket=?`, shared).Scan(&entries); err != nil {
		t.Fatal(err)
	}
	if runs != 1 || entries != 1 {
		t.Fatalf("%d runs and %d entries of %s, want only the first cell's", runs, entries, shared)
	}
	if got := h.run(run.ID); got.Disposition != ledger.WeftDispositionRetained {
		t.Fatalf("the first cell's run = %+v", got)
	}
	if _, err := os.Stat(run.Worktree); err != nil {
		t.Fatalf("the first cell's worktree is gone: %v", err)
	}
}
