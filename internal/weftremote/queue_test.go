package weftremote

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"maps"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/EnderRealm/ticket/v8/pkg/ticket"

	"shuttle/internal/config"
	"shuttle/internal/ledger"
	"shuttle/internal/sandbox"
)

// queueHost is a host whose dispatch spawns a fake worker binary through the
// real supervisor and sandbox preflight. Each ticket's segments behave as the
// control file named for it says: park (the default), hold until released and
// then park, finish, stale (refuse stale_base before hello), or die (killed
// mid-stage, with no terminal event).
type queueHost struct {
	*serviceFixture
	t        *testing.T
	central  string
	tkConfig string
	code     string
	control  string
	projects map[string]string
	cfg      *config.Config
	logs     *syncBuffer
	// capabilities are what a weft-worker/v2 fixture worker's probe lists.
	capabilities []any
}

type syncBuffer struct {
	mu sync.Mutex
	b  strings.Builder
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.String()
}

const queueWorkerScript = `#!/bin/sh
case "$1" in version|allowance-probe) exit 64 ;; esac
run= segment= ticket= repo= weftroot= resume= answerfile=
while [ "$#" -gt 0 ]; do
  case "$1" in
    --run) run="$2"; shift 2 ;;
    --segment) segment="$2"; shift 2 ;;
    --ticket) ticket="$2"; shift 2 ;;
    --repo) repo="$2"; shift 2 ;;
    --weft-root) weftroot="$2"; shift 2 ;;
    --resume) resume="$2"; shift 2 ;;
    --answer-file) answerfile="$2"; shift 2 ;;
    *) shift ;;
  esac
done
control=CONTROL
slug=$(printf '%s' "$ticket" | tr / -)
printf '%s %s\n' "$run" "$segment" >> "$control/$slug.log"
mode=park
if [ -f "$control/$slug" ]; then mode=$(cat "$control/$slug"); fi
project=${ticket%%/*}
hash=$(printf '%s' "$repo" | /usr/bin/shasum -a 256 | cut -c1-8)
worktree="$weftroot/worktrees/$(basename "$repo")-$hash/$slug"
worker="20261011T12000${segment}Z-0000000${segment}"
resumed=null
if [ -n "$resume" ]; then resumed="\"$resume\""; fi
answer=null
if [ -n "$answerfile" ]; then answer="\"$(sed -n 's/.*"question_id":"\([^"]*\)".*/\1/p' "$answerfile")\""; fi
env="\"contract_version\":\"weft-remote/v1\",\"run_id\":\"$run\",\"segment\":$segment"
if [ "$mode" = stale ]; then
  printf '{%s,"worker_run_id":null,"seq":1,"ts":"2026-10-11T12:00:00.000Z","event":"ended","outcome":"failed","stopped":false,"halt":null,"failure":"stale_base","detail":"main in %s moved past the base this run recorded; nothing was relaunched and the answer was not consumed","head":"","summary":null}\n' "$env" "$repo"
  exit 0
fi
mkdir -p "$worktree" "$repo/.git/worktrees/$slug"
printf 'gitdir: %s\n' "$repo/.git/worktrees/$slug" > "$worktree/.git"
printf '{%s,"worker_run_id":"%s","seq":1,"ts":"2026-10-11T12:00:00.000Z","event":"hello","worker_version":"fixture","ticket":"%s","project":"%s","branch":"%s","worktree":"%s","base":"base","criteria":"criteria","resumed":%s,"inherited":[],"answer":%s,"stages":[]}\n' "$env" "$worker" "$ticket" "$project" "$ticket" "$worktree" "$resumed" "$answer"
if [ "$mode" = hold ]; then
  while [ ! -f "$control/$slug.release" ]; do sleep 0.02; done
  rm -f "$control/$slug.release"
fi
if [ "$mode" = die ]; then
  printf '{%s,"worker_run_id":"%s","seq":2,"ts":"2026-10-11T12:01:00.000Z","event":"stage","stage":"summary","backend":"claude","occurrence":1,"attempt":1,"state":"running","session":null,"detail":null,"exit_code":null,"turns":null,"spend":null,"processing_ms":null,"tokens":null}\n' "$env" "$worker"
  kill -KILL $$
fi
if [ "$mode" = stopstage ]; then
  printf '{%s,"worker_run_id":"%s","seq":2,"ts":"2026-10-11T12:01:00.000Z","event":"stage","stage":"work","backend":"claude","occurrence":1,"attempt":1,"state":"stopped","session":null,"detail":null,"exit_code":null,"turns":1,"spend":null,"processing_ms":100,"tokens":null}\n' "$env" "$worker"
  printf '{%s,"worker_run_id":"%s","seq":3,"ts":"2026-10-11T12:01:01.000Z","event":"ended","outcome":"stopped","stopped":true,"halt":null,"failure":null,"detail":null,"head":"head","summary":{"state":"unavailable","reason":"fixture worker"}}\n' "$env" "$worker"
  exit 0
fi
if [ "$mode" = finish ]; then
  printf '{%s,"worker_run_id":"%s","seq":2,"ts":"2026-10-11T12:01:00.000Z","event":"ended","outcome":"finished","stopped":false,"halt":null,"failure":null,"detail":null,"head":"head","summary":{"state":"unavailable","reason":"fixture worker"}}\n' "$env" "$worker"
  exit 0
fi
printf '{%s,"worker_run_id":"%s","seq":2,"ts":"2026-10-11T12:01:00.000Z","event":"stage","stage":"work","backend":"claude","occurrence":1,"attempt":1,"state":"needs_input","session":null,"detail":null,"exit_code":null,"turns":1,"spend":null,"processing_ms":100,"tokens":null}\n' "$env" "$worker"
printf '{%s,"worker_run_id":"%s","seq":3,"ts":"2026-10-11T12:01:01.000Z","event":"parked","stage":"work","occurrence":1,"attempt":1,"worker_outcome":"blocked","question":"Which shape?","head":"head","summary":{"state":"unavailable","reason":"fixture worker"}}\n' "$env" "$worker"
`

func newQueueHost(t *testing.T) *queueHost {
	t.Helper()
	// Short: the sandbox preflight's socket path under each run directory
	// must fit the platform's limit. That is /tmp, not os.TempDir(): srt's mux
	// socket has to fit 103 bytes, which the preflight enforces even against a
	// stand-in srt, and on macOS os.TempDir() alone is 49–57.
	root, err := os.MkdirTemp("/tmp", "wq-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(root) })
	h := &queueHost{
		t: t, central: filepath.Join(root, "store"), tkConfig: filepath.Join(root, ".ticket", "config.yaml"),
		code: filepath.Join(root, "code"), control: filepath.Join(root, "control"), projects: map[string]string{},
		logs: &syncBuffer{},
	}
	for _, dir := range []string{filepath.Dir(h.tkConfig), filepath.Join(h.central, "tickets"), h.control} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	for _, name := range []string{"loom", "ticket", "warp", "weft"} {
		h.checkout(name)
		h.projects[name] = filepath.Join(h.code, name)
	}
	srt := filepath.Join(root, "srt")
	worker := filepath.Join(root, "weft-worker")
	h.cfg = &config.Config{
		Timezone: "UTC",
		Paths:    config.Paths{TKConfig: h.tkConfig, SRTBin: srt, LoomHome: filepath.Join(root, "loom")},
		Weft: config.Weft{
			Listen: "127.0.0.1:0", WorkerBin: worker, Root: fixtureWeftRoot(t),
			OAuthKeychainItem: fixtureOAuthItem, SegmentTimeoutSeconds: 30,
		},
	}
	h.writeConfig()
	if err := os.WriteFile(srt, []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(worker, []byte(strings.Replace(queueWorkerScript, "CONTROL", h.control, 1)), 0o700); err != nil {
		t.Fatal(err)
	}
	store, err := ledger.Open(filepath.Join(root, "runs.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	service, err := New(h.cfg, store, "secret-token", root, log.New(h.logs, "", 0))
	if err != nil {
		t.Fatal(err)
	}
	service.segments.findKey = func(item string) (string, error) {
		if item != fixtureOAuthItem {
			return "", fmt.Errorf("keychain item %q: not found", item)
		}
		return fixtureOAuthToken, nil
	}
	h.serviceFixture = &serviceFixture{root: root, store: store, service: service}
	// Every worker this host spawned has exited and been reaped before the
	// temporary root goes.
	t.Cleanup(func() {
		entries, _ := os.ReadDir(h.control)
		for _, e := range entries {
			if !strings.HasSuffix(e.Name(), ".log") && !strings.Contains(e.Name(), ".") {
				_ = os.WriteFile(filepath.Join(h.control, e.Name()+".release"), nil, 0o600)
			}
		}
		h.settle()
	})
	return h
}

func (h *queueHost) checkout(name string) {
	if err := os.MkdirAll(filepath.Join(h.code, name, ".git", "worktrees"), 0o755); err != nil {
		h.t.Fatal(err)
	}
}

// writeConfig registers h.projects in tk's config.yaml and binds each in
// weft.repositories to the same path, the binding the catalog reads. The map
// is replaced, never edited, so a catalog read already holding the old one
// is not raced.
func (h *queueHost) writeConfig() {
	h.cfg.Weft.Repositories = maps.Clone(h.projects)
	var b strings.Builder
	b.WriteString("central_root: " + h.central + "\nprojects:\n")
	for _, name := range keysOf(h.projects) {
		b.WriteString("  " + name + ":\n    path: " + h.projects[name] + "\n")
	}
	if err := os.WriteFile(h.tkConfig, []byte(b.String()), 0o600); err != nil {
		h.t.Fatal(err)
	}
}

func (h *queueHost) ticket(id string, status ticket.Status, priority int) {
	h.t.Helper()
	project, slug, _ := strings.Cut(id, "/")
	writeTicketIn(h.t, h.central, project, &ticket.Ticket{
		ID: slug, Status: status, Type: ticket.TypeFeature, Priority: priority, Title: "Work " + slug, Body: criterionBody,
		Created: time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC), Updated: time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC),
	})
}

func (h *queueHost) repositoryID(project string) string {
	h.t.Helper()
	canonical, err := sandbox.CanonicalRepository(h.projects[project])
	if err != nil {
		h.t.Fatal(err)
	}
	return sandbox.RepositoryID(canonical)
}

func (h *queueHost) repository(project string) string {
	h.t.Helper()
	canonical, err := sandbox.CanonicalRepository(h.projects[project])
	if err != nil {
		h.t.Fatal(err)
	}
	return canonical
}

func slugOf(id string) string { return strings.ReplaceAll(id, "/", "-") }

func (h *queueHost) mode(ticketID, mode string) {
	if err := os.WriteFile(filepath.Join(h.control, slugOf(ticketID)), []byte(mode), 0o600); err != nil {
		h.t.Fatal(err)
	}
}

func (h *queueHost) release(ticketID string) {
	if err := os.WriteFile(filepath.Join(h.control, slugOf(ticketID)+".release"), nil, 0o600); err != nil {
		h.t.Fatal(err)
	}
}

// invocations are the worker launches recorded for a ticket.
func (h *queueHost) invocations(ticketID string) []string {
	data, err := os.ReadFile(filepath.Join(h.control, slugOf(ticketID)+".log"))
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		h.t.Fatal(err)
	}
	return strings.Fields(strings.ReplaceAll(string(data), " ", ":"))
}

// settle waits until no segment is in flight.
func (h *queueHost) settle() {
	h.t.Helper()
	waitFor(h.t, func() bool {
		c, err := h.store.WeftCapacity()
		return err == nil && c.Running == 0
	})
}

type response struct {
	code     int
	replayed bool
	body     map[string]any
	raw      string
}

func (h *queueHost) send(method, path, key, body string) response {
	h.t.Helper()
	rec := h.request(h.t, method, path, key, body)
	doc, _ := decode(h.t, rec.Body.Bytes()).(map[string]any)
	return response{code: rec.Code, replayed: rec.Header().Get("Idempotent-Replayed") == "true", body: doc, raw: rec.Body.String()}
}

func (h *queueHost) launch(ticketID, key string) response {
	h.t.Helper()
	project, _, _ := strings.Cut(ticketID, "/")
	return h.send(http.MethodPost, "/v2/launches", key, fmt.Sprintf(
		`{"contract_version":"weft-service/v2","ticket":%q,"repository_id":%q,"checkout":null,"environment":null}`,
		ticketID, h.repositoryID(project)))
}

func revisionBody(revision any) string {
	return fmt.Sprintf(`{"contract_version":"weft-service/v2","revision":%v}`, revision)
}

func (h *queueHost) queue() map[string]any {
	h.t.Helper()
	rec := h.request(h.t, http.MethodGet, "/v2/queue", "", "")
	if rec.Code != http.StatusOK {
		h.t.Fatalf("GET /v2/queue = %d %s", rec.Code, rec.Body.String())
	}
	doc := decode(h.t, rec.Body.Bytes()).(map[string]any)
	queueShape(h.t).check(h.t, "queue", doc)
	return doc["queue"].(map[string]any)
}

func (h *queueHost) entry(id string) map[string]any {
	h.t.Helper()
	for _, e := range h.queue()["entries"].([]any) {
		if e.(map[string]any)["entry_id"] == id {
			return e.(map[string]any)
		}
	}
	h.t.Fatalf("entry %s is not listed", id)
	return nil
}

func (h *queueHost) run(id string) *ledger.WeftRun {
	h.t.Helper()
	run, err := h.store.GetWeftRun(id)
	if err != nil {
		h.t.Fatal(err)
	}
	return run
}

// waitPhase waits for the run to reach phase — and, for a phase with no
// process, for its segment to be reaped.
func (h *queueHost) waitPhase(runID, phase string) *ledger.WeftRun {
	h.t.Helper()
	var run *ledger.WeftRun
	waitFor(h.t, func() bool {
		var err error
		run, err = h.store.GetWeftRun(runID)
		return err == nil && run.Phase == phase && (phase == ledger.WeftPhaseRunning || h.reaped(run))
	})
	return run
}

// reaped is true once the run's latest segment has been settled.
func (h *queueHost) reaped(run *ledger.WeftRun) bool {
	if len(run.Segments) == 0 {
		return false
	}
	row, err := h.store.Get(run.Segments[len(run.Segments)-1].LedgerRunID)
	return err == nil && row.Status != ledger.StatusClaimed && row.Status != ledger.StatusRunning
}

// waitDispatched waits for the entry to dispatch and returns its run.
func (h *queueHost) waitDispatched(entryID string) string {
	h.t.Helper()
	var runID string
	waitFor(h.t, func() bool {
		e := h.entry(entryID)
		runID, _ = e["run_id"].(string)
		return e["state"] == "dispatched"
	})
	return runID
}

func (h *queueHost) db() *sql.DB {
	h.t.Helper()
	db, err := sql.Open("sqlite", "file:"+filepath.Join(h.root, "runs.db")+"?_pragma=busy_timeout(5000)")
	if err != nil {
		h.t.Fatal(err)
	}
	h.t.Cleanup(func() { db.Close() })
	return db
}

func (h *queueHost) exec(query string, args ...any) {
	h.t.Helper()
	if _, err := h.db().Exec(query, args...); err != nil {
		h.t.Fatalf("%s: %v", query, err)
	}
}

func (h *queueHost) queueRevision() int64 {
	return int64(h.queue()["revision"].(float64))
}

func entryOf(r response) map[string]any { return r.body["entry"].(map[string]any) }

func queueShape(t *testing.T) *shape {
	var samples []any
	for _, name := range []string{"queue.json", "capacity-response.json", "queue-clear-response.json", "queue-archiving.json"} {
		samples = append(samples, decode(t, fixtureBytes(t, name)))
	}
	return learn(samples...)
}

// entryShape is every entry the fixtures show, in any response.
func entryShape(t *testing.T) *shape {
	var samples []any
	for _, name := range []string{"launch-response.json", "resume-response.json", "queue-cancel-response.json",
		"answer-response.json", "fresh-response.json"} {
		samples = append(samples, decode(t, fixtureBytes(t, name)).(map[string]any)["entry"])
	}
	for _, name := range []string{"queue.json", "capacity-response.json", "queue-clear-response.json", "queue-archiving.json"} {
		samples = append(samples, decode(t, fixtureBytes(t, name)).(map[string]any)["queue"].(map[string]any)["entries"].([]any)...)
	}
	samples = append(samples, decode(t, fixtureBytes(t, "frontier-response.json")).(map[string]any)["entries"].([]any)...)
	return learn(samples...)
}

// expect holds a response to its status and to the key sets the fixtures
// named show for it.
func (r response) expect(t *testing.T, status int, fixtures ...string) {
	t.Helper()
	if r.code != status {
		t.Fatalf("status %d, want %d: %s", r.code, status, r.raw)
	}
	var samples []any
	for _, name := range fixtures {
		if name != "" {
			samples = append(samples, decode(t, fixtureBytes(t, name)))
		}
	}
	if len(samples) > 0 {
		learn(samples...).check(t, strings.Join(fixtures, "+"), r.body)
	}
	if status < 300 && (r.body["contract_version"] != ServiceVersion || r.body["host_id"] == nil || r.body["as_of"] == nil) {
		t.Fatalf("envelope = %s", r.raw)
	}
}

func (r response) refused(t *testing.T, status int, code string) string {
	t.Helper()
	if r.code != status {
		t.Fatalf("status %d, want %d %s: %s", r.code, status, code, r.raw)
	}
	errorShape(t).check(t, code, r.body)
	e := r.body["error"].(map[string]any)
	if e["code"] != code {
		t.Fatalf("error %v, want %s", e, code)
	}
	return e["message"].(string)
}

func waiting(e map[string]any) (string, map[string]any) {
	w, _ := e["waiting"].(map[string]any)
	if w == nil {
		return "", nil
	}
	holder, _ := w["holder"].(map[string]any)
	return w["reason"].(string), holder
}

// Two launches into one repository at once: both admitted, the first
// dispatches in its transaction and the second waits on its run (§13).
func TestConcurrentSameRepositoryLaunchesDispatchOnce(t *testing.T) {
	h := newQueueHost(t)
	h.ticket("weft/first-0001", ticket.StatusReady, 2)
	h.ticket("weft/second-0002", ticket.StatusReady, 2)
	h.mode("weft/first-0001", "hold")
	h.mode("weft/second-0002", "hold")

	results := make([]response, 2)
	var wg sync.WaitGroup
	for i, id := range []string{"weft/first-0001", "weft/second-0002"} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i] = h.launch(id, "launch-"+id)
		}()
	}
	wg.Wait()
	var dispatched, held map[string]any
	for _, r := range results {
		r.expect(t, http.StatusAccepted, "launch-response.json")
		entryShape(t).check(t, "entry", entryOf(r))
		switch e := entryOf(r); e["state"] {
		case "dispatched":
			if dispatched != nil {
				t.Fatalf("both launches dispatched: %v / %v", dispatched, e)
			}
			dispatched = e
		case "waiting":
			held = e
		}
	}
	if dispatched == nil || held == nil {
		t.Fatalf("launches = %v", results)
	}
	runID := dispatched["run_id"].(string)
	reason, holder := waiting(held)
	if reason != "repository_reserved" || !reflect.DeepEqual(holder, map[string]any{"kind": "run", "id": runID}) ||
		held["waiting"].(map[string]any)["detail"] != runID+" holds "+h.repository("weft") || held["run_id"] != nil ||
		held["revision"] != float64(1) || held["repository_id"] != h.repositoryID("weft") {
		t.Fatalf("held entry = %v", held)
	}
	run := h.run(runID)
	if run.EntryID.String != dispatched["entry_id"] || run.RecordedUnder != ledger.WeftRecordedV2 ||
		run.SettlementDetail.String != ledger.WeftLandingNotServed || run.RepositoryID != h.repositoryID("weft") {
		t.Fatalf("minted run = %+v", run)
	}
	h.waitPhase(runID, ledger.WeftPhaseRunning)

	// A retry with the same key returns the same entry and does nothing.
	again := h.launch(dispatched["ticket"].(string), "launch-"+dispatched["ticket"].(string))
	if again.code != http.StatusAccepted || !again.replayed || again.body["entry"].(map[string]any)["entry_id"] != dispatched["entry_id"] {
		t.Fatalf("replay = %d %v %s", again.code, again.replayed, again.raw)
	}
	reused := h.send(http.MethodPost, "/v2/launches", "launch-"+dispatched["ticket"].(string), `{"contract_version":"weft-service/v2"}`)
	reused.refused(t, http.StatusUnprocessableEntity, "idempotency_key_reused")
	q := h.queue()
	if len(q["entries"].([]any)) != 2 || !reflect.DeepEqual(q["capacity"], map[string]any{"cap": float64(3), "running": float64(1), "waiting": float64(1)}) {
		t.Fatalf("queue = %v", q)
	}
	// Another launch of a ticket with a waiting entry is in flight.
	h.launch(held["ticket"].(string), "launch-again").refused(t, http.StatusConflict, "ticket_in_flight")

	// The first run parks; once reaped its reservation passes to the second.
	h.release(dispatched["ticket"].(string))
	h.waitPhase(runID, ledger.WeftPhaseParked)
	second := h.waitDispatched(held["entry_id"].(string))
	if second == "" || second == runID {
		t.Fatalf("second run = %q", second)
	}
	h.release(held["ticket"].(string))
	h.waitPhase(second, ledger.WeftPhaseParked)
	if !strings.Contains(h.logs.String(), "entry="+held["entry_id"].(string)+" run="+second+": dispatched launch") {
		t.Errorf("log has no dispatch line:\n%s", h.logs.String())
	}
	if strings.Contains(h.logs.String(), "secret-token") || strings.Contains(h.logs.String(), "launch-weft") {
		t.Errorf("log carries the token or a raw idempotency key:\n%s", h.logs.String())
	}
}

// A free slot dispatches the oldest entry whose repository is free, past an
// older one waiting on a reserved repository (§3 queue).
func TestIndependentRepositoriesProgressPastABlockedEntry(t *testing.T) {
	h := newQueueHost(t)
	for _, id := range []string{"weft/hold-0001", "weft/blocked-0002", "loom/free-0003", "warp/later-0004"} {
		h.ticket(id, ticket.StatusReady, 2)
		h.mode(id, "hold")
	}
	h.send(http.MethodPut, "/v2/queue/capacity", "cap-2", fmt.Sprintf(
		`{"contract_version":"weft-service/v2","cap":2,"revision":%d}`, h.queueRevision())).expect(t, http.StatusOK, "capacity-response.json")
	first := entryOf(h.launch("weft/hold-0001", "k1"))
	blocked := entryOf(h.launch("weft/blocked-0002", "k2"))
	free := entryOf(h.launch("loom/free-0003", "k3"))
	later := entryOf(h.launch("warp/later-0004", "k4"))
	if first["state"] != "dispatched" || free["state"] != "dispatched" {
		t.Fatalf("first %v free %v", first, free)
	}
	if reason, _ := waiting(blocked); reason != "repository_reserved" {
		t.Fatalf("blocked = %v", blocked)
	}
	if reason, _ := waiting(later); reason != "capacity" || later["waiting"].(map[string]any)["detail"] != "2 of 2 slots are running" {
		t.Fatalf("later = %v", later)
	}
	if free["position"].(float64) <= blocked["position"].(float64) {
		t.Fatalf("positions: blocked %v, free %v", blocked["position"], free["position"])
	}
	if c := h.queue()["capacity"]; !reflect.DeepEqual(c, map[string]any{"cap": float64(2), "running": float64(2), "waiting": float64(2)}) {
		t.Fatalf("capacity = %v", c)
	}
	// Lowering the cap stops nothing; raising it dispatches at once.
	lowered := h.send(http.MethodPut, "/v2/queue/capacity", "cap-1", fmt.Sprintf(
		`{"contract_version":"weft-service/v2","cap":1,"revision":%d}`, h.queueRevision()))
	lowered.expect(t, http.StatusOK, "capacity-response.json")
	if c := lowered.body["queue"].(map[string]any)["capacity"]; !reflect.DeepEqual(c, map[string]any{"cap": float64(1), "running": float64(2), "waiting": float64(2)}) {
		t.Fatalf("lowered capacity = %v", c)
	}
	h.send(http.MethodPut, "/v2/queue/capacity", "cap-0", fmt.Sprintf(
		`{"contract_version":"weft-service/v2","cap":0,"revision":%d}`, h.queueRevision())).refused(t, http.StatusBadRequest, "malformed")
	h.send(http.MethodPut, "/v2/queue/capacity", "cap-stale", `{"contract_version":"weft-service/v2","cap":4,"revision":1}`).
		refused(t, http.StatusConflict, "revision_conflict")
	raised := h.send(http.MethodPut, "/v2/queue/capacity", "cap-3", fmt.Sprintf(
		`{"contract_version":"weft-service/v2","cap":3,"revision":%d}`, h.queueRevision()))
	raised.expect(t, http.StatusOK, "capacity-response.json")
	for _, e := range raised.body["queue"].(map[string]any)["entries"].([]any) {
		e := e.(map[string]any)
		if e["entry_id"] == later["entry_id"] && e["state"] != "dispatched" {
			t.Fatalf("raise left %v waiting", e)
		}
		if e["entry_id"] == blocked["entry_id"] && e["state"] != "waiting" {
			t.Fatalf("raise dispatched the reserved repository's entry %v", e)
		}
	}
}

// The frontier preview, its confirmation and a stale one (§6.4); the cap
// counts frontier and manual work alike (§6.1).
func TestFrontierPreviewConfirmAndCapacity(t *testing.T) {
	h := newQueueHost(t)
	h.ticket("ticket/frontier-3c4d", ticket.StatusReady, 1)
	h.ticket("ticket/frontier-4d5e", ticket.StatusReady, 2)
	h.ticket("ticket/frontier-5e6f", ticket.StatusReady, 2)
	h.ticket("ticket/backlog-6f70", ticket.StatusBacklog, 1)
	for _, id := range []string{"ticket/frontier-3c4d", "ticket/frontier-4d5e", "ticket/frontier-5e6f", "loom/manual-0001", "warp/third-0002"} {
		h.mode(id, "hold")
	}
	h.ticket("loom/manual-0001", ticket.StatusReady, 2)
	h.ticket("warp/third-0002", ticket.StatusReady, 2)

	preview := h.send(http.MethodGet, "/v2/frontier?project=ticket", "", "")
	preview.expect(t, http.StatusOK, "frontier.json")
	frontier := preview.body["frontier"].(map[string]any)
	want := []any{"ticket/frontier-3c4d", "ticket/frontier-4d5e", "ticket/frontier-5e6f"}
	if !reflect.DeepEqual(frontier["admissible"], want) || len(frontier["tickets"].([]any)) != 3 {
		t.Fatalf("frontier = %v", frontier)
	}
	revision := int64(frontier["catalog_revision"].(float64))
	body := func(revision int64, tickets ...string) string {
		quoted, _ := json.Marshal(tickets)
		return fmt.Sprintf(`{"contract_version":"weft-service/v2","project":"ticket","catalog_revision":%d,"tickets":%s}`, revision, quoted)
	}
	h.send(http.MethodPost, "/v2/frontier", "f-short", body(revision, "ticket/frontier-3c4d")).refused(t, http.StatusConflict, "revision_conflict")

	// The frontier changes in tk after the preview.
	h.ticket("ticket/frontier-5e6f", ticket.StatusClosed, 2)
	message := h.send(http.MethodPost, "/v2/frontier", "f-stale", body(revision, want[0].(string), want[1].(string), want[2].(string))).
		refused(t, http.StatusConflict, "revision_conflict")
	if !strings.Contains(message, fmt.Sprintf("confirmation was for %d", revision)) || !strings.Contains(message, "Preview again") {
		t.Fatalf("stale confirmation = %s", message)
	}
	frontier = h.send(http.MethodGet, "/v2/frontier?project=ticket", "", "").body["frontier"].(map[string]any)
	revision = int64(frontier["catalog_revision"].(float64))
	submitted := h.send(http.MethodPost, "/v2/frontier", "f-ok", body(revision, "ticket/frontier-3c4d", "ticket/frontier-4d5e"))
	submitted.expect(t, http.StatusAccepted, "frontier-response.json")
	entries := submitted.body["entries"].([]any)
	if len(entries) != 2 {
		t.Fatalf("entries = %v", entries)
	}
	firstEntry, sibling := entries[0].(map[string]any), entries[1].(map[string]any)
	selection := submitted.body["selection_id"]
	if firstEntry["state"] != "dispatched" || firstEntry["selection_id"] != selection || sibling["selection_id"] != selection ||
		firstEntry["kind"] != "frontier" {
		t.Fatalf("entries = %v", entries)
	}
	if reason, holder := waiting(sibling); reason != "repository_reserved" || holder["id"] != firstEntry["run_id"] {
		t.Fatalf("sibling = %v", sibling)
	}

	// With a cap of two, a manual launch takes the second slot and a third
	// waits on capacity.
	h.send(http.MethodPut, "/v2/queue/capacity", "cap-2", fmt.Sprintf(
		`{"contract_version":"weft-service/v2","cap":2,"revision":%d}`, h.queueRevision())).expect(t, http.StatusOK, "")
	if e := entryOf(h.launch("loom/manual-0001", "m1")); e["state"] != "dispatched" {
		t.Fatalf("manual = %v", e)
	}
	third := entryOf(h.launch("warp/third-0002", "m2"))
	if reason, _ := waiting(third); reason != "capacity" {
		t.Fatalf("third = %v", third)
	}
	if c := h.queue()["capacity"]; !reflect.DeepEqual(c, map[string]any{"cap": float64(2), "running": float64(2), "waiting": float64(2)}) {
		t.Fatalf("capacity = %v", c)
	}
	h.send(http.MethodGet, "/v2/frontier?project=nowhere", "", "").refused(t, http.StatusNotFound, "project_not_found")
}

// A parked run holds no slot: it has no process (§6.1).
func TestParkedRunFreesItsSlot(t *testing.T) {
	h := newQueueHost(t)
	h.ticket("weft/parks-0001", ticket.StatusReady, 2)
	h.ticket("loom/next-0002", ticket.StatusReady, 2)
	h.mode("weft/parks-0001", "hold")
	h.send(http.MethodPut, "/v2/queue/capacity", "cap-1", fmt.Sprintf(
		`{"contract_version":"weft-service/v2","cap":1,"revision":%d}`, h.queueRevision())).expect(t, http.StatusOK, "")
	parks := entryOf(h.launch("weft/parks-0001", "p1"))
	next := entryOf(h.launch("loom/next-0002", "p2"))
	if reason, _ := waiting(next); parks["state"] != "dispatched" || reason != "capacity" {
		t.Fatalf("parks %v next %v", parks, next)
	}
	h.release("weft/parks-0001")
	h.waitPhase(parks["run_id"].(string), ledger.WeftPhaseParked)
	nextRun := h.waitDispatched(next["entry_id"].(string))
	h.waitPhase(nextRun, ledger.WeftPhaseParked)
	if c := h.queue()["capacity"]; !reflect.DeepEqual(c, map[string]any{"cap": float64(1), "running": float64(0), "waiting": float64(0)}) {
		t.Fatalf("capacity with two parked runs = %v", c)
	}
}

// An answer is recorded once against its pending question, waits visibly,
// survives its entry's cancellation, and is queued again by resume — the
// same answer, never a second (§6.5). Stopping the parked run invalidates
// the waiting entry (§6.3).
func TestAnswerRecordedOnceSurvivesCancelAndStop(t *testing.T) {
	h := newQueueHost(t)
	h.ticket("weft/asks-0001", ticket.StatusReady, 2)
	h.ticket("weft/holds-0002", ticket.StatusReady, 2)
	h.mode("weft/holds-0002", "hold")
	asks := entryOf(h.launch("weft/asks-0001", "a1"))
	runID := asks["run_id"].(string)
	parked := h.waitPhase(runID, ledger.WeftPhaseParked)
	if parked.Question == nil {
		t.Fatalf("parked run has no question: %+v", parked)
	}
	qid := parked.Question.ID
	holds := entryOf(h.launch("weft/holds-0002", "a2"))
	holderRun := holds["run_id"].(string)
	h.waitPhase(holderRun, ledger.WeftPhaseRunning)
	before, err := h.store.WeftAsOf()
	if err != nil {
		t.Fatal(err)
	}

	answer := func(key, text string) response {
		return h.send(http.MethodPost, "/v2/runs/"+runID+"/answer", key, fmt.Sprintf(
			`{"contract_version":"weft-service/v2","question_id":%q,"answer":%q}`, qid, text))
	}
	results := make([]response, 2)
	var wg sync.WaitGroup
	for i, key := range []string{"mac-studio", "macbook"} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i] = answer(key, "Answer from "+key)
		}()
	}
	wg.Wait()
	slices.SortFunc(results, func(a, b response) int { return a.code - b.code })
	won, lost := results[0], results[1]
	won.expect(t, http.StatusAccepted, "answer-response.json")
	entryShape(t).check(t, "entry", entryOf(won))
	recorded := won.body["question"].(map[string]any)["answer"].(map[string]any)
	answerEntry := entryOf(won)
	message := lost.refused(t, http.StatusConflict, "question_not_pending")
	if !strings.Contains(message, recorded["answer_id"].(string)) || !strings.Contains(message, answerEntry["entry_id"].(string)) {
		t.Fatalf("losing answer = %s, want the recorded answer and its entry named", message)
	}
	if reason, holder := waiting(answerEntry); reason != "repository_reserved" || holder["id"] != holderRun ||
		answerEntry["kind"] != "answer" || answerEntry["run_id"] != runID || answerEntry["answer_id"] != recorded["answer_id"] ||
		recorded["entry_id"] != answerEntry["entry_id"] || recorded["consumed_segment"] != nil {
		t.Fatalf("answer = %v entry = %v", recorded, answerEntry)
	}
	// The same client retrying gets its 202 back; the loser its 409.
	var winningKey string
	for _, key := range []string{"mac-studio", "macbook"} {
		if r := answer(key, "Answer from "+key); r.replayed && r.code == http.StatusAccepted {
			winningKey = key
		}
	}
	if winningKey == "" {
		t.Fatal("no key replayed the 202")
	}

	// The answer's recording is one answer, question, queue and capacity
	// event, in the shapes the stream fixtures carry.
	events, err := h.store.WeftEventsAfter(before)
	if err != nil {
		t.Fatal(err)
	}
	samples := fixtureEvents(t, "events.sse")
	for name, more := range fixtureEvents(t, "events-reset.sse") {
		samples[name] = append(samples[name], more...)
	}
	var kinds []string
	for _, ev := range events {
		kinds = append(kinds, ev.Type)
		learn(samples[ev.Type]...).check(t, ev.Type, decode(t, ev.Payload))
	}
	if !reflect.DeepEqual(kinds, []string{"answer", "question", "queue", "capacity"}) {
		t.Fatalf("answer events = %v", kinds)
	}

	question := h.send(http.MethodGet, "/v2/questions/"+qid, "", "")
	question.expect(t, http.StatusOK, "question-answered.json")

	// Cancelling the queued answer keeps it, recorded and unconsumed.
	cancelled := h.send(http.MethodPost, "/v2/queue/"+answerEntry["entry_id"].(string)+"/cancel", "c1", revisionBody(answerEntry["revision"]))
	cancelled.expect(t, http.StatusOK, "queue-cancel-response.json")
	if entryOf(cancelled)["state"] != "cancelled" || entryOf(cancelled)["revision"] != float64(2) {
		t.Fatalf("cancelled = %v", entryOf(cancelled))
	}
	kept := h.send(http.MethodGet, "/v2/questions/"+qid, "", "").body["question"].(map[string]any)
	if kept["state"] != "answered" || kept["answer"].(map[string]any)["answer_id"] != recorded["answer_id"] ||
		kept["answer"].(map[string]any)["consumed_segment"] != nil {
		t.Fatalf("question after cancel = %v", kept)
	}
	answer("third-key", "another").refused(t, http.StatusConflict, "question_not_pending")

	// Resume queues the same answer again.
	run := h.run(runID)
	h.send(http.MethodPost, "/v2/runs/"+runID+"/resume", "r-stale", revisionBody(run.Revision-1)).refused(t, http.StatusConflict, "revision_conflict")
	resumed := h.send(http.MethodPost, "/v2/runs/"+runID+"/resume", "r1", revisionBody(run.Revision))
	resumed.expect(t, http.StatusAccepted, "resume-response.json", "launch-response.json")
	requeued := entryOf(resumed)
	if requeued["kind"] != "answer" || requeued["answer_id"] != recorded["answer_id"] || requeued["state"] != "waiting" ||
		requeued["entry_id"] == answerEntry["entry_id"] {
		t.Fatalf("resume = %v", requeued)
	}
	if a, err := h.store.WeftAnswer(recorded["answer_id"].(string)); err != nil || a.EntryID.String != requeued["entry_id"] {
		t.Fatalf("answer after resume = %+v %v", a, err)
	}
	h.send(http.MethodPost, "/v2/runs/"+runID+"/resume", "r2", revisionBody(run.Revision)).refused(t, http.StatusConflict, "ticket_in_flight")

	// Stopping the parked run stops it and invalidates its waiting entry.
	stopped := h.send(http.MethodPost, "/v2/runs/"+runID+"/stop", "s1", revisionBody(h.run(runID).Revision))
	stopped.expect(t, http.StatusOK, "run-stopped.json", "run.json", "run-stale-base.json")
	if r := stopped.body["run"].(map[string]any); r["state"] != "over" || r["outcome"] != "stopped" || r["stopped"] != true {
		t.Fatalf("stopped run = %v", r)
	}
	invalidated := h.entry(requeued["entry_id"].(string))
	if invalidated["state"] != "invalidated" || invalidated["invalidated"].(map[string]any)["reason"] != "run_changed" {
		t.Fatalf("entry after stop = %v", invalidated)
	}
	if h.run(holderRun).Phase != ledger.WeftPhaseRunning {
		t.Fatalf("stop touched another run: %+v", h.run(holderRun))
	}
	h.send(http.MethodPost, "/v2/runs/"+runID+"/stop", "s2", revisionBody(h.run(runID).Revision)).refused(t, http.StatusConflict, "run_over")
	h.release("weft/holds-0002")
	h.waitPhase(holderRun, ledger.WeftPhaseParked)
	if n := len(h.invocations("weft/asks-0001")); n != 1 {
		t.Fatalf("the answered run launched %d segments", n)
	}
}

// A segment refused for a stale base leaves the run blocked with its
// options, its answer recorded and unconsumed, no stage started, and every
// answer and resume refused stale_base (§6.7).
func TestStaleBaseHoldsTheRunAndItsAnswer(t *testing.T) {
	h := newQueueHost(t)
	h.ticket("weft/fourth-6e4f", ticket.StatusReady, 2)
	runID := entryOf(h.launch("weft/fourth-6e4f", "l1"))["run_id"].(string)
	qid := h.waitPhase(runID, ledger.WeftPhaseParked).Question.ID
	h.mode("weft/fourth-6e4f", "stale")
	accepted := h.send(http.MethodPost, "/v2/runs/"+runID+"/answer", "a1", fmt.Sprintf(
		`{"contract_version":"weft-service/v2","question_id":%q,"answer":"The project."}`, qid))
	accepted.expect(t, http.StatusAccepted, "answer-response.json")
	if e := entryOf(accepted); e["state"] != "dispatched" || e["run_id"] != runID {
		t.Fatalf("answer entry = %v", e)
	}
	var held *ledger.WeftRun
	waitFor(t, func() bool {
		held = h.run(runID)
		return held.ActionRequired.Valid && h.reaped(held)
	})
	doc := h.send(http.MethodGet, "/v2/runs/"+runID, "", "")
	learn(decode(t, fixtureBytes(t, "run.json")), decode(t, fixtureBytes(t, "run-stale-base.json")),
		decode(t, fixtureBytes(t, "run-stopped.json"))).check(t, "run", doc.body)
	run := doc.body["run"].(map[string]any)
	action := run["action_required"].(map[string]any)
	if run["state"] != "blocked" || run["phase"] != "parked" || run["outcome"] != nil || action["reason"] != "stale_base" ||
		!reflect.DeepEqual(action["options"], []any{"fresh", "terminal", "dismiss"}) || !strings.Contains(action["detail"].(string), "not consumed") {
		t.Fatalf("held run = %v", run)
	}
	segments := run["segments"].([]any)
	last := segments[len(segments)-1].(map[string]any)
	if len(segments) != 2 || last["failure"] != "stale_base" || last["terminal"] != "ended" || last["worker_run_id"] != nil ||
		last["answer"].(map[string]any)["question_id"] != qid {
		t.Fatalf("segments = %v", segments)
	}
	for _, st := range run["stages"].([]any) {
		if st.(map[string]any)["segment"] != float64(1) {
			t.Fatalf("a stage ran in the refused segment: %v", st)
		}
	}
	question := run["question"].(map[string]any)
	if question["state"] != "answered" || question["answer"].(map[string]any)["consumed_segment"] != nil {
		t.Fatalf("question = %v", question)
	}
	// The v1 segment's answer file keeps weft-remote/v1's keys.
	answerFile, err := os.ReadFile(filepath.Join(held.Segments[1].RunDir.String, "answer.json"))
	if err != nil {
		t.Fatal(err)
	}
	if file := decode(t, answerFile).(map[string]any); file["contract_version"] != ContractVersion || file["answer_id"] != nil {
		t.Fatalf("answer file = %s", answerFile)
	}
	h.send(http.MethodPost, "/v2/runs/"+runID+"/resume", "r1", revisionBody(held.Revision)).refused(t, http.StatusConflict, "stale_base")
	message := h.send(http.MethodPost, "/v2/runs/"+runID+"/answer", "a2", fmt.Sprintf(
		`{"contract_version":"weft-service/v2","question_id":%q,"answer":"Again."}`, qid)).refused(t, http.StatusConflict, "stale_base")
	if !strings.Contains(message, runID) || !strings.Contains(message, "fresh") {
		t.Fatalf("stale_base message = %s", message)
	}
	if n := len(h.invocations("weft/fourth-6e4f")); n != 2 {
		t.Fatalf("worker launched %d times, want the first segment and the refused one", n)
	}
	if c := h.queue()["capacity"].(map[string]any); c["running"] != float64(0) {
		t.Fatalf("the held run holds a slot: %v", c)
	}
}

// A worker killed mid-stage leaves no stage running on the run it ended: the
// stage it last reported running reads failed with the segment's failure.
func TestDeadRunReportsNoRunningStage(t *testing.T) {
	h := newQueueHost(t)
	h.ticket("weft/dies-7a1c", ticket.StatusReady, 2)
	h.mode("weft/dies-7a1c", "die")
	runID := entryOf(h.launch("weft/dies-7a1c", "l1"))["run_id"].(string)
	h.waitReaped(runID)
	run := h.get("/v2/runs/" + runID)["run"].(map[string]any)
	if run["phase"] == "running" || run["outcome"] != "failed" {
		t.Fatalf("dead run = %v", run)
	}
	stages := run["stages"].([]any)
	if len(stages) != 1 {
		t.Fatalf("stages = %v", stages)
	}
	stage := stages[0].(map[string]any)
	if stage["stage"] != "summary" || stage["state"] != "failed" ||
		stage["detail"] != "worker exited -1 with no terminal event" {
		t.Fatalf("stage the worker died in = %v", stage)
	}
}

// The stage an operator stop cancelled is reported stopped, not failed, and
// is stored and served as the worker reported it.
func TestStoppedStageStateStoredAndServed(t *testing.T) {
	h := newQueueHost(t)
	h.ticket("weft/stops-4c2e", ticket.StatusReady, 2)
	h.mode("weft/stops-4c2e", "stopstage")
	runID := entryOf(h.launch("weft/stops-4c2e", "l1"))["run_id"].(string)
	h.waitReaped(runID)
	run := h.get("/v2/runs/" + runID)["run"].(map[string]any)
	if run["outcome"] != "stopped" || run["stopped"] != true {
		t.Fatalf("stopped run = %v", run)
	}
	stages := run["stages"].([]any)
	if len(stages) != 1 {
		t.Fatalf("stages = %v", stages)
	}
	if stage := stages[0].(map[string]any); stage["stage"] != "work" || stage["state"] != "stopped" || stage["detail"] != nil {
		t.Fatalf("stage the stop cancelled = %v", stage)
	}
}

// Cancel drops one waiting entry; clear drops the waiting launches and
// frontier entries and leaves queued answers (§6.3).
func TestCancelAndClearScope(t *testing.T) {
	h := newQueueHost(t)
	for _, id := range []string{"weft/holds-0001", "weft/waits-0002", "loom/asks-0003", "loom/holds-0004"} {
		h.ticket(id, ticket.StatusReady, 2)
	}
	h.mode("weft/holds-0001", "hold")
	h.mode("loom/holds-0004", "hold")
	weftHolder := entryOf(h.launch("weft/holds-0001", "k1"))
	waitsLaunch := entryOf(h.launch("weft/waits-0002", "k2"))
	asks := entryOf(h.launch("loom/asks-0003", "k3"))
	qid := h.waitPhase(asks["run_id"].(string), ledger.WeftPhaseParked).Question.ID
	loomHolder := entryOf(h.launch("loom/holds-0004", "k4"))
	queuedAnswer := entryOf(h.send(http.MethodPost, "/v2/runs/"+asks["run_id"].(string)+"/answer", "k5", fmt.Sprintf(
		`{"contract_version":"weft-service/v2","question_id":%q,"answer":"Yes."}`, qid)))
	if queuedAnswer["state"] != "waiting" || waitsLaunch["state"] != "waiting" {
		t.Fatalf("answer %v launch %v", queuedAnswer, waitsLaunch)
	}

	h.send(http.MethodPost, "/v2/queue/"+weftHolder["entry_id"].(string)+"/cancel", "c1", revisionBody(1)).
		refused(t, http.StatusConflict, "entry_not_waiting")
	h.send(http.MethodPost, "/v2/queue/"+waitsLaunch["entry_id"].(string)+"/cancel", "c2", revisionBody(7)).
		refused(t, http.StatusConflict, "revision_conflict")
	h.send(http.MethodPost, "/v2/queue/e-20200101T000000Z-00000000/cancel", "c3", revisionBody(1)).
		refused(t, http.StatusNotFound, "entry_not_found")
	h.send(http.MethodPost, "/v2/queue/clear", "clear-stale", revisionBody(h.queueRevision()-1)).
		refused(t, http.StatusConflict, "revision_conflict")

	cleared := h.send(http.MethodPost, "/v2/queue/clear", "clear", revisionBody(h.queueRevision()))
	cleared.expect(t, http.StatusOK, "queue-clear-response.json")
	states := map[string]any{}
	for _, e := range cleared.body["queue"].(map[string]any)["entries"].([]any) {
		states[e.(map[string]any)["entry_id"].(string)] = e.(map[string]any)["state"]
	}
	if states[waitsLaunch["entry_id"].(string)] != "cancelled" || states[queuedAnswer["entry_id"].(string)] != "waiting" ||
		states[weftHolder["entry_id"].(string)] != "dispatched" || states[loomHolder["entry_id"].(string)] != "dispatched" {
		t.Fatalf("after clear = %v", states)
	}
	if c := cleared.body["queue"].(map[string]any)["capacity"].(map[string]any); c["waiting"] != float64(1) {
		t.Fatalf("capacity after clear = %v", c)
	}
	// The queued answer dispatches once its repository is free.
	h.release("loom/holds-0004")
	h.waitDispatched(queuedAnswer["entry_id"].(string))
	h.waitPhase(asks["run_id"].(string), ledger.WeftPhaseParked)
	h.release("weft/holds-0001")
	if a, err := h.store.WeftAnswer(queuedAnswer["answer_id"].(string)); err != nil || a.ConsumedSegment.Int64 != 2 {
		t.Fatalf("answer = %+v %v, want consumed by segment 2", a, err)
	}
}

// Reservations other tickets write — a terminal, a landing pause — and the
// operator's pause hold dispatch without refusing admission (§6.2, §6.3).
func TestSeededHoldersPausesAndHostPauseHoldDispatch(t *testing.T) {
	h := newQueueHost(t)
	for _, id := range []string{"weft/terminal-0001", "loom/paused-0002", "warp/host-0003"} {
		h.ticket(id, ticket.StatusReady, 2)
	}
	h.exec(`INSERT INTO weft_reservations(repository_id,repository,holder_kind,holder_id,acquired_at)
		VALUES(?,?,'terminal','t-20261011T120000Z-1b0a9c8d','2026-10-11T12:00:00.000Z')`, h.repositoryID("weft"), h.repository("weft"))
	h.exec(`INSERT INTO weft_repositories(repository_id,repository,dispatch,paused_run,paused_reason,paused_detail,paused_at,revision)
		VALUES(?,?,'paused','r-20261011T110000Z-00000000','not_landed','the push to origin failed','2026-10-11T11:00:00.000Z',1)`,
		h.repositoryID("loom"), h.repository("loom"))
	terminal := entryOf(h.launch("weft/terminal-0001", "k1"))
	if reason, holder := waiting(terminal); reason != "repository_reserved" || holder["kind"] != "terminal" ||
		terminal["waiting"].(map[string]any)["detail"] != "terminal t-20261011T120000Z-1b0a9c8d holds "+h.repository("weft") {
		t.Fatalf("terminal-held entry = %v", terminal)
	}
	paused := entryOf(h.launch("loom/paused-0002", "k2"))
	if reason, _ := waiting(paused); reason != "repository_paused" || !strings.Contains(paused["waiting"].(map[string]any)["detail"].(string), "push to origin failed") {
		t.Fatalf("paused entry = %v", paused)
	}
	if err := h.store.Pause(ledger.WeftAgent); err != nil {
		t.Fatal(err)
	}
	host := h.launch("warp/host-0003", "k3")
	host.expect(t, http.StatusAccepted, "launch-response.json")
	if reason, _ := waiting(entryOf(host)); reason != "host_paused" {
		t.Fatalf("host-paused entry = %v", entryOf(host))
	}
	for _, e := range h.queue()["entries"].([]any) {
		if reason, _ := waiting(e.(map[string]any)); reason != "host_paused" {
			t.Fatalf("with the host paused %v", e)
		}
	}
	if err := h.store.Resume(ledger.WeftAgent); err != nil {
		t.Fatal(err)
	}
	h.exec(`DELETE FROM weft_reservations WHERE holder_kind='terminal'`)
	h.service.dispatch()
	h.waitDispatched(terminal["entry_id"].(string))
	h.waitDispatched(entryOf(host)["entry_id"].(string))
	if reason, _ := waiting(h.entry(paused["entry_id"].(string))); reason != "repository_paused" {
		t.Fatalf("paused repository dispatched: %v", h.entry(paused["entry_id"].(string)))
	}
	h.settle()
}

// A waiting entry is revalidated on its project's catalog event, and
// invalidated visibly when tk closed its ticket or the project was rebound
// (§6.3).
func TestWaitingEntriesInvalidatedOnCatalogChange(t *testing.T) {
	h := newQueueHost(t)
	for _, id := range []string{"weft/holds-0001", "weft/closed-0002", "loom/holds-0003", "loom/rebound-0004"} {
		h.ticket(id, ticket.StatusReady, 2)
	}
	h.mode("weft/holds-0001", "hold")
	h.mode("loom/holds-0003", "hold")
	h.launch("weft/holds-0001", "k1")
	closed := entryOf(h.launch("weft/closed-0002", "k2"))
	h.launch("loom/holds-0003", "k3")
	rebound := entryOf(h.launch("loom/rebound-0004", "k4"))
	h.queue()
	loomID := h.repositoryID("loom")

	h.ticket("weft/closed-0002", ticket.StatusClosed, 2)
	h.checkout("loom-moved")
	h.projects["loom"] = filepath.Join(h.code, "loom-moved")
	h.writeConfig()
	if _, err := h.service.syncCatalog(); err != nil {
		t.Fatal(err)
	}
	gone := h.entry(closed["entry_id"].(string))
	if gone["state"] != "invalidated" || gone["invalidated"].(map[string]any)["reason"] != "ticket_ineligible" ||
		!strings.Contains(gone["invalidated"].(map[string]any)["detail"].(string), "weft/closed-0002 is closed in tk") {
		t.Fatalf("closed ticket's entry = %v", gone)
	}
	moved := h.entry(rebound["entry_id"].(string))
	if moved["state"] != "invalidated" || moved["invalidated"].(map[string]any)["reason"] != "binding_changed" ||
		!strings.Contains(moved["invalidated"].(map[string]any)["detail"].(string), "loom-moved") {
		t.Fatalf("rebound project's entry = %v", moved)
	}
	if c := h.queue()["capacity"].(map[string]any); c["waiting"] != float64(0) {
		t.Fatalf("capacity = %v", c)
	}
	h.send(http.MethodPost, "/v2/launches", "k5", fmt.Sprintf(
		`{"contract_version":"weft-service/v2","ticket":"loom/rebound-0004","repository_id":%q,"checkout":null,"environment":null}`, loomID)).
		refused(t, http.StatusConflict, "binding_changed")
	h.release("weft/holds-0001")
	h.release("loom/holds-0003")
}

func TestLaunchRefusals(t *testing.T) {
	h := newQueueHost(t)
	h.ticket("weft/ready-0001", ticket.StatusReady, 2)
	h.ticket("weft/backlog-0002", ticket.StatusBacklog, 2)
	h.launch("weft/backlog-0002", "k1").refused(t, http.StatusConflict, "ticket_not_eligible")
	h.launch("weft/missing-0003", "k2").refused(t, http.StatusNotFound, "ticket_not_found")
	h.send(http.MethodPost, "/v2/launches", "k3", `{"contract_version":"weft-service/v2","ticket":"nowhere/x-0004","repository_id":"nowhere-00000000","checkout":null,"environment":null}`).
		refused(t, http.StatusNotFound, "project_not_found")
	h.send(http.MethodPost, "/v2/launches", "k4", `{"contract_version":"weft-service/v2","ticket":"weft/ready-0001","repository_id":"weft-00000000","checkout":null,"environment":null}`).
		refused(t, http.StatusConflict, "binding_changed")
	unknown := strings.Replace(string(fixtureBytes(t, "negative/launch-request-unknown-field.json")), "weft-47327fe5", h.repositoryID("weft"), 1)
	if message := h.send(http.MethodPost, "/v2/launches", "k5", unknown).refused(t, http.StatusBadRequest, "malformed"); !strings.Contains(message, "priority") {
		t.Fatalf("unknown key refusal = %s", message)
	}
	h.send(http.MethodPost, "/v2/launches", "k6", `{"contract_version":"weft-service/v2","ticket":"weft/ready-0001","checkout":null,"environment":null}`).
		refused(t, http.StatusBadRequest, "malformed")
	h.send(http.MethodPost, "/v2/launches", "k7", `{"ticket":"weft/ready-0001"}`).refused(t, http.StatusBadRequest, "malformed")
	h.send(http.MethodPost, "/v2/launches", "k8", `{"contract_version":"weft-service/v3"}`).refused(t, http.StatusUpgradeRequired, "upgrade_required")
	h.send(http.MethodPost, "/v2/launches", "k9", string(fixtureBytes(t, "negative/launch-request-checkout-and-repository.json"))).
		refused(t, http.StatusNotImplemented, "capability_not_served")
	h.send(http.MethodPost, "/v2/launches", "", `{}`).refused(t, http.StatusBadRequest, "idempotency_key_missing")
	h.send(http.MethodPost, "/v2/runs/r-20200101T000000Z-00000000/answer", "k10", string(fixtureBytes(t, "negative/answer-request-malformed.json"))).
		refused(t, http.StatusBadRequest, "malformed")

	// A ticket whose prior run is retained is refused prior_run.
	runID := entryOf(h.launch("weft/ready-0001", "k11"))["run_id"].(string)
	qid := h.waitPhase(runID, ledger.WeftPhaseParked).Question.ID
	before, err := h.store.WeftAsOf()
	if err != nil {
		t.Fatal(err)
	}
	h.send(http.MethodPost, "/v2/runs/"+runID+"/stop", "k12", revisionBody(h.run(runID).Revision)).expect(t, http.StatusOK, "run-stopped.json", "run.json", "run-stale-base.json")
	events, err := h.store.WeftEventsAfter(before)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 || events[0].Type != "question" || !strings.Contains(string(events[0].Payload), `"state":"withdrawn"`) ||
		!strings.Contains(string(events[0].Payload), qid) || events[1].Type != "run" {
		t.Fatalf("stopping the parked run wrote %v", events)
	}
	message := h.launch("weft/ready-0001", "k13").refused(t, http.StatusConflict, "prior_run")
	if !strings.Contains(message, runID) || !strings.Contains(message, "start that run fresh") {
		t.Fatalf("prior_run = %s", message)
	}
}

// §12: the restart pass closes the interrupted segment as v1's unknown; then
// steps 4 and 5 keep the waiting entries, delete reservations whose holders
// are gone and relaunch nothing — and dispatch resumes afterwards.
func TestRestartKeepsTheQueueAndRelaunchesNothing(t *testing.T) {
	h := newQueueHost(t)
	for _, id := range []string{"weft/interrupted-0001", "weft/waiting-0002"} {
		h.ticket(id, ticket.StatusReady, 2)
	}
	// Admitted while dispatch is paused, then dispatched with no worker
	// spawned: the daemon died after the dispatch committed.
	if err := h.store.Pause(ledger.WeftAgent); err != nil {
		t.Fatal(err)
	}
	interrupted := entryOf(h.launch("weft/interrupted-0001", "k1"))
	waitingEntry := entryOf(h.launch("weft/waiting-0002", "k2"))
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
	if reason, _ := waiting(h.entry(waitingEntry["entry_id"].(string))); reason != "repository_reserved" {
		t.Fatalf("waiting entry = %v", h.entry(waitingEntry["entry_id"].(string)))
	}
	// Reservations whose holders are gone, and one a settlement keeps.
	h.exec(`INSERT INTO weft_reservations(repository_id,repository,holder_kind,holder_id,acquired_at) VALUES
		(?,?,'run','r-20261001T000000Z-dead0000','2026-10-01T00:00:00.000Z'),
		(?,?,'terminal','t-20261001T000000Z-gone0000','2026-10-01T00:00:00.000Z'),
		(?,?,'settlement','r-20261001T000000Z-land0000','2026-10-01T00:00:00.000Z')`,
		h.repositoryID("loom"), h.repository("loom"), h.repositoryID("ticket"), h.repository("ticket"),
		h.repositoryID("warp"), h.repository("warp"))

	// Step 1, as the daemon runs it: reap closes the in-flight row unknown
	// and the hosted run is reconciled.
	ledgerRun := h.run(runID).Segments[0].LedgerRunID
	if err := h.store.MarkUnknown(ledgerRun); err != nil {
		t.Fatal(err)
	}
	if err := h.store.ReconcileWeftUnknown(ledgerRun); err != nil {
		t.Fatal(err)
	}
	restarted, err := New(h.service.cfg, h.store, "secret-token", h.root, log.New(h.logs, "", 0))
	if err != nil {
		t.Fatal(err)
	}
	restarted.segments.findKey = h.service.segments.findKey
	h.service = restarted
	if err := restarted.Restart(); err != nil {
		t.Fatal(err)
	}
	var holders []string
	rows, err := h.db().Query(`SELECT holder_kind FROM weft_reservations ORDER BY holder_kind`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var kind string
		if err := rows.Scan(&kind); err != nil {
			t.Fatal(err)
		}
		holders = append(holders, kind)
	}
	rows.Close()
	if !reflect.DeepEqual(holders, []string{"settlement"}) {
		t.Fatalf("reservations after restart = %v", holders)
	}
	for _, line := range []string{"released " + h.repository("loom"), "released " + h.repository("ticket")} {
		if !strings.Contains(h.logs.String(), line) {
			t.Errorf("restart log lacks %q:\n%s", line, h.logs.String())
		}
	}
	run := h.run(runID)
	if run.Phase != ledger.WeftPhaseEnded || run.Outcome.String != "failed" || len(run.Segments) != 1 ||
		run.Segments[0].Failure.String != ledger.ClassUnknown {
		t.Fatalf("interrupted run = %+v", run)
	}
	if e := h.entry(interrupted["entry_id"].(string)); e["state"] != "dispatched" || e["run_id"] != runID {
		t.Fatalf("interrupted entry = %v", e)
	}
	if e := h.entry(waitingEntry["entry_id"].(string)); e["state"] != "waiting" {
		t.Fatalf("waiting entry after restart = %v", e)
	}
	if len(h.invocations("weft/interrupted-0001")) != 0 {
		t.Fatal("restart launched the interrupted segment")
	}

	// Dispatch resumes: the waiting entry runs; the interrupted run is not
	// relaunched and stays resumable.
	restarted.dispatch()
	next := h.waitDispatched(waitingEntry["entry_id"].(string))
	h.waitPhase(next, ledger.WeftPhaseParked)
	if len(h.invocations("weft/interrupted-0001")) != 0 || len(h.run(runID).Segments) != 1 {
		t.Fatal("dispatch relaunched the interrupted run")
	}
	resumed := h.send(http.MethodPost, "/v2/runs/"+runID+"/resume", "k3", revisionBody(h.run(runID).Revision))
	resumed.expect(t, http.StatusAccepted, "resume-response.json", "launch-response.json")
	if e := entryOf(resumed); e["kind"] != "resume" {
		t.Fatalf("resume of the interrupted run = %v", e)
	}
	h.settle()
}
