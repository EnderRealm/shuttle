package weftremote

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/EnderRealm/ticket/v8/pkg/ticket"

	"shuttle/internal/config"
	"shuttle/internal/ledger"
	"shuttle/internal/provider/tkconfig"
	"shuttle/internal/sandbox"
)

type fakeSegments struct {
	mu     sync.Mutex
	starts []string
	stops  []string
}

func (f *fakeSegments) Start(id string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.starts = append(f.starts, id)
}
func (f *fakeSegments) Stop(id string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.stops = append(f.stops, id)
}
func (f *fakeSegments) startCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.starts)
}

type serviceFixture struct {
	root     string
	store    *ledger.Store
	service  *Service
	segments *fakeSegments
}

func newServiceFixture(t *testing.T) *serviceFixture {
	t.Helper()
	root := t.TempDir()
	ticketHome := filepath.Join(root, ".ticket")
	central := filepath.Join(root, "tickets-store")
	projectPath := filepath.Join(root, "code", "demo")
	for _, dir := range []string{
		ticketHome, filepath.Join(central, "tickets", "demo"), projectPath,
	} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	tkConfig := filepath.Join(ticketHome, "config.yaml")
	if err := os.WriteFile(tkConfig, []byte(
		"central_root: "+central+"\nprojects:\n  demo:\n    path: "+projectPath+"\n",
	), 0o600); err != nil {
		t.Fatal(err)
	}
	writeTicket(t, central, &ticket.Ticket{
		ID: "ready-one", Status: ticket.StatusReady, Type: ticket.TypeFeature, Priority: 2,
		Created: time.Now().UTC(), Updated: time.Now().UTC(), Title: "Ready ticket",
		Body: "Why this ticket exists.\n\n## Acceptance Criteria\n\n- It works.\n",
	})
	writeTicket(t, central, &ticket.Ticket{
		ID: "blocked-two", Status: ticket.StatusReady, Type: ticket.TypeFeature, Priority: 2,
		Deps: []string{"missing"}, Created: time.Now().UTC(), Updated: time.Now().UTC(),
		Title: "Blocked ticket", Body: "Blocked.\n",
	})
	store, err := ledger.Open(filepath.Join(root, "runs.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	cfg := &config.Config{
		Timezone: "UTC",
		Paths:    config.Paths{TKConfig: tkConfig, SRTBin: "/usr/bin/true"},
		Weft: config.Weft{
			Listen: "127.0.0.1:0", WorkerBin: "/usr/bin/true",
			Root: filepath.Join(root, "weft"), SegmentTimeoutSeconds: 10,
		},
	}
	service, err := New(cfg, store, "secret-token", root, log.New(io.Discard, "", 0))
	if err != nil {
		t.Fatal(err)
	}
	segments := &fakeSegments{}
	service.SetSegmentControl(segments)
	return &serviceFixture{root: root, store: store, service: service, segments: segments}
}

func writeTicket(t *testing.T, central string, tk *ticket.Ticket) {
	t.Helper()
	doc, err := ticket.Serialize(tk)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(central, "tickets", "demo", tk.ID+".md"), doc, 0o600); err != nil {
		t.Fatal(err)
	}
}

func (f *serviceFixture) request(t *testing.T, method, path, key, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer secret-token")
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	rec := httptest.NewRecorder()
	f.service.ServeHTTP(rec, req)
	return rec
}

func TestServiceLaunchReplayParkAnswerAndReconnect(t *testing.T) {
	f := newServiceFixture(t)
	list := f.request(t, http.MethodGet, "/v1/tickets", "", "")
	if list.Code != http.StatusOK || strings.Contains(list.Body.String(), "blocked-two") || !strings.Contains(list.Body.String(), "demo/ready-one") {
		t.Fatalf("ticket list: %d %s", list.Code, list.Body.String())
	}
	detail := f.request(t, http.MethodGet, "/v1/tickets/demo/ready-one", "", "")
	if detail.Code != http.StatusOK || !strings.Contains(detail.Body.String(), "Why this ticket exists.") {
		t.Fatalf("ticket detail: %d %s", detail.Code, detail.Body.String())
	}
	body := `{"contract_version":"weft-remote/v1","ticket":"demo/ready-one"}`
	launch := f.request(t, http.MethodPost, "/v1/runs", "launch-1", body)
	if launch.Code != http.StatusCreated {
		t.Fatalf("launch: %d %s", launch.Code, launch.Body.String())
	}
	var launched struct {
		Run struct {
			ID string `json:"run_id"`
		} `json:"run"`
	}
	if err := json.Unmarshal(launch.Body.Bytes(), &launched); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return f.segments.startCount() == 1 })
	replay := f.request(t, http.MethodPost, "/v1/runs", "launch-1", body)
	if replay.Code != http.StatusCreated || replay.Header().Get("Idempotent-Replayed") != "true" ||
		replay.Body.String() != launch.Body.String() {
		t.Fatalf("replay: %d headers=%v body=%s", replay.Code, replay.Header(), replay.Body.String())
	}
	time.Sleep(10 * time.Millisecond)
	if got := f.segments.startCount(); got != 1 {
		t.Fatalf("replay launched %d workers", got)
	}

	run, err := f.store.GetWeftRun(launched.Run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.store.RecordWeftHello(run.ID, 1, "20260916T120000Z-12345678", "base", "criteria"); err != nil {
		t.Fatal(err)
	}
	if err := f.store.RecordWeftStage(ledger.WeftStage{
		RunID: run.ID, Segment: 1, Stage: "work", Occurrence: 1, Attempt: 1,
		State: "needs_input", At: "2026-09-16T12:01:00.000Z",
	}); err != nil {
		t.Fatal(err)
	}
	qid, err := f.store.RecordWeftParked(run.ID, 1, "work", 1, 1, "Which implementation?", "head", json.RawMessage(`{"state":"rendered"}`))
	if err != nil {
		t.Fatal(err)
	}
	if err := f.store.CompleteWeftSegment(run.ID, 1); err != nil {
		t.Fatal(err)
	}
	read := f.request(t, http.MethodGet, "/v1/runs/"+run.ID, "", "")
	if read.Code != http.StatusOK || !strings.Contains(read.Body.String(), qid) || !strings.Contains(read.Body.String(), `"state":"blocked"`) {
		t.Fatalf("parked read: %d %s", read.Code, read.Body.String())
	}
	questions := f.request(t, http.MethodGet, "/v1/questions", "", "")
	if questions.Code != http.StatusOK || !strings.Contains(questions.Body.String(), qid) {
		t.Fatalf("questions: %d %s", questions.Code, questions.Body.String())
	}
	runs := f.request(t, http.MethodGet, "/v1/runs?state=blocked", "", "")
	if runs.Code != http.StatusOK || !strings.Contains(runs.Body.String(), run.ID) {
		t.Fatalf("runs: %d %s", runs.Code, runs.Body.String())
	}

	answerBody := `{"contract_version":"weft-remote/v1","question_id":"` + qid + `","answer":"Use the existing shape."}`
	answer := f.request(t, http.MethodPost, "/v1/runs/"+run.ID+"/answer", "answer-1", answerBody)
	if answer.Code != http.StatusOK {
		t.Fatalf("answer: %d %s", answer.Code, answer.Body.String())
	}
	waitFor(t, func() bool { return f.segments.startCount() == 2 })
	question := f.request(t, http.MethodGet, "/v1/questions/"+qid, "", "")
	if question.Code != http.StatusOK || !strings.Contains(question.Body.String(), "Use the existing shape.") ||
		!strings.Contains(question.Body.String(), `"state":"answered"`) {
		t.Fatalf("answered question: %d %s", question.Code, question.Body.String())
	}
	answered, err := f.store.GetWeftRun(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.store.FailWeftSegment(run.ID, 2, ledger.ClassDied, "fixture worker died"); err != nil {
		t.Fatal(err)
	}
	if err := f.store.ReleaseWeftRepository(run.ID); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(answered.Worktree, 0o755); err != nil {
		t.Fatal(err)
	}
	result := f.request(t, http.MethodGet, "/v1/runs/"+run.ID+"/result", "", "")
	if result.Code != http.StatusOK || !strings.Contains(result.Body.String(), `"outcome":"failed"`) {
		t.Fatalf("result: %d %s", result.Code, result.Body.String())
	}
	resumed := f.request(t, http.MethodPost, "/v1/runs/"+run.ID+"/resume", "resume-1",
		`{"contract_version":"weft-remote/v1"}`)
	if resumed.Code != http.StatusOK {
		t.Fatalf("resume: %d %s", resumed.Code, resumed.Body.String())
	}
	stopped := f.request(t, http.MethodPost, "/v1/runs/"+run.ID+"/stop", "stop-1",
		`{"contract_version":"weft-remote/v1"}`)
	if stopped.Code != http.StatusOK || !strings.Contains(stopped.Body.String(), `"stopped":true`) {
		t.Fatalf("stop: %d %s", stopped.Code, stopped.Body.String())
	}
}

func TestServiceStrictBodiesAuthAndIdempotencyConflicts(t *testing.T) {
	f := newServiceFixture(t)
	req := httptest.NewRequest(http.MethodGet, "/v1/tickets", nil)
	rec := httptest.NewRecorder()
	f.service.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("unauthorized = %d", rec.Code)
	}
	unknown := f.request(t, http.MethodPost, "/v1/runs", "bad-1",
		`{"contract_version":"weft-remote/v1","ticket":"demo/ready-one","typo":true}`)
	if unknown.Code != http.StatusBadRequest || !strings.Contains(unknown.Body.String(), "typo") {
		t.Fatalf("unknown key: %d %s", unknown.Code, unknown.Body.String())
	}
	reused := f.request(t, http.MethodPost, "/v1/runs", "bad-1",
		`{"contract_version":"weft-remote/v1","ticket":"demo/blocked-two"}`)
	if reused.Code != http.StatusUnprocessableEntity {
		t.Fatalf("reused key = %d %s", reused.Code, reused.Body.String())
	}
	missing := f.request(t, http.MethodPost, "/v1/runs", "",
		`{"contract_version":"weft-remote/v1","ticket":"demo/ready-one"}`)
	if missing.Code != http.StatusBadRequest || !strings.Contains(missing.Body.String(), "idempotency_key_missing") {
		t.Fatalf("missing key = %d %s", missing.Code, missing.Body.String())
	}
	if err := f.store.Pause(ledger.WeftAgent); err != nil {
		t.Fatal(err)
	}
	paused := f.request(t, http.MethodPost, "/v1/runs", "paused-1",
		`{"contract_version":"weft-remote/v1","ticket":"demo/ready-one"}`)
	if paused.Code != http.StatusServiceUnavailable || !strings.Contains(paused.Body.String(), "host_paused") {
		t.Fatalf("paused host = %d %s", paused.Code, paused.Body.String())
	}
}

func TestEventStreamReconnectIsReadOnly(t *testing.T) {
	f := newServiceFixture(t)
	launch := f.request(t, http.MethodPost, "/v1/runs", "launch-events",
		`{"contract_version":"weft-remote/v1","ticket":"demo/ready-one"}`)
	if launch.Code != http.StatusCreated {
		t.Fatalf("launch: %s", launch.Body.String())
	}
	server := httptest.NewServer(f.service)
	defer server.Close()
	req, _ := http.NewRequest(http.MethodGet, server.URL+"/v1/events", nil)
	req.Header.Set("Authorization", "Bearer secret-token")
	req.Header.Set("Last-Event-ID", "0")
	resp, err := server.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(resp.Body)
	line, err := reader.ReadString('\n')
	if err != nil || !strings.HasPrefix(line, "id: 1") {
		t.Fatalf("first event line = %q, %v", line, err)
	}
	resp.Body.Close()
	runs, err := f.store.WeftRuns("", "")
	if err != nil || len(runs) != 1 || runs[0].Phase != ledger.WeftPhaseQueued {
		t.Fatalf("disconnect changed run: %+v, %v", runs, err)
	}
}

func TestEventStreamResetsWhenCursorPredatesRetention(t *testing.T) {
	f := newServiceFixture(t)
	now := time.Now().UTC()
	f.store.SetClock(func() time.Time { return now.Add(-8 * 24 * time.Hour) })
	old, err := f.store.ClaimWeft(ledger.WeftLaunch{
		Ticket: "demo/old", Project: "demo", Repo: "/tmp/old",
		Branch: "demo/old", Worktree: "/tmp/old-worktree",
		RunDirRoot: filepath.Join(f.root, "runs", ledger.WeftAgent),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := f.store.ReleaseWeftRepository(old.ID); err != nil {
		t.Fatal(err)
	}
	f.store.SetClock(func() time.Time { return now })
	if _, err := f.store.ClaimWeft(ledger.WeftLaunch{
		Ticket: "demo/current", Project: "demo", Repo: "/tmp/current",
		Branch: "demo/current", Worktree: "/tmp/current-worktree",
		RunDirRoot: filepath.Join(f.root, "runs", ledger.WeftAgent),
	}); err != nil {
		t.Fatal(err)
	}
	if err := f.store.PruneWeftRetention(now.Add(-7 * 24 * time.Hour)); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(f.service)
	defer server.Close()
	req, _ := http.NewRequest(http.MethodGet, server.URL+"/v1/events", nil)
	req.Header.Set("Authorization", "Bearer secret-token")
	req.Header.Set("Last-Event-ID", "0")
	resp, err := server.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(resp.Body)
	line, err := reader.ReadString('\n')
	resp.Body.Close()
	if err != nil || line != "event: reset\n" {
		t.Fatalf("reset line = %q, %v", line, err)
	}
}

func TestValidateListenAllowsOnlyLoopbackAndTailnet(t *testing.T) {
	for _, address := range []string{"127.0.0.1:7431", "100.64.0.1:7431", "100.127.255.254:7431"} {
		if err := ValidateListen(address); err != nil {
			t.Errorf("%s: %v", address, err)
		}
	}
	for _, address := range []string{"0.0.0.0:7431", "192.168.1.2:7431", "example.com:7431", "100.128.0.1:7431"} {
		if err := ValidateListen(address); err == nil {
			t.Errorf("%s was accepted", address)
		}
	}
}

func TestRepositoryAliasesShareOneReservation(t *testing.T) {
	root := t.TempDir()
	repo := filepath.Join(root, "repo")
	alias := filepath.Join(root, "alias")
	if err := os.MkdirAll(repo, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(repo, alias); err != nil {
		t.Fatal(err)
	}
	catalog := ticketCatalog{config: tkconfig.Config{Projects: map[string]tkconfig.Project{
		"direct": {Path: repo}, "alias": {Path: alias},
	}}}
	direct, err := catalog.projectPath("direct")
	if err != nil {
		t.Fatal(err)
	}
	throughAlias, err := catalog.projectPath("alias")
	if err != nil {
		t.Fatal(err)
	}
	if direct != throughAlias {
		t.Fatalf("repository identities differ: %s != %s", direct, throughAlias)
	}
	repositoryKey, err := sandbox.RepositoryIdentity(direct)
	if err != nil {
		t.Fatal(err)
	}
	store, err := ledger.Open(filepath.Join(root, "runs.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	first, err := store.ClaimWeft(ledger.WeftLaunch{
		Ticket: "direct/one", Project: "direct", Repo: direct, Branch: "direct/one",
		Worktree: filepath.Join(root, "one"), RunDirRoot: filepath.Join(root, "runs"),
		RepositoryKey: repositoryKey,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.ClaimWeft(ledger.WeftLaunch{
		Ticket: "alias/two", Project: "alias", Repo: throughAlias, Branch: "alias/two",
		Worktree: filepath.Join(root, "two"), RunDirRoot: filepath.Join(root, "runs"),
		RepositoryKey: repositoryKey,
	})
	var busy *ledger.RepositoryBusyError
	if !errors.As(err, &busy) || busy.HeldBy != first.ID {
		t.Fatalf("alias launch = %v, want reservation held by %s", err, first.ID)
	}
}

func waitFor(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("condition did not become true")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestErrorShapeCarriesOnlyContractAndError(t *testing.T) {
	f := newServiceFixture(t)
	rec := f.request(t, http.MethodGet, "/v1/runs/missing", "", "")
	var body map[string]json.RawMessage
	if err := json.Unmarshal(bytes.TrimSpace(rec.Body.Bytes()), &body); err != nil {
		t.Fatal(err)
	}
	if len(body) != 2 || body["contract_version"] == nil || body["error"] == nil {
		t.Fatalf("error body = %s", rec.Body.String())
	}
}
