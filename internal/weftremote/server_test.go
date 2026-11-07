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
	"testing"
	"time"

	"github.com/EnderRealm/ticket/v8/pkg/ticket"

	"shuttle/internal/config"
	"shuttle/internal/ledger"
	"shuttle/internal/sandbox"
)

type serviceFixture struct {
	root    string
	store   *ledger.Store
	service *Service
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
		Paths:    config.Paths{TKConfig: tkConfig},
		Weft: config.Weft{
			Listen: "127.0.0.1:0", WorkerBin: "/usr/bin/true",
			Root: filepath.Join(root, "weft"), SegmentTimeoutSeconds: 10,
			Repositories: map[string]string{"demo": projectPath}, AgentUsers: fixtureAgentUsers,
		},
	}
	service, err := New(cfg, store, "secret-token", root, log.New(io.Discard, "", 0))
	if err != nil {
		t.Fatal(err)
	}
	return &serviceFixture{root: root, store: store, service: service}
}

// criterionBody is a ticket body declaring one acceptance criterion, the
// least a ticket needs to be eligible.
const criterionBody = "## Acceptance Criteria\n\n- It works.\n"

func writeTicket(t *testing.T, central string, tk *ticket.Ticket) {
	t.Helper()
	writeTicketIn(t, central, "demo", tk)
}

func writeTicketIn(t *testing.T, central, namespace string, tk *ticket.Ticket) {
	t.Helper()
	doc, err := ticket.Serialize(tk)
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(central, "tickets", namespace)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, tk.ID+".md"), doc, 0o600); err != nil {
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

// parkedV1Run records a weft-remote/v1 run of demo/ready-one parked on a
// question, the way the supervisor records one.
func (f *serviceFixture) parkedV1Run(t *testing.T) (*ledger.WeftRun, string) {
	t.Helper()
	run, err := f.store.ClaimWeft(ledger.WeftLaunch{
		Ticket: "demo/ready-one", Project: "demo", Repo: filepath.Join(f.root, "code", "demo"),
		Branch: "demo/ready-one", Worktree: filepath.Join(f.root, "weft", "worktrees", "demo-ready-one"),
		RunDirRoot: filepath.Join(f.root, "runs", ledger.WeftAgent),
	})
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
	return run, qid
}

func TestV1RoutesServeReadsAndRefuseMutations(t *testing.T) {
	f := newServiceFixture(t)
	run, qid := f.parkedV1Run(t)
	list := f.request(t, http.MethodGet, "/v1/tickets", "", "")
	if list.Code != http.StatusOK || strings.Contains(list.Body.String(), "blocked-two") || !strings.Contains(list.Body.String(), "demo/ready-one") {
		t.Fatalf("ticket list: %d %s", list.Code, list.Body.String())
	}
	detail := f.request(t, http.MethodGet, "/v1/tickets/demo/ready-one", "", "")
	if detail.Code != http.StatusOK || !strings.Contains(detail.Body.String(), "Why this ticket exists.") {
		t.Fatalf("ticket detail: %d %s", detail.Code, detail.Body.String())
	}
	read := f.request(t, http.MethodGet, "/v1/runs/"+run.ID, "", "")
	if read.Code != http.StatusOK || !strings.Contains(read.Body.String(), qid) || !strings.Contains(read.Body.String(), `"state":"blocked"`) ||
		!strings.Contains(read.Body.String(), `"contract_version":"weft-remote/v1"`) || strings.Contains(read.Body.String(), "recorded_under") {
		t.Fatalf("parked read: %d %s", read.Code, read.Body.String())
	}
	questions := f.request(t, http.MethodGet, "/v1/questions", "", "")
	if questions.Code != http.StatusOK || !strings.Contains(questions.Body.String(), qid) {
		t.Fatalf("questions: %d %s", questions.Code, questions.Body.String())
	}
	question := f.request(t, http.MethodGet, "/v1/questions/"+qid, "", "")
	if question.Code != http.StatusOK || !strings.Contains(question.Body.String(), `"state":"pending"`) {
		t.Fatalf("question: %d %s", question.Code, question.Body.String())
	}
	runs := f.request(t, http.MethodGet, "/v1/runs?state=blocked", "", "")
	if runs.Code != http.StatusOK || !strings.Contains(runs.Body.String(), run.ID) {
		t.Fatalf("runs: %d %s", runs.Code, runs.Body.String())
	}

	asOf, err := f.store.WeftAsOf()
	if err != nil {
		t.Fatal(err)
	}
	for _, mutation := range []struct{ path, key, body string }{
		{"/v1/runs", "launch-2", `{"contract_version":"weft-remote/v1","ticket":"demo/ready-one"}`},
		{"/v1/runs/" + run.ID + "/answer", "answer-1", `{"contract_version":"weft-remote/v1","question_id":"` + qid + `","answer":"Use it."}`},
		{"/v1/runs/" + run.ID + "/stop", "stop-1", `{"contract_version":"weft-remote/v1"}`},
		{"/v1/runs/" + run.ID + "/resume", "resume-1", `{"contract_version":"weft-remote/v1"}`},
		{"/v1/runs", "typo-1", `{"contract_version":"weft-remote/v1","ticket":"demo/ready-one","typo":true}`},
	} {
		rec := f.request(t, http.MethodPost, mutation.path, mutation.key, mutation.body)
		if rec.Code != http.StatusServiceUnavailable || !strings.Contains(rec.Body.String(), `"code":"host_paused"`) ||
			!strings.Contains(rec.Body.String(), "weft-service/v2") || !strings.Contains(rec.Body.String(), `"contract_version":"weft-remote/v1"`) {
			t.Fatalf("POST %s = %d %s", mutation.path, rec.Code, rec.Body.String())
		}
	}
	after, err := f.store.WeftAsOf()
	if err != nil {
		t.Fatal(err)
	}
	parked, err := f.store.GetWeftRun(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	all, err := f.store.WeftRuns("", "")
	if err != nil {
		t.Fatal(err)
	}
	if after != asOf || len(all) != 1 || parked.Phase != ledger.WeftPhaseParked || parked.Question == nil {
		t.Fatalf("a refused v1 mutation changed state: as_of %d -> %d, %d runs, run %+v", asOf, after, len(all), parked)
	}
}

func TestV1IdempotencyRecordsStillReplay(t *testing.T) {
	f := newServiceFixture(t)
	body := `{"contract_version":"weft-remote/v1","ticket":"demo/ready-one"}`
	recorded := []byte(`{"as_of":7,"contract_version":"weft-remote/v1","run":{"run_id":"r-20260916T120000Z-0a1b2c3d"}}` + "\n")
	if err := f.store.PutWeftRequest(ledger.WeftRequest{
		TokenID: ledger.TokenID("secret-token"), Key: "launch-1", Method: http.MethodPost, Path: "/v1/runs",
		BodySHA: ledger.BodySHA([]byte(body)), Status: http.StatusCreated, Response: recorded,
		CreatedAt: time.Now().UTC().Format("2006-01-02T15:04:05.000Z"),
	}); err != nil {
		t.Fatal(err)
	}
	replay := f.request(t, http.MethodPost, "/v1/runs", "launch-1", body)
	if replay.Code != http.StatusCreated || replay.Header().Get("Idempotent-Replayed") != "true" ||
		replay.Body.String() != string(recorded) {
		t.Fatalf("replay: %d headers=%v body=%s", replay.Code, replay.Header(), replay.Body.String())
	}
	reused := f.request(t, http.MethodPost, "/v1/runs", "launch-1",
		`{"contract_version":"weft-remote/v1","ticket":"demo/blocked-two"}`)
	if reused.Code != http.StatusUnprocessableEntity {
		t.Fatalf("reused key = %d %s", reused.Code, reused.Body.String())
	}
	missing := f.request(t, http.MethodPost, "/v1/runs", "", body)
	if missing.Code != http.StatusBadRequest || !strings.Contains(missing.Body.String(), "idempotency_key_missing") {
		t.Fatalf("missing key = %d %s", missing.Code, missing.Body.String())
	}
	refused := f.request(t, http.MethodPost, "/v1/runs", "launch-3", body)
	again := f.request(t, http.MethodPost, "/v1/runs", "launch-3", body)
	if refused.Code != http.StatusServiceUnavailable || again.Code != http.StatusServiceUnavailable ||
		again.Header().Get("Idempotent-Replayed") != "true" || again.Body.String() != refused.Body.String() {
		t.Fatalf("refusal replay: %d %s / %d %v %s", refused.Code, refused.Body.String(), again.Code, again.Header(), again.Body.String())
	}
	req := httptest.NewRequest(http.MethodGet, "/v1/tickets", nil)
	rec := httptest.NewRecorder()
	f.service.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("unauthorized = %d", rec.Code)
	}
}

func TestV1EventStreamCarriesNoV2Payload(t *testing.T) {
	f := newServiceFixture(t)
	f.parkedV1Run(t)
	server := httptest.NewServer(f.service)
	defer server.Close()
	req, _ := http.NewRequest(http.MethodGet, server.URL+"/v1/events", nil)
	req.Header.Set("Authorization", "Bearer secret-token")
	req.Header.Set("Last-Event-ID", "0")
	client := server.Client()
	client.Timeout = 750 * time.Millisecond
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err == nil || len(data) != 0 {
		t.Fatalf("v1 stream after the migration sent %q (%v)", data, err)
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
	if _, err := f.store.PruneWeftRetention(now.Add(-eventRetention), now.Add(-idempotencyRetention)); err != nil {
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
	direct, err := sandbox.CanonicalRepository(repo)
	if err != nil {
		t.Fatal(err)
	}
	throughAlias, err := sandbox.CanonicalRepository(alias)
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
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.ClaimWeft(ledger.WeftLaunch{
		Ticket: "alias/two", Project: "alias", Repo: throughAlias, Branch: "alias/two",
		Worktree: filepath.Join(root, "two"), RunDirRoot: filepath.Join(root, "runs"),
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
