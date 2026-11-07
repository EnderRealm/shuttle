package weftremote

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/EnderRealm/ticket/v8/pkg/ticket"

	"shuttle/internal/config"
	"shuttle/internal/ledger"
	"shuttle/internal/sandbox"
	"shuttle/internal/weftfixtures"
)

// contractDir is weft's docs/service-v2, resolved from this package's checkout.
func contractDir(t *testing.T) string {
	t.Helper()
	return weftfixtures.Dir(t, ".")
}

func fixtureBytes(t *testing.T, name string) []byte {
	t.Helper()
	doc, err := os.ReadFile(filepath.Join(contractDir(t), name))
	if err != nil {
		t.Fatal(err)
	}
	return doc
}

func fixtureJSON(t *testing.T, name string, into any) {
	t.Helper()
	if err := json.Unmarshal(fixtureBytes(t, name), into); err != nil {
		t.Fatalf("%s: %v", name, err)
	}
}

// fixtureEvents parses a .sse fixture into its events, by name.
func fixtureEvents(t *testing.T, name string) map[string][]any {
	t.Helper()
	out := map[string][]any{}
	for _, block := range strings.Split(string(fixtureBytes(t, name)), "\n\n") {
		var event, data string
		for _, line := range strings.Split(block, "\n") {
			if v, ok := strings.CutPrefix(line, "event: "); ok {
				event = v
			}
			if v, ok := strings.CutPrefix(line, "data: "); ok {
				data = v
			}
		}
		if event == "" {
			continue
		}
		var payload any
		if err := json.Unmarshal([]byte(data), &payload); err != nil {
			t.Fatalf("%s %s: %v", name, event, err)
		}
		out[event] = append(out[event], payload)
	}
	return out
}

// shape is what the fixtures show at one position of a payload: the key sets
// an object there has had, the JSON kinds a value there has had, and the
// shapes beneath. A response is held to it so it writes every declared key
// and no undeclared one, with values of the declared kinds.
type shape struct {
	kinds   map[string]bool
	keySets map[string]bool
	fields  map[string]*shape
	elem    *shape
}

func kindOf(v any) string {
	switch v.(type) {
	case nil:
		return "null"
	case map[string]any:
		return "object"
	case []any:
		return "array"
	case string:
		return "string"
	case float64:
		return "number"
	case bool:
		return "bool"
	}
	return fmt.Sprintf("%T", v)
}

func keySet(m map[string]any) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return strings.Join(keys, ",")
}

func learn(samples ...any) *shape {
	sh := &shape{kinds: map[string]bool{}, keySets: map[string]bool{}, fields: map[string]*shape{}}
	for _, v := range samples {
		sh.add(v)
	}
	return sh
}

func (sh *shape) add(v any) {
	sh.kinds[kindOf(v)] = true
	switch v := v.(type) {
	case map[string]any:
		sh.keySets[keySet(v)] = true
		for k, child := range v {
			if sh.fields[k] == nil {
				sh.fields[k] = learn()
			}
			sh.fields[k].add(child)
		}
	case []any:
		for _, child := range v {
			if sh.elem == nil {
				sh.elem = learn()
			}
			sh.elem.add(child)
		}
	}
}

// check holds v to the shape. Null is accepted anywhere — the contract writes
// null where nothing applies — and a position the fixtures only ever show
// null or empty says nothing about kind.
func (sh *shape) check(t *testing.T, path string, v any) {
	t.Helper()
	if v == nil || sh == nil {
		return
	}
	kind := kindOf(v)
	known := false
	for k := range sh.kinds {
		known = known || k != "null"
	}
	if known && !sh.kinds[kind] {
		t.Errorf("%s: %s where the fixtures have %v", path, kind, sh.kinds)
		return
	}
	switch v := v.(type) {
	case map[string]any:
		if !sh.keySets[keySet(v)] {
			t.Errorf("%s: keys %s, the fixtures declare %v", path, keySet(v), sh.keySets)
			return
		}
		for k, child := range v {
			sh.fields[k].check(t, path+"."+k, child)
		}
	case []any:
		for i, child := range v {
			sh.elem.check(t, fmt.Sprintf("%s[%d]", path, i), child)
		}
	}
}

func decode(t *testing.T, body []byte) any {
	t.Helper()
	var v any
	if err := json.Unmarshal(body, &v); err != nil {
		t.Fatalf("decode %s: %v", body, err)
	}
	return v
}

// studio is a host whose tk store and ledger reproduce the worked example of
// the weft-service/v2 fixtures: six registered projects, the weft project's
// tickets as tickets.json lists them, and the runs and entry they show.
type studio struct {
	*serviceFixture
	t         *testing.T
	central   string
	tkConfig  string
	code      string
	projects  map[string]string
	cfg       *config.Config
	runIDs    map[string]string // fixture run id -> this host's
	ticketRun map[string]string // ticket -> this host's run
}

func newStudio(t *testing.T) *studio {
	t.Helper()
	root := t.TempDir()
	s := &studio{
		t: t, central: filepath.Join(root, "tickets-store"), tkConfig: filepath.Join(root, ".ticket", "config.yaml"),
		code: filepath.Join(root, "code"), projects: map[string]string{},
		runIDs: map[string]string{}, ticketRun: map[string]string{},
	}
	for _, dir := range []string{filepath.Dir(s.tkConfig), filepath.Join(s.central, "tickets")} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	for _, name := range []string{"loom", "scratch", "ticket", "warp", "weft"} {
		s.checkout(name)
		s.projects[name] = filepath.Join(s.code, name)
	}
	s.projects["docs-site"] = filepath.Join(s.code, "docs-site")
	s.cfg = &config.Config{
		Timezone: "UTC",
		Paths:    config.Paths{TKConfig: s.tkConfig, SRTBin: "/usr/bin/true"},
		Weft: config.Weft{
			Listen: "127.0.0.1:0", WorkerBin: "/usr/bin/true",
			Root: filepath.Join(root, "weft"), SegmentTimeoutSeconds: 10,
		},
	}
	s.writeConfig()
	store, err := ledger.Open(filepath.Join(root, "runs.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	service, err := New(s.cfg, store, "secret-token", root, log.New(io.Discard, "", 0))
	if err != nil {
		t.Fatal(err)
	}
	s.serviceFixture = &serviceFixture{root: root, store: store, service: service}
	return s
}

func (s *studio) checkout(name string) {
	if err := os.MkdirAll(filepath.Join(s.code, name, ".git"), 0o755); err != nil {
		s.t.Fatal(err)
	}
}

// writeConfig registers s.projects in tk's config.yaml and binds each in
// weft.repositories to the same path, the binding the catalog reads. The map
// is replaced, never edited, so a catalog read already holding the old one
// is not raced.
func (s *studio) writeConfig() {
	s.cfg.Weft.Repositories = maps.Clone(s.projects)
	var b strings.Builder
	b.WriteString("central_root: " + s.central + "\nprojects:\n")
	names := make([]string, 0, len(s.projects))
	for name := range s.projects {
		names = append(names, name)
	}
	slices.Sort(names)
	for _, name := range names {
		if s.projects[name] == "" {
			b.WriteString("  " + name + ": {}\n")
		} else {
			b.WriteString("  " + name + ":\n    path: " + s.projects[name] + "\n")
		}
	}
	if err := os.WriteFile(s.tkConfig, []byte(b.String()), 0o600); err != nil {
		s.t.Fatal(err)
	}
}

func (s *studio) ticket(namespace, slug, title string, status ticket.Status) *ticket.Ticket {
	tk := &ticket.Ticket{
		ID: slug, Status: status, Type: ticket.TypeFeature, Priority: 2, Title: title, Body: criterionBody,
		Created: time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC), Updated: time.Date(2026, 9, 2, 0, 0, 0, 0, time.UTC),
	}
	writeTicketIn(s.t, s.central, namespace, tk)
	return tk
}

type fixtureTicket struct {
	ID       string  `json:"id"`
	Title    string  `json:"title"`
	Status   string  `json:"status"`
	Type     string  `json:"type"`
	Priority *int    `json:"priority"`
	Updated  string  `json:"updated"`
	Parent   *string `json:"parent"`
	Deps     []struct {
		ID     string `json:"id"`
		Status string `json:"status"`
	} `json:"deps"`
	Blocked       bool    `json:"blocked"`
	Branch        *string `json:"branch"`
	Question      *string `json:"question"`
	CriteriaCount int     `json:"criteria_count"`
	Eligibility   struct {
		State   string `json:"state"`
		Reasons []struct {
			Code   string `json:"code"`
			Detail string `json:"detail"`
		} `json:"reasons"`
	} `json:"eligibility"`
	Run *struct {
		RunID          string  `json:"run_id"`
		State          string  `json:"state"`
		Phase          string  `json:"phase"`
		Outcome        *string `json:"outcome"`
		ActionRequired *string `json:"action_required"`
		Landing        string  `json:"landing"`
	} `json:"run"`
	Entry *struct {
		EntryID string `json:"entry_id"`
		Kind    string `json:"kind"`
		State   string `json:"state"`
	} `json:"entry"`
	Description *string `json:"description"`
	Acceptance  *string `json:"acceptance"`
	NotesTotal  int     `json:"notes_total"`
}

func bare(id string) string {
	_, slug, _ := strings.Cut(id, "/")
	return slug
}

// criteriaBody is a ticket body declaring n acceptance criteria, and no section
// at all for none.
func criteriaBody(n int) string {
	if n == 0 {
		return ""
	}
	return "## Acceptance Criteria\n\n" + strings.Repeat("- It works.\n", n)
}

// seedFixtureCatalog writes tickets.json's weft tickets, the other projects'
// tickets, and the runs and entry tickets.json shows.
func (s *studio) seedFixtureCatalog() []fixtureTicket {
	t := s.t
	var list struct {
		Tickets []fixtureTicket `json:"tickets"`
	}
	fixtureJSON(t, "tickets.json", &list)
	var detail struct {
		Ticket fixtureTicket `json:"ticket"`
	}
	fixtureJSON(t, "ticket.json", &detail)
	for _, ft := range list.Tickets {
		updated, err := time.Parse(time.RFC3339Nano, ft.Updated)
		if err != nil {
			t.Fatal(err)
		}
		tk := &ticket.Ticket{
			ID: bare(ft.ID), Title: ft.Title, Status: ticket.Status(ft.Status), Type: ticket.TicketType(ft.Type),
			Created: updated.Add(-time.Hour), Updated: updated, Body: criteriaBody(ft.CriteriaCount),
		}
		if ft.Priority != nil {
			tk.Priority = *ft.Priority
		}
		if ft.Parent != nil {
			tk.Parent = bare(*ft.Parent)
		}
		for _, dep := range ft.Deps {
			tk.Deps = append(tk.Deps, bare(dep.ID))
		}
		if ft.Branch != nil {
			tk.Branch = *ft.Branch
		}
		if ft.Question != nil {
			tk.Extra = map[string]string{ticket.QuestionField: *ft.Question}
		}
		if ft.ID == detail.Ticket.ID {
			tk.Body = *detail.Ticket.Description + "\n\n## Acceptance Criteria\n\n" + *detail.Ticket.Acceptance + "\n"
			for i := range detail.Ticket.NotesTotal {
				tk.Notes = append(tk.Notes, ticket.Note{Timestamp: updated.Add(time.Duration(i) * time.Minute), Text: fmt.Sprintf("note %d", i+1)})
			}
		}
		writeTicketIn(t, s.central, "weft", tk)
	}
	// docs-site's counts as projects.json gives them; one ticket in each
	// other bound project; scratch has none.
	for i, status := range []ticket.Status{"backlog", "backlog", "ready", "done", "done", "done", "done"} {
		s.ticket("docs-site", fmt.Sprintf("page-%d", i), "Page", status)
	}
	for _, name := range []string{"loom", "ticket", "warp"} {
		s.ticket(name, "first-0001", "First", ticket.StatusReady)
	}
	return list.Tickets
}

func (s *studio) summary(name string, path ...string) json.RawMessage {
	var doc map[string]any
	fixtureJSON(s.t, name, &doc)
	var v any = doc
	for _, key := range path {
		v = v.(map[string]any)[key]
	}
	raw, err := json.Marshal(v)
	if err != nil {
		s.t.Fatal(err)
	}
	return raw
}

func (s *studio) claim(ticketID string) *ledger.WeftRun {
	s.t.Helper()
	repo, err := sandbox.CanonicalRepository(s.projects["weft"])
	if err != nil {
		s.t.Fatal(err)
	}
	worktree, err := sandbox.WorktreePath(filepath.Join(s.root, "weft"), repo, ticketID)
	if err != nil {
		s.t.Fatal(err)
	}
	run, err := s.store.ClaimWeft(ledger.WeftLaunch{
		Ticket: ticketID, Project: "weft", Repo: repo, Branch: ticketID, Worktree: worktree,
		RunDirRoot: filepath.Join(s.root, "runs", ledger.WeftAgent),
	})
	if err != nil {
		s.t.Fatal(err)
	}
	s.ticketRun[ticketID] = run.ID
	return run
}

const fixtureBase = "27a28640112c1f6f09c83012db1a02dd6f46b7ce"

// seedFixtureRuns records the runs tickets.json shows, through the ledger
// calls the supervisor makes, and the entry it shows waiting.
func (s *studio) seedFixtureRuns(tickets []fixtureTicket) {
	t := s.t
	var parkedDoc struct {
		Run struct {
			Stages   []map[string]any `json:"stages"`
			Question struct {
				Text string `json:"text"`
			} `json:"question"`
		} `json:"run"`
	}
	fixtureJSON(t, "run.json", &parkedDoc)
	stage := func(runID string, segment int, row map[string]any) {
		doc, _ := json.Marshal(row)
		var st struct {
			Stage      string          `json:"stage"`
			Backend    string          `json:"backend"`
			Occurrence int             `json:"occurrence"`
			Attempt    int             `json:"attempt"`
			State      string          `json:"state"`
			Session    *string         `json:"session"`
			Turns      *int64          `json:"turns"`
			Spend      json.RawMessage `json:"spend"`
			Processing *int64          `json:"processing_ms"`
			Tokens     json.RawMessage `json:"tokens"`
			At         string          `json:"at"`
		}
		if err := json.Unmarshal(doc, &st); err != nil {
			t.Fatal(err)
		}
		if err := s.store.RecordWeftStage(ledger.WeftStage{
			RunID: runID, Segment: segment, Stage: st.Stage, Occurrence: st.Occurrence, Attempt: st.Attempt,
			Backend: nullSQL(&st.Backend), State: st.State, Session: nullSQL(st.Session),
			Turns: nullInt64(st.Turns), Spend: st.Spend, Processing: nullInt64(st.Processing),
			Tokens: st.Tokens, At: st.At,
		}); err != nil {
			t.Fatal(err)
		}
	}
	rendered := s.summary("run-result.json", "result", "summary")
	finished := func(ticketID string, answered bool) {
		run := s.claim(ticketID)
		if err := s.store.RecordWeftHello(run.ID, 1, "20260912T091424Z-1a2b3c4d", fixtureBase, "criteria"); err != nil {
			t.Fatal(err)
		}
		segment := 1
		if answered {
			qid, err := s.store.RecordWeftParked(run.ID, 1, "work", 1, 1, "Which shape?", fixtureBase, rendered)
			if err != nil {
				t.Fatal(err)
			}
			if err := s.store.CompleteWeftSegment(run.ID, 1); err != nil {
				t.Fatal(err)
			}
			if _, err := s.store.ClaimWeftAnswer(run.ID, qid, "The existing one.", filepath.Join(s.root, "runs", ledger.WeftAgent)); err != nil {
				t.Fatal(err)
			}
			segment = 2
			if err := s.store.RecordWeftHello(run.ID, 2, "20260913T150002Z-22222222", fixtureBase, "criteria"); err != nil {
				t.Fatal(err)
			}
		}
		if err := s.store.RecordWeftEnded(run.ID, segment, "finished", false, "", "", "", "", "",
			"90e485d97c77c8503459ec274a8fb41f13a1e625", rendered); err != nil {
			t.Fatal(err)
		}
		if err := s.store.CompleteWeftSegment(run.ID, segment); err != nil {
			t.Fatal(err)
		}
	}
	for _, ft := range tickets {
		if ft.Run == nil || ft.Run.Phase != "ended" {
			continue
		}
		finished(ft.ID, ft.ID == "weft/example-ticket-0a1b")
		s.runIDs[ft.Run.RunID] = s.ticketRun[ft.ID]
	}
	for _, ft := range tickets {
		if ft.Run == nil || ft.Run.Phase != "parked" {
			continue
		}
		run := s.claim(ft.ID)
		if err := s.store.RecordWeftHello(run.ID, 1, "20261001T163022Z-4d5e6f70", fixtureBase, "criteria"); err != nil {
			t.Fatal(err)
		}
		for _, row := range parkedDoc.Run.Stages {
			stage(run.ID, 1, row)
		}
		if _, err := s.store.RecordWeftParked(run.ID, 1, "work", 1, 1, parkedDoc.Run.Question.Text, fixtureBase,
			s.summary("run.json", "run", "summary")); err != nil {
			t.Fatal(err)
		}
		if err := s.store.CompleteWeftSegment(run.ID, 1); err != nil {
			t.Fatal(err)
		}
		s.runIDs[ft.Run.RunID] = run.ID
	}
	for _, ft := range tickets {
		if ft.Run == nil || ft.Run.Phase != "running" {
			continue
		}
		run := s.claim(ft.ID)
		if err := s.store.RecordWeftHello(run.ID, 1, "20261002T091500Z-2c3d4e5f", fixtureBase, "criteria"); err != nil {
			t.Fatal(err)
		}
		s.runIDs[ft.Run.RunID] = run.ID
	}
	// The row launch-response.json shows, written directly so it waits on the
	// seeded run without a dispatch pass spawning anything.
	db, err := sql.Open("sqlite", "file:"+filepath.Join(s.root, "runs.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	repo, _ := sandbox.CanonicalRepository(s.projects["weft"])
	if _, err := db.Exec(`INSERT INTO weft_entries(id,kind,ticket,project,repository_id,repository,position,state,
		waiting_reason,holder_kind,holder_id,token_id,request_key,created_at,updated_at,revision)
		VALUES('e-20261002T095017Z-9a0b1c2d','launch','weft/example-third-5d3e','weft',?,?,8,'waiting',
		'repository_reserved','run',?,'token','launch-third','2026-10-02T09:50:17.402Z','2026-10-02T09:50:17.402Z',1)`,
		sandbox.RepositoryID(repo), repo, s.ticketRun["weft/example-second-4c2d"]); err != nil {
		t.Fatal(err)
	}
}

// get is a v2 read: 200, the v2 contract, this host's id and an as_of no
// later than the stream's current seq.
func (s *studio) get(path string) map[string]any {
	s.t.Helper()
	rec := s.request(s.t, http.MethodGet, path, "", "")
	if rec.Code != http.StatusOK {
		s.t.Fatalf("GET %s = %d %s", path, rec.Code, rec.Body.String())
	}
	doc := decode(s.t, rec.Body.Bytes()).(map[string]any)
	asOf, err := s.store.WeftAsOf()
	if err != nil {
		s.t.Fatal(err)
	}
	if doc["contract_version"] != ServiceVersion || doc["host_id"] != s.service.hostID ||
		doc["as_of"] == nil || int64(doc["as_of"].(float64)) > asOf {
		s.t.Fatalf("GET %s read envelope = %v %v %v (stream at %d)", path, doc["contract_version"], doc["host_id"], doc["as_of"], asOf)
	}
	return doc
}

func (s *studio) errorBody(method, path string, status int, code string) map[string]any {
	s.t.Helper()
	rec := s.request(s.t, method, path, "", "")
	doc := decode(s.t, rec.Body.Bytes()).(map[string]any)
	if rec.Code != status || doc["error"].(map[string]any)["code"] != code {
		s.t.Fatalf("%s %s = %d %s, want %d %s", method, path, rec.Code, rec.Body.String(), status, code)
	}
	return doc
}

func errorShape(t *testing.T) *shape {
	var samples []any
	for _, name := range []string{"error-capability-not-served.json", "error-prior-run.json", "error-question-not-pending.json",
		"error-repository-reserved.json", "error-revision-conflict.json", "error-stale-base.json", "error-upgrade-required.json"} {
		samples = append(samples, decode(t, fixtureBytes(t, name)))
	}
	return learn(samples...)
}

func TestServiceDiscoveryMirrorsDevelopmentalSlice(t *testing.T) {
	s := newStudio(t)
	doc := s.get("/v2/service")
	learn(decode(t, fixtureBytes(t, "service.json")), decode(t, fixtureBytes(t, "service-developmental.json"))).check(t, "service", doc)
	var want map[string]any
	fixtureJSON(t, "service-developmental.json", &want)
	got, slice := doc["service"].(map[string]any), want["service"].(map[string]any)
	// output, landing and cleanup are gated by the worker probe (§2.6):
	// unprobed, each is not served and says what it needs, where the fixture
	// names the ticket that adds it. Archive import, terminals, the folder
	// picker and ticket edits are the host's own and are served (§5.4, §9,
	// §14.3). The rest is the slice as the fixture declares it.
	fixtureReasons := map[string]any{}
	for _, c := range slice["capabilities"].([]any) {
		fixtureReasons[c.(map[string]any)["id"].(string)] = c.(map[string]any)["reason"]
	}
	for _, c := range got["capabilities"].([]any) {
		c := c.(map[string]any)
		switch c["id"] {
		case "output", "landing", "cleanup":
			if c["state"] != "not_served" || !strings.Contains(c["reason"].(string), "has not been probed yet") {
				t.Errorf("unprobed %s = %v", c["id"], c)
			}
			c["reason"] = fixtureReasons[c["id"].(string)]
		case "archive", "terminals", "folders", "ticket_edit":
			if c["state"] != "served" || c["reason"] != nil {
				t.Errorf("%s = %v", c["id"], c)
			}
			c["state"], c["reason"] = "not_served", fixtureReasons[c["id"].(string)]
		}
	}
	for _, key := range []string{"service_versions", "worker_versions", "conformance", "capabilities", "limits"} {
		if !reflect.DeepEqual(got[key], slice[key]) {
			t.Errorf("service.%s = %v, service-developmental.json has %v", key, got[key], slice[key])
		}
	}
	if !strings.HasPrefix(got["host_build"].(string), "shuttle ") {
		t.Errorf("host_build = %v", got["host_build"])
	}
	persisted, err := s.store.WeftHostID()
	if err != nil || doc["host_id"] != persisted || !regexp.MustCompile(`^h-[a-z0-9-]{1,40}-[0-9a-f]{8}$`).MatchString(persisted) {
		t.Fatalf("host_id = %v, ledger has %q (%v)", doc["host_id"], persisted, err)
	}
}

func TestServiceRefusesUnauthorizedUnspokenAndNotServed(t *testing.T) {
	s := newStudio(t)
	shape := errorShape(t)
	for _, token := range []string{"", "Bearer wrong-token"} {
		req := httptest.NewRequest(http.MethodGet, "/v2/service", nil)
		if token != "" {
			req.Header.Set("Authorization", token)
		}
		rec := httptest.NewRecorder()
		s.service.ServeHTTP(rec, req)
		body := decode(t, rec.Body.Bytes())
		shape.check(t, "unauthorized", body)
		if rec.Code != http.StatusUnauthorized || body.(map[string]any)["contract_version"] != ServiceVersion {
			t.Fatalf("auth %q = %d %s", token, rec.Code, rec.Body.String())
		}
	}
	upgrade := s.errorBody(http.MethodGet, "/v3/service", http.StatusUpgradeRequired, "upgrade_required")
	if want := decode(t, fixtureBytes(t, "error-upgrade-required.json")); !reflect.DeepEqual(upgrade, want) {
		t.Errorf("426 = %v, want %v", upgrade, want)
	}
	s.errorBody(http.MethodPost, "/v9/launches", http.StatusUpgradeRequired, "upgrade_required")

	// The fixture refuses terminals, which this host serves; its message is
	// the shape every not-served capability's refusal takes.
	var fixture map[string]any
	fixtureJSON(t, "error-capability-not-served.json", &fixture)
	template := strings.ReplaceAll(fixture["error"].(map[string]any)["message"].(string), "h-studio-7c41e09a", s.service.hostID)
	var terminalsReason string
	for _, c := range decode(t, fixtureBytes(t, "service-developmental.json")).(map[string]any)["service"].(map[string]any)["capabilities"].([]any) {
		if c.(map[string]any)["id"] == "terminals" {
			terminalsReason = c.(map[string]any)["reason"].(string)
		}
	}
	var outputReason string
	for _, c := range s.service.capabilities() {
		if c.ID == "output" {
			outputReason = c.Reason
		}
	}
	message := "output" + strings.TrimSuffix(strings.TrimPrefix(template, "terminals"), terminalsReason) + outputReason
	output := s.errorBody(http.MethodGet, "/v2/runs/r-1/output", http.StatusNotImplemented, "capability_not_served")
	if output["error"].(map[string]any)["message"] != message {
		t.Errorf("501 message = %v, want %q", output["error"], message)
	}
	for _, path := range []string{"/v2/terminals", "/v2/folders"} {
		if rec := s.request(t, http.MethodGet, path, "", ""); rec.Code != http.StatusOK {
			t.Errorf("GET %s = %d %s; it is served", path, rec.Code, rec.Body.String())
		}
	}
	for _, route := range []struct{ method, path, capability string }{
		{http.MethodGet, "/v2/runs/r-1/output", "output"},
		{http.MethodGet, "/v2/runs/r-1/output/stream", "output"},
		{http.MethodGet, "/v2/repositories", "landing"},
		{http.MethodPost, "/v2/repositories/weft-47327fe5/continue", "landing"},
		{http.MethodPost, "/v2/runs/r-1/reconcile", "landing"},
		{http.MethodPost, "/v2/runs/r-1/dismiss", "cleanup"},
		{http.MethodPost, "/v2/runs/r-1/fresh", "cleanup"},
	} {
		body := s.errorBody(route.method, route.path, http.StatusNotImplemented, "capability_not_served")
		shape.check(t, route.path, body)
		if !strings.HasPrefix(body["error"].(map[string]any)["message"].(string), route.capability+" is not served by host "+s.service.hostID) {
			t.Errorf("%s %s = %v", route.method, route.path, body)
		}
	}
	shape.check(t, "upgrade", upgrade)
}

func TestCatalogServesEveryProjectAndTicketWithEligibility(t *testing.T) {
	s := newStudio(t)
	tickets := s.seedFixtureCatalog()
	s.seedFixtureRuns(tickets)

	projects := s.get("/v2/projects")
	learn(decode(t, fixtureBytes(t, "projects.json"))).check(t, "projects", projects)
	var fixtureProjects struct {
		Projects []map[string]any `json:"projects"`
	}
	fixtureJSON(t, "projects.json", &fixtureProjects)
	byName := map[string]map[string]any{}
	for _, p := range projects["projects"].([]any) {
		byName[p.(map[string]any)["project"].(string)] = p.(map[string]any)
	}
	if len(byName) != len(fixtureProjects.Projects) {
		t.Fatalf("projects = %v, want every one of projects.json", keysOf(byName))
	}
	for _, want := range fixtureProjects.Projects {
		name := want["project"].(string)
		got := byName[name]
		if got == nil {
			t.Fatalf("project %s is not listed", name)
		}
		if got["state"] != want["state"] || !reflect.DeepEqual(reasonCode(got), reasonCode(want)) {
			t.Errorf("%s: state %v %v, want %v %v", name, got["state"], got["reason"], want["state"], want["reason"])
		}
		if name == "weft" || name == "docs-site" || name == "scratch" {
			if !reflect.DeepEqual(got["counts"], want["counts"]) {
				t.Errorf("%s counts = %v, want %v", name, got["counts"], want["counts"])
			}
		}
		repository, _ := got["repository"].(string)
		if repository == "" || got["repository_id"] != sandbox.RepositoryID(repository) {
			t.Errorf("%s repository = %v %v", name, got["repository"], got["repository_id"])
		}
	}
	if repo := byName["docs-site"]["repository"]; repo != s.projects["docs-site"] {
		t.Errorf("docs-site repository = %v, want the configured path", repo)
	}

	catalog := s.get("/v2/tickets?project=weft")
	learn(decode(t, fixtureBytes(t, "tickets.json"))).check(t, "tickets", catalog)
	rows := map[string]map[string]any{}
	for _, row := range catalog["tickets"].([]any) {
		rows[row.(map[string]any)["id"].(string)] = row.(map[string]any)
	}
	if len(rows) != len(tickets) {
		t.Fatalf("weft tickets = %v, want every one of tickets.json", keysOf(rows))
	}
	for _, ft := range tickets {
		s.compareTicket(rows[ft.ID], ft)
	}

	var detail struct {
		Ticket fixtureTicket `json:"ticket"`
	}
	fixtureJSON(t, "ticket.json", &detail)
	one := s.get("/v2/tickets/" + detail.Ticket.ID)
	learn(decode(t, fixtureBytes(t, "ticket.json"))).check(t, "ticket", one)
	row := one["ticket"].(map[string]any)
	s.compareTicket(row, detail.Ticket)
	if row["description"] != *detail.Ticket.Description || row["acceptance"] != *detail.Ticket.Acceptance ||
		row["notes_total"] != float64(detail.Ticket.NotesTotal) ||
		!reflect.DeepEqual(row["runs"], []any{s.ticketRun[detail.Ticket.ID]}) {
		t.Errorf("ticket detail = %v", row)
	}

	unavailable := s.get("/v2/tickets?project=docs-site")
	for _, r := range unavailable["tickets"].([]any) {
		reasons := r.(map[string]any)["eligibility"].(map[string]any)["reasons"].([]any)
		last := reasons[len(reasons)-1].(map[string]any)
		if last["code"] != "project_unavailable" || !strings.Contains(last["detail"].(string), "does not exist") {
			t.Errorf("docs-site ticket %v reasons = %v", r.(map[string]any)["id"], reasons)
		}
	}
	if empty := s.get("/v2/tickets?project=scratch"); len(empty["tickets"].([]any)) != 0 {
		t.Errorf("scratch tickets = %v", empty["tickets"])
	}
	s.errorBody(http.MethodGet, "/v2/tickets?project=nowhere", http.StatusNotFound, "project_not_found")
	s.errorBody(http.MethodGet, "/v2/tickets/weft/missing-0000", http.StatusNotFound, "ticket_not_found")
	s.errorBody(http.MethodGet, "/v2/tickets/nowhere/first-0001", http.StatusNotFound, "ticket_not_found")
}

// A board reads each card's count off the listing, so the listing and a
// ticket's detail both carry what tk parses, 0 written out where there is
// none: on a ticket another reason already refuses, and in a project with no
// checkout, as much as on a launchable one.
func TestV2TicketListingCriteriaCount(t *testing.T) {
	s := newStudio(t)
	tickets := s.seedFixtureCatalog()

	listing := s.get("/v2/tickets?project=weft")
	learn(decode(t, fixtureBytes(t, "tickets.json"))).check(t, "tickets", listing)
	rows := map[string]map[string]any{}
	for _, row := range listing["tickets"].([]any) {
		rows[row.(map[string]any)["id"].(string)] = row.(map[string]any)
	}
	if len(rows) != len(tickets) {
		t.Fatalf("weft tickets = %v, want every one of tickets.json", keysOf(rows))
	}
	zeros := 0
	for _, ft := range tickets {
		if ft.CriteriaCount == 0 {
			zeros++
		}
		want := float64(ft.CriteriaCount)
		if got := rows[ft.ID]["criteria_count"]; got != want {
			t.Errorf("listing %s criteria_count = %v, want %v", ft.ID, got, want)
		}
		if got := s.get("/v2/tickets/" + ft.ID)["ticket"].(map[string]any)["criteria_count"]; got != want {
			t.Errorf("detail %s criteria_count = %v, want %v", ft.ID, got, want)
		}
	}
	if zeros == 0 {
		t.Fatal("tickets.json shows no ticket with 0 criteria")
	}

	var detail struct {
		Ticket fixtureTicket `json:"ticket"`
	}
	fixtureJSON(t, "ticket.json", &detail)
	one := s.get("/v2/tickets/" + detail.Ticket.ID)
	learn(decode(t, fixtureBytes(t, "ticket.json"))).check(t, "ticket", one)
	if got := one["ticket"].(map[string]any)["criteria_count"]; got != float64(detail.Ticket.CriteriaCount) {
		t.Errorf("ticket.json's %s criteria_count = %v, want %d", detail.Ticket.ID, got, detail.Ticket.CriteriaCount)
	}

	// What tk does and does not read as a criterion, spelled out rather than
	// derived from the parser under test.
	created := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	for _, c := range []struct {
		slug, body string
		want       float64
	}{
		{"empty-0001", "", 0},
		{"prose-0002", "Why.\n\n## Acceptance Criteria\n\nIt should work.\n", 0},
		{"elsewhere-0003", "- A bullet in the description.\n\n## Design\n\n- A bullet in the design.\n", 0},
		{"mixed-0004", "- A bullet in the description.\n\n## Acceptance Criteria\n\n" +
			"- Checked.\n  verify: go test ./...\n- Read.\n  unverifiable: a human read-through.\n- Stated.\n  - A nested bullet.\n", 3},
	} {
		writeTicketIn(t, s.central, "loom", &ticket.Ticket{
			ID: c.slug, Status: ticket.StatusReady, Type: ticket.TypeFeature, Priority: 1, Title: "Case",
			Body: c.body, Created: created, Updated: created,
		})
		if got := s.get("/v2/tickets/loom/" + c.slug)["ticket"].(map[string]any)["criteria_count"]; got != c.want {
			t.Errorf("detail %s criteria_count = %v, want %v", c.slug, got, c.want)
		}
		found := false
		for _, row := range s.get("/v2/tickets?project=loom")["tickets"].([]any) {
			if row := row.(map[string]any); row["id"] == "loom/"+c.slug {
				found = true
				if row["criteria_count"] != c.want {
					t.Errorf("listing %s criteria_count = %v, want %v", c.slug, row["criteria_count"], c.want)
				}
			}
		}
		if !found {
			t.Errorf("loom/%s is not in the listing", c.slug)
		}
	}

	for _, row := range s.get("/v2/tickets?project=docs-site")["tickets"].([]any) {
		if row := row.(map[string]any); row["criteria_count"] != float64(1) {
			t.Errorf("docs-site %s criteria_count = %v, want 1", row["id"], row["criteria_count"])
		}
	}
}

func reasonCode(project map[string]any) any {
	if r, ok := project["reason"].(map[string]any); ok {
		return r["code"]
	}
	return nil
}

func keysOf[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

// compareTicket holds a catalog row to its tickets.json entry: the same
// qualified identity, status, graph and every eligibility reason, the run
// and entry ids being this host's own.
func (s *studio) compareTicket(row map[string]any, want fixtureTicket) {
	t := s.t
	t.Helper()
	if row == nil {
		t.Fatalf("%s is not in the catalog", want.ID)
	}
	deps := []any{}
	for _, d := range want.Deps {
		deps = append(deps, map[string]any{"id": d.ID, "status": d.Status})
	}
	for key, value := range map[string]any{
		"id": want.ID, "project": "weft", "title": want.Title, "status": want.Status, "type": want.Type,
		"blocked": want.Blocked, "deps": deps, "parent": optional(want.Parent), "branch": optional(want.Branch),
		"question": optional(want.Question), "criteria_count": float64(want.CriteriaCount),
	} {
		if !reflect.DeepEqual(row[key], value) {
			t.Errorf("%s.%s = %v, want %v", want.ID, key, row[key], value)
		}
	}
	if want.Priority != nil && row["priority"] != float64(*want.Priority) {
		t.Errorf("%s.priority = %v, want %d", want.ID, row["priority"], *want.Priority)
	}
	eligibility := row["eligibility"].(map[string]any)
	reasons := []any{}
	for _, r := range want.Eligibility.Reasons {
		detail := r.Detail
		for fixtureID, ours := range s.runIDs {
			detail = strings.ReplaceAll(detail, fixtureID, ours)
		}
		reasons = append(reasons, map[string]any{"code": r.Code, "detail": detail})
	}
	if eligibility["state"] != want.Eligibility.State || !reflect.DeepEqual(eligibility["reasons"], reasons) {
		t.Errorf("%s eligibility = %v, want %s %v", want.ID, eligibility, want.Eligibility.State, reasons)
	}
	switch run := row["run"].(type) {
	case nil:
		if want.Run != nil {
			t.Errorf("%s has no run, tickets.json shows %s", want.ID, want.Run.RunID)
		}
	case map[string]any:
		if want.Run == nil || run["run_id"] != s.ticketRun[want.ID] || run["state"] != want.Run.State ||
			run["phase"] != want.Run.Phase || run["outcome"] != optional(want.Run.Outcome) ||
			run["action_required"] != optional(want.Run.ActionRequired) {
			t.Errorf("%s run = %v, want %+v", want.ID, run, want.Run)
		}
		// The seeded runs are recorded under weft-remote/v1, which never
		// lands; tickets.json's in-flight runs are v2 ones.
		if run["landing"] != "not_applicable" {
			t.Errorf("%s run landing = %v", want.ID, run["landing"])
		}
	}
	entry, _ := row["entry"].(map[string]any)
	if (want.Entry == nil) != (entry == nil) ||
		(entry != nil && (entry["entry_id"] != want.Entry.EntryID || entry["kind"] != want.Entry.Kind || entry["state"] != want.Entry.State)) {
		t.Errorf("%s entry = %v, want %+v", want.ID, row["entry"], want.Entry)
	}
}

func optional(v *string) any {
	if v == nil {
		return nil
	}
	return *v
}

// catalogRow is the project's catalog row for one ticket.
func (h *queueHost) catalogRow(id string) map[string]any {
	h.t.Helper()
	project, _, _ := strings.Cut(id, "/")
	for _, row := range h.send(http.MethodGet, "/v2/tickets?project="+project, "", "").body["tickets"].([]any) {
		if row.(map[string]any)["id"] == id {
			return row.(map[string]any)
		}
	}
	h.t.Fatalf("%s is not in the catalog", id)
	return nil
}

func TestCatalogZeroCriteriaIneligible(t *testing.T) {
	h := newQueueHost(t)
	created := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	writeTicketIn(t, h.central, "weft", &ticket.Ticket{
		ID: "uncriteria-0001", Status: ticket.StatusReady, Type: ticket.TypeFeature, Priority: 1, Title: "Prose only",
		Body: "Why.\n\n## Acceptance Criteria\n\nIt should work.\n", Created: created, Updated: created,
	})
	writeTicketIn(t, h.central, "weft", &ticket.Ticket{
		ID: "epic-0002", Status: ticket.StatusOpen, Type: ticket.TypeEpic, Priority: 1, Title: "Container",
		Created: created, Updated: created,
	})
	h.ticket("weft/ready-0003", ticket.StatusReady, 2)
	want := []any{map[string]any{"code": "no_acceptance_criteria", "detail": "the ticket declares no acceptance criteria; add them before it can run"}}

	if e := h.catalogRow("weft/uncriteria-0001")["eligibility"].(map[string]any); e["state"] != "ineligible" || !reflect.DeepEqual(e["reasons"], want) {
		t.Fatalf("catalog eligibility = %v, want ineligible %v", e, want)
	}
	// An epic is refused for being an epic, not also for the contract it has
	// no use for.
	for _, r := range h.catalogRow("weft/epic-0002")["eligibility"].(map[string]any)["reasons"].([]any) {
		if r.(map[string]any)["code"] == "no_acceptance_criteria" {
			t.Fatalf("epic reasons name no_acceptance_criteria: %v", r)
		}
	}

	if message := h.launch("weft/uncriteria-0001", "k1").refused(t, http.StatusConflict, "ticket_not_eligible"); !strings.Contains(message, "no_acceptance_criteria") {
		t.Fatalf("launch refusal = %s", message)
	}

	frontier := h.send(http.MethodGet, "/v2/frontier?project=weft", "", "").body["frontier"].(map[string]any)
	if !reflect.DeepEqual(frontier["admissible"], []any{"weft/ready-0003"}) {
		t.Fatalf("frontier admissible = %v", frontier["admissible"])
	}
	listed := false
	for _, row := range frontier["tickets"].([]any) {
		if row := row.(map[string]any); row["id"] == "weft/uncriteria-0001" {
			listed = reflect.DeepEqual(row["eligibility"].(map[string]any)["reasons"], want)
		}
	}
	if !listed {
		t.Fatalf("frontier tickets = %v, want weft/uncriteria-0001 with %v", frontier["tickets"], want)
	}
	h.send(http.MethodPost, "/v2/frontier", "f1", fmt.Sprintf(
		`{"contract_version":"weft-service/v2","project":"weft","catalog_revision":%v,"tickets":["weft/uncriteria-0001","weft/ready-0003"]}`,
		frontier["catalog_revision"])).refused(t, http.StatusConflict, "revision_conflict")
}

func TestCatalogUnverifiableOnlyEligible(t *testing.T) {
	h := newQueueHost(t)
	body := "Why.\n\n## Acceptance Criteria\n\n- Reviewed by hand.\n  unverifiable: a human read-through.\n"
	if c := ticket.ParseCriteria(ticket.AcceptanceCriteria(body)); len(c) != 1 || !c[0].Unverifiable {
		t.Fatalf("criteria = %+v, want one unverifiable criterion", c)
	}
	created := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	writeTicketIn(t, h.central, "weft", &ticket.Ticket{
		ID: "by-hand-0001", Status: ticket.StatusReady, Type: ticket.TypeFeature, Priority: 1, Title: "By hand",
		Body: body, Created: created, Updated: created,
	})

	if e := h.catalogRow("weft/by-hand-0001")["eligibility"].(map[string]any); e["state"] != "eligible" || len(e["reasons"].([]any)) != 0 {
		t.Fatalf("catalog eligibility = %v, want eligible", e)
	}
	frontier := h.send(http.MethodGet, "/v2/frontier?project=weft", "", "").body["frontier"].(map[string]any)
	if !reflect.DeepEqual(frontier["admissible"], []any{"weft/by-hand-0001"}) {
		t.Fatalf("frontier admissible = %v", frontier["admissible"])
	}
	h.launch("weft/by-hand-0001", "k1").expect(t, http.StatusAccepted, "launch-response.json")
}

func TestCatalogBindingsNeverDefaultARepository(t *testing.T) {
	s := newStudio(t)
	home := filepath.Join(s.root, "home")
	if err := os.MkdirAll(filepath.Join(home, ".git"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("HOME", home)
	if err := os.MkdirAll(filepath.Join(s.code, "bare"), 0o755); err != nil {
		t.Fatal(err)
	}
	s.projects["bare"] = filepath.Join(s.code, "bare")
	s.projects["unbound"] = ""
	s.projects["slash"] = "/"
	s.projects["homed"] = home
	s.writeConfig()
	for _, name := range []string{"bare", "unbound", "slash", "homed"} {
		s.ticket(name, "work-0001", "Work", ticket.StatusReady)
	}
	projects := s.get("/v2/projects")
	learn(decode(t, fixtureBytes(t, "projects.json"))).check(t, "projects", projects)
	want := map[string]string{"bare": "not_a_repository", "unbound": "no_binding", "slash": "root_binding", "homed": "root_binding"}
	for _, p := range projects["projects"].([]any) {
		p := p.(map[string]any)
		code, ok := want[p["project"].(string)]
		if !ok {
			continue
		}
		if p["state"] != "unavailable" || reasonCode(p) != code {
			t.Errorf("%s = %v %v, want unavailable %s", p["project"], p["state"], p["reason"], code)
		}
		if code != "not_a_repository" && (p["repository"] != nil || p["repository_id"] != nil) {
			t.Errorf("%s resolved to repository %v %v", p["project"], p["repository"], p["repository_id"])
		}
		rows := s.get("/v2/tickets?project=" + p["project"].(string))["tickets"].([]any)
		reasons := rows[0].(map[string]any)["eligibility"].(map[string]any)["reasons"].([]any)
		if len(rows) != 1 || reasons[len(reasons)-1].(map[string]any)["code"] != "project_unavailable" {
			t.Errorf("%s ticket = %v", p["project"], rows)
		}
	}
}

// A project is bound by weft.repositories alone: tk's own path for it is
// never a fallback, and a bound name an agent swapped for a symlink names no
// checkout and no repository.
func TestCatalogBindsFromWeftRepositoriesOnly(t *testing.T) {
	s := newStudio(t)
	s.checkout("linked-target")
	s.projects["linked"] = filepath.Join(s.code, "linked")
	if err := os.Symlink(filepath.Join(s.code, "linked-target"), s.projects["linked"]); err != nil {
		t.Fatal(err)
	}
	s.writeConfig()
	repositories := maps.Clone(s.cfg.Weft.Repositories)
	delete(repositories, "loom")
	s.cfg.Weft.Repositories = repositories
	want := map[string][2]string{
		"loom":   {"no_binding", "weft.repositories in shuttle's config.json binds no hosted checkout for loom"},
		"linked": {"not_a_repository", s.projects["linked"] + ", the path weft.repositories binds linked to, is a symlink"},
	}
	seen := 0
	for _, p := range s.get("/v2/projects")["projects"].([]any) {
		p := p.(map[string]any)
		name := p["project"].(string)
		if name == "weft" && p["state"] == "unavailable" {
			t.Errorf("weft, bound to a checkout, is unavailable: %v", p["reason"])
		}
		w, ok := want[name]
		if !ok {
			continue
		}
		seen++
		reason, _ := p["reason"].(map[string]any)
		detail, _ := reason["detail"].(string)
		if p["state"] != "unavailable" || reasonCode(p) != w[0] || !strings.HasPrefix(detail, w[1]) {
			t.Errorf("%s = %v %v, want unavailable %s: %s", name, p["state"], p["reason"], w[0], w[1])
		}
		if p["repository"] != nil || p["repository_id"] != nil {
			t.Errorf("%s names a repository: %v %v", name, p["repository"], p["repository_id"])
		}
	}
	if seen != len(want) {
		t.Fatalf("saw %d of %d projects under test", seen, len(want))
	}
}

// A bound directory an agent swaps for a link between the catalog inspecting
// it and resolving it is refused: the operator-owned checkout the link names,
// outside the bound parent, is never served as the project's repository.
func TestCatalogRefusesABindingReplacedWhileBound(t *testing.T) {
	s := newStudio(t)
	private := filepath.Join(s.root, "home", "code", "shuttle")
	if err := os.MkdirAll(filepath.Join(private, ".git"), 0o755); err != nil {
		t.Fatal(err)
	}
	s.checkout("swapped")
	bound := filepath.Join(s.code, "swapped")
	s.projects["swapped"] = bound
	s.writeConfig()
	swaps := 0
	prior := afterBindingInspected
	afterBindingInspected = func(path string) {
		if path != bound || swaps > 0 {
			return
		}
		swaps++
		if err := os.Rename(bound, bound+".inspected"); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(private, bound); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() { afterBindingInspected = prior })
	for _, p := range s.get("/v2/projects")["projects"].([]any) {
		p := p.(map[string]any)
		if p["project"] != "swapped" {
			continue
		}
		if swaps != 1 {
			t.Fatalf("binding swapped %d times, want 1", swaps)
		}
		if p["state"] != "unavailable" || reasonCode(p) != "not_a_repository" {
			t.Fatalf("swapped = %v %v, want unavailable not_a_repository", p["state"], p["reason"])
		}
		if p["repository"] != nil || p["repository_id"] != nil {
			t.Fatalf("swapped names a repository: %v %v", p["repository"], p["repository_id"])
		}
		return
	}
	t.Fatal("swapped is not in the catalog")
}

func TestCatalogRefusesAnIncompleteTKRead(t *testing.T) {
	s := newStudio(t)
	s.ticket("weft", "fine-0001", "Fine", ticket.StatusReady)
	s.get("/v2/projects")
	refuse := func(what string) {
		t.Helper()
		for _, path := range []string{"/v2/projects", "/v2/tickets?project=weft", "/v2/tickets/weft/fine-0001"} {
			body := s.errorBody(http.MethodGet, path, http.StatusInternalServerError, "malformed")
			errorShape(t).check(t, path, body)
			if message := body["error"].(map[string]any)["message"].(string); !strings.Contains(message, "partial catalog") ||
				!strings.Contains(message, what) {
				t.Errorf("%s with %s = %s", path, what, message)
			}
		}
	}
	unreadable := filepath.Join(s.central, "tickets", "weft", "garbled-0002.md")
	if err := os.WriteFile(unreadable, []byte("---\nid: [unterminated\n---\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	refuse("garbled-0002.md")
	if err := os.Remove(unreadable); err != nil {
		t.Fatal(err)
	}
	s.get("/v2/projects")
	catalogFile := filepath.Join(s.central, ticket.CatalogFile)
	if err := os.WriteFile(catalogFile, []byte("namespaces:\n  ghost:\n    kind: project\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	refuse("ghost")
	if err := os.Remove(catalogFile); err != nil {
		t.Fatal(err)
	}
	s.projects["_root"] = filepath.Join(s.code, "weft")
	s.writeConfig()
	refuse("_root")
}

func TestRunHistoryQuestionsAndDashboard(t *testing.T) {
	s := newStudio(t)
	tickets := s.seedFixtureCatalog()
	s.seedFixtureRuns(tickets)

	runs := s.get("/v2/runs")
	learn(decode(t, fixtureBytes(t, "runs.json"))).check(t, "runs", runs)
	var fixtureRuns struct {
		Runs []map[string]any `json:"runs"`
	}
	fixtureJSON(t, "runs.json", &fixtureRuns)
	listed := map[string]map[string]any{}
	for _, r := range runs["runs"].([]any) {
		listed[r.(map[string]any)["run_id"].(string)] = r.(map[string]any)
	}
	if len(listed) != len(s.ticketRun) || runs["page"].(map[string]any)["next_before"] != nil {
		t.Fatalf("runs = %v", runs)
	}
	compared := 0
	for _, want := range fixtureRuns.Runs {
		if want["recorded_under"] != "weft-remote/v1" {
			continue
		}
		got := listed[s.runIDs[want["run_id"].(string)]]
		for _, key := range []string{"ticket", "project", "origin", "recorded_under", "archive_id", "state", "phase",
			"outcome", "failed", "stopped", "action_required", "landing", "disposition", "segment", "question_id"} {
			if !reflect.DeepEqual(got[key], want[key]) {
				t.Errorf("v1 run of %s: %s = %v, runs.json has %v", want["ticket"], key, got[key], want[key])
			}
		}
		compared++
	}
	if compared != 2 {
		t.Fatalf("compared %d v1 runs of runs.json", compared)
	}

	runShape := learn(decode(t, fixtureBytes(t, "run.json")), decode(t, fixtureBytes(t, "run-landed.json")),
		decode(t, fixtureBytes(t, "run-not-landed.json")), decode(t, fixtureBytes(t, "run-stale-base.json")),
		decode(t, fixtureBytes(t, "run-stopped.json")))
	parkedID := s.ticketRun["weft/example-fourth-6e4f"]
	parked := s.get("/v2/runs/" + parkedID)
	runShape.check(t, "run", parked)
	run := parked["run"].(map[string]any)
	settlement := map[string]any{"state": "not_applicable", "step": nil, "reason": nil,
		"detail": "recorded under weft-remote/v1, which never lands", "branch": nil, "commit": nil, "steps": []any{}}
	if run["state"] != "blocked" || run["phase"] != "parked" || run["recorded_under"] != "weft-remote/v1" ||
		run["origin"] != "hosted" || run["disposition"] != "retained" || !reflect.DeepEqual(run["settlement"], settlement) ||
		run["question"].(map[string]any)["state"] != "pending" || run["repository_id"] != sandbox.RepositoryID(run["repo"].(string)) {
		t.Errorf("parked run = %v", run)
	}
	s.errorBody(http.MethodGet, "/v2/runs/"+parkedID+"/result", http.StatusConflict, "run_not_over")
	s.errorBody(http.MethodGet, "/v2/runs/r-20200101T000000Z-00000000", http.StatusNotFound, "run_not_found")

	answeredID := s.ticketRun["weft/example-ticket-0a1b"]
	over := s.get("/v2/runs/" + answeredID)
	runShape.check(t, "run", over)
	result := s.get("/v2/runs/" + answeredID + "/result")
	learn(decode(t, fixtureBytes(t, "run-result.json"))).check(t, "result", result)
	if r := result["result"].(map[string]any); r["outcome"] != "finished" || r["segments"] != float64(2) ||
		!reflect.DeepEqual(r["settlement"], settlement) {
		t.Errorf("result = %v", r)
	}

	questions := s.get("/v2/questions")
	learn(decode(t, fixtureBytes(t, "questions.json"))).check(t, "questions", questions)
	pending := questions["questions"].([]any)
	if len(pending) != 1 || pending[0].(map[string]any)["run_id"] != parkedID {
		t.Fatalf("pending questions = %v", pending)
	}
	answered := over["run"].(map[string]any)["question"].(map[string]any)
	one := s.get("/v2/questions/" + answered["question_id"].(string))
	learn(decode(t, fixtureBytes(t, "question-answered.json"))).check(t, "question", one)
	answer := one["question"].(map[string]any)["answer"].(map[string]any)
	if one["question"].(map[string]any)["state"] != "answered" || answer["entry_id"] != nil || answer["consumed_segment"] != float64(2) {
		t.Errorf("answered question = %v", one)
	}
	s.errorBody(http.MethodGet, "/v2/questions/q-20200101T000000Z-00000000", http.StatusNotFound, "question_not_found")

	dashboard := s.get("/v2/dashboard")
	learn(decode(t, fixtureBytes(t, "dashboard.json"))).check(t, "dashboard", dashboard)
	d := dashboard["dashboard"].(map[string]any)
	if !reflect.DeepEqual(d["capacity"], map[string]any{"cap": float64(3), "running": float64(1), "waiting": float64(1)}) ||
		!reflect.DeepEqual(d["runs"], map[string]any{"active": float64(1), "blocked": float64(1), "action_required": float64(0)}) ||
		d["questions_pending"] != float64(1) || d["repositories_paused"] != float64(0) || d["terminals_open"] != float64(0) ||
		!reflect.DeepEqual(d["host"], map[string]any{"dispatch": "open", "reason": nil}) {
		t.Errorf("dashboard = %v", d)
	}
	// Nothing observed and an unprobed worker: no tile carries a figure, and
	// each says why (§5.3).
	var states []string
	for _, tile := range d["allowance"].([]any) {
		tile := tile.(map[string]any)
		states = append(states, tile["provider"].(string)+" "+tile["state"].(string))
		if tile["observed_at"] != nil || len(tile["windows"].([]any)) != 0 || tile["reason"] == "" ||
			(tile["state"] == "unavailable") != (tile["source"] == nil) {
			t.Errorf("tile = %v: a tile with nothing observed carries no figure and says why", tile)
		}
	}
	if !reflect.DeepEqual(states, []string{"claude unknown", "codex unavailable", "cursor unavailable"}) {
		t.Errorf("allowance tiles = %v", states)
	}
	if err := s.store.Pause(ledger.WeftAgent); err != nil {
		t.Fatal(err)
	}
	host := s.get("/v2/dashboard")["dashboard"].(map[string]any)["host"].(map[string]any)
	if host["dispatch"] != "paused" || !strings.Contains(host["reason"].(string), "shuttle resume weft-worker") {
		t.Errorf("paused host = %v", host)
	}
}

func TestRunHistoryPagesByBefore(t *testing.T) {
	s := newStudio(t)
	// Ledger run ids are second-stamped with 16 random bits; a clock that
	// moves keeps 101 claims from colliding on one.
	clock := time.Date(2026, 10, 2, 9, 0, 0, 0, time.UTC)
	s.store.SetClock(func() time.Time {
		clock = clock.Add(time.Second)
		return clock
	})
	for i := range runsPage + 1 {
		run := s.claim(fmt.Sprintf("weft/page-%04d", i))
		if err := s.store.ReleaseWeftRepository(run.ID); err != nil {
			t.Fatal(err)
		}
	}
	all, err := s.store.WeftRuns("", "")
	if err != nil {
		t.Fatal(err)
	}
	first := s.get("/v2/runs")
	page := first["runs"].([]any)
	next := first["page"].(map[string]any)["next_before"]
	if len(page) != runsPage || next != all[runsPage-1].ID || page[0].(map[string]any)["run_id"] != all[0].ID {
		t.Fatalf("first page: %d runs, next_before %v", len(page), next)
	}
	second := s.get("/v2/runs?before=" + next.(string))
	if rest := second["runs"].([]any); len(rest) != 1 || rest[0].(map[string]any)["run_id"] != all[runsPage].ID ||
		second["page"].(map[string]any)["next_before"] != nil {
		t.Fatalf("second page = %v", second)
	}
	if filtered := s.get("/v2/runs?ticket=weft/page-0007&state=active&origin=hosted")["runs"].([]any); len(filtered) != 1 {
		t.Fatalf("filtered = %v", filtered)
	}
	if over := s.get("/v2/runs?state=over")["runs"].([]any); len(over) != 0 {
		t.Fatalf("over = %v", over)
	}
	s.errorBody(http.MethodGet, "/v2/runs?before=r-20200101T000000Z-00000000", http.StatusBadRequest, "malformed")
	s.errorBody(http.MethodGet, "/v2/runs?state=sideways", http.StatusBadRequest, "malformed")
	s.errorBody(http.MethodGet, "/v2/runs?origin=elsewhere", http.StatusBadRequest, "malformed")
}

type sseEvent struct{ id, name, data string }

type stream struct {
	resp   *http.Response
	events chan sseEvent
}

func openStream(t *testing.T, server *httptest.Server, lastID string) *stream {
	t.Helper()
	return openPath(t, server, "/v2/events", lastID)
}

func openPath(t *testing.T, server *httptest.Server, path, lastID string) *stream {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, server.URL+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer secret-token")
	if lastID != "" {
		req.Header.Set("Last-Event-ID", lastID)
	}
	resp, err := server.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK || resp.Header.Get("Content-Type") != "text/event-stream" {
		t.Fatalf("GET %s = %d %s", path, resp.StatusCode, resp.Header.Get("Content-Type"))
	}
	s := &stream{resp: resp, events: make(chan sseEvent, 256)}
	// Closed however the test ends, so a server closed in a later cleanup is
	// not left waiting on this connection.
	t.Cleanup(s.close)
	go func() {
		defer close(s.events)
		reader := bufio.NewReader(resp.Body)
		var ev sseEvent
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				return
			}
			line = strings.TrimSuffix(line, "\n")
			switch {
			case line == "":
				if ev.name != "" {
					s.events <- ev
				}
				ev = sseEvent{}
			case strings.HasPrefix(line, "id: "):
				ev.id = strings.TrimPrefix(line, "id: ")
			case strings.HasPrefix(line, "event: "):
				ev.name = strings.TrimPrefix(line, "event: ")
			case strings.HasPrefix(line, "data: "):
				ev.data = strings.TrimPrefix(line, "data: ")
			}
		}
	}()
	return s
}

func (s *stream) next(t *testing.T) sseEvent {
	t.Helper()
	select {
	case ev, ok := <-s.events:
		if !ok {
			t.Fatal("stream closed")
		}
		return ev
	case <-time.After(5 * time.Second):
		t.Fatal("no event within 5s")
	}
	return sseEvent{}
}

func (s *stream) close() { s.resp.Body.Close() }

// client is a desktop's model of the catalog: what it read through GETs,
// refreshed on each catalog event and rebuilt on a reset.
type client struct {
	studio   *studio
	lastID   string
	projects string
	tickets  map[string]string
}

func strip(doc map[string]any) string {
	delete(doc, "as_of")
	b, _ := json.Marshal(doc)
	return string(b)
}

func (c *client) rebuild() {
	c.studio.t.Helper()
	doc := c.studio.get("/v2/projects")
	c.tickets = map[string]string{}
	for _, p := range doc["projects"].([]any) {
		name := p.(map[string]any)["project"].(string)
		c.tickets[name] = strip(c.studio.get("/v2/tickets?project=" + name))
	}
	c.projects = strip(doc)
}

// opening reads the stream event every connection opens with.
func (c *client) opening(t *testing.T, s *stream, wantReset bool) map[string]any {
	t.Helper()
	ev := s.next(t)
	if ev.name != "stream" || ev.id != "" {
		t.Fatalf("first event = %+v, want stream with no id", ev)
	}
	doc := decode(t, []byte(ev.data)).(map[string]any)
	learn(fixtureEvents(t, "events.sse")["stream"]...).check(t, "stream", doc)
	if doc["host_id"] != c.studio.service.hostID || doc["reset"] != wantReset {
		t.Fatalf("stream event = %v, want reset %v", doc, wantReset)
	}
	if wantReset {
		c.rebuild()
	}
	c.lastID = fmt.Sprint(int64(doc["seq"].(float64)))
	return doc
}

// consume applies events until one names project at revision.
func (c *client) consume(t *testing.T, s *stream, project string, revision int64) {
	t.Helper()
	catalogShape := learn(fixtureEvents(t, "events.sse")["catalog"]...)
	for {
		ev := s.next(t)
		c.lastID = ev.id
		if ev.name != "catalog" {
			continue
		}
		doc := decode(t, []byte(ev.data)).(map[string]any)
		catalogShape.check(t, "catalog", doc)
		if doc["seq"] != decode(t, []byte(ev.id)) {
			t.Fatalf("catalog event id %s carries seq %v", ev.id, doc["seq"])
		}
		c.rebuild()
		if doc["project"] == project && int64(doc["catalog_revision"].(float64)) == revision {
			return
		}
	}
}

func (s *studio) revision(project string) int64 {
	s.t.Helper()
	for _, p := range s.get("/v2/projects")["projects"].([]any) {
		if p.(map[string]any)["project"] == project {
			return int64(p.(map[string]any)["catalog_revision"].(float64))
		}
	}
	s.t.Fatalf("%s is not listed", project)
	return 0
}

// sync is one pass of the host's tk watch.
func (s *studio) sync() {
	s.t.Helper()
	if _, err := s.service.syncCatalog(); err != nil {
		s.t.Fatal(err)
	}
}

func TestTwoClientsConvergeThroughCatalogEventsAndGets(t *testing.T) {
	s := newStudio(t)
	s.seedFixtureCatalog()
	server := httptest.NewServer(s.service)
	defer server.Close()
	s.sync()

	a, b := &client{studio: s}, &client{studio: s}
	a.rebuild()
	b.rebuild()
	streamA, streamB := openStream(t, server, ""), openStream(t, server, "")
	a.opening(t, streamA, false)
	b.opening(t, streamB, false)

	// A ticket edited in tk, outside Shuttle.
	edited := s.ticket("weft", "example-ready-a0b1", "Render capacity in the rail header", ticket.StatusReady)
	s.sync()
	weft := s.revision("weft")
	a.consume(t, streamA, "weft", weft)
	b.consume(t, streamB, "weft", weft)
	if a.tickets["weft"] != b.tickets["weft"] || !strings.Contains(a.tickets["weft"], edited.Title) {
		t.Fatalf("after an external edit the clients disagree:\n%s\n%s", a.tickets["weft"], b.tickets["weft"])
	}

	// A disconnects; a ticket changes and a project is registered.
	streamA.close()
	s.ticket("loom", "second-0002", "Second", ticket.StatusBacklog)
	s.checkout("added")
	s.projects["added"] = filepath.Join(s.code, "added")
	s.writeConfig()
	s.sync()
	b.consume(t, streamB, "added", 0)
	streamA = openStream(t, server, a.lastID)
	a.opening(t, streamA, false)
	a.consume(t, streamA, "added", 0)
	if !strings.Contains(b.projects, `"project":"added"`) || a.projects != b.projects ||
		!reflect.DeepEqual(a.tickets, b.tickets) {
		t.Fatalf("after reconnecting the clients disagree:\n%s\n%s", a.projects, b.projects)
	}

	// A disconnects again and its cursor ages out of retention, while the
	// project map loses a project.
	streamA.close()
	stale := a.lastID
	s.ticket("weft", "example-ready-a0b1", "Render capacity on the rail", ticket.StatusReady)
	s.sync()
	b.consume(t, streamB, "weft", weft+1)
	if _, err := s.store.PruneWeftRetention(time.Now().Add(time.Hour), time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	delete(s.projects, "added")
	s.writeConfig()
	s.sync()
	b.consume(t, streamB, "added", 1)
	streamA = openStream(t, server, stale)
	opening := a.opening(t, streamA, true)
	if opening["seq"] != decode(t, []byte(b.lastID)) {
		t.Fatalf("reset stream opens at %v, the stream is at %s", opening["seq"], b.lastID)
	}
	fresh := &client{studio: s}
	fresh.rebuild()
	if strings.Contains(fresh.projects, `"project":"added"`) || a.projects != b.projects || a.projects != fresh.projects ||
		!reflect.DeepEqual(a.tickets, b.tickets) || !reflect.DeepEqual(a.tickets, fresh.tickets) {
		t.Fatalf("after a reset the clients disagree:\n%s\n%s\n%s", a.projects, b.projects, fresh.projects)
	}
	streamA.close()
	streamB.close()
}

// A desktop that was a v1 client may still hold a Last-Event-ID at or below
// the migration's floor, and a cursor past newest is one this ledger never
// allocated; neither is continued (§14.2).
func TestStateStreamResetsAtTheV1FloorAndPastNewest(t *testing.T) {
	f := newServiceFixture(t)
	path := filepath.Join(f.root, "schema-two.db")
	db, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatal(err)
	}
	for _, stmt := range []string{
		`CREATE TABLE schema_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)`,
		`INSERT INTO schema_meta(key,value) VALUES('schema_version','2')`,
		`CREATE TABLE weft_events (seq INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT NOT NULL,
			run_id TEXT NOT NULL, type TEXT NOT NULL, payload TEXT NOT NULL)`,
		`INSERT INTO weft_events(ts,run_id,type,payload) VALUES
			('2026-09-13T14:22:33.412Z','r-20260913T142233Z-6b2f0c1a','run','{"contract_version":"weft-remote/v1"}'),
			('2026-09-13T16:49:03.556Z','r-20260913T142233Z-6b2f0c1a','ended','{"contract_version":"weft-remote/v1"}'),
			('2026-09-14T09:00:00.000Z','r-20260914T090000Z-0c1d2e3f','run','{"contract_version":"weft-remote/v1"}')`,
	} {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	db.Close()
	store, err := ledger.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	service, err := New(f.service.cfg, store, "secret-token", f.root, log.New(io.Discard, "", 0))
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(service)
	defer server.Close()
	opening := func(lastID string, wantSeq, wantOldest int64, wantReset bool) {
		t.Helper()
		str := openStream(t, server, lastID)
		defer str.close()
		ev := str.next(t)
		if ev.name != "stream" {
			t.Fatalf("Last-Event-ID %q: first event = %+v, want stream", lastID, ev)
		}
		doc := decode(t, []byte(ev.data)).(map[string]any)
		if doc["host_id"] != service.hostID || doc["seq"] != float64(wantSeq) ||
			doc["oldest"] != float64(wantOldest) || doc["reset"] != wantReset {
			t.Fatalf("Last-Event-ID %q: stream event = %v, want seq %d oldest %d reset %v",
				lastID, doc, wantSeq, wantOldest, wantReset)
		}
	}

	// Migrated with no v2 event yet: floor 3, oldest 4, newest 3.
	opening("", 3, 4, false)
	opening("2", 3, 4, true)
	opening("3", 3, 4, true)
	opening("4", 3, 4, true)

	// The first v2 events land above the floor, and only a cursor among
	// them continues.
	if _, err := store.ClaimWeft(ledger.WeftLaunch{
		Ticket: "demo/ready-one", Project: "demo", Repo: filepath.Join(f.root, "code", "demo"),
		Branch: "demo/ready-one", Worktree: filepath.Join(f.root, "weft", "worktrees", "demo-ready-one"),
		RunDirRoot: filepath.Join(f.root, "runs", ledger.WeftAgent),
	}); err != nil {
		t.Fatal(err)
	}
	newest, err := store.WeftAsOf()
	if err != nil || newest < 4 {
		t.Fatalf("as_of after the claim = %d, %v", newest, err)
	}
	opening("3", newest, 4, true)
	opening("4", 4, 4, false)
	opening(fmt.Sprint(newest), newest, 4, false)
	opening(fmt.Sprint(newest+1), newest, 4, true)
}

func TestStateStreamCarriesV2RunEvents(t *testing.T) {
	s := newStudio(t)
	s.seedFixtureCatalog()
	server := httptest.NewServer(s.service)
	defer server.Close()
	c := &client{studio: s}
	str := openStream(t, server, "0")
	c.opening(t, str, false)
	run := s.claim("weft/example-ready-a0b1")
	if err := s.store.RecordWeftHello(run.ID, 1, "20261002T091500Z-2c3d4e5f", fixtureBase, "criteria"); err != nil {
		t.Fatal(err)
	}
	if err := s.store.RecordWeftStage(ledger.WeftStage{
		RunID: run.ID, Segment: 1, Stage: "review", Occurrence: 1, Attempt: 1,
		Backend: sql.NullString{String: "claude", Valid: true}, State: "running", At: "2026-10-02T10:36:02.118Z",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.store.RecordWeftParked(run.ID, 1, "review", 1, 1, "Which label?", fixtureBase,
		s.summary("run.json", "run", "summary")); err != nil {
		t.Fatal(err)
	}
	samples := fixtureEvents(t, "events.sse")
	for name, more := range fixtureEvents(t, "events-reset.sse") {
		samples[name] = append(samples[name], more...)
	}
	previous := int64(0)
	for i, name := range []string{"repository", "run", "run", "stage", "question", "run"} {
		ev := str.next(t)
		if ev.name != name {
			t.Fatalf("event %d = %s, want %s", i, ev.name, name)
		}
		doc := decode(t, []byte(ev.data)).(map[string]any)
		learn(samples[ev.name]...).check(t, ev.name, doc)
		seq := int64(doc["seq"].(float64))
		if doc["contract_version"] != ServiceVersion || fmt.Sprint(seq) != ev.id || seq <= previous {
			t.Fatalf("event %s id %s = %v", ev.name, ev.id, doc)
		}
		previous = seq
		if i == 1 && (doc["run_id"] != run.ID || doc["phase"] != "starting" || doc["landing"] != "not_applicable" ||
			doc["disposition"] != "retained" || doc["revision"] != float64(1)) {
			t.Errorf("claimed run event = %v", doc)
		}
	}
	str.close()
}

func TestHostWatchesTKForExternalEdits(t *testing.T) {
	s := newStudio(t)
	s.ticket("weft", "watched-0001", "Watched", ticket.StatusReady)
	server := httptest.NewServer(s.service)
	defer server.Close()
	s.get("/v2/projects")
	c := &client{studio: s}
	str := openStream(t, server, "")
	defer str.close()
	c.opening(t, str, false)
	s.service.catalogPoll = 20 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.service.watchCatalog(ctx)
	s.ticket("weft", "watched-0001", "Watched and edited", ticket.StatusOpen)
	c.consume(t, str, "weft", 1)
	if !strings.Contains(c.tickets["weft"], "Watched and edited") {
		t.Fatalf("catalog after the watched edit = %s", c.tickets["weft"])
	}
}

// A host whose traffic is all v2 still enforces the retention /v2/service
// advertises: the retention watch, with no request in flight, drops state
// events and idempotency records older than their windows and keeps those
// inside them, and a cursor the prune left behind opens with a reset.
func TestHostPrunesRetentionOnASchedule(t *testing.T) {
	s := newStudio(t)
	s.ticket("weft", "aged-0001", "Aged", ticket.StatusReady)
	server := httptest.NewServer(s.service)
	defer server.Close()
	now := time.Now()

	// Events from beyond the event window, with a cursor among them, then
	// events just inside it.
	s.store.SetClock(func() time.Time { return now.Add(-eventRetention - time.Hour) })
	s.sync()
	stale, err := s.store.WeftAsOf()
	if err != nil {
		t.Fatal(err)
	}
	s.ticket("weft", "aged-0001", "Aged and edited", ticket.StatusReady)
	s.sync()
	kept, err := s.store.WeftAsOf()
	if err != nil {
		t.Fatal(err)
	}
	kept++
	s.store.SetClock(func() time.Time { return now.Add(-eventRetention + time.Hour) })
	s.ticket("weft", "aged-0001", "Kept", ticket.StatusOpen)
	s.sync()
	s.store.SetClock(time.Now)

	tokenID := ledger.TokenID("secret-token")
	for key, created := range map[string]time.Time{
		"aged": now.Add(-idempotencyRetention - time.Hour),
		"kept": now.Add(-idempotencyRetention + time.Hour),
	} {
		if err := s.store.PutWeftRequest(ledger.WeftRequest{
			TokenID: tokenID, Key: key, Method: http.MethodPost, Path: "/v2/launches",
			BodySHA: ledger.BodySHA(nil), Status: http.StatusAccepted, Response: []byte("{}\n"),
			CreatedAt: formatTime(created),
		}); err != nil {
			t.Fatal(err)
		}
	}

	// Before the watch runs nothing has aged out: the cursor continues.
	c := &client{studio: s}
	str := openStream(t, server, fmt.Sprint(stale))
	c.opening(t, str, false)
	str.close()

	s.service.prunePoll = 20 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.service.watchRetention(ctx)
	deadline := time.Now().Add(5 * time.Second)
	for {
		_, err := s.store.GetWeftRequest(tokenID, "aged")
		if errors.Is(err, sql.ErrNoRows) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if time.Now().After(deadline) {
			t.Fatal("the retention watch did not prune the aged idempotency record within 5s")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, err := s.store.GetWeftRequest(tokenID, "kept"); err != nil {
		t.Fatalf("idempotency record inside the window: %v", err)
	}
	_, oldest, _, err := s.store.WeftStreamBounds()
	if err != nil || oldest != kept {
		t.Fatalf("oldest after the prune = %d %v, want %d", oldest, err, kept)
	}

	str = openStream(t, server, fmt.Sprint(stale))
	defer str.close()
	opening := c.opening(t, str, true)
	if opening["oldest"] != float64(kept) {
		t.Fatalf("reset stream names oldest %v, want %d", opening["oldest"], kept)
	}
}

// The retention /v2/service advertises is the retention the host enforces.
func TestServiceAdvertisesTheEnforcedRetention(t *testing.T) {
	s := newStudio(t)
	limits := s.get("/v2/service")["service"].(map[string]any)["limits"].(map[string]any)
	for key, window := range map[string]time.Duration{
		"event_retention_days":       eventRetention,
		"idempotency_retention_days": idempotencyRetention,
	} {
		if limits[key] != float64(window/(24*time.Hour)) {
			t.Errorf("limits.%s = %v, enforced window is %v", key, limits[key], window)
		}
	}
}
