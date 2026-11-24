package weftremote

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/EnderRealm/ticket/v8/pkg/ticket"

	"shuttle/internal/config"
)

const imageReaderItem = "folio-image-reader-token"

// seedPictures writes tickets-attachments.json's tickets, and answers what the
// fixture lists as each one's attachments.
func (s *studio) seedPictures() map[string]any {
	s.t.Helper()
	var list struct {
		Tickets []fixtureTicket `json:"tickets"`
	}
	fixtureJSON(s.t, "tickets-attachments.json", &list)
	want := map[string]any{}
	for _, row := range decode(s.t, fixtureBytes(s.t, "tickets-attachments.json")).(map[string]any)["tickets"].([]any) {
		row := row.(map[string]any)
		want[row["id"].(string)] = row["attachments"]
	}
	for _, ft := range list.Tickets {
		updated, err := time.Parse(time.RFC3339, ft.Updated)
		if err != nil {
			s.t.Fatal(err)
		}
		tk := &ticket.Ticket{
			ID: bare(ft.ID), Title: ft.Title, Status: ticket.Status(ft.Status), Type: ticket.TicketType(ft.Type),
			Priority: *ft.Priority, Created: updated.Add(-time.Hour), Updated: updated, Body: criteriaBody(ft.CriteriaCount),
		}
		ft.attach(s.t, tk)
		writeTicketIn(s.t, s.central, "scratch", tk)
	}
	return want
}

// pictured holds every fixture ticket's listing and detail to the attachments
// the fixture lists.
func (s *studio) pictured(want map[string]any) {
	s.t.Helper()
	for id, attachments := range want {
		if got := s.row(id)["attachments"]; !reflect.DeepEqual(got, attachments) {
			s.t.Errorf("listing %s attachments = %v, want %v", id, got, attachments)
		}
		if got := s.get("/v2/tickets/" + id)["ticket"].(map[string]any)["attachments"]; !reflect.DeepEqual(got, attachments) {
			s.t.Errorf("detail %s attachments = %v, want %v", id, got, attachments)
		}
	}
}

// capture sends the service's log to what it returns.
func (s *studio) capture() *syncBuffer {
	logs := &syncBuffer{}
	s.service.log = log.New(logs, "", 0)
	s.service.segments.log = s.service.log
	return logs
}

// probed is a worker with every capability worker-version.json lists, under
// which the host serves every desktop capability.
func (s *studio) probed() {
	s.t.Helper()
	var version struct {
		Version      string   `json:"worker_version"`
		Contracts    []string `json:"contracts"`
		Capabilities []string `json:"capabilities"`
	}
	fixtureJSON(s.t, "worker-version.json", &version)
	s.service.segments.worker.Store(&workerInfo{
		Version: version.Version, Contracts: []string{ContractVersion, WorkerContractV2}, Capabilities: version.Capabilities,
	})
}

// imageReader is the capability as discovery declares it, and the host's
// conformance.
func (s *studio) imageReader() (map[string]any, string) {
	s.t.Helper()
	service := s.get("/v2/service")["service"].(map[string]any)
	listed := service["capabilities"].([]any)
	last := listed[len(listed)-1].(map[string]any)
	if last["id"] != "image_reader" {
		s.t.Fatalf("the last capability is %v", last)
	}
	return last, service["conformance"].(string)
}

func TestCatalogCarriesAttachments(t *testing.T) {
	s := newStudio(t)
	t.Setenv("HOME", s.root)
	want := s.seedPictures()
	listing := learn(decode(t, fixtureBytes(t, "tickets-attachments.json")))
	detail := learn(decode(t, fixtureBytes(t, "ticket.json")))
	listing.check(t, "tickets", s.get("/v2/tickets?project=scratch"))
	s.pictured(want)
	counts := map[int]bool{}
	for id, attachments := range want {
		counts[len(attachments.([]any))] = true
		detail.check(t, id, s.get("/v2/tickets/"+id))
	}
	if !counts[0] || !counts[1] || !counts[2] {
		t.Fatalf("tickets-attachments.json lists tickets with %v attachments", counts)
	}

	// Stored values outside §5.6's shape, which only a hand edit or a scheme
	// in capitals leaves, are left out and cost nothing else of the listing.
	const id, link = "31e15d33", "https://folio.example/api/images/31e15d33"
	shouted := ticket.Attachment{ID: id, Role: ticket.AttachmentAfter, Link: "HTTPS://folio.example/api/images/31e15d33"}
	if err := shouted.Validate(); err != nil {
		t.Fatalf("tk refuses a scheme in capitals, which the host need not leave out: %v", err)
	}
	valid := map[string]any{"id": id, "role": "before", "link": link}
	for i, c := range []struct {
		name  string
		extra map[string]string
		want  []any
	}{
		{"scheme in capitals", map[string]string{"attachment-after": shouted.ID + " " + shouted.Link}, []any{}},
		{"scheme in capitals beside a valid one", map[string]string{"attachment-before": id + " " + link, "attachment-after": shouted.ID + " " + shouted.Link}, []any{valid}},
		{"http beside a valid one", map[string]string{"attachment-before": id + " " + link, "attachment-after": id + " http://folio.example/x"}, []any{valid}},
		{"no host", map[string]string{"attachment-before": id + " https:///x"}, []any{}},
		{"no link", map[string]string{"attachment-before": id}, []any{}},
		{"no id", map[string]string{"attachment-before": " " + link}, []any{}},
		{"space in the link", map[string]string{"attachment-before": id + " " + link + " x"}, []any{}},
		{"line break in the link", map[string]string{"attachment-before": id + " " + link + "\nx"}, []any{}},
		{"non-ASCII link", map[string]string{"attachment-before": id + " https://folio.example/é"}, []any{}},
		{"unknown role", map[string]string{"attachment-during": id + " " + link}, []any{}},
		{"unknown key", map[string]string{"attachments": link}, []any{}},
	} {
		created := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
		slug := fmt.Sprintf("shape-%04d", i)
		writeTicketIn(t, s.central, "scratch", &ticket.Ticket{
			ID: slug, Title: c.name, Status: ticket.StatusReady, Type: ticket.TypeFeature, Body: criterionBody,
			Created: created, Updated: created, Extra: c.extra,
		})
		if got := s.row("scratch/" + slug)["attachments"]; !reflect.DeepEqual(got, any(c.want)) {
			t.Errorf("%s: listed %v, want %v", c.name, got, c.want)
		}
		if got := s.get("/v2/tickets/scratch/" + slug)["ticket"].(map[string]any)["attachments"]; !reflect.DeepEqual(got, any(c.want)) {
			t.Errorf("%s: detail %v, want %v", c.name, got, c.want)
		}
	}
	all := s.get("/v2/tickets?project=scratch")
	listing.check(t, "tickets", all)
	served := regexp.MustCompile(`^https://[!-~]+$`)
	for _, row := range all["tickets"].([]any) {
		for _, a := range row.(map[string]any)["attachments"].([]any) {
			if link := a.(map[string]any)["link"].(string); !served.MatchString(link) {
				t.Errorf("%s serves the link %q", row.(map[string]any)["id"], link)
			}
		}
	}
	s.pictured(want)

	// A change of a ticket's attachments is an ordinary catalog event.
	server := httptest.NewServer(s.service)
	// A cleanup, so the stream's own cleanup closes it first.
	t.Cleanup(server.Close)
	events := openStream(t, server, "")
	events.next(t)
	revision := s.revision("scratch")
	const changed = "scratch/shape-0000"
	stored := s.stored(changed)
	if err := ticket.SetAttachment(stored, ticket.Attachment{ID: id, Role: ticket.AttachmentBefore, Link: link}); err != nil {
		t.Fatal(err)
	}
	if err := s.tk().Update(stored); err != nil {
		t.Fatal(err)
	}
	s.sync()
	if event := nextCatalog(t, events); event["project"] != "scratch" || int64(event["catalog_revision"].(float64)) <= revision {
		t.Errorf("catalog event %v after revision %d", event, revision)
	}
	if got := s.row(changed)["attachments"]; !reflect.DeepEqual(got, any([]any{valid})) {
		t.Errorf("%s after its attachment = %v", changed, got)
	}
}

func TestImageReaderTokenServedToAuthenticatedClient(t *testing.T) {
	s := newStudio(t)
	t.Setenv("HOME", s.root)
	logs := s.capture()
	want := s.seedPictures()
	s.sync()
	var fixture map[string]any
	fixtureJSON(t, "image-reader.json", &fixture)
	reader := fixture["image_reader"].(map[string]any)
	first, rotated := reader["token"].(string), "example-rotated-token-not-a-secret"
	s.cfg.Weft.ImageReader = config.ImageReader{Origin: reader["origin"].(string), KeychainItem: imageReaderItem}
	token, reads := first, 0
	s.service.segments.findKey = func(item string) (string, error) {
		if item != imageReaderItem {
			t.Errorf("read keychain item %q", item)
		}
		reads++
		return token, nil
	}
	seq := s.asOf()

	for _, auth := range []string{"", "Bearer wrong-token", "Bearer " + first, "Basic secret-token"} {
		req := httptest.NewRequest(http.MethodGet, "/v2/image-reader", nil)
		if auth != "" {
			req.Header.Set("Authorization", auth)
		}
		rec := httptest.NewRecorder()
		s.service.ServeHTTP(rec, req)
		body := decode(t, rec.Body.Bytes())
		errorShape(t).check(t, "unauthorized", body)
		if rec.Code != http.StatusUnauthorized || body.(map[string]any)["error"].(map[string]any)["code"] != "unauthorized" ||
			strings.Contains(rec.Body.String(), first) || reads != 0 {
			t.Fatalf("auth %q = %d %s after %d keychain reads", auth, rec.Code, rec.Body.String(), reads)
		}
	}

	if declared, _ := s.imageReader(); declared["state"] != "served" || declared["reason"] != nil {
		t.Errorf("discovery declares %v", declared)
	}
	read := func(want string) {
		t.Helper()
		rec := s.request(t, http.MethodGet, "/v2/image-reader", "", "")
		if rec.Code != http.StatusOK || rec.Header().Get("Cache-Control") != "no-store" {
			t.Fatalf("GET /v2/image-reader = %d %v %s", rec.Code, rec.Header(), rec.Body.String())
		}
		got := decode(t, rec.Body.Bytes()).(map[string]any)
		learn(decode(t, fixtureBytes(t, "image-reader.json"))).check(t, "image-reader", got)
		if got["contract_version"] != fixture["contract_version"] || got["host_id"] != s.service.hostID || got["as_of"] != float64(seq) ||
			!reflect.DeepEqual(got["image_reader"], map[string]any{"origin": reader["origin"], "token": want}) {
			t.Fatalf("image reader = %s", rec.Body.String())
		}
	}
	read(first)
	token = rotated
	read(rotated)
	if s.asOf() != seq {
		t.Fatalf("serving and rotating the token moved the stream from %d to %d", seq, s.asOf())
	}

	// No other request reads the Keychain, another capability's refusal included.
	held := reads
	s.pictured(want)
	s.errorBody(http.MethodGet, "/v2/repositories", http.StatusNotImplemented, "capability_not_served")
	if reads != held {
		t.Errorf("requests with nothing to do with image_reader read the keychain %d times", reads-held)
	}

	// The tokens are in those responses and nowhere else: no other read, no
	// event, and neither the ledger, which replays requests and events, nor
	// the log of a host that committed and logged a write since.
	nowhere := func(where, text string) {
		t.Helper()
		for _, secret := range []string{first, rotated} {
			if strings.Contains(text, secret) {
				t.Errorf("%s carries the token", where)
			}
		}
	}
	server := httptest.NewServer(s.service)
	t.Cleanup(server.Close)
	const edited = "scratch/example-rename-3e1a"
	if r := s.edit(edited, "after-token", editBody(nil, 1, "ready", 2)); r.code != http.StatusOK {
		t.Fatalf("edit = %d %s", r.code, r.raw)
	}
	posted := s.request(t, http.MethodPost, "/v2/image-reader", "post-token", "{}")
	if posted.Code != http.StatusNotFound {
		t.Errorf("POST /v2/image-reader = %d %s", posted.Code, posted.Body.String())
	}
	nowhere("POST /v2/image-reader", posted.Body.String())
	for _, path := range []string{"/v2/service", "/v2/projects", "/v2/tickets?project=scratch", "/v2/tickets/" + edited,
		"/v2/queue", "/v2/runs", "/v2/questions", "/v2/frontier?project=scratch"} {
		doc, _ := json.Marshal(s.get(path))
		nowhere("GET "+path, string(doc))
	}
	events, end := openStream(t, server, "0"), s.asOf()
	for seen := int64(0); seen < end; {
		ev := events.next(t)
		nowhere("event "+ev.name, ev.data)
		if ev.id != "" {
			seen = int64(decode(t, []byte(ev.id)).(float64))
		}
	}
	for _, file := range []string{"runs.db", "runs.db-wal"} {
		doc, err := os.ReadFile(filepath.Join(s.root, file))
		if err != nil && !os.IsNotExist(err) {
			t.Fatal(err)
		}
		nowhere(file, string(doc))
	}
	if logs.String() == "" {
		t.Error("the host logged nothing, so its log shows nothing")
	}
	nowhere("log", logs.String())
}

func TestImageReaderTokenAbsentIsNotServed(t *testing.T) {
	s := newStudio(t)
	logs := s.capture()
	s.probed()
	want := s.seedPictures()
	s.service.segments.findKey = func(item string) (string, error) {
		t.Errorf("read keychain item %q with none configured", item)
		return "", nil
	}

	var discovery, refusal map[string]any
	fixtureJSON(t, "service-image-reader-not-served.json", &discovery)
	fixtureJSON(t, "error-image-reader-not-served.json", &refusal)
	service, fixture := s.get("/v2/service")["service"].(map[string]any), discovery["service"].(map[string]any)
	for _, key := range []string{"conformance", "capabilities"} {
		if !reflect.DeepEqual(service[key], fixture[key]) {
			t.Errorf("service.%s = %v, service-image-reader-not-served.json has %v", key, service[key], fixture[key])
		}
	}
	refused := func() string {
		t.Helper()
		rec := s.request(t, http.MethodGet, "/v2/image-reader", "", "")
		doc := decode(t, rec.Body.Bytes()).(map[string]any)
		learn(decode(t, fixtureBytes(t, "error-image-reader-not-served.json"))).check(t, "not served", doc)
		if rec.Code != http.StatusNotImplemented || doc["error"].(map[string]any)["code"] != "capability_not_served" {
			t.Fatalf("GET /v2/image-reader = %d %s", rec.Code, rec.Body.String())
		}
		return doc["error"].(map[string]any)["message"].(string)
	}
	message := strings.ReplaceAll(refusal["error"].(map[string]any)["message"].(string), "h-studio-7c41e09a", s.service.hostID)
	if got := refused(); got != message {
		t.Errorf("message %q, want %q", got, message)
	}
	s.pictured(want)

	// A configured item that yields no token a client could send is not served
	// either, with a reason that names the item and never the value.
	prefix := strings.TrimSuffix(message, imageReaderUnconfigured)
	s.cfg.Weft.ImageReader = config.ImageReader{Origin: "https://folio.example", KeychainItem: imageReaderItem}
	for _, c := range []struct {
		name, value string
		err         error
		says        string
	}{
		{"missing item", "", fmt.Errorf("keychain item %q: exit status 44", imageReaderItem), "exit status 44"},
		{"empty", "", nil, "visible ASCII"},
		{"space", "unsendable value", nil, "visible ASCII"},
		{"line break", "unsendable\nvalue", nil, "visible ASCII"},
		{"trailing line break", "unsendable-value\n", nil, "visible ASCII"},
		{"delete", "unsendable\x7fvalue", nil, "visible ASCII"},
		{"non-ASCII", "unsendablé-value", nil, "visible ASCII"},
	} {
		s.service.segments.findKey = func(string) (string, error) {
			// Held over a Keychain prompt, the state lock would stop the host.
			if !s.service.stateMu.TryLock() {
				t.Errorf("%s: the keychain is read under the state lock", c.name)
			} else {
				s.service.stateMu.Unlock()
			}
			return c.value, c.err
		}
		declared, conformance := s.imageReader()
		reason, _ := declared["reason"].(string)
		if declared["state"] != "not_served" || conformance != "complete" ||
			!strings.Contains(reason, imageReaderItem) || !strings.Contains(reason, c.says) || strings.Contains(reason, "unsendabl") {
			t.Errorf("%s: discovery declares %v under %s conformance", c.name, declared, conformance)
		}
		if got := refused(); got != prefix+reason {
			t.Errorf("%s: message %q, want %q", c.name, got, prefix+reason)
		}
		s.pictured(want)
	}
	if strings.Contains(logs.String(), "unsendabl") {
		t.Errorf("log carries the item's value:\n%s", logs.String())
	}
}
