package weftremote

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"shuttle/internal/ledger"
	"shuttle/internal/version"
)

// ServiceVersion is the client ↔ host contract this file serves.
const ServiceVersion = ledger.WeftServiceVersion

// capability is one row of weft-service/v2 §2.3, as this host build declares
// it: served, or not served with the ticket that adds it.
type capability struct {
	ID     string
	Reason string
}

// capabilities mirrors docs/service-v2/service-developmental.json, the first
// developmental slice (§2.4). Its order is the contract's.
var capabilities = []capability{
	{ID: "dashboard"},
	{ID: "catalog"},
	{ID: "launch"},
	{ID: "frontier"},
	{ID: "queue"},
	{ID: "questions"},
	{ID: "control"},
	{ID: "output", Reason: "not served by this host build: stage output and liveness arrive with shuttle/serve-hosted-run-6a0b and weft/stream-hosted-output-f94e"},
	{ID: "history"},
	{ID: "landing", Reason: "not served by this host build: runs end with their branch unlanded until shuttle/reconcile-hosted-landing-5675 and weft/expose-recoverable-hosted-b935"},
	{ID: "cleanup", Reason: "not served by this host build: dismiss and fresh work arrive with shuttle/reconcile-hosted-landing-5675; until then a run refused prior_run or stale_base stays retained and its ticket cannot be relaunched through the service"},
	{ID: "terminals", Reason: "not served by this host build: hosted terminals arrive with shuttle/host-terminal-sessions-bbbe; until then a stale branch cannot be resolved by hand through the service"},
	{ID: "folders", Reason: "not served by this host build: the host folder picker arrives with shuttle/host-terminal-sessions-bbbe"},
	{ID: "archive", Reason: "not served by this host build: legacy archive import arrives with weft/migrate-local-history-7d8d"},
	{ID: "trial_launch", Reason: "not served by this host build: checkout launches arrive with warp/host-weft-worker-cd59"},
}

// notServedRoutes maps each route of a not-served capability (§3, §4.3) to
// its capability: every one answers 501 whoever calls it.
var notServedRoutes = []struct {
	method     string
	pattern    *regexp.Regexp
	capability string
}{
	{http.MethodGet, regexp.MustCompile(`^/v2/runs/[^/]+/output(/stream)?$`), "output"},
	{http.MethodGet, regexp.MustCompile(`^/v2/repositories$`), "landing"},
	{http.MethodPost, regexp.MustCompile(`^/v2/repositories/[^/]+/continue$`), "landing"},
	{http.MethodPost, regexp.MustCompile(`^/v2/runs/[^/]+/reconcile$`), "landing"},
	{http.MethodPost, regexp.MustCompile(`^/v2/runs/[^/]+/(dismiss|fresh)$`), "cleanup"},
	{http.MethodGet, regexp.MustCompile(`^/v2/terminals(/[^/]+(/attach)?)?$`), "terminals"},
	{http.MethodPost, regexp.MustCompile(`^/v2/terminals(/[^/]+/close)?$`), "terminals"},
	{http.MethodGet, regexp.MustCompile(`^/v2/folders$`), "folders"},
	{http.MethodPost, regexp.MustCompile(`^/v2/archives$`), "archive"},
}

// Each route answers its version's errors; §2.5 names the versions this host
// speaks in every upgrade_required.
var spokenVersions = []string{ContractVersion, ServiceVersion}

var versionSegment = regexp.MustCompile(`^v[0-9]+$`)

// unspokenVersion reports whether a first path segment names a contract
// version this host does not speak.
func unspokenVersion(segment string) bool {
	return versionSegment.MatchString(segment) && segment != "v1" && segment != "v2"
}

func (s *Service) serveV2(w http.ResponseWriter, r *http.Request, path string) {
	for _, route := range notServedRoutes {
		if r.Method == route.method && route.pattern.MatchString(path) {
			s.capabilityNotServed(w, route.capability)
			return
		}
	}
	switch {
	case r.Method == http.MethodGet && path == "/v2/service":
		s.handleService(w)
	case r.Method == http.MethodGet && path == "/v2/dashboard":
		s.handleDashboard(w)
	case r.Method == http.MethodGet && path == "/v2/projects":
		s.handleProjects(w)
	case r.Method == http.MethodGet && path == "/v2/tickets":
		s.handleCatalogTickets(w, r.URL.Query().Get("project"))
	case r.Method == http.MethodGet && strings.HasPrefix(path, "/v2/tickets/"):
		s.handleCatalogTicket(w, strings.TrimPrefix(path, "/v2/tickets/"))
	case r.Method == http.MethodGet && path == "/v2/runs":
		s.handleV2Runs(w, r)
	case r.Method == http.MethodGet && strings.HasPrefix(path, "/v2/runs/"):
		s.handleV2RunPath(w, strings.TrimPrefix(path, "/v2/runs/"))
	case r.Method == http.MethodPost && runCommand.MatchString(path):
		m := runCommand.FindStringSubmatch(path)
		s.handleRunCommand(w, r, m[1], m[2])
	case r.Method == http.MethodPost && path == "/v2/launches":
		s.handleLaunch(w, r)
	case r.Method == http.MethodGet && path == "/v2/frontier":
		s.handleFrontier(w, r.URL.Query().Get("project"))
	case r.Method == http.MethodPost && path == "/v2/frontier":
		s.handleFrontierSubmit(w, r)
	case r.Method == http.MethodGet && path == "/v2/queue":
		s.handleQueue(w)
	case r.Method == http.MethodPost && path == "/v2/queue/clear":
		s.handleClear(w, r)
	case r.Method == http.MethodPut && path == "/v2/queue/capacity":
		s.handleCapacity(w, r)
	case r.Method == http.MethodPost && entryCancel.MatchString(path):
		s.handleCancel(w, r, entryCancel.FindStringSubmatch(path)[1])
	case r.Method == http.MethodGet && path == "/v2/questions":
		s.handleV2Questions(w)
	case r.Method == http.MethodGet && strings.HasPrefix(path, "/v2/questions/"):
		s.handleV2Question(w, strings.TrimPrefix(path, "/v2/questions/"))
	case r.Method == http.MethodGet && path == "/v2/events":
		s.handleV2Events(w, r)
	default:
		s.v2Error(w, http.StatusNotFound, "malformed", r.Method+" "+path+" is not a weft-service/v2 route on host "+s.hostID)
	}
}

func (s *Service) v2Error(w http.ResponseWriter, status int, code, message string) {
	s.writeJSON(w, status, map[string]any{
		"contract_version": ServiceVersion, "error": map[string]any{"code": code, "message": message},
	})
}

func (s *Service) v2Internal(w http.ResponseWriter, err error) {
	s.log.Printf("weft service host=%s: %v", s.hostID, err)
	s.v2Error(w, http.StatusInternalServerError, "malformed", "internal service error on host "+s.hostID)
}

// upgradeRequired is §2.5's refusal of a path naming a version this host does
// not speak, answered in the v2 error shape a client reading it will speak.
func (s *Service) upgradeRequired(w http.ResponseWriter, segment string) {
	s.v2Error(w, http.StatusUpgradeRequired, "upgrade_required", upgradeMessage("weft-service/"+segment))
}

func upgradeMessage(version string) string {
	return fmt.Sprintf("%s is not spoken here: this host speaks %s, and GET /v2/service lists what it serves",
		version, strings.Join(spokenVersions, " and "))
}

func (s *Service) capabilityNotServed(w http.ResponseWriter, id string) {
	message, ok := s.notServedMessage(id)
	if !ok {
		s.v2Internal(w, fmt.Errorf("route names undeclared capability %s", id))
		return
	}
	s.v2Error(w, http.StatusNotImplemented, "capability_not_served", message)
}

// notServedMessage is error-capability-not-served.json's message for id.
func (s *Service) notServedMessage(id string) (string, bool) {
	for _, c := range capabilities {
		if c.ID == id {
			return fmt.Sprintf("%s is not served by host %s, which declares %s conformance: %s",
				id, s.hostID, conformance(), c.Reason), true
		}
	}
	return "", false
}

// conformance is complete only when every desktop capability — all but
// trial_launch — is served (§2.3).
func conformance() string {
	for _, c := range capabilities {
		if c.ID != "trial_launch" && c.Reason != "" {
			return "developmental"
		}
	}
	return "complete"
}

// read writes a v2 read: every one carries host_id and the state-stream seq
// it reflects (§4.2).
func (s *Service) read(w http.ResponseWriter, asOf int64, fields map[string]any) {
	fields["contract_version"] = ServiceVersion
	fields["host_id"] = s.hostID
	fields["as_of"] = asOf
	s.writeJSON(w, http.StatusOK, fields)
}

func (s *Service) handleService(w http.ResponseWriter) {
	s.stateMu.RLock()
	asOf, err := s.store.WeftAsOf()
	s.stateMu.RUnlock()
	if err != nil {
		s.v2Internal(w, err)
		return
	}
	listed := make([]map[string]any, 0, len(capabilities))
	for _, c := range capabilities {
		state, reason := "served", any(nil)
		if c.Reason != "" {
			state, reason = "not_served", c.Reason
		}
		listed = append(listed, map[string]any{"id": c.ID, "state": state, "reason": reason})
	}
	hostName, err := os.Hostname()
	if err != nil {
		s.v2Internal(w, err)
		return
	}
	s.read(w, asOf, map[string]any{"service": map[string]any{
		"host_name":        hostName,
		"host_build":       "shuttle " + version.String(),
		"service_versions": spokenVersions,
		// The developmental slice's worker contracts, as service-developmental.json
		// declares them.
		"worker_versions": []string{ContractVersion, "weft-worker/v2"},
		"conformance":     conformance(),
		"capabilities":    listed,
		"limits": map[string]any{
			"capacity_default": ledger.WeftCapacityDefault, "segment_limit": 8,
			"idempotency_retention_days": 7, "event_retention_days": 7, "settled_entry_hours": 12,
			"output_retention_bytes": 256 << 10, "terminal_retention_bytes": 4 << 20,
			"terminal_scrollback_lines": 10000, "answer_max_bytes": maxAnswerBytes,
			"folder_handle_seconds": 900,
		},
	}})
}

func (s *Service) handleDashboard(w http.ResponseWriter) {
	s.stateMu.RLock()
	counts, err := s.store.WeftDashboardCounts()
	var agent *ledger.Agent
	if err == nil {
		agent, err = s.store.Agent(ledger.WeftAgent)
	}
	var asOf int64
	if err == nil {
		asOf, err = s.store.WeftAsOf()
	}
	s.stateMu.RUnlock()
	if err != nil {
		s.v2Internal(w, err)
		return
	}
	host := map[string]any{"dispatch": "open", "reason": nil}
	if agent.Paused {
		host = map[string]any{"dispatch": "paused", "reason": ledger.WeftHostPausedDetail(agent.PausedReason.String, agent.PausedAt.String)}
	}
	// No tile here carries a figure: an unknown allowance rendered as zero is
	// a claim nobody made (§5.3), and this build reads no allowance source.
	allowance := []map[string]any{
		unavailableTile("claude", "no source: this host build does not relay the workers' Claude rate_limit_event observations; they arrive with shuttle/serve-hosted-run-6a0b"),
		unavailableTile("codex", "no source: this host build runs no Codex allowance probe; it arrives with shuttle/serve-hosted-run-6a0b"),
		unavailableTile("cursor", "no source: no credential decision has been made for Cursor on this host"),
	}
	s.read(w, asOf, map[string]any{"dashboard": map[string]any{
		"host":      host,
		"allowance": allowance,
		"capacity":  map[string]any{"cap": counts.Capacity, "running": counts.Running, "waiting": counts.Waiting},
		"runs": map[string]any{
			"active": counts.Active, "blocked": counts.Blocked, "action_required": counts.ActionRequired,
		},
		"questions_pending":   counts.QuestionsPending,
		"repositories_paused": counts.RepositoriesPaused,
		"terminals_open":      counts.TerminalsOpen,
	}})
}

func unavailableTile(provider, reason string) map[string]any {
	return map[string]any{
		"provider": provider, "state": "unavailable", "source": nil, "observed_at": nil,
		"windows": []any{}, "reason": reason,
	}
}

// handleV2Events is the state stream (§10). Its first event is stream, with
// no id: the seq this connection's events follow, the oldest v2 event
// retained, and whether the client's Last-Event-ID was older than that, in
// which case it rebuilds from the GETs at seq.
func (s *Service) handleV2Events(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		s.v2Internal(w, fmt.Errorf("response does not support streaming"))
		return
	}
	s.stateMu.RLock()
	floor, oldest, newest, err := s.store.WeftStreamBounds()
	s.stateMu.RUnlock()
	if err != nil {
		s.v2Internal(w, err)
		return
	}
	last, reset := newest, false
	if value := r.Header.Get("Last-Event-ID"); value != "" {
		cursor, err := strconv.ParseInt(value, 10, 64)
		if err != nil || cursor < 0 {
			s.v2Error(w, http.StatusBadRequest, "malformed", "Last-Event-ID must be a non-negative integer")
			return
		}
		// Every event after cursor must still be retained, and a v2 one:
		// a cursor before the oldest (retention), at or before the v1 floor
		// (a seq a v1 client was handed; §14.2), or past newest (a seq this
		// ledger never allocated) cannot be continued. floor > 0 keeps a
		// fresh ledger's cursor 0 continuable.
		if cursor < oldest-1 || (floor > 0 && cursor <= floor) || cursor > newest {
			reset = true
		} else {
			last = cursor
		}
	}
	opening, err := json.Marshal(map[string]any{
		"contract_version": ServiceVersion, "host_id": s.hostID, "seq": last, "oldest": oldest, "reset": reset,
	})
	if err != nil {
		s.v2Internal(w, err)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	if _, err := fmt.Fprintf(w, "event: stream\ndata: %s\n\n", opening); err != nil {
		return
	}
	flusher.Flush()
	poll := time.NewTicker(250 * time.Millisecond)
	defer poll.Stop()
	keepAlive := time.NewTicker(15 * time.Second)
	defer keepAlive.Stop()
	for {
		s.stateMu.RLock()
		events, err := s.store.WeftEventsAfter(last)
		s.stateMu.RUnlock()
		if err != nil {
			s.log.Printf("weft service host=%s: state stream after %d: %v", s.hostID, last, err)
			return
		}
		for _, event := range events {
			if _, err := fmt.Fprintf(w, "id: %d\nevent: %s\ndata: %s\n\n", event.Seq, event.Type, event.Payload); err != nil {
				return
			}
			last = event.Seq
			flusher.Flush()
		}
		select {
		case <-r.Context().Done():
			return
		case <-poll.C:
		case <-keepAlive.C:
			if _, err := io.WriteString(w, ": keep-alive\n\n"); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

// watchCatalog rereads tk on an interval so a change made outside Shuttle —
// a ticket edited, a project registered or rebound — becomes a catalog event
// with no client asking (§5.2). Catalog reads run the same sync, so a read
// never reports a revision the stream has not carried.
func (s *Service) watchCatalog(ctx context.Context) {
	ticker := time.NewTicker(s.catalogPoll)
	defer ticker.Stop()
	var reported string
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		c, err := s.syncCatalog()
		// Each pass also dispatches: it is how a pause lifted from another
		// process — shuttle resume weft-worker — reaches the queue.
		if err == nil {
			s.dispatchWith(c)
		}
		// A refusal persists until tk changes; it is logged when it changes,
		// not on every poll.
		message := ""
		if err != nil {
			message = err.Error()
		}
		if message != reported {
			if err != nil {
				s.log.Printf("weft catalog host=%s: %v", s.hostID, err)
			} else {
				s.log.Printf("weft catalog host=%s: tk read complete again", s.hostID)
			}
			reported = message
		}
	}
}
