package weftremote

import (
	"bytes"
	"context"
	"crypto/subtle"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/EnderRealm/ticket/v8/pkg/ticket"

	"shuttle/internal/config"
	"shuttle/internal/ledger"
)

const (
	maxRequestBytes = 70 << 10
	maxAnswerBytes  = 64 << 10
	// eventRetention is how long state events and an ended run's output are
	// kept (weft-service/v2 §8.1, §10), and idempotencyRetention how long an
	// idempotency record is (§4.2). /v2/service advertises both.
	eventRetention       = 7 * 24 * time.Hour
	idempotencyRetention = 7 * 24 * time.Hour
)

// Service is the authenticated weft host: weft-service/v2, and weft-remote/v1
// in the route mode of weft-service/v2 §14.2.
type Service struct {
	cfg      *config.Config
	store    *ledger.Store
	token    string
	tokenID  string
	hostID   string
	home     string
	log      *log.Logger
	segments *Supervisor
	mutateMu sync.Mutex
	stateMu  sync.RWMutex
	// catalogPoll is how often the host rereads tk for changes made outside
	// Shuttle (§5.2). A full read of the Studio's store costs ~150ms, so the
	// interval trades an external edit's latency against a daemon's idle CPU.
	catalogPoll time.Duration
	// prunePoll is how often the host enforces the retention windows. Neither
	// weft_events.ts nor weft_requests.created_at is indexed, so each pass
	// scans both tables; windows measured in days need nothing finer than
	// an hour.
	prunePoll time.Duration
	// started is when this host process started, which the allowance tiles
	// name while nothing has been observed.
	started time.Time
	codex   codexProbe
	// probes counts the provider probes in flight.
	probes sync.WaitGroup
	// folders are the folder handles this process minted (§9.2), and terms
	// the terminal sessions it owns (§9).
	folders *folderHandles
	termMu  sync.Mutex
	terms   map[string]*terminalSession
}

func New(cfg *config.Config, store *ledger.Store, token, home string, logger *log.Logger) (*Service, error) {
	if cfg == nil || store == nil {
		return nil, fmt.Errorf("weft service requires config and ledger")
	}
	if token == "" {
		return nil, fmt.Errorf("weft bearer token is empty")
	}
	if home == "" {
		return nil, fmt.Errorf("shuttle home is empty")
	}
	if !filepath.IsAbs(home) || home != filepath.Clean(home) {
		return nil, fmt.Errorf("shuttle home %q is not a clean absolute path", home)
	}
	if cfg.Weft.SegmentTimeoutSeconds <= 0 {
		return nil, fmt.Errorf("weft.segment_timeout_seconds must be positive")
	}
	for name, path := range map[string]string{
		"weft.worker_bin": cfg.Weft.WorkerBin, "weft.weft_root": cfg.Weft.Root,
		"paths.srt_bin": cfg.Paths.SRTBin, "paths.tk_config": cfg.Paths.TKConfig,
	} {
		if !filepath.IsAbs(path) || path != filepath.Clean(path) {
			return nil, fmt.Errorf("%s %q is not a clean absolute path", name, path)
		}
	}
	for _, path := range cfg.Weft.Sandbox.AllowWrite {
		if !filepath.IsAbs(path) || path != filepath.Clean(path) {
			return nil, fmt.Errorf("weft.sandbox.allow_write %q is not a clean absolute path", path)
		}
	}
	for _, domain := range cfg.Weft.Sandbox.AllowedDomains {
		if domain == "" || domain == "*" || strings.ContainsAny(domain, " \t\r\n") {
			return nil, fmt.Errorf("weft.sandbox.allowed_domains contains invalid domain %q", domain)
		}
	}
	if logger == nil {
		logger = log.Default()
	}
	hostID, err := store.WeftHostID()
	if err != nil {
		return nil, fmt.Errorf("weft host_id: %w", err)
	}
	s := &Service{
		cfg: cfg, store: store, token: token, tokenID: ledger.TokenID(token), hostID: hostID,
		home: home, log: logger, catalogPoll: 5 * time.Second, prunePoll: time.Hour, started: time.Now(),
		folders: newFolderHandles(), terms: map[string]*terminalSession{},
	}
	// Dispatch spawns segments through the supervisor, under the contract the
	// worker probe found; each settled segment may free the slot and
	// reservation a waiting entry needs.
	s.segments = NewSupervisor(cfg, store, home, logger, &s.stateMu)
	s.segments.settled = s.dispatch
	s.segments.hostID = hostID
	return s, nil
}

// ValidateListen refuses public and non-tailnet listener addresses.
func ValidateListen(address string) error {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("weft.listen: %w", err)
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return fmt.Errorf("weft.listen: %q is not an IP address", host)
	}
	v4 := ip.To4()
	if v4 == nil {
		return fmt.Errorf("weft.listen: %q is outside the supported private ranges", host)
	}
	loopback := v4[0] == 127
	tailnet := v4[0] == 100 && v4[1]&0xc0 == 0x40
	if !loopback && !tailnet {
		return fmt.Errorf("weft.listen: %q is outside 100.64.0.0/10 and 127.0.0.0/8", host)
	}
	return nil
}

// ListenAndServe binds the configured private endpoint until ctx is cancelled.
func (s *Service) ListenAndServe(ctx context.Context) error {
	if err := ValidateListen(s.cfg.Weft.Listen); err != nil {
		return err
	}
	listener, err := net.Listen("tcp", s.cfg.Weft.Listen)
	if err != nil {
		return err
	}
	go s.watchCatalog(ctx)
	go s.watchRetention(ctx)
	// Waiting entries survive a restart (§12): dispatch resumes once the
	// restart pass and Restart have run.
	go s.dispatch()
	server := &http.Server{Handler: s, ReadHeaderTimeout: 10 * time.Second}
	stopped := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_ = server.Shutdown(shutdown)
		case <-stopped:
		}
	}()
	err = server.Serve(listener)
	close(stopped)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimSuffix(r.URL.Path, "/")
	version, _, _ := strings.Cut(strings.TrimPrefix(path, "/"), "/")
	if !s.authorized(r) {
		if version == "v1" {
			s.writeError(w, http.StatusUnauthorized, "unauthorized", "a bearer token is required")
		} else {
			s.v2Error(w, http.StatusUnauthorized, "unauthorized", "a bearer token is required")
		}
		return
	}
	switch {
	case version == "v2":
		s.serveV2(w, r, path)
		return
	case version != "v1" && unspokenVersion(version):
		s.upgradeRequired(w, version)
		return
	}
	if r.Method == http.MethodGet && path != "/v1/events" {
		s.stateMu.RLock()
		defer s.stateMu.RUnlock()
	}
	switch {
	case r.Method == http.MethodGet && path == "/v1/tickets":
		s.handleTickets(w)
	case r.Method == http.MethodGet && strings.HasPrefix(path, "/v1/tickets/"):
		s.handleTicket(w, strings.TrimPrefix(path, "/v1/tickets/"))
	case r.Method == http.MethodPost && path == "/v1/runs":
		s.handleV1Mutation(w, r)
	case r.Method == http.MethodGet && path == "/v1/runs":
		s.handleRuns(w, r)
	case r.Method == http.MethodGet && path == "/v1/questions":
		s.handleQuestions(w)
	case r.Method == http.MethodGet && strings.HasPrefix(path, "/v1/questions/"):
		s.handleQuestion(w, strings.TrimPrefix(path, "/v1/questions/"))
	case r.Method == http.MethodGet && path == "/v1/events":
		s.handleEvents(w, r)
	case strings.HasPrefix(path, "/v1/runs/"):
		s.handleRunPath(w, r, strings.TrimPrefix(path, "/v1/runs/"))
	default:
		s.writeError(w, http.StatusNotFound, "run_not_found", "resource not found")
	}
}

func (s *Service) authorized(r *http.Request) bool {
	const prefix = "Bearer "
	value := r.Header.Get("Authorization")
	if !strings.HasPrefix(value, prefix) {
		return false
	}
	got := []byte(strings.TrimPrefix(value, prefix))
	want := []byte(s.token)
	return len(got) == len(want) && subtle.ConstantTimeCompare(got, want) == 1
}

func (s *Service) handleTickets(w http.ResponseWriter) {
	catalog, all, byID, ok := s.catalog(w)
	_ = catalog
	if !ok {
		return
	}
	runs, err := s.store.WeftRuns("", "")
	if err != nil {
		s.internal(w, err)
		return
	}
	latest := make(map[string]*ledger.WeftRun)
	for _, run := range runs {
		if latest[run.Ticket] == nil {
			latest[run.Ticket] = run
		}
	}
	items := []map[string]any{}
	for _, t := range all {
		if yes, _ := launchable(t, byID); yes {
			items = append(items, ticketListItem(t, latest[t.ID]))
		}
	}
	asOf, err := s.store.WeftAsOf()
	if err != nil {
		s.internal(w, err)
		return
	}
	s.writeJSON(w, http.StatusOK, map[string]any{
		"contract_version": ContractVersion, "as_of": asOf, "tickets": items,
	})
}

func (s *Service) handleTicket(w http.ResponseWriter, id string) {
	if strings.Count(id, "/") != 1 {
		s.writeError(w, http.StatusNotFound, "ticket_not_found", id+" was not found")
		return
	}
	catalog, all, byID, ok := s.catalog(w)
	if !ok {
		return
	}
	t, err := catalog.get(id)
	if err != nil {
		s.writeError(w, http.StatusNotFound, "ticket_not_found", id+" was not found")
		return
	}
	_ = all
	runs, err := s.store.WeftRuns(id, "")
	if err != nil {
		s.internal(w, err)
		return
	}
	var latest *ledger.WeftRun
	if len(runs) > 0 {
		latest = runs[0]
	}
	asOf, err := s.store.WeftAsOf()
	if err != nil {
		s.internal(w, err)
		return
	}
	s.writeJSON(w, http.StatusOK, map[string]any{
		"contract_version": ContractVersion, "as_of": asOf,
		"ticket": ticketDetail(t, byID, latest),
	})
}

func (s *Service) catalog(w http.ResponseWriter) (*ticketCatalog, []*ticket.Ticket, map[string]*ticket.Ticket, bool) {
	catalog, err := loadTickets(s.cfg.Paths.TKConfig)
	if err != nil {
		s.internal(w, err)
		return nil, nil, nil, false
	}
	all, err := catalog.all()
	if err != nil {
		s.internal(w, err)
		return nil, nil, nil, false
	}
	byID := make(map[string]*ticket.Ticket, len(all))
	for _, t := range all {
		byID[t.ID] = t
	}
	return catalog, all, byID, true
}

func (s *Service) handleRuns(w http.ResponseWriter, r *http.Request) {
	state := r.URL.Query().Get("state")
	if state != "" && state != "active" && state != "blocked" && state != "over" {
		s.writeError(w, http.StatusBadRequest, "malformed", "state must be active, blocked or over")
		return
	}
	runs, err := s.store.WeftRuns(r.URL.Query().Get("ticket"), state)
	if err != nil {
		s.internal(w, err)
		return
	}
	items := make([]map[string]any, 0, len(runs))
	for _, run := range runs {
		items = append(items, runListItem(run))
	}
	asOf, err := s.store.WeftAsOf()
	if err != nil {
		s.internal(w, err)
		return
	}
	s.writeJSON(w, http.StatusOK, map[string]any{
		"contract_version": ContractVersion, "as_of": asOf, "runs": items,
	})
}

func (s *Service) handleRunPath(w http.ResponseWriter, r *http.Request, rest string) {
	parts := strings.Split(rest, "/")
	runID := parts[0]
	if !validProtocolID(runID) {
		s.writeError(w, http.StatusNotFound, "run_not_found", runID+" was not found")
		return
	}
	if len(parts) == 1 && r.Method == http.MethodGet {
		run, err := s.store.GetWeftRun(runID)
		if err != nil {
			s.writeError(w, http.StatusNotFound, "run_not_found", runID+" was not found")
			return
		}
		resource, err := runResource(run, s.store)
		if err != nil {
			s.internal(w, err)
			return
		}
		asOf, err := s.store.WeftAsOf()
		if err != nil {
			s.internal(w, err)
			return
		}
		s.writeJSON(w, http.StatusOK, map[string]any{"contract_version": ContractVersion, "as_of": asOf, "run": resource})
		return
	}
	if len(parts) != 2 {
		s.writeError(w, http.StatusNotFound, "run_not_found", runID+" was not found")
		return
	}
	switch {
	case r.Method == http.MethodGet && parts[1] == "result":
		run, err := s.store.GetWeftRun(runID)
		if err != nil {
			s.writeError(w, http.StatusNotFound, "run_not_found", runID+" was not found")
		} else if run.Phase != ledger.WeftPhaseEnded {
			s.writeError(w, http.StatusConflict, "run_not_over", runID+" is not over")
		} else {
			asOf, err := s.store.WeftAsOf()
			if err != nil {
				s.internal(w, err)
				return
			}
			s.writeJSON(w, http.StatusOK, map[string]any{"contract_version": ContractVersion, "as_of": asOf, "result": resultResource(run)})
		}
	case r.Method == http.MethodPost && oneOf(parts[1], "answer", "stop", "resume"):
		s.handleV1Mutation(w, r)
	default:
		s.writeError(w, http.StatusNotFound, "run_not_found", runID+" was not found")
	}
}

func (s *Service) handleQuestions(w http.ResponseWriter) {
	questions, err := s.store.PendingWeftQuestions()
	if err != nil {
		s.internal(w, err)
		return
	}
	items := make([]map[string]any, 0, len(questions))
	for _, question := range questions {
		items = append(items, questionResource(question))
	}
	asOf, err := s.store.WeftAsOf()
	if err != nil {
		s.internal(w, err)
		return
	}
	s.writeJSON(w, http.StatusOK, map[string]any{
		"contract_version": ContractVersion, "as_of": asOf, "questions": items,
	})
}

func (s *Service) handleQuestion(w http.ResponseWriter, id string) {
	if !validProtocolID(id) {
		s.writeError(w, http.StatusNotFound, "question_not_found", id+" was not found")
		return
	}
	q, err := s.store.WeftQuestion(id)
	if err != nil {
		s.writeError(w, http.StatusNotFound, "question_not_found", id+" was not found")
		return
	}
	asOf, err := s.store.WeftAsOf()
	if err != nil {
		s.internal(w, err)
		return
	}
	s.writeJSON(w, http.StatusOK, map[string]any{
		"contract_version": ContractVersion, "as_of": asOf, "question": questionResource(q),
	})
}

// handleV1Mutation is weft-service/v2 §14.2's v1 route mode: a v1 launch
// cannot honestly be a waiting entry with no run, and a v1 client must not
// bypass the shared queue, so every v1 mutation is refused with the
// host_paused every v1 client already handles. mutate still replays a key
// recorded before the upgrade.
func (s *Service) handleV1Mutation(w http.ResponseWriter, r *http.Request) {
	s.mutate(w, r, func([]byte) mutation {
		return apiFailure(http.StatusServiceUnavailable, "host_paused",
			"weft-remote/v1 mutations are refused on host "+s.hostID+
				": launches, answers, stops and resumes go through weft-service/v2")
	})
}

type mutation struct {
	status int
	body   any
	err    error
}

func apiFailure(status int, code, message string) mutation {
	return mutation{status: status, body: errorBody(code, message)}
}

func (s *Service) mutate(w http.ResponseWriter, r *http.Request, execute func([]byte) mutation) {
	key := r.Header.Get("Idempotency-Key")
	if !validIdempotencyKey(key) {
		s.writeError(w, http.StatusBadRequest, "idempotency_key_missing", "Idempotency-Key must be printable ASCII and at most 128 bytes")
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxRequestBytes))
	if err != nil {
		s.writeError(w, http.StatusBadRequest, "malformed", "request body is too large")
		return
	}
	bodySHA := ledger.BodySHA(body)
	s.mutateMu.Lock()
	defer s.mutateMu.Unlock()
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if err := s.pruneRetention(); err != nil {
		s.internal(w, err)
		return
	}
	prior, err := s.store.GetWeftRequest(s.tokenID, key)
	if err == nil {
		if prior.Method != r.Method || prior.Path != r.URL.Path || prior.BodySHA != bodySHA {
			s.writeError(w, http.StatusUnprocessableEntity, "idempotency_key_reused", "Idempotency-Key was already used for another request")
			return
		}
		w.Header().Set("Idempotent-Replayed", "true")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(prior.Status)
		_, _ = w.Write(prior.Response)
		return
	}
	if !errors.Is(err, sql.ErrNoRows) {
		s.internal(w, err)
		return
	}
	result := execute(body)
	if result.err != nil {
		s.internal(w, result.err)
		return
	}
	doc, err := json.Marshal(result.body)
	if err != nil {
		s.internal(w, err)
		return
	}
	doc = append(doc, '\n')
	if err := s.store.PutWeftRequest(ledger.WeftRequest{
		TokenID: s.tokenID, Key: key, Method: r.Method, Path: r.URL.Path,
		BodySHA: bodySHA, Status: result.status, Response: doc,
		CreatedAt: time.Now().UTC().Format("2006-01-02T15:04:05.000Z"),
	}); err != nil {
		s.internal(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(result.status)
	_, _ = w.Write(doc)
}

func validIdempotencyKey(key string) bool {
	if key == "" || len(key) > 128 {
		return false
	}
	for i := range len(key) {
		if key[i] < 0x20 || key[i] > 0x7e {
			return false
		}
	}
	return true
}

func validProtocolID(id string) bool {
	if id == "" || len(id) > 64 {
		return false
	}
	for i := range len(id) {
		c := id[i]
		if (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') &&
			(c < '0' || c > '9') && c != '-' {
			return false
		}
	}
	return true
}

func decodeStrict(body []byte, dst any) error {
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return err
	}
	var extra any
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return fmt.Errorf("trailing JSON value")
		}
		return err
	}
	return nil
}

func errorBody(code, message string) map[string]any {
	return map[string]any{"contract_version": ContractVersion, "error": map[string]any{"code": code, "message": message}}
}

func (s *Service) writeError(w http.ResponseWriter, status int, code, message string) {
	s.writeJSON(w, status, errorBody(code, message))
}

func (s *Service) internal(w http.ResponseWriter, err error) {
	s.log.Printf("weft service: %v", err)
	s.writeError(w, http.StatusInternalServerError, "malformed", "internal service error")
}

func (s *Service) writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func (s *Service) handleEvents(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		s.internal(w, fmt.Errorf("response does not support streaming"))
		return
	}
	s.stateMu.RLock()
	oldest, current, err := s.store.WeftEventBounds()
	var floor int64
	if err == nil {
		floor, err = s.store.WeftV1EventFloor()
	}
	s.stateMu.RUnlock()
	if err != nil {
		s.internal(w, err)
		return
	}
	last := current
	if value := r.Header.Get("Last-Event-ID"); value != "" {
		last, err = strconv.ParseInt(value, 10, 64)
		if err != nil || last < 0 {
			s.writeError(w, http.StatusBadRequest, "malformed", "Last-Event-ID must be a non-negative integer")
			return
		}
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	flusher.Flush()
	if oldest > 0 && last < oldest-1 {
		reset, _ := json.Marshal(map[string]any{
			"contract_version": ContractVersion, "seq": current, "oldest": oldest,
		})
		if _, err := fmt.Fprintf(w, "event: reset\ndata: %s\n\n", reset); err != nil {
			return
		}
		last = current
		flusher.Flush()
	}
	poll := time.NewTicker(250 * time.Millisecond)
	defer poll.Stop()
	keepAlive := time.NewTicker(15 * time.Second)
	defer keepAlive.Stop()
	for {
		// Only the events written under weft-remote/v1 are v1 payloads
		// (weft-service/v2 §14.2). They are history — nothing at or below the
		// migration's floor is written after it — so once they are sent this
		// stream carries keep-alives alone, and v2 state is read through /v2.
		if last < floor {
			s.stateMu.RLock()
			events, err := s.store.WeftEventsAfter(last)
			s.stateMu.RUnlock()
			if err != nil {
				return
			}
			for _, event := range events {
				if event.Seq > floor {
					break
				}
				if _, err := fmt.Fprintf(w, "id: %d\nevent: %s\ndata: %s\n\n", event.Seq, event.Type, event.Payload); err != nil {
					return
				}
				flusher.Flush()
			}
			last = floor
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
