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
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/EnderRealm/ticket/v8/pkg/ticket"

	"shuttle/internal/config"
	"shuttle/internal/ledger"
	"shuttle/internal/sandbox"
)

const (
	maxRequestBytes = 70 << 10
	maxAnswerBytes  = 64 << 10
)

type segmentControl interface {
	Start(runID string)
	Stop(runID string)
}

// Service is the authenticated weft-remote/v1 HTTP host.
type Service struct {
	cfg      *config.Config
	store    *ledger.Store
	token    string
	tokenID  string
	home     string
	log      *log.Logger
	segments segmentControl
	mutateMu sync.Mutex
	stateMu  sync.RWMutex
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
	s := &Service{cfg: cfg, store: store, token: token, tokenID: ledger.TokenID(token), home: home, log: logger}
	s.segments = NewSupervisor(cfg, store, home, logger, &s.stateMu)
	return s, nil
}

// SetSegmentControl replaces process supervision for protocol tests.
func (s *Service) SetSegmentControl(control segmentControl) {
	if control != nil {
		s.segments = control
	}
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
	if !s.authorized(r) {
		s.writeError(w, http.StatusUnauthorized, "unauthorized", "a bearer token is required")
		return
	}
	path := strings.TrimSuffix(r.URL.Path, "/")
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
		s.handleLaunch(w, r)
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

func (s *Service) handleLaunch(w http.ResponseWriter, r *http.Request) {
	type request struct {
		ContractVersion string `json:"contract_version"`
		Ticket          string `json:"ticket"`
	}
	s.mutate(w, r, func(body []byte) mutation {
		var req request
		if err := decodeStrict(body, &req); err != nil {
			return apiFailure(http.StatusBadRequest, "malformed", err.Error())
		}
		if req.ContractVersion != ContractVersion {
			return apiFailure(http.StatusBadRequest, "contract_version", "unsupported contract_version")
		}
		if strings.Count(req.Ticket, "/") != 1 {
			return apiFailure(http.StatusBadRequest, "malformed", "ticket must be a qualified project/id")
		}
		catalog, err := loadTickets(s.cfg.Paths.TKConfig)
		if err != nil {
			return mutation{err: err}
		}
		all, err := catalog.all()
		if err != nil {
			return mutation{err: err}
		}
		byID := make(map[string]*ticket.Ticket, len(all))
		for _, t := range all {
			byID[t.ID] = t
		}
		t := byID[req.Ticket]
		if t == nil {
			return apiFailure(http.StatusNotFound, "ticket_not_found", req.Ticket+" was not found")
		}
		if yes, reason := launchable(t, byID); !yes {
			return apiFailure(http.StatusConflict, "ticket_not_launchable", req.Ticket+": "+reason)
		}
		repo, err := catalog.projectPath(projectOf(t.ID))
		if err != nil {
			return mutation{err: err}
		}
		worktree, err := sandbox.WorktreePath(s.cfg.Weft.Root, repo, t.ID)
		if err != nil {
			return mutation{err: err}
		}
		repositoryKey, err := sandbox.RepositoryIdentity(repo)
		if err != nil {
			return mutation{err: err}
		}
		run, err := s.store.ClaimWeft(ledger.WeftLaunch{
			Ticket: t.ID, Project: projectOf(t.ID), Repo: repo, Branch: t.ID,
			Worktree: worktree, RunDirRoot: s.weftRunsRoot(), RepositoryKey: repositoryKey,
		})
		if err != nil {
			return s.mapLedgerError(err, req.Ticket)
		}
		resource, err := runResource(run, s.store)
		if err != nil {
			return mutation{err: err}
		}
		asOf, err := s.store.WeftAsOf()
		if err != nil {
			return mutation{err: err}
		}
		return mutation{
			status: http.StatusCreated,
			body:   map[string]any{"contract_version": ContractVersion, "as_of": asOf, "run": resource},
			after:  func() { s.segments.Start(run.ID) },
		}
	})
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
	case r.Method == http.MethodPost && parts[1] == "answer":
		s.handleAnswer(w, r, runID)
	case r.Method == http.MethodPost && parts[1] == "stop":
		s.handleStop(w, r, runID)
	case r.Method == http.MethodPost && parts[1] == "resume":
		s.handleResume(w, r, runID)
	default:
		s.writeError(w, http.StatusNotFound, "run_not_found", runID+" was not found")
	}
}

func (s *Service) handleAnswer(w http.ResponseWriter, r *http.Request, runID string) {
	type request struct {
		ContractVersion string `json:"contract_version"`
		QuestionID      string `json:"question_id"`
		Answer          string `json:"answer"`
	}
	s.mutate(w, r, func(body []byte) mutation {
		var req request
		if err := decodeStrict(body, &req); err != nil {
			return apiFailure(http.StatusBadRequest, "malformed", err.Error())
		}
		if req.ContractVersion != ContractVersion {
			return apiFailure(http.StatusBadRequest, "contract_version", "unsupported contract_version")
		}
		if !validProtocolID(req.QuestionID) {
			return apiFailure(http.StatusBadRequest, "malformed", "question_id is not well formed")
		}
		if len([]byte(req.Answer)) > maxAnswerBytes || !utf8.ValidString(req.Answer) {
			return apiFailure(http.StatusBadRequest, "malformed", "answer must be UTF-8 and at most 64 KiB")
		}
		prior, err := s.store.GetWeftRun(runID)
		if err != nil {
			return apiFailure(http.StatusNotFound, "run_not_found", runID+" was not found")
		}
		repositoryKey, err := sandbox.RepositoryIdentity(prior.Repo)
		if err != nil {
			return mutation{err: err}
		}
		run, err := s.store.ClaimWeftAnswer(runID, req.QuestionID, req.Answer, s.weftRunsRoot(), repositoryKey)
		if err != nil {
			return s.mapLedgerError(err, runID)
		}
		resource, err := runResource(run, s.store)
		if err != nil {
			return mutation{err: err}
		}
		asOf, err := s.store.WeftAsOf()
		if err != nil {
			return mutation{err: err}
		}
		return mutation{
			status: http.StatusOK,
			body:   map[string]any{"contract_version": ContractVersion, "as_of": asOf, "run": resource},
			after:  func() { s.segments.Start(run.ID) },
		}
	})
}

func (s *Service) handleStop(w http.ResponseWriter, r *http.Request, runID string) {
	type request struct {
		ContractVersion string `json:"contract_version"`
	}
	s.mutate(w, r, func(body []byte) mutation {
		var req request
		if err := decodeStrict(body, &req); err != nil {
			return apiFailure(http.StatusBadRequest, "malformed", err.Error())
		}
		if req.ContractVersion != ContractVersion {
			return apiFailure(http.StatusBadRequest, "contract_version", "unsupported contract_version")
		}
		run, err := s.store.GetWeftRun(runID)
		if err != nil {
			return apiFailure(http.StatusNotFound, "run_not_found", runID+" was not found")
		}
		if run.Phase == ledger.WeftPhaseEnded {
			return apiFailure(http.StatusConflict, "run_over", runID+" is already over")
		}
		var after func()
		if run.Phase == ledger.WeftPhaseParked {
			run, err = s.store.StopWeftParked(runID)
		} else {
			err = s.store.MarkWeftStopped(runID)
			if err == nil {
				run, err = s.store.GetWeftRun(runID)
				after = func() { s.segments.Stop(runID) }
			}
		}
		if err != nil {
			return s.mapLedgerError(err, runID)
		}
		resource, err := runResource(run, s.store)
		if err != nil {
			return mutation{err: err}
		}
		asOf, err := s.store.WeftAsOf()
		if err != nil {
			return mutation{err: err}
		}
		return mutation{status: http.StatusOK, body: map[string]any{
			"contract_version": ContractVersion, "as_of": asOf, "run": resource,
		}, after: after}
	})
}

func (s *Service) handleResume(w http.ResponseWriter, r *http.Request, runID string) {
	type request struct {
		ContractVersion string `json:"contract_version"`
	}
	s.mutate(w, r, func(body []byte) mutation {
		var req request
		if err := decodeStrict(body, &req); err != nil {
			return apiFailure(http.StatusBadRequest, "malformed", err.Error())
		}
		if req.ContractVersion != ContractVersion {
			return apiFailure(http.StatusBadRequest, "contract_version", "unsupported contract_version")
		}
		prior, err := s.store.GetWeftRun(runID)
		if err != nil {
			return apiFailure(http.StatusNotFound, "run_not_found", runID+" was not found")
		}
		hasWorker := false
		for _, seg := range prior.Segments {
			hasWorker = hasWorker || seg.WorkerRunID.Valid
		}
		if hasWorker {
			if info, err := os.Stat(prior.Worktree); err != nil || !info.IsDir() {
				return apiFailure(http.StatusConflict, "worktree_missing", prior.Worktree+" is missing")
			}
		}
		repositoryKey, err := sandbox.RepositoryIdentity(prior.Repo)
		if err != nil {
			return mutation{err: err}
		}
		run, err := s.store.ClaimWeftResume(runID, s.weftRunsRoot(), repositoryKey)
		if err != nil {
			return s.mapLedgerError(err, runID)
		}
		resource, err := runResource(run, s.store)
		if err != nil {
			return mutation{err: err}
		}
		asOf, err := s.store.WeftAsOf()
		if err != nil {
			return mutation{err: err}
		}
		return mutation{
			status: http.StatusOK,
			body:   map[string]any{"contract_version": ContractVersion, "as_of": asOf, "run": resource},
			after:  func() { s.segments.Start(run.ID) },
		}
	})
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

type mutation struct {
	status int
	body   any
	after  func()
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
	if err := s.store.PruneWeftRetention(time.Now().Add(-7 * 24 * time.Hour)); err != nil {
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
	if result.after != nil {
		go result.after()
	}
}

func (s *Service) mapLedgerError(err error, id string) mutation {
	var busy *ledger.RepositoryBusyError
	switch {
	case errors.Is(err, ledger.ErrWeftRunInFlight):
		return apiFailure(http.StatusConflict, "run_in_flight", id+" already has a run in flight")
	case errors.Is(err, ledger.ErrWeftHostPaused):
		return apiFailure(http.StatusServiceUnavailable, "host_paused", "weft-worker is paused")
	case errors.As(err, &busy):
		return apiFailure(http.StatusConflict, "repository_busy", "repository is held by "+busy.HeldBy)
	case errors.Is(err, ledger.ErrWeftQuestionPending):
		return apiFailure(http.StatusConflict, "question_not_pending", id+" has no matching pending question")
	case errors.Is(err, ledger.ErrWeftRunOver):
		return apiFailure(http.StatusConflict, "run_over", id+" is over")
	case errors.Is(err, ledger.ErrWeftRunNotOver):
		return apiFailure(http.StatusConflict, "run_not_over", id+" is not over")
	case errors.Is(err, ledger.ErrWeftRunNotResumable):
		return apiFailure(http.StatusConflict, "run_not_resumable", id+" is not resumable")
	case errors.Is(err, ledger.ErrWeftSegmentLimit):
		return apiFailure(http.StatusConflict, "segment_limit", id+" has reached the segment limit")
	case errors.Is(err, ledger.ErrWeftWorktreeMissing):
		return apiFailure(http.StatusConflict, "worktree_missing", id+" has no worktree")
	default:
		return mutation{err: err}
	}
}

func (s *Service) weftRunsRoot() string {
	return filepath.Join(s.home, "runs", ledger.WeftAgent)
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
		s.stateMu.RLock()
		events, err := s.store.WeftEventsAfter(last)
		s.stateMu.RUnlock()
		if err != nil {
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
