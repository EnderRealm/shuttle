package weftremote

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/coder/websocket"

	"shuttle/internal/ledger"
	"shuttle/internal/reap"
	"shuttle/internal/sandbox"
	"shuttle/internal/vt"
)

var (
	terminalPath   = regexp.MustCompile(`^/v2/terminals/([^/]+)$`)
	terminalClose  = regexp.MustCompile(`^/v2/terminals/([^/]+)/close$`)
	terminalAttach = regexp.MustCompile(`^/v2/terminals/([^/]+)/attach$`)
)

const (
	// terminalSubprotocol is the WebSocket subprotocol a terminal attachment
	// speaks (§9.5).
	terminalSubprotocol = "weft-service.v2"
	// terminalListWindow is how long a closed or interrupted session stays
	// listed (§9.4).
	terminalListWindow = 24 * time.Hour
	// terminalCloseWait bounds how long a close waits for the session's
	// process group to be reaped and the session settled.
	terminalCloseWait = 30 * time.Second
	// A session is at most the size whose snapshot always fits in 1 MiB.
	maxTerminalCols = vt.MaxCols
	maxTerminalRows = vt.MaxRows
	maxClientFrame  = 1 << 20
)

type terminalRequest struct {
	ContractVersion string         `json:"contract_version"`
	Kind            string         `json:"kind"`
	Target          terminalTarget `json:"target"`
	Cols            int            `json:"cols"`
	Rows            int            `json:"rows"`
}

type terminalTarget struct {
	Kind string `json:"kind"`
	ID   string `json:"id"`
}

func (s *Service) rememberTerminal(ts *terminalSession) {
	s.termMu.Lock()
	defer s.termMu.Unlock()
	s.terms[ts.id] = ts
}

func (s *Service) forgetTerminal(id string) {
	s.termMu.Lock()
	defer s.termMu.Unlock()
	delete(s.terms, id)
}

func (s *Service) terminal(id string) *terminalSession {
	s.termMu.Lock()
	defer s.termMu.Unlock()
	return s.terms[id]
}

// terminalResource is the session's resource, with what a session this
// process owns holds now.
func (s *Service) terminalResource(row *ledger.WeftTerminal) map[string]any {
	resource := row.Resource()
	if ts := s.terminal(row.ID); ts != nil && row.Open() {
		resource = ts.overlay(resource)
	}
	return resource
}

func refusal(code, format string, args ...any) error {
	return &ledger.WeftRefusal{Code: code, Message: fmt.Sprintf(format, args...)}
}

// handleTerminalOpen opens a session (§9.3): the target resolved and
// validated, then — in one ledger transaction, before any child exists —
// the worktree's run checked, the repository's terminal reservation taken
// and the session recorded starting. The shell is spawned after the commit.
func (s *Service) handleTerminalOpen(w http.ResponseWriter, r *http.Request) {
	s.mutateV2(w, r, func(body []byte, _ string) (v2Commit, error) {
		var req terminalRequest
		if err := decodeV2(body, &req, "kind", "target", "cols", "rows"); err != nil {
			return refusedBy(err), nil
		}
		if req.Kind != "shell" && req.Kind != "claude" {
			return refused("malformed", "kind %q is neither shell nor claude", req.Kind), nil
		}
		if req.Cols < 2 || req.Cols > maxTerminalCols || req.Rows < 2 || req.Rows > maxTerminalRows {
			return refused("malformed", "a terminal is 2 to %d columns by 2 to %d rows, not %dx%d",
				maxTerminalCols, maxTerminalRows, req.Cols, req.Rows), nil
		}
		open, err := s.terminalTarget(req.Target)
		var refused *ledger.WeftRefusal
		if errors.As(err, &refused) {
			return refusedBy(err), nil
		}
		if err != nil {
			return nil, err
		}
		open.Kind, open.TargetKind, open.TargetID = req.Kind, req.Target.Kind, req.Target.ID
		open.Cols, open.Rows, open.Title = req.Cols, req.Rows, filepath.Base(open.Path)
		return func(t *ledger.WeftTx) (v2Outcome, error) {
			row, err := t.OpenTerminal(open)
			if err != nil {
				return v2Outcome{}, err
			}
			ts := newTerminalSession(s, row)
			s.rememberTerminal(ts)
			return v2Outcome{status: http.StatusCreated, fields: map[string]any{"terminal": row.Resource()},
				after: func() { s.startTerminal(ts) },
				abandon: func() {
					s.forgetTerminal(ts.id)
					close(ts.started)
					close(ts.done)
				}}, nil
		}, nil
	})
}

// terminalTarget resolves a target to the canonical directory a session
// opens in and the registered repository and project that directory lies in
// (§9.2, §9.3). Only a project's bound checkout, a run's retained worktree
// or a folder handle is ever a working directory.
func (s *Service) terminalTarget(target terminalTarget) (ledger.WeftTerminalOpen, error) {
	switch target.Kind {
	case ledger.WeftTargetProject:
		c, err := s.readCatalog()
		if err != nil {
			return ledger.WeftTerminalOpen{}, err
		}
		p := c.byName[target.ID]
		if p == nil {
			return ledger.WeftTerminalOpen{}, refusal("project_not_found", "%s is not a project registered on host %s", target.ID, s.hostID)
		}
		if p.Repository == "" || (p.State != projectAvailable && p.State != projectEmpty) {
			return ledger.WeftTerminalOpen{}, refusal("binding_changed", "project %s is %s (%s): %s; a terminal opens only in its bound checkout",
				p.Name, p.State, p.ReasonCode, p.ReasonDetail)
		}
		canonical, _, err := canonicalDir(p.Repository)
		if err != nil || canonical != p.Repository {
			return ledger.WeftTerminalOpen{}, refusal("binding_changed", "project %s's checkout %s no longer resolves to itself (%s, %v)",
				p.Name, p.Repository, canonical, err)
		}
		return ledger.WeftTerminalOpen{Path: canonical, RepositoryID: p.RepositoryID, Repository: p.Repository, Project: p.Name}, nil
	case ledger.WeftTargetWorktree:
		if !validProtocolID(target.ID) {
			return ledger.WeftTerminalOpen{}, refusal("run_not_found", "%s is not a run on host %s", target.ID, s.hostID)
		}
		s.stateMu.RLock()
		run, err := s.store.GetWeftRun(target.ID)
		s.stateMu.RUnlock()
		if err != nil {
			return ledger.WeftTerminalOpen{}, refusal("run_not_found", "%s is not a run on host %s", target.ID, s.hostID)
		}
		open := ledger.WeftTerminalOpen{RunID: run.ID, Path: run.Worktree, RepositoryID: run.RepositoryID,
			Repository: run.Repo, Project: run.Project}
		// A run that is history or whose worktree was removed is refused by
		// the ledger, which knows why.
		if run.Origin == ledger.WeftOriginArchive || run.Disposition != ledger.WeftDispositionRetained {
			return open, nil
		}
		canonical, _, err := canonicalDir(run.Worktree)
		if err != nil {
			return ledger.WeftTerminalOpen{}, refusal("worktree_missing", "run %s's worktree %s is missing: %v", run.ID, run.Worktree, err)
		}
		repo, err := sandbox.CanonicalRepository(run.Repo)
		if err != nil {
			return ledger.WeftTerminalOpen{}, refusal("binding_changed", "run %s's repository %s no longer resolves: %v", run.ID, run.Repo, err)
		}
		if named := worktreeRepository(canonical); named != repo {
			return ledger.WeftTerminalOpen{}, refusal("binding_changed", "worktree %s names %q as its repository, not run %s's %s",
				canonical, named, run.ID, repo)
		}
		open.Path = canonical
		return open, nil
	case ledger.WeftTargetFolder:
		dir, err := s.folders.resolve(target.ID)
		if err != nil {
			return ledger.WeftTerminalOpen{}, err
		}
		c, err := s.readCatalog()
		if err != nil {
			return ledger.WeftTerminalOpen{}, err
		}
		repositoryID, repository, project := repositoryAt(c, dir)
		return ledger.WeftTerminalOpen{Path: dir, RepositoryID: repositoryID, Repository: repository, Project: project}, nil
	}
	return ledger.WeftTerminalOpen{}, refusal("malformed", "target kind %q is not project, worktree or folder", target.Kind)
}

// startTerminal spawns a recorded session's shell. A spawn that fails, or
// whose shell could not be recorded, closes the session.
func (s *Service) startTerminal(ts *terminalSession) {
	if pid, unresolved, err := ts.start(); err != nil {
		ts.failed(err, unresolved, pid)
	}
	close(ts.started)
}

func (s *Service) handleTerminals(w http.ResponseWriter) {
	s.stateMu.RLock()
	rows, err := s.store.WeftTerminals(time.Now().Add(-terminalListWindow))
	var asOf int64
	if err == nil {
		asOf, err = s.store.WeftAsOf()
	}
	s.stateMu.RUnlock()
	if err != nil {
		s.v2Internal(w, err)
		return
	}
	listed := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		listed = append(listed, s.terminalResource(row))
	}
	s.read(w, asOf, map[string]any{"terminals": listed})
}

// readTerminal is the session id names, or false once a 404 is written.
func (s *Service) readTerminal(w http.ResponseWriter, id string) (*ledger.WeftTerminal, int64, bool) {
	if !validProtocolID(id) {
		s.v2Error(w, http.StatusNotFound, "terminal_not_found", id+" is not a terminal on host "+s.hostID)
		return nil, 0, false
	}
	s.stateMu.RLock()
	row, err := s.store.WeftTerminal(id)
	var asOf int64
	if err == nil {
		asOf, err = s.store.WeftAsOf()
	}
	s.stateMu.RUnlock()
	if errors.Is(err, sql.ErrNoRows) {
		s.v2Error(w, http.StatusNotFound, "terminal_not_found", id+" is not a terminal on host "+s.hostID)
		return nil, 0, false
	}
	if err != nil {
		s.v2Internal(w, err)
		return nil, 0, false
	}
	return row, asOf, true
}

func (s *Service) handleTerminal(w http.ResponseWriter, id string) {
	row, asOf, ok := s.readTerminal(w, id)
	if !ok {
		return
	}
	s.read(w, asOf, map[string]any{"terminal": s.terminalResource(row)})
}

// handleTerminalClose ends a session (§9.4): its process group is hung up
// and reaped, and only then is it closed and its reservation share released.
func (s *Service) handleTerminalClose(w http.ResponseWriter, r *http.Request, id string) {
	s.mutateV2(w, r, func(body []byte, _ string) (v2Commit, error) {
		var req revisionRequest
		if err := decodeV2(body, &req, "revision"); err != nil {
			return refusedBy(err), nil
		}
		if !validProtocolID(id) {
			return refused("terminal_not_found", "%s is not a terminal on host %s", id, s.hostID), nil
		}
		s.stateMu.RLock()
		row, err := s.store.WeftTerminal(id)
		s.stateMu.RUnlock()
		if errors.Is(err, sql.ErrNoRows) {
			return refused("terminal_not_found", "%s is not a terminal on host %s", id, s.hostID), nil
		}
		if err != nil {
			return nil, err
		}
		if !row.Open() {
			return refused("session_closed", "terminal %s is %s (closed_by %s)", id, row.State, row.ClosedBy.String), nil
		}
		ts := s.terminal(id)
		if ts == nil {
			// It may have ended between the read and the lookup.
			if over, ok := s.endedTerminal(id); ok {
				return refused("session_closed", "terminal %s is %s (closed_by %s)", id, over.State, over.ClosedBy.String), nil
			}
			return nil, fmt.Errorf("terminal %s is open in the ledger and has no session on this host", id)
		}
		if err := ts.requestClose(req.Revision); err != nil {
			return refusedBy(err), nil
		}
		select {
		case <-ts.done:
		case <-time.After(terminalCloseWait):
			return nil, fmt.Errorf("terminal %s did not settle within %s of its close", id, terminalCloseWait)
		}
		return func(t *ledger.WeftTx) (v2Outcome, error) {
			row, err := t.Terminal(id)
			if err != nil {
				return v2Outcome{}, err
			}
			return v2Outcome{status: http.StatusOK, fields: map[string]any{"terminal": row.Resource()}}, nil
		}, nil
	})
}

// endedTerminal is the session as the ledger holds it now, when it is no
// longer open.
func (s *Service) endedTerminal(id string) (*ledger.WeftTerminal, bool) {
	s.stateMu.RLock()
	row, err := s.store.WeftTerminal(id)
	s.stateMu.RUnlock()
	return row, err == nil && !row.Open()
}

// refuseEnded answers an attach to a closed or interrupted session (§9.4),
// reporting whether it did.
func (s *Service) refuseEnded(w http.ResponseWriter, row *ledger.WeftTerminal) bool {
	switch row.State {
	case ledger.WeftTerminalClosed:
		s.v2Error(w, http.StatusConflict, "session_closed", fmt.Sprintf("terminal %s is closed (closed_by %s)", row.ID, row.ClosedBy.String))
		return true
	case ledger.WeftTerminalInterrupted:
		s.v2Error(w, http.StatusConflict, "session_interrupted",
			fmt.Sprintf("terminal %s was interrupted by a restart of host %s; open a new session on %s %s", row.ID, s.hostID, row.TargetKind, row.TargetID))
		return true
	}
	return false
}

// offeredSubprotocols are the WebSocket subprotocols the client offers.
func offeredSubprotocols(r *http.Request) []string {
	var offered []string
	for _, value := range r.Header.Values("Sec-WebSocket-Protocol") {
		for _, p := range strings.Split(value, ",") {
			if p = strings.TrimSpace(p); p != "" {
				offered = append(offered, p)
			}
		}
	}
	return offered
}

// handleTerminalAttach is the attachment WebSocket (§9.5): bearer-authorized
// like every route, subprotocol weft-service.v2 or 426.
func (s *Service) handleTerminalAttach(w http.ResponseWriter, r *http.Request, id string) {
	row, _, ok := s.readTerminal(w, id)
	if !ok || s.refuseEnded(w, row) {
		return
	}
	offered := offeredSubprotocols(r)
	speaks := false
	for _, p := range offered {
		speaks = speaks || p == terminalSubprotocol
	}
	if !strings.EqualFold(r.Header.Get("Upgrade"), "websocket") || !speaks {
		message := "attach is a WebSocket upgrade with subprotocol " + terminalSubprotocol
		for _, p := range offered {
			if v, ok := strings.CutPrefix(p, "weft-service."); ok {
				message = upgradeMessage("weft-service/" + v)
			}
		}
		s.v2Error(w, http.StatusUpgradeRequired, "upgrade_required", message)
		return
	}
	var offset int64
	hasOffset := r.URL.Query().Has("offset")
	if hasOffset {
		n, err := strconv.ParseInt(r.URL.Query().Get("offset"), 10, 64)
		if err != nil || n < 0 {
			s.v2Error(w, http.StatusBadRequest, "malformed", "offset must be a non-negative integer")
			return
		}
		offset = n
	}
	ts := s.terminal(id)
	if ts == nil {
		// It may have ended between the read and the lookup.
		if over, ok := s.endedTerminal(id); ok && s.refuseEnded(w, over) {
			return
		}
		s.v2Internal(w, fmt.Errorf("terminal %s is open in the ledger and has no session on this host", id))
		return
	}
	ts.mu.Lock()
	end := ts.ring.end
	ts.mu.Unlock()
	if offset > end {
		s.v2Error(w, http.StatusBadRequest, "malformed", fmt.Sprintf("offset %d is past terminal %s's output, which ends at %d", offset, id, end))
		return
	}
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{Subprotocols: []string{terminalSubprotocol}})
	if err != nil {
		s.log.Printf("weft terminal host=%s session=%s: accept: %v", s.hostID, id, err)
		return
	}
	conn.SetReadLimit(maxClientFrame)
	attachmentID, err := ledger.NewRemoteID("ta", time.Now())
	if err != nil {
		_ = conn.Close(websocket.StatusInternalError, closeReason("attachment id"))
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	a := &attachment{id: attachmentID, conn: conn, wake: make(chan struct{}, 1), ctx: ctx,
		stop: func() { cancel(); _ = conn.CloseNow() }}
	ts.attach(a, offset, hasOffset)
	go ts.write(a)
	code, reason := ts.readClient(a)
	ts.detach(a)
	if code != 0 {
		if code != websocket.StatusNormalClosure {
			ts.logf("attachment %s closed %d: %s", a.id, code, reason)
		}
		_ = conn.Close(code, closeReason(reason))
	}
	a.stop()
}

// maxCloseReason is the most a WebSocket close frame's reason may hold
// (RFC 6455 §5.5: a 125-byte control payload less the 2-byte code).
const maxCloseReason = 123

// closeReason is reason cut to what a close frame carries, on a UTF-8
// character boundary; a longer one would send no close frame at all.
func closeReason(reason string) string {
	if len(reason) <= maxCloseReason {
		return reason
	}
	cut := maxCloseReason
	for cut > 0 && !utf8.RuneStart(reason[cut]) {
		cut--
	}
	return reason[:cut]
}

// readClient applies the client's frames until it detaches, disconnects or
// sends one that is not a weft-service/v2 client frame, answering the close
// status to end the WebSocket with (0 when it is already gone).
func (ts *terminalSession) readClient(a *attachment) (websocket.StatusCode, string) {
	for {
		typ, data, err := a.conn.Read(a.ctx)
		if err != nil {
			return 0, ""
		}
		if typ != websocket.MessageText {
			return websocket.StatusUnsupportedData, "frames are text JSON"
		}
		var head struct {
			ContractVersion string `json:"contract_version"`
			Type            string `json:"type"`
		}
		if err := json.Unmarshal(data, &head); err != nil {
			return websocket.StatusPolicyViolation, "malformed frame: " + err.Error()
		}
		if head.ContractVersion != ServiceVersion {
			return websocket.StatusPolicyViolation, upgradeMessage(head.ContractVersion)
		}
		switch head.Type {
		case "input":
			var f struct {
				ContractVersion string `json:"contract_version"`
				Type            string `json:"type"`
				Data            string `json:"data"`
			}
			if err := decodeStrict(data, &f); err != nil {
				return websocket.StatusPolicyViolation, "malformed input frame: " + err.Error()
			}
			bytes, err := base64.StdEncoding.DecodeString(f.Data)
			if err != nil {
				return websocket.StatusPolicyViolation, "input data is not base64"
			}
			ts.input(a, bytes)
		case "resize":
			var f struct {
				ContractVersion string `json:"contract_version"`
				Type            string `json:"type"`
				Cols            int    `json:"cols"`
				Rows            int    `json:"rows"`
			}
			if err := decodeStrict(data, &f); err != nil {
				return websocket.StatusPolicyViolation, "malformed resize frame: " + err.Error()
			}
			if f.Cols < 2 || f.Cols > maxTerminalCols || f.Rows < 2 || f.Rows > maxTerminalRows {
				return websocket.StatusPolicyViolation, fmt.Sprintf("a terminal is 2 to %d columns by 2 to %d rows", maxTerminalCols, maxTerminalRows)
			}
			ts.resize(a, f.Cols, f.Rows)
		case "take_over", "detach":
			var f struct {
				ContractVersion string `json:"contract_version"`
				Type            string `json:"type"`
			}
			if err := decodeStrict(data, &f); err != nil {
				return websocket.StatusPolicyViolation, "malformed " + head.Type + " frame: " + err.Error()
			}
			if head.Type == "detach" {
				return websocket.StatusNormalClosure, "detached"
			}
			ts.takeOver(a)
		default:
			return websocket.StatusPolicyViolation, fmt.Sprintf("%q is not a client frame", head.Type)
		}
	}
}

// interruptTerminals is §12 step 3: every session not closed is interrupted,
// closed_by restart, and everything its shell started established gone on
// the terms of v1's restart pass — the shell's whole session killed by group
// when the shell is established as the one the session spawned — before its
// reservation share is released. A session nothing could be established
// about keeps the share.
func (s *Service) interruptTerminals() error {
	s.stateMu.RLock()
	rows, err := s.store.WeftTerminalsToInterrupt()
	s.stateMu.RUnlock()
	if err != nil {
		return err
	}
	for _, row := range rows {
		var start time.Time
		if row.ProcessStart.Valid {
			at, err := time.Parse(time.RFC3339Nano, row.ProcessStart.String)
			if err != nil {
				s.log.Printf("weft restart host=%s: terminal %s records process start %q, which does not parse, so its shell cannot be established: %v",
					s.hostID, row.ID, row.ProcessStart.String, err)
			}
			start = at
		}
		pid := int(row.PID.Int64)
		action, detail := reap.RestartSession(pid, start)
		unresolved := ""
		switch {
		case action == reap.ActionKillFailed || action == reap.ActionUnresolved:
			unresolved = fmt.Sprintf("%s: %s", action, detail)
		case action == reap.ActionNoPID && row.GroupUnresolved.Valid:
			// Missing identity never clears uncertainty already recorded.
			unresolved = row.GroupUnresolved.String
		}
		// A session with no pid and nothing unresolved can only have a
		// launcher that was never let through to its shell — the go-ahead is
		// written only after the pid is durably recorded — and a launcher
		// exits at the EOF the daemon's death gave it, so ActionNoPID there
		// is the session gone.
		s.stateMu.Lock()
		_, err := s.store.InterruptWeftTerminal(row.ID, unresolved)
		s.stateMu.Unlock()
		if err != nil {
			return fmt.Errorf("interrupt terminal %s: %w", row.ID, err)
		}
		line := fmt.Sprintf("weft restart host=%s: terminal %s pid=%d action=%s", s.hostID, row.ID, pid, action)
		if detail != "" {
			line += fmt.Sprintf(" detail=%q", detail)
		}
		if unresolved != "" {
			line += " (its reservation share is kept)"
		}
		s.log.Print(line)
	}
	return nil
}
