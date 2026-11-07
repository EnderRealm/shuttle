package weftremote

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"regexp"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"shuttle/internal/ledger"
)

var (
	runCommand         = regexp.MustCompile(`^/v2/runs/([^/]+)/(answer|stop|resume|reconcile|dismiss|fresh)$`)
	entryCancel        = regexp.MustCompile(`^/v2/queue/([^/]+)/cancel$`)
	repositoryContinue = regexp.MustCompile(`^/v2/repositories/([^/]+)/continue$`)
)

// settledEntryWindow is how long a settled entry stays listed (§6.3).
const settledEntryWindow = 12 * time.Hour

// v2Commit is a mutation's work inside its one ledger transaction. A
// *ledger.WeftRefusal it returns is answered and recorded like any other
// response; any other error rolls everything back and is a 500.
type v2Commit func(t *ledger.WeftTx) (v2Outcome, error)

// v2Outcome is a successful mutation: its status, its body's fields — the
// read envelope is added — what runs once the transaction commits, and what
// undoes the commit's work outside the ledger when the transaction does not.
type v2Outcome struct {
	status  int
	fields  map[string]any
	after   func()
	abandon func()
}

func refused(code, format string, args ...any) v2Commit {
	return func(*ledger.WeftTx) (v2Outcome, error) {
		return v2Outcome{}, &ledger.WeftRefusal{Code: code, Message: fmt.Sprintf(format, args...)}
	}
}

func refusedBy(err error) v2Commit {
	return func(*ledger.WeftTx) (v2Outcome, error) { return v2Outcome{}, err }
}

// refusalStatus is §4.2's status for a code.
func refusalStatus(code string) int {
	switch {
	case code == "malformed":
		return http.StatusBadRequest
	case strings.HasSuffix(code, "_not_found"):
		return http.StatusNotFound
	case code == "upgrade_required":
		return http.StatusUpgradeRequired
	case code == "capability_not_served":
		return http.StatusNotImplemented
	}
	return http.StatusConflict
}

// mutateV2 is every v2 POST and PUT (§4.2, §6.3): a replay returns the
// original status and body and does nothing; otherwise prepare validates the
// body and reads tk outside any lock, and its commit runs in one ledger
// transaction with the idempotency record, so an admission, its dispatch and
// the response it returned are one durable fact. The workers that dispatch
// claimed start only after the commit. Each line logged names the host, the
// key's SHA-256, and the entries and runs it touched — never the token or an
// answer's text.
func (s *Service) mutateV2(w http.ResponseWriter, r *http.Request, prepare func(body []byte, key string) (v2Commit, error)) {
	s.mutateV2Limit(w, r, maxRequestBytes, prepare)
}

// mutateV2Limit is mutateV2 for a body of at most limit bytes.
func (s *Service) mutateV2Limit(w http.ResponseWriter, r *http.Request, limit int64, prepare func(body []byte, key string) (v2Commit, error)) {
	key := r.Header.Get("Idempotency-Key")
	if !validIdempotencyKey(key) {
		s.v2Error(w, http.StatusBadRequest, "idempotency_key_missing", "Idempotency-Key must be printable ASCII and at most 128 bytes")
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, limit))
	if err != nil {
		s.v2Error(w, http.StatusBadRequest, "malformed", "request body is too large")
		return
	}
	keySum := sha256.Sum256([]byte(key))
	keyHash := hex.EncodeToString(keySum[:])
	bodySHA := ledger.BodySHA(body)
	s.stateMu.RLock()
	prior, err := s.store.GetWeftRequest(s.tokenID, key)
	s.stateMu.RUnlock()
	if err == nil {
		s.replayV2(w, r, prior, bodySHA, keyHash)
		return
	}
	if !errors.Is(err, sql.ErrNoRows) {
		s.v2Internal(w, err)
		return
	}
	commit, err := prepare(body, key)
	if err != nil {
		s.log.Printf("weft service host=%s key=%s: %s %s: %v", s.hostID, keyHash, r.Method, r.URL.Path, err)
		s.v2Internal(w, err)
		return
	}
	var (
		status  int
		doc     []byte
		outcome v2Outcome
		spawn   []string
		changes []ledger.WeftChange
	)
	prior = nil
	s.stateMu.Lock()
	err = s.store.WeftTx(func(t *ledger.WeftTx) error {
		recorded, err := t.Request(s.tokenID, key)
		if err == nil {
			prior = recorded
			return nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		err = t.Refusable(func() error {
			var err error
			outcome, err = commit(t)
			return err
		})
		var refusal *ledger.WeftRefusal
		var fields map[string]any
		switch {
		case errors.As(err, &refusal):
			status, outcome = refusalStatus(refusal.Code), v2Outcome{}
			fields = map[string]any{"contract_version": ServiceVersion,
				"error": map[string]any{"code": refusal.Code, "message": refusal.Message}}
		case err != nil:
			return err
		default:
			asOf, err := t.AsOf()
			if err != nil {
				return err
			}
			status, fields = outcome.status, outcome.fields
			fields["contract_version"], fields["host_id"], fields["as_of"] = ServiceVersion, s.hostID, asOf
		}
		if doc, err = json.Marshal(fields); err != nil {
			return err
		}
		doc = append(doc, '\n')
		spawn, changes = t.Spawn, t.Changes
		return t.PutRequest(ledger.WeftRequest{
			TokenID: s.tokenID, Key: key, Method: r.Method, Path: r.URL.Path,
			BodySHA: bodySHA, Status: status, Response: doc,
			CreatedAt: time.Now().UTC().Format("2006-01-02T15:04:05.000Z"),
		})
	})
	s.stateMu.Unlock()
	if err != nil {
		if outcome.abandon != nil {
			outcome.abandon()
		}
		s.log.Printf("weft service host=%s key=%s: %s %s: %v", s.hostID, keyHash, r.Method, r.URL.Path, err)
		s.v2Internal(w, err)
		return
	}
	if prior != nil {
		s.replayV2(w, r, prior, bodySHA, keyHash)
		return
	}
	s.log.Printf("weft service host=%s key=%s: %s %s %d", s.hostID, keyHash, r.Method, r.URL.Path, status)
	s.logChanges(keyHash, changes)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(doc)
	s.spawn(spawn)
	if outcome.after != nil {
		outcome.after()
	}
}

func (s *Service) replayV2(w http.ResponseWriter, r *http.Request, prior *ledger.WeftRequest, bodySHA, keyHash string) {
	if prior.Method != r.Method || prior.Path != r.URL.Path || prior.BodySHA != bodySHA {
		s.v2Error(w, http.StatusUnprocessableEntity, "idempotency_key_reused",
			"Idempotency-Key was already used for "+prior.Method+" "+prior.Path+" with another body")
		return
	}
	s.log.Printf("weft service host=%s key=%s: %s %s replayed %d", s.hostID, keyHash, r.Method, r.URL.Path, prior.Status)
	w.Header().Set("Idempotent-Replayed", "true")
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(prior.Status)
	_, _ = w.Write(prior.Response)
}

func (s *Service) logChanges(keyHash string, changes []ledger.WeftChange) {
	if keyHash == "" {
		keyHash = "-"
	}
	for _, c := range changes {
		run := c.RunID
		if run == "" {
			run = "-"
		}
		entry := c.EntryID
		if entry == "" {
			entry = "-"
		}
		line := fmt.Sprintf("weft queue host=%s key=%s entry=%s run=%s: %s %s %s", s.hostID, keyHash, entry, run, c.Action, c.Kind, c.Ticket)
		if c.Detail != "" {
			line += " (" + strings.TrimSpace(c.Detail) + ")"
		}
		s.log.Print(line)
	}
}

func (s *Service) spawn(runs []string) {
	for _, id := range runs {
		go s.segments.Start(id)
	}
}

// decodeV2 reads a weft-service/v2 body (§1, §2.5): a body naming another
// version is 426 before anything else is read, one naming none is 400, every
// declared key must be present, and an undeclared one is refused by name.
func decodeV2(body []byte, dst any, keys ...string) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(body, &fields); err != nil || fields == nil {
		return &ledger.WeftRefusal{Code: "malformed", Message: "the body is not a JSON object"}
	}
	raw, ok := fields["contract_version"]
	if !ok {
		return &ledger.WeftRefusal{Code: "malformed", Message: "the body has no contract_version"}
	}
	var version string
	if err := json.Unmarshal(raw, &version); err != nil {
		return &ledger.WeftRefusal{Code: "malformed", Message: "contract_version is not a string"}
	}
	if version != ServiceVersion {
		return &ledger.WeftRefusal{Code: "upgrade_required", Message: upgradeMessage(version)}
	}
	for _, key := range keys {
		if _, ok := fields[key]; !ok {
			return &ledger.WeftRefusal{Code: "malformed", Message: "the body has no " + key}
		}
	}
	if err := decodeStrict(body, dst); err != nil {
		return &ledger.WeftRefusal{Code: "malformed", Message: err.Error()}
	}
	return nil
}

// dispatchConfig is dispatch against one tk read: entries are revalidated
// against it, and runs are minted pending their landing while it is served.
func (s *Service) dispatchConfig(c *catalog) ledger.WeftDispatch {
	d := s.segments.dispatchBase()
	d.Check = s.entryCheck(c)
	return d
}

// entryCheck is dispatch's and the catalog event's revalidation (§6.3): the
// project still bound to the repository the entry was admitted for, and the
// ticket still eligible as tk reads it. A reconcile repairs a landing in the
// repository the run was recorded in, whatever tk says now; a cleanup needs
// the binding and not the ticket, which a landing may have made done.
func (s *Service) entryCheck(c *catalog) func(*ledger.WeftEntry) (string, string) {
	return func(e *ledger.WeftEntry) (string, string) {
		if e.Kind == ledger.WeftEntryReconcile {
			return "", ""
		}
		p := c.byName[e.Project]
		if p == nil {
			return ledger.WeftInvalidBinding, fmt.Sprintf("project %s is no longer registered in %s", e.Project, s.cfg.Paths.TKConfig)
		}
		if p.RepositoryID != e.RepositoryID {
			now := "no checkout"
			if p.Repository != "" {
				now = p.Repository + " (" + p.RepositoryID + ")"
			}
			return ledger.WeftInvalidBinding, fmt.Sprintf("project %s is now bound to %s, not %s (%s), which %s was admitted for",
				e.Project, now, e.Repository, e.RepositoryID, e.ID)
		}
		if e.Kind == ledger.WeftEntryCleanup {
			return "", ""
		}
		t, ok := c.snap.Get(e.Ticket)
		if !ok {
			return ledger.WeftInvalidTicket, e.Ticket + " is no longer in tk"
		}
		if reasons := tkEligibility(c.snap, p, t); len(reasons) > 0 {
			return ledger.WeftInvalidTicket, describeIneligible(e.Ticket, reasons)
		}
		return "", ""
	}
}

func describeIneligible(id string, reasons []map[string]any) string {
	parts := make([]string, 0, len(reasons))
	for _, r := range reasons {
		if r["code"] == "status" {
			parts = append(parts, fmt.Sprintf("%s is %s in tk", id, r["detail"]))
			continue
		}
		parts = append(parts, fmt.Sprintf("%s is not eligible: %s (%s)", id, r["code"], r["detail"]))
	}
	return strings.Join(parts, "; ")
}

// dispatch is one pass after a commit that may have freed a slot, a
// reservation or a pause: a segment settled, or the daemon started.
func (s *Service) dispatch() {
	c, err := s.readCatalog()
	if err != nil {
		s.log.Printf("weft dispatch host=%s: nothing dispatched, tk could not be read: %v", s.hostID, err)
		return
	}
	s.dispatchWith(c)
}

func (s *Service) dispatchWith(c *catalog) {
	// worker_bin is re-probed when it changed (§2.6), so the capabilities a
	// client reads follow the installed worker.
	s.segments.refreshWorker()
	s.stateMu.Lock()
	spawn, changes, err := s.store.DispatchWeft(s.dispatchConfig(c))
	s.stateMu.Unlock()
	if err != nil {
		s.log.Printf("weft dispatch host=%s: %v", s.hostID, err)
		return
	}
	s.logChanges("", changes)
	s.spawn(spawn)
}

// Restart is §12's steps 3, 4 and 5, which follow the restart pass the
// daemon has already run (steps 1 and 2) and precede the listener and any
// dispatch; then the worker probe (§2.6), the retention prune (§8.1) and the
// Codex allowance probe, which runs in the background (§5.3). The reconciles
// step 2 claimed start here, with the runs step 5 minted, before dispatch.
func (s *Service) Restart() error {
	if err := s.interruptTerminals(); err != nil {
		return err
	}
	s.stateMu.Lock()
	released, changes, spawn, err := s.store.RestartWeftQueue(s.segments.dispatchBase(), archiveHead)
	s.stateMu.Unlock()
	if err != nil {
		return err
	}
	for _, r := range released {
		s.log.Printf("weft restart host=%s: released %s, held by gone %s %s since %s",
			s.hostID, r.Repository, r.HolderKind, r.HolderID, r.AcquiredAt)
	}
	s.logChanges("", changes)
	s.stateMu.Lock()
	err = s.pruneRetention()
	s.stateMu.Unlock()
	if err != nil {
		return err
	}
	s.segments.refreshWorker()
	s.probeCodex(true)
	s.spawn(spawn)
	return nil
}

// archiveHead is the commit ref names in repo, or "" when there is none.
func archiveHead(repo, ref string) (string, error) {
	out, err := exec.Command("git", "-C", repo, "rev-parse", "--verify", "--quiet", ref+"^{commit}").Output()
	var exit *exec.ExitError
	if errors.As(err, &exit) && exit.ExitCode() == 1 && len(out) == 0 {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(out)), nil
}

type revisionRequest struct {
	ContractVersion string `json:"contract_version"`
	Revision        int64  `json:"revision"`
}

func (s *Service) admission(key string) ledger.WeftAdmission {
	return ledger.WeftAdmission{TokenID: s.tokenID, RequestKey: key}
}

// handleLaunch admits one ticket (§6.3).
func (s *Service) handleLaunch(w http.ResponseWriter, r *http.Request) {
	s.mutateV2(w, r, func(body []byte, key string) (v2Commit, error) {
		var req struct {
			ContractVersion string  `json:"contract_version"`
			Ticket          string  `json:"ticket"`
			RepositoryID    *string `json:"repository_id"`
			Checkout        any     `json:"checkout"`
			Environment     any     `json:"environment"`
		}
		if err := decodeV2(body, &req, "ticket", "repository_id", "checkout", "environment"); err != nil {
			return refusedBy(err), nil
		}
		if req.Checkout != nil || req.Environment != nil {
			message, _ := s.notServedMessage("trial_launch")
			return refused("capability_not_served", "%s", message), nil
		}
		if req.RepositoryID == nil {
			return refused("malformed", "repository_id is required: only a checkout launch names none"), nil
		}
		project, slug, _ := strings.Cut(req.Ticket, "/")
		if slug == "" || strings.Contains(slug, "/") {
			return refused("malformed", "ticket must be a qualified project/id"), nil
		}
		c, err := s.readCatalog()
		if err != nil {
			return nil, err
		}
		p := c.byName[project]
		if p == nil {
			return refused("project_not_found", "%s is not registered on host %s", project, s.hostID), nil
		}
		t, ok := c.snap.Get(req.Ticket)
		if !ok {
			return refused("ticket_not_found", "%s was not found on host %s", req.Ticket, s.hostID), nil
		}
		if reasons := tkEligibility(c.snap, p, t); len(reasons) > 0 {
			return refused("ticket_not_eligible", "%s", describeIneligible(req.Ticket, reasons)), nil
		}
		if p.RepositoryID != *req.RepositoryID {
			return refused("binding_changed", "project %s is bound to %s (%s), not %s; read the project again",
				project, p.Repository, p.RepositoryID, *req.RepositoryID), nil
		}
		return func(tx *ledger.WeftTx) (v2Outcome, error) {
			a := s.admission(key)
			a.Kind, a.Ticket, a.Project, a.RepositoryID, a.Repository = ledger.WeftEntryLaunch, t.ID, p.Name, p.RepositoryID, p.Repository
			entry, err := tx.Admit(a)
			if err != nil {
				return v2Outcome{}, err
			}
			if err := tx.Dispatch(s.dispatchConfig(c)); err != nil {
				return v2Outcome{}, err
			}
			if entry, err = tx.Entry(entry.ID); err != nil {
				return v2Outcome{}, err
			}
			return v2Outcome{status: http.StatusAccepted, fields: map[string]any{"entry": entry.Resource()}}, nil
		}, nil
	})
}

// handleFrontier is the frontier preview (§6.4).
func (s *Service) handleFrontier(w http.ResponseWriter, project string) {
	if project == "" {
		s.v2Error(w, http.StatusBadRequest, "malformed", "GET /v2/frontier requires ?project=")
		return
	}
	v, err := s.readCatalogView()
	if err != nil {
		s.catalogError(w, err)
		return
	}
	p := v.byName[project]
	if p == nil {
		s.v2Error(w, http.StatusNotFound, "project_not_found", project+" is not registered on host "+s.hostID)
		return
	}
	tickets, admissible := v.frontier(p)
	s.read(w, v.asOf, map[string]any{"frontier": map[string]any{
		"project": p.Name, "catalog_revision": p.Revision, "tickets": tickets,
		"admissible": admissible, "capacity": v.capacity.Resource(),
	}})
}

// handleFrontierSubmit admits a confirmed frontier: only the admissible list
// the project shows now, at the revision the preview was computed at.
func (s *Service) handleFrontierSubmit(w http.ResponseWriter, r *http.Request) {
	s.mutateV2(w, r, func(body []byte, key string) (v2Commit, error) {
		var req struct {
			ContractVersion string   `json:"contract_version"`
			Project         string   `json:"project"`
			CatalogRevision int64    `json:"catalog_revision"`
			Tickets         []string `json:"tickets"`
		}
		if err := decodeV2(body, &req, "project", "catalog_revision", "tickets"); err != nil {
			return refusedBy(err), nil
		}
		v, err := s.readCatalogView()
		if err != nil {
			return nil, err
		}
		p := v.byName[req.Project]
		if p == nil {
			return refused("project_not_found", "%s is not registered on host %s", req.Project, s.hostID), nil
		}
		_, admissible := v.frontier(p)
		if req.CatalogRevision != p.Revision {
			return refused("revision_conflict", "the frontier of %s is at catalog revision %d and the confirmation was for %d. Preview again and confirm what is shown",
				p.Name, p.Revision, req.CatalogRevision), nil
		}
		if !slices.Equal(req.Tickets, admissible) {
			return refused("revision_conflict", "the frontier of %s at catalog revision %d admits [%s] and the confirmation named [%s]. Preview again and confirm what is shown",
				p.Name, p.Revision, strings.Join(admissible, ", "), strings.Join(req.Tickets, ", ")), nil
		}
		return func(tx *ledger.WeftTx) (v2Outcome, error) {
			items := make([]ledger.WeftAdmission, 0, len(admissible))
			for _, id := range admissible {
				a := s.admission(key)
				a.Ticket, a.Project, a.RepositoryID, a.Repository = id, p.Name, p.RepositoryID, p.Repository
				items = append(items, a)
			}
			selection, entries, err := tx.AdmitFrontier(p.Name, items)
			if err != nil {
				return v2Outcome{}, err
			}
			if err := tx.Dispatch(s.dispatchConfig(v.catalog)); err != nil {
				return v2Outcome{}, err
			}
			listed := make([]map[string]any, 0, len(entries))
			for _, e := range entries {
				if e, err = tx.Entry(e.ID); err != nil {
					return v2Outcome{}, err
				}
				listed = append(listed, e.Resource())
			}
			return v2Outcome{status: http.StatusAccepted, fields: map[string]any{"selection_id": selection, "entries": listed}}, nil
		}, nil
	})
}

func queueResource(q *ledger.WeftQueue) map[string]any {
	entries := make([]map[string]any, 0, len(q.Entries))
	for _, e := range q.Entries {
		entries = append(entries, e.Resource())
	}
	return map[string]any{"revision": q.Revision, "capacity": q.Capacity.Resource(), "entries": entries}
}

func (s *Service) handleQueue(w http.ResponseWriter) {
	s.stateMu.RLock()
	q, err := s.store.WeftQueue(time.Now().Add(-settledEntryWindow))
	var asOf int64
	if err == nil {
		asOf, err = s.store.WeftAsOf()
	}
	s.stateMu.RUnlock()
	if err != nil {
		s.v2Internal(w, err)
		return
	}
	s.read(w, asOf, map[string]any{"queue": queueResource(q)})
}

func (s *Service) handleCancel(w http.ResponseWriter, r *http.Request, entryID string) {
	s.mutateV2(w, r, func(body []byte, _ string) (v2Commit, error) {
		var req revisionRequest
		if err := decodeV2(body, &req, "revision"); err != nil {
			return refusedBy(err), nil
		}
		if !validProtocolID(entryID) {
			return refused("entry_not_found", "%s is not a queue entry on host %s", entryID, s.hostID), nil
		}
		return func(tx *ledger.WeftTx) (v2Outcome, error) {
			entry, err := tx.CancelEntry(entryID, req.Revision)
			if err != nil {
				return v2Outcome{}, err
			}
			return v2Outcome{status: http.StatusOK, fields: map[string]any{"entry": entry.Resource()}}, nil
		}, nil
	})
}

func (s *Service) handleClear(w http.ResponseWriter, r *http.Request) {
	s.mutateV2(w, r, func(body []byte, _ string) (v2Commit, error) {
		var req revisionRequest
		if err := decodeV2(body, &req, "revision"); err != nil {
			return refusedBy(err), nil
		}
		return func(tx *ledger.WeftTx) (v2Outcome, error) {
			if err := tx.ClearQueue(req.Revision); err != nil {
				return v2Outcome{}, err
			}
			q, err := tx.Queue(time.Now().Add(-settledEntryWindow))
			if err != nil {
				return v2Outcome{}, err
			}
			return v2Outcome{status: http.StatusOK, fields: map[string]any{"queue": queueResource(q)}}, nil
		}, nil
	})
}

// handleCapacity sets the cap (§6.1); a raise dispatches in the same
// transaction.
func (s *Service) handleCapacity(w http.ResponseWriter, r *http.Request) {
	s.mutateV2(w, r, func(body []byte, _ string) (v2Commit, error) {
		var req struct {
			ContractVersion string `json:"contract_version"`
			Cap             int    `json:"cap"`
			Revision        int64  `json:"revision"`
		}
		if err := decodeV2(body, &req, "cap", "revision"); err != nil {
			return refusedBy(err), nil
		}
		if req.Cap < 1 {
			return refused("malformed", "cap must be at least 1, not %d", req.Cap), nil
		}
		// Every live segment runs as its own agent user, so a cap above them
		// could never be met.
		if users := len(s.cfg.Weft.AgentUsers); req.Cap > users {
			return refused("malformed", "cap %d is more than the %d agent user(s) in weft.agent_users; each live segment runs as its own", req.Cap, users), nil
		}
		c, err := s.readCatalog()
		if err != nil {
			return nil, err
		}
		return func(tx *ledger.WeftTx) (v2Outcome, error) {
			if err := tx.SetCapacity(req.Cap, req.Revision); err != nil {
				return v2Outcome{}, err
			}
			if err := tx.Dispatch(s.dispatchConfig(c)); err != nil {
				return v2Outcome{}, err
			}
			q, err := tx.Queue(time.Now().Add(-settledEntryWindow))
			if err != nil {
				return v2Outcome{}, err
			}
			return v2Outcome{status: http.StatusOK, fields: map[string]any{"queue": queueResource(q)}}, nil
		}, nil
	})
}

func (s *Service) handleRunCommand(w http.ResponseWriter, r *http.Request, runID, command string) {
	s.mutateV2(w, r, func(body []byte, key string) (v2Commit, error) {
		switch command {
		case "answer":
			return s.answerCommit(body, key, runID)
		case "dismiss", "fresh":
			return s.retainedCommit(body, key, runID, command)
		}
		var req revisionRequest
		if err := decodeV2(body, &req, "revision"); err != nil {
			return refusedBy(err), nil
		}
		if !validProtocolID(runID) {
			return refused("run_not_found", "%s is not a run on host %s", runID, s.hostID), nil
		}
		if command == "stop" {
			return func(tx *ledger.WeftTx) (v2Outcome, error) {
				live, err := tx.StopRun(runID, req.Revision)
				if err != nil {
					return v2Outcome{}, err
				}
				run, err := tx.GetWeftRun(runID)
				if err != nil {
					return v2Outcome{}, err
				}
				resource, err := v2Run(run, tx)
				if err != nil {
					return v2Outcome{}, err
				}
				outcome := v2Outcome{status: http.StatusOK, fields: map[string]any{"run": resource}}
				if live {
					outcome.after = func() { go s.segments.Stop(runID) }
				}
				return outcome, nil
			}, nil
		}
		c, err := s.readCatalog()
		if err != nil {
			return nil, err
		}
		return func(tx *ledger.WeftTx) (v2Outcome, error) {
			var entry *ledger.WeftEntry
			var err error
			if command == "reconcile" {
				entry, err = tx.AdmitReconcile(runID, req.Revision, s.admission(key))
			} else {
				entry, err = tx.AdmitResume(runID, req.Revision, directoryExists, s.admission(key))
			}
			if err != nil {
				return v2Outcome{}, err
			}
			if err := tx.Dispatch(s.dispatchConfig(c)); err != nil {
				return v2Outcome{}, err
			}
			if entry, err = tx.Entry(entry.ID); err != nil {
				return v2Outcome{}, err
			}
			return v2Outcome{status: http.StatusAccepted, fields: map[string]any{"entry": entry.Resource()}}, nil
		}, nil
	})
}

// answerCommit records an answer once against its pending question and
// queues it (§6.5).
func (s *Service) answerCommit(body []byte, key, runID string) (v2Commit, error) {
	var req struct {
		ContractVersion string `json:"contract_version"`
		QuestionID      string `json:"question_id"`
		Answer          string `json:"answer"`
	}
	if err := decodeV2(body, &req, "question_id", "answer"); err != nil {
		return refusedBy(err), nil
	}
	if !validProtocolID(req.QuestionID) {
		return refused("malformed", "question_id is not well formed"), nil
	}
	if strings.TrimSpace(req.Answer) == "" || len(req.Answer) > maxAnswerBytes || !utf8.ValidString(req.Answer) {
		return refused("malformed", "answer must be non-empty UTF-8 of at most %d bytes", maxAnswerBytes), nil
	}
	if !validProtocolID(runID) {
		return refused("run_not_found", "%s is not a run on host %s", runID, s.hostID), nil
	}
	c, err := s.readCatalog()
	if err != nil {
		return nil, err
	}
	return func(tx *ledger.WeftTx) (v2Outcome, error) {
		question, entry, err := tx.RecordAnswer(runID, req.QuestionID, req.Answer, s.admission(key))
		if err != nil {
			return v2Outcome{}, err
		}
		if err := tx.Dispatch(s.dispatchConfig(c)); err != nil {
			return v2Outcome{}, err
		}
		if entry, err = tx.Entry(entry.ID); err != nil {
			return v2Outcome{}, err
		}
		if question, err = tx.WeftQuestion(question.ID); err != nil {
			return v2Outcome{}, err
		}
		return v2Outcome{status: http.StatusAccepted, fields: map[string]any{
			"question": v2Question(question), "entry": entry.Resource(),
		}}, nil
	}, nil
}

// retainedCommit queues cleanup (§6.8) or fresh work (§6.7) over a retained
// run, against the worktree — and for fresh, the head — the confirmation
// showed.
func (s *Service) retainedCommit(body []byte, key, runID, command string) (v2Commit, error) {
	type dismissRequest struct {
		ContractVersion    string `json:"contract_version"`
		Revision           int64  `json:"revision"`
		AuthorizedWorktree string `json:"authorized_worktree"`
	}
	var req struct {
		dismissRequest
		AuthorizedHead string `json:"authorized_head"`
	}
	var err error
	if command == "fresh" {
		err = decodeV2(body, &req, "revision", "authorized_worktree", "authorized_head")
	} else {
		err = decodeV2(body, &req.dismissRequest, "revision", "authorized_worktree")
	}
	if err != nil {
		return refusedBy(err), nil
	}
	// The head is what an archive of a run with none is written at (§6.7).
	if command == "fresh" && !commitPattern.MatchString(req.AuthorizedHead) {
		return refused("malformed", "authorized_head must be a commit: 40 lowercase hex digits"), nil
	}
	if !validProtocolID(runID) {
		return refused("run_not_found", "%s is not a run on host %s", runID, s.hostID), nil
	}
	c, err := s.readCatalog()
	if err != nil {
		return nil, err
	}
	return func(tx *ledger.WeftTx) (v2Outcome, error) {
		var entry *ledger.WeftEntry
		var err error
		if command == "fresh" {
			entry, err = tx.AdmitFresh(runID, req.Revision, req.AuthorizedWorktree, req.AuthorizedHead, s.admission(key))
		} else {
			entry, err = tx.AdmitCleanup(runID, req.Revision, req.AuthorizedWorktree, directoryExists, s.admission(key))
		}
		if err != nil {
			return v2Outcome{}, err
		}
		if err := tx.Dispatch(s.dispatchConfig(c)); err != nil {
			return v2Outcome{}, err
		}
		if entry, err = tx.Entry(entry.ID); err != nil {
			return v2Outcome{}, err
		}
		return v2Outcome{status: http.StatusAccepted, fields: map[string]any{"entry": entry.Resource()}}, nil
	}, nil
}

func directoryExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.IsDir()
}
