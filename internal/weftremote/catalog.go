package weftremote

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"syscall"

	"github.com/EnderRealm/ticket/v8/pkg/ticket"

	"shuttle/internal/ledger"
	"shuttle/internal/provider/tkconfig"
	"shuttle/internal/sandbox"
)

// Project states and unavailability reasons (weft-service/v2 §5.1).
const (
	projectAvailable   = "available"
	projectEmpty       = "empty"
	projectUnavailable = "unavailable"
)

// v2ProjectID is the contract's project id; a config.yaml entry outside it —
// Root's reserved _root among them — cannot be served as a project.
var v2ProjectID = regexp.MustCompile(`^[a-z0-9][a-z0-9-]*$`)

// catalogProject is one project registered in tk's config.yaml, its binding
// in weft.repositories resolved independently of its tickets: a namespace is
// always browsable, and only an available binding names a repository work may
// run in.
type catalogProject struct {
	Name         string
	State        string
	ReasonCode   string
	ReasonDetail string
	// Repository and RepositoryID are empty when the binding names no
	// checkout (no_binding, root_binding): no default working directory is
	// ever substituted for one.
	Repository   string
	RepositoryID string
	Tickets      []*ticket.Ticket
	Revision     int64
	digest       string
}

// catalog is one complete tk read: every registered project with its tickets,
// off one snapshot.
type catalog struct {
	store    *ticket.MultiStore
	snap     *ticket.Snapshot
	projects []*catalogProject
	byName   map[string]*catalogProject
}

// incompleteReadError is a tk read that may not hold every ticket there is.
// The catalog refuses it rather than serve a partial list a client would take
// for the whole one.
type incompleteReadError struct{ diagnostics []string }

func (e *incompleteReadError) Error() string {
	return "tk read is incomplete: " + strings.Join(e.diagnostics, "; ")
}

func (s *Service) readCatalog() (*catalog, error) {
	cfg, err := tkconfig.Load(s.cfg.Paths.TKConfig)
	if err != nil {
		return nil, err
	}
	store := ticket.NewMultiStore(cfg.TicketsRoot())
	snap, err := store.Snapshot()
	if err != nil {
		return nil, err
	}
	problems := snap.Diagnostics()
	if !snap.Complete && len(problems) == 0 {
		problems = append(problems, "the snapshot reports itself incomplete")
	}
	for _, name := range cfg.Names() {
		if !v2ProjectID.MatchString(name) {
			problems = append(problems, fmt.Sprintf("%s registers %q, which is not a project id this contract can serve", s.cfg.Paths.TKConfig, name))
		}
	}
	if len(problems) > 0 {
		return nil, &incompleteReadError{diagnostics: problems}
	}
	byNamespace := map[string][]*ticket.Ticket{}
	for _, t := range snap.Tickets {
		ns, _ := ticket.ParseNamespacedID(t.ID)
		byNamespace[ns] = append(byNamespace[ns], t)
	}
	homes := homeDirectories()
	c := &catalog{store: store, snap: snap, byName: map[string]*catalogProject{}}
	for _, name := range cfg.Names() {
		p := bindProject(name, s.cfg.Weft.Repositories[name], homes)
		p.Tickets = byNamespace[name]
		ticket.SortByStatusPriorityID(p.Tickets)
		if p.State == projectAvailable && len(p.Tickets) == 0 {
			p.State = projectEmpty
		}
		if p.digest, err = projectDigest(p, snap); err != nil {
			return nil, err
		}
		c.projects = append(c.projects, p)
		c.byName[name] = p
	}
	return c, nil
}

// repositoriesKey is shuttle's config.json key that binds each hosted project
// to its checkout; tk's own path for the project is never consulted.
const repositoriesKey = "weft.repositories"

// bindProject resolves a weft.repositories binding to the checkout it names,
// or to the reason it names none.
func bindProject(name, path string, homes []string) *catalogProject {
	p := &catalogProject{Name: name}
	unavailable := func(code, detail string) *catalogProject {
		p.State, p.ReasonCode, p.ReasonDetail = projectUnavailable, code, detail
		return p
	}
	if path == "" {
		return unavailable("no_binding", fmt.Sprintf("%s in shuttle's config.json binds no hosted checkout for %s", repositoriesKey, name))
	}
	if !filepath.IsAbs(path) {
		return unavailable("no_binding", fmt.Sprintf("%s binds %s to %q, which is not an absolute path", repositoriesKey, name, path))
	}
	rootBinding := func(path string) *catalogProject {
		return unavailable("root_binding", fmt.Sprintf("%s binds %s to %s; a binding to / or the home directory names no checkout", repositoriesKey, name, path))
	}
	clean := filepath.Clean(path)
	if clean == "/" || slices.Contains(homes, clean) {
		return rootBinding(clean)
	}
	// An agent can add entries to repos/, so a bound name can be squatted, or
	// swapped for a link elsewhere, after config.json was loaded. The entry is
	// judged as found, before anything is resolved: a link, a non-directory or
	// a foreign owner names no repository, so nothing (archive import included)
	// adopts the place it points.
	info, err := os.Lstat(clean)
	if errors.Is(err, fs.ErrNotExist) {
		p.Repository, p.RepositoryID = clean, sandbox.RepositoryID(clean)
		return unavailable("checkout_missing", fmt.Sprintf("%s, the path %s binds %s to, does not exist on this host", clean, repositoriesKey, name))
	}
	if err != nil {
		return unavailable("not_a_repository", fmt.Sprintf("%s, the path %s binds %s to, cannot be inspected: %v", clean, repositoriesKey, name, err))
	}
	if info.Mode()&fs.ModeSymlink != 0 {
		return unavailable("not_a_repository", fmt.Sprintf("%s, the path %s binds %s to, is a symlink; a hosted checkout is a directory of its own", clean, repositoriesKey, name))
	}
	if !info.IsDir() {
		return unavailable("not_a_repository", fmt.Sprintf("%s, the path %s binds %s to, is not a directory", clean, repositoriesKey, name))
	}
	if !ownedByDaemon(info) {
		return unavailable("not_a_repository", fmt.Sprintf("%s, the path %s binds %s to, is not owned by uid %d, which this host runs as; an agent may have made it",
			clean, repositoriesKey, name, os.Getuid()))
	}
	afterBindingInspected(clean)
	canonical, err := sandbox.CanonicalRepository(clean)
	if errors.Is(err, fs.ErrNotExist) {
		p.Repository, p.RepositoryID = clean, sandbox.RepositoryID(clean)
		return unavailable("checkout_missing", fmt.Sprintf("%s, the path %s binds %s to, does not exist on this host", clean, repositoriesKey, name))
	}
	if err != nil {
		p.Repository, p.RepositoryID = clean, sandbox.RepositoryID(clean)
		return unavailable("not_a_repository", fmt.Sprintf("%s, the path %s binds %s to, cannot be resolved: %v", clean, repositoriesKey, name, err))
	}
	if canonical == "/" || slices.Contains(homes, canonical) {
		return rootBinding(canonical)
	}
	// Each lookup walks the path afresh, so the entry swapped between them is
	// caught by identity: the resolved checkout must be the directory inspected,
	// still a direct child of the bound parent.
	parent, err := sandbox.CanonicalRepository(filepath.Dir(clean))
	resolved, statErr := os.Stat(canonical)
	if err != nil || statErr != nil || !os.SameFile(info, resolved) || filepath.Dir(canonical) != parent {
		return unavailable("not_a_repository", fmt.Sprintf("%s, the path %s binds %s to, resolved to %s, which is not the directory inspected there; it was replaced while being bound",
			clean, repositoriesKey, name, canonical))
	}
	p.Repository, p.RepositoryID = canonical, sandbox.RepositoryID(canonical)
	gitInfo, err := os.Lstat(filepath.Join(canonical, ".git"))
	if err != nil {
		return unavailable("not_a_repository", fmt.Sprintf("%s, the path %s binds %s to, has no .git: it is not a git checkout", canonical, repositoriesKey, name))
	}
	if gitInfo.Mode()&fs.ModeSymlink != 0 || !ownedByDaemon(gitInfo) {
		return unavailable("not_a_repository", fmt.Sprintf("%s/.git, in the path %s binds %s to, is a symlink or not owned by uid %d, which this host runs as; an agent may have made it",
			canonical, repositoriesKey, name, os.Getuid()))
	}
	p.State = projectAvailable
	return p
}

// afterBindingInspected runs between a binding's inspection and its
// resolution; a var so a test can replace the entry in that window.
var afterBindingInspected = func(string) {}

// ownedByDaemon is whether info's file belongs to the uid this process runs as.
func ownedByDaemon(info os.FileInfo) bool {
	st, ok := info.Sys().(*syscall.Stat_t)
	return ok && int(st.Uid) == os.Getuid()
}

// homeDirectories is the user's home as spelled and as resolved.
func homeDirectories() []string {
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return nil
	}
	homes := []string{filepath.Clean(home)}
	if canonical, err := sandbox.CanonicalRepository(home); err == nil {
		homes = append(homes, canonical)
	}
	return homes
}

// projectDigest is what catalog_revision is raised on: the binding, and every
// ticket's file content with the graph state tk derives for it, so an epic
// whose child changed elsewhere, or a dep closed in another project, moves the
// digest of the project that shows it.
func projectDigest(p *catalogProject, snap *ticket.Snapshot) (string, error) {
	h := sha256.New()
	fmt.Fprintf(h, "%s\x00%s\x00%s\x00%s\n", p.State, p.ReasonCode, p.ReasonDetail, p.Repository)
	tickets := slices.Clone(p.Tickets)
	slices.SortFunc(tickets, func(a, b *ticket.Ticket) int { return strings.Compare(a.ID, b.ID) })
	for _, t := range tickets {
		doc, err := ticket.Serialize(t)
		if err != nil {
			return "", fmt.Errorf("digest %s: %w", t.ID, err)
		}
		h.Write(doc)
		fmt.Fprintf(h, "\x00%s\x00%t\x00%s", t.Status, snap.IsBlocked(t.ID), ticket.RelationshipIssue(t))
		for _, dep := range qualifiedDeps(t) {
			if d, ok := snap.Get(dep); ok {
				fmt.Fprintf(h, "\x00%s=%s", dep, d.Status)
			}
		}
		h.Write([]byte{'\n'})
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// recordCatalog raises catalog_revision for every project whose digest moved,
// with its catalog event, and revalidates those projects' waiting entries
// against the read (§5.2, §6.3). The caller holds stateMu for writing.
func (s *Service) recordCatalog(c *catalog) error {
	var changes []ledger.WeftChange
	err := s.store.WeftTx(func(tx *ledger.WeftTx) error {
		if err := s.recordCatalogTx(tx, c); err != nil {
			return err
		}
		changes = tx.Changes
		return nil
	})
	if err != nil {
		return err
	}
	s.logChanges("", changes)
	return nil
}

// recordCatalogTx is recordCatalog inside a transaction the caller commits.
func (s *Service) recordCatalogTx(tx *ledger.WeftTx, c *catalog) error {
	bindings := make(map[string]ledger.WeftBinding, len(c.projects))
	for _, p := range c.projects {
		bindings[p.Name] = ledger.WeftBinding{Digest: p.digest, RepositoryID: p.RepositoryID, Repository: p.Repository}
	}
	revisions, changed, err := tx.SyncCatalog(bindings)
	if err != nil {
		return err
	}
	for _, p := range c.projects {
		p.Revision = revisions[p.Name]
	}
	if len(changed) == 0 {
		return nil
	}
	return tx.RevalidateEntries(changed, s.entryCheck(c))
}

// syncCatalog is one pass of the host watching tk.
func (s *Service) syncCatalog() (*catalog, error) {
	c, err := s.readCatalog()
	if err != nil {
		return nil, err
	}
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	return c, s.recordCatalog(c)
}

// catalogView is a catalog read with the host state its tickets are judged
// against, all at one as_of.
type catalogView struct {
	*catalog
	runs     map[string][]*ledger.WeftRun
	entries  map[string]ledger.WeftEntryRef
	capacity ledger.WeftCapacity
	asOf     int64
}

func (s *Service) readCatalogView() (*catalogView, error) {
	c, err := s.readCatalog()
	if err != nil {
		return nil, err
	}
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if err := s.recordCatalog(c); err != nil {
		return nil, err
	}
	v := &catalogView{catalog: c, runs: map[string][]*ledger.WeftRun{}}
	runs, err := s.store.WeftRuns("", "")
	if err != nil {
		return nil, err
	}
	for _, run := range runs {
		v.runs[run.Ticket] = append(v.runs[run.Ticket], run)
	}
	if v.entries, err = s.store.WeftPendingEntries(); err != nil {
		return nil, err
	}
	if v.capacity, err = s.store.WeftCapacity(); err != nil {
		return nil, err
	}
	if v.asOf, err = s.store.WeftAsOf(); err != nil {
		return nil, err
	}
	return v, nil
}

func (s *Service) catalogError(w http.ResponseWriter, err error) {
	var incomplete *incompleteReadError
	if !errors.As(err, &incomplete) {
		s.v2Internal(w, err)
		return
	}
	// The v2 error codes have no partial-read refusal; malformed with a 500 is
	// this service's internal-error answer, and the message names every file
	// and namespace tk could not read.
	s.log.Printf("weft catalog host=%s: refused: %v", s.hostID, err)
	s.v2Error(w, http.StatusInternalServerError, "malformed",
		"host "+s.hostID+" refuses to serve a partial catalog: "+err.Error())
}

func (s *Service) handleProjects(w http.ResponseWriter) {
	v, err := s.readCatalogView()
	if err != nil {
		s.catalogError(w, err)
		return
	}
	projects := make([]map[string]any, 0, len(v.projects))
	for _, p := range v.projects {
		var reason, repository, repositoryID any
		if p.ReasonCode != "" {
			reason = map[string]any{"code": p.ReasonCode, "detail": p.ReasonDetail}
		}
		if p.Repository != "" {
			repository, repositoryID = p.Repository, p.RepositoryID
		}
		counts := map[string]int{"backlog": 0, "ready": 0, "open": 0, "done": 0, "closed": 0, "epics": 0}
		for _, t := range p.Tickets {
			if _, ok := counts[string(t.Status)]; ok {
				counts[string(t.Status)]++
			}
			if t.Type == ticket.TypeEpic {
				counts["epics"]++
			}
		}
		projects = append(projects, map[string]any{
			"project": p.Name, "state": p.State, "reason": reason,
			"repository_id": repositoryID, "repository": repository,
			"catalog_revision": p.Revision, "counts": counts,
		})
	}
	s.read(w, v.asOf, map[string]any{"projects": projects})
}

func (s *Service) handleCatalogTickets(w http.ResponseWriter, project string) {
	if project == "" {
		s.v2Error(w, http.StatusBadRequest, "malformed", "GET /v2/tickets requires ?project=")
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
	rows := make([]map[string]any, 0, len(p.Tickets))
	for _, t := range p.Tickets {
		rows = append(rows, v.ticketRow(p, t))
	}
	s.read(w, v.asOf, map[string]any{"project": p.Name, "catalog_revision": p.Revision, "tickets": rows})
}

func (s *Service) handleCatalogTicket(w http.ResponseWriter, id string) {
	project, slug, _ := strings.Cut(id, "/")
	v, err := s.readCatalogView()
	if err != nil {
		s.catalogError(w, err)
		return
	}
	p := v.byName[project]
	t, ok := v.snap.Get(id)
	if p == nil || slug == "" || strings.Contains(slug, "/") || !ok {
		s.v2Error(w, http.StatusNotFound, "ticket_not_found", id+" was not found on host "+s.hostID)
		return
	}
	s.read(w, v.asOf, map[string]any{"ticket": v.ticketDetail(p, t)})
}

// ticketDetail is one ticket as GET /v2/tickets/{project}/{slug} reads it:
// its catalog row with its sections, its notes count and its runs.
func (v *catalogView) ticketDetail(p *catalogProject, t *ticket.Ticket) map[string]any {
	row := v.ticketRow(p, t)
	description, _, acceptance, _ := ticket.BodySections(t.Body)
	row["description"] = nullableText(strings.TrimSpace(description))
	row["acceptance"] = nullableText(strings.TrimSpace(acceptance))
	row["notes_total"] = len(t.Notes)
	runIDs := []string{}
	for _, run := range v.runs[t.ID] {
		runIDs = append(runIDs, run.ID)
	}
	row["runs"] = runIDs
	return row
}

// ticketRow is one catalog entry (§5.2): the ticket in whatever status it
// holds, with why it is or is not eligible beside it.
func (v *catalogView) ticketRow(p *catalogProject, t *ticket.Ticket) map[string]any {
	ns, _ := ticket.ParseNamespacedID(t.ID)
	var parent, branch, question, runRef, entryRef any
	if t.Parent != "" {
		parent = ticket.QualifyRef(ns, t.Parent)
	}
	if t.Branch != "" {
		branch = t.Branch
	}
	if q := strings.TrimSpace(t.Extra[ticket.QuestionField]); q != "" {
		question = q
	}
	// A dep that does not resolve has no status to show; it still blocks,
	// and eligibility names it.
	deps := []map[string]any{}
	for _, dep := range qualifiedDeps(t) {
		if d, ok := v.snap.Get(dep); ok {
			deps = append(deps, map[string]any{"id": dep, "status": string(d.Status)})
		}
	}
	runs := v.runs[t.ID]
	if len(runs) > 0 {
		runRef = v2RunRef(runs[0])
	}
	entry, hasEntry := v.entries[t.ID]
	if hasEntry {
		entryRef = map[string]any{"entry_id": entry.ID, "kind": entry.Kind, "state": entry.State}
	}
	reasons := v.ineligibility(p, t, runs, entry, hasEntry)
	state := "eligible"
	if len(reasons) > 0 {
		state = "ineligible"
	}
	return map[string]any{
		"id": t.ID, "project": p.Name, "title": t.Title, "status": string(t.Status),
		"type": string(t.Type), "priority": t.Priority, "updated": formatTime(t.Updated),
		"parent": parent, "deps": deps, "blocked": v.snap.IsBlocked(t.ID), "branch": branch,
		"question": question, "eligibility": map[string]any{"state": state, "reasons": reasons},
		"run": runRef, "entry": entryRef, "criteria_count": criteriaCount(t),
	}
}

// criteriaCount is how many acceptance criteria tk parses from the ticket.
func criteriaCount(t *ticket.Ticket) int {
	return len(ticket.ParseCriteria(ticket.AcceptanceCriteria(t.Body)))
}

// ineligibility lists why the host would not launch the ticket. What tk says
// about the ticket — its status, its kind, its criteria, its graph — comes
// first, read through pkg/ticket's own rules; what the host holds for it — a
// run, an entry, a retained branch — is only named once tk would let it
// launch, so a done ticket reads done rather than as the run that finished
// it. A project with no usable checkout makes every one of its tickets
// ineligible.
func (v *catalogView) ineligibility(p *catalogProject, t *ticket.Ticket, runs []*ledger.WeftRun, entry ledger.WeftEntryRef, hasEntry bool) []map[string]any {
	reasons := tkIneligibility(v.snap, t)
	add := func(code, detail string) {
		reasons = append(reasons, map[string]any{"code": code, "detail": detail})
	}
	if len(reasons) == 0 {
		for _, run := range runs {
			switch run.Phase {
			case ledger.WeftPhaseQueued:
				add("run_in_flight", "run "+run.ID+" is starting")
			case ledger.WeftPhaseRunning:
				add("run_in_flight", "run "+run.ID+" is running")
			case ledger.WeftPhaseParked:
				add("run_in_flight", "run "+run.ID+" is parked on a question")
			case ledger.WeftPhaseSettling:
				add("run_in_flight", "run "+run.ID+" is landing")
			}
		}
		if hasEntry {
			detail := "entry " + entry.ID + " is waiting to dispatch"
			if entry.State == "archiving" {
				detail = "entry " + entry.ID + " is archiving its prior run"
			}
			add("entry_waiting", detail)
		}
		for _, run := range runs {
			if run.Origin == ledger.WeftOriginHosted && run.Phase == ledger.WeftPhaseEnded && run.Disposition != ledger.WeftDispositionArchived {
				add("prior_run", "run "+run.ID+" retains branch "+run.Branch+"; start it fresh")
				break
			}
		}
	}
	if p.State == projectUnavailable {
		add("project_unavailable", p.ReasonDetail)
	}
	return reasons
}

// tkIneligibility is what tk itself says against launching the ticket — its
// status, its kind, its acceptance criteria, its graph — read through
// pkg/ticket's own rules.
func tkIneligibility(snap *ticket.Snapshot, t *ticket.Ticket) []map[string]any {
	reasons := []map[string]any{}
	add := func(code, detail string) {
		reasons = append(reasons, map[string]any{"code": code, "detail": detail})
	}
	if t.Status != ticket.StatusReady && t.Status != ticket.StatusOpen {
		add("status", string(t.Status))
	}
	if t.Type == ticket.TypeEpic {
		add("epic", "an epic is a container; launch its tickets")
	}
	// A ticket with no criteria has no contract for a run to be held to. Its
	// status or kind, where either already refuses it, is the reason to show;
	// criteria that are all unverifiable are still a contract.
	if len(reasons) == 0 && criteriaCount(t) == 0 {
		add("no_acceptance_criteria", "the ticket declares no acceptance criteria; add them before it can run")
	}
	if snap.IsBlocked(t.ID) {
		named := false
		for _, dep := range qualifiedDeps(t) {
			d, ok := snap.Get(dep)
			switch {
			case !ok:
				add("dependency_unresolved", dep+" does not resolve")
			case d.Status != ticket.StatusDone && d.Status != ticket.StatusClosed:
				add("dependency_unresolved", dep+" is "+string(d.Status))
			default:
				continue
			}
			named = true
		}
		if !named {
			add("dependency_unresolved", "a dependency does not resolve")
		}
	}
	// A parent relationship tk cannot place is a relationship to resolve
	// before the work is automatically runnable, as tk's frontier treats it.
	if issue := ticket.RelationshipIssue(t); issue != "" {
		add("dependency_unresolved", issue)
	}
	return reasons
}

// tkEligibility is tk's reasons with the project's unavailability: what a
// launch is refused ticket_not_eligible for, and dispatch invalidates on.
func tkEligibility(snap *ticket.Snapshot, p *catalogProject, t *ticket.Ticket) []map[string]any {
	reasons := tkIneligibility(snap, t)
	if p.State == projectUnavailable {
		reasons = append(reasons, map[string]any{"code": "project_unavailable", "detail": p.ReasonDetail})
	}
	return reasons
}

// frontier is tk's frontier for the project in tk's order (§6.4) — what
// `tk frontier --project` lists — each ticket with its eligibility, and the
// admissible subset in the same order.
func (v *catalogView) frontier(p *catalogProject) ([]map[string]any, []string) {
	var mine []*ticket.Ticket
	for _, t := range ticket.FrontierOf(v.store, v.store.ListFromSnapshot(v.snap)) {
		if ns, _ := ticket.ParseNamespacedID(t.ID); ns == p.Name {
			mine = append(mine, t)
		}
	}
	ticket.SortByPriorityID(mine)
	rows, admissible := []map[string]any{}, []string{}
	for _, t := range mine {
		row := v.ticketRow(p, t)
		rows = append(rows, map[string]any{"id": t.ID, "title": t.Title, "eligibility": row["eligibility"]})
		if row["eligibility"].(map[string]any)["state"] == "eligible" {
			admissible = append(admissible, t.ID)
		}
	}
	return rows, admissible
}

// qualifiedDeps is the ticket's deps as the qualified ids tk resolves them to.
func qualifiedDeps(t *ticket.Ticket) []string {
	ns, _ := ticket.ParseNamespacedID(t.ID)
	deps := make([]string, 0, len(t.Deps))
	for _, dep := range t.Deps {
		deps = append(deps, ticket.QualifyRef(ns, dep))
	}
	return deps
}
