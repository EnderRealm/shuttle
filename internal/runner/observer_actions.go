package runner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/EnderRealm/ticket/v8/pkg/ticket"

	"shuttle/internal/agentdef"
	"shuttle/internal/config"
	"shuttle/internal/ledger"
	observercontract "shuttle/internal/observer"
	"shuttle/internal/provider"
	"shuttle/internal/provider/tkconfig"
)

// Stages of one action's application. The record is written after every
// change of stage, so a crash leaves the exact stage behind and a later
// reconciliation resumes from it rather than from the start.
const (
	actionStagePending = "pending"
	actionStageRefused = "refused"
	actionStageDecided = "decided"
	actionStageClaimed = "claimed"
	actionStageWritten = "written"
	actionStageSettled = "settled"
)

// observerReopenActionID is the tk action id every observer reopen is written
// under. It is fixed per ticket on purpose: tk keeps one receipt per action id
// in the ticket file, replays an identical request under it and refuses a
// different one with ErrActionConflict. A second reopen proposal carries a
// different note, so it is refused by the ticket itself. That refusal is the
// once-per-ticket cap, durable in the file and replicated with it, rather than
// a counter shuttle would have to keep in step.
const observerReopenActionID = "observer-reopen"

// relatedTitleTerms is the number of distinct query terms (relatedTicketTerms)
// that must occur as whole words in an existing ticket's title for it to be
// taken as already tracking a discovery. Only the title counts: body and note
// text repeat terms freely. A heuristic, not an exactly-once guarantee. Exact
// retry identity is the finding key tk enforces; this is the explicit
// duplicate review in front of it.
const relatedTitleTerms = 2

const reasonTicketsNotConfigured = "ticket output is not configured for this observer"

// observerActionRecord is the durable outcome of one proposed action. The
// action itself is retained in observer/validated.json; the record carries
// what became of it and, on a refusal or failure, why.
//
// What the ticket store was read as is persisted, with the claim's request
// id, before the claim is attempted, and reused by every retry of the claim
// and the write, never re-read: Precondition is the token a note or reopen is
// held against; Prior is the ticket already holding a discovery's finding key;
// Related is the ticket a discovery notes instead of creating, with
// Precondition read from it. Created is set when this run's write made the
// discovery's ticket.
//
// Store and Project are the destination the decision was taken against — the
// tk store root the ticket was read from and, for a discovery, the project it
// is created in — persisted with the decision so a resume writes where the
// decision did, never where the registration points now.
type observerActionRecord struct {
	ActionID      string `json:"action_id"`
	Kind          string `json:"kind"`
	Ticket        string `json:"ticket,omitempty"`
	Project       string `json:"project,omitempty"`
	Store         string `json:"store,omitempty"`
	Stage         string `json:"stage"`
	Outcome       string `json:"outcome,omitempty"`
	Reason        string `json:"reason,omitempty"`
	Reference     string `json:"reference,omitempty"`
	At            string `json:"at,omitempty"`
	ClaimRequest  string `json:"claim_request,omitempty"`
	SettleRequest string `json:"settle_request,omitempty"`
	Precondition  string `json:"precondition,omitempty"`
	Prior         string `json:"prior_ticket,omitempty"`
	Related       string `json:"related_ticket,omitempty"`
	Replayed      bool   `json:"replayed,omitempty"`
	Created       bool   `json:"created,omitempty"`
}

// errActionFailed marks a failure that is recorded on the action at its stage
// after everything the runner could do for it: the loop goes on to the next
// action and the run fails once all are driven, so a reconciliation re-drives
// the failed record and finds the ones behind it settled rather than pending.
var errActionFailed = errors.New("the action failed at its stage")

func newObserverActionRecords(actions []*observercontract.Action) []observerActionRecord {
	records := make([]observerActionRecord, len(actions))
	for i, action := range actions {
		records[i] = observerActionRecord{
			ActionID: action.ID, Kind: action.Kind, Ticket: action.Ticket, Stage: actionStagePending,
		}
	}
	return records
}

// applyObserverActions applies every validated action after the assessments
// are recorded, resuming each from the stage its record holds. Authorization
// and the ticket write's outcome are recorded per action and never abort the
// run; a notebook the runner cannot reach after a replay, or a destination
// that moved under a decided action, is recorded as failed and returned once
// every action has been driven.
func (p *Pipeline) applyObserverActions(ctx context.Context, runID, runDir string, state *observerRunState, out *validatedObserverOutput, applied *observerApplied) error {
	if len(applied.Actions) != len(out.Actions) {
		return fmt.Errorf("apply record holds %d actions, validated output holds %d", len(applied.Actions), len(out.Actions))
	}
	var store ticket.Store
	root := ""
	if state.Registration.Tickets != nil {
		cfg, err := tkconfig.Load(p.Config.Paths.TKConfig)
		if err != nil {
			return err
		}
		identity := state.Definition.Identity
		root = cfg.TicketsRoot()
		store = ticket.WithSource(ticket.NewMultiStore(root), "observer:"+identity.Domain+"/"+identity.Observer)
	}
	// pkg/ticket warns rather than failing over a skipped file; those go to
	// the run's logger for the length of the apply, as tktickets does, rather
	// than to the daemon's stderr.
	restore := ticket.Warnf
	ticket.Warnf = func(format string, args ...any) { p.logger().Printf(strings.TrimRight(format, "\n"), args...) }
	defer func() { ticket.Warnf = restore }()

	var failed error
	for i, action := range out.Actions {
		record := &applied.Actions[i]
		if record.Stage == actionStageRefused || record.Stage == actionStageSettled {
			continue
		}
		err := p.applyObserverAction(ctx, runID, runDir, i, store, root, state, out, applied, action, record)
		if errors.Is(err, errActionFailed) {
			failed = errors.Join(failed, err)
			continue
		}
		if err != nil {
			return err
		}
	}
	revision, err := state.Notebook.Revision(ctx)
	if err != nil {
		return errors.Join(failed, fmt.Errorf("read notebook revision after actions: %w", err))
	}
	applied.NotebookRevision = revision
	if err := writeObserverApplied(p, runID, runDir, applied); err != nil {
		return err
	}
	return failed
}

func (p *Pipeline) applyObserverAction(ctx context.Context, runID, runDir string, index int, store ticket.Store, root string, state *observerRunState, out *validatedObserverOutput, applied *observerApplied, action *observercontract.Action, record *observerActionRecord) error {
	tickets := state.Registration.Tickets
	assessment := authorizingAssessment(out, action.QuestionID)
	refuse := func(reason string) error {
		record.Stage, record.Outcome, record.Reason = actionStageRefused, observercontract.ReceiptRefused, reason
		return writeObserverApplied(p, runID, runDir, applied)
	}
	// A mechanical failure ahead of the claim is not a decision about the
	// proposal: the action stays at its stage for a reconciliation to drive
	// again, and the run fails once the remaining actions are driven.
	fail := func(reason string) error {
		record.Outcome, record.Reason = observercontract.ReceiptFailed, reason
		if err := writeObserverApplied(p, runID, runDir, applied); err != nil {
			return err
		}
		return fmt.Errorf("action %s: %s: %w", action.ID, reason, errActionFailed)
	}
	if record.Stage == actionStagePending {
		record.Outcome, record.Reason = "", ""
		if reason := authorizeObserverAction(tickets, state, assessment, discoveriesDecided(applied.Actions), action); reason != "" {
			return refuse(reason)
		}
		// The assessment's outcome authorizes nothing until the notebook has
		// admitted the assessment: only a record receipt, persisted under the
		// request id the assessment's position derives, shows it did. A run
		// that failed on the record leaves the action here, and a
		// reconciliation admits it only after its replay of the record lands.
		if !observationRecorded(applied, assessmentRequestID(runID, slices.Index(out.Assessments, assessment))) {
			return fail("authorizing assessment was not recorded")
		}
		// The store is read once, and what was read is persisted with the
		// claim's request id before the claim is attempted: a ticket that is
		// gone or not in the state the proposal needs refuses the action with
		// no notebook write, and a ticket that changes after the read is
		// refused by tk against the recorded precondition rather than re-read
		// and accepted. Persisting ahead of the claim is what keeps that so
		// when the claim commits but its response is lost: the record already
		// holds the decision, so the resume replays the claim under the
		// recorded request id instead of deciding again from a fresh read.
		record.Store = root
		if action.Kind == observercontract.ActionTicketOpen {
			record.Project = tickets.DiscoveryProject
			if err := discoveryTarget(store, tickets, action, assessment.Finding, record); err != nil {
				return fail(err.Error())
			}
		} else {
			t, err := store.Get(action.Ticket)
			if err != nil {
				return fail("read ticket: " + err.Error())
			}
			if reason := reopenPrecondition(action, t); reason != "" {
				return refuse(reason)
			}
			record.Precondition = t.Precondition()
		}
		record.ClaimRequest = fmt.Sprintf("%s-claim-%03d", runID, index+1)
		record.Stage = actionStageDecided
		if err := writeObserverApplied(p, runID, runDir, applied); err != nil {
			return err
		}
	} else if store == nil || assessment == nil ||
		((action.Kind != observercontract.ActionTicketOpen || record.Related != "") && record.Precondition == "") {
		// Decided or claimed under a configuration or output that no longer
		// authorizes it, or without the read the write is held against: the
		// in-flight action is left for a reconciliation that can.
		return fmt.Errorf("action %s is %s but the configuration or retained output no longer authorizes it", action.ID, record.Stage)
	} else if record.Store != root || (action.Kind == observercontract.ActionTicketOpen && record.Project != tickets.DiscoveryProject) {
		// The decision — the duplicate search, the finding key's scope, the
		// precondition — was taken against one store and project. A
		// registration that now names another is not followed: the finding
		// key is project-scoped, so the landed write could not answer the
		// redirected one, and the other project never had the search.
		recorded, configured := record.Ticket, action.Ticket
		if action.Kind == observercontract.ActionTicketOpen {
			recorded, configured = record.Project, tickets.DiscoveryProject
		}
		return fail(fmt.Sprintf("destination changed since the decision: recorded %s at %s, configured %s at %s", recorded, record.Store, configured, root))
	}
	if record.Stage == actionStageDecided {
		record.Outcome, record.Reason = "", ""
		err := p.claimObserverAction(ctx, state, record.ClaimRequest, action)
		switch {
		case errors.Is(err, observercontract.ErrRetired):
			return refuse("the question was retired before the action was claimed")
		case errors.Is(err, observercontract.ErrActionHeld):
			return refuse("action id is already held by the notebook; a new proposal needs its own id")
		case errors.Is(err, observercontract.ErrStaleRevision):
			return fail("the notebook moved twice while the claim was decided: " + err.Error())
		case err != nil:
			return fmt.Errorf("claim action %s with request %s: %w", action.ID, record.ClaimRequest, fail("claim: "+err.Error()))
		}
		record.Stage = actionStageClaimed
		if err := writeObserverApplied(p, runID, runDir, applied); err != nil {
			return err
		}
	}
	if record.Stage == actionStageClaimed {
		p.writeObserverTicket(store, runID, state.Definition.Identity, tickets, action, assessment, record)
		record.Stage = actionStageWritten
		if err := writeObserverApplied(p, runID, runDir, applied); err != nil {
			return err
		}
	}
	if record.SettleRequest == "" {
		record.SettleRequest = fmt.Sprintf("%s-settle-%03d", runID, index+1)
	}
	receipt := &observercontract.Receipt{
		Envelope: observercontract.Envelope{ContractVersion: observercontract.ContractVersion},
		Identity: state.Definition.Identity, ActionID: action.ID,
		Status: observercontract.ReceiptSettled, Outcome: record.Outcome,
		Reference: record.Reference, At: record.At,
	}
	err := p.settleObserverAction(ctx, state, record.SettleRequest, receipt)
	if errors.Is(err, observercontract.ErrNotClaimed) {
		record.Outcome, record.Reason = observercontract.ReceiptFailed, "settle: "+err.Error()
		record.Stage = actionStageSettled
		return writeObserverApplied(p, runID, runDir, applied)
	}
	if err != nil {
		_ = writeObserverApplied(p, runID, runDir, applied)
		return fmt.Errorf("settle action %s with request %s: %w", action.ID, record.SettleRequest, err)
	}
	record.Stage = actionStageSettled
	return writeObserverApplied(p, runID, runDir, applied)
}

// authorizingAssessment is the one assessment in this run about the action's
// entry. None, or more than one, authorizes nothing.
func authorizingAssessment(out *validatedObserverOutput, entryID string) *observercontract.Assessment {
	var found *observercontract.Assessment
	for _, assessment := range out.Assessments {
		if assessment.EntryID() != entryID {
			continue
		}
		if found != nil {
			return nil
		}
		found = assessment
	}
	return found
}

// authorizeObserverAction is the shuttle-side rule set, decided from the
// retained snapshot and this run's validated output alone: no notebook call,
// no ticket read, and nothing taken from source prose. An empty reason
// authorizes the action.
func authorizeObserverAction(tickets *config.ObserverTickets, state *observerRunState, assessment *observercontract.Assessment, decided int, action *observercontract.Action) string {
	if tickets == nil {
		return reasonTicketsNotConfigured
	}
	if assessment == nil {
		return "no assessment in this run authorizes the action"
	}
	if assessment.Outcome == observercontract.OutcomeInsufficient {
		return "the authorizing assessment found insufficient evidence; an impression authorizes nothing"
	}
	if action.Kind == observercontract.ActionTicketOpen {
		if assessment.Finding == nil || assessment.Finding.ID != action.QuestionID {
			return "a discovery opens from a finding, not from a standing question"
		}
		if decided >= tickets.MaxDiscoveries {
			return fmt.Sprintf("this run already decided %d of at most %d discoveries", decided, tickets.MaxDiscoveries)
		}
		return ""
	}
	entry := snapshotEntry(state, action.QuestionID)
	if entry == nil {
		return "the entry was opened by a finding in this run, not a standing question with a ticket"
	}
	provenance := ""
	if entry.Question != nil {
		provenance = entry.Question.Ticket
	}
	if entry.Experiment != nil && entry.Experiment.Ticket != "" {
		provenance = entry.Experiment.Ticket
	}
	if provenance == "" {
		return "the entry has no ticket provenance"
	}
	if action.Ticket != provenance {
		return fmt.Sprintf("ticket %q is not the entry's provenance %q", action.Ticket, provenance)
	}
	project, _ := ticket.ParseNamespacedID(action.Ticket)
	if !slices.Contains(tickets.Projects, project) {
		return fmt.Sprintf("ticket %q is outside the configured projects", action.Ticket)
	}
	if action.Kind != observercontract.ActionTicketReopen {
		return ""
	}
	if assessment.Outcome != observercontract.OutcomeEffectAbsent {
		return fmt.Sprintf("a reopen needs %s; the assessment found %s", observercontract.OutcomeEffectAbsent, assessment.Outcome)
	}
	if entry.Experiment == nil {
		return "a reopen needs a live experiment; the entry declares none"
	}
	if entry.Experiment.Status() != observercontract.ExperimentLive {
		return fmt.Sprintf("a reopen needs a live experiment; the entry's is %s", entry.Experiment.Status())
	}
	if err := observercontract.CheckRequirements(entry, assessment); err != nil {
		return err.Error()
	}
	return ""
}

func snapshotEntry(state *observerRunState, id string) *observercontract.Entry {
	for _, entry := range state.Snapshot.Entries {
		if entry.ID == id {
			return entry
		}
	}
	return nil
}

// discoveriesDecided counts the discoveries this run has decided to create,
// against max_discoveries. Capacity is reserved at the decision, not at the
// write: a discovery counts from the moment its persisted decision is a
// creation — past pending, not refused, no prior ticket holding the finding
// key and no related ticket to note — whether its claim is still unresolved,
// its write landed, or its write failed. Prior and Related are decided
// before the claim and never change after it, so a record counted here never
// resolves as an earlier run's ticket or a related note; those are not
// creations and do not count. Counting only writes that landed let a
// discovery left failed at the claim, resumed by a reconciliation that does
// not authorize again, create beside one the run admitted in its place.
func discoveriesDecided(records []observerActionRecord) int {
	decided := 0
	for _, record := range records {
		if record.Kind == observercontract.ActionTicketOpen && record.Stage != actionStagePending &&
			record.Stage != actionStageRefused && record.Prior == "" && record.Related == "" {
			decided++
		}
	}
	return decided
}

// reopenPrecondition holds a reopen to a ticket that is done. Only the status
// and the receipts already on the ticket decide it; a writer label on either
// is attribution, not authentication, and is never read here.
func reopenPrecondition(action *observercontract.Action, t *ticket.Ticket) string {
	if action.Kind != observercontract.ActionTicketReopen || t.Status == ticket.StatusDone {
		return ""
	}
	if _, ok := tkReceipt(t, ticket.ReceiptApply, observerReopenActionID); ok {
		// The receipt decides: an identical replay lands, a different
		// proposal is refused by tk under the fixed id.
		return ""
	}
	if t.Status == ticket.StatusClosed {
		return fmt.Sprintf("ticket %s is closed; a closure is not reopened", t.ID)
	}
	return fmt.Sprintf("ticket %s is %s, not done", t.ID, t.Status)
}

func (p *Pipeline) claimObserverAction(ctx context.Context, state *observerRunState, requestID string, action *observercontract.Action) error {
	revision, err := state.Notebook.Revision(ctx)
	if err != nil {
		return err
	}
	request := observercontract.Request{Identity: state.Definition.Identity, Revision: revision, RequestID: requestID}
	_, err = state.Notebook.Claim(ctx, request, action)
	if errors.Is(err, observercontract.ErrStaleRevision) {
		// The notebook moved between the read and the claim; decide once more
		// against the revision it is at now.
		if request.Revision, err = state.Notebook.Revision(ctx); err != nil {
			return err
		}
		_, err = state.Notebook.Claim(ctx, request, action)
	}
	if err != nil && !notebookRefusal(err) {
		// The adapter may have committed before its process failed. Replay
		// the identical request id and revision once; a conforming adapter
		// returns the original receipt without a second claim.
		_, err = state.Notebook.Claim(ctx, request, action)
	}
	return err
}

func (p *Pipeline) settleObserverAction(ctx context.Context, state *observerRunState, requestID string, receipt *observercontract.Receipt) error {
	revision, err := state.Notebook.Revision(ctx)
	if err != nil {
		return err
	}
	request := observercontract.Request{Identity: state.Definition.Identity, Revision: revision, RequestID: requestID}
	_, err = state.Notebook.Settle(ctx, request, receipt)
	if errors.Is(err, observercontract.ErrStaleRevision) {
		if request.Revision, err = state.Notebook.Revision(ctx); err != nil {
			return err
		}
		_, err = state.Notebook.Settle(ctx, request, receipt)
	}
	if err != nil && !notebookRefusal(err) {
		_, err = state.Notebook.Settle(ctx, request, receipt)
	}
	return err
}

func notebookRefusal(err error) bool {
	for _, sentinel := range []error{
		observercontract.ErrStaleRevision, observercontract.ErrRetired, observercontract.ErrNoEntry,
		observercontract.ErrNotClaimed, observercontract.ErrRequestConflict, observercontract.ErrActionHeld,
	} {
		if errors.Is(err, sentinel) {
			return true
		}
	}
	return false
}

// writeObserverTicket performs the one tk write an action proposes and fills
// the record with the receipt outcome. Notes are the whole write surface: a
// note on the entry's ticket, the reopen transition tk permits, or a backlog
// ticket a discovery creates. No description, acceptance or design is edited
// on any path.
func (p *Pipeline) writeObserverTicket(store ticket.Store, runID string, identity observercontract.Identity, tickets *config.ObserverTickets, action *observercontract.Action, assessment *observercontract.Assessment, record *observerActionRecord) {
	var result *ticket.ActionResult
	var err error
	switch action.Kind {
	case observercontract.ActionTicketNote:
		result, err = ticket.ApplyAction(store, ticket.ActionRequest{
			ID: action.Ticket, ActionID: action.ID, Precondition: record.Precondition,
			Note: observerNoteText(runID, identity, action, assessment), Transition: ticket.TransitionNone,
		})
	case observercontract.ActionTicketReopen:
		result, err = ticket.ApplyAction(store, ticket.ActionRequest{
			ID: action.Ticket, ActionID: observerReopenActionID, Precondition: record.Precondition,
			Note: observerNoteText(runID, identity, action, assessment), Transition: ticket.TransitionReopen,
		})
	case observercontract.ActionTicketOpen:
		result, err = p.discoverObserverTicket(store, runID, identity, tickets, action, assessment, record)
	default:
		record.Outcome, record.Reason, record.At = observercontract.ReceiptFailed, fmt.Sprintf("unsupported action kind %q", action.Kind), stamp(p.now())
		return
	}
	switch {
	case err == nil:
		record.Outcome, record.Replayed, record.At = observercontract.ReceiptApplied, result.Replayed, result.Receipt.At
		record.Ticket = action.Ticket
		if action.Kind == observercontract.ActionTicketOpen {
			record.Ticket = result.Ticket.ID
			record.Created = record.Related == "" && (!result.Replayed || record.Prior == "")
		}
		record.Reference = record.Ticket + "@" + result.Receipt.At
		return
	case errors.Is(err, ticket.ErrConflict):
		record.Outcome, record.Reason = observercontract.ReceiptRefused, "the ticket changed since it was read; re-read it before deciding again"
	case errors.Is(err, ticket.ErrActionConflict) && action.Kind == observercontract.ActionTicketReopen:
		record.Outcome, record.Reason = observercontract.ReceiptRefused, "the ticket was already reopened once by an observer"
	case errors.Is(err, ticket.ErrActionConflict), errors.Is(err, ticket.ErrTransitionNotPermitted):
		record.Outcome, record.Reason = observercontract.ReceiptRefused, err.Error()
	default:
		record.Outcome, record.Reason = observercontract.ReceiptFailed, err.Error()
	}
	record.At = stamp(p.now())
}

// discoveryTarget decides, before the claim, what a discovery's write is: a
// retry under a finding key a ticket already holds, a note on a related
// ticket, or a creation. The decision is persisted with the claim so every
// retry performs the same write. A ticket already created under the key is
// answered by the key rather than by the search — the ticket it created would
// otherwise be found as its own duplicate.
func discoveryTarget(store ticket.Store, tickets *config.ObserverTickets, action *observercontract.Action, finding *observercontract.Finding, record *observerActionRecord) error {
	all, err := store.List()
	if err != nil {
		return fmt.Errorf("search %s for a related ticket: %w", tickets.DiscoveryProject, err)
	}
	var candidates []*ticket.Ticket
	for _, t := range all {
		if project, _ := ticket.ParseNamespacedID(t.ID); project != tickets.DiscoveryProject {
			continue
		}
		if _, ok := tkReceipt(t, ticket.ReceiptDiscover, finding.ID); ok {
			record.Prior = t.ID
			return nil
		}
		candidates = append(candidates, t)
	}
	if related := relatedTicket(candidates, relatedTicketTerms(action.Title, finding.Question)); related != nil {
		record.Related, record.Precondition = related.ID, related.Precondition()
	}
	return nil
}

// discoverObserverTicket performs the write discoveryTarget decided: the
// evidence as a note under the action's own id on the related ticket, else
// the creation under the finding key, which answers a retry with the ticket
// the key already created.
func (p *Pipeline) discoverObserverTicket(store ticket.Store, runID string, identity observercontract.Identity, tickets *config.ObserverTickets, action *observercontract.Action, assessment *observercontract.Assessment, record *observerActionRecord) (*ticket.ActionResult, error) {
	if record.Related != "" {
		record.Reason = fmt.Sprintf("related ticket %s noted instead of creating", record.Related)
		return ticket.ApplyAction(store, ticket.ActionRequest{
			ID: record.Related, ActionID: action.ID, Precondition: record.Precondition,
			Note: observerNoteText(runID, identity, action, assessment), Transition: ticket.TransitionNone,
		})
	}
	return ticket.CreateDiscovery(store, observerDiscovery(p.now(), identity, tickets, action, assessment))
}

// relatedTicket is the candidate whose title shares the most query terms,
// when that is at least relatedTitleTerms; whole tokens of the title, so
// "line" does not match "pipeline".
func relatedTicket(candidates []*ticket.Ticket, terms []string) *ticket.Ticket {
	var related *ticket.Ticket
	best := 0
	for _, t := range candidates {
		title := ticket.Tokenize(t.Title)
		shared := 0
		for _, term := range terms {
			if slices.Contains(title, term) {
				shared++
			}
		}
		if shared >= relatedTitleTerms && shared > best {
			related, best = t, shared
		}
	}
	return related
}

// observerDiscovery is the backlog ticket a finding is filed as, built the way
// tk's own create builds one, under the finding's id as its key.
func observerDiscovery(now time.Time, identity observercontract.Identity, tickets *config.ObserverTickets, action *observercontract.Action, assessment *observercontract.Assessment) ticket.DiscoveryRequest {
	return ticket.DiscoveryRequest{
		FindingKey: assessment.Finding.ID,
		Ticket: &ticket.Ticket{
			ID:       ticket.FormatNamespacedID(tickets.DiscoveryProject, ticket.GenerateIDFrom(action.Title, now)),
			Title:    action.Title,
			Status:   ticket.StatusBacklog,
			Type:     ticket.TypeFeature,
			Priority: 2,
			Tags:     []string{"observer"},
			Created:  now.UTC(),
			Body:     observerDiscoveryBody(identity, action, assessment),
		},
	}
}

func tkReceipt(t *ticket.Ticket, kind ticket.ReceiptKind, id string) (ticket.ActionReceipt, bool) {
	for _, receipt := range t.Actions {
		if receipt.Kind == kind && receipt.ID == id {
			return receipt, true
		}
	}
	return ticket.ActionReceipt{}, false
}

// relatedTicketTerms is the duplicate search's query: the distinct terms of
// four or more characters in the proposal's title and the finding's question.
// tk's tokenizer keeps every word, and the function words ("the", "in", "a")
// would put two title hits on nearly any ticket.
func relatedTicketTerms(title, question string) []string {
	var terms []string
	for _, term := range ticket.Tokenize(title + " " + question) {
		if len(term) >= 4 && !slices.Contains(terms, term) {
			terms = append(terms, term)
		}
	}
	return terms
}

// observerNoteText composes the note from validated fields: who observed what
// under which run, the proposal's prose, and the references a reader follows
// to check it.
func observerNoteText(runID string, identity observercontract.Identity, action *observercontract.Action, assessment *observercontract.Assessment) string {
	var b strings.Builder
	fmt.Fprintf(&b, "Observer %s/%s on %s: %s (run %s)\n\n", identity.Domain, identity.Observer, singleLine(action.QuestionID), assessment.Outcome, runID)
	b.WriteString(inertProse(action.Body))
	b.WriteString("\n\n")
	observerEvidenceList(&b, action, assessment)
	return b.String()
}

// inertProse is the model's prose with every line tk's ticket format would
// read as structure escaped so it reads back as text. Line endings are first
// normalised the way tk's scanner reads them: splitFrontmatter goes through
// bufio.Scanner, which strips the `\r` of a `\r\n`, so a delimiter under a
// CRLF ending is one tk splits on even though a split on `\n` alone would
// leave it hidden behind the `\r`; a bare `\r` is taken as a line break too.
// The rule is tk's own grammar (pkg/ticket/format.go): a line opening with
// `## ` is a heading — `## Notes` ends the body and opens the notes,
// `## Review Log` ends the notes and is dropped on the next write, and any
// other heading bounds a BodySections section — and a line of the shape
// `**<RFC3339>**` is the delimiter parseNotes splits notes on. Either,
// arriving in a note or a discovery body, would hide the evidence or criteria
// composed after it. The escape is Markdown's backslash, which tk stores and
// parses back unchanged, so the composed text round-trips with the same
// sections and note count. Only those two shapes are escaped: the criterion
// and every evidence value go through singleLine instead, and the title is
// written after `# ` on its own line, so none of them can form a line tk
// classifies.
func inertProse(text string) string {
	text = strings.ReplaceAll(text, "\r\n", "\n")
	text = strings.ReplaceAll(text, "\r", "\n")
	lines := strings.Split(strings.TrimSpace(text), "\n")
	for i, line := range lines {
		if strings.HasPrefix(line, "## ") || noteDelimiter(line) {
			lines[i] = `\` + line
		}
	}
	return strings.Join(lines, "\n")
}

// noteDelimiter matches a line the way tk's parseNotes does before it takes
// the line as the start of another note.
func noteDelimiter(line string) bool {
	if !strings.HasPrefix(line, "**") || !strings.HasSuffix(line, "**") {
		return false
	}
	_, err := time.Parse(time.RFC3339, strings.Trim(line, "*"))
	return err == nil
}

// singleLine renders a value as one literal line: every run of whitespace,
// line breaks of either ending included, collapses to one space. A value that
// cannot hold a line break cannot open a line tk classifies — a heading, a
// note delimiter, or in the acceptance section a further `- ` criterion or a
// `verify:` command ParseCriteria would attach to the one before it. The
// evidence values are validated for presence only, the finding's changes are
// prose, and an entry id's name segment is any non-empty string; all are
// written through this rather than trusted to be one line.
func singleLine(value string) string {
	return strings.Join(strings.Fields(value), " ")
}

func observerEvidenceList(b *strings.Builder, action *observercontract.Action, assessment *observercontract.Assessment) {
	b.WriteString("Evidence:\n")
	for _, source := range action.Evidence {
		fmt.Fprintf(b, "- %s: %s\n", singleLine(source.Kind), singleLine(source.Ref))
	}
	window := assessment.Evidence.Window
	fmt.Fprintf(b, "- window: %s to %s (%s)\n", singleLine(window.Start), singleLine(window.End), singleLine(window.Timezone))
	fmt.Fprintf(b, "- snapshot: %s\n", singleLine(assessment.Evidence.Scope.Snapshot))
}

// observerDiscoveryBody is the body a discovery ticket is created with, in the
// sections tk's own create writes: the finding's question and the proposal as
// the description, its consequence as a criterion that a person confirms, and
// the evidence under its own heading. The run id is deliberately absent: tk
// digests the body under the finding key, and a retry from a later run has to
// produce the same bytes to be answered with the ticket already created.
func observerDiscoveryBody(identity observercontract.Identity, action *observercontract.Action, assessment *observercontract.Assessment) string {
	var b strings.Builder
	b.WriteString(inertProse(assessment.Finding.Question) + "\n\n" + inertProse(action.Body) + "\n")
	b.WriteString("\n## Acceptance Criteria\n\n- " + singleLine(assessment.Finding.Changes) + "\n  unverifiable: observer finding; confirm by hand\n")
	b.WriteString("\n## Evidence\n\n")
	fmt.Fprintf(&b, "Observer %s/%s on %s: %s\n\n", identity.Domain, identity.Observer, singleLine(action.QuestionID), assessment.Outcome)
	observerEvidenceList(&b, action, assessment)
	return b.String()
}

// ReconcileObserverActions resumes the apply stage of a run whose assessments
// or actions did not all land: a lost adapter response, a crash between a
// claim and its ticket write, or between the write and its settlement. Every
// step is keyed by a durable id — request ids on the notebook side, action
// ids and finding keys on tk's — so a step that landed is answered with its
// original outcome rather than performed twice. An assessment without a
// record receipt is replayed first under its original request id; one the
// notebook still refuses is returned as the failure it is, after the actions
// are driven, and the action it would have authorized stays unadmitted.
//
// Only a failed run is reconciled, as RetryDelivery takes only a completed
// one. A failed run whose records are all terminal makes no notebook call —
// the retained notebook_revision stands — and goes straight to the finish, so
// a reconciliation whose finish failed part-way is healed by running it again.
// A reconciliation that converges finishes the run the way a live one does:
// the report is prepared, observer-result.json is rewritten from the retained
// collection and the reconciled records, the row moves to completed and the
// report is delivered. One that does not records its counts as
// conflict-or-failure and leaves the row failed.
func (p *Pipeline) ReconcileObserverActions(ctx context.Context, def *agentdef.Definition, row *ledger.Run) error {
	if row.Status != ledger.StatusFailed || !row.RunDir.Valid {
		return fmt.Errorf("run %s: only a failed run with a retained directory is reconciled; it is %s", row.ID, row.Status)
	}
	runDir := row.RunDir.String
	recordData, err := os.ReadFile(filepath.Join(runDir, "run.json"))
	if err != nil {
		return err
	}
	var retained runRecord
	if err := json.Unmarshal(recordData, &retained); err != nil {
		return fmt.Errorf("retained run.json: %w", err)
	}
	if retained.Observer == nil {
		return fmt.Errorf("run %s is not an observer run", row.ID)
	}
	// The report is prepared and delivered under the capabilities the run
	// retained, as a delivery retry is, not whatever the definition says now.
	retainedDef := *def
	retainedDef.Manifest.Outputs = retained.Outputs
	retainedDef.Manifest.Observer = &agentdef.Observer{Config: retained.Observer.Config}
	registration, err := observerRegistration(p.Config, &retainedDef)
	if err != nil {
		return err
	}
	if err := validateObserverRegistration(registration, def.Manifest.Runtime.Kind, p.Config.Paths.TKConfig); err != nil {
		return err
	}
	if len(def.Manifest.Triggers) == 0 {
		return fmt.Errorf("%s: triggers: none", def.Dir)
	}
	loc, err := time.LoadLocation(def.Manifest.Triggers[0].Timezone)
	if err != nil {
		return fmt.Errorf("%s: triggers[0].timezone: %w", def.Dir, err)
	}
	_, definition, _, err := loadObserverDefinition(filepath.Join(runDir, "observer", "definition.json"))
	if err != nil {
		return fmt.Errorf("retained observer definition: %w", err)
	}
	snapshotData, err := readBounded(filepath.Join(runDir, "observer", "notebook.json"), maxCollectorResponse)
	if err != nil {
		return err
	}
	var snapshot observerSnapshot
	if err := strictJSON(snapshotData, &snapshot); err != nil {
		return fmt.Errorf("retained notebook snapshot: %w", err)
	}
	validated, err := loadValidatedObserverOutput(runDir)
	if err != nil {
		return err
	}
	appliedData, err := os.ReadFile(filepath.Join(runDir, appliedFileName()))
	if err != nil {
		return err
	}
	var applied observerApplied
	if err := json.Unmarshal(appliedData, &applied); err != nil {
		return err
	}
	if observerDeliveryEligible(&retainedDef, validated) {
		if err := validateObserverSubject(validated.Subject); err != nil {
			return err
		}
	}
	collectionData, err := readBounded(filepath.Join(runDir, "observer", "evidence.json"), maxCollectorResponse)
	if err != nil {
		return err
	}
	collection, err := validateCollection(collectionData, definition.Identity)
	if err != nil {
		return fmt.Errorf("retained collection: %w", err)
	}
	slot, err := time.Parse(time.RFC3339, retained.Slot)
	if err != nil {
		return fmt.Errorf("retained run.json slot %q: %w", retained.Slot, err)
	}
	window, err := provider.Resolve(provider.WindowPreviousDay, slot, loc)
	if err != nil {
		return err
	}
	state := &observerRunState{
		Registration: registration, Definition: definition,
		Notebook: newObserverNotebook(registration, domainCommandEnv(p.HomeDir, loc)),
		Snapshot: snapshot, Collection: collection,
	}
	if observerReconcilable(row.ID, validated, &applied) {
		var recordErr error
		for i, assessment := range validated.Assessments {
			if observationRecorded(&applied, assessmentRequestID(row.ID, i)) {
				continue
			}
			if recordErr = p.recordObserverAssessment(ctx, row.ID, runDir, state, &applied, i, assessment); recordErr != nil {
				// Each later record's revision assumes this one landed.
				break
			}
		}
		if err := errors.Join(recordErr, p.applyObserverActions(ctx, row.ID, runDir, state, validated, &applied)); err != nil {
			return errors.Join(err, writeObserverResult(runDir, observerOutcome(state, validated, &applied, "conflict-or-failure", err.Error())))
		}
	}

	run := &preparedRun{ID: row.ID, Dir: runDir, Window: window, Loc: loc}
	if err := p.finishReconciledObserver(&retainedDef, row, run, state, validated, &applied); err != nil {
		// Every record has converged, so the counts are the reconciled ones;
		// the detail and the row still failed say the finish did not.
		return errors.Join(err, writeObserverResult(runDir, observerOutcome(state, validated, &applied, "applied", err.Error())))
	}
	if err := p.deliverObserverReport(ctx, &retainedDef, row, run, validated, &applied); err != nil {
		return fmt.Errorf("observer delivery: %w", err)
	}
	return nil
}

// finishReconciledObserver is the finish of a converged reconciliation, in
// the order a live run takes it. result.json reads completed only once the
// row does, so a failure anywhere here leaves both failed for the next
// reconciliation to finish.
func (p *Pipeline) finishReconciledObserver(def *agentdef.Definition, row *ledger.Run, run *preparedRun, state *observerRunState, out *validatedObserverOutput, applied *observerApplied) error {
	if err := p.prepareObserverReport(def, row, run, Options{}, out, applied); err != nil {
		return err
	}
	if err := writeObserverResult(run.Dir, observerOutcome(state, out, applied, "applied", "")); err != nil {
		return err
	}
	resultData, err := os.ReadFile(filepath.Join(run.Dir, resultFile))
	if err != nil {
		return err
	}
	var record resultRecord
	if err := json.Unmarshal(resultData, &record); err != nil {
		return fmt.Errorf("retained %s: %w", resultFile, err)
	}
	if err := p.Ledger.Reconciled(row.ID, out.Silent); err != nil {
		return err
	}
	record.Status, record.FailureClass, record.Detail = ledger.StatusCompleted, "", ""
	record.Silent = boolPointer(out.Silent)
	return writeResult(run.Dir, record)
}

// observerReconcilable reports whether a reconciliation has anything to
// drive: an assessment without its record receipt, or an action short of
// settled or refused. A run whose records are all terminal makes no notebook
// call, so nothing moves the revision it retained.
func observerReconcilable(runID string, out *validatedObserverOutput, applied *observerApplied) bool {
	for i := range out.Assessments {
		if !observationRecorded(applied, assessmentRequestID(runID, i)) {
			return true
		}
	}
	for _, record := range applied.Actions {
		if record.Stage != actionStageRefused && record.Stage != actionStageSettled {
			return true
		}
	}
	return false
}

// observerActionCounts folds the action records into the outcome counts
// observer-result.json shows.
func observerActionCounts(records []observerActionRecord) (applied, refused, failed int) {
	for _, record := range records {
		switch record.Outcome {
		case observercontract.ReceiptApplied:
			applied++
		case observercontract.ReceiptRefused:
			refused++
		case observercontract.ReceiptFailed:
			failed++
		}
	}
	return applied, refused, failed
}
