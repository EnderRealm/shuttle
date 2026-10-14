package weftremote

import (
	"fmt"
	"slices"

	"shuttle/internal/ledger"
)

// The landing, reconcile, cleanup and archive events of weft-worker/v2
// (§7, §11.3). The host takes no landing step: it holds each report to what
// it recorded and acknowledges a step only once its intent is durable.

type landingReport struct {
	State  string  `json:"state"`
	Step   *string `json:"step"`
	Reason *string `json:"reason"`
	Detail *string `json:"detail"`
	Branch *string `json:"branch"`
	Commit *string `json:"commit"`
}

func (l landingReport) landing() ledger.WeftLanding {
	return ledger.WeftLanding{State: l.State, Step: text(l.Step), Reason: text(l.Reason), Detail: text(l.Detail),
		Branch: text(l.Branch), Commit: text(l.Commit)}
}

type landIntentEvent struct {
	envelope
	Step   string `json:"step"`
	Branch string `json:"branch"`
	Commit string `json:"commit"`
	Base   string `json:"base"`
}

type landStepResultEvent struct {
	envelope
	Step   string  `json:"step"`
	Result string  `json:"result"`
	Branch string  `json:"branch"`
	Commit string  `json:"commit"`
	Detail *string `json:"detail"`
}

type observedEvent struct {
	envelope
	Outcome     *string `json:"outcome"`
	Branch      *string `json:"branch"`
	Commit      *string `json:"commit"`
	FastForward string  `json:"fast_forward"`
	Push        string  `json:"push"`
	MarkDone    string  `json:"mark_done"`
	Detail      *string `json:"detail"`
}

type reconciledEvent struct {
	envelope
	Landing landingReport `json:"landing"`
}

type maintainedEvent struct {
	envelope
	Op              string  `json:"op"`
	Result          string  `json:"result"`
	WorktreeRemoved bool    `json:"worktree_removed"`
	ArchiveRef      *string `json:"archive_ref"`
	Head            *string `json:"head"`
	Detail          *string `json:"detail"`
}

// reconcileHandlers are a reconcile's events: observed first, the steps it
// takes under fresh intents, and reconciled.
func (r *segmentStream) reconcileHandlers() map[string]eventHandler {
	return map[string]eventHandler{
		"observed": r.observedEvent, "land_intent": r.landIntent, "land_step_result": r.landStepResult,
		"reconciled": r.reconciled,
	}
}

// maintenanceEnvelope checks a reconcile, cleanup or archive event, whose
// worker_run_id is null throughout (§11.2).
func (r *segmentStream) maintenanceEnvelope(e envelope) error {
	if err := validateEnvelopeAt(e, r.contract, r.run, r.segment, r.seq, "", true); err != nil {
		return err
	}
	if e.WorkerRunID != "" {
		return fmt.Errorf("worker_run_id is not null in a %s segment", r.segment.Kind)
	}
	return nil
}

func (r *segmentStream) landEnvelope(e envelope) error {
	if r.segment.Kind == ledger.WeftSegmentReconcile {
		return r.maintenanceEnvelope(e)
	}
	if err := r.envelope(e); err != nil {
		return err
	}
	return r.afterHello(e.Event)
}

// landIntent is §7.2: the intent is recorded durably, then acknowledged on
// stdin. Whether it may be — the step order, the grade or observation it
// follows, the segment's own evidence, an operator's stop — is decided in one
// place, RecordWeftLandIntent; here the intent is held to the one step in
// flight and to the graded head on the run's base.
func (r *segmentStream) landIntent(line []byte) error {
	var event landIntentEvent
	if err := decodeEvent(line, &event, "land_intent"); err != nil {
		return err
	}
	if err := r.landEnvelope(event.envelope); err != nil {
		return err
	}
	switch {
	case !slices.Contains(ledger.WeftLandSteps, event.Step):
		return fmt.Errorf("land_intent names no landing step")
	case r.intent != "":
		return fmt.Errorf("land_intent %s before %s reported its result", event.Step, r.intent)
	case r.landCommit == "" || r.landBase == "":
		// A reconcile launched without --commit observes and takes no step.
		return fmt.Errorf("land_intent with no graded head on the run's base to land")
	case event.Commit != r.landCommit || event.Base != r.landBase:
		return fmt.Errorf("land_intent names a commit or base other than the graded head on the run's base")
	}
	r.s.stateMu.Lock()
	ack, err := r.s.store.RecordWeftLandIntent(r.run.ID, r.segment.Segment, event.Step, event.Branch, event.Commit, event.Base, event.TS)
	r.s.stateMu.Unlock()
	if err != nil {
		return err
	}
	r.intent, r.acked = event.Step, ack
	if !ack {
		r.s.log.Printf("weft landing %s: %s intended and not acknowledged: the run is stopping", r.ids(), event.Step)
		return nil
	}
	if err := r.active.send(map[string]any{"command": "ack_land_intent", "step": event.Step, "commit": event.Commit}); err != nil {
		r.s.log.Printf("weft landing %s: %s intended; the acknowledgement could not be written: %v", r.ids(), event.Step, err)
		return nil
	}
	r.s.log.Printf("weft landing %s: %s intended and acknowledged", r.ids(), event.Step)
	return nil
}

func (r *segmentStream) landStepResult(line []byte) error {
	var event landStepResultEvent
	if err := decodeEvent(line, &event, "land_step_result"); err != nil {
		return err
	}
	if err := r.landEnvelope(event.envelope); err != nil {
		return err
	}
	switch {
	case event.Step != r.intent:
		return fmt.Errorf("land_step_result for a step other than the one intended")
	case !r.acked:
		return fmt.Errorf("land_step_result for %s, which the host did not acknowledge", event.Step)
	case !oneOf(event.Result, "done", "refused", "failed"):
		return fmt.Errorf("land_step_result result is not done, refused or failed")
	case event.Commit != r.landCommit:
		return fmt.Errorf("land_step_result names a commit other than the one intended")
	}
	r.s.stateMu.Lock()
	err := r.s.store.RecordWeftLandResult(r.run.ID, r.segment.Segment, event.Step, event.Result, text(event.Detail), event.TS)
	r.s.stateMu.Unlock()
	if err != nil {
		return err
	}
	r.intent = ""
	r.s.log.Printf("weft landing %s: %s %s", r.ids(), event.Step, event.Result)
	return nil
}

// recordLanding records a landing segment's ended landing: what it landed is
// the graded head, and a landing step still awaiting its result was not done.
func (r *segmentStream) recordLanding(l ledger.WeftLanding) error {
	if l.State == ledger.WeftSettlementLanded && l.Commit != r.landCommit {
		return fmt.Errorf("ended lands a commit other than the graded head")
	}
	if unreviewed(l, r.landCommit) {
		return fmt.Errorf("ended reports a landing not landed of a commit other than the graded head")
	}
	r.s.stateMu.Lock()
	err := r.s.store.RecordWeftLanding(r.run.ID, r.segment.Segment, l)
	r.s.stateMu.Unlock()
	if err != nil {
		return err
	}
	r.s.log.Printf("weft landing %s: %s %s %s", r.ids(), l.State, l.Step, l.Reason)
	return nil
}

// unreviewed is a not_landed report naming a commit other than the graded
// head the host recorded. One naming none is held to that head in the ledger.
func unreviewed(l ledger.WeftLanding, head string) bool {
	return l.State == ledger.WeftSettlementNotLanded && l.Commit != "" && head != "" && l.Commit != head
}

// observedEvent is a reconcile's reading of the run's actual state (§7.4).
func (r *segmentStream) observedEvent(line []byte) error {
	var event observedEvent
	if err := decodeEvent(line, &event, "observed"); err != nil {
		return err
	}
	if err := r.maintenanceEnvelope(event.envelope); err != nil {
		return err
	}
	switch {
	case !r.first:
		return fmt.Errorf("observed is not a reconcile's first event")
	case event.Outcome != nil && !oneOf(*event.Outcome, "finished", "halted", "failed", "stopped"):
		return fmt.Errorf("observed outcome is not finished, halted, failed or stopped")
	case !oneOf(event.FastForward, "done", "not_done") || !oneOf(event.Push, "done", "not_done", "unknown") ||
		!oneOf(event.MarkDone, "done", "not_done", "unknown"):
		return fmt.Errorf("observed reports a step as other than done, not_done or unknown")
	case r.landCommit != "" && event.Commit != nil && *event.Commit != r.landCommit:
		return fmt.Errorf("observed names a commit other than the graded head")
	case r.landCommit != "" && text(event.Outcome) != "finished":
		// The host recorded a finished grade: the reading cannot undo it.
		return fmt.Errorf("observed an outcome other than the finished grade the host recorded")
	}
	r.s.stateMu.Lock()
	err := r.s.store.RecordWeftObserved(r.run.ID, r.segment.Segment, text(event.Outcome), ledger.WeftObserved{
		FastForward: event.FastForward, Push: event.Push, MarkDone: event.MarkDone,
	}, event.TS)
	r.s.stateMu.Unlock()
	if err != nil {
		return err
	}
	r.observed = &event
	r.s.log.Printf("weft reconcile %s: observed fast_forward %s, push %s, mark_done %s", r.ids(),
		event.FastForward, event.Push, event.MarkDone)
	return nil
}

func (r *segmentStream) reconciled(line []byte) error {
	var event reconciledEvent
	if err := decodeEvent(line, &event, "reconciled"); err != nil {
		return err
	}
	if err := r.maintenanceEnvelope(event.envelope); err != nil {
		return err
	}
	if r.observed == nil {
		return fmt.Errorf("reconciled before observed")
	}
	l := event.Landing.landing()
	if l.State == ledger.WeftSettlementLanded && l.Commit != r.landCommit {
		return fmt.Errorf("reconciled lands a commit other than the graded head")
	}
	if unreviewed(l, r.landCommit) {
		return fmt.Errorf("reconciled reports a landing not landed of a commit other than the graded head")
	}
	// A finished grade's landing ends only landed or not landed.
	if text(r.observed.Outcome) == "finished" && l.State == ledger.WeftSettlementNone {
		return fmt.Errorf("reconciled reports no landing after observing a finished grade")
	}
	r.s.stateMu.Lock()
	err := r.s.store.RecordWeftReconciled(r.run.ID, r.segment.Segment, text(r.observed.Outcome), l)
	r.s.stateMu.Unlock()
	if err != nil {
		return err
	}
	r.terminal = true
	r.s.log.Printf("weft reconcile %s: reconciled %s %s %s", r.ids(), l.State, l.Step, l.Reason)
	return nil
}

// maintained is a cleanup's or archive's one event (§6.7, §6.8): done
// removed the worktree — and for an archive wrote the run's archive ref at
// its head — or refused with the reason and changed nothing.
func (r *segmentStream) maintained(line []byte) error {
	var event maintainedEvent
	if err := decodeEvent(line, &event, "maintained"); err != nil {
		return err
	}
	if err := r.maintenanceEnvelope(event.envelope); err != nil {
		return err
	}
	done := event.Result == "done"
	archiveHead := ""
	if done && event.Op == ledger.WeftSegmentArchive {
		r.s.stateMu.RLock()
		head, err := r.s.store.WeftArchiveHead(r.run.ID)
		r.s.stateMu.RUnlock()
		if err != nil {
			return err
		}
		archiveHead = head
	}
	switch {
	case !r.first:
		return fmt.Errorf("maintained is not the segment's first event")
	case event.Op != r.segment.Kind:
		return fmt.Errorf("maintained op is not the segment's kind")
	case !oneOf(event.Result, "done", "refused"):
		return fmt.Errorf("maintained result is not done or refused")
	case !done && text(event.Detail) == "":
		return fmt.Errorf("a refused %s carries its reason", event.Op)
	case done && event.Op == ledger.WeftSegmentCleanup && (!event.WorktreeRemoved || event.ArchiveRef != nil):
		return fmt.Errorf("a cleanup done removed the worktree and wrote no archive ref")
	case done && event.Op == ledger.WeftSegmentArchive &&
		(text(event.ArchiveRef) != ledger.WeftArchiveRef(r.run.ID) || text(event.Head) != archiveHead ||
			(!event.WorktreeRemoved && r.run.Disposition != ledger.WeftDispositionCleaned)):
		return fmt.Errorf("an archive done names the run's archive ref at the head its fresh entry archives and removed the worktree")
	}
	r.s.stateMu.Lock()
	err := r.s.store.RecordWeftMaintained(r.run.ID, r.segment.Segment, event.Op, event.Result, text(event.Detail))
	r.s.stateMu.Unlock()
	if err != nil {
		return err
	}
	if event.Op == ledger.WeftSegmentArchive {
		r.archive = &ledger.WeftArchiveReport{Done: done, Head: text(event.Head), Detail: text(event.Detail)}
	}
	r.terminal = true
	r.s.log.Printf("weft %s %s: %s", event.Op, r.ids(), event.Result)
	return nil
}
