package weftremote

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"

	"shuttle/internal/ledger"
)

type workerRead struct {
	terminal bool
	err      error
	// stage is the last stage the worker reported, for diagnostics.
	stage string
	// archive is an archive worker's report, settled at reap.
	archive *ledger.WeftArchiveReport
}

// segmentStream reads one segment's worker events (v1 §4.2, weft-worker/v2
// §11.2): each line is checked against the launched segment and recorded
// before the next is read. Its handlers are chosen by the contract the
// segment was launched under; the landing, reconcile, cleanup and archive
// streams of weft-worker/v2 extend the table with their own events.
type segmentStream struct {
	s        *Supervisor
	run      *ledger.WeftRun
	segment  ledger.WeftSegment
	contract string
	// worker is the probe the segment was launched under; hello must repeat it.
	worker   *workerInfo
	seq      int
	first    bool
	workerID string
	terminal bool
	graded   *gradedEvent
	// summarized is whether a summary followed a graded that carried none;
	// intended is whether any land_intent was read.
	summarized, intended bool
	// stage is the last stage reported, for diagnostics.
	stage  string
	chunks map[ledger.WeftAttempt]int
	// land is whether the segment was launched with --land; active is its
	// process, which acknowledgements are written to (§7.2).
	land   bool
	active *activeSegment
	// landCommit and landBase are the graded head and the run's base, the
	// only commit and base a landing step may be acknowledged for; intent is
	// the step intended and not yet reported.
	landCommit, landBase, intent string
	// acked is whether the host acknowledged intent.
	acked bool
	// observed is a reconcile's reading; archive an archive's report.
	observed *observedEvent
	archive  *ledger.WeftArchiveReport
}

type eventHandler func(line []byte) error

func (s *Supervisor) readEvents(run *ledger.WeftRun, segment ledger.WeftSegment, contract string, worker *workerInfo,
	land bool, active *activeSegment, stdout io.Reader, record io.Writer) workerRead {
	r := &segmentStream{
		s: s, run: run, segment: segment, contract: contract, worker: worker,
		seq: 1, first: true, chunks: map[ledger.WeftAttempt]int{}, land: land, active: active,
	}
	var handlers map[string]eventHandler
	switch {
	case segment.Kind == ledger.WeftSegmentReconcile:
		r.landCommit, r.landBase = reconcileCommit(run)
		handlers = r.reconcileHandlers()
	case segment.Kind == ledger.WeftSegmentCleanup || segment.Kind == ledger.WeftSegmentArchive:
		handlers = map[string]eventHandler{"maintained": r.maintained}
	case contract == WorkerContractV2:
		handlers = r.v2Handlers()
	default:
		handlers = r.v1Handlers()
	}
	read := r.read(handlers, stdout, record)
	read.stage, read.archive = r.stage, r.archive
	return read
}

func (r *segmentStream) read(handlers map[string]eventHandler, stdout io.Reader, record io.Writer) workerRead {
	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 64*1024), 1<<20)
	for scanner.Scan() {
		line := append([]byte(nil), scanner.Bytes()...)
		var kind struct {
			Event string `json:"event"`
		}
		kindErr := json.Unmarshal(line, &kind)
		// Every line but an output chunk is recorded verbatim. Output is
		// retained in the ledger under its caps; recording it here too would
		// keep all of a verbose worker's text on disk, past any retention.
		if kindErr != nil || kind.Event != "output" {
			if _, err := record.Write(append(line, '\n')); err != nil {
				return workerRead{err: err}
			}
		}
		if r.terminal {
			return workerRead{terminal: true, err: fmt.Errorf("worker event followed a terminal event")}
		}
		if kindErr != nil {
			return workerRead{err: fmt.Errorf("a worker line is not a JSON object naming an event")}
		}
		handle := handlers[kind.Event]
		if handle == nil {
			return workerRead{err: fmt.Errorf("a worker line names an event this contract does not declare")}
		}
		if err := handle(line); err != nil {
			return workerRead{err: err}
		}
		r.first = false
		r.seq++
	}
	if err := scanner.Err(); err != nil {
		return workerRead{terminal: r.terminal, err: err}
	}
	return workerRead{terminal: r.terminal}
}

// v1Handlers are weft-remote/v1's segment events.
func (r *segmentStream) v1Handlers() map[string]eventHandler {
	return map[string]eventHandler{
		"hello": r.helloV1, "stage": r.stageEvent, "parked": r.parked, "ended": r.endedV1,
	}
}

// v2Handlers are a weft-worker/v2 segment's events (§11.3). A segment
// launched without --land takes no landing step, so its landing events are a
// protocol failure.
func (r *segmentStream) v2Handlers() map[string]eventHandler {
	handlers := map[string]eventHandler{
		"hello": r.helloV2, "stage": r.stageEvent, "output": r.output, "liveness": r.liveness,
		"allowance": r.allowance, "parked": r.parked, "graded": r.gradedEvent, "summarized": r.summarizedEvent, "ended": r.endedV2,
		"land_intent": r.unlanded("land_intent"), "land_step_result": r.unlanded("land_step_result"),
	}
	if r.land {
		handlers["land_intent"], handlers["land_step_result"] = r.landIntent, r.landStepResult
	}
	return handlers
}

func (r *segmentStream) envelope(e envelope) error {
	return validateEnvelopeAt(e, r.contract, r.run, r.segment, r.seq, r.workerID, false)
}

// afterHello refuses an event that may only follow hello.
func (r *segmentStream) afterHello(event string) error {
	if r.first {
		return fmt.Errorf("first worker event is %s, want hello", event)
	}
	return nil
}

// decodeEvent reads one event strictly. Its failure names the event, the
// declared field and the failure class, never what the worker wrote: a
// rejected value or key may be anything a worker put on the line — an
// answer's text included — and the failure reaches the log, the segment's
// ledger row and the run's detail. The raw line, unless it is an output
// chunk, stays in the run directory's events.ndjson.
func decodeEvent(line []byte, dst any, event string) error {
	err := decodeStrict(line, dst)
	if err == nil {
		return nil
	}
	var wrongType *json.UnmarshalTypeError
	switch {
	case errors.As(err, &wrongType):
		return fmt.Errorf("%s field %s has the wrong type", event, wrongType.Field)
	case strings.HasPrefix(err.Error(), "json: unknown field "):
		return fmt.Errorf("%s carries an undeclared key", event)
	}
	return fmt.Errorf("%s is not a well-formed event", event)
}

func (r *segmentStream) helloV1(line []byte) error {
	var event helloEvent
	if err := decodeEvent(line, &event, "hello"); err != nil {
		return err
	}
	if err := r.checkHello(event); err != nil {
		return err
	}
	return r.recordHello(event)
}

// helloV2 is v1's hello plus what §2.6 and §11.3 add: the version and
// capabilities the probe found, no landing, no trial environment, and a
// registered repository — the only launch this host makes.
func (r *segmentStream) helloV2(line []byte) error {
	var event helloV2Event
	if err := decodeEvent(line, &event, "hello"); err != nil {
		return err
	}
	if err := r.checkHello(event.helloEvent); err != nil {
		return err
	}
	if r.worker == nil || event.WorkerVersion != r.worker.Version || !sameSet(event.Capabilities, r.worker.Capabilities) {
		return fmt.Errorf("hello worker_version or capabilities differ from the probe's")
	}
	if event.Land && !r.land {
		return fmt.Errorf("hello reports land, but the segment was launched without --land")
	}
	if !event.Land && r.land {
		return fmt.Errorf("hello does not report land, but the segment was launched with --land")
	}
	if event.Environment.TKStoreRoot != nil || event.Environment.WorkEvidenceDir != nil || !event.Registered {
		return fmt.Errorf("hello reports a trial environment or unregistered checkout no launch named")
	}
	r.workerID, r.landBase = event.WorkerRunID, event.Base
	r.s.stateMu.Lock()
	err := r.s.store.RecordWeftLandingHello(r.run.ID, r.segment.Segment, event.WorkerRunID, event.Base, event.Criteria, r.land)
	r.s.stateMu.Unlock()
	return err
}

func (r *segmentStream) checkHello(event helloEvent) error {
	if err := r.envelope(event.envelope); err != nil {
		return err
	}
	run := r.run
	if !r.first || event.Worktree != run.Worktree || event.Ticket != run.Ticket || event.Project != run.Project {
		return fmt.Errorf("hello does not match the claimed run")
	}
	if event.Branch != run.Branch || event.Criteria == "" {
		return fmt.Errorf("hello branch or criteria does not match the protocol")
	}
	for _, stage := range event.Stages {
		if stage.Stage == "" || stage.Backend == "" {
			return fmt.Errorf("hello carries an invalid stage plan")
		}
	}
	expectedResume := latestWorkerBefore(run, r.segment.Segment)
	if text(event.Resumed) != expectedResume {
		return fmt.Errorf("hello resumed does not name the run's latest worker run")
	}
	expectedAnswer := ""
	if r.segment.AnswerID.Valid {
		r.s.stateMu.RLock()
		answer, err := r.s.store.WeftAnswer(r.segment.AnswerID.String)
		r.s.stateMu.RUnlock()
		if err != nil {
			return err
		}
		expectedAnswer = answer.QuestionID
	}
	if text(event.Answer) != expectedAnswer {
		return fmt.Errorf("hello answer does not name the question the segment's answer is for")
	}
	return nil
}

// recordHello records a weft-remote/v1 hello: a segment that lands nothing,
// whatever its run was minted expecting.
func (r *segmentStream) recordHello(event helloEvent) error {
	r.workerID = event.WorkerRunID
	r.s.stateMu.Lock()
	err := r.s.store.RecordWeftLandingHello(r.run.ID, r.segment.Segment, event.WorkerRunID, event.Base, event.Criteria, false)
	r.s.stateMu.Unlock()
	return err
}

func (r *segmentStream) stageEvent(line []byte) error {
	var event stageEvent
	if err := decodeEvent(line, &event, "stage"); err != nil {
		return err
	}
	if err := r.envelope(event.envelope); err != nil {
		return err
	}
	if err := r.afterHello("stage"); err != nil {
		return err
	}
	if event.Stage == "" || event.Occurrence < 1 || event.Attempt < 1 ||
		!oneOf(event.State, "running", "done", "failed", "needs_input", "stopped") {
		return fmt.Errorf("invalid stage transition")
	}
	r.stage = event.Stage
	stage := ledger.WeftStage{
		RunID: r.run.ID, Segment: r.segment.Segment, Stage: event.Stage,
		Occurrence: event.Occurrence, Attempt: event.Attempt,
		Backend: nullSQL(event.Backend), State: event.State, Session: nullSQL(event.Session),
		Detail: nullSQL(event.Detail), ExitCode: nullInt64(event.ExitCode),
		Turns: nullInt64(event.Turns), Spend: event.Spend,
		Processing: nullInt64(event.ProcessingMS), Tokens: event.Tokens, At: event.TS,
	}
	r.s.stateMu.Lock()
	err := r.s.store.RecordWeftStage(stage)
	r.s.stateMu.Unlock()
	if err != nil {
		return err
	}
	r.s.log.Printf("weft stage %s occurrence=%d attempt=%d: %s", r.ids(), event.Occurrence, event.Attempt, event.State)
	return nil
}

func (r *segmentStream) parked(line []byte) error {
	var event parkedEvent
	if err := decodeEvent(line, &event, "parked"); err != nil {
		return err
	}
	if err := r.envelope(event.envelope); err != nil {
		return err
	}
	if err := r.afterHello("parked"); err != nil {
		return err
	}
	// A graded segment never parks: its run may already have landing steps,
	// so the failure goes to reconciliation and no answer restarts the
	// pipeline over them.
	if r.graded != nil {
		return fmt.Errorf("parked after graded")
	}
	if event.WorkerOutcome != "blocked" || event.Question == "" ||
		len([]byte(event.Question)) > maxAnswerBytes || event.Occurrence < 1 || event.Attempt < 1 {
		return fmt.Errorf("invalid parked event")
	}
	r.s.stateMu.Lock()
	_, err := r.s.store.RecordWeftParked(r.run.ID, r.segment.Segment, event.Stage,
		event.Occurrence, event.Attempt, event.Question, event.Head, event.Summary)
	r.s.stateMu.Unlock()
	if err != nil {
		return err
	}
	r.terminal = true
	return nil
}

func (r *segmentStream) endedV1(line []byte) error {
	var event endedEvent
	if err := decodeEvent(line, &event, "ended"); err != nil {
		return err
	}
	// A refusal before hello precedes weft's started line, so it has no
	// worker_run_id yet.
	refusal := r.first && event.Failure != nil
	if err := validateEnvelopeAt(event.envelope, r.contract, r.run, r.segment, r.seq, r.workerID, refusal); err != nil {
		return err
	}
	if r.first && event.Failure == nil {
		return fmt.Errorf("first ended event must carry a worker refusal")
	}
	if !oneOf(event.Outcome, "finished", "halted", "failed", "stopped") {
		return fmt.Errorf("ended outcome is not finished, halted, failed or stopped")
	}
	if text(event.Failure) == ledger.WeftActionStaleBase {
		return r.staleBase(refusal, event.Outcome, text(event.Detail))
	}
	haltStage, haltReason, haltDetail := "", "", ""
	if event.Halt != nil {
		haltStage, haltReason, haltDetail = event.Halt.Stage, event.Halt.Reason, event.Halt.Detail
	}
	r.s.stateMu.Lock()
	err := r.s.store.RecordWeftEnded(r.run.ID, r.segment.Segment, event.Outcome, event.Stopped,
		haltStage, haltReason, haltDetail, text(event.Failure), text(event.Detail),
		event.Head, event.Summary)
	r.s.stateMu.Unlock()
	if err != nil {
		return err
	}
	r.terminal = true
	return nil
}

// staleBase is weft-service/v2 §6.7: an answer or resume segment refused
// before any stage because the default branch moved past the run's base.
// The run is held for the operator, its answer unconsumed.
func (r *segmentStream) staleBase(refusal bool, outcome, detail string) error {
	if !refusal || outcome != "failed" || r.segment.Kind == "launch" {
		return fmt.Errorf("stale_base is a refusal of an answer or resume segment before hello")
	}
	r.s.stateMu.Lock()
	err := r.s.store.RecordWeftStaleBase(r.run.ID, r.segment.Segment, detail)
	r.s.stateMu.Unlock()
	if err != nil {
		return err
	}
	r.terminal = true
	return nil
}

// output is one chunk of an attempt's output (§8.1): numbered from 1 within
// the attempt, contiguous, retained under its cursor and announced to the
// output streams without waiting on any of them.
func (r *segmentStream) output(line []byte) error {
	var event outputEvent
	if err := decodeEvent(line, &event, "output"); err != nil {
		return err
	}
	if err := r.envelope(event.envelope); err != nil {
		return err
	}
	if err := r.afterHello("output"); err != nil {
		return err
	}
	if event.Stage == "" || event.Occurrence < 1 || event.Attempt < 1 {
		return fmt.Errorf("invalid output event")
	}
	if event.Text == nil {
		return fmt.Errorf("output field text is missing or null")
	}
	attempt := ledger.WeftAttempt{Segment: r.segment.Segment, Stage: event.Stage, Occurrence: event.Occurrence, Attempt: event.Attempt}
	if want := r.chunks[attempt] + 1; event.Chunk != want {
		return fmt.Errorf("output chunk is out of sequence for its attempt, which wants chunk %d", want)
	}
	r.chunks[attempt] = event.Chunk
	r.s.stateMu.Lock()
	_, err := r.s.store.RecordWeftOutput(ledger.WeftOutputChunk{
		RunID: r.run.ID, WeftAttempt: attempt, Chunk: event.Chunk, At: event.TS, Text: *event.Text,
	})
	r.s.stateMu.Unlock()
	if err != nil {
		r.s.log.Printf("weft output %s occurrence=%d attempt=%d: capture of chunk %d failed: %v",
			r.idsAt(event.Stage), event.Occurrence, event.Attempt, event.Chunk, err)
		return fmt.Errorf("capture output chunk %d: %w", event.Chunk, err)
	}
	r.s.output.notify()
	return nil
}

func (r *segmentStream) liveness(line []byte) error {
	var event livenessEvent
	if err := decodeEvent(line, &event, "liveness"); err != nil {
		return err
	}
	if err := r.envelope(event.envelope); err != nil {
		return err
	}
	if err := r.afterHello("liveness"); err != nil {
		return err
	}
	if _, err := time.Parse(time.RFC3339Nano, event.LastEventAt); err != nil || event.Stage == "" ||
		event.Occurrence < 1 || event.Attempt < 1 {
		return fmt.Errorf("invalid liveness event")
	}
	r.s.stateMu.Lock()
	err := r.s.store.RecordWeftLiveness(r.run.ID, ledger.WeftLiveness{
		WeftAttempt: ledger.WeftAttempt{Segment: r.segment.Segment, Stage: event.Stage, Occurrence: event.Occurrence, Attempt: event.Attempt},
		Stalled:     event.Stalled, LastEventAt: event.LastEventAt, ObservedAt: event.TS,
	})
	r.s.stateMu.Unlock()
	if err != nil {
		return err
	}
	state := "recovered"
	if event.Stalled {
		state = "stalled"
	}
	r.s.log.Printf("weft liveness %s occurrence=%d attempt=%d: %s", r.idsAt(event.Stage), event.Occurrence, event.Attempt, state)
	return nil
}

// allowance is a Claude rate_limit_event observation (§5.3), kept when it is
// newer than the one held.
func (r *segmentStream) allowance(line []byte) error {
	var event allowanceEvent
	if err := decodeEvent(line, &event, "allowance"); err != nil {
		return err
	}
	if err := r.envelope(event.envelope); err != nil {
		return err
	}
	if err := r.afterHello("allowance"); err != nil {
		return err
	}
	windows, err := allowanceWindows(event.Windows)
	if _, terr := time.Parse(time.RFC3339Nano, event.ObservedAt); err != nil || terr != nil ||
		event.Provider != "claude" || event.Source != "claude_stream" {
		return fmt.Errorf("invalid allowance event")
	}
	r.s.stateMu.Lock()
	_, err = r.s.store.RecordWeftAllowance(ledger.WeftAllowance{
		Provider: event.Provider, Source: event.Source, ObservedAt: event.ObservedAt, Windows: windows,
	})
	r.s.stateMu.Unlock()
	return err
}

// gradedEvent is the grading half of v1's ended (§11.3): recorded as the
// run's outcome, after which the segment's ended must repeat it. A segment
// launched without --land ends its run here only when graded carries the
// summary; otherwise the run is over once summarized or ended arrives.
func (r *segmentStream) gradedEvent(line []byte) error {
	var event gradedEvent
	if err := decodeEvent(line, &event, "graded"); err != nil {
		return err
	}
	if err := r.envelope(event.envelope); err != nil {
		return err
	}
	if err := r.afterHello("graded"); err != nil {
		return err
	}
	if r.graded != nil {
		return fmt.Errorf("graded twice in one segment")
	}
	if !oneOf(event.Outcome, "finished", "halted", "failed", "stopped") {
		return fmt.Errorf("graded outcome is not finished, halted, failed or stopped")
	}
	r.graded, r.landCommit = &event, text(event.Head)
	if !r.land && rawJSON(event.Summary) != nil {
		return r.endGraded(event.Summary)
	}
	haltStage, haltReason, haltDetail := event.halt()
	r.s.stateMu.Lock()
	var err error
	if r.land {
		err = r.s.store.RecordWeftGraded(r.run.ID, r.segment.Segment, event.Outcome, event.Stopped,
			haltStage, haltReason, haltDetail, text(event.Detail), text(event.Head), event.Summary)
	} else {
		err = r.s.store.RecordWeftGradedBeforeSummary(r.run.ID, event.Outcome, event.Stopped,
			haltStage, haltReason, haltDetail, text(event.Detail), text(event.Head))
	}
	r.s.stateMu.Unlock()
	return err
}

// endGraded records the end of a segment launched without --land under its
// grade, with the summary it reported, or none.
func (r *segmentStream) endGraded(summary json.RawMessage) error {
	haltStage, haltReason, haltDetail := r.graded.halt()
	r.s.stateMu.Lock()
	err := r.s.store.RecordWeftEnded(r.run.ID, r.segment.Segment, r.graded.Outcome, r.graded.Stopped,
		haltStage, haltReason, haltDetail, "", text(r.graded.Detail), text(r.graded.Head), summary)
	r.s.stateMu.Unlock()
	return err
}

// summarizedEvent is the summary of a worker that grades before it
// summarizes, so a segment lost in the summary step still leaves its graded
// head: it follows a graded whose summary is null, once, before any
// land_intent. A graded that carried its summary takes none after it.
func (r *segmentStream) summarizedEvent(line []byte) error {
	var event summarizedEvent
	if err := decodeEvent(line, &event, "summarized"); err != nil {
		return err
	}
	if err := r.envelope(event.envelope); err != nil {
		return err
	}
	if err := r.afterHello("summarized"); err != nil {
		return err
	}
	switch {
	case r.graded == nil:
		return fmt.Errorf("summarized before graded")
	case r.summarized || rawJSON(r.graded.Summary) != nil:
		return fmt.Errorf("summarized after the segment's summary was already reported")
	case r.intended:
		return fmt.Errorf("summarized after a land_intent")
	case !bytes.HasPrefix(event.Summary, []byte("{")):
		return fmt.Errorf("summarized field summary is not an object")
	}
	r.summarized = true
	if !r.land {
		return r.endGraded(event.Summary)
	}
	r.s.stateMu.Lock()
	err := r.s.store.RecordWeftSummarized(r.run.ID, event.Summary)
	r.s.stateMu.Unlock()
	return err
}

// endedV2 is the terminal event of a weft-worker/v2 segment: after graded it
// repeats the outcome, and a landing segment's carries the landing (§7.3);
// with no graded before it, it is a refusal that landed nothing.
func (r *segmentStream) endedV2(line []byte) error {
	var event endedV2Event
	if err := decodeEvent(line, &event, "ended"); err != nil {
		return err
	}
	refusal := r.first && event.Failure != nil
	if err := validateEnvelopeAt(event.envelope, r.contract, r.run, r.segment, r.seq, r.workerID, refusal); err != nil {
		return err
	}
	if !oneOf(event.Outcome, "finished", "halted", "failed", "stopped") {
		return fmt.Errorf("ended outcome is not finished, halted, failed or stopped")
	}
	landing := event.Landing.landing()
	if landing.State != ledger.WeftSettlementNone && (!r.land || r.graded == nil) {
		if !r.land {
			return fmt.Errorf("ended reports a landing other than not_applicable, but the segment was launched without --land")
		}
		return fmt.Errorf("ended reports a landing with no graded before it")
	}
	if r.graded != nil {
		if event.Outcome != r.graded.Outcome || event.Failure != nil {
			return fmt.Errorf("ended does not repeat the graded outcome")
		}
		// A finished grade's landing ends only landed or not landed.
		if r.land && r.graded.Outcome == "finished" && landing.State == ledger.WeftSettlementNone {
			return fmt.Errorf("ended reports no landing after a finished grade")
		}
		if r.land {
			if err := r.recordLanding(landing); err != nil {
				return err
			}
		} else if !r.summarized && rawJSON(r.graded.Summary) == nil {
			// Graded before its summary, which never came: the run ends
			// here, with none.
			if err := r.endGraded(nil); err != nil {
				return err
			}
		}
		r.terminal = true
		return nil
	}
	if event.Failure == nil {
		return fmt.Errorf("ended with no graded before it must carry a worker refusal")
	}
	if *event.Failure == ledger.WeftActionStaleBase {
		return r.staleBase(refusal, event.Outcome, text(event.Detail))
	}
	r.s.stateMu.Lock()
	err := r.s.store.RecordWeftEnded(r.run.ID, r.segment.Segment, event.Outcome, false,
		"", "", "", *event.Failure, text(event.Detail), "", nil)
	r.s.stateMu.Unlock()
	if err != nil {
		return err
	}
	// A refused landing segment lands nothing: its run is not pending any
	// landing.
	if r.land {
		if err := r.recordLanding(landing); err != nil {
			return err
		}
	}
	r.terminal = true
	return nil
}

// unlanded refuses a landing event: this host launches no segment with
// --land, so a worker taking a landing step is acting unacknowledged (§7.2).
func (r *segmentStream) unlanded(event string) eventHandler {
	return func([]byte) error {
		return fmt.Errorf("%s from a segment launched without --land", event)
	}
}

// ids joins the host, run, ledger row, segment and last stage, as every
// line about a segment carries them (§4.2).
func (r *segmentStream) ids() string { return r.idsAt(r.stage) }

func (r *segmentStream) idsAt(stage string) string {
	return segmentIDs(r.s.hostID, r.run.ID, r.segment, stage)
}

func segmentIDs(hostID, runID string, segment ledger.WeftSegment, stage string) string {
	if stage == "" {
		stage = "-"
	}
	return fmt.Sprintf("host=%s run=%s ledger=%s segment=%d stage=%s", hostID, runID, segment.LedgerRunID, segment.Segment, stage)
}

func sameSet(a, b []string) bool {
	x, y := slices.Clone(a), slices.Clone(b)
	slices.Sort(x)
	slices.Sort(y)
	return slices.Equal(x, y)
}

// allowanceWindow is one provider window; utilization has no default, so a
// window that omits it is refused rather than read as zero (§5.3).
type allowanceWindow struct {
	ID          string   `json:"id"`
	Utilization *float64 `json:"utilization"`
	ResetsAt    string   `json:"resets_at"`
}

// allowanceWindows checks an observation's windows and renders them as kept.
func allowanceWindows(windows []allowanceWindow) (json.RawMessage, error) {
	if len(windows) == 0 {
		return nil, fmt.Errorf("an observation carries at least one window")
	}
	for _, w := range windows {
		if _, err := time.Parse(time.RFC3339Nano, w.ResetsAt); err != nil || w.ID == "" || w.Utilization == nil {
			return nil, fmt.Errorf("a window has no id, utilization or reset")
		}
	}
	return json.Marshal(windows)
}

type envelope struct {
	ContractVersion string `json:"contract_version"`
	RunID           string `json:"run_id"`
	WorkerRunID     string `json:"worker_run_id"`
	Segment         int    `json:"segment"`
	Seq             int    `json:"seq"`
	TS              string `json:"ts"`
	Event           string `json:"event"`
}

type helloEvent struct {
	envelope
	WorkerVersion string            `json:"worker_version"`
	Ticket        string            `json:"ticket"`
	Project       string            `json:"project"`
	Branch        string            `json:"branch"`
	Worktree      string            `json:"worktree"`
	Base          string            `json:"base"`
	Criteria      string            `json:"criteria"`
	Resumed       *string           `json:"resumed"`
	Inherited     []json.RawMessage `json:"inherited"`
	Answer        *string           `json:"answer"`
	Stages        []helloStage      `json:"stages"`
}

type helloV2Event struct {
	helloEvent
	Capabilities []string `json:"capabilities"`
	Land         bool     `json:"land"`
	Environment  struct {
		TKStoreRoot     *string `json:"tk_store_root"`
		WorkEvidenceDir *string `json:"work_evidence_dir"`
	} `json:"environment"`
	Registered bool `json:"registered"`
}

type helloStage struct {
	Stage   string `json:"stage"`
	Backend string `json:"backend"`
}

type stageEvent struct {
	envelope
	Stage        string          `json:"stage"`
	Backend      *string         `json:"backend"`
	Occurrence   int             `json:"occurrence"`
	Attempt      int             `json:"attempt"`
	State        string          `json:"state"`
	Session      *string         `json:"session"`
	Detail       *string         `json:"detail"`
	ExitCode     *int64          `json:"exit_code"`
	Turns        *int64          `json:"turns"`
	Spend        json.RawMessage `json:"spend"`
	ProcessingMS *int64          `json:"processing_ms"`
	Tokens       json.RawMessage `json:"tokens"`
}

type parkedEvent struct {
	envelope
	Stage         string          `json:"stage"`
	Occurrence    int             `json:"occurrence"`
	Attempt       int             `json:"attempt"`
	WorkerOutcome string          `json:"worker_outcome"`
	Question      string          `json:"question"`
	Head          string          `json:"head"`
	Summary       json.RawMessage `json:"summary"`
}

type endedEvent struct {
	envelope
	Outcome string          `json:"outcome"`
	Stopped bool            `json:"stopped"`
	Halt    *halt           `json:"halt"`
	Failure *string         `json:"failure"`
	Detail  *string         `json:"detail"`
	Head    string          `json:"head"`
	Summary json.RawMessage `json:"summary"`
}

type halt struct {
	Stage  string `json:"stage"`
	Reason string `json:"reason"`
	Detail string `json:"detail"`
}

type outputEvent struct {
	envelope
	Stage      string `json:"stage"`
	Occurrence int    `json:"occurrence"`
	Attempt    int    `json:"attempt"`
	Chunk      int    `json:"chunk"`
	// Text is required: a missing or null text is refused, not read as "".
	Text *string `json:"text"`
}

type livenessEvent struct {
	envelope
	Stage       string `json:"stage"`
	Occurrence  int    `json:"occurrence"`
	Attempt     int    `json:"attempt"`
	Stalled     bool   `json:"stalled"`
	LastEventAt string `json:"last_event_at"`
}

type allowanceEvent struct {
	envelope
	Provider   string            `json:"provider"`
	Source     string            `json:"source"`
	ObservedAt string            `json:"observed_at"`
	Windows    []allowanceWindow `json:"windows"`
}

type gradedEvent struct {
	envelope
	Outcome string          `json:"outcome"`
	Stopped bool            `json:"stopped"`
	Halt    *halt           `json:"halt"`
	Detail  *string         `json:"detail"`
	Head    *string         `json:"head"`
	Summary json.RawMessage `json:"summary"`
}

func (e *gradedEvent) halt() (stage, reason, detail string) {
	if e.Halt == nil {
		return "", "", ""
	}
	return e.Halt.Stage, e.Halt.Reason, e.Halt.Detail
}

type summarizedEvent struct {
	envelope
	Summary json.RawMessage `json:"summary"`
}

type endedV2Event struct {
	envelope
	Outcome string        `json:"outcome"`
	Failure *string       `json:"failure"`
	Detail  *string       `json:"detail"`
	Landing landingReport `json:"landing"`
}

// validateEnvelopeAt checks an event's envelope against the launched segment
// and its contract; preHello admits the null worker_run_id of a refusal
// written before weft's started line.
func validateEnvelopeAt(e envelope, contract string, run *ledger.WeftRun, segment ledger.WeftSegment, seq int, workerID string, preHello bool) error {
	if e.ContractVersion != contract || e.RunID != run.ID || e.Segment != segment.Segment || e.Seq != seq {
		return fmt.Errorf("worker event envelope does not match the launched segment")
	}
	if _, err := time.Parse(time.RFC3339Nano, e.TS); err != nil {
		return fmt.Errorf("worker event ts is not an RFC 3339 timestamp")
	}
	if preHello && e.WorkerRunID == "" {
		return nil
	}
	if !validProtocolID(e.WorkerRunID) || (workerID != "" && e.WorkerRunID != workerID) {
		return fmt.Errorf("worker_run_id changed within the segment")
	}
	return nil
}
