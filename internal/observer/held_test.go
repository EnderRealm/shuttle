package observer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

// The helper process is this test binary re-executed with these set: the
// mode, the notebook directory it opens, and the file it reports through.
const (
	heldHelperMode = "SHUTTLE_HELD_HELPER"
	heldHelperDir  = "SHUTTLE_HELD_DIR"
	heldHelperOut  = "SHUTTLE_HELD_OUT"
	heldHelperArg  = "SHUTTLE_HELD_ARG"

	// heldWriterRecords is how many records each concurrent writer makes.
	heldWriterRecords = 6
)

func newHeld(t *testing.T) Held {
	t.Helper()
	return Held{Dir: filepath.Join(t.TempDir(), "notebook"), Identity: mustQuestion(t, "wren", "question.json").Identity}
}

// heldConformance presents a Held through the context-free Notebook the
// conformance driver takes. Only the context is dropped: every refusal and
// every outcome is the held notebook's own.
type heldConformance struct {
	t *testing.T
	h Held
}

func (n heldConformance) Revision() int64 {
	revision, err := n.h.Revision(context.Background())
	if err != nil {
		n.t.Fatalf("revision: %v", err)
	}
	return revision
}

func (n heldConformance) Entry(id string) (*Entry, error) {
	return n.h.Entry(context.Background(), id)
}

func (n heldConformance) Register(req Request, q *Question) (*Entry, error) {
	return n.h.Register(context.Background(), req, q)
}

func (n heldConformance) Activate(req Request, exp *Experiment) (*Entry, error) {
	return n.h.Activate(context.Background(), req, exp)
}

func (n heldConformance) Record(req Request, a *Assessment) (*Entry, error) {
	return n.h.Record(context.Background(), req, a)
}

func (n heldConformance) Claim(req Request, act *Action) (*Receipt, error) {
	return n.h.Claim(context.Background(), req, act)
}

func (n heldConformance) Settle(req Request, rec *Receipt) (*Receipt, error) {
	return n.h.Settle(context.Background(), req, rec)
}

func (n heldConformance) Retire(req Request, entryID, reason string) (*Retirement, error) {
	return n.h.Retire(context.Background(), req, entryID, reason)
}

func (n heldConformance) Reopen(req Request, entryID, condition string) (*Entry, error) {
	return n.h.Reopen(context.Background(), req, entryID, condition)
}

func TestHeldNotebookConformance(t *testing.T) {
	runConformance(t, func(t *testing.T) Notebook { return heldConformance{t: t, h: newHeld(t)} })
}

// heldAt composes a request against the held notebook's current revision.
func heldAt(t *testing.T, h Held, id Identity, requestID string) Request {
	t.Helper()
	revision, err := h.Revision(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return Request{Identity: id, Revision: revision, RequestID: requestID}
}

// heldUntil is until for a notebook another process is writing: a stale
// revision is re-read and decided again, anything else is the outcome.
func heldUntil[T any](t *testing.T, h Held, id Identity, requestID string, do func(Request) (T, error)) (T, Request, error) {
	t.Helper()
	var out T
	var err error
	var req Request
	for range maxAttempts {
		req = heldAt(t, h, id, requestID)
		out, err = do(req)
		if !errors.Is(err, ErrStaleRevision) {
			return out, req, err
		}
	}
	t.Fatalf("%s lost the revision precondition %d times running", requestID, maxAttempts)
	return out, req, err
}

// seedHeld registers the Wren question and activates its experiment, the
// state the process tests start from.
func seedHeld(t *testing.T, h Held) (*Question, *Experiment) {
	t.Helper()
	ctx := context.Background()
	q := mustQuestion(t, "wren", "question.json")
	if _, err := h.Register(ctx, heldAt(t, h, q.Identity, "seed-register"), q); err != nil {
		t.Fatal(err)
	}
	exp := mustExperiment(t, "wren", "experiment.json")
	if _, err := h.Activate(ctx, heldAt(t, h, q.Identity, "seed-activate"), exp); err != nil {
		t.Fatal(err)
	}
	return q, exp
}

// numberedAction is the Wren action under a further id, so one question can
// carry several claims.
func numberedAction(t *testing.T, n int) *Action {
	t.Helper()
	act := mustAction(t, "wren", "action.json")
	act.ID = act.Identity.Qualify(fmt.Sprintf("note-days-repeating-%d", n))
	return act
}

// startHeldHelper starts this binary as a helper process over dir and returns
// the command and the file it reports through.
func startHeldHelper(t *testing.T, mode, dir, arg string) (*exec.Cmd, string, *strings.Builder) {
	t.Helper()
	out := filepath.Join(t.TempDir(), mode+".json")
	cmd := exec.Command(os.Args[0], "-test.run=^TestHeldNotebookHelper$", "-test.count=1")
	cmd.Env = append(os.Environ(), heldHelperMode+"="+mode, heldHelperDir+"="+dir, heldHelperOut+"="+out, heldHelperArg+"="+arg)
	var output strings.Builder
	cmd.Stdout, cmd.Stderr = &output, &output
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	return cmd, out, &output
}

func waitHeldHelper(t *testing.T, cmd *exec.Cmd, out string, output *strings.Builder, report any) {
	t.Helper()
	if err := cmd.Wait(); err != nil {
		t.Fatalf("helper %s: %v\n%s", out, err, output)
	}
	data, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("helper report: %v\n%s", err, output)
	}
	if err := json.Unmarshal(data, report); err != nil {
		t.Fatal(err)
	}
}

func runHeldHelper(t *testing.T, mode, dir, arg string, report any) {
	t.Helper()
	cmd, out, output := startHeldHelper(t, mode, dir, arg)
	waitHeldHelper(t, cmd, out, output, report)
}

// reopenReport is what a second process reads back from a notebook the first
// one wrote: the revision, the entry, the receipts its claims and settlement
// replay to, a fresh claim of a settled id, and the revision after all of it.
type reopenReport struct {
	Revision      int64    `json:"revision"`
	Entry         *Entry   `json:"entry"`
	Claimed       *Receipt `json:"claimed"`
	Settled       *Receipt `json:"settled"`
	InFlight      *Receipt `json:"in_flight"`
	Reclaim       string   `json:"reclaim"`
	AfterRevision int64    `json:"after_revision"`
}

func TestHeldNotebookSurvivesReopen(t *testing.T) {
	ctx := context.Background()
	h := newHeld(t)
	q, _ := seedHeld(t, h)
	id := q.Identity
	a := mustAssessment(t, "wren", "assessment.json")
	for _, requestID := range []string{"record-1", "record-2"} {
		if _, err := h.Record(ctx, heldAt(t, h, id, requestID), a); err != nil {
			t.Fatal(err)
		}
	}
	act := mustAction(t, "wren", "action.json")
	claimed, err := h.Claim(ctx, heldAt(t, h, id, "claim"), act)
	if err != nil {
		t.Fatal(err)
	}
	settled, err := h.Settle(ctx, heldAt(t, h, id, "settle"), settle(claimed, "village2/show-whether-week-a0f2#note-1"))
	if err != nil {
		t.Fatal(err)
	}
	inFlight, err := h.Claim(ctx, heldAt(t, h, id, "claim-2"), numberedAction(t, 2))
	if err != nil {
		t.Fatal(err)
	}
	written, err := h.Entry(ctx, q.ID)
	if err != nil {
		t.Fatal(err)
	}
	revision, err := h.Revision(ctx)
	if err != nil {
		t.Fatal(err)
	}

	var got reopenReport
	runHeldHelper(t, "reopen", h.Dir, "", &got)

	if got.Revision != revision || got.AfterRevision != revision {
		t.Fatalf("revision read back %d, %d after replays; the writer left it at %d", got.Revision, got.AfterRevision, revision)
	}
	if !reflect.DeepEqual(got.Entry, written) {
		t.Fatalf("entry read back\n%+v\nwant the one written\n%+v", got.Entry, written)
	}
	if len(got.Entry.Observations) != 2 || got.Entry.State != StateAnswered || got.Entry.ExperimentStatus != ExperimentLive ||
		!reflect.DeepEqual(got.Entry.InFlight, []string{inFlight.ActionID}) {
		t.Fatalf("entry = %+v, want both observations, the live experiment and the one action still in flight", got.Entry)
	}
	if !reflect.DeepEqual(got.Claimed, claimed) || !reflect.DeepEqual(got.Settled, settled) || !reflect.DeepEqual(got.InFlight, inFlight) {
		t.Fatalf("receipts read back %+v / %+v / %+v, want %+v / %+v / %+v", got.Claimed, got.Settled, got.InFlight, claimed, settled, inFlight)
	}
	if got.Reclaim != "action-held" {
		t.Fatalf("a fresh claim of the settled id = %q, want it refused as held", got.Reclaim)
	}
}

// recordReport is one writer's accepted records: the revision each was
// decided against.
type recordReport struct {
	Revisions []int64 `json:"revisions"`
}

// claimReport is the claims a writer attempted in order, while another
// process retired the question.
type claimReport struct {
	Claims []claimAttempt `json:"claims"`
}

type claimAttempt struct {
	ActionID string `json:"action_id"`
	Revision int64  `json:"revision"`
	Refusal  string `json:"refusal,omitempty"`
}

func TestHeldNotebookSerializesAcrossProcesses(t *testing.T) {
	const perWriter = heldWriterRecords
	t.Run("concurrent records", func(t *testing.T) {
		ctx := context.Background()
		h := newHeld(t)
		q, _ := seedHeld(t, h)
		seeded, err := h.Revision(ctx)
		if err != nil {
			t.Fatal(err)
		}
		// Both writers are started before either is waited on, so they run
		// at once.
		reports := make([]recordReport, 2)
		aCmd, aOut, aOutput := startHeldHelper(t, "record", h.Dir, "a")
		bCmd, bOut, bOutput := startHeldHelper(t, "record", h.Dir, "b")
		waitHeldHelper(t, aCmd, aOut, aOutput, &reports[0])
		waitHeldHelper(t, bCmd, bOut, bOutput, &reports[1])

		decided := map[int64]bool{}
		for _, report := range reports {
			if len(report.Revisions) != perWriter {
				t.Fatalf("a writer recorded %d, want %d", len(report.Revisions), perWriter)
			}
			for _, revision := range report.Revisions {
				if decided[revision] {
					t.Fatalf("two records were both admitted at revision %d", revision)
				}
				decided[revision] = true
			}
		}
		for revision := seeded; revision < seeded+2*perWriter; revision++ {
			if !decided[revision] {
				t.Fatalf("no record admitted at revision %d: decided %v", revision, decided)
			}
		}
		revision, err := h.Revision(ctx)
		if err != nil {
			t.Fatal(err)
		}
		e, err := h.Entry(ctx, q.ID)
		if err != nil {
			t.Fatal(err)
		}
		if revision != seeded+2*perWriter || len(e.Observations) != 2*perWriter {
			t.Fatalf("revision %d with %d observations, want %d with %d: a write was lost",
				revision, len(e.Observations), seeded+2*perWriter, 2*perWriter)
		}
		if commits := heldCommits(t, h); commits != int(revision) {
			t.Fatalf("%d commits, want one per accepted mutation (%d)", commits, revision)
		}
	})

	t.Run("claims racing a retirement", func(t *testing.T) {
		ctx := context.Background()
		h := newHeld(t)
		q, _ := seedHeld(t, h)
		seeded, err := h.Revision(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var claims claimReport
		var ret Retirement
		claimer, claimOut, claimOutput := startHeldHelper(t, "claim", h.Dir, "")
		retirer, retireOut, retireOutput := startHeldHelper(t, "retire", h.Dir, strconv.FormatInt(seeded+2, 10))
		waitHeldHelper(t, retirer, retireOut, retireOutput, &ret)
		waitHeldHelper(t, claimer, claimOut, claimOutput, &claims)

		var admitted []string
		refused := false
		for _, claim := range claims.Claims {
			switch claim.Refusal {
			case "":
				if refused {
					t.Fatalf("claim %s was admitted after an earlier claim was refused as retired", claim.ActionID)
				}
				// The claim's commit is the revision after the one it was
				// decided against; the retirement's is ret.Revision.
				if claim.Revision+1 >= ret.Revision {
					t.Fatalf("claim %s was admitted at revision %d, at or after the retirement at %d", claim.ActionID, claim.Revision+1, ret.Revision)
				}
				admitted = append(admitted, claim.ActionID)
			case "retired":
				refused = true
			default:
				t.Fatalf("claim %s refused as %s", claim.ActionID, claim.Refusal)
			}
		}
		if !refused || len(admitted) < 2 {
			t.Fatalf("claims = %+v, want some admitted before the retirement and the rest refused", claims.Claims)
		}
		if !reflect.DeepEqual(ret.InFlight, admitted) {
			t.Fatalf("retirement reports in flight %v, want exactly the admitted claims %v", ret.InFlight, admitted)
		}
		e, err := h.Entry(ctx, q.ID)
		if err != nil {
			t.Fatal(err)
		}
		if e.State != StateRetired || !reflect.DeepEqual(e.InFlight, admitted) {
			t.Fatalf("entry = %+v, want it retired with the admitted claims still in flight", e)
		}
		revision, err := h.Revision(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if revision != seeded+int64(len(admitted))+1 {
			t.Fatalf("revision %d, want the seed %d, %d claims and the retirement", revision, seeded, len(admitted))
		}
	})
}

// TestHeldNotebookHelper is the second process of the tests above. It does
// nothing unless started by startHeldHelper.
func TestHeldNotebookHelper(t *testing.T) {
	mode := os.Getenv(heldHelperMode)
	if mode == "" {
		t.Skip("helper process only")
	}
	ctx := context.Background()
	q := mustQuestion(t, "wren", "question.json")
	id := q.Identity
	h := Held{Dir: os.Getenv(heldHelperDir), Identity: id}
	var report any
	switch mode {
	case "reopen":
		var r reopenReport
		var err error
		if r.Revision, err = h.Revision(ctx); err != nil {
			t.Fatal(err)
		}
		if r.Entry, err = h.Entry(ctx, q.ID); err != nil {
			t.Fatal(err)
		}
		// A replay is answered from the request log the writer left, and
		// returns the receipt exactly as that process first produced it.
		stale := Request{Identity: id, Revision: 0}
		act := mustAction(t, "wren", "action.json")
		stale.RequestID = "claim"
		if r.Claimed, err = h.Claim(ctx, stale, act); err != nil {
			t.Fatal(err)
		}
		stale.RequestID = "settle"
		if r.Settled, err = h.Settle(ctx, stale, settle(r.Claimed, "village2/show-whether-week-a0f2#note-1")); err != nil {
			t.Fatal(err)
		}
		stale.RequestID = "claim-2"
		if r.InFlight, err = h.Claim(ctx, stale, numberedAction(t, 2)); err != nil {
			t.Fatal(err)
		}
		_, err = h.Claim(ctx, heldAt(t, h, id, "reclaim"), act)
		r.Reclaim, _ = conformanceRefusal(err)
		if r.AfterRevision, err = h.Revision(ctx); err != nil {
			t.Fatal(err)
		}
		report = r
	case "record":
		prefix := os.Getenv(heldHelperArg)
		a := mustAssessment(t, "wren", "assessment.json")
		var r recordReport
		for i := range heldWriterRecords {
			_, req, err := heldUntil(t, h, id, fmt.Sprintf("%s-%d", prefix, i), func(req Request) (*Entry, error) {
				return h.Record(ctx, req, a)
			})
			if err != nil {
				t.Fatal(err)
			}
			r.Revisions = append(r.Revisions, req.Revision)
		}
		report = r
	case "claim":
		// Claim until the retirement has refused two in a row, so the run
		// always holds both sides of the order.
		var r claimReport
		refusals := 0
		for n := 1; refusals < 2; n++ {
			if n > 500 {
				t.Fatal("no retirement landed within 500 claims")
			}
			act := numberedAction(t, n)
			_, req, err := heldUntil(t, h, id, "claim-"+strconv.Itoa(n), func(req Request) (*Receipt, error) {
				return h.Claim(ctx, req, act)
			})
			attempt := claimAttempt{ActionID: act.ID, Revision: req.Revision}
			if err != nil {
				refusal, ok := conformanceRefusal(err)
				if !ok {
					t.Fatal(err)
				}
				attempt.Refusal = refusal
				refusals++
			}
			r.Claims = append(r.Claims, attempt)
			time.Sleep(5 * time.Millisecond)
		}
		report = r
	case "retire":
		// Retire once the claimer has claims in flight.
		after, err := strconv.ParseInt(os.Getenv(heldHelperArg), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		deadline := time.Now().Add(30 * time.Second)
		for heldAt(t, h, id, "").Revision < after {
			if time.Now().After(deadline) {
				t.Fatalf("revision never reached %d", after)
			}
			time.Sleep(time.Millisecond)
		}
		ret, _, err := heldUntil(t, h, id, "retire", func(req Request) (*Retirement, error) {
			return h.Retire(ctx, req, q.ID, "Wren-1 was archived")
		})
		if err != nil {
			t.Fatal(err)
		}
		report = ret
	default:
		t.Fatalf("unknown helper mode %q", mode)
	}
	data, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(os.Getenv(heldHelperOut), data, 0o600); err != nil {
		t.Fatal(err)
	}
}

func heldGit(t *testing.T, h Held, args ...string) string {
	t.Helper()
	out, err := exec.Command("git", append([]string{"-C", h.Dir}, args...)...).CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v: %s", args, err, out)
	}
	return strings.TrimSpace(string(out))
}

func heldCommits(t *testing.T, h Held) int {
	t.Helper()
	n, err := strconv.Atoi(heldGit(t, h, "rev-list", "--count", "--all"))
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func TestHeldNotebookCommitsEachMutation(t *testing.T) {
	ctx := context.Background()
	h := newHeld(t)
	q := mustQuestion(t, "wren", "question.json")
	id := q.Identity
	exp := mustExperiment(t, "wren", "experiment.json")
	a := mustAssessment(t, "wren", "assessment.json")
	changed := *a
	changed.Interpretation = "A different reading under the same request id."
	act := mustAction(t, "wren", "action.json")
	var claimed *Receipt

	steps := []struct {
		name    string
		do      func() error
		refusal error
	}{
		{name: "register", do: func() error { _, err := h.Register(ctx, heldAt(t, h, id, "register"), q); return err }},
		{name: "stale register", refusal: ErrStaleRevision, do: func() error {
			_, err := h.Register(ctx, Request{Identity: id, Revision: 0, RequestID: "register-again"}, q)
			return err
		}},
		{name: "activate", do: func() error { _, err := h.Activate(ctx, heldAt(t, h, id, "activate"), exp); return err }},
		{name: "record", do: func() error { _, err := h.Record(ctx, heldAt(t, h, id, "record"), a); return err }},
		{name: "replayed record", do: func() error {
			_, err := h.Record(ctx, Request{Identity: id, Revision: 2, RequestID: "record"}, a)
			return err
		}},
		{name: "conflicting record", refusal: ErrRequestConflict, do: func() error {
			_, err := h.Record(ctx, Request{Identity: id, Revision: 2, RequestID: "record"}, &changed)
			return err
		}},
		{name: "claim", do: func() error {
			var err error
			claimed, err = h.Claim(ctx, heldAt(t, h, id, "claim"), act)
			return err
		}},
		{name: "settle", do: func() error {
			_, err := h.Settle(ctx, heldAt(t, h, id, "settle"), settle(claimed, "village2/show-whether-week-a0f2#note-1"))
			return err
		}},
		{name: "retire", do: func() error {
			_, err := h.Retire(ctx, heldAt(t, h, id, "retire"), q.ID, "Wren-1 was archived")
			return err
		}},
		{name: "claim after retirement", refusal: ErrRetired, do: func() error {
			_, err := h.Claim(ctx, heldAt(t, h, id, "claim-2"), numberedAction(t, 2))
			return err
		}},
	}
	accepted := 0
	for _, step := range steps {
		before, err := h.Revision(ctx)
		if err != nil {
			t.Fatal(err)
		}
		err = step.do()
		if step.refusal != nil && !errors.Is(err, step.refusal) || step.refusal == nil && err != nil {
			t.Fatalf("%s: got %v, want %v", step.name, err, step.refusal)
		}
		after, err := h.Revision(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if after > before {
			accepted++
		}
		if commits := heldCommits(t, h); commits != accepted || int64(accepted) != after {
			t.Fatalf("%s: %d commits at revision %d, want one per accepted mutation (%d)", step.name, commits, after, accepted)
		}
		if status := heldGit(t, h, "status", "--porcelain"); status != "" {
			t.Fatalf("%s left the tree uncommitted:\n%s", step.name, status)
		}
	}
	if accepted != 6 {
		t.Fatalf("%d accepted, want register, activate, record, claim, settle and retire", accepted)
	}
	subjects := heldGit(t, h, "log", "--reverse", "--format=%s")
	want := strings.Join([]string{
		"register " + q.ID, "activate " + q.ID, "record " + q.ID,
		"claim " + act.ID, "settle " + act.ID, "retire " + q.ID,
	}, "\n")
	if subjects != want {
		t.Fatalf("commit subjects:\n%s\nwant:\n%s", subjects, want)
	}
	entries, err := os.ReadDir(filepath.Join(h.Dir, heldEntriesDir))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("%d entry files, want one for the one entry", len(entries))
	}

	// A writer that died between writing its files and committing them left
	// the pending marker; the next operation rolls the tree back to the last
	// commit rather than sweeping that write into a later one.
	if err := os.WriteFile(h.pending(), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	stray := filepath.Join(h.Dir, heldEntriesDir, "stray.json")
	if err := os.WriteFile(stray, []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(h.Dir, heldRevisionFile), []byte(`{"revision":99}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if revision, err := h.Revision(ctx); err != nil || revision != int64(accepted) {
		t.Fatalf("revision after an interrupted write = %d, %v; want %d", revision, err, accepted)
	}
	if status := heldGit(t, h, "status", "--porcelain"); status != "" {
		t.Fatalf("interrupted write was not rolled back:\n%s", status)
	}
	if _, err := os.Stat(h.pending()); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("pending marker after recovery: %v", err)
	}

	// A git add killed at its timeout also leaves index.lock behind; the
	// rollback clears it rather than failing on it from then on.
	indexLock := filepath.Join(h.Dir, ".git", "index.lock")
	for _, path := range []string{h.pending(), indexLock} {
		if err := os.WriteFile(path, nil, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(h.Dir, heldRevisionFile), []byte(`{"revision":99}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if revision, err := h.Revision(ctx); err != nil || revision != int64(accepted) {
		t.Fatalf("revision after a write killed holding index.lock = %d, %v; want %d", revision, err, accepted)
	}
	if _, err := os.Stat(indexLock); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("index.lock after recovery: %v", err)
	}

	// A file the notebook did not write, such as Finder's .DS_Store, is not
	// one of its entries and does not fail the read.
	if err := os.WriteFile(filepath.Join(h.Dir, heldEntriesDir, ".DS_Store"), []byte("\x00\x00\x00\x01Bud1"), 0o600); err != nil {
		t.Fatal(err)
	}
	if e, err := h.Entry(ctx, q.ID); err != nil || e.State != StateRetired {
		t.Fatalf("entry beside a .DS_Store = %+v, %v; want it read as retired", e, err)
	}
}
