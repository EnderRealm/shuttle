package observer

import (
	"encoding/json"
	"testing"
)

// mustQuestion and friends are the fixtures as the notebook receives them:
// validated payloads, never structs built in the test, so the fixtures and the
// contract cannot drift apart.
func mustQuestion(t *testing.T, domain, name string) *Question {
	t.Helper()
	q, err := ValidateQuestion(load(t, domain, name))
	if err != nil {
		t.Fatal(err)
	}
	return q
}

func mustExperiment(t *testing.T, domain, name string) *Experiment {
	t.Helper()
	e, err := ValidateExperiment(load(t, domain, name))
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func mustAssessment(t *testing.T, domain, name string) *Assessment {
	t.Helper()
	a, err := ValidateAssessment(load(t, domain, name))
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func mustAction(t *testing.T, domain, name string) *Action {
	t.Helper()
	a, err := ValidateAction(load(t, domain, name))
	if err != nil {
		t.Fatal(err)
	}
	return a
}

// at composes a request against the notebook's current revision.
func at(nb *Memory, id Identity, requestID string) Request {
	return Request{Identity: id, Revision: nb.Revision(), RequestID: requestID}
}

// TestDomainsDriveOneNotebook runs Wren's questions and the harness's through
// the same contract, in one notebook, and holds the two apart: same local name,
// same shape of action, the same raw request ids, and neither observer's entry,
// receipt nor request outcome is reachable from the other's.
func TestDomainsDriveOneNotebook(t *testing.T) {
	nb := NewMemory()
	receipts := map[string]*Receipt{}
	findings := map[string]*Entry{}

	for _, domain := range []string{"wren", "harness"} {
		t.Run(domain, func(t *testing.T) {
			q := mustQuestion(t, domain, "question.json")
			id := q.Identity

			e, err := nb.Register(at(nb, id, "register"), q)
			if err != nil {
				t.Fatal(err)
			}
			if e.State != StateObserving {
				t.Fatalf("state = %q, want %q", e.State, StateObserving)
			}

			// The change is registered when it lands and activated when it
			// ships: the first payload names only the commit.
			pending := mustExperiment(t, domain, "experiment-pending.json")
			e, err = nb.Activate(at(nb, id, "register-experiment"), pending)
			if err != nil {
				t.Fatal(err)
			}
			if e.ExperimentStatus != ExperimentPending {
				t.Fatalf("status = %q, want %q on a commit time alone", e.ExperimentStatus, ExperimentPending)
			}

			live := mustExperiment(t, domain, "experiment.json")
			e, err = nb.Activate(at(nb, id, "activate"), live)
			if err != nil {
				t.Fatal(err)
			}
			if e.ExperimentStatus != ExperimentLive {
				t.Fatalf("status = %q, want %q", e.ExperimentStatus, ExperimentLive)
			}

			a := mustAssessment(t, domain, "assessment.json")
			e, err = nb.Record(at(nb, id, "record"), a)
			if err != nil {
				t.Fatal(err)
			}
			if e.State != StateAnswered {
				t.Fatalf("state = %q, want %q after %q", e.State, StateAnswered, a.Outcome)
			}
			if len(e.Observations) != 1 {
				t.Fatalf("observations = %d, want 1", len(e.Observations))
			}

			act := mustAction(t, domain, "action.json")
			rec, err := nb.Claim(at(nb, id, "claim"), act)
			if err != nil {
				t.Fatal(err)
			}
			if rec.Status != ReceiptInFlight {
				t.Fatalf("status = %q, want %q", rec.Status, ReceiptInFlight)
			}

			settled := *rec
			settled.Status = ReceiptSettled
			settled.Outcome = ReceiptApplied
			settled.Reference = act.Ticket + "#note-1"
			// The settlement comes back over the wire like everything else, so
			// it is decoded and validated rather than handed over as a struct.
			wire, err := json.Marshal(&settled)
			if err != nil {
				t.Fatal(err)
			}
			reconciled, err := ValidateReceipt(wire)
			if err != nil {
				t.Fatal(err)
			}
			rec, err = nb.Settle(at(nb, id, "settle"), reconciled)
			if err != nil {
				t.Fatal(err)
			}
			if rec.Status != ReceiptSettled || rec.Reference != settled.Reference {
				t.Fatalf("receipt = %+v, want it settled against %s", rec, settled.Reference)
			}
			receipts[act.ID] = rec

			e, err = nb.Entry(q.ID)
			if err != nil {
				t.Fatal(err)
			}
			if len(e.InFlight) != 0 {
				t.Fatalf("in flight = %v, want none once settled", e.InFlight)
			}

			// An unprompted finding takes the same full Record path in each
			// domain and opens its own entry under the observer's qualified id.
			finding := mustAssessment(t, domain, "assessment-unprompted.json")
			e, err = nb.Record(at(nb, finding.Identity, "finding"), finding)
			if err != nil {
				t.Fatal(err)
			}
			if e.ID != finding.Finding.ID {
				t.Fatalf("entry id = %q, want the finding's own id %q", e.ID, finding.Finding.ID)
			}
			if e.Question != nil {
				t.Fatal("an unprompted finding was filed under a question")
			}
			if e.Finding.Subject != finding.Evidence.Subject {
				t.Fatalf("entry subject = %+v, evidence is about %+v", e.Finding.Subject, finding.Evidence.Subject)
			}
			// insufficient-evidence never closes anything, least of all a
			// first sighting.
			if e.State != StateObserving {
				t.Fatalf("state = %q, want %q", e.State, StateObserving)
			}
			findings[domain] = e
		})
	}

	// The same local names and raw request ids under two observers produce
	// distinct question and finding entries.
	wren, err := nb.Entry("wren/mead/days-repeating")
	if err != nil {
		t.Fatal(err)
	}
	harness, err := nb.Entry("harness/shuttle/days-repeating")
	if err != nil {
		t.Fatal(err)
	}
	if wren.Identity == harness.Identity {
		t.Fatalf("both entries are held by %v", wren.Identity)
	}
	if wren.Question.Question == harness.Question.Question {
		t.Fatal("the two observers' questions are the same text")
	}
	if len(receipts) != 2 {
		t.Fatalf("receipts = %d, want one per observer", len(receipts))
	}
	if receipts["wren/mead/note-days-repeating"].Reference == receipts["harness/shuttle/note-days-repeating"].Reference {
		t.Fatal("the two observers' receipts carry the same reference")
	}
	if findings["wren"].ID == findings["harness"].ID {
		t.Fatalf("both findings became %q", findings["wren"].ID)
	}
	if findings["wren"].Identity == findings["harness"].Identity {
		t.Fatalf("both findings are held by %v", findings["wren"].Identity)
	}
}
