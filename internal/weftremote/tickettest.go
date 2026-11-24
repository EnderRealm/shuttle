package weftremote

import (
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/EnderRealm/ticket/v8/pkg/ticket"

	"shuttle/internal/ledger"
)

var ticketTest = regexp.MustCompile(`^/v2/tickets/([^/]+)/([^/]+)/test$`)

// testVerb is editVerb for a ticket's test.
const testVerb = "test"

// The contract's test actions (§5.5).
const (
	testActionPass = "pass"
	testActionFail = "fail"
	testActionUndo = "undo"
)

// tk requires a note on every action, and a pass and an undo carry none.
const (
	passNote = "Test passed."
	undoNote = "Test undone."
)

type ticketTestRequest struct {
	ContractVersion string     `json:"contract_version"`
	Action          string     `json:"action"`
	Checks          []int      `json:"checks"`
	Note            *string    `json:"note"`
	ReadUpdated     *time.Time `json:"read_updated"`
}

// handleTicketTest is the other of the host's two ticket writes (§5.5): a
// human's pass, fail or undo of a landed ticket's test, written through
// pkg/ticket's own actions. Its refusals come in the contract's order, and
// each writes nothing.
func (s *Service) handleTicketTest(w http.ResponseWriter, r *http.Request, project, slug string) {
	s.mutateV2(w, r, func(body []byte, key string) (v2Commit, error) {
		var req ticketTestRequest
		if err := decodeV2(body, &req, "action", "checks", "note", "read_updated"); err != nil {
			return refusedBy(err), nil
		}
		// The action's id in tk is the request's, so a retry whose first
		// answer was lost with its ledger commit is the same action.
		action := ticket.ActionRequest{ActionID: "weft-test-" + ledger.BodySHA(slices.Concat([]byte(key), []byte{0}, body))}
		carriesNothing := len(req.Checks) == 0 && req.Note == nil
		switch req.Action {
		case testActionPass:
			if !carriesNothing {
				return refused("malformed", "a pass carries no checks and no note"), nil
			}
			action.Transition, action.Note = ticket.TransitionPass, passNote
		case testActionUndo:
			if !carriesNothing {
				return refused("malformed", "an undo carries no checks and no note"), nil
			}
			action.Transition, action.Note = ticket.TransitionRetest, undoNote
		case testActionFail:
			switch {
			case len(req.Checks) == 0:
				return refused("malformed", "a fail names at least one check"), nil
			case !distinctPositive(req.Checks):
				return refused("malformed", "checks %s are not distinct positive indexes", strings.ReplaceAll(fmt.Sprint(req.Checks), " ", ", ")), nil
			case req.Note == nil || strings.TrimSpace(*req.Note) == "":
				return refused("malformed", "a fail carries a note"), nil
			}
			action.Transition, action.Note, action.Checks = ticket.TransitionFail, *req.Note, req.Checks
		default:
			return refused("malformed", "action '%s' is not one of pass, fail, undo", req.Action), nil
		}
		switch {
		case req.Checks == nil:
			return refused("malformed", "checks is null"), nil
		case req.ReadUpdated == nil:
			return refused("malformed", "read_updated is null"), nil
		}
		return s.writeTicket(project, slug, func(tx *ledger.WeftTx, c *catalog, id string) (bool, error) {
			action.ID = id
			return true, s.testTicket(tx, c, *req.ReadUpdated, action)
		})
	})
}

// testTicket guards the ticket under the state lock, so no dispatch lands
// between the guard and the write, holds it as tk reads it now to what the
// client read, then applies the action against that read. Whether the action
// applies to the ticket is tk's to decide.
func (s *Service) testTicket(tx *ledger.WeftTx, c *catalog, read time.Time, action ticket.ActionRequest) error {
	id := action.ID
	t, err := c.store.Get(id)
	if err != nil {
		return err
	}
	// An action tk has already applied moved the ticket itself, so neither the
	// guard nor the read is held against its retry: tk answers it as recorded.
	if !slices.ContainsFunc(t.Actions, func(r ticket.ActionReceipt) bool {
		return r.Kind == ticket.ReceiptApply && r.ID == action.ActionID
	}) {
		if err := tx.EditGuard(id, testVerb); err != nil {
			return err
		}
		// The instant the host serves is to the millisecond.
		if !read.Equal(t.Updated.Truncate(time.Millisecond)) {
			return &ledger.WeftRefusal{Code: "revision_conflict", Message: fmt.Sprintf(
				"%s was updated at %s and the test read it at %s: read it again and decide again",
				id, formatTime(t.Updated), formatTime(read))}
		}
	}
	action.Precondition = t.Precondition()
	_, err = ticket.ApplyAction(c.store, action)
	switch {
	case err == nil:
		return nil
	case errors.Is(err, ticket.ErrConflict):
		// The file moved between the read above and the write, which wrote
		// nothing.
		return &ledger.WeftRefusal{Code: "revision_conflict", Message: fmt.Sprintf(
			"%s changed in tk while the test was being written: read it again and decide again", id)}
	case errors.Is(err, ticket.ErrTransitionNotPermitted) && action.Transition == ticket.TransitionRetest:
		return &ledger.WeftRefusal{Code: "no_test_to_undo", Message: fmt.Sprintf(
			"%s is %s and holds no test that left it there", id, t.Status)}
	case errors.Is(err, ticket.ErrTransitionNotPermitted):
		return &ledger.WeftRefusal{Code: "ticket_not_testing", Message: fmt.Sprintf(
			"%s is %s, not testing", id, t.Status)}
	}
	// tk refuses a check past the ticket's criteria, with an error no caller
	// can tell from a failed write.
	if count := testCriteriaCount(t); action.Transition == ticket.TransitionFail && slices.Max(action.Checks) > count {
		return &ledger.WeftRefusal{Code: "malformed", Message: fmt.Sprintf(
			"%s has %d checks, and the fail names check %d", id, count, slices.Max(action.Checks))}
	}
	return fmt.Errorf("test %s: %w", id, err)
}

// testCriteriaCount is how many of the ticket's criteria a person checks.
func testCriteriaCount(t *ticket.Ticket) int {
	count := 0
	for _, c := range ticket.ParseCriteria(ticket.AcceptanceCriteria(t.Body)) {
		if c.Kind() == ticket.CriterionTest {
			count++
		}
	}
	return count
}

func distinctPositive(checks []int) bool {
	distinct := slices.Compact(slices.Sorted(slices.Values(checks)))
	return distinct[0] >= 1 && len(distinct) == len(checks)
}
