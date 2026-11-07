package weftremote

import (
	"errors"
	"fmt"
	"net/http"
	"regexp"

	"github.com/EnderRealm/ticket/v8/pkg/ticket"

	"shuttle/internal/ledger"
)

var ticketEdit = regexp.MustCompile(`^/v2/tickets/([^/]+)/([^/]+)/edit$`)

type ticketEditRequest struct {
	ContractVersion string  `json:"contract_version"`
	Status          *string `json:"status"`
	Priority        *int    `json:"priority"`
	ReadStatus      string  `json:"read_status"`
	ReadPriority    *int    `json:"read_priority"`
}

// handleTicketEdit is the host's one ticket write (§5.4): a ticket's status,
// its priority or both, written through pkg/ticket and nothing else with
// them. Its refusals come in the contract's order, and each writes nothing.
func (s *Service) handleTicketEdit(w http.ResponseWriter, r *http.Request, project, slug string) {
	id := project + "/" + slug
	s.mutateV2(w, r, func(body []byte, _ string) (v2Commit, error) {
		var req ticketEditRequest
		if err := decodeV2(body, &req, "status", "priority", "read_status", "read_priority"); err != nil {
			return refusedBy(err), nil
		}
		switch {
		case req.Status == nil && req.Priority == nil:
			return refused("malformed", "an edit of %s names a status, a priority or both, and this one names neither", id), nil
		case req.Status != nil && ticket.ValidateStatus(ticket.Status(*req.Status)) != nil:
			return refused("malformed", "status '%s' is not one of backlog, ready, open, done, closed", *req.Status), nil
		case req.Priority != nil && ticket.ValidatePriority(*req.Priority) != nil:
			return refused("malformed", "priority %d is outside 0-4", *req.Priority), nil
		}
		c, err := s.readCatalog()
		if err != nil {
			return nil, err
		}
		if _, ok := c.snap.Get(id); c.byName[project] == nil || !ok {
			return refused("ticket_not_found", "ticket %s is not on this host", id), nil
		}
		return func(tx *ledger.WeftTx) (v2Outcome, error) {
			// Under the state lock, so no dispatch lands between the guard
			// and the write.
			if err := tx.EditGuard(id); err != nil {
				return v2Outcome{}, err
			}
			wrote, err := s.editTicket(tx, c, id, req)
			if err != nil {
				return v2Outcome{}, err
			}
			// An edit that changed nothing wrote nothing and is no catalog
			// event; one that wrote is read back and recorded, raising
			// catalog_revision before the response's as_of is taken.
			if wrote {
				if c, err = s.readCatalog(); err != nil {
					return v2Outcome{}, err
				}
				if err := s.recordCatalogTx(tx, c); err != nil {
					return v2Outcome{}, err
				}
			}
			t, ok := c.snap.Get(id)
			if !ok || c.byName[project] == nil {
				return v2Outcome{}, fmt.Errorf("%s is no longer in tk after its edit", id)
			}
			runs, err := tx.WeftRuns(id)
			if err != nil {
				return v2Outcome{}, err
			}
			v := &catalogView{catalog: c, runs: map[string][]*ledger.WeftRun{id: runs}}
			if v.entries, err = tx.PendingEntries(); err != nil {
				return v2Outcome{}, err
			}
			return v2Outcome{status: http.StatusOK, fields: map[string]any{"ticket": v.ticketDetail(c.byName[project], t)}}, nil
		}, nil
	})
}

// editTicket holds the ticket as tk reads it now to what the client read,
// then writes the edit through pkg/ticket's own edit path — an epic set
// closed is abandoned as tk abandons one. It reports whether anything was
// written: a priority equal to the current one changes nothing.
func (s *Service) editTicket(tx *ledger.WeftTx, c *catalog, id string, req ticketEditRequest) (bool, error) {
	t, err := c.store.Get(id)
	if err != nil {
		return false, err
	}
	if t.Status != ticket.Status(req.ReadStatus) || req.ReadPriority == nil || *req.ReadPriority != t.Priority {
		read := "none"
		if req.ReadPriority != nil {
			read = fmt.Sprint(*req.ReadPriority)
		}
		return false, &ledger.WeftRefusal{Code: "revision_conflict", Message: fmt.Sprintf(
			"%s is at status %s, priority %d and the edit read status %s, priority %s: read it again and decide again",
			id, t.Status, t.Priority, req.ReadStatus, read)}
	}
	changed := false
	if req.Status != nil {
		status := ticket.Status(*req.Status)
		switch {
		case status == t.Status:
			return false, &ledger.WeftRefusal{Code: "malformed", Message: fmt.Sprintf(
				"%s is already %s: a status edit names another status", id, t.Status)}
		case t.Type == ticket.TypeEpic && status != ticket.StatusClosed:
			return false, &ledger.WeftRefusal{Code: "malformed", Message: fmt.Sprintf(
				"%s is an epic, whose status tk derives: closed is the only status an edit can set on it", id)}
		}
		t.Status, changed = status, true
	}
	if req.Priority != nil && *req.Priority != t.Priority {
		t.Priority, changed = *req.Priority, true
	}
	if !changed {
		return false, nil
	}
	if t.Type == ticket.TypeEpic && req.Status != nil {
		// The abandon closes every non-terminal child in the epic's own
		// project, so each is held to the epic's guard: the cascade must not
		// write a status under a child's run any more than the edit itself.
		snap, err := c.store.Snapshot()
		if err != nil {
			return false, err
		}
		project, _ := ticket.ParseNamespacedID(id)
		for _, child := range snap.Children(id) {
			ns, _ := ticket.ParseNamespacedID(child.ID)
			if ns != project || child.Status == ticket.StatusDone || child.Status == ticket.StatusClosed {
				continue
			}
			if err := tx.EditGuard(child.ID); err != nil {
				return false, err
			}
		}
	}
	closed, err := ticket.SaveEdit(c.store, t, req.Status != nil)
	if len(closed) > 0 {
		s.log.Printf("weft ticket edit host=%s: %s abandoned%s", s.hostID, id, ticket.ClosedChildrenNote(closed))
	}
	if err != nil {
		// The file moved between the read above and the write, which wrote
		// nothing; the values compared may no longer be the ticket's.
		if errors.Is(err, ticket.ErrConflict) {
			return false, &ledger.WeftRefusal{Code: "revision_conflict", Message: fmt.Sprintf(
				"%s changed in tk while the edit was being written: read it again and decide again", id)}
		}
		return false, fmt.Errorf("edit %s: %w", id, err)
	}
	return true, nil
}
