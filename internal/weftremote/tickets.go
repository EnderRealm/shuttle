package weftremote

import (
	"strings"

	"github.com/EnderRealm/ticket/v8/pkg/ticket"

	"shuttle/internal/provider/tkconfig"
)

type ticketCatalog struct {
	config tkconfig.Config
	store  *ticket.MultiStore
}

func loadTickets(configPath string) (*ticketCatalog, error) {
	cfg, err := tkconfig.Load(configPath)
	if err != nil {
		return nil, err
	}
	return &ticketCatalog{config: cfg, store: ticket.NewMultiStore(cfg.TicketsRoot())}, nil
}

func (c *ticketCatalog) all() ([]*ticket.Ticket, error) {
	tickets, _, err := c.store.ListWithSkips()
	return tickets, err
}

func (c *ticketCatalog) get(id string) (*ticket.Ticket, error) {
	return c.store.Get(id)
}

func projectOf(id string) string {
	project, _, _ := strings.Cut(id, "/")
	return project
}

func qualifyDep(ticketID, dep string) string {
	if strings.Contains(dep, "/") {
		return dep
	}
	return projectOf(ticketID) + "/" + dep
}

// resolved is whether the ticket's work is over: testing, done or closed. As a
// dependency it is satisfied, which pkg/ticket decides the same way and does
// not export (§5.2); as a run's ticket it is finished (§6.8). It is not
// whether an epic's child is finished, which one in testing is not (§5.4).
func resolved(t *ticket.Ticket) bool {
	return t.Status == ticket.StatusTesting || t.Status == ticket.StatusDone || t.Status == ticket.StatusClosed
}

// failedTest is whether the ticket reads ready with its last test failed
// (§6.8).
func failedTest(t *ticket.Ticket) bool {
	row := ticket.LastTest(t)
	return t.Status == ticket.StatusReady && row != nil && row.Result == ticket.TestFailed
}

func launchable(t *ticket.Ticket, byID map[string]*ticket.Ticket) (bool, string) {
	if t.Status != ticket.StatusReady && t.Status != ticket.StatusOpen {
		return false, "status is " + string(t.Status)
	}
	if t.Type == ticket.TypeEpic {
		return false, "type is epic"
	}
	for _, dep := range t.Deps {
		id := qualifyDep(t.ID, dep)
		d, ok := byID[id]
		if !ok {
			return false, "dependency " + id + " is missing"
		}
		if !resolved(d) {
			return false, "dependency " + id + " is " + string(d.Status)
		}
	}
	return true, ""
}
