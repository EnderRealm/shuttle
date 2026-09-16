package weftremote

import (
	"fmt"
	"strings"

	"github.com/EnderRealm/ticket/v8/pkg/ticket"

	"shuttle/internal/provider/tkconfig"
	"shuttle/internal/sandbox"
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

func (c *ticketCatalog) projectPath(project string) (string, error) {
	p, ok := c.config.Projects[project]
	if !ok || p.Path == "" {
		return "", fmt.Errorf("project %s has no configured path", project)
	}
	path, err := sandbox.CanonicalRepository(p.Path)
	if err != nil {
		return "", fmt.Errorf("project %s: %w", project, err)
	}
	return path, nil
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
		if d.Status != ticket.StatusDone && d.Status != ticket.StatusClosed {
			return false, "dependency " + id + " is " + string(d.Status)
		}
	}
	return true, ""
}
