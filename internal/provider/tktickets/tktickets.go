// Package tktickets reports the tickets the window touched, read through
// pkg/ticket from the central store (§8.4).
package tktickets

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/EnderRealm/ticket/v8/pkg/ticket"

	"shuttle/internal/provider"
	"shuttle/internal/provider/tkconfig"
)

const (
	mutationsFile = "mutations.jsonl"

	statusField = "status"

	// limitations is emitted in the document, not only recorded here: the model
	// summarising the day has to know what the status changes it is reading
	// cannot tell it (§8.4).
	limitations = "mutations.jsonl records only writes made on this machine, so a status change synced " +
		"in from elsewhere appears as a ticket updated in the window with no status change entry. " +
		"tk keeps no from/to transition history, so a change shows the ticket's current status rather " +
		"than the status it moved from."
)

type document struct {
	Tickets       []entry           `json:"tickets"`
	StatusChanges []statusChange    `json:"status_changes"`
	Skips         []ticket.FileSkip `json:"skips"`
	SkippedLines  int               `json:"skipped_lines"`
	Limitations   string            `json:"limitations"`
}

type entry struct {
	ID        string `json:"id"`
	Project   string `json:"project"`
	Title     string `json:"title"`
	Status    string `json:"status"`
	Type      string `json:"type"`
	Priority  int    `json:"priority"`
	Completed string `json:"completed,omitempty"`
	Notes     []note `json:"notes"`
}

type note struct {
	At   string `json:"at"`
	Text string `json:"text"`
}

type statusChange struct {
	Ticket string `json:"ticket"`
	At     string `json:"at"`
	Source string `json:"source"`
	// Status is the ticket's status now, which is the transition's destination
	// only when nothing has moved it since. Empty when the ticket is no longer
	// in the listing.
	Status string `json:"status"`
}

// Run implements provider.Func.
func Run(_ context.Context, env provider.Env, _ map[string]any, w provider.Window) (json.RawMessage, error) {
	cfg, err := tkconfig.Load(env.TKConfig)
	if err != nil {
		return nil, err
	}
	// pkg/ticket warns rather than failing over a skip; the run's logger takes
	// those for the length of the call so they land in the run's stderr log
	// rather than the daemon's. Runs are sequential per agent (§7.1), and the
	// providers within one run run one at a time, so the swap has no reader
	// racing it.
	restore := ticket.Warnf
	ticket.Warnf = func(format string, args ...any) { env.Logger.Printf(strings.TrimRight(format, "\n"), args...) }
	defer func() { ticket.Warnf = restore }()

	// Reads take no lock in pkg/ticket and start no goroutines, so a running
	// `tk serve` is unaffected. ListWithSkips rather than List: a file the
	// store could not read is reported in the document, not swallowed.
	store := ticket.NewMultiStore(cfg.TicketsRoot())
	tickets, skips, err := store.ListWithSkips()
	if err != nil {
		return nil, err
	}
	if skips == nil {
		skips = []ticket.FileSkip{}
	}

	doc := document{Tickets: []entry{}, StatusChanges: []statusChange{}, Skips: skips, Limitations: limitations}
	status := make(map[string]string, len(tickets))
	for _, t := range tickets {
		status[t.ID] = string(t.Status)
		if !w.Contains(t.Updated) {
			continue
		}
		project, _, _ := strings.Cut(t.ID, "/")
		e := entry{
			ID:       t.ID,
			Project:  project,
			Title:    t.Title,
			Status:   string(t.Status),
			Type:     string(t.Type),
			Priority: t.Priority,
			Notes:    []note{},
		}
		if !t.Completed.IsZero() {
			e.Completed = t.Completed.UTC().Format(time.RFC3339)
		}
		for _, n := range t.Notes {
			if w.Contains(n.Timestamp) {
				e.Notes = append(e.Notes, note{At: n.Timestamp.UTC().Format(time.RFC3339), Text: n.Text})
			}
		}
		doc.Tickets = append(doc.Tickets, e)
	}
	slices.SortFunc(doc.Tickets, func(a, b entry) int { return strings.Compare(a.ID, b.ID) })

	for _, project := range cfg.Names() {
		changes, skipped, err := readStatusChanges(filepath.Join(tkconfig.StateDir(env.TKConfig, project), mutationsFile),
			project, w, status, env)
		if err != nil {
			return nil, err
		}
		doc.StatusChanges = append(doc.StatusChanges, changes...)
		doc.SkippedLines += skipped
	}
	return json.Marshal(doc)
}

// readStatusChanges reconstructs the day's status transitions from the project's
// mutation log, which is the only record tk keeps of one. It also returns the
// number of lines it could not use: a malformed line costs the day a status
// change, so the document says how many rather than only the log.
func readStatusChanges(path, project string, w provider.Window, status map[string]string, env provider.Env) ([]statusChange, int, error) {
	f, err := os.Open(path)
	if err != nil {
		// A project nobody has written to on this machine has no log.
		if errors.Is(err, os.ErrNotExist) {
			return nil, 0, nil
		}
		return nil, 0, err
	}
	defer f.Close()

	var (
		changes []statusChange
		skipped int
	)
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64*1024), 1024*1024)
	for sc.Scan() {
		if len(sc.Bytes()) == 0 {
			continue
		}
		var m ticket.MutationEntry
		if err := json.Unmarshal(sc.Bytes(), &m); err != nil {
			env.Logger.Printf("tk-tickets: %s: %v", path, err)
			skipped++
			continue
		}
		if !w.Contains(m.Timestamp) || !slices.Contains(m.FieldsChanged, statusField) {
			continue
		}
		id := project + "/" + m.TicketID
		changes = append(changes, statusChange{
			Ticket: id,
			At:     m.Timestamp.UTC().Format(time.RFC3339),
			Source: m.Source,
			Status: status[id],
		})
	}
	return changes, skipped, sc.Err()
}
