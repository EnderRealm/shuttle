// Package tktickets reports the tickets the window touched, read through
// pkg/ticket from the central store (§8.4). It walks ticket namespaces — Root
// and namespaces with no repository included — never repositories.
package tktickets

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"maps"
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
		"than the status it moved from. " +
		"An epic's status and completed are derived from all of its children in every namespace, and " +
		"that derivation writes nothing to the epic, so an epic is listed when its own updated falls in " +
		"the window or a child changed in it; changed_by names those children — updated or completed in " +
		"the window, or a status change logged on this machine. A child's completion or reopen synced " +
		"in from another machine shows only through that child's updated time, and a child that stopped " +
		"being the epic's child in the window (re-parented or deleted) is not traced back to the epic. " +
		"While complete is false no epic reads done or closed, and diagnostics names what could not be read."
)

type document struct {
	Tickets       []entry           `json:"tickets"`
	StatusChanges []statusChange    `json:"status_changes"`
	Skips         []ticket.FileSkip `json:"skips"`
	SkippedLines  int               `json:"skipped_lines"`
	// Complete is the snapshot's: false when the tickets read may not be the
	// tickets there are, and with it every epic's derived status is degraded.
	Complete bool `json:"complete"`
	// Namespaces is every ticket namespace the snapshot read in full, Root
	// included when it has a directory.
	Namespaces  []string `json:"namespaces"`
	Diagnostics []string `json:"diagnostics"`
	Limitations string   `json:"limitations"`
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
	// ChangedBy is, for an epic, the qualified IDs of its children — in any
	// namespace — that changed in the window, which is how an epic whose
	// derived status moved without its file being written is found.
	ChangedBy []string `json:"changed_by,omitempty"`
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

	// Snapshot takes only the store's shared lock, starts no goroutines and
	// runs no git, so a running `tk serve` is unaffected. A file or namespace
	// the store could not read is reported in the document, not swallowed.
	snap, err := ticket.NewMultiStore(cfg.TicketsRoot()).Snapshot()
	if err != nil {
		return nil, err
	}
	skips := snap.Skips
	if skips == nil {
		skips = []ticket.FileSkip{}
	}
	namespaces := snap.Namespaces()
	if namespaces == nil {
		namespaces = []string{}
	}
	diagnostics := snap.Diagnostics()
	if diagnostics == nil {
		diagnostics = []string{}
	}

	doc := document{
		Tickets: []entry{}, StatusChanges: []statusChange{}, Skips: skips,
		Complete: snap.Complete, Namespaces: namespaces, Diagnostics: diagnostics, Limitations: limitations,
	}
	status := make(map[string]string, len(snap.Tickets))
	for _, t := range snap.Tickets {
		status[t.ID] = string(t.Status)
	}

	// Every ticket namespace's log is read once: the ones the snapshot read or
	// could not, the registered ones, and Root, which config never registers
	// and which may have a log on this machine before it has a directory.
	logged := map[string]bool{tkconfig.RootNamespace: true}
	for _, ns := range namespaces {
		logged[ns] = true
	}
	for _, skip := range snap.Skips {
		if skip.Project != "" {
			logged[skip.Project] = true
		}
	}
	for _, name := range cfg.Names() {
		logged[name] = true
	}
	statusChanged := map[string]bool{}
	for _, ns := range slices.Sorted(maps.Keys(logged)) {
		changes, skipped, err := readStatusChanges(filepath.Join(tkconfig.StateDir(env.TKConfig, ns), mutationsFile),
			ns, w, status, env)
		if err != nil {
			return nil, err
		}
		for _, c := range changes {
			statusChanged[c.Ticket] = true
		}
		doc.StatusChanges = append(doc.StatusChanges, changes...)
		doc.SkippedLines += skipped
	}

	for _, t := range snap.Tickets {
		var changedBy []string
		if t.Type == ticket.TypeEpic {
			for _, c := range snap.Children(t.ID) {
				if w.Contains(c.Updated) || w.Contains(c.Completed) || statusChanged[c.ID] {
					changedBy = append(changedBy, c.ID)
				}
			}
			slices.Sort(changedBy)
		}
		if !w.Contains(t.Updated) && len(changedBy) == 0 {
			continue
		}
		project, _ := ticket.ParseNamespacedID(t.ID)
		e := entry{
			ID:        t.ID,
			Project:   project,
			Title:     t.Title,
			Status:    string(t.Status),
			Type:      string(t.Type),
			Priority:  t.Priority,
			Notes:     []note{},
			ChangedBy: changedBy,
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
	return json.Marshal(doc)
}

// readStatusChanges reconstructs the day's status transitions from the
// namespace's mutation log, which is the only record tk keeps of one. It also returns the
// number of lines it could not use: a malformed line costs the day a status
// change, so the document says how many rather than only the log.
func readStatusChanges(path, namespace string, w provider.Window, status map[string]string, env provider.Env) ([]statusChange, int, error) {
	f, err := os.Open(path)
	if err != nil {
		// A namespace nobody has written to on this machine has no log.
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
		id := ticket.FormatNamespacedID(namespace, m.TicketID)
		changes = append(changes, statusChange{
			Ticket: id,
			At:     m.Timestamp.UTC().Format(time.RFC3339),
			Source: m.Source,
			Status: status[id],
		})
	}
	return changes, skipped, sc.Err()
}
