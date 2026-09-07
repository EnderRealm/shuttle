// Package tkjournal reports the commits tk's journal linked to tickets in the
// window, read from every registered project's commits.jsonl (§8.4).
package tkjournal

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/EnderRealm/ticket/v8/pkg/journal"

	"shuttle/internal/provider"
	"shuttle/internal/provider/tkconfig"
)

const journalFile = "commits.jsonl"

type document struct {
	Entries      []entry `json:"entries"`
	SkippedLines int     `json:"skipped_lines"`
}

type entry struct {
	Project         string   `json:"project"`
	SHA             string   `json:"sha"`
	Ticket          string   `json:"ticket"`
	Repo            string   `json:"repo"`
	TS              string   `json:"ts"`
	Message         string   `json:"message"`
	Action          string   `json:"action"`
	FilesChanged    []string `json:"files_changed,omitempty"`
	LinesAdded      int      `json:"lines_added"`
	LinesRemoved    int      `json:"lines_removed"`
	Branch          string   `json:"branch,omitempty"`
	DurationSeconds int      `json:"duration_seconds,omitempty"`
}

// Run implements provider.Func.
func Run(_ context.Context, env provider.Env, _ map[string]any, w provider.Window) (json.RawMessage, error) {
	cfg, err := tkconfig.Load(env.TKConfig)
	if err != nil {
		return nil, err
	}
	doc := document{Entries: []entry{}}
	for _, project := range cfg.Names() {
		path := filepath.Join(tkconfig.StateDir(env.TKConfig, project), journalFile)
		f, err := os.Open(path)
		if err != nil {
			// A project whose journal has never been written is an empty
			// project, not a failure: tk writes the file on the first linked
			// commit.
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return nil, err
		}
		skipped, err := read(f, project, w, &doc.Entries, env.Logger, path)
		f.Close()
		if err != nil {
			return nil, err
		}
		doc.SkippedLines += skipped
	}
	return json.Marshal(doc)
}

// read appends project's in-window entries to out and returns the number of
// lines it could not use. A malformed line is reported and skipped: one bad
// append must not cost the day its other commits.
func read(f *os.File, project string, w provider.Window, out *[]entry, logger *log.Logger, path string) (int, error) {
	var skipped int
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64*1024), 1024*1024)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var e journal.Entry
		if err := json.Unmarshal(line, &e); err != nil {
			logger.Printf("tk-journal: %s: %v", path, err)
			skipped++
			continue
		}
		ts, err := time.Parse(time.RFC3339, e.TS)
		if err != nil {
			logger.Printf("tk-journal: %s: ts %q: %v", path, e.TS, err)
			skipped++
			continue
		}
		if !w.Contains(ts) {
			continue
		}
		*out = append(*out, entry{
			Project:         project,
			SHA:             e.SHA,
			Ticket:          e.Ticket,
			Repo:            e.Repo,
			TS:              e.TS,
			Message:         e.Msg,
			Action:          e.Action,
			FilesChanged:    e.FilesChanged,
			LinesAdded:      e.LinesAdded,
			LinesRemoved:    e.LinesRemoved,
			Branch:          e.Branch,
			DurationSeconds: e.DurationSecs,
		})
	}
	return skipped, sc.Err()
}
