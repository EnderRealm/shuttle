// Package priorreports carries the agent's own last reports forward, which is
// the only continuity phase 1 has (§8.6).
package priorreports

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"shuttle/internal/agentdef"
	"shuttle/internal/provider"
)

// defaultCount is what the manifest omitting count asks for: yesterday's
// report and nothing more.
const defaultCount = 1

const reportExt = ".md"

type document struct {
	Reports []report `json:"reports"`
}

type report struct {
	File    string `json:"file"`
	Content string `json:"content"`
}

// Run implements provider.Func.
func Run(_ context.Context, env provider.Env, args map[string]any, _ provider.Window) (json.RawMessage, error) {
	agent, ok := args["agent"].(string)
	if !ok || agent == "" {
		return nil, errors.New("args.agent: an agent name is required")
	}
	// The name is joined to ReportsDir, so it is held against the §4.1 name
	// rule before anything touches the filesystem: "../../private" would
	// otherwise copy a directory outside the reports repo into the bundle the
	// model reads.
	if !agentdef.NameRE.MatchString(agent) {
		return nil, fmt.Errorf("args.agent: %q must match %s", agent, agentdef.NameRE)
	}
	count := defaultCount
	if raw, ok := args["count"]; ok {
		// JSON numbers decode to float64; the manifest's count is a whole
		// number of reports.
		n, ok := raw.(float64)
		if !ok || n < 0 {
			return nil, fmt.Errorf("args.count: %v is not a count", raw)
		}
		count = int(n)
	}

	doc := document{Reports: []report{}}
	// Every path is resolved through its symlinks and required to stay under
	// the resolved reports root: a name that passes the rule can still be a
	// symlink out of the repo, and only the repo's own reports may be read.
	root, err := filepath.EvalSymlinks(env.ReportsDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return json.Marshal(doc)
		}
		return nil, err
	}
	dir, err := filepath.EvalSymlinks(filepath.Join(root, agent))
	if err != nil {
		// The reports repo has no directory for an agent that has never
		// produced one; the first run of an agent is not a failure.
		if errors.Is(err, os.ErrNotExist) {
			return json.Marshal(doc)
		}
		return nil, err
	}
	if !within(root, dir) {
		return nil, fmt.Errorf("args.agent: %q resolves to %s, outside the reports directory", agent, dir)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	// The file name is the window label (§9.2), so name order is date order and
	// descending takes the most recent.
	var names []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), reportExt) {
			names = append(names, e.Name())
		}
	}
	sort.Sort(sort.Reverse(sort.StringSlice(names)))
	if len(names) > count {
		names = names[:count]
	}
	for _, name := range names {
		path, err := filepath.EvalSymlinks(filepath.Join(dir, name))
		if err != nil {
			return nil, err
		}
		if !within(root, path) {
			return nil, fmt.Errorf("%s: resolves to %s, outside the reports directory", name, path)
		}
		content, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		doc.Reports = append(doc.Reports, report{File: name, Content: string(content)})
	}
	return json.Marshal(doc)
}

// within reports whether path is root or inside it. Both are already resolved
// through their symlinks, so the comparison is of real paths.
func within(root, path string) bool {
	rel, err := filepath.Rel(root, path)
	if err != nil {
		return false
	}
	return rel == "." || (rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)))
}
