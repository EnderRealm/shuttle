// Package tkconfig reads ~/.ticket/config.yaml directly for the three
// providers that need tk's layout. tk's internal/project is unimportable from
// another module, and TK_STORE_ROOT is deliberately not used: it relocates
// config and state paths and disables the watch loops in any `tk serve` that
// inherits it (§8.4).
package tkconfig

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"gopkg.in/yaml.v3"
)

// RootNamespace is tk's built-in Root namespace. It mirrors
// internal/project.RootNamespace in github.com/EnderRealm/ticket, which
// pkg/ticket does not export. Root holds tickets and a mutation log but is
// bound to no repository, whatever config.yaml says about it.
const RootNamespace = "_root"

// Config is the part of config.yaml the providers read. Every other key —
// verify_allow, the per-project auto_link flags — is ignored rather than
// refused, because this is tk's file and tk owns its schema.
type Config struct {
	CentralRoot string             `yaml:"central_root"`
	Projects    map[string]Project `yaml:"projects"`
}

type Project struct {
	Path string `yaml:"path"`
}

// Load reads and validates the config at path.
func Load(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, err
	}
	var c Config
	if err := yaml.Unmarshal(data, &c); err != nil {
		return Config{}, fmt.Errorf("%s: %w", path, err)
	}
	if strings.TrimSpace(c.CentralRoot) == "" {
		return Config{}, fmt.Errorf("%s: central_root: required", path)
	}
	return c, nil
}

// Names returns the registered project names, sorted, so every provider walks
// the estate in the same order and two documents diff cleanly.
func (c Config) Names() []string {
	names := make([]string, 0, len(c.Projects))
	for name := range c.Projects {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

// Unbound is a registered project that names no repository on this machine,
// with the reason, so a provider that walks repositories reports it rather
// than scanning nothing.
type Unbound struct {
	Name   string
	Reason string
}

// Repositories splits the registered projects into those bound to a
// repository — a non-empty path, never Root — and those that are not, both
// sorted. Ticket namespaces are a separate set: a namespace may hold tickets
// with no repository behind it, and only a bound one is ever handed to git,
// the commit journal or an execution path.
func (c Config) Repositories() (bound []string, unbound []Unbound) {
	for _, name := range c.Names() {
		switch {
		case name == RootNamespace:
			unbound = append(unbound, Unbound{Name: name, Reason: "Root is a ticket namespace bound to no repository"})
		case strings.TrimSpace(c.Projects[name].Path) == "":
			unbound = append(unbound, Unbound{Name: name, Reason: "registered with no repository path"})
		default:
			bound = append(bound, name)
		}
	}
	return bound, unbound
}

// TicketsRoot is the multistore root: <central_root>/tickets/<project>/*.md.
func (c Config) TicketsRoot() string {
	return filepath.Join(c.CentralRoot, "tickets")
}

// StateDir is where pkg/ticket writes a project's mutations.jsonl and tk's
// journal its commits.jsonl. Derived from the config's own directory rather
// than $HOME so the two always name the same tree.
func StateDir(configPath, project string) string {
	return filepath.Join(filepath.Dir(configPath), "state", project)
}
