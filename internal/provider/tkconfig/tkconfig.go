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
