package runner

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"shuttle/internal/agentdef"
	"shuttle/internal/config"
	"shuttle/internal/keychain"
	"shuttle/internal/ledger"
	"shuttle/internal/provider"
	"shuttle/internal/provider/loomsessions"
	"shuttle/internal/provider/priorreports"
	"shuttle/internal/provider/tkconfig"
	"shuttle/internal/runtime/claude"
	"shuttle/internal/sandbox"
)

// CheckError identifies the requirement that refused a run before model spend.
type CheckError struct {
	Name  string
	Class string
	Err   error
}

func (e *CheckError) Error() string { return fmt.Sprintf("preflight %s: %v", e.Name, e.Err) }
func (e *CheckError) Unwrap() error { return e.Err }

// Preflight owns the local checks for one configured agent. RunDir is retained
// by a live run; the standalone command supplies a temporary directory.
type Preflight struct {
	Config          *config.Config
	Home            string
	HomeDir         string
	PATH            string
	UnsafeNoSandbox bool
	findKey         func(string) (string, error)
}

// Check performs no model call. The returned names include every check reached;
// the first failed requirement stops the run before providers or authentication.
func (p Preflight) Check(ctx context.Context, def *agentdef.Definition, runDir string) ([]string, error) {
	var names []string
	check := func(name string, f func() error) error {
		names = append(names, name)
		ctxErr := ctx.Err()
		if ctxErr == nil {
			ctxErr = f()
		}
		if ctxErr != nil {
			return &CheckError{Name: name, Class: ledger.ClassPreflight, Err: ctxErr}
		}
		return nil
	}
	if err := check("runtime", func() error {
		if def.Manifest.Runtime.Kind != "claude" {
			return fmt.Errorf("unsupported kind %q", def.Manifest.Runtime.Kind)
		}
		if def.Manifest.Runtime.Auth != claude.AuthAPIKey {
			return fmt.Errorf("unsupported auth %q", def.Manifest.Runtime.Auth)
		}
		r := def.Manifest.Runtime
		if r.TimeoutSeconds <= 0 {
			return fmt.Errorf("timeout_seconds must be positive")
		}
		_, err := claude.Argv(claude.Options{Bin: p.Config.Paths.ClaudeBin, RunDir: runDir,
			Auth: r.Auth, Model: r.Model, MaxTurns: r.MaxTurns, MaxBudgetUSD: r.MaxBudgetUSD,
			Tools: r.Tools, APIKey: "preflight", PATH: p.PATH, HomeDir: p.HomeDir, Timezone: "UTC"})
		return err
	}); err != nil {
		return names, err
	}
	if err := check("claude", func() error {
		if err := executable(p.Config.Paths.ClaudeBin); err != nil {
			return err
		}
		bound, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		cmd := exec.CommandContext(bound, p.Config.Paths.ClaudeBin, "--version")
		cmd.Dir = runDir
		cmd.Env = []string{"PATH=" + p.PATH, "HOME=" + p.HomeDir}
		cmd.WaitDelay = time.Second
		return cmd.Run()
	}); err != nil {
		return names, err
	}
	// Ahead of srt: the sandbox profile folds every registration's deny_read
	// in, so a bad path must fail as observer configuration, not as srt.
	if def.Manifest.Observer != nil {
		if err := check("observer", func() error {
			registration, err := observerRegistration(p.Config, def)
			if err != nil {
				return err
			}
			if err := validateObserverRegistration(registration, def.Manifest.Runtime.Kind, p.Config.Paths.TKConfig); err != nil {
				return err
			}
			return validateObserverDeniedReads(p.Config, runDir)
		}); err != nil {
			return names, err
		}
	}
	if !p.UnsafeNoSandbox {
		if err := check("srt", func() error {
			if def.Manifest.Sandbox.Kind != "srt" {
				return fmt.Errorf("unsupported sandbox %q", def.Manifest.Sandbox.Kind)
			}
			if err := executable(p.Config.Paths.SRTBin); err != nil {
				return err
			}
			o := p.sandbox(def, runDir)
			if _, err := sandbox.WriteProfile(o); err != nil {
				return err
			}
			bound, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()
			return sandbox.Check(bound, o)
		}); err != nil {
			return names, err
		}
	}
	if err := check("anthropic.keychain_item", func() error {
		_, err := p.key(p.Config.Anthropic.KeychainItem)
		return err
	}); err != nil {
		return names, err
	}
	for _, in := range def.Manifest.Inputs {
		if err := check("input."+in.Provider, func() error {
			switch in.Provider {
			case "loom-sessions":
				if err := directory(p.Config.Paths.LoomHome); err != nil {
					return err
				}
				return loomsessions.Check(ctx, p.Config.Paths.LoomHome)
			case "loom-workreport":
				if err := executable(p.Config.Paths.LoomBin); err != nil {
					return err
				}
				if err := directory(p.Config.Paths.LoomHome); err != nil {
					return err
				}
				return loomsessions.Check(ctx, p.Config.Paths.LoomHome)
			case "tk-journal", "tk-tickets", "git-log":
				if !filepath.IsAbs(p.Config.Paths.TKConfig) {
					return fmt.Errorf("tk config must be absolute")
				}
				cfg, err := tkconfig.Load(p.Config.Paths.TKConfig)
				if err != nil {
					return err
				}
				if err := directory(cfg.CentralRoot); err != nil {
					return err
				}
				if in.Provider == "git-log" {
					_, err = exec.LookPath("git")
				}
				return err
			case "prior-reports":
				_, err := priorreports.Run(ctx, provider.Env{ReportsDir: filepath.Join(p.Home, reportsDir)}, in.Args, provider.Window{})
				return err
			default:
				return fmt.Errorf("provider is not registered")
			}
		}); err != nil {
			return names, err
		}
	}
	if out := def.Manifest.Outputs.Email; out != nil && out.Max > 0 {
		if err := check("output.email", func() error {
			e := p.Config.Email
			if e.Provider != "cloudflare" || e.AccountID == "" || e.From == "" || len(e.To) == 0 {
				return fmt.Errorf("cloudflare provider, account_id, from and to are required")
			}
			for _, to := range e.To {
				if to == "" {
					return fmt.Errorf("to contains an empty recipient")
				}
			}
			_, err := p.key(e.KeychainItem)
			return err
		}); err != nil {
			return names, err
		}
	}
	if out := def.Manifest.Outputs.File; out != nil && out.Max > 0 {
		if err := check("output.file", func() error { return directory(filepath.Join(p.Home, reportsDir)) }); err != nil {
			return names, err
		}
	}
	return names, nil
}

func (p Preflight) sandbox(def *agentdef.Definition, dir string) sandbox.Options {
	options := sandbox.Options{
		SRTBin: p.Config.Paths.SRTBin, RunDir: dir, HomeDir: p.HomeDir,
		PATH: p.PATH, AllowedDomains: def.Manifest.Sandbox.AllowedDomains,
	}
	if def.Manifest.Observer != nil {
		seen := map[string]bool{}
		for _, path := range append([]string{observerStagingDir(dir)}, observerDeniedReads(p.Config)...) {
			if !seen[path] {
				options.DeniedRead = append(options.DeniedRead, path)
				seen[path] = true
			}
		}
	}
	return options
}

func observerDeniedReads(cfg *config.Config) []string {
	var paths []string
	for _, registration := range cfg.Observers {
		paths = append(paths, registration.DenyRead...)
	}
	sort.Strings(paths)
	return paths
}

func validateObserverDeniedReads(cfg *config.Config, runDir string) error {
	names := make([]string, 0, len(cfg.Observers))
	for name := range cfg.Observers {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if len(cfg.Observers[name].DenyRead) == 0 {
			return fmt.Errorf("observers.%s.deny_read: at least one domain store root is required", name)
		}
		for i, path := range cfg.Observers[name].DenyRead {
			if !filepath.IsAbs(path) || filepath.Clean(path) != path {
				return fmt.Errorf("observers.%s.deny_read[%d]: %q must be an absolute clean path", name, i, path)
			}
			rel, err := filepath.Rel(runDir, path)
			if err != nil {
				return fmt.Errorf("observers.%s.deny_read[%d]: %q relative to run dir: %w", name, i, path, err)
			}
			if rel == "." || rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
				return fmt.Errorf("observers.%s.deny_read[%d]: %q is inside run dir %s", name, i, path, runDir)
			}
		}
	}
	return nil
}

func (p Preflight) key(item string) (string, error) {
	if p.findKey != nil {
		return p.findKey(item)
	}
	return keychain.Find(item)
}

func executable(path string) error {
	if !filepath.IsAbs(path) {
		return fmt.Errorf("%q must be an absolute binary path", path)
	}
	fi, err := os.Stat(path)
	if err != nil {
		return err
	}
	if !fi.Mode().IsRegular() || fi.Mode().Perm()&0o111 == 0 {
		return fmt.Errorf("%s is not executable", path)
	}
	return nil
}

func directory(path string) error {
	if !filepath.IsAbs(path) {
		return fmt.Errorf("%q must be an absolute directory", path)
	}
	fi, err := os.Stat(path)
	if err != nil {
		return err
	}
	if !fi.IsDir() {
		return fmt.Errorf("%s is not a directory", path)
	}
	return nil
}

func regularAbsoluteFile(path string) error {
	if !filepath.IsAbs(path) {
		return fmt.Errorf("%q must be an absolute file path", path)
	}
	fi, err := os.Stat(path)
	if err != nil {
		return err
	}
	if !fi.Mode().IsRegular() {
		return fmt.Errorf("%s is not a regular file", path)
	}
	return nil
}
