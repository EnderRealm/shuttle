package cmd

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"shuttle/internal/config"
	"shuttle/internal/ledger"
	"shuttle/internal/updater"
)

// hostedConfig is a config.json the daemon's loader passes, written the way the
// operator's is: two-space indent, keys in the operator's order rather than
// the struct's.
const hostedConfig = `{
  "agents_dir": "/opt/shuttle/agents",
  "timezone": "America/Los_Angeles",
  "email": {
    "provider": "cloudflare",
    "to": [
      "operator@example.com"
    ],
    "keychain_item": "shuttle-cloudflare-token"
  },
  "weft": {
    "listen": "127.0.0.1:7431",
    "keychain_item": "weft-remote-token",
    "segment_timeout_seconds": 21600,
    "repositories": {
      "loom": "/Users/Shared/weft/repos/loom"
    }
  },
  "observers": {}
}
`

// weftHost stands in for the clone, launchd and the service. The service it
// serves reads config.json from disk, as the restarted daemon does, so what a
// command reports is what it wrote.
type weftHost struct {
	t           *testing.T
	provisioned []string
	restarts    int
	// duringProvision runs inside the stand-in clone.
	duringProvision func()
	// duringRestart runs inside the stand-in restart, once config.json is
	// written and before the daemon would go down.
	duringRestart func()
}

func newWeftHost(t *testing.T, body string) *weftHost {
	t.Helper()
	shuttleEnv(t)
	writeConfig(t, body)
	h := &weftHost{t: t}
	provision, restart, token, read := runProvision, restartDaemon, weftToken, readProjects
	t.Cleanup(func() { runProvision, restartDaemon, weftToken, readProjects = provision, restart, token, read })
	runProvision = func(_, origin string, stdout, _ io.Writer) error {
		h.provisioned = append(h.provisioned, origin)
		if h.duringProvision != nil {
			h.duringProvision()
		}
		name, _ := tkProjectFromRemote(origin)
		_, err := io.WriteString(stdout, "published: "+filepath.Join(config.HostedRepositoriesRoot, name)+"\n")
		return err
	}
	restartDaemon = func() error {
		h.restarts++
		if h.duringRestart != nil {
			h.duringRestart()
		}
		return nil
	}
	weftToken = func(*config.Config) (string, error) { return "token", nil }
	readProjects = func(context.Context, string, string) ([]v2Project, error) {
		cfg, err := config.Load()
		if err != nil {
			return nil, err
		}
		var projects []v2Project
		for _, name := range []string{"loom", "ticket", "warp"} {
			p := v2Project{Project: name, State: "available"}
			if cfg.Weft.Repositories[name] == "" {
				p.State = "unavailable"
				p.Reason = &struct {
					Code   string `json:"code"`
					Detail string `json:"detail"`
				}{"no_binding", "weft.repositories in shuttle's config.json binds no hosted checkout for " + name}
			}
			projects = append(projects, p)
		}
		return projects, nil
	}
	return h
}

func (h *weftHost) run(args ...string) (string, error) {
	h.t.Helper()
	var out bytes.Buffer
	rootCmd.SetOut(&out)
	rootCmd.SetErr(&out)
	rootCmd.SetArgs(append([]string{"weft"}, args...))
	defer func() {
		rootCmd.SetOut(nil)
		rootCmd.SetErr(nil)
		rootCmd.SetArgs(nil)
	}()
	err := rootCmd.Execute()
	h.t.Logf("shuttle weft %s:\n%s(err: %v)", strings.Join(args, " "), out.String(), err)
	return out.String(), err
}

func configPath(t *testing.T) string {
	t.Helper()
	path, err := config.Path()
	if err != nil {
		t.Fatal(err)
	}
	return path
}

func readConfigFile(t *testing.T) string {
	t.Helper()
	data, err := os.ReadFile(configPath(t))
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

// requireUntouched fails unless config.json is still body and nothing beside
// it was written: no backup, no temp file.
func (h *weftHost) requireUntouched(body string) {
	h.t.Helper()
	if got := readConfigFile(h.t); got != body {
		h.t.Errorf("config.json changed:\n%s", got)
	}
	extra, err := filepath.Glob(configPath(h.t) + ".bak-*")
	if err != nil {
		h.t.Fatal(err)
	}
	temps, err := filepath.Glob(filepath.Join(filepath.Dir(configPath(h.t)), ".config.json.*"))
	if err != nil {
		h.t.Fatal(err)
	}
	if left := append(extra, temps...); len(left) > 0 {
		h.t.Errorf("a refused edit left %v beside config.json", left)
	}
	if h.restarts != 0 {
		h.t.Errorf("the daemon was restarted %d time(s)", h.restarts)
	}
}

// requireBackup fails unless exactly one backup beside config.json holds body.
func requireBackup(t *testing.T, body string) {
	t.Helper()
	backups, err := filepath.Glob(configPath(t) + ".bak-*")
	if err != nil {
		t.Fatal(err)
	}
	if len(backups) != 1 {
		t.Fatalf("backups = %v, want one", backups)
	}
	data, err := os.ReadFile(backups[0])
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != body {
		t.Errorf("backup %s holds:\n%s\nwant the file it replaced:\n%s", backups[0], data, body)
	}
}

func claimSegment(t *testing.T, ticket string) {
	t.Helper()
	if err := tryClaimSegment(t, ticket); err != nil {
		t.Fatal(err)
	}
}

// tryClaimSegment is a hosted segment's admission, as a client's launch makes it.
func tryClaimSegment(t *testing.T, ticket string) error {
	t.Helper()
	store, err := openLedger()
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	home, err := config.Home()
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.ClaimWeft(ledger.WeftLaunch{
		Ticket: ticket, Project: "loom", Repo: "/Users/Shared/weft/repos/loom", Branch: ticket,
		Worktree: filepath.Join(home, "worktrees", ticket), RunDirRoot: filepath.Join(home, "runs", ledger.WeftAgent),
	})
	return err
}

// requireNoHold fails unless weft-worker's pause is wantReason ("" for none)
// and the restart barrier is down: whatever a command did, it left no hold of
// its own and lifted no other.
func requireNoHold(t *testing.T, wantReason string) {
	t.Helper()
	store, err := openLedger()
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	agent, err := store.Agent(ledger.WeftAgent)
	if err != nil {
		t.Fatal(err)
	}
	if agent.Paused != (wantReason != "") || agent.PausedReason.String != wantReason {
		t.Errorf("%s paused=%t reason=%q, want reason %q", ledger.WeftAgent, agent.Paused, agent.PausedReason.String, wantReason)
	}
	if held, err := store.RestartBarrier(); err != nil || held {
		t.Errorf("restart barrier held=%t (err %v), want it down", held, err)
	}
}

func pauseWeftWorker(t *testing.T) {
	t.Helper()
	store, err := openLedger()
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.Pause(ledger.WeftAgent); err != nil {
		t.Fatal(err)
	}
}

func TestWeftBindValidatesConfigBeforeWriting(t *testing.T) {
	t.Run("a binding the loader passes is written, backed up and reported", func(t *testing.T) {
		h := newWeftHost(t, hostedConfig)
		out, err := h.run("bind", "git@github.com:EnderRealm/ticket.git")
		if err != nil {
			t.Fatalf("bind: %v", err)
		}
		want := strings.Replace(hostedConfig, `"loom": "/Users/Shared/weft/repos/loom"`,
			`"loom": "/Users/Shared/weft/repos/loom",
      "ticket": "/Users/Shared/weft/repos/ticket"`, 1)
		if got := readConfigFile(t); got != want {
			t.Errorf("config.json is:\n%s\nwant:\n%s", got, want)
		}
		if _, err := config.Load(); err != nil {
			t.Errorf("the daemon's loader refuses the written config: %v", err)
		}
		requireBackup(t, hostedConfig)
		if len(h.provisioned) != 1 || h.provisioned[0] != "git@github.com:EnderRealm/ticket.git" {
			t.Errorf("provisioned %v, want the origin once", h.provisioned)
		}
		if h.restarts != 1 {
			t.Errorf("restarts = %d, want 1", h.restarts)
		}
		if !strings.Contains(out, "/v2/projects reports ticket available") {
			t.Errorf("bind did not report ticket available:\n%s", out)
		}
	})

	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatal(err)
	}
	refusals := []struct {
		name, body, origin, want string
	}{
		{"a name tk would not derive from the origin", hostedConfig, "https://github.com/EnderRealm/..",
			"names no project tk can resolve"},
		{"a checkout agents cannot reach",
			strings.Replace(hostedConfig, `"loom": "/Users/Shared/weft/repos/loom"`, `"loom": "`+filepath.Join(home, "code", "loom")+`"`, 1),
			"git@github.com:EnderRealm/ticket.git", "agent users cannot traverse"},
		{"a project already bound",
			strings.Replace(hostedConfig, `"loom": "/Users/Shared/weft/repos/loom"`,
				`"loom": "/Users/Shared/weft/repos/loom", "ticket": "/Users/Shared/weft/repos/ticket"`, 1),
			"git@github.com:EnderRealm/ticket.git", "ticket is already bound"},
		{"a key the daemon no longer reads",
			strings.Replace(hostedConfig, `"listen": "127.0.0.1:7431",`, `"listen": "127.0.0.1:7431", "allow_write": true,`, 1),
			"git@github.com:EnderRealm/ticket.git", "weft.allow_write"},
	}
	for _, r := range refusals {
		t.Run(r.name, func(t *testing.T) {
			h := newWeftHost(t, r.body)
			_, err := h.run("bind", r.origin)
			if err == nil || !strings.Contains(err.Error(), r.want) {
				t.Fatalf("bind: %v, want a refusal naming %q", err, r.want)
			}
			if !strings.Contains(err.Error(), "config.json left as it was") {
				t.Errorf("the refusal does not say config.json was left as it was: %v", err)
			}
			h.requireUntouched(r.body)
			if len(h.provisioned) != 0 {
				t.Errorf("provisioned %v before the edit was validated", h.provisioned)
			}
		})
	}
}

// A weft.listen ValidateListen passes can still name another host once pasted
// into a URL; bind and unbind refuse it before the write, so the Keychain token
// is never read and no request is made.
func TestWeftRefusesAListenNamingAnotherHost(t *testing.T) {
	body := strings.Replace(hostedConfig, `"listen": "127.0.0.1:7431",`, `"listen": "127.0.0.1:7431@attacker.example",`, 1)
	for _, args := range [][]string{
		{"bind", "git@github.com:EnderRealm/ticket.git"},
		{"unbind", "loom"},
	} {
		t.Run(args[0], func(t *testing.T) {
			h := newWeftHost(t, body)
			weftToken = func(*config.Config) (string, error) {
				t.Error("the token was read")
				return "token", nil
			}
			readProjects = func(_ context.Context, endpoint, _ string) ([]v2Project, error) {
				t.Errorf("a request was made to %s", endpoint)
				return nil, nil
			}
			_, err := h.run(args...)
			if err == nil || !strings.Contains(err.Error(), "weft.listen") {
				t.Fatalf("%s: %v, want a refusal naming weft.listen", args[0], err)
			}
			h.requireUntouched(body)
			if len(h.provisioned) != 0 {
				t.Errorf("provisioned %v", h.provisioned)
			}
		})
	}

	for listen, want := range map[string]string{
		"127.0.0.1:7431":                  "http://127.0.0.1:7431/v2/projects",
		"100.64.0.1:7431":                 "http://100.64.0.1:7431/v2/projects",
		"127.0.0.1:7431@attacker.example": "",
		"127.0.0.1:0":                     "",
		"127.0.0.1:65536":                 "",
		"127.0.0.1:http":                  "",
		"127.0.0.1:+7431":                 "",
	} {
		got, err := projectsURL(listen)
		if got != want || (err == nil) != (want != "") {
			t.Errorf("projectsURL(%q) = %q, %v; want %q", listen, got, err, want)
		}
	}
}

func TestWeftBindRefusesWhileASegmentRuns(t *testing.T) {
	t.Run("bind", func(t *testing.T) {
		h := newWeftHost(t, hostedConfig)
		claimSegment(t, "loom-a1b2")
		_, err := h.run("bind", "git@github.com:EnderRealm/ticket.git")
		if err == nil || !strings.Contains(err.Error(), "a daemon restart would kill") {
			t.Fatalf("bind: %v, want a refusal naming the running segment", err)
		}
		h.requireUntouched(hostedConfig)
		if len(h.provisioned) != 0 {
			t.Errorf("provisioned %v while a segment ran", h.provisioned)
		}
		requireNoHold(t, "")
	})

	t.Run("bind, once the clone is done", func(t *testing.T) {
		h := newWeftHost(t, hostedConfig)
		h.duringProvision = func() { claimSegment(t, "loom-c3d4") }
		_, err := h.run("bind", "git@github.com:EnderRealm/ticket.git")
		if err == nil || !strings.Contains(err.Error(), "a daemon restart would kill") {
			t.Fatalf("bind: %v, want a refusal naming the segment that started during the clone", err)
		}
		if !strings.Contains(err.Error(), "/Users/Shared/weft/repos/ticket is published and unbound") {
			t.Errorf("the refusal does not name the checkout it left unbound: %v", err)
		}
		h.requireUntouched(hostedConfig)
		requireNoHold(t, "")
	})

	t.Run("unbind", func(t *testing.T) {
		h := newWeftHost(t, hostedConfig)
		claimSegment(t, "loom-e5f6")
		_, err := h.run("unbind", "loom")
		if err == nil || !strings.Contains(err.Error(), "a daemon restart would kill") {
			t.Fatalf("unbind: %v, want a refusal naming the running segment", err)
		}
		h.requireUntouched(hostedConfig)
		requireNoHold(t, "")
	})

	// The race the hold closes: a claim made after the final check, while the
	// daemon is going down, would start only for the restart to kill it.
	t.Run("a claim between the final check and the restart cannot start", func(t *testing.T) {
		h := newWeftHost(t, hostedConfig)
		h.duringRestart = func() {
			if err := tryClaimSegment(t, "loom-g7h8"); !errors.Is(err, ledger.ErrWeftHostPaused) {
				t.Errorf("a hosted segment claimed during the restart: %v, want %v", err, ledger.ErrWeftHostPaused)
			}
			store, err := openLedger()
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			if _, _, err := store.Claim("daily", time.Now(), ledger.TriggerCron); !errors.Is(err, ledger.ErrRestarting) {
				t.Errorf("a scheduled run claimed during the restart: %v, want %v", err, ledger.ErrRestarting)
			}
		}
		if _, err := h.run("bind", "git@github.com:EnderRealm/ticket.git"); err != nil {
			t.Fatalf("bind: %v", err)
		}
		if h.restarts != 1 {
			t.Fatalf("restarts = %d, want 1", h.restarts)
		}
		requireNoHold(t, "")
		if err := tryClaimSegment(t, "loom-i9j0"); err != nil {
			t.Errorf("a hosted segment cannot start once bind ends: %v", err)
		}
	})

	pauses := []struct {
		name  string
		pause func(t *testing.T, h *weftHost)
		want  string
	}{
		{"bind, under an operator's pause", func(t *testing.T, _ *weftHost) { pauseWeftWorker(t) }, "nothing provisioned"},
		{"bind, under an operator's pause taken during the clone",
			func(t *testing.T, h *weftHost) { h.duringProvision = func() { pauseWeftWorker(t) } }, "is published and unbound"},
	}
	for _, p := range pauses {
		t.Run(p.name, func(t *testing.T) {
			h := newWeftHost(t, hostedConfig)
			p.pause(t, h)
			_, err := h.run("bind", "git@github.com:EnderRealm/ticket.git")
			if err == nil || !strings.Contains(err.Error(), ledger.WeftAgent+" is already paused ("+ledger.PauseManual+")") {
				t.Fatalf("bind: %v, want a refusal naming the operator's pause", err)
			}
			if !strings.Contains(err.Error(), p.want) {
				t.Errorf("the refusal does not say %q: %v", p.want, err)
			}
			h.requireUntouched(hostedConfig)
			requireNoHold(t, ledger.PauseManual)
		})
	}

	t.Run("bind, while the updater holds its lock", func(t *testing.T) {
		h := newWeftHost(t, hostedConfig)
		home, err := config.Home()
		if err != nil {
			t.Fatal(err)
		}
		unlock, held, err := updater.Lock(home)
		if err != nil || !held {
			t.Fatalf("take the updater lock: held=%t err=%v", held, err)
		}
		defer unlock()
		_, err = h.run("bind", "git@github.com:EnderRealm/ticket.git")
		if err == nil || !strings.Contains(err.Error(), "the updater's lock is held") {
			t.Fatalf("bind: %v, want a refusal naming the updater", err)
		}
		h.requireUntouched(hostedConfig)
		if len(h.provisioned) != 0 {
			t.Errorf("provisioned %v while the updater held its lock", h.provisioned)
		}
	})
}

func TestWeftUnbindRemovesOnlyTheBinding(t *testing.T) {
	// unbind reads only config.json, so a checkout outside the shared root,
	// which a test can make, stands for one under it; the edited config no
	// longer names it, so the loader passes.
	checkout := filepath.Join(t.TempDir(), "ticket")
	if err := os.MkdirAll(filepath.Join(checkout, ".git"), 0o755); err != nil {
		t.Fatal(err)
	}
	ticketLine := `
      "ticket": "` + checkout + `",`
	body := strings.Replace(hostedConfig, `
      "loom": "/Users/Shared/weft/repos/loom"`, `
      "loom": "/Users/Shared/weft/repos/loom",`+ticketLine+`
      "warp": "/Users/Shared/weft/repos/warp"`, 1)

	t.Run("removes the binding", func(t *testing.T) {
		h := newWeftHost(t, body)
		out, err := h.run("unbind", "ticket")
		if err != nil {
			t.Fatalf("unbind: %v", err)
		}
		if want := strings.Replace(body, ticketLine, "", 1); readConfigFile(t) != want {
			t.Errorf("config.json is:\n%s\nwant every other key as it was:\n%s", readConfigFile(t), want)
		}
		requireBackup(t, body)
		if _, err := os.Stat(filepath.Join(checkout, ".git")); err != nil {
			t.Errorf("the checkout is gone: %v", err)
		}
		if h.restarts != 1 {
			t.Errorf("restarts = %d, want 1", h.restarts)
		}
		if len(h.provisioned) != 0 {
			t.Errorf("unbind provisioned %v", h.provisioned)
		}
		if !strings.Contains(out, "/v2/projects reports ticket unavailable (no_binding)") {
			t.Errorf("unbind did not report ticket unbound:\n%s", out)
		}
	})

	t.Run("refuses a project that is not bound", func(t *testing.T) {
		h := newWeftHost(t, body)
		_, err := h.run("unbind", "shuttle")
		if err == nil || !strings.Contains(err.Error(), "shuttle is not bound") {
			t.Fatalf("unbind: %v, want a refusal", err)
		}
		h.requireUntouched(body)
	})
}
