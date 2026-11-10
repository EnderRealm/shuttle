package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"shuttle/internal/config"
	"shuttle/internal/ledger"
	"shuttle/internal/updater"
	"shuttle/internal/weftremote"
)

const (
	// projectsBound outlasts a daemon start, as the updater's confirmation
	// does: the restart pass, the worker probe and launchd's 30s throttle.
	projectsBound = 2 * time.Minute
	projectsPoll  = 2 * time.Second
	// projectsReadBound bounds one read of /v2/projects.
	projectsReadBound = 10 * time.Second
	// restartHoldLease is when the restart barrier lapses if nothing takes it
	// down, as the updater's restartLease is: it outlasts the restart and
	// projectsBound, and holds new work this long at worst after a bind that
	// died with no updater poll to recover it.
	restartHoldLease = 30 * time.Minute
)

var weftCmd = &cobra.Command{
	Use:   "weft",
	Short: "Bind hosted weft projects to their checkouts",
	Long: `Bind and unbind the hosted checkouts weft runs work in (docs/agent-users.md).

  shuttle weft bind <origin-url>   provision the checkout, bind it, restart the daemon
  shuttle weft unbind <project>    remove the binding, leaving the checkout on disk

Each edit of config.json is held to the daemon's own loader before it is
written, and neither command restarts the daemon while anything a restart
would kill is running. Both run under the updater's lock and hold new work
through the restart as an update does: weft-worker paused for the update and
the restart barrier up, so hosted clients read "shuttle is updating" until the
daemon answers. Every step is printed as it completes.`,
}

var weftBindCmd = &cobra.Command{
	Use:   "bind <origin-url>",
	Short: "Provision a hosted checkout, bind it and restart the daemon",
	Long: `Provision <origin-url> under /Users/Shared/weft/repos with
scripts/agent-users/provision-checkout.sh, add its weft.repositories binding to
config.json, restart the daemon and report the project as /v2/projects sees it.

The project is the name tk derives from the origin. Nothing is provisioned or
written when the binding exists, when the edited config.json is one the daemon
would refuse, when a restart would kill running work, when the updater is
mid-poll or another bind runs, or when weft-worker is already paused.`,
	Args:          cobra.ExactArgs(1),
	SilenceUsage:  true,
	SilenceErrors: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		return weftBind(cmd.Context(), cmd.OutOrStdout(), cmd.ErrOrStderr(), args[0])
	},
}

var weftUnbindCmd = &cobra.Command{
	Use:   "unbind <project>",
	Short: "Remove a hosted project's binding and restart the daemon",
	Long: `Remove weft.repositories.<project> from config.json, restart the daemon and
report the project as /v2/projects sees it. The checkout stays on disk.`,
	Args:          cobra.ExactArgs(1),
	SilenceUsage:  true,
	SilenceErrors: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		return weftUnbind(cmd.Context(), cmd.OutOrStdout(), args[0])
	},
}

func init() {
	weftCmd.AddCommand(weftBindCmd, weftUnbindCmd)
	rootCmd.AddCommand(weftCmd)
}

// Seams for a test to stand in for the clone, launchd and the service.
var (
	runProvision = func(script, origin string, stdout, stderr io.Writer) error {
		c := exec.Command(script, origin)
		c.Stdout, c.Stderr = stdout, stderr
		return c.Run()
	}
	restartDaemon = rebootstrapDaemon
	weftToken     = (*config.Config).WeftToken
	readProjects  = readV2Projects
)

// errTokenRefused is a /v2/projects answer no retry changes: the service is
// up and refuses the token config.json names.
var errTokenRefused = errors.New("the service refuses the token weft.keychain_item names")

func weftBind(ctx context.Context, out, errOut io.Writer, origin string) error {
	name, ok := tkProjectFromRemote(origin)
	if !ok {
		return fmt.Errorf("bind: refusing %s: it names no project tk can resolve (derived name: %q); nothing provisioned, config.json left as it was", origin, name)
	}
	unlock, err := lockUpdater()
	if err != nil {
		return fmt.Errorf("bind: %w; nothing provisioned, config.json left as it was", err)
	}
	defer unlock()
	checkout := filepath.Join(config.HostedRepositoriesRoot, name)
	add := func(repos object) (object, error) {
		if i := repos.index(name); i >= 0 {
			return nil, fmt.Errorf("%s is already bound, to %s", name, repos[i].value)
		}
		value, err := json.Marshal(checkout)
		if err != nil {
			return nil, err
		}
		return append(repos, member{key: name, value: value}), nil
	}

	// Everything that can refuse without a side effect comes first, so a
	// refused bind leaves no checkout behind.
	path, _, _, cfg, err := editConfig(add)
	if err != nil {
		return fmt.Errorf("bind: %w; nothing provisioned, config.json left as it was", err)
	}
	if err := servable(path, cfg); err != nil {
		return fmt.Errorf("bind: %w; nothing provisioned, config.json left as it was", err)
	}
	fmt.Fprintf(out, "bind: %s with weft.repositories.%s = %s passes the daemon's loader\n", path, name, checkout)
	if err := restartable(); err != nil {
		return fmt.Errorf("bind: %w; nothing provisioned, config.json left as it was", err)
	}
	fmt.Fprintf(out, "bind: nothing a daemon restart would kill is running, and %s is not paused\n", ledger.WeftAgent)

	script, err := provisionScript()
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "bind: provisioning %s: %s %s\n", checkout, script, origin)
	var stdout bytes.Buffer
	if err := runProvision(script, origin, &stdout, errOut); err != nil {
		return fmt.Errorf("bind: %s %s: %w; config.json left as it was", script, origin, err)
	}
	// The script derives the name itself; holding its output to the name
	// derived here catches the two drifting apart before the binding is
	// written under the wrong one.
	if !slices.Contains(strings.Split(stdout.String(), "\n"), "published: "+checkout) {
		return fmt.Errorf("bind: %s did not report publishing %s (it printed %q); config.json left as it was", script, checkout, stdout.String())
	}
	fmt.Fprintf(out, "bind: published %s\n", checkout)

	// The clone can take minutes, during which config.json can change, work
	// can start and weft-worker can be paused, so the edit and the restart
	// check are made again. A refusal now leaves the checkout published, and
	// provision-checkout.sh refuses a name that exists.
	unbound := fmt.Sprintf("; %s is published and unbound: once nothing runs, remove it and run bind again", checkout)
	p, err := commitBinding(ctx, out, "bind", name, add, unbound)
	if err != nil {
		return err
	}
	if p == nil {
		return fmt.Errorf("bind: /v2/projects lists no %s: tk's config.yaml (paths.tk_config) registers no such project, so the binding has nothing to serve", name)
	}
	if p.State != "available" && p.State != "empty" {
		return fmt.Errorf("bind: /v2/projects reports %s %s%s", name, p.State, p.reason())
	}
	fmt.Fprintf(out, "bind: /v2/projects reports %s %s\n", name, p.State)
	return nil
}

func weftUnbind(ctx context.Context, out io.Writer, name string) error {
	unlock, err := lockUpdater()
	if err != nil {
		return fmt.Errorf("unbind: %w; config.json left as it was", err)
	}
	defer unlock()
	remove := func(repos object) (object, error) {
		if repos.index(name) < 0 {
			return nil, fmt.Errorf("%s is not bound in weft.repositories", name)
		}
		return slices.DeleteFunc(repos, func(m member) bool { return m.key == name }), nil
	}
	p, err := commitBinding(ctx, out, "unbind", name, remove, "")
	if err != nil {
		return err
	}
	if p == nil {
		fmt.Fprintf(out, "unbind: /v2/projects lists no %s\n", name)
		return nil
	}
	if p.State == "available" || p.State == "empty" {
		return fmt.Errorf("unbind: /v2/projects still reports %s %s after the restart", name, p.State)
	}
	fmt.Fprintf(out, "unbind: /v2/projects reports %s %s%s\n", name, p.State, p.reason())
	return nil
}

// commitBinding makes edit to weft.repositories, writes it once the daemon's
// loader passes it and the restart hold is up, restarts the daemon and returns
// name as /v2/projects reports it, nil when unlisted. refused is appended to a
// refusal that left config.json as it was. The caller holds the updater's lock.
//
// The hold is the updater's own: the update pause shuts every weft claim, and
// ArmRestartBarrier is the final restart check made in one transaction with
// raising the barrier, which every other claim reads, so nothing starts between
// that check and the restart. The updater's lock is what makes the pause this
// command's to release: no updater cycle claimed it, and none can run to clear
// it as stale until the release.
func commitBinding(ctx context.Context, out io.Writer, verb, name string, edit func(object) (object, error), refused string) (*v2Project, error) {
	path, before, after, cfg, err := editConfig(edit)
	if err == nil {
		err = servable(path, cfg)
	}
	if err != nil {
		return nil, fmt.Errorf("%s: %w; config.json left as it was%s", verb, err, refused)
	}
	store, err := openLedger()
	if err != nil {
		return nil, fmt.Errorf("%s: %w; config.json left as it was%s", verb, err, refused)
	}
	defer store.Close()
	claimed, reason, err := store.ClaimUpdatePause()
	if err == nil && !claimed {
		err = alreadyPaused(reason)
	}
	if err != nil {
		return nil, fmt.Errorf("%s: %w; config.json left as it was%s", verb, err, refused)
	}
	defer releaseHold(out, verb, store)
	err = idle(store)
	if err == nil {
		err = store.ArmRestartBarrier(restartHoldLease)
	}
	var backup string
	if err == nil {
		fmt.Fprintf(out, "%s: %s paused and the restart barrier up, so no new work starts until the daemon answers\n", verb, ledger.WeftAgent)
		backup, err = replaceConfig(path, before, after)
	}
	if err != nil {
		return nil, fmt.Errorf("%s: %w; config.json left as it was%s", verb, err, refused)
	}
	fmt.Fprintf(out, "%s: wrote %s; the file it replaced is %s\n", verb, path, backup)
	if err := restartDaemon(); err != nil {
		return nil, fmt.Errorf("%s: restart the daemon: %w; %s is written, and the daemon reads it at its next start", verb, err, path)
	}
	fmt.Fprintln(out, verb+": restarted the daemon and watchdog")
	projects, err := awaitProjects(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", verb, err)
	}
	for i := range projects {
		if projects[i].Project == name {
			return &projects[i], nil
		}
	}
	return nil, nil
}

// releaseHold takes down the restart hold: the update pause, and only that
// pause, and the barrier. A failure leaves both to the updater's next poll,
// which finds them stale once the caller's lock is gone, or to the lease.
func releaseHold(out io.Writer, verb string, store *ledger.Store) {
	lifted, _, err := store.ReleaseUpdatePause()
	switch {
	case err != nil:
		fmt.Fprintf(out, "%s: release the restart hold: %v; the updater's next poll lifts it, or the barrier lapses within %s\n", verb, err, restartHoldLease)
	case lifted:
		fmt.Fprintf(out, "%s: restart hold released; dispatch resumed\n", verb)
	default:
		fmt.Fprintf(out, "%s: restart barrier down; the pause that replaced the hold on %s still stands\n", verb, ledger.WeftAgent)
	}
}

// lockUpdater takes the updater's lock, refusing while an updater cycle has
// it: that cycle may be about to claim the update pause and restart the
// daemon itself.
func lockUpdater() (func(), error) {
	home, err := config.Home()
	if err != nil {
		return nil, err
	}
	unlock, held, err := updater.Lock(home)
	if err != nil {
		return nil, err
	}
	if !held {
		return nil, errors.New("the updater's lock is held, by an updater poll, which may restart the daemon itself, or another weft bind or unbind; run this again once it is free")
	}
	return unlock, nil
}

// tkProjectFromRemote is the project tk derives from a remote.origin.url, as
// provision-checkout.sh derives it (tk's projectFromGitRemote, then its
// ValidName): ok is false for a name tk cannot resolve.
func tkProjectFromRemote(origin string) (string, bool) {
	remote := strings.TrimSpace(origin)
	remote = strings.TrimSuffix(remote, ".git")
	remote = strings.TrimSuffix(remote, "/")
	if i := strings.LastIndex(remote, "/"); i >= 0 {
		remote = remote[i+1:]
	} else if i := strings.LastIndex(remote, ":"); i >= 0 {
		remote = remote[i+1:]
	}
	name := strings.TrimSpace(remote)
	return name, name != "" && name != "." && name != ".." && !strings.Contains(name, "/")
}

// provisionScript is provision-checkout.sh in the shuttle checkout. A release
// ships the binary alone, so the scripts are run from where the operator runs
// the rest of scripts/agent-users, ~/code/shuttle, the checkout agents_dir
// also defaults into.
func provisionScript() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, "code", "shuttle", "scripts", "agent-users", "provision-checkout.sh"), nil
}

// servable refuses a config whose weft.listen the restarted daemon would not
// serve, or this command could not read /v2/projects from: unset, no hosted
// service answers; one projectsURL refuses, the daemon's start fails on it or
// the token would go somewhere else.
func servable(path string, cfg *config.Config) error {
	if cfg.Weft.Listen == "" {
		return fmt.Errorf("%s sets no weft.listen, so no hosted service would answer /v2/projects", path)
	}
	_, err := projectsURL(cfg.Weft.Listen)
	return err
}

// projectsURL is /v2/projects on the endpoint listen names, built from its
// validated host and port. ValidateListen holds the host to the daemon's rule
// but not the port, and listen pasted into a URL can name another host:
// "127.0.0.1:7431@attacker.example" passes it and would send the bearer token
// to attacker.example.
func projectsURL(listen string) (string, error) {
	if err := weftremote.ValidateListen(listen); err != nil {
		return "", err
	}
	host, port, _ := net.SplitHostPort(listen)
	n, err := strconv.ParseUint(port, 10, 16)
	if err != nil || n == 0 {
		return "", fmt.Errorf("weft.listen: port %q is not a number in 1-65535", port)
	}
	u := url.URL{Scheme: "http", Host: net.JoinHostPort(host, strconv.FormatUint(n, 10)), Path: "/v2/projects"}
	return u.String(), nil
}

// restartable refuses, ahead of the clone, what commitBinding would refuse
// after it: a pause it cannot claim, or work a restart would kill.
func restartable() error {
	store, err := openLedger()
	if err != nil {
		return err
	}
	defer store.Close()
	agent, err := store.Agent(ledger.WeftAgent)
	if err != nil {
		return err
	}
	if agent.Paused {
		return alreadyPaused(agent.PausedReason.String)
	}
	return idle(store)
}

// alreadyPaused refuses a restart hold over a pause that stands: an
// operator's, auth or streak pause is not this command's to take over or
// lift, and an update's restarts the daemon itself.
func alreadyPaused(reason string) error {
	return fmt.Errorf("%s is already paused (%s), and the restart holds new work by pausing it, which takes over no other pause; run this again once that pause lifts", ledger.WeftAgent, reason)
}

// idle refuses a restart that would kill something, as the updater's drain
// reads it.
func idle(store *ledger.Store) error {
	work, err := store.RestartWork()
	if err != nil {
		return err
	}
	if !work.Empty() {
		return fmt.Errorf("a daemon restart would kill what is running: %s", strings.Join(slices.Concat(work.Runs, work.Terminals), ", "))
	}
	return nil
}

// editConfig reads config.json, applies edit to weft.repositories and holds
// the result to the daemon's loader.
func editConfig(edit func(object) (object, error)) (path string, before, after []byte, cfg *config.Config, err error) {
	path, err = config.Path()
	if err != nil {
		return "", nil, nil, nil, err
	}
	before, err = os.ReadFile(path)
	if err != nil {
		return "", nil, nil, nil, err
	}
	after, err = editRepositories(before, edit)
	if err != nil {
		return "", nil, nil, nil, fmt.Errorf("%s: %w", path, err)
	}
	cfg, err = config.Parse(path, after)
	if err != nil {
		return "", nil, nil, nil, fmt.Errorf("the daemon's loader refuses the edited config: %w", err)
	}
	return path, before, after, cfg, nil
}

// replaceConfig puts after in place of config.json by rename, so the daemon
// never reads a half-written file. The file it replaces is kept by a hard link
// beside it, under the name it returns, made only once the new file is on disk
// so a failed write leaves no backup behind; os.Link refuses a name that
// exists, so no earlier backup is overwritten.
func replaceConfig(path string, before, after []byte) (string, error) {
	info, err := os.Stat(path)
	if err != nil {
		return "", err
	}
	current, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	if !bytes.Equal(current, before) {
		return "", fmt.Errorf("%s changed while being edited", path)
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".*")
	if err != nil {
		return "", err
	}
	defer os.Remove(tmp.Name())
	_, err = tmp.Write(after)
	if err == nil {
		err = tmp.Chmod(info.Mode().Perm())
	}
	if err == nil {
		err = tmp.Sync()
	}
	if closeErr := tmp.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return "", fmt.Errorf("write %s: %w", path, err)
	}
	backup := path + ".bak-" + time.Now().UTC().Format("20060102T150405Z")
	if err := os.Link(path, backup); err != nil {
		return "", fmt.Errorf("back up %s: %w", path, err)
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		os.Remove(backup)
		return "", fmt.Errorf("write %s: %w", path, err)
	}
	return backup, nil
}

// member is one key of a JSON object, its value kept as written.
type member struct {
	key   string
	value json.RawMessage
}

// object is a JSON object in its written key order, so an edit of one key
// leaves every other where and as it was.
type object []member

func parseObject(data []byte) (object, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	if tok, err := dec.Token(); err != nil || tok != json.Delim('{') {
		return nil, errors.New("not a JSON object")
	}
	var o object
	for dec.More() {
		tok, err := dec.Token()
		if err != nil {
			return nil, err
		}
		var value json.RawMessage
		if err := dec.Decode(&value); err != nil {
			return nil, err
		}
		o = append(o, member{key: tok.(string), value: value})
	}
	if _, err := dec.Token(); err != nil {
		return nil, err
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return nil, errors.New("trailing content after the object")
	}
	return o, nil
}

// index is the last member named key, the one encoding/json decodes.
func (o object) index(key string) int {
	for i := len(o) - 1; i >= 0; i-- {
		if o[i].key == key {
			return i
		}
	}
	return -1
}

func (o object) set(key string, value []byte) object {
	if i := o.index(key); i >= 0 {
		o[i].value = value
		return o
	}
	return append(o, member{key: key, value: value})
}

func (o object) marshal() ([]byte, error) {
	var b bytes.Buffer
	b.WriteByte('{')
	for i, m := range o {
		if i > 0 {
			b.WriteByte(',')
		}
		key, err := json.Marshal(m.key)
		if err != nil {
			return nil, err
		}
		b.Write(key)
		b.WriteByte(':')
		b.Write(m.value)
	}
	b.WriteByte('}')
	return b.Bytes(), nil
}

// editRepositories applies edit to weft.repositories in data, creating the
// key and its parent when absent, and returns the config indented as
// config.json is: two spaces, a trailing newline.
func editRepositories(data []byte, edit func(object) (object, error)) ([]byte, error) {
	top, err := parseObject(data)
	if err != nil {
		return nil, err
	}
	weft := object{}
	if i := top.index("weft"); i >= 0 {
		if weft, err = parseObject(top[i].value); err != nil {
			return nil, fmt.Errorf("weft: %w", err)
		}
	}
	repos := object{}
	if i := weft.index("repositories"); i >= 0 {
		if repos, err = parseObject(weft[i].value); err != nil {
			return nil, fmt.Errorf("weft.repositories: %w", err)
		}
	}
	if repos, err = edit(repos); err != nil {
		return nil, err
	}
	value, err := repos.marshal()
	if err != nil {
		return nil, err
	}
	if value, err = weft.set("repositories", value).marshal(); err != nil {
		return nil, err
	}
	if value, err = top.set("weft", value).marshal(); err != nil {
		return nil, err
	}
	var compact, out bytes.Buffer
	if err := json.Compact(&compact, value); err != nil {
		return nil, err
	}
	if err := json.Indent(&out, compact.Bytes(), "", "  "); err != nil {
		return nil, err
	}
	out.WriteByte('\n')
	return out.Bytes(), nil
}

// v2Project is the part of a /v2/projects row these commands report.
type v2Project struct {
	Project string `json:"project"`
	State   string `json:"state"`
	Reason  *struct {
		Code   string `json:"code"`
		Detail string `json:"detail"`
	} `json:"reason"`
}

func (p *v2Project) reason() string {
	if p.Reason == nil {
		return ""
	}
	return fmt.Sprintf(" (%s): %s", p.Reason.Code, p.Reason.Detail)
}

// awaitProjects reads /v2/projects from the restarted daemon, retrying until
// its service answers or projectsBound passes. The bearer token goes over
// plain HTTP, so the address is held to projectsURL before the token is read.
// Neither a refused address nor a refused token is retried: no restart changes
// either.
func awaitProjects(ctx context.Context, cfg *config.Config) ([]v2Project, error) {
	endpoint, err := projectsURL(cfg.Weft.Listen)
	if err != nil {
		return nil, err
	}
	token, err := weftToken(cfg)
	if err != nil {
		return nil, err
	}
	deadline := time.Now().Add(projectsBound)
	for {
		projects, err := readProjects(ctx, endpoint, token)
		if err == nil {
			return projects, nil
		}
		if errors.Is(err, errTokenRefused) {
			return nil, err
		}
		if !time.Now().Before(deadline) {
			return nil, fmt.Errorf("/v2/projects did not answer within %s: %w", projectsBound, err)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(projectsPoll):
		}
	}
}

// projectsClient takes no proxy from the environment and keeps no
// connection, for the reasons the updater's serviceClient gives.
var projectsClient = &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}

// readV2Projects reads /v2/projects at endpoint, a projectsURL, with the
// bearer token, which every route requires.
func readV2Projects(ctx context.Context, endpoint, token string) ([]v2Project, error) {
	ctx, cancel := context.WithTimeout(ctx, projectsReadBound)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := projectsClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		err := fmt.Errorf("GET /v2/projects: %s: %s", resp.Status, strings.TrimSpace(string(body)))
		if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
			err = fmt.Errorf("%w: %w", errTokenRefused, err)
		}
		return nil, err
	}
	var doc struct {
		Projects []v2Project `json:"projects"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&doc); err != nil {
		return nil, fmt.Errorf("GET /v2/projects: %w", err)
	}
	return doc.Projects, nil
}
