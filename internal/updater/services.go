package updater

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"shuttle/internal/config"
	"shuttle/internal/launchd"
	"shuttle/internal/weftremote"
)

const (
	// buildPrefix is how /v2/service spells host_build before the version.
	buildPrefix = "shuttle "
	// buildReadBound bounds one read of /v2/service.
	buildReadBound = 10 * time.Second
	// restartBound covers two jobs' bootout, bootstrap and start checks.
	restartBound = 2 * time.Minute
)

// serviceClient takes no proxy from the environment: net/http exempts loopback
// but not a tailnet listener, so an inherited HTTP_PROXY would be handed the
// bearer token in plain HTTP. A read can straddle a daemon restart, so it keeps
// no connection.
var serviceClient = &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}

// Services is the daemon and watchdog as the updater drives them.
type Services interface {
	// Build reads host_build from /v2/service, the build actually serving.
	Build(ctx context.Context) (string, error)
	// Restart re-bootstraps the daemon and watchdog under bin.
	Restart(ctx context.Context, bin string) error
	// Stop unloads both, so nothing has the ledger open but the updater.
	Stop(ctx context.Context) error
	// RestartSelf hands the updater's own job to bin. It returns before the
	// job is replaced.
	RestartSelf(bin string) error
}

// launchdServices is Services on the host's launchd jobs.
type launchdServices struct{}

func (launchdServices) Build(ctx context.Context) (string, error) {
	cfg, err := config.Load()
	if err != nil {
		return "", err
	}
	return buildAt(ctx, cfg.Weft.Listen, cfg.WeftToken)
}

// buildAt refuses a host with no weft.listen: without /v2/service nothing can
// show the new build came up, and an update that cannot be confirmed cannot
// be fallen back from. The token goes over plain HTTP, so the address is held
// to the daemon's own rule before the token is even read.
func buildAt(ctx context.Context, listen string, token func() (string, error)) (string, error) {
	if listen == "" {
		return "", errors.New("weft.listen is not set, so no /v2/service can confirm a build")
	}
	base, err := serviceBase(listen)
	if err != nil {
		return "", err
	}
	secret, err := token()
	if err != nil {
		return "", err
	}
	return readBuild(ctx, base, secret)
}

// serviceBase is the endpoint listen names, built from its validated host and
// port. ValidateListen holds the host to the daemon's rule but not the port,
// and listen pasted into a URL can name another host:
// "127.0.0.1:7431@attacker.example" passes it and would send the bearer token
// to attacker.example. A listen that net.JoinHostPort would not rebuild
// exactly is refused rather than normalised, so the address read is the one
// configured.
func serviceBase(listen string) (string, error) {
	if err := weftremote.ValidateListen(listen); err != nil {
		return "", err
	}
	host, port, _ := net.SplitHostPort(listen)
	n, err := strconv.ParseUint(port, 10, 16)
	if err != nil || n == 0 {
		return "", fmt.Errorf("weft.listen: port %q is not a number in 1-65535", port)
	}
	hostPort := net.JoinHostPort(host, strconv.FormatUint(n, 10))
	if hostPort != listen {
		return "", fmt.Errorf("weft.listen: %q is not in the canonical form %q", listen, hostPort)
	}
	u := url.URL{Scheme: "http", Host: hostPort}
	return u.String(), nil
}

// readBuild reads host_build from the service at base, which every route
// guards with the bearer token, discovery included.
func readBuild(ctx context.Context, base, token string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, buildReadBound)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/v2/service", nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := serviceClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("GET /v2/service: %s", resp.Status)
	}
	var doc struct {
		Service struct {
			HostBuild string `json:"host_build"`
		} `json:"service"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&doc); err != nil {
		return "", fmt.Errorf("GET /v2/service: %w", err)
	}
	if doc.Service.HostBuild == "" {
		return "", errors.New("GET /v2/service: no host_build")
	}
	return doc.Service.HostBuild, nil
}

// Restart runs the binary being installed rather than launchd.Install here:
// that binary writes the plists it needs, and a job is re-registered under
// its code signature only by bootout and bootstrap. `launchctl kickstart`
// leaves a running job alone, and -k has launchd refuse the respawn of a
// binary signed differently from the one it bootstrapped (EX_CONFIG, loom's
// "macOS caveat"). The child replaces other jobs, so this one can wait on it.
func (launchdServices) Restart(ctx context.Context, bin string) error {
	ctx, cancel := context.WithTimeout(ctx, restartBound)
	defer cancel()
	out, err := exec.CommandContext(ctx, bin, "updater", "rebootstrap").CombinedOutput()
	if err != nil {
		return fmt.Errorf("%s updater rebootstrap: %w: %s", bin, err, strings.TrimSpace(string(out)))
	}
	return nil
}

// Stop takes the watchdog down first, or it revives the daemon.
func (launchdServices) Stop(context.Context) error {
	if err := launchd.Bootout(launchd.WatchdogLabel); err != nil {
		return err
	}
	return launchd.Bootout(launchd.DaemonLabel)
}

// RestartSelf spawns the detached `updater reexec` helper: a job cannot boot
// itself out and survive, and a child in its own session escapes the
// teardown. Its output goes to updater.log, the only record of whether the
// updater came back. A host where the job is not installed, as under a
// hand-run `updater once`, has nothing to restart.
func (launchdServices) RestartSelf(bin string) error {
	plist, err := launchd.PlistPath(launchd.UpdaterLabel)
	if err != nil {
		return err
	}
	if _, err := os.Stat(plist); err != nil {
		return nil
	}
	home, err := config.Home()
	if err != nil {
		return err
	}
	logFile, err := OpenLog(home)
	if err != nil {
		return err
	}
	defer logFile.Close()
	cmd := exec.Command(bin, "updater", "reexec")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	cmd.Stdout, cmd.Stderr = logFile, logFile
	return cmd.Start()
}

// probeWorker is what weft.worker_bin says it is, by the daemon's own probe.
func probeWorker() (string, error) {
	cfg, err := config.Load()
	if err != nil {
		return "", err
	}
	version, _, _, failure := weftremote.ProbeWorker(cfg.Weft.WorkerBin)
	if failure != "" {
		return "", errors.New(failure)
	}
	return version, nil
}

// OpenLog opens updater.log under the state root for appending.
func OpenLog(home string) (*os.File, error) {
	return os.OpenFile(filepath.Join(home, logFile), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
}
