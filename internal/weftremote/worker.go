package weftremote

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"time"
)

// WorkerContractV2 is the host ↔ worker protocol of weft-service/v2 §11.
const WorkerContractV2 = "weft-worker/v2"

// workerCapabilities are the worker capability ids of §2.6, the only ones a
// version document may list.
var workerCapabilities = []string{
	"segment", "land", "reconcile", "cleanup", "archive", "output", "liveness", "allowance",
	"stale_base_check", "trial_environment",
}

// The bounds every probe of worker_bin runs under: a probe that outlives its
// timeout is killed by process group, and stdout past the cap is discarded
// and fails the probe.
const (
	workerProbeTimeout = 10 * time.Second
	probeStdoutLimit   = 64 << 10
)

// workerReprobeAfter is how long a failed version probe stands before an
// unchanged worker_bin is probed again: a probe can fail for a reason outside
// the binary, such as a timeout under load.
var workerReprobeAfter = 60 * time.Second

var providerProbeTimeout = 30 * time.Second

// workerInfo is what `weft-worker version --json` said (§2.6), or why it said
// nothing usable: Failure is set, bounded and free of the probe's output.
type workerInfo struct {
	stamp        workerStamp
	probedAt     time.Time
	Version      string
	Contracts    []string
	Capabilities []string
	Failure      string
}

// workerStamp identifies one worker_bin: a change in any part re-probes it.
// The SHA-256 is computed only when size or mtime moved.
type workerStamp struct {
	size  int64
	mtime time.Time
	sha   string
}

func (w *workerInfo) speaks(contract string) bool {
	return w != nil && w.Failure == "" && slices.Contains(w.Contracts, contract)
}

func (w *workerInfo) has(capability string) bool {
	return w.speaks(WorkerContractV2) && slices.Contains(w.Capabilities, capability)
}

// refreshWorker returns what worker_bin speaks, probing it again when its
// size, mtime or SHA-256 moved since the last probe, or when the last probe
// failed more than workerReprobeAfter ago. A failed probe is a logged
// diagnostic, once per distinct failure: the worker keeps v1 dispatch, and
// what needs weft-worker/v2 stays not served.
func (s *Supervisor) refreshWorker() *workerInfo {
	s.probeMu.Lock()
	defer s.probeMu.Unlock()
	bin := s.cfg.Weft.WorkerBin
	current := s.worker.Load()
	stamp, err := stampWorker(bin, current)
	if current != nil && current.stamp == stamp &&
		(current.Failure == "" || time.Since(current.probedAt) < workerReprobeAfter) {
		return current
	}
	var info *workerInfo
	if err != nil {
		info = &workerInfo{Failure: "cannot read the worker binary: " + err.Error()}
	} else {
		info = probeWorker(bin)
	}
	info.stamp, info.probedAt = stamp, time.Now()
	s.worker.Store(info)
	if current != nil && current.Failure != "" && current.Failure == info.Failure {
		return info
	}
	if info.Failure != "" {
		s.log.Printf("weft worker host=%s: probe of %s failed: %s; segments launch under %s and output is not served",
			s.hostID, bin, info.Failure, ContractVersion)
	} else {
		s.log.Printf("weft worker host=%s: %s is %s, contracts [%s], capabilities [%s]", s.hostID, bin,
			info.Version, strings.Join(info.Contracts, " "), strings.Join(info.Capabilities, " "))
	}
	return info
}

func stampWorker(bin string, prior *workerInfo) (workerStamp, error) {
	info, err := os.Stat(bin)
	if err != nil {
		return workerStamp{}, err
	}
	if prior != nil && prior.stamp.sha != "" && prior.stamp.size == info.Size() && prior.stamp.mtime.Equal(info.ModTime()) {
		return prior.stamp, nil
	}
	f, err := os.Open(bin)
	if err != nil {
		return workerStamp{}, err
	}
	defer f.Close()
	sum := sha256.New()
	if _, err := io.Copy(sum, f); err != nil {
		return workerStamp{}, err
	}
	return workerStamp{size: info.Size(), mtime: info.ModTime(), sha: hex.EncodeToString(sum.Sum(nil))}, nil
}

// probeWorker runs `weft-worker version --json` and reads worker-version.json
// strictly. A v1-only worker exits 64 on the unknown subcommand.
func probeWorker(bin string) *workerInfo {
	out, failure := runProbe(bin, []string{"version", "--json"}, workerProbeTimeout)
	if failure != "" {
		return &workerInfo{Failure: failure}
	}
	var doc struct {
		ContractVersion string   `json:"contract_version"`
		Event           string   `json:"event"`
		WorkerVersion   string   `json:"worker_version"`
		Contracts       []string `json:"contracts"`
		Capabilities    []string `json:"capabilities"`
	}
	if failure := decodeWorkerDoc(out, &doc, "contract_version", "event", "worker_version", "contracts", "capabilities"); failure != "" {
		return &workerInfo{Failure: "version document: " + failure}
	}
	switch {
	case doc.ContractVersion != WorkerContractV2 || doc.Event != "version":
		return &workerInfo{Failure: "version document: not a " + WorkerContractV2 + " version event"}
	case doc.WorkerVersion == "" || len(doc.WorkerVersion) > 200:
		return &workerInfo{Failure: "version document: worker_version is empty or over 200 bytes"}
	case len(doc.Contracts) == 0:
		return &workerInfo{Failure: "version document: contracts is empty"}
	}
	for _, c := range doc.Contracts {
		if c != ContractVersion && c != WorkerContractV2 {
			return &workerInfo{Failure: "version document: contracts names one this host does not speak"}
		}
	}
	for i, c := range doc.Capabilities {
		if !slices.Contains(workerCapabilities, c) || slices.Contains(doc.Capabilities[:i], c) {
			return &workerInfo{Failure: "version document: capabilities names an undeclared or repeated capability"}
		}
	}
	return &workerInfo{Version: doc.WorkerVersion, Contracts: doc.Contracts, Capabilities: doc.Capabilities}
}

// decodeWorkerDoc reads a document a probe printed: a JSON object with
// exactly keys. Its failure never quotes the document, which is the probe's
// stdout and may hold anything a provider CLI printed.
func decodeWorkerDoc(doc []byte, dst any, keys ...string) string {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(doc, &fields); err != nil || fields == nil {
		return "not a JSON object"
	}
	for _, key := range keys {
		if _, ok := fields[key]; !ok {
			return "no " + key
		}
	}
	if len(fields) != len(keys) {
		return "an undeclared key"
	}
	if err := decodeStrict(doc, dst); err != nil {
		return "a value of the wrong type"
	}
	return ""
}

// runProbe runs worker_bin once, bounded: stdin and stderr are the null
// device, stdout is capped, the environment carries no credential, and the
// process group is killed at the timeout and once the probe exits. The
// failure it returns names the bound or exit, never what the probe printed.
func runProbe(bin string, args []string, timeout time.Duration) ([]byte, string) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, bin, args...)
	cmd.Env = probeEnviron()
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
	cmd.WaitDelay = time.Second
	out := &cappedBuffer{limit: probeStdoutLimit}
	cmd.Stdout = out
	err := cmd.Start()
	if err != nil {
		return nil, "could not start: " + err.Error()
	}
	err = cmd.Wait()
	_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	var exit *exec.ExitError
	switch {
	case errors.Is(ctx.Err(), context.DeadlineExceeded):
		return nil, fmt.Sprintf("killed at its %s bound", timeout)
	case out.over:
		return nil, fmt.Sprintf("stdout exceeded %d bytes", probeStdoutLimit)
	case errors.As(err, &exit) && exit.ExitCode() >= 0:
		return nil, fmt.Sprintf("exited %d", exit.ExitCode())
	case err != nil:
		return nil, "did not exit cleanly"
	}
	return out.buf.Bytes(), ""
}

// probeEnviron is a probe's from-scratch environment: what a CLI needs to
// find its own configuration, and none of the segment's credentials.
func probeEnviron() []string {
	home, _ := os.UserHomeDir()
	user := filepath.Base(home)
	return []string{"PATH=" + workerPath, "HOME=" + home, "USER=" + user, "LOGNAME=" + user, "LANG=en_US.UTF-8"}
}

// cappedBuffer keeps the first limit bytes and drains the rest, so a chatty
// probe neither blocks on a full pipe nor grows the host's memory.
type cappedBuffer struct {
	buf   bytes.Buffer
	limit int
	over  bool
}

func (c *cappedBuffer) Write(p []byte) (int, error) {
	if room := c.limit - c.buf.Len(); room < len(p) {
		c.over = true
		if room > 0 {
			c.buf.Write(p[:room])
		}
		return len(p), nil
	}
	return c.buf.Write(p)
}
