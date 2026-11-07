package updater

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"time"
)

const (
	binaryName     = "shuttle"
	checksumsAsset = "checksums.txt"
	// manifestAsset is release/manifest.json, published beside the tarball
	// and listed in checksums.txt.
	manifestAsset = "manifest.json"
	// stagedPattern names a verified binary waiting beside the install target.
	stagedPattern = ".shuttle-update-*"
	// stagedVersionBound bounds running a staged binary's `version`.
	stagedVersionBound = 10 * time.Second
	// downloadBound bounds one asset: a stalled fetch must not hold the poll
	// loop, or the stale-pause recovery at its head, indefinitely.
	downloadBound = 5 * time.Minute
)

// githubAPIBase is the root the default release source reads. A var so a test
// can point it at a local server.
var githubAPIBase = "https://api.github.com"

// Release is one published release: its tag and each asset's download URL.
type Release struct {
	Tag    string
	Assets map[string]string
}

// Source fetches releases.
type Source interface {
	Latest(ctx context.Context) (Release, error)
	Download(ctx context.Context, url string) ([]byte, error)
}

// githubSource reads EnderRealm/shuttle's GitHub Releases. The repository is
// public, so it holds no credential.
type githubSource struct{}

func (githubSource) get(ctx context.Context, url string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, downloadBound)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET %s: %s", url, resp.Status)
	}
	return io.ReadAll(resp.Body)
}

func (g githubSource) Latest(ctx context.Context) (Release, error) {
	body, err := g.get(ctx, githubAPIBase+"/repos/EnderRealm/shuttle/releases/latest")
	if err != nil {
		return Release{}, err
	}
	var doc struct {
		TagName string `json:"tag_name"`
		Assets  []struct {
			Name        string `json:"name"`
			DownloadURL string `json:"browser_download_url"`
		} `json:"assets"`
	}
	if err := json.Unmarshal(body, &doc); err != nil {
		return Release{}, err
	}
	release := Release{Tag: doc.TagName, Assets: make(map[string]string, len(doc.Assets))}
	for _, a := range doc.Assets {
		release.Assets[a.Name] = a.DownloadURL
	}
	return release, nil
}

func (g githubSource) Download(ctx context.Context, url string) ([]byte, error) {
	return g.get(ctx, url)
}

// semver is a release version, its three numbers in significance order.
type semver [3]int

var (
	semverPattern = regexp.MustCompile(`^v?(\d+)\.(\d+)\.(\d+)$`)
	// workerPattern reads the version out of the worker's self-description:
	// worker-version.json's worker_version is a sentence, "weft-worker 0.2.0
	// (weft@dev)", whatever the build.
	workerPattern = regexp.MustCompile(`^weft-worker (\d+\.\d+\.\d+)(?: |$)`)
)

func parseSemver(value string) (semver, bool) {
	var v semver
	m := semverPattern.FindStringSubmatch(value)
	if m == nil {
		return v, false
	}
	for i := range v {
		n, err := strconv.Atoi(m[i+1])
		if err != nil {
			return v, false
		}
		v[i] = n
	}
	return v, true
}

func (v semver) before(other semver) bool { return slices.Compare(v[:], other[:]) < 0 }

// wanted reports whether tag should replace current. A dev build, or any
// version that is not a release's, always updates, as in loom: a host must
// not stay on an unreleased binary. A release moves only forward: a lower
// tag would hand a migrated ledger to a binary that refuses it.
func wanted(current, tag string) (bool, error) {
	latest, ok := parseSemver(tag)
	if !ok {
		return false, fmt.Errorf("release tag %q is not a semver", tag)
	}
	running, ok := parseSemver(current)
	return !ok || running.before(latest), nil
}

// manifest is what a release declares about the host it needs. Unknown keys
// are ignored: an updater that refused them could never install the release
// that teaches it what they mean.
type manifest struct {
	MinWorkerVersion string `json:"min_worker_version"`
}

// parseManifest reads a manifest and the minimum it declares.
func parseManifest(data []byte) (semver, string, error) {
	var m manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return semver{}, "", fmt.Errorf("%s: %w", manifestAsset, err)
	}
	minimum, ok := parseSemver(m.MinWorkerVersion)
	if !ok {
		return semver{}, "", fmt.Errorf("%s: min_worker_version %q is not a semver", manifestAsset, m.MinWorkerVersion)
	}
	return minimum, m.MinWorkerVersion, nil
}

// checkWorker refuses a release that needs a newer worker than the probe
// found. A worker that cannot be read is a refusal too: a host that cannot
// show it meets the minimum must not risk failing to.
func checkWorker(data []byte, probe func() (string, error)) error {
	minimum, stated, err := parseManifest(data)
	if err != nil {
		return err
	}
	description, err := probe()
	if err != nil {
		return fmt.Errorf("probe weft.worker_bin: %w", err)
	}
	m := workerPattern.FindStringSubmatch(description)
	if m == nil {
		return fmt.Errorf("worker reports %q, which names no semver to hold against the release's minimum %s", description, stated)
	}
	have, ok := parseSemver(m[1])
	if !ok {
		return fmt.Errorf("worker version %q is not a semver", m[1])
	}
	if have.before(minimum) {
		return fmt.Errorf("release needs worker %s or newer and weft.worker_bin is %s", stated, m[1])
	}
	return nil
}

// assetName is the GoReleaser tarball name for tag on this host.
func assetName(tag string) string {
	return fmt.Sprintf("%s_%s_%s_%s.tar.gz", binaryName, strings.TrimPrefix(tag, "v"), runtime.GOOS, runtime.GOARCH)
}

// verifyChecksum holds data to name's line in checksums.txt.
func verifyChecksum(data, sums []byte, name string) error {
	want := ""
	for line := range strings.SplitSeq(string(sums), "\n") {
		if fields := strings.Fields(line); len(fields) == 2 && fields[1] == name {
			want = fields[0]
			break
		}
	}
	if want == "" {
		return fmt.Errorf("%s has no line for %s", checksumsAsset, name)
	}
	sum := sha256.Sum256(data)
	if got := hex.EncodeToString(sum[:]); got != want {
		return fmt.Errorf("%s: sha256 mismatch: got %s, want %s", name, got, want)
	}
	return nil
}

// extractBinary returns the shuttle binary from the gzip+tar archive, and
// nothing else in it reaches the disk. It takes one regular file named
// exactly shuttle at the archive's root, and refuses an archive holding any
// entry that climbs out of it: a checksum proves only that the bytes are the
// ones published.
func extractBinary(tarball []byte) ([]byte, error) {
	gz, err := gzip.NewReader(bytes.NewReader(tarball))
	if err != nil {
		return nil, fmt.Errorf("gzip: %w", err)
	}
	defer gz.Close()
	var binary []byte
	tr := tar.NewReader(gz)
	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("tar: %w", err)
		}
		name := path.Clean(hdr.Name)
		if path.IsAbs(hdr.Name) || name == ".." || strings.HasPrefix(name, "../") {
			return nil, fmt.Errorf("archive entry %q leaves the archive", hdr.Name)
		}
		if name != binaryName {
			continue
		}
		if hdr.Typeflag != tar.TypeReg {
			return nil, fmt.Errorf("archive entry %q is not a regular file", hdr.Name)
		}
		if binary != nil {
			return nil, fmt.Errorf("archive holds %s twice", binaryName)
		}
		if binary, err = io.ReadAll(tr); err != nil {
			return nil, fmt.Errorf("read %s: %w", binaryName, err)
		}
	}
	if binary == nil {
		return nil, fmt.Errorf("archive holds no %s binary", binaryName)
	}
	return binary, nil
}

// staged is a release that has passed everything short of installing.
type staged struct {
	tag  string
	path string
	// build is what /v2/service reports once the daemon runs this binary.
	build string
}

// stage writes the verified binary beside bin, so the install is a rename on
// one filesystem, and runs it: a binary that will not execute here, or that
// was stamped with another version, would otherwise surface only once the
// daemon was already down.
func stage(ctx context.Context, binary []byte, bin, tag string) (*staged, error) {
	tmp, err := os.CreateTemp(filepath.Dir(bin), stagedPattern)
	if err != nil {
		return nil, fmt.Errorf("stage: %w", err)
	}
	s := &staged{tag: tag, path: tmp.Name()}
	_, err = tmp.Write(binary)
	err = errors.Join(err, tmp.Close(), os.Chmod(s.path, 0o755))
	if err != nil {
		os.Remove(s.path)
		return nil, fmt.Errorf("stage: %w", err)
	}
	ctx, cancel := context.WithTimeout(ctx, stagedVersionBound)
	defer cancel()
	out, err := exec.CommandContext(ctx, s.path, "version").Output()
	if err != nil {
		os.Remove(s.path)
		return nil, fmt.Errorf("run staged binary: %w", err)
	}
	reported := strings.TrimSpace(string(out))
	if strings.TrimPrefix(reported, "v") != strings.TrimPrefix(tag, "v") {
		os.Remove(s.path)
		return nil, fmt.Errorf("staged binary reports %q, not release %s", reported, tag)
	}
	s.build = buildPrefix + reported
	return s, nil
}
