package observer

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"time"

	"shuttle/internal/gitenv"
)

// Held push outcomes. A failed push is the error Push returns, and
// HeldPushFailed is what its callers record for it.
const (
	HeldPushed        = "pushed"
	HeldNoRemote      = "no-remote"
	HeldNothingToPush = "nothing-to-push"
	HeldPushFailed    = "failed"
)

const (
	heldRevisionFile = "notebook.json"
	heldEntriesDir   = "entries"
	heldRequestsDir  = "requests"
	heldBranch       = "main"
	heldRemote       = "origin"

	// heldGitTimeout bounds one git call. Every call but the push is against
	// the local repository; the push is bounded by the same figure so a remote
	// that never answers cannot hold a run open.
	heldGitTimeout = time.Minute

	// heldCommitName and heldCommitEmail are passed with -c on every call: a
	// launchd job inherits no login-shell configuration, and an unset identity
	// would fail every mutation.
	heldCommitName  = "shuttle"
	heldCommitEmail = "shuttle@localhost"
)

// Held is a notebook Shuttle holds itself rather than reaching through a
// domain command (docs/observer-contract.md §5). Dir is a git repository of
// its own: notebook.json carries the revision, entries/ one file per entry
// with the actions claimed against it and their receipts, and requests/ one
// file per applied request id with the outcome a replay returns. Every
// accepted mutation is exactly one commit; a refusal or a replay writes and
// commits nothing.
//
// The rules are Memory's, not a second copy of them. Each operation takes an
// exclusive flock on Dir+".lock", loads the repository into a Memory, applies
// the operation there and, only when the revision advanced, writes the files
// that changed and commits them. The lock is what serializes the daemon
// against `shuttle reconcile`, and it is what keeps Claim and Retire ordered
// across processes the way Memory's mutex orders them within one.
//
// The method set is CommandNotebook's, so a run reads and records through one
// interface whichever transport the registration names.
//
// Identity is the observer the notebook belongs to. notebook.json records it
// with the first commit, and every operation, Push included, refuses a
// repository recorded for another: identities differing only in case share a
// directory on a case-folding volume, and one must never read, write or push
// the other's records.
type Held struct {
	Dir      string
	Identity Identity
}

type heldRevision struct {
	Revision int64    `json:"revision"`
	Owner    Identity `json:"owner"`
}

// heldEntry is one entry's file: the entry and every action claimed against
// it, each beside its receipt, ordered by action id so the file is stable.
type heldEntry struct {
	Entry   *Entry       `json:"entry"`
	Actions []heldAction `json:"actions"`
}

type heldAction struct {
	Action  *Action  `json:"action"`
	Receipt *Receipt `json:"receipt"`
}

// heldRequest is one applied request: the identity-scoped id, the fingerprint
// a replay is compared against, and the outcome the first attempt produced.
type heldRequest struct {
	Identity    Identity    `json:"identity"`
	RequestID   string      `json:"request_id"`
	Fingerprint string      `json:"fingerprint"`
	Entry       *Entry      `json:"entry,omitempty"`
	Receipt     *Receipt    `json:"receipt,omitempty"`
	Retirement  *Retirement `json:"retirement,omitempty"`
}

func (h Held) Revision(ctx context.Context) (int64, error) {
	var revision int64
	err := h.locked(ctx, func() error {
		m, _, err := h.load()
		if err != nil {
			return err
		}
		revision = m.revision
		return nil
	})
	return revision, err
}

func (h Held) Entry(ctx context.Context, id string) (*Entry, error) {
	var out *Entry
	err := h.locked(ctx, func() error {
		m, _, err := h.load()
		if err != nil {
			return err
		}
		out, err = m.Entry(id)
		return err
	})
	return out, err
}

func (h Held) List(ctx context.Context) ([]*Entry, error) {
	var out []*Entry
	err := h.locked(ctx, func() error {
		m, _, err := h.load()
		if err != nil {
			return err
		}
		out = m.List()
		return nil
	})
	return out, err
}

func (h Held) Register(ctx context.Context, req Request, q *Question) (*Entry, error) {
	var out *Entry
	err := h.mutate(ctx, "register", req, func(m *Memory) (string, error) {
		var err error
		out, err = m.Register(req, q)
		return entryIDOf(out), err
	})
	return out, err
}

func (h Held) Activate(ctx context.Context, req Request, exp *Experiment) (*Entry, error) {
	var out *Entry
	err := h.mutate(ctx, "activate", req, func(m *Memory) (string, error) {
		var err error
		out, err = m.Activate(req, exp)
		return entryIDOf(out), err
	})
	return out, err
}

func (h Held) Record(ctx context.Context, req Request, a *Assessment) (*Entry, error) {
	var out *Entry
	err := h.mutate(ctx, "record", req, func(m *Memory) (string, error) {
		var err error
		out, err = m.Record(req, a)
		return entryIDOf(out), err
	})
	return out, err
}

func (h Held) Claim(ctx context.Context, req Request, act *Action) (*Receipt, error) {
	var out *Receipt
	err := h.mutate(ctx, "claim", req, func(m *Memory) (string, error) {
		var err error
		out, err = m.Claim(req, act)
		return actionIDOf(out), err
	})
	return out, err
}

func (h Held) Settle(ctx context.Context, req Request, rec *Receipt) (*Receipt, error) {
	var out *Receipt
	err := h.mutate(ctx, "settle", req, func(m *Memory) (string, error) {
		var err error
		out, err = m.Settle(req, rec)
		return actionIDOf(out), err
	})
	return out, err
}

func (h Held) Retire(ctx context.Context, req Request, entryID, reason string) (*Retirement, error) {
	var out *Retirement
	err := h.mutate(ctx, "retire", req, func(m *Memory) (string, error) {
		var err error
		out, err = m.Retire(req, entryID, reason)
		return entryID, err
	})
	return out, err
}

func (h Held) Reopen(ctx context.Context, req Request, entryID, condition string) (*Entry, error) {
	var out *Entry
	err := h.mutate(ctx, "reopen", req, func(m *Memory) (string, error) {
		var err error
		out, err = m.Reopen(req, entryID, condition)
		return entryID, err
	})
	return out, err
}

// Push pushes the notebook's branch to its origin remote. A repository with
// no origin, or with no commit yet, has nothing to push and says which.
//
// Pushes are serialized on an exclusive flock of their own, on the sibling
// Dir+".push.lock": two pushes reading the remote's ref at once would have the
// later refused by receive-pack's old-value check, leaving its commit only
// here. One that waited reads the branch after the push before it, so it
// carries every commit. The mutation lock is not taken, so a slow remote never
// holds a write; git reads the branch ref atomically, so a commit landing
// meanwhile is either in this push or in the next.
func (h Held) Push(ctx context.Context) (string, error) {
	if err := h.checkDir(); err != nil {
		return "", err
	}
	remotes, err := h.git(ctx, "remote")
	if err != nil {
		return "", err
	}
	if !slices.Contains(strings.Fields(remotes), heldRemote) {
		return HeldNoRemote, nil
	}
	lock, err := os.OpenFile(h.Dir+".push.lock", os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return "", fmt.Errorf("notebook %s: %w", h.Dir, err)
	}
	defer lock.Close()
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX); err != nil {
		return "", fmt.Errorf("notebook %s: push lock: %w", h.Dir, err)
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	head, err := h.git(ctx, "for-each-ref", "--count=1", "refs/heads/"+heldBranch)
	if err != nil {
		return "", err
	}
	if head == "" {
		return HeldNothingToPush, nil
	}
	// The owner is read from the branch being pushed rather than the working
	// tree, which a concurrent writer may be partway through rewriting.
	data, err := h.git(ctx, "show", "refs/heads/"+heldBranch+":"+heldRevisionFile)
	if err != nil {
		return "", err
	}
	if _, err := h.decodeRevision([]byte(data)); err != nil {
		return "", err
	}
	if _, err := h.git(ctx, "push", "--quiet", heldRemote, heldBranch); err != nil {
		return "", err
	}
	return HeldPushed, nil
}

func entryIDOf(e *Entry) string {
	if e == nil {
		return ""
	}
	return e.ID
}

func actionIDOf(r *Receipt) string {
	if r == nil {
		return ""
	}
	return r.ActionID
}

// mutate applies one operation through Memory and commits it when it was
// accepted. apply returns the id the commit subject names. A refusal is
// returned as Memory produced it, so callers match the same sentinels and
// fields against either notebook.
func (h Held) mutate(ctx context.Context, op string, req Request, apply func(*Memory) (string, error)) error {
	return h.locked(ctx, func() error {
		m, onDisk, err := h.load()
		if err != nil {
			return err
		}
		before := m.revision
		subject, err := apply(m)
		if err != nil || m.revision == before {
			// Refused, or a replay answered from the request log: nothing
			// changed, so nothing is written.
			return err
		}
		message := fmt.Sprintf("%s %s\n\nrequest %s, revision %d -> %d\n", op, subject, requestKey(req), before, m.revision)
		if err := h.commit(ctx, m, onDisk, message); err != nil {
			// The run's own context may be what failed the commit; the
			// rollback still has to run, or the next operation inherits it.
			return errors.Join(err, h.restore(context.WithoutCancel(ctx)))
		}
		return nil
	})
}

// locked runs f under the notebook's exclusive lock, with the repository
// initialised and any mutation a crashed writer left uncommitted rolled back.
func (h Held) locked(ctx context.Context, f func() error) error {
	if err := h.checkDir(); err != nil {
		return err
	}
	if err := os.MkdirAll(h.Dir, 0o700); err != nil {
		return fmt.Errorf("notebook %s: %w", h.Dir, err)
	}
	lock, err := os.OpenFile(h.Dir+".lock", os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return fmt.Errorf("notebook %s: %w", h.Dir, err)
	}
	defer lock.Close()
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX); err != nil {
		return fmt.Errorf("notebook %s: lock: %w", h.Dir, err)
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	if _, err := os.Stat(filepath.Join(h.Dir, ".git")); errors.Is(err, os.ErrNotExist) {
		if _, err := h.git(ctx, "init", "--quiet", "--initial-branch="+heldBranch); err != nil {
			return err
		}
	} else if err != nil {
		return fmt.Errorf("notebook %s: %w", h.Dir, err)
	}
	// A git add or commit killed at its timeout leaves index.lock behind, and
	// every later reset would refuse on it. Under this lock no other git
	// process touches the index (Push never does), so one seen here is stale.
	if err := os.Remove(filepath.Join(h.Dir, ".git", "index.lock")); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("notebook %s: %w", h.Dir, err)
	}
	// The pending marker outlives a writer that died between writing its
	// files and committing them. That mutation was never acknowledged, and
	// leaving it in the tree would sweep it into the next one's commit.
	if _, err := os.Stat(h.pending()); err == nil {
		if err := h.restore(ctx); err != nil {
			return err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("notebook %s: %w", h.Dir, err)
	}
	return f()
}

func (h Held) checkDir() error {
	if !filepath.IsAbs(h.Dir) || filepath.Clean(h.Dir) != h.Dir {
		return fmt.Errorf("notebook directory %q must be an absolute clean path", h.Dir)
	}
	if h.Identity.Domain == "" || h.Identity.Observer == "" {
		return fmt.Errorf("notebook %s: an owning identity is required", h.Dir)
	}
	return nil
}

// decodeRevision decodes notebook.json and refuses it unless it was recorded
// for this notebook's identity.
func (h Held) decodeRevision(data []byte) (heldRevision, error) {
	var revision heldRevision
	if err := json.Unmarshal(data, &revision); err != nil {
		return heldRevision{}, fmt.Errorf("notebook %s: %s: %w", h.Dir, heldRevisionFile, err)
	}
	if revision.Owner != h.Identity {
		return heldRevision{}, fmt.Errorf("notebook %s belongs to %s/%s, not %s/%s",
			h.Dir, revision.Owner.Domain, revision.Owner.Observer, h.Identity.Domain, h.Identity.Observer)
	}
	return revision, nil
}

func (h Held) pending() string { return h.Dir + ".pending" }

// load reads the repository's working tree into a Memory, and returns the
// bytes of every file it read keyed by repository-relative path so a commit
// rewrites only what changed.
func (h Held) load() (*Memory, map[string][]byte, error) {
	m := NewMemory()
	onDisk := map[string][]byte{}
	data, err := os.ReadFile(filepath.Join(h.Dir, heldRevisionFile))
	if errors.Is(err, os.ErrNotExist) {
		return m, onDisk, nil
	}
	if err != nil {
		return nil, nil, fmt.Errorf("notebook %s: %w", h.Dir, err)
	}
	revision, err := h.decodeRevision(data)
	if err != nil {
		return nil, nil, err
	}
	m.revision = revision.Revision
	onDisk[heldRevisionFile] = data

	if err := h.readDir(heldEntriesDir, onDisk, func(data []byte) error {
		var file heldEntry
		if err := json.Unmarshal(data, &file); err != nil {
			return err
		}
		if file.Entry == nil {
			return errors.New("entry: missing")
		}
		m.entries[file.Entry.ID] = file.Entry
		for _, held := range file.Actions {
			if held.Action == nil || held.Receipt == nil {
				return errors.New("actions: an action and its receipt are both required")
			}
			m.actions[held.Action.ID] = held.Action
			m.receipts[held.Action.ID] = held.Receipt
		}
		return nil
	}); err != nil {
		return nil, nil, err
	}
	if err := h.readDir(heldRequestsDir, onDisk, func(data []byte) error {
		var file heldRequest
		if err := json.Unmarshal(data, &file); err != nil {
			return err
		}
		m.requests[file.Identity.Qualify(file.RequestID)] = &applied{
			fingerprint: file.Fingerprint, entry: file.Entry,
			receipt: file.Receipt, retirement: file.Retirement,
		}
		return nil
	}); err != nil {
		return nil, nil, err
	}
	return m, onDisk, nil
}

func (h Held) readDir(dir string, onDisk map[string][]byte, decode func(data []byte) error) error {
	names, err := os.ReadDir(filepath.Join(h.Dir, dir))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("notebook %s: %w", h.Dir, err)
	}
	for _, name := range names {
		// Only names heldName produces are the notebook's; anything else, such
		// as a .DS_Store, is not read.
		if name.IsDir() || !isHeldName(name.Name()) {
			continue
		}
		rel := filepath.Join(dir, name.Name())
		data, err := os.ReadFile(filepath.Join(h.Dir, rel))
		if err != nil {
			return fmt.Errorf("notebook %s: %w", h.Dir, err)
		}
		if err := decode(data); err != nil {
			return fmt.Errorf("notebook %s: %s: %w", h.Dir, rel, err)
		}
		onDisk[rel] = data
	}
	return nil
}

// heldName is the file name an id is stored under. Ids are qualified with
// "/" and are the observer's own strings, and the volume may fold case, so a
// name derived from the id's text could traverse or collide; its digest can
// do neither. The id itself is inside the file and in the commit subject.
func heldName(id string) string {
	sum := sha256.Sum256([]byte(id))
	return hex.EncodeToString(sum[:]) + ".json"
}

func isHeldName(name string) bool {
	digest, ok := strings.CutSuffix(name, ".json")
	if !ok || len(digest) != hex.EncodedLen(sha256.Size) {
		return false
	}
	for _, c := range digest {
		if !('0' <= c && c <= '9' || 'a' <= c && c <= 'f') {
			return false
		}
	}
	return true
}

// commit writes every file the accepted mutation changed and commits them as
// one commit. Request files are immutable once written, so only the new one
// is rendered; entries are compared against what was read.
func (h Held) commit(ctx context.Context, m *Memory, onDisk map[string][]byte, message string) error {
	files := map[string]any{heldRevisionFile: heldRevision{Revision: m.revision, Owner: h.Identity}}
	byEntry := map[string][]heldAction{}
	for id, act := range m.actions {
		byEntry[act.QuestionID] = append(byEntry[act.QuestionID], heldAction{Action: act, Receipt: m.receipts[id]})
	}
	for id, e := range m.entries {
		actions := byEntry[id]
		slices.SortFunc(actions, func(a, b heldAction) int { return strings.Compare(a.Action.ID, b.Action.ID) })
		if actions == nil {
			actions = []heldAction{}
		}
		files[filepath.Join(heldEntriesDir, heldName(id))] = heldEntry{Entry: e, Actions: actions}
	}
	for key, out := range m.requests {
		rel := filepath.Join(heldRequestsDir, heldName(key))
		if _, ok := onDisk[rel]; ok {
			continue
		}
		identity, requestID := splitRequestKey(key)
		files[rel] = heldRequest{
			Identity: identity, RequestID: requestID, Fingerprint: out.fingerprint,
			Entry: out.entry, Receipt: out.receipt, Retirement: out.retirement,
		}
	}

	if err := os.WriteFile(h.pending(), nil, 0o600); err != nil {
		return fmt.Errorf("notebook %s: %w", h.Dir, err)
	}
	for rel, value := range files {
		data, err := json.MarshalIndent(value, "", "  ")
		if err != nil {
			return fmt.Errorf("notebook %s: %s: %w", h.Dir, rel, err)
		}
		data = append(data, '\n')
		if bytes.Equal(onDisk[rel], data) {
			continue
		}
		path := filepath.Join(h.Dir, rel)
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			return fmt.Errorf("notebook %s: %w", h.Dir, err)
		}
		if err := os.WriteFile(path, data, 0o600); err != nil {
			return fmt.Errorf("notebook %s: %w", h.Dir, err)
		}
	}
	if _, err := h.git(ctx, "add", "--all", "--", "."); err != nil {
		return err
	}
	if _, err := h.git(ctx, "commit", "--quiet", "-m", message); err != nil {
		return err
	}
	if err := os.Remove(h.pending()); err != nil {
		return fmt.Errorf("notebook %s: %w", h.Dir, err)
	}
	return nil
}

// splitRequestKey inverts requestKey. The domain and observer cannot contain
// the separator, so the first two cuts are theirs and the rest is the
// caller's request id, separators and all.
func splitRequestKey(key string) (Identity, string) {
	domain, rest, _ := strings.Cut(key, idSeparator)
	observer, requestID, _ := strings.Cut(rest, idSeparator)
	return Identity{Domain: domain, Observer: observer}, requestID
}

// restore returns the working tree and index to the last commit, or to empty
// before the first, and clears the pending marker.
func (h Held) restore(ctx context.Context) error {
	if head, err := h.git(ctx, "for-each-ref", "--count=1", "refs/heads/"+heldBranch); err != nil {
		return err
	} else if head != "" {
		if _, err := h.git(ctx, "reset", "--quiet", "--hard", "HEAD"); err != nil {
			return err
		}
	} else if _, err := h.git(ctx, "read-tree", "--empty"); err != nil {
		return err
	}
	if _, err := h.git(ctx, "clean", "--quiet", "-d", "--force"); err != nil {
		return err
	}
	if err := os.Remove(h.pending()); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("notebook %s: %w", h.Dir, err)
	}
	return nil
}

// git runs one git command against the notebook. A failure names the
// notebook, the command and what git said, bounded.
func (h Held) git(ctx context.Context, args ...string) (string, error) {
	bound, cancel := context.WithTimeout(ctx, heldGitTimeout)
	defer cancel()
	cmd := exec.CommandContext(bound, "git", append([]string{
		"-C", h.Dir, "-c", "user.name=" + heldCommitName, "-c", "user.email=" + heldCommitEmail,
	}, args...)...)
	// No prompt can be answered from a daemon: a push that needs credentials
	// fails rather than waiting out the timeout.
	cmd.Env = append(gitenv.Env(), "GIT_TERMINAL_PROMPT=0")
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &limitedBuffer{Buffer: &stderr, N: maxCommandStderrBytes}
	if err := cmd.Run(); err != nil {
		if bound.Err() != nil {
			err = bound.Err()
		}
		// git commit reports why it refused on stdout, not stderr.
		detail := strings.TrimSpace(stderr.String())
		if detail == "" {
			detail = strings.TrimSpace(stdout.String())
			if len(detail) > maxCommandStderrBytes {
				detail = detail[:maxCommandStderrBytes]
			}
		}
		if detail != "" {
			return "", fmt.Errorf("notebook %s: git %s: %w: %s", h.Dir, args[0], err, detail)
		}
		return "", fmt.Errorf("notebook %s: git %s: %w", h.Dir, args[0], err)
	}
	return strings.TrimSpace(stdout.String()), nil
}
