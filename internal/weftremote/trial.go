package weftremote

import (
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"

	"github.com/EnderRealm/ticket/v8/pkg/ticket"

	"shuttle/internal/ledger"
	"shuttle/internal/provider/tkconfig"
	"shuttle/internal/sandbox"
)

// folderRef is a directory as a request names one: by folder handle (§9.2),
// never by path.
type folderRef struct {
	Handle string `json:"handle"`
}

// launchEnvironment is a trial launch's per-launch environment (§11.7).
type launchEnvironment struct {
	TKStoreRoot     *folderRef `json:"tk_store_root"`
	WorkEvidenceDir *folderRef `json:"work_evidence_dir"`
}

// trialCommit admits a trial launch (§11.7): a checkout no project is bound
// to, its ticket read from the tk store the launch names rather than the host
// catalog, and each directory canonicalized from its handle. The entry
// reserves the repository the checkout's canonical path derives, exactly as
// a registered repository's.
func (s *Service) trialCommit(ticketID, project string, checkout *folderRef, env *launchEnvironment, key string) (v2Commit, error) {
	var trial ledger.WeftTrial
	var err error
	if trial.Checkout, err = s.folders.resolve(checkout.Handle); err != nil {
		return refusedBy(err), nil
	}
	if trial.TKStoreRoot, err = s.folders.resolve(env.TKStoreRoot.Handle); err != nil {
		return refusedBy(err), nil
	}
	if env.WorkEvidenceDir != nil {
		if trial.WorkEvidenceDir, err = s.folders.resolve(env.WorkEvidenceDir.Handle); err != nil {
			return refusedBy(err), nil
		}
	}
	c, err := s.readCatalog()
	if err != nil {
		return nil, err
	}
	if _, repository, bound := repositoryAt(c, trial.Checkout); bound != "" {
		return refused("handle_invalid", "%s is in %s, project %s's checkout; a checkout launch names a checkout no project is bound to, and %s's tickets launch by repository_id",
			trial.Checkout, repository, bound, bound), nil
	}
	if reason := unregisteredCheckout(trial.Checkout); reason != "" {
		return refused("handle_invalid", "%s", reason), nil
	}
	snap, err := readTrialStore(trial.TKStoreRoot)
	if err != nil {
		return nil, err
	}
	t, ok := snap.Get(ticketID)
	if !ok {
		return refused("ticket_not_found", "%s was not found in the tk store at %s on host %s", ticketID, trial.TKStoreRoot, s.hostID), nil
	}
	if reasons := tkIneligibility(snap, t); len(reasons) > 0 {
		return refused("ticket_not_eligible", "%s", describeIneligible(ticketID, reasons)), nil
	}
	return func(tx *ledger.WeftTx) (v2Outcome, error) {
		a := s.admission(key)
		a.Kind, a.Ticket, a.Project, a.Trial = ledger.WeftEntryLaunch, t.ID, project, trial
		a.RepositoryID, a.Repository = sandbox.RepositoryID(trial.Checkout), trial.Checkout
		entry, err := tx.Admit(a)
		if err != nil {
			return v2Outcome{}, err
		}
		if err := tx.Dispatch(s.dispatchConfig(c)); err != nil {
			return v2Outcome{}, err
		}
		if entry, err = tx.Entry(entry.ID); err != nil {
			return v2Outcome{}, err
		}
		return v2Outcome{status: http.StatusAccepted, fields: map[string]any{"entry": entry.Resource()}}, nil
	}, nil
}

// unregisteredCheckout is why the canonical directory is not a checkout a
// trial may run in, or "" when it is: as a binding is judged (bindProject),
// the directory and its .git are this host's own, never a link an agent made.
func unregisteredCheckout(canonical string) string {
	info, err := os.Lstat(canonical)
	if err != nil {
		return fmt.Sprintf("%s cannot be inspected: %v", canonical, err)
	}
	if !ownedByDaemon(info) {
		return fmt.Sprintf("%s is not owned by uid %d, which this host runs as; an agent may have made it", canonical, os.Getuid())
	}
	git, err := os.Lstat(filepath.Join(canonical, ".git"))
	if err != nil {
		return fmt.Sprintf("%s has no .git: it is not a git checkout", canonical)
	}
	if git.Mode()&fs.ModeSymlink != 0 || !ownedByDaemon(git) {
		return fmt.Sprintf("%s/.git is a symlink or not owned by uid %d, which this host runs as; an agent may have made it", canonical, os.Getuid())
	}
	return ""
}

// readTrialStore is a complete read of the tk store at root, laid out as
// TK_STORE_ROOT lays one out: <root>/tickets/<project>/.
func readTrialStore(root string) (*ticket.Snapshot, error) {
	snap, err := ticket.NewMultiStore(tkconfig.Config{CentralRoot: root}.TicketsRoot()).Snapshot()
	if err != nil {
		return nil, err
	}
	if problems := snapshotProblems(snap); len(problems) > 0 {
		return nil, &incompleteReadError{diagnostics: problems}
	}
	return snap, nil
}

// trialPathMoved is how a trial path, recorded canonical at admission, no
// longer names that directory — gone, or resolving elsewhere through a link —
// or "" while it still canonicalizes to itself.
func trialPathMoved(path string) string {
	canonical, _, err := canonicalDir(path)
	switch {
	case err != nil:
		return fmt.Sprintf("is no longer a directory here: %v", err)
	case canonical != path:
		return "now resolves to " + canonical
	}
	return ""
}

// trialStores are the trial stores one pass has read, by root, so a store is
// read once however many of its entries wait or runs are evaluated.
type trialStores map[string]trialRead

type trialRead struct {
	snap *ticket.Snapshot
	err  error
}

func (m trialStores) read(root string) trialRead {
	read, ok := m[root]
	if !ok {
		read.snap, read.err = readTrialStore(root)
		m[root] = read
	}
	return read
}

// finished is catalog.finished for a trial run's ticket (§6.8, §11.7): done
// or closed in the store at root, the path its run was admitted against. A
// store that no longer canonicalizes to that path is another store, and one
// that cannot be read, or a ticket not in it, is no trigger.
func (m trialStores) finished(root, id string) bool {
	if trialPathMoved(root) != "" {
		return false
	}
	read := m.read(root)
	if read.err != nil {
		return false
	}
	t, ok := read.snap.Get(id)
	return ok && (t.Status == ticket.StatusDone || t.Status == ticket.StatusClosed)
}

// check is a trial entry's revalidation (§11.7, §6.3): each directory its
// segment is launched with still canonicalizes to the path recorded, and its
// ticket still eligible in its own store. A cleanup is launched with the
// checkout alone and needs no ticket, so the store and evidence directory a
// harness may already have removed do not hold its worktree. A store that
// cannot be read is no ticket to run.
func (m trialStores) check(e *ledger.WeftEntry) (string, string) {
	paths := []string{e.Trial.Checkout, e.Trial.TKStoreRoot, e.Trial.WorkEvidenceDir}
	if e.Kind == ledger.WeftEntryCleanup {
		paths = paths[:1]
	}
	for _, path := range paths {
		if path == "" {
			continue
		}
		if moved := trialPathMoved(path); moved != "" {
			return ledger.WeftInvalidBinding, fmt.Sprintf("%s, which %s was admitted for, %s", path, e.ID, moved)
		}
	}
	if e.Kind == ledger.WeftEntryCleanup {
		return "", ""
	}
	root := e.Trial.TKStoreRoot
	read := m.read(root)
	if read.err != nil {
		return ledger.WeftInvalidTicket, fmt.Sprintf("the tk store at %s cannot be read: %v", root, read.err)
	}
	t, ok := read.snap.Get(e.Ticket)
	if !ok {
		return ledger.WeftInvalidTicket, e.Ticket + " is no longer in the tk store at " + root
	}
	if reasons := tkIneligibility(read.snap, t); len(reasons) > 0 {
		return ledger.WeftInvalidTicket, describeIneligible(e.Ticket, reasons)
	}
	return "", ""
}
