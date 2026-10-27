package weftremote

import (
	"encoding/json"
	"fmt"
	"net/http"
	"path/filepath"
	"regexp"
	"strings"

	"shuttle/internal/ledger"
)

// maxArchiveBytes bounds one legacy archive: a Mac's whole local history,
// summaries included, in one document.
const maxArchiveBytes = 64 << 20

var (
	archiveIDPattern   = regexp.MustCompile(`^ar-\d{8}T\d{6}Z-[0-9a-f]{8}$`)
	legacyRunPattern   = regexp.MustCompile(`^\d{8}T\d{6}Z-[0-9a-f]{8}$`)
	ticketPattern      = regexp.MustCompile(`^[a-z0-9][a-z0-9-]*/[a-z0-9][a-z0-9-]*$`)
	timestampPattern   = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$`)
	archiveBasePattern = regexp.MustCompile(`^([0-9a-f]{40})?$`)
	commitPattern      = regexp.MustCompile(`^[0-9a-f]{40}$`)
)

type archiveOrigin struct {
	HostName string `json:"host_name"`
	Machine  string `json:"machine"`
	Producer string `json:"producer"`
	WeftRoot string `json:"weft_root"`
	Kind     string `json:"kind"`
}

type archiveDocument struct {
	ContractVersion string        `json:"contract_version"`
	ArchiveID       string        `json:"archive_id"`
	ExportedAt      string        `json:"exported_at"`
	Origin          archiveOrigin `json:"origin"`
	Source          struct {
		RunsJSONLSHA256 string `json:"runs_jsonl_sha256"`
		Lines           int    `json:"lines"`
		SkippedLines    int    `json:"skipped_lines"`
		Summaries       int    `json:"summaries"`
	} `json:"source"`
	Runs []archiveRun `json:"runs"`
}

type archiveRun struct {
	LegacyRunID string  `json:"legacy_run_id"`
	Ticket      string  `json:"ticket"`
	Project     string  `json:"project"`
	Repo        *string `json:"repo"`
	Branch      string  `json:"branch"`
	Worktree    string  `json:"worktree"`
	Base        string  `json:"base"`
	StartedAt   string  `json:"started_at"`
	EndedAt     *string `json:"ended_at"`
	Outcome     *string `json:"outcome"`
	Interrupted bool    `json:"interrupted"`
	Resumed     *string `json:"resumed"`
	Halt        *halt   `json:"halt"`
	Land        *struct {
		Landed bool    `json:"landed"`
		Branch *string `json:"branch"`
		Commit *string `json:"commit"`
		Detail *string `json:"detail"`
	} `json:"land"`
	Stages []struct {
		stageFields
		At string `json:"at"`
	} `json:"stages"`
	Summary json.RawMessage `json:"summary"`
}

// stageFields are a stage row's fields as a legacy run recorded them.
type stageFields struct {
	Stage        string          `json:"stage"`
	Backend      *string         `json:"backend"`
	Occurrence   int             `json:"occurrence"`
	Attempt      int             `json:"attempt"`
	State        string          `json:"state"`
	Session      *string         `json:"session"`
	Detail       *string         `json:"detail"`
	ExitCode     *int64          `json:"exit_code"`
	Turns        *int64          `json:"turns"`
	Spend        json.RawMessage `json:"spend"`
	ProcessingMS *int64          `json:"processing_ms"`
	Tokens       json.RawMessage `json:"tokens"`
}

// decodeArchive reads a weft-archive/v1 document (§14.3): a version this
// host does not import is refused before anything else is read, and every
// run is held to the shapes it is imported under. A refusal names the field,
// never what the document put there.
func decodeArchive(body []byte) (*ledger.WeftArchive, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(body, &fields); err != nil || fields == nil {
		return nil, &ledger.WeftRefusal{Code: "malformed", Message: "the archive is not a JSON object"}
	}
	var version string
	if raw, ok := fields["contract_version"]; !ok || json.Unmarshal(raw, &version) != nil {
		return nil, &ledger.WeftRefusal{Code: "malformed", Message: "the archive has no contract_version string"}
	}
	if version != ledger.WeftArchiveVersion {
		return nil, &ledger.WeftRefusal{Code: "upgrade_required", Message: fmt.Sprintf(
			"%q is not an archive version this host imports: it imports %s", version, ledger.WeftArchiveVersion)}
	}
	for _, key := range []string{"archive_id", "exported_at", "origin", "source", "runs"} {
		if _, ok := fields[key]; !ok {
			return nil, &ledger.WeftRefusal{Code: "malformed", Message: "the archive has no " + key}
		}
	}
	var doc archiveDocument
	if err := decodeEvent(body, &doc, "archive"); err != nil {
		return nil, &ledger.WeftRefusal{Code: "malformed", Message: err.Error()}
	}
	malformed := func(format string, args ...any) error {
		return &ledger.WeftRefusal{Code: "malformed", Message: fmt.Sprintf(format, args...)}
	}
	o := doc.Origin
	switch {
	case !archiveIDPattern.MatchString(doc.ArchiveID):
		return nil, malformed("archive_id is not an ar- id")
	case !timestampPattern.MatchString(doc.ExportedAt):
		return nil, malformed("exported_at is not a timestamp")
	case o.HostName == "" || o.Machine == "" || o.Producer == "" || !filepath.IsAbs(o.WeftRoot) || o.Kind != "app":
		return nil, malformed("origin names no host, machine, producer, absolute weft_root and kind app")
	}
	origin, err := json.Marshal(o)
	if err != nil {
		return nil, err
	}
	out := &ledger.WeftArchive{ArchiveID: doc.ArchiveID, Origin: origin}
	seen := map[string]bool{}
	for i, r := range doc.Runs {
		at := fmt.Sprintf("runs[%d]", i)
		project, _, _ := strings.Cut(r.Ticket, "/")
		switch {
		case !legacyRunPattern.MatchString(r.LegacyRunID) || seen[r.LegacyRunID]:
			return nil, malformed("%s.legacy_run_id is not a weft run id, or repeats one", at)
		case !ticketPattern.MatchString(r.Ticket) || project != r.Project:
			return nil, malformed("%s.ticket is not a qualified ticket of its project", at)
		case (r.Repo != nil && !filepath.IsAbs(*r.Repo)) || !filepath.IsAbs(r.Worktree) || r.Branch == "" ||
			!archiveBasePattern.MatchString(r.Base):
			return nil, malformed("%s names no absolute worktree, branch and base, or a repo that is not absolute", at)
		case !timestampPattern.MatchString(r.StartedAt) || (r.EndedAt != nil && !timestampPattern.MatchString(*r.EndedAt)):
			return nil, malformed("%s.started_at or ended_at is not a timestamp", at)
		case r.Outcome != nil && !oneOf(*r.Outcome, "finished", "halted", "failed", "stopped"):
			return nil, malformed("%s.outcome is not finished, halted, failed or stopped", at)
		case r.Interrupted != (r.Outcome == nil):
			return nil, malformed("%s.interrupted is not exactly a run with no outcome", at)
		case r.Land != nil && r.Land.Commit != nil && !commitPattern.MatchString(*r.Land.Commit):
			return nil, malformed("%s.land.commit is not a commit", at)
		case len(r.Summary) > 0 && string(r.Summary) != "null" && r.Summary[0] != '{':
			return nil, malformed("%s.summary is not an object", at)
		}
		seen[r.LegacyRunID] = true
		run := ledger.WeftArchiveRun{
			LegacyRunID: r.LegacyRunID, Ticket: r.Ticket, Project: r.Project, Repo: text(r.Repo), Branch: r.Branch,
			Worktree: r.Worktree, Base: r.Base, StartedAt: r.StartedAt, EndedAt: nullSQL(r.EndedAt),
			Outcome: nullSQL(r.Outcome), Summary: r.Summary,
		}
		if h := r.Halt; h != nil {
			run.HaltStage, run.HaltReason, run.HaltDetail = nullSQL(&h.Stage), nullSQL(&h.Reason), nullSQL(&h.Detail)
		}
		if l := r.Land; l != nil {
			run.Land = &ledger.WeftArchiveLand{Landed: l.Landed, Branch: nullSQL(l.Branch), Commit: nullSQL(l.Commit), Detail: nullSQL(l.Detail)}
		}
		for j, st := range r.Stages {
			if st.Stage == "" || st.Occurrence < 1 || st.Attempt < 1 || !timestampPattern.MatchString(st.At) ||
				!oneOf(st.State, "running", "done", "failed", "needs_input", "stopped") {
				return nil, malformed("%s.stages[%d] is not a stage row", at, j)
			}
			run.Stages = append(run.Stages, ledger.WeftStage{
				Stage: st.Stage, Occurrence: st.Occurrence, Attempt: st.Attempt, Backend: nullSQL(st.Backend),
				State: st.State, Session: nullSQL(st.Session), Detail: nullSQL(st.Detail), ExitCode: nullInt64(st.ExitCode),
				Turns: nullInt64(st.Turns), Spend: st.Spend, Processing: nullInt64(st.ProcessingMS), Tokens: st.Tokens, At: st.At,
			})
		}
		out.Runs = append(out.Runs, run)
	}
	return out, nil
}

// handleArchiveImport imports a legacy archive (§14.3): idempotent by
// archive id and content, so a second import of the same document changes
// nothing and answers what the first did. A legacy run that recorded no repo
// is bound to its project's repository on this host.
func (s *Service) handleArchiveImport(w http.ResponseWriter, r *http.Request) {
	s.mutateV2Limit(w, r, maxArchiveBytes, func(body []byte, _ string) (v2Commit, error) {
		doc, err := decodeArchive(body)
		if err != nil {
			return refusedBy(err), nil
		}
		c, err := s.readCatalog()
		if err != nil {
			return nil, err
		}
		for i := range doc.Runs {
			run := &doc.Runs[i]
			if run.Repo != "" {
				continue
			}
			p := c.byName[run.Project]
			if p == nil || p.Repository == "" {
				return refused("project_not_found", "%s records no repo and its project %s binds no repository on host %s",
					ledger.WeftArchivedRunID(run.LegacyRunID), run.Project, s.hostID), nil
			}
			run.Repo = p.Repository
		}
		sha := ledger.BodySHA(body)
		return func(tx *ledger.WeftTx) (v2Outcome, error) {
			imp, err := tx.ImportArchive(doc, sha)
			if err != nil {
				return v2Outcome{}, err
			}
			return v2Outcome{status: http.StatusCreated, fields: map[string]any{"import": imp.Resource()}}, nil
		}, nil
	})
}
