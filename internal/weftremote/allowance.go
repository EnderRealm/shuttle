package weftremote

import (
	"fmt"
	"sync"
	"time"

	"shuttle/internal/ledger"
)

// codexProbeInterval is how long after the last Codex probe a dashboard read
// starts another (weft-service/v2 §5.3).
const codexProbeInterval = 300 * time.Second

// The outcomes of the Codex probe: worker-allowance-probe.json's, and failed
// for a probe that ran out of its bounds or printed no usable document.
const (
	probeObserved = "observed"
	probeNoSource = "no_source"
	probeTimedOut = "timed_out"
	probeFailed   = "failed"
)

// codexProbe is the Codex allowance probe's schedule and last outcome. The
// observation itself is durable in the ledger; this is only why there is
// none to show.
type codexProbe struct {
	mu       sync.Mutex
	running  bool
	started  time.Time
	outcome  string
	detail   string
	finished time.Time
}

// probeCodex starts `weft-worker allowance-probe` for Codex unless one is
// running or, unless forced, one started within codexProbeInterval. It never
// waits on the probe: a dashboard read answers from what is held.
func (s *Service) probeCodex(force bool) {
	if !s.segments.worker.Load().speaks(WorkerContractV2) {
		return
	}
	p := &s.codex
	p.mu.Lock()
	if p.running || (!force && time.Since(p.started) <= codexProbeInterval) {
		p.mu.Unlock()
		return
	}
	p.running, p.started = true, time.Now()
	p.mu.Unlock()
	s.probes.Add(1)
	go func() {
		defer s.probes.Done()
		s.runCodexProbe()
	}()
}

// runCodexProbe runs the probe on this host under runProbe's bounds and keeps
// an observation newer than the one held. What it logs is the outcome class
// and a bounded failure, never what the probe printed.
func (s *Service) runCodexProbe() {
	out, failure := runProbe(s.cfg.Weft.WorkerBin,
		[]string{"allowance-probe", "--contract", WorkerContractV2, "--provider", "codex"}, providerProbeTimeout)
	outcome, detail := probeFailed, failure
	if failure == "" {
		var observation *ledger.WeftAllowance
		outcome, observation, detail = parseCodexProbe(out)
		if observation != nil {
			s.stateMu.Lock()
			_, err := s.store.RecordWeftAllowance(*observation)
			s.stateMu.Unlock()
			if err != nil {
				outcome, detail = probeFailed, "the observation could not be recorded: "+err.Error()
			}
		}
	}
	p := &s.codex
	p.mu.Lock()
	p.running, p.outcome, p.detail, p.finished = false, outcome, detail, time.Now()
	p.mu.Unlock()
	line := fmt.Sprintf("weft allowance host=%s provider=codex: probe %s", s.hostID, outcome)
	if detail != "" {
		line += ": " + detail
	}
	s.log.Print(line)
}

// parseCodexProbe reads worker-allowance-probe.json strictly. Only an
// observed outcome carries a time and windows; any other with a figure is
// refused, since an unobserved allowance with a number is a claim nobody made.
func parseCodexProbe(doc []byte) (string, *ledger.WeftAllowance, string) {
	var p struct {
		ContractVersion string            `json:"contract_version"`
		Event           string            `json:"event"`
		Provider        string            `json:"provider"`
		Source          string            `json:"source"`
		Outcome         string            `json:"outcome"`
		ObservedAt      *string           `json:"observed_at"`
		Windows         []allowanceWindow `json:"windows"`
	}
	if failure := decodeWorkerDoc(doc, &p, "contract_version", "event", "provider", "source", "outcome",
		"observed_at", "windows"); failure != "" {
		return probeFailed, nil, "probe document: " + failure
	}
	if p.ContractVersion != WorkerContractV2 || p.Event != "allowance_probe" || p.Provider != "codex" ||
		p.Source != "codex_app_server" || !oneOf(p.Outcome, probeObserved, probeNoSource, probeTimedOut) {
		return probeFailed, nil, "probe document: not a Codex allowance_probe event of " + WorkerContractV2
	}
	if p.Outcome != probeObserved {
		if p.ObservedAt != nil || len(p.Windows) > 0 {
			return probeFailed, nil, "probe document: an unobserved outcome carries a figure"
		}
		return p.Outcome, nil, ""
	}
	windows, err := allowanceWindows(p.Windows)
	if p.ObservedAt == nil || err != nil {
		return probeFailed, nil, "probe document: an observation lacks its time or a well-formed window"
	}
	if _, err := time.Parse(time.RFC3339Nano, *p.ObservedAt); err != nil {
		return probeFailed, nil, "probe document: observed_at is not a timestamp"
	}
	return probeObserved, &ledger.WeftAllowance{
		Provider: p.Provider, Source: p.Source, ObservedAt: *p.ObservedAt, Windows: windows,
	}, ""
}

// allowanceTiles are the dashboard's three tiles (§5.3). A held observation
// is shown with its own time, however old; without one a tile is unknown or
// unavailable with its reason, and never carries a figure.
func (s *Service) allowanceTiles(held map[string]ledger.WeftAllowance) []map[string]any {
	return []map[string]any{
		s.claudeTile(held),
		s.codexTile(held),
		unavailableTile("cursor", "no source: no credential decision has been made for Cursor on this host"),
	}
}

func (s *Service) claudeTile(held map[string]ledger.WeftAllowance) map[string]any {
	if a, ok := held["claude"]; ok {
		return a.Tile()
	}
	worker := s.segments.worker.Load()
	switch {
	case worker == nil:
		return unknownTile("claude", "claude_stream", "the worker has not been probed since the host started at "+
			formatTime(s.started)+", so whether it relays Claude rate_limit_event observations is not known")
	case worker.Failure != "":
		return unknownTile("claude", "claude_stream", "the probe of "+s.cfg.Weft.WorkerBin+" failed: "+worker.Failure+
			", so whether the worker relays Claude rate_limit_event observations is not known")
	case !worker.has("allowance"):
		// §2.3: a worker without allowance leaves the tile unknown, not
		// unavailable, with that reason.
		return unknownTile("claude", "claude_stream", "the installed worker does not relay Claude rate_limit_event "+
			"observations: it does not list the allowance capability of "+WorkerContractV2)
	}
	return unknownTile("claude", "claude_stream", "no Claude stage on this host has reported a rate_limit_event")
}

func (s *Service) codexTile(held map[string]ledger.WeftAllowance) map[string]any {
	if a, ok := held["codex"]; ok {
		return a.Tile()
	}
	if !s.segments.worker.Load().speaks(WorkerContractV2) {
		return unavailableTile("codex", "no source: the installed worker does not speak "+WorkerContractV2+
			", whose allowance-probe reads the codex app-server's rate limits")
	}
	p := &s.codex
	p.mu.Lock()
	outcome, detail, finished := p.outcome, p.detail, formatTime(p.finished)
	p.mu.Unlock()
	switch outcome {
	case probeNoSource:
		return unavailableTile("codex", "no source: the codex app-server reported no rate-limit source at "+finished)
	case probeTimedOut:
		return unknownTile("codex", "codex_app_server", "the codex app-server reported no rate limits within the probe at "+finished)
	case probeFailed:
		return unknownTile("codex", "codex_app_server", "the Codex allowance probe failed at "+finished+": "+detail)
	}
	return unknownTile("codex", "codex_app_server", "the codex app-server has reported no rate limits since the host started at "+
		formatTime(s.started))
}

func unknownTile(provider, source, reason string) map[string]any {
	return map[string]any{
		"provider": provider, "state": "unknown", "source": source, "observed_at": nil,
		"windows": []any{}, "reason": reason,
	}
}
