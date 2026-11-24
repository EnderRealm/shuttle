package cmd

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"shuttle/internal/config"
	"shuttle/internal/launchd"
	"shuttle/internal/ledger"
	"shuttle/internal/schedule"
	"shuttle/internal/weftremote"
)

// statusProcess reads the daemon's launchd process; a var so a test never
// reaches the real launchd.
var statusProcess = launchd.ProcessFor

var statusJSON bool

var statusCmd = &cobra.Command{
	Use:   "status",
	Short: "Daemon, agents, next slot, last outcome, streak, paused, open incidents, today's spend",
	// The ledger errors name the agent and the row; cobra printing them a
	// second time, with usage, would bury them.
	SilenceUsage:  true,
	SilenceErrors: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		report := &statusReport{}
		defer report.close()
		var fatal error
		for _, section := range statusSections {
			err := section.read(report)
			if !statusJSON {
				section.print(report)
				if err != nil {
					return err
				}
			}
			// The object carries every reading that can be taken, so a failure
			// the rows stop at is held for the exit status.
			if fatal == nil {
				fatal = err
			}
		}
		if statusJSON {
			data, err := json.MarshalIndent(report, "", "  ")
			if err != nil {
				return err
			}
			fmt.Println(string(data))
		}
		if fatal != nil {
			return fatal
		}
		if report.failed {
			return errors.New("one or more rows could not be listed")
		}
		return nil
	},
}

func init() {
	statusCmd.Flags().BoolVar(&statusJSON, "json", false, "Print one JSON object rather than rows")
	rootCmd.AddCommand(statusCmd)
}

// statusSections are the readings in the order their rows print. read returns
// the failure that ends the rows; one that costs only its own row is recorded
// in the report. Each section's rows print before the next is read.
var statusSections = []struct {
	read  func(*statusReport) error
	print func(*statusReport)
}{
	// First and before any load: a config.json the daemon cannot start on
	// is one status cannot list agents from either, and the daemon row is
	// what says so.
	{(*statusReport).readDaemon, (*statusReport).printDaemon},
	{(*statusReport).readAgents, (*statusReport).printAgents},
	{(*statusReport).readWeft, (*statusReport).printWeft},
	{(*statusReport).readSpend, (*statusReport).printSpend},
	{(*statusReport).readIncidents, (*statusReport).printIncidents},
}

// statusReport is the readings, and the object --json prints. Strings are raw:
// encoding/json escapes control characters, and the rows escape as they print.
// A reading that could not be taken is null beside the error that says why.
type statusReport struct {
	Daemon             daemonStatus     `json:"daemon"`
	Agents             []agentStatus    `json:"agents"`
	AgentsError        readError        `json:"agents_error"`
	WeftWorker         weftStatus       `json:"weft_worker"`
	SpendTodayUSD      nullFloat        `json:"spend_today_usd"`
	SpendTodayError    readError        `json:"spend_today_error"`
	OpenIncidents      []incidentStatus `json:"open_incidents"`
	OpenIncidentsError readError        `json:"open_incidents_error"`

	store    *ledger.Store
	storeErr error
	sched    *schedule.Scheduler
	schedErr error
	// failed is a reading that did not end the rows but fails the command.
	failed bool
}

type daemonStatus struct {
	Running           *bool      `json:"running"`
	PID               *int       `json:"pid"`
	Error             readError  `json:"error"`
	StartFailure      nullString `json:"start_failure"`
	StartFailureError readError  `json:"start_failure_error"`
}

// rowStatus is an agent's ledger row.
type rowStatus struct {
	Paused        *bool      `json:"paused"`
	PausedReason  nullString `json:"paused_reason"`
	PausedAt      nullString `json:"paused_at"`
	FailureStreak *int       `json:"failure_streak"`
	RowError      readError  `json:"row_error"`
}

// agentStatus is one definition. LastRun is null with no error for an agent
// that has never run.
type agentStatus struct {
	Name string `json:"name"`
	rowStatus
	// Both instants: the slot is what the ledger records as scheduled_for and
	// what `shuttle agents` prints, and the jittered one is the minute the
	// daemon actually acts on (§5 step 6).
	NextSlot      *time.Time `json:"next_slot"`
	FiresAt       *time.Time `json:"fires_at"`
	NextSlotError readError  `json:"next_slot_error"`
	LastRun       *runStatus `json:"last_run"`
	LastRunError  readError  `json:"last_run_error"`
}

type runStatus struct {
	Status       string     `json:"status"`
	SkipReason   nullString `json:"skip_reason"`
	ScheduledFor string     `json:"scheduled_for"`
	CostUSD      nullFloat  `json:"cost_usd"`
}

// weftStatus is the hosted worker: its pause from the ledger's agents row, the
// queue's running and waiting against the cap (the figures /v2/dashboard
// reports), and worker_bin's answer to the daemon's version probe. Where
// Hosted is not true nothing else was read.
type weftStatus struct {
	Hosted      *bool     `json:"hosted"`
	HostedError readError `json:"hosted_error"`
	rowStatus
	Counts      *countStatus `json:"counts"`
	CountsError readError    `json:"counts_error"`
	Probe       *probeStatus `json:"probe"`
}

type countStatus struct {
	Running  int `json:"running"`
	Waiting  int `json:"waiting"`
	Capacity int `json:"capacity"`
}

type probeStatus struct {
	WorkerBin    string     `json:"worker_bin"`
	OK           bool       `json:"ok"`
	Failure      nullString `json:"failure"`
	Version      nullString `json:"version"`
	Contracts    []string   `json:"contracts"`
	Capabilities []string   `json:"capabilities"`
}

type incidentStatus struct {
	ID        int64  `json:"id"`
	Agent     string `json:"agent"`
	Signature string `json:"signature"`
}

func (r *statusReport) close() {
	if r.store != nil {
		r.store.Close()
	}
}

// readDaemon reads the daemon's launchd process and, while one stands, the
// start failure it last exited on.
func (r *statusReport) readDaemon() error {
	proc, running, err := statusProcess(launchd.DaemonLabel)
	if err != nil {
		r.Daemon.Error = readError{err}
		r.failed = true
	} else {
		r.Daemon.Running = &running
		if running {
			r.Daemon.PID = &proc.PID
		}
	}
	failure, recorded, err := readStartFailure()
	r.Daemon.StartFailure.String, r.Daemon.StartFailure.Valid = failure, recorded
	r.Daemon.StartFailureError = readError{err}
	return nil
}

func (r *statusReport) printDaemon() {
	d := r.Daemon
	state := "not running"
	switch {
	case d.Error.error != nil:
		state = "unknown: " + escapeControl(d.Error.Error())
	case *d.Running:
		state = fmt.Sprintf("running (pid %d)", *d.PID)
	}
	row := "daemon\t" + state
	if d.StartFailure.Valid {
		row += "\tstart failed: " + escapeControl(d.StartFailure.String)
	}
	fmt.Println(row)
}

// readAgents opens the ledger and loads the definitions, which the later
// sections read too, and reads every agent.
func (r *statusReport) readAgents() error {
	r.store, r.storeErr = openLedger()
	if r.storeErr != nil {
		r.schedErr = r.storeErr
		r.AgentsError = readError{r.storeErr}
		return r.storeErr
	}
	var loadErr error
	r.sched, loadErr, r.schedErr = loadScheduler(r.store, log.New(os.Stderr, "", 0))
	if r.schedErr != nil {
		r.AgentsError = readError{r.schedErr}
		return r.schedErr
	}
	r.AgentsError = readError{loadErr}
	if loadErr == nil {
		r.Agents = []agentStatus{}
	}
	var fatal error
	now := time.Now()
	for _, def := range r.sched.Defs {
		agent := agentStatus{Name: def.Name, rowStatus: r.readRow(def.Name)}
		if fatal == nil {
			fatal = agent.RowError.error
		}
		next, err := def.NextSlot(now)
		if err != nil {
			agent.NextSlotError = readError{err}
			r.failed = true
		} else {
			fires := next.Add(schedule.Jitter(def.Name))
			agent.NextSlot, agent.FiresAt = &next, &fires
		}
		runs, err := r.store.Runs(def.Name, 1)
		if err != nil {
			agent.LastRunError = readError{err}
			r.failed = true
		} else if len(runs) > 0 {
			agent.LastRun = &runStatus{
				Status: runs[0].Status, SkipReason: nullString{runs[0].SkipReason},
				ScheduledFor: runs[0].ScheduledFor, CostUSD: nullFloat{runs[0].CostUSD},
			}
		}
		r.Agents = append(r.Agents, agent)
	}
	return fatal
}

func (r *statusReport) printAgents() {
	for _, agent := range r.Agents {
		if agent.RowError.error != nil {
			return
		}
		if agent.NextSlotError.error != nil {
			// One definition whose triggers will not resolve must not hide
			// every other agent's row.
			fmt.Fprintln(os.Stderr, agent.NextSlotError)
			continue
		}
		paused := "active"
		if *agent.Paused {
			paused = "paused (" + nullOr(agent.PausedReason.NullString) + ")"
		}
		fmt.Printf("%s\tnext %s (fires %s)\tlast %s\tstreak %d\t%s\n",
			escapeControl(agent.Name), agent.NextSlot.Format("2006-01-02 15:04:05 MST"),
			agent.FiresAt.Format("15:04:05"), lastOutcome(agent), *agent.FailureStreak, paused)
	}
}

// lastOutcome renders the agent's most recent ledger row: its status, the slot
// it covered and what it cost. A ledger read that fails is reported in the
// column rather than aborting the whole status.
func lastOutcome(agent agentStatus) string {
	if agent.LastRunError.error != nil {
		return "error: " + escapeControl(agent.LastRunError.Error())
	}
	if agent.LastRun == nil {
		return "never run"
	}
	run := agent.LastRun
	status := escapeControl(run.Status)
	if run.SkipReason.Valid {
		status += "/" + escapeControl(run.SkipReason.String)
	}
	return fmt.Sprintf("%s %s %s", status, escapeControl(run.ScheduledFor), formatCost(run.CostUSD.NullFloat64))
}

// readRow reads name's ledger row.
func (r *statusReport) readRow(name string) rowStatus {
	if r.storeErr != nil {
		return rowStatus{RowError: readError{r.storeErr}}
	}
	agent, err := r.store.Agent(name)
	if err != nil {
		return rowStatus{RowError: readError{err}}
	}
	return rowStatus{
		Paused: &agent.Paused, PausedReason: nullString{agent.PausedReason},
		PausedAt: nullString{agent.PausedAt}, FailureStreak: &agent.FailureStreak,
	}
}

// readWeft reads weft-worker, the ledger's hosted agent rather than a
// definition, so it has no slot of its own. It is read wherever the daemon
// would host the service: the same empty-Listen rule. The probe is run by this
// command, now, and labelled so in its row: status answers with the daemon
// down and never claims the daemon's view.
func (r *statusReport) readWeft() error {
	w := &r.WeftWorker
	cfg, err := config.Load()
	if err != nil && !os.IsNotExist(err) {
		w.HostedError = readError{err}
		r.failed = true
		return nil
	}
	hosted := cfg != nil && cfg.Weft.Listen != ""
	w.Hosted = &hosted
	if !hosted {
		return nil
	}
	w.rowStatus = r.readRow(ledger.WeftAgent)
	if r.storeErr != nil {
		w.CountsError = readError{r.storeErr}
	} else if counts, err := r.store.WeftDashboardCounts(); err != nil {
		w.CountsError = readError{err}
	} else {
		w.Counts = &countStatus{Running: counts.Running, Waiting: counts.Waiting, Capacity: counts.Capacity}
	}
	if w.RowError.error != nil || w.CountsError.error != nil {
		r.failed = true
	}
	version, contracts, capabilities, failure := weftremote.ProbeWorker(cfg.Weft.WorkerBin)
	w.Probe = &probeStatus{
		WorkerBin: cfg.Weft.WorkerBin, OK: failure == "",
		Contracts: contracts, Capabilities: capabilities,
	}
	w.Probe.Failure.String, w.Probe.Failure.Valid = failure, failure != ""
	w.Probe.Version.String, w.Probe.Version.Valid = version, failure == ""
	return nil
}

func (r *statusReport) printWeft() {
	w := r.WeftWorker
	if w.HostedError.error != nil {
		fmt.Fprintln(os.Stderr, w.HostedError)
		return
	}
	if !*w.Hosted {
		return
	}
	for _, failure := range []readError{w.RowError, w.CountsError} {
		if failure.error != nil {
			fmt.Fprintf(os.Stderr, "%s: %v\n", ledger.WeftAgent, failure)
			return
		}
	}
	paused := "active"
	if *w.Paused {
		paused = "paused (" + nullOr(w.PausedReason.NullString) + ") since " + nullOr(w.PausedAt.NullString)
	}
	worker := "worker (probed now) "
	if !w.Probe.OK {
		worker += escapeControl(w.Probe.WorkerBin) + ": " + escapeControl(w.Probe.Failure.String)
	} else {
		worker += fmt.Sprintf("%s contracts [%s] capabilities [%s]", escapeControl(w.Probe.Version.String),
			escapeControl(strings.Join(w.Probe.Contracts, " ")), escapeControl(strings.Join(w.Probe.Capabilities, " ")))
	}
	fmt.Printf("%s\trunning %d waiting %d cap %d\t%s\t%s\n",
		ledger.WeftAgent, w.Counts.Running, w.Counts.Waiting, w.Counts.Capacity, paused, worker)
}

// readSpend sums the window the daily limits use: midnight in the configured
// zone. A sum over the runs that reported a cost. A window whose runs all
// reported none sums to NULL and reads as unpriced: $0.00 would claim the day
// was free (§12).
func (r *statusReport) readSpend() error {
	if r.schedErr != nil {
		r.SpendTodayError = readError{r.schedErr}
		return r.schedErr
	}
	spent, err := r.store.SpendSince(r.sched.DayStart())
	if err != nil {
		r.SpendTodayError = readError{err}
		return err
	}
	r.SpendTodayUSD = nullFloat{spent}
	return nil
}

func (r *statusReport) printSpend() {
	if r.SpendTodayError.error == nil {
		fmt.Printf("today's spend\t%s\n", formatCost(r.SpendTodayUSD.NullFloat64))
	}
}

func (r *statusReport) readIncidents() error {
	if r.storeErr != nil {
		r.OpenIncidentsError = readError{r.storeErr}
		return r.storeErr
	}
	incidents, err := r.store.OpenIncidents()
	if err != nil {
		r.OpenIncidentsError = readError{err}
		return err
	}
	r.OpenIncidents = make([]incidentStatus, 0, len(incidents))
	for _, i := range incidents {
		r.OpenIncidents = append(r.OpenIncidents, incidentStatus{ID: i.ID, Agent: i.Agent, Signature: i.Signature})
	}
	return nil
}

func (r *statusReport) printIncidents() {
	if r.OpenIncidentsError.error == nil {
		fmt.Printf("open incidents\t%d\n", len(r.OpenIncidents))
	}
}

// readError is why a reading could not be taken: its text in the object, null
// where the reading was taken.
type readError struct{ error }

func (e readError) MarshalJSON() ([]byte, error) {
	if e.error == nil {
		return []byte("null"), nil
	}
	return json.Marshal(e.Error())
}

// nullString and nullFloat are a ledger column the run or the agent may never
// have set. An unpriced cost is null, never 0 (§12).
type nullString struct{ sql.NullString }

func (v nullString) MarshalJSON() ([]byte, error) {
	if !v.Valid {
		return []byte("null"), nil
	}
	return json.Marshal(v.String)
}

type nullFloat struct{ sql.NullFloat64 }

func (v nullFloat) MarshalJSON() ([]byte, error) {
	if !v.Valid {
		return []byte("null"), nil
	}
	return json.Marshal(v.Float64)
}
