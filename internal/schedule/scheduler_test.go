package schedule

import (
	"bytes"
	"database/sql"
	"log"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"shuttle/internal/agentdef"
	"shuttle/internal/ledger"
)

// harness drives a real ledger on a real file — SetMaxOpenConns(1) over WAL is
// what production does — with one injected clock behind both the scheduler and
// the store, so claimed_at moves with the test rather than with the wall clock.
type harness struct {
	at    time.Time
	store *ledger.Store
	sched *Scheduler
	// logs is the scheduler's decision record (§13). A tick that re-derives a
	// slot it already recorded writes rows the unique index refuses, which the
	// ledger cannot show afterwards but these lines can.
	logs bytes.Buffer
}

func newHarness(t *testing.T, now string, defs ...*agentdef.Definition) *harness {
	t.Helper()
	store, err := ledger.Open(filepath.Join(t.TempDir(), "runs.db"))
	if err != nil {
		t.Fatalf("open ledger: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	h := &harness{at: mustParse(t, now), store: store}
	store.SetClock(h.now)
	h.sched = &Scheduler{
		Ledger: store,
		Defs:   defs,
		Loc:    losAngeles(t),
		Now:    h.now,
		Log:    log.New(&h.logs, "", 0),
	}
	return h
}

func (h *harness) now() time.Time { return h.at }

// tickAt moves the clock and runs one pass.
func (h *harness) tickAt(t *testing.T, now string) {
	t.Helper()
	h.at = mustParse(t, now)
	if err := h.sched.Tick(); err != nil {
		t.Fatalf("tick at %s: %v", now, err)
	}
}

// fire claims a slot and closes it, which is how an agent acquires a
// last_fired_slot without leaving a run in flight.
func (h *harness) fire(t *testing.T, name, at string, res ledger.Result) {
	t.Helper()
	run := h.claim(t, name, at)
	if err := h.store.Complete(run.ID, res); err != nil {
		t.Fatalf("complete: %v", err)
	}
}

func (h *harness) claim(t *testing.T, name, at string) *ledger.Run {
	t.Helper()
	run, ok, err := h.store.Claim(name, mustParse(t, at), ledger.TriggerCron)
	if err != nil || !ok {
		t.Fatalf("claim %s at %s = (%t, %v)", name, at, ok, err)
	}
	return run
}

// ledgerRows renders every row as status/skip_reason@slot, the three columns
// these tests assert on, sorted so the comparison does not depend on the order
// rows come back in.
func (h *harness) ledgerRows(t *testing.T) []string {
	t.Helper()
	runs, err := h.store.Runs("", 0)
	if err != nil {
		t.Fatalf("runs: %v", err)
	}
	var rows []string
	for _, r := range runs {
		row := r.Agent + " " + r.Status
		if r.SkipReason.Valid {
			row += "/" + r.SkipReason.String
		}
		rows = append(rows, row+"@"+r.ScheduledFor)
	}
	slices.Sort(rows)
	return rows
}

func (h *harness) assertLedger(t *testing.T, want ...string) {
	t.Helper()
	got := h.ledgerRows(t)
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Fatalf("ledger =\n  %v\nwant\n  %v", got, want)
	}
}

func losAngeles(t *testing.T) *time.Location {
	t.Helper()
	loc, err := time.LoadLocation("America/Los_Angeles")
	if err != nil {
		t.Fatalf("load location: %v", err)
	}
	return loc
}

func TestTick(t *testing.T) {
	// Both definitions carry the agent name "daily" on purpose, so every case's
	// expected rows read uniformly whichever cadence it drives.
	daily := agent("daily", cronTrigger("30 4 * * *", "America/Los_Angeles"))
	hourly := agent("daily", cronTrigger("0 * * * *", "America/Los_Angeles"))
	tests := []struct {
		name          string
		def           *agentdef.Definition
		maxRunsPerDay int
		maxSpendUSD   float64
		setup         func(t *testing.T, h *harness)
		now           string
		want          []string
	}{
		{
			name: "a due slot is claimed",
			def:  daily,
			setup: func(t *testing.T, h *harness) {
				h.fire(t, "daily", "2026-09-01T04:30:00-07:00", ledger.Result{})
			},
			now: "2026-09-02T04:36:00-07:00",
			want: []string{
				"daily completed@2026-09-01T04:30:00-07:00",
				"daily claimed@2026-09-02T04:30:00-07:00",
			},
		},
		{
			name: "a slot that is not due yet is left alone",
			def:  daily,
			setup: func(t *testing.T, h *harness) {
				h.fire(t, "daily", "2026-09-01T04:30:00-07:00", ledger.Result{})
			},
			now:  "2026-09-02T04:00:00-07:00",
			want: []string{"daily completed@2026-09-01T04:30:00-07:00"},
		},
		{
			name: "an agent that has never fired does not replay its history",
			def:  daily,
			now:  "2026-09-02T12:00:00-07:00",
		},
		{
			// Catch-up-one: the machine slept through three slots, so the
			// newest fires and the other two are recorded as missed.
			name: "three slots missed by a sleep fire one run",
			def:  daily,
			setup: func(t *testing.T, h *harness) {
				h.fire(t, "daily", "2026-08-30T04:30:00-07:00", ledger.Result{})
			},
			now: "2026-09-02T04:36:00-07:00",
			want: []string{
				"daily completed@2026-08-30T04:30:00-07:00",
				"daily skipped/missed@2026-08-31T04:30:00-07:00",
				"daily skipped/missed@2026-09-01T04:30:00-07:00",
				"daily claimed@2026-09-02T04:30:00-07:00",
			},
		},
		{
			name: "a slot overlapping a run in flight is skipped",
			def:  daily,
			setup: func(t *testing.T, h *harness) {
				h.claim(t, "daily", "2026-09-01T04:30:00-07:00")
			},
			now: "2026-09-02T04:36:00-07:00",
			want: []string{
				"daily claimed@2026-09-01T04:30:00-07:00",
				"daily skipped/overlap@2026-09-02T04:30:00-07:00",
			},
		},
		{
			name: "a paused agent's slot is skipped",
			def:  daily,
			setup: func(t *testing.T, h *harness) {
				// The first auth failure pauses the agent (§6.3).
				run := h.claim(t, "daily", "2026-09-01T04:30:00-07:00")
				if err := h.store.Fail(run.ID, ledger.ClassAuth, "authentication_failed", ledger.Result{}); err != nil {
					t.Fatalf("fail: %v", err)
				}
			},
			now: "2026-09-02T04:36:00-07:00",
			want: []string{
				"daily failed@2026-09-01T04:30:00-07:00",
				"daily skipped/paused@2026-09-02T04:30:00-07:00",
			},
		},
		{
			name:          "the daily run cap skips the slot",
			def:           hourly,
			maxRunsPerDay: 1,
			setup: func(t *testing.T, h *harness) {
				h.fire(t, "daily", "2026-09-02T09:00:00-07:00", ledger.Result{})
			},
			now: "2026-09-02T10:06:00-07:00",
			want: []string{
				"daily completed@2026-09-02T09:00:00-07:00",
				"daily skipped/limit@2026-09-02T10:00:00-07:00",
			},
		},
		{
			// A skipped slot never launched, so it costs nothing and must not
			// count against the cap.
			name:          "skipped slots do not count against the run cap",
			def:           hourly,
			maxRunsPerDay: 2,
			setup: func(t *testing.T, h *harness) {
				h.fire(t, "daily", "2026-09-02T08:00:00-07:00", ledger.Result{})
			},
			now: "2026-09-02T10:06:00-07:00",
			want: []string{
				"daily completed@2026-09-02T08:00:00-07:00",
				"daily skipped/missed@2026-09-02T09:00:00-07:00",
				"daily claimed@2026-09-02T10:00:00-07:00",
			},
		},
		{
			name:        "the daily spend cap skips the slot",
			def:         hourly,
			maxSpendUSD: 1.0,
			setup: func(t *testing.T, h *harness) {
				h.fire(t, "daily", "2026-09-02T09:00:00-07:00", ledger.Result{
					CostUSD: sql.NullFloat64{Float64: 2.0, Valid: true},
				})
			},
			now: "2026-09-02T10:06:00-07:00",
			want: []string{
				"daily completed@2026-09-02T09:00:00-07:00",
				"daily skipped/limit@2026-09-02T10:00:00-07:00",
			},
		},
		{
			// The limits are measured from midnight in the scheduler's zone,
			// so yesterday's spend does not hold today's slot back.
			name:        "yesterday's spend is outside today's cap",
			def:         hourly,
			maxSpendUSD: 1.0,
			setup: func(t *testing.T, h *harness) {
				h.at = mustParse(t, "2026-09-01T09:06:00-07:00")
				h.fire(t, "daily", "2026-09-01T09:00:00-07:00", ledger.Result{
					CostUSD: sql.NullFloat64{Float64: 2.0, Valid: true},
				})
				h.at = mustParse(t, "2026-09-02T09:06:00-07:00")
				h.fire(t, "daily", "2026-09-02T09:00:00-07:00", ledger.Result{})
			},
			now: "2026-09-02T10:06:00-07:00",
			want: []string{
				"daily completed@2026-09-01T09:00:00-07:00",
				"daily completed@2026-09-02T09:00:00-07:00",
				"daily claimed@2026-09-02T10:00:00-07:00",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newHarness(t, tt.now, tt.def)
			h.sched.MaxRunsPerDay = tt.maxRunsPerDay
			h.sched.MaxSpendUSD = tt.maxSpendUSD
			if tt.setup != nil {
				tt.setup(t, h)
			}
			h.tickAt(t, tt.now)
			h.assertLedger(t, tt.want...)
		})
	}
}

func TestTickLaunchesAClaimedRun(t *testing.T) {
	def := agent("daily", cronTrigger("30 4 * * *", "America/Los_Angeles"))
	h := newHarness(t, "2026-09-02T04:36:00-07:00", def)
	h.fire(t, "daily", "2026-09-01T04:30:00-07:00", ledger.Result{})
	var launched []string
	h.sched.Launch = func(d *agentdef.Definition, run *ledger.Run) error {
		launched = append(launched, d.Name+" "+run.ScheduledFor)
		return nil
	}
	h.tickAt(t, "2026-09-02T04:36:00-07:00")
	if len(launched) != 1 || launched[0] != "daily 2026-09-02T04:30:00-07:00" {
		t.Fatalf("launched = %v, want the claimed slot once", launched)
	}
	// A second tick over the same slot must not launch again.
	h.tickAt(t, "2026-09-02T04:40:00-07:00")
	if len(launched) != 1 {
		t.Fatalf("launched = %v, want the slot launched once", launched)
	}
}

func TestTickKeepsGoingAfterABrokenDefinition(t *testing.T) {
	broken := agent("broken", cronTrigger("30 4 * * *", "Mars/Olympus"))
	daily := agent("daily", cronTrigger("30 4 * * *", "America/Los_Angeles"))
	h := newHarness(t, "2026-09-02T04:36:00-07:00", broken, daily)
	h.fire(t, "daily", "2026-09-01T04:30:00-07:00", ledger.Result{})

	err := h.sched.Tick()
	if err == nil {
		t.Fatal("tick over a broken definition returned no error")
	}
	h.assertLedger(t,
		"daily completed@2026-09-01T04:30:00-07:00",
		"daily claimed@2026-09-02T04:30:00-07:00",
	)
}

// The 02:30 slot does not exist on the spring-forward day, so no tick on that
// day may claim one — and the agent must still fire the next day.
func TestSpringForwardSlotNeverFires(t *testing.T) {
	def := agent("daily", cronTrigger("30 2 * * *", "America/Los_Angeles"))
	h := newHarness(t, "2026-03-07T02:36:00-08:00", def)
	h.fire(t, "daily", "2026-03-07T02:30:00-08:00", ledger.Result{})

	for _, at := range []string{
		"2026-03-08T01:30:00-08:00",
		"2026-03-08T03:00:00-07:00",
		"2026-03-08T12:00:00-07:00",
		"2026-03-08T23:59:00-07:00",
	} {
		h.tickAt(t, at)
	}
	h.assertLedger(t, "daily completed@2026-03-07T02:30:00-08:00")

	h.tickAt(t, "2026-03-09T02:36:00-07:00")
	h.assertLedger(t,
		"daily completed@2026-03-07T02:30:00-08:00",
		"daily claimed@2026-03-09T02:30:00-07:00",
	)
}

// 01:30 occurs twice on the fall-back day, at two instants that render as two
// different scheduled_for strings, so the unique index cannot refuse the
// second: the scheduler treats the repeat as the same slot and never offers it.
func TestFallBackSlotFiresOnce(t *testing.T) {
	def := agent("daily", cronTrigger("30 1 * * *", "America/Los_Angeles"))
	h := newHarness(t, "2026-10-31T01:36:00-07:00", def)
	h.fire(t, "daily", "2026-10-31T01:30:00-07:00", ledger.Result{})

	// One tick in each offset of the repeated hour.
	h.tickAt(t, "2026-11-01T01:36:00-07:00")
	claimed := h.ledgerRows(t)
	h.tickAt(t, "2026-11-01T01:36:00-08:00")
	h.tickAt(t, "2026-11-01T12:00:00-08:00")

	want := []string{
		"daily completed@2026-10-31T01:30:00-07:00",
		"daily claimed@2026-11-01T01:30:00-07:00",
	}
	h.assertLedger(t, want...)
	// The repeat is not a slot at all, so it left no skipped row either.
	slices.Sort(want)
	if !slices.Equal(claimed, want) {
		t.Fatalf("first tick left %v, want %v", claimed, want)
	}

	// And the day after still fires: the filter drops the repeat, not the
	// agent's schedule.
	if err := h.store.Complete(h.claimedRun(t, "daily").ID, ledger.Result{}); err != nil {
		t.Fatalf("complete: %v", err)
	}
	h.tickAt(t, "2026-11-02T01:36:00-08:00")
	h.assertLedger(t,
		"daily completed@2026-10-31T01:30:00-07:00",
		"daily completed@2026-11-01T01:30:00-07:00",
		"daily claimed@2026-11-02T01:30:00-08:00",
	)
}

// claimedRun returns the agent's run in flight, which the scheduler left
// claimed because these tests set no Launch.
func (h *harness) claimedRun(t *testing.T, name string) *ledger.Run {
	t.Helper()
	runs, err := h.store.Runs(name, 0)
	if err != nil {
		t.Fatalf("runs: %v", err)
	}
	for _, run := range runs {
		if run.Status == ledger.StatusClaimed {
			return run
		}
	}
	t.Fatalf("agent %s has no run in flight", name)
	return nil
}

// A gated slot leaves last_fired_slot where it was (§5), so the cursor is the
// newest slot recorded instead: without it, a paused agent re-derives the same
// growing slot set on every tick, forever.
func TestGatedSlotsAreRecordedOncePerSlot(t *testing.T) {
	def := agent("daily", cronTrigger("0 * * * *", "America/Los_Angeles"))
	h := newHarness(t, "2026-09-02T09:06:00-07:00", def)
	// The first auth failure pauses the agent (§6.3).
	run := h.claim(t, "daily", "2026-09-02T09:00:00-07:00")
	if err := h.store.Fail(run.ID, ledger.ClassAuth, "authentication_failed", ledger.Result{}); err != nil {
		t.Fatalf("fail: %v", err)
	}
	// Four ticks across three later slot periods.
	for _, at := range []string{
		"2026-09-02T10:06:00-07:00",
		"2026-09-02T10:36:00-07:00",
		"2026-09-02T11:06:00-07:00",
		"2026-09-02T12:06:00-07:00",
	} {
		h.tickAt(t, at)
	}
	want := []string{
		"daily failed@2026-09-02T09:00:00-07:00",
		"daily skipped/paused@2026-09-02T10:00:00-07:00",
		"daily skipped/paused@2026-09-02T11:00:00-07:00",
		"daily skipped/paused@2026-09-02T12:00:00-07:00",
	}
	h.assertLedger(t, want...)
	// Each slot was offered once. A cursor taken from last_fired_slot alone
	// would re-derive every slot since the failed run on every tick, which the
	// ledger cannot show — the unique index refuses the repeats — but the log
	// does.
	if n := strings.Count(h.logs.String(), "already-recorded"); n != 0 {
		t.Fatalf("%d slot(s) re-derived after being recorded:\n%s", n, h.logs.String())
	}
	// A later tick inside the same period does nothing at all.
	h.logs.Reset()
	h.tickAt(t, "2026-09-02T12:36:00-07:00")
	h.assertLedger(t, want...)
	if h.logs.Len() != 0 {
		t.Fatalf("a tick inside a period already recorded logged:\n%s", h.logs.String())
	}
}

// A manual run bypasses the schedule (§5), so a manual claim for a future day
// must not move the scheduler's cursor past that day and silence the agent.
func TestManualClaimForAFutureDayDoesNotSuppressTheNextSlot(t *testing.T) {
	def := agent("daily", cronTrigger("30 4 * * *", "America/Los_Angeles"))
	h := newHarness(t, "2026-09-02T04:36:00-07:00", def)
	h.fire(t, "daily", "2026-09-01T04:30:00-07:00", ledger.Result{})
	manual, ok, err := h.store.Claim("daily", mustParse(t, "2026-12-01T04:30:00-08:00"), ledger.TriggerManual)
	if err != nil || !ok {
		t.Fatalf("manual claim = (%t, %v), want claimed", ok, err)
	}
	if err := h.store.Complete(manual.ID, ledger.Result{}); err != nil {
		t.Fatalf("complete: %v", err)
	}
	h.tickAt(t, "2026-09-02T04:36:00-07:00")
	h.assertLedger(t,
		"daily completed@2026-09-01T04:30:00-07:00",
		"daily completed@2026-12-01T04:30:00-08:00",
		"daily claimed@2026-09-02T04:30:00-07:00",
	)
}

// An agent the ledger has no slot for fires at its next slot: it does not
// replay its history, and it does not sit idle forever either.
func TestNewAgentFiresAtItsNextSlot(t *testing.T) {
	def := agent("daily", cronTrigger("30 4 * * *", "America/Los_Angeles"))
	h := newHarness(t, "2026-09-01T12:00:00-07:00", def)
	// Two ticks before the next slot: neither replays yesterday's.
	h.tickAt(t, "2026-09-01T12:00:00-07:00")
	h.tickAt(t, "2026-09-01T23:00:00-07:00")
	h.assertLedger(t)

	// Jitter is 3m18s for "daily", so 04:30 is not due at 04:31.
	h.tickAt(t, "2026-09-02T04:31:00-07:00")
	h.assertLedger(t)
	h.tickAt(t, "2026-09-02T04:36:00-07:00")
	h.assertLedger(t, "daily claimed@2026-09-02T04:30:00-07:00")
}
