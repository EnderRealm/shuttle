package schedule

import (
	"fmt"
	"hash/fnv"
	"slices"
	"time"

	"shuttle/internal/agentdef"
)

const (
	// jitterSeconds is §5 step 6's spread: the offset keeps two agents sharing
	// an expression off the same second, and being derived from the name it is
	// the same minute every night.
	jitterSeconds = 300

	// maxDueSlots bounds one batch. A cursor left behind by a machine that was
	// off for months, against a per-minute expression, would otherwise walk
	// every minute in between. The batch stops here; every slot in it is
	// recorded — claimed or skipped — and the caller's cursor is the newest
	// slot recorded, so the next tick resumes from where this one stopped.
	maxDueSlots = 1000

	// wallLayout renders the local wall clock a cron expression matched. Two
	// slots with the same rendering in the same zone are one slot (§5).
	wallLayout = "2006-01-02 15:04:05"

	// dateLayout is the form `shuttle run --slot` takes.
	dateLayout = "2006-01-02"
)

// Jitter is the deterministic per-agent offset, fnv32(name) mod 300 seconds
// (§5 step 6). FNV-1a rather than Go's map hash: maphash is seeded per process
// and its result is not fixed across Go versions, so the same agent would
// start at a different minute after every daemon restart.
func Jitter(name string) time.Duration {
	h := fnv.New32a()
	// hash.Hash's Write never returns an error.
	h.Write([]byte(name))
	return time.Duration(h.Sum32()%jitterSeconds) * time.Second
}

// Due returns the slots due at now, oldest first, merged across the
// definition's triggers (§5 steps 1 and 2). last is the newest slot already
// recorded for the agent; the zero time means the agent has none.
func Due(def *agentdef.Definition, last, now time.Time) ([]time.Time, error) {
	// A cursor after now cannot yield a due slot, but it would suspend the
	// agent until the cursor's own date — so a hand-edited ledger row or a
	// clock moved backwards is clamped rather than silently obeyed.
	if last.After(now) {
		last = now
	}
	type slot struct {
		at  time.Time
		loc *time.Location
	}
	var (
		slots  []slot
		jitter = Jitter(def.Name)
		// seen holds the wall clock of every slot already accounted for, keyed
		// by zone, starting with the cursor's own.
		seen = map[string]bool{}
	)
	for i, t := range def.Manifest.Triggers {
		loc, err := time.LoadLocation(t.Timezone)
		if err != nil {
			return nil, fmt.Errorf("%s: triggers[%d].timezone: %w", def.Dir, i, err)
		}
		sched, err := agentdef.ParseCron(t.Expression)
		if err != nil {
			return nil, fmt.Errorf("%s: triggers[%d].expression: %w", def.Dir, i, err)
		}
		// The zero cursor is now: no basis to catch up from, so nothing is due
		// before the next slot. The scheduler holds the real basis for an agent
		// the ledger has no slot for (Scheduler.cursor).
		cursor := now.In(loc)
		if !last.IsZero() {
			cursor = last.In(loc)
			seen[wallKey(loc, cursor)] = true
		}
		for n := 0; n < maxDueSlots; n++ {
			at := sched.Next(cursor)
			// Jitter delays the fire, not the slot: what the ledger records
			// stays the canonical un-jittered instant (§5 step 6).
			if now.Before(at.Add(jitter)) {
				break
			}
			slots = append(slots, slot{at: at, loc: loc})
			cursor = at
		}
	}
	slices.SortFunc(slots, func(a, b slot) int { return a.at.Compare(b.at) })

	var due []time.Time
	for _, s := range slots {
		// Two triggers that matched the same instant are one slot.
		if len(due) > 0 && due[len(due)-1].Equal(s.at) {
			continue
		}
		// The fall-back rule (§5). On the repeated hour the expression matches
		// twice, at two distinct instants that render as two distinct
		// scheduled_for strings — so the ledger's unique index on
		// (agent, scheduled_for, trigger_kind) cannot refuse the second claim,
		// and the rule has to be enforced here instead. The same wall clock on
		// the same local date is one slot, and the repeat is dropped before any
		// ledger write: it is not a slot at all, so it gets no skipped row.
		key := wallKey(s.loc, s.at)
		if seen[key] {
			continue
		}
		seen[key] = true
		due = append(due, s.at)
	}
	return due, nil
}

// ManualSlot resolves `shuttle run --slot 2026-09-01` to the earliest slot
// falling on that local day, across the definition's triggers. A day with no
// slot — a spring-forward 02:30 is exactly that — is an error rather than a run
// silently reporting some other day.
func ManualSlot(def *agentdef.Definition, date string) (time.Time, error) {
	var found time.Time
	for i, t := range def.Manifest.Triggers {
		loc, err := time.LoadLocation(t.Timezone)
		if err != nil {
			return time.Time{}, fmt.Errorf("%s: triggers[%d].timezone: %w", def.Dir, i, err)
		}
		sched, err := agentdef.ParseCron(t.Expression)
		if err != nil {
			return time.Time{}, fmt.Errorf("%s: triggers[%d].expression: %w", def.Dir, i, err)
		}
		day, err := time.ParseInLocation(dateLayout, date, loc)
		if err != nil {
			return time.Time{}, fmt.Errorf("agent %s: slot %q: not a %s date", def.Name, date, dateLayout)
		}
		// Next is strictly after its argument, so the cursor sits just before
		// midnight to keep a slot at midnight on the day it belongs to.
		at := sched.Next(day.Add(-time.Nanosecond))
		if at.In(loc).Format(dateLayout) != date {
			continue
		}
		if found.IsZero() || at.Before(found) {
			found = at
		}
	}
	if found.IsZero() {
		return time.Time{}, fmt.Errorf("agent %s: no slot on %s", def.Name, date)
	}
	return found, nil
}

// ManualSlotNow is the slot a manual run without --slot carries: now truncated
// to the second in the first trigger's zone, so the row reads as wall clock the
// way a cron slot does.
func ManualSlotNow(def *agentdef.Definition, now time.Time) (time.Time, error) {
	if len(def.Manifest.Triggers) == 0 {
		return time.Time{}, fmt.Errorf("%s: triggers: none", def.Dir)
	}
	loc, err := time.LoadLocation(def.Manifest.Triggers[0].Timezone)
	if err != nil {
		return time.Time{}, fmt.Errorf("%s: triggers[0].timezone: %w", def.Dir, err)
	}
	return now.In(loc).Truncate(time.Second), nil
}

// wallKey identifies a slot by the wall clock its expression matched, in its
// own zone.
func wallKey(loc *time.Location, at time.Time) string {
	return loc.String() + " " + at.In(loc).Format(wallLayout)
}
