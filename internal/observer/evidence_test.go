package observer

import "testing"

// An incompatible baseline is refused rather than silently compared: each of
// these deltas would be a number about the measurement rather than about the
// subject.
func TestIncompatibleBaselinesRefuseToCompare(t *testing.T) {
	cases := []struct {
		name   string
		field  string
		mutate func(base *Baseline)
	}{
		{"another method revision", "baseline.method", func(b *Baseline) { b.Method.Revision = "2026-09-01" }},
		{"another subject instance", "baseline.subject", func(b *Baseline) { b.Subject.Instance = "wren-2" }},
		{"another collection", "baseline.scope.collection", func(b *Baseline) { b.Scope.Collection = "wren-archive.sqlite" }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			bundle, err := ValidateEvidence(load(t, "wren", "evidence.json"))
			if err != nil {
				t.Fatal(err)
			}
			base := mustExperiment(t, "wren", "experiment.json").Evidence.Baseline
			c.mutate(base)
			refusal(t, bundle.Comparable(base), c.field)
			comparison, err := bundle.Compare(base)
			refusal(t, err, c.field)
			if comparison != nil {
				t.Fatalf("got a comparison %+v alongside the refusal", comparison)
			}
		})
	}

	bundle, err := ValidateEvidence(load(t, "wren", "evidence.json"))
	if err != nil {
		t.Fatal(err)
	}
	refusal(t, bundle.Comparable(nil), "baseline")
}

// A missing reading is missing, not zero. It is listed as uncompared rather
// than measured against the baseline as a fall to nothing.
func TestMissingEvidenceIsNotZero(t *testing.T) {
	bundle, err := ValidateEvidence(load(t, "wren", "evidence.json"))
	if err != nil {
		t.Fatal(err)
	}
	base := mustExperiment(t, "wren", "experiment.json").Evidence.Baseline
	comparison, err := bundle.Compare(base)
	if err != nil {
		t.Fatal(err)
	}
	if len(comparison.Deltas) != 2 {
		t.Fatalf("deltas = %+v, want the two readings both sides carry", comparison.Deltas)
	}
	for _, d := range comparison.Deltas {
		if d.Name == "mana_spend" {
			t.Fatal("an absent reading was compared")
		}
	}
	if len(comparison.Missing) != 1 || comparison.Missing[0] != "mana_spend" {
		t.Fatalf("missing = %v, want the reading that could not be taken", comparison.Missing)
	}

	// The distinction is in the payload, not only in the comparison: an absent
	// reading carries a reason and no value, and zero is a value.
	var absent, present Reading
	for _, r := range bundle.Readings {
		if r.Name == "mana_spend" {
			absent = r
		}
		if r.Name == "repeat_rate" {
			present = r
		}
	}
	if absent.Value != nil || absent.Unavailable == "" {
		t.Fatalf("mana_spend = %+v, want no value and a reason", absent)
	}
	if present.Value == nil {
		t.Fatal("repeat_rate has no value")
	}
}

// The window is explicit or the bundle is refused: a bare date, a naive
// timestamp, a host-relative zone and a zone that does not resolve are all the
// same failure — a span nobody can place.
func TestWindowMustBeExplicit(t *testing.T) {
	cases := []struct {
		name   string
		field  string
		mutate func(w *Window)
	}{
		{"bare date", "window.start", func(w *Window) { w.Start = "2026-09-10" }},
		{"naive timestamp", "window.end", func(w *Window) { w.End = "2026-09-17T00:00:00" }},
		{"no zone", "window.timezone", func(w *Window) { w.Timezone = "" }},
		{"host-relative zone", "window.timezone", func(w *Window) { w.Timezone = "Local" }},
		{"unknown zone", "window.timezone", func(w *Window) { w.Timezone = "America/Nowhere" }},
		{"end before start", "window.end", func(w *Window) { w.End = "2026-09-01T00:00:00-07:00" }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			bundle, err := ValidateEvidence(load(t, "wren", "evidence.json"))
			if err != nil {
				t.Fatal(err)
			}
			c.mutate(&bundle.Window)
			refusal(t, bundle.validate(), c.field)
		})
	}
}

// Coverage, sources and unavailable reasons are what make a reading checkable:
// who could not be read, and out of how much.
func TestEvidenceIdentifiesWhatItCouldNotRead(t *testing.T) {
	bundle, err := ValidateEvidence(load(t, "wren", "evidence.json"))
	if err != nil {
		t.Fatal(err)
	}
	if len(bundle.Unavailable) == 0 || bundle.Unavailable[0].Reason == "" {
		t.Fatal("the fixture is meant to name a source it could not read")
	}
	if bundle.Coverage.Observed >= bundle.Coverage.Available {
		t.Fatal("the fixture is meant to cover less than the whole population")
	}

	bundle.Sources = nil
	bundle.Unavailable = nil
	refusal(t, bundle.validate(), "sources")
}

// A reading the baseline carries and this bundle never took is missing on the
// same terms as one that was attempted and failed: it is reported rather than
// dropped out of the comparison.
func TestBaselineOnlyReadingsAreReportedMissing(t *testing.T) {
	bundle, err := ValidateEvidence(load(t, "wren", "evidence.json"))
	if err != nil {
		t.Fatal(err)
	}
	base := mustExperiment(t, "wren", "experiment.json").Evidence.Baseline
	// Drop the reading from this window's bundle, keeping it in the baseline.
	kept := bundle.Readings[:0]
	for _, r := range bundle.Readings {
		if r.Name != "distinct_activities" {
			kept = append(kept, r)
		}
	}
	bundle.Readings = kept

	comparison, err := bundle.Compare(base)
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range comparison.Deltas {
		if d.Name == "distinct_activities" {
			t.Fatal("a reading the bundle never took was compared")
		}
	}
	var found bool
	for _, name := range comparison.Missing {
		if name == "distinct_activities" {
			found = true
		}
	}
	if !found {
		t.Fatalf("missing = %v, want it to name the baseline reading this window never took", comparison.Missing)
	}
}

// A reading name is a key. A bundle, a baseline or a set of requirements that
// carries one twice makes every lookup answer by position, which is the same
// ambiguity the qualified ids exist to prevent.
func TestDuplicateReadingNames(t *testing.T) {
	bundle, err := ValidateEvidence(load(t, "wren", "evidence.json"))
	if err != nil {
		t.Fatal(err)
	}
	bundle.Readings = append(bundle.Readings, bundle.Readings[0])
	refusal(t, bundle.validate(), "readings[3].name")

	exp := mustExperiment(t, "wren", "experiment.json")
	exp.Evidence.Baseline.Readings = append(exp.Evidence.Baseline.Readings, exp.Evidence.Baseline.Readings[0])
	refusal(t, exp.validate(), "evidence.baseline.readings[2].name")

	exp = mustExperiment(t, "wren", "experiment.json")
	exp.Evidence.Readings = append(exp.Evidence.Readings, "repeat_rate")
	refusal(t, exp.validate(), "evidence.readings")
}
