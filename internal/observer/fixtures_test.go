package observer

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// load reads a fixture as the bytes the adapter would hand a validator.
func load(t *testing.T, domain, name string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", domain, name))
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// kindOf reads the payload kind off the fixture's name. The fixtures are the
// wire, so the name is all the test knows before a validator has run.
func kindOf(name string) string {
	base := strings.TrimSuffix(name, ".json")
	if i := strings.Index(base, "-"); i >= 0 {
		base = base[:i]
	}
	return base
}

// validateFixture drives a fixture through the entry point its kind takes on
// the wire, so both domains are held to one code path rather than two.
func validateFixture(kind string, data []byte) error {
	var err error
	switch kind {
	case "question":
		_, err = ValidateQuestion(data)
	case "experiment":
		_, err = ValidateExperiment(data)
	case "evidence":
		_, err = ValidateEvidence(data)
	case "assessment":
		_, err = ValidateAssessment(data)
	case "action":
		_, err = ValidateAction(data)
	default:
		return fmt.Errorf("unknown fixture kind %q", kind)
	}
	return err
}

// TestFixturesValidate holds every versioned fixture of both domains to the
// same validators: a domain-neutral contract that only Wren's payloads passed
// would be a Wren schema with a general name.
func TestFixturesValidate(t *testing.T) {
	for _, domain := range []string{"wren", "harness"} {
		entries, err := os.ReadDir(filepath.Join("testdata", domain))
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) == 0 {
			t.Fatalf("%s: no fixtures", domain)
		}
		for _, e := range entries {
			t.Run(domain+"/"+e.Name(), func(t *testing.T) {
				if err := validateFixture(kindOf(e.Name()), load(t, domain, e.Name())); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

// TestInvalidFixturesNameTheField is the refusal half of the contract: every
// one of these is refused, and the refusal says which field to fix.
func TestInvalidFixturesNameTheField(t *testing.T) {
	fields := map[string]string{
		"question-unsupported-version.json":              "contract_version",
		"question-no-version.json":                       "contract_version",
		"question-unqualified-id.json":                   "id",
		"question-foreign-id.json":                       "id",
		"question-missing-changes.json":                  "changes",
		"question-asked-not-a-date.json":                 "asked",
		"question-no-author.json":                        "author.kind",
		"question-unknown-author-kind.json":              "author.kind",
		"evidence-naive-window.json":                     "window.start",
		"evidence-bare-date-window.json":                 "window.start",
		"evidence-no-timezone.json":                      "window.timezone",
		"evidence-no-snapshot.json":                      "scope.snapshot",
		"evidence-reading-neither-value-nor-reason.json": "readings[2]",
		"evidence-reading-value-and-reason.json":         "readings[0]",
		"evidence-unavailable-no-kind.json":              "unavailable[0]",
		"evidence-coverage-over-population.json":         "coverage",
		"experiment-commit-as-activation.json":           "activation.kind",
		"experiment-no-expected-effect.json":             "expected_effect",
		"experiment-no-opportunity.json":                 "observation.min_opportunities",
		"experiment-no-baseline.json":                    "evidence.baseline",
		"experiment-baseline-no-method-revision.json":    "evidence.baseline.method.revision",
		"experiment-activation-other-instance.json":      "activation.instance",
		"experiment-no-activation.json":                  "activation",
		"experiment-empty-activation.json":               "activation",
		"assessment-unknown-field.json":                  "payload",
		"assessment-foreign-evidence.json":               "evidence.identity",
		"assessment-unknown-outcome.json":                "outcome",
		"assessment-question-and-finding.json":           "finding",
		"assessment-finding-no-subject.json":             "finding.subject.kind",
		"action-no-body.json":                            "body",
		"action-no-evidence.json":                        "evidence",
		"action-foreign-id.json":                         "id",
	}
	entries, err := os.ReadDir(filepath.Join("testdata", "invalid"))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != len(fields) {
		t.Fatalf("testdata/invalid holds %d fixtures, the table names %d", len(entries), len(fields))
	}
	for _, e := range entries {
		t.Run(e.Name(), func(t *testing.T) {
			want, ok := fields[e.Name()]
			if !ok {
				t.Fatalf("no expected field for %s", e.Name())
			}
			err := validateFixture(kindOf(e.Name()), load(t, "invalid", e.Name()))
			if err == nil {
				t.Fatal("got no error, want a refusal")
			}
			var verr *Error
			if !errors.As(err, &verr) {
				t.Fatalf("got %T (%v), want an *Error naming the field", err, err)
			}
			if verr.Field != want {
				t.Fatalf("field = %q, want %q (%v)", verr.Field, want, err)
			}
		})
	}
}
