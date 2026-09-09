package observer

import (
	"strings"
	"testing"
)

// An unsupported version is refused naming the field and listing what this
// binary does read, so a producer knows what it may send instead.
func TestUnsupportedVersion(t *testing.T) {
	q := mustQuestion(t, "wren", "question.json")
	q.ContractVersion = "observer/v9"
	err := refusal(t, q.validate(), "contract_version")
	if !strings.Contains(err.Error(), ContractVersion) {
		t.Fatalf("the refusal does not name the supported version %s: %v", ContractVersion, err)
	}
	q.ContractVersion = ""
	refusal(t, q.validate(), "contract_version")
}

// Ambiguous identity is refused in all its forms: an identity with nothing to
// qualify by, one that cannot be split back apart, an id that carries no
// qualification, and an id qualified for a second observer.
func TestAmbiguousIdentity(t *testing.T) {
	cases := []struct {
		name   string
		field  string
		mutate func(q *Question)
	}{
		{"no observer", "identity.observer", func(q *Question) { q.Identity.Observer = "" }},
		{"no domain", "identity.domain", func(q *Question) { q.Identity.Domain = "" }},
		{"separator in the observer", "identity", func(q *Question) { q.Identity.Observer = "mead/2" }},
		{"unqualified id", "id", func(q *Question) { q.ID = "days-repeating" }},
		{"id qualified for another observer", "id", func(q *Question) { q.Identity.Observer = "boswell" }},
		{"id qualified for another domain", "id", func(q *Question) { q.Identity.Domain = "harness" }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			q := mustQuestion(t, "wren", "question.json")
			c.mutate(q)
			refusal(t, q.validate(), c.field)
		})
	}

	// The same id under two observers is two entries, not one: this is what
	// the qualification is for.
	nb := NewMemory()
	wren := mustQuestion(t, "wren", "question.json")
	harness := mustQuestion(t, "harness", "question.json")
	if localName(wren.ID) != localName(harness.ID) {
		t.Fatalf("the fixtures no longer share a local name: %q and %q", wren.ID, harness.ID)
	}
	if _, err := nb.Register(at(nb, wren.Identity, "wren"), wren); err != nil {
		t.Fatal(err)
	}
	if _, err := nb.Register(at(nb, harness.Identity, "harness"), harness); err != nil {
		t.Fatal(err)
	}
	// And one observer cannot register the same id twice.
	_, err := nb.Register(at(nb, wren.Identity, "wren-again"), wren)
	refusal(t, err, "id")
}

// Strict decoding, config.Load's rule: an unknown key is refused rather than
// dropped, since a field this binary does not understand must not vanish from a
// record that is never rewritten. Trailing content and an oversized payload are
// refused for the same reason the out/ manifest refuses them.
func TestStrictDecoding(t *testing.T) {
	valid := load(t, "wren", "question.json")
	if _, err := ValidateQuestion(valid); err != nil {
		t.Fatal(err)
	}

	unknown := strings.Replace(string(valid), `"speed"`, `"confidence": 0.9,
  "speed"`, 1)
	_, err := ValidateQuestion([]byte(unknown))
	refusal(t, err, "payload")

	_, err = ValidateQuestion(append(append([]byte{}, valid...), []byte("{}")...))
	refusal(t, err, "payload")

	_, err = ValidateQuestion(make([]byte, maxPayloadBytes+1))
	refusal(t, err, "payload")
}

// localName is the last segment of a qualified id, which is the part the two
// domains' fixtures deliberately share.
func localName(id string) string {
	parts := strings.Split(id, "/")
	return parts[len(parts)-1]
}
