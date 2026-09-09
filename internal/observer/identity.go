package observer

import "strings"

// idSeparator joins the three parts of a qualified id. Neither an observer nor
// a domain may contain it, or the id could not be split back apart.
const idSeparator = "/"

// Identity is what keeps two observers apart. Every entry and action id is
// qualified by domain and observer, so the harness observer and the Wren
// observer can hold a question of the same name, and write about the same
// ticket, without either one answering for the other.
type Identity struct {
	Observer string `json:"observer"`
	Domain   string `json:"domain"`
}

// Subject is the instance under observation. The instance is part of a
// reading's identity: two instances running the same code are not the same
// subject, and a baseline taken on one says nothing about the other.
type Subject struct {
	Kind     string `json:"kind"`
	Instance string `json:"instance"`
}

// Method is the procedure a reading was taken by, at the revision it was taken
// at. A number whose method changed between two windows measures the change in
// the method, which is why the revision travels with every reading.
type Method struct {
	Name     string `json:"name"`
	Revision string `json:"revision"`
}

// Qualify builds the stable id form, <domain>/<observer>/<name>.
func (id Identity) Qualify(name string) string {
	return id.Domain + idSeparator + id.Observer + idSeparator + name
}

func (id Identity) validate(field string) error {
	if id.Observer == "" {
		return fieldErr(field+".observer", "required")
	}
	if id.Domain == "" {
		return fieldErr(field+".domain", "required")
	}
	if strings.Contains(id.Observer, idSeparator) || strings.Contains(id.Domain, idSeparator) {
		return fieldErr(field, "observer and domain may not contain %q, which separates them in a qualified id", idSeparator)
	}
	return nil
}

// checkQualified holds an id to its owner's qualification. An unqualified id is
// ambiguous — two observers would collide on the same local name — and an id
// qualified for somebody else is a second observer claiming the first's entry,
// which is the same ambiguity from the other end.
func checkQualified(field, id string, owner Identity) error {
	if id == "" {
		return fieldErr(field, "required")
	}
	parts := strings.Split(id, idSeparator)
	if len(parts) != 3 || parts[2] == "" {
		return fieldErr(field, "%q is not qualified; ids read <domain>/<observer>/<name>", id)
	}
	if parts[0] != owner.Domain || parts[1] != owner.Observer {
		return fieldErr(field, "%q is qualified for %s/%s, claimed by %s/%s", id, parts[0], parts[1], owner.Domain, owner.Observer)
	}
	return nil
}

func (s Subject) validate(field string) error {
	if s.Kind == "" {
		return fieldErr(field+".kind", "required")
	}
	if s.Instance == "" {
		return fieldErr(field+".instance", "required: a subject without an instance cannot carry a baseline")
	}
	return nil
}

func (m Method) validate(field string) error {
	if m.Name == "" {
		return fieldErr(field+".name", "required")
	}
	if m.Revision == "" {
		return fieldErr(field+".revision", "required: a reading whose method has no revision cannot be compared with a later one")
	}
	return nil
}
