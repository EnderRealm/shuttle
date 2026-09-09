package observer

// Author kinds: who wrote a record. Human is a person writing directly, worker
// an agent run declaring what the change it just landed should do, observer the
// observer proposing its own. The vocabulary is fixed because the three carry
// different authority, and a reader who cannot tell them apart reads a model's
// proposal as a person's decision.
const (
	AuthorHuman    = "human"
	AuthorWorker   = "worker"
	AuthorObserver = "observer"
)

// Author is who a record came from. Ref is free text and optional — a person's
// name, a ticket id, a run id — because what identifies an author depends on
// which kind it is, and a shape imposed here would only be guessed at by the
// two thirds of authors it did not fit.
type Author struct {
	Kind string `json:"kind"`
	Ref  string `json:"ref,omitempty"`
}

func (a Author) validate(field string) error {
	// An omitted author object and one carrying no kind decode to the same
	// zero value, so both take the required message: a record whose author was
	// never stated is not one the notebook can stamp with the observer's.
	if a.Kind == "" {
		return fieldErr(field+".kind", "required: one of %s, %s, %s", AuthorHuman, AuthorWorker, AuthorObserver)
	}
	switch a.Kind {
	case AuthorHuman, AuthorWorker, AuthorObserver:
		return nil
	}
	return fieldErr(field+".kind", "%q, one of %s, %s, %s", a.Kind, AuthorHuman, AuthorWorker, AuthorObserver)
}

// clone copies an author. Both fields are values, so the struct copy is the
// deep one; the method exists so the notebook's copy-in, copy-out rule reads
// the same at every call site.
func (a Author) clone() Author { return a }
