package weftremote

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"unicode/utf8"

	"shuttle/internal/ledger"
)

// choiceQuestion is one of a structured park's questions (§6.5), the worker
// outcome block's blocked form. MultiSelect is required: a missing one is
// refused, not read as false.
type choiceQuestion struct {
	Question    string         `json:"question"`
	Header      string         `json:"header"`
	MultiSelect *bool          `json:"multiSelect"`
	Options     []choiceOption `json:"options"`
}

type choiceOption struct {
	Label       string `json:"label"`
	Description string `json:"description"`
}

// choiceAnswer is a structured answer's entry for one question. Selected is
// required: a missing or null one is refused, not read as empty.
type choiceAnswer struct {
	Header   string   `json:"header"`
	Selected []string `json:"selected"`
	Other    *string  `json:"other"`
	Notes    *string  `json:"notes"`
}

// scalars is whether s is lo to hi Unicode scalar values long, the unit
// every structured bound counts in.
func scalars(s string, lo, hi int) bool {
	n := utf8.RuneCountInString(s)
	return n >= lo && n <= hi
}

// checkQuestions holds a structured park to §6.5's bounds. Its failures name
// positions only, never what the worker wrote: they reach the log, the
// segment's ledger row and the run's detail.
func checkQuestions(qs []choiceQuestion) error {
	if len(qs) < 1 || len(qs) > 4 {
		return fmt.Errorf("questions carries %d questions, want 1 to 4", len(qs))
	}
	headers := map[string]bool{}
	for i, q := range qs {
		if !scalars(q.Question, 1, 1000) || !strings.HasSuffix(q.Question, "?") {
			return fmt.Errorf("questions[%d].question must be 1 to 1000 characters ending in ?", i)
		}
		if !scalars(q.Header, 1, 12) || headers[q.Header] {
			return fmt.Errorf("questions[%d].header must be 1 to 12 characters, unique within the park", i)
		}
		headers[q.Header] = true
		if q.MultiSelect == nil {
			return fmt.Errorf("questions[%d] has no multiSelect", i)
		}
		if len(q.Options) < 2 || len(q.Options) > 4 {
			return fmt.Errorf("questions[%d] carries %d options, want 2 to 4", i, len(q.Options))
		}
		labels := map[string]bool{}
		for j, o := range q.Options {
			if !scalars(o.Label, 1, 60) || labels[o.Label] || strings.EqualFold(o.Label, "other") {
				return fmt.Errorf("questions[%d].options[%d].label must be 1 to 60 characters, unique within the question and not Other", i, j)
			}
			labels[o.Label] = true
			if !scalars(o.Description, 1, 300) {
				return fmt.Errorf("questions[%d].options[%d].description must be 1 to 300 characters", i, j)
			}
		}
	}
	return nil
}

// checkAnswers holds a structured answer to the questions it answers: one
// entry per question, in question order, each selecting only its question's
// options (§6.5). Its failures name positions, never a label or text.
func checkAnswers(qs []choiceQuestion, as []choiceAnswer) error {
	if len(as) != len(qs) {
		return fmt.Errorf("answers carries %d entries for %d questions", len(as), len(qs))
	}
	for i, a := range as {
		q := qs[i]
		if a.Header != q.Header {
			return fmt.Errorf("answers[%d].header is not its question's header", i)
		}
		if a.Selected == nil {
			return fmt.Errorf("answers[%d] has no selected", i)
		}
		for j, label := range a.Selected {
			if slices.Contains(a.Selected[:j], label) {
				return fmt.Errorf("answers[%d].selected names a label twice", i)
			}
			if !slices.ContainsFunc(q.Options, func(o choiceOption) bool { return o.Label == label }) {
				return fmt.Errorf("answers[%d].selected[%d] is not an option of its question", i, j)
			}
		}
		if len(a.Selected) > 1 && !*q.MultiSelect {
			return fmt.Errorf("answers[%d] selects %d options of a question that is not multiSelect", i, len(a.Selected))
		}
		if a.Other != nil && !scalars(*a.Other, 1, 1000) {
			return fmt.Errorf("answers[%d].other must be null or 1 to 1000 characters", i)
		}
		if a.Notes != nil && !scalars(*a.Notes, 1, 1000) {
			return fmt.Errorf("answers[%d].notes must be null or 1 to 1000 characters", i)
		}
		if len(a.Selected) == 0 && a.Other == nil {
			return fmt.Errorf("answers[%d] selects no option and carries no other", i)
		}
	}
	return nil
}

// answerFor checks an answer request's form against the question it answers
// — answers for a structured question, answer for a legacy one — and a
// structured answer against the question's options. It returns the answers
// as the ledger records them, nil for a legacy answer; any refusal is
// malformed and comes before anything is recorded.
func answerFor(q *ledger.WeftQuestion, answers []choiceAnswer) (json.RawMessage, error) {
	switch {
	case q.Questions == nil && answers != nil:
		return nil, &ledger.WeftRefusal{Code: "malformed", Message: q.ID + " is asked as free text and takes answer, not answers"}
	case q.Questions != nil && answers == nil:
		return nil, &ledger.WeftRefusal{Code: "malformed", Message: q.ID + " is asked as structured questions and takes answers, not answer"}
	case answers == nil:
		return nil, nil
	}
	var qs []choiceQuestion
	if err := json.Unmarshal(q.Questions, &qs); err != nil {
		return nil, err
	}
	if err := checkAnswers(qs, answers); err != nil {
		return nil, &ledger.WeftRefusal{Code: "malformed", Message: err.Error()}
	}
	return json.Marshal(answers)
}
