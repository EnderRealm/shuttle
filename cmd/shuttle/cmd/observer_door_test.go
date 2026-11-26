package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/spf13/pflag"

	"shuttle/internal/config"
	observercontract "shuttle/internal/observer"
)

// doorFixtures are the harness domain's observer/v1 payloads, the same files
// the contract package's validators and notebooks are exercised against.
const doorFixtures = "../../../internal/observer/testdata/harness"

var doorIdentity = observercontract.Identity{Domain: "harness", Observer: "shuttle"}

type syncBuffer struct {
	mu sync.Mutex
	b  strings.Builder
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.String()
}

// doorEnv is a running daemon serving the observer door for one held_notebook
// registration, harness/shuttle, and the notebook it holds for it.
type doorEnv struct {
	home   string
	socket string
	held   observercontract.Held
	logs   *syncBuffer
}

func startObserverDoor(t *testing.T) *doorEnv {
	t.Helper()
	return startObserverDoorWith(t, "")
}

// startObserverDoorWith is startObserverDoor whose registration, when
// tkConfig names a tk config, carries a ticket write scope over its shuttle
// project.
func startObserverDoorWith(t *testing.T, tkConfig string) *doorEnv {
	t.Helper()
	shuttleEnv(t)
	home := os.Getenv("SHUTTLE_HOME")
	// os.TempDir() rather than t.TempDir(): the socket path has to fit the
	// 104-byte sun_path limit, and t.TempDir() adds the test's name to it.
	root, err := os.MkdirTemp("", "door-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(root) })
	socket := filepath.Join(root, "observer", "door.sock")
	t.Setenv(config.EnvObserverSocket, socket)
	definition := filepath.Join(home, "definition.json")
	if err := os.WriteFile(definition, []byte(`{
  "contract_version": "observer-run/v1",
  "revision": "door-r1",
  "identity": {"domain": "harness", "observer": "shuttle"},
  "questions": [],
  "instructions": "Report only what the retained evidence supports."
}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if tkConfig == "" {
		writeConfig(t, fmt.Sprintf(`{"observers": {"fixture": {"definition": %q, "held_notebook": true}}}`, definition))
	} else {
		writeConfig(t, fmt.Sprintf(`{"paths": {"tk_config": %q}, "observers": {"fixture": {"definition": %q, "held_notebook": true,
  "tickets": {"projects": ["shuttle"], "discovery_project": "shuttle", "max_discoveries": 0}}}}`, tkConfig, definition))
	}

	logs := &syncBuffer{}
	ctx, cancel := context.WithCancel(context.Background())
	daemonCmd.SetErr(logs)
	daemonCmd.SetContext(ctx)
	done := make(chan error, 1)
	go func() { done <- daemonCmd.RunE(daemonCmd, nil) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("daemon: %v\n%s", err, logs.String())
		}
		daemonCmd.SetErr(nil)
		daemonCmd.SetContext(context.Background())
	})
	deadline := time.Now().Add(30 * time.Second)
	for !strings.Contains(logs.String(), "observer door: serving "+socket) {
		select {
		case err := <-done:
			done <- err
			t.Fatalf("daemon ended before serving the door: %v\n%s", err, logs.String())
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("the door was not served, log:\n%s", logs.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
	return &doorEnv{
		home:   home,
		socket: socket,
		held:   observercontract.Held{Dir: filepath.Join(home, "notebooks", "harness", "shuttle"), Identity: doorIdentity},
		logs:   logs,
	}
}

// execObserver drives `shuttle observer ...` through cobra with stdin as the
// payload source. Flag values outlive an execution, so they are put back to
// their defaults after each.
func execObserver(t *testing.T, stdin string, args ...string) (string, error) {
	t.Helper()
	var out bytes.Buffer
	rootCmd.SetOut(&out)
	rootCmd.SetIn(strings.NewReader(stdin))
	rootCmd.SetArgs(append([]string{"observer"}, args...))
	defer func() {
		rootCmd.SetOut(nil)
		rootCmd.SetIn(nil)
		rootCmd.SetArgs(nil)
		for _, sub := range observerCmd.Commands() {
			sub.Flags().VisitAll(func(f *pflag.Flag) {
				_ = f.Value.Set(f.DefValue)
				f.Changed = false
			})
		}
	}()
	err := rootCmd.Execute()
	return out.String(), err
}

// doorPayload is a harness fixture, edited.
func doorPayload(t *testing.T, file string, edit func(map[string]any)) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(doorFixtures, file))
	if err != nil {
		t.Fatal(err)
	}
	var payload map[string]any
	if err := json.Unmarshal(data, &payload); err != nil {
		t.Fatal(err)
	}
	if edit != nil {
		edit(payload)
	}
	out, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	return string(out)
}

// decodeAnswer decodes a command's stdout and requires its keys to be exactly
// keys: the answer carries what the criterion names and nothing more.
func decodeAnswer(t *testing.T, out string, keys ...string) map[string]any {
	t.Helper()
	var answer map[string]any
	if err := json.Unmarshal([]byte(out), &answer); err != nil {
		t.Fatalf("answer %q: %v", out, err)
	}
	if got := slices.Sorted(maps.Keys(answer)); !slices.Equal(got, slices.Sorted(slices.Values(keys))) {
		t.Fatalf("answer keys = %v, want exactly %v: %s", got, keys, out)
	}
	return answer
}

func (e *doorEnv) entry(t *testing.T, id string) *observercontract.Entry {
	t.Helper()
	entry, err := e.held.Entry(context.Background(), id)
	if err != nil {
		t.Fatalf("held entry %s: %v", id, err)
	}
	return entry
}

func (e *doorEnv) requireLog(t *testing.T, want string) {
	t.Helper()
	if !strings.Contains(e.logs.String(), want) {
		t.Fatalf("daemon log lacks %q:\n%s", want, e.logs.String())
	}
}

func (e *doorEnv) commits(t *testing.T) string {
	t.Helper()
	return git(t, e.held.Dir, "rev-list", "--count", "main")
}

func TestObserverRegisterRecordsAuthor(t *testing.T) {
	env := startObserverDoor(t)
	authors := []observercontract.Author{
		{Kind: observercontract.AuthorHuman, Ref: "steve"},
		{Kind: observercontract.AuthorWorker, Ref: "shuttle/vary-report-headline-1a2b"},
		{Kind: observercontract.AuthorObserver, Ref: "observer-20260907-043000-3b71"},
	}
	for i, author := range authors {
		id := doorIdentity.Qualify("asked-by-" + author.Kind)
		requestID := fmt.Sprintf("ask-%d", i)
		out, err := execObserver(t, doorPayload(t, "question.json", func(q map[string]any) {
			q["id"] = id
			q["author"] = map[string]any{"kind": author.Kind, "ref": author.Ref}
		}), "register", "--request-id", requestID)
		if err != nil {
			t.Fatalf("register as %s: %v\n%s", author.Kind, err, env.logs.String())
		}
		answer := decodeAnswer(t, out, "entry_id", "state")
		if answer["entry_id"] != id || answer["state"] != observercontract.StateObserving {
			t.Fatalf("answer = %v, want %s observing", answer, id)
		}
		if got := env.entry(t, id).Question.Author; got != author {
			t.Fatalf("recorded author = %+v, want %+v as declared", got, author)
		}
		env.requireLog(t, fmt.Sprintf("uid=%d operation=\"register\" request=%q entry=%q outcome=ok", os.Getuid(), requestID, id))
	}

	// No author is refused naming the field, never stamped with whoever
	// transported the payload.
	unauthored := doorIdentity.Qualify("unauthored")
	before := env.commits(t)
	_, err := execObserver(t, doorPayload(t, "question.json", func(q map[string]any) {
		q["id"] = unauthored
		delete(q, "author")
	}), "register", "--request-id", "ask-unauthored")
	if err == nil || !strings.Contains(err.Error(), "author.kind") {
		t.Fatalf("register with no author = %v, want a refusal naming author.kind", err)
	}
	if code := exitCode(err); code != 1 {
		t.Fatalf("exit = %d, want 1 for a payload the validators refuse", code)
	}
	if _, err := env.held.Entry(context.Background(), unauthored); !errors.Is(err, observercontract.ErrNoEntry) {
		t.Fatalf("unauthored entry = %v, want none", err)
	}
	if after := env.commits(t); after != before {
		t.Fatalf("commits %s -> %s, want the refusal to write nothing", before, after)
	}
	env.requireLog(t, `request="ask-unauthored" entry="" outcome=invalid exit=0 field="author.kind"`)
}

func TestObserverActivatePendingAndLive(t *testing.T) {
	env := startObserverDoor(t)
	id := doorIdentity.Qualify("days-repeating")
	if _, err := execObserver(t, doorPayload(t, "question.json", nil), "register", "--request-id", "ask"); err != nil {
		t.Fatal(err)
	}

	// Naming only a commit is pending: merged code is not running code.
	pending := filepath.Join(t.TempDir(), "experiment-pending.json")
	if err := os.WriteFile(pending, []byte(doorPayload(t, "experiment-pending.json", nil)), 0o600); err != nil {
		t.Fatal(err)
	}
	out, err := execObserver(t, "", "activate", pending, "--request-id", "pending")
	if err != nil {
		t.Fatalf("activate pending: %v", err)
	}
	answer := decodeAnswer(t, out, "entry_id", "state", "experiment_status")
	if answer["entry_id"] != id || answer["experiment_status"] != observercontract.ExperimentPending {
		t.Fatalf("answer = %v, want %s pending", answer, id)
	}
	entry := env.entry(t, id)
	if entry.ExperimentStatus != observercontract.ExperimentPending || entry.Experiment.Activation.At != "" ||
		entry.Experiment.Author.Kind != observercontract.AuthorWorker {
		t.Fatalf("entry = %s %+v, want the worker's pending registration with no activation filled in",
			entry.ExperimentStatus, entry.Experiment.Activation)
	}

	// Naming the restart that made it live is live, and replaces the pending
	// registration.
	out, err = execObserver(t, doorPayload(t, "experiment.json", nil), "activate", "--request-id", "live")
	if err != nil {
		t.Fatalf("activate live: %v", err)
	}
	answer = decodeAnswer(t, out, "entry_id", "state", "experiment_status")
	if answer["experiment_status"] != observercontract.ExperimentLive {
		t.Fatalf("answer = %v, want live", answer)
	}
	entry = env.entry(t, id)
	if entry.ExperimentStatus != observercontract.ExperimentLive || entry.Experiment.Activation.Kind != observercontract.ActivationRestart {
		t.Fatalf("entry = %s %+v, want the live restart", entry.ExperimentStatus, entry.Experiment.Activation)
	}

	// Naming nothing is not pending: it is refused, not defaulted.
	_, err = execObserver(t, doorPayload(t, "experiment-pending.json", func(e map[string]any) {
		delete(e, "activation")
	}), "activate", "--request-id", "nothing")
	if err == nil || !strings.Contains(err.Error(), "observer activation:") {
		t.Fatalf("activate naming nothing = %v, want a refusal naming activation", err)
	}
}

func TestObserverRegisterReplay(t *testing.T) {
	env := startObserverDoor(t)
	question := doorPayload(t, "question.json", nil)
	first, err := execObserver(t, question, "register", "--request-id", "ask")
	if err != nil {
		t.Fatal(err)
	}
	// Another write moves the notebook past the revision the first call was
	// applied at; the caller never sent one, and the replay is answered from
	// the request log ahead of any revision.
	if _, err := execObserver(t, doorPayload(t, "question.json", func(q map[string]any) {
		q["id"] = doorIdentity.Qualify("headline-length")
	}), "register", "--request-id", "ask-other"); err != nil {
		t.Fatal(err)
	}
	before := env.commits(t)
	replay, err := execObserver(t, question, "register", "--request-id", "ask")
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if replay != first {
		t.Fatalf("replay answered %q, want the original %q", replay, first)
	}
	if after := env.commits(t); after != before {
		t.Fatalf("commits %s -> %s, want a replay to write nothing", before, after)
	}

	_, err = execObserver(t, doorPayload(t, "question.json", func(q map[string]any) {
		q["question"] = "Are the daily reports repeating last week's headlines?"
	}), "register", "--request-id", "ask")
	if err == nil || !strings.Contains(err.Error(), "the request id was applied with different content") {
		t.Fatalf("changed content under the same id = %v, want a request conflict", err)
	}
	if code := exitCode(err); code != 14 {
		t.Fatalf("exit = %d, want §8's 14 for a request conflict", code)
	}
	if got := env.entry(t, doorIdentity.Qualify("days-repeating")).Question.Question; got != "Are the daily reports repeating?" {
		t.Fatalf("question = %q, want the original kept", got)
	}
	if after := env.commits(t); after != before {
		t.Fatalf("commits %s -> %s, want a conflict to write nothing", before, after)
	}
	env.requireLog(t, `request="ask" entry="harness/shuttle/days-repeating" outcome=refused exit=14 field="request.request_id"`)
}

func TestObserverRetireThroughShuttle(t *testing.T) {
	ctx := context.Background()
	env := startObserverDoor(t)
	id := doorIdentity.Qualify("days-repeating")
	if _, err := execObserver(t, doorPayload(t, "question.json", nil), "register", "--request-id", "ask"); err != nil {
		t.Fatal(err)
	}
	// The observer's run has a ticket note in flight when the retirement
	// lands.
	action, err := observercontract.ValidateAction([]byte(doorPayload(t, "action.json", nil)))
	if err != nil {
		t.Fatal(err)
	}
	revision, err := env.held.Revision(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := env.held.Claim(ctx, observercontract.Request{Identity: doorIdentity, Revision: revision, RequestID: "claim"}, action); err != nil {
		t.Fatal(err)
	}

	_, err = execObserver(t, "", "retire", id, "--request-id", "retire-silently")
	if err == nil || !strings.Contains(err.Error(), "observer reason:") {
		t.Fatalf("retire without a reason = %v, want a refusal naming reason", err)
	}
	if state := env.entry(t, id).State; state != observercontract.StateObserving {
		t.Fatalf("state = %s after a refused retirement, want observing", state)
	}

	out, err := execObserver(t, "", "retire", id, "--reason", "the prompt was rewritten; the question no longer applies", "--request-id", "retire")
	if err != nil {
		t.Fatalf("retire: %v", err)
	}
	answer := decodeAnswer(t, out, "entry_id", "reason", "in_flight")
	inFlight, _ := answer["in_flight"].([]any)
	if answer["entry_id"] != id || len(inFlight) != 1 || inFlight[0] != action.ID {
		t.Fatalf("answer = %v, want %s with %s in flight", answer, id, action.ID)
	}
	entry := env.entry(t, id)
	if entry.State != observercontract.StateRetired || entry.Retirement.Reason != "the prompt was rewritten; the question no longer applies" {
		t.Fatalf("entry = %s %+v, want retired with the reason given", entry.State, entry.Retirement)
	}
	env.requireLog(t, fmt.Sprintf("uid=%d operation=\"retire\" request=%q entry=%q outcome=ok", os.Getuid(), "retire", id))

	// Off the list: an experiment is no longer admitted against it.
	_, err = execObserver(t, doorPayload(t, "experiment.json", nil), "activate", "--request-id", "late")
	if code := exitCode(err); err == nil || code != 11 {
		t.Fatalf("activate after retirement = %v (exit %d), want §8's 11", err, code)
	}
}

func TestObserverReopenThroughShuttle(t *testing.T) {
	ctx := context.Background()
	env := startObserverDoor(t)
	id := doorIdentity.Qualify("days-repeating")
	if _, err := execObserver(t, doorPayload(t, "question.json", nil), "register", "--request-id", "ask"); err != nil {
		t.Fatal(err)
	}
	if _, err := execObserver(t, doorPayload(t, "experiment.json", nil), "activate", "--request-id", "live"); err != nil {
		t.Fatal(err)
	}
	assessment, err := observercontract.ValidateAssessment([]byte(doorPayload(t, "assessment.json", nil)))
	if err != nil {
		t.Fatal(err)
	}
	revision, err := env.held.Revision(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := env.held.Record(ctx, observercontract.Request{Identity: doorIdentity, Revision: revision, RequestID: "record"}, assessment); err != nil {
		t.Fatal(err)
	}

	_, err = execObserver(t, "", "reopen", id, "--request-id", "reopen-bare")
	if err == nil || !strings.Contains(err.Error(), "observer condition:") {
		t.Fatalf("reopen without a condition = %v, want a refusal naming condition", err)
	}
	out, err := execObserver(t, "", "reopen", id, "--condition", observercontract.ReopenContradicted, "--request-id", "reopen")
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	answer := decodeAnswer(t, out, "entry_id", "state")
	if answer["entry_id"] != id || answer["state"] != observercontract.StateObserving {
		t.Fatalf("answer = %v, want %s observing", answer, id)
	}
	if state := env.entry(t, id).State; state != observercontract.StateObserving {
		t.Fatalf("state = %s, want observing", state)
	}
}

func TestObserverRegisterDisclosesNothingElse(t *testing.T) {
	ctx := context.Background()
	env := startObserverDoor(t)
	// The notebook already holds another question with a live experiment
	// and an action in flight.
	held := env.held
	question, err := observercontract.ValidateQuestion([]byte(doorPayload(t, "question.json", nil)))
	if err != nil {
		t.Fatal(err)
	}
	experiment, err := observercontract.ValidateExperiment([]byte(doorPayload(t, "experiment.json", nil)))
	if err != nil {
		t.Fatal(err)
	}
	action, err := observercontract.ValidateAction([]byte(doorPayload(t, "action.json", nil)))
	if err != nil {
		t.Fatal(err)
	}
	request := func(id string, revision int64) observercontract.Request {
		return observercontract.Request{Identity: doorIdentity, Revision: revision, RequestID: id}
	}
	if _, err := held.Register(ctx, request("seed-ask", 0), question); err != nil {
		t.Fatal(err)
	}
	if _, err := held.Activate(ctx, request("seed-activate", 1), experiment); err != nil {
		t.Fatal(err)
	}
	if _, err := held.Claim(ctx, request("seed-claim", 2), action); err != nil {
		t.Fatal(err)
	}

	id := doorIdentity.Qualify("headline-length")
	text := "Are the daily reports' headlines getting longer?"
	payload := doorPayload(t, "question.json", func(q map[string]any) {
		q["id"] = id
		q["question"] = text
	})
	out, err := execObserver(t, payload, "register", "--request-id", "ask")
	if err != nil {
		t.Fatal(err)
	}
	decodeAnswer(t, out, "entry_id", "state")
	for _, other := range []string{question.ID, action.ID, text, "revision", "observations", "author"} {
		if strings.Contains(out, other) {
			t.Fatalf("answer %s carries %q from the notebook", out, other)
		}
	}

	pending := doorPayload(t, "experiment-pending.json", func(e map[string]any) { e["question_id"] = id })
	out, err = execObserver(t, pending, "activate", "--request-id", "activate")
	if err != nil {
		t.Fatal(err)
	}
	decodeAnswer(t, out, "entry_id", "state", "experiment_status")

	// The door's own envelope around the answer carries nothing more either.
	var call observercontract.DoorCall
	call.Operation = observercontract.DoorRegister
	call.Request.RequestID = "ask"
	call.Payload = json.RawMessage(payload)
	resp, err := observercontract.CallDoor(ctx, env.socket, call)
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := json.Marshal(resp)
	if err != nil {
		t.Fatal(err)
	}
	decodeAnswer(t, string(envelope), "status", "answer")
}

func TestObserverDoorWithholdsStoredValues(t *testing.T) {
	env := startObserverDoor(t)
	id := doorIdentity.Qualify("days-repeating")
	if _, err := execObserver(t, doorPayload(t, "question.json", nil), "register", "--request-id", "ask"); err != nil {
		t.Fatal(err)
	}
	refused := func(name, field string, err error, stored ...string) {
		t.Helper()
		if err == nil || !strings.Contains(err.Error(), "observer "+field+":") {
			t.Fatalf("%s = %v, want a refusal naming %s", name, err, field)
		}
		for _, value := range stored {
			if strings.Contains(err.Error(), value) {
				t.Fatalf("%s refusal %q carries %q from the notebook", name, err, value)
			}
		}
	}

	// An experiment about another subject is refused without saying which
	// subject the stored question is about; the daemon log says it.
	_, err := execObserver(t, doorPayload(t, "experiment-pending.json", func(e map[string]any) {
		e["evidence"].(map[string]any)["baseline"].(map[string]any)["subject"] = map[string]any{"kind": "service", "instance": "nightly"}
	}), "activate", "--request-id", "probe-subject")
	refused("activate on another subject", "evidence.baseline.subject", err, "agent", "daily")
	env.requireLog(t, "the question is about agent daily")

	// Nor which state the entry is in.
	_, err = execObserver(t, "", "reopen", id, "--condition", observercontract.ReopenContradicted, "--request-id", "probe-state")
	refused("reopen of an observing question", "state", err, observercontract.StateObserving)

	// Nor what experiment it already carries.
	if _, err := execObserver(t, doorPayload(t, "experiment.json", nil), "activate", "--request-id", "live"); err != nil {
		t.Fatal(err)
	}
	_, err = execObserver(t, doorPayload(t, "experiment.json", nil), "activate", "--request-id", "probe-live")
	refused("a second live activation", "activation", err, observercontract.ExperimentLive)

	// An identity Shuttle holds no notebook for is refused without the
	// operator's registrations behind the lookup.
	_, err = execObserver(t, doorPayload(t, "question.json", func(q map[string]any) {
		q["identity"] = map[string]any{"domain": "harness", "observer": "elsewhere"}
		q["id"] = "harness/elsewhere/days-repeating"
	}), "register", "--request-id", "probe-identity")
	refused("an identity no registration names", "identity", err, env.home)
}

// TestObserverDoorClientHelper is the shuttle CLI in a process of its own,
// for TestObserverRegisterNeedsNoStoreAccess. Run directly it does nothing.
func TestObserverDoorClientHelper(t *testing.T) {
	var args []string
	for i, arg := range os.Args {
		if arg == "--" {
			args = os.Args[i+1:]
			break
		}
	}
	if len(args) == 0 {
		return
	}
	rootCmd.SetArgs(args)
	if err := rootCmd.Execute(); err != nil {
		reportError(os.Stderr, err)
		os.Exit(exitCode(err))
	}
	os.Exit(0)
}

func TestObserverRegisterNeedsNoStoreAccess(t *testing.T) {
	env := startObserverDoor(t)
	stateRoot, err := filepath.EvalSymlinks(env.home)
	if err != nil {
		t.Fatal(err)
	}
	// The client runs where the kernel refuses it every file under the state
	// root, the notebooks included, as a hosted agent user is refused the
	// operator's home. Its environment names that root, so a command that
	// reached for config.json or a notebook would fail rather than find
	// another.
	profile := fmt.Sprintf(`(version 1)(allow default)(deny file* (subpath %q))`, stateRoot)
	probe := exec.Command("/usr/bin/sandbox-exec", "-p", profile, "/bin/ls", filepath.Join(stateRoot, "notebooks"))
	if out, err := probe.CombinedOutput(); err == nil {
		t.Fatalf("the profile does not deny the notebooks root: %s", out)
	}
	if _, err := os.Stat(env.held.Dir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("notebook %s before the call: %v, want none yet", env.held.Dir, err)
	}

	client := exec.Command("/usr/bin/sandbox-exec", "-p", profile,
		os.Args[0], "-test.run=^TestObserverDoorClientHelper$", "--",
		"observer", "register", "--request-id", "from-a-slot")
	client.Env = []string{
		"HOME=" + stateRoot,
		config.EnvHome + "=" + stateRoot,
		config.EnvObserverSocket + "=" + env.socket,
		"PATH=/usr/bin:/bin",
	}
	client.Stdin = strings.NewReader(doorPayload(t, "question.json", func(q map[string]any) {
		q["author"] = map[string]any{"kind": observercontract.AuthorWorker, "ref": "shuttle/vary-report-headline-1a2b"}
	}))
	var stdout, stderr bytes.Buffer
	client.Stdout = &stdout
	client.Stderr = &stderr
	if err := client.Run(); err != nil {
		t.Fatalf("sandboxed register: %v\nstderr: %s\nlog:\n%s", err, stderr.String(), env.logs.String())
	}
	answer := decodeAnswer(t, strings.TrimSpace(stdout.String()), "entry_id", "state")
	id := doorIdentity.Qualify("days-repeating")
	if answer["entry_id"] != id {
		t.Fatalf("answer = %v, want %s", answer, id)
	}
	// The daemon wrote it, into the notebook the client could not open.
	if got := env.entry(t, id).Question.Author; got.Kind != observercontract.AuthorWorker {
		t.Fatalf("author = %+v, want the worker the payload declared", got)
	}
	if count := env.commits(t); count != "1" {
		t.Fatalf("commits = %s, want the one registration", count)
	}
	env.requireLog(t, fmt.Sprintf("uid=%d operation=\"register\" request=%q entry=%q outcome=ok", os.Getuid(), "from-a-slot", id))
}
