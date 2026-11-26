package observer

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/user"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

// The door's operations: the ways an author reaches a notebook Shuttle holds
// (docs/observer-contract.md §12). Observe, correct and carry each record one
// observation of their own kind; nothing else in the notebook is reachable
// through it.
const (
	DoorRegister = "register"
	DoorActivate = "activate"
	DoorRetire   = "retire"
	DoorReopen   = "reopen"
	DoorObserve  = "observe"
	DoorCorrect  = "correct"
	DoorCarry    = "carry"
)

// doorKinds is the one observation kind each recording operation records.
var doorKinds = map[string]string{
	DoorObserve: ObservationPerson,
	DoorCorrect: ObservationCorrection,
	DoorCarry:   ObservationCarried,
}

// DoorReleaseNone is the release an observe answers when its observation
// authorizes none. The others are the receipt outcomes.
const DoorReleaseNone = "none"

// Door response statuses. Refused carries §8's exit status for the sentinel
// refusal; invalid is a payload or call the validators refused, naming the
// field; failed is the door or the notebook's storage, not the caller.
const (
	DoorOK      = "ok"
	DoorRefused = "refused"
	DoorInvalid = "invalid"
	DoorFailed  = "failed"
)

const (
	// maxDoorCallBytes is one payload at its cap and the envelope around it.
	maxDoorCallBytes = maxPayloadBytes + 16<<10
	// maxDoorResponseBytes bounds what a client reads; an answer is an id, a
	// state and at most a retirement's in-flight ids.
	maxDoorResponseBytes = 1 << 20
	// doorIOTimeout bounds reading the call and writing the answer, so a
	// caller that never half-closes cannot hold a handler open. The operation
	// between them is bounded by Held's own git timeout.
	doorIOTimeout = 30 * time.Second
	// doorRevisionAttempts bounds re-reading the revision when a concurrent
	// writer moved the notebook between the read and the mutation. A retry is
	// safe because the request id is checked before the revision.
	doorRevisionAttempts = 3
	// doorAcceptBackoff and doorAcceptBackoffCap pace retries of a failed
	// accept, as net/http's server paces its own.
	doorAcceptBackoff    = 5 * time.Millisecond
	doorAcceptBackoffCap = time.Second
)

// DoorCall is one request on the door: one JSON object written to the socket,
// then the write side closed. Request carries only the request id: Shuttle
// supplies the revision, and the identity is the payload's or the entry id's,
// so a call naming either is refused as an unknown field.
type DoorCall struct {
	Operation string          `json:"operation"`
	Request   DoorRequest     `json:"request"`
	Payload   json.RawMessage `json:"payload,omitempty"`
	EntryID   string          `json:"entry_id,omitempty"`
	Reason    string          `json:"reason,omitempty"`
	Condition string          `json:"condition,omitempty"`
}

type DoorRequest struct {
	RequestID string `json:"request_id"`
}

// DoorResponse is the door's one JSON object in reply. Answer is a DoorEntry,
// or a DoorRetirement for a retire, and is present only when Status is ok.
type DoorResponse struct {
	Status string          `json:"status"`
	Exit   int             `json:"exit,omitempty"`
	Field  string          `json:"field,omitempty"`
	Error  string          `json:"error,omitempty"`
	Answer json.RawMessage `json:"answer,omitempty"`
}

// DoorEntry is the whole answer to a register, activate, reopen or recorded
// observation. Watching is one direction: the caller learns the id and state
// of the entry it wrote and nothing else the notebook holds — no
// observations, no other entry, no revision. An observe adds what became of
// the release it may authorize.
type DoorEntry struct {
	EntryID          string       `json:"entry_id"`
	State            string       `json:"state"`
	ExperimentStatus string       `json:"experiment_status,omitempty"`
	Release          *DoorRelease `json:"release,omitempty"`
}

// DoorRelease is what became of the release a person's observation may
// authorize: applied, refused or failed with the reason, or none when the
// observation authorizes none. Ticket is the ticket it was decided on.
type DoorRelease struct {
	Status string `json:"status"`
	Ticket string `json:"ticket,omitempty"`
	Reason string `json:"reason,omitempty"`
}

// DoorRetirement is the answer to a retire: the entry, the reason recorded and
// the actions already in flight, which the retirement reports rather than
// swallows (§6).
type DoorRetirement struct {
	EntryID  string   `json:"entry_id"`
	Reason   string   `json:"reason"`
	InFlight []string `json:"in_flight"`
}

// Door serves its operations to callers that cannot open the notebook's
// files: a worker in a hosted slot, a /work run, a person. Notebook resolves
// the notebook Shuttle holds for an identity, refusing one no registration
// holds; Log receives one line per request.
//
// Release applies the release a person's effect-observed authorizes (§11),
// called after the observe is recorded — a replay included, so a release that
// failed is tried again by repeating the call — with the entry as recorded,
// the observation and the request id. It lives outside the contract package
// because the ticket store is the runner's; nil applies none and says so.
//
// Push sends the notebook for identity off the machine after a write the door
// applied — a replay included, so a push that failed is tried again by the
// next write or the next run — returning Held.Push's outcome or the runner's
// own. It runs after the answer is written: the write stands whatever the
// push does, and the caller never waits on the remote. Its outcome is logged
// and never answered. nil pushes nothing.
//
// Operator is the uid observe and correct are applied for, the operator's
// own: a person's observation carries release authority, and the door is
// reachable by every hosted agent user, so the author a payload declares
// cannot be what makes it a person's. Every other operation takes the peer
// uid as a record only.
type Door struct {
	Notebook func(Identity) (Held, error)
	Release  func(entry *Entry, observation *Assessment, requestID string) DoorRelease
	Push     func(ctx context.Context, identity Identity) (string, error)
	Operator int
	Log      *log.Logger
}

// ListenDoor binds the door's socket at path. Its directory is created if
// missing, must be the operator's own and not a symlink, and is set 0750; the
// socket is set 0660. Both take the directory's group, so under the hosted
// shared root a hosted agent user can connect without reaching the operator's
// home. A socket left by a daemon that died is replaced; one that still
// answers is another Shuttle serving, and is refused.
func ListenDoor(path string) (*net.UnixListener, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return nil, fmt.Errorf("observer door: socket %q must be an absolute clean path", path)
	}
	dir := filepath.Dir(path)
	if err := os.Mkdir(dir, 0o750); err != nil && !errors.Is(err, os.ErrExist) {
		return nil, fmt.Errorf("observer door: %w", err)
	}
	info, err := os.Lstat(dir)
	if err != nil {
		return nil, fmt.Errorf("observer door: %w", err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("observer door: %s is not a directory", dir)
	}
	if stat, ok := info.Sys().(*syscall.Stat_t); !ok || int(stat.Uid) != os.Getuid() {
		return nil, fmt.Errorf("observer door: %s is not owned by the operator", dir)
	}
	// Mkdir is subject to the umask, and a directory left from before may
	// carry anything; 0750 is what lets the group traverse and nobody else.
	if err := os.Chmod(dir, 0o750); err != nil {
		return nil, fmt.Errorf("observer door: %w", err)
	}
	if info, err := os.Lstat(path); err == nil {
		if info.Mode().Type() != os.ModeSocket {
			return nil, fmt.Errorf("observer door: %s exists and is not a socket", path)
		}
		if conn, err := net.Dial("unix", path); err == nil {
			conn.Close()
			return nil, fmt.Errorf("observer door: %s is already served", path)
		}
		if err := os.Remove(path); err != nil {
			return nil, fmt.Errorf("observer door: %w", err)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("observer door: %w", err)
	}
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		return nil, fmt.Errorf("observer door: %w", err)
	}
	// connect(2) needs write permission on the socket. Until this chmod the
	// umask's mode leaves the group without it, which is narrower, not wider.
	if err := os.Chmod(path, 0o660); err != nil {
		listener.Close()
		return nil, fmt.Errorf("observer door: %w", err)
	}
	return listener, nil
}

// Serve answers calls on listener until ctx is done, then waits for the calls
// in hand. An operation runs to completion past ctx: a notebook commit cut off
// part-way is rolled back, and the caller would replay it anyway.
func (d Door) Serve(ctx context.Context, listener *net.UnixListener) error {
	path := listener.Addr().String()
	d.Log.Printf("observer door: serving %s (group %s)", path, socketGroup(path))
	stop := context.AfterFunc(ctx, func() { listener.Close() })
	defer stop()
	var handlers sync.WaitGroup
	var backoff time.Duration
	for {
		conn, err := listener.AcceptUnix()
		if err != nil {
			if ctx.Err() == nil && !errors.Is(err, net.ErrClosed) {
				// A failed accept, such as one out of descriptors, says
				// nothing about the next; net/http waits and retries the
				// same way rather than ending the server.
				backoff = min(max(2*backoff, doorAcceptBackoff), doorAcceptBackoffCap)
				d.Log.Printf("observer door: accept: %v; retrying in %s", err, backoff)
				time.Sleep(backoff)
				continue
			}
			handlers.Wait()
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("observer door: %w", err)
		}
		backoff = 0
		handlers.Add(1)
		go func() {
			defer handlers.Done()
			d.handle(context.WithoutCancel(ctx), conn)
		}()
	}
}

func socketGroup(path string) string {
	info, err := os.Stat(path)
	if err != nil {
		return "unknown: " + err.Error()
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return "unknown"
	}
	gid := strconv.FormatUint(uint64(stat.Gid), 10)
	if group, err := user.LookupGroupId(gid); err == nil {
		return group.Name
	}
	return gid
}

func (d Door) handle(ctx context.Context, conn *net.UnixConn) {
	defer conn.Close()
	var (
		call    DoorCall
		entryID string
		resp    DoorResponse
		detail  error
	)
	peer := "unknown"
	uid, err := peerUID(conn)
	if err == nil {
		peer = strconv.Itoa(uid)
		call, err = readDoorCall(conn)
		if err == nil {
			entryID, resp, detail = d.apply(ctx, call, uid)
		} else {
			resp = doorFailure(err)
		}
	} else {
		// The log line is the record of who wrote what; a call that cannot
		// be attributed is not applied.
		resp = doorFailure(fmt.Errorf("peer credentials: %w", err))
	}
	data, err := json.Marshal(resp)
	if err == nil {
		_ = conn.SetWriteDeadline(time.Now().Add(doorIOTimeout))
		_, err = conn.Write(append(data, '\n'))
	}
	// Operation, request id and entry id are the caller's strings, quoted so
	// none can forge a line or a field of its own.
	line := fmt.Sprintf("observer door: uid=%s operation=%q request=%q entry=%q outcome=%s",
		peer, call.Operation, call.Request.RequestID, entryID, resp.Status)
	if resp.Status != DoorOK {
		reported := resp.Error
		if detail != nil {
			reported = detail.Error()
		}
		line += fmt.Sprintf(" exit=%d field=%q error=%q", resp.Exit, resp.Field, reported)
	}
	if err != nil {
		// The operation's outcome stands; the caller replays under the same
		// request id to learn it.
		line += fmt.Sprintf(" reply=%q", err.Error())
	}
	d.Log.Print(line)
	if resp.Status == DoorOK {
		// The caller reads to EOF, so the connection is closed before the
		// push: a slow remote must not hold the answer. The deferred Close
		// is then a no-op.
		conn.Close()
		d.push(ctx, call.Request.RequestID, entryID)
	}
}

// push pushes the notebook an applied write went to and logs the outcome. The
// entry id of an applied write is qualified for that notebook's identity: the
// notebook refused it otherwise.
func (d Door) push(ctx context.Context, requestID, entryID string) {
	if d.Push == nil {
		return
	}
	status := ""
	identity, err := entryIdentity(entryID)
	if err == nil {
		status, err = d.Push(ctx, identity)
	}
	if err != nil {
		d.Log.Printf("observer door: request=%q entry=%q push=%s error=%q", requestID, entryID, HeldPushFailed, err.Error())
		return
	}
	d.Log.Printf("observer door: request=%q entry=%q push=%s", requestID, entryID, status)
}

func readDoorCall(conn *net.UnixConn) (DoorCall, error) {
	var call DoorCall
	_ = conn.SetReadDeadline(time.Now().Add(doorIOTimeout))
	data, err := io.ReadAll(io.LimitReader(conn, maxDoorCallBytes+1))
	if err != nil {
		return call, fmt.Errorf("read call: %w", err)
	}
	if len(data) > maxDoorCallBytes {
		return call, fieldErr("call", "more than %d bytes", maxDoorCallBytes)
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&call); err != nil {
		return DoorCall{}, &Error{Field: "call", Err: err}
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return DoorCall{}, fieldErr("call", "trailing content after the object")
	}
	return call, nil
}

// doorTakes is the field each operation reads beyond the request id. A call
// carrying another is refused rather than ignored, so a caller cannot believe
// a field it sent was applied.
var doorTakes = map[string][]string{
	DoorRegister: {"payload"},
	DoorActivate: {"payload"},
	DoorRetire:   {"entry_id", "reason"},
	DoorReopen:   {"entry_id", "condition"},
	DoorObserve:  {"payload"},
	DoorCorrect:  {"payload"},
	DoorCarry:    {"payload"},
}

func (c DoorCall) check() error {
	takes, ok := doorTakes[c.Operation]
	if !ok {
		return fieldErr("operation", "%q, one of %s, %s, %s, %s, %s, %s, %s", c.Operation,
			DoorRegister, DoorActivate, DoorRetire, DoorReopen, DoorObserve, DoorCorrect, DoorCarry)
	}
	for _, carried := range []struct {
		field string
		set   bool
	}{
		{"payload", len(c.Payload) != 0},
		{"entry_id", c.EntryID != ""},
		{"reason", c.Reason != ""},
		{"condition", c.Condition != ""},
	} {
		if carried.set && !slices.Contains(takes, carried.field) {
			return fieldErr(carried.field, "not taken by %s", c.Operation)
		}
	}
	// The notebook refuses these too, but what it says about a refusal is
	// withheld from the caller (notebookFailure); an absent field is the
	// caller's own to be told about in full.
	if c.Request.RequestID == "" {
		return fieldErr("request.request_id", "required: it is what makes a repeat of the call safe")
	}
	if c.Operation == DoorRetire && c.Reason == "" {
		return fieldErr("reason", "required: retirement is a decision somebody took, not a deadline that passed")
	}
	if c.Operation == DoorReopen && c.Condition == "" {
		return fieldErr("condition", "required: one of %s, %s, %s", ReopenReactivated, ReopenMethodRevised, ReopenContradicted)
	}
	return nil
}

// apply validates the call from the peer uid, resolves the notebook for the
// identity it is qualified for and applies the operation at that notebook's
// current revision. It returns the entry id for the log beside the response,
// and the error behind the response when the response withholds its detail.
func (d Door) apply(ctx context.Context, call DoorCall, uid int) (string, DoorResponse, error) {
	// Everything before the notebook is reached judges only what the caller
	// sent, so its refusals travel back whole and name the field.
	if err := call.check(); err != nil {
		return "", doorFailure(err), nil
	}
	if (call.Operation == DoorObserve || call.Operation == DoorCorrect) && uid != d.Operator {
		return "", doorFailure(fieldErr("author.kind", "%s records a person's observation, which only the operator's own uid may record; peer uid %d is not the operator's",
			call.Operation, uid)), nil
	}
	requestID := call.Request.RequestID
	switch call.Operation {
	case DoorRegister:
		q, err := ValidateQuestion(call.Payload)
		if err != nil {
			return "", doorFailure(err), nil
		}
		held, err := d.Notebook(q.Identity)
		if err != nil {
			return q.ID, notHeld(q.Identity, "identity"), err
		}
		e, err := atCurrentRevision(ctx, held, requestID, func(req Request) (*Entry, error) {
			return held.Register(ctx, req, q)
		})
		if err != nil {
			return q.ID, notebookFailure(err), err
		}
		return q.ID, doorAnswer(DoorEntry{EntryID: e.ID, State: e.State}), nil
	case DoorActivate:
		exp, err := ValidateExperiment(call.Payload)
		if err != nil {
			return "", doorFailure(err), nil
		}
		held, err := d.Notebook(exp.Identity)
		if err != nil {
			return exp.QuestionID, notHeld(exp.Identity, "identity"), err
		}
		e, err := atCurrentRevision(ctx, held, requestID, func(req Request) (*Entry, error) {
			return held.Activate(ctx, req, exp)
		})
		if err != nil {
			return exp.QuestionID, notebookFailure(err), err
		}
		return exp.QuestionID, doorAnswer(DoorEntry{EntryID: e.ID, State: e.State, ExperimentStatus: e.ExperimentStatus}), nil
	case DoorObserve, DoorCorrect, DoorCarry:
		// The kind is the operation's before anything else is judged, so a
		// person's payload missing its author is told so rather than held to
		// the observer's rules.
		a := &Assessment{}
		if err := decode(call.Payload, a); err != nil {
			return "", doorFailure(err), nil
		}
		if err := checkDoorKind(call.Operation, a); err != nil {
			return a.EntryID(), doorFailure(err), nil
		}
		if err := a.validate(); err != nil {
			return a.EntryID(), doorFailure(err), nil
		}
		held, err := d.Notebook(a.Identity)
		if err != nil {
			return a.EntryID(), notHeld(a.Identity, "identity"), err
		}
		e, err := atCurrentRevision(ctx, held, requestID, func(req Request) (*Entry, error) {
			return held.Record(ctx, req, a)
		})
		if err != nil {
			return a.EntryID(), notebookFailure(err), err
		}
		answer := DoorEntry{EntryID: e.ID, State: e.State}
		if call.Operation == DoorObserve {
			release := d.release(e, a, requestID)
			// The release's outcome never undoes the observation: it is
			// answered and logged beside it, and repeating the call retries it.
			d.Log.Printf("observer door: request=%q entry=%q release=%s ticket=%q reason=%q",
				requestID, e.ID, release.Status, release.Ticket, release.Reason)
			if release.Status == ReceiptFailed {
				// A failure's text can name the operator's definition files or
				// the ticket store's paths; the log line above carries it.
				release.Reason = doorWithheld
			}
			answer.Release = &release
		}
		return e.ID, doorAnswer(answer), nil
	case DoorRetire:
		identity, err := entryIdentity(call.EntryID)
		if err != nil {
			return call.EntryID, doorFailure(err), nil
		}
		held, err := d.Notebook(identity)
		if err != nil {
			return call.EntryID, notHeld(identity, "entry_id"), err
		}
		ret, err := atCurrentRevision(ctx, held, requestID, func(req Request) (*Retirement, error) {
			return held.Retire(ctx, req, call.EntryID, call.Reason)
		})
		if err != nil {
			return call.EntryID, notebookFailure(err), err
		}
		// Always a list: an empty one says nothing was in flight, where an
		// absent one would leave the caller unable to tell.
		inFlight := append([]string{}, ret.InFlight...)
		return call.EntryID, doorAnswer(DoorRetirement{EntryID: ret.EntryID, Reason: ret.Reason, InFlight: inFlight}), nil
	default: // DoorReopen; check admits nothing else
		identity, err := entryIdentity(call.EntryID)
		if err != nil {
			return call.EntryID, doorFailure(err), nil
		}
		held, err := d.Notebook(identity)
		if err != nil {
			return call.EntryID, notHeld(identity, "entry_id"), err
		}
		e, err := atCurrentRevision(ctx, held, requestID, func(req Request) (*Entry, error) {
			return held.Reopen(ctx, req, call.EntryID, call.Condition)
		})
		if err != nil {
			return call.EntryID, notebookFailure(err), err
		}
		return call.EntryID, doorAnswer(DoorEntry{EntryID: e.ID, State: e.State}), nil
	}
}

// checkDoorKind holds a recording operation to the one observation kind it
// records, naming the field that makes the payload another kind.
func checkDoorKind(operation string, a *Assessment) error {
	want := doorKinds[operation]
	if kind := a.Kind(); kind == want {
		return nil
	}
	switch {
	case a.Corrects != nil:
		return fieldErr("corrects", "not taken by %s; a correction is recorded by %s", operation, DoorCorrect)
	case a.CarriedOver != nil:
		return fieldErr("carried_over", "not taken by %s; a carried-over reading is recorded by %s", operation, DoorCarry)
	case want == ObservationPerson:
		return fieldErr("author.kind", "required %s: %s records a person's observation; a run records the observer's", AuthorHuman, operation)
	case want == ObservationCorrection:
		return fieldErr("corrects", "required: the observation corrected and what was wrong with it")
	default:
		return fieldErr("carried_over", "required: the source the reading was first recorded in")
	}
}

// release is what becomes of the release a person's observation may
// authorize. Only effect-observed — the event the ticket waits on occurred —
// can authorize one; any other outcome reads and writes no ticket.
func (d Door) release(e *Entry, a *Assessment, requestID string) DoorRelease {
	if a.Outcome != OutcomeEffectObserved {
		return DoorRelease{Status: DoorReleaseNone}
	}
	if d.Release == nil {
		return DoorRelease{Status: ReceiptFailed, Reason: "this door applies no release"}
	}
	return d.Release(e, a, requestID)
}

// entryIdentity is the identity an entry id is qualified for. The notebook
// checks the id against it again.
func entryIdentity(entryID string) (Identity, error) {
	parts := strings.Split(entryID, idSeparator)
	if len(parts) != 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
		return Identity{}, fieldErr("entry_id", "%q is not qualified; ids read <domain>/<observer>/<name>", entryID)
	}
	return Identity{Domain: parts[0], Observer: parts[1]}, nil
}

// atCurrentRevision applies op at the notebook's current revision, reading it
// again when a concurrent writer moved the notebook in between. The caller
// supplied no revision, so a stale one is the door's own race and never the
// caller's decision.
func atCurrentRevision[T any](ctx context.Context, held Held, requestID string, op func(Request) (T, error)) (T, error) {
	var out T
	var err error
	for range doorRevisionAttempts {
		var revision int64
		revision, err = held.Revision(ctx)
		if err != nil {
			return out, err
		}
		out, err = op(Request{Identity: held.Identity, Revision: revision, RequestID: requestID})
		if !errors.Is(err, ErrStaleRevision) {
			return out, err
		}
	}
	return out, err
}

func doorAnswer(answer any) DoorResponse {
	data, err := json.Marshal(answer)
	if err != nil {
		return doorFailure(err)
	}
	return DoorResponse{Status: DoorOK, Answer: data}
}

// doorFailure renders a refusal of what the caller sent, whole.
func doorFailure(err error) DoorResponse {
	var ferr *Error
	field := ""
	if errors.As(err, &ferr) {
		field = ferr.Field
	}
	if code := refusalExit(err); code != 0 {
		return DoorResponse{Status: DoorRefused, Exit: code, Field: field, Error: err.Error()}
	}
	if field != "" {
		return DoorResponse{Status: DoorInvalid, Field: field, Error: err.Error()}
	}
	return DoorResponse{Status: DoorFailed, Error: err.Error()}
}

// doorWithheld is what a withheld detail is replaced with; the daemon log
// carries the error itself.
const doorWithheld = "the daemon log has the detail"

// notebookFailure renders an error from the notebook's side of an operation.
// Its text may quote what the notebook holds — a stored subject, an entry's
// state, the revision — which the door does not read out, so the caller gets
// the field and the refusal class and nothing drawn from the notebook.
func notebookFailure(err error) DoorResponse {
	var ferr *Error
	field := ""
	if errors.As(err, &ferr) {
		field = ferr.Field
	}
	if code := refusalExit(err); code != 0 {
		return DoorResponse{Status: DoorRefused, Exit: code, Field: field,
			Error: fmt.Sprintf("observer %s: %v", orNotebook(field), refusalForExit(code))}
	}
	if field != "" {
		return DoorResponse{Status: DoorInvalid, Field: field,
			Error: fmt.Sprintf("observer %s: refused by the notebook; %s", field, doorWithheld)}
	}
	return DoorResponse{Status: DoorFailed,
		Error: "observer door: the notebook could not be read or written; " + doorWithheld}
}

func orNotebook(field string) string {
	if field == "" {
		return "notebook"
	}
	return field
}

// notHeld refuses an identity no held_notebook registration names. The
// lookup's own error can name the operator's definition files, so it is
// logged and not returned.
func notHeld(identity Identity, field string) DoorResponse {
	return DoorResponse{Status: DoorInvalid, Field: field, Error: fmt.Sprintf(
		"observer %s: Shuttle holds no notebook for %s/%s; %s", field, identity.Domain, identity.Observer, doorWithheld)}
}

// CallDoor makes one call on the door at path and returns its response. A
// transport failure is the error; a refusal is a response, with the exit §8
// gives its class.
func CallDoor(ctx context.Context, path string, call DoorCall) (*DoorResponse, error) {
	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, "unix", path)
	if err != nil {
		return nil, fmt.Errorf("observer door: %w", err)
	}
	defer conn.Close()
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	}
	data, err := json.Marshal(call)
	if err != nil {
		return nil, fmt.Errorf("observer door: %w", err)
	}
	if _, err := conn.Write(data); err != nil {
		return nil, fmt.Errorf("observer door: %w", err)
	}
	// The half-close is the end of the call: the door reads to EOF.
	if err := conn.(*net.UnixConn).CloseWrite(); err != nil {
		return nil, fmt.Errorf("observer door: %w", err)
	}
	reply, err := io.ReadAll(io.LimitReader(conn, maxDoorResponseBytes+1))
	if err != nil {
		return nil, fmt.Errorf("observer door: %w", err)
	}
	if len(reply) > maxDoorResponseBytes {
		return nil, fmt.Errorf("observer door: response exceeds %d bytes", maxDoorResponseBytes)
	}
	var resp DoorResponse
	dec := json.NewDecoder(bytes.NewReader(reply))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&resp); err != nil {
		return nil, fmt.Errorf("observer door: response: %w", err)
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return nil, errors.New("observer door: response: trailing content after the object")
	}
	switch resp.Status {
	case DoorOK:
		if len(resp.Answer) == 0 {
			return nil, errors.New("observer door: an ok response carries no answer")
		}
	case DoorRefused:
		if refusalForExit(resp.Exit) == nil {
			return nil, fmt.Errorf("observer door: refusal exit %d is not a refusal class", resp.Exit)
		}
	case DoorInvalid, DoorFailed:
	default:
		return nil, fmt.Errorf("observer door: status %q", resp.Status)
	}
	return &resp, nil
}
