package weftremote

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/EnderRealm/ticket/v8/pkg/ticket"
	"github.com/coder/websocket"
	"github.com/creack/pty"

	"shuttle/internal/ledger"
	"shuttle/internal/reap"
	"shuttle/internal/vt"
)

// echoShell is the login shell a terminal test runs: it reports its window
// size on SIGWINCH, echoes each line back, floods 16 MiB on request and exits
// with a status on request.
const echoShell = `trap 'printf "winch %s\n" "$(stty size)"' WINCH
printf 'ready\n'
while :; do
  if IFS= read -r line; then
    case "$line" in
      flood) head -c 16777216 /dev/zero | tr '\000' x; printf '\nEND\n' ;;
      exit*) exit ${line#exit } ;;
      *) printf 'got:%s\n' "$line" ;;
    esac
  else
    [ $? -gt 128 ] || exit 0
  fi
done
`

// terminalHost is a queue host whose terminal sessions run a controlled
// program as the login shell on a real PTY, served over a real listener.
type terminalHost struct {
	*queueHost
	server  *httptest.Server
	spawned string
	scratch string
}

func newTerminalHost(t *testing.T, shell string) *terminalHost {
	t.Helper()
	h := newQueueHost(t)
	th := &terminalHost{queueHost: h, spawned: filepath.Join(h.root, "spawned"), scratch: filepath.Join(h.root, "scratch")}
	if err := os.MkdirAll(th.scratch, 0o755); err != nil {
		t.Fatal(err)
	}
	script := filepath.Join(h.root, "login-shell")
	if err := os.WriteFile(script, []byte("#!/bin/sh\necho $$ >> "+th.spawned+"\n"+shell), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("SHELL", script)
	th.server = httptest.NewServer(h.service)
	t.Cleanup(th.server.Close)
	// Every session's process group is gone, and the session settled, before
	// the ledger and the temporary root go.
	t.Cleanup(func() { th.reapAll(h.service) })
	return th
}

func (th *terminalHost) reapAll(s *Service) {
	s.termMu.Lock()
	var open []*terminalSession
	for _, ts := range s.terms {
		open = append(open, ts)
	}
	s.termMu.Unlock()
	for _, ts := range open {
		<-ts.started
		ts.mu.Lock()
		pid := ts.pid
		ts.mu.Unlock()
		if pid > 0 {
			_ = syscall.Kill(-pid, syscall.SIGKILL)
		}
		select {
		case <-ts.done:
		case <-time.After(30 * time.Second):
			th.t.Errorf("terminal %s did not settle", ts.id)
		}
	}
}

// spawns is how many login shells have run.
func (th *terminalHost) spawns() int {
	data, err := os.ReadFile(th.spawned)
	if os.IsNotExist(err) {
		return 0
	}
	if err != nil {
		th.t.Fatal(err)
	}
	return len(strings.Fields(string(data)))
}

func terminalBody(kind, targetKind, id string, cols, rows int) string {
	return fmt.Sprintf(`{"contract_version":"weft-service/v2","kind":%q,"target":{"kind":%q,"id":%q},"cols":%d,"rows":%d}`,
		kind, targetKind, id, cols, rows)
}

var terminalKeys int

func (th *terminalHost) open(kind, targetKind, id string) response {
	th.t.Helper()
	terminalKeys++
	return th.send(http.MethodPost, "/v2/terminals", fmt.Sprintf("open-%d", terminalKeys), terminalBody(kind, targetKind, id, 120, 36))
}

// opened requires a 201 shaped like terminal-response.json, and the shell
// to be running.
func (th *terminalHost) opened(r response) map[string]any {
	th.t.Helper()
	r.expect(th.t, http.StatusCreated, "terminal-response.json", "terminal-closed.json")
	session := r.body["terminal"].(map[string]any)
	if session["state"] != "starting" || session["offset"] != float64(0) || session["attachments"] != float64(0) {
		th.t.Fatalf("new session = %v", session)
	}
	ts := th.service.terminal(session["session_id"].(string))
	if ts == nil {
		th.t.Fatalf("no session for %v", session)
	}
	<-ts.started
	return session
}

func (th *terminalHost) terminal(id string) map[string]any {
	th.t.Helper()
	r := th.send(http.MethodGet, "/v2/terminals/"+id, "", "")
	r.expect(th.t, http.StatusOK, "terminal-response.json", "terminal-closed.json")
	return r.body["terminal"].(map[string]any)
}

func (th *terminalHost) pid(id string) int {
	th.t.Helper()
	row, err := th.store.WeftTerminal(id)
	if err != nil || !row.PID.Valid {
		th.t.Fatalf("terminal %s pid: %+v %v", id, row, err)
	}
	return int(row.PID.Int64)
}

func (th *terminalHost) close(id string, revision any) response {
	th.t.Helper()
	terminalKeys++
	return th.send(http.MethodPost, "/v2/terminals/"+id+"/close", fmt.Sprintf("close-%d", terminalKeys), revisionBody(revision))
}

func alive(pid int) bool { return syscall.Kill(pid, 0) == nil }

// repositoryState is the repository resource as the ledger holds it, which
// GET /v2/repositories serves where landing is served.
func (th *terminalHost) repositoryState(project string) map[string]any {
	th.t.Helper()
	repositories, err := th.store.WeftRepositories()
	if err != nil {
		th.t.Fatal(err)
	}
	for _, r := range repositories {
		if r.RepositoryID == th.repositoryID(project) {
			doc, err := json.Marshal(r.Resource())
			if err != nil {
				th.t.Fatal(err)
			}
			resource := decode(th.t, doc).(map[string]any)
			learn(decode(th.t, fixtureBytes(th.t, "repositories.json")).(map[string]any)["repositories"].([]any)...).
				check(th.t, "repository", resource)
			return resource
		}
	}
	th.t.Fatalf("repository of %s is not held", project)
	return nil
}

func waitLong(t *testing.T, what string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatalf("%s did not happen", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// frameShapes are the host frames the three transcripts show, by type.
func frameShapes(t *testing.T) map[string]*shape {
	samples := map[string][]any{}
	for _, name := range []string{"terminal-restore.ndjson", "terminal-takeover.ndjson", "terminal-gap.ndjson"} {
		for _, line := range strings.Split(strings.TrimSpace(string(fixtureBytes(t, name))), "\n") {
			f := decode(t, []byte(line)).(map[string]any)
			samples[f["type"].(string)] = append(samples[f["type"].(string)], f)
		}
	}
	shapes := map[string]*shape{}
	for typ, s := range samples {
		shapes[typ] = learn(s...)
	}
	return shapes
}

// wsClient is one attachment as a client sees it. It holds the host's frames
// to the transcripts' shapes, and its output to the offsets: contiguous from
// the snapshot or offset it attached at, broken only by a reset that a
// snapshot at the reset's offset follows.
type wsClient struct {
	t        *testing.T
	conn     *websocket.Conn
	shapes   map[string]*shape
	expected int64
	reset    int64
	output   bytes.Buffer
	id       string
}

func (th *terminalHost) dial(server *httptest.Server, id, query string, subprotocols ...string) (*websocket.Conn, *http.Response, error) {
	if subprotocols == nil {
		subprotocols = []string{terminalSubprotocol}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	url := "ws" + strings.TrimPrefix(server.URL, "http") + "/v2/terminals/" + id + "/attach" + query
	return websocket.Dial(ctx, url, &websocket.DialOptions{
		HTTPHeader:   http.Header{"Authorization": {"Bearer secret-token"}},
		Subprotocols: subprotocols,
	})
}

func (th *terminalHost) attach(id, query string) *wsClient {
	th.t.Helper()
	conn, _, err := th.dial(th.server, id, query)
	if err != nil {
		th.t.Fatalf("attach %s%s: %v", id, query, err)
	}
	if conn.Subprotocol() != terminalSubprotocol {
		th.t.Fatalf("negotiated %q", conn.Subprotocol())
	}
	conn.SetReadLimit(4 << 20)
	th.t.Cleanup(func() { conn.CloseNow() })
	c := &wsClient{t: th.t, conn: conn, shapes: frameShapes(th.t), expected: -1, reset: -1}
	attached := c.next()
	if attached["type"] != "attached" || attached["session_id"] != id {
		th.t.Fatalf("first frame = %v", attached)
	}
	c.id = attached["attachment_id"].(string)
	return c
}

func (c *wsClient) next() map[string]any {
	c.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	typ, data, err := c.conn.Read(ctx)
	if err != nil {
		c.t.Fatalf("read frame: %v", err)
	}
	if typ != websocket.MessageText {
		c.t.Fatalf("frame type %v", typ)
	}
	f := decode(c.t, data).(map[string]any)
	if f["contract_version"] != ServiceVersion {
		c.t.Fatalf("frame %s", data)
	}
	if sh := c.shapes[f["type"].(string)]; sh != nil {
		sh.check(c.t, f["type"].(string), f)
	}
	switch f["type"] {
	case "reset":
		c.reset = int64(f["offset"].(float64))
	case "snapshot":
		offset := int64(f["offset"].(float64))
		if c.reset >= 0 && offset != c.reset {
			c.t.Fatalf("snapshot at %d after a reset to %d", offset, c.reset)
		}
		raw, _ := base64.StdEncoding.DecodeString(f["data"].(string))
		if len(raw) > maxSnapshotFrame {
			c.t.Fatalf("snapshot of %d bytes", len(raw))
		}
		c.reset, c.expected = -1, offset
	case "output":
		if c.reset >= 0 {
			c.t.Fatalf("output after a reset with no snapshot: %v", f)
		}
		offset := int64(f["offset"].(float64))
		if c.expected >= 0 && offset != c.expected {
			c.t.Fatalf("output at %d, expected %d", offset, c.expected)
		}
		raw, _ := base64.StdEncoding.DecodeString(f["data"].(string))
		if len(raw) > maxOutputFrame {
			c.t.Fatalf("output frame of %d bytes", len(raw))
		}
		c.output.Write(raw)
		c.expected = offset + int64(len(raw))
	}
	return f
}

// until reads frames until one satisfies match, returning it.
func (c *wsClient) until(what string, match func(map[string]any) bool) map[string]any {
	c.t.Helper()
	for {
		if f := c.next(); match(f) {
			return f
		} else if f["type"] == "ended" && what != "ended" {
			c.t.Fatalf("session ended waiting for %s: %v", what, f)
		}
	}
}

// sees reads until the output holds text.
func (c *wsClient) sees(text string) {
	c.t.Helper()
	if strings.Contains(c.output.String(), text) {
		return
	}
	c.until(text, func(map[string]any) bool { return strings.Contains(c.output.String(), text) })
}

func (c *wsClient) send(frame string) {
	c.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := c.conn.Write(ctx, websocket.MessageText, []byte(frame)); err != nil {
		c.t.Fatalf("send %s: %v", frame, err)
	}
}

func (c *wsClient) input(text string) {
	c.send(fmt.Sprintf(`{"contract_version":"weft-service/v2","type":"input","data":%q}`, base64.StdEncoding.EncodeToString([]byte(text))))
}

func (c *wsClient) resize(cols, rows int) {
	c.send(fmt.Sprintf(`{"contract_version":"weft-service/v2","type":"resize","cols":%d,"rows":%d}`, cols, rows))
}

func (c *wsClient) control(typ string) {
	c.send(fmt.Sprintf(`{"contract_version":"weft-service/v2","type":%q}`, typ))
}

// AC1: sessions open from a project, a run's retained worktree and folder
// handles — an unregistered folder reserving nothing — and share the one
// terminal row of their repository.
func TestTerminalsOpenFromProjectWorktreeAndFolder(t *testing.T) {
	th := newTerminalHost(t, echoShell)
	th.ticket("weft/parked-0001", ticket.StatusReady, 2)
	runID := th.waitDispatched(entryOf(th.launch("weft/parked-0001", "k1"))["entry_id"].(string))
	run := th.waitPhase(runID, ledger.WeftPhaseParked)
	before := asOf(t, th.store)

	project := th.opened(th.open("shell", "project", "weft"))
	if project["path"] != th.repository("weft") || project["repository_id"] != th.repositoryID("weft") ||
		project["project"] != "weft" || project["title"] != "weft" || project["target"].(map[string]any)["id"] != "weft" {
		t.Fatalf("project session = %v", project)
	}
	worktree := th.opened(th.open("claude", "worktree", runID))
	if worktree["path"] != run.Worktree || worktree["repository_id"] != th.repositoryID("weft") || worktree["project"] != "weft" ||
		worktree["kind"] != "claude" || worktree["title"] != filepath.Base(run.Worktree) {
		t.Fatalf("worktree session = %v", worktree)
	}
	repository := th.repositoryState("weft")
	if holderOf(repository) != "terminal "+project["session_id"].(string) ||
		!slices.Equal(repository["terminals"].([]any), []any{project["session_id"], worktree["session_id"]}) {
		t.Fatalf("repository with two sessions = %v", repository)
	}

	// The claude session typed claude into its shell, which outlives it.
	c := th.attach(worktree["session_id"].(string), "")
	c.sees("got:claude")

	receipts := filepath.Join(th.scratch, "receipts")
	inLoom := filepath.Join(th.code, "loom", "docs")
	for _, dir := range []string{receipts, inLoom} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	listing := th.send(http.MethodGet, "/v2/folders?path="+th.scratch, "", "")
	listing.expect(t, http.StatusOK, "folder.json")
	entries := listing.body["folder"].(map[string]any)["entries"].([]any)
	if len(entries) != 1 || entries[0].(map[string]any)["path"] != receipts || entries[0].(map[string]any)["repository_id"] != nil {
		t.Fatalf("scratch entries = %v", entries)
	}
	reservations := func() int {
		var n int
		if err := th.db().QueryRow(`SELECT count(*) FROM weft_reservations`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	held := reservations()
	unregistered := th.opened(th.open("shell", "folder", entries[0].(map[string]any)["handle"].(string)))
	if unregistered["path"] != receipts || unregistered["repository_id"] != nil || unregistered["project"] != nil ||
		unregistered["title"] != "receipts" {
		t.Fatalf("unregistered folder session = %v", unregistered)
	}
	if reservations() != held {
		t.Fatal("an unregistered folder took a reservation")
	}
	docs := th.send(http.MethodGet, "/v2/folders?path="+filepath.Join(th.code, "loom"), "", "")
	docs.expect(t, http.StatusOK, "folder.json")
	var docsHandle string
	for _, e := range docs.body["folder"].(map[string]any)["entries"].([]any) {
		if e.(map[string]any)["name"] == "docs" {
			docsHandle = e.(map[string]any)["handle"].(string)
			if e.(map[string]any)["repository_id"] != th.repositoryID("loom") || e.(map[string]any)["project"] != "loom" {
				t.Fatalf("docs entry = %v", e)
			}
		}
	}
	inRepository := th.opened(th.open("shell", "folder", docsHandle))
	if inRepository["repository_id"] != th.repositoryID("loom") || inRepository["project"] != "loom" ||
		holderOf(th.repositoryState("loom")) != "terminal "+inRepository["session_id"].(string) {
		t.Fatalf("folder in loom = %v, loom = %v", inRepository, th.repositoryState("loom"))
	}

	list := th.send(http.MethodGet, "/v2/terminals", "", "")
	list.expect(t, http.StatusOK, "terminals.json")
	if n := len(list.body["terminals"].([]any)); n != 4 {
		t.Fatalf("listed %d sessions", n)
	}
	events := th.eventsSince(before)
	var terminalEvents int
	for _, ev := range events {
		if ev.Type == "terminal" {
			terminalEvents++
		}
	}
	if terminalEvents < 8 {
		t.Fatalf("%d terminal events for four sessions started and one attached", terminalEvents)
	}
	waitFor(t, func() bool { return th.spawns() == 4 })
}

// AC1: every refusal comes before a child is launched — no shell runs, no
// session is recorded and nothing is reserved.
func TestTerminalRefusalsLaunchNothing(t *testing.T) {
	th := newTerminalHost(t, echoShell)
	th.ticket("weft/held-0001", ticket.StatusReady, 2)
	th.ticket("loom/cleaned-0002", ticket.StatusReady, 2)
	th.mode("weft/held-0001", "hold")
	held := th.waitDispatched(entryOf(th.launch("weft/held-0001", "k1"))["entry_id"].(string))
	th.waitPhase(held, ledger.WeftPhaseRunning)
	cleaned := th.waitPhase(th.waitDispatched(entryOf(th.launch("loom/cleaned-0002", "k2"))["entry_id"].(string)), ledger.WeftPhaseParked)
	th.exec(`UPDATE weft_runs SET disposition='cleaned' WHERE id=?`, cleaned.ID)

	dir := filepath.Join(th.scratch, "swapped")
	other := filepath.Join(th.scratch, "other")
	for _, d := range []string{dir, other} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	handleFor := func(path string) string {
		r := th.send(http.MethodGet, "/v2/folders?path="+path, "", "")
		r.expect(t, http.StatusOK, "folder.json")
		return r.body["folder"].(map[string]any)["handle"].(string)
	}
	expired := handleFor(dir)
	clock := th.service.folders.now
	th.service.folders.now = func() time.Time { return time.Now().Add(folderHandleTTL + time.Second) }
	th.open("shell", "folder", expired).refused(t, http.StatusConflict, "handle_invalid")
	th.service.folders.now = clock

	replaced := handleFor(dir)
	if err := os.Rename(dir, dir+".old"); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if message := th.open("shell", "folder", replaced).refused(t, http.StatusConflict, "handle_invalid"); !strings.Contains(message, "no longer the directory") {
		t.Fatalf("replaced directory: %s", message)
	}
	swapped := handleFor(dir)
	if err := os.Remove(dir); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(other, dir); err != nil {
		t.Fatal(err)
	}
	if message := th.open("shell", "folder", swapped).refused(t, http.StatusConflict, "handle_invalid"); !strings.Contains(message, "now resolves to "+other) {
		t.Fatalf("symlinked directory: %s", message)
	}
	for _, path := range []string{other, "../../etc", "file:///etc", "hd-0123456789abcdef"} {
		th.open("shell", "folder", path).refused(t, http.StatusConflict, "handle_invalid")
	}

	running := th.open("shell", "worktree", held).refused(t, http.StatusConflict, "repository_reserved")
	byProject := th.open("shell", "project", "weft").refused(t, http.StatusConflict, "repository_reserved")
	want := th.repository("weft") + " is held by run " + held + ", which is running; a writable terminal on it opens once that run has settled"
	if running != want || byProject != want {
		t.Fatalf("repository_reserved = %q / %q, want %q", running, byProject, want)
	}
	th.open("shell", "worktree", cleaned.ID).refused(t, http.StatusConflict, "worktree_missing")
	th.exec(`UPDATE weft_runs SET disposition='retained' WHERE id=?`, cleaned.ID)
	if err := os.RemoveAll(cleaned.Worktree); err != nil {
		t.Fatal(err)
	}
	th.open("shell", "worktree", cleaned.ID).refused(t, http.StatusConflict, "worktree_missing")
	th.open("shell", "worktree", "r-20261001T000000Z-00000000").refused(t, http.StatusNotFound, "run_not_found")
	th.open("shell", "project", "nowhere").refused(t, http.StatusNotFound, "project_not_found")
	th.projects["warp"] = filepath.Join(th.root, "missing")
	th.writeConfig()
	th.open("shell", "project", "warp").refused(t, http.StatusConflict, "binding_changed")
	// A retained worktree whose .git now names another checkout is refused.
	th.ticket("ticket/rebound-0003", ticket.StatusReady, 2)
	rebound := th.waitPhase(th.waitDispatched(entryOf(th.launch("ticket/rebound-0003", "k3"))["entry_id"].(string)), ledger.WeftPhaseParked)
	if err := os.WriteFile(filepath.Join(rebound.Worktree, ".git"),
		[]byte("gitdir: "+filepath.Join(th.code, "loom", ".git", "worktrees", "rebound")+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if message := th.open("shell", "worktree", rebound.ID).refused(t, http.StatusConflict, "binding_changed"); !strings.Contains(message, th.repository("loom")) {
		t.Fatalf("rebound worktree: %s", message)
	}
	th.open("shell", "elsewhere", "x").refused(t, http.StatusBadRequest, "malformed")
	th.send(http.MethodPost, "/v2/terminals", "k-big", terminalBody("shell", "project", "loom", 5000, 36)).
		refused(t, http.StatusBadRequest, "malformed")

	if th.spawns() != 0 {
		t.Fatalf("%d shells spawned by refused requests", th.spawns())
	}
	var sessions, terminalHolders int
	if err := th.db().QueryRow(`SELECT count(*) FROM weft_terminals`).Scan(&sessions); err != nil {
		t.Fatal(err)
	}
	if err := th.db().QueryRow(`SELECT count(*) FROM weft_reservations WHERE holder_kind='terminal'`).Scan(&terminalHolders); err != nil {
		t.Fatal(err)
	}
	if sessions != 0 || terminalHolders != 0 || len(th.service.terms) != 0 {
		t.Fatalf("refusals left %d sessions, %d terminal reservations, %d live", sessions, terminalHolders, len(th.service.terms))
	}
	th.release("weft/held-0001")
	th.settle()
}

// The folder picker lists Studio directories by canonical path with fresh
// handles, home and every registered checkout as shortcuts, and refuses
// what it cannot list.
func TestFolderPicker(t *testing.T) {
	th := newTerminalHost(t, echoShell)
	home := filepath.Join(th.root, "home")
	for _, d := range []string{"Downloads/receipts", ".hidden"} {
		if err := os.MkdirAll(filepath.Join(home, d), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(home, "notes.txt"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(th.code, "weft"), filepath.Join(home, "weft-link")); err != nil {
		t.Fatal(err)
	}
	t.Setenv("HOME", home)

	r := th.send(http.MethodGet, "/v2/folders", "", "")
	r.expect(t, http.StatusOK, "folder.json")
	folder := r.body["folder"].(map[string]any)
	if folder["path"] != home || folder["repository_id"] != nil || folder["parent"] == nil {
		t.Fatalf("home = %v", folder)
	}
	var names, paths []string
	for _, e := range folder["entries"].([]any) {
		names = append(names, e.(map[string]any)["name"].(string))
		paths = append(paths, e.(map[string]any)["path"].(string))
	}
	// Subdirectories only, by name, dot-directories included; a symlink to a
	// directory lists at the path it resolves to. What else the host's own
	// libraries put under the home is listed too, and not asserted.
	listedAt := map[string]string{}
	for i, name := range names {
		listedAt[name] = paths[i]
	}
	if !slices.IsSorted(names) || listedAt[".hidden"] != filepath.Join(home, ".hidden") ||
		listedAt["Downloads"] != filepath.Join(home, "Downloads") || listedAt["weft-link"] != th.repository("weft") ||
		listedAt["notes.txt"] != "" {
		t.Fatalf("entries %v at %v", names, paths)
	}
	var shortcuts []string
	for _, s := range folder["shortcuts"].([]any) {
		shortcuts = append(shortcuts, s.(map[string]any)["name"].(string))
	}
	if !slices.Equal(shortcuts, []string{"Home", "loom", "ticket", "warp", "weft"}) {
		t.Fatalf("shortcuts = %v", shortcuts)
	}
	entry := func(name string) map[string]any {
		for _, e := range folder["entries"].([]any) {
			if e.(map[string]any)["name"] == name {
				return e.(map[string]any)
			}
		}
		t.Fatalf("%s is not listed", name)
		return nil
	}
	if link := entry("weft-link"); link["repository_id"] != th.repositoryID("weft") || link["project"] != "weft" {
		t.Fatalf("symlinked checkout = %v", link)
	}
	down := th.send(http.MethodGet, "/v2/folders?handle="+entry("Downloads")["handle"].(string), "", "")
	down.expect(t, http.StatusOK, "folder.json")
	if down.body["folder"].(map[string]any)["path"] != filepath.Join(home, "Downloads") {
		t.Fatalf("Downloads = %v", down.body["folder"])
	}
	th.send(http.MethodGet, "/v2/folders?path=relative/path", "", "").refused(t, http.StatusBadRequest, "malformed")
	th.send(http.MethodGet, "/v2/folders?path="+home+"/../home", "", "").refused(t, http.StatusBadRequest, "malformed")
	th.send(http.MethodGet, "/v2/folders?path="+home+"/missing", "", "").refused(t, http.StatusNotFound, "folder_not_found")
	th.send(http.MethodGet, "/v2/folders?path="+home+"/notes.txt", "", "").refused(t, http.StatusNotFound, "folder_not_found")
	th.send(http.MethodGet, "/v2/folders?handle=hd-0123456789abcdef", "", "").refused(t, http.StatusConflict, "handle_invalid")
	th.send(http.MethodGet, "/v2/folders?handle=x&path=/", "", "").refused(t, http.StatusBadRequest, "malformed")
}

// AC2: one input and size owner among two attachments, explicit take-over,
// detach that never closes, and a close that reaps the process group before
// the reservation is released.
func TestTerminalOwnershipTakeOverDetachAndClose(t *testing.T) {
	th := newTerminalHost(t, echoShell)
	session := th.opened(th.open("shell", "project", "weft"))
	id := session["session_id"].(string)
	pid := th.pid(id)

	a := th.attach(id, "")
	a.until("snapshot", func(f map[string]any) bool { return f["type"] == "snapshot" })
	b := th.attach(id, "")
	b.until("snapshot", func(f map[string]any) bool { return f["type"] == "snapshot" })
	if now := th.terminal(id); now["state"] != "live" || now["owner"] != a.id || now["attachments"] != float64(2) {
		t.Fatalf("two attachments = %v", now)
	}

	b.input("nope\n")
	refused := b.until("refused", func(f map[string]any) bool { return f["type"] == "refused" })
	if refused["code"] != "not_owner" || refused["message"] != "input belongs to "+a.id+"; take over to type" {
		t.Fatalf("viewer input = %v", refused)
	}
	b.resize(80, 24)
	if f := b.until("refused", func(f map[string]any) bool { return f["type"] == "refused" }); f["code"] != "not_owner" {
		t.Fatalf("viewer resize = %v", f)
	}
	a.input("one\n")
	a.sees("got:one")
	if strings.Contains(a.output.String(), "got:nope") {
		t.Fatal("a viewer's input reached the shell")
	}

	b.control("take_over")
	for _, c := range []*wsClient{a, b} {
		f := c.until("owner", func(f map[string]any) bool { return f["type"] == "owner" })
		role := map[bool]string{true: "owner", false: "viewer"}[c == b]
		if f["owner"] != b.id || f["role"] != role {
			t.Fatalf("owner frame to %s = %v", c.id, f)
		}
	}
	b.input("two\n")
	b.sees("got:two")
	a.sees("got:two")
	a.input("three\n")
	if f := a.until("refused", func(f map[string]any) bool { return f["type"] == "refused" }); f["message"] != "input belongs to "+b.id+"; take over to type" {
		t.Fatalf("former owner input = %v", f)
	}

	b.control("detach")
	if f := a.until("owner", func(f map[string]any) bool { return f["type"] == "owner" }); f["owner"] != nil || f["role"] != "viewer" {
		t.Fatalf("after the owner detached = %v", f)
	}
	waitFor(t, func() bool { return th.terminal(id)["attachments"] == float64(1) })
	if now := th.terminal(id); now["state"] != "live" || now["owner"] != nil {
		t.Fatalf("after the owner detached = %v", now)
	}
	a.control("detach")
	waitFor(t, func() bool { return th.terminal(id)["state"] == "detached" })
	if now := th.terminal(id); now["attachments"] != float64(0) || now["owner"] != nil || !alive(pid) {
		t.Fatalf("detached = %v, shell alive %v", now, alive(pid))
	}

	// A client that kept its screen resumes from its offset with output.
	c := th.attach(id, fmt.Sprintf("?offset=%d", a.expected))
	c.expected = a.expected
	c.input("four\n")
	if f := c.next(); f["type"] != "output" || f["offset"] != float64(a.expected) {
		t.Fatalf("resumed attachment's next frame = %v", f)
	}
	c.sees("got:four")

	// Queued work for the repository waits on the terminal, named.
	th.ticket("weft/queued-0001", ticket.StatusReady, 2)
	queued := entryOf(th.launch("weft/queued-0001", "k1"))
	if reason, holder := waiting(queued); reason != "repository_reserved" || holder["kind"] != "terminal" || holder["id"] != id {
		t.Fatalf("entry behind the terminal = %v", queued)
	}

	th.close(id, 1).refused(t, http.StatusConflict, "revision_conflict")
	repositoryID := th.repositoryID("weft")
	seq := asOf(t, th.store)
	released := make(chan bool, 1)
	go func() {
		for {
			events, _ := th.store.WeftEventsAfter(seq)
			for _, ev := range events {
				seq = ev.Seq
				var payload struct {
					Repository map[string]any `json:"repository"`
				}
				_ = json.Unmarshal(ev.Payload, &payload)
				if ev.Type == "repository" && payload.Repository["repository_id"] == repositoryID && payload.Repository["reservation"] == nil {
					live, _ := processGroupLive(pid)
					released <- live || alive(pid)
					return
				}
			}
			time.Sleep(time.Millisecond)
		}
	}()
	closed := th.close(id, th.terminal(id)["revision"])
	closed.expect(t, http.StatusOK, "terminal-closed.json", "terminal-response.json")
	final := closed.body["terminal"].(map[string]any)
	if final["state"] != "closed" || final["closed_by"] != "operator" || final["exit_code"] != nil || final["owner"] != nil {
		t.Fatalf("closed = %v", final)
	}
	select {
	case stillLive := <-released:
		if stillLive {
			t.Fatal("the reservation was released while the shell's process group was alive")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("no repository event released the row")
	}
	if alive(pid) {
		t.Fatal("close left the shell alive")
	}
	if f := c.until("ended", func(f map[string]any) bool { return f["type"] == "ended" }); f["state"] != "closed" || f["closed_by"] != "operator" {
		t.Fatalf("ended = %v", f)
	}
	if repository := th.repositoryState("weft"); repository["reservation"] != nil || len(repository["terminals"].([]any)) != 0 {
		t.Fatalf("repository after close = %v", repository)
	}
	th.waitPhase(th.waitDispatched(queued["entry_id"].(string)), ledger.WeftPhaseParked)
	th.close(id, final["revision"]).refused(t, http.StatusConflict, "session_closed")
	if _, resp, err := th.dial(th.server, id, ""); err == nil || resp == nil || resp.StatusCode != http.StatusConflict {
		t.Fatalf("attach to a closed session = %v %v", resp, err)
	}
}

// AC2: a shell that exits closes its session with its exit code; an attach
// without the subprotocol is 426.
func TestTerminalShellExitAndSubprotocol(t *testing.T) {
	th := newTerminalHost(t, echoShell)
	id := th.opened(th.open("shell", "project", "loom"))["session_id"].(string)
	if _, resp, err := th.dial(th.server, id, "", "weft-service.v3"); err == nil || resp == nil || resp.StatusCode != http.StatusUpgradeRequired {
		t.Fatalf("attach with weft-service.v3 = %v %v", resp, err)
	}
	req, _ := http.NewRequest(http.MethodGet, th.server.URL+"/v2/terminals/"+id+"/attach", nil)
	req.Header.Set("Authorization", "Bearer secret-token")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body := response{code: resp.StatusCode}
	_ = json.NewDecoder(resp.Body).Decode(&body.body)
	resp.Body.Close()
	body.refused(t, http.StatusUpgradeRequired, "upgrade_required")

	c := th.attach(id, "")
	c.input("exit 3\n")
	f := c.until("ended", func(f map[string]any) bool { return f["type"] == "ended" })
	if f["closed_by"] != "exit" || f["exit_code"] != float64(3) {
		t.Fatalf("ended = %v", f)
	}
	waitFor(t, func() bool { return th.terminal(id)["state"] == "closed" })
	if now := th.terminal(id); now["closed_by"] != "exit" || now["exit_code"] != float64(3) {
		t.Fatalf("exited = %v", now)
	}
	if th.repositoryState("loom")["reservation"] != nil {
		t.Fatal("an exited shell kept its reservation")
	}
}

// AC2: a repair terminal takes a not_landed-paused repository's reservation
// and leaves the pause — and the entries waiting on it — standing, during
// and after.
func TestRepairTerminalLeavesThePauseStanding(t *testing.T) {
	th := newTerminalHost(t, echoShell)
	th.ticket("warp/after-0001", ticket.StatusReady, 2)
	th.exec(`INSERT INTO weft_repositories(repository_id,repository,dispatch,paused_run,paused_reason,paused_detail,paused_at,revision)
		VALUES(?,?,'paused','r-20261011T110000Z-00000000','not_landed','the push to origin failed','2026-10-11T11:00:00.000Z',1)`,
		th.repositoryID("warp"), th.repository("warp"))
	entry := entryOf(th.launch("warp/after-0001", "k1"))
	if reason, _ := waiting(entry); reason != "repository_paused" {
		t.Fatalf("entry = %v", entry)
	}
	id := th.opened(th.open("shell", "project", "warp"))["session_id"].(string)
	repository := th.repositoryState("warp")
	if holderOf(repository) != "terminal "+id || repository["dispatch"] != "paused" || repository["paused"] == nil {
		t.Fatalf("repaired repository = %v", repository)
	}
	th.service.dispatch()
	if reason, _ := waiting(th.entry(entry["entry_id"].(string))); reason != "repository_paused" {
		t.Fatalf("entry under the repair terminal = %v", th.entry(entry["entry_id"].(string)))
	}
	th.close(id, th.terminal(id)["revision"]).expect(t, http.StatusOK, "terminal-closed.json", "terminal-response.json")
	repository = th.repositoryState("warp")
	if repository["reservation"] != nil || repository["dispatch"] != "paused" || repository["paused"] == nil {
		t.Fatalf("after the repair terminal = %v", repository)
	}
	if e := th.entry(entry["entry_id"].(string)); e["state"] != "waiting" {
		t.Fatalf("entry after the repair terminal = %v", e)
	}
	if reason, _ := waiting(th.entry(entry["entry_id"].(string))); reason != "repository_paused" {
		t.Fatalf("entry after the repair terminal = %v", th.entry(entry["entry_id"].(string)))
	}
}

// AC3: output retention is bounded and offset-addressed; a reconnect inside
// it resumes exactly, one older than it gets reset retention and a snapshot,
// and a viewer too slow for the stream gets reset resync and a snapshot.
func TestTerminalRetentionReplayAndResync(t *testing.T) {
	th := newTerminalHost(t, echoShell)
	id := th.opened(th.open("shell", "folder", func() string {
		r := th.send(http.MethodGet, "/v2/folders?path="+th.scratch, "", "")
		return r.body["folder"].(map[string]any)["handle"].(string)
	}()))["session_id"].(string)
	owner := th.attach(id, "")
	owner.sees("ready")
	slow := th.attach(id, "")
	slow.until("snapshot", func(f map[string]any) bool { return f["type"] == "snapshot" })

	owner.input("flood\n")
	owner.sees("END")
	ts := th.service.terminal(id)
	ts.mu.Lock()
	retained, scrollback, end := len(ts.ring.buf), ts.emu.ScrollbackLines(), ts.ring.end
	ts.mu.Unlock()
	if retained != terminalRetentionBytes || scrollback > vt.MaxScrollback || end < 16<<20 {
		t.Fatalf("retained %d bytes, %d scrollback lines, offset %d", retained, scrollback, end)
	}
	now := th.terminal(id)
	if int64(now["offset"].(float64)) != end || int64(now["retained_from"].(float64)) != end-terminalRetentionBytes {
		t.Fatalf("session = %v, offset %d", now, end)
	}

	// The slow viewer was passed by the retention: reset resync, then a
	// snapshot at the reset's offset — never a silent drop.
	reset := slow.until("reset", func(f map[string]any) bool { return f["type"] == "reset" })
	if reset["reason"] != "resync" || reset["requested"].(float64) >= reset["oldest"].(float64) {
		t.Fatalf("slow viewer reset = %v", reset)
	}
	if f := slow.next(); f["type"] != "snapshot" {
		t.Fatalf("after resync = %v", f)
	}

	old := th.attach(id, "?offset=0")
	gap := old.next()
	if gap["type"] != "reset" || gap["reason"] != "retention" || gap["requested"] != float64(0) ||
		int64(gap["oldest"].(float64)) != end-terminalRetentionBytes || int64(gap["offset"].(float64)) != end {
		t.Fatalf("attach at 0 = %v", gap)
	}
	if f := old.next(); f["type"] != "snapshot" || int64(f["offset"].(float64)) != end {
		t.Fatalf("after the retention reset = %v", f)
	}

	resumed := th.attach(id, fmt.Sprintf("?offset=%d", end-7))
	resumed.expected = end - 7
	f := resumed.next()
	data, _ := base64.StdEncoding.DecodeString(f["data"].(string))
	if f["type"] != "output" || int64(f["offset"].(float64)) != end-7 || string(data) != "\r\nEND\r\n" {
		t.Fatalf("resume at %d = %v (%q)", end-7, f, data)
	}
	owner.input("after\n")
	resumed.sees("got:after")
	old.sees("got:after")
}

// tuiStream is what a full-screen program writes: shell output, then the
// alternate screen and its modes set once, a framed input box drawn, and in-
// place repaints until more than the retention lies after the setup.
func tuiStream(cols, rows int) []byte {
	var b bytes.Buffer
	for i := 0; i < 200; i++ {
		fmt.Fprintf(&b, "\x1b[1;32msteve@studio\x1b[0m %% ls\r\nREADME.md  日本語 %d\r\n", i)
	}
	b.WriteString("$ claude\r\n\x1b]0;✳ Claude Code\x07\x1b[?1049h\x1b[?1h\x1b=\x1b[?2004h\x1b[?1002h\x1b[?1006h\x1b[?25l")
	b.WriteString("\x1b[H\x1b[2J\x1b[1m✳ Claude Code\x1b[0m\r\n\x1b[2m  cwd: /Users/steve/code/weft\x1b[0m\r\n")
	fmt.Fprintf(&b, "\x1b[%d;1H╭%s╮\x1b[%d;1H│ > %s│\x1b[%d;1H╰%s╯\x1b[4;%dr",
		rows-2, strings.Repeat("─", cols-2), rows-1, strings.Repeat(" ", cols-5), rows, strings.Repeat("─", cols-2), rows-3)
	setup := b.Len()
	for i := 0; b.Len()-setup < terminalRetentionBytes+(64<<10); i++ {
		fmt.Fprintf(&b, "\x1b[?2026h\x1b[%d;1H\x1b[48;5;236m thinking %d \x1b[0m\x1b[K\x1b[%d;5H%s\x1b[%d;%dH\x1b[?2026l",
			rows-3, i, rows-1, strings.Repeat("y", i%40), rows-1, 5+i%40)
	}
	return b.Bytes()
}

// AC3: the snapshot restores a full-screen program whose setup lies outside
// the retained 4 MiB, which the raw tail alone does not; the owner's size is
// applied with SIGWINCH, even unchanged, and the repaint arrives as output.
func TestTerminalSnapshotRestoresATUIBeyondTheRetention(t *testing.T) {
	const cols, rows = 120, 36
	stream := tuiStream(cols, rows)
	root, err := os.MkdirTemp("/tmp", "wt-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(root) })
	file := filepath.Join(root, "tui")
	if err := os.WriteFile(file, stream, 0o600); err != nil {
		t.Fatal(err)
	}
	th := newTerminalHost(t, `trap 'printf "\033[?25l\033[H\033[2J\033[1m✳ Claude Code\033[0m repaint %s\033[?25h" "$(stty size)"' WINCH
stty -opost
cat `+file+`
while :; do sleep 1 & wait $!; done
`)
	id := th.opened(th.open("shell", "project", "ticket"))["session_id"].(string)
	waitLong(t, "the TUI's output", func() bool { return th.terminal(id)["offset"].(float64) >= float64(len(stream)) })
	if offset := th.terminal(id)["offset"]; offset != float64(len(stream)) {
		t.Fatalf("the session's offset is %v after a %d byte stream", offset, len(stream))
	}
	if now := th.terminal(id); int64(now["retained_from"].(float64)) != int64(len(stream))-terminalRetentionBytes || now["title"] != "✳ Claude Code" {
		t.Fatalf("session = %v", now)
	}

	reference := vt.New(cols, rows)
	reference.Write(stream)
	want, _ := reference.Snapshot(maxSnapshotFrame)
	tail := vt.New(cols, rows)
	tail.Write(stream[len(stream)-terminalRetentionBytes:])
	if got, _ := tail.Snapshot(maxSnapshotFrame); bytes.Equal(got, want) {
		t.Fatal("the raw retained tail rebuilds the screen; the stream does not put its setup outside it")
	}

	c := th.attach(id, "")
	snapshot := c.until("snapshot", func(f map[string]any) bool { return f["type"] == "snapshot" })
	if snapshot["offset"] != float64(len(stream)) || snapshot["cols"] != float64(cols) || snapshot["rows"] != float64(rows) {
		t.Fatalf("snapshot = %v", snapshot)
	}
	data, _ := base64.StdEncoding.DecodeString(snapshot["data"].(string))
	restored := vt.New(cols, rows)
	restored.Write(data)
	if got, _ := restored.Snapshot(maxSnapshotFrame); !bytes.Equal(got, want) {
		t.Fatalf("the snapshot does not restore the program's screen:\n got %q\nwant %q", got, want)
	}
	if !bytes.Contains(data, []byte("\x1b[?1049h")) || !bytes.Contains(data, []byte("✳ Claude Code")) ||
		!bytes.Contains(data, []byte("\x1b[?1002h")) || !bytes.Contains(data, []byte("\x1b[4;33r")) {
		t.Fatalf("snapshot %q", data)
	}

	c.resize(132, 40)
	repaint := c.until("repaint", func(f map[string]any) bool { return f["type"] == "output" })
	if repaint["offset"] != float64(len(stream)) {
		t.Fatalf("the repaint starts at %v, the snapshot at %d", repaint["offset"], len(stream))
	}
	c.sees("repaint 40 132")
	if now := th.terminal(id); now["cols"] != float64(132) || now["rows"] != float64(40) {
		t.Fatalf("resized session = %v", now)
	}
	before := strings.Count(c.output.String(), "repaint 40 132")
	c.resize(132, 40)
	c.until("second repaint", func(map[string]any) bool { return strings.Count(c.output.String(), "repaint 40 132") > before })
}

// The fixture snapshots decode through the emulator into the screens they
// describe, and serialize back to a stable state.
func TestFixtureSnapshotsDecodeThroughTheEmulator(t *testing.T) {
	for name, text := range map[string]string{
		"terminal-restore.ndjson":  "✳ Claude Code",
		"terminal-takeover.ndjson": "> y",
		"terminal-gap.ndjson":      "steve@studio receipts %",
	} {
		scanner := bufio.NewScanner(bytes.NewReader(fixtureBytes(t, name)))
		scanner.Buffer(nil, 1<<20)
		for scanner.Scan() {
			f := decode(t, scanner.Bytes()).(map[string]any)
			if f["type"] != "snapshot" {
				continue
			}
			data, _ := base64.StdEncoding.DecodeString(f["data"].(string))
			cols, rows := int(f["cols"].(float64)), int(f["rows"].(float64))
			screen := vt.New(cols, rows)
			screen.Write(data)
			once, _ := screen.Snapshot(maxSnapshotFrame)
			again := vt.New(cols, rows)
			again.Write(once)
			twice, _ := again.Snapshot(maxSnapshotFrame)
			if !bytes.Equal(once, twice) || !bytes.Contains(once, []byte(text)) {
				t.Errorf("%s: snapshot does not decode to a stable screen holding %q:\n%q\n%q", name, text, once, twice)
			}
		}
	}
}

// AC3: the restart pass interrupts every session that was not closed: a
// live shell group is killed, a gone one closed, and one nothing can be
// established about keeps its reservation share; an interrupted session
// refuses attach.
func TestRestartInterruptsTerminals(t *testing.T) {
	th := newTerminalHost(t, echoShell)
	// Each seeded shell leads its own session, as a session's shell does.
	group := func(script string) (*exec.Cmd, int, string) {
		cmd := exec.Command("/bin/sh", "-c", script)
		cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		pid := cmd.Process.Pid
		started, err := reap.ProcessStart(pid)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = syscall.Kill(-pid, syscall.SIGKILL) })
		return cmd, pid, started.UTC().Format(time.RFC3339Nano)
	}
	// The live shell started a job in its own process group that ignores
	// SIGHUP: the restart establishes the whole session gone, not the
	// shell's group alone.
	jobFile := filepath.Join(th.root, "job")
	liveCmd, livePID, liveStart := group(`set -m; sh -c 'trap "" HUP; exec sleep 60' & echo $! > ` + jobFile + `; exec sleep 60`)
	reaped := make(chan struct{})
	go func() { _ = liveCmd.Wait(); close(reaped) }()
	var jobPID int
	waitFor(t, func() bool {
		data, err := os.ReadFile(jobFile)
		_, scanned := fmt.Sscan(string(data), &jobPID)
		return err == nil && scanned == nil && jobPID > 0
	})
	t.Cleanup(func() { _ = syscall.Kill(-jobPID, syscall.SIGKILL) })
	if pgid, err := syscall.Getpgid(jobPID); err != nil || pgid == livePID {
		t.Fatalf("job %d is in group %d (%v), the shell's is %d", jobPID, pgid, err, livePID)
	}
	goneCmd, gonePID, goneStart := group("sleep 0.2")
	if err := goneCmd.Wait(); err != nil {
		t.Fatal(err)
	}
	strandedCmd, strandedPID, strandedStart := group("sleep 60 & exit 0")
	if err := strandedCmd.Wait(); err != nil {
		t.Fatal(err)
	}
	// A live shell whose start time was never recorded cannot be
	// established: it is not signalled and keeps its share.
	unrecordedCmd, unrecordedPID, _ := group("exec sleep 60")
	go func() { _ = unrecordedCmd.Wait() }()
	seed := []struct {
		id, project string
		pid         int
		start       any
	}{
		{"t-20261011T120000Z-00000001", "warp", livePID, liveStart},
		{"t-20261011T120000Z-00000002", "loom", gonePID, goneStart},
		{"t-20261011T120000Z-00000003", "ticket", strandedPID, strandedStart},
		{"t-20261011T120000Z-00000004", "weft", unrecordedPID, nil},
	}
	for _, s := range seed {
		th.exec(`INSERT INTO weft_terminals(id,kind,target_kind,target_id,path,repository_id,project,state,pid,pgid,process_start,
			width,height,title,owner,attachments,created_at,updated_at,revision)
			VALUES(?,'shell','project',?,?,?,?,'live',?,?,?,120,36,?,'ta-20261011T120001Z-00000001',1,
			'2026-10-11T12:00:00.000Z','2026-10-11T12:00:00.000Z',2)`,
			s.id, s.project, th.repository(s.project), th.repositoryID(s.project), s.project, s.pid, s.pid, s.start, s.project)
		th.exec(`INSERT INTO weft_reservations(repository_id,repository,holder_kind,holder_id,acquired_at)
			VALUES(?,?,'terminal',?,'2026-10-11T12:00:00.000Z')`, th.repositoryID(s.project), th.repository(s.project), s.id)
	}

	restarted, err := New(th.service.cfg, th.store, "secret-token", th.root, log.New(th.logs, "", 0))
	if err != nil {
		t.Fatal(err)
	}
	restarted.segments.findKey = th.service.segments.findKey
	if err := restarted.Restart(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-reaped:
	case <-time.After(15 * time.Second):
		t.Fatal("the live shell group was not killed")
	}
	waitFor(t, func() bool { return !alive(jobPID) })
	for _, s := range seed {
		row, err := th.store.WeftTerminal(s.id)
		if err != nil {
			t.Fatal(err)
		}
		if row.State != ledger.WeftTerminalInterrupted || row.ClosedBy.String != ledger.WeftClosedByRestart ||
			row.Owner.Valid || row.Attachments != 0 {
			t.Fatalf("%s after restart = %+v", s.id, row)
		}
	}
	for _, project := range []string{"warp", "loom"} {
		if r := th.repositoryState(project); r["reservation"] != nil {
			t.Fatalf("%s kept its reservation: %v", project, r)
		}
	}
	if holderOf(th.repositoryState("ticket")) != "terminal "+seed[2].id {
		t.Fatalf("the unestablishable group's share was released: %v", th.repositoryState("ticket"))
	}
	if holderOf(th.repositoryState("weft")) != "terminal "+seed[3].id || !alive(unrecordedPID) {
		t.Fatalf("the unrecorded shell's share was released (%v) or it was signalled (alive %v)",
			th.repositoryState("weft"), alive(unrecordedPID))
	}
	if !strings.Contains(th.logs.String(), "terminal "+seed[2].id+" pid="+fmt.Sprint(strandedPID)+" action=unresolved") ||
		!strings.Contains(th.logs.String(), "its reservation share is kept") {
		t.Fatalf("restart log:\n%s", th.logs.String())
	}
	th.ticket("ticket/after-0001", ticket.StatusReady, 2)
	th.service = restarted
	if reason, holder := waiting(entryOf(th.launch("ticket/after-0001", "k1"))); reason != "repository_reserved" || holder["kind"] != "terminal" {
		t.Fatalf("launch into the kept share = %s %v", reason, holder)
	}
	server := httptest.NewServer(restarted)
	t.Cleanup(server.Close)
	_, resp, err := th.dial(server, seed[0].id, "")
	if err == nil || resp == nil || resp.StatusCode != http.StatusConflict {
		t.Fatalf("attach to an interrupted session = %v %v", resp, err)
	}
	list := th.send(http.MethodGet, "/v2/terminals", "", "")
	list.expect(t, http.StatusOK, "terminals.json")
	for _, s := range list.body["terminals"].([]any) {
		if s.(map[string]any)["state"] != "interrupted" {
			t.Fatalf("listed = %v", s)
		}
	}
	// The next restart establishes the stranded group gone once it is.
	_ = syscall.Kill(-strandedPID, syscall.SIGKILL)
	waitFor(t, func() bool { live, err := processGroupLive(strandedPID); return err == nil && !live })
	again, err := New(th.service.cfg, th.store, "secret-token", th.root, log.New(th.logs, "", 0))
	if err != nil {
		t.Fatal(err)
	}
	again.segments.findKey = th.service.segments.findKey
	if err := again.Restart(); err != nil {
		t.Fatal(err)
	}
	if r := th.repositoryState("ticket"); r["reservation"] != nil && r["reservation"].(map[string]any)["kind"] == "terminal" {
		t.Fatalf("the stranded share outlived its group: %v", r)
	}
	th.settle()
}

// A shell whose identity cannot be read or durably recorded never outlives
// its spawn: it is killed, the session ends closed rather than left starting,
// and the share is released once its processes are gone.
func TestUnrecordableShellIsKilledAndItsSessionClosed(t *testing.T) {
	th := newTerminalHost(t, echoShell)
	realStart, realRecord := terminalProcessStart, recordTerminalProcess
	t.Cleanup(func() { terminalProcessStart, recordTerminalProcess = realStart, realRecord })
	var spawned int
	check := func(cause string) {
		t.Helper()
		r := th.open("shell", "project", "weft")
		r.expect(t, http.StatusCreated, "terminal-response.json", "terminal-closed.json")
		id := r.body["terminal"].(map[string]any)["session_id"].(string)
		if spawned == 0 {
			t.Fatal("no shell was spawned")
		}
		if live, err := processGroupLive(spawned); err != nil || live || alive(spawned) {
			t.Fatalf("%s: shell %d survived (group live %v, %v)", cause, spawned, live, err)
		}
		row, err := th.store.WeftTerminal(id)
		if err != nil || row.State != ledger.WeftTerminalClosed || row.ClosedBy.String != ledger.WeftClosedByExit ||
			row.PID.Int64 != int64(spawned) {
			t.Fatalf("%s: session = %+v %v", cause, row, err)
		}
		// The launcher was never let through: no login shell ran.
		if th.spawns() != 0 {
			t.Fatalf("%s: %d login shells ran", cause, th.spawns())
		}
		if r := th.repositoryState("weft"); r["reservation"] != nil {
			t.Fatalf("%s: reservation kept: %v", cause, r)
		}
		if !strings.Contains(th.logs.String(), cause) {
			t.Fatalf("log lacks %q:\n%s", cause, th.logs.String())
		}
		if _, resp, err := th.dial(th.server, id, ""); err == nil || resp == nil || resp.StatusCode != http.StatusConflict {
			t.Fatalf("%s: attach = %v %v", cause, resp, err)
		}
		spawned = 0
	}
	terminalProcessStart = func(pid int) (time.Time, error) {
		spawned = pid
		return time.Time{}, fmt.Errorf("kern.proc.pid %d: unreadable", pid)
	}
	check("no kernel start time")
	terminalProcessStart = func(pid int) (time.Time, error) {
		spawned = pid
		return realStart(pid)
	}
	recordTerminalProcess = func(*ledger.Store, string, int, string, ledger.WeftTerminalState) (*ledger.WeftTerminal, error) {
		return nil, fmt.Errorf("disk I/O error")
	}
	check("record pid")
}

// The launcher executes the login shell only on the host's go-ahead; at EOF
// — the host abandoned it, or died — it exits without executing anything.
func TestShellLauncherWaitsForTheGoAhead(t *testing.T) {
	th := newTerminalHost(t, echoShell)
	// Each launcher runs on a PTY of its own, as a session's does, whose
	// output is read so the shell can exit.
	onPTY := func(cmd *exec.Cmd) {
		ptmx, tty, err := pty.Open()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { ptmx.Close(); tty.Close() })
		go func() { _, _ = io.Copy(io.Discard, ptmx) }()
		cmd.Stdin, cmd.Stdout, cmd.Stderr = tty, tty, tty
	}
	cmd, goAhead, err := shellLauncher(loginShell())
	if err != nil {
		t.Fatal(err)
	}
	onPTY(cmd)
	err = cmd.Start()
	cmd.ExtraFiles[0].Close()
	if err != nil {
		t.Fatal(err)
	}
	pid := cmd.Process.Pid
	t.Cleanup(func() { _ = syscall.Kill(-pid, syscall.SIGKILL) })
	time.Sleep(200 * time.Millisecond)
	if !alive(pid) || th.spawns() != 0 {
		t.Fatalf("before the go-ahead: launcher alive %v, %d shells ran", alive(pid), th.spawns())
	}
	goAhead.Close()
	if err := cmd.Wait(); err != nil {
		t.Fatalf("launcher at EOF: %v", err)
	}
	if live, err := processGroupLive(pid); err != nil || live || th.spawns() != 0 {
		t.Fatalf("after EOF: group live %v (%v), %d shells ran", live, err, th.spawns())
	}

	cmd, goAhead, err = shellLauncher(loginShell())
	if err != nil {
		t.Fatal(err)
	}
	onPTY(cmd)
	err = cmd.Start()
	cmd.ExtraFiles[0].Close()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) })
	if _, err := goAhead.Write([]byte("go\n")); err != nil {
		t.Fatal(err)
	}
	goAhead.Close()
	waitFor(t, func() bool { return th.spawns() == 1 })
	data, _ := os.ReadFile(th.spawned)
	if strings.TrimSpace(string(data)) != fmt.Sprint(cmd.Process.Pid) {
		t.Fatalf("the shell ran as %q, not as the launcher's pid %d", data, cmd.Process.Pid)
	}
	_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	_ = cmd.Wait()
}

// A session closed with its process groups unresolved keeps its share
// through a restart even when it never recorded a pid: missing identity does
// not clear the uncertainty.
func TestRestartKeepsAnUnresolvedShareWithNoPID(t *testing.T) {
	th := newTerminalHost(t, echoShell)
	const id = "t-20261011T120000Z-00000009"
	th.exec(`INSERT INTO weft_terminals(id,kind,target_kind,target_id,path,repository_id,project,state,closed_by,
		group_unresolved,width,height,title,attachments,created_at,updated_at,revision)
		VALUES(?,'shell','project','loom',?,?,'loom','closed','exit','[worker process group 4242 outlived SIGKILL]',
		120,36,'loom',0,'2026-10-11T12:00:00.000Z','2026-10-11T12:00:00.000Z',3)`,
		id, th.repository("loom"), th.repositoryID("loom"))
	th.exec(`INSERT INTO weft_reservations(repository_id,repository,holder_kind,holder_id,acquired_at)
		VALUES(?,?,'terminal',?,'2026-10-11T12:00:00.000Z')`, th.repositoryID("loom"), th.repository("loom"), id)
	restarted, err := New(th.service.cfg, th.store, "secret-token", th.root, log.New(th.logs, "", 0))
	if err != nil {
		t.Fatal(err)
	}
	restarted.segments.findKey = th.service.segments.findKey
	if err := restarted.Restart(); err != nil {
		t.Fatal(err)
	}
	if holderOf(th.repositoryState("loom")) != "terminal "+id {
		t.Fatalf("the unresolved share was released: %v", th.repositoryState("loom"))
	}
	row, err := th.store.WeftTerminal(id)
	if err != nil || row.State != ledger.WeftTerminalClosed || !row.GroupUnresolved.Valid {
		t.Fatalf("session after restart = %+v %v", row, err)
	}
	if !strings.Contains(th.logs.String(), "terminal "+id+" pid=0 action=no_pid (its reservation share is kept)") {
		t.Fatalf("restart log:\n%s", th.logs.String())
	}
}

// A client frame naming another contract version closes the WebSocket with
// policy violation and a reason that fits in a close frame.
func TestAttachmentClosesWithABoundedReason(t *testing.T) {
	th := newTerminalHost(t, echoShell)
	id := th.opened(th.open("shell", "project", "loom"))["session_id"].(string)
	c := th.attach(id, "")
	c.send(`{"contract_version":"weft-service/v3","type":"take_over"}`)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		_, _, err := c.conn.Read(ctx)
		cancel()
		if err == nil {
			continue
		}
		var closed websocket.CloseError
		if !errors.As(err, &closed) || closed.Code != websocket.StatusPolicyViolation || closed.Reason == "" ||
			len(closed.Reason) > maxCloseReason || !strings.HasPrefix(closed.Reason, "weft-service/v3 is not spoken here") {
			t.Fatalf("close = %v", err)
		}
		break
	}
	if long := closeReason(strings.Repeat("日", 60)); len(long) > maxCloseReason || !utf8.ValidString(long) {
		t.Fatalf("cut reason = %q", long)
	}
}

// A shell whose termination handler starts a job in a new process group and
// exits leaves that job for the next listing of its session and PTY: it is
// dead before the reservation is released.
func TestCloseCatchesAJobStartedOnTermination(t *testing.T) {
	root, err := os.MkdirTemp("/tmp", "wj-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(root) })
	jobFile := filepath.Join(root, "job")
	th := newTerminalHost(t, `trap '' HUP
trap 'set -m; sleep 60 & echo "$! $(ps -o pgid= -p $!)" > `+jobFile+`; exit 0' TERM
printf 'ready\n'
while :; do sleep 1 & wait $!; done
`)
	id := th.opened(th.open("shell", "project", "weft"))["session_id"].(string)
	shell := th.pid(id)
	th.attach(id, "").sees("ready")
	repositoryID := th.repositoryID("weft")
	seq := asOf(t, th.store)
	type sighting struct {
		job, group int
		alive      bool
	}
	released := make(chan sighting, 1)
	go func() {
		for {
			events, _ := th.store.WeftEventsAfter(seq)
			for _, ev := range events {
				seq = ev.Seq
				var payload struct {
					Repository map[string]any `json:"repository"`
				}
				_ = json.Unmarshal(ev.Payload, &payload)
				if ev.Type == "repository" && payload.Repository["repository_id"] == repositoryID && payload.Repository["reservation"] == nil {
					var s sighting
					data, _ := os.ReadFile(jobFile)
					fmt.Sscan(string(data), &s.job, &s.group)
					s.alive = s.job > 0 && alive(s.job)
					released <- s
					return
				}
			}
			time.Sleep(time.Millisecond)
		}
	}()
	th.close(id, th.terminal(id)["revision"]).expect(t, http.StatusOK, "terminal-closed.json", "terminal-response.json")
	select {
	case s := <-released:
		if s.job == 0 || s.group == shell {
			t.Fatalf("the termination handler's job = %+v, the shell's group is %d", s, shell)
		}
		t.Cleanup(func() { _ = syscall.Kill(-s.group, syscall.SIGKILL) })
		if s.alive {
			live, _ := processGroupLive(s.group)
			if live {
				t.Fatalf("the reservation was released while job %d in group %d was alive", s.job, s.group)
			}
		}
	case <-time.After(30 * time.Second):
		if holderOf(th.repositoryState("weft")) == "" {
			t.Fatal("no repository event released the row")
		}
		// The share was kept: acceptable only with the job unresolved.
		row, err := th.store.WeftTerminal(id)
		if err != nil || !row.GroupUnresolved.Valid {
			t.Fatalf("share kept without an unresolved group: %+v %v", row, err)
		}
	}
}

// A recorded pid now alive under another user is pid reuse: the restart
// takes the session as gone and releases its share.
func TestRestartReleasesAShareWhosePidWasHandedOn(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root may signal pid 1")
	}
	th := newTerminalHost(t, echoShell)
	const id = "t-20261011T120000Z-0000000a"
	th.exec(`INSERT INTO weft_terminals(id,kind,target_kind,target_id,path,repository_id,project,state,pid,pgid,process_start,
		width,height,title,attachments,created_at,updated_at,revision)
		VALUES(?,'shell','project','loom',?,?,'loom','detached',1,1,'2026-10-11T12:00:00Z',120,36,'loom',0,
		'2026-10-11T12:00:00.000Z','2026-10-11T12:00:00.000Z',2)`, id, th.repository("loom"), th.repositoryID("loom"))
	th.exec(`INSERT INTO weft_reservations(repository_id,repository,holder_kind,holder_id,acquired_at)
		VALUES(?,?,'terminal',?,'2026-10-11T12:00:00.000Z')`, th.repositoryID("loom"), th.repository("loom"), id)
	restarted, err := New(th.service.cfg, th.store, "secret-token", th.root, log.New(th.logs, "", 0))
	if err != nil {
		t.Fatal(err)
	}
	restarted.segments.findKey = th.service.segments.findKey
	if err := restarted.Restart(); err != nil {
		t.Fatal(err)
	}
	if r := th.repositoryState("loom"); r["reservation"] != nil {
		t.Fatalf("a share whose pid was handed on was kept: %v", r)
	}
	if row, err := th.store.WeftTerminal(id); err != nil || row.State != ledger.WeftTerminalInterrupted || row.GroupUnresolved.Valid {
		t.Fatalf("session after restart = %+v %v", row, err)
	}
	if !strings.Contains(th.logs.String(), "terminal "+id+" pid=1 action=foreign") {
		t.Fatalf("restart log:\n%s", th.logs.String())
	}
}
