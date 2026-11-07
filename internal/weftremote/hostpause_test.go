package weftremote

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/EnderRealm/ticket/v8/pkg/ticket"

	"shuttle/internal/ledger"
)

// otherProcess is the ledger as `shuttle pause` and the updater reach it: its
// own handle on the file, nothing shared with the daemon's.
func otherProcess(t *testing.T, root string) *ledger.Store {
	t.Helper()
	store, err := ledger.Open(filepath.Join(root, "runs.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	return store
}

// nextHost skips the stream to its next host event, held to the shape the
// fixtures show.
func nextHost(t *testing.T, str *stream) map[string]any {
	t.Helper()
	for {
		ev := str.next(t)
		if ev.name != "host" {
			continue
		}
		doc := decode(t, []byte(ev.data)).(map[string]any)
		learn(fixtureEvents(t, "events-reset.sse")["host"]...).check(t, "host", doc)
		return doc
	}
}

func TestHostWidePauseServedToClients(t *testing.T) {
	h := newQueueHost(t)
	h.ticket("weft/held-0001", ticket.StatusReady, 2)
	h.mode("weft/held-0001", "finish")
	server := httptest.NewServer(h.service)
	t.Cleanup(server.Close)
	updater := otherProcess(t, h.root)
	str := openStream(t, server, "")
	str.next(t)

	dashboardHost := func() map[string]any {
		t.Helper()
		r := h.send(http.MethodGet, "/v2/dashboard", "", "")
		r.expect(t, http.StatusOK, "dashboard.json")
		return r.body["dashboard"].(map[string]any)["host"].(map[string]any)
	}
	if got := dashboardHost(); got["dispatch"] != "open" || got["reason"] != nil {
		t.Fatalf("open host = %v", got)
	}

	// A pause set by another process reaches the stream, the dashboard and a
	// waiting entry in the same words.
	if claimed, _, err := updater.ClaimUpdatePause(); err != nil || !claimed {
		t.Fatalf("claim = %t/%v", claimed, err)
	}
	paused := nextHost(t, str)
	reason, _ := paused["reason"].(string)
	if paused["dispatch"] != "paused" || !strings.Contains(reason, "updating") || strings.Contains(reason, "shuttle resume") {
		t.Fatalf("host event = %v", paused)
	}
	if got := dashboardHost(); got["dispatch"] != "paused" || got["reason"] != reason {
		t.Fatalf("dashboard host = %v, event reason %q", got, reason)
	}
	r := h.launch("weft/held-0001", "held")
	r.expect(t, http.StatusAccepted, "launch-response.json")
	entry := entryOf(r)
	waitReason, _ := waiting(entry)
	if waitReason != ledger.WeftWaitHostPaused {
		t.Fatalf("entry = %v", entry)
	}
	if detail := entry["waiting"].(map[string]any)["detail"]; detail != reason {
		t.Fatalf("waiting detail = %v, host reason %q", detail, reason)
	}

	// The lift comes from the other process too, and nothing in the daemon is
	// told: the catalog watch's dispatch is what starts the waiting work.
	if lifted, _, err := updater.ReleaseUpdatePause(); err != nil || !lifted {
		t.Fatalf("release = %t/%v", lifted, err)
	}
	if open := nextHost(t, str); open["dispatch"] != "open" || open["reason"] != nil {
		t.Fatalf("host event = %v", open)
	}
	if got := dashboardHost(); got["dispatch"] != "open" || got["reason"] != nil {
		t.Fatalf("lifted host = %v", got)
	}
	if e := h.entry(entry["entry_id"].(string)); e["state"] != "waiting" {
		t.Fatalf("entry dispatched before any wake: %v", e)
	}
	h.service.catalogPoll = 20 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go h.service.watchCatalog(ctx)
	h.waitDispatched(entry["entry_id"].(string))
	h.settle()

	// `shuttle pause` and `shuttle resume` write through the same path, and
	// the operator's words keep the command that lifts them.
	if err := updater.Pause(ledger.WeftAgent); err != nil {
		t.Fatal(err)
	}
	if got := nextHost(t, str); got["dispatch"] != "paused" || !strings.Contains(got["reason"].(string), "shuttle resume "+ledger.WeftAgent) {
		t.Fatalf("host event = %v", got)
	}
	if err := updater.Resume(ledger.WeftAgent); err != nil {
		t.Fatal(err)
	}
	if got := nextHost(t, str); got["dispatch"] != "open" {
		t.Fatalf("host event = %v", got)
	}
}

// Under the restart barrier a launch waits on the pause in the words clients
// already read, an operator's resume does not reopen it, and the end of the
// update starts it. The barrier itself writes nothing a client reads.
func TestRestartBarrierHoldsDispatchThroughAResume(t *testing.T) {
	h := newQueueHost(t)
	h.ticket("weft/held-0001", ticket.StatusReady, 2)
	h.mode("weft/held-0001", "finish")
	updater := otherProcess(t, h.root)
	if claimed, _, err := updater.ClaimUpdatePause(); err != nil || !claimed {
		t.Fatalf("claim = %t/%v", claimed, err)
	}
	if err := updater.ArmRestartBarrier(time.Hour); err != nil {
		t.Fatal(err)
	}
	r := h.launch("weft/held-0001", "held")
	r.expect(t, http.StatusAccepted, "launch-response.json")
	entry := entryOf(r)
	id := entry["entry_id"].(string)
	if reason, _ := waiting(entry); reason != ledger.WeftWaitHostPaused {
		t.Fatalf("entry = %v", entry)
	}
	if err := updater.Resume(ledger.WeftAgent); err != nil {
		t.Fatal(err)
	}
	h.service.dispatch()
	if e := h.entry(id); e["state"] != "waiting" {
		t.Fatalf("entry dispatched under the barrier: %v", e)
	}
	host, err := updater.WeftHost()
	if err != nil || host.Dispatch != ledger.WeftDispatchPaused || !strings.Contains(host.Reason, "updating") {
		t.Fatalf("host = %+v/%v", host, err)
	}
	if lifted, barrier, err := updater.ReleaseUpdatePause(); err != nil || !lifted || !barrier {
		t.Fatalf("release = %t/%t/%v", lifted, barrier, err)
	}
	h.service.dispatch()
	h.waitDispatched(id)
	h.settle()
}

// The refusal reaches a client as a code its strict decoder knows, in words
// that name the update.
func TestRestartBarrierRefusesATerminalOverTheService(t *testing.T) {
	th := newTerminalHost(t, "/bin/sh")
	updater := otherProcess(t, th.root)
	if claimed, _, err := updater.ClaimUpdatePause(); err != nil || !claimed {
		t.Fatalf("claim = %t/%v", claimed, err)
	}
	if err := updater.ArmRestartBarrier(time.Hour); err != nil {
		t.Fatal(err)
	}
	r := th.open("shell", "project", "weft")
	r.expect(t, http.StatusConflict, "error-repository-reserved.json")
	if message := r.body["error"].(map[string]any)["message"].(string); !strings.Contains(message, "update") {
		t.Fatalf("message = %q", message)
	}
}
