package weftremote

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/EnderRealm/ticket/v8/pkg/ticket"

	"shuttle/internal/ledger"
	"shuttle/internal/sandbox"
)

// v2WorkerScript is a fixture weft-worker/v2 worker. version prints the
// control directory's version.json; allowance-probe prints probe.json, or
// hangs while probe.hang exists; a segment — of any kind — replays the ndjson
// written for its ticket and segment, holding at each @HOLD@ line until
// released, reading one command from stdin at each @ACK@ line into
// <slug>-<segment>.commands, and exiting with no terminal event at @DIE@; it
// keeps the answer file it was handed.
const v2WorkerScript = `#!/bin/sh
control=CONTROL
case "$1" in
  version) cat "$control/version.json"; exit 0 ;;
  allowance-probe)
    printf '%s\n' "$*" >> "$control/probe.log"
    printf '%s' "$$" > "$control/probe.pid"
    if [ -f "$control/probe.hang" ]; then exec sleep 60; fi
    cat "$control/probe.json"
    exit 0 ;;
esac
printf '%s\n' "$*" >> "$control/argv.log"
run= segment= ticket= answerfile=
while [ "$#" -gt 0 ]; do
  case "$1" in
    --run) run="$2"; shift 2 ;;
    --segment) segment="$2"; shift 2 ;;
    --ticket) ticket="$2"; shift 2 ;;
    --answer-file) answerfile="$2"; shift 2 ;;
    *) shift ;;
  esac
done
slug=$(printf '%s' "$ticket" | tr / -)
qid=
if [ -n "$answerfile" ]; then
  cp "$answerfile" "$control/$slug-$segment.answer.json"
  qid=$(sed -n 's/.*"question_id":"\([^"]*\)".*/\1/p' "$answerfile")
fi
exec 3<&0
sed -e "s/@RUN@/$run/g" -e "s/@SEG@/$segment/g" -e "s/@QID@/$qid/g" "$control/$slug-$segment.ndjson" |
while IFS= read -r line; do
  case "$line" in
    @HOLD@)
      while [ ! -f "$control/$slug.release" ]; do sleep 0.02; done
      rm -f "$control/$slug.release"
      continue ;;
    @ACK@)
      IFS= read -r command <&3
      printf '%s\n' "$command" >> "$control/$slug-$segment.commands"
      continue ;;
    @DIE@) exit 9 ;;
  esac
  printf '%s\n' "$line"
done
`

const fixtureWorkerRun = "20261002T091500Z-2c3d4e5f"

// newV2Host is a queue host whose worker speaks weft-worker/v2 as
// worker-version.json declares it, less land and reconcile: landing is not
// served, so its segments launch without --land, as the developmental slice
// launches them (§2.4).
func newV2Host(t *testing.T) *queueHost {
	t.Helper()
	return newWorkerHost(t, "land", "reconcile")
}

// newWorkerHost is a queue host whose worker speaks weft-worker/v2 with
// worker-version.json's capabilities less those named.
func newWorkerHost(t *testing.T, without ...string) *queueHost {
	t.Helper()
	h := newQueueHost(t)
	version := decode(t, fixtureBytes(t, "worker-version.json")).(map[string]any)
	for _, c := range version["capabilities"].([]any) {
		if !slices.Contains(without, c.(string)) {
			h.capabilities = append(h.capabilities, c)
		}
	}
	version["capabilities"] = h.capabilities
	doc, err := json.Marshal(version)
	if err != nil {
		t.Fatal(err)
	}
	h.writeControl("version.json", doc)
	h.worker(strings.Replace(v2WorkerScript, "CONTROL", h.control, 1))
	t.Cleanup(func() { h.service.probes.Wait() })
	return h
}

func (h *queueHost) writeControl(name string, data []byte) {
	h.t.Helper()
	if err := os.WriteFile(filepath.Join(h.control, name), data, 0o600); err != nil {
		h.t.Fatal(err)
	}
}

func (h *queueHost) worker(script string) {
	h.t.Helper()
	if err := os.WriteFile(h.service.cfg.Weft.WorkerBin, []byte(script), 0o700); err != nil {
		h.t.Fatal(err)
	}
}

func (h *queueHost) worktree(ticketID string) string {
	h.t.Helper()
	project, _, _ := strings.Cut(ticketID, "/")
	worktree, err := sandbox.WorktreePath(h.service.cfg.Weft.Root, h.repository(project), ticketID)
	if err != nil {
		h.t.Fatal(err)
	}
	return worktree
}

// workerLines is a segment's events; a nil entry is a hold point.
type workerLines []map[string]any

// fixtureSegment is worker-segment.ndjson as a segment this host launches:
// this host's worktree, no --land, so no landing events and an ended whose
// landing is not_applicable.
func (h *queueHost) fixtureSegment(ticketID string) workerLines {
	h.t.Helper()
	var out workerLines
	for _, line := range strings.Split(strings.TrimSpace(string(fixtureBytes(h.t, "worker-segment.ndjson"))), "\n") {
		ev := decode(h.t, []byte(line)).(map[string]any)
		switch ev["event"] {
		case "land_intent", "land_step_result":
			continue
		case "hello":
			ev["worktree"], ev["land"], ev["ticket"], ev["branch"] = h.worktree(ticketID), false, ticketID, ticketID
			ev["capabilities"] = h.capabilities
		case "graded":
			ev["summary"].(map[string]any)["report"].(map[string]any)["ticket"] = ticketID
		case "ended":
			ev["landing"] = map[string]any{"state": "not_applicable", "step": nil, "reason": nil,
				"detail": "landing is not served by this host", "branch": nil, "commit": nil}
		}
		out = append(out, ev)
	}
	return out
}

func outputLine(stage string, chunk int, text string) map[string]any {
	return map[string]any{
		"contract_version": WorkerContractV2, "worker_run_id": fixtureWorkerRun, "ts": "2026-10-02T10:36:11.204Z",
		"event": "output", "stage": stage, "occurrence": 1, "attempt": 1, "chunk": chunk, "text": text,
	}
}

func splice(lines workerLines, at int, insert ...map[string]any) workerLines {
	out := append(workerLines{}, lines[:at]...)
	out = append(out, insert...)
	return append(out, lines[at:]...)
}

func indexOf(lines workerLines, event string) int {
	for i, ev := range lines {
		if ev != nil && ev["event"] == event {
			return i
		}
	}
	return -1
}

// script writes the events the fixture worker replays for one segment of
// the ticket, numbered from seq 1.
func (h *queueHost) script(ticketID string, segment int, lines workerLines) {
	h.t.Helper()
	var b bytes.Buffer
	seq := 0
	for _, ev := range lines {
		if ev == nil {
			b.WriteString("@HOLD@\n")
			continue
		}
		if marker, ok := ev["@"].(string); ok {
			b.WriteString("@" + marker + "@\n")
			continue
		}
		seq++
		ev = maps.Clone(ev)
		ev["seq"], ev["run_id"], ev["segment"] = seq, "@RUN@", "@SEG@"
		doc, err := json.Marshal(ev)
		if err != nil {
			h.t.Fatal(err)
		}
		b.Write(bytes.ReplaceAll(doc, []byte(`"@SEG@"`), []byte("@SEG@")))
		b.WriteByte('\n')
	}
	h.writeControl(slugOf(ticketID)+"-"+strconv.Itoa(segment)+".ndjson", b.Bytes())
	// A held worker is released before the host's cleanup waits it out.
	h.t.Cleanup(func() { h.release(ticketID) })
}

func (h *queueHost) get(path string) map[string]any {
	h.t.Helper()
	r := h.send(http.MethodGet, path, "", "")
	if r.code != http.StatusOK {
		h.t.Fatalf("GET %s = %d %s", path, r.code, r.raw)
	}
	return r.body
}

// waitReaped waits for the run to end and its segment to settle, then reads
// it again: the read that saw the settlement may have read the run row
// before the settlement's transaction committed.
func (h *queueHost) waitReaped(runID string) *ledger.WeftRun {
	h.t.Helper()
	waitFor(h.t, func() bool {
		run := h.run(runID)
		return run.Phase == ledger.WeftPhaseEnded && h.reaped(run)
	})
	return h.run(runID)
}

func sample(t *testing.T, fixture, event string) *shape {
	return learn(fixtureEvents(t, fixture)[event]...)
}

// runShapeOf is every run fixture, plus run.json carrying the liveness
// events.sse shows: no run fixture is caught mid-stall.
func runShapeOf(t *testing.T) *shape {
	stalled := decode(t, fixtureBytes(t, "run.json")).(map[string]any)
	stalled["run"].(map[string]any)["liveness"] = fixtureEvents(t, "events.sse")["liveness"][0].(map[string]any)["liveness"]
	return learn(decode(t, fixtureBytes(t, "run.json")), decode(t, fixtureBytes(t, "run-landed.json")),
		decode(t, fixtureBytes(t, "run-not-landed.json")), decode(t, fixtureBytes(t, "run-stale-base.json")),
		decode(t, fixtureBytes(t, "run-stopped.json")), stalled)
}

// stateSamples are events.sse's and events-reset.sse's events, with a stage
// event for each of run-landed.json's stage rows: the streams only show
// stages still running, which carry no spend or tokens.
func stateSamples(t *testing.T) map[string][]any {
	samples := fixtureEvents(t, "events.sse")
	for name, more := range fixtureEvents(t, "events-reset.sse") {
		samples[name] = append(samples[name], more...)
	}
	base := samples["stage"][0].(map[string]any)
	for _, row := range decode(t, fixtureBytes(t, "run-landed.json")).(map[string]any)["run"].(map[string]any)["stages"].([]any) {
		ev := maps.Clone(base)
		ev["stage"] = row
		samples["stage"] = append(samples["stage"], ev)
	}
	return samples
}

// A fixture worker's segment reaches two independent clients as the same
// sequenced output, the state stream carries its stage, liveness and
// allowance events and never its output, and the run, its history and the
// dashboard read what the worker reported.
func TestHostedRunStreamsOutputLivenessAndMetrics(t *testing.T) {
	h := newV2Host(t)
	id := "weft/example-second-4c2d"
	h.ticket(id, ticket.StatusReady, 2)
	lines := h.fixtureSegment(id)
	stalled := indexOf(lines, "liveness") + 1
	lines = splice(lines, stalled, nil)
	review := 0
	for i, ev := range lines {
		if ev != nil && ev["stage"] == "review" && ev["state"] == "running" && ev["session"] != nil {
			review = i + 1
		}
	}
	lines = splice(lines, review,
		outputLine("review", 1, "Reading the diff.\n"), outputLine("review", 2, "Checking the cycle path.\n"),
		outputLine("review", 3, "Writing the verdict.\n"))
	h.script(id, 1, lines)
	server := httptest.NewServer(h.service)
	t.Cleanup(server.Close)

	entry := entryOf(h.launch(id, "launch-visible"))
	runID := h.waitDispatched(entry["entry_id"].(string))
	waitFor(t, func() bool { return strings.Contains(string(h.run(runID).Liveness), `"stalled":true`) })

	// Held mid-attempt: the run carries the stall, output is served, and the
	// attempt reads as running with what it has written so far.
	service := h.get("/v2/service")["service"].(map[string]any)
	for _, c := range service["capabilities"].([]any) {
		if c := c.(map[string]any); c["id"] == "output" && (c["state"] != "served" || c["reason"] != nil) {
			t.Fatalf("output under a weft-worker/v2 worker = %v", c)
		}
	}
	held := h.get("/v2/runs/" + runID)
	runShapeOf(t).check(t, "run", held)
	run := held["run"].(map[string]any)
	livenessShape := learn(fixtureEvents(t, "events.sse")["liveness"][0].(map[string]any)["liveness"])
	livenessShape.check(t, "liveness", run["liveness"])
	if want := map[string]any{"segment": float64(1), "stage": "work", "occurrence": float64(1), "attempt": float64(1),
		"stalled": true, "last_event_at": "2026-10-02T09:31:02.551Z", "observed_at": "2026-10-02T09:36:02.600Z"}; !reflect.DeepEqual(run["liveness"], want) {
		t.Fatalf("held run liveness = %v", run["liveness"])
	}
	if seg := run["segments"].([]any)[0].(map[string]any); seg["worker_contract"] != WorkerContractV2 {
		t.Fatalf("segment = %v", seg)
	}
	output := h.get("/v2/runs/" + runID + "/output")
	learn(decode(t, fixtureBytes(t, "output.json"))).check(t, "output", output)
	o := output["output"].(map[string]any)
	if o["state"] != "running" || o["first_chunk"] != float64(1) || o["last_chunk"] != float64(2) ||
		o["truncated"] != false || o["cursor"] != float64(2) || len(o["chunks"].([]any)) != 2 ||
		!reflect.DeepEqual(o["attempt"], map[string]any{"segment": float64(1), "stage": "work", "occurrence": float64(1), "attempt": float64(1)}) {
		t.Fatalf("held output = %v", o)
	}

	first := openPath(t, server, "/v2/runs/"+runID+"/output/stream", "0")
	second := openPath(t, server, "/v2/runs/"+runID+"/output/stream", "0")
	live := openPath(t, server, "/v2/runs/"+runID+"/output/stream", "")
	state := openStream(t, server, "0")
	h.release(id)

	outputShape := sample(t, "output.sse", "output")
	read := func(s *stream, n int) []sseEvent {
		var got []sseEvent
		for len(got) < n {
			ev := s.next(t)
			if ev.name != "output" {
				t.Fatalf("output stream event %+v", ev)
			}
			doc := decode(t, []byte(ev.data)).(map[string]any)
			outputShape.check(t, "output", doc)
			if fmt.Sprint(doc["cursor"]) != ev.id || doc["run_id"] != runID {
				t.Fatalf("output event id %s = %v", ev.id, doc)
			}
			got = append(got, ev)
		}
		return got
	}
	a, b := read(first, 5), read(second, 5)
	if !reflect.DeepEqual(a, b) {
		t.Fatalf("two clients disagree:\n%v\n%v", a, b)
	}
	for i, ev := range a {
		if ev.id != strconv.Itoa(i+1) {
			t.Fatalf("event %d id %s", i, ev.id)
		}
	}
	if tail := read(live, 3); !reflect.DeepEqual(tail, a[2:]) {
		t.Fatalf("a stream opened with no Last-Event-ID = %v, want the live tail %v", tail, a[2:])
	}

	seen := map[string]int{}
	samples := stateSamples(t)
	for {
		ev := state.next(t)
		seen[ev.name]++
		doc := decode(t, []byte(ev.data)).(map[string]any)
		if s := samples[ev.name]; s != nil {
			learn(s...).check(t, ev.name, doc)
		}
		if ev.name == "run" && doc["run_id"] == runID && doc["state"] == "over" {
			break
		}
	}
	if seen["stage"] != 13 || seen["liveness"] != 2 || seen["allowance"] != 1 || seen["output"] != 0 {
		t.Fatalf("state stream carried %v", seen)
	}

	h.waitReaped(runID)
	over := h.get("/v2/runs/" + runID)
	runShapeOf(t).check(t, "run", over)
	run = over["run"].(map[string]any)
	landed := decode(t, fixtureBytes(t, "run-landed.json")).(map[string]any)["run"].(map[string]any)
	if !reflect.DeepEqual(run["stages"], landed["stages"]) {
		t.Errorf("stages = %v\nrun-landed.json has %v", run["stages"], landed["stages"])
	}
	settlement := map[string]any{"state": "not_applicable", "step": nil, "reason": nil,
		"detail": ledger.WeftLandingNotServed, "branch": nil, "commit": nil, "steps": []any{}}
	if run["outcome"] != "finished" || run["liveness"] != nil || run["recorded_under"] != "weft-service/v2" ||
		!reflect.DeepEqual(run["settlement"], settlement) || run["head"] != landed["head"] {
		t.Errorf("finished run = %v", run)
	}
	result := h.get("/v2/runs/" + runID + "/result")
	learn(decode(t, fixtureBytes(t, "run-result.json"))).check(t, "result", result)
	if r := result["result"].(map[string]any); !reflect.DeepEqual(r["settlement"], settlement) ||
		r["summary"].(map[string]any)["state"] != "rendered" {
		t.Errorf("result = %v", r)
	}
	learn(decode(t, fixtureBytes(t, "runs.json"))).check(t, "runs", h.get("/v2/runs"))
	if o := h.get("/v2/runs/" + runID + "/output?segment=1&stage=review&occurrence=1&attempt=1")["output"].(map[string]any); o["state"] != "settled" || o["last_chunk"] != float64(3) || o["cursor"] != float64(5) {
		t.Errorf("review output = %v", o)
	}

	dashboard := h.get("/v2/dashboard")
	learn(decode(t, fixtureBytes(t, "dashboard.json"))).check(t, "dashboard", dashboard)
	claude := dashboard["dashboard"].(map[string]any)["allowance"].([]any)[0]
	if want := map[string]any{"provider": "claude", "state": "observed", "source": "claude_stream",
		"observed_at": "2026-10-02T09:16:40.003Z", "reason": nil, "windows": []any{
			map[string]any{"id": "five_hour", "utilization": 0.38, "resets_at": "2026-10-02T13:00:00.000Z"},
			map[string]any{"id": "seven_day", "utilization": 0.17, "resets_at": "2026-10-06T00:00:00.000Z"},
		}}; !reflect.DeepEqual(claude, want) {
		t.Errorf("claude tile = %v", claude)
	}

	argv, err := os.ReadFile(filepath.Join(h.control, "argv.log"))
	if err != nil || !strings.Contains(string(argv), "segment --contract weft-worker/v2 ") || strings.Contains(string(argv), "--land") {
		t.Errorf("worker argv = %s %v", argv, err)
	}
	ledgerID := h.run(runID).Segments[0].LedgerRunID
	ids := fmt.Sprintf("host=%s run=%s ledger=%s segment=1", h.service.hostID, runID, ledgerID)
	logs := h.logs.String()
	for _, line := range []string{
		"weft worker host=" + h.service.hostID + ": " + h.service.cfg.Weft.WorkerBin + " is weft-worker 0.2.0 (weft@dev)",
		"weft segment " + ids + " stage=-: launched launch segment",
		"weft stage " + ids + " stage=work occurrence=1 attempt=1: running",
		"weft liveness " + ids + " stage=work occurrence=1 attempt=1: stalled",
		"weft liveness " + ids + " stage=work occurrence=1 attempt=1: recovered",
		"weft segment " + ids + " stage=summary: settled after its terminal event",
	} {
		if !strings.Contains(logs, line) {
			t.Errorf("log lacks %q:\n%s", line, logs)
		}
	}
	for _, secret := range []string{"secret-token", fixtureOAuthToken} {
		if strings.Contains(logs, secret) {
			t.Errorf("log carries %q", secret)
		}
	}
}

// stuckWriter is a client that never reads: its first write blocks until
// released.
type stuckWriter struct {
	header  http.Header
	once    sync.Once
	entered chan struct{}
	release chan struct{}
}

func (w *stuckWriter) Header() http.Header { return w.header }
func (w *stuckWriter) WriteHeader(int)     {}
func (w *stuckWriter) Flush()              {}
func (w *stuckWriter) Write(p []byte) (int, error) {
	w.once.Do(func() { close(w.entered) })
	<-w.release
	return 0, io.ErrClosedPipe
}

// An attempt past 256 KiB reads truncated; a reconnect inside retention
// resumes exactly, one past it gets an explicit gap; and a client that never
// reads blocks neither the worker reader nor control.
func TestOutputRetentionGapsAndSlowClients(t *testing.T) {
	h := newV2Host(t)
	id := "weft/example-second-4c2d"
	h.ticket(id, ticket.StatusReady, 2)
	lines := h.fixtureSegment(id)
	var bulk []map[string]any
	for chunk := 3; chunk <= 42; chunk++ {
		bulk = append(bulk, outputLine("work", chunk, strings.Repeat(strconv.Itoa(chunk%10), 8<<10)))
	}
	lines = splice(lines, indexOf(lines, "liveness"), bulk...)
	graded := indexOf(lines, "graded")
	lines = splice(lines, graded, nil, outputLine("summary", 1, "Summarizing.\n"), outputLine("summary", 2, "Done.\n"))
	h.script(id, 1, lines)
	server := httptest.NewServer(h.service)
	t.Cleanup(server.Close)
	runID := h.waitDispatched(entryOf(h.launch(id, "launch-retention"))["entry_id"].(string))
	waitFor(t, func() bool {
		run := h.run(runID)
		_, newest, err := h.store.WeftOutputBounds(runID)
		return err == nil && newest == 42 && len(run.Stages) == 5 && run.Stages[4].State == "done"
	})

	output := h.get("/v2/runs/" + runID + "/output?segment=1&stage=work&occurrence=1&attempt=1")
	learn(decode(t, fixtureBytes(t, "output.json"))).check(t, "output", output)
	o := output["output"].(map[string]any)
	firstChunk := int(o["first_chunk"].(float64))
	chunks := o["chunks"].([]any)
	if o["truncated"] != true || firstChunk <= 1 || o["last_chunk"] != float64(42) || o["cursor"] != float64(42) ||
		len(chunks) != 42-firstChunk+1 || o["state"] != "settled" {
		t.Fatalf("truncated output = first %v last %v cursor %v truncated %v, %d chunks", o["first_chunk"], o["last_chunk"], o["cursor"], o["truncated"], len(chunks))
	}
	retained := 0
	for _, c := range chunks {
		retained += len(c.(map[string]any)["text"].(string))
	}
	if retained > ledger.WeftOutputRetainBytes {
		t.Fatalf("retained %d bytes", retained)
	}
	if latest := h.get("/v2/runs/" + runID + "/output")["output"].(map[string]any); latest["last_chunk"] != float64(42) {
		t.Fatalf("latest attempt = %v", latest["attempt"])
	}
	h.send(http.MethodGet, "/v2/runs/"+runID+"/output?segment=1&stage=work", "", "").refused(t, http.StatusBadRequest, "malformed")
	h.send(http.MethodGet, "/v2/runs/"+runID+"/output?segment=1&stage=deploy&occurrence=1&attempt=1", "", "").refused(t, http.StatusNotFound, "run_not_found")
	h.send(http.MethodGet, "/v2/runs/r-20200101T000000Z-00000000/output", "", "").refused(t, http.StatusNotFound, "run_not_found")

	// Past retention: one gap naming the cursor asked for and the oldest
	// retained, no id, then replay from exactly that oldest.
	gapped := openPath(t, server, "/v2/runs/"+runID+"/output/stream", "0")
	gap := gapped.next(t)
	sample(t, "output.sse", "gap").check(t, "gap", decode(t, []byte(gap.data)))
	if g := decode(t, []byte(gap.data)).(map[string]any); gap.name != "gap" || gap.id != "" || g["requested"] != float64(0) ||
		g["oldest"] != float64(firstChunk) || g["reason"] != "retention" || g["run_id"] != runID {
		t.Fatalf("first event past retention = %+v", gap)
	}
	if next := gapped.next(t); next.name != "output" || next.id != strconv.Itoa(firstChunk) {
		t.Fatalf("after the gap = %+v, want cursor %d", next, firstChunk)
	}
	gapped.close()
	// Inside retention: no gap, the next cursor exactly.
	resumed := openPath(t, server, "/v2/runs/"+runID+"/output/stream", "40")
	if next := resumed.next(t); next.name != "output" || next.id != "41" {
		t.Fatalf("resume after 40 = %+v", next)
	}
	resumed.close()
	refuseCursorPastNewest(t, h, runID)

	// A client that never reads: its stream blocks on its own write, holding
	// nothing the worker reader or a control route needs.
	w := &stuckWriter{header: http.Header{}, entered: make(chan struct{}), release: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest(http.MethodGet, "/v2/runs/"+runID+"/output/stream", nil).WithContext(ctx)
	req.Header.Set("Authorization", "Bearer secret-token")
	// Inside retention, so the write it blocks on is a replayed chunk.
	req.Header.Set("Last-Event-ID", strconv.Itoa(firstChunk))
	done := make(chan struct{})
	go func() {
		h.service.ServeHTTP(w, req)
		close(done)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-w.release:
		default:
			close(w.release)
		}
		<-done
	})
	<-w.entered
	prompt := func(what string, do func()) {
		t.Helper()
		start := time.Now()
		do()
		if elapsed := time.Since(start); elapsed > 2*time.Second {
			t.Fatalf("%s took %s behind a client that never reads", what, elapsed)
		}
	}
	prompt("GET /v2/queue", func() { h.queue() })
	h.release(id)
	prompt("the worker reader", func() { h.waitReaped(runID) })
	prompt("POST stop", func() {
		h.send(http.MethodPost, "/v2/runs/"+runID+"/stop", "stop-over", revisionBody(h.run(runID).Revision)).
			refused(t, http.StatusConflict, "run_over")
	})
	if _, newest, _ := h.store.WeftOutputBounds(runID); newest != 44 {
		t.Fatalf("output after the stuck client = %d chunks", newest)
	}
}

// refuseCursorPastNewest holds the output stream's refusal of a
// Last-Event-ID past what the run allocated.
func refuseCursorPastNewest(t *testing.T, h *queueHost, runID string) {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/v2/runs/"+runID+"/output/stream", nil)
	req.Header.Set("Authorization", "Bearer secret-token")
	req.Header.Set("Last-Event-ID", "999")
	rec := httptest.NewRecorder()
	h.service.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), `"malformed"`) {
		t.Fatalf("Last-Event-ID past newest = %d %s", rec.Code, rec.Body.String())
	}
}

// The worker probe gates output; a worker that does not speak
// weft-worker/v2 keeps weft-remote/v1 dispatch.
func TestWorkerProbeGatesOutputAndKeepsV1Dispatch(t *testing.T) {
	h := newQueueHost(t)
	if err := h.service.Restart(); err != nil {
		t.Fatal(err)
	}
	output := func() map[string]any {
		for _, c := range h.get("/v2/service")["service"].(map[string]any)["capabilities"].([]any) {
			if c := c.(map[string]any); c["id"] == "output" {
				return c
			}
		}
		t.Fatal("no output capability")
		return nil
	}
	if c := output(); c["state"] != "not_served" || !strings.Contains(c["reason"].(string), "failed: exited 64") {
		t.Fatalf("output under a v1 worker = %v", c)
	}
	// With no allowance relay to read, the Claude tile is unknown, says why,
	// and carries no figure (§2.3).
	claudeTile := func(reason string) {
		t.Helper()
		dashboard := h.get("/v2/dashboard")
		learn(decode(t, fixtureBytes(t, "dashboard.json"))).check(t, "dashboard", dashboard)
		if claude := dashboard["dashboard"].(map[string]any)["allowance"].([]any)[0].(map[string]any); claude["provider"] != "claude" ||
			claude["state"] != "unknown" || claude["source"] != "claude_stream" || claude["observed_at"] != nil ||
			len(claude["windows"].([]any)) != 0 || !strings.Contains(claude["reason"].(string), reason) {
			t.Fatalf("claude tile = %v, want unknown because %q", claude, reason)
		}
	}
	claudeTile("the probe of " + h.service.cfg.Weft.WorkerBin + " failed: exited 64")
	h.send(http.MethodGet, "/v2/runs/r-20261002T000000Z-00000000/output", "", "").refused(t, http.StatusNotImplemented, "capability_not_served")
	h.send(http.MethodGet, "/v2/runs/r-20261002T000000Z-00000000/output/stream", "", "").refused(t, http.StatusNotImplemented, "capability_not_served")
	if !strings.Contains(h.logs.String(), "weft worker host="+h.service.hostID+": probe of "+h.service.cfg.Weft.WorkerBin+" failed: exited 64") {
		t.Errorf("log lacks the probe diagnostic:\n%s", h.logs.String())
	}
	contract := func(ticketID string) string {
		h.ticket(ticketID, ticket.StatusReady, 2)
		runID := h.waitDispatched(entryOf(h.launch(ticketID, "launch-"+ticketID))["entry_id"].(string))
		h.waitPhase(runID, ledger.WeftPhaseParked)
		return h.get("/v2/runs/" + runID)["run"].(map[string]any)["segments"].([]any)[0].(map[string]any)["worker_contract"].(string)
	}
	if got := contract("weft/v1-only-0001"); got != ContractVersion {
		t.Fatalf("a v1 worker's segment ran under %s", got)
	}

	// A worker whose version names only weft-remote/v1 keeps v1 dispatch too.
	version := func(contracts, capabilities string) {
		h.writeControl("version.json", []byte(`{"contract_version":"weft-worker/v2","event":"version","worker_version":"weft-worker 0.1.9 (weft@old)","contracts":`+contracts+`,"capabilities":`+capabilities+`}`))
		h.worker(strings.Replace(strings.Replace(queueWorkerScript, "CONTROL", h.control, 1),
			`case "$1" in version|allowance-probe) exit 64 ;; esac`,
			`case "$1" in version) cat "`+h.control+`/version.json"; exit 0 ;; allowance-probe) exit 64 ;; esac`, 1))
		h.service.dispatch()
	}
	version(`["weft-remote/v1"]`, `["segment"]`)
	if c := output(); c["state"] != "not_served" || !strings.Contains(c["reason"].(string), "speaks only weft-remote/v1") {
		t.Fatalf("output under a v1-only version = %v", c)
	}
	claudeTile("it does not list the allowance capability of weft-worker/v2")
	if got := contract("weft/v1-only-0002"); got != ContractVersion {
		t.Fatalf("a v1-only worker's segment ran under %s", got)
	}
	version(`["weft-remote/v1","weft-worker/v2"]`, `["segment","output","stale_base_check"]`)
	if c := output(); c["state"] != "not_served" || !strings.Contains(c["reason"].(string), "lacks liveness") {
		t.Fatalf("output without liveness = %v", c)
	}
	version(`["weft-remote/v1","weft-worker/v2"]`, `["segment","output","liveness","telepathy"]`)
	if c := output(); c["state"] != "not_served" || !strings.Contains(c["reason"].(string), "undeclared or repeated capability") {
		t.Fatalf("output under an undeclared capability = %v", c)
	}
	h.settle()
}

// The Codex probe runs on this host, bounded and in the background: a tile
// with nothing observed never carries a figure, newer wins, a stale
// dashboard read starts one probe and waits on none, a hung probe is killed
// at its bound, and nothing the probe printed reaches a log or a response.
func TestCodexAllowanceProbe(t *testing.T) {
	h := newV2Host(t)
	prior := providerProbeTimeout
	providerProbeTimeout = 500 * time.Millisecond
	t.Cleanup(func() { providerProbeTimeout = prior })
	tiles := func() (claude, codex map[string]any, raw string) {
		r := h.send(http.MethodGet, "/v2/dashboard", "", "")
		learn(decode(t, fixtureBytes(t, "dashboard.json"))).check(t, "dashboard", r.body)
		allowance := r.body["dashboard"].(map[string]any)["allowance"].([]any)
		for _, tile := range allowance {
			tile := tile.(map[string]any)
			if tile["state"] != "observed" && (tile["observed_at"] != nil || len(tile["windows"].([]any)) != 0 || tile["reason"] == nil) {
				t.Fatalf("tile with nothing observed = %v", tile)
			}
		}
		return allowance[0].(map[string]any), allowance[1].(map[string]any), r.raw
	}
	probe := func(doc string) {
		t.Helper()
		h.writeControl("probe.json", []byte(doc))
		h.service.probeCodex(true)
		h.service.probes.Wait()
	}
	unobserved := func(outcome, windows string) string {
		return `{"contract_version":"weft-worker/v2","event":"allowance_probe","provider":"codex","source":"codex_app_server","outcome":"` +
			outcome + `","observed_at":null,"windows":` + windows + `}`
	}
	observed := func(at string, utilization float64) string {
		return fmt.Sprintf(`{"contract_version":"weft-worker/v2","event":"allowance_probe","provider":"codex","source":"codex_app_server","outcome":"observed","observed_at":%q,"windows":[{"id":"primary","utilization":%v,"resets_at":"2026-10-02T13:00:00.000Z"}]}`, at, utilization)
	}

	h.writeControl("probe.json", fixtureBytes(t, "worker-allowance-probe.json"))
	if err := h.service.Restart(); err != nil {
		t.Fatal(err)
	}
	h.service.probes.Wait()
	claude, codex, _ := tiles()
	if claude["state"] != "unknown" || claude["source"] != "claude_stream" || codex["state"] != "unknown" ||
		codex["source"] != "codex_app_server" || !strings.Contains(codex["reason"].(string), "within the probe") {
		t.Fatalf("after a timed-out probe at start: claude %v, codex %v", claude, codex)
	}
	probe(unobserved("no_source", "[]"))
	if _, codex, _ = tiles(); codex["state"] != "unavailable" || codex["source"] != nil {
		t.Fatalf("no_source = %v", codex)
	}
	probe(unobserved("timed_out", `[{"id":"primary","utilization":0,"resets_at":"2026-10-02T13:00:00.000Z"}]`))
	if _, codex, _ = tiles(); codex["state"] != "unknown" || !strings.Contains(codex["reason"].(string), "an unobserved outcome carries a figure") {
		t.Fatalf("an unobserved outcome with a figure = %v", codex)
	}
	for _, leak := range []string{
		"token: sk-ant-oat01-PROBESECRET-0001\n",
		`{"contract_version":"weft-worker/v2","event":"allowance_probe","provider":"codex","source":"codex_app_server","outcome":"timed_out","observed_at":null,"windows":[],"sk-ant-oat01-PROBESECRET-0002":true}`,
	} {
		probe(leak)
		if _, codex, raw := tiles(); codex["state"] != "unknown" || strings.Contains(raw, "PROBESECRET") {
			t.Fatalf("a token-looking probe = %s", raw)
		}
	}
	probe(observed("2026-10-02T09:58:41.306Z", 0.42))
	_, codex, _ = tiles()
	if codex["state"] != "observed" || codex["observed_at"] != "2026-10-02T09:58:41.306Z" || codex["reason"] != nil ||
		codex["windows"].([]any)[0].(map[string]any)["utilization"] != 0.42 {
		t.Fatalf("observed = %v", codex)
	}
	probe(observed("2026-10-02T09:00:00.000Z", 0.10))
	if _, again, _ := tiles(); !reflect.DeepEqual(again, codex) {
		t.Fatalf("an older observation replaced a newer one: %v", again)
	}

	// Stale: concurrent reads answer at once and start exactly one probe,
	// which hangs, is killed at its bound, and leaves the observation held.
	probes := func() int {
		data, _ := os.ReadFile(filepath.Join(h.control, "probe.log"))
		return strings.Count(string(data), "\n")
	}
	before := probes()
	h.writeControl("probe.hang", nil)
	h.service.codex.mu.Lock()
	h.service.codex.started = time.Now().Add(-codexProbeInterval - time.Second)
	h.service.codex.mu.Unlock()
	var wg sync.WaitGroup
	for range 3 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			start := time.Now()
			if r := h.send(http.MethodGet, "/v2/dashboard", "", ""); r.code != http.StatusOK {
				t.Errorf("dashboard = %d", r.code)
			}
			if elapsed := time.Since(start); elapsed >= providerProbeTimeout {
				t.Errorf("a dashboard read waited %s on the probe", elapsed)
			}
		}()
	}
	wg.Wait()
	h.service.probes.Wait()
	if n := probes(); n != before+1 {
		t.Fatalf("a stale read started %d probes", n-before)
	}
	pid, err := os.ReadFile(filepath.Join(h.control, "probe.pid"))
	if err != nil {
		t.Fatal(err)
	}
	if n, _ := strconv.Atoi(string(pid)); n <= 0 || !errors.Is(syscall.Kill(n, 0), syscall.ESRCH) {
		t.Fatalf("the hung probe %s outlived its bound", pid)
	}
	if _, after, _ := tiles(); !reflect.DeepEqual(after, codex) {
		t.Fatalf("a failed probe replaced the held observation: %v", after)
	}
	h.service.probes.Wait()
	if n := probes(); n != before+1 {
		t.Fatalf("a fresh read started a probe: %d", n-before)
	}
	logs := h.logs.String()
	if !strings.Contains(logs, "weft allowance host="+h.service.hostID+" provider=codex: probe failed: killed at its 500ms bound") ||
		strings.Contains(logs, "PROBESECRET") {
		t.Fatalf("probe log:\n%s", logs)
	}
}

// A run's projections read the same before and after the ledger is reopened
// under a new host process.
func TestRunProjectionsSurviveRestart(t *testing.T) {
	h := newV2Host(t)
	id := "weft/example-second-4c2d"
	h.ticket(id, ticket.StatusReady, 2)
	h.script(id, 1, h.fixtureSegment(id))
	runID := h.waitDispatched(entryOf(h.launch(id, "launch-restart"))["entry_id"].(string))
	h.waitReaped(runID)
	paths := []string{"/v2/runs", "/v2/runs/" + runID, "/v2/runs/" + runID + "/result",
		"/v2/runs/" + runID + "/output?segment=1&stage=work&occurrence=1&attempt=1"}
	// as_of may move: the restart writes what it writes. The projections
	// may not.
	read := func(path string) map[string]any {
		doc := h.get(path)
		delete(doc, "as_of")
		return doc
	}
	before := map[string]map[string]any{}
	for _, path := range paths {
		before[path] = read(path)
	}
	// A new host process: its own ledger handle and service over the same
	// file. The first host is idle, every segment reaped.
	store, err := ledger.Open(filepath.Join(h.root, "runs.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	restarted, err := New(h.service.cfg, store, "secret-token", h.root, log.New(h.logs, "", 0))
	if err != nil {
		t.Fatal(err)
	}
	if err := restarted.Restart(); err != nil {
		t.Fatal(err)
	}
	restarted.probes.Wait()
	other := &queueHost{serviceFixture: &serviceFixture{root: h.root, store: store, service: restarted}, t: t}
	for _, path := range paths {
		doc := other.get(path)
		delete(doc, "as_of")
		if after := doc; !reflect.DeepEqual(after, before[path]) {
			t.Errorf("GET %s after restart = %v\nbefore = %v", path, after, before[path])
		}
	}
}

// A parked weft-worker/v2 run answered through the service resumes with
// answer-file.json, and nothing logged about it carries the token, the
// question or the answer, while its lines join host, request, run, segment
// and stage.
func TestHostedAnswerSegmentAndDiagnostics(t *testing.T) {
	h := newV2Host(t)
	id := "weft/example-fourth-6e4f"
	h.ticket(id, ticket.StatusReady, 2)
	lines := h.fixtureSegment(id)
	hello := maps.Clone(lines[0])
	var parked map[string]any
	fixtureJSON(t, "worker-parked.json", &parked)
	question := parked["question"].(string)
	hello["worker_run_id"], parked["worker_run_id"] = parked["worker_run_id"], parked["worker_run_id"]
	work := maps.Clone(lines[1])
	work["worker_run_id"] = parked["worker_run_id"]
	out := outputLine("work", 1, "Reading the rail.\n")
	out["worker_run_id"] = parked["worker_run_id"]
	h.script(id, 1, workerLines{hello, work, out, parked})

	resumed := maps.Clone(hello)
	resumed["worker_run_id"], resumed["resumed"], resumed["answer"] = "20261002T100510Z-73e4f5a6", parked["worker_run_id"], "@QID@"
	tail := workerLines{resumed}
	for _, ev := range lines[1:] {
		if ev["event"] == "allowance" || ev["event"] == "liveness" {
			continue
		}
		ev = maps.Clone(ev)
		ev["worker_run_id"] = resumed["worker_run_id"]
		tail = append(tail, ev)
	}
	h.script(id, 2, tail)

	runID := h.waitDispatched(entryOf(h.launch(id, "launch-parked"))["entry_id"].(string))
	h.waitPhase(runID, ledger.WeftPhaseParked)
	run := h.get("/v2/runs/" + runID)["run"].(map[string]any)
	q := run["question"].(map[string]any)
	if q["state"] != "pending" || q["text"] != question || run["liveness"] != nil {
		t.Fatalf("parked run = %v", run)
	}
	const answerText = "The project. SECRET-ANSWER-BODY stays out of the log."
	const key = "answer-key-0001"
	body := fmt.Sprintf(`{"contract_version":"weft-service/v2","question_id":%q,"answer":%q}`, q["question_id"], answerText)
	answered := h.send(http.MethodPost, "/v2/runs/"+runID+"/answer", key, body)
	answered.expect(t, http.StatusAccepted, "answer-response.json")
	answerID := answered.body["question"].(map[string]any)["answer"].(map[string]any)["answer_id"]
	over := h.waitReaped(runID)
	if over.Outcome.String != "finished" || len(over.Segments) != 2 || over.Segments[1].WorkerContract.String != WorkerContractV2 {
		t.Fatalf("answered run = %+v", over)
	}

	var file map[string]any
	data, err := os.ReadFile(filepath.Join(h.control, slugOf(id)+"-2.answer.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &file); err != nil {
		t.Fatal(err)
	}
	learn(decode(t, fixtureBytes(t, "answer-file.json"))).check(t, "answer file", file)
	if file["contract_version"] != WorkerContractV2 || file["question_id"] != q["question_id"] || file["answer_id"] != answerID ||
		file["question"] != question || file["answer"] != answerText {
		t.Fatalf("answer file = %v", file)
	}

	sum := sha256.Sum256([]byte(key))
	logs := h.logs.String()
	ids := fmt.Sprintf("host=%s run=%s ledger=%s segment=2", h.service.hostID, runID, over.Segments[1].LedgerRunID)
	for _, line := range []string{
		"weft service host=" + h.service.hostID + " key=" + hex.EncodeToString(sum[:]) + ": POST /v2/runs/" + runID + "/answer 202",
		"key=" + hex.EncodeToString(sum[:]) + " entry=" + answered.body["entry"].(map[string]any)["entry_id"].(string) + " run=" + runID + ": dispatched answer",
		"weft segment " + ids + " stage=-: launched answer segment",
		"weft stage " + ids + " stage=review occurrence=1 attempt=1: done",
		"weft segment " + ids + " stage=summary: settled after its terminal event",
	} {
		if !strings.Contains(logs, line) {
			t.Errorf("log lacks %q:\n%s", line, logs)
		}
	}
	for _, secret := range []string{"secret-token", fixtureOAuthToken, "SECRET-ANSWER-BODY", question[:40], key} {
		if strings.Contains(logs, secret) {
			t.Errorf("log carries %q:\n%s", secret, logs)
		}
	}
}

// A weft-worker/v2 segment fails protocol on what the reader refuses: the
// worker negatives of the manifest, a landing event or landed ended from a
// segment launched without --land, a hello that disagrees with the probe,
// and output chunks out of sequence.
func TestV2SegmentRefusals(t *testing.T) {
	const sentinel = "SENTINEL-ANSWER-BODY-7f3c"
	h := newV2Host(t)
	var runIDs []string
	base := h.fixtureSegment("weft/refusal-0000")
	hello := func(ticketID string) map[string]any {
		ev := maps.Clone(base[0])
		ev["worktree"], ev["ticket"], ev["branch"] = h.worktree(ticketID), ticketID, ticketID
		return ev
	}
	var unknownField, v1Stage map[string]any
	fixtureJSON(t, "negative/worker-hello-unknown-field.json", &unknownField)
	fixtureJSON(t, "negative/worker-stage-v1.json", &v1Stage)
	// The fixture's first land_intent and its landed ended, as written.
	var landIntent, landedEnded map[string]any
	for _, line := range strings.Split(strings.TrimSpace(string(fixtureBytes(t, "worker-segment.ndjson"))), "\n") {
		switch ev := decode(t, []byte(line)).(map[string]any); {
		case ev["event"] == "land_intent" && landIntent == nil:
			landIntent = ev
		case ev["event"] == "ended":
			landedEnded = ev
		}
	}
	graded := base[indexOf(base, "graded")]
	for i, tt := range []struct {
		name   string
		lines  func(id string) workerLines
		detail string
	}{
		{"unknown hello key", func(id string) workerLines {
			ev := maps.Clone(unknownField)
			ev["worktree"], ev["ticket"], ev["branch"], ev["land"] = h.worktree(id), id, id, false
			return workerLines{ev}
		}, "hello carries an undeclared key"},
		{"v1 event in a v2 segment", func(id string) workerLines { return workerLines{hello(id), v1Stage} },
			"envelope does not match the launched segment"},
		{"capabilities the probe did not find", func(id string) workerLines {
			ev := hello(id)
			ev["capabilities"] = []any{"segment", "output", "liveness"}
			return workerLines{ev}
		}, "hello worker_version or capabilities differ from the probe's"},
		{"land_intent without --land", func(id string) workerLines { return workerLines{hello(id), graded, landIntent} },
			"land_intent from a segment launched without --land"},
		{"landed without --land", func(id string) workerLines { return workerLines{hello(id), graded, landedEnded} },
			"ended reports a landing other than not_applicable"},
		{"output out of sequence", func(id string) workerLines {
			return workerLines{hello(id), outputLine("work", 1, "one\n"), outputLine("work", 3, "three\n")}
		}, "output chunk is out of sequence for its attempt, which wants chunk 2"},
		// A malformed hello's values — an answer body where the question id
		// belongs, a key, a timestamp, a mistyped field — are named by field
		// and class, never quoted.
		{"answer body in hello", func(id string) workerLines {
			ev := hello(id)
			ev["answer"] = sentinel + " the answer's text"
			return workerLines{ev}
		}, "hello answer does not name the question the segment's answer is for"},
		{"undeclared key", func(id string) workerLines {
			ev := hello(id)
			ev[sentinel] = true
			return workerLines{ev}
		}, "hello carries an undeclared key"},
		{"timestamp", func(id string) workerLines {
			ev := hello(id)
			ev["ts"] = sentinel
			return workerLines{ev}
		}, "worker event ts is not an RFC 3339 timestamp"},
		{"output without text", func(id string) workerLines {
			ev := outputLine("work", 1, "")
			delete(ev, "text")
			return workerLines{hello(id), ev}
		}, "output field text is missing or null"},
		{"output with null text", func(id string) workerLines {
			ev := outputLine("work", 1, "")
			ev["text"] = nil
			return workerLines{hello(id), ev}
		}, "output field text is missing or null"},
		{"wrong type", func(id string) workerLines {
			ev := hello(id)
			ev["answer"] = map[string]any{"text": sentinel}
			return workerLines{ev}
		}, "hello field answer has the wrong type"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			id := fmt.Sprintf("weft/refusal-%04d", i+1)
			h.ticket(id, ticket.StatusReady, 2)
			h.script(id, 1, tt.lines(id))
			runID := h.waitDispatched(entryOf(h.launch(id, "launch-"+id))["entry_id"].(string))
			runIDs = append(runIDs, runID)
			run := h.waitReaped(runID)
			if row, err := h.store.Get(run.Segments[0].LedgerRunID); err != nil || strings.Contains(row.ErrorSignature.String, sentinel) ||
				strings.Contains(run.HaltDetail.String, sentinel) {
				t.Fatalf("the ledger records a worker value: %q / %q %v", row.ErrorSignature.String, run.HaltDetail.String, err)
			}
			// Each refusal counts toward the failure streak that pauses the
			// host; the next case starts from none.
			if err := h.store.Resume(ledger.WeftAgent); err != nil {
				t.Fatal(err)
			}
			if run.Outcome.String != "failed" || run.Segments[0].Failure.String != ledger.ClassProtocol ||
				!strings.Contains(run.HaltDetail.String, tt.detail) {
				t.Fatalf("run = outcome %s failure %s detail %q, want protocol %q",
					run.Outcome.String, run.Segments[0].Failure.String, run.HaltDetail.String, tt.detail)
			}
			if line := fmt.Sprintf("weft segment host=%s run=%s ledger=%s segment=1", h.service.hostID, runID, run.Segments[0].LedgerRunID); !strings.Contains(h.logs.String(), line) ||
				!strings.Contains(h.logs.String(), ": failed protocol, exit ") {
				t.Fatalf("log lacks the protocol failure of %s:\n%s", runID, h.logs.String())
			}
		})
	}
	if strings.Contains(h.logs.String(), sentinel) {
		t.Errorf("log quotes a worker value:\n%s", h.logs.String())
	}
	paths := []string{"/v2/runs", "/v2/questions", "/v2/dashboard", "/v2/events", "/v1/runs"}
	for _, runID := range runIDs {
		paths = append(paths, "/v2/runs/"+runID, "/v2/runs/"+runID+"/result", "/v1/runs/"+runID, "/v1/runs/"+runID+"/result")
	}
	for _, path := range paths {
		if path == "/v2/events" {
			continue
		}
		if r := h.send(http.MethodGet, path, "", ""); r.code != http.StatusOK || strings.Contains(r.raw, sentinel) {
			t.Errorf("GET %s = %d, quoting a worker value: %v", path, r.code, strings.Contains(r.raw, sentinel))
		}
	}
	server := httptest.NewServer(h.service)
	t.Cleanup(server.Close)
	state := openStream(t, server, "0")
	for {
		ev := state.next(t)
		if strings.Contains(ev.data, sentinel) {
			t.Fatalf("state event quotes a worker value: %s", ev.data)
		}
		if ev.id == fmt.Sprint(mustAsOf(t, h)) {
			break
		}
	}
}

func mustAsOf(t *testing.T, h *queueHost) int64 {
	t.Helper()
	asOf, err := h.store.WeftAsOf()
	if err != nil {
		t.Fatal(err)
	}
	return asOf
}

// Once retention drops an ended run's output, its attempts read as
// truncated through their last chunk, and a stream from before them gets a
// gap: missing output never reads as output never written.
func TestPrunedOutputReadsTruncated(t *testing.T) {
	h := newV2Host(t)
	id := "weft/example-second-4c2d"
	h.ticket(id, ticket.StatusReady, 2)
	h.script(id, 1, h.fixtureSegment(id))
	runID := h.waitDispatched(entryOf(h.launch(id, "launch-pruned"))["entry_id"].(string))
	h.waitReaped(runID)
	if err := h.store.PruneWeftRetention(time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	outputShape := learn(decode(t, fixtureBytes(t, "output.json")))
	pruned := h.get("/v2/runs/" + runID + "/output?segment=1&stage=work&occurrence=1&attempt=1")
	outputShape.check(t, "output", pruned)
	o := pruned["output"].(map[string]any)
	if o["truncated"] != true || o["first_chunk"] != float64(3) || o["last_chunk"] != float64(2) ||
		o["cursor"] != float64(2) || len(o["chunks"].([]any)) != 0 || o["state"] != "settled" {
		t.Fatalf("pruned attempt = %v", o)
	}
	if latest := h.get("/v2/runs/" + runID + "/output")["output"].(map[string]any); !reflect.DeepEqual(latest, o) {
		t.Fatalf("latest attempt after the prune = %v", latest)
	}
	silent := h.get("/v2/runs/" + runID + "/output?segment=1&stage=review&occurrence=1&attempt=1")["output"].(map[string]any)
	if silent["truncated"] != false || silent["first_chunk"] != float64(0) || silent["last_chunk"] != float64(0) {
		t.Fatalf("an attempt that wrote nothing = %v", silent)
	}
	server := httptest.NewServer(h.service)
	t.Cleanup(server.Close)
	gap := openPath(t, server, "/v2/runs/"+runID+"/output/stream", "0").next(t)
	if g := decode(t, []byte(gap.data)).(map[string]any); gap.name != "gap" || g["requested"] != float64(0) || g["oldest"] != float64(3) {
		t.Fatalf("stream over a pruned run = %+v", gap)
	}
}

// An output chunk the host cannot retain fails the segment, and the log line
// saying so joins the host, run, ledger row, segment, stage and attempt.
func TestOutputCaptureFailureIsLogged(t *testing.T) {
	h := newV2Host(t)
	id := "weft/example-second-4c2d"
	h.ticket(id, ticket.StatusReady, 2)
	lines := h.fixtureSegment(id)
	h.script(id, 1, workerLines{lines[0], nil, outputLine("work", 1, "Reading.\n")})
	runID := h.waitDispatched(entryOf(h.launch(id, "launch-capture"))["entry_id"].(string))
	h.waitPhase(runID, ledger.WeftPhaseRunning)
	h.exec(`CREATE TRIGGER refuse_output BEFORE INSERT ON weft_output BEGIN SELECT RAISE(ABORT, 'output store refused the write'); END`)
	h.release(id)
	run := h.waitReaped(runID)
	if run.Segments[0].Failure.String != ledger.ClassProtocol || !strings.Contains(run.HaltDetail.String, "capture output chunk 1") {
		t.Fatalf("run = failure %s detail %q", run.Segments[0].Failure.String, run.HaltDetail.String)
	}
	line := fmt.Sprintf("weft output host=%s run=%s ledger=%s segment=1 stage=work occurrence=1 attempt=1: capture of chunk 1 failed: ",
		h.service.hostID, runID, run.Segments[0].LedgerRunID)
	if !strings.Contains(h.logs.String(), line) {
		t.Fatalf("log lacks %q:\n%s", line, h.logs.String())
	}
}

// An attempt flooded with empty chunks stays bounded in rows and in the size
// of its read, and still reports what it dropped: truncated, and a gap for a
// stream from before it.
func TestEmptyChunkFloodStaysBounded(t *testing.T) {
	h := newV2Host(t)
	id := "weft/example-second-4c2d"
	h.ticket(id, ticket.StatusReady, 2)
	h.script(id, 1, h.fixtureSegment(id))
	runID := h.waitDispatched(entryOf(h.launch(id, "launch-flood"))["entry_id"].(string))
	h.waitReaped(runID)
	_, before, err := h.store.WeftOutputBounds(runID)
	if err != nil {
		t.Fatal(err)
	}
	flood := ledger.WeftAttempt{Segment: 1, Stage: "summary", Occurrence: 1, Attempt: 1}
	total := ledger.WeftOutputRetainChunks + 300
	for i := 1; i <= total; i++ {
		if _, err := h.store.RecordWeftOutput(ledger.WeftOutputChunk{RunID: runID, WeftAttempt: flood, Chunk: i, At: "2026-10-02T10:29:00.000Z"}); err != nil {
			t.Fatal(err)
		}
	}
	r := h.send(http.MethodGet, "/v2/runs/"+runID+"/output?segment=1&stage=summary&occurrence=1&attempt=1", "", "")
	if r.code != http.StatusOK {
		t.Fatalf("output = %d %s", r.code, r.raw)
	}
	learn(decode(t, fixtureBytes(t, "output.json"))).check(t, "output", r.body)
	o := r.body["output"].(map[string]any)
	if n := len(o["chunks"].([]any)); n != ledger.WeftOutputRetainChunks || o["first_chunk"] != float64(301) ||
		o["last_chunk"] != float64(total) || o["truncated"] != true || o["cursor"] != float64(before+int64(total)) {
		t.Fatalf("flooded attempt = %d chunks, first %v last %v truncated %v cursor %v", n, o["first_chunk"], o["last_chunk"], o["truncated"], o["cursor"])
	}
	if limit := ledger.WeftOutputRetainChunks * 100; len(r.raw) > limit {
		t.Fatalf("flooded read is %d bytes, over %d", len(r.raw), limit)
	}
	server := httptest.NewServer(h.service)
	t.Cleanup(server.Close)
	str := openPath(t, server, "/v2/runs/"+runID+"/output/stream", strconv.FormatInt(before, 10))
	gap := str.next(t)
	if g := decode(t, []byte(gap.data)).(map[string]any); gap.name != "gap" || g["requested"] != float64(before) ||
		g["oldest"] != float64(before+301) {
		t.Fatalf("stream over the flood = %+v", gap)
	}
	if next := str.next(t); next.name != "output" || next.id != strconv.FormatInt(before+301, 10) {
		t.Fatalf("after the gap = %+v", next)
	}
}

// Output past the retention cap leaves the run directory's events.ndjson
// holding every other event verbatim and none of the output: its size does
// not follow the text a worker writes.
func TestEventsRecordKeepsNoOutputText(t *testing.T) {
	h := newV2Host(t)
	id := "weft/example-second-4c2d"
	h.ticket(id, ticket.StatusReady, 2)
	lines := h.fixtureSegment(id)
	const flood = "FLOOD-TEXT-"
	var bulk []map[string]any
	for chunk := 3; chunk <= 42; chunk++ {
		bulk = append(bulk, outputLine("work", chunk, strings.Repeat(flood, (8<<10)/len(flood))))
	}
	lines = splice(lines, indexOf(lines, "liveness"), bulk...)
	h.script(id, 1, lines)
	runID := h.waitDispatched(entryOf(h.launch(id, "launch-record"))["entry_id"].(string))
	run := h.waitReaped(runID)
	if run.Outcome.String != "finished" {
		t.Fatalf("run = %+v", run)
	}
	if _, newest, err := h.store.WeftOutputBounds(runID); err != nil || newest != 42 {
		t.Fatalf("output cursor = %d %v", newest, err)
	}
	record, err := os.ReadFile(filepath.Join(run.Segments[0].RunDir.String, "events.ndjson"))
	if err != nil {
		t.Fatal(err)
	}
	others := 0
	for _, ev := range lines {
		if ev != nil && ev["event"] != "output" {
			others++
		}
	}
	recorded := strings.Split(strings.TrimSpace(string(record)), "\n")
	if len(recorded) != others || strings.Contains(string(record), flood) || strings.Contains(string(record), `"event":"output"`) {
		t.Fatalf("events.ndjson holds %d lines (%d bytes), want the %d events other than output", len(recorded), len(record), others)
	}
	if len(record) > 32<<10 {
		t.Fatalf("events.ndjson is %d bytes after 320 KiB of output", len(record))
	}
}
