package weftremote

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"sync"
	"time"

	"shuttle/internal/ledger"
)

var outputRoute = regexp.MustCompile(`^/v2/runs/([^/]+)/output(/stream)?$`)

// outputBatch bounds one ledger read of an output stream, so a stream far
// behind holds the ledger's one connection only briefly.
const outputBatch = 256

const outputKeepAlive = 15 * time.Second

// broadcast wakes every waiting output stream without ever blocking the
// notifier: notify closes the current channel and installs a fresh one. A
// stream re-reads the ledger from its own cursor when woken, so a slow client
// costs the worker reader nothing.
type broadcast struct {
	mu sync.Mutex
	ch chan struct{}
}

func newBroadcast() *broadcast { return &broadcast{ch: make(chan struct{})} }

func (b *broadcast) wait() <-chan struct{} {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.ch
}

func (b *broadcast) notify() {
	b.mu.Lock()
	close(b.ch)
	b.ch = make(chan struct{})
	b.mu.Unlock()
}

// handleOutput is GET /v2/runs/{id}/output (§8.1): one attempt's retained
// chunks — the attempt the query names, or the run's latest. Up to 256 KiB
// of text, so it is read under the lock and written after it.
func (s *Service) handleOutput(w http.ResponseWriter, r *http.Request, runID string) {
	if !validProtocolID(runID) {
		s.v2Error(w, http.StatusNotFound, "run_not_found", runID+" is not a run on host "+s.hostID)
		return
	}
	attempt, named, err := attemptQuery(r)
	if err != nil {
		s.v2Error(w, http.StatusBadRequest, "malformed", err.Error())
		return
	}
	s.stateMu.RLock()
	o, err := s.readOutput(runID, attempt, named)
	s.stateMu.RUnlock()
	if o.missing != "" {
		s.v2Error(w, http.StatusNotFound, "run_not_found", o.missing)
		return
	}
	if err != nil {
		s.v2Internal(w, err)
		return
	}
	state := "settled"
	for _, st := range o.run.Stages {
		if st.Segment == o.attempt.Segment && st.Stage == o.attempt.Stage && st.Occurrence == o.attempt.Occurrence &&
			st.Attempt == o.attempt.Attempt && st.State == "running" && o.run.Phase == ledger.WeftPhaseRunning {
			state = "running"
		}
	}
	// An attempt that never wrote streams on from the run's newest cursor, so
	// a client reading it next misses nothing. One whose output retention
	// dropped entirely reads as truncated through its last chunk: first_chunk
	// is past last_chunk, nothing lies between them, and the loss is explicit.
	first, last, cursor := 0, 0, o.newest
	listed := make([]map[string]any, 0, len(o.chunks))
	for _, c := range o.chunks {
		listed = append(listed, map[string]any{"chunk": c.Chunk, "at": c.At, "text": c.Text})
	}
	switch n := len(o.chunks); {
	case n > 0:
		first, last, cursor = o.chunks[0].Chunk, o.chunks[n-1].Chunk, o.chunks[n-1].Cursor
	case o.highChunk > 0:
		first, last, cursor = o.highChunk+1, o.highChunk, o.highCursor
	}
	s.read(w, o.asOf, map[string]any{"output": map[string]any{
		"run_id": runID, "attempt": o.attempt, "state": state, "first_chunk": first, "last_chunk": last,
		"truncated": first > 1, "cursor": cursor, "chunks": listed,
	}})
}

// outputRead is what an output response is built from; missing says why the
// run or attempt was not found.
type outputRead struct {
	run          *ledger.WeftRun
	attempt      ledger.WeftAttempt
	chunks       []ledger.WeftOutputChunk
	newest, asOf int64
	// highChunk and highCursor are the attempt's last chunk written, retained
	// or not.
	highChunk  int
	highCursor int64
	missing    string
}

func (s *Service) readOutput(runID string, attempt ledger.WeftAttempt, named bool) (outputRead, error) {
	o := outputRead{attempt: attempt}
	var err error
	if o.run, err = s.store.GetWeftRun(runID); err != nil {
		o.missing = runID + " is not a run on host " + s.hostID
		return o, nil
	}
	if !named {
		o.attempt, err = s.store.WeftLatestAttempt(runID)
		if errors.Is(err, sql.ErrNoRows) {
			o.missing = "run " + runID + " on host " + s.hostID + " has reported no stage attempt"
			return o, nil
		}
	} else {
		var known bool
		known, err = s.store.WeftAttemptKnown(runID, attempt)
		if err == nil && !known {
			o.missing = fmt.Sprintf("run %s on host %s has no attempt %d of %s in segment %d, occurrence %d",
				runID, s.hostID, attempt.Attempt, attempt.Stage, attempt.Segment, attempt.Occurrence)
			return o, nil
		}
	}
	if err != nil {
		return o, err
	}
	if o.chunks, err = s.store.WeftAttemptOutput(runID, o.attempt); err != nil {
		return o, err
	}
	if o.highChunk, o.highCursor, err = s.store.WeftOutputHighWater(runID, o.attempt); err != nil {
		return o, err
	}
	if _, o.newest, err = s.store.WeftOutputBounds(runID); err != nil {
		return o, err
	}
	o.asOf, err = s.store.WeftAsOf()
	return o, err
}

// attemptQuery reads ?segment=&stage=&occurrence=&attempt=: all four, or
// none for the run's latest attempt.
func attemptQuery(r *http.Request) (ledger.WeftAttempt, bool, error) {
	q := r.URL.Query()
	keys := []string{"segment", "stage", "occurrence", "attempt"}
	given := 0
	for _, k := range keys {
		if q.Has(k) {
			given++
		}
	}
	if given == 0 {
		return ledger.WeftAttempt{}, false, nil
	}
	if given != len(keys) {
		return ledger.WeftAttempt{}, false, fmt.Errorf("output takes segment, stage, occurrence and attempt together, or none of them")
	}
	a := ledger.WeftAttempt{Stage: q.Get("stage")}
	for _, n := range []struct {
		key string
		dst *int
	}{{"segment", &a.Segment}, {"occurrence", &a.Occurrence}, {"attempt", &a.Attempt}} {
		v, err := strconv.Atoi(q.Get(n.key))
		if err != nil || v < 1 {
			return a, false, fmt.Errorf("%s must be a positive integer", n.key)
		}
		*n.dst = v
	}
	if a.Stage == "" {
		return a, false, fmt.Errorf("stage must be named")
	}
	return a, true, nil
}

// handleOutputStream is GET /v2/runs/{id}/output/stream (§8.2): one output
// event per chunk, its id the cursor. A Last-Event-ID older than what is
// retained — and any later hole retention cut under a lagging stream — gets
// one gap event naming the cursor asked for and the oldest retained, and the
// stream replays from that oldest; nothing is skipped silently. No lock is
// held while a client is written to.
func (s *Service) handleOutputStream(w http.ResponseWriter, r *http.Request, runID string) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		s.v2Internal(w, fmt.Errorf("response does not support streaming"))
		return
	}
	if !validProtocolID(runID) {
		s.v2Error(w, http.StatusNotFound, "run_not_found", runID+" is not a run on host "+s.hostID)
		return
	}
	s.stateMu.RLock()
	oldest, newest, err := s.store.WeftOutputBounds(runID)
	s.stateMu.RUnlock()
	if errors.Is(err, sql.ErrNoRows) {
		s.v2Error(w, http.StatusNotFound, "run_not_found", runID+" is not a run on host "+s.hostID)
		return
	}
	if err != nil {
		s.v2Internal(w, err)
		return
	}
	last := newest
	if value := r.Header.Get("Last-Event-ID"); value != "" {
		cursor, err := strconv.ParseInt(value, 10, 64)
		if err != nil || cursor < 0 || cursor > newest {
			s.v2Error(w, http.StatusBadRequest, "malformed", fmt.Sprintf(
				"Last-Event-ID must be a cursor run %s has allocated, at most %d", runID, newest))
			return
		}
		last = cursor
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)
	if last < oldest-1 {
		if !s.writeGap(w, runID, last, oldest) {
			return
		}
		last = oldest - 1
	}
	flusher.Flush()
	keepAlive := time.NewTicker(outputKeepAlive)
	defer keepAlive.Stop()
	for {
		// Taken before the read, so a chunk retained after it still wakes us.
		wake := s.segments.output.wait()
		s.stateMu.RLock()
		chunks, err := s.store.WeftOutputAfter(runID, last, outputBatch)
		s.stateMu.RUnlock()
		if err != nil {
			s.log.Printf("weft output host=%s run=%s: stream after cursor %d: %v", s.hostID, runID, last, err)
			return
		}
		for _, c := range chunks {
			if c.Cursor > last+1 && !s.writeGap(w, runID, last, c.Cursor) {
				return
			}
			doc, err := json.Marshal(map[string]any{
				"contract_version": ServiceVersion, "cursor": c.Cursor, "run_id": runID,
				"attempt": c.WeftAttempt, "chunk": c.Chunk, "at": c.At, "text": c.Text,
			})
			if err != nil {
				s.log.Printf("weft output host=%s run=%s: encode cursor %d: %v", s.hostID, runID, c.Cursor, err)
				return
			}
			if _, err := fmt.Fprintf(w, "id: %d\nevent: output\ndata: %s\n\n", c.Cursor, doc); err != nil {
				return
			}
			last = c.Cursor
		}
		flusher.Flush()
		if len(chunks) == outputBatch {
			continue
		}
		select {
		case <-r.Context().Done():
			return
		case <-wake:
		case <-keepAlive.C:
			if _, err := io.WriteString(w, ": keep-alive\n\n"); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

func (s *Service) writeGap(w io.Writer, runID string, requested, oldest int64) bool {
	doc, _ := json.Marshal(map[string]any{
		"contract_version": ServiceVersion, "run_id": runID, "requested": requested, "oldest": oldest,
		"reason": "retention",
	})
	_, err := fmt.Fprintf(w, "event: gap\ndata: %s\n\n", doc)
	return err == nil
}
