// Package vt is the headless terminal emulator a hosted terminal session
// feeds every PTY byte through (weft-service/v2 §9.5). A raw byte tail cannot
// rebuild a full-screen program's screen — its last repaint may be older than
// the tail and its modes were set long before — so the host keeps the screen
// itself: an xterm-compatible grid at the session's size with bounded
// scrollback, and Snapshot serializes it as a byte sequence that, written to a
// reset terminal of the same size, reproduces it.
//
// It models what shells and full-screen programs such as Claude Code emit and
// consumes everything else without effect. It never answers a query: the
// attached client's own terminal does, through the owner's input.
package vt
