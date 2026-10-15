package vt

import (
	"strconv"
	"unicode/utf8"
)

// snapshotWriter builds a snapshot, tracking the pen the receiving terminal
// has so each SGR is written only where it changes.
type snapshotWriter struct {
	b   []byte
	pen attr
	// plain writes cells without their attributes and combining marks.
	plain bool
}

func (w *snapshotWriter) str(s string) { w.b = append(w.b, s...) }

func (w *snapshotWriter) csi(params ...int) []byte {
	w.b = append(w.b, 0x1b, '[')
	for i, p := range params {
		if i > 0 {
			w.b = append(w.b, ';')
		}
		w.b = strconv.AppendInt(w.b, int64(p), 10)
	}
	return w.b
}

func (w *snapshotWriter) cup(x, y int) {
	w.b = append(w.csi(y+1, x+1), 'H')
}

// sgr sets the pen from the default, so the result never depends on what
// the receiving terminal's pen was.
func (w *snapshotWriter) sgr(a attr) {
	if a == w.pen {
		return
	}
	w.pen = a
	params := []int{0}
	for _, f := range []struct {
		bit flags
		n   int
	}{{fBold, 1}, {fFaint, 2}, {fItalic, 3}, {fUnderline, 4}, {fBlink, 5}, {fInverse, 7},
		{fInvisible, 8}, {fStrike, 9}, {fOverline, 53}} {
		if a.f&f.bit != 0 {
			params = append(params, f.n)
		}
	}
	params = appendColor(params, a.fg, 30, 90, 38)
	params = appendColor(params, a.bg, 40, 100, 48)
	w.b = append(w.csi(params...), 'm')
}

func appendColor(params []int, c Color, base, bright, extended int) []int {
	v := int(c &^ colorKind)
	switch c & colorKind {
	case colorIndexed:
		switch {
		case v < 8:
			return append(params, base+v)
		case v < 16:
			return append(params, bright+v-8)
		}
		return append(params, extended, 5, v)
	case colorRGB:
		return append(params, extended, 2, v>>16&0xff, v>>8&0xff, v&0xff)
	}
	return params
}

// line writes the line's cells up to its last one that is not a default
// blank; the right half of a wide character is written by its left half.
func (w *snapshotWriter) line(l *line) {
	n := len(l.trimmed().c)
	for x := 0; x < n; x++ {
		c := l.c[x]
		if c.w == 0 {
			continue
		}
		w.cell(c, l.extra[x])
	}
}

func (w *snapshotWriter) cell(c cell, extra string) {
	if !w.plain {
		w.sgr(c.attr())
	}
	w.b = utf8.AppendRune(w.b, c.r)
	if !w.plain {
		w.str(extra)
	}
}

// newline ends a line with the pen at its default, so a line the newline
// scrolls in is blank in every terminal.
func (w *snapshotWriter) newline() {
	w.sgr(attr{})
	w.str("\r\n")
}

func (w *snapshotWriter) charsets(cs charsets) {
	if cs.g[0] != 'B' {
		w.str("\x1b(0")
	}
	if cs.g[1] != 'B' {
		w.str("\x1b)0")
	}
	if cs.gl == 1 {
		w.str("\x0e")
	}
}

// savedCursor puts the cursor, pen and charsets at s and writes save — DECSC,
// or the DECSC that entering the alternate screen does — leaving the
// charsets at their defaults again.
func (w *snapshotWriter) savedCursor(s saved, save string) {
	w.cup(s.x, s.y)
	w.sgr(s.pen)
	w.charsets(s.cs)
	w.str(save)
	if s.cs != defaultCharsets() {
		w.str("\x1b(B\x1b)B\x0f")
	}
}

// Snapshot serializes the terminal as weft-service/v2 §9.5 defines it: a byte
// sequence that, written to a reset terminal of the same size, reproduces the
// scrollback, the screens with their attributes, the alternate-screen,
// cursor-key, keypad, bracketed-paste, mouse, focus, insert and wrap modes,
// the scroll region, the pen, the charsets, the saved cursors and the
// cursor's position, pending wrap and visibility — followed by any sequence
// the parser is in the middle of, so the bytes that follow continue it. It is
// at most limit bytes by leaving out the oldest scrollback lines; the screen
// itself is always whole. A terminal no larger than MaxCols × MaxRows whose
// screens alone would not fit has them written without attributes or
// combining marks, which keeps a 1 MiB limit whatever they hold. It answers
// how many scrollback lines it carries.
func (t *Terminal) Snapshot(limit int) ([]byte, int) {
	head := &snapshotWriter{}
	// Wrap is set explicitly either way: DECSTR turns it off on a VT220 and
	// on in xterm.js-family clients.
	head.str("\x1b[!p")
	if t.m.autowrap {
		head.str("\x1b[?7h")
	} else {
		head.str("\x1b[?7l")
	}
	head.str("\x1b[?25l\x1b[H\x1b[2J")

	plain := false
	tail := t.snapshotScreens(true, plain)
	if len(head.b)+len(tail) > limit {
		plain = true
		tail = t.snapshotScreens(true, plain)
	}
	budget := limit - len(head.b) - len(tail)
	var lines [][]byte
	for i := len(t.scrollback) - 1; i >= 0; i-- {
		w := &snapshotWriter{}
		w.line(t.scrollbackLine(i))
		w.newline()
		if len(w.b) > budget {
			break
		}
		budget -= len(w.b)
		lines = append(lines, w.b)
	}
	if len(lines) == 0 {
		tail = t.snapshotScreens(false, plain)
	}
	out := head.b
	for i := len(lines) - 1; i >= 0; i-- {
		out = append(out, lines[i]...)
	}
	return append(out, tail...), len(lines)
}

// snapshotScreens is everything after the scrollback, starting at the
// primary screen's top-left with the default pen. full writes every primary
// row, which the scrollback before it needs to scroll off the screen; plain
// writes the cells without attributes or combining marks.
func (t *Terminal) snapshotScreens(full, plain bool) []byte {
	w := &snapshotWriter{plain: plain}
	rows := t.rows
	if !full {
		for rows > 0 && len(t.primary[rows-1].trimmed().c) == 0 {
			rows--
		}
	}
	for y := 0; y < rows; y++ {
		if y > 0 {
			w.newline()
		}
		w.line(t.primary[y])
	}
	if t.alt {
		w.savedCursor(t.save[0], "\x1b[?1049h")
		// The alternate screen was cleared with the saved pen; it is cleared
		// again with the default one, so blank cells are default blanks.
		w.sgr(attr{})
		w.str("\x1b[2J")
		for y, l := range t.alternate {
			if len(l.trimmed().c) == 0 {
				continue
			}
			w.cup(0, y)
			w.line(l)
		}
		if t.save[1] != (saved{cs: defaultCharsets()}) {
			w.savedCursor(t.save[1], "\x1b7")
		}
	} else if t.save[0] != (saved{cs: defaultCharsets()}) {
		w.savedCursor(t.save[0], "\x1b7")
	}
	if t.top != 0 || t.bot != t.rows-1 {
		w.b = append(w.csi(t.top+1, t.bot+1), 'r')
	}
	if t.m.origin {
		w.str("\x1b[?6h")
	}
	// The cursor waiting to wrap, and REP's character, are restored by
	// writing that character again where it is; nothing after it moves the
	// cursor or forgets the character.
	rx, ry, rewrite := t.x, t.y, t.pending || t.last != 0
	if t.last != 0 {
		rx, ry = t.lastX, t.lastY
	} else if t.pending && t.screen()[ry].c[rx].w == 0 && rx > 0 {
		rx--
	}
	if t.m.origin {
		w.b = append(w.csi(ry-t.top+1, rx+1), 'H')
	} else {
		w.cup(rx, ry)
	}
	if rewrite {
		l := t.screen()[ry]
		w.cell(l.c[rx], l.extra[rx])
	}
	w.charsets(t.cs)
	for _, m := range []struct {
		on  bool
		seq string
	}{
		{t.m.appCursor, "\x1b[?1h"}, {t.m.appKeypad, "\x1b="}, {t.m.insert, "\x1b[4h"},
		{t.m.newline, "\x1b[20h"}, {t.m.focus, "\x1b[?1004h"}, {t.m.bracketedPaste, "\x1b[?2004h"},
	} {
		if m.on {
			w.str(m.seq)
		}
	}
	for _, mode := range []int{t.m.mouse, t.m.mouseEncoding} {
		if mode != 0 {
			w.b = append(append(w.b, 0x1b, '[', '?'), strconv.Itoa(mode)+"h"...)
		}
	}
	if t.m.cursorVisible {
		w.str("\x1b[?25h")
	}
	w.sgr(t.pen)
	if rewrite && t.last == 0 {
		// A NUL, which terminals ignore, forgets the rewritten character.
		w.str("\x00")
	}
	return append(w.b, t.p.pending()...)
}
