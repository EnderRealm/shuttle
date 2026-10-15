package vt

// MaxScrollback is how many lines scrolled off the primary screen the
// emulator keeps (weft-service/v2 §9.5).
const MaxScrollback = 10000

// MaxCols and MaxRows are the largest terminal whose snapshot Snapshot holds
// to 1 MiB whatever its screens hold: written plain, a cell is at most four
// bytes, and both screens of 500 × 200 come to 800,000 with room for the rest.
const (
	MaxCols = 500
	MaxRows = 200
)

// maxExtra bounds the combining marks kept on one cell.
const maxExtra = 32

// Color is a cell's foreground or background: the default, an indexed
// palette entry or a 24-bit RGB value.
type Color uint32

const (
	colorDefault Color = 0
	colorIndexed Color = 1 << 24
	colorRGB     Color = 2 << 24
	colorKind    Color = 0xff << 24
)

type flags uint16

const (
	fBold flags = 1 << iota
	fFaint
	fItalic
	fUnderline
	fBlink
	fInverse
	fInvisible
	fStrike
	fOverline
)

// attr is the pen: what a printed or erased cell takes.
type attr struct {
	fg, bg Color
	f      flags
}

// cell is one column of a line. A wide character occupies two: the left one
// carries the rune with w 2, the right one rune 0 with w 0.
type cell struct {
	r      rune
	fg, bg Color
	f      flags
	w      uint8
}

func (c cell) attr() attr { return attr{fg: c.fg, bg: c.bg, f: c.f} }

func blankCell(a attr) cell { return cell{r: ' ', w: 1, bg: a.bg} }

// line is one row. extra holds the combining marks printed onto a cell, by
// column.
type line struct {
	c     []cell
	extra map[int]string
}

func newLine(cols int, a attr) *line {
	l := &line{c: make([]cell, cols)}
	l.clear(0, cols, a)
	return l
}

func (l *line) clear(from, to int, a attr) {
	b := blankCell(a)
	for i := from; i < to && i < len(l.c); i++ {
		l.c[i] = b
		if l.extra != nil {
			delete(l.extra, i)
		}
	}
}

// repair blanks the half of a wide character an edit left without its other
// half, so every line stays drawable as a sequence of whole characters.
func (l *line) repair(a attr) {
	n := len(l.c)
	for i := 0; i < n; i++ {
		switch l.c[i].w {
		case 2:
			if i+1 >= n || l.c[i+1].w != 0 {
				l.clear(i, i+1, a)
			} else {
				i++
			}
		case 0:
			l.clear(i, i+1, a)
		}
	}
}

// trimmed is the line without its trailing default blanks, as scrollback
// keeps it.
func (l *line) trimmed() *line {
	n := len(l.c)
	for n > 0 {
		c := l.c[n-1]
		if c.r != ' ' || c.w != 1 || c.attr() != (attr{}) || l.extra[n-1] != "" {
			break
		}
		n--
	}
	out := &line{c: append([]cell(nil), l.c[:n]...)}
	for i, s := range l.extra {
		if i < n {
			if out.extra == nil {
				out.extra = map[int]string{}
			}
			out.extra[i] = s
		}
	}
	return out
}

// shiftExtra moves the combining marks of columns at or past from by delta,
// dropping those that leave [0, cols).
func (l *line) shiftExtra(from, delta, cols int) {
	if l.extra == nil {
		return
	}
	moved := map[int]string{}
	for i, s := range l.extra {
		j := i
		if i >= from {
			j = i + delta
		}
		if j >= 0 && j < cols {
			moved[j] = s
		}
	}
	l.extra = moved
}

func (l *line) resize(cols int, a attr) {
	switch {
	case cols < len(l.c):
		l.c = l.c[:cols]
		for i := range l.extra {
			if i >= cols {
				delete(l.extra, i)
			}
		}
		l.repair(a)
	case cols > len(l.c):
		old := len(l.c)
		l.c = append(l.c, make([]cell, cols-old)...)
		l.clear(old, cols, attr{})
	}
}

// charsets are the G0 and G1 designations and which one GL shifts to.
type charsets struct {
	g  [2]byte
	gl int
}

// saved is what DECSC keeps and DECRC restores.
type saved struct {
	x, y int
	pen  attr
	cs   charsets
}

type modes struct {
	appCursor, appKeypad, origin, autowrap, cursorVisible, insert, newline bool
	bracketedPaste, focus                                                  bool
	// mouse and mouseEncoding are the DEC private mode numbers in force, 0
	// for none.
	mouse, mouseEncoding int
}

// Terminal is one session's emulator.
type Terminal struct {
	cols, rows int
	primary    []*line
	alternate  []*line
	alt        bool
	scrollback []*line // a ring: oldest at sbHead
	sbHead     int
	x, y       int
	pen        attr
	pending    bool // the next printed character wraps first
	cs         charsets
	save       [2]saved // primary, alternate
	top, bot   int
	m          modes
	title      string
	// last is the character REP repeats, written at lastX, lastY; 0 once
	// anything but a pen, mode or charset change followed it.
	last         rune
	lastX, lastY int
	p            parser
}

// New is a reset terminal of cols × rows.
func New(cols, rows int) *Terminal {
	if cols < 1 {
		cols = 1
	}
	if rows < 1 {
		rows = 1
	}
	t := &Terminal{cols: cols, rows: rows}
	t.reset()
	return t
}

func defaultCharsets() charsets { return charsets{g: [2]byte{'B', 'B'}} }

func (t *Terminal) reset() {
	t.primary = t.blankScreen()
	t.alternate = t.blankScreen()
	t.alt = false
	t.x, t.y, t.pen, t.pending = 0, 0, attr{}, false
	t.cs = defaultCharsets()
	t.save = [2]saved{{cs: defaultCharsets()}, {cs: defaultCharsets()}}
	t.top, t.bot = 0, t.rows-1
	t.m = modes{autowrap: true, cursorVisible: true}
	t.last = 0
}

func (t *Terminal) blankScreen() []*line {
	s := make([]*line, t.rows)
	for i := range s {
		s[i] = newLine(t.cols, attr{})
	}
	return s
}

func (t *Terminal) screen() []*line {
	if t.alt {
		return t.alternate
	}
	return t.primary
}

// Size is the terminal's columns and rows.
func (t *Terminal) Size() (cols, rows int) { return t.cols, t.rows }

// Title is the window title the program last set (OSC 0 or 2).
func (t *Terminal) Title() string { return t.title }

// ScrollbackLines is how many lines the scrollback holds.
func (t *Terminal) ScrollbackLines() int { return len(t.scrollback) }

func (t *Terminal) scrollbackLine(i int) *line {
	return t.scrollback[(t.sbHead+i)%len(t.scrollback)]
}

func (t *Terminal) pushScrollback(l *line) {
	l = l.trimmed()
	if len(t.scrollback) < MaxScrollback {
		t.scrollback = append(t.scrollback, l)
		return
	}
	t.scrollback[t.sbHead] = l
	t.sbHead = (t.sbHead + 1) % len(t.scrollback)
}

func (t *Terminal) clearScrollback() {
	t.scrollback, t.sbHead = nil, 0
}

// Resize changes the size without reflow: lines are cut or padded, the rows a
// shrink pushes above the cursor go to scrollback, and the scroll region is
// the whole screen again.
func (t *Terminal) Resize(cols, rows int) {
	if cols < 1 {
		cols = 1
	}
	if rows < 1 {
		rows = 1
	}
	if cols == t.cols && rows == t.rows {
		return
	}
	if cols < t.cols {
		for i := range t.scrollback {
			t.scrollback[i].resize(cols, attr{})
			t.scrollback[i] = t.scrollback[i].trimmed()
		}
	}
	for _, s := range []*[]*line{&t.primary, &t.alternate} {
		lines := *s
		for _, l := range lines {
			l.resize(cols, attr{})
		}
		if rows < len(lines) {
			drop := 0
			if t.y >= rows {
				drop = t.y - rows + 1
			}
			if s == &t.primary {
				for _, l := range lines[:drop] {
					t.pushScrollback(l)
				}
			}
			lines = lines[drop : drop+rows]
		}
		for len(lines) < rows {
			lines = append(lines, newLine(cols, attr{}))
		}
		*s = lines
	}
	if t.y >= rows {
		t.y = rows - 1
	}
	t.cols, t.rows = cols, rows
	t.x = min(t.x, cols-1)
	for i := range t.save {
		t.save[i].x = min(t.save[i].x, cols-1)
		t.save[i].y = min(t.save[i].y, rows-1)
	}
	t.top, t.bot = 0, rows-1
	t.pending = false
	t.last = 0
}

// ---------------------------------------------------------------- printing

func (t *Terminal) print(r rune) {
	if r < 0x80 && t.cs.g[t.cs.gl] == '0' && r >= 0x5f && r <= 0x7e {
		r = decGraphics[r-0x5f]
	}
	w := runeWidth(r)
	if w == 0 {
		t.combine(r)
		return
	}
	if w == 2 && t.cols < 2 {
		return
	}
	if t.pending && t.m.autowrap {
		t.x = 0
		t.index()
	}
	t.pending = false
	scr := t.screen()
	if w == 2 && t.x == t.cols-1 {
		if t.m.autowrap {
			scr[t.y].clear(t.x, t.x+1, t.pen)
			scr[t.y].repair(t.pen)
			t.x = 0
			t.index()
		} else {
			t.x = t.cols - 2
		}
	}
	l := scr[t.y]
	if t.m.insert {
		t.insertCells(l, t.x, w)
	}
	t.place(l, t.x, w)
	l.c[t.x] = cell{r: r, w: uint8(w), fg: t.pen.fg, bg: t.pen.bg, f: t.pen.f}
	if w == 2 {
		l.c[t.x+1] = cell{w: 0, fg: t.pen.fg, bg: t.pen.bg, f: t.pen.f}
	}
	t.last, t.lastX, t.lastY = r, t.x, t.y
	t.x += w
	if t.x >= t.cols {
		t.x = t.cols - 1
		t.pending = t.m.autowrap
	}
}

// place clears what a character of width w written at x overwrites, and the
// other half of any wide character it cuts.
func (t *Terminal) place(l *line, x, w int) {
	if l.c[x].w == 0 && x > 0 {
		l.clear(x-1, x, t.pen)
	}
	end := x + w - 1
	if l.c[end].w == 2 && end+1 < t.cols {
		l.clear(end+1, end+2, t.pen)
	}
	if l.extra != nil {
		for i := x; i <= end; i++ {
			delete(l.extra, i)
		}
	}
}

// combine adds a zero-width mark to the character just printed or, after
// anything else, to the one before the cursor.
func (t *Terminal) combine(r rune) {
	x, y := t.x-1, t.y
	switch {
	case t.last != 0:
		x, y = t.lastX, t.lastY
	case t.pending:
		x = t.x
	}
	if x < 0 {
		return
	}
	l := t.screen()[y]
	if l.c[x].w == 0 && x > 0 {
		x--
	}
	if l.extra == nil {
		l.extra = map[int]string{}
	}
	if len(l.extra[x]) < maxExtra {
		l.extra[x] += string(r)
	}
}

// ---------------------------------------------------------------- movement

// index is LF: down one line, scrolling the region at its bottom margin.
func (t *Terminal) index() {
	switch {
	case t.y == t.bot:
		t.scrollUp(1)
	case t.y < t.rows-1:
		t.y++
	}
}

func (t *Terminal) reverseIndex() {
	switch {
	case t.y == t.top:
		t.scrollDown(1)
	case t.y > 0:
		t.y--
	}
}

// scrollUp moves the region's lines up n, the lines leaving a full-screen
// region of the primary screen going to scrollback.
func (t *Terminal) scrollUp(n int) {
	scr := t.screen()
	height := t.bot - t.top + 1
	n = min(n, height)
	keep := !t.alt && t.top == 0 && t.bot == t.rows-1
	for i := 0; i < n; i++ {
		gone := scr[t.top]
		if keep {
			t.pushScrollback(gone)
		}
		copy(scr[t.top:t.bot], scr[t.top+1:t.bot+1])
		gone.extra = nil
		gone.clear(0, t.cols, t.pen)
		scr[t.bot] = gone
	}
}

func (t *Terminal) scrollDown(n int) {
	scr := t.screen()
	height := t.bot - t.top + 1
	n = min(n, height)
	for i := 0; i < n; i++ {
		gone := scr[t.bot]
		copy(scr[t.top+1:t.bot+1], scr[t.top:t.bot])
		gone.extra = nil
		gone.clear(0, t.cols, t.pen)
		scr[t.top] = gone
	}
}

func (t *Terminal) moveTo(x, y int) {
	t.x = max(0, min(x, t.cols-1))
	lo, hi := 0, t.rows-1
	if t.m.origin {
		y += t.top
		lo, hi = t.top, t.bot
	}
	t.y = max(lo, min(y, hi))
	t.pending = false
}

func (t *Terminal) cursorUp(n int) {
	lo := 0
	if t.y >= t.top {
		lo = t.top
	}
	t.y = max(lo, t.y-n)
	t.pending = false
}

func (t *Terminal) cursorDown(n int) {
	hi := t.rows - 1
	if t.y <= t.bot {
		hi = t.bot
	}
	t.y = min(hi, t.y+n)
	t.pending = false
}

func (t *Terminal) tab(n int) {
	for ; n > 0 && t.x < t.cols-1; n-- {
		t.x = min(t.cols-1, (t.x/8+1)*8)
	}
	t.pending = false
}

func (t *Terminal) backTab(n int) {
	for ; n > 0 && t.x > 0; n-- {
		t.x = ((t.x - 1) / 8) * 8
	}
	t.pending = false
}

func (t *Terminal) saveCursor() {
	t.save[t.screenIndex()] = saved{x: t.x, y: t.y, pen: t.pen, cs: t.cs}
}

func (t *Terminal) restoreCursor() {
	s := t.save[t.screenIndex()]
	t.x, t.y, t.pen, t.cs = min(s.x, t.cols-1), min(s.y, t.rows-1), s.pen, s.cs
	if t.m.origin {
		t.y = max(t.top, min(t.y, t.bot))
	}
	t.pending = false
}

func (t *Terminal) screenIndex() int {
	if t.alt {
		return 1
	}
	return 0
}

// ---------------------------------------------------------------- editing

func (t *Terminal) eraseDisplay(mode int) {
	scr := t.screen()
	switch mode {
	case 0:
		scr[t.y].clear(t.x, t.cols, t.pen)
		scr[t.y].repair(t.pen)
		for _, l := range scr[t.y+1:] {
			l.clear(0, t.cols, t.pen)
		}
	case 1:
		for _, l := range scr[:t.y] {
			l.clear(0, t.cols, t.pen)
		}
		scr[t.y].clear(0, t.x+1, t.pen)
		scr[t.y].repair(t.pen)
	case 2:
		for _, l := range scr {
			l.clear(0, t.cols, t.pen)
		}
	case 3:
		t.clearScrollback()
	}
}

func (t *Terminal) eraseLine(mode int) {
	l := t.screen()[t.y]
	switch mode {
	case 0:
		l.clear(t.x, t.cols, t.pen)
	case 1:
		l.clear(0, t.x+1, t.pen)
	case 2:
		l.clear(0, t.cols, t.pen)
	}
	l.repair(t.pen)
}

func (t *Terminal) insertCells(l *line, x, n int) {
	n = min(n, t.cols-x)
	copy(l.c[x+n:], l.c[x:t.cols-n])
	l.shiftExtra(x, n, t.cols)
	l.clear(x, x+n, t.pen)
	l.repair(t.pen)
}

func (t *Terminal) deleteCells(n int) {
	l := t.screen()[t.y]
	n = min(n, t.cols-t.x)
	// A wide character the deletion starts inside is blanked whole first, so
	// the right half of another cannot slide in beside its left half.
	if t.x > 0 && l.c[t.x].w == 0 {
		l.clear(t.x-1, t.x+1, t.pen)
	}
	copy(l.c[t.x:], l.c[t.x+n:])
	if l.extra != nil {
		for i := t.x; i < t.x+n; i++ {
			delete(l.extra, i)
		}
	}
	l.shiftExtra(t.x+n, -n, t.cols)
	l.clear(t.cols-n, t.cols, t.pen)
	l.repair(t.pen)
	t.pending = false
}

func (t *Terminal) eraseCells(n int) {
	l := t.screen()[t.y]
	l.clear(t.x, t.x+n, t.pen)
	l.repair(t.pen)
	t.pending = false
}

func (t *Terminal) insertLines(n int) {
	if t.y < t.top || t.y > t.bot {
		return
	}
	top := t.top
	t.top = t.y
	t.scrollDown(n)
	t.top = top
	t.x, t.pending = 0, false
}

func (t *Terminal) deleteLines(n int) {
	if t.y < t.top || t.y > t.bot {
		return
	}
	top := t.top
	t.top = t.y
	scr := t.screen()
	height := t.bot - t.top + 1
	n = min(n, height)
	for i := 0; i < n; i++ {
		gone := scr[t.top]
		copy(scr[t.top:t.bot], scr[t.top+1:t.bot+1])
		gone.extra = nil
		gone.clear(0, t.cols, t.pen)
		scr[t.bot] = gone
	}
	t.top = top
	t.x, t.pending = 0, false
}

func (t *Terminal) setRegion(top, bot int) {
	if top < 0 {
		top = 0
	}
	if bot <= 0 || bot > t.rows {
		bot = t.rows
	}
	if top >= bot-1 {
		return
	}
	t.top, t.bot = top, bot-1
	t.moveTo(0, 0)
}

// enterAlternate switches to the alternate screen; leaving clears it, so an
// inactive alternate screen is always blank.
func (t *Terminal) enterAlternate(clear bool) {
	if !t.alt {
		t.alt = true
	}
	if clear {
		for _, l := range t.alternate {
			l.extra = nil
			l.clear(0, t.cols, t.pen)
		}
	}
}

func (t *Terminal) leaveAlternate() {
	if !t.alt {
		return
	}
	for _, l := range t.alternate {
		l.extra = nil
		l.clear(0, t.cols, attr{})
	}
	t.save[1] = saved{cs: defaultCharsets()}
	t.alt = false
}

// softReset is DECSTR: modes, margins, pen, charsets and the saved cursor to
// their defaults, with autowrap off as the VT220 defines it; the screen and
// the cursor position stay.
func (t *Terminal) softReset() {
	t.m.cursorVisible = true
	t.m.insert = false
	t.m.origin = false
	t.m.autowrap = false
	t.m.appCursor = false
	t.m.appKeypad = false
	t.top, t.bot = 0, t.rows-1
	t.cs = defaultCharsets()
	t.pen = attr{}
	t.save[t.screenIndex()] = saved{cs: defaultCharsets()}
	t.pending = false
}
