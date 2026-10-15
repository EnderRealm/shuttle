package vt

import (
	"strings"
	"unicode/utf8"
)

type pstate uint8

const (
	sGround pstate = iota
	sEscape
	sCSI
	sOSC
	sOSCEscape
	sString       // DCS, SOS, PM, APC: consumed to ST
	sStringEscape // an ESC inside one, which ST ends with a backslash
)

const (
	maxParams = 32
	maxParam  = 65535
	maxOSC    = 4096
	maxTitle  = 1024
)

// parser is the DEC/xterm state machine's state between writes, so a
// sequence or a UTF-8 character split across two reads decodes whole.
type parser struct {
	state  pstate
	utf    [4]byte
	un     int
	uneed  int
	inter  byte
	priv   byte
	params [maxParams]int
	sub    [maxParams]bool // introduced by ':' rather than ';'
	np     int
	cur    int
	bad    bool
	osc    []byte
	// raw is the escape or CSI sequence read so far, and intro the byte
	// that opened a control string: what a snapshot replays when it is
	// taken mid-sequence.
	raw   []byte
	intro byte
}

// maxRaw bounds the escape or CSI sequence kept for replay; one longer is
// refused as malformed.
const maxRaw = 64

// Write feeds the bytes a program wrote, in order. It never fails.
func (t *Terminal) Write(b []byte) (int, error) {
	for i := 0; i < len(b); i++ {
		c := b[i]
		if t.p.state == sGround && t.p.uneed == 0 && c >= 0x20 && c < 0x7f {
			t.print(rune(c))
			continue
		}
		t.step(c)
	}
	return len(b), nil
}

func (t *Terminal) step(c byte) {
	p := &t.p
	switch p.state {
	case sGround:
		t.ground(c)
	case sEscape:
		t.escape(c)
	case sCSI:
		t.csi(c)
	case sOSC:
		switch c {
		case 0x07:
			t.dispatchOSC()
			p.state = sGround
		case 0x1b:
			p.state = sOSCEscape
		case 0x18, 0x1a:
			p.state = sGround
		default:
			if len(p.osc) < maxOSC {
				p.osc = append(p.osc, c)
			}
		}
	case sOSCEscape:
		if c == '\\' {
			t.dispatchOSC()
			p.state = sGround
			return
		}
		t.beginEscape()
		t.escape(c)
	case sString:
		switch c {
		case 0x1b:
			p.state = sStringEscape
		case 0x18, 0x1a:
			p.state = sGround
		}
	case sStringEscape:
		if c == '\\' {
			p.state = sGround
			return
		}
		t.beginEscape()
		t.escape(c)
	}
}

func (t *Terminal) ground(c byte) {
	p := &t.p
	if p.uneed > 0 {
		if c&0xc0 == 0x80 {
			p.utf[p.un] = c
			p.un++
			if p.un == p.uneed {
				r, _ := utf8.DecodeRune(p.utf[:p.un])
				p.un, p.uneed = 0, 0
				t.print(r)
			}
			return
		}
		p.un, p.uneed = 0, 0
		t.print(utf8.RuneError)
	}
	switch {
	case c < 0x20:
		t.execute(c)
	case c == 0x7f:
	case c < 0x80:
		t.print(rune(c))
	case c&0xe0 == 0xc0:
		p.utf[0], p.un, p.uneed = c, 1, 2
	case c&0xf0 == 0xe0:
		p.utf[0], p.un, p.uneed = c, 1, 3
	case c&0xf8 == 0xf0:
		p.utf[0], p.un, p.uneed = c, 1, 4
	default:
		t.print(utf8.RuneError)
	}
}

// execute is a C0 control, wherever it arrives.
func (t *Terminal) execute(c byte) {
	if c != 0x0e && c != 0x0f && c != 0x1b {
		t.last = 0
	}
	switch c {
	case 0x08:
		t.x = max(0, t.x-1)
		t.pending = false
	case 0x09:
		t.tab(1)
	case 0x0a, 0x0b, 0x0c:
		t.index()
		if t.m.newline {
			t.x = 0
		}
		t.pending = false
	case 0x0d:
		t.x, t.pending = 0, false
	case 0x0e:
		t.cs.gl = 1
	case 0x0f:
		t.cs.gl = 0
	case 0x18, 0x1a:
		t.p.state = sGround
	case 0x1b:
		t.beginEscape()
	}
}

func (t *Terminal) beginEscape() {
	t.p.state = sEscape
	t.p.inter = 0
	t.p.raw = append(t.p.raw[:0], 0x1b)
}

func (p *parser) keep(c byte) {
	if len(p.raw) < maxRaw {
		p.raw = append(p.raw, c)
	} else {
		p.bad = true
	}
}

// pending is the sequence the parser is inside, as the bytes that put it
// there.
func (p *parser) pending() []byte {
	switch p.state {
	case sGround:
		return p.utf[:p.un]
	case sEscape, sCSI:
		return p.raw
	case sOSC:
		return append([]byte("\x1b]"), p.osc...)
	case sOSCEscape:
		return append(append([]byte("\x1b]"), p.osc...), 0x1b)
	case sString:
		return []byte{0x1b, p.intro}
	case sStringEscape:
		return []byte{0x1b, p.intro, 0x1b}
	}
	return nil
}

func (t *Terminal) escape(c byte) {
	p := &t.p
	switch {
	case c == 0x1b:
		t.beginEscape()
		return
	case c < 0x20:
		t.execute(c)
		return
	case c < 0x30:
		p.inter = c
		p.keep(c)
		return
	case c >= 0x7f:
		p.state = sGround
		return
	}
	p.state = sGround
	if p.inter != '(' && p.inter != ')' && c != '[' && c != ']' && c != '=' && c != '>' {
		t.last = 0
	}
	if p.inter != 0 {
		switch p.inter {
		case '(', ')':
			set := byte('B')
			if c == '0' {
				set = '0'
			}
			t.cs.g[p.inter-'('] = set
		}
		return
	}
	switch c {
	case '[':
		p.state, p.priv, p.inter, p.np, p.cur, p.bad = sCSI, 0, 0, 0, 0, false
		p.sub[0] = false
		p.keep(c)
	case ']':
		p.state, p.osc = sOSC, p.osc[:0]
	case 'P', 'X', '^', '_':
		p.state, p.intro = sString, c
	case '7':
		t.saveCursor()
	case '8':
		t.restoreCursor()
	case 'c':
		t.reset()
	case 'D':
		t.index()
		t.pending = false
	case 'E':
		t.index()
		t.x, t.pending = 0, false
	case 'M':
		t.reverseIndex()
		t.pending = false
	case '=':
		t.m.appKeypad = true
	case '>':
		t.m.appKeypad = false
	}
}

func (t *Terminal) csi(c byte) {
	p := &t.p
	switch {
	case c >= '0' && c <= '9':
		if p.inter != 0 {
			p.bad = true
		}
		p.keep(c)
		p.cur = min(p.cur*10+int(c-'0'), maxParam)
	case c == ';' || c == ':':
		p.keep(c)
		if p.np+1 >= maxParams {
			p.bad = true
			return
		}
		p.params[p.np] = p.cur
		p.np++
		p.sub[p.np] = c == ':'
		p.cur = 0
	case c >= '<' && c <= '?':
		p.keep(c)
		if p.np == 0 && p.cur == 0 && p.priv == 0 {
			p.priv = c
		} else {
			p.bad = true
		}
	case c >= 0x20 && c < 0x30:
		p.keep(c)
		p.inter = c
	case c >= 0x40 && c < 0x7f:
		p.params[p.np] = p.cur
		p.np++
		p.state = sGround
		keeps := !p.bad && p.inter == 0 && t.keepsLast(c)
		if !p.bad {
			t.dispatchCSI(c)
		}
		if !keeps {
			t.last = 0
		}
	case c == 0x1b:
		t.beginEscape()
	case c < 0x20:
		t.execute(c)
	case c >= 0x80:
		p.state = sGround
	}
}

// keepsLast is whether the CSI with this final leaves REP's character in
// place: REP itself, and the pen and mode changes that neither move the
// cursor nor change a cell.
func (t *Terminal) keepsLast(final byte) bool {
	p := &t.p
	switch {
	case final == 'b' && p.priv == 0, final == 'm' && p.priv == 0:
		return true
	case (final == 'h' || final == 'l') && (p.priv == 0 || p.priv == '?'):
		for i := 0; i < p.np; i++ {
			switch p.params[i] {
			case 6, 7, 47, 1047, 1048, 1049:
				return p.priv == 0 && p.params[i] != 6 && p.params[i] != 7
			}
		}
		return true
	}
	return false
}

// param is the i-th parameter, def when absent or zero.
func (t *Terminal) param(i, def int) int {
	if i < t.p.np && t.p.params[i] != 0 {
		return t.p.params[i]
	}
	return def
}

func (t *Terminal) dispatchCSI(final byte) {
	p := &t.p
	if p.inter != 0 {
		if p.inter == '!' && final == 'p' && p.priv == 0 {
			t.softReset()
		}
		return
	}
	switch p.priv {
	case '?':
		switch final {
		case 'h', 'l':
			for i := 0; i < p.np; i++ {
				t.setPrivate(p.params[i], final == 'h')
			}
		case 'J':
			t.eraseDisplay(p.params[0])
		case 'K':
			t.eraseLine(p.params[0])
		}
		return
	case 0:
	default:
		return
	}
	n := t.param(0, 1)
	switch final {
	case '@':
		t.insertCells(t.screen()[t.y], t.x, n)
		t.pending = false
	case 'A':
		t.cursorUp(n)
	case 'B':
		t.cursorDown(n)
	case 'C':
		t.x, t.pending = min(t.cols-1, t.x+n), false
	case 'D':
		t.x, t.pending = max(0, t.x-n), false
	case 'E':
		t.cursorDown(n)
		t.x = 0
	case 'F':
		t.cursorUp(n)
		t.x = 0
	case 'G', '`':
		t.x, t.pending = min(t.cols-1, n-1), false
	case 'H', 'f':
		t.moveTo(t.param(1, 1)-1, n-1)
	case 'I':
		t.tab(n)
	case 'J':
		t.eraseDisplay(p.params[0])
	case 'K':
		t.eraseLine(p.params[0])
	case 'L':
		t.insertLines(n)
	case 'M':
		t.deleteLines(n)
	case 'P':
		t.deleteCells(n)
	case 'S':
		t.scrollUp(n)
	case 'T':
		if p.np <= 1 {
			t.scrollDown(n)
		}
	case 'X':
		t.eraseCells(n)
	case 'Z':
		t.backTab(n)
	case 'a':
		t.x, t.pending = min(t.cols-1, t.x+n), false
	case 'b':
		if t.last != 0 {
			for i := min(n, t.cols*t.rows); i > 0; i-- {
				t.print(t.last)
			}
		}
	case 'd':
		t.moveTo(t.x, n-1)
	case 'e':
		hi := t.rows - 1
		if t.m.origin {
			hi = t.bot
		}
		t.y, t.pending = min(hi, t.y+n), false
	case 'h', 'l':
		for i := 0; i < p.np; i++ {
			switch p.params[i] {
			case 4:
				t.m.insert = final == 'h'
			case 20:
				t.m.newline = final == 'h'
			}
		}
	case 'm':
		t.sgr()
	case 'r':
		t.setRegion(t.param(0, 1)-1, t.param(1, t.rows))
	case 's':
		t.saveCursor()
	case 'u':
		t.restoreCursor()
	}
}

func (t *Terminal) setPrivate(mode int, on bool) {
	switch mode {
	case 1:
		t.m.appCursor = on
	case 6:
		t.m.origin = on
		t.moveTo(0, 0)
	case 7:
		t.m.autowrap = on
		if !on {
			t.pending = false
		}
	case 25:
		t.m.cursorVisible = on
	case 66:
		t.m.appKeypad = on
	case 9, 1000, 1002, 1003:
		if on {
			t.m.mouse = mode
		} else if t.m.mouse == mode {
			t.m.mouse = 0
		}
	case 1005, 1006, 1015, 1016:
		if on {
			t.m.mouseEncoding = mode
		} else if t.m.mouseEncoding == mode {
			t.m.mouseEncoding = 0
		}
	case 1004:
		t.m.focus = on
	case 2004:
		t.m.bracketedPaste = on
	case 47, 1047:
		if on {
			t.enterAlternate(false)
		} else {
			t.leaveAlternate()
		}
	case 1048:
		if on {
			t.saveCursor()
		} else {
			t.restoreCursor()
		}
	case 1049:
		if on {
			if !t.alt {
				t.saveCursor()
			}
			t.enterAlternate(true)
		} else if t.alt {
			t.leaveAlternate()
			t.restoreCursor()
		}
	}
}

func (t *Terminal) sgr() {
	p := &t.p
	for i := 0; i < p.np; i++ {
		v := p.params[i]
		switch {
		case v == 0:
			t.pen = attr{}
		case v == 1:
			t.pen.f |= fBold
		case v == 2:
			t.pen.f |= fFaint
		case v == 3:
			t.pen.f |= fItalic
		case v == 4:
			if i+1 < p.np && p.sub[i+1] {
				i++
				if p.params[i] == 0 {
					t.pen.f &^= fUnderline
					continue
				}
			}
			t.pen.f |= fUnderline
		case v == 5 || v == 6:
			t.pen.f |= fBlink
		case v == 7:
			t.pen.f |= fInverse
		case v == 8:
			t.pen.f |= fInvisible
		case v == 9:
			t.pen.f |= fStrike
		case v == 21:
			t.pen.f |= fUnderline
		case v == 22:
			t.pen.f &^= fBold | fFaint
		case v == 23:
			t.pen.f &^= fItalic
		case v == 24:
			t.pen.f &^= fUnderline
		case v == 25:
			t.pen.f &^= fBlink
		case v == 27:
			t.pen.f &^= fInverse
		case v == 28:
			t.pen.f &^= fInvisible
		case v == 29:
			t.pen.f &^= fStrike
		case v >= 30 && v <= 37:
			t.pen.fg = colorIndexed | Color(v-30)
		case v == 38:
			var c Color
			c, i = t.extendedColor(i)
			if c != colorDefault {
				t.pen.fg = c
			}
		case v == 39:
			t.pen.fg = colorDefault
		case v >= 40 && v <= 47:
			t.pen.bg = colorIndexed | Color(v-40)
		case v == 48:
			var c Color
			c, i = t.extendedColor(i)
			if c != colorDefault {
				t.pen.bg = c
			}
		case v == 49:
			t.pen.bg = colorDefault
		case v == 53:
			t.pen.f |= fOverline
		case v == 55:
			t.pen.f &^= fOverline
		case v == 58:
			_, i = t.extendedColor(i)
		case v >= 90 && v <= 97:
			t.pen.fg = colorIndexed | Color(v-90+8)
		case v >= 100 && v <= 107:
			t.pen.bg = colorIndexed | Color(v-100+8)
		}
		// Subparameters of anything else are consumed with it.
		for i+1 < p.np && p.sub[i+1] {
			i++
		}
	}
}

// extendedColor reads the 38/48/58 color at parameter i in either the
// semicolon or the colon form, answering it and the last parameter it used.
func (t *Terminal) extendedColor(i int) (Color, int) {
	p := &t.p
	b := func(v int) Color { return Color(min(v, 255)) }
	if i+1 < p.np && p.sub[i+1] {
		var s []int
		j := i + 1
		for ; j < p.np && p.sub[j]; j++ {
			s = append(s, p.params[j])
		}
		j--
		switch {
		case len(s) >= 2 && s[0] == 5:
			return colorIndexed | b(s[1]), j
		case len(s) == 4 && s[0] == 2:
			return colorRGB | b(s[1])<<16 | b(s[2])<<8 | b(s[3]), j
		case len(s) >= 5 && s[0] == 2:
			return colorRGB | b(s[2])<<16 | b(s[3])<<8 | b(s[4]), j
		}
		return colorDefault, j
	}
	if i+2 < p.np && p.params[i+1] == 5 {
		return colorIndexed | b(p.params[i+2]), i + 2
	}
	if i+4 < p.np && p.params[i+1] == 2 {
		return colorRGB | b(p.params[i+2])<<16 | b(p.params[i+3])<<8 | b(p.params[i+4]), i + 4
	}
	return colorDefault, p.np - 1
}

// dispatchOSC takes the window title from OSC 0 and 2; every other OSC is
// consumed.
func (t *Terminal) dispatchOSC() {
	t.last = 0
	code, text, ok := strings.Cut(string(t.p.osc), ";")
	if !ok || (code != "0" && code != "2") {
		return
	}
	title := strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f || (r >= 0x80 && r < 0xa0) {
			return -1
		}
		return r
	}, strings.ToValidUTF8(text, string(utf8.RuneError)))
	if len(title) > maxTitle {
		title = strings.ToValidUTF8(title[:maxTitle], "")
	}
	t.title = title
}
