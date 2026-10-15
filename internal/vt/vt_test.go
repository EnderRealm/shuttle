package vt

import (
	"bytes"
	"fmt"
	"math/rand"
	"strings"
	"testing"
)

// diff names the first way two terminals' states differ, or "" when they are
// the same: everything a snapshot claims to reproduce.
func diff(a, b *Terminal) string {
	switch {
	case a.cols != b.cols || a.rows != b.rows:
		return fmt.Sprintf("size %dx%d vs %dx%d", a.cols, a.rows, b.cols, b.rows)
	case a.alt != b.alt:
		return fmt.Sprintf("alternate %v vs %v", a.alt, b.alt)
	case a.x != b.x || a.y != b.y:
		return fmt.Sprintf("cursor %d,%d vs %d,%d", a.x, a.y, b.x, b.y)
	case a.pending != b.pending:
		return fmt.Sprintf("pending wrap %v vs %v", a.pending, b.pending)
	case a.pen != b.pen:
		return fmt.Sprintf("pen %+v vs %+v", a.pen, b.pen)
	case a.cs != b.cs:
		return fmt.Sprintf("charsets %+v vs %+v", a.cs, b.cs)
	case a.save != b.save:
		return fmt.Sprintf("saved cursors %+v vs %+v", a.save, b.save)
	case a.top != b.top || a.bot != b.bot:
		return fmt.Sprintf("region %d..%d vs %d..%d", a.top, a.bot, b.top, b.bot)
	case a.m != b.m:
		return fmt.Sprintf("modes %+v vs %+v", a.m, b.m)
	}
	if d := diffLines("primary", a.primary, b.primary); d != "" {
		return d
	}
	if d := diffLines("alternate", a.alternate, b.alternate); d != "" {
		return d
	}
	if a.ScrollbackLines() != b.ScrollbackLines() {
		return fmt.Sprintf("scrollback %d lines vs %d", a.ScrollbackLines(), b.ScrollbackLines())
	}
	for i := 0; i < a.ScrollbackLines(); i++ {
		if d := diffLine(fmt.Sprintf("scrollback[%d]", i), a.scrollbackLine(i), b.scrollbackLine(i)); d != "" {
			return d
		}
	}
	return ""
}

// broken names a line holding half a wide character, or a wide character
// whose halves disagree; "" when every line is whole characters.
func broken(t *Terminal) string {
	for si, scr := range [][]*line{t.primary, t.alternate} {
		for y, l := range scr {
			if len(l.c) != t.cols {
				return fmt.Sprintf("screen %d row %d: %d cells", si, y, len(l.c))
			}
			for x, c := range l.c {
				if c.w == 2 && (x+1 >= len(l.c) || l.c[x+1].w != 0 || l.c[x+1].attr() != c.attr()) {
					return fmt.Sprintf("screen %d row %d col %d: wide character without its right half", si, y, x)
				}
				if c.w == 0 && (x == 0 || l.c[x-1].w != 2) {
					return fmt.Sprintf("screen %d row %d col %d: right half without its character", si, y, x)
				}
			}
		}
	}
	return ""
}

func diffLines(name string, a, b []*line) string {
	for y := range a {
		if d := diffLine(fmt.Sprintf("%s[%d]", name, y), a[y], b[y]); d != "" {
			return d
		}
	}
	return ""
}

func diffLine(name string, a, b *line) string {
	if len(a.c) != len(b.c) {
		return fmt.Sprintf("%s: %d cells vs %d (%q vs %q)", name, len(a.c), len(b.c), text(a), text(b))
	}
	for x := range a.c {
		if a.c[x] != b.c[x] {
			return fmt.Sprintf("%s col %d: %+v vs %+v (%q vs %q)", name, x, a.c[x], b.c[x], text(a), text(b))
		}
		if a.extra[x] != b.extra[x] {
			return fmt.Sprintf("%s col %d marks %q vs %q", name, x, a.extra[x], b.extra[x])
		}
	}
	return ""
}

func text(l *line) string {
	var s strings.Builder
	for x, c := range l.c {
		if c.w != 0 {
			s.WriteRune(c.r)
			s.WriteString(l.extra[x])
		}
	}
	return strings.TrimRight(s.String(), " ")
}

// roundTrip snapshots a, replays the snapshot into a reset terminal of a's
// size, and requires the two to be the same; it returns the replay.
func roundTrip(t *testing.T, name string, a *Terminal) *Terminal {
	t.Helper()
	data, scrollback := a.Snapshot(1 << 20)
	if len(data) > 1<<20 {
		t.Fatalf("%s: snapshot is %d bytes", name, len(data))
	}
	if scrollback != a.ScrollbackLines() {
		t.Fatalf("%s: snapshot carries %d of %d scrollback lines", name, scrollback, a.ScrollbackLines())
	}
	b := New(a.cols, a.rows)
	b.Write(data)
	if d := diff(a, b); d != "" {
		t.Fatalf("%s: replayed snapshot differs: %s\nsnapshot %q", name, d, data)
	}
	return b
}

func feed(cols, rows int, chunks ...string) *Terminal {
	t := New(cols, rows)
	for _, c := range chunks {
		t.Write([]byte(c))
	}
	return t
}

// shellSession is what a login shell and the commands typed into it write:
// prompts, colors, wrapping, tabs, backspaces, wide and combining characters,
// and enough lines to fill scrollback.
func shellSession() string {
	var b strings.Builder
	b.WriteString("\x1b]2;steve@studio: ~/code/warp\x07")
	for i := 0; i < 300; i++ {
		fmt.Fprintf(&b, "\x1b[1;32msteve@studio\x1b[0m:\x1b[34m~/code/warp\x1b[0m %% ls -la\r\n")
		fmt.Fprintf(&b, "total %d\r\n", i)
		fmt.Fprintf(&b, "drwxr-xr-x\t5 steve\tstaff\t160 \x1b[1;34m.git\x1b[0m\r\n")
		fmt.Fprintf(&b, "-rw-r--r--\t1 steve\tstaff\t%d \x1b[38;5;208mREADME.md\x1b[39m 日本語 🙂 e\u0301\r\n", i*7)
		b.WriteString(strings.Repeat("long-line-", 17) + "\r\n")
		b.WriteString("typo\b\b\b\bfixed\r\n")
		b.WriteString("progress 10%\rprogress 99%\x1b[K\r\n")
	}
	b.WriteString("\x1b[1;32msteve@studio\x1b[0m % \x1b[?2004h")
	return b.String()
}

// claudeSetup is a full-screen program's start: the alternate screen and
// its modes set once and a framed input box drawn; claudeUpdate repaints it
// in place.
func claudeSetup(cols, rows int) string {
	var b strings.Builder
	b.WriteString("$ claude\r\n\x1b]0;✳ Claude Code\x07")
	b.WriteString("\x1b[?1049h\x1b[?1h\x1b=\x1b[?2004h\x1b[?1002h\x1b[?1006h\x1b[?1004h\x1b[?25l")
	b.WriteString("\x1b[H\x1b[2J\x1b[1m✳ Claude Code\x1b[0m\r\n")
	b.WriteString("\x1b[2m  cwd: /Users/steve/.shuttle/weft/worktrees/weft-47327fe5/weft-example-fourth-6e4f\x1b[0m\r\n")
	b.WriteString("\x1b[38;2;215;119;87m● \x1b[0mEditing Sources/Weft/Views/QueueRail.swift\r\n")
	fmt.Fprintf(&b, "\x1b[%d;1H╭%s╮", rows-2, strings.Repeat("─", cols-2))
	fmt.Fprintf(&b, "\x1b[%d;1H│ > %s│", rows-1, strings.Repeat(" ", cols-5))
	fmt.Fprintf(&b, "\x1b[%d;1H╰%s╯", rows, strings.Repeat("─", cols-2))
	fmt.Fprintf(&b, "\x1b[4;%dr", rows-3)
	return b.String()
}

func claudeUpdate(i, cols, rows int) string {
	return fmt.Sprintf("\x1b[?2026h\x1b[%d;1H\x1b[48;5;236m spinner %c tokens %d \x1b[0m\x1b[K\x1b[%d;5H%s\x1b[%d;%dH\x1b[?2026l",
		rows-3, []rune("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏")[i%10], i, rows-1, strings.Repeat("y", i%40), rows-1, 5+i%40)
}

func TestSnapshotRoundTripsAShellSession(t *testing.T) {
	a := feed(120, 36, shellSession())
	if a.Title() != "steve@studio: ~/code/warp" {
		t.Fatalf("title = %q", a.Title())
	}
	if a.ScrollbackLines() == 0 || !a.m.bracketedPaste {
		t.Fatalf("scrollback %d, bracketed paste %v", a.ScrollbackLines(), a.m.bracketedPaste)
	}
	roundTrip(t, "shell", a)
}

// A full-screen program's state set up before the retained tail: the raw
// tail alone does not rebuild its screen, and the snapshot does.
func TestSnapshotRestoresATUIWhoseSetupIsOutsideTheTail(t *testing.T) {
	const cols, rows, window = 132, 40, 4 << 20
	var stream bytes.Buffer
	stream.WriteString(shellSession())
	stream.WriteString(claudeSetup(cols, rows))
	setup := stream.Len()
	for i := 0; stream.Len()-setup < window+(64<<10); i++ {
		stream.WriteString(claudeUpdate(i, cols, rows))
	}
	a := New(cols, rows)
	for b := stream.Bytes(); len(b) > 0; {
		n := min(len(b), 32<<10)
		a.Write(b[:n])
		b = b[n:]
	}
	if !a.alt || !a.m.appCursor || !a.m.appKeypad || a.m.mouse != 1002 || a.m.mouseEncoding != 1006 ||
		a.m.cursorVisible || a.top != 3 || a.bot != rows-4 || a.Title() != "✳ Claude Code" {
		t.Fatalf("TUI state not reached: alt %v modes %+v region %d..%d title %q", a.alt, a.m, a.top, a.bot, a.Title())
	}
	if !strings.Contains(text(a.alternate[0]), "✳ Claude Code") || !strings.HasPrefix(text(a.alternate[rows-3]), "╭──") {
		t.Fatalf("screen = %q / %q", text(a.alternate[0]), text(a.alternate[rows-3]))
	}

	tail := stream.Bytes()[stream.Len()-window:]
	raw := New(cols, rows)
	raw.Write(tail)
	if diff(a, raw) == "" {
		t.Fatal("the raw 4 MiB tail rebuilt the screen; the fixture does not put its setup outside the tail")
	}
	b := roundTrip(t, "tui", a)
	// The replay continues as the original does.
	more := claudeUpdate(7, cols, rows) + "\x1b[?1049l\x1b[?25h$ "
	a.Write([]byte(more))
	b.Write([]byte(more))
	if d := diff(a, b); d != "" {
		t.Fatalf("after leaving the TUI: %s", d)
	}
}

func TestSnapshotRoundTripsModesAndEdges(t *testing.T) {
	cases := map[string][]string{
		"pending wrap":       {"\x1b[1;1H" + strings.Repeat("x", 20)},
		"pending wide wrap":  {strings.Repeat("x", 18) + "日"},
		"no autowrap":        {"\x1b[?7l" + strings.Repeat("abc", 10)},
		"origin and region":  {"\x1b[3;8r\x1b[?6h\x1b[2;4Hin region\x1b[5S"},
		"insert and newline": {"hello\x1b[1G\x1b[4hXY\x1b[20h\nnext"},
		"dec graphics":       {"\x1b(0lqqk\x1b(B text \x1b)0\x0elqk"},
		"saved cursors":      {"\x1b[5;7H\x1b[1;31m\x1b7\x1b[0m\x1b[?1049h\x1b[2;2H\x1b[4m\x1b7\x1b[0malt"},
		"saved primary only": {"\x1b[3;3H\x1b[7m\x1b7\x1b[0m\x1b[10;1Hplain"},
		"bce erase":          {"\x1b[44m\x1b[2J\x1b[3;3Hblue\x1b[K\x1b[0m"},
		"erase in region":    {"line1\r\nline2\r\nline3\x1b[2;1H\x1b[L\x1b[2P\x1b[3X\x1b[1J"},
		"wide cut":           {"日本語\x1b[1;2Hx\x1b[1;5H\x1b[1@"},
		"marks":              {"e\u0301 a\u0308\u0323 👍🏽 ok"},
		"truecolor":          {"\x1b[38:2::1:2:3;48:5:200;1;3;9;53mcolor\x1b[22;23m tail"},
		"hidden cursor":      {"\x1b[?25ltext"},
		"mouse modes":        {"\x1b[?1000h\x1b[?1015h\x1b[?1003h"},
		"alternate 47":       {"main\x1b[?47hon alt\x1b[5;5H"},
		"leave alternate":    {"main\x1b[?1049halt\x1b[?1049lback"},
		"scroll down":        {"a\r\nb\r\nc\x1b[1;1H\x1bM\x1bM\x1b[2T"},
		"reset":              {"stuff\x1b[?1h\x1bc after"},
		"soft reset":         {"\x1b[?6h\x1b[4h\x1b[31m\x1b[!pafter"},
		"split csi":          {"x\x1b[3"},
		"split osc":          {"x\x1b]2;half a ti"},
		"split utf8":         {"x\xe6\x97"},
		"split dcs":          {"x\x1bPq#0;2;0;0;0"},
		"unknown sequences":  {"\x1b[>4;2m\x1b[>1u\x1b[?2026h\x1b[5 q\x1b[c\x1b[6n\x1bP+q544e\x1b\\\x1b_apc\x1b\\\x1b#8ok"},
		"tabs and backspace": {"a\tb\tc\x1b[2Zd\x1b[3Ie\b\bf"},
		"repeat":             {"=\x1b[5b"},
	}
	for name, chunks := range cases {
		a := feed(20, 10, chunks...)
		b := roundTrip(t, name, a)
		// Bytes that follow continue on the replay exactly as on the original:
		// the second half of a split sequence among them.
		rest := "1mZ\x07\xa5\\" + "\x1b[0m tail\r\n"
		a.Write([]byte(rest))
		b.Write([]byte(rest))
		if d := diff(a, b); d != "" {
			t.Errorf("%s: continuing after the snapshot: %s", name, d)
		}
	}
}

func TestSnapshotDropsOldestScrollbackToFitItsLimit(t *testing.T) {
	a := New(80, 24)
	for i := 0; i < MaxScrollback+500; i++ {
		fmt.Fprintf(a, "\x1b[3%dmline %06d %s\x1b[0m\r\n", i%8, i, strings.Repeat("=", 40))
	}
	if a.ScrollbackLines() != MaxScrollback {
		t.Fatalf("scrollback = %d, bounded at %d", a.ScrollbackLines(), MaxScrollback)
	}
	// 23 of the lines are still on the screen above the cursor's blank row.
	if first := text(a.scrollbackLine(0)); !strings.HasPrefix(first, fmt.Sprintf("line %06d", 500-23)) {
		t.Fatalf("oldest kept line = %q", first)
	}
	const limit = 64 << 10
	data, kept := a.Snapshot(limit)
	if len(data) > limit || kept == 0 || kept >= MaxScrollback {
		t.Fatalf("snapshot %d bytes with %d lines", len(data), kept)
	}
	b := New(80, 24)
	b.Write(data)
	if b.ScrollbackLines() != kept {
		t.Fatalf("replay holds %d scrollback lines, snapshot says %d", b.ScrollbackLines(), kept)
	}
	for i := 0; i < kept; i++ {
		if d := diffLine("scrollback", a.scrollbackLine(MaxScrollback-kept+i), b.scrollbackLine(i)); d != "" {
			t.Fatal(d)
		}
	}
	if d := diffLines("primary", a.primary, b.primary); d != "" {
		t.Fatal(d)
	}
}

func TestResizeKeepsStateSnapshotable(t *testing.T) {
	a := feed(40, 12, shellSession())
	a.Resize(25, 8)
	roundTrip(t, "shrunk", a)
	a.Write([]byte(claudeSetup(25, 8)))
	a.Resize(60, 20)
	roundTrip(t, "grown on the alternate screen", a)
}

// Randomized streams of the sequences the emulator models, cut at random
// points: whatever state they reach, the snapshot reproduces it.
var randomPieces = []string{
	"a", "Z", "日", "🙂", "e\u0301", " ", "\t", "\b", "\r", "\n", "\x1bD", "\x1bE", "\x1bM", "\x1b7", "\x1b8",
	"\x1b[H", "\x1b[3;7H", "\x1b[20;1H", "\x1b[2A", "\x1b[3B", "\x1b[4C", "\x1b[5D", "\x1b[2E", "\x1b[F", "\x1b[9G",
	"\x1b[J", "\x1b[1J", "\x1b[2J", "\x1b[3J", "\x1b[K", "\x1b[1K", "\x1b[2K", "\x1b[2@", "\x1b[3P", "\x1b[2X",
	"\x1b[L", "\x1b[2M", "\x1b[S", "\x1b[2T", "\x1b[4d", "\x1b[3b", "\x1b[2;6r", "\x1b[r",
	"\x1b[1;31m", "\x1b[38;5;99;48;2;1;2;3m", "\x1b[0m", "\x1b[7;4m", "\x1b[44m",
	"\x1b[?1h", "\x1b[?1l", "\x1b[?6h", "\x1b[?6l", "\x1b[?7l", "\x1b[?7h", "\x1b[?25l", "\x1b[?25h",
	"\x1b[?1049h", "\x1b[?1049l", "\x1b[?47h", "\x1b[?47l", "\x1b[?1047h", "\x1b[?1047l", "\x1b[?1048h", "\x1b[?1048l",
	"\x1b[?1000h", "\x1b[?1002h", "\x1b[?1000l", "\x1b[?1006h", "\x1b[?2004h", "\x1b[?2004l", "\x1b[4h", "\x1b[4l",
	"\x1b[20h", "\x1b[20l", "\x1b(0", "\x1b(B", "\x1b)0", "\x0e", "\x0f", "\x1b=", "\x1b>", "\x1b[!p",
	"\x1b]0;title\x07", "\x1b[s", "\x1b[u",
}

func TestSnapshotRoundTripsRandomStreams(t *testing.T) {
	pieces := randomPieces
	for seed := int64(1); seed <= 300; seed++ {
		rng := rand.New(rand.NewSource(seed))
		cols, rows := 2+rng.Intn(30), 2+rng.Intn(12)
		a := New(cols, rows)
		var stream strings.Builder
		for i := 0; i < 400; i++ {
			stream.WriteString(pieces[rng.Intn(len(pieces))])
			if rng.Intn(150) == 0 {
				a.Write([]byte(stream.String()))
				stream.Reset()
				a.Resize(2+rng.Intn(30), 2+rng.Intn(12))
			}
		}
		s := stream.String()
		cut := rng.Intn(len(s) + 1)
		a.Write([]byte(s[:cut]))
		if d := broken(a); d != "" {
			t.Fatalf("seed %d: %s", seed, d)
		}
		b := roundTrip(t, fmt.Sprintf("seed %d", seed), a)
		a.Write([]byte(s[cut:]))
		b.Write([]byte(s[cut:]))
		if d := diff(a, b); d != "" {
			t.Fatalf("seed %d: continuing after the snapshot: %s", seed, d)
		}
	}
}

// The densest screens a session may have — both screens at MaxCols × MaxRows,
// every cell its own truecolor pen with combining marks — still snapshot
// within 1 MiB, the characters intact.
func TestSnapshotOfTheDensestScreensFitsItsLimit(t *testing.T) {
	a := New(MaxCols, MaxRows)
	dense := func() string {
		var b strings.Builder
		for y := 0; y < MaxRows; y++ {
			fmt.Fprintf(&b, "\x1b[%d;1H", y+1)
			for x := 0; x < MaxCols; x++ {
				fmt.Fprintf(&b, "\x1b[1;3;4;9;53;38;2;%d;%d;%d;48;2;%d;%d;%dm\u00e9\u0301\u0323", x%256, y%256, (x*y)%256, y%256, x%256, 7)
			}
		}
		return b.String()
	}
	a.Write([]byte(dense()))
	a.Write([]byte("\x1b[?1049h"))
	a.Write([]byte(dense()))
	data, _ := a.Snapshot(1 << 20)
	if len(data) > 1<<20 {
		t.Fatalf("snapshot of %d bytes", len(data))
	}
	b := New(MaxCols, MaxRows)
	b.Write(data)
	for y := 0; y < MaxRows; y++ {
		for _, screens := range [][2]*line{{a.primary[y], b.primary[y]}, {a.alternate[y], b.alternate[y]}} {
			for x := range screens[0].c {
				if screens[0].c[x].r != screens[1].c[x].r {
					t.Fatalf("row %d col %d: %q vs %q", y, x, screens[0].c[x].r, screens[1].c[x].r)
				}
			}
		}
	}
	if !b.alt || b.x != a.x || b.y != a.y {
		t.Fatalf("replay alt %v cursor %d,%d vs %d,%d", b.alt, b.x, b.y, a.x, a.y)
	}
}

// A snapshot sets autowrap explicitly, so a client whose soft reset turns
// wrap on still ends with it off.
func TestSnapshotSetsAutowrapWhateverTheSoftResetDoes(t *testing.T) {
	a := feed(20, 5, "\x1b[?7l"+strings.Repeat("abc", 10))
	data, _ := a.Snapshot(1 << 20)
	if !bytes.HasPrefix(data, []byte("\x1b[!p\x1b[?7l")) {
		t.Fatalf("snapshot %q", data)
	}
	wrapping := bytes.Replace(data, []byte("\x1b[!p"), []byte("\x1b[!p\x1b[?7h"), 1)
	b := New(20, 5)
	b.Write(wrapping)
	if b.m.autowrap {
		t.Fatal("autowrap is on after replay")
	}
	if d := diff(a, b); d != "" {
		t.Fatalf("replay into a wrap-on soft reset differs: %s", d)
	}
}
