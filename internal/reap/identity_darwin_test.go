package reap

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// procArgs lays a buffer out the way kern.procargs2 does: the int32 argc, the
// exec path with its terminator and the NUL padding that follows it, the argv
// strings, then the environment, which is not argv and must not be read as it.
func procArgs(argc int32, execPath string, argv, env []string) []byte {
	buf := binary.LittleEndian.AppendUint32(nil, uint32(argc))
	buf = append(buf, execPath...)
	buf = append(buf, 0, 0, 0, 0)
	for _, s := range append(append([]string{}, argv...), env...) {
		buf = append(buf, s...)
		buf = append(buf, 0)
	}
	return buf
}

func TestParseProcArgsTakesExactlyArgcArguments(t *testing.T) {
	argv := []string{"sh", "--settings", "/runs/daily-1/settings.json"}
	got, err := parseProcArgs(procArgs(3, "/bin/sh", argv, []string{"PATH=/usr/bin", "HOME=/tmp"}))
	if err != nil {
		t.Fatalf("parseProcArgs: %v", err)
	}
	if strings.Join(got, " ") != strings.Join(argv, " ") {
		t.Fatalf("argv = %q, want %q", got, argv)
	}
}

func TestParseProcArgsRejectsAShortBuffer(t *testing.T) {
	// argc says three and only two strings follow: reading the answer as if it
	// held the argv would identify a process on an argument it never carried.
	_, err := parseProcArgs(procArgs(3, "/bin/sh", []string{"sh", "--settings"}, nil))
	if err == nil || !strings.Contains(err.Error(), "2 of 3") {
		t.Fatalf("err = %v, want one naming the missing argument", err)
	}
}

func TestParseProcArgsReadsThisProcess(t *testing.T) {
	buf, err := unix.SysctlRaw("kern.procargs2", os.Getpid())
	if err != nil {
		t.Fatalf("kern.procargs2: %v", err)
	}
	got, err := parseProcArgs(buf)
	if err != nil {
		t.Fatalf("parseProcArgs: %v", err)
	}
	if strings.Join(got, " ") != strings.Join(os.Args, " ") {
		t.Fatalf("argv = %q, want %q", got, os.Args)
	}
}

// Every probe in the kill path is decided on this listing, so it is read
// against a real group: the leader Setpgid makes, and the descendant a run's
// own process tree has.
func TestGroupMembersListsTheGroupWithStartTimes(t *testing.T) {
	child := filepath.Join(t.TempDir(), "child.pid")
	cmd := spawn(t, []string{"sh", "-c", "sleep 300 & echo $! > " + child + "; wait"}, true)
	leader := cmd.Process.Pid
	descendant := readPID(t, child)

	members, err := groupMembers(leader)
	if err != nil {
		t.Fatalf("groupMembers: %v", err)
	}
	var start time.Time
	found := map[int]bool{}
	for _, m := range members {
		found[m.PID] = true
		if m.PID == leader {
			start = m.Start
		}
	}
	// The descendant too: it is the member that outlives a leader which took
	// the SIGTERM, and the one the SIGKILL is for.
	if !found[leader] || !found[descendant] {
		t.Fatalf("members = %+v, want the leader %d and its descendant %d", members, leader, descendant)
	}
	if age := time.Since(start); age < 0 || age > 30*time.Second {
		t.Fatalf("leader start = %s, want it moments ago", start)
	}
}
