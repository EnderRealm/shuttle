package reap

import (
	"encoding/binary"
	"fmt"
	"os"
	"os/exec"
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

// pWexit is a process that has begun exiting (P_WEXIT in sys/proc.h, which x/sys
// does not export). Its address space can already be gone while kill 0, getpgid
// and kern.proc.pid all still answer for it, so an argv read that fails on it
// looks like any other lookup error unless the flag is read alongside.
const pWexit = 0x00002000

// procStates names P_stat (SIDL..SZOMB in sys/proc.h).
var procStates = map[int8]string{1: "SIDL", 2: "SRUN", 3: "SSLEEP", sstop: "SSTOP", szomb: "SZOMB"}

// describePID is the kernel's view of pid at this moment, for a failure message
// whose expected outcome was missed on a lookup error: whether the process is a
// zombie or exiting, when it started, and whether kern.procargs2 still fails on
// a second read. Together those tell an exiting process, a reused pid and a
// transiently unreliable interface apart.
func describePID(pid int) string {
	var b strings.Builder
	fmt.Fprintf(&b, "kernel view of pid %d: ", pid)
	kp, err := unix.SysctlKinfoProc("kern.proc.pid", pid)
	switch {
	case err != nil:
		fmt.Fprintf(&b, "kern.proc.pid: %v", err)
	case kp.Proc.P_pid != int32(pid):
		fmt.Fprintf(&b, "kern.proc.pid: record for pid %d", kp.Proc.P_pid)
	default:
		sec, nsec := kp.Proc.P_starttime.Unix()
		fmt.Fprintf(&b, "p_stat %d (%s), exiting %t, started %s",
			kp.Proc.P_stat, procStates[kp.Proc.P_stat], kp.Proc.P_flag&pWexit != 0,
			time.Unix(sec, nsec).UTC().Format(time.RFC3339Nano))
	}
	if buf, err := unix.SysctlRaw("kern.procargs2", pid); err != nil {
		fmt.Fprintf(&b, "; kern.procargs2 re-read: %v", err)
	} else {
		fmt.Fprintf(&b, "; kern.procargs2 re-read: answers %d bytes", len(buf))
	}
	return b.String()
}

// The dump has to tell the live process a fixture started from one that has
// exited and is not reaped: that is the state in which every earlier check of
// identify still answers and the argv read is the first to fail.
func TestDescribePID(t *testing.T) {
	live := spawn(t, []string{"sleep", "300"}, false).Process.Pid
	got := describePID(live)
	// XNU keeps p_stat at SRUN for a blocked process; sleeping is Mach thread
	// state, so a live sleeper never reads SSLEEP.
	if !strings.Contains(got, "(SRUN), exiting false") || !strings.Contains(got, "kern.procargs2 re-read: answers") {
		t.Fatalf("live: %s", got)
	}

	cmd := exec.Command("true")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start true: %v", err)
	}
	// Not waited on until the dump is taken, so the exited child stays a zombie.
	t.Cleanup(func() { cmd.Wait() })
	zombie := cmd.Process.Pid
	deadline := time.Now().Add(5 * time.Second)
	for {
		kp, err := unix.SysctlKinfoProc("kern.proc.pid", zombie)
		if err == nil && kp.Proc.P_stat == szomb {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("pid %d never became a zombie", zombie)
		}
		time.Sleep(5 * time.Millisecond)
	}
	got = describePID(zombie)
	if !strings.Contains(got, "(SZOMB), exiting true") || strings.Contains(got, "re-read: answers") {
		t.Fatalf("zombie: %s", got)
	}
}
