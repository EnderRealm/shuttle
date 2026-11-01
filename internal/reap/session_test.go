package reap

import (
	"errors"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"
)

// fakeSessions stands in for the kernel's process table under
// ContainSession: sid is each live pid's session, a pid in immortal outlives
// SIGKILL, and a pid in reused answers the state lookup with another start
// time, as the pid handed on between the listing and the stop does.
type fakeSessions struct {
	sid      map[int]int
	immortal map[int]bool
	reused   map[int]bool
	signals  map[int][]syscall.Signal
}

func (f *fakeSessions) install(t *testing.T) {
	t.Helper()
	start := time.Unix(1_700_000_000, 0)
	f.signals = map[int][]syscall.Signal{}
	priorList, priorState, priorSignal, priorGrace := listSession, readState, sendSignal, killGrace
	listSession = func(sid int) ([]sessionMember, error) {
		var members []sessionMember
		for pid, s := range f.sid {
			if s == sid {
				members = append(members, sessionMember{PID: pid, PGID: pid, Start: start, Comm: "git"})
			}
		}
		return members, nil
	}
	readState = func(pid int) (member, bool, error) {
		if _, ok := f.sid[pid]; !ok {
			return member{}, false, errProcessGone
		}
		if f.reused[pid] {
			return member{PID: pid, Start: start.Add(time.Second)}, false, nil
		}
		stopped := false
		for _, sig := range f.signals[pid] {
			stopped = stopped || sig == syscall.SIGSTOP
		}
		return member{PID: pid, Start: start}, stopped, nil
	}
	sendSignal = func(pid int, sig syscall.Signal) error {
		f.signals[pid] = append(f.signals[pid], sig)
		if sig == syscall.SIGKILL && !f.immortal[pid] {
			delete(f.sid, pid)
		}
		if sig == syscall.SIGCONT && f.reused[pid] {
			// The process now holding the pid is in a session of its own.
			f.sid[pid] = pid
		}
		return nil
	}
	killGrace = 100 * time.Millisecond
	t.Cleanup(func() {
		listSession, readState, sendSignal, killGrace = priorList, priorState, priorSignal, priorGrace
	})
}

func TestContainSessionKillsOnlyTheSessionsProcesses(t *testing.T) {
	f := &fakeSessions{sid: map[int]int{1: 100, 2: 200, 3: 3}}
	f.install(t)
	c, err := ContainSession(100)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(c.Stopped, []int{1}) || !reflect.DeepEqual(c.Killed, []int{1}) ||
		!reflect.DeepEqual(f.signals, map[int][]syscall.Signal{1: {syscall.SIGSTOP, syscall.SIGKILL}}) {
		t.Fatalf("containment %s, signals %v: want only pid 1 stopped and killed", c, f.signals)
	}
}

func TestContainSessionFailsWhileAMemberLives(t *testing.T) {
	f := &fakeSessions{sid: map[int]int{1: 100}, immortal: map[int]bool{1: true}}
	f.install(t)
	c, err := ContainSession(100)
	if err == nil || !strings.Contains(err.Error(), "still live") || !strings.Contains(err.Error(), "1(git)") {
		t.Fatalf("err = %v, want the live member named", err)
	}
	if !reflect.DeepEqual(c.Killed, []int{1}) {
		t.Fatalf("killed %v, want pid 1 signalled once", c.Killed)
	}
}

func TestContainSessionReleasesAPidHandedOnBeforeTheStop(t *testing.T) {
	f := &fakeSessions{sid: map[int]int{1: 100}, reused: map[int]bool{1: true}}
	f.install(t)
	c, err := ContainSession(100)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(c.Released, []int{1}) || len(c.Killed) != 0 ||
		!reflect.DeepEqual(f.signals[1], []syscall.Signal{syscall.SIGSTOP, syscall.SIGCONT}) {
		t.Fatalf("containment %s, signals %v: want pid 1 continued, never killed", c, f.signals)
	}
}

func TestContainSessionFailsOnAListingError(t *testing.T) {
	prior := listSession
	listSession = func(int) ([]sessionMember, error) { return nil, errors.New("kern.proc.all: invalid argument") }
	t.Cleanup(func() { listSession = prior })
	if _, err := ContainSession(100); err == nil || !strings.Contains(err.Error(), "kern.proc.all") {
		t.Fatalf("err = %v, want the failed listing", err)
	}
}
