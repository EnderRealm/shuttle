//go:build !darwin

package reap

import (
	"errors"
	"time"
)

// errNoIdentity is a lookup that establishes nothing, so on a platform whose
// kernel this cannot read, a live recorded pid is never signalled and its row is
// held in flight rather than closed on an identity nobody checked.
var errNoIdentity = errors.New("process identity is not readable on this platform")

func processStart(int) (time.Time, error) { return time.Time{}, errNoIdentity }

func processArgv(int) ([]string, error) { return nil, errNoIdentity }

func groupMembers(int) ([]member, error) { return nil, errNoIdentity }

func sessionMembers(int) ([]sessionMember, error) { return nil, errNoIdentity }

func userProcesses() ([]member, error) { return nil, errNoIdentity }

func processState(int) (member, bool, error) { return member{}, false, errNoIdentity }

func uidProcesses(int) ([]UserProcess, error) { return nil, errNoIdentity }
