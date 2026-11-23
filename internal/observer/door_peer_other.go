//go:build !darwin

package observer

import (
	"errors"
	"net"
)

// peerUID reads no peer credentials off darwin, so the door attributes, and
// therefore applies, no call there.
func peerUID(*net.UnixConn) (int, error) {
	return 0, errors.New("peer credentials are read only on darwin")
}
