package keychain

import (
	"fmt"
	"os/exec"
	"strings"
)

// securityBin is absolute: a launchd job inherits no PATH from a login shell,
// and a PATH lookup that finds a different binary is worst of all for the tool
// that hands back credentials.
const securityBin = "/usr/bin/security"

// Find returns the password of the generic Keychain item named item. The
// item name, never the value, appears in the error.
func Find(item string) (string, error) {
	if item == "" {
		return "", fmt.Errorf("keychain item: no name given")
	}
	out, err := exec.Command(securityBin, "find-generic-password", "-s", item, "-w").Output()
	if err != nil {
		return "", fmt.Errorf("keychain item %q: %w", item, err)
	}
	value := strings.TrimSuffix(string(out), "\n")
	if value == "" {
		return "", fmt.Errorf("keychain item %q: empty value", item)
	}
	return value, nil
}
