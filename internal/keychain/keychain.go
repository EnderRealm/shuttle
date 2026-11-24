package keychain

import (
	"context"
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
	return FindContext(context.Background(), item)
}

// FindContext is Find for a caller that cannot wait on a Keychain prompt no
// one is there to answer: security is killed once ctx ends.
func FindContext(ctx context.Context, item string) (string, error) {
	if item == "" {
		return "", fmt.Errorf("keychain item: no name given")
	}
	out, err := exec.CommandContext(ctx, securityBin, "find-generic-password", "-s", item, "-w").Output()
	if err != nil && ctx.Err() != nil {
		err = ctx.Err()
	}
	if err != nil {
		return "", fmt.Errorf("keychain item %q: %w", item, err)
	}
	value := strings.TrimSuffix(string(out), "\n")
	if value == "" {
		return "", fmt.Errorf("keychain item %q: empty value", item)
	}
	return value, nil
}
