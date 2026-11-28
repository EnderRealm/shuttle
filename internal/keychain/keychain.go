package keychain

import (
	"bytes"
	"context"
	"encoding/hex"
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

// PrintedPassword is what `security find-generic-password -g` prints for item
// on stderr, for ParsePassword. Unlike -w, whose output for a value holding a
// byte outside printable ASCII is that value's hex and so cannot be told from
// a token of hex digits, -g marks which form it printed. security is killed
// once ctx ends; the error names the item and carries nothing security printed.
func PrintedPassword(ctx context.Context, item string) ([]byte, error) {
	if item == "" {
		return nil, fmt.Errorf("keychain item: no name given")
	}
	var printed bytes.Buffer
	cmd := exec.CommandContext(ctx, securityBin, "find-generic-password", "-s", item, "-g")
	// stdout carries the item's attributes and no secret.
	cmd.Stderr = &printed
	err := cmd.Run()
	if err != nil && ctx.Err() != nil {
		err = ctx.Err()
	}
	if err != nil {
		return nil, fmt.Errorf("keychain item %q: %w", item, err)
	}
	return printed.Bytes(), nil
}

// passwordLine starts the line of -g's output that carries the password.
const passwordLine = "password: "

// ParsePassword returns item's password from what PrintedPassword returned:
// `password: "<value>"` for a value security prints as it stands, and
// `password: 0x<hex>` followed by a rendering for any other, whose hex is the
// value's bytes. The error names the item and never the value.
func ParsePassword(item string, printed []byte) (string, error) {
	for line := range strings.SplitSeq(string(printed), "\n") {
		rest, ok := strings.CutPrefix(line, passwordLine)
		if !ok {
			continue
		}
		var value string
		if digits, ok := strings.CutPrefix(rest, "0x"); ok {
			digits, _, _ = strings.Cut(digits, " ")
			decoded, err := hex.DecodeString(digits)
			if err != nil {
				return "", fmt.Errorf("keychain item %q: security printed a password that is not hex", item)
			}
			value = string(decoded)
		} else if len(rest) >= 2 && rest[0] == '"' && rest[len(rest)-1] == '"' {
			value = rest[1 : len(rest)-1]
		} else if rest != "" {
			return "", fmt.Errorf("keychain item %q: security printed the password in an unrecognised form", item)
		}
		if value == "" {
			return "", fmt.Errorf("keychain item %q: empty value", item)
		}
		return value, nil
	}
	return "", fmt.Errorf("keychain item %q: security printed no password", item)
}
