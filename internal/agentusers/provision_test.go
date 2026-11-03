// Package agentusers holds integration tests of scripts/agent-users against the
// real shared root setup.sh lays out. It is test-only; where that root, or the
// operator who owns it, is absent, its tests skip.
package agentusers

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
)

const (
	sharedRoot = "/Users/Shared/weft"
	agentGroup = "weftagents"
)

// The deny sets protect-checkout.sh must leave, as /bin/ls -le spells them.
// They are written out here rather than read from common.sh, so a change there
// that weakens a deny fails this test instead of moving it. On a directory ls
// spells write and append as add_file and add_subdirectory, so the file deny
// inherited from .git/hooks reads with those names.
var (
	fileDeny        = []string{"write", "append", "delete", "writeattr", "writeextattr", "writesecurity", "chown"}
	dirDeny         = []string{"add_file", "add_subdirectory", "delete_child", "delete", "writeattr", "writeextattr", "writesecurity", "chown"}
	inheritFileDeny = []string{"add_file", "add_subdirectory", "delete", "writeattr", "writeextattr", "writesecurity", "chown",
		"file_inherit", "only_inherit", "limit_inherit"}
	keepDeny = []string{"delete"}
)

// ace is one entry of a path's ACL as /bin/ls -le renders it.
type ace struct {
	principal string
	inherited bool
	kind      string
	perms     []string
}

func aces(t *testing.T, path string) []ace {
	t.Helper()
	out, err := exec.Command("/bin/ls", "-lde", path).CombinedOutput()
	if err != nil {
		t.Fatalf("ls -lde %s: %v: %s", path, err, out)
	}
	var list []ace
	for _, line := range strings.Split(strings.TrimRight(string(out), "\n"), "\n")[1:] {
		fields := strings.Fields(line)
		if len(fields) < 4 {
			t.Fatalf("%s: unreadable ACE %q", path, line)
		}
		a := ace{principal: fields[1]}
		rest := fields[2:]
		if rest[0] == "inherited" {
			a.inherited, rest = true, rest[1:]
		}
		if len(rest) != 2 {
			t.Fatalf("%s: unreadable ACE %q", path, line)
		}
		a.kind, a.perms = rest[0], strings.Split(rest[1], ",")
		list = append(list, a)
	}
	return list
}

func sameSet(a, b []string) bool {
	a, b = slices.Sorted(slices.Values(a)), slices.Sorted(slices.Values(b))
	return slices.Equal(a, b)
}

// requireDeny fails unless path carries a group:weftagents deny of exactly perms.
func requireDeny(t *testing.T, path string, perms []string) {
	t.Helper()
	list := aces(t, path)
	for _, a := range list {
		if a.principal == "group:"+agentGroup && a.kind == "deny" && sameSet(a.perms, perms) {
			return
		}
	}
	t.Errorf("%s carries no group:%s deny %s; its ACL is %+v", path, agentGroup, strings.Join(perms, ","), list)
}

// tkProject is tk's projectFromGitRemote: the project a checkout resolves to
// from its remote.origin.url.
func tkProject(remote string) string {
	remote = strings.TrimSpace(remote)
	remote = strings.TrimSuffix(remote, ".git")
	remote = strings.TrimSuffix(remote, "/")
	if i := strings.LastIndex(remote, "/"); i >= 0 {
		remote = remote[i+1:]
	} else if i := strings.LastIndex(remote, ":"); i >= 0 {
		remote = remote[i+1:]
	}
	return strings.TrimSpace(remote)
}

func git(t *testing.T, args ...string) string {
	t.Helper()
	out, err := exec.Command("git", args...).CombinedOutput()
	if err != nil {
		t.Fatalf("git %s: %v: %s", strings.Join(args, " "), err, out)
	}
	return strings.TrimSpace(string(out))
}

// requireSharedRoot skips unless this user is the operator of a shared root
// setup.sh laid out: only the operator can make the stage provisioning needs,
// so an agent user running the suite skips rather than fails.
func requireSharedRoot(t *testing.T) {
	t.Helper()
	if _, err := user.LookupGroup(agentGroup); err != nil {
		t.Skipf("group %s does not exist (%v); setup.sh has not run on this host", agentGroup, err)
	}
	if info, err := os.Stat(filepath.Join(sharedRoot, "repos")); err != nil || !info.IsDir() {
		t.Skipf("%s/repos is not a directory; setup.sh has not run on this host", sharedRoot)
	}
	if os.Getuid() == 0 {
		t.Skip("provision-checkout.sh refuses root; run as the operator")
	}
	info, err := os.Stat(sharedRoot)
	if err != nil {
		t.Skipf("%s: %v", sharedRoot, err)
	}
	if st, ok := info.Sys().(*syscall.Stat_t); !ok || int(st.Uid) != os.Getuid() {
		t.Skipf("%s is not owned by uid %d, so this user cannot stage a checkout in it", sharedRoot, os.Getuid())
	}
}

func TestProvisionCheckoutProtectsBeforePublishing(t *testing.T) {
	requireSharedRoot(t)
	script, err := filepath.Abs(filepath.Join("..", "..", "scripts", "agent-users", "provision-checkout.sh"))
	if err != nil {
		t.Fatal(err)
	}

	// A dot-prefixed name keeps the checkout off any real one's and out of the
	// catalog's project id shape.
	suffix := make([]byte, 4)
	if _, err := rand.Read(suffix); err != nil {
		t.Fatal(err)
	}
	name := fmt.Sprintf(".provision-test-%d-%s", os.Getpid(), hex.EncodeToString(suffix))
	dir := t.TempDir()
	work, origin := filepath.Join(dir, "work"), filepath.Join(dir, name+".git")
	git(t, "init", "-q", "-b", "main", work)
	if err := os.WriteFile(filepath.Join(work, "README"), []byte("provision test\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	git(t, "-C", work, "add", "README")
	git(t, "-C", work, "-c", "user.name=provision test", "-c", "user.email=provision@example.com",
		"-c", "commit.gpgsign=false", "commit", "-q", "-m", "base")
	git(t, "clone", "-q", "--bare", work, origin)

	checkout := filepath.Join(sharedRoot, "repos", name)
	t.Cleanup(func() {
		if err := os.RemoveAll(checkout); err != nil {
			t.Errorf("remove %s: %v", checkout, err)
		}
		stages, _ := filepath.Glob(filepath.Join(sharedRoot, ".provision-"+name+"-*"))
		for _, stage := range stages {
			if err := os.RemoveAll(stage); err != nil {
				t.Errorf("remove %s: %v", stage, err)
			}
		}
	})

	out, err := exec.Command(script, origin).CombinedOutput()
	t.Logf("provision-checkout.sh %s:\n%s", origin, out)
	if err != nil {
		t.Fatalf("provision-checkout.sh: %v", err)
	}
	if !strings.Contains(string(out), "published: "+checkout) {
		t.Errorf("output does not name the published %s", checkout)
	}

	gitDir := filepath.Join(checkout, ".git")
	requireDeny(t, filepath.Join(gitDir, "config"), fileDeny)
	requireDeny(t, filepath.Join(gitDir, "hooks"), dirDeny)
	requireDeny(t, filepath.Join(gitDir, "hooks"), inheritFileDeny)
	hooks, err := os.ReadDir(filepath.Join(gitDir, "hooks"))
	if err != nil {
		t.Fatal(err)
	}
	if len(hooks) == 0 {
		t.Errorf("%s/hooks is empty: there is no hook file to check the deny on", gitDir)
	}
	for _, hook := range hooks {
		requireDeny(t, filepath.Join(gitDir, "hooks", hook.Name()), fileDeny)
	}
	requireDeny(t, checkout, keepDeny)
	requireDeny(t, gitDir, keepDeny)
	allowed := false
	for _, a := range aces(t, checkout) {
		if a.principal == "group:"+agentGroup && a.inherited && a.kind == "allow" {
			allowed = true
		}
	}
	if !allowed {
		t.Errorf("%s carries no inherited group:%s allow, without which agents cannot commit in it", checkout, agentGroup)
	}
	if got := tkProject(git(t, "-C", checkout, "config", "--get", "remote.origin.url")); got != name {
		t.Errorf("tk resolves the checkout's origin to project %q, not its directory name %q", got, name)
	}

	head := git(t, "-C", checkout, "rev-parse", "HEAD")
	if out, err := exec.Command(script, origin).CombinedOutput(); err == nil {
		t.Errorf("provisioning %s again succeeded:\n%s", name, out)
	}
	if got := git(t, "-C", checkout, "rev-parse", "HEAD"); got != head {
		t.Errorf("after the refused re-run the checkout is at %s, not %s", got, head)
	}
	requireDeny(t, filepath.Join(gitDir, "config"), fileDeny)
}
