# Names and permission sets shared by the agent-user scripts. Sourced, never run: setup.sh,
# teardown.sh, protect-checkout.sh, provision-checkout.sh and check.sh must agree on every value
# here.
# docs/agent-users.md says what each piece is for.

AGENT_PREFIX=weftagent
AGENT_GROUP=weftagents
SHARED_ROOT=/Users/Shared/weft
SHARED_TREES=(repos worktrees)
# No dot in the name: sudo skips files in sudoers.d whose names contain one.
SUDOERS_FILE=/etc/sudoers.d/weft-agents

# Allowed, and inherited by everything created under repos/ and worktrees/, for
# user:<operator> and group:weftagents alike.
SHARED_PERMS="list,add_file,search,add_subdirectory,delete_child,delete,readattr,writeattr,readextattr,writeextattr,readsecurity,file_inherit,directory_inherit"
# Denied to group:weftagents by protect-checkout.sh on what decides the repository, config and
# hooks git uses as whoever invokes it next: FILE_DENY on files such as .git/config and each hook,
# DIR_DENY on .git/hooks and .git/worktrees. A deny is evaluated before the group allow.
# DIR_DENY's delete_child does not stop an agent deleting an entry whose own ACL allows it
# delete; only a deny on the entry does.
FILE_DENY="write,append,delete,writeattr,writeextattr,writesecurity,chown"
DIR_DENY="add_file,add_subdirectory,delete_child,delete,writeattr,writeextattr,writesecurity,chown"
# FILE_DENY, inherited only by files created directly in the directory carrying it (.git/hooks),
# and not applied to the directory itself: a hook the operator adds, or replaces by renaming a new
# file over it, is denied from creation.
NEW_FILE_DENY="$FILE_DENY,file_inherit,only_inherit,limit_inherit"
# Denied to group:weftagents on a directory that must stay where git and the operator expect it
# while its contents stay agent-writable: repos/ and worktrees/ themselves, each checkout and its
# .git, and each linked worktree and its admin directory. Renaming or removing a directory takes
# delete on it, and the deny holds even where the parent allows delete_child.
KEEP_DENY="delete"

# apply_shared_acl <dir> <operator>: replace the ACL of <dir> with that of repos/ and worktrees/.
# The deny goes on before either allow: the group allow carries delete, so were it added first a
# running agent could move <dir> out of the shared root before the deny landed. chmod +a puts
# each ACE in canonical order, so the resulting ACL is the same as for any other order.
apply_shared_acl() {
  chmod -N "$1"
  chmod +a "group:$AGENT_GROUP deny $KEEP_DENY" "$1"
  chmod +a "user:$2 allow $SHARED_PERMS" "$1"
  chmod +a "group:$AGENT_GROUP allow $SHARED_PERMS" "$1"
}

# publish <from> <to>: rename(2), which unlike mv never moves into a directory at <to>: it
# replaces an empty one and fails on anything else.
publish() {
  /usr/bin/perl -e 'rename($ARGV[0], $ARGV[1]) or die "rename $ARGV[0] to $ARGV[1]: $!\n"' "$1" "$2"
}

# kill_agent <user>: SIGKILL every process whose real uid is <user> until none is left. pkill
# lists and then signals, so a child forked in between escapes one pass. Fails, listing what
# survived on stderr, if one still is after five seconds, or pgrep fails.
kill_agent() {
  local i rc
  for i in $(seq 1 20); do
    pkill -KILL -U "$1" || true
    rc=0
    pgrep -U "$1" >/dev/null || rc=$?
    [ "$rc" -eq 1 ] && return 0
    [ "$rc" -eq 0 ] || break
    sleep 0.25
  done
  echo "still running as $1 (pid, command):" >&2
  ps -U "$1" -o pid=,comm= >&2 || true
  return 1
}

# The ACEs of a path as ls renders them, index stripped, one per line. ls prints an ACE's
# permissions in its own order, so compare against a reference path given the same ACEs
# rather than against the strings passed to chmod.
ace_lines() {
  /bin/ls -lde "$1" | tail -n +2 | sed -E 's/^ *[0-9]+: //'
}
