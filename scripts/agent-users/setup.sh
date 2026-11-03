#!/bin/bash
# Create or repair the macOS users hosted agents run as: weftagent1..N, the weftagents group,
# the sudoers rule that lets the operator run commands as them, and the shared root their
# checkouts and worktrees live in. Run with sudo from the operator's account:
#
#   sudo scripts/agent-users/setup.sh N
#
# Each item is compared with its desired state and changed only when it differs; every change
# prints one "changed: ..." line, and a run that changes nothing says so. Then run check.sh as
# the operator. See docs/agent-users.md.
set -euo pipefail

source "$(dirname "$0")/common.sh"
# sudo keeps the operator's PATH; pin the tools this runs as root.
PATH=/opt/homebrew/bin:/usr/bin:/bin:/usr/sbin:/sbin
export PATH
# git, run as root or as the operator, must not discover a repository from the caller's cwd.
cd /

OPERATOR="${SUDO_USER:-}"
if [ "$(id -u)" -ne 0 ] || [ -z "$OPERATOR" ] || [ "$OPERATOR" = root ]; then
  echo "run with sudo from the operator's account: sudo $0 N" >&2
  exit 1
fi
if [ $# -ne 1 ] || ! [[ "$1" =~ ^[1-9][0-9]*$ ]]; then
  echo "usage: sudo $0 N   (N >= 1 agent users)" >&2
  exit 64
fi
N=$1

CHANGES=0
changed() {
  echo "changed: $*"
  CHANGES=$((CHANGES + 1))
}
die() {
  echo "setup: $*" >&2
  exit 1
}
is_member() {
  [ "$(dsmemberutil checkmembership -U "$1" -G "$2" 2>/dev/null)" = "user is a member of the group" ]
}
in_membership_list() {
  case " $(dscl . -read "/Groups/$2" GroupMembership 2>/dev/null) " in
    *" $1 "*) return 0 ;;
  esac
  return 1
}
# dscl -read of a key the record lacks exits 0, printing "No such key" on stderr and nothing on
# stdout (measured, as the operator against _www), so absence is a successful read with no output.
lacks_key() {
  local out
  out=$(dscl . -read "/Users/$1" "$2" 2>/dev/null) && [ -z "$out" ]
}
# No password authentication: Password "*" and neither an AuthenticationAuthority nor a shadow
# hash, exactly what the repair below leaves.
password_disabled() {
  [ "$(dscl . -read "/Users/$1" Password 2>/dev/null)" = "Password: *" ] \
    && lacks_key "$1" AuthenticationAuthority \
    && lacks_key "$1" dsAttrTypeNative:ShadowHashData
}

# A process keeps the groups it started with, and a login session outlives the password that
# opened it, so repairing a user's credentials reaches only processes started after the repair.
# stop_before_repair <user>: called before each repair of <user>'s password authentication,
# primary group or group removals; kills its processes on the first call. setup_user kills them
# again once the repairs are in, so anything started in between with the old credentials goes too.
STOPPED=""
stop_before_repair() {
  [ "$STOPPED" = "$1" ] && return 0
  kill_agent "$1" || die "processes running as $1 survived SIGKILL; its credentials were not repaired"
  STOPPED=$1
  changed "killed every process running as $1: they held the credentials being repaired"
}

# Agents commit under the operator's identity: weft lands their commits by fast-forward, so
# this is the authorship that reaches history.
op_git() { sudo -u "$OPERATOR" -H git config --global --get "$1" || true; }
OP_NAME=$(op_git user.name)
OP_EMAIL=$(op_git user.email)
if [ -z "$OP_NAME" ] || [ -z "$OP_EMAIL" ]; then
  die "$OPERATOR has no global git user.name and user.email; set them (git config --global) and re-run"
fi

# --- group -----------------------------------------------------------------------------
# The operator is not a member: the agent-only denies in a checkout are written against this
# group, and the operator reaches agent-made files through its own inherited ACE instead.
if ! dscl . -read "/Groups/$AGENT_GROUP" >/dev/null 2>&1; then
  dseditgroup -o create -r "weft agents" "$AGENT_GROUP"
  changed "created group $AGENT_GROUP"
fi
GID=$(dscl . -read "/Groups/$AGENT_GROUP" PrimaryGroupID | awk '{print $2}')
[ -n "$GID" ] || die "group $AGENT_GROUP has no PrimaryGroupID"

# --- users -----------------------------------------------------------------------------
setup_user() {
  local user="$1" home="/Users/$1" g tmp cfg tree uid

  if ! id "$user" >/dev/null 2>&1; then
    # Built with dscl rather than sysadminctl, which takes a password in argv, where ps shows
    # it to every user, and makes the user a member of staff. Nothing logs in as an agent; the
    # operator reaches it through sudo. So the record has no password from the start ("*"
    # matches none, as for the system's own _ accounts) and is in weftagents, never staff, and
    # its UniqueID is set last: until then it is not a user any process can run as.
    uid=$(dscl . -list /Users UniqueID | awk '$2 >= 501 && $2 > max {max = $2} END {print (max ? max : 500) + 1}')
    dscl . -create "/Users/$user"
    dscl . -create "/Users/$user" Password '*'
    dscl . -create "/Users/$user" PrimaryGroupID "$GID"
    dscl . -create "/Users/$user" NFSHomeDirectory "$home"
    dscl . -create "/Users/$user" UserShell /bin/zsh
    dscl . -create "/Users/$user" RealName "weft agent ${user#"$AGENT_PREFIX"}"
    dscl . -create "/Users/$user" UniqueID "$uid"
    dsmemberutil flushcache
    id "$user" >/dev/null 2>&1 || die "dscl did not create $user"
    changed "created user $user"
  fi

  # No password authentication. A user made by sysadminctl, as the spike's weftagent1 was,
  # carries a shadow-hash password.
  if ! password_disabled "$user"; then
    stop_before_repair "$user"
    dscl . -delete "/Users/$user" AuthenticationAuthority 2>/dev/null || true
    dscl . -delete "/Users/$user" dsAttrTypeNative:ShadowHashData 2>/dev/null || true
    dscl . -create "/Users/$user" Password '*'
    password_disabled "$user" || die "could not disable password authentication for $user"
    changed "$user password authentication disabled"
  fi

  # Out of staff: the operator's home is 750 <operator>:staff, so a staff member could read
  # ~/.claude, ~/.gitconfig and ~/code.
  if [ "$(dscl . -read "/Users/$user" PrimaryGroupID | awk '{print $2}')" != "$GID" ]; then
    stop_before_repair "$user"
    dscl . -create "/Users/$user" PrimaryGroupID "$GID"
    dsmemberutil flushcache
    changed "$user primary group $AGENT_GROUP"
  fi
  # Joining the group takes nothing away from a running process, so it kills nothing.
  if ! in_membership_list "$user" "$AGENT_GROUP"; then
    dseditgroup -o edit -a "$user" -t user "$AGENT_GROUP"
    changed "$user added to $AGENT_GROUP"
  fi
  for g in staff admin; do
    if is_member "$user" "$g"; then
      stop_before_repair "$user"
      dseditgroup -o edit -d "$user" -t user "$g"
      dsmemberutil flushcache
      is_member "$user" "$g" && die "$user is still a member of $g after removal; check nested groups"
      changed "$user removed from $g"
    fi
  done
  # Anything started as $user since the first kill may hold the old credentials.
  if [ "$STOPPED" = "$user" ]; then
    dsmemberutil flushcache
    kill_agent "$user" \
      || die "processes running as $user survived SIGKILL after its credentials were repaired and may hold the old ones; kill them (sudo pkill -KILL -U $user) before using $user"
  fi

  [ "$(dscl . -read "/Users/$user" NFSHomeDirectory | awk '{print $2}')" = "$home" ] \
    || die "$user's home is not $home; fix it by hand"
  [ -L "$home" ] && die "$home is a symlink"
  if [ ! -d "$home" ]; then
    createhomedir -c -u "$user" >/dev/null 2>&1 || true
    [ -d "$home" ] || die "createhomedir did not create $home"
    changed "created $home"
  fi
  # Root changes the home directory itself and nothing in it: the agent controls the contents,
  # and can hard-link any file it can reach on this volume into them (measured: a checkout's
  # .git/config, a Homebrew binary), so a recursive chown would hand it those files. With the
  # home 0700 nobody else can reach what is in it, whatever its group.
  if [ "$(stat -f '%Su:%Sg %Lp' "$home")" != "$user:$AGENT_GROUP 700" ]; then
    chown "$user:$AGENT_GROUP" "$home"
    chmod 0700 "$home"
    changed "$home owner $user:$AGENT_GROUP mode 700"
  fi

  # Identity, and trust in the operator-owned repositories in the two agent-writable trees:
  # git refuses a repository its user does not own unless safe.directory names it.
  tmp=$(mktemp)
  git config --file "$tmp" user.name "$OP_NAME"
  git config --file "$tmp" user.email "$OP_EMAIL"
  for tree in "${SHARED_TREES[@]}"; do
    git config --file "$tmp" --add safe.directory "$SHARED_ROOT/$tree/*"
  done
  cfg="$home/.gitconfig"
  # Read and written as the agent, the desired content on stdin: anything the agent planted at
  # the path, such as a symlink to a directory root writes in, can then only redirect the
  # agent's own privileges. The file is made in the home, so it takes the home's group; mv -h
  # renames over a symlink rather than moving into the directory it names.
  if [ -L "$cfg" ] || ! sudo -u "$user" /usr/bin/cmp -s - "$cfg" < "$tmp" \
    || [ "$(stat -f '%Su:%Sg %Lp' "$cfg")" != "$user:$AGENT_GROUP 644" ]; then
    sudo -u "$user" /bin/sh -c 't=$(/usr/bin/mktemp "$1.XXXXXX") && /bin/cat > "$t" && /bin/chmod 0644 "$t" && /bin/mv -f -h "$t" "$1"' \
      sh "$cfg" < "$tmp" || die "could not write $cfg as $user"
    changed "$cfg"
  fi
  rm -f "$tmp"
}

agents=()
for i in $(seq 1 "$N"); do
  setup_user "$AGENT_PREFIX$i"
  agents+=("$AGENT_PREFIX$i")
done
for user in $(dscl . -list /Users | grep -E "^$AGENT_PREFIX[0-9]+\$" || true); do
  if [ "${user#"$AGENT_PREFIX"}" -gt "$N" ]; then
    echo "note: $user is above $N: left alone and not in sudoers; teardown.sh removes it"
  fi
done

# --- sudoers ---------------------------------------------------------------------------
# The operator runs commands as the agent users without a password: a step down in privilege,
# never up, since the runas list names only the agents. env_reset is sudo's default; the
# OAuth token is kept so it reaches the agent through the environment, never through argv,
# which ps shows to every user.
runas=$(printf '%s, ' "${agents[@]}")
runas=${runas%, }
tmp=$(mktemp)
cat > "$tmp" <<EOF
# Managed by shuttle scripts/agent-users/setup.sh; teardown.sh removes it. See docs/agent-users.md.
Runas_Alias WEFT_AGENTS = $runas
Defaults>WEFT_AGENTS env_keep += "CLAUDE_CODE_OAUTH_TOKEN"
$OPERATOR ALL = (WEFT_AGENTS) NOPASSWD: ALL
EOF
visudo -cf "$tmp" >/dev/null || die "generated sudoers failed visudo -cf; candidate left at $tmp"
if [ -L "$SUDOERS_FILE" ] || ! cmp -s "$tmp" "$SUDOERS_FILE" \
  || [ "$(stat -f '%Su:%Sg %Lp' "$SUDOERS_FILE")" != "root:wheel 440" ]; then
  backup=""
  if [ -e "$SUDOERS_FILE" ]; then
    backup=$(mktemp)
    cp -p "$SUDOERS_FILE" "$backup"
  fi
  install -S -m 0440 -o root -g wheel "$tmp" "$SUDOERS_FILE"
  if ! visudo -c >/dev/null; then
    if [ -n "$backup" ]; then
      install -S -m 0440 -o root -g wheel "$backup" "$SUDOERS_FILE"
    else
      rm -f "$SUDOERS_FILE"
    fi
    die "sudo config failed visudo -c with the new $SUDOERS_FILE; previous state restored"
  fi
  [ -z "$backup" ] || rm -f "$backup"
  changed "$SUDOERS_FILE"
fi
rm -f "$tmp"

# --- shared root -----------------------------------------------------------------------
# $SHARED_ROOT itself carries no ACL: agents traverse it but cannot write it. repos/ and
# worktrees/ are the only agent-writable trees. Everything created in them inherits two allow
# ACEs: the operator's, so the human can open and continue anything an agent made, and the
# group's, so an agent can work in anything the operator made. The trees themselves also deny
# the group delete, without which an agent could move a whole tree out of the shared root into
# its home. New files take their parent directory's group (BSD semantics), so ownership never
# needs fixing up. Existing contents are not re-ACL'd.
ref=$(mktemp -d)
apply_shared_acl "$ref" "$OPERATOR"
SHARED_ACES=$(ace_lines "$ref")
rm -rf "$ref"

# setup_dir <path> <mode> <want-aces>: <path> owned <operator>:<group>, mode <mode>, carrying
# exactly <want-aces> (none when empty).
setup_dir() {
  local dir="$1" mode="$2" want="$3"
  [ -L "$dir" ] && die "$dir is a symlink"
  if [ ! -d "$dir" ]; then
    mkdir "$dir"
    changed "created $dir"
  fi
  if [ "$(stat -f '%Su:%Sg' "$dir")" != "$OPERATOR:$AGENT_GROUP" ]; then
    chown "$OPERATOR:$AGENT_GROUP" "$dir"
    changed "$dir owner $OPERATOR:$AGENT_GROUP"
  fi
  if [ "$(stat -f '%Lp' "$dir")" != "$mode" ]; then
    chmod "$mode" "$dir"
    changed "$dir mode $mode"
  fi
  if [ "$(ace_lines "$dir")" != "$want" ]; then
    if [ -z "$want" ]; then
      chmod -N "$dir"
    else
      apply_shared_acl "$dir" "$OPERATOR"
    fi
    changed "$dir ACL"
  fi
}

setup_dir "$SHARED_ROOT" 750 ""
for tree in "${SHARED_TREES[@]}"; do
  setup_dir "$SHARED_ROOT/$tree" 770 "$SHARED_ACES"
done

if [ "$CHANGES" -eq 0 ]; then
  echo "nothing changed: ${#agents[@]} agent users, $SUDOERS_FILE and $SHARED_ROOT already as desired"
else
  echo "$CHANGES changes made"
fi
echo "next, as $OPERATOR: $(dirname "$0")/check.sh"
