#!/usr/bin/env bash
#
# Copyright IBM Corp.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Builds the in-JVM dtest jar of HCD 1.2 (branch hcd-1.2.3, Cassandra 4.0 line) and puts it into
# build/ of this checkout, where the DSE 6 compatibility tests under
# test/distributed/org/apache/cassandra/distributed/test/dsecompat/ find it (see DseDialectTestBase).
#
#   test/bin/build-hcd12-dtest-jar.sh [<ref>]
#
# The default ref is the pinned hcd-1.2.3 commit below, so that every build produces the same jar; a ref of the
# form <remote>/<branch> (the remote of HCD12_REMOTE) builds the current tip of that branch instead (it is always
# fetched), any other ref (a commit, a tag) is fetched from that remote when it is not available locally; a warning
# is printed when a fetch fails. A build starts from `ant realclean`, unless the worktree already built the same
# commit, whose jar is then reused; build/hcd12-dtest-jar.source records the ref and the commit the jar was built
# from. The jar must carry the version of the build.xml property dse.compat.hcd12.dtest.jar.version. `ant realclean`
# in this checkout deletes the jar from build/: run the script again, which then copies the jar it already built.
#
# Environment:
#   JAVA11_HOME     the JDK 11 to build with; without it, JAVA_HOME if set, else the java on the PATH, must be a JDK 11
#   HCD12_WORKTREE  the git worktree <ref> is checked out and built in; default: build-hcd12/ next to build/, which
#                   `ant realclean` leaves alone (git ignores it)
#   HCD12_REMOTE    the git remote that carries the hcd-1.2.3 branch (github.com/datastax/cassandra); default: origin
#
# Takes about ten minutes when it builds (a build starts from `ant realclean`).
set -euo pipefail

HCD12_BRANCH=hcd-1.2.3
# the tip of hcd-1.2.3 when the harness was written; move it deliberately, and rerun the dsecompat tests
HCD12_COMMIT=0c7d6f7c94124a2b234237136aaef5e972715c46
REF=${1:-$HCD12_COMMIT}
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
WORK=${HCD12_WORKTREE:-$ROOT/build-hcd12}
REMOTE=${HCD12_REMOTE:-origin}

# ant runs on JAVA_HOME when it is set, so that is the JDK to check
if [ -n "${JAVA11_HOME:-}" ]; then
  JAVA_HOME=$JAVA11_HOME
elif [ -z "${JAVA_HOME:-}" ]; then
  JAVA_BIN=$(command -v java) || { echo "No java on the PATH: set JAVA11_HOME to a JDK 11" >&2; exit 1; }
  JAVA_HOME=$(dirname "$(dirname "$(readlink -f "$JAVA_BIN")")")
fi
export JAVA_HOME
export PATH=$JAVA_HOME/bin:$PATH
if [ ! -x "$JAVA_HOME/bin/java" ]; then
  echo "$JAVA_HOME/bin/java not found: set JAVA11_HOME to a JDK 11" >&2
  exit 1
fi
JAVA_VERSION=$("$JAVA_HOME/bin/java" -version 2>&1) || true
case "$JAVA_VERSION" in
  *'version "11'*) ;;
  *) echo "A JDK 11 is required (set JAVA11_HOME); $JAVA_HOME is: ${JAVA_VERSION%%$'\n'*}" >&2
     exit 1 ;;
esac

command -v zip > /dev/null || { echo "zip is required (it removes the signature files from the jar)" >&2; exit 1; }

EXPECTED_VERSION=$(sed -n 's/.*name="dse\.compat\.hcd12\.dtest\.jar\.version" value="\([^"]*\)".*/\1/p' "$ROOT/build.xml")
if [ -z "$EXPECTED_VERSION" ]; then
  echo "No dse.compat.hcd12.dtest.jar.version property in $ROOT/build.xml" >&2
  exit 1
fi

# the repository to fetch into and resolve the ref in: the worktree's own when it exists (HCD12_WORKTREE may point
# to a separate clone), this checkout's otherwise
if [ -e "$WORK/.git" ]; then
  if ! git -C "$WORK" rev-parse --git-dir > /dev/null 2>&1; then
    echo "$WORK has a .git but is not a usable git checkout (a pruned worktree?): delete $WORK and run again" >&2
    exit 1
  fi
  REPO=$WORK
else
  REPO=$ROOT
fi
case "$REF" in
  "$REMOTE"/*) FETCH=${REF#"$REMOTE"/} ;;
  "$HCD12_COMMIT") git -C "$REPO" rev-parse -q --verify "$REF^{commit}" > /dev/null && FETCH= || FETCH=$HCD12_BRANCH ;;
  *) git -C "$REPO" rev-parse -q --verify "$REF^{commit}" > /dev/null && FETCH= || FETCH=$REF ;;
esac
if [ -n "$FETCH" ] && ! git -C "$REPO" fetch -q "$REMOTE" "$FETCH"; then
  echo "WARNING: could not fetch $FETCH from $REMOTE (set HCD12_REMOTE to the remote that carries $HCD12_BRANCH)" >&2
fi
if ! COMMIT=$(git -C "$REPO" rev-parse -q --verify "$REF^{commit}"); then
  echo "Cannot resolve $REF to a commit (a branch of $REMOTE is given as $REMOTE/<branch>; set HCD12_REMOTE to the" \
       "remote that carries $HCD12_BRANCH)" >&2
  exit 1
fi

if [ "$REPO" = "$WORK" ]; then
  git -C "$WORK" checkout -q --detach "$COMMIT"
else
  # -f: a worktree whose directory was deleted by hand is still registered, and add refuses its path without it
  git -C "$ROOT" worktree add -f --detach "$WORK" "$COMMIT"
fi

# the commit the worktree's build/ was last built from, written once its dtest jar is complete
BUILT=$WORK/build/hcd12-built-commit
if [ -f "$BUILT" ] && [ "$(cat "$BUILT")" = "$COMMIT" ] && compgen -G "$WORK/build/dtest-*.jar" > /dev/null; then
  echo "==> reusing the HCD 1.2 dtest jar built from $COMMIT in $WORK"
else
  echo "==> building the HCD 1.2 dtest jar from $REF ($COMMIT) in $WORK"
  (cd "$WORK" && ant realclean && ant dtest-jar)
  echo "$COMMIT" > "$BUILT"
fi
JARS=("$WORK"/build/dtest-*.jar)
if [ ${#JARS[@]} -ne 1 ] || [ ! -f "${JARS[0]}" ]; then
  echo "Expected one dtest jar in $WORK/build, found: ${JARS[*]}" >&2
  exit 1
fi
JAR=${JARS[0]}
NAME=$(basename "$JAR")
if [ "$NAME" != "dtest-$EXPECTED_VERSION.jar" ]; then
  echo "$REF built $NAME, but build.xml expects dtest-$EXPECTED_VERSION.jar (dse.compat.hcd12.dtest.jar.version)" >&2
  exit 1
fi
mkdir -p "$ROOT/build"

# The HCD 1.2 dtest jar merges a signed dependency (META-INF/ECLIPSE_.SF/.RSA); the JDK then refuses every class
# in the jar ("Invalid signature file digest"). Drop the signature files from a copy, as this build's own dtest jar
# target does, and move the copy into place only once it is complete. (zip -d edits the archive in place: extracting
# it is not an option on a case-insensitive file system, where META-INF/LICENSE clashes with META-INF/license/.)
TMP=$ROOT/build/hcd12-dtest-jar.tmp
trap 'rm -f "$TMP"' EXIT
cp "$JAR" "$TMP"
status=0
zip -q -d "$TMP" 'META-INF/*.SF' 'META-INF/*.RSA' 'META-INF/*.DSA' 'META-INF/*.EC' || status=$?
# 12: nothing to delete, the jar was not signed
if [ "$status" -ne 0 ] && [ "$status" -ne 12 ]; then
  echo "zip -d failed with status $status" >&2
  exit 1
fi
mv -f "$TMP" "$ROOT/build/$NAME"
printf 'ref=%s\ncommit=%s\njar=%s\n' "$REF" "$COMMIT" "$NAME" > "$ROOT/build/hcd12-dtest-jar.source"
echo "==> $NAME (from $COMMIT) written to $ROOT/build/"
