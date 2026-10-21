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
# Single-node chunk cache stress test: compares two builds of this repository under a point-read load that
# misses the chunk cache, with a major compaction and an SAI initial index build started during the reads.
#
# usage:
#   ccblast.sh check                              check the prerequisites (JDKs, nb5, cqlsh python, ports)
#   ccblast.sh setup <variant> <git-ref>          git worktree of <git-ref> at $CCB_WORKDIR/builds/<variant>, ant jar
#   ccblast.sh load <variant>                     load the golden data set once with <variant>, drain, keep a copy
#   ccblast.sh run <variant> <label> <threads> [profile.yaml]
#                                                 restore the golden data, run one test, results in results/<label>/
#   ccblast.sh compare <base> <candidate> <threads> <profile.yaml|-> <repeats> <tag>
#                                                 <repeats> runs per variant, interleaved candidate/base
#   ccblast.sh catalogue <label>                  group the ERROR/WARN entries of a run, one stack trace per group
#   ccblast.sh summarize <label>                  print the summary of a run again
#   ccblast.sh stop                               stop the test node
#
# Environment (defaults in brackets):
#   CCB_WORKDIR [$HOME/chunkcache-blast]   builds/, golden/, node/, results/ live here (outside the repository)
#   JAVA17_HOME [first ~/.sdkman/candidates/java/17*, then /usr/lib/jvm/*17*]   builds and runs Cassandra
#   NB5_JAR [required]                      NoSQLBench 5 jar (5.21+)
#   NB5_JAVA_HOME [first ~/.sdkman/candidates/java/25*]   JDK that runs nb5 (5.25.x needs Java 25)
#   CQLSH_PYTHON [python3.12 if present]    Python for cqlsh (3.14 is not supported by the bundled cqlsh)
#   FILE_CACHE_MB [128]  KEYCOUNT [2000000]  MAIN_CYCLES [6000000]  JVM_OPTS_EXTRA ['']
#   CCB_NATIVE_PORT [9542]  CCB_STORAGE_PORT [7500]  CCB_SSL_STORAGE_PORT [7501]  CCB_JMX_PORT [7599]
set -euo pipefail
H="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SKILL="$(dirname "$H")"
WORKLOAD="$SKILL/workload/ccblast.yaml"
ROOT="${CCB_WORKDIR:-$HOME/chunkcache-blast}"
REPO="${CCB_REPO:-$(git -C "$SKILL" rev-parse --show-toplevel)}"

first_dir() { local d; for d in "$@"; do [ -x "$d/bin/java" ] && { echo "$d"; return; }; done; echo ""; }
J17="${JAVA17_HOME:-$(first_dir "$HOME"/.sdkman/candidates/java/17* /usr/lib/jvm/*17*)}"
JNB="${NB5_JAVA_HOME:-$(first_dir "$HOME"/.sdkman/candidates/java/25* /usr/lib/jvm/*25*)}"
NB5_JAR="${NB5_JAR:-}"
CQLSH_PY="${CQLSH_PYTHON:-$(command -v python3.12 || command -v python3)}"
NATIVE_PORT=${CCB_NATIVE_PORT:-9542} STORAGE_PORT=${CCB_STORAGE_PORT:-7500}
SSL_STORAGE_PORT=${CCB_SSL_STORAGE_PORT:-7501} JMX_PORT=${CCB_JMX_PORT:-7599}
FILE_CACHE_MB=${FILE_CACHE_MB:-128}
KEYCOUNT=${KEYCOUNT:-2000000}
MAIN_CYCLES=${MAIN_CYCLES:-6000000}
GOLDEN="$ROOT/golden"
NODE="$ROOT/node"
BUILDS="$ROOT/builds"
RESULTS="$ROOT/results"

log() { echo "[$(date +%T)] $*" >&2; }
die() { echo "ERROR: $*" >&2; exit 1; }

cmd_check() {
    local ok=0
    [ -n "$J17" ] && "$J17/bin/java" -version 2>&1 | grep -q 'version "17' && echo "JDK 17: $J17" || { echo "missing JDK 17 (set JAVA17_HOME)"; ok=1; }
    [ -n "$NB5_JAR" ] && [ -f "$NB5_JAR" ] && echo "nb5: $NB5_JAR" || { echo "missing nb5 jar (set NB5_JAR)"; ok=1; }
    [ -n "$JNB" ] && echo "nb5 JDK: $JNB" || { echo "missing JDK for nb5 (set NB5_JAVA_HOME)"; ok=1; }
    [ -n "$CQLSH_PY" ] && echo "cqlsh python: $CQLSH_PY ($("$CQLSH_PY" --version 2>&1))" || { echo "missing python for cqlsh"; ok=1; }
    local p
    for p in $NATIVE_PORT $STORAGE_PORT $SSL_STORAGE_PORT $JMX_PORT; do
        if ss -ltn 2>/dev/null | grep -q ":$p "; then echo "port $p is in use"; ok=1; fi
    done
    command -v ant >/dev/null && echo "ant: $(ant -version 2>&1 | head -1)" || { echo "missing ant"; ok=1; }
    echo "workdir: $ROOT (free: $(df -h "$(dirname "$ROOT")" | awk 'NR==2{print $4}'))"
    return $ok
}

cmd_setup() {   # <variant> <git-ref>
    local variant="$1" ref="$2" dir="$BUILDS/$1"
    mkdir -p "$BUILDS"
    if [ -d "$dir" ]; then
        [ "$(git -C "$dir" rev-parse HEAD)" = "$(git -C "$REPO" rev-parse "$ref^{commit}")" ] || die "$dir exists at another commit; remove it with 'git worktree remove $dir'"
    else
        git -C "$REPO" worktree add --detach "$dir" "$ref"
    fi
    log "building $variant at $(git -C "$dir" rev-parse --short HEAD) with $J17"
    (cd "$dir" && JAVA_HOME="$J17" PATH="$J17/bin:$PATH" ant -q jar) > "$BUILDS/$variant.build.log" 2>&1 || die "build failed, see $BUILDS/$variant.build.log"
    git -C "$dir" log -1 --format='%H %s' > "$BUILDS/$variant.commit"
    cat "$BUILDS/$variant.commit"
}

make_conf() {   # <variant> <confdir> [profile yaml]
    local src="$BUILDS/$1/conf" dst="$2" extra="${3:-}"
    rm -rf "$dst"; cp -a "$src" "$dst"
    sed -i \
        -e "s/^cluster_name: .*/cluster_name: 'ccblast'/" \
        -e "s/127.0.0.1:7000/127.0.0.1:$STORAGE_PORT/" \
        -e "s/^storage_port: .*/storage_port: $STORAGE_PORT/" \
        -e "s/^ssl_storage_port: .*/ssl_storage_port: $SSL_STORAGE_PORT/" \
        -e "s/^native_transport_port: .*/native_transport_port: $NATIVE_PORT/" \
        "$dst/cassandra.yaml"
    # pin the default memtable to TrieMemtableStage2, whatever the stock default is
    sed -i -e '/^    default:$/{n;s/^      inherits: .*/      inherits: trie_stage2/}' "$dst/cassandra.yaml"
    grep -A1 '^    default:$' "$dst/cassandra.yaml" | grep -q "inherits: trie_stage2" || die "memtable pin failed"
    grep -q '^    trie_stage2:' "$dst/cassandra.yaml" || die "no trie_stage2 memtable configuration in this build's cassandra.yaml"
    # top-level keys of the profile replace the stock ones (no duplicate keys)
    if [ -n "$extra" ]; then
        local k
        for k in $(grep -oE '^[a-z_]+:' "$extra" | tr -d ':'); do sed -i -e "/^$k:/d" "$dst/cassandra.yaml"; done
    fi
    {
        echo ""
        echo "# --- ccblast overrides ---"
        echo "file_cache_enabled: true"
        echo "file_cache_size: ${FILE_CACHE_MB}MiB"
        [ -n "$extra" ] && cat "$extra"
    } >> "$dst/cassandra.yaml"
    sed -i -e "s/^JMX_PORT=.*/JMX_PORT=\"$JMX_PORT\"/" "$dst/cassandra-env.sh"
}

start_node() {   # <variant> <confdir> <logdir>
    local variant="$1" conf="$2" logs="$3"
    mkdir -p "$logs"
    export JAVA_HOME=$J17 CASSANDRA_CONF="$conf" CASSANDRA_LOG_DIR="$logs" MAX_HEAP_SIZE=4G HEAP_NEWSIZE=800M
    export CASSANDRA_HOME="$BUILDS/$variant"
    # bin/cassandra.in.sh hard-codes cassandra_storagedir=$CASSANDRA_HOME/data: use a copy pointing at $NODE
    sed -e "s#^cassandra_storagedir=.*#cassandra_storagedir=\"$NODE\"#" "$BUILDS/$variant/bin/cassandra.in.sh" > "$conf/cassandra.in.sh"
    export CASSANDRA_INCLUDE="$conf/cassandra.in.sh"
    export JVM_EXTRA_OPTS="${JVM_OPTS_EXTRA:-}"
    "$BUILDS/$variant/bin/cassandra" -p "$ROOT/node.pid" > "$logs/startup.out" 2>&1
    log "starting $variant (pid $(cat "$ROOT/node.pid" 2>/dev/null || echo ?))"
    for _ in $(seq 1 120); do
        if cql "SELECT release_version FROM system.local" >/dev/null 2>&1; then log "node up"; return 0; fi
        sleep 2
    done
    die "node did not start, see $logs"
}

stop_node() {
    [ -f "$ROOT/node.pid" ] || return 0
    local pid; pid=$(cat "$ROOT/node.pid")
    kill "$pid" 2>/dev/null || true
    for _ in $(seq 1 60); do kill -0 "$pid" 2>/dev/null || break; sleep 1; done
    kill -9 "$pid" 2>/dev/null || true
    rm -f "$ROOT/node.pid"
}

nt() { JAVA_HOME=$J17 "$BUILDS/$VARIANT/bin/nodetool" -p "$JMX_PORT" "$@"; }
cql() { CQLSH_PYTHON="$CQLSH_PY" JAVA_HOME=$J17 "$BUILDS/$VARIANT/bin/cqlsh" 127.0.0.1 "$NATIVE_PORT" -e "$1" 2>&1; }

nb5() {  # <results dir> <block> [params...]
    local out="$1" block="$2"; shift 2
    mkdir -p "$out"
    "$JNB/bin/java" -jar "$NB5_JAR" run driver=cqld4 workload="$WORKLOAD" tags=block:"$block" \
        hosts=127.0.0.1 port="$NATIVE_PORT" localdc=datacenter1 keycount="$KEYCOUNT" \
        errors='counter,warn' --report-summary-to "$out/summary.txt" --logs-dir "$out/logs" "$@" \
        > "$out/nb5.out" 2>&1
}

sample() {  # <out file>; one CSV line every 2 s until killed
    exec "$J17/bin/java" "$H/JmxSampler.java" "$JMX_PORT" "$(cat "$ROOT/node.pid")" > "$1" 2> "$1.err"
}

cmd_load() {   # <variant>
    VARIANT="$1"
    [ -d "$BUILDS/$VARIANT" ] || die "unknown variant $VARIANT, run setup first"
    stop_node; rm -rf "$NODE" "$GOLDEN"; mkdir -p "$NODE"
    make_conf "$VARIANT" "$ROOT/conf-load"
    start_node "$VARIANT" "$ROOT/conf-load" "$ROOT/logs-load"
    nb5 "$ROOT/logs-load/nb5-schema" schema threads=1
    log "rampup of $KEYCOUNT rows"
    nb5 "$ROOT/logs-load/nb5-rampup" rampup cycles="$KEYCOUNT" threads=auto
    nt flush ccblast
    nt drain
    stop_node
    cp -a "$NODE" "$GOLDEN"
    git -C "$BUILDS/$VARIANT" rev-parse HEAD > "$GOLDEN/loaded-by.commit"
    log "golden data: $(du -sh "$GOLDEN/data/ccblast" | cut -f1) in $(ls "$GOLDEN"/data/ccblast/*/ | grep -c 'Data.db') sstables"
}

cmd_run() {   # <variant> <label> <threads> [profile]
    local variant="$1" label="$2" threads="$3" extra="${4:-}"
    VARIANT=$variant
    [ -d "$BUILDS/$variant" ] || die "unknown variant $variant, run setup first"
    [ -d "$GOLDEN" ] || die "no golden data, run load first"
    [ -z "$extra" ] || [ "$extra" = "-" ] || [ -f "$extra" ] || die "profile $extra not found"
    [ "$extra" = "-" ] && extra=""
    local R="$RESULTS/$label"
    stop_node; rm -rf "$NODE" "$R"; mkdir -p "$R"
    cp -a "$GOLDEN" "$NODE"
    make_conf "$variant" "$R/conf" "$extra"
    {
        echo "variant=$variant commit=$(git -C "$BUILDS/$variant" rev-parse HEAD)"
        echo "file_cache_size=${FILE_CACHE_MB}MiB read_threads=$threads main_cycles=$MAIN_CYCLES keycount=$KEYCOUNT"
        echo "jdk=$("$J17/bin/java" -version 2>&1 | sed -n 2p)"
        echo "jvm_extra=${JVM_OPTS_EXTRA:-}"
        if [ -n "$extra" ]; then echo "profile $extra:"; cat "$extra"; fi
    } > "$R/params.txt"
    start_node "$variant" "$R/conf" "$R/logs"
    "$J17/bin/jcmd" "$(cat "$ROOT/node.pid")" GC.class_histogram 2>/dev/null | grep -E "Memtable[A-Za-z0-9]*$" > "$R/memtable-classes.out" || true
    sample "$R/samples.csv" & local sampler=$!
    local reader="" compactor=""
    # never leave the load generator, the sampler or the node behind, whatever happens below
    trap 'kill $sampler $reader $compactor 2>/dev/null || true; stop_node' EXIT

    log "main-read with $threads threads, $MAIN_CYCLES cycles"
    local t0=$SECONDS
    nb5 "$R/nb5-read" main-read cycles="$MAIN_CYCLES" threads="$threads" & reader=$!

    sleep 15
    log "major compaction"
    ( nt compact ccblast data > "$R/compact.out" 2>&1; echo "exit=$?" >> "$R/compact.out" ) & compactor=$!
    sleep 5
    log "SAI index build"
    # cqlsh itself can fail to connect while the node fails reads (it reads the system tables): retry, keep every attempt
    local attempt
    for attempt in $(seq 1 30); do
        if cql "CREATE CUSTOM INDEX IF NOT EXISTS data_tag ON ccblast.data (tag) USING 'StorageAttachedIndex'" >> "$R/create-index.out"; then
            echo "CREATE INDEX succeeded at attempt $attempt" >> "$R/create-index.out"; break
        fi
        echo "CREATE INDEX attempt $attempt failed at $(date +%T)" >> "$R/create-index.out"; sleep 2
    done

    wait $reader || true
    echo "read_phase_seconds=$((SECONDS - t0))" >> "$R/params.txt"
    log "reads done after $((SECONDS - t0)) s"
    wait $compactor || true
    # give the index build up to 120 s to finish
    for _ in $(seq 1 60); do
        grep -qE "data_tag\\] became queryable" "$R/logs/system.log" && break
        grep -qE "(index|Index).*data_tag.*(fail|Fail)" "$R/logs/system.log" && break
        sleep 2
    done
    kill $sampler 2>/dev/null || true

    cql "SELECT * FROM system_views.sai_column_indexes" > "$R/sai-status.out" || true
    cql "SELECT count(*) FROM ccblast.data WHERE tag = 7" > "$R/sai-query.out" || true
    nt info > "$R/info.out" 2>&1 || true
    nt compactionhistory > "$R/compactionhistory.out" 2>&1 || true
    ls "$NODE"/data/ccblast/*/ | grep -c 'Data.db' > "$R/sstables-after.out" || true
    stop_node
    trap - EXIT
    summarize "$R" > "$R/SUMMARY.txt"
    rm -f "$R/logs/all-system.log"
    cat "$R/SUMMARY.txt"
}

cmd_compare() {   # <base> <candidate> <threads> <profile|-> <repeats> <tag>
    local base="$1" cand="$2" threads="$3" profile="$4" repeats="$5" tag="$6" i v
    for i in $(seq 1 "$repeats"); do
        for v in "$cand" "$base"; do
            log "=== $v-$tag-r$i"
            ( cmd_run "$v" "$v-$tag-r$i" "$threads" "$profile" ) > "$RESULTS/$v-$tag-r$i.driver.log" 2>&1 || log "run $v-$tag-r$i exited with $?"
            grep -E 'client errors|BufferPoolExhaustedException lines|^ERROR threads|sstables after|^sync_reclaims|max pool' "$RESULTS/$v-$tag-r$i/SUMMARY.txt" 2>/dev/null || true
        done
    done
}

cmd_catalogue() {   # <label>
    local R="$RESULTS/$1"
    python3 -I "$H/error_catalogue.py" "$R" "$R/server-errors-with-stacktraces.txt" "$R/server-errors.tsv"
    grep 'error with cycle' "$R/nb5-read/nb5.out" | sed -E 's/.*errmsg: //' | sort | uniq -c > "$R/client-errors.txt" || true
    cut -f1-5,8 "$R/server-errors.tsv" | cut -c1-220
}

alllogs() {  # every system.log line of a run, including the rotated (zipped) files, in order
    local d="$1/logs" f
    for f in $(ls "$d"/system.log.*.zip 2>/dev/null | sort -t. -k3 -n); do unzip -p "$f"; done
    cat "$d/system.log"
}

summarize() {
    local R="$1" L="$1/logs/all-system.log"
    alllogs "$R" > "$L"
    cat "$R/params.txt"
    echo "--- client (nb5 main-read)"
    echo "client errors: $(grep -c 'error with cycle' "$R/nb5-read/nb5.out" || true)"
    grep 'error with cycle' "$R/nb5-read/nb5.out" | sed -E 's/.*errmsg: //' | sort | uniq -c | sort -rn | head -5 || true
    echo "--- server log"
    echo "BufferPoolExhaustedException lines: $(grep -c BufferPoolExhaustedException "$L" || true)"
    echo "OutOfMemoryError lines:             $(grep -c OutOfMemoryError "$L" || true)"
    echo "BufferPoolExhaustedException by thread: $(grep -B1 -E '^(java.lang.RuntimeException: )?org.apache.cassandra.utils.memory.BufferPoolExhaustedException' "$L" | grep -oE '^(ERROR|WARN) +\[[A-Za-z-]+' | sed -E 's/^(ERROR|WARN) +\[//' | sort | uniq -c | tr '\n' ';' || true)"
    echo "ERROR lines:                        $(grep -c '^ERROR' "$L" || true)"
    echo "ERROR threads: $(grep '^ERROR' "$L" | grep -oE '^ERROR +\[[A-Za-z-]+' | sed -E 's/^ERROR +\[//' | sort | uniq -c | tr '\n' ';' || true)"
    echo "--- compaction"
    cat "$R/compact.out"; echo "sstables after: $(cat "$R/sstables-after.out")"
    echo "log volume: $(du -sh "$R/logs" | cut -f1) on disk, $(ls "$R"/logs/system.log.*.zip 2>/dev/null | wc -l) rotated system.log files"
    echo "--- SAI"
    grep -E "data_tag|SecondaryIndexExecutor" "$L" | grep -vE "^\s+at " | grep -iE "Submitting|fail|queryable|Exception in thread" | sort -u | cut -c1-200 | tail -5 || true
    grep -A1 "Exception in thread Thread\[SecondaryIndexExecutor" "$L" | grep -vE "Exception in thread|^--" | cut -c1-120 | sort | uniq -c || true
    grep -c 'succeeded' "$R/create-index.out" | sed 's/^/CREATE INDEX succeeded: /' || true
    echo "--- peaks (samples.csv)"
    awk -F, 'NR==1 { for (i=1;i<=NF;i++) c[$i]=i; next }
             { for (k in c) if ($c[k] != "" && (!(k in m) || $c[k]+0 > m[k])) m[k]=$c[k]+0 }
             END { printf "max rss %.0f MiB, max pool used %.1f MiB, max pool overflow %.1f MiB, max direct %.0f MiB, max cache size %.1f MiB\n", m["rss_kb"]/1024, m["bp_used"]/1048576, m["bp_overflow"]/1048576, m["direct_used"]/1048576, m["cc_size"]/1048576;
                   printf "sync_reclaims %s, reclaim_retry_ok %s, blocked_on_pool %s, pool_wait_ok %s, pool_exhausted %s, cache misses %s, max threads %s\n", m["cc_sync_reclaims"], m["cc_reclaim_retry_ok"], m["cc_blocked"], m["cc_wait_ok"], m["cc_pool_exhausted"], m["cc_misses"], m["threads"] }' "$R/samples.csv"
}

case "${1:-}" in
    check) cmd_check ;;
    setup) shift; cmd_setup "$@" ;;
    load) shift; cmd_load "$@" ;;
    run) shift; [ $# -ge 3 ] || die "usage: run <variant> <label> <threads> [profile]"; mkdir -p "$RESULTS"; cmd_run "$@" ;;
    compare) shift; [ $# -eq 6 ] || die "usage: compare <base> <candidate> <threads> <profile|-> <repeats> <tag>"; mkdir -p "$RESULTS"; cmd_compare "$@" ;;
    catalogue) shift; cmd_catalogue "$1" ;;
    summarize) summarize "$RESULTS/$2" ;;
    stop) stop_node ;;
    *) sed -n '17,40p' "$0"; exit 64 ;;
esac
