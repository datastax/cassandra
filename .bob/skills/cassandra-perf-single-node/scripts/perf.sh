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
# Single-node NoSQLBench performance run of one build of this repository (main-5.0 line).
#
# usage:
#   perf.sh check                                   check the prerequisites (JDKs, nb5, cqlsh python, ports, ant)
#   perf.sh setup <variant> <branch|sha>            git worktree at $PERF_WORKDIR/builds/<variant>, ant jar (JDK 17)
#   perf.sh run <variant> <label> [options]         run the selected workloads, results in results/<label>/
#       --workloads all|kv,lwt,sai,vector           default: kv,lwt,sai (vector only on request)
#       --steps <wl>=<step>[+<step>...],...         override the main steps of a workload, e.g. kv=main-read+main-write
#       --size small|medium|real                    data and op counts (default small), see SIZES below
#       --threads auto|<n>                          nb5 threads of the main steps (default auto)
#       --rate <ops/s>                              throttle the main steps (default: unthrottled)
#       --profile <file.yaml>                       cassandra.yaml overrides (top-level keys replace the stock ones)
#   perf.sh summary <label> [--baseline <label>]    markdown summary of a run, optionally compared with another run
#   perf.sh stop                                    stop the test node
#
# Every workload runs on a fresh node (empty data directory): schema, rampup, flush, settle, then each main step
# followed by a settle (pending compactions == 0, at most $SETTLE_TIMEOUT s).
#
# Environment (defaults in brackets):
#   PERF_WORKDIR [$HOME/cassandra-perf]   builds/, node/, results/ (outside the repository)
#   JAVA17_HOME, NB5_JAR [required], NB5_JAVA_HOME (JDK 25), CQLSH_PYTHON: as in chunk-cache-stress-compare
#   MEMTABLE [trie_stage2]   memtable configuration the default memtable inherits from ('' keeps the stock one)
#   HEAP [8G]  KEYCOUNT / MAIN_CYCLES override the size  SETTLE_TIMEOUT [600]  JVM_OPTS_EXTRA ['']
#   PERF_NATIVE_PORT [9642]  PERF_STORAGE_PORT [7600]  PERF_SSL_STORAGE_PORT [7601]  PERF_JMX_PORT [7699]
set -euo pipefail
H="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SKILL="$(dirname "$H")"
ROOT="${PERF_WORKDIR:-$HOME/cassandra-perf}"
REPO="${PERF_REPO:-$(git -C "$SKILL" rev-parse --show-toplevel)}"

first_dir() { local d; for d in "$@"; do [ -x "$d/bin/java" ] && { echo "$d"; return; }; done; echo ""; }
J17="${JAVA17_HOME:-$(first_dir "$HOME"/.sdkman/candidates/java/17* /usr/lib/jvm/*17*)}"
JNB="${NB5_JAVA_HOME:-$(first_dir "$HOME"/.sdkman/candidates/java/25* /usr/lib/jvm/*25*)}"
NB5_JAR="${NB5_JAR:-}"
CQLSH_PY="${CQLSH_PYTHON:-$(command -v python3.12 || command -v python3)}"
NATIVE_PORT=${PERF_NATIVE_PORT:-9642} STORAGE_PORT=${PERF_STORAGE_PORT:-7600}
SSL_STORAGE_PORT=${PERF_SSL_STORAGE_PORT:-7601} JMX_PORT=${PERF_JMX_PORT:-7699}
MEMTABLE="${MEMTABLE-trie_stage2}"
HEAP="${HEAP:-8G}"
SETTLE_TIMEOUT=${SETTLE_TIMEOUT:-600}
BUILDS="$ROOT/builds" NODE="$ROOT/node" RESULTS="$ROOT/results"

# SIZES: <keycount> <main cycles per step>
size_of() {
    case "$1" in
        small) echo "200000 300000" ;;
        medium) echo "1000000 1000000" ;;
        real) echo "2000000 3000000" ;;
        *) die "unknown size $1" ;;
    esac
}
ALL_WORKLOADS="kv lwt sai vector"
DEFAULT_WORKLOADS="kv lwt sai"
wl_file() { case "$1" in kv) echo uct_keyvalue.yaml ;; lwt) echo uct_lwt.yaml ;; sai) echo uct_sai.yaml ;; vector) echo uct_vector.yaml ;; *) die "unknown workload $1" ;; esac; }
default_steps() {
    case "$1" in
        kv) echo "main-read main-write main-mixed" ;;
        lwt) echo "main main-read" ;;
        sai) echo "main-eq main-range main-write main-mixed" ;;
        vector) echo "main-ann main-write" ;;
    esac
}

log() { echo "[$(date +%T)] $*" >&2; }
die() { echo "ERROR: $*" >&2; exit 1; }

cmd_check() {
    local ok=0 p
    [ -n "$J17" ] && "$J17/bin/java" -version 2>&1 | grep -q 'version "17' && echo "JDK 17: $J17" || { echo "missing JDK 17 (set JAVA17_HOME)"; ok=1; }
    [ -n "$NB5_JAR" ] && [ -f "$NB5_JAR" ] && echo "nb5: $NB5_JAR" || { echo "missing nb5 jar (set NB5_JAR)"; ok=1; }
    [ -n "$JNB" ] && echo "nb5 JDK: $JNB" || { echo "missing JDK for nb5 (set NB5_JAVA_HOME)"; ok=1; }
    [ -n "$CQLSH_PY" ] && echo "cqlsh python: $CQLSH_PY ($("$CQLSH_PY" --version 2>&1))" || { echo "missing python for cqlsh"; ok=1; }
    for p in $NATIVE_PORT $STORAGE_PORT $SSL_STORAGE_PORT $JMX_PORT; do
        if ss -ltn 2>/dev/null | grep -q ":$p "; then echo "port $p is in use"; ok=1; fi
    done
    command -v ant >/dev/null && echo "ant: $(ant -version 2>&1 | head -1)" || { echo "missing ant"; ok=1; }
    echo "cpu: $(nproc) threads, load average: $(cut -d' ' -f1-3 /proc/loadavg)"
    echo "workdir: $ROOT (free: $(df -h "$(dirname "$ROOT")" | awk 'NR==2{print $4}'))"
    return $ok
}

cmd_setup() {   # <variant> <branch|sha>
    local variant="$1" ref="$2" dir="$BUILDS/$1" sha
    mkdir -p "$BUILDS"
    sha=$(git -C "$REPO" rev-parse --verify --quiet "$ref^{commit}" || true)
    if [ -z "$sha" ]; then
        log "fetching $ref from origin"
        git -C "$REPO" fetch -q origin "$ref" && sha=$(git -C "$REPO" rev-parse FETCH_HEAD)
    fi
    [ -n "$sha" ] || die "cannot resolve $ref"
    # the build must be on the main-5.0 line
    git -C "$REPO" show "$sha:build.xml" | grep -qE '<property name="base.version" value="5\.0\.[0-9]+\.[0-9]+"' \
        || die "$ref is not on the main-5.0 line (build.xml base.version is not 5.0.N.N, the DataStax/IBM fork numbering)"
    local mb behind=""
    if git -C "$REPO" rev-parse --verify --quiet origin/main-5.0 >/dev/null; then
        mb=$(git -C "$REPO" merge-base "$sha" origin/main-5.0 || true)
        [ -n "$mb" ] || die "$ref has no common history with origin/main-5.0"
        behind=$(git -C "$REPO" rev-list --count "$mb..origin/main-5.0")
    fi
    if [ -d "$dir" ]; then
        [ "$(git -C "$dir" rev-parse HEAD)" = "$sha" ] || die "$dir exists at another commit; use another variant name or 'git worktree remove $dir'"
    else
        git -C "$REPO" worktree add --detach "$dir" "$sha"
    fi
    log "building $variant at ${sha:0:11} with $J17"
    (cd "$dir" && JAVA_HOME="$J17" PATH="$J17/bin:$PATH" ant -q jar) > "$BUILDS/$variant.build.log" 2>&1 || die "build failed, see $BUILDS/$variant.build.log"
    {
        echo "ref=$ref"
        echo "commit=$sha"
        echo "subject=$(git -C "$dir" log -1 --format=%s)"
        echo "merge_base_main_5_0=${mb:-unknown}"
        echo "commits_behind_main_5_0=${behind:-unknown}"
    } > "$BUILDS/$variant.commit"
    cat "$BUILDS/$variant.commit"
}

make_conf() {   # <variant> <confdir> [profile]
    local dst="$2" extra="${3:-}" k
    rm -rf "$dst"; cp -a "$BUILDS/$1/conf" "$dst"
    sed -i \
        -e "s/^cluster_name: .*/cluster_name: 'perf'/" \
        -e "s/127.0.0.1:7000/127.0.0.1:$STORAGE_PORT/" \
        -e "s/^storage_port: .*/storage_port: $STORAGE_PORT/" \
        -e "s/^ssl_storage_port: .*/ssl_storage_port: $SSL_STORAGE_PORT/" \
        -e "s/^native_transport_port: .*/native_transport_port: $NATIVE_PORT/" \
        "$dst/cassandra.yaml"
    if [ -n "$MEMTABLE" ]; then
        grep -q "^    $MEMTABLE:" "$dst/cassandra.yaml" || die "no memtable configuration '$MEMTABLE' in this build's cassandra.yaml"
        sed -i -e "/^    default:\$/{n;s/^      inherits: .*/      inherits: $MEMTABLE/}" "$dst/cassandra.yaml"
        grep -A1 '^    default:$' "$dst/cassandra.yaml" | grep -q "inherits: $MEMTABLE" || die "memtable pin failed"
    fi
    if [ -n "$extra" ]; then
        for k in $(grep -oE '^[a-z_]+:' "$extra" | tr -d ':'); do sed -i -e "/^$k:/d" "$dst/cassandra.yaml"; done
        { echo ""; echo "# --- profile $(basename "$extra") ---"; cat "$extra"; } >> "$dst/cassandra.yaml"
    fi
    sed -i -e "s/^JMX_PORT=.*/JMX_PORT=\"$JMX_PORT\"/" "$dst/cassandra-env.sh"
}

start_node() {   # <variant> <confdir> <logdir>
    local variant="$1" conf="$2" logs="$3"
    mkdir -p "$logs"
    export JAVA_HOME=$J17 CASSANDRA_CONF="$conf" CASSANDRA_LOG_DIR="$logs" MAX_HEAP_SIZE=$HEAP HEAP_NEWSIZE=800M
    export CASSANDRA_HOME="$BUILDS/$variant"
    sed -e "s#^cassandra_storagedir=.*#cassandra_storagedir=\"$NODE\"#" "$BUILDS/$variant/bin/cassandra.in.sh" > "$conf/cassandra.in.sh"
    export CASSANDRA_INCLUDE="$conf/cassandra.in.sh"
    export JVM_EXTRA_OPTS="${JVM_OPTS_EXTRA:-}"
    "$BUILDS/$variant/bin/cassandra" -p "$ROOT/node.pid" > "$logs/startup.out" 2>&1
    for _ in $(seq 1 120); do
        if cql "SELECT release_version FROM system.local" >/dev/null 2>&1; then log "node up ($variant)"; return 0; fi
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

settle() {   # wait for pending compactions == 0; prints the seconds waited
    local t0=$SECONDS pending
    nt flush uct_perf >/dev/null 2>&1 || true
    while [ $((SECONDS - t0)) -lt "$SETTLE_TIMEOUT" ]; do
        pending=$(nt compactionstats 2>/dev/null | grep -i 'pending tasks' | grep -oE '[0-9]+' | head -1 || true)
        [ "${pending:-1}" = 0 ] && break
        sleep 5
    done
    echo $((SECONDS - t0))
}

nb5_step() {   # <workload> <block> <out dir> [params...]
    local wl="$1" block="$2" out="$3"; shift 3
    local alias errors start end code
    alias=$(echo "$block" | tr -c 'A-Za-z0-9_\n' '_')
    errors='counter,warn'
    [ "$wl" = lwt ] && errors='ChangeUnappliedCycleException:counter;.*:counter,warn'
    mkdir -p "$out/csv" "$out/logs"
    local cmd=("$JNB/bin/java" -jar "$NB5_JAR" run driver=cqld4 workload="$SKILL/workloads/$(wl_file "$wl")"
               tags=block:"$block" alias="$alias" hosts=127.0.0.1 port="$NATIVE_PORT" localdc=datacenter1 dc=datacenter1 rf=1
               errors="$errors" "$@"
               --report-csv-to "$out/csv" --report-summary-to "$out/summary.txt" --log-histograms "$out/histograms.hdr"
               --logs-dir "$out/logs" --progress console:30s)
    printf '%q ' "${cmd[@]}" > "$out/cmdline.txt"
    start=$(date +%s)
    set +e; "${cmd[@]}" > "$out/nb5.out" 2>&1; code=$?; set -e
    end=$(date +%s)
    printf '{"workload": "%s", "step": "%s", "start_epoch": %s, "end_epoch": %s, "duration_s": %s, "exit_code": %s}\n' \
        "$wl" "$block" "$start" "$end" $((end - start)) "$code" > "$out/run.json"
    log "$wl/$block exit=$code $((end - start)) s"
}

cmd_run() {   # <variant> <label> [options]
    local variant="$1" label="$2"; shift 2
    local workloads="$DEFAULT_WORKLOADS" size=small threads=auto rate="" profile="" steps_override=""
    while [ $# -gt 0 ]; do
        case "$1" in
            --workloads) workloads="$2"; shift 2 ;;
            --steps) steps_override="$2"; shift 2 ;;
            --size) size="$2"; shift 2 ;;
            --threads) threads="$2"; shift 2 ;;
            --rate) rate="$2"; shift 2 ;;
            --profile) profile="$2"; shift 2 ;;
            *) die "unknown option $1" ;;
        esac
    done
    [ "$workloads" = all ] && workloads="$ALL_WORKLOADS"
    workloads=$(echo "$workloads" | tr ',' ' ')
    [ -d "$BUILDS/$variant" ] || die "unknown variant $variant, run setup first"
    [ -z "$profile" ] || [ -f "$profile" ] || die "profile $profile not found"
    read -r keycount cycles <<< "$(size_of "$size")"
    keycount=${KEYCOUNT:-$keycount}; cycles=${MAIN_CYCLES:-$cycles}
    VARIANT=$variant
    local R="$RESULTS/$label" wl st steps
    [ -e "$R" ] && die "$R exists; use another label"
    mkdir -p "$R"
    {
        cat "$BUILDS/$variant.commit"
        echo "variant=$variant"
        echo "jdk=$("$J17/bin/java" -version 2>&1 | sed -n 2p)"
        echo "nb5=$(unzip -p "$NB5_JAR" META-INF/maven/io.nosqlbench/nb5/pom.properties 2>/dev/null | grep ^version= | cut -d= -f2)"
        echo "nb5_jdk=$("$JNB/bin/java" -version 2>&1 | sed -n 2p)"
        echo "workloads=$workloads size=$size keycount=$keycount main_cycles=$cycles threads=$threads rate=${rate:-unthrottled}"
        echo "memtable=${MEMTABLE:-stock} heap=$HEAP jvm_extra=${JVM_OPTS_EXTRA:-}"
        echo "host=$(hostname) cpus=$(nproc) mem=$(free -g | awk '/Mem:/{print $2}')G kernel=$(uname -r) loadavg_at_start=$(cut -d' ' -f1-3 /proc/loadavg)"
        if [ -n "$profile" ]; then echo "profile $profile:"; cat "$profile"; fi
    } > "$R/params.txt"
    trap 'stop_node' EXIT
    for wl in $workloads; do
        wl_file "$wl" >/dev/null
        steps=$(default_steps "$wl")
        local o; for o in $(echo "$steps_override" | tr ',' ' '); do
            [ "${o%%=*}" = "$wl" ] && steps=$(echo "${o#*=}" | tr '+' ' ')
        done
        log "=== $label/$wl: rampup of $keycount rows, steps: $steps"
        stop_node; rm -rf "$NODE"; mkdir -p "$NODE"
        make_conf "$variant" "$R/$wl/conf" "$profile"
        start_node "$variant" "$R/$wl/conf" "$R/$wl/server-logs"
        "$J17/bin/jcmd" "$(cat "$ROOT/node.pid")" GC.class_histogram 2>/dev/null | grep -E "Memtable[A-Za-z0-9]*$" > "$R/$wl/memtable-classes.out" || true
        nb5_step "$wl" schema "$R/$wl/schema" threads=1 keycount="$keycount"
        nb5_step "$wl" rampup "$R/$wl/rampup" cycles="$keycount" threads="$threads" keycount="$keycount"
        echo "rampup settle_s=$(settle)" >> "$R/$wl/settle.txt"
        for st in $steps; do
            local extra=(cycles="$cycles" threads="$threads" keycount="$keycount")
            [ -n "$rate" ] && extra+=(cyclerate="$rate")
            nb5_step "$wl" "$st" "$R/$wl/$st" "${extra[@]}"
            echo "$st settle_s=$(settle)" >> "$R/$wl/settle.txt"
        done
        nt tablestats uct_perf > "$R/$wl/tablestats.out" 2>&1 || true
        nt compactionhistory > "$R/$wl/compactionhistory.out" 2>&1 || true
        stop_node
        grep -c '^ERROR' "$R/$wl/server-logs/system.log" > "$R/$wl/server-errors.count" || true
    done
    trap - EXIT
    python3 -I "$H/perf_summary.py" "$R" | tee "$R/SUMMARY.md"
}

case "${1:-}" in
    check) cmd_check ;;
    setup) shift; [ $# -eq 2 ] || die "usage: setup <variant> <branch|sha>"; cmd_setup "$@" ;;
    run) shift; [ $# -ge 2 ] || die "usage: run <variant> <label> [options]"; mkdir -p "$RESULTS"; cmd_run "$@" ;;
    summary) shift; [ $# -ge 1 ] || die "usage: summary <label> [--baseline <label>]"
             if [ "${2:-}" = --baseline ]; then python3 -I "$H/perf_summary.py" "$RESULTS/$1" --baseline "$RESULTS/$3"
             else python3 -I "$H/perf_summary.py" "$RESULTS/$1"; fi ;;
    stop) stop_node ;;
    *) sed -n '17,41p' "$0"; exit 64 ;;
esac
