---
name: cassandra-perf-single-node
description: Run a NoSQLBench performance test of one branch or commit of this Cassandra fork (main-5.0 line, HCD 2.0) on the local machine with a single node, for all workloads (key/value, LWT, SAI, vector) or a selection of them, and report throughput, latency percentiles and errors, optionally compared with another branch. Use when asked to benchmark, measure performance, run perf or load tests, or compare the throughput or latency of two branches.
---

<!--
Copyright IBM Corp.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Single-node performance test (main-5.0 line)

This skill builds one branch or commit of this repository and starts a single local node. On that node it runs NoSQLBench 5 workloads and writes a summary of throughput, latency and errors for each workload and step. Two runs (for example a branch and `main-5.0`) can be compared with `summary --baseline`.

**Only branches of the main-5.0 line are accepted** (HCD 2.0, `build.xml` `base.version` 5.0.N.N). `setup` refuses anything else, for example upstream Apache Cassandra 5.0, whose version is 5.0.N.

## Files in this skill

| file | purpose |
|---|---|
| `scripts/perf.sh` | the harness: `check`, `setup`, `run`, `summary`, `stop` |
| `scripts/perf_summary.py` | markdown table from the nb5 CSV metrics; with `--baseline`, adds the deltas |
| `workloads/uct_keyvalue.yaml` | `kv`: `kv (key text PRIMARY KEY, value text)`, values of about 100 bytes |
| `workloads/uct_lwt.yaml` | `lwt`: `INSERT … IF NOT EXISTS` rampup; `main` = 60 % applied CAS, 20 % rejected CAS, 20 % `SELECT` at `LOCAL_SERIAL` |
| `workloads/uct_sai.yaml` | `sai`: `sai (pk int, ck int, name text, score int)` with SAI indexes on `name` and `score`; equality, range and write ops |
| `workloads/uct_vector.yaml` | `vector`: `vec (id int PRIMARY KEY, v vector<float, 64>)` with an SAI cosine index; ANN queries and writes |

The workloads come from the cassandra-upgrade-tester project. They are run with `rf=1` and `dc=datacenter1`.

## Workloads and steps

| workload | steps run by default, in order (each after `schema` and `rampup`) |
|---|---|
| `kv` | `main-read`, `main-write`, `main-mixed` (50/50) |
| `lwt` | `main`, `main-read` (serial selects) |
| `sai` | `main-eq`, `main-range`, `main-write`, `main-mixed` (eq 40 / range 40 / write 20) |
| `vector` | `main-ann`, `main-write` |

**Selection:**
- the default is `--workloads kv,lwt,sai`;
- `--workloads all` adds `vector`;
- `--workloads sai` runs one workload;
- `--steps kv=main-read+main-write,sai=main-eq` overrides the steps of a workload.

**Sizes** (`--size`): `small` (200k rows, 300k ops per step, the default), `medium` (1M rows, 1M ops), `real` (2M rows, 3M ops). `KEYCOUNT` and `MAIN_CYCLES` override them.

**Each workload runs on a fresh node** (empty data directory). The sequence is:
1. `schema`, then `rampup` (one write per key);
2. a flush, then a wait until pending compactions reach 0 (at most `SETTLE_TIMEOUT`, 600 s);
3. each main step, each followed by the same settle.

## Rules

- **JDK:** build and run Cassandra with **JDK 17**. NoSQLBench 5.25 needs **JDK 25**; it is used only for the load generator.
- **Memtable:** pin it to **TrieMemtableStage2** (`MEMTABLE=trie_stage2`, the default of this skill), because the stock default is not the one we measure. Use another value only when asked, and write it in the report.
- **Single node:** one node on the local machine, on ports CQL 9642, storage 7600/7601, JMX 7699. These differ from the chunk-cache-stress-compare skill, but don't run both skills at the same time: they compete for CPU and disk.
- **Work directory:** everything goes under `$PERF_WORKDIR` (default `~/cassandra-perf`), never inside the repository.
- **Quiet host:** no builds, tests or other benchmarks during a run. Check the load average (`perf.sh check` prints it) and record what else runs on the host.
- **Comparing two branches:** run both with the same `--size`, `--threads`, profile and environment, and interleave the runs (A, B, A, B). Use at least 2 runs per branch. Report every run and the spread between runs of the same branch. A difference smaller than that spread is noise, and must be reported as such.
- **Evidence only:** report the numbers as measured; don't explain a difference unless the evidence shows the cause.
- **Git:** don't commit or push anything as part of this skill.

## Procedure

### 1. Prerequisites
```
export PERF_WORKDIR=$HOME/cassandra-perf
export NB5_JAR=/path/to/nb5.jar        # NoSQLBench 5.21 or later
S=.bob/skills/cassandra-perf-single-node/scripts/perf.sh
$S check
```
Fix everything `check` reports. Set `JAVA17_HOME`, `NB5_JAVA_HOME` and `CQLSH_PYTHON` (Python 3.12 or older) if they aren't found automatically.

### 2. Build the branch
```
$S setup feature <branch-or-sha>       # e.g. $S setup feature origin/my-branch
$S setup base origin/main-5.0          # only when comparing
```
`setup` does the following:
- resolves the ref, fetching it from `origin` if it isn't present locally;
- checks that it is on the main-5.0 line;
- creates a detached worktree under `$PERF_WORKDIR/builds/<variant>` and runs `ant jar` with JDK 17;
- writes `$PERF_WORKDIR/builds/<variant>.commit` with the full SHA, the subject, the merge base with `origin/main-5.0`, and how many commits it is behind.

For a pull request of `datastax/cassandra`, use the SHA from `git fetch origin pull/<N>/head && git rev-parse FETCH_HEAD`. Don't use `gh`: it is blocked by SAML on that organization.

### 3. Run
```
$S run feature feature-r1 --workloads all --size small
$S run feature feature-kv-r1 --workloads kv --steps kv=main-read --size medium --threads 64
```
`run <variant> <label> [--workloads …] [--steps …] [--size …] [--threads auto|N] [--rate ops/s] [--profile file.yaml]`
- `--threads auto` is the nb5 default: 10 × the cores of the machine running nb5.
- `--rate` throttles the main steps; use it to compare latency at the same throughput.
- `--profile` points to a `cassandra.yaml` override file; its top-level keys replace the stock ones.

A `small` run of the three default workloads takes about 15–30 minutes. Run it in the background and wait for it to finish; don't poll in a tight loop.

**Results** go to `$PERF_WORKDIR/results/<label>/`:
- `params.txt`;
- `SUMMARY.md`;
- per workload: `conf/`, `server-logs/`, `memtable-classes.out`, `settle.txt`, `tablestats.out`, `compactionhistory.out` and `server-errors.count`;
- per step: `cmdline.txt`, `run.json`, `nb5.out`, `summary.txt`, `csv/` and `histograms.hdr`.

### 4. Check the run before using the numbers
- **Exit codes:** every step has `exit_code` 0 in `run.json`. A non-zero exit or a missing `csv/` means the step didn't run properly; rerun it, don't report it as a result.
- **Memtable:** `memtable-classes.out` lists `TrieMemtableStage2`.
- **Client errors:** `errors` in the summary is 0, or explained. For `lwt`, conditions that were not applied are counted separately; they're expected.
- **Server errors:** `server-errors.count` is 0. Otherwise look at `server-logs/system.log` and report each distinct error with its stack trace.
- **Settling:** `settle.txt` shows that every settle ended before `SETTLE_TIMEOUT`. A step that started while compactions were still pending must be flagged.

### 5. Summary and comparison
```
$S summary feature-r1                       # the table again
$S summary feature-r1 --baseline base-r1    # with deltas (ops/s, p50, p99, p99.9) against the baseline
```

### 6. Report
Write `$PERF_WORKDIR/REPORT-<label>.md` with:
1. **Versions:** the full SHAs from `builds/<variant>.commit` (and the merge base with `origin/main-5.0`), the JDK, the NoSQLBench version, and the host (CPU, RAM, kernel, load average, other activity), all from `params.txt`.
2. **Configuration:** the memtable pin, the heap, the profile overrides and the `JVM_OPTS_EXTRA`.
3. **Workloads:** the workloads and steps run, size, threads, rate; and the nb5 command line of one step, from `cmdline.txt`.
4. **Results:** the `SUMMARY.md` table of every run. When comparing, give the per-run tables and then the comparison table, with the run-to-run spread.
5. **Errors:** client and server errors, each with a stack trace or error message.
6. **Conclusions:** only differences larger than the run-to-run spread, with the numbers.

To share the report, zip it with the workloads and, for each run, `params.txt`, `SUMMARY.md` and the per-step `summary.txt`, `run.json` and `csv/`. Leave out the server logs unless they're asked for.

### 7. Clean up
```
$S stop
```
Keep the builds for later runs. Remove a worktree only when asked: `git worktree remove $PERF_WORKDIR/builds/<variant>`.
