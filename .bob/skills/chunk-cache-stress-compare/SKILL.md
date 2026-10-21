---
name: chunk-cache-stress-compare
description: Run a single-node chunk cache stress comparison between two commits of this Cassandra fork (for example a pull request head against its base) and write a report with the failures found. Use when asked to stress test, compare or find regressions in the chunk cache, the buffer pool, ChunkCache/BufferPool changes, file_cache_size, BufferPoolExhaustedException, or to check the blast radius of a read-path memory change on reads, compaction and SAI index builds.
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

# Chunk cache stress comparison

This skill compares two builds of this repository under the same load:
- a **base** build, usually the commit a pull request is based on;
- a **candidate** build, usually the pull request head.

**One test run:**
1. Start a single node with the chunk cache enabled (`file_cache_size: 128MiB` by default).
2. Run 6,000,000 uniformly random point reads with NoSQLBench, over a data set about 15× larger than the cache.
3. At T+15 s, start a **major compaction** (`nodetool compact`).
4. At T+20 s, create an **SAI index**, which starts the SAI initial build.
5. Collect the client errors, the server errors with stack traces, JMX samples of the buffer pool and chunk cache, and the outcome of the compaction and the index build.

The aim is to show where the two builds behave differently: failures that one build has and the other doesn't.

**A comparison where neither build fails tells you nothing.** In that case, change the parameters (step 5) until the load stresses the pool, and run both builds again with the new parameters.

## Files in this skill

| file | purpose |
|---|---|
| `scripts/ccblast.sh` | the harness: `check`, `setup`, `load`, `run`, `compare`, `catalogue`, `summarize`, `stop` |
| `scripts/JmxSampler.java` | JMX sampler, started by `run`: one CSV row every 2 s (`samples.csv`) |
| `scripts/error_catalogue.py` | groups the ERROR/WARN entries of `system.log` (rotated files included) and keeps one full stack trace per group |
| `workload/ccblast.yaml` | the NoSQLBench workload (blocks `schema`, `rampup`, `main-read`) |
| `profiles/*.yaml` | `cassandra.yaml` overrides; top-level keys replace the stock ones |
| `reference.md` | what the numbers mean, known failure modes, pitfalls |
| `report-template.md` | the structure of the report to write |

Read `reference.md` before interpreting results.

## Rules

- **JDK:** build and run Cassandra with **JDK 17**. NoSQLBench 5.25 needs **JDK 25**, used only for the load generator.
- **Memtable:** always pin it to **TrieMemtableStage2**. The script does this, and fails if it can't. Don't remove the pin; the default memtable is not the one under test.
- **Single node:** use one node with the ports in the script: CQL 9542, storage 7500/7501, JMX 7599.
- **Work directory:** everything goes under `$CCB_WORKDIR` (default `~/chunkcache-blast`), never inside the repository: worktrees, data, results.
- **Host:** run nothing CPU- or disk-heavy on the host during a test (no `ant` builds, no other tests). Record what else was running, for example with `top -bn1 | head -15` before the round.
- **Order:** always interleave the builds: candidate, base, candidate, base, and so on. Never run all of one build first.
- **Repeats:** use at least 2 runs per build and configuration. Report every run, including those that look like outliers.
- **Evidence only:** report only what the evidence shows. If something is unexplained, list it as an open question and don't speculate in the conclusions.
- **Report:** it must include full commit SHAs, the JDK version, the NoSQLBench version, the profile used, and the error list with stack traces.
- **Git:** don't commit or push anything as part of this skill.

## Procedure

### 1. Prerequisites
```
export CCB_WORKDIR=$HOME/chunkcache-blast      # outside the repository
export NB5_JAR=/path/to/nb5.jar                # NoSQLBench 5.21 or later
.bob/skills/chunk-cache-stress-compare/scripts/ccblast.sh check
```
Fix every line that `check` reports as missing or in use before continuing.
- Set `JAVA17_HOME` and `NB5_JAVA_HOME` if they're not found automatically.
- Set `CQLSH_PYTHON` to a Python 3.12 or older (the bundled cqlsh doesn't support 3.14).

Allow about 6 GB of free disk per configuration. A failing run can write about 20 GB of uncompressed logs; logback compresses them to about 400 MB.

### 2. Builds
Resolve the two commits.

For a pull request of `datastax/cassandra`, fetch it with git:
```
git fetch origin pull/<N>/head && git rev-parse FETCH_HEAD
```
Don't use `gh` for `datastax/cassandra`: it is blocked by SAML. The base is normally the merge base with `origin/main-5.0`.
```
S=.bob/skills/chunk-cache-stress-compare/scripts/ccblast.sh
$S setup base      <base-sha>
$S setup candidate <candidate-sha>
```
Each `setup` creates a detached git worktree under `$CCB_WORKDIR/builds/<variant>` and runs `ant jar` with JDK 17. Record the full SHAs from `$CCB_WORKDIR/builds/<variant>.commit`.

If the pull request gets new commits later, set up a new variant name, for example `candidate2`. Keep the old one so earlier results stay reproducible.

### 3. Golden data (once)
```
$S load base
```
This loads 2,000,000 rows of about 1 KB each: about 2.0 GB in about 7 sstables, with LZ4 and 16 KiB chunks. It then flushes, drains and keeps a copy in `$CCB_WORKDIR/golden`. Every run starts from an identical copy.

Load with the **base** build, so both builds read sstables written by the same code. If the candidate changes the sstable format, note it in the report.

### 4. Comparison rounds
Start with the stock settings, then the more aggressive profiles:
```
$S compare base candidate 1024 -                         2 default
$S compare base candidate 1024 .bob/skills/chunk-cache-stress-compare/profiles/cr128.yaml 2 cr128
$S compare base candidate 1024 .bob/skills/chunk-cache-stress-compare/profiles/cr256.yaml 2 cr256
$S compare base candidate 256  -                         2 t256
```
Arguments: `<base> <candidate> <client threads> <profile or -> <repeats> <tag>`. Each run is called `<variant>-<tag>-r<i>`.
- A run that doesn't fail takes about 3 minutes.
- A run that collapses can take about 16 minutes, because every failed read waits.
- Run the rounds in the background and wait for them; don't poll in a tight loop.

### 5. When neither build fails
Change one parameter at a time, and run both builds with each new setting. Useful levers, in order:
1. `concurrent_reads`: 128, 256, 512 (`profiles/cr*.yaml`). More read threads use more of the pool at once.
2. More background readers: `concurrent_compactors: 4` and `concurrent_index_builders: 4` (write a new profile).
3. More client threads: 2048.
4. A smaller cache: `FILE_CACHE_MB=64 $S compare ...`, keeping the data set size.
5. The reserved pool headroom, if the candidate has one: `JVM_OPTS_EXTRA='-Dcassandra.chunk_cache_reserved_pool_space_in_mb=32'`. Report this as an explicit non-default setting.

For a quick search, use `MAIN_CYCLES=1500000` (about 1 minute per run). Once a setting shows a difference, confirm it with the full 6M cycles and at least 2 interleaved runs per build.

Record every setting you tried in the report, including those that showed nothing.

### 6. Evidence per run
```
$S catalogue <label>
```
This writes, in `results/<label>/`:
- `server-errors.tsv`: distinct errors with counts and first and last timestamps;
- `server-errors-with-stacktraces.txt`: one full trace per group;
- `client-errors.txt`: the distinct client-side errors.

`SUMMARY.txt` in the same directory has:
- the client errors, and the `BufferPoolExhaustedException` and ERROR counts by thread pool;
- the compaction outcome and the sstables left;
- the SAI build outcome;
- the JMX peaks (pool used, overflow, direct memory, cache size, chunk cache counters, threads).

Check each failure's root cause in the stack trace, the last `Caused by:`. Don't go by the outer exception alone; see `reference.md` on `CorruptSSTableException`.

### 7. Report
Write the report from `report-template.md`, as `$CCB_WORKDIR/REPORT-<candidate-short-sha>.md`.

To share it:
- zip the report together with `workload/ccblast.yaml`, the profiles used, the scripts and, for each run, `params.txt`, `SUMMARY.txt`, `samples.csv`, `client-errors.txt`, `server-errors.tsv`, `server-errors-with-stacktraces.txt`, `compact.out`, `create-index.out` and `sai-status.out`;
- leave out the full logs, which can be hundreds of MB, and say so in the report.

### 8. Clean up
```
$S stop
```
Keep `golden/` and the builds for later rounds. Remove a worktree only when asked: `git worktree remove $CCB_WORKDIR/builds/<variant>`.
