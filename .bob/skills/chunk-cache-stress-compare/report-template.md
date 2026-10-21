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

# Chunk cache stress test: <candidate description> vs <base description>

**Executed:** <date>. Summary in one or two sentences: what differs between the builds, with numbers.

## 1. Versions under test
| label | commit (full SHA) | description |
|---|---|---|
| base | `<sha>` | `<subject line>` |
| candidate | `<sha>` | `<subject line>` |

| component | version |
|---|---|
| JDK (build and server) | `<java -version line>`, heap 4 GB |
| load generator | NoSQLBench `<version>` (`<git sha>`), run on `<JDK>` |
| cqlsh | bundled, Python `<version>` |
| host | `<CPU, cores, RAM, OS>`; other load on the host: `<what top showed>` |

## 2. Node configuration
- **Stock `conf/cassandra.yaml` changes:** the ports, `file_cache_enabled: true` and `file_cache_size: <N>MiB`; the memtable pinned to `TrieMemtableStage2` (`memtable-classes.out` confirms it).
- **Profile overrides:** list every key of each profile used, for example `concurrent_reads: 128`.
- **Non-default system properties:** list each one (`JVM_OPTS_EXTRA`), or write "none".
- **Effective cache capacity per build:** the JMX `ChunkCache Size` peak.

## 3. Workload
- **Data set:** the rows, the size on disk, the sstable count, compression and chunk size, and which build loaded it.
- **NoSQLBench workload:** paste `workload/ccblast.yaml` and the nb5 command line.
- **Timeline of one run:** reads from T+0, `nodetool compact` at T+15 s, `CREATE CUSTOM INDEX … USING 'StorageAttachedIndex'` at T+20 s; the evidence collected.
- **Order of the runs:** list them as executed.

## 4. Results
One row per run, every run included:

| run | build | profile | client threads | failed reads (of N) | read phase | major compaction | SAI index | sstables after | ERROR entries |
|---|---|---|---|---|---|---|---|---|---|

JMX peaks, one row per run:

| run | max pool used | max overflow | max direct | cache size | SyncReclaims | ReclaimRetrySuccesses | BlockedOnPoolAllocation | PoolWaitSuccesses | PoolExhausted | misses |
|---|---|---|---|---|---|---|---|---|---|---|

**Parameter search:** if you ran one, give a table of each setting tried and what it showed.

## 5. Errors found
For each run:
- a table of the distinct errors: where (thread pool and operation), exception and root cause, count, and the trace id;
- then each trace in full, in a code block (B1, B2, …).

Mark failures that happen on both builds and aren't related to the change (for example https://github.com/riptano/cndb/issues/19876) as such.

## 6. Conclusions
Only what the evidence shows, numbered. Put anything unexplained under "Open questions", not in the conclusions.

## 7. Not tested
List what this round didn't cover: other caches, `disk_access_mode`, repair, multiple nodes, and so on.

## 8. Package contents
The files included in the zip, and a note that the full logs are not included.
