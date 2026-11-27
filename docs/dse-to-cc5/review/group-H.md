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

# Group H — preflight tool (DSE 6)

Part of the [reviewer triage guide](README.md), which explains the tiers, the review order and the layout of a brief.

All four patches build one offline command line tool, `tools/bin/dsepreflight`
(`org.apache.cassandra.tools.dsepreflight.DsePreflight`), with three modes: node inspection (default), cluster aggregate
(`--aggregate <directory>`) and Paxos rollback (`--paxos-rollback`). The tool starts with
`DatabaseDescriptor.clientInitialization(false)` (no server configuration, no directories created, no schema, keyspace
or commit log loaded, which `DsePreflightToolTest` asserts). No class of the group is reachable from `CassandraDaemon`
or any other server code path. The only files any mode writes are the ones the operator names with `--json` and, in
the rollback mode, `--cql` (plus its sibling `<name>-local-serial.cql`), through `DsePreflight.writeAtomically`
(temporary file in the target directory, then an atomic rename). No mode writes to the node's data, commit log, hints
or metadata directories; the tests check this with `DseNodeFixture.fingerprint()` (size, mtime and CRC32 of every file
under the fixture node, before and after a run).

## P22 — preflight tool, node inspection (#2759)

**Tier:** 2 — a new, explicitly invoked, read-only offline tool; the three existing production files it touches get a
log message wording change (DSE 6 mode only), one new enum constant and a javadoc edit.

**Size and shape:** production: 9 new files, about 3,700 lines (`NodeInspector` about 2,100, `DsePreflight` 380,
`NodeLayout` 300, `LiveState` 235, `NodeReport` 200, `DseTableReader` 160, `Finding` 80, plus `nodes/DseNodesFiles`
164 and `nodes/DseNodeRecord` 88) and the 45-line script `tools/bin/dsepreflight`; 3 existing production files, about
10 lines. Tests: 5 new files, about 2,830 lines (`DsePreflightTest` 2,250, `DseNodeFixture` 307, `DseNodesFilesTest`
148, `DsePreflightToolTest` 78, `HintsDescriptorBytes` 46); 5 existing test files, about 60 lines. The finding texts of
`NodeInspector` (most of its length) can be skimmed; the read paths (`DseTableReader`, `DseNodesFiles`, the hints and
commit log scans) cannot.

**Gating:** the tool runs only when an operator starts `tools/bin/dsepreflight`. For a cluster that never had DSE,
nothing changes at runtime: `BatchlogManager` changes only the text of an ERROR built under
`DatabaseDescriptor.isDse6CompatibilityMode()`; `CassandraRelevantEnv.DSE_HOME` is read only by `DsePreflight.layout`;
`DseTlsPorts` is a javadoc edit.

**Where to look**
* `src/java/org/apache/cassandra/tools/dsepreflight/DseTableReader.java` — `sstablesIn`, `rows`: the sstables of
  `system_schema` and `system_auth` are listed through `LifecycleTransaction.getFiles(..., OnTxnErr.THROW)` and opened
  with `SSTableReader.open(null, descriptor, components, TableMetadataRef.forOfflineTools(metadata), false, true)`,
  with the components passed in so that no `TOC.txt` is written. Is opening a BTI sstable offline, without validation,
  free of any write on every code path (chunk cache, key cache, `Reference-Reaper` release), and is
  `LifecycleTransaction.getFiles` free of any cleanup of a transaction log left by a running DSE?
* `DseTableReader.read` — refuses an sstable not in `BtiFormat` before opening it (the comment states that opening a
  big-format sstable loads its summary, which deletes a `Summary.db` it cannot load and may rewrite `TOC.txt`). Is
  this the only open path of the tool? (`SSTableReader.open` appears only there at the group's head.)
* `src/java/org/apache/cassandra/nodes/DseNodesFiles.java` — `readLocal`, `readPeers`: reuse
  `CC4NodesFileReader.createCC4ObjectMapper()` and `CC4NodesFileReader.holdsNoPeer` without recovering
  `local.old`/`peers.old`, unlike the startup import. Should a class used only by the tool live in the server package
  `org.apache.cassandra.nodes` (it is public there)?
* `NodeInspector.peersFileProblem`, `peersReadFailure`, `PeersFileState` — the tool runs while DSE may be rewriting
  `nodes/peers`; the file's key, size and mtime are compared before and after the read. Is a rewrite between the two
  stats always detected (same inode reused, same size, coarse mtime)?
* `NodeInspector.guarded` — every part of the inspection catches `RuntimeException | AssertionError` and turns it into a
  blocking finding, so the node is never reported ready unchecked. Confirm no part can return early without a finding.
* `LiveState.collect` — with `--nodetool`, runs DSE's nodetool through `ProcessBuilder` with the fixed command list
  `version, status, describecluster, nodesyncservice status, compactionstats, netstats, listpendinghints, gossipinfo,
  tpstats` (all read-only) and the operator's extra `--nodetool-arg` values; environment stripped as
  `LiveState.TOOL_ENVIRONMENT` says. Is passing arbitrary `--nodetool-arg` through acceptable?
* `NodeInspector.inspectCommitLog` — counts segments by the version in the file name (`CommitLog-<v>-<id>.log`)
  against `CommitLogDescriptor.VERSION_DSE_68`; headers are not read. Is the file name a sufficient signal?

**Modified or deleted existing code**
* `src/java/org/apache/cassandra/batchlog/BatchlogManager.java` — the ERROR on an undecodable batch now adds "repair
  is refused while it is in progress" in DSE 6 mode — History: added by this series (patch "batchlog and Paxos state
  stored by DSE", P19).
* `src/java/org/apache/cassandra/config/CassandraRelevantEnv.java` — new constant `DSE_HOME`; additive.
* `src/java/org/apache/cassandra/net/DseTlsPorts.java` — javadoc only — History: added by this series (patch "internode
  TLS ports of DSE 6 peers", P18).

**Failure and rollback risks**
* The tool runs on a live DSE node: sstables may be compacted away between listing and opening; the texts tell the
  operator to rerun (`SchemaInspector`, `UNREADABLE_SCHEMA` remedy). The scan fails closed (blocking finding), which is
  the right direction.
* `system_schema`/`system_auth` are read from sstables only, so unflushed DSE changes are not seen; the tool emits the
  `SSTABLES_ONLY` note asking for `nodetool flush` first. A forgotten flush gives a stale but not unsafe report.
* `--json` in node mode replaces any existing file at that path without the "is it a report of this tool" check that
  the aggregate mode has (`DsePreflight.isAggregateReport`).
* Rollback to DSE: not applicable, nothing on the node is written.

**Test integrity**
* Existing tests changed: `DsePaxosStateTest` (in-JVM dtest) — the DSE_6 branch used to `continue` without repair; it
  now asserts that `nodetool repair --paxos-only`, `-full` and incremental are refused with the guard's message and that
  `DseCompatRefusals.count("repair")` grows by one on each node (strengthened; it tests guards that are already in the
  base, not this tool). `DseBatchlogReplayTest.repairWhen` — expected message updated with the production text.
  `DseBatchlogStateTest`, `DseViewWireFormatTest`, `DsePaxosRowsTest` — comments only.
* New tests: `DsePreflightTest` runs the tool on `DseNodeFixture`, a node laid out from the real DSE 6.8.65 captures of
  `test/data/dse-compat` (configuration, `system_schema` and `system_auth.role_permissions` sstables, `nodes` files),
  edited per test; some tests add a `bb` sstable written by this software's `SSTableTxnWriter` (not by DSE) to simulate
  a flush in progress. `DseNodesFilesTest` reads the real DSE 6.8.65 and 6.9.26 `nodes` files. `DsePreflightToolTest`
  runs `main` through `ToolRunner.invokeClass` in a fresh JVM and asserts no schema, keyspace, commit log or server is
  loaded. `HintsDescriptorBytes` builds hint descriptors as hand-built bytes.
* Not covered by any test in the patch: `LiveState` against a real DSE nodetool (not established whether any test runs
  a real one); a `nodes/peers` rewritten in place with identical key, size and mtime.

**Suggested reviewer questions**
* Can `SSTableReader.open(..., validate=false, isOffline=true)` on a BTI sstable, or the release of the reader, write
  or delete anything (for instance a missing filter or a `TOC.txt`), and does the components argument cover all
  cases?
* Can `LifecycleTransaction.getFiles` with `OnTxnErr.THROW` delete or rewrite any file of a transaction a running DSE
  has in flight?
* Should `DseNodesFiles`/`DseNodeRecord` live in the tool package rather than as public classes of
  `org.apache.cassandra.nodes`?
* Do the `DsePaxosStateTest` changes belong in this patch, or with the guards patch they test?

## P23 — preflight tool, schema inspection (#2760)

**Tier:** 2 — schema and access-control checks inside the offline tool; the shared production classes it touches get
javadoc, one visibility widening and one new pure accessor.

**Size and shape:** production: 1 new file `SchemaInspector` (about 1,980 lines); tool files changed about 600 lines
(`NodeInspector` +426, `DseTableReader` +133, `DsePreflight`, `NodeReport`); 4 shared production files about 35 lines.
Tests: `DsePreflightTest` +3,200, `DseNodeFixture` +64, `DsePreflightToolTest` +14, `CassandraAuthorizerDseNamesTest`
(javadoc). Finding texts can be skimmed; the schema decoding (`SchemaInspector` reading of `system_schema` rows and
`DseTableReader.metadataFromHeaders`) cannot.

**Gating:** only when the tool runs. For a cluster that never had DSE: `SchemaKeyspace.createTableParamsFromRow`
becomes `public` (no behaviour change); `Dse6NonCoreTraces.nonCoreKeyspaceFeature` is a new read-only accessor of the
existing `NON_CORE_KEYSPACES` map; `CassandraAuthorizer` and `DseViewHiddenColumns` are javadoc only. Nothing changes at
runtime.

**Where to look**
* `SchemaInspector` — builds `TableMetadata` from DSE's `system_schema` rows read offline, calling
  `SchemaKeyspace.createTableParamsFromRow`, `CQLTypeParser`, `TargetParser`. Does it reproduce the decisions the real
  schema load of HCD 2.0 makes in DSE 6 mode (which DSE table/index/compaction/compression classes refuse a start), so
  that a READY report does not precede a refused start? Point to the server-side check each finding mirrors.
* `SchemaInspector` role-permission scan (around `Dse6AccessControlCheck.permissionsEntry`) — uses
  `CassandraAuthorizer.dseLoneWriteGrantName`, `unknownPermissionNames`, `isKnownResourceName` and the
  `restricted`/`grantables` columns read from the sstable header union. Are these the same rules the authorizer applies
  at runtime (they are the same static methods, so only the inputs differ)?
* `SchemaInspector` vector checks (`VECTOR_COLUMN`, `checkVectorColumns`) and TDE checks (`TDE_TABLE`) — blocking
  findings; confirm the conditions match the server refusals of earlier patches.
* `DseTableReader.metadataFromHeaders` — the table is described by the union of the serialization headers (first type
  met for a name). Can two DSE sstables of one system table carry different types for one column, and does "first met"
  then decode wrongly?
* `SchemaInspector` replication checks (`RECOMMENDED_REPLICAS`, `MIXED_PHASE_NO_CHANGE`) — advice only; check the
  mixed-phase remedies never ask for a schema change or repair that the DSE 6 guards refuse.

**Modified or deleted existing code**
* `src/java/org/apache/cassandra/schema/SchemaKeyspace.java` — `createTableParamsFromRow`: `@VisibleForTesting static`
  becomes `public static`, with a javadoc — History: b31845c4a79 (2015, CASSANDRA-9712) factored `TableParams` out of
  `CFMetaData`; 2b2c6decfaf (2021, CASSANDRA-17044) marked it `@VisibleForTesting`; last line change 2f41817e721 (2026,
  HCD-601, added the keyspace and table names for the compression warning). Invariant: the method was private to the
  schema load; making it public lets other callers build `TableParams` from rows that did not come through
  `SchemaKeyspace`, which can throw `ConfigurationException` on unknown classes (documented).
* `src/java/org/apache/cassandra/service/Dse6NonCoreTraces.java` — new `nonCoreKeyspaceFeature(String)` — History: the
  map was added by this series (patch "refusal of non-core DSE features", P17).
* `src/java/org/apache/cassandra/auth/CassandraAuthorizer.java` — javadoc of `dseLoneWriteGrantName`,
  `unknownPermissionNames`, `isKnownResourceName` — History: added by this series (patch "DSE grants in system_auth and
  DSE index classes", P06).
* `src/java/org/apache/cassandra/db/view/DseViewHiddenColumns.java` — javadoc only; added by this series.

**Failure and rollback risks**
* A divergence between `SchemaInspector` and the server-side refusals gives either a false BLOCKED (safe) or a false
  READY, after which the HCD 2.0 start itself refuses (the server guards still apply). The tool is advisory; no
  silent-corruption path was found.
* `createTableParamsFromRow` can log the compression-normalisation warning from the tool process; harmless.
* Rollback to DSE: not applicable.

**Test integrity**
* Existing tests changed (all added by P22, within this group): `DsePreflightTest` assertions were reworked, mostly
  tightened (for example `TDE_TABLE` now expects two findings with exact texts; `AUTH_CONFIG_CHANGE` now also expects
  `PROXY_GRANT`; `TIERED_STORAGE` text changed from the `dse.yaml` option to the table's compaction class).
  `DsePreflightToolTest.OPTIONAL_THREADS` grows from `Attach Listener` to twelve thread patterns (`Reference-Reaper`,
  `ScheduledTasks`, `OptionalTasks`, `ForkJoinPool.commonPool-worker-*`, ...): a weakened leftover-thread check,
  explained as threads started by the sstable read; the same test gains an assertion on `VIEWS_HANDLED`.
  `CassandraAuthorizerDseNamesTest`: javadoc only.
* New tests: real DSE 6.8.65 `system_schema`/`system_auth` sstables from the fixtures, plus tables and rows added per
  test with this software's writer (`DseNodeFixture`), so the "DSE wrote this" part is real only for the base fixture.
* Not covered by any test in the patch: a DSE 6.9 schema fixture is not established as used by `SchemaInspector` tests
  (the fixture node is DSE 6.8.65).

**Suggested reviewer questions**
* Is widening `SchemaKeyspace.createTableParamsFromRow` to `public` acceptable, or should the tool get a narrower
  helper?
* For each blocking schema finding, which server-side refusal does it predict, and is there a test that both reject the
  same input?
* Is the larger `OPTIONAL_THREADS` list hiding a thread the tool should not start (for example `ScheduledTasks` from
  a code path that should not run in client mode)?

## P24 — preflight tool, cluster aggregate (#2761)

**Tier:** 3 — reads only JSON reports from a directory and writes at most the `--json` file; no shared production
class touched.

**Size and shape:** production: `ClusterAggregate` new (852 lines), `DsePreflight` +131, small edits to `NodeInspector`,
`NodeReport`, `SchemaInspector`. Tests: `ClusterAggregateTest` new (1,478 lines), `DsePreflightTest` and
`DsePreflightToolTest` about 70 lines.

**Gating:** only `dsepreflight --aggregate <directory>`. Nothing changes for any cluster.

**Failure and rollback risks**
* Fails closed: unreadable, foreign, wrong-release or older-than-24-hours reports block the cluster
  (`ClusterAggregate` class javadoc, `MAX_REPORT_AGE`, `MAX_CLOCK_SKEW`, duplicate-key detection). The 24-hour window
  and the "same release of the tool" rule are policy choices for the reviewer.
* `--json` refuses to replace an existing file that is not an aggregate report (`DsePreflight.isAggregateReport`).

**Test integrity**
* Existing tests changed: `DsePreflightTest` (added by P22) — the help text and exit-code text assertions were updated
  for the new mode; none removed without replacement.
* New tests: `ClusterAggregateTest` builds node reports as JSON and, for some cases, takes them from real runs of the
  tool on fixtures.
* Not covered by any test in the patch: none.

**Suggested reviewer questions**
* Is `MIXED_STORAGE_PORTS` (every node must have the same `storage_port`) a hard requirement of the server patches, and
  is it enforced at start too, or only here?
* Is a 24-hour maximum report age right for the operator workflow?

## P25 — preflight tool, Paxos rollback (#2762)

**Tier:** 2 — new offline mode that never writes to the node, but whose output (serial reads to run before DSE starts
again) is what protects open Paxos rounds at a rollback, so a wrong or missing read leads to a silent outcome (an old
proposal replayed over newer data) rather than a refusal; review `openRow`, `plan`, `read` and `replication` at Tier-1
depth.

**Size and shape:** production: `PaxosRollback` new (1,663 lines), `PaxosRollbackReport` new (283), `DsePreflight`
+145 (option parsing, `--cql` writing), `DseTableReader` +100 (snapshot read, single-sstable streaming read),
`NodeInspector` +32 (shared `PEERS_NOT_CHECKED`, visibility), `ClusterAggregate` +9 (skips rollback reports), the script
comment. Tests: `PaxosRollbackTest` new (2,349 lines), `DsePreflightTest` +33. The report and printing code
(`PaxosRollbackReport`, `PaxosRollback.print`, `attention`, `describe`) can be skimmed.

**Gating:** only `dsepreflight --paxos-rollback`, which refuses the node-mode options (`PAXOS_ROLLBACK_EXCLUDED`) and is
documented to run with HCD 2.0 stopped after its drain and DSE not started. Nothing changes for any cluster.

**Does any mode write to the node's data?** No, from the code: `PaxosRollback` reads `system.paxos` sstables of the data
directories, of the `pre-hcd-<timestamp>` snapshot and of `--paxos-dir` directories through
`DseTableReader.readSSTable`, which calls `checkTrieFormat` before opening; the only writes are
`Files.deleteIfExists` of the `--cql` target and its `-local-serial.cql` sibling followed by `writeAtomically` of both,
and `writeAtomically` of `--json`. `PaxosRollbackTest` asserts `DseNodeFixture.fingerprint()` unchanged in 20 places.
The generated CQL, run by the operator, does write to the cluster: a serial read completes (commits) an open
proposal on the replicas it reaches.

**Where to look**
* `PaxosRollback.openRow` — a row is open when `proposal_ballot` is live and either `most_recent_commit_at` is absent
  or the proposal ballot's 60-bit timestamp is greater than the commit's; additionally listed when the commit cell
  expires within `COMMIT_EXPIRY_MARGIN_SECONDS` (1 day) and before the proposal cell. Does this match how Paxos v1 on
  DSE 6 decides an in-progress round at its start (`Commit.isAfter`), including a non-time-UUID ballot (listed) and a
  missing proposal (listed)?
* `PaxosRollback.read` — RF 1 → `CANNOT_PROTECT`; RF 2 → `SKIPPED` ("every quorum includes all the other replicas");
  RF ≥ 3 → `READ`. Is skipping RF 2 sound: after the rollback the node holds the older snapshot state, and the only
  other replica holds the newer state; does every later quorum-2 Paxos round then supersede the snapshot's open proposal
  rather than complete it?
* `PaxosRollback.replication` — SimpleStrategy is "local only" only when the nodes files show one data center; RF is
  taken from the schema without capping by the number of nodes (a SimpleStrategy RF 3 on two nodes is treated as RF 3).
  EverywhereStrategy counts nodes from the nodes files. Are these right for the clusters in scope (single-node and
  small data centers are supported)?
* `PaxosRollback.plan` — where `SERIAL` and `LOCAL_SERIAL` may be different quorums the tool lists no read until the
  operator passes `--serial-consistency`; confirm that an operator who passes the wrong level is the only remaining
  hazard and that the report says so.
* `PaxosRollback.sources`, `scan`, `restoreState`, `noPaxosSSTable` — which directories are read (data directories as
  HCD 2.0 left them, the snapshot, `--paxos-dir`) and how a missing or empty `system.paxos` is reported. Is it possible
  to end with `NOTHING_TO_READ` while HCD 2.0's Paxos sstables were moved elsewhere by the restore and not given?
* `PaxosRollback.where`, `literal` — the serialized `row_key` is rendered as CQL literals per partition key type and
  each is parsed back to the same bytes. Check composite keys, frozen UDTs, reversed or custom types.
* `DsePreflight.paxosRollback` — `Files.deleteIfExists` on the `--cql` path and on the derived
  `<name>-local-serial.cql` with no check of what those files are (the aggregate mode has such a check for `--json`).
* `PartitionUpdate.PartitionUpdateSerializer.isEmpty(..., DseCompat.storedDecodingVersion(proposalVersion))` — the
  empty-proposal flag decodes DSE- and HCD-written proposals; only informative (the row is listed either way).

**Modified or deleted existing code**
* None outside the tool: `DseTableReader`, `NodeInspector`, `ClusterAggregate`, `DsePreflight` and the script were added
  by this series (P22–P24). `NodeInspector.PeersFileState`, `peersFileProblem`, `peersReadFailure` lose
  `@VisibleForTesting` because `PaxosRollback` now calls them.

**Failure and rollback risks**
* Time-bound output: the plan is valid for `COMMIT_EXPIRY_MARGIN_SECONDS` (the report's `validUntil`); the operator must
  rerun if DSE does not start in that time. A stale `.cql` is the main operational hazard; the tool deletes both files
  first so a partial earlier run is never mistaken for this one.
* Each sstable is read on its own, so shadowed rows are still listed (extra reads are harmless by design).
* All open partitions are held in memory; the class javadoc says very many need a larger heap than 512M; failure is an
  OOM, not a wrong result.
* Rerun: idempotent on the node (nothing written); the generated serial reads are repeatable.

**Test integrity**
* Existing tests changed: `DsePreflightTest` (added by P22) — the `UNREADABLE_SCHEMA` text assertion is changed to the
  new refusal text and the old "run nodetool upgradesstables" text moved to the remedy assertion; the aggregate misuse
  check accepts the rollback mode's error for `--paxos-rollback`; a new `DOCUMENT_REFERENCE` check is added. Nothing
  weakened.
* New tests: `PaxosRollbackTest` (29 tests) runs the command line on a `DseNodeFixture` node. Most `system.paxos`
  sstables are written by this software's writer in BTI `bb` from `DseFixtures.paxosRows()` and from a dump
  (`DseFixtures.paxosRollbackDump()`) written back cell by cell, so the format is produced by the code under test's
  own library; one real DSE 6.9.26 `system.paxos` sstable (`DseFixtures.dse6926PaxosSSTableDirectory()`) is read as DSE
  wrote it. Replication cases RF 1/2/3, DC-local, LocalStrategy and EverywhereStrategy are covered as plan output.
* Not covered by any test in the patch: running the generated CQL against a cluster (in-JVM or otherwise) and checking
  that the open rounds are completed and that DSE then starts without replaying them; the RF 2 skip rule end to end.

**Suggested reviewer questions**
* Is the RF 2 `SKIPPED` rule correct for Paxos v1 on DSE 6 once the rolled-back node comes back with its snapshot state?
* Does `openRow` agree with the in-progress decision of Paxos v1 at DSE 6 for every combination of expired commit and
  live proposal cells, given that the tool uses `FBUtilities.nowInSeconds()` of the run, not of DSE's start?
* Should the tool refuse to run when it detects a running HCD 2.0 or DSE process, rather than documenting it?
* Should `--cql` refuse to delete an existing file that is not a previous output of the tool, as `--aggregate --json`
  does?

## Group notes
* Strict order P22 → P23 → P24 → P25; each extends `DsePreflight`, `NodeInspector` and `DsePreflightTest` of the
  previous one, so later patches rewrite assertions earlier ones added; review the head state of `DsePreflightTest`
  rather than each step.
* The read-only guarantee rests on two shared pieces reviewed once: `DseTableReader` (P22, extended in P25 with
  `checkTrieFormat` and `readSSTable`) and `DsePreflight.writeAtomically`. Every mode goes through them.
* `SchemaInspector` (P23) and `PaxosRollback` (P25) each compute replication from the schema rows and the nodes files
  separately (`RECOMMENDED_REPLICAS` vs `PaxosRollback.replication`), with their own handling of
  SimpleStrategy/NetworkTopologyStrategy/EverywhereStrategy: duplicated logic that can drift.
* The tool mirrors server-side decisions of patches outside the group (the DSE 6 guards, the non-core feature refusal,
  the SAI version pin `Dse6SaiVersionPin.saiVersionOf`, `DseCompat.isDse6ReleaseVersion`, `Dse6AccessControlCheck`,
  `DseUpgradeMarker`, `DsePeersRecord`, `Dse6UpgradeNodesPersistence.IMPORT_MARKER_NAME`); it calls them rather than
  copying them where they are static, which keeps the two consistent.
* P22 carries test changes for the repair guard of an earlier patch (`DsePaxosStateTest`) and a message change in
  `BatchlogManager`; they do not depend on the tool.
