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

# Group J — preflight tool (DSE 5.1)

Part of the [reviewer triage guide](README.md), which explains the tiers, the review order and the layout of a brief.

All three patches extend the offline `dsepreflight` tool that P22–P25 added to this series. None of them writes to a
node's data in any mode. The tool's own writes, read from the code at P37, are these: the `--json` and `--cql` output
files the operator names (`DsePreflight.writeAtomically`, a temporary file next to the target, then an atomic move),
and the scratch directories of `DseTableReader.readBig`/`readBigSSTable`. Those are created with
`Files.createTempDirectory` in the JVM temporary directory, hold copies of small components and symbolic links to
`Data.db`/`Index.db`, and are removed by `deleteScratch`, which deletes the links without following them. No other
`Files.write/delete/move/copy` call exists under `tools/dsepreflight`. The `nodetool` commands of the live mode are
read-only (`LiveState.DSE51_COMMANDS`). The rollback check only lists the files to remove (`Report.toRemove`) and
deletes nothing.

## P35 — preflight tool, node inspection (#2789)

**Tier:** 2 — a new offline, read-only inspection of DSE 5.1 nodes inside the tool. The only server-side edits widen
the visibility of constants and refactor the DSE_5_1 start check (`Dse6NonCoreTraces`, added by this series in P33)
so the tool can share it. Its behaviour should be unchanged.

**Size and shape:** tool code about +2400/−130 lines, most of it the new `Dse51NodeInspector.java` (about 1750). The
rest is `NodeInspector` refactored into package-level helpers, plus `DsePreflight` routing, `DseTableReader.readBig`,
`LiveState`, `NodeLayout` and `NodeReport`. Server code about +115/−25 (`Dse6NonCoreTraces`, `Dse51SystemTables`).
Tests about +1500 (`Dse51PreflightTest` about 1100 lines, plus the new fixture `Dse51NodeFixture`). The long finding and
remedy strings of `Dse51NodeInspector` can be skimmed.

**Gating:** `DsePreflight.isDse51Node(layout)` chooses the DSE 5.1 inspection when three things hold:
* the metadata directory has no `nodes/local` and no `nodes/local.old`;
* `ds_version.txt` names a major version below 6, or, if that file is missing, `system/local*` holds `m*-big-Data.db`;
* `--jvm-options` is accepted only on that path.

For clusters that never had DSE: nothing changes. The tool is never run there, and the server edits only widen
visibility (`RECORD_FILE_NAME`, `UNREADABLE_SUFFIX`, `Feature`), add `TEMPORARY_SUFFIX` with the same `.tmp` text, and
extract helpers from `Dse6NonCoreTraces.verifyDse51AtStartup`, which runs only in DSE_5_1 mode.

**Where to look**
* `tools/dsepreflight/DseTableReader.java` — `readBig`, `scratchCopy`, `deleteScratch`. A big sstable is opened from a
  scratch copy because, as the javadoc says, opening one "can delete its `Summary.db` and write its `TOC.txt`, even
  offline". `Data.db` and `Index.db` are linked symbolically, not copied. Question: can any path of the big-format open
  or failure handling (for example a component rebuild or a `delete` on a corrupt sstable) write, truncate or delete a
  `LINKED` component through the link? The safety of the node's files rests on that.
* `tools/dsepreflight/DsePreflight.java` — `isDse51Node`. The routing decides which inspection runs and, later
  (P37), which Paxos rollback path runs. Question: can a DSE 6 node be routed to the DSE 5.1 inspection? For example,
  `--metadata-dir` points at the wrong directory, `ds_version.txt` is missing, and `system/local-*` still holds
  `mc`/`md` sstables left from an earlier 5.1→6 upgrade. If so, what does the operator see?
* `service/Dse6NonCoreTraces.java` — `verifyDse51AtStartup`, `addDse51WorkloadTrace`, `addDse51SearchIndexTrace`,
  `Dse51Markers`. Start-check loops (P33) were moved into helpers that the server and the tool share. Check that the
  start-time refusal still lists the same traces in the same order. `columns.peer` became
  `columns.nodeWorkloads().peer`, built in `StoredColumns.nodeWorkloads()` from the same `peer` field.
* `tools/dsepreflight/NodeInspector.java` — `checkPartitioner`, `checkDseYamlFeatures`, `checkAuthentication`,
  `scanDataDirectories`, `scanTableDirectory`, `checkDiskSpace`, `checkLiveCommands/Status/Schema/...`. The DSE 6
  inspection (P22) was split into static helpers "moved ... as it was". Confirm that the DSE 6 order of checks is kept,
  including the `if (dse.isEmpty()) return;` that now sits after the hints check, and the SOLR_DATA check that moved
  into the `beforeEach` callback.
* `tools/dsepreflight/Dse51NodeInspector.java` — `inspectSystemTables`, `checkLocalRelease`, `inspectHints`,
  `inspectCommitLog`, `inspectMetadataDirectory`. These are the DSE 5.1 identity, release and file-format gates.
  Questions: is the release gate (only 5.1.51) strict enough? Are hints and commit log segments read from their
  headers only, without decoding?
* `tools/dsepreflight/ClusterAggregate.java` — `aggregate`. DSE 5.1 reports are refused with `DSE51_REPORT` (this
  finding is removed again in P36).

**Modified or deleted existing code**
* `db/Dse51SystemTables.java` — `RECORD_FILE_NAME` and `UNREADABLE_SUFFIX` made public; the new constant
  `TEMPORARY_SUFFIX` replaces two `".tmp"` literals. History: added by this series (P30); the invariant is the name of
  the on-disk record, and the text is unchanged.
* `service/Dse6NonCoreTraces.java` — `Feature` made public, `displayName()` added, `verifyDse51AtStartup` loop bodies
  extracted. History: added by this series (P33).
* `tools/dsepreflight/NodeInspector.java`, `LiveState.java`, `NodeLayout.java`, `DsePreflight.java` — refactored and
  extended. History: added by this series (P22).

**Failure and rollback risks**
* A run that is killed leaves its scratch directory in the temporary directory. The javadoc says so. That directory
  holds copies of small components of `system.local`, `system.peers`, `system_schema` and
  `system_auth.role_permissions` (no secrets were found among the tables read through `readBig`).
* `DsePreflight` quiets `BigSSTableReaderLoadingBuilder` to ERROR in the tool's JVM, so the warning about a missing
  bloom filter is not printed. This affects the tool's own logging only.
* No rollback hazard: the node's files are not written.

**Test integrity**
* Existing tests changed: `DsePreflightTest` (help test). The `--aggregate` and exit-code assertions moved into the
  whitespace-normalised `contains` chain, and `--jvm-options` and the new description were added; this is not a
  weakening. New case `theInspectionIsChosenFromTheReleaseOfTheNode`. `DseNodeFixture` has 6 lines added.
* New tests: `Dse51PreflightTest` and `DsePreflightToolTest.aDse51NodeIsInspectedAsAnOfflineClientThroughMain` run on
  node 1 of the captured DSE 5.1.51 cluster (`Dse51NodeFixture`). The tool-level test fingerprints the node's files
  before and after the run and asserts that no schema is loaded. Rows a test adds are written as `me` sstables by this
  software in DSE_5_1 client mode, not by DSE. `Dse6NonCoreTracesTest` feeds the same rows to `Dse51Markers` and to the
  start check and compares the groups.
* Not covered by any test in the patch: misrouting by `isDse51Node` for a DSE 6 node that has `m*` sstables of
  `system.local` and no readable `nodes/local`; any check that a symbolically linked component is not written by the
  big-format open.

**Suggested reviewer questions**
* Is a symbolic link to `Data.db`/`Index.db` safe for every big-format open path, including the corrupt-sstable and
  failed-open paths? Would copying `Index.db` too (it is usually small next to `Data.db`) remove the question?
* Should `isDse51Node` refuse an ambiguous node (no `nodes/local`, DSE 6 sstables and `m*` sstables both present)
  instead of choosing one inspection?
* Does the `Dse51Markers` API, used offline, keep exactly the rule of `verifyDse51AtStartup`? Is the comparison in
  `Dse6NonCoreTracesTest` enough to keep the two from drifting apart?

## P36 — preflight tool, schema inspection and cluster aggregate (#2790)

**Tier:** 2 — DSE 5.1 schema checks and DSE 5.1 report aggregation inside the tool. The server-side edits only make
helpers public and add new public helpers (`Dse51ColumnTypes`, `Dse6NonCoreTraces`), plus one sentence in a DSE_5_1
start refusal.

**Size and shape:** tool code about +1370/−215 (`SchemaInspector` about 900 changed lines, `Dse51NodeInspector`
about 420, `ClusterAggregate` about 240). Server code about +70/−15. Tests about +1790 (`Dse51PreflightTest` about
1340, new `ClusterAggregateTest` cases about 340). The mixed-phase remedy strings can be skimmed.

**Gating:** `SchemaInspector.forDse51(...)` runs only from the DSE 5.1 inspection (P35 routing). The aggregate
changes only its header and its text when DSE 5.1 reports are present. For clusters that never had DSE: nothing
changes. `Dse51ColumnTypes.unreadableColumns` becomes public, and `PREFLIGHT_CODE` is added. The refusal text of
`Dse51ColumnTypes` (DSE_5_1 only) gains "which the preflight inspection of a DSE 5.1 node blocks on as COLUMN_TYPE".
`Dse6NonCoreTraces` gains `dse51TracedKeyspaces`, `isDse51CompactionTrace` and `Dse51Markers.dsefsKeyspace`, and
extracts `isDse51GraphPrivateKeyspace` (same rule).

**Where to look**
* `tools/dsepreflight/SchemaInspector.java` — the private constructor and `forDse51`, and the `if (dse51)` /
  `if (!dse51)` branches in the main check sequence (`checkViewTableRows`, `checkVectorColumns`, `checkColumnTypes`,
  `HIDDEN_COLUMNS`, `GRAPH_SCHEMA_TABLES`). The DSE 6 path takes the public constructor with `dse51=false`. Question:
  the commit states that DSE 6 output is unchanged, and `DsePreflightTest` (149) is reported unchanged; does any shared
  branch, for example the new `if (schemaRead)` block, change the order of DSE 6 findings?
* `tools/dsepreflight/SchemaInspector.java` — `checkColumnTypes` uses `Dse51ColumnTypes.unreadableColumns`, the same
  rule as the start check of P34. It runs on tables built from DSE 5.1's schema and on views whose base table is built.
  Question: does a table that cannot be built (UNLOADABLE_TABLE) ever hide a COLUMN_TYPE that the server would refuse?
* `tools/dsepreflight/SchemaInspector.java` — `schemaChange` and `NO_SCHEMA_CHANGE_IN_MIXED_PHASE`. Mixed-phase
  remedies depend on `mixedPhase`, which comes from UPGRADED_PEERS. Question: is UPGRADED_PEERS a reliable signal on a
  DSE 5.1 node, whose `system.peers` is the only source?
* `tools/dsepreflight/SchemaInspector.java` — replication checks for DSE 5.1 (`SYSTEM_AUTH_REPLICATION`,
  `KEYSPACE_REPLICATION`). These count nodes per data centre from `system.local`/`system.peers`, and keyspaces reported
  as traces are left out (`keyspacesLeftOut`).
* `tools/dsepreflight/ClusterAggregate.java` — `aggregate`. `DSE51_REPORT` is replaced by `MIXED_SOURCES`, plus
  `Sources`, `DSE_RELEASE` (5.1.51 only), `NO_PEERS_RECORDED`, and `RESTRICTED_VIEW` across nodes. `source` is
  `@JsonIgnore`, so the JSON of DSE 6 aggregates stays as it was.
* `service/Dse6NonCoreTraces.java` — `dse51TracedKeyspaces` and `isDse51CompactionTrace` restate the start check's
  rules for the tool. Confirm that they match `addDse51KeyspaceTraces` and `addCompactionTrace` exactly.

**Modified or deleted existing code**
* `schema/Dse51ColumnTypes.java` — `unreadableColumns` made public; refusal text extended. History: added by this
  series (P34).
* `service/Dse6NonCoreTraces.java` — `addDse51KeyspaceTraces` now calls the extracted `isDse51GraphPrivateKeyspace`,
  with the same condition. History: added by this series (P33).
* `tools/dsepreflight/SchemaInspector.java`, `ClusterAggregate.java` — History: added by this series (P23, P24).

**Failure and rollback risks**
* Node data: nothing written. The schema is read through `readBig` scratch copies.
* The aggregate's `MIXED_SOURCES` blocks on a directory that mixes DSE 5.1 and DSE 6 reports. This is intended.

**Test integrity**
* Existing tests changed: `Dse51ColumnTypesTest` (the expected refusal text updated to the new sentence, still
  compared whole). `DsePreflightToolTest.aDse51NodeIsInspectedAsAnOfflineClientThroughMain` now expects blocking
  findings `THRIFT, COMPACT_STORAGE, SASI_INDEX, ROW_LEVEL_ACCESS, RESTRICTED_VIEW, VIEW_TABLE_ROW` in any order,
  where it expected exactly `THRIFT`, because the schema is now checked. Inside this group, `Dse51PreflightTest`
  (added in P35) loses `theAggregateRefusesTheReportsOfDse51Nodes`, whose behaviour was removed; it is replaced by
  `ClusterAggregateTest` cases. It also loses the "remedy not validated" text assertions (decision on item 64).
* New tests: `Dse51PreflightTest` cases run on the captured DSE 5.1.51 schema. Some schema rows (a custom column type,
  restricted tables and views, UDFs) are written by this software as `me` sstables, not captured from DSE.
  `ClusterAggregateTest` builds reports in memory and as JSON files.
* Not covered by any test in the patch: nothing found beyond the mixed-phase detection relying on
  `system.peers`.

**Suggested reviewer questions**
* `Dse51ColumnTypes.unreadableColumns` is now called offline on `TableMetadata` built from DSE 5.1's schema rows.
  Could the server's refusal and the tool's COLUMN_TYPE disagree, because the tool builds the table differently from
  the server's schema loading in DSE_5_1 mode?
* The aggregate accepts DSE 5.1 reports only when every one of them is DSE 5.1.51 (`DSE_RELEASE`). Is that the
  intended product limit, also enforced by the server?
* Should `dse51TracedKeyspaces` and `isDse51CompactionTrace` be derived from the same tables as the start check, so
  that they cannot drift apart, rather than restating them?

## P37 — preflight tool, Paxos rollback and rollback inspection (#2791)

**Tier:** 2 — a new read-only rollback check (`--rollback-check`) and a DSE 5.1 path of `--paxos-rollback`. On the
server side, three log texts now come from constants with the same output, including one in upstream
`StartupChecks.verify`, which runs on every cluster. One DSE_5_1 refusal changes its rollback instruction, and the
operator's rollback procedure depends on that instruction.

**Size and shape:** tool code about +1620/−100 (new `Dse51RollbackInspection.java` about 1050, `PaxosRollback`
about 270, `DsePreflight` about 125). Server code about +50/−25 (`StartupChecks`, `DseUpgradeMarker`,
`Dse6NonCoreTraces`, `Dse51SystemTables` javadoc). Tests about +2000 (`Dse51RollbackInspectionTest` about 1120,
`Dse51PaxosRollbackTest` about 580, `Dse51RollbackCommitLogTest` 80).

**Gating:** `--rollback-check` runs only when `isDse51Node` holds, and refuses otherwise ("inspected as a DSE 6
node"). `--paxos-rollback` routes on `isDse51Node` and sets `Options.dse51`. Changes for clusters that never had DSE,
all always-on:
* `StartupChecks.verify` now logs `POST_ACTION_FAILED + ' ' + type`. The bytes are identical to
  "Failed to run startup check post-action on <type>".
* `writeUpgradeMarker` logs through `DseUpgradeMarker.writtenText/notWrittenText`, with the same bytes. This path
  runs in DSE modes only.

The DSE_5_1 refusal in `Dse6NonCoreTraces` now tells the rollback to delete only the record and keep the marker.

**Where to look**
* `service/Dse6NonCoreTraces.java` — the workload-trace sentence of the refusal (DSE_5_1). The instruction changed
  from "delete the marker and the record" (P33) to "delete the record, keep the marker". When the marker is present,
  the next start does not run `deleteRecordOfAnEarlierUpgrade` (see the `Dse51SystemTables.loadOrReadAtStart`
  javadoc) and loads the record, which may be stale. Only the preflight node inspection catches a record kept next to
  the marker (`DSE51_SYSTEM_COLUMNS`). Question: should the server itself detect a record older than the rollback,
  instead of relying on the operator running the tool?
* `tools/dsepreflight/Dse51RollbackInspection.java` — `checkCommitLog` and `countEntries`. A subclass of
  `CommitLogReader` (overriding the upstream `protected readMutation`) counts entries without decoding them, and any
  entry blocks with COMMITLOG_UNFLUSHED. Segments ending in `.replayed` are skipped. Question: is "a drain leaves no
  entry" true for HCD 2.0's commit log in DSE_5_1 mode, including compressed or encrypted segments, for which
  `CommitLogReader` needs the configuration? This was not established; the only test uses the unit-test defaults.
* `tools/dsepreflight/Dse51RollbackInspection.java` — `checkMetadataDirectory`, `startShown`,
  `markerNotFoundRemedy`. The check refuses when the DSE 5.1 marker is missing and infers "HCD 2.0 started" from
  `.replayed` DSE 5.1 segments, `dse51-system-columns`, or segments of another version. Question: are these inferences
  sound? The remedies tell the operator to create the marker by hand.
* `tools/dsepreflight/Dse51RollbackInspection.java` — `checkDataDirectories`, `checkSnapshot`, `checkHints`,
  `targets`. Refuses sstables that are not big `me` with a numeric generation, leftover transaction logs, hints that
  are not version 1, and a missing or incomplete `pre-hcd-*` snapshot. Question: does this list cover every file that
  HCD 2.0 in DSE_5_1 mode can leave and DSE 5.1.51 cannot read?
* `tools/dsepreflight/PaxosRollback.java` — `openRow`, `writtenByOnDse51Node`, `readDse51DataCenters`, `readTable`.
  DSE 5.1 proposals at messaging version 10/11 are decoded through `DseCompat.storedDecodingVersion`, which returns
  VERSION_40 only when `!isDse6()`. Question: is the tool's JVM (`DatabaseDescriptor.clientInitialization`) guaranteed
  to run in a mode where `isDse6()` is false?
* `tools/dsepreflight/DsePreflight.java` — `rollbackCheck`, `ROLLBACK_CHECK_OPTIONS`, and the `isDse51Node` routing
  of `--paxos-rollback`. On an HCD 2.0 node in DSE_5_1 mode, `isDse51Node` needs `nodes/local` to be absent from the
  metadata directory. Whether HCD 2.0 in DSE_5_1 mode ever writes `nodes/local` was not established.
* `service/StartupChecks.java` — `verify` (post-action WARN) and `writeUpgradeMarker`. Byte-identical output; confirm.

**Modified or deleted existing code**
* `service/StartupChecks.java` — `verify`: the post-action WARN literal was replaced by the public constant
  `POST_ACTION_FAILED`. History: bd36cb9f6b8, 2022, CASSANDRA-17180 (the startup check against data resurrection
  added post-actions). The invariant is that a failed post-action only warns and does not stop the start; this is
  unchanged.
* `service/StartupChecks.java` — `writeUpgradeMarker`: log texts now come from `DseUpgradeMarker`. History: added by
  this series (P29).
* `service/Dse6NonCoreTraces.java` — the rollback sentence of the DSE 5.1 workload refusal. History: added by this
  series (P33); the record it names was added by P30.
* `db/Dse51SystemTables.java` — `loadOrReadAtStart` javadoc only. History: added by this series (P30).
* `tools/dsepreflight/PaxosRollback.java`, `PaxosRollbackReport.java` — History: added by this series (P25). The DSE 6
  path is kept behind `!options.dse51`, and the "nodes files" wording became `nodesSource`, which keeps the DSE 6 text.

**Failure and rollback risks**
* Rollback to DSE 5.1: correctness depends on the operator removing `dse51-system-columns` (and its `.tmp`) while
  keeping the marker. The rollback check lists the record for removal but does not remove it. A kept record would be
  loaded by the next HCD 2.0 start and would replace values DSE 5.1 wrote after the rollback. Only the tool's next node
  inspection blocks on it.
* The rollback check's precondition is a drained HCD 2.0 node, with system and system_schema not yet restored and
  DSE 5.1 not started. The output prints this as "not verified by the tool".
* The commit-log rule refuses any entry. A node that did not drain cleanly is refused (this fails safe), but a
  commit log the reader cannot parse is also blocking (COMMITLOG_UNREADABLE).
* Node data: nothing written. The check writes nothing outside its `--json` output.

**Test integrity**
* Existing tests changed: `Dse51StartupChecksTest` (`startsWith` changed to `containsExactly` on the marker ERROR,
  which is stricter; new case for the post-action WARN). `Dse6StartupChecksTest` (two `contains` changed to `isEqualTo`
  on the full ERROR, stricter). `Dse6NonCoreTracesTest` (expected rollback sentence updated to the new instruction).
  Inside this group, `Dse51PreflightTest` (added in P35) drops the expectation that the record next to the marker is
  only a note; the record next to the marker now blocks (DSE51_SYSTEM_COLUMNS), and the new assertions compare the
  message and remedy whole.
* New tests: `Dse51RollbackInspectionTest` runs on the captured DSE 5.1.51 node layout, with files added by the test.
  `Dse51PaxosRollbackTest` uses DSE 5.1.51's captured `system.paxos` sstable, which holds committed rounds only. The
  open proposals at versions 10/11 are written by this software, one carrying the captured commit's bytes, so decoding
  a real DSE 5.1 open proposal is not tested on DSE-written sstables. `Dse51RollbackCommitLogTest` (CQLTester) models
  a drain as flush + `forceRecycleAllSegments` + `shutdownBlocking`, not `StorageService.drain`, and uses default
  (uncompressed) commit log settings.
* Not covered by any test in the patch: `countEntries` on compressed or encrypted segments; `isDse51Node` on a real
  HCD 2.0 metadata directory after a start in DSE_5_1 mode; the server loading a stale record after a rollback that
  kept it.

**Suggested reviewer questions**
* With the marker kept on rollback, is "the operator deletes `dse51-system-columns`" the only safeguard against
  loading values recorded before the rollback? Should the server check this?
* Does an HCD 2.0 drain in DSE_5_1 mode, with the production commit log settings (compression, encryption), always
  leave segments that `countEntries` reads as empty?
* Is `DseCompat.storedDecodingVersion` stable for the tool's JVM mode, so that a version 10/11 proposal is always
  decoded at VERSION_40?
* Could HCD 2.0 in DSE_5_1 mode create `nodes/local` in the metadata directory? If it can, both rollback modes would
  route the node as DSE 6.

## Group notes
* Order: P35 adds the routing (`isDse51Node`), `readBig`, and the refusal of DSE 5.1 reports by the aggregate. P36
  removes that refusal and aggregates DSE 5.1 reports. P37 reuses `isDse51Node` for both rollback modes and extends
  `readBig` to snapshots. Read P35's `DseTableReader.readBig` before the other two: every DSE 5.1 read in the group
  goes through it.
* The group duplicates server rules in the tool on purpose: start-check traces through `Dse6NonCoreTraces.Dse51Markers`,
  `dse51TracedKeyspaces` and `isDse51CompactionTrace`; column types through `Dse51ColumnTypes.unreadableColumns`; log
  texts through `DseUpgradeMarker.writtenText/notWrittenText` and `StartupChecks.POST_ACTION_FAILED`. Shared code keeps
  most of these aligned. `dse51TracedKeyspaces` and `isDse51CompactionTrace` restate a rule, and the rollback check
  matches log lines by text.
* P37 changes the rollback contract set by P30 and P33 (the marker is now kept and the record removed). The operator
  procedure (operator documents, outside this group) must match.
* Dependencies outside the group: P22–P25 (the tool), P29 (marker, `StartupChecks.writeUpgradeMarker`), P30
  (`Dse51SystemTables` record), P33 (`Dse6NonCoreTraces` DSE 5.1 traces), P34 (`Dse51ColumnTypes`, big `me` writer
  used by the test fixtures).
