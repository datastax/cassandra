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

# Group I — DSE 5.1 server side

Part of the [reviewer triage guide](README.md), which explains the tiers, the review order and the layout of a brief.

## P27 — test fixtures written by DSE 5.1.51 (#2781)

**Tier:** 3 — test data and test helpers only; no production path changed.

**Size and shape:** 0 production files. 3 test files (`Dse51Fixtures` new, ~490 lines; `DseFixturesTest` +597 lines,
purely additive; `DseFixtures` +9 lines, a `dse51()` accessor). 312 fixture files under `test/data/dse-compat/`
(~10,100 text lines plus binaries): big `me` sstables of `system_schema`, `system_auth`, `system.local`/`system.peers`
(flushed and compacted, Cassandra-only node and Search/Graph node) and a user keyspace; commit log segments; a hints
file; transaction logs; gossip, handshake and message captures; `cassandra.yaml`/`dse.yaml`/`jvm.options` (installed
and running); `sstablemetadata`/`cqlsh` dumps. `test/data/dse-compat/README.md` gains a ~300-line DSE 5.1.51 section
(3 lines of the existing intro rewritten). `.build/build-rat.xml` excludes `test/data/dse-compat/**/*_txn_*.log` from
RAT. Everything can be skimmed; binaries reviewed with `--stat` only.

**Gating:** none needed: nothing outside `test/` changes except the RAT exclusion. Nothing changes for a cluster that
never had DSE.

**Test integrity**
* Existing tests changed: none (`DseFixturesTest` only gains new `dse51*` test methods).
* New tests: `DseFixturesTest.dse51*` (e.g. `dse51SSTablesAreCompleteBigMeSetsWithNumericGenerations`,
  `dse51CommitLogSegmentsAreAtVersion6`, `dse51HintsFileIsAtVersion1`, `dse51TransactionLogsAreWhatTheReadmeSays`)
  check that the files are what the README says; they test the fixtures, not product code.
* Not covered by any test in the patch: nothing found (Tier 3).

**Suggested reviewer questions**
* The README states that every file was written by real DSE 5.1.51 nodes "except where a paragraph below names an
  edit" (the `running/` configuration files have directory paths cut; `system_auth.roles` is left out because it holds
  password hashes). Are the named edits the only ones, and is any edited file used as input of a production read path
  test later in the series?
* The captures come from three local loopback clusters on Java 8, not from the lab. Is that provenance adequate for
  the fixtures that later patches treat as ground truth (handshakes, commit log segment headers, transaction logs with
  millisecond times)?

## P28 — the DSE_5_1 mode and the 5.1 peer identity (#2782)

**Tier:** 2 — a new `DSE_5_1` value of `internode_compatibility_mode` whose effects are refused starts and refused
settings; the shared DSE 6 code it refactors must keep its DSE 6 behaviour byte for byte.

**Size and shape:** 13 production files, ~590 added lines (new `schema/Dse51Compressors` ~140; `DatabaseDescriptor`
+177/−38; `DseCompat` +98; `AuthConfig` +68/−17; `InternodeCompatibilityMode` +43; small edits to `StartupChecks`,
`StorageProxy`, `CassandraDaemon`, `Config`, `CassandraRelevantProperties`, `MessagingServiceMBean`, both
`conf/*.yaml`). 12 test files, ~1,430 lines (new `Dse51ModeConfigTest`, `Dse51ModeToolTest`, `Dse51ModeClientTest`,
`Dse51CompressorsTest`, in-JVM `Dse51CompressorsStartupTest`; additions to `DseCompatTest`, `AuthConfigDse6Test`,
`CompatStatusTest`; the in-JVM `Instance` mirrors the daemon hook). Six commits on the branch, all titled as the patch.
The yaml and javadoc text can be skimmed.

**Gating:** `internode_compatibility_mode: DSE_5_1` (or `-Dcassandra.internode_compatibility_mode`). New gates:
`InternodeCompatibilityMode.isDse51()/isDse()`, `DatabaseDescriptor.isDse51CompatibilityMode()`,
`isDseCompatibilityMode()`, `DseCompat.isDse51()`. `DseCompat.enabled()` stays false in `DSE_5_1`, so no pre-4.0 code
path opens. For a cluster that never had DSE: no behaviour change; the text of the "unknown value" refusal now reads
"expected NONE, DSE_6 or DSE_5_1", and `checkDsePeersWithoutFlag` formats the running mode into its refusal (still
"NONE" for such a node).

**Where to look**
* `src/java/org/apache/cassandra/config/DatabaseDescriptor.java` — `applyInternodeCompatibilityMode` /
  `applyDse51SSTableOutput`: requires `storage_compatibility_mode: HCD_1` and `paxos_variant: v1`, refuses a
  `selected_format` other than `big` (forces it for a client), forces numeric generations. Does the tool path (mode
  requested but `internodeCompatibilityMode` reset to `NONE`) run exactly the checks the yaml comment promises?
* same file — `checkDseFlushAndCompactionCompressors`, `forceNumericSSTableGenerations`: extracted from
  `applyDse6SSTableOutput` and parameterised by mode. Confirm the DSE_6 messages and the client/daemon log-level split
  are unchanged.
* `src/java/org/apache/cassandra/schema/Dse51Compressors.java` — `verifyAtStartup` / `tables`: refuses the start while
  any table or view uses a compressor other than LZ4/Snappy/Deflate, or is encrypted
  (`getSstableCompressor().encryptionOnly() != null`). Is a table with compression disabled really readable by DSE 5.1
  (`if (!compression.isEnabled()) continue`), and are system/virtual keyspaces meant to be included?
* `src/java/org/apache/cassandra/service/CassandraDaemon.java` — `setup`: the compressor check runs after the schema is
  loaded and before `setupVirtualKeyspaces`. Is that before any keyspace is opened and before anything is flushed, as
  the class javadoc claims?
* `src/java/org/apache/cassandra/net/DseCompat.java` — `isDse51Peer`, `isDse51ReleaseVersion`, `dse51Patch`: a known
  messaging version 10 or 11 decides alone, so any pre-4.0 peer is a DSE 5.1 peer; otherwise release `3.11.3.x` or
  `dse_version` `5.1.<n>`. No production caller in P28–P30; check the rule where it is first used.
* `src/java/org/apache/cassandra/auth/AuthConfig.java` — `checkDse51CompatibilityMode`,
  `refuseWhatNeedsTablesDseLacks(mode, …)`: the same three refusals as DSE 6. Is the DSE_6 text unchanged (the network
  authorizer branch keeps `DseSystemKeyspaces.DEFERRED` and "until then")?
* `src/java/org/apache/cassandra/service/StorageProxy.java` — `setPaxosVariant`: in DSE_5_1 a non-v1 variant is
  "refused" here, but `DseUpgradeGuards.refuseWhileUpgrading` returns early because `upgradeInProgress()` is false
  (`DseCompat.enabled()` is false), so the actual refusal comes from `Paxos.setPaxosVariant` →
  `checkPaxosVariantAllowed` and is not counted. Is that the intended behaviour?

**Modified or deleted existing code**
* `DatabaseDescriptor.applyInternodeCompatibilityMode`, `applyDse6SSTableOutput`, `isDse6PaxosVariant`,
  `checkPaxosVariantAllowed` — generalised to DSE_5_1 — History: added by this series (P02; the
  Paxos check by the DSE 6 operational guards patch, P15).
* `AuthConfig.refuseWhatNeedsTablesDseLacks`, `dse6Refusal` → `dseRefusal`, `storage50Tables` — History: added by this
  series (DSE 6 schema exchange patch, P14).
* `StartupChecks.checkDsePeersWithoutFlag` — message formats the mode — History: added by this series (P05, 2026).
* `StorageProxy.setPaxosVariant` — gate widened from `isDse6CompatibilityMode` to `isDseCompatibilityMode` —
  History: added by this series (P15).
* `InternodeCompatibilityMode` — enum gains a `source` field and `DSE_5_1` — History: added by this series (P02).

**Failure and rollback risks**
* Rollback: the patch states the node still writes big `nb` sstables (tools `oa`), which DSE 5.1 does not read, so
  after this code runs a node can return to DSE 5.1 only from its pre-upgrade snapshot. The mode does not connect to
  DSE 5.1 nodes at this patch.
* The upgrade guards (`DseUpgradeGuards.upgradeInProgress`) stay inactive in DSE_5_1 at this patch, so the operations
  they refuse in DSE_6 (topology changes, repair, schema changes) are not refused here.
* `Dse51Compressors` notes that what the node wrote for a table created or altered with an unsupported compressor while
  in the mode stays on disk until rewritten; the start refusal only fires at the next start.
* Concurrency: none.

**Test integrity**
* Existing tests changed: `InternodeCompatibilityModeTest` (expected message updated to the new value list);
  `Dse6StartupChecksTest.theFilesDseWroteAreNotFlagged` now filters out every path for which
  `Dse51Fixtures.isDse51Fixture` is true (narrowed scope, needed because the new fixtures hold version-6 segments);
  `Dse6StartupChecksTest` gains `dse51IsRefusedWhileTheMarkerExists`; `AuthConfigDse6Test`, `DseCompatTest`,
  `CompatStatusTest` additions only.
* New tests: real configuration loading (`Dse51ModeConfigTest`, `Dse51ModeToolTest`, `Dse51ModeClientTest`); in-JVM
  `Dse51CompressorsStartupTest` (real start refused); `DseCompatTest.theRuleRecognisesTheCapturedDse51NodesBy…` runs
  the peer rule on the captured gossip and handshakes of P27. `AuthConfigDse6Test` mocks
  `MutualTlsWithPasswordFallbackAuthenticator`.
* Not covered by any test in the patch: the runtime `setPaxosVariant` path in DSE_5_1 through `StorageProxy` (counted
  vs. not counted).

**Suggested reviewer questions**
* `isDse51Peer` treats every peer at messaging version 10/11 as DSE 5.1. What prevents an Apache Cassandra 3.11 node
  (or a DSE 5.1 node started with `force_3_0_protocol_version`) from being misclassified when the rule is wired in?
* Should a table with compression disabled, or a system keyspace table, be subject to `Dse51Compressors`?
* Is it acceptable that the DSE_5_1 mode, unlike DSE_6, does not activate the upgrade guards at this patch, given the
  yaml text tells operators to set it on every node of a cluster being upgraded?

## P29 — startup checks, upgrade marker, commit log, hints and transaction logs (#2783)

**Tier:** 1 — changes the on-disk transaction log records, the commit log segment handling and the sstable statistics
written in DSE_5_1, adds start refusals for `NONE` nodes, and changes one hints read path in every mode.

**Size and shape:** 18 production files, ~650 added / ~230 removed (`StartupChecks` +233/−98; `DseTransactionLogs`
+86; `DsePeersRecord` +79; `DseUpgradeMarker` +59; `HintsDispatcher` +39; `DatabaseDescriptor` +31; `CommitLog` +19;
`LogRecord`, `LogFile`, `HintsDescriptor`, `HintsCatalog`, `MetadataCollector`, `Dse6SaiVersionPin`,
`Dse51Compressors`, `Config`, `InternodeCompatibilityMode`, both `conf/*.yaml`). 18 test files, ~2,170 lines (new
`Dse51StartupChecksTest` 501, `LogTransactionDse51Test`, `DseHintsFileTest` 205, in-JVM `Dse51ModeSmokeTest`; large
additions to `DseTransactionLogsTest` +546, `DseCommitLogReplayTest` +184, `DseHintsContentTest` +135). Four commits.
Javadoc/yaml/log-text changes can be skimmed; the lifecycle and hints hunks cannot.

**Gating:** `DSE_5_1` via `isDse51CompatibilityMode()` (node only), and the new `isDseSSTableOutput()` /
`isDse51SSTableOutput()` (node and offline tools that read the yaml). Always-on changes for a cluster that never had
DSE:
* `StartupChecks.checkDsePeersWithoutFlag` in `NONE` now also checks `<metadata_directory>/dse51-upgrade-in-progress`
  and `dse51-peers-seen` (only when no DSE 6 file is found) and refuses the start if either exists.
* `HintsDescriptor.loadStatsComponent` catches `NoSuchFileException` as well as `FileNotFoundException`: a hints file
  without its `-Statistics.hints` companion is loaded with a WARN instead of an ERROR with stack trace (the commit
  message states that this case was expected).
* `LogRecord.makeForSSTable` now always passes through `DseTransactionLogs.recordPath` (identity outside DSE_5_1).

**Where to look**
* `src/java/org/apache/cassandra/db/lifecycle/LogRecord.java` — `makeForSSTable`: in DSE_5_1 every sstable record
  names the absolute path without the trailing `-`, and REMOVE records take millisecond times including the stats
  file. Do the in-memory records (`makeAddRecord`, `contains`) and the on-disk ones always agree within one process,
  including a tool that runs in the mode?
* `src/java/org/apache/cassandra/db/lifecycle/LogFile.java` — `verify`: in DSE_5_1 a record missing from
  `recordFiles` is looked up again by `DseTransactionLogs.uriOfRecordPath(key)`. The javadoc of `DseTransactionLogs`
  states that once the flag is dropped such records no longer match their files and the log completes without the
  update-time check. Is skipping the check safe for a committed REMOVE whose files were restored?
* `src/java/org/apache/cassandra/db/lifecycle/LogRecord.java` — `getExistingFiles(Set)`: the `floor` prefix match now
  also sees prefixes ending in the format (`nb-11-big`) rather than `-`. Check against mixed URI/path prefixes in one
  directory.
* `src/java/org/apache/cassandra/db/lifecycle/DseTransactionLogs.java` — `recordPath`, `DSE_51_OPERATION_TYPES`: the
  patch states DSE 5.1.51 writes the canonical path. If a data directory is reached through a symlink, will
  `uriOfRecordPath` of DSE's canonical path find the files this software lists under the configured path?
* `src/java/org/apache/cassandra/service/StartupChecks.java` — `checkDse6Artefacts` (DSE_5_1 branch:
  `isSSTableWrittenWithoutDse51Mode`, `isCommitLogSegmentWrittenWithoutDse51Mode`) and `refuseWhileDsePeersMayRemain`:
  the big version is compared as a string with `"oa"`; any commit log version above `VERSION_40` is refused. Do these
  rules match every file the node and its tools write in the mode?
* `src/java/org/apache/cassandra/db/commitlog/CommitLog.java` — `moveAsideReplayedDseSegment`: in DSE_5_1 every
  replayed version-6 segment is renamed `.replayed`. Any version-6 segment (Apache 3.x too) is treated as DSE 5.1's; is
  that the intent?
* `src/java/org/apache/cassandra/hints/HintsDescriptor.java` — `loadStatsComponent`: always-on catch change. Confirm
  nothing downstream relied on the old ERROR path.
* `src/java/org/apache/cassandra/io/sstable/metadata/MetadataCollector.java` —
  `defaultCellPerPartitionCountHistogram`: 114 buckets now also in DSE_5_1 and for tools reading such a yaml.

**Modified or deleted existing code**
* `LogRecord.makeForSSTable`, `LogFile.verify`/`toString`/`fileName`, `DseTransactionLogs.isDse6RemoveRecord` →
  `isDseRemoveRecord`, `dseOperationType`; `DseTransactionLogs.removeRecordTime` deleted — History: added by this
  series (P02). The surrounding code comparing update times truncated to seconds dates from the
  JDK 8/11 resolution fix noted inline; the stats-file exclusion is CASSANDRA-18119 (cited in the javadoc).
* `LogRecord.getExistingFiles(Set)` — only a comment changed; the prefix rule ("prefixes always end with '-'")
  comes from CASSANDRA-12261 (cited in `LogFile`); the new record form no longer ends with `-`, so that invariant is
  replaced by the "ends with the format" argument in the comment.
* `HintsDescriptor.loadStatsComponent` — catch widened — History: `c5bbd0838fc`, 2024, CNDB-9185 (hints-on-disk
  metrics) added the statistics component and its `FileNotFoundException` fallback; statistics feed metrics only.
* `CommitLog.moveAsideReplayedDseSegment`, `HintsDispatcher.warnUnacknowledgedByDse6` → `warnUnacknowledgedByDse` —
  History: added by this series (DSE 6 commit log and hints patch, P08).
* `StartupChecks.checkDse6Artefacts`, `checkDsePeersWithoutFlag` (body moved to `refuseWhileDsePeersMayRemain`),
  `writeUpgradeMarker`, `checkDse6Ec2NamingScheme`, `loadDsePeersRecord`; `DseUpgradeMarker`, `DsePeersRecord`
  (constant `TEMPORARY_FILE_NAME` replaced) — History: added by this series (P05, 2026).
* `MetadataCollector.defaultCellPerPartitionCountHistogram`, `DatabaseDescriptor.dse6SSTableOutput` (replaced by
  `sstableOutputMode`) — History: added by this series (P02).

**Failure and rollback risks**
* Rollback: the patch's own javadoc states that a DSE 5.1.51 on a Java 8 build without the JDK-8177809 fix reads whole
  seconds and would refuse the millisecond REMOVE records; it relies on the rollback procedure draining the node so no
  transaction log is left.
* Rollback: the start check accepts big `nb` and `oa` sstables and version-7 commit log segments, none of which DSE 5.1
  reads (stated by the patch); "every file listed is one DSE 5.1 could not read" does not mean every file kept is
  readable.
* Flag drop: a DSE 5.1-style log left on disk is checked without its files (see `LogFile` above); the javadoc says a
  corrupt last record then refuses it.
* The DSE 5.1 peers record is never maintained at this patch (`DseCompat.enabled()` is false in DSE_5_1); only the
  marker can refuse a later `NONE` start. With the override property set and a DSE 6 file present, the DSE 5.1 files
  are not even reported (intentional per the javadoc).
* Marker write failure is logged, not thrown (`writeUpgradeMarker`), as in DSE 6: a node can leave the mode without the
  `NONE` refusal if the marker could not be created.
* Any `NONE` node whose metadata directory holds a stray `dse51-upgrade-in-progress` is refused at start.

**Test integrity**
* Existing tests changed: `Dse6StartupChecksTest` (marker content assertion strengthened from `contains` to exact
  `startsWith`/`endsWith`); `Dse51ModeConfigTest` (the P28 assertion "no DSE 6 transaction logs or histograms"
  replaced by `isDseSSTableOutput()`/`isDse51SSTableOutput()` true); `Dse51CompressorsTest` (expected refusal text
  updated); other existing files only gain cases.
* New tests: real files and real startup checks in `Dse51StartupChecksTest`, `DseHintsFileTest` (captured hints file,
  and a file of this software without companion, in `NONE`), `LogTransactionDse51Test` (runs every
  `LogTransactionTest` case in DSE_5_1), in-JVM `Dse51ModeSmokeTest`. `DseTransactionLogsTest` checks the record builder
  line by line against both captured logs; its three tests on a committed log use the captured log rewritten into a
  test directory with paths changed and checksums recomputed (input not byte-identical to DSE's).
  `DseCommitLogReplayTest` replay cases keep a generated version-6 segment (the captured ones stop replay on DSE
  columns until P30); the header is compared with the captured one.
* Not covered by any test in the patch: rollback to a real DSE 5.1 node reading the transaction logs this mode writes
  (no DSE process in the tests); the symlinked data directory case.

**Suggested reviewer questions**
* Is it safe that, after the flag is dropped, a DSE 5.1-style REMOVE record is completed without its update-time check?
  Would it be safer to keep the path-to-URI lookup in every mode?
* The always-on `NoSuchFileException` catch in `HintsDescriptor`: is there any case where a missing statistics
  component should still be an ERROR (e.g. a corrupted hints directory), and should it be a separate change?
* Should `checkDse6Artefacts` in DSE_5_1 reject `nb`/`oa` sstables once a later patch makes the node write a format
  DSE 5.1 reads, and is the `"oa"` string comparison robust for future versions?
* Does treating every version-6 commit log segment as DSE 5.1's (renamed aside after replay) risk hiding a segment
  that should be replayed again after a failed start?

## P30 — system.local and system.peers written by DSE 5.1 (#2784)

**Tier:** 1 — changes the definitions of `system.local` and `system.peers` in every mode (five recorded dropped
columns) and touches the row deserialization path (`Columns.deserialize`, `SerializationHeader`) and startup order.

**Size and shape:** 8 production files, ~830 added (new `db/Dse51SystemTables` 739; `CommitLogReader` +32; `Columns`
+16; `StartupChecks` +16; `CassandraDaemon` +13; `TableMetadata` +11; `SerializationHeader` +5/−2; `SystemKeyspace`
+4). 6 test files, ~2,430 lines (new `Dse51SystemTablesTest` 1,519; `DseCommitLogReplayTest` +568/−18;
`SerializationHeaderTest` +148; in-JVM `Dse51ModeSmokeTest` +128; `Dse51StartupChecksTest` +54; `Instance` +12).
Two commits. The logging and record format code in `Dse51SystemTables` can be skimmed after the read path.

**Gating:** the read and record of the values, the commit-log replay of mutations naming the five columns, and the
INFO counter are gated by `isDse51CompatibilityMode()` (and messaging version 10 for the replay). Always-on changes
for a cluster that never had DSE:
* `SystemKeyspace` — `system.local` and `system.peers` record `workload`, `workloads`, `graph`, `dse_version`,
  `server_id` as deprecated system columns (dropped at `Long.MAX_VALUE`), so their `TableMetadata` (equals, `DESCRIBE …
  WITH INTERNALS`) changes and the commit message states ten more rows are written to `system_schema.dropped_columns`
  at every start; `system.peers`, which had no dropped column, now takes the dropped-column path.
* sstables of those tables naming the five columns are read in every mode with the cells skipped.
* `Columns.deserialize` now nulls a found dropped column if it is one of the five and not `storedByDse51`, i.e. such a
  name in a mutation is still refused outside DSE_5_1.

**Where to look**
* `src/java/org/apache/cassandra/db/SystemKeyspace.java` — `Local` / `LegacyPeers` builders with
  `recordDeprecatedSystemColumns(Dse51SystemTables.COLUMNS)`: what reads the saved local keyspace definitions
  (`SchemaKeyspace.saveSystemKeyspacesSchema`), and does a downgrade to HCD 1.x, or a mixed HCD 2 cluster with nodes
  without this patch, care about the extra `dropped_columns` rows? (`calculateSchemaDigest` skips system keyspace
  partitions, so the schema version is not affected.)
* `src/java/org/apache/cassandra/db/Columns.java` — `Serializer.deserialize(…, storedByDse51)`: the new nulling only
  applies when the dropped column is one of the five on these two tables. Confirm no other caller of the dropped-column
  fallback on `system.local`/`system.peers` (e.g. `thrift_version`) changes.
* `src/java/org/apache/cassandra/db/SerializationHeader.java` — `deserializeForMessaging`:
  `Dse51SystemTables.readsStoredDse51Columns(metadata, version)` is evaluated for every update decoded; it is true only
  in DSE_5_1, at version 10, for these two tables. The patch argues no peer sends these local tables; is that true for
  every verb, including streaming and repair?
* `src/java/org/apache/cassandra/db/Dse51SystemTables.java` — `loadOrReadAtStart`, `read(ColumnFamilyStore, …)`,
  `partitions`: reads every live big sstable of both tables fully into memory with a widened `TableMetadata`
  (`withDse51Columns`) through `SSTableSimpleIterator`, before `persistLocalMetadata` and before commit log replay. Are
  compactions of `system.local`/`system.peers` impossible at that point, and is the `selectAndReference` view enough?
* same file — `deleteRecordOfAnEarlierUpgrade` / static `earlierRecordNotDeleted` and
  `StartupChecks.checkDse6Artefacts.postAction`: the record is deleted only when the DSE 5.1 marker is absent. If a
  rolled-back node keeps its marker and is upgraded again, the old record is reused; is the procedure guaranteed to
  delete both?
* `src/java/org/apache/cassandra/db/commitlog/CommitLogReader.java` — `readMutation` / `countDse51Columns`: per-segment
  counter fields reset in `readCommitLogSegment`; only logging. Is the reader ever shared across concurrent segment
  reads?
* `src/java/org/apache/cassandra/service/CassandraDaemon.java` — `setup`: new call before
  `SystemKeyspace.persistLocalMetadata()` (the "first write to SystemKeyspace" ordering of CASSANDRA-11742). Is any
  other write to `system.local` possible before it?

**Modified or deleted existing code**
* `SystemKeyspace` local/peers definitions — History: the `recordDeprecatedSystemColumn("thrift_version", …)` pattern
  comes from `4881d9c308c`, 2016, CASSANDRA-11115 (Thrift removal); `TableMetadata.Builder.recordDeprecatedSystemColumn`
  from `af3fe39dcab`, 2016 (immutable TableMetadata), which asserts the keyspace is a local system keyspace because it
  "plays fast and loose with the removal timestamp".
* `Columns.Serializer.deserialize` dropped-column fallback — History: `a991b64811f`, 2014, CASSANDRA-8099; invariant:
  data for dropped columns must deserialize; the patch adds a refusal on top of it for five names. The
  `storedByDse`/`dseViewHiddenColumns` parameters were added by this series.
* `SerializationHeader.Component.toHeader` (comment only) and `deserializeForMessaging` (new argument) — History of the
  dropped-column lookup: `af3fe39dcab`, 2016; the DSE view arguments were added by this series.
* `CassandraDaemon.setup` ordering — History: `360541f16a1`, 2016, CASSANDRA-11742 (persist local metadata as the first
  write).
* `StartupChecks.checkDse6Artefacts.postAction`, `writeUpgradeMarker` — History: added by this series (P05, P29).

**Failure and rollback risks**
* Values that only the commit log holds (DSE 5.1 stopped without drain) are not read: replay runs after
  `loadOrReadAtStart` and drops those cells (stated by the patch).
* If an sstable cannot be read, no record is written and the start continues; a compaction before the next start can
  rewrite the rows without the values (the WARN says to restart now). With the default `disk_failure_policy: stop`, a
  corrupt sstable stops the start (stated in the javadoc).
* The values are consumed by later patches only (`Dse51SystemTables.stored()` has no production caller in P30), so a
  wrong or missing record has no effect until then.
* Rollback to DSE 5.1: the system tables this node rewrites drop the five columns' values; the patch states DSE 5.1
  re-adds the columns at every start, and its persister rewrites them; not established from this repository.
* The record (`dse51-system-columns`) is written atomically (temp, sync, move, dir sync); an unreadable record is
  renamed aside; a rename failure logs an ERROR and every later start re-reads from sstables.

**Test integrity**
* Existing tests changed: `DseCommitLogReplayTest` — the P29 test `dse51ModeReplaysADse51SegmentAndMovesItAside` now
  replays the captured segment instead of a generated one (with stronger counts); `noneModeKeepsADse51Segment…` is
  kept, and new tests show the captured segments stop the replay in `NONE` and `DSE_6`; `Dse51StartupChecksTest`
  updates one expected ERROR text; `Dse51ModeSmokeTest` javadoc changed.
* New tests: `Dse51SystemTablesTest` reads the captured flushed and compacted sstables of both nodes (real DSE files)
  plus generated ones for deleted/expired/merged values; in-JVM `Dse51ModeSmokeTest` starts on captured sstables and
  checks the record and its reload; `SerializationHeaderTest` uses generated headers (tests the rule).
  `DseCommitLogReplayTest` removes a `system.peers` truncation record left by earlier tests before replaying.
* Not covered by any test in the patch: a downgrade or mixed-version read of the new `system_schema.dropped_columns`
  rows; concurrent compaction of `system.local` during `loadOrReadAtStart`; streaming/repair of these tables (local, so
  presumably impossible).

**Suggested reviewer questions**
* Is changing the definition of `system.local` and `system.peers` in every mode (rather than only in DSE_5_1) the right
  trade-off, given `DESCRIBE … WITH INTERNALS` and `system_schema.dropped_columns` change for all HCD 2 users?
* `recordDeprecatedSystemColumn` uses `Long.MAX_VALUE` as the drop time: any future need to add a real column with one
  of these names (`workload`, `graph`, `server_id`…) to `system.local` would be silently dropped. Is that acceptable?
* Why record the values in a side file rather than reading them once into a table of this software, and who deletes
  `dse51-system-columns` and its `.unreadable-*` copies after the upgrade?
* Should the replay without the mode, which stops on the unknown column `dse_version`, give an actionable error naming
  `DSE_5_1`?

## Group notes (P27 to P30)
* The four patches build on the DSE 6 series (P02 flag, P05 startup checks and marker, the DSE 6 commit log/hints and
  guards commits): most modified lines in P28–P30 were added by this series, and each patch generalises a DSE_6
  helper to a `mode` argument. The DSE 6 behaviour and texts should be checked for byte-identity in every such
  refactor (`AuthConfig`, `DatabaseDescriptor`, `StartupChecks.refuseWhileDsePeersMayRemain`, `DsePeersRecord`,
  `DseUpgradeMarker`).
* P27's fixtures drive P28 (gossip/handshake rule), P29 (transaction logs, hints, segment headers) and P30 (system table
  sstables, captured segments); P29 and P30 each carry a "on the DSE 5.1.51 fixtures" follow-up commit replacing
  generated inputs where captures exist, so the original commit of each patch alone was tested on generated artefacts.
* Two checks are duplicated between `CassandraDaemon.setup` and the in-JVM `Instance`
  (`Dse51Compressors.verifyAtStartup` in P28, `Dse51SystemTables.loadOrReadAtStart` in P30); the in-JVM tests cover
  the copy, not the daemon.
* In P28–P30, `DSE_5_1` keeps `DseCompat.enabled()` false: no pre-4.0 dialect, no upgrade guards, the DSE 5.1 peers
  record never maintained, `isDse51Peer` and `Dse51SystemTables.stored()` without production callers. Their consumers
  are outside this group and should be reviewed against these definitions.
* Startup ordering to keep in mind across P29 and P30: startup checks (P29 artefact refusal, then the post action that
  deletes the old record and writes the marker) → `Dse51SystemTables.loadOrReadAtStart` (P30) →
  `persistLocalMetadata` → commit log replay (P29 move-aside of version-6 segments, P30 replay of the five columns).

## P31 — messaging, peers, guards and schema with DSE 5.1 nodes (#2785)

**Tier:** 1 — it opens the pre-4.0 wire path (codec, connections, inbound framing) to `DSE_5_1`, changes how stored
batches and Paxos rows at messaging versions 10 and 11 are decoded with `NONE` and in `DSE_5_1` (not in `DSE_6`),
adds a new refusal to `DSE_6` starts, and touches the hot inbound message path (`InboundMessageHandler`).

**Size and shape:** about 70 production files, roughly +1,500 / -570 lines, most of them one-line gate swaps
(`DseCompat.enabled()` to `DseCompat.isDse6()` or the reverse) and text changes that make messages name "DSE 5.1"
(`DseCompat.source()` / `textMode()`). The new logic sits in about ten files: `InboundMessageHandler`, `Message`,
`DataLimits`, `ReadCommand`, `RowFilter`, `DsePeers`, `Dse51Compressors`, `AlterSchemaStatement`,
`DefaultSchemaUpdateHandler`, `StartupChecks`, `DseCompat`. 31 test files (+3,250 lines: 5 new unit classes, 26
modified). The comment-only edits in `HandshakeProtocol`, `InboundConnectionInitiator`, `OutboundConnection*`,
`ForwardingInfo`, `FrameDecoder`, `LegacyFlag`, `ParamType`, `RequestCallbacks`, `PagingState`, `ColumnFilter`,
`Mutation`, `CounterMutation`, `PrepareMessage` and the yaml files can be skimmed.

**Gating:** `DseCompat.enabled()` now means `DatabaseDescriptor.isDseCompatibilityMode()` (`DSE_6` or `DSE_5_1`).
Before this patch it meant `DSE_6` only, so every pre-4.0 path, operational guard and schema freeze of the DSE 6 work
now also runs in `DSE_5_1`. The gossip, TLS, view wire-format, access-control and dialect-seeding sites are pinned to
`DseCompat.isDse6()` (P32 and P33 reopen several of them). New `DSE_5_1`-only branches use `DseCompat.isDse51()`.
These changes reach nodes that are not in `DSE_5_1`:
* With `NONE`: `DseCompat.storedDecodingVersion` now maps stored versions 10 and 11 (`VERSION_30`, `VERSION_3014`)
  to `VERSION_40` for `system.batches` and `system.paxos` rows (and the preflight `PaxosRollback`). Before, they were
  returned unchanged. `DsePaxosRowsTest` changes its `NONE` assertion to match.
* With `DSE_6`: `StartupChecks.checkDsePeersWithoutFlag` now refuses a start while `dse51-upgrade-in-progress`
  exists, or while `dse51-peers-seen` exists and lists a peer or cannot be read.
* In every mode: `InboundMessageHandler` has a new `catch (DseRefusedMessageException)`.
  `DefaultSchemaUpdateHandler` calls `refusesDse51Compressors` on every schema push and pull answer, a no-op
  outside `DSE_5_1`. `SchemaAnnouncementEvent` takes its statement through `AlterSchemaStatement.statementOf`, which
  is equivalent outside `DSE_5_1`.

**Where to look**
* `src/java/org/apache/cassandra/net/InboundMessageHandler.java` — `processOneContainedMessage`,
  `processFirstFrameOfLargeMessage`, `SkippedRepairMessage`: in `DSE_5_1`, a pre-4.0 message with verb id 32 is
  sniffed before `extractHeader`, counted, optionally answered, and its bytes are skipped. For a large message,
  `SkippedRepairMessage` swallows the remaining frames, and it is built with `isExpired = true` and
  `expiresAtNanos = Long.MAX_VALUE`. Is the byte and capacity accounting right when the frames are skipped, and is
  `largeMessage` always cleared?
* `src/java/org/apache/cassandra/net/Message.java` — `Serializer.pre40RepairMessage`, `pre40FailureResponse`: the
  repair message is parsed by hand (address length, verb, params, payload size). Can a truncated first frame make
  `extractParams` or `buf.getInt(index)` read past `limit`, or throw something other than the
  `InvalidPre40MessageException` the caller expects? Is a failure response with `RequestFailureReason.UNKNOWN` and no
  `REQUEST_VERB_NAME` what a DSE 5.1 coordinator reads as a failure?
* `src/java/org/apache/cassandra/net/DseCompat.java` — `storedDecodingVersion`: the `pre40 && !isDse6()` branch is
  the one change on persistent-state decoding that runs with `NONE`. Confirm that decoding a 10/11 row at
  `VERSION_40` is byte-compatible for mutations and partition updates (the javadoc says it is the 3.0 layout).
  Confirm also that a node that never had DSE cannot meet such a row in a way that used to fail safely and now
  decodes wrongly.
* `src/java/org/apache/cassandra/db/filter/DataLimits.java` — `Serializer.serialize/deserialize`: in `DSE_5_1` the
  pre-4.0 layout writes and reads the enum ordinals (GROUP BY at 4 and 5) instead of the DSE 6 shifted ordinals, and
  ordinals 2 and 3 (Thrift kinds) are refused. A wrong ordinal here silently changes paging or grouping of a read.
* `src/java/org/apache/cassandra/schema/DefaultSchemaUpdateHandler.java` — `refusesDse51Compressors`,
  `applyMutationsFromCoordinator`, the schema push handler: one refused table drops the whole batch of mutations
  received. Is the schema disagreement and repeated pull loop this leaves acceptable, and is it ever reached outside a
  misconfiguration?
* `src/java/org/apache/cassandra/schema/Dse51Compressors.java` — `refusedSchemaMutations`, `refusal`,
  `refusedStatementChanges`: the compression is read back from raw `system_schema.tables`/`views` rows. A class this
  node cannot load counts as unreadable, and encryption is detected by `Encryptor.class.isAssignableFrom` without
  initialising the class.
* `src/java/org/apache/cassandra/cql3/statements/schema/AlterSchemaStatement.java` — `execute`,
  `Dse51CompressorsCheck`: a second wrapper around the transformation, applied under the same lock. Check that it
  composes correctly with `FrozenSchemaStatement` (wrapper order) and with `fixedTimestampMicros`.
* `src/java/org/apache/cassandra/service/StartupChecks.java` — `checkDsePeersWithoutFlag`,
  `refuseWhileDsePeersMayRemain`: the new `DSE_6` refusal uses `Files.exists` (false when the directory cannot be
  searched), while the `NONE` path fails closed. Is that asymmetry intended?

**Modified or deleted existing code**
* `src/java/org/apache/cassandra/net/InboundMessageHandler.java` — `processOneContainedMessage`,
  `processFirstFrameOfLargeMessage`: a sniff before `extractHeader`, and a new catch beside the
  `IncompatibleSchemaException` one. — History: 310a48efcfd, 2019, CASSANDRA-15066 (rewrite of internode messaging).
  The invariant is that every received frame is either dispatched or reported through `callbacks` with exact
  received counts. The skipped repair message bypasses `callbacks.onHeaderArrived` and similar calls by design.
* `src/java/org/apache/cassandra/db/ReadCommand.java` — `Serializer.deserialize`: the Thrift-flag
  `IllegalStateException` now becomes a counted `DseRefusedMessageException` in `DSE_5_1` at pre-4.0 versions.
  — History: 4881d9c308c, 2016, CASSANDRA-11115 (Thrift removal), whose loud failure assumed no Thrift peer in a 4.0
  cluster. Unchanged outside `DSE_5_1`.
* `src/java/org/apache/cassandra/db/filter/RowFilter.java` — `Expression.Serializer.deserialize`: kind 2 (`UNUSED1`)
  is refused before its bytes are read. — History: the `Kind.fromVal(in.readByte())` line was last touched by
  b73199b7fd3, 2023 (GEO_DISTANCE); the kind numbering comes from Apache Cassandra 3.x.
* `src/java/org/apache/cassandra/db/ColumnFamilyStore.java` — `setCompressionParameters`: a JMX compression change is
  refused in `DSE_5_1`. — History: b59b832eba0, 2023 (removal of `crc_check_chance` from `CompressionParams`); a
  local override that the schema and the start check do not see.
* `src/java/org/apache/cassandra/schema/SchemaAnnouncementEvent.java` — constructor: `instanceof CQLStatement` is
  replaced by `AlterSchemaStatement.statementOf`. — History: 2846b22a70d, 2017, CASSANDRA-13457 (diagnostic events).
* `src/java/org/apache/cassandra/schema/DefaultSchemaUpdateHandler.java` — `applyMutationsFromCoordinator` becomes
  `@VisibleForTesting` package-private, and the push handler gains a guard. — History: 2b2c6decfaf, 2021,
  CASSANDRA-17044 (pluggable schema management); the pull/push reset invariant under `synchronized`.
* `src/java/org/apache/cassandra/net/DseCompat.java` (`enabled`, `storedDecodingVersion`), `DsePeers`,
  `DseDialectVersions`, `DseTlsPorts`, `DseGossipStates`, `EndpointState`, `Gossiper`, `GossipDigestAck*`,
  `VersionedValue.networkVersion`, `MessagingService.seedDseDialectPeers`, `DseUpgradeGuards`, `DseReadGuard`,
  `Dse6AccessControlCheck`, `DseViewWireFormat`, `CassandraAuthorizer.maintainsResourceIndex`, `PartitionDenylist`,
  `DseSystemKeyspaces`, `BatchlogManager` — added by this series (P02, P05, P06, P10, P11, P12, P14, P15, P16, P18,
  P19, P20, P28).
* `src/java/org/apache/cassandra/service/StartupChecks.java` — `checkDsePeersWithoutFlag`: added by this series (P05,
  extended in P29).

**Failure and rollback risks**
* Hot path: every pre-4.0 message received in `DSE_5_1` is parsed twice (`pre40RepairMessage`, then
  `extractHeader`). This costs nothing with `NONE`/`DSE_6`, where `isDse51()` is false.
* Partial schema refusal: a node of this software not in the mode that alters one table's compression makes
  `DSE_5_1` nodes reject all its later schema pushes and pull answers until that change is reverted. Unrelated DDL
  from that node is lost meanwhile, and only a NoSpam WARN every 10 minutes and a counter show it.
* Rollback and state: with `NONE`, a stored batch or Paxos row at 10/11 is now decoded as `VERSION_40`. If that is
  wrong for some layout, the failure is a decoding error on replay (`UndecodableBatchException` path), not a refusal
  at start. A batch at versions 1 to 9 is kept and never replayed (`BatchlogManager`). The ERROR names it, but the
  row stays until removed.
* `DSE_6` behaviour change: a node of a DSE 6 upgrade that kept DSE 5.1 files from an older upgrade now refuses to
  start. The override `ignore_dse_peers` lets it through, but then writes the DSE 6 marker, which refuses `DSE_5_1`
  later.
* Interim state of the stack: in this patch alone, `DSE_5_1` talks to DSE 5.1 nodes, but gossip still sends all
  states every round (`sendsAllOwnStates` returns true outside `DSE_6`) and publishes no DSE state. TLS ports and the
  plain-text fallback guard are off (DSE 6 only), and the access-control and non-core checks do not run. P32 and P33
  close these gaps.

**Test integrity**
* Existing tests changed:
  * `Dse51ModeConfigTest` and `DseCompatTest`: the assertions that `DseCompat.enabled()` and `isPre40Layout` are
    false in `DSE_5_1` are inverted. `Dse51ModeConfigTest` now expects `UnsupportedOperationException` (a guard)
    instead of `IllegalArgumentException` for the MBean operation.
  * `Dse51StartupChecksTest`: `dse6DoesNotReadTheFilesOfTheUpgradeFromDse51` is replaced by
    `dse6IsRefusedWhileTheDse51MarkerExists`, a reversed expectation. The `DSE_5_1` record remedy text changes from
    "delete it by hand" to "empties itself".
  * `DsePaxosRowsTest`: the `NONE` identity assertion on `storedDecodingVersion` is changed for 10/11.
  * `DsePeersRecordTest`: "nothing records a peer in `DSE_5_1`" is replaced by recording assertions.
  * `CompatStatusTest`: "not tracked / not maintained" in `DSE_5_1` is replaced by counts.
  * `V11SerializationAuditTest`, `Dse6StartupChecksTest`, `DseBatchlogReplayTest`: expected texts are parameterised
    by mode.
  * The others only add cases.
* New tests:
  * Real DSE 5.1.51 captures (fixtures of P27): `Dse51RepairMessageTest` feeds captured repair messages through
    `EmbeddedChannel` into the real inbound handler, `Dse51GossipDigestAckTest` decodes a captured ack, and
    `Dse51CompressorsTest` and `DseBatchlogReplayTest` read fixture rows.
  * Hand-modified bytes made from captures: the `CLEANUP` type, type 9, the empty payload, the multi-frame split and
    the Thrift-flagged read in `Dse51RepairMessageTest`.
  * `Dse51PeersTest`, `Dse51UpgradeGuardsTest` and `Dse51SchemaExchangeTest` (`CQLTester`) drive gossip states and
    versions set by the test.
  * In-JVM: `Dse51ModeSmokeTest`, `Dse51CompressorsStartupTest` and `Dse6GuardsTest` run only nodes of this
    software. No DSE 5.1 process takes part.
* Not covered by any test in the patch: whether a real DSE 5.1 coordinator accepts `pre40FailureResponse` and stops
  the repair (only the bytes written are asserted). Also `NONE` replay of a real batch stored at 10/11 by a non-DSE
  node.

**Suggested reviewer questions**
* Is mapping stored versions 10/11 to `VERSION_40` with `NONE` safe for every `system.batches` / `system.paxos`
  layout Apache Cassandra 3.0/3.11 wrote, or should it be limited to nodes that ran `DSE_5_1`?
* In `pre40RepairMessage`, what happens when the first frame of a large message ends inside the params block?
  `inferMessageSizePre40` returning a size does not guarantee that the params are in the buffer.
* Should a refused schema change from a peer drop the whole mutation batch, or only the offending tables, given the
  pull loop it creates?
* The `DSE_6` refusal on DSE 5.1 files is an exception to "DSE_6 is unchanged". Is `Files.exists` (fail open on an
  unreadable directory) the right choice here when `NONE` fails closed?

## P32 — handshake, gossip and TLS ports with DSE 5.1 nodes (#2786)

**Tier:** 1 — it changes the wire for `DSE_5_1`: handshake version choice and refusals, gossip state selection and
content (a new published DSE state, ordinal filtering), the internode TLS port choice, and refusal of the plain-text
fallback for messaging and streaming.

**Size and shape:** 23 production and config files, about +450 / -110 lines. `Dse51GossipStates` (151 lines) is new;
the rest edits `DseCompat`, `DseDialectVersions`, `DseTlsPorts`, `EndpointState`, `DseGossipStates`,
`OutboundConnectionInitiator`, `InboundConnectionInitiator`, `OutboundConnection`, `StorageService.addDseState`.
11 test files, about +1,140 lines (one helper, `Dse51SystemTablesRecords`). The comment-only changes in
`GossipDigestAckVerbHandler`, `Gossiper`, `NettyStreamingConnectionFactory` and the yaml files can be skimmed.

**Gating:** each new branch is behind `DseCompat.isDse51()`, or behind `DseCompat.enabled()` where a `DSE_6` rule is
widened to `DSE_5_1` (`EndpointState` unknown-ordinal drop, `Gossiper` echo retries,
`MessagingService.seedDseDialectPeers`, `DseTlsPorts.listensOnSslStoragePortForDse`, `sslFallback`,
`plainFallbackRefusal`, `OutboundConnectionSettings.withLegacyPortIfNecessary`).
`StartupChecks.checkDse6GossipedDseVersion` now runs with `isDseCompatibilityMode()`. For a cluster that never had
DSE nothing changes: every gate is false with `NONE`.
`OutboundConnectionInitiator`'s constructor now always calls `dse51RequestedVersion`, which returns its argument
outside `DSE_5_1`.

**Where to look**
* `src/java/org/apache/cassandra/gms/DseGossipStates.java` — `sendsAllOwnStates`: in `DSE_5_1` a peer identified as
  DSE 5.1, or whose stored `NET_VERSION` is 10/11, gets only the states above its digest version. The commit
  message attributes this to DSE 5.1.51 firing listeners on every state (no CASSANDRA-15097). Is a peer that is not
  yet identified, on its first exchange, still sent everything, and is that harmful?
* `src/java/org/apache/cassandra/gms/EndpointState.java` — `EndpointStateSerializer.filterOutgoingStates`: drops
  states with ordinal > 24 at every pre-4.0 version in `DSE_5_1`. The receive side now drops unknown ordinals in
  both DSE modes. Is relaying another node's states (not only this node's) covered by the same filter?
* `src/java/org/apache/cassandra/gms/Dse51GossipStates.java` — `publishedState`: the JSON gossiped at
  `X_11_PADDING` (`server_id` null, no `health`, `active` as a string). The patch states that DSE 5.1.51 accepts
  this. Only a captured-state comparison can confirm it.
* `src/java/org/apache/cassandra/net/OutboundConnectionInitiator.java` — constructor (`dse51RequestedVersion`) and
  the pre-4.0 reply handling: a request below 11 is raised to 11, and a DSE 6 version answer is now `incompatible`
  in `DSE_5_1`. Is the retry behaviour of `OutboundConnection` on `INCOMPATIBLE` (keep trying) acceptable for a node
  that will never be compatible?
* `src/java/org/apache/cassandra/net/InboundConnectionInitiator.java` — `decodePre40Initiate` (the DSE-flagged and
  12-requesting refusal) and `confirmPre40`: inbound refusals, before the second handshake message is sent.
* `src/java/org/apache/cassandra/net/DseTlsPorts.java` — `dialsSslStoragePort` (always true in `DSE_5_1`),
  `listensOnSslStoragePortForDse`, `sslFallback`: an encrypted pre-4.0 connection always dials `ssl_storage_port`
  and never falls back to plain. Before a peer's version is known, the 4.0-style attempt goes to `storage_port` and
  fails without fallback until gossip records 11. Is the time this takes bounded on a cluster with
  `internode_encryption: all`?
* `src/java/org/apache/cassandra/net/DseDialectVersions.java` — `seedDse51`: seeds saved peers at 11 from
  `RELEASE_VERSION`, or from the DSE 5.1 `dse_version` read once at the first start (`Dse51SystemTables.stored()`)
  when no release is stored. Can a peer already upgraded to this software but stored without a release be seeded at
  11 indefinitely?
* `src/java/org/apache/cassandra/net/DseCompat.java` — `gossipedDse51DseVersion`: any value but `5.1.51`, `none`
  included, refuses the start; unset defaults to `5.1.51`.

**Modified or deleted existing code**
* `src/java/org/apache/cassandra/net/OutboundConnectionInitiator.java` — constructor: `requestMessagingVersion` is now
  filtered. — History: 310a48efcfd, 2019, CASSANDRA-15066. The pre-4.0 branch of the reply handling was added by this
  series (P11).
* `src/java/org/apache/cassandra/net/OutboundConnection.java` — the `INCOMPATIBLE` case now uses
  `incompatiblePeerMessage`, the same text plus a suffix in `DSE_5_1`. — History: 310a48efcfd, 2019,
  CASSANDRA-15066.
* `src/java/org/apache/cassandra/gms/EndpointState.java`, `DseGossipStates.java`, `Gossiper.java` (echo retries),
  `GossipDigestAckVerbHandler.java` — added by this series (P12).
* `src/java/org/apache/cassandra/net/DseTlsPorts.java`, `InboundSockets.java` (extra socket),
  `OutboundConnectionSettings.withLegacyPortIfNecessary`, `NettyStreamingConnectionFactory` — added by this series
  (P18).
* `src/java/org/apache/cassandra/net/DseDialectVersions.java`, `InboundConnectionInitiator.java` pre-4.0 branches,
  `EndpointMessagingVersions.assume` — added by this series (P11).
* `src/java/org/apache/cassandra/service/StorageService.java` — `addDseState`;
  `StartupChecks.checkDse6GossipedDseVersion` — added by this series (P12, P05).

**Failure and rollback risks**
* Encrypted clusters: until a DSE 5.1 peer's version is known (seeded, gossiped, or from its inbound connection),
  outbound attempts on `storage_port` fail with no plain-text retry. This is by design and fails safe, but a slow
  start of connectivity can stall gossip-dependent startup steps (`waitToSettle`, the access check of P33).
* A DSE 5.1 node started with `-Dcassandra.force_3_0_protocol_version=true` is refused in both directions and keeps
  being retried. This is visible only through WARNs and failed connections.
* The published DSE state carries `dse_version 5.1.51`. After rollback this node is DSE 5.1 again, so no residue.
* Concurrency: `sendsAllOwnStates` reads `Gossiper.instance.getEndpointStateForEndpoint(peer)` from the gossip
  stage, which is the same stage that writes it. No new lock.

**Test integrity**
* Existing tests changed: none weakened. The single deleted lines in `DseGossipTest`, `DsePeerGossipTest`,
  `DseTlsPortsTest`, `DseDialectVersionsTest` and `DseHandshakeTest` are import or signature reorders, and
  `Dse51StartupChecksTest` replaces 5 lines in a helper. `Dse51ModeSmokeTest` gains assertions on STATUS,
  RPC_ADDRESS, RPC_READY and the DSE state.
* New tests:
  * Captured DSE 5.1.51 handshakes and acks (fixtures of P27): `DseHandshakeTest`
    (`theCapturedDse51HandshakesCompleteAt11`, `weConnectToADse51NodeAsDse51NodesDo`) and `Dse51GossipDigestAckTest`.
  * Unit-level, with settings and versions set by the test: `DseTlsPortsTest` (no real TLS peer on the other end) and
    `DseDialectVersionsTest`.
  * `Dse51SystemTablesRecords` writes the P30 record through its own writer, so the seeding test reads data produced
    by the code under test.
* Not covered by any test in the patch: a TLS connection between this software and a DSE 5.1 process, the 4.0-style
  attempt on `storage_port` failing and then switching to `ssl_storage_port`, and a DSE 5.1 node parsing the
  published JSON state (only its bytes are asserted).

**Suggested reviewer questions**
* Should `INCOMPATIBLE` towards a DSE 5.1 node at 10, or towards a DSE 6 node, stop reconnection attempts, rather
  than retry with backoff indefinitely?
* Does sending only the delta states to an unidentified peer that stores `NET_VERSION` 10/11 risk a DSE 5.1 node
  never learning `STATUS`/`TOKENS` after it restarts with a new generation?
* Is the ordinal-24 cap applied to third-party states relayed in `GossipDigestAck2`, or only to this node's own
  states?
* Is refusing `dse_version` `none` in `DSE_5_1` (the commit message says DSE 5.1 then counts the node as DSE 4.8.0)
  backed by DSE 5.1 source, or by observation?

## P33 — grants, access controls, non-core features and indexes (#2787)

**Tier:** 1 — it adds startup refusals that read `system_schema` and `system.local`/`system.peers` before the schema
is loaded and that run after the node has joined the ring. It changes the deserialization of a row filter (wire), the
`GRANT` path of `CassandraAuthorizer`, and `SchemaKeyspace.fetchKeyspace`, all behind `DSE_5_1`.

**Size and shape:** 22 production and config files, about +1,450 / -200 lines. Almost all of it is in
`Dse6NonCoreTraces` (+506) and `Dse6AccessControlCheck` (+377). The smaller edits are `SchemaKeyspace`,
`CassandraAuthorizer`, `RowFilter`, `CreateIndexStatement`, `CassandraDaemon`, `DseUpgradeGuards`, `NoopIndex`,
`SecondaryIndexManager` and `IndexMetadata`. The remaining production files (`InternodeCompatibilityMode`,
`CassandraRelevantProperties`, `MessagingServiceMBean`, `DseViewWireFormat`, `Dse51GossipStates`, the yaml files)
change only javadoc or text.
17 test files, about +2,000 lines, including the test harness `distributed/impl/Instance.java`.

**Gating:** new branches check `DatabaseDescriptor.isDse51CompatibilityMode()` / `DseCompat.isDse51()`. The `DSE_6`
sites that move to `DseCompat.enabled()` or `isDseCompatibilityMode()` are the `Dse6AccessControlCheck` passes and
`recheck`, the Search index refusal and the no-op index loading in `SecondaryIndexManager`, `IndexMetadata.validate`
and `NoopIndex`, the custom-class check in `CreateIndexStatement`, `getDseNonCoreTraces`, and
`CassandraAuthorizer.writesDsePermissionsOnly`. For a cluster that never had DSE nothing changes. The three calls
added to `CassandraDaemon.setup` (`verifyDse51AtStartup`, `verifyDse51SchemaRowsAtStartup`,
`verifyDse51GossipedWorkloads`) return at once outside `DSE_5_1`. `DseUpgradeGuards.logStatus` calls
`recheckDse51GossipedWorkloads` on every run in the DSE modes. The `DSE_6` texts are meant to be unchanged byte for
byte.

**Where to look**
* `src/java/org/apache/cassandra/service/CassandraDaemon.java` — `setup`: two checks run before
  `SystemKeyspace.persistLocalMetadata` and `Schema.instance.loadFromDisk` (reading `system_schema` with
  `executeInternal`), and one runs after `StorageService.initServer` and `Gossiper.waitToSettle`. A refusal there
  exits a node that has already joined and announced itself. Is that acceptable, and what does it leave in the
  peers' gossip state?
* `src/java/org/apache/cassandra/service/Dse6AccessControlCheck.java` — `verifyAfterJoin` and the retry loop (new
  `failedOnTheRequest`, `dse51Fallback`, `isDse51PermissionsTable`): in `DSE_5_1`, when the cluster-wide read fails
  on the request, the node falls back to its own replica, which is less strict than `DSE_6`. The safety argument is
  in the class javadoc: DSE 5.1 has no RESTRICT or delegation, and `rows/` grants matter only with the schema key. Is
  that argument sound, given that `rows/` grants on other replicas are then unchecked until the 10-minute recheck?
* `src/java/org/apache/cassandra/service/Dse6AccessControlCheck.java` — `verifyDse51SchemaRowsAtStartup` and
  `src/java/org/apache/cassandra/schema/SchemaKeyspace.java` — `fetchKeyspace`, `refuseDse51TableRowsOfViews`: they
  detect a `system_schema.tables` row sharing a view's name or id. The second one throws `ConfigurationException`
  from inside every schema read in `DSE_5_1`, including reads at runtime. Can it fire after start, for example on a
  schema pull?
* `src/java/org/apache/cassandra/db/filter/RowFilter.java` — `Expression.Serializer`: a USER expression (kind 4) from
  a pre-4.0 peer in `DSE_5_1` is refused before its bytes are read, and serializing one towards such a peer throws
  `IllegalStateException`. Is the coordinator-side `DseReadGuard` guaranteed to reject it first, so that the
  serialize-side throw is unreachable?
* `src/java/org/apache/cassandra/service/Dse6NonCoreTraces.java` — `verifyDse51AtStartup`, `addDse51KeyspaceTraces`
  (graph detection by three keyspace names, `_pvt` with `LocalStrategy`), `addCompactionTrace` (DTCS, tiered
  storage, in-memory), `verifyDse51GossipedWorkloads`, `recheckDse51GossipedWorkloads`. Are there false positives on
  user keyspaces, and do these checks read the stored rows correctly when columns are missing (`row.has(...)`)?
* `src/java/org/apache/cassandra/auth/CassandraAuthorizer.java` — `grant`, `writesDsePermissionsOnly` vs
  `writesDseModify`: in `DSE_5_1`, `MODIFY` is written as `MODIFY` (not `UPDATE`+`TRUNCATE`), and `UNMASK`/
  `SELECT_MASKED` are refused or left out. Check that the two predicates are not swapped anywhere `revoke` or the
  read path uses them.
* `src/java/org/apache/cassandra/cql3/statements/schema/CreateIndexStatement.java` — `apply`: SAI is refused in
  `DSE_5_1` even with `allow_ddl`. The check compares `className` to `StorageAttachedIndex`, so check that the
  default `sai` alias and `USING 'sai'` both resolve to that name before the comparison.
* `src/java/org/apache/cassandra/service/DseUpgradeGuards.java` — `schedulePeriodicTasks`, `logStatus`: the
  access-control recheck is scheduled in `DSE_5_1` again (P31 had excluded it), and the gossiped-workload recheck
  runs on the status executor.

**Modified or deleted existing code**
* `src/java/org/apache/cassandra/service/CassandraDaemon.java` — `setup`: new steps around the first write to
  `system.local`. — History: 360541f16a1, 2016, CASSANDRA-11742 (persist local metadata before anything else
  writes to `SystemKeyspace`). The new steps only read, so the "first write" invariant holds if `executeInternal` on
  `system_schema` causes no write.
* `src/java/org/apache/cassandra/schema/SchemaKeyspace.java` — `fetchKeyspace`: a refusal between `fetchViews` and
  `KeyspaceMetadata.create`. — History: the surrounding fetch code dates back to 340df43fb74, 2015 (CQL type names in
  schema tables); the invariant is that schema reads throw only on corruption.
* `src/java/org/apache/cassandra/db/filter/RowFilter.java` — `Expression.Serializer.serialize/deserialize` (USER
  kind). — History: the USER kind is Apache Cassandra 3.x's; the deserialize lines were last touched by b73199b7fd3,
  2023.
* `src/java/org/apache/cassandra/auth/CassandraAuthorizer.java`, `index/NoopIndex.java`,
  `index/SecondaryIndexManager.java` (no-op and Search branches), `schema/IndexMetadata.java`,
  `CreateIndexStatement` custom-class check — added by this series (P06).
* `src/java/org/apache/cassandra/service/Dse6AccessControlCheck.java` — added by this series (P16).
  `Dse6NonCoreTraces.java` — added by this series (P17). `DseUpgradeGuards` periodic tasks — added by this series
  (P15, P31). `DseViewWireFormat` — added by this series (P20). `Dse51GossipStates` — added by this series (P32).
* `test/distributed/org/apache/cassandra/distributed/impl/Instance.java` — the in-JVM harness copies the new
  `CassandraDaemon` steps by hand, so drift between the two orderings goes unnoticed.

**Failure and rollback risks**
* Refusal after join: `verifyDse51GossipedWorkloads` can exit the process after `initServer`. The node has then
  gossiped NORMAL and persisted its tokens. A rerun with the same state refuses again, and the peers see it as down.
* The `DSE_5_1` fallback to the local replica starts the node with a WARN while cluster-wide `rows/` grants are
  unverified. During that window, until the 10-minute recheck, this software serves reads that a DSE 5.1 node would
  restrict, if such grants exist only on other replicas.
* `refuseDse51TableRowsOfViews` in `fetchKeyspace` turns a DSE 5.1 schema anomaly into a `ConfigurationException` on
  any later schema read. The startup check runs first, so this is reachable only if the row appears later. DSE schema
  changes do not reach this node, so it is probably unreachable, but this is not established.
* Rollback: none of these checks writes data. The refusal texts direct operators to roll back to DSE 5.1 and change
  schema there only "if no other node runs this software".

**Test integrity**
* Existing tests changed:
  * `Dse6GuardsTest`: the assertion that the `DSE_5_1` status line has no access-control recheck (`grep(...).isEmpty()`)
    is replaced by assertions that the recheck is scheduled, plus `compatstatus` lines. This reverses P31's
    expectation, as intended by this patch.
  * `CompatStatusTest`: 4 changed lines for the `DSE_5_1` non-core line.
  * The others only add cases.
* New tests:
  * Captured DSE 5.1.51 RLAC reads (`rlac-read`, fixtures of P27) go through the real inbound handler on an
    `EmbeddedChannel` in `Dse51RlacExpressionTest`. Its version-1 expression is hand-modified from a capture.
  * `Dse51SchemaRows` loads P27's real `system_schema` sstables for `Dse6AccessControlCheckTest` and
    `DseLocalKeyspacesTest`.
  * `Dse6AccessControlCheckTest` and `Dse6NonCoreTracesTest` pass the scans and the stored columns in as functions or
    lists. They mock the cluster-wide read failure rather than exercise it.
  * In-JVM: `Dse51ModeSmokeTest`, `Dse6ViewStartupTest` and `Dse6GuardsTest` run only nodes of this software. The
    smoke test covers the fallback WARN with a 6 s wait.
* Not covered by any test in the patch: the post-join refusal on gossiped non-core workloads in a real multi-node
  cluster with a DSE 5.1 peer (unit-level only), the `SchemaKeyspace.fetchKeyspace` refusal at runtime, and the
  serialize-side `IllegalStateException` for a USER expression.

**Suggested reviewer questions**
* Should `verifyDse51GossipedWorkloads` run before `initServer` (on shadow-round state), so that the node never joins
  a ring it will leave?
* Is the `DSE_5_1` local-replica fallback consistent with "fail closed" elsewhere in the series, and should it
  require the explicit `dse_restrictions_local_check_only` flag, as `DSE_6` does?
* Can `refuseDse51TableRowsOfViews` in `fetchKeyspace` break `nodetool` or the driver's schema reads at runtime on a
  node that started cleanly?
* Does the graph detection in `Dse6NonCoreTraces` (three keyspaces `<g>`, `<g>_system`, `<g>_pvt`) match how DSE 5.1
  names them, and are short and long compaction class names enough for DSE's tiered-storage and in-memory classes?

## Group notes (P31 to P33)
* P31, P32 and P33 only make sense merged together. P31 alone lets a `DSE_5_1` node exchange messages with DSE 5.1
  nodes, but in that state:
  * it sends all gossip states every round, gossips no DSE state and dials `storage_port` for TLS;
  * it does not refuse the plain-text fallback;
  * it runs no access-control or non-core check, so a DSE 5.1 RLAC read (kind 4) is deserialized as a
    `UserExpression` rather than refused.

  P32 closes the gossip and TLS gaps and P33 the access-control ones. The P33 commit message says so explicitly
  ("P31 and P32 ... left the access controls ... so a DSE_5_1 node checked none of them").
* Gate churn: P31 swaps many `DseCompat.enabled()` calls to `isDse6()`, and P32/P33 swap several back to `enabled()`
  (`EndpointState`, `Gossiper` echo retries, `DseTlsPorts`, `seedDseDialectPeers`, `Dse6AccessControlCheck`). Read
  the final state at P33 rather than each step. The access-control recheck in `DseUpgradeGuards.schedulePeriodicTasks`
  is excluded in P31 and restored in P33, and `Dse6GuardsTest` flips its assertion accordingly.
* The only changes that reach `NONE` are in P31 (`storedDecodingVersion` for 10/11). The only change that reaches
  `DSE_6` is P31's start refusal on DSE 5.1 files. All other behaviour is `DSE_5_1`-only.
* The "DSE 5.1 peer" identity (`DseCompat.isDse51Peer`, version 10 or 11) comes from P28, `Dse51SystemTables.stored()`
  from P30, and the captured messages and schema sstables used as test inputs from P27. P32 refuses version 10 that
  P28 and P31 still accept as identifying a DSE 5.1 peer.
* No test in the three patches runs a DSE 5.1 process. All the DSE 5.1 interoperability evidence is captured bytes
  replayed into this software. The response direction (failure responses, the published gossip JSON, TLS ports) is
  asserted only on what this software writes.

## P34 — the writer of big `me` sstables and the `DSE_5_1` storage compatibility mode (#2788)

**Tier:** 1 — it changes the sstable write path (version selection, Data/CompressionInfo/Filter/Statistics layouts,
row encoding of LIVE deletion times), adds a storage compatibility mode, and changes one always-on behaviour: the
serialization of bloom filters in the old layout.

**Size and shape:** 35 production files (~1,780 added / 200 removed lines), 2 yaml files (~75 lines of comments),
28 test files (~3,750 added / 270 removed lines). One new production file, `schema/Dse51ColumnTypes.java` (~430
lines). `schema/Dse51Compressors.java` grows by ~400 lines; it was added by this series (P28). About a third of the
production diff is javadoc, yaml comments and refusal texts (`InternodeCompatibilityMode`, `Config`,
`CassandraRelevantProperties`, `AuthConfig`, `DseSystemKeyspaces`, `MemtableParams`, `DseUpgradeGuards`,
`LogRecord`). These can be skimmed. The Tier 1 files are `BigFormat`, `Version`, `SerializationHeader`,
`CompressedSequentialWriter`, `CompressionMetadata`, `DataComponent`, `SSTableWriter`, `BloomFilterSerializer`,
`OffHeapBitSet`/`IBitSet`, `TombstoneHistogram`, `Upgrader`, `UnfilteredSerializer`, `CellData`,
`StorageCompatibilityMode` and `DatabaseDescriptor`.

**Gating:** A new `storage_compatibility_mode: DSE_5_1` (major 3). `DatabaseDescriptor.applyInternodeCompatibilityMode`
requires it together with `internode_compatibility_mode: DSE_5_1`, each refused without the other. Offline tools and
clients in the internode mode take the same output through `DatabaseDescriptor.isDse51SSTableOutput()`, without the
storage mode. The version-gated paths fire only for `Version.hasPre40Encoding()` (big `m*`). Outside the mode,
nothing writes those versions. For a cluster that never had DSE, these changes are always on:
* `BloomFilterSerializer.serialize`: the assertion `!oldFormat` is removed and the old-format serializer now writes
  the old layout (`IBitSet.serializeOldBfFormat`). In every mode, a bloom filter rebuilt for a legacy big `m*` or trie
  `a*` sstable (missing `Filter.db`, or a changed fp chance) is saved in the layout its version reads. Before this
  patch that path hit the assertion with `-ea`, or wrote the new layout into an old-format file without it.
* `OffHeapBitSet.serializeOldBfFormat` is rewritten from byte-by-byte assembly to `bytes.getLong(i)`. `Memory.getLong`
  is explicitly little-endian (`LittleEndianMemoryUtil`), so the bytes should be the same. The method was
  `@VisibleForTesting` and is now production code.
* The `StorageCompatibilityMode` enum gains a constant, `DSE_5_1(3)`. Every `isBefore(5)` rule in the code base
  (system/auth/schema table layouts, `PartitionerDefinedOrder`, `CassandraIndex`, `CellData`) now also applies to it.
  The only `isBefore(4)` rule is the new one in `BigFormat`.
* Plumbing that is identical when the version is not `m*`: `SSTableWriter.Builder.setSerializationHeader` now calls
  `forSSTableVersion`, which returns `this`. The new `CompressionMetadata.Writer.open(.., true)` and
  `CompressedSequentialWriter(.., false)` overloads keep the old behaviour. `BigVersion.isCompatible` compares against
  `newest_readable_version_family`, which equals `current_version.charAt(0)` unless `me` is written. `Upgrader`
  computes one boolean per sstable.
* `MemtableParams`: the refusal text of a sharded memtable now names `DSE_5_1`.

**Where to look**
* `src/java/org/apache/cassandra/io/sstable/format/big/BigFormat.java` — `BigVersion.currentVersion()` and
  `newest_readable_version_family`: the written version is still chosen once, when the class loads. Is
  `DatabaseDescriptor.verifyDse51SSTableFormat` (called on the daemon, tool and client paths) the only safeguard
  against an early class load? Is it enough? It checks only when `sstableOutputMode.isDse51()`.
* `src/java/org/apache/cassandra/db/SerializationHeader.java` — `forSSTableVersion`, `writeDeletionTime` and
  `deletionTimeSerializedSize`: in a `me` sstable, a LIVE deletion time is written as
  `Integer.MAX_VALUE - (int) stats.minLocalDeletionTime`. Check that the size and write paths always agree, and that
  the row index and the partition writer see the same header (the patch states that `SSTableWriter.Builder` shares
  it).
* `SerializationHeader.Serializer.serialize/serializedSize` (`hasPre40TypeNames`, `writePre40Type`): these write the
  header types with `Dse51ColumnTypes.sstableTypeName`, which strips `FrozenType` from tuples and the partition-key
  suffix from `PartitionerDefinedOrder`. It is gated on `version.hasPre40Encoding() && isDse51SSTableOutput()`.
  Confirm that the string rewriting (`withoutFrozenTuples`, `withoutPartitionKeyTypes`) is correct for nested types.
* `src/java/org/apache/cassandra/io/compress/CompressedSequentialWriter.java` and `CompressionMetadata.Writer`: for
  `m*`, the compression info has no maximum compressed length, every chunk is compressed
  (`CompressionParams.withEveryChunkCompressed`, forced in `DataComponent`), and the ratio is computed as
  `chunkOffset - 4L * chunkCount`. Is `chunkCount` right after a `resetAndTruncate`?
* `src/java/org/apache/cassandra/utils/BloomFilterSerializer.java` — `serialize`: this always-on change needs review
  even for clusters with no DSE history. Is anything else serializing with the old-format instance and relying on the
  removed assertion?
* `src/java/org/apache/cassandra/db/compaction/Upgrader.java` — `RefusePast2038`: during a rewrite to `me` from an
  sstable with unsigned deletion times (`oa`), any time past 2038-01-19 throws. Check that the throw aborts the
  transaction cleanly and that no partial `me` sstable or transaction log is left. The patch says `sstableupgrade` then
  prints "Error upgrading" and exits 0.
* `src/java/org/apache/cassandra/utils/streamhist/TombstoneHistogram.java` — `Pre40HistogramSerializer`: it writes the
  constant 100 as the max bin size. Its `serializedSize` is inherited and has the same length.
* `src/java/org/apache/cassandra/index/internal/CassandraIndex.java` — `getMetadataReloadTask`: in the mode, it calls
  `setLocalOverrides` on the index table's metadata with the base table's compression, on every base-table reload.
  Can a later schema reload of the index table drop or race with this override?

**Modified or deleted existing code**
* `BigFormat.java` — `BigVersion.current_version`: was `isBefore(5) ? "nb" : "oa"` and is now `currentVersion()`,
  which returns `me` for a storage mode before 4 or for DSE 5.1 output. History: 0e4c2f4befa (2023, CASSANDRA-19010)
  defaults to `nb` under a pre-5.0 storage mode. Invariant: the version is fixed at class load.
* `BigFormat.java` — `BigVersion.isCompatible`: the upper bound moves from `current_version.charAt(0)` to
  `newest_readable_version_family` (`o` while `me` is written). History: 24ebd24c791 (2023, CASSANDRA-18134).
  Invariant: no version newer than the written family is read. It is relaxed on purpose in the mode.
* `BigFormat.java` — `BigVersion` constructor: the local `nOrLater` becomes a field, for `hasPre40Encoding()`. This is
  mechanical.
* `BloomFilterSerializer.java` — `serialize`: the assertion `"Filter should not be serialized in old format"` is
  removed. History: b7e1e44a909 (2023, CASSANDRA-17056) moved it there. The old format has been read-only since
  d6cc5943250 (2018, CASSANDRA-9067). Invariant: no old-layout filter is ever written. It is deliberately broken
  here, in every mode.
* `OffHeapBitSet.java` — `serializeOldBfFormat`: the loop is rewritten and the method changes from
  `@VisibleForTesting` to `@Override`. History: d6cc5943250 (2018, CASSANDRA-9067), a test helper for the old format.
* `CompressionMetadata.java` — `Writer.writeHeader`: `out.writeInt(parameters.maxCompressedLength())` is now
  conditional. History: f97db26f8e9 (2015, CASSANDRA-10520) added the field with uncompressed chunks in `na`.
* `CompressedSequentialWriter.java` — `doPrepare`: `addCompressionRatio(chunkOffset, ..)` becomes `chunkOffset - 4 *
  chunkCount` for `m*`. History: dde2807e151 (2024, a CNDB merge) replaced the separately counted compressed size
  with `chunkOffset`, which includes the per-chunk checksums. The constructor body moves into a delegating
  constructor plus `writerOption`.
* `DataComponent.java` — `buildWriter`: the `final` is dropped, and an `m*` precondition plus
  `withEveryChunkCompressed` are added. History: a9a236c8683 (2026, CNDB-18110) added pluggable compression params
  selection.
* `SerializationHeader.java` — `writeDeletionTime(DeletionTime, DataOutputPlus)` and `deletionTimeSerializedSize`: a
  new LIVE branch for sstable headers. History: 0d581fa9366 (2026, CNDB-17045) writes a zero delta for LIVE because
  `Long.MAX_VALUE - min` overflows an int. That zero-delta invariant now holds for every sstable version except
  `m*`. The private `pre40LiveLocalDeletionTimeDelta(int)` was added by this series (P10).
* `SSTableWriter.java` — constructor: a new `checkState` that refuses a pending repair for `m*`. History: b7e1e44a909
  (2023, CASSANDRA-17056). `Builder.setSerializationHeader` was added by this series (P07)
  and now also calls `forSSTableVersion`.
* `TombstoneHistogram.java` — `getSerializer`: a third branch. History: 1adbea5a068 (2022, CASSANDRA-14227)
  introduced the legacy versus unsigned-int serializers.
* `CellData.java` — `getVersionedMaxDeletiontionTime`: adds `|| isDse51SSTableOutput()`. History: introduced with
  1adbea5a068 (2022, CASSANDRA-14227), last touched by f70a65fa6bf (2025, CNDB-15669). Invariant: a pre-5.0 storage
  mode caps deletion times at 2038.
* `Upgrader.java` — `upgrade`: `writer.append(iter.next())` may now be wrapped in a `Transformation`. History:
  a991b64811f (2014, CASSANDRA-8099).
* `UnfilteredSerializer.java` — `checkPre40DeletionTime*`: these get an sstable-version variant and message. Added by
  this series (P10).
* `StorageCompatibilityMode.java` — `validateSstableFormat` refuses `bti` for `DSE_5_1` too, and
  `storageMessagingVersion` adds `case DSE_5_1`. History: 70389abcf78 (2023, CASSANDRA-18569) refused bti under
  `CASSANDRA_4`; 60b2bdfd7bb (2026) renamed `CC_4` to `HCD_1`.
* `RepairMessageVerbHandler.java` — `doVerb` (PREPARE_MSG): the refusal of incremental repair is placed before
  `verifyCompactionsPendingThreshold` and uses the same `state.phase.fail` + `sendFailureResponse` pattern. History:
  9a66f14a014 (2021, STAR-881).
* `CassandraEntireSSTableStreamReader.java` — `read`: a refusal before the component manifest is read. History:
  47a12c52a31 (2018, CASSANDRA-14566), entire-sstable streaming. Imports reordered.
* `CassandraIndex.java` — `getMetadataReloadTask`: a mode-gated local override is set before the reload task.
  History: 0626be8667a (2015), the 2i API.
* `DatabaseDescriptor.java` (`applyInternodeCompatibilityMode`, `applyDse51SSTableOutput`, `announceDse51Mode`),
  `StartupChecks.java` (`checkDse6Artefacts`, `DSE_51_MODE_MAX_BIG_VERSION` tightened from `oa` to `me`, remedy
  texts), `StorageService.refuseSSTableRewriteWhileUpgrading` (now returns early in `DSE_5_1`),
  `AlterSchemaStatement.Dse51CompressorsCheck` (renamed `Dse51SSTableOutputCheck`), `DefaultSchemaUpdateHandler`,
  `CassandraDaemon.setup`: added by this series (P15, P28, P29 and P31).

**Failure and rollback risks**
* Class-load ordering: if anything initialises `BigFormat` before the modes are applied, the node would write `nb`
  in the mode. `verifyDse51SSTableFormat` refuses rather than writes, but it only runs where `DatabaseDescriptor`
  calls it (daemon, tool, client init). A test harness or embedded user that loads the formats differently is
  covered only by that check.
* The patch states it does not refuse times past 2038 in the `me` writer itself. CASSANDRA-14092 overflowed times
  read from DSE 5.1 sstables are written back as read on flush, compaction, scrub and streaming. Only `Upgrader`
  refuses. As the commit says, `sstablescrub`/`sstablesplit`/`sstableloader` from an `oa` source into the mode are
  left open.
* `Dse51ColumnTypes.sstableTypeName` throws `IllegalStateException` while serializing a header. A flush or compaction
  of a table with a refused type fails there. Its disk/commit log consequences in a running node are not
  established. Start, DDL and schema-apply checks are meant to make this unreachable.
* Rollback: a node rolled back to DSE 5.1 keeps the `me` sstables written here. Whether DSE 5.1.51 reads them (wide
  partitions with row index blocks, LIVE complex deletions, geo/date-range types, uncompressed tables with
  `CRC.db`) is not tested. The commit leaves it to the lab.
* After the flag drop the node writes `nb` again. Incremental repair is refused while the storage mode is `DSE_5_1`.
  The remedy text says to rewrite `me` sstables with `upgradesstables` once the flag is dropped.
* In the mode every entire-sstable stream from a sender of this software is refused (`dse51Refusal`). Operations
  that stream with `stream_entire_sstables: true` on the sender fail until the sender changes it.
* `StorageService.refuseSSTableRewriteWhileUpgrading` returns early for `DSE_5_1`, so `upgradesstables` and
  `recompress_sstables` run during the mixed phase. That is safe only if every rewrite really writes `me`.

**Test integrity**
* Existing tests changed:
  - `SerializationsTest`, `BloomFilterTest` and `BloomFilterSerializerBench`: the test-only helper
    `serializeOldBfFormat` is removed and replaced by the production serializer. Assertions are unchanged, and the
    reference files they read (`test/data/serialization/3.0/...`) still apply.
  - `DseTransactionLogsTest`: the DSE 5.1 expectations move from `nb_` to `me_`, and the DSE_6 `RESTORE` case now
    uses the trie `bb` version instead of big `nb`.
  - `Dse51StartupChecksTest`, `Dse51ModeConfigTest`, `AuthConfigDse6Test`, `DseCommitLogReplayTest`,
    `Dse6GuardsTest` and `Dse51ModeSmokeTest`: expectations moved from `HCD_1`/`nb`/`oa` to `DSE_5_1`/`me`.
    `Dse6GuardsTest` no longer expects `upgradesstables` to be refused in `DSE_5_1` (the refusal counter changes
    accordingly).
  - `Instance.java` (dtest harness) mirrors the new `CassandraDaemon` start checks.
  - No assertion was found weakened beyond these semantic changes.
* New tests:
  - `BigMeWriterTest.theSSTablesOfDse51AreWrittenAsDse51WroteThem` is the strongest proof. Six user sstables written
    by DSE 5.1.51 (fixtures from P27) are rewritten from their rows with DSE's own header, host id and commit log
    intervals, then compared byte for byte for Data, CompressionInfo, Digest, Filter, Index, Summary and Statistics,
    and the TOC as a set.
  - Two caveats on that test. The compressed components are compared only when native LZ4 loads. The flush and
    compaction paths are not compared with DSE bytes, only checked for version, components and round trip.
  - LIVE complex deletion, wide-partition row index and `oa`-past-2038 refusal cases are round trips through this
    software or a test-side 32-bit reader. `CompressedSequentialWriterTest`, `MetadataSerializerTest` and
    `LegacySSTableTest` (an `me` round trip and rebuilt filters of `me`/`aa` without any mode) are also round trips.
  - `CassandraEntireSSTableStreamReaderDse51Test` and `RepairMessageVerbHandlerDse51Test` unit-test the refusals.
* Not covered by any test in the patch:
  - A DSE 5.1.51 node reading what is written here.
  - The offline tools and partial-stream writers actually writing `me` (only the version is checked).
  - A two-node incremental repair refused by a node in the storage mode.
  - The `IllegalStateException` in `sstableTypeName` during a live flush.

**Suggested reviewer questions**
* Is it acceptable that the old-layout bloom filter write (`BloomFilterSerializer.serialize`) becomes reachable in
  every mode, and is `LegacySSTableTest` enough evidence that rebuilt filters of legacy `me` and trie `aa` sstables are
  now read back correctly?
* `SerializationHeader.forSSTableVersion` is applied in `SSTableWriter.Builder`. Are there writers that build their
  own header outside the builder (zero-copy streaming, `SSTableZeroCopyWriter`, CQLSSTableWriter) and would then write
  the zero-delta LIVE into a `me` file?
* Should the `me` writer itself refuse (or clamp) deletion times past the 2038 cap, rather than only `Upgrader`, given
  that DSE 5.1 is stated to read such a time wrapped?
* Is the `CassandraIndex.getMetadataReloadTask` local override safe against concurrent reloads of the index table, and
  is it reverted after the flag drop without a restart?
* `DSE_5_1(3)` makes every `isBefore(5)` rule apply. Was each of them (system table layouts, `PartitionerDefinedOrder`,
  CIDR/role tables) reviewed for DSE 5.1 rather than only for HCD 1?

## P38 — the writer of hints files version 1 (#2806)

**Tier:** 1 — it changes the hints write path: the on-disk hints version, the messaging version of buffered hints, the
refusal of hints before buffering, and the `StorageProxy` hint accounting at `ANY`.

**Size and shape:** 17 production files (~1,010 added / 65 removed lines), the yaml (~17 lines of comments), 13 test
files (~2,065 added / 40 removed lines). Three new production files: `hints/Dse51Hints.java` (~195),
`hints/Dse51HintsCompression.java` (~130) and `tools/dsepreflight/Dse51HintsFileCheck.java` (~415, offline tool
only). The Tier 1 files are `HintsDescriptor`, `HintsBuffer`, `HintsBufferPool`, `HintsService`, `StorageProxy` and
`UnfilteredSerializer`. These can be skimmed: `ReadResponse` and `SerializationHeader` (javadoc only), and the comments
in `BatchlogManager`, `HintVerbHandler` and `HintsDispatchExecutor`. `Dse51RollbackInspection` and
`Dse51HintsFileCheck` belong to the preflight tool (Tier 2).

**Gating:** `storage_compatibility_mode: DSE_5_1` (`StorageCompatibilityMode.current() == DSE_5_1`), which P34 ties to
`internode_compatibility_mode: DSE_5_1`. The `hints_compression` start check is gated by the internode mode, node path
only. The preflight file check runs only under `dsepreflight --rollback-check`. For a cluster that never had DSE,
these changes are always on:
* `HintsService.write` now delegates to `tryWrite`. Every hint write calls `Dse51Hints.refuse`, which returns `false`
  after one enum comparison outside the mode.
* `StorageProxy` hint submission branches on `tryWrite`'s result, which is always `true` outside the mode.
* `HintsBuffer` and `HintsBufferPool` serialize and size at `HintsDescriptor.currentWriteMessagingVersion()`, which
  equals `storageMessagingVersion()` outside the mode.
* `UnfilteredSerializer.serialize(RangeTombstoneMarker, ..)`: the inline marker checks are replaced by
  `checkPre40DeletionTimes(marker, version)`, and the refusal becomes `Pre40DeletionTimeException`, a subclass of
  `IllegalStateException` with the same text. This is reachable only at pre-4.0 messaging versions, so also in
  `DSE_6`.

**Where to look**
* `src/java/org/apache/cassandra/hints/HintsDescriptor.java` — `currentStorageVersion`,
  `currentWriteMessagingVersion` and `messagingVersion(int)`: in the mode, version-1 files are read at 11 instead of
  10. That includes DSE 5.1's own pre-upgrade hints files and any version-1 file present at start. The claim "a
  mutation has the same bytes at 10 and 11" is pinned on the DSE 5.1.51 capture. Is it true for every mutation shape
  (counters, range tombstones, static rows)?
* `src/java/org/apache/cassandra/hints/HintsService.java` — `tryWrite`: `Dse51Hints.refuse` runs before
  `catalog.maybeLoadStores`. In the mode, the hint is walked for deletion times and fully sized at 11, then sized
  again by `HintsBufferPool.write`. Check the hot-path cost, and check that `maxHintSize()`
  (`bufferSize / 2 - ENTRY_OVERHEAD_SIZE`) matches the `HintsBuffer.allocate` limit (`totalSize > capacity / 2`).
* `src/java/org/apache/cassandra/service/StorageProxy.java` — hint submission: a refused hint returns before
  `incrCreatedHints` and before the `ANY` handler is notified. Is a write at `ANY` timing out the right client-facing
  outcome, and is this consistent with the batchlog and `HintVerbHandler`, which ignore the result?
* `src/java/org/apache/cassandra/hints/Dse51Hints.java` — `pre40DeletionTimeRefusal`: does it walk exactly what
  `UnfilteredRowIteratorSerializer` checks (partition deletion, static row, rows, markers), so that no hint can still
  fail inside the buffer after being accepted?
* `src/java/org/apache/cassandra/hints/HintsDispatchExecutor.java` — `dispatchVersion`, unchanged code: files written
  at 11 go raw to a DSE 5.1 peer at 11, and are decoded and re-encoded for peers at 12. After the flag drop they are
  read at 10. Does the decode at 10/11 depend on `DseCompat` being enabled?
* `src/java/org/apache/cassandra/config/DatabaseDescriptor.java` — `checkDse51HintsCompression`: it refuses
  compressions DSE 5.1 cannot build. Do the rules in `Dse51HintsCompression` (short-name resolution, the LZ4 options
  and their ranges) match DSE 5.1.51?
* `src/java/org/apache/cassandra/tools/dsepreflight/Dse51HintsFileCheck.java` — `check`: a second, independent reader
  of the version-1 framing (compressed block header, entry CRCs). Its classification of damaged, unverified and
  truncated decides whether a rollback is blocked.

**Modified or deleted existing code**
* `HintsDescriptor.java` — `currentStorageVersion`: forces `VERSION_30` for `DSE_5_1`. History: e13420e2675 (2025,
  CNDB-16249) translated the storage messaging version to a hints version.
* `HintsDescriptor.java` — `messagingVersion(int)`, `case VERSION_30`: now mode-dependent (11 in `DSE_5_1`).
  History: 96d41f0e0e4 (2014, CASSANDRA-6230), the hints rewrite. Invariant: a file's messaging version depends only
  on its hints version. That no longer holds.
* `HintsBuffer.java` — `Allocation.write` — and `HintsBufferPool.java` — `write`: the serialize and size version
  becomes `currentWriteMessagingVersion()`. History: 54e1e1f8d38 (2025, CNDB-16249) introduced
  `storageMessagingVersion()` there. Invariant: the buffer version equals the version of the descriptor of the file
  it flushes to. The patch keeps both derived from the storage mode.
* `HintsService.java` — `write`: the body moves to `tryWrite`, and an empty target list now returns `true`. History:
  04f21a575a0 (2022, STAR-1525).
* `StorageProxy.java` — hint submission: `write` becomes `tryWrite` with an early return. History: 4f0ad22f122
  (2024, CASSANDRA-20014), creation-time based hint expiry.
* `HintsDispatchExecutor.java`: a comment only. History: ef5ec0fea12 (2025, CNDB-16307).
* `UnfilteredSerializer.java` — `serialize(RangeTombstoneMarker, ..)` and `checkPre40DeletionTime`: added by this
  series (P10) and refactored by P34. Now it throws `Pre40DeletionTimeException`.
* `Dse51RollbackInspection.java` — `checkHints`: added by this series (P37). A file named differently from its
  descriptor now blocks (`HINTS_DAMAGED`); before, it was accepted and attributed by descriptor.
* `BatchlogManager.java`, `HintVerbHandler.java`, `ReadResponse.java`, `SerializationHeader.java`,
  `StorageCompatibilityMode.java`: comments and javadoc only.

**Failure and rollback risks**
* Silent data-path change in the mode: a refused hint is dropped for all targets. It is counted in `compatstatus` and
  logged at WARN at most once a minute per reason. For the batchlog, the batch is deleted all the same, so
  consistency relies on a later repair. Repair is refused in the mode, which the WARN acknowledges ("once the cluster
  runs one release").
* The mode-dependent `messagingVersion(VERSION_30)` is evaluated at read time. A file is read at 11 or 10 depending on
  the mode at that moment. This is safe only if the 10/11 byte identity holds for all content. The patch states it,
  and tests it on the fixture and on the shapes in `hintsOfEveryKindRoundTripThroughAFileOfTheMode`.
* Rollback: the compressed version-1 block layout is not compared with DSE 5.1.51 bytes (the commit says the capture
  holds no compressed file). The preflight check validates it only against this software's own reading of the
  format.
* Changing `hints_compression` across restarts in the mode is refused at start, not at runtime. The behaviour of
  existing compressed files written before the setting changed is not established.
* No start check of version-1 files after the flag drop. The commit says they are delivered there, read at 10, as
  `HintsServiceTest`/`Dse51HintsWriterTest` pin.

**Test integrity**
* Existing tests changed:
  - `DseHintsFileTest`: `messagingVersion()` expectations become mode-dependent (11 in `DSE_5_1`), a change of
    meaning, not a weakening.
  - `V11SerializationAuditTest`: the variable `dse6` is renamed `reader32`, the messages name the mode's release,
    and the LIVE-deletion audit also runs in `DSE_5_1`. Assertions are kept.
  - `Dse51RollbackInspectionTest`: a file named after another host now expects `HINTS_DAMAGED` instead of being
    accepted (stricter), and a correctly named copy is attributed as before.
* New tests:
  - `Dse51HintsWriterTest.theHintsDse51WroteDecodeAndEncodeAgainAtElevenAndTenByteForByte` decodes the records of
    the real DSE 5.1.51 hints capture and re-encodes them byte for byte at 11 and at 10. This is real-reference
    evidence for the codec.
  - `aFileWrittenInTheModeIsTheFileDse51Reads` compares the descriptor with bytes from a test-side writer
    (`DseFixtures.writeDse51HintsFile`), which the test states equals the capture. The records are compared with this
    software's own serialization at 11 (a round trip). The companions are checked against the layout the patch
    attributes to DSE.
  - The compressed-file, past-2038, oversize-hint, log-throttle and LIVE-complex-deletion cases are round trips or
    use a test-side 32-bit reader.
  - `Dse51HintsFileCheck` is exercised on files written by the same code under test.
  - `Dse51ModeSmokeTest` (in-JVM) adds the mode's hints path.
* Not covered by any test in the patch (as the commit states):
  - A DSE 5.1.51 node reading or delivering the files.
  - Compressed block bytes against DSE's.
  - The rotation of a file at its size limit.
  - A trace row's hint.
  - The I/O-error and `LinkageError` catch in the rollback inspection.

**Suggested reviewer questions**
* Should `HintsDescriptor.messagingVersion` stay a pure function of the hints version (reading at 10 always, given the
  identical bytes) instead of depending on the current storage mode at read time?
* Is dropping a hint for every target (instead of failing the write) the right behaviour for `StorageProxy` at
  consistency levels other than `ANY`? The coordinator then treats the replica as neither written nor hinted.
* Given that repair is refused in the mode, how does an operator recover data from refused hints before the flag
  drop, and is the WARN rate limit (once a minute per reason) enough to notice it?
* Is `Pre40DeletionTimeException` caught anywhere that previously matched on `IllegalStateException` text, in the
  DSE 6 path too?

## Group notes (P34, P38)
* Ordering: P38 builds on P34. It uses `StorageCompatibilityMode.DSE_5_1`, the storage/internode pairing enforced in
  `DatabaseDescriptor.applyInternodeCompatibilityMode`, and P34's refactor of
  `UnfilteredSerializer.checkPre40DeletionTimes`. P38 replaces P34's `IllegalStateException` with
  `Pre40DeletionTimeException`. P34's `Upgrader.RefusePast2038` and P38's `Dse51Hints` now share that walk.
* Both rely on the pre-4.0 LIVE deletion-time delta from P10 (`SerializationHeader.pre40LiveLocalDeletionTimeDelta`),
  which they extend from messages to `me` sstables (P34) and to version-1 hints (P38). They also rely on the
  `CellData` 2038 cap, which P34 extends to tools and clients.
* Asymmetric 2038 policy, best read together: P34's `me` writer writes back past-cap times read from DSE sstables
  (CASSANDRA-14092 data) and refuses them only in `sstableupgrade`. P38 refuses any hint holding such a time.
* Evidence of readability by the old product: neither patch runs a DSE 5.1.51 binary. P34 has byte-for-byte
  comparison against six real DSE 5.1.51 sstables, rewritten from their rows with DSE's header, with native LZ4
  required for the compressed components. P38 has byte-for-byte re-encoding of the real DSE 5.1.51 hints records.
  Everything else (flush/compaction output, LIVE complex deletions, row index blocks, compressed hints blocks) is a
  round trip through this software or a test-side reader.
* Both patches extend files added earlier in the DSE 5.1 series: `StartupChecks` DSE 5.1 checks (P29),
  `Dse51Compressors` (P28), `AlterSchemaStatement`/`DefaultSchemaUpdateHandler` refusals (P31) and
  `Dse51RollbackInspection` (P37). Review them against those patches' invariants.
