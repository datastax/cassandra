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

# Group D — materialized views

Part of the [reviewer triage guide](README.md), which explains the tiers, the review order and the layout of a brief.

## P07 — sstables of DSE materialized views (#2742)

**Tier:** 1 — changes the sstable read path (`UnfilteredSerializer` row decoding, `SerializationHeader` column
resolution) and the header every `SSTableWriter` is built with, in every compatibility mode including `NONE`.

**Size and shape:** 6 production files (~990 added lines: `DseViewHiddenColumnCatalog` 543 and `DseViewHiddenColumns`
293 new, ~150 in `SerializationHeader`, `UnfilteredSerializer`, `SSTableWriter`, `CassandraDaemon`); 5 test files
(~2150 lines, including a 12-line hook in the in-JVM `Instance`); 355 fixture files (~3600 stat lines, almost all
binary): `test/data/dse-compat/README.md` plus `test/data/dse-compat/sstables/dse-6.8.65-mv-updates/` and
`dse-6.9.26-mv-updates/` (`system_schema/hidden_columns`, `test_ks/base`, `base_by_v`, `legacy_base`, `legacy_by_v`,
eight flushes each). Fixtures can be skimmed (`--stat` only); the README is the provenance record.

**Gating:**
* Reading a hidden column from an sstable header (`DseViewHiddenColumns.resolve`) is **not** gated by the mode. It
  applies to a view sstable whose header names a non-static column `.<c>` that is neither in the schema nor dropped,
  where `<c>` is a view primary-key column that is a regular column of the base table, of the same type. Before this
  patch, such a header failed with `UnknownColumnException`; other `.`-prefixed unknown columns still fail, with a
  new message.
* `DseViewHiddenColumnCatalog.checkViewsAtStartup` runs only when `DatabaseDescriptor.isDse6CompatibilityMode()` is
  true and the schema has at least one view.
* What changes for a cluster that never had DSE: `SerializationHeader.make` and
  `SSTableWriter.Builder.setSerializationHeader` now always call `withoutDseViewLivenessColumn()`, which returns
  the same header when the field is null.
  `UnfilteredSerializer.deserializeRowBody` gains one null check per row. `SerializationHeader.toString` is unchanged
  when the field is null. The new startup call returns at once outside the mode. In-JVM `Instance` startup also
  calls the check, which does nothing outside the mode.

**Where to look**
* `src/java/org/apache/cassandra/db/rows/UnfilteredSerializer.java` — `deserializeDseViewRowBody`: the row's encoded
  liveness is used only to decode the cells. It is then replaced by the hidden cell's liveness, or by
  `LIVENESS_WITHOUT_HIDDEN_CELL` (an expired liveness at `NO_TIMESTAMP + 1`) when the row has no hidden cell. Is it
  correct to add the liveness after the row deletion (the comment says "the builder does not keep a liveness the row
  deletion shadows")? Does a skipped/unfetched column (helper) path need the same handling for the hidden cell, which
  bypasses `readSimpleColumn`?
* `src/java/org/apache/cassandra/db/view/DseViewHiddenColumns.java` — class Javadoc: the three facts about DSE the rule
  depends on are stated: the hidden cell is written in every update of a live entry; DSE uses the same cell order as
  this software; every DSE view header names the hidden column. They are checked against the two fixtures only.
  The reviewer should decide whether the cell-order claim ("after a column named `-x`, before `a`") holds for every
  type, and whether a header that does not name the column (which the Javadoc admits would be misread) can occur in
  practice.
* `src/java/org/apache/cassandra/db/SerializationHeader.java` — `Component.toHeader`: `resolve` is called only when
  `column == null` and there is no dropped column. A second hidden column fails through `multipleRequiredColumns`. Is
  the check `type.unwrap().equals(viewColumn.type.unwrap())` strict enough for reversed or frozen view key types?
* `src/java/org/apache/cassandra/io/sstable/format/SSTableWriter.java` — `Builder.setSerializationHeader`: removes the
  hidden column from every header passed in. Do all writers that copy an input header (scrub, cleanup, anticompaction,
  partial streaming, `SSTableImporter`) go through this setter? Entire-sstable streaming does not, and the Javadoc
  says this is deliberate.
* `src/java/org/apache/cassandra/db/view/DseViewHiddenColumns.java` — `LIVENESS_WITHOUT_HIDDEN_CELL`: the Javadoc
  admits that sstables written by scrub or partial stream keep this liveness. Their minimum timestamp is then the
  smallest possible value until a compaction purges it. Check the effect on compaction strategies that use sstable
  timestamps (overlap, expired-sstable drop, TWCS windows).
* `src/java/org/apache/cassandra/db/view/DseViewHiddenColumnCatalog.java` — `load`: opens DSE's
  `system_schema.hidden_columns` sstables offline. The table metadata is rebuilt by hand in `hiddenColumnsMetadata`,
  with the partitioner of `system_schema.columns`. Only FINAL files of committed transactions are read. Is that
  hand-built schema exactly DSE's (column types, clustering order)?
* `DseViewHiddenColumnCatalog.startupProblems` / `reproduces`: types are compared as CQL text
  (`derived.type.asCQL3Type().toString().equals(column.type)`), and any other spelling refuses the view. Confirm that
  this fails closed, not open.
* `src/java/org/apache/cassandra/service/CassandraDaemon.java` — `setup`: the check runs after
  `Keyspace.setInitialized()` and the auto-compaction loop, before `loadRowAndKeyCacheAsync` and the commit log
  replay. P26 relies on this order.

**Modified or deleted existing code**
* `SerializationHeader.java` — `Component.toHeader`: the unconditional
  `throw new UnknownColumnException("Unknown column … during deserialization")` now goes through `resolve` first.
  History: the line was last touched by 310a48efcfd (2019, CASSANDRA-15066). The unknown-column refusal dates from
  a991b64811f (2014, CASSANDRA-8099), and the dropped-column branch in front of it from d52317f5b5a (2026, CNDB-16146).
  Invariant: no sstable is read with a column the schema does not know about.
* `SerializationHeader.java` — `make`: `columns.addAll(sstable.header.columns())` now strips the hidden column.
  History: af3fe39dcab (2016, CASSANDRA-9425). The header of a compaction output is the union of its input headers.
* `SSTableWriter.java` — `Builder.setSerializationHeader`: it used to store the header as given; it now stores a copy
  without the hidden column. History: b7e1e44a909 (2023, CASSANDRA-17056). Invariant: a written sstable's header
  describes exactly the rows written.
* `UnfilteredSerializer.java` — `deserializeRowBody`: a new early branch is added before
  `builder.addPrimaryKeyLivenessInfo(rowLiveness)`. History: 2457599427d (2015, CASSANDRA-9705).

**Failure and rollback risks**
* Rollback to DSE: the Javadoc states that view rows written by this software, including every compaction or scrub
  output, carry no hidden cell, so DSE treats them as dead. Views must be rebuilt after a rollback.
* `NONE` mode: a DSE view sstable is read with the new rule without any catalog check. The startup cross-check of
  `hidden_columns` against sstable headers is skipped.
* Repair: the Javadoc states that DSE's Merkle trees include the hidden cells. Incremental repair over a sstable
  carrying `LIVENESS_WITHOUT_HIDDEN_CELL` may stream dead rows again.
* `sstableloader` still refuses DSE view sstables (stated in the Javadoc).
* The catalog load failure is remembered until restart (`loadFailure`). A transient I/O error therefore blocks
  startup in DSE_6 mode until the node restarts, and `JVMStabilityInspector.inspectThrowable` may stop the node.
* `DseViewUpdatesFixtureTest.viewSSTablesGiveTheRowsDseAnswered` asserts `nowInSeconds() < 2093214360` (2036-05-01).
  After that date the fixtures must be recorded again.

**Test integrity**
* Existing tests changed: none (only the in-JVM `Instance` harness gains the startup call).
* New tests: `DseViewUpdatesFixtureTest` uses real DSE 6.8.65 and 6.9.26 sstables, checks them against DSE's
  `sstabledump` and cqlsh output in the fixture, and runs a major compaction plus a purge. `DseViewHiddenColumnsTest`
  (scrub, streaming, merge with local view updates) and `DseViewHiddenColumnCatalogTest` (real DSE `hidden_columns`
  sstable, corrupt or unlistable directories) import fixtures, but some cases build the catalog by hand
  (`new DseViewHiddenColumnCatalog(map)`). `Dse6ViewStartupTest` is an in-JVM single node with copied fixture
  directories.
* Not covered by any test in the patch: anticompaction and cleanup of a DSE view sstable; cursor-based readers other
  than compaction (cursor compaction is excluded for strict-liveness tables in `CompactionTask`; whether other cursor
  readers apply the rule is not established); a DSE view sstable whose header does not name the hidden column.

**Suggested reviewer questions**
* Is it acceptable for a reader change to apply in `NONE` mode without consulting `hidden_columns`, given that the
  `.`-prefixed name check is the only guard?
* Which compaction strategies read sstable min/max timestamps in a way that a `Long.MIN_VALUE + 1` liveness would
  distort, and should scrub/partial-stream outputs be forced through a purge?
* Does any code path deserialize view rows without `UnfilteredSerializer` (cursor readers, verify, digest of
  repaired data)? If so, it would see the raw primary key liveness.
* Is the cell-order assumption (hidden column placed by name among simple columns) safe for names that sort before
  `.` (the Javadoc cites `-x`)?

## P20 — materialized view data exchanged with DSE 6 peers (#2756)

**Tier:** 1 — changes message serialization of read commands, read responses, digests and partition updates
(`ReadCommand`, `ReadResponse`, `PartitionUpdate`, `ColumnFilter`, `Columns`, `SerializationHeader`, `RowFilter`).

**Size and shape:** 15 production files (~800 added lines, 413 of them in the new `DseViewWireFormat`; ~300 across 12
existing core files; `BatchlogManager` 70 is log text only); 10 test files (~1580 lines, `DseViewWireFormatTest` 1197,
in-JVM `MixedModeMaterializedViewTest` 146). No fixtures. Changes to `DseViewHiddenColumnCatalog` and
`DseViewHiddenColumns` are mostly Javadoc and message text.

**Gating:** `DseViewWireFormat.translates`: `metadata.isView() && version == DseCompat.DSE_DIALECT (11) &&
DseCompat.enabled()`. The digest path is gated by `command.digestVersion() == 11`. A view is translated only when
the catalog lists exactly the derived hidden column. Always-on changes for a cluster that never had DSE:
* `ColumnFilter` gains a `dseColumnFilter` field (null), new constructors, and `withDseViewHiddenColumn` /
  `withoutDseViewHiddenColumn` methods. The field is not part of `equals`.
* `ReadCommand.Serializer.serialize/serializedSize/deserialize`, `ReadResponse.makeDigest`, the `LocalDataResponse`
  constructor, `RemoteDataResponse.makeIterator/digest` and `PartitionUpdate.serialize/serializedSize` each call a
  `DseViewWireFormat` helper per message. Outside the mode the helper returns the original object.
* `UnfilteredRowIteratorSerializer.deserializeHeader` builds the iterator's columns from
  `sHeader.withoutDseViewLivenessColumn()`, which returns the same header when the field is null.
* `BatchlogManager.handleUndecodable`: the ERROR text for kept batches now comes from `describeRepair`, and the advice
  for views differs. It is log-only, but it changes existing message text in every mode.

**Where to look**
* `src/java/org/apache/cassandra/db/view/DseViewWireFormat.java` — `withHiddenCell` / `hiddenCellOf`: outbound
  rows get a `.v` cell derived from the primary key liveness. An expired liveness becomes a live `NO_TTL` liveness
  plus a hidden tombstone. Rows with empty liveness are sent without a hidden cell. Does a partial update made by this
  software (a non-key column changed, empty liveness) merge correctly on a DSE replica that holds the hidden cell in
  another sstable?
* `src/java/org/apache/cassandra/db/view/DseViewWireFormat.java` — `digest(Row, …)` / `digestHiddenCell`: the code
  states that this digest is "neither … checked against the digests of real DSE nodes". Should it be verified
  against a captured DSE digest before merge, or is a guaranteed digest mismatch (an extra data read) acceptable?
* `src/java/org/apache/cassandra/db/ReadCommand.java` — `Serializer.deserialize`: the local read runs with
  `columnFilter.withoutDseViewHiddenColumn(dseHidden)`, and the original filter is kept inside the new filter. Does
  every replica-side copy of the command (index query plans, the synthetic-column `tmb` rebuild just below,
  short-read or paging copies) keep that same `ColumnFilter` instance? If one does not, the response is serialized
  without hidden cells and a DSE coordinator would see rows with no hidden cell.
* `src/java/org/apache/cassandra/db/ReadResponse.java` — `LocalDataResponse(iter, command, rdi)` / `forCoordinator`
  and `RemoteDataResponse.digest`: data in DSE's encoding, digests computed by `digestDseData` with
  `keepDseViewHiddenCells = true`. Check that `serializedSize` and `serialize` see the same transformed iterator.
  Check the pre40 re-serialization note in the Javadoc above `pre40Data`.
* `src/java/org/apache/cassandra/db/SerializationHeader.java` — `Serializer.deserializeForMessaging`: a view at
  version 11 always gets `dseViewLivenessColumn`, even when the peer's header does not name `.v`, so every row
  without a hidden cell gets no liveness. Is that right for peers on DSE's old view format? The catalog lists nothing
  for those views, so `hiddenColumn` returns null there; confirm.
* `src/java/org/apache/cassandra/db/partitions/PartitionUpdate.java` — `forPeer`: applied in both `serialize` and
  `serializedSize`. Can a catalog `IllegalStateException` thrown here (on the outbound path) leave a partially
  written frame?
* `src/java/org/apache/cassandra/db/filter/RowFilter.java` — `Serializer.deserialize`: a filter on a hidden column is
  refused with `UnknownColumnException` and counted. It was a `RuntimeException` before; is that change of exception
  type handled by the verb handler?
* `src/java/org/apache/cassandra/db/view/DseViewWireFormat.java` — `responseHiddenColumn`: the Javadoc says a DSE
  filter that does not fetch `.v` "costs a data read …, not a wrong row". Confirm this against DSE's rule that a row
  without a hidden cell is dead.

**Modified or deleted existing code**
* `ReadCommand.java` — `Serializer.serialize/serializedSize/deserialize`: `command.columnFilter()` is replaced by
  `DseViewWireFormat.columnFilterFor(...)`, and the deserialized filter is rewritten. History: a991b64811f (2014,
  CASSANDRA-8099). Invariant: the replica runs exactly the filter the coordinator sent.
* `ReadResponse.java` — `makeDigest`: History: 39eb7db65fd (2019, CASSANDRA-15461). Invariant: every replica digests
  the same rows identically. `LocalDataResponse` constructor: added by this series (P10, the LIVE
  complex deletion patch). `RemoteDataResponse.makeIterator` `command.columnFilter()` argument: 3e37b4a90d4 (2016,
  CASSANDRA-10762). Invariant: the response is decoded with the filter it was encoded with.
* `PartitionUpdate.java` — `PartitionUpdateSerializer.serialize/serializedSize`: History: 4fb559b58a8 (2015,
  CASSANDRA-9986, from the commit subject). Invariant: `serializedSize` matches the bytes written.
* `Columns.java` — `Serializer.deserialize`: the unknown-column branch first tries `resolveHiddenColumn`. History:
  c378874a9fa (2022, CASSANDRA-15511).
* `RowFilter.java` — unknown-column branch: History: 64e2f5ddaa5 (2015, CASSANDRA-10217); the comment refers to
  CASSANDRA-16217.
* `ColumnFilter.java` — `deserializeRegularAndStaticColumns`: History: 94942404200 (2021, a merge commit; ticket not
  established).
* `UnfilteredRowIteratorSerializer.java` — `deserialize(…, Header)`: the columns now come from
  `withoutDseViewLivenessColumn()`. History: 8a9796902b9 (2015, CASSANDRA-9847).
* `SerializationHeader.java` — `deserializeForMessaging` regulars line: 1d87da3f6fc (2021, CASSANDRA-16735).
  Constructor call: af3fe39dcab (2016, CASSANDRA-9425).
* `UnfilteredSerializer.java` — `deserializeDseViewRowBody`: the `LIVENESS_WITHOUT_HIDDEN_CELL` fallback now applies
  only when `header.isForSSTable()`. Added by this series (P07).
* `BatchlogManager.java` — `handleUndecodable`, `describeTables`: added by this series (P19, the
  batchlog and Paxos state patch).

**Failure and rollback risks**
* Concurrency: `DseViewHiddenColumnCatalog.instance()` may load the catalog lazily on a messaging thread, under a
  class lock, when the startup check did not load it. This happens only if the node had no views at startup.
* Catalog failure at runtime: `hiddenColumn` throws `IllegalStateException` from serializers in both directions.
  The `ReadResponse.digest` path wraps `IOException` only.
* Mixed versions: the Javadoc states a known limit. A DSE row's primary key liveness (that of the base row) can
  differ from this software's, so reads at CL > ONE across versions always fall back to a data read for such rows.
* Rollback: no new on-disk state. Inbound rows are stored without hidden cells (see the P07 rollback note).
* Mutations DSE stored on disk are not translated in this patch; P26 adds that.

**Test integrity**
* Existing tests changed: `Dse6ViewStartupTest`, `DseViewHiddenColumnCatalogTest`, `DseViewUpdatesFixtureTest`: log
  and exception texts updated to the new wording, one test renamed
  (`startupRefusesTheViewsWhoseDataCannotBeRead` → `…CannotBeExchanged`), and `@AfterClass` null guards added. No
  assertion was weakened. `DseDialectTestBase` (shared harness): every in-JVM dialect test now creates an empty DSE
  `hidden_columns` directory before upgrading a node. Without it, a node with views would refuse to start.
* New tests: `DseViewWireFormatTest` is the only coverage of the translation. The DSE side is simulated: this
  software's serializers run on view metadata with `.v` added (`asDse`). Only the encoding is anchored to the DSE
  6.8.65 sstable fixture. `MixedModeMaterializedViewTest` (in-JVM, HCD 1.2 as the DSE stand-in) runs with an empty
  catalog, so the views are exchanged untranslated. Its Javadoc says the translation "was observed with DSE 6.9.26
  nodes"; that cannot be verified from the repository. `ColumnFilterTest` and `DseBatchlogReplayTest` additions are
  unit tests.
* Not covered by any test in the patch: digests compared with a real DSE replica; read repair writes and hints sent
  to a DSE peer for a translated view; a replica-side command copy that drops `dseColumnFilter`.

**Suggested reviewer questions**
* Can the digest encoding be checked against bytes captured from a DSE 6.8/6.9 node, given that it decides whether
  every mixed-version view read at CL > ONE does an extra round trip?
* Is keeping `dseColumnFilter` outside `equals` safe for any cache or map keyed on `ColumnFilter` or `ReadCommand`?
* Should `withHiddenCell` also emit a hidden cell for rows with empty primary key liveness, which this software
  writes for partial view updates, or does DSE never need one there?
* Is it acceptable to change `BatchlogManager` ERROR text in all modes in a view-wire patch?

## P26 — view updates DSE stored on its own disk (#2764)

**Tier:** 1 — changes how commit log replay, batchlog replay and hints conversion decode stored DSE mutations
(`Columns`, `SerializationHeader.deserializeForMessaging`) at `VERSION_DSE_68`.

**Size and shape:** 6 production files (~210 added lines; mostly `DseViewWireFormat` +138 and
`DseViewHiddenColumnCatalog` +45; one-line hooks in `Columns`, `SerializationHeader`; Javadoc in
`UnfilteredSerializer`, `DseHintsConverter`); 6 test files (~950 lines), including the new in-JVM
`Dse6StoredViewUpdatesTest`. No fixtures.

**Gating:** `DseViewWireFormat.translatesStored`: `metadata.isView() && version == MessagingService.VERSION_DSE_68 &&
DseCompat.enabled()`. Translation happens only when the catalog can be read and lists exactly that column; otherwise
the update fails to decode as before. For a cluster that never had DSE: nothing (no data is decoded at
`VERSION_DSE_68` outside DSE files, and the mode is required).

**Where to look**
* `src/java/org/apache/cassandra/db/view/DseViewWireFormat.java` — `inboundHiddenColumn`: stored data uses
  `readableInstance()` / `storedTranslatedColumn`, which swallow `IllegalStateException` and return null. If the
  hidden column is not named in the header, the update then decodes with the plain rule. Can a stored DSE view row
  without a `.v` cell whose catalog lookup failed be applied as live, when the same row would be dead under the
  translation?
* `DseViewWireFormat.resolveStoredHiddenColumn`: returns null on every uncertain case (WARN once per view), so the
  caller throws "Unknown column". Confirm that each consumer (commit log replay, batchlog, `DseHintsConverter`)
  turns that into keep/drop/fail as the commit message states and never into a partial apply.
* `src/java/org/apache/cassandra/db/Columns.java` — `Serializer.deserialize(…, storedByDse)`: the stored path is an
  `else if`, so it runs only when the wire flag is false. The two flags are exclusive by version; check that no
  caller passes both.
* `src/java/org/apache/cassandra/db/view/DseViewHiddenColumnCatalog.java` — `readableInstance` / `instance` split:
  refusals are no longer counted for stored data. Check that `instance()` still counts exactly once per call, as
  before.
* Startup ordering (`CassandraDaemon.setup`): the catalog must be loaded before the commit log replay, which happens
  only when views exist. Hints conversion and batchlog replay come later; verify that this still holds on the
  feature branch.

**Modified or deleted existing code**
* `Columns.java`, `SerializationHeader.java`, `UnfilteredSerializer.java`, `DseViewHiddenColumnCatalog.java`,
  `DseViewWireFormat.java`: every modified line was added by this series (P07, P20).
* `DseHintsConverter.java`: Javadoc only. The class was added by this series (the commit log and hints patch).

**Failure and rollback risks**
* Idempotency: the commit message states that a batch is replayed or kept exactly as before, never twice, and that
  nothing kept before is dropped. Dropped hints are counted, not retried. A commit log mutation that fails to decode
  fails its replay (this blocks startup, as before).
* A rerun after a half-finished start: a kept batch or an unconverted hints file is decoded again with the same
  catalog, so the same decision is made.
* Rollback to DSE: replayed view updates are written without hidden cells, so DSE treats them as dead (P07 note).
* DSE 6.0 batches (`VERSION_40`) are not translated. The commit message says DSE views with hidden columns started
  in DSE 6.7.

**Test integrity**
* Existing tests changed: none weakened. `DseFixtures` and `DseHintsConversionTest` helpers were refactored (hints
  fixture builders generalised).
* New tests: there is no DSE-written stored view update anywhere in the fixtures (stated in the commit message).
  Every stored update is built by `DseFixtures.dse68Mutation`, which serializes with this software at `VERSION_40`
  and splices in a max-timestamp field. `DseBatchlogReplayTest` checks that helper byte for byte against the
  mutations in the DSE 6.8.65/6.9.26 hints fixtures (non-view tables). The view update itself
  (`DseFixtures.dseViewUpdate`) is built by this software. `Dse6StoredViewUpdatesTest` is an in-JVM test whose
  stored data is the same hand-built bytes.
* Not covered by any test in the patch: a real DSE commit log segment or batch carrying a view update; a stored view
  row without a `.v` cell when catalog lookup failed.

**Suggested reviewer questions**
* Can a DSE 6.8/6.9 commit log segment or batchlog containing a new-format view update be captured as a fixture,
  given that the layout of a view update with `.v` is only inferred?
* When the catalog cannot decide, should a stored update that does not name `.v` (for example a row deletion) still
  be applied untranslated, as now, or refused too?
* Is the WARN once per view (`untranslatedStoredViews`) enough for an operator to notice that a commit log replay
  failed for that reason?

## Group notes
* The patches must be applied in order P07 → P20 → P26. P20 reuses P07's `SerializationHeader.dseViewLivenessColumn`
  and `deserializeDseViewRowBody`, and narrows P07's `LIVENESS_WITHOUT_HIDDEN_CELL` fallback to `isForSSTable()`
  headers. P26 reuses P20's catalog and resolution, split into a counted path (wire) and an uncounted path (stored).
* The same rule (row liveness taken from the `.v` cell) has three entry points: sstables (every mode, no catalog),
  the wire at version 11 (DSE_6 mode, catalog required), and stored mutations at `VERSION_DSE_68` (DSE_6 mode,
  catalog required, fails silently to the old behaviour). Read the gating difference between P07 and P20/P26
  together.
* No test anywhere in the group compares against a live DSE node. The sstable side (P07) is anchored to real DSE
  fixtures. The wire side (P20) and the stored side (P26) use bytes produced by this software on modified metadata.
* External dependencies: `DseCompat`, `DseCompatRefusals` and the pre-4.0 codec (`isPre40Layout`, `DSE_DIALECT`)
  come from earlier patches. P20 builds on the batchlog patch (P19), and P26 on the hints
  converter and commit log replay of an earlier patch. `DseDialectTestBase` (in-JVM harness) is changed by P20 for
  every dialect test.
* After the views are rebuilt and every DSE sstable has been compacted, none of this code runs. Rollback to DSE
  always requires rebuilding views, a fact stated only in the `DseViewHiddenColumns` Javadoc.
