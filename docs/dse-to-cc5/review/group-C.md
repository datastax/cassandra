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

# Group C — commit log, hints, batchlog and Paxos state written by DSE

Part of the [reviewer triage guide](README.md), which explains the tiers, the review order and the layout of a brief.

## P08 — DSE's commit log segments and hints files at the first start (#2744)

**Tier:** 1 — it renames commit log segments after the startup replay and converts, writes and deletes hints files on
disk during the node start, on the thread that starts hints dispatch.

**Size and shape:** production: one new file (`DseHintsConverter`, ~980 lines) and ~170 added lines in six existing
files (`CommitLog`, `AbstractCommitLogSegmentManager`, `CommitLogSegmentManagerCDC`, `HintsCatalog`,
`HintsDescriptor`, `HintsDispatcher`, `HintsService`), 2 deleted lines. Tests: ~3090 added lines in six files (five
new, one existing file extended). No new fixture files (the tests use the DSE 6.8.65 and 6.9.26 fixtures already in
the tree through `DseFixtures`). The log-message-heavy parts of `DseHintsConverter` (`warnDropped`, `reason`,
`appendReason`, `deleteOrphanCompanions` logging) and the `HintsDescriptor`/`HintsCatalog` regex refactor can be
skimmed.

**Gating:** `DatabaseDescriptor.isDse6CompatibilityMode()` (`internode_compatibility_mode: DSE_6`) for both the
segment rename (`CommitLog.moveAsideReplayedDseSegment`, additionally only for `FlushReason.STARTUP` and only for
segments whose file name carries version 680) and the hints conversion (`DseHintsConverter.convertIfDse6`). The
unacknowledged-hints WARN in `HintsDispatcher` is gated on the peer's messaging version (`DseCompat.isPre40Layout`).
For a cluster that never had DSE: no behaviour change; always-on code paths are a mode check in
`HintsService.startDispatch`, a version check per replayed segment in `CommitLog.recoverSegmentsOnDiskNoArchive`, a
version check on page failure in `HintsDispatcher.sendHintsAndAwait`, and the `HintsDescriptor.pattern` regex
rebuilt from the new `HOST_ID_REGEX` constant (same expression).

**Where to look**
* `src/java/org/apache/cassandra/hints/DseHintsConverter.java` — `convert`: per-file all-or-nothing conversion with
  the commit point at `dseFileDeleter.delete(dseFile)`. Confirm the ordering new file fsync -> directory sync (best
  effort, `SyncUtil.trySyncDir`) -> DSE file delete -> hand-over `catalog.get(hostId).offerLast(descriptor)` is
  crash-safe, and that the `catch (Throwable t)` branch never deletes the new file once `dseFileDeleted` is true.
* `DseHintsConverter.convert` — the per-hint drop rules: host not in the ring (`isPeerInRing`: token metadata or
  `system.peers`, own host id excluded), `UnknownTableException`, unknown column, TTL via
  `Hint.isLive(hint.creationTime, now, hint.ttl())`. Is dropping silently (with a WARN) correct for a host that is
  temporarily absent from both token metadata and `system.peers` at this point of the start?
* `DseHintsConverter.isUnknownColumn` — classifies a decode failure by `e.getClass() == RuntimeException.class` and
  the message text `"Unknown column ... during deserialization of ..."` thrown by `Columns.Serializer` (added by
  d4c5b46fc9d, 2025, CNDB-13403). Should this be a typed exception rather than a string match that a later change
  of the message would silently turn into "file kept forever"?
* `DseHintsConverter.convert` — decodes every record with `Hint.serializer.deserialize(in, VERSION_DSE_68)` for both
  6.8 and 6.9 files and rejects any non-empty descriptor `parameters` (compression or encryption). Is a DSE 6 node
  with compressed or encrypted hints a supported starting point, and does the operator learn it before the switch?
* `src/java/org/apache/cassandra/hints/HintsService.java` — `startDispatch`: the conversion runs inside this
  `synchronized` method before `isDispatchPaused.set(false)`, so it delays the node start by the conversion time.
  Is a large hints directory (many GB) acceptable on this path, and can live hint writes for the same host race
  `freeDescriptor` (name chosen from the DSE file's timestamp, retried on `FileAlreadyExistsException`)?
* `src/java/org/apache/cassandra/db/commitlog/CommitLog.java` — `moveAsideReplayedDseSegment`: renames every
  version-680 segment without failed mutations to `<name>.replayed` after the replay flush, bypassing
  `commitLogSegmentHandler` (the default handler or one installed through `setCommitLogSegmentHandler`). Confirm
  that no custom handler (for example the one a downstream project installs) depends on seeing every replayed
  segment, and that `CommitLogDescriptor.isValid` never matches the `.replayed` name.
* `src/java/org/apache/cassandra/db/commitlog/CommitLogSegmentManagerCDC.java` —
  `handleMovedAsideReplayedSegment`/`deleteUntrackedCdcLink`: the CDC hard link of a moved-aside segment is cleaned
  up under its original name. Confirm a tracked CDC link (index file present) of a DSE segment is still consumable
  after its commit log file was renamed.
* `src/java/org/apache/cassandra/hints/HintsDispatcher.java` — `warnUnacknowledgedByDse6`: WARN only, no change to
  the `Action.ABORT` outcome. The patch states that DSE 6.9 does not acknowledge a hint it drops and assumes the same
  of 6.8, so one such hint blocks the file and later files for that peer until expiry. Is a WARN enough, or should
  the page be skipped?

**Modified or deleted existing code**
* `src/java/org/apache/cassandra/db/commitlog/CommitLog.java` — `recoverSegmentsOnDiskNoArchive`: the per-file
  `segmentManager.handleReplayedSegment(...)` call is now preceded by the DSE move-aside branch (`continue` when the
  segment was renamed). — History: 2b3fe1cc69b (2024, commit subject `cassandra-8773`) and 496fa97caec (2025, CNDB-15916
  / CNDB-11565) introduced the invalid/failed-mutation flags so that segments with skipped mutations of unknown tables
  are kept for a later replay and not replayed over and over. Invariant at risk: a kept segment is the only copy of its
  unapplied mutations; the patch deliberately discards that for DSE-only tables (the renamed file is no longer
  replayed).
* `src/java/org/apache/cassandra/db/commitlog/CommitLogSegmentManagerCDC.java` — `handleReplayedSegment`: CDC link
  cleanup extracted into `deleteUntrackedCdcLink`, behaviour unchanged for the existing path. — History: not
  established for these lines.
* `src/java/org/apache/cassandra/hints/HintsDescriptor.java` — `pattern`: rebuilt from `HOST_ID_REGEX`, same
  expression. — History: not needed (pure refactor).
* `src/java/org/apache/cassandra/hints/HintsDispatcher.java` — `sendHintsAndAwait`: added a WARN before
  `return Action.ABORT`; return value unchanged.

**Failure and rollback risks**
* Rollback to DSE after the first start: the DSE hints files are deleted once converted, and the new files carry
  this software's name and descriptor, which (per the patch's own log text) DSE does not read; hints not yet
  dispatched at rollback time are lost and need a repair. Same for `.replayed` segments: DSE does not replay them
  and the class comment says writes to DSE-only tables not flushed by DSE are lost either way.
* Crash between the new file and the DSE file deletion: documented as "delivered twice, never lost", which depends
  on the best-effort `SyncUtil.trySyncDir` before the commit point holding; a lost directory sync after the DSE file
  delete reached disk would lose the hints. Not tested (cannot be, in a unit test).
* A DSE file that cannot be converted (checksum, cut record, unknown layout) stays in place and fails identically
  at every start, with a WARN each time; nothing escalates it. The node starts.
* Commit log: if the rename fails, the default handler keeps a segment with skipped unknown-table mutations, and it
  is replayed at every start, resurrecting rows whose tombstones were purged since; the WARN says so but the node
  keeps running.
* Commit log: the move-aside decision ignores `hasInvalidMutations`; a DSE segment whose mutations of tables that do
  exist here were skipped for another reason (not an unknown table) is still renamed. Not established whether such a
  case exists.
* Hints dispatch: `DseHintsConverter` returns `null` without doing anything in mode `NONE`, so a node switched from
  DSE straight into `NONE` keeps the DSE files forever, unread.

**Test integrity**
* Existing tests changed: `test/unit/org/apache/cassandra/db/commitlog/CommitLogSegmentManagerCDCTest.java` — two
  tests added (`testMovedAsideReplayedSegmentCleansUpItsUntrackedCdcLink`,
  `testStartupReplayInDse6ModeMovesTheDseSegmentAsideAndCleansUpItsCdcLink`); no existing assertion changed or
  removed.
* New tests: `DseCommitLogReplayTest` replays the captured DSE 6.8.65 and 6.9.26 segments (real files) in both modes.
  `DseHintsContentTest` decodes the captured hints files. `DseHintsConversionTest` (~60 cases) converts hints files
  written by `DseFixtures.writeDseHintsFile`, which rebuilds the descriptor header (host id, timestamp, version,
  parameters) around content taken from the captured fixtures; failure cases inject faults through the
  `DseHintsFileLister`, `HintsWriterOpener` and `dseFileDeleter` test seams and through hand-cut or bit-flipped bytes.
  `DseHintsFileTest` checks that the catalog ignores DSE file names. `DseReplayedSegmentTest` drives
  `moveAsideReplayedDseSegment` directly on header-only segments. `dse6ModeConvertsWhenTheHintsDispatchStarts` runs
  the real `HintsService.startDispatch` call site with a "never alive" endpoint, so nothing is dispatched.
* Not covered by any test in the patch: delivery of a converted hints file to a live peer (no in-JVM cluster test);
  `HintsDispatcher.warnUnacknowledgedByDse6` (no test references it); a real node restart after the segment rename
  (stated by the `DseCommitLogReplayTest` class comment); interaction with a custom `CommitLogSegmentHandler`.

**Suggested reviewer questions**
* Is renaming DSE segments regardless of `hasInvalidMutations` the right trade, given that the original keep-segment
  logic (CNDB-15916) exists precisely so that skipped mutations are not lost?
* Should the hints conversion be moved off the startup path (or bounded), given it runs synchronously inside
  `HintsService.startDispatch` before the node finishes starting?
* Is the string match in `DseHintsConverter.isUnknownColumn` acceptable, or should `Columns.Serializer` throw a typed
  exception that the converter can catch?
* For a DSE 6 peer that does not acknowledge dropped hints, should the dispatcher skip the page (and count it)
  instead of retrying it until the hint expires?
* Are compressed or encrypted DSE hints files ruled out by the preflight tool, since the converter only keeps them in
  place with a WARN?

## P19 — batchlog and Paxos state stored by DSE (#2753)

**Tier:** 1 — it changes the batchlog replay loop and the decoding of `system.paxos` rows for every cluster, including
ones that never ran DSE.

**Size and shape:** production: ~370 added and 29 removed lines in four existing files (`BatchlogManager` ~310,
`PaxosRows` ~45, `PartitionUpdate` 15, `SystemKeyspace` 1); no new production files. Tests: ~2440 added lines in
seven files (six new; `BatchlogManagerInterceptorTest` extended). The log-message construction in
`BatchlogManager.keepUndecodableBatch`/`warnAboutUndecodableBatches` and the `getAccepted` overload plumbing can be
skimmed.

**Gating:** none for the decoding. The stored-version mapping (`DseCompat.storedDecodingVersion`,
`DseCompat.isUnknownStoredDseVersion`, added by P02) is applied in every mode, by design ("the rows outlive the
mode"); `isDse6CompatibilityMode()` only changes a phrase of the ERROR text. Always-on changes for a cluster that never
had DSE:
* a batch whose mutations fail to decode with a `RuntimeException` or `AssertionError` is now kept in
  `system.batches` and skipped until the next start, instead of the exception leaving the replay loop;
* a batch whose `version` cell is missing or not 4 bytes is kept instead of failing in `row.getInt("version")`;
* a batch with an empty or null `mutations` list is removed as replayed instead of reaching `ReplayingBatch`;
* a batch stored at a version below `VERSION_40` that fails to decode with an `IOException` is now kept instead of
  dropped (the drop is preserved only for `VERSION_40 <= version < DSE_60_VERSION`);
* `PaxosRows.getCommitState` returns `null` for a row with neither an in-progress nor a committed ballot;
* `PaxosRows.proposalIsEmpty` now passes the mapped version to `PartitionUpdate.isEmpty`, which skips 8 extra bytes
  at `VERSION_DSE_68`.

**Where to look**
* `src/java/org/apache/cassandra/batchlog/BatchlogManager.java` — `processBatchlogEntries`: new
  `undecodableBatches` skip and the restructured `try`. Settle whether keeping a batch (row never deleted, skipped
  until the next start because the scan bound `token(id) > token(lastReplayedUuid)` moves past it) is acceptable for
  clusters that never ran DSE, compared with the old behaviour of the cycle aborting on such a row.
* `BatchlogManager.ReplayingBatch.addMutations` — the IOException split: `VERSION_40 <= version < DSE_60_VERSION`
  keeps the drop, everything else becomes `UndecodableBatchException`; `UnknownTableException` is rethrown first and
  still dropped. Is the version range right, and is dropping a DSE batch that writes to a table unknown here (the
  `IOException` branch with the DSE-specific WARN) intended when some of its other mutations target known tables?
* `BatchlogManager.retryBatchesAwaitingInterceptor` — the new `UndecodableBatchException` catch: wrapped
  `IOException` drops the interceptor callbacks and deletes the row, anything else is retried. Check this keeps the
  semantics of a132f1c2669 (2026, HCD-438) for batches written by this software.
* `BatchlogManager.warnAboutUndecodableBatches` — a point read per kept batch every 10 minutes on the batchlog thread;
  `undecodableBatches` is an unbounded `HashMap` accessed from that thread only. Any concern with many corrupt rows?
* `src/java/org/apache/cassandra/service/paxos/uncommitted/PaxosRows.java` — `getUpdate`: an unknown DSE-range
  version throws `IllegalStateException`, which fails every LWT on the partition until the row expires or is deleted.
  Is failing the LWT (rather than ignoring the row) the right choice?
* `PaxosRows.proposalIsEmpty` — an unknown DSE-range proposal version returns `false` (in progress), so that the key
  is not taken for committed. Confirm Paxos repair and `PaxosStateTracker` behave as the comment states.
* `PaxosRows.getCommitState` — new `return null` for a row without any ballot. Both callers
  (`PaxosMemtableToKeyStateIterator.computeNext`, `PaxosStateTracker`) already `continue` on `null`; confirm no other
  consumer relies on a key state for such a row.
* `src/java/org/apache/cassandra/db/partitions/PartitionUpdate.java` — `isEmpty(ByteBuffer, Flag, DecoratedKey, int)`:
  offset +8 at `VERSION_DSE_68`. Check it matches the deserializer's skip of DSE's maximum timestamp.

**Modified or deleted existing code**
* `src/java/org/apache/cassandra/batchlog/BatchlogManager.java` — `processBatchlogEntries`: `int version =
  row.getInt("version")` replaced by `storedVersion(row)`; `new ReplayingBatch(...)` wrapped with empty-mutation and
  null-version handling; new `catch (UndecodableBatchException)`. — History: the loop and its `catch (IOException)`
  drop come from 92c38c0e6a5 (2014, CASSANDRA-6134, asynchronous replay), 53a177a9150 (2015, CASSANDRA-9673) and
  762db474273 (2015, CASSANDRA-7237, scan from `lastReplayedUuid`); ba87ab4e954 (2017) for the log line. Invariant:
  a processed batch is either deleted or replayed, and the scan never revisits rows behind the bound; kept batches are
  a new class of live rows behind the bound.
* `BatchlogManager.ReplayingBatch.addMutations` — decoding now at `DseCompat.storedDecodingVersion(version)` with
  three catch blocks. — History: 53a177a9150 (2015, CASSANDRA-9673) and 87097066e7c (2014, CASSANDRA-6999,
  truncation records). Invariant: an `IOException` drops the batch; now only for the `VERSION_40`..`DSE_60_VERSION`
  range.
* `BatchlogManager.retryBatchesAwaitingInterceptor` — `catch (IOException)` and `catch (Throwable)` bodies extracted
  into `dropInterceptorCallbacks` and `retryInterceptorRead`, plus the new catch. — History: a132f1c2669 (2026,
  HCD-438, interceptor hook). Invariant: an unclassified error never deletes the batch.
* `src/java/org/apache/cassandra/service/paxos/uncommitted/PaxosRows.java` — `getUpdate`, `getAccepted`,
  `getCommitted`, `proposalIsEmpty`, `getCommitState`. — History: d2923275e36 (2020, CASSANDRA-17164, CEP-14 Paxos
  improvements); the `VERSION_40` default reads from 4bbfd64fcd7 (2023, CASSANDRA-18314). Invariant: `getCommitState`
  always returned a key state for a row of the target table.
* `src/java/org/apache/cassandra/db/partitions/PartitionUpdate.java` — `isEmpty(ByteBuffer, Flag, DecoratedKey)` now
  delegates to a new versioned overload. — History: not established for these lines (blame spans several commits,
  including aa4a63314fd from 2022, STAR-1042).
* `src/java/org/apache/cassandra/db/SystemKeyspace.java` — `loadPaxosState`: passes the partition key to
  `PaxosRows.getAccepted` (for the error message only).

**Failure and rollback risks**
* Always-on: a corrupt batch that previously aborted every replay cycle (and so was noticed) is now kept with an
  ERROR once per start and a WARN every 10 minutes; its mutations may stay missing on some replicas until an operator
  acts. This is a semantics change for clusters that never ran DSE.
* Kept batches are never deleted automatically, even past `gc_grace_seconds`; the ERROR tells the operator to save,
  delete and repair by hand.
* The DSE 6.0 / 6.0.3 mapping to `VERSION_40` and the batchlog mapping as a whole are inferred: per the `DseCompat`
  comment from P02, no `system.batches` row written by DSE was captured; only DSE 6.8.65 and 6.9.26 Paxos rows are
  real fixtures.
* LWT on a partition holding a Paxos row of an unknown DSE version fails until the row expires (TTL from the table's
  `gc_grace_seconds`) or a superuser deletes it on every replica.
* Rollback to DSE: DSE batches replayed by this software are deleted from `system.batches`, after their mutations
  were applied; no row is rewritten. Whether DSE reads the `system.paxos` rows this software writes after the switch
  is not decided by this patch (not established here).

**Test integrity**
* Existing tests changed: `test/unit/org/apache/cassandra/batchlog/BatchlogManagerInterceptorTest.java` — one test
  added (`testBatchAwaitingInterceptorThatNoLongerDecodes`); no existing assertion changed or removed.
* New tests: `DsePaxosRowsTest` and `DsePaxosStateTest` (in-JVM cluster) read the real DSE 6.8.65 `system.paxos`
  rows and a real DSE 6.9.26 `system.paxos` sstable. `MixedModePaxosV1Test` runs LWTs between real HCD 1.2 nodes and
  nodes of this software. The batchlog tests (`DseBatchlogReplayTest`, `DseBatchlogStateTest`,
  `MixedModeDseBatchlogTest`) build `system.batches` rows by hand: mutations taken from the DSE hints fixtures, tagged
  with versions 1024 and 1536 chosen by the test, not rows DSE wrote. Corrupt-row cases use hand-cut bytes.
* Not covered by any test in the patch: DSE 6.0 / 6.0.3 stored versions (256, 512) with real bytes; the always-on
  change for a corrupt batch of this software's own version raising a `RuntimeException` is covered only by
  hand-built rows; behaviour of a kept batch across a node restart in an in-JVM cluster is not established.

**Suggested reviewer questions**
* Should keeping undecodable batches be limited to DSE-range versions, leaving the old behaviour for batches stored
  by this software and HCD?
* Is it correct that a pre-4.0 version with an `IOException` is now kept while `VERSION_40`..`DSE_60_VERSION` is still
  dropped? Can such rows exist on a node of this software?
* For a DSE batch where one mutation targets an unknown (DSE-only) table, the whole batch is dropped through the
  `UnknownTableException` path; should the mutations to known tables still be replayed?
* Is failing every LWT on a partition with an unknown DSE Paxos version preferable to treating the row as absent?
* Is the inference that DSE stores its messaging version in `system.batches.version` enough without a captured row?

## Group notes
* P08 and P19 share the DSE hints fixtures: the P19 batchlog tests reuse the mutations of the hints fixtures that P08
  converts, so both rely on the same `VERSION_DSE_68` mutation decoding (`PartitionUpdate` / `Mutation` serializers,
  already in the base before P08).
* P08 is gated on `DSE_6` mode while P19 decodes DSE-stored rows in every mode; reviewers should check that the two
  policies are intended (a node switched straight into `NONE` replays DSE batches and Paxos state but never converts
  DSE hints and replays DSE segments at every start if they hold unknown-table mutations).
* Both patches choose "keep and warn" for undecodable DSE data (unconvertible hints files left in place, undecodable
  batches kept), with operator remediation by hand followed by a repair after the compatibility mode is dropped.
* P19 depends on `DseCompat.storedDecodingVersion` / `isUnknownStoredDseVersion` from P02 and on the dtest harness
  `DseDialectTestBase` from P13 (its base); P08 depends on `CommitLogDescriptor.VERSION_DSE_68` and the startup check
  that lets version-680 segments through (outside this group).
