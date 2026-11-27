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

# Group E — SAI on-disk format

Part of the [reviewer triage guide](README.md), which explains the tiers, the review order and the layout of a brief.

## P09 — SAI on-disk version pinned to the DSE release (#2743)

**Tier:** 1 — it decides which SAI on-disk version every index file flushed or compacted during the mixed phase
carries (the pin of `cassandra.sai.latest.version`), by a new startup check inserted in the default startup sequence
of every node; the code itself only refuses a start, but a wrong or missing pin is what a rollback to DSE would pay for.

**Size and shape:** production: one new class `src/java/org/apache/cassandra/service/Dse6SaiVersionPin.java`
(~415 lines), ~75 lines in existing files (`StartupChecks`, `CompatStatus`, `MessagingServiceMBean(Impl)`,
`NodeProbe`, `CassandraRelevantProperties`, comments in `conf/cassandra.yaml`, `conf/cassandra_latest.yaml`,
`conf/jvm-server.options`). Tests: new `test/unit/org/apache/cassandra/service/Dse6SaiVersionPinTest.java` (~554
lines), ~90 lines added to `CompatStatusTest` and the in-JVM `Dse6ModeSmokeTest`. The nodetool, MBean and yaml
comment hunks can be skimmed.

**Gating:** `internode_compatibility_mode: DSE_6` (`DatabaseDescriptor.isDse6CompatibilityMode()`); the override
`-Dcassandra.internode_compatibility_mode.ignore_sai_version_pin=true` turns every refusal into a WARN. For a cluster
that never had DSE: the new check is always in the default startup list but returns at once outside `DSE_6`; the new
JMX attribute `MessagingServiceMBean.getDseSaiVersionPin` is always registered and returns null; `nodetool
compatstatus` (a command added by this series, P02) prints one more line, `SAI version pinned : not checked`. Nothing
else: the SAI version written is still chosen by the existing `Version.Selector`, unchanged.

**Where to look**
* `src/java/org/apache/cassandra/service/Dse6SaiVersionPin.java` — `verify(List<StoredRelease>, Settings)`: the four
  rules (custom selector refused first; releases of more than one DSE version refused; one DSE version requires the
  pin equal to `aa` for 6.8 / `ca` for 6.9; no known release requires an explicit `aa` or `ca`). Is accepting any
  explicit `aa`/`ca` when no DSE release is stored (the case of every in-JVM test and of a node whose `nodes/local` is
  gone) acceptable, given that the wrong one of the two is exactly the failure the check exists to prevent?
* `Dse6SaiVersionPin.dseVersion` with `DseCompat.dse6MinorAndPatch`: the DSE minor version is read from the first two
  digits of the `4.0.0.<build>` hotfix (`68xx`, `685`, `69x`). Confirm that the patch's statement that DSE 6.8 and 6.9
  builds always encode as `68…` / `69…` holds for every release in support (e.g. a future 6.8.100 would be a 5-digit
  build and fall into "other DSE 6 release", which is refused when mixed with a known one).
* `Dse6SaiVersionPin.verifyAtStartup` — reads `Dse6UpgradeNodesPersistence.localReleaseVersionInNodesDirectory()`,
  `Nodes.local()` and `Nodes.peers()`; any `RuntimeException` (e.g. a corrupt `nodes/local`) is a refusal with
  `ERR_WRONG_DISK_STATE`. Does refusing on an unreadable DSE `nodes/local` (a file DSE wrote, kept in place) block a
  node that the earlier import step already accepted?
* `Dse6SaiVersionPin.Settings.fromSystemProperties` versus `Version.Selector.DEFAULT_VERSION`: the check reads the
  system property at startup-check time, the selector parsed it once when `Version` was initialised. Both read the
  same JVM property, so they agree unless something sets it between the two; is `Version` ever initialised before
  `jvm-server.options` properties apply (offline tools, `CQLSSTableWriter`)?
* `src/java/org/apache/cassandra/service/StartupChecks.java` — the new `checkDse6SaiVersionPin`, placed right after
  `checkSystemKeyspaceState` because that check initialises `Nodes`. The javadoc states that on a first start in the
  mode the refusal comes after the import of DSE's node metadata and the system keyspace scrub, both said to be
  repeatable. Is that ordering acceptable, or should the property half of the check (selector, missing pin) run before
  any state is touched?
* `Dse6SaiVersionPin.storedReleases` — a peer row with a stale DSE release (a node upgraded while this one was down)
  makes the releases disagree and refuses the start; the message tells the operator to start once with the override.
  Is a refused start the right outcome there, rather than ignoring peers whose row is older than the local release?

**Modified or deleted existing code**
* `src/java/org/apache/cassandra/service/StartupChecks.java` — default check list: one entry inserted between
  `checkSystemKeyspaceState` and `checkDse6Ec2NamingScheme` — History: the list and `checkSystemKeyspaceState` come
  from 194bad22f71 (2015, CASSANDRA-8049, pre-startup checks); the neighbouring DSE entries were added by this series
  (P05). Invariant: checks before `checkSystemKeyspaceState` must not read system tables; the new one is after it.
* `src/java/org/apache/cassandra/net/MessagingServiceMBean.java` / `MessagingServiceMBeanImpl` — one new attribute
  `DseSaiVersionPin` — History: the neighbouring DSE attributes were added by this series (P05); adding a method to a
  public MBean interface is source-incompatible for out-of-tree implementors only.
* `src/java/org/apache/cassandra/tools/nodetool/CompatStatus.java` — one new output line, with the same
  "attribute missing on an older server" fallback as the other lines — added by this series (P02/P05).
* `src/java/org/apache/cassandra/config/CassandraRelevantProperties.java` — new
  `INTERNODE_COMPATIBILITY_MODE_IGNORE_SAI_VERSION_PIN`; `SAI_CURRENT_VERSION` (default `ec`) is unchanged.

**Failure and rollback risks**
* Rollback to DSE: the whole point of the pin. The class javadoc states that DSE 6.8.65 with `aa` and DSE 6.9.26 with
  `ca` were observed to keep index files written by this software without rebuilding them; nothing in this repository
  can prove that (see P40, which says so explicitly).
* With the override set, a node writes the unpinned default (`ec`) and a node rolled back to DSE rebuilds those
  indexes, per the patch; the override is logged at WARN on every start and shown by `compatstatus`, so it is visible.
* Partial failure: a refusal on the first start comes after the DSE node-metadata import; rerun safety relies on that
  import being idempotent (stated, not tested in this patch).
* A custom `cassandra.sai.version.selector.class` is refused in `DSE_6` mode; that also blocks any deployment that
  relies on a selector for other reasons (only relevant if combined with the mode).
* Status is a static volatile string set by the last check; in-JVM tests sharing a JVM must reset it
  (`resetStatus()`), which the tests do.

**Test integrity**
* Existing tests changed: `Dse6ModeSmokeTest.formerDseNodeWritesBbSstablesWithNumericGenerations` now sets
  `cassandra.sai.latest.version=aa` for the cluster (required to start) and gains assertions; `CompatStatusTest` gains
  assertions for the new line and its order, and the "old server" mock gains one stub. No assertion weakened or
  removed.
* New tests: `Dse6SaiVersionPinTest` drives `verify` with hand-built `StoredRelease`/`Settings` for the rules, and the
  whole check against the DSE `nodes/local` fixtures (`DseFixtures.installNodesDirectory`) and real `Nodes.peers()`
  rows; `saiVersionOfEachDseReleaseNamesTheIndexFilesItWrote` ties `aa`/`ca` to the file names of the DSE 6.8.65 and
  6.9.26 sstable fixtures (real DSE-written files). The in-JVM smoke test calls `verifyAtStartup` inside an instance
  rather than restarting it (a refused start would exit the test JVM).
* Not covered by any test in the patch: that the pinned property actually makes a flush write `aa`/`ca` files in
  `DSE_6` mode end to end (no SAI index is created in `Dse6ModeSmokeTest`; the write side is tested in P40, outside
  the mode and through a reflective selector); a real refused start (exit code) of a node.

**Suggested reviewer questions**
* Should the check refuse to accept an explicit pin when no DSE release can be determined, or at least require the
  operator to state the DSE release (rather than the SAI version) so that `aa` on a 6.9 cluster cannot slip through?
* Is the per-build-number decoding of the DSE minor version (`DseCompat.dse6MinorAndPatch`) robust for every DSE 6.8/6.9
  build that customers run, including hotfix builds?
* Does any SAI component get written with `Version.LATEST` regardless of the selector (e.g.
  `SAICodecUtils.writeHeader(DataOutput)` falls back to `Version.LATEST` for a non-`IndexOutput`)? If such a path is
  reachable, the pin does not cover it.
* Should a stale DSE release in a peer row (node since upgraded) be tolerated automatically instead of needing a
  one-off override start?

## P40 — SAI on-disk format of DSE 6.8 and 6.9, read and written (tests) (#2817)

**Tier:** 3 — test-only: one new unit test class, one javadoc line in a test fixture helper and a correction of the
fixture README; no production file changed.

**Size and shape:** test: `test/unit/org/apache/cassandra/index/sai/disk/format/DseSaiFormatCompatibilityTest.java`
(~1321 lines, new); `test/unit/org/apache/cassandra/dsecompat/DseFixtures.java` (javadoc, 3 lines);
`test/data/dse-compat/README.md` (~15 lines). No new binary fixtures: it reuses the DSE 6.8.65 and 6.9.26 `t`
sstable sets.

**Gating:** none at runtime. Nothing changes for a cluster that never had DSE.

**Test integrity**
* Existing tests changed: none. The README edit corrects a documented fact: the 6.9.26 fixture set holds different
  values from the 6.8.65 one (`WHERE v = 'val50'` is (24, ck2) there, not (25, ck2)); existing tests that assert
  (25, ck2) use the 6.8.65 set.
* New tests (real files): DSE-written index files are placed in a table directory and opened as at node start;
  the test asserts they are served without an index build (counted by a Byteman-style `Injections.Counter` on
  `CompactionManager.submitIndexBuild`, which only counts), with the pin and without it; a corrupted
  `TokenValues` component triggers a rebuild at DSE's version; with the pin, a flush of the same rows is compared with
  DSE's files component by component (some byte for byte, the others by decoded header, footer, metadata and
  postings). Two known divergences are asserted as such: an `aa` flush posts one row per partition (DSE one per row),
  and DSE's `ca` files keep a posting for a cell deleted after indexing.
* Boundaries replaced: the node does not run in `DSE_6` mode; the pin is applied with `SAIUtil.setCurrentVersion`,
  which swaps `Version.SELECTOR` by reflection (the very mechanism P09 refuses in production), not through the
  `cassandra.sai.latest.version` property; sstables flushed here keep the test configuration's identifiers. The class
  javadoc states that whether DSE reads the files this tree writes needs a DSE binary and is not tested.
* Not covered: DSE reading files written by this tree (the rollback case P09 exists for); the property-to-selector
  path in `DSE_6` mode.

**Suggested reviewer questions**
* Is the `aa` divergence (one posting per partition, at its last row, where DSE posts every row) safe for DSE 6.8
  after a rollback, i.e. does DSE 6.8 read an `aa` posting as a partition and filter its rows, as this tree does?
* The tests assert that a rebuild of the index of a DSE sstable, even unpinned, writes DSE's version again: which
  production code makes a rebuild keep the existing version, and is that behaviour intended or incidental?
* Should one test go through the real system property (and `DSE_6` mode) instead of the reflective selector swap, so
  that the production wiring of P09 is covered once?

## Group notes
* P40 tests what P09 relies on (that `aa`/`ca` written by this tree match DSE's files), but through a different
  mechanism (reflective selector) than P09's production path (system property, default selector); neither patch
  covers the end-to-end path "node in `DSE_6` mode with the property set flushes an SAI index".
* P40 depends on `Dse6SaiVersionPin.saiVersionOf` (P09) for its expected versions, so a wrong mapping in P09 would be
  mirrored, not caught, by P40; the independent anchor is the fixture file names (`SAI_…` for 6.8.65, `SAI+ca+…` for
  6.9.26), checked in P09's `saiVersionOfEachDseReleaseNamesTheIndexFilesItWrote`.
* P09 is on the feature branch; P40 sits on the series preview branch, far above it; the in-JVM harness of P13 pins
  `aa` JVM-wide for every mixed-cluster test because of P09.
