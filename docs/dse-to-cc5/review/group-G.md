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

# Group G — mixed-cluster test harness

Part of the [reviewer triage guide](README.md), which explains the tiers, the review order and the layout of a brief.

## P13 — in-JVM mixed-cluster harness on HCD 1.2 nodes (#2751)

**Tier:** 3 — test code and build/CI scripts only; no file under `src/` or `conf/` is changed. The CI script change
removes tests from the regular `jvm-dtest` split, which matters for coverage, not for runtime.

**Size and shape:** test: ~2900 lines in 12 new in-JVM test classes under
`test/distributed/org/apache/cassandra/distributed/test/dsecompat/` (base class `DseDialectTestBase`, layout helper
`TwoDataCenters`, and tests `Dse6DialectSmokeTest`, `Dse6GossipTest`, `Dse6HintsConversionTest`,
`Dse6RollbackArtefactsTest`, `MixedModeBatchTest`, `MixedModeForwardingTest`, `MixedModeHintsTest`,
`MixedModeReadWriteTest`, `MixedModeSpeculativeReadRepairTest`, `MixedModeTruncateTest`); comment-only edits to two
existing unit tests. Build/CI: `build.xml` (~60 lines, new targets `_check-hcd12-dtest-jar`, `test-jvm-dtest-dse`),
`.build/run-tests.sh` (~30), `.build/docker/run-tests.sh` (1), new `test/bin/build-hcd12-dtest-jar.sh` (~146),
`.gitignore` and `ide/idea-iml-file.xml` (1 line each). The individual test classes can be skimmed once
`DseDialectTestBase` is understood.

**Gating:** none at runtime. For a cluster that never had DSE: nothing. For CI: classes that `extend
DseDialectTestBase` are removed from the `jvm-dtest` split and run only by the new target type `jvm-dtest-dse`.

**Modified or deleted existing code**
* `.build/run-tests.sh` — `jvm-dtest` target: the test list now filters out `_list_dse_dialect_tests` (grep for
  `^public class <name> extends DseDialectTestBase`), and an explicit `-t` that matches such a class is refused; new
  target type `jvm-dtest-dse` (no repeats, no splits) — History: the target-type list and `_list_tests` belong to the
  upstream CI scripts of the 5.0 line; invariant: every distributed test runs in some CI target. `jvm-dtest-dse` is not
  referenced by `.jenkins/Jenkinsfile` in this patch; `ds/Jenkinsfile` delegates to an external pipeline library
  (`dsCassandraPRGate`), so whether the pull request gate runs the excluded tests cannot be established from this
  repository.
* `.build/docker/run-tests.sh` — `jvm-dtest-dse` added to the 6 GB memory case.
* `test/unit/org/apache/cassandra/dsecompat/DseCommitLogReplayTest.java`,
  `test/unit/org/apache/cassandra/service/reads/repair/BlockingReadRepairTest.java` — comments only, pointing at the
  new in-JVM tests.

**Failure and rollback risks**
* The exclusion of tests from `jvm-dtest` relies on a textual grep of the class declaration; a class that extends
  `DseDialectTestBase` indirectly, or with a different declaration form, would run in `jvm-dtest` and skip itself
  (`Assume` on the missing HCD 1.2 jar) — silently green.
* Without the HCD 1.2 dtest jar the tests are skipped by `assumeHcd12DtestJar`, not failed, under any target other
  than `test-jvm-dtest-dse` (which fails fast in `_check-hcd12-dtest-jar`).
* `test/bin/build-hcd12-dtest-jar.sh` builds a pinned `hcd-1.2.3` commit with a JDK 11 from a remote (default
  `origin`); a CI machine without access to that branch cannot build it.
* `DseDialectTestBase.initializeInstance` documents a race: HCD 1.2 nodes and upgraded nodes must not start in
  parallel, or an upgraded node may silently come up at messaging version 11 (the JVM-wide
  `ds.current_messaging_version` property).

**Test integrity**
* Existing tests changed: none with assertions (two comment edits).
* What the "old" nodes really are: HCD 1.2 (pinned `hcd-1.2.3` commit) started with `-Dds.current_messaging_version=11`
  and trie version `bb`, standing in for DSE 6. The base class javadoc states what this does not reproduce: DSE's
  gossip (HCD 1.2 sends its own 4.0 application-state ordinals, which differ from DSE's from ordinal 15 on), DSE's
  handshake flags, gossip JSON and schema-compatibility rules; GROUP BY limits and the `FETCH_ALL_STATICS` bit are
  declared out of scope because HCD 1.2 differs from DSE there.
* What is simulated:
  - No network and no gossip in any mixed-cluster test: messages are delivered in-JVM and serialized at the version
    the sender records for the peer (`Instance.serializeMessage`), so the pre-4.0 handshake, framing and connection
    pools are not exercised. The one test that would run with `GOSSIP`/`NETWORK`
    (`Dse6DialectSmokeTest.hcd12PeerAtTheDialectJoinsDse6Nodes`) is `@Ignore`d: HCD 1.2 forced to 11 refuses an
    inbound connection requesting 11.
  - The HCD 1.2 peers are never identified as DSE peers (that test asserts `DSE peers : 0`); code paths gated on a
    peer being DSE (`DsePeers.isDse`) are therefore not reached by these tests.
  - `Batch` assertions are switched off in the HCD 1.2 class loaders (`setClassAssertionStatus`), because the HCD 1.2
    in-JVM message filter re-serializes received remote batches; the javadoc states those are the only two
    assertions of that class in `hcd-1.2.3`.
  - The dtest config check is off (`KEY_DTEST_API_CONFIG_CHECK=false`) for both versions, so a misspelt key in
    `configureDse6` is not rejected.
  - The SAI pin `aa` is set JVM-wide, so HCD 1.2 nodes also write `aa` (instead of `dc`), and the upgraded nodes
    pass P09's check through its "no DSE release stored" branch only.
  - The hints dispatch is started by reflection (`startHintsDispatch`), not by the gossip startup path; the
    access-control pass and the production call site of the hints conversion are not exercised from startup.
  - `MixedModeForwardingTest`'s second test builds a forwarded mutation with DSE's per-target message ids by hand and
    calls `MutationVerbHandler.instance.doVerb` directly, since HCD 1.2 reuses the forwarder's id.
  - `MixedModeHintsTest.hintsFromHcdToUpgradedNode` is `@Ignore`d: per its reason text, a node in `DSE_6` mode
    computes a different schema version than HCD 1.2 for the same schema, so HCD 1.2 holds the hints.
* Real parts: the HCD 1.2 nodes run real 4.0-line code, so message bodies at version 11 are produced and consumed by
  an independent implementation; sstables written by HCD 1.2 at `bb` are read by the upgraded node;
  `Dse6RollbackArtefactsTest` and `Dse6HintsConversionTest` use DSE-written fixtures (commit log segment, hints file).
  `Dse6GossipTest` runs only nodes of this software (plain `TestBaseImpl`), with real gossip and network between them.
* Not covered by any test in the patch: handshake and framing against an old peer; gossip with any node speaking the
  dialect; DSE-peer-specific behaviour; hints from an old node to an upgraded one.

**Suggested reviewer questions**
* Is `jvm-dtest-dse` going to be added to a CI pipeline (and the HCD 1.2 jar built there)? Until then, the mixed-mode
  tests of the whole series run nowhere in CI.
* Given that the upgraded node never classifies HCD 1.2 as a DSE peer, which DSE-only behaviours of the series
  (refused verbs to DSE peers, gossip translation, schema-version handling) are covered only by unit tests against
  captured bytes, and is that list written down?
* The ignored hints test reports that a `DSE_6` node computes a different schema version from HCD 1.2 for the same
  schema: is the same expected against real DSE, and what does it do to hint dispatch and schema pulls from DSE nodes?
* Should the exclusion in `.build/run-tests.sh` use a marker (annotation or naming rule) rather than a grep of the
  declaration line, so that an indirect subclass cannot fall into `jvm-dtest` and skip silently?

## P21 — mixed-cluster tests with two data centers (#2763)

**Tier:** 3 — seven new in-JVM test classes; no production, build or existing test file changed.

**Size and shape:** test: ~1528 lines in `MixedModeMultiDcAccessControlTest`, `MixedModeMultiDcBatchlogCounterTest`,
`MixedModeMultiDcEachQuorumTest`, `MixedModeMultiDcPaxosTest`, `MixedModeMultiDcPlacementTest`,
`MixedModeMultiDcReadRepairTest`, `MixedModeMultiDcViewTest` under
`test/distributed/org/apache/cassandra/distributed/test/dsecompat/`. All extend `DseDialectTestBase` and use the
six-node `TwoDataCenters` layout of P13 (nodes 1, 4, 5 upgraded).

**Gating:** none at runtime; nothing changes for a cluster that never had DSE. They run only under `jvm-dtest-dse`.

**Test integrity**
* Existing tests changed: none.
* New tests: same harness and same limits as P13 (HCD 1.2 at version 11 as the old nodes, in-JVM delivery, no
  gossip). Message filters drop chosen verbs (`MUTATION_RSP`, `READ_REPAIR_REQ`, all verbs to the other data center) to
  force the paths under test; these are real messages dropped, not forged ones. `MixedModeMultiDcPaxosTest` uses the
  native protocol (`NATIVE_PROTOCOL`, `TwoDataCenters.withSession`) because the HCD 1.2 in-JVM coordinator cannot set a
  serial consistency.
* Simulated or out of scope, as the classes state: `MixedModeMultiDcAccessControlTest` calls
  `Dse6AccessControlCheck.verifyAfterJoin` by reflection, not through startup (no gossip);
  `MixedModeMultiDcPaxosTest` checks that the HCD 1.2 coordinator writes a hint and a read repairs the row, not what
  DSE's hint dispatch does; `MixedModeMultiDcViewTest` states DSE 6's own view-update code (deletions of old view
  entries sent to every replica of every DC) is not exercised; `MixedModeMultiDcPlacementTest` compares placements with
  those HCD 1.2 computed before the upgrade, not with DSE's (its rack layout is stated to be the one observed on a DSE
  6.9.26 cluster).
* Not covered: the same gaps as P13 (handshake, gossip, DSE-peer classification), now across data centers.

**Suggested reviewer questions**
* For placement, HCD 1.2's `NetworkTopologyStrategy` is the oracle: is there evidence (beyond the observed rack layout)
  that DSE 6 places replicas identically, e.g. for racks with more replicas than racks, which the patch says this
  layout cannot exercise?
* For LWT at `LOCAL_SERIAL`, the patch states that DSE 6 commits to every replica of every data center: is that
  confirmed for both 6.8 and 6.9, since the test can only show HCD 1.2's behaviour?

## Group notes
* P13 defines what every mixed-cluster test of the series exercises: real HCD 1.2 code at messaging version 11, in-JVM
  message delivery, no gossip, no handshake, and peers never seen as DSE. Any claim elsewhere in the series that a
  behaviour is "tested against DSE peers in a mixed cluster" should be read with these limits; DSE gossip and
  handshake rely on unit tests against captured bytes.
* No production file is changed in P13 or P21 to make the harness work; the workarounds live in test code
  (`initializeInstance` system-property pinning, class-assertion switch for HCD 1.2 `Batch`, isolated native-library
  classes, config check off). A later series patch (P20) extends `DseDialectTestBase`
  to create an empty DSE `system_schema.hidden_columns` directory before each upgrade, because without it an upgraded
  node with views refuses to start; that is another simulated piece of DSE state.
* P21 depends on `TwoDataCenters` and `DseDialectTestBase` from P13 and on P09's SAI pin (the harness pins `aa`
  JVM-wide, so the DSE 6.9 `ca` combination is not run by these mixed-cluster tests).
* The CI wiring of `jvm-dtest-dse` (Jenkinsfile) is not part of either patch.
