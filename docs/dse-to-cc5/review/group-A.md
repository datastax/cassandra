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

# Group A — startup, schema exchange and operational guards

Part of the [reviewer triage guide](README.md), which explains the tiers, the review order and the layout of a brief.

## P14 — schema exchange and system keyspace definitions (#2752)

**Tier:** 1 — changes the schema exchange verbs (pull answers and push handling) and how the replicated system
keyspaces' schema is set up at startup, the persistent schema state that a rollback to DSE must find unchanged.

**Size and shape:** 8 production files (~370 added, ~17 removed), 9 test files (~1050 added), no fixtures. The new
`DseSystemKeyspaces` is mostly javadoc plus one diff helper. `CassandraAuthorizer` and
`CassandraAuthorizerDse6Test` change only in javadoc and can be skimmed. The weight is in `DefaultSchemaUpdateHandler`,
`MigrationCoordinator` and `StorageService.setUpSystemKeyspace`.

**Gating:** every change is behind `DseCompat.enabled()` / `DatabaseDescriptor.isDse6CompatibilityMode()`
(`internode_compatibility_mode: DSE_6`). For a cluster that never had DSE, nothing changes. Pulls are still answered
with the schema and pushes applied. `setUpDistributedSystemKeyspaces` still calls `updateSystemKeyspace` for all three
keyspaces, through the new `setUpSystemKeyspace` wrapper. The auth check, denylist and trace rows are unchanged. The
only always-on edits are that indirection and an extra `DseCompat.enabled()` term in the hot-path checks of
`PartitionDenylist.isKeyPermitted` and `getDeniedKeysInRangeCount`.

**Where to look**
* `src/java/org/apache/cassandra/schema/DefaultSchemaUpdateHandler.java` — `schemaMutationsForPull` and the
  `SchemaPullVerbHandler` lambda. In the mode, a peer that `exchangesSchemaWith` rejects gets `Collections.emptyList()`
  if it is DSE (`DseDialectVersions.isPre40` or `DsePeers.isDse`). Any other such peer gets
  `msg.failureResponse(RequestFailureReason.UNKNOWN)`. Questions: is the failure branch reachable for a node of this
  software whose version is not yet recorded, and does the requester's retry from another node really converge? The
  javadoc says an empty answer is taken as the schema and not pulled again.
* `src/java/org/apache/cassandra/schema/MigrationCoordinator.java` — `exchangesSchemaWith` delegates to
  `shouldPushSchemaTo` (`versions.getRaw(endpoint) == MessagingService.current_version`). Two upgraded nodes in the
  mode exchange schema only once a handshake has recorded `current_version` for each other. `StorageService` (gossip
  patch) keeps a negotiated version above the gossiped `NET_VERSION` 12. Confirm that no window leaves two upgraded
  nodes permanently refusing each other's pulls and pushes.
* `src/java/org/apache/cassandra/schema/DefaultSchemaUpdateHandler.java` — the `SchemaPushVerbHandler` lambda drops
  pushes, before taking the handler lock, from any peer `exchangesSchemaWith` rejects, including a node of this
  software whose version is unknown at that moment. The javadoc says the later pull catches up. Check that the gossiped
  schema version always triggers that pull.
* `src/java/org/apache/cassandra/service/StorageService.java` — `setUpSystemKeyspace`. In the mode, a keyspace that
  exists keeps its loaded definition (returns before `Schema.instance.transform`). A missing keyspace is still created
  with this software's definition. Question: can a node in the mode ever lack these keyspaces, and if so, can its
  definition reach DSE through an upgraded peer that pulled it? P15 refuses bootstrap in the mode, which closes this.
* `src/java/org/apache/cassandra/auth/AuthConfig.java` — `checkDse6CompatibilityMode` / `refuseWhatNeedsTablesDseLacks`
  refuses `CassandraNetworkAuthorizer`, `CassandraCIDRAuthorizer` and both mutual TLS authenticators at startup. Is
  the list complete for every `system_auth` table in `DseSystemKeyspaces.deferredAdditions`, for example a custom role
  manager or other `IAuthorizer` implementations that read new tables?
* `src/java/org/apache/cassandra/schema/PartitionDenylist.java` — `refuseInDse6` throws `IllegalStateException` from
  `load`, `addKeyToDenylist` and `removeKeyFromDenylist`. `initialLoad` returns before `loadAttempts++`. Check what the
  JMX callers (`StorageProxy`) surface to nodetool.
* `src/java/org/apache/cassandra/tracing/TraceKeyspace.java` — `writesPortColumns` adds `!DseCompat.enabled()` to the
  existing mixed-version rule.

**Modified or deleted existing code**
* `src/java/org/apache/cassandra/schema/DefaultSchemaUpdateHandler.java` — constructor: the pull handler may now send a
  failure or an empty answer, and the push handler may return early. History: 2b2c6decfaf, 2021, CASSANDRA-17044 (schema
  management made pluggable). The original invariant is that every pull is answered with the full schema and every push
  is applied. In the mode it no longer holds for peers that are not at `current_version`.
* `src/java/org/apache/cassandra/service/StorageService.java` — `setUpDistributedSystemKeyspaces` and the auth step of
  `setUpAuth` (`setUpSchema`) now go through `setUpSystemKeyspace`. History: 24915cd48e9, 2015, CASSANDRA-8853 (system
  keyspaces created by fixed-timestamp mutations so that every node converges on the same schema). In the mode,
  convergence relies on every upgraded node having loaded the same DSE definition.
* `src/java/org/apache/cassandra/tracing/TraceKeyspace.java` — `makeStartSessionMutation`, `makeEventMutation`.
  History: 9b1f3796a65, 2019, CASSANDRA-15385 (port columns left out while 3.x nodes may be present, so trace mutations
  do not break mixed-mode connections). Renamed in 475c0035e6e, 2024, CASSANDRA-18999.
* `src/java/org/apache/cassandra/schema/PartitionDenylist.java` — `initialLoad`, `isKeyPermitted`,
  `getDeniedKeysInRangeCount`. History: ab920c30310, 2021, CASSANDRA-12106 (the denylist). In the mode, denylisted keys
  are permitted.
* `src/java/org/apache/cassandra/auth/AuthConfig.java` — `applyAuth` gains one call. History: 933137fb14d, 2023,
  CASSANDRA-18592 (CIDR authorizer wiring next to the insertion point).
* `src/java/org/apache/cassandra/auth/CassandraAuthorizer.java` — javadoc of `maintainsResourceIndex` only. The method
  was added by an earlier patch of this series (P06).

**Failure and rollback risks**
* Rollback: the patch's purpose is to keep DSE's `system_auth` / `system_distributed` / `system_traces` definitions
  intact while in the mode. The patch states that, with DSE 6.8.65, DSE merged pulled tables into its schema and kept
  them after rollback. Once a node has started with `NONE`, the deferred tables are created and the javadoc offers no
  rollback path.
* Table options: the `DseSystemKeyspaces` javadoc says this software's table options take effect at the first `NONE`
  start only where their fixed timestamp is newer than DSE's. Not established whether any option stays at DSE's value
  for good.
* Resource index: the `resource_role_permissons_index` created at the first `NONE` start holds none of the earlier
  grants until it is rebuilt by hand (javadoc of `maintainsResourceIndex`). DROPs before that rebuild leave stale
  grants.
* Log volume: in a mixed cluster DSE keeps pulling a schema whose version differs from its own. Every such pull is
  answered with nothing and counted. Logging is limited to once per ten minutes, but the DSE-side cost of an endless
  pull loop is not established from this repository.
* Before P15 lands (see Group notes), a node in the mode can still bootstrap with fresh system keyspaces, and DDL is not
  frozen.

**Test integrity**
* Existing tests changed: `CassandraAuthorizerDse6Test` (added by this series), class javadoc only. No assertion
  changed in `MigrationCoordinatorTest` or `PartitionDenylistTest`; new tests only.
* New tests: `Dse6SystemKeyspacesTest` (in-JVM). Real nodes, but the "DSE" side is an HCD 1.2 node that sends a pull and
  a push at the DSE dialect by hand, because the harness runs without gossip. It also covers the first restart with
  `NONE` and the deferred table appearing. `Dse6SchemaExchangeTest` is a CQLTester-level test of the handler's branch
  selection. `MigrationCoordinatorTest.schemaExchangedOnlyWithPeersAtThisVersion` and
  `noSchemaPushToDsePeersInEitherMode` use the existing mocked wrapper. `DseSystemKeyspacesTest`,
  `StorageServiceDse6SystemKeyspacesTest`, `AuthConfigDse6Test`, `PartitionDenylistTest.testDenylistOffInDse6Mode` and
  `TraceKeyspaceDse6Test` are unit-level.
* Not covered by any test in the patch:
  * Schema propagation between two upgraded nodes in the mode (the in-JVM test upgrades one of two nodes).
  * A real DSE node's reaction to empty pull answers.
  * The failure answer reaching a real requester and its retry.

**Suggested reviewer questions**
* Between two upgraded nodes in the mode, is `versions.getRaw == current_version` guaranteed after the handshake, even
  when the gossiped `NET_VERSION` 12 arrives first? If not, schema pushes are silently dropped between them.
* Does the requester's `MigrationCoordinator` treat `failureResponse(UNKNOWN)` as "retry later or elsewhere" in this
  fork, as the javadoc claims, and with bounded retries?
* Is every reader of a deferred table or column handled? The list in the `DseSystemKeyspaces` javadoc covers auth, the
  denylist, the trace ports and repair history. Other features writing to `system_distributed` (for example view build
  status) are not listed.
* Should `setUpSystemKeyspace` refuse instead of create when a replicated keyspace is missing in the mode, rather than
  rely on P15's bootstrap refusal?

## P15 — operational guards and the schema freeze (#2755)

**Tier:** 1 — adds checks to existing lifecycle and topology entry points that run on every cluster: startup join,
shadow rounds, removenode, assassinate, repair, sstable rewrites and import. It also adds a schema-freeze test to
every DDL in `AlterSchemaStatement.execute` (the refusing wrapper applies only in `DSE_6` without `allow_ddl`) and a
refusal inside the repair `PrepareMessage` serializer.

**Size and shape:** 22 production files (~940 added, ~60 removed), of which the new `DseUpgradeGuards` is ~620. 21 test
files (~2460 added), including `Dse6UpgradeGuardsTest` (~1130) and three in-JVM classes. No fixtures. Javadoc-only
production edits can be skimmed: `CassandraAuthorizer`, `DatabaseDescriptor`, `InternodeCompatibilityMode`,
`DseCompatRefusals`, `DsePeers` (except `presumedSnapshot`), `DseSystemKeyspaces`, `MigrationCoordinator`, the
`DefaultSchemaUpdateHandler` log texts, `DseViewHiddenColumnCatalog` and `CassandraRelevantProperties` (except the new
key).

**Gating:** the upgrade counts as in progress while `DseUpgradeGuards.upgradeInProgress()` is true: in `DSE_6` mode, or
while `pre40Peers()` is not empty. `pre40Peers()` lists any entry of `MessagingService.versions` whose value maps below
`VERSION_40` through `DseCompat.messagingVersionFromGossip` (11 or less, or 256 and up). The schema freeze, the status
line and the `PrepareMessage` refusal apply only in `DSE_6`. Always-on changes for a cluster that never had DSE:
* `refuseJoinWithDsePeersInGossip` runs in every mode, in `prepareForReplacement` and in `checkForEndpointCollision`
  (when bootstrap is not complete). It refuses on any shadow-round `NET_VERSION` that maps below 4.0.
* Each guarded operation calls `upgradeInProgress()`. Without the mode that reads a snapshot of the version map, only
  when `MessagingService.isInstantiated()`. The guarded operations are decommission, move, rebuild, repair,
  removenode (also forced), assassinate, `resumeBootstrap`, `joinRing` without a completed bootstrap, upgradesstables,
  recompress_sstables, both `loadNewSSTables`, `SSTableImporter.importNewSSTables`, resetlocalschema and
  `StorageProxy.setPaxosVariant`.
* `prepareToJoin` checks `upgradeInProgress()` before `MessagingService.listen()`.
* New startup check `checkLostNodeSilence`, a no-op unless the new property is set.
* `StorageService.DEFAULT_RING_DELAY_MILLIS` constant.
* `MessagingService.instantiated` flag.
* `DsePeersRecord.persistAtDrain()` in `drain`, a no-op without the mode.
* `AlterSchemaStatement.execute` evaluates `schemaChangesFrozen()` on every DDL.

**Where to look**
* `src/java/org/apache/cassandra/service/DseUpgradeGuards.java` — `upgradeInProgress` / `pre40Peers`. This is the gate
  of every always-on guard. Question: can a peer ever be recorded in `versions` at a value 11 or less, or 256 or more,
  without the mode on a cluster that never ran DSE? `StorageService` records the gossiped `NET_VERSION` as is without
  the mode, and the codebase still has a `VERSION_30` "DSE <-> CC migration" path (`DataLimits`,
  `DseCompat.isPre40Layout` javadoc). If so, topology operations on such a cluster are refused.
* `src/java/org/apache/cassandra/service/StorageService.java` — `prepareToJoin`, `prepareForReplacement`,
  `checkForEndpointCollision`, `joinRing`. These are the startup refusals. A former DSE node must have bootstrap state
  `COMPLETED` (imported by P04, #2739). Check that no legitimate restart of an upgraded node reaches
  `refuseAtStartupWhileUpgrading`, for example after a failed import or with `join_ring: false`.
* `src/java/org/apache/cassandra/cql3/statements/schema/AlterSchemaStatement.java` — `FrozenSchemaStatement.apply`
  applies the statement and then refuses if `Keyspaces.diff` is not empty. Questions:
  * The wrapper is not a `CQLStatement`, so `SchemaAnnouncementEvent` loses `statement` for a no-op DDL. Does that
    matter?
  * Paths that change the schema without `AlterSchemaStatement` are not frozen (for example `CQLSSTableWriter`,
    `SchemaTransformations` users). Is that intended?
* `src/java/org/apache/cassandra/service/DseUpgradeGuards.java` — `lostNodeKind`, `isLostNode`, `liveDsePeers`,
  `refuseRemoveNodeWhileUpgrading`, `refuseAssassinateWhileUpgrading`, `refuseForcedRemovalWhileUpgrading`. "Lost"
  means not alive and `nanoTime() - state.getUpdateTimestamp()` at or above `lostNodeSilenceNanos()`. Question: does
  `getUpdateTimestamp` advance only on real news, or also on local gossip bookkeeping (for example a restart of the
  observing node recreating the state), which would make the silence restart unexpectedly?
* `src/java/org/apache/cassandra/service/StorageService.java` — `removeNode` calls the guard before the existing
  `endpoint == null` check. `forceRemoveCompletion` checks `getTokenMetadata().getLeavingEndpoints()`. Confirm that the
  leaving set is exactly the nodes being removed by force.
* `src/java/org/apache/cassandra/service/DsePeersRecord.java` — `persistAtDrain`, `CHANGED`. It waits up to 10 s for
  `lastPersist`, then calls `persist()` synchronously on the drain thread, restoring the interrupt afterwards. It is
  called in `drain` right after the messaging shutdown, before the flushes. Check the ordering against
  `ScheduledExecutors.nonPeriodicTasks` shutdown, and that `persist()` (which swallows `IOException`/`FSError`) cannot
  block drain.
* `src/java/org/apache/cassandra/repair/messages/PrepareMessage.java` — `refuseDsePeer` runs in `serialize`,
  `deserialize` and `serializedSize`, gated by `DseCompat.enabled()` and `version < VERSION_40`. It throws
  `UnsupportedOperationException` from a serializer. What does the messaging layer do with that on the inbound side:
  close the connection, or drop the message?
* `src/java/org/apache/cassandra/service/StorageProxy.java` — `setPaxosVariant`. In `DSE_6` it refuses anything but
  `isDse6PaxosVariant`. Without the mode it refuses v2 variants while DSE peers are seen. The
  `Paxos.setPaxosVariant` check is kept.

**Modified or deleted existing code**
* `src/java/org/apache/cassandra/service/StorageService.java` — `checkForEndpointCollision`. History: 28ccf3fe398,
  2017, CASSANDRA-13851 (shadow round with the known peers, so startup does not fail when peers are not live). The new
  call runs on the states of that round and is skipped with `allow_unsafe_join` like the rest.
* `src/java/org/apache/cassandra/service/StorageService.java` — `prepareForReplacement`, after `doShadowRound()`.
  History: 351d43ef120, 2013, CASSANDRA-5916 (node replacement reworked around the shadow round).
* `src/java/org/apache/cassandra/service/StorageService.java` — `resumeBootstrap`. History: e5b5a91fb61, 2015,
  CASSANDRA-8942. The guard runs before the `isBootstrapMode` check, so it refuses even when there is nothing to resume.
* `src/java/org/apache/cassandra/service/StorageService.java` — `upgradeSSTables` / `recompressSSTables` now throw
  `IOException` when refused. The javadoc of `refuseSSTableRewriteWhileUpgrading` says nodetool swallows a
  `RuntimeException` (CASSANDRA-18635). Not established from the code that nodetool shows the `IOException`.
* `src/java/org/apache/cassandra/service/StorageService.java` — `drain`, after the messaging shutdown. History:
  7ca997ba351, 2020, CASSANDRA-16276 (drain tolerates a slow messaging shutdown). The new synchronous write sits between
  that block and the flushes.
* `src/java/org/apache/cassandra/service/StorageService.java` — `getRingDelay`: `30 * 1000` becomes
  `DEFAULT_RING_DELAY_MILLIS`. History: b8edee15fc0, 2022, STAR-790. No behaviour change.
* `src/java/org/apache/cassandra/cql3/statements/schema/AlterSchemaStatement.java` — `execute`. History: 2b2c6decfaf,
  2021, CASSANDRA-17044. Invariant: the transformation passed to `Schema.instance.transform` is the statement itself.
* `src/java/org/apache/cassandra/repair/messages/PrepareMessage.java` — `serializer`. History: f9ff8843774, 2019,
  CASSANDRA-15163, and efe830e1f7e, 2021, CASSANDRA-16542 (the `MIXED_MODE_ERROR` check that stops a 4.0 node breaking
  a 3.x connection). The new check runs before it.
* `src/java/org/apache/cassandra/service/StorageProxy.java` — `setPaxosVariant`. History: a965699c9e0, 2021, "[CEP-10]
  Phase 3: Use paxos variant to specify behaviour" (ticket id not in the commit body).
* `src/java/org/apache/cassandra/db/ColumnFamilyStore.java` — `loadNewSSTables`. History: 3b6c93828c2, 2018,
  CASSANDRA-14417.
* `src/java/org/apache/cassandra/db/SSTableImporter.java` — `importNewSSTables`. History: 0f79427758c, 2018,
  CASSANDRA-14442. This is the single entry for every import path (JMX `importNewSSTables` and refresh).
* `src/java/org/apache/cassandra/gms/Gossiper.java` — `assassinateEndpoint`. History: bf0f80cbb67, 2011,
  CASSANDRA-3337. The guard runs outside the gossip stage, before `runInGossipStageBlocking`.
* `src/java/org/apache/cassandra/service/StorageService.java` — `decommission`, `move`, `rebuild`, `repair`,
  `forceRemoveCompletion`, `removeNode`, `loadNewSSTables`, `resetLocalSchema`, `joinRing`: one guard line each, at
  entry.
* `src/java/org/apache/cassandra/net/DseDialectVersions.java` — `gossipedVersion40OrLater` goes from package-private
  `@VisibleForTesting` to public. `DsePeersRecord`, `DsePeers`, `DseDialectVersions`, `DefaultSchemaUpdateHandler`,
  `DseSystemKeyspaces` and `MigrationCoordinator` hunks touch code added by this series (P05 #2740, P12,
  P14).

**Failure and rollback risks**
* Always-on false positive: on a NONE cluster, any peer recorded with a pre-4.0 or 256+ version turns on every guard
  and refuses bootstrap in the shadow round. It stays that way until the peer leaves gossip. The only way out without
  removing the peer is `allow_unsafe_join`, which skips only the shadow-round check.
* Lost-node removal is a heuristic. The default silence is the largest of 30 min, `max_hint_window` and 2x ring delay.
  The WARN says the operator must confirm that the node is gone. With a short configured silence (floor: 60 s or 2x
  ring delay), a slowly restarting DSE node could be assassinated. After a rollback to DSE, the node is back in DSE's
  ring (caveat text).
* `removenode` of a lost node while a DSE node "comes back" during streaming: the javadoc accepts ranges without data
  until a full repair after the upgrade. Data stays under-replicated meanwhile.
* Drain: `persistAtDrain` may add up to 10 s plus a synced write to every drain in `DSE_6` when the record changed.
  Interrupt handling is deliberate.
* Status log: `startStatusLog` schedules a WARN line every 10 min on `ScheduledExecutors.scheduledTasks`, and it is
  never cancelled. That is fine for a server, but `STATUS_LOG_STARTED` is static.
* Partial start: refusals of the node's own start throw `ConfigurationException` before `listen()`, so nothing has been
  written to the network. A rerun is idempotent.
* Schema freeze: a refused statement is applied in full before the refusal and may emit guardrail warnings (javadoc).
  An update handler that applies the transformation more than once counts each refusal more than once.

**Test integrity**
* Existing tests changed (all added by earlier patches of this series):
  * Comment-only edits: `Dse6GossipTest`, `Dse6ModeSmokeTest`, `Dse6RollbackArtefactsTest`,
    `CassandraAuthorizerDse6Test`, `Dse6IndexClassesTest`, `MetadataCollectorDse6Test`, `V11SerializationAuditTest`,
    `DseCommitLogReplayTest`.
  * `Dse6SchemaExchangeTest`: expected log texts updated to the new wording.
  * `DseViewHiddenColumnCatalogTest`, `DseViewHiddenColumnsTest`, `DseViewUpdatesFixtureTest`: switch the mode to `NONE`
    around `importNewSSTables` (now refused in `DSE_6`) and restore it. The assertions are unchanged, but the import
    under test no longer runs in the mode.
  * `Dse6StartupChecksTest` and `DsePeersRecordTest`: additions only.
  * No upstream test file is modified.
* New tests:
  * `Dse6UpgradeGuardsTest`: unit, no mocks; sets versions and gossip states directly.
  * `Dse6SchemaFreezeTest`: CQLTester, client and internal paths.
  * `DsePeersRecordDrainTest`: per the commit message, it fails without the drain call.
  * `DseUpgradeGuardsWithoutMessagingTest`.
  * In-JVM `Dse6GuardsTest`: real nodes of this software restarted in `DSE_6`.
  * `Dse6LostDseNodeTest` and `Dse6LostNodeOfThisSoftwareTest`: in-JVM, but the lost node is a gossip state injected
    on node 1, and the silence is lowered to one second after startup, below the floor the startup check enforces.
    Failure detection and real heartbeat relay are not exercised.
* Not covered by any test in the patch:
  * A real DSE (or HCD 1.2 with gossip) peer driving `pre40Peers` on a `NONE` node.
  * The `PrepareMessage` serializer refusal on the inbound side of a real connection.
  * nodetool's handling of the `IOException` from upgradesstables.
  * `startStatusLog` scheduling in a running node.
  * Freeze bypass through non-`AlterSchemaStatement` schema paths.

**Suggested reviewer questions**
* Is there any deployment of this fork (for example the DSE-to-CNDB migration that still uses `VERSION_30`) where a
  `NONE` node records a peer below 4.0? If so, the "inert outside DSE" claim of `upgradeInProgress` fails, and a
  `NONE`-mode opt-out is needed.
* Does `EndpointState.getUpdateTimestamp` reliably measure "nothing heard" for a node this node cannot reach but others
  can? What happens to the silence after the observing node restarts?
* Should `FrozenSchemaStatement` be rejected up front for statements whose `apply` has side effects (guardrail
  warnings, UDF compilation), instead of applying in full and then refusing?
* Throwing `UnsupportedOperationException` from `PrepareMessage.serializer.deserialize`: what does inbound messaging do
  with it, and can a DSE peer trigger it repeatedly?
* `resumeBootstrap` and `joinRing` refuse while upgrading, but the startup path already refuses an incomplete
  bootstrap. Which concrete sequence (`join_ring: false`, then `nodetool join`, in `NONE` with DSE peers) are these
  meant to close, and is it tested?

## Group notes
* P15 completes P14: P14 closes the schema exchange and keeps DSE's system keyspace definitions, but only P15 stops the
  two cases P14 leaves open. A fresh node in the mode would get this software's definitions (P15 refuses bootstrap and
  replace), and local DDL would still go through (P15 freezes it). P15 also rewrites P14's javadocs and log texts
  ("the schema is frozen") and turns P14's "repair history insert fails and is logged" into "repair is refused". The two
  should be reviewed together, and neither is safe to ship alone.
* The upgrade detection is spread over several classes from earlier patches: `DsePeers` (P12),
  `DsePeersRecord` and startup checks (P05, #2740), `DseDialectVersions`, `DseCompat.messagingVersionFromGossip`. P15
  adds a third notion of "DSE peer" (`knownDsePeers`, `liveDsePeers`, `gossipsPre40Version`), deliberately wider than
  `DsePeers.gossipedDseVersion` (it counts 11 or less too). A reviewer should check that the three definitions cannot
  disagree in a way that leaves a node both "not upgrading" and "seeing DSE".
* P15's start refusal assumes bootstrap state `COMPLETED` on former DSE nodes, which is set by the DSE node metadata
  import (P04, #2739).
* The schema freeze makes P14's push-ignore path the only remaining way DSE-side DDL could reach this software. Both
  assume DDL is not run on DSE nodes during the upgrade. That is an operator rule, not enforced on the DSE side.
* The in-JVM harness (P13) runs the HCD 1.2 stand-ins without gossip. As a result, both patches test the DSE-facing
  side by hand-sent messages (P14) or injected gossip states (P15), never by a live pre-4.0 peer in gossip.
