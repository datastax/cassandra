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

# Group F — authorization data, access controls and non-core DSE features

Part of the [reviewer triage guide](README.md), which explains the tiers, the review order and the layout of a brief.

## P06 — DSE grants in system_auth and DSE index classes (#2741)

**Tier:** 1 — `CassandraAuthorizer` changes how every `system_auth.role_permissions` row is parsed, in every
`internode_compatibility_mode`, and the index-class handling touches schema validation and index loading.

**Size and shape:** production ~820 added lines in 10 existing files (about 600 of them in
`CassandraAuthorizer.java`); tests ~1,780 added lines (4 new test classes, 2 existing test classes extended); no new
fixtures (`CassandraAuthorizerDseRowsTest` reads the existing DSE 6.8.65 `system_auth` fixture through
`DseFixtures.schemaDataDirectory()`). Two commits: the second one fixes the first. In the first, DSE's `GRANT MODIFY`
was taken to write `UPDATE` + `TRUNCATE`; the second says that it writes the name `MODIFY`. Skim: the
`MessagingServiceMBean`/`NodeProbe`/`CompatStatus` plumbing and the `NoopIndex` message text.

**Gating:**
* Always on, in every mode:
  * `CassandraAuthorizer.permissions` skips permission names this software does not know (`BACKUP`, `PROXY.LOGIN`,
    `SEARCH.*`, …), logging each once. Before, `Permission.valueOf` threw.
  * On data resources the pair `UPDATE` + `TRUNCATE` is read as `MODIFY`. Either name alone is dropped and listed.
  * `knownResource` skips the rows of resource types this software does not have (`rows/…`, `rpc/…`, `search/…`) and
    names it cannot parse. This applies to authorization, to `list` (`LIST PERMISSIONS`) and to the cache bulk loader.
  * A row with a null `permissions` set no longer causes a `NullPointerException` in `GRANT`/`REVOKE`.
  * `REVOKE MODIFY` on a data resource also removes the stored `UPDATE`/`TRUNCATE` names.
  * The alias `com.datastax.bdp.index.storageattachedindex` resolves to SAI.
  * A missing `com.datastax.bdp.*` index class gets a dedicated refusal text. The start is still refused, as before.
  * `IndexMetadata.validate` accepts an unknown custom class when `INDEX_UNKNOWN_IGNORE` is set, even without the mode.
  * `compatstatus` prints a new "DSE UPDATE/TRUNCATE alone" line.
* In `DSE_6` mode only:
  * `GRANT MODIFY` on a data resource writes `UPDATE` + `TRUNCATE`.
  * `UNMASK`/`SELECT_MASKED` on data resources are refused, or silently left out of `GRANT ALL` with a client warning.
  * The reverse index `resource_role_permissons_index` is maintained only if the table exists. Without it,
    `revokeAllOn` finds the roles with `ALLOW FILTERING` on `role_permissions`.
  * Unknown custom index classes (any class, not only DSE ones) load as `NoopIndex`.
  * `CreateIndexStatement` still requires the class of a new index to exist.

**Where to look**
* `src/java/org/apache/cassandra/auth/CassandraAuthorizer.java` — `permissions` / `mapDseWriteGrants`: this is the
  always-on parser of every row. Is "pair = `MODIFY`, lone name = nothing" the right mapping on clusters that never
  ran DSE? Could a row there legitimately hold a name that is now skipped silently instead of failing loudly?
* `CassandraAuthorizer.java` — `knownResource` and its use in `list` and the bulk loader: rows of DSE resource types
  disappear from `LIST PERMISSIONS`, so an operator can no longer see from CQL what DSE left behind. Is that intended?
* `CassandraAuthorizer.java` — `grant`: the `dseForm` branch writes `UPDATE`,`TRUNCATE` even when `MODIFY` looks held
  (the read is at `LOCAL_ONE`). The `UNKNOWN_TO_DSE` refusal compares against `resource.applicablePermissions()` to
  recognise `GRANT ALL`. Check that the set returned to the caller (`nonExistingPermissions`) still gives the
  CASSANDRA-17333 "already granted" error for every case.
* `CassandraAuthorizer.java` — `revoke`: in the mode it always writes the removal of `MODIFY`,`UPDATE`,`TRUNCATE`.
  `removeLookupEntry` is now conditional on `names.containsAll(storedNames)`. Is the reverse-index entry kept exactly
  while any permission name remains?
* `CassandraAuthorizer.java` — `maintainsResourceIndex`, `revokeAllFrom`, `revokeAllOn`: the javadoc says an index
  created by this software on a DSE `system_auth` holds none of DSE's grants until it is rebuilt by hand with `COPY`.
  Until then, dropping a resource leaves grants behind that reattach to a resource recreated with the same name. Is a
  manual runbook step an acceptable guard, or should the rebuild be done automatically?
* `CassandraAuthorizer.java` — the static `dseLoneWriteGrants` map, `dseLoneWriteGrantsScanned`,
  `forgetDseLoneWriteGrants`: this state is mutated concurrently by cache loads, statements and scans. The javadoc
  admits that entries can lag. Confirm it is diagnostic only and never consulted for an authorization decision.
* `src/java/org/apache/cassandra/schema/IndexMetadata.java` — `validate`: the new catch returns early (no options
  validated) for any unknown class when `INDEX_UNKNOWN_IGNORE` is set or the mode is on. Was this acceptance intended
  for `INDEX_UNKNOWN_IGNORE` users who have nothing to do with DSE?
* `src/java/org/apache/cassandra/index/SecondaryIndexManager.java` — `createInstance` and
  `src/java/org/apache/cassandra/index/NoopIndex.java` — constructor, `listedColumns`, `dependsOn`: a target that is
  not a column (`solr_query`) is now accepted in the mode. Can `dependsOn` returning false for every column let a
  column be dropped that DSE still indexes after a rollback?

**Modified or deleted existing code**
* `CassandraAuthorizer.java` — `getExistingPermissions` replaced by `storedPermissionNames` + `existingPermissions`;
  `Permission.valueOf` replaced by a name lookup that skips unknown names. — History: e780b5a45b8, 2022,
  CASSANDRA-17333 (error out on no-op GRANT/REVOKE) introduced `getExistingPermissions`. The invariant is that
  `grant`/`revoke` return exactly what changed.
* `CassandraAuthorizer.java` — `permissions(Set<String>)` (cache bulk load) and `addPermissionsForRole` / `list`
  loops. — History: 89c6addb505, 2021, CASSANDRA-16958 (prewarm role and permission caches), and 879b694d346, 2015,
  CASSANDRA-7653 (role-based access control). Both assumed that every stored name is a valid `Permission` and every
  resource parses. Unknown rows are now skipped, not fatal.
* `CassandraAuthorizer.java` — `revokeAllFrom` / `revokeAllOn`: the reverse-index reads and deletes are now
  conditional. — History: 879b694d346, 2015, CASSANDRA-7653 introduced `resource_role_permissons_index` so that a drop
  finds its roles without a full scan. In the mode without the table, a filtered scan replaces it.
* `src/java/org/apache/cassandra/auth/Resources.java` — `fromName` split into `tryFromName` (returns null for an
  unknown type) and `fromName` (still throws). — History: 217721ae95c, 2015, CASSANDRA-8650.
* `IndexMetadata.java` — `validate`: `classForName` failure caught. — History: 0626be8667a, 2015, CASSANDRA-9459 (new
  2i API). The invariant was that a custom index always names a loadable `Index` class.
* `NoopIndex.java` and `SecondaryIndexManager.createInstance` — new DSE_6 branch and the `com.datastax.bdp.` refusal.
  — History: 11158a02cf3, 2025, CNDB-15582/CNDB-15362 (remove SASI, `NoopIndex` for unknown custom indexes under
  `INDEX_UNKNOWN_IGNORE`). The original assumed that the target is always a column (`column` non-null).

**Failure and rollback risks**
* Rollback to DSE: the rows written in the mode use names DSE has (`UPDATE`,`TRUNCATE`), and `UNMASK`/`SELECT_MASKED`
  are never written on data resources. The patch states that DSE reads both forms. How DSE reads unknown names is
  "not known" according to the patch itself, which is why they are withheld.
* After the switch to `NONE`, rows written in the mode keep `UPDATE`+`TRUNCATE` for good. They are read as `MODIFY`
  forever, so the mapping can never be removed from the parser.
* The reverse-index gap described above: until it is rebuilt by hand, grants survive `DROP` and reattach when a
  resource is recreated with the same name. This is a permission leak, not a refused operation.
* `revokeAllOn` without the index runs a range scan with `ALLOW FILTERING` on `role_permissions` at the auth read
  consistency level on each resource drop. DDL is frozen in the mode unless `allow_ddl` is set, but `DROP ROLE` is not.
* A DSE `RESTRICT` (the `restricted` column) is never read here, so the restricted access opens up. P16 is the guard
  for this case.

**Test integrity**
* Existing tests changed: `ResourcesTest` (one test added, nothing changed); `CompatStatusTest` (the output-order
  assertion now also places the new line; nothing weakened).
* New tests:
  * `CassandraAuthorizerDseRowsTest` (`CQLTester`) is real for the parsing: it reads the DSE 6.8.65 `role_permissions`
    sstables of the fixture. Its other cases insert DSE-shaped rows by hand with CQL.
  * `CassandraAuthorizerDse6Test` drops `resource_role_permissons_index` to imitate a DSE 6.9.26 `system_auth`. The
    absence of that table on DSE is taken from the patch, not from a fixture.
  * `CassandraAuthorizerDseNamesTest` covers the pure name helpers.
  * `Dse6IndexClassesTest` uses fabricated class names and hand-built `IndexMetadata`.
  * The `CompatStatusTest` additions use a mocked `NodeProbe` for the missing-attribute path.
* Not covered by any test in the patch: rollback to a real DSE node reading the `UPDATE`+`TRUNCATE` rows that this
  software writes (no DSE binary in the tests); the reverse-index-gap leak after the switch to `NONE`; the
  `ALLOW FILTERING` path under load or with more than one node.

**Suggested reviewer questions**
* Should the always-on skipping of unknown permission names and resources be limited to clusters with DSE history
  (for example, a node that has run in `DSE_6` once), so that corruption on a clean cluster still fails loudly?
* Does this software create `resource_role_permissons_index` on a DSE `system_auth` at the first start with `NONE`?
  If it does, why is the rebuild from `role_permissions` not done at that moment instead of relying on a `COPY` step?
* Is it acceptable that `LIST PERMISSIONS` hides the `rows/`, `rpc/` and `search/` grants DSE left, while P16 still
  refuses to start because of them?
* Is the `IndexMetadata.validate` relaxation for `INDEX_UNKNOWN_IGNORE` without the mode intended, given that
  CNDB-15582 only covered loading?

## P16 — startup check of DSE access controls (#2757)

**Tier:** 1 — two passes are added to the `CassandraDaemon.setup` sequence, and the second one blocks for up to
120 s after the node has joined the ring. A periodic blocking cluster-wide read is also scheduled on the shared
`ScheduledExecutors.optionalTasks` executor. All of it is in `DSE_6` mode only.

**Size and shape:** production ~1,055 lines in the new `Dse6AccessControlCheck.java` plus ~190 lines in 8 existing
files (`CassandraDaemon`, `DseUpgradeGuards`, `CassandraRelevantProperties`, the MBean/`NodeProbe`/`CompatStatus`
plumbing, and javadoc and the source string in `CassandraAuthorizer`); test harness ~24 lines in `Instance.java`;
tests ~1,700 lines (2 new classes, 5 existing ones extended). Skim: the `CassandraAuthorizer` hunks (javadoc and
the `loneDseWriteGrantsSource` text) and the `compatstatus` printing.

**Gating:**
* `DseCompat.enabled()` / `DSE_6` mode. With `NONE`, `verifyAtStartup`, `verifyAfterJoin` and `recheck` return at
  once, and `startStatusLog` (which schedules the recheck) is a no-op.
* Three new properties: `dse_restrictions_check_wait_seconds` (120), `dse_restrictions_local_check_only` and
  `ignore_dse_restrictions`.
* Always-on changes for a cluster that never had DSE:
  * `compatstatus` prints "DSE access controls ignored : not checked".
  * The "DSE UPDATE/TRUNCATE alone" source string now names the mode.
  * The in-JVM `Instance` startup calls both passes; they are no-ops without the mode.

**Where to look**
* `src/java/org/apache/cassandra/service/CassandraDaemon.java` — `setup`: the first pass sits after
  `preloadPreparedStatements` and before `initServer`; the second sits after `doAuthSetup` and
  `AuditLogManager.initialize`, before the client transports. A refusal from the second pass calls `exitOrFail` on a
  node that has already gossiped `NORMAL` and serves internode reads and writes. Is exiting at that point safe (hints,
  in-flight coordinator requests, the peers' view of the node)? Would a pass before the join, reading at `QUORUM`
  through `initServer`, be possible instead?
* `src/java/org/apache/cassandra/service/Dse6AccessControlCheck.java` — `verifyAfterJoin(Function, LongSupplier,
  LongConsumer, long, long)`: retries only `RequestExecutionException`, fails closed on `ReadAbortException` and any
  other `RuntimeException`, and sleeps uninterruptibly. Is the classification right? For example, could an
  `UnavailableException` of a cluster with a DC down be retried for 120 s on every node start?
* `Dse6AccessControlCheck.java` — `scan`: the cluster-wide read is `QueryProcessor.process(query, consistency)`,
  unpaged, over the whole `role_permissions` table. The comment says the table is small. Is that true for the large
  DSE deployments (many roles × resources, plus `rows/` grants)? Is a guardrail abort (failing closed) the intended
  outcome when it is not?
* `Dse6AccessControlCheck.java` — `readRow`: the triggers are a non-empty `restricted` set, a `rows/` resource with
  permissions, and a non-empty `grantables` set (a warning only). The column names `restricted` and `grantables` and
  the `DSE_RLACA` extension key are asserted from the patch's description of DSE. Are they the exact DSE 6.8/6.9 names?
* `src/java/org/apache/cassandra/service/DseUpgradeGuards.java` — `schedulePeriodicTasks` / `recheckAccessControls`:
  the recheck runs every 10 minutes on `optionalTasks`, which also runs the size-estimate recorder, the compaction
  submitter backstop and the speculative-retry updater. The javadoc accepts the delay. Is it acceptable?
* `Dse6AccessControlCheck.java` — `recheck`: it logs at ERROR and keeps serving when a DSE node adds a restriction
  during the upgrade. `RESTRICT ROWS` run on DSE is never seen, because the schema is frozen. Is "do not run it" in
  a log line enough of a guard?
* `Dse6AccessControlCheck.java` — the static state (`lastCheck`, `firstPassPermissionEntries`,
  `ignoredEntriesLogged`): written by the startup thread and the `optionalTasks` thread without coordination. The
  javadoc accepts that a lone grant can be logged twice. Check that no decision depends on that state.

**Modified or deleted existing code**
* `DseUpgradeGuards.startStatusLog` — now also schedules the recheck, on `optionalTasks`. Added by this series (P15).
* `CassandraAuthorizer.loneDseWriteGrantsSource` and the javadoc of the scan helpers. Added by this series (P06).
* `CassandraDaemon.setup` — two inserted blocks; no existing line changed.
* `test/distributed/org/apache/cassandra/distributed/impl/Instance.java` — the startup calls mirror the daemon, but
  the second pass runs before `postStartup`'s auth setup, whereas the daemon runs it after `doAuthSetup`. The
  ordering in the tests therefore differs from production.

**Failure and rollback risks**
* Restart: with `system_auth` at RF = node count, a node restarted while another replica is down waits for
  `dse_restrictions_check_wait_seconds` and then refuses to start. `Dse6GossipTest` had to set
  `dse_restrictions_local_check_only` to restart its 2-node cluster one node at a time. A full cold restart where the
  nodes come up one by one has the same exposure, so operators need the property.
* A node refused by the second pass has already joined the ring, run `doAuthSetup` and written local state. The next
  start repeats both passes (the check is idempotent), but the peers see a node flap.
* The check fails closed whatever authorizer is configured, `AllowAllAuthorizer` included.
* `ignore_dse_restrictions` opens up the access that DSE restricted, by design. It is logged at WARN on every start.
* Rollback: nothing is written to `system_auth`. A rollback to DSE is unaffected.

**Test integrity**
* Existing tests changed:
  * `Dse6GossipTest`: a restart test now sets `dse_restrictions_local_check_only`. This relaxes the conditions of an
    existing scenario to keep it passing.
  * `Dse6GuardsTest`: the expected scheduling log line is replaced by the new text; a compatstatus assertion is added.
  * `CassandraAuthorizerDseRowsTest`: the expected `loneDseWriteGrantsSource` strings are updated to the new text.
  * `CompatStatusTest`: the expected output order is extended; the NONE-mode assertion is moved.
  * `Dse6UpgradeGuardsTest`: extended.
* New tests:
  * `Dse6AccessControlMixedTest` is real: in-JVM, an HCD 1.2 peer, the cluster-wide read crosses the legacy codec.
    The peer is HCD 1.2, not DSE, so its table has no `restricted`/`grantables` columns.
  * `Dse6GossipTest.theAccessControlCheckWaitsForTheReplicasOfSystemAuth` is real: in-JVM, retry while a replica is
    down.
  * The `Dse6GuardsTest` refusal cases are real in-JVM starts.
  * `Dse6AccessControlCheckTest` (33 tests) adds the DSE columns with `ALTER TABLE system_auth.role_permissions ADD
    (restricted …, grantables …)`, writes `DSE_RLACA` as a hand-written hex value, and injects the scan, the clock and
    the pause through `verifyAfterJoin(Function, LongSupplier, LongConsumer, …)` stubs.
* Not covered by any test in the patch: a `role_permissions` table with `restricted`/`grantables` written by DSE (no
  DSE fixture is used); the `exitOrFail` path after the join in the real daemon (dtests throw from `Instance`); the
  delay that the recheck causes to other `optionalTasks` work.

**Suggested reviewer questions**
* Should the cluster-wide pass run before the join, so that a refusal never leaves a node that joined and then exited?
* Is `LOCAL_QUORUM`, taken from `auth_read_consistency_level`, enough to see a `RESTRICT` written in another DC under
  `NetworkTopologyStrategy` for `system_auth`?
* Should the default wait, or the use of `local_check_only`, be in the runbook for rolling restarts of small clusters
  (1–2 replicas of `system_auth`)?
* Should the periodic recheck get its own executor rather than `optionalTasks`?

## P17 — refusal of non-core DSE features (#2758)

**Tier:** 2 — a new `DSE_6`-only startup refusal (workloads in DSE's `nodes/*` files, Search indexes, `solr.data`,
Graph, Analytics, DSEFS/CFS and Advanced Replication keyspaces). Its failure mode is a refused start; the index
hunks only tighten P06 code within the mode.

**Size and shape:** production ~504 lines in the new `Dse6NonCoreTraces.java` plus ~100 lines in 12 existing files
(mostly comments, plumbing and message text); test harness ~11 lines in `Instance.java`; tests ~1,590 lines (2 new
classes plus 3 extended). Skim: `CC4NodesFileReader` and `DseNodeWorkloads` (comment-only), `NoopIndex` (message
text) and the `compatstatus` printing. `DseSchemaLoadTest` (665 lines) tests the loading of the DSE schema
(`LocalStrategy` keyspaces and system keyspace definitions). No production change in this patch touches that loading.

**Gating:**
* `DatabaseDescriptor.isDse6CompatibilityMode()`; the override `ignore_dse_peer_workloads` covers `nodes/peers` only.
* Always-on changes for a cluster that never had DSE:
  * `compatstatus` prints "Non-core DSE traces : not checked".
  * The `SecondaryIndexManager.dseOnlyIndexRefusal` text gains a sentence.

**Where to look**
* `src/java/org/apache/cassandra/service/CassandraDaemon.java` — `setup`: the check runs right after
  `Schema.instance.loadFromDisk()` and before `Keyspace.open`. Does `loadFromDisk` call `IndexMetadata.validate`
  (which now throws for a Search class in the mode)? If it does, a Search index aborts the schema load with the index
  message before the grouped refusal can list every trace. The in-JVM test asserts the grouped text, so the answer
  appears to be no, but this was not established from the code.
* `src/java/org/apache/cassandra/service/Dse6NonCoreTraces.java` — `addWorkloadTraces` / `nonCore`: `Unknown` is
  not treated as a trace, so that an entry for a peer already upgraded does not block the next node. Could a Search
  or Analytics peer ever be recorded as `Unknown` by DSE?
* `Dse6NonCoreTraces.java` — `NON_CORE_KEYSPACES` and the list of keyspaces that are not traces (`solr_admin`,
  `dse_system`, …): the classification rests on the DSE 6.9.26 schema fixture. Is it the same on DSE 6.8 and on
  clusters with a renamed DSEFS keyspace, which the javadoc says is not detected?
* `Dse6NonCoreTraces.java` — `verifyAtStartup`: `keyspacesWithGraphEngine` reads `system_schema.keyspaces` with
  `executeInternal` directly, not through `Schema`. Is that correct before `Keyspace.open` of `system_schema`?
* `src/java/org/apache/cassandra/index/SecondaryIndexManager.java` — `createInstance`, and
  `src/java/org/apache/cassandra/schema/IndexMetadata.java` — `validate`: in the mode a Search class is refused
  before the `INDEX_UNKNOWN_IGNORE` branch, so the property no longer rescues it. Is that precedence intended?

**Modified or deleted existing code**
* `SecondaryIndexManager.createInstance`, `IndexMetadata.validate`, `NoopIndex` message text. Added by this series
  (P06). The `INDEX_UNKNOWN_IGNORE` branch, 11158a02cf3, 2025, CNDB-15582, now comes second in the mode.
* `CC4NodesFileReader` and `DseNodeWorkloads` comments. Added by this series (an earlier `HCD-608` patch).
* `CassandraDaemon.setup` — an inserted block; no existing line changed.

**Failure and rollback risks**
* The node has written its first files (imported node metadata, local metadata, commit log segments) before the
  refusal. The javadoc names a rollback as the way back to DSE, so a refused node needs the rollback procedure, not
  just a DSE restart.
* `nodes/peers` entries can be stale and this software never rewrites the file. The override is the only way out
  and stays needed until the flag is dropped (per the refusal text).
* `dse.yaml` relocations (the DSEFS keyspace name, `solr_data_dir`, the Advanced Replication directory) are not seen.
  The patch states this as a known gap.

**Test integrity**
* Existing tests changed:
  * `Dse6IndexClassesTest`: the expected messages are updated to the new texts (drop on DSE before the upgrade); a
    comment is updated.
  * `CompatStatusTest`: the expected output order is extended with the new line.
  * `Dse6GuardsTest`: new tests only; the class comment is reworded. No assertion was weakened.
* New tests:
  * `Dse6NonCoreTracesTest` (`CQLTester`) installs the real DSE `nodes/` fixtures (`DseFixtures.installNodesDirectory`)
    and edits their workload fields.
  * The `Dse6GuardsTest` cases are real in-JVM starts. The Search index row is inserted by hand into
    `system_schema.indexes`, and `solr.data` is an empty directory.
  * `noneModeIsUnchanged` checks that `NONE` ignores the traces.
  * `DseSchemaLoadTest` parses the DSE 6.9.26 `describe-schema.txt` fixture into `system_schema` rows.
* Not covered by any test in the patch: a real DSE Search schema row (with options beyond `class_name`/`target`), a
  `graph_engine` value written by DSE, and DSE 6.8 node files with non-core workloads, if the fixtures lack them (not
  established).

**Suggested reviewer questions**
* Can `Schema.loadFromDisk` throw on a DSE Search index before `Dse6NonCoreTraces.verifyAtStartup` runs, for example
  through `TableMetadata.validate`?
* Is excluding `Unknown` safe, or should an `Unknown` entry in `nodes/local` (this node's own record) count as a trace?
* Should `DseSchemaLoadTest` move to the patch whose production code it exercises?

## Group notes
* Order: P16 builds on P06's `CassandraAuthorizer.readDseLoneWriteGrant` / `dseLoneWriteGrantsScanned` API and
  changes the meaning of P06's `loneDseWriteGrantsSource`. P17 rewrites P06's index branches in
  `SecondaryIndexManager.createInstance`, `IndexMetadata.validate` and the `NoopIndex` text (DSE Search moves from
  "load as no-op" to "refuse"). Read P06's index hunks with P17's open alongside.
* P06 makes the authorizer ignore DSE `RESTRICT` and `rows/` grants silently, in every mode. P16 is the only guard
  that turns this into a refusal, and only in `DSE_6` mode. After the switch to `NONE`, a `RESTRICT` left in
  `system_auth` is ignored without any check.
* `CassandraDaemon.setup` now runs three new DSE checks at three points: P17 after the schema load, the first P16
  pass before `initServer`, and the second P16 pass after the join. `Instance.java` mirrors them, with the second P16
  pass ordered differently from production.
* All three patches extend `MessagingServiceMBean`/`NodeProbe`/`CompatStatus` with one attribute each, and add an
  `AttributeNotFoundException` fallback for an older server. The pattern is duplicated, not shared.
* Dependencies outside the group: `DseUpgradeGuards.startStatusLog` and the schema freeze (P15);
  `Dse6UpgradeNodesPersistence` and the `nodes/` readers (an earlier `HCD-608` patch); the HCD 1.2 mixed-cluster
  harness used by `Dse6AccessControlMixedTest`.
