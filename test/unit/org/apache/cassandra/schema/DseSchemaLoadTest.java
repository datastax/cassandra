/*
 * Copyright IBM Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.schema;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.SimpleStatement;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.auth.AuthKeyspace;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.InternodeCompatibilityMode;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy;
import org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.dsecompat.DseFixtures;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.compress.LZ4Compressor;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.locator.EverywhereStrategy;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.LocalStrategy;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.net.DseCompatRefusals;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.NoPayload;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.schema.SchemaTransformation.SchemaTransformationResult;
import org.apache.cassandra.service.Dse6NonCoreTraces;
import org.apache.cassandra.service.DsePeersRecord;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.reads.PercentileSpeculativeRetryPolicy;
import org.apache.cassandra.service.reads.repair.ReadRepairStrategy;
import org.apache.cassandra.tracing.TraceKeyspace;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.StorageCompatibilityMode;

import static org.apache.cassandra.cql3.QueryProcessor.executeOnceInternal;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The schema of a DataStax Enterprise 6 node upgraded in place loads, whatever the internode compatibility mode:
 * the rows DSE left in {@code system_schema} (reconstructed from the schema as cqlsh describes it on a DSE cluster, in the
 * format DSE stores them) go through {@link Schema#loadFromDisk()}, the call that failed at startup with
 * {@code AssertionError: LocalStrategy should not be used} on DSE's two {@code LocalStrategy} keyspaces
 * ({@code dse_system_local} and {@code dse_insights_local}). A user keyspace with {@code LocalStrategy} is still refused.
 * <p>
 * In {@code DSE_6} mode the replicated system keyspaces keep DSE's definitions through the startup step that brings them to
 * this software's ({@code StorageService.setUpDistributedSystemKeyspaces}), and a schema pull from a peer speaking the DSE
 * dialect is answered with no schema; the additions are made, once, at the first start with {@code NONE}.
 */
public class DseSchemaLoadTest extends CQLTester
{
    /** The replication class of every keyspace of the dump that is not a local system keyspace. */
    private static final Map<String, Class<?>> EXPECTED_STRATEGIES = ImmutableMap.<String, Class<?>>builder()
                                                                                 .put("system_auth", SimpleStrategy.class)
                                                                                 .put("dse_system_local", LocalStrategy.class)
                                                                                 .put("dse_system", EverywhereStrategy.class)
                                                                                 .put("test_ks", SimpleStrategy.class)
                                                                                 .put("dse_leases", SimpleStrategy.class)
                                                                                 .put("solr_admin", EverywhereStrategy.class)
                                                                                 .put("dse_insights", EverywhereStrategy.class)
                                                                                 .put("dse_insights_local", LocalStrategy.class)
                                                                                 .put("system_distributed", SimpleStrategy.class)
                                                                                 .put("system_backups", EverywhereStrategy.class)
                                                                                 .put("dse_perf", SimpleStrategy.class)
                                                                                 .put("system_traces", SimpleStrategy.class)
                                                                                 .put("dse_security", SimpleStrategy.class)
                                                                                 .build();

    /**
     * The keyspaces the DSE 6.8.65 and 6.9.26 test clusters switched to {@code NetworkTopologyStrategy}; DSE creates
     * them with {@code SimpleStrategy}.
     */
    private static final Set<String> NTS_SWITCHED_KEYSPACES = ImmutableSet.of("system_auth", "system_distributed", "system_traces",
                                                                              "dse_security", "dse_leases", "dse_perf");

    /** The latest of the fixed timestamps this software writes the {@code system_schema} rows of the replicated system keyspaces with. */
    private static final long LATEST_REPLICATED_SYSTEM_KEYSPACE_GENERATION = Math.max(TraceKeyspace.GENERATION,
                                                                                       Math.max(AuthKeyspace.GENERATION, SystemDistributedKeyspace.GENERATION));

    /** Columns of {@code system_schema.tables} DSE writes with the same value for every table (as DSE 6.9.26 writes them). */
    private static final String DSE_CONSTANT_TABLE_COLUMNS = "extensions, dclocal_read_repair_chance, read_repair_chance";
    private static final String DSE_CONSTANT_TABLE_VALUES = "{}, 0.0, 0.0";

    private static final Pattern KEYSPACE = Pattern.compile("CREATE KEYSPACE (\\w+) WITH replication = (\\{[^}]*\\})\\s+AND durable_writes = (true|false)");
    private static final Pattern TYPE = Pattern.compile("CREATE TYPE (\\w+)\\.\"?(\\w+)\"? \\((.*)\\)", Pattern.DOTALL);
    private static final Pattern TABLE = Pattern.compile("CREATE TABLE (\\w+)\\.\"?(\\w+)\"? \\(");
    private static final Pattern MAP_ENTRY = Pattern.compile("'([^']*)': '([^']*)'");
    private static final Pattern TABLE_OPTION = Pattern.compile("^(?:\\) WITH|\\s+AND) (\\w+) = (.*)$", Pattern.MULTILINE);

    private InternodeCompatibilityMode modeBefore;

    /**
     * The native network (and so {@code StorageService.initServer}) is started once, in NONE mode: in DSE_6 mode a
     * fresh node that has not bootstrapped is refused (the guards of the upgrade), and an upgraded node always starts bootstrapped.
     */
    @BeforeClass
    public static void startNetworkBeforeTheModeIsSet()
    {
        requireNetwork();
    }

    @Before
    public void saveMode()
    {
        modeBefore = DatabaseDescriptor.getInternodeCompatibilityMode();
    }

    /**
     * Drops the keyspaces only DSE has. The replicated system keyspaces are not restored: each load first removes
     * every {@code system_schema} row this software wrote for them (see {@link #loadAndCheckDseSchema}), so both
     * modes load exactly DSE's definitions whatever ran before. The partition tombstones of that removal stay: they
     * also hide what {@code setUpDistributedSystemKeyspaces} writes afterwards at the same fixed timestamps, so after
     * a load test this software's additions to those keyspaces exist in memory only, and a later reload from disk
     * brings them back to DSE's tables.
     */
    @After
    public void dropDseKeyspacesAndRestoreMode() throws Throwable
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(modeBefore);
        for (String keyspace : EXPECTED_STRATEGIES.keySet())
            if (!SchemaConstants.isReplicatedSystemKeyspace(keyspace))
                schemaChange("DROP KEYSPACE IF EXISTS " + keyspace);
        // without the DSE local keyspaces the filtered list is the distributed keyspaces themselves again
        assertThat(Schema.instance.getNonLocalStrategyKeyspaces()).isSameAs(Schema.instance.distributedKeyspaces());
    }

    @Test
    public void dseSchemaLoadsInNoneMode() throws Throwable
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.NONE);
        loadAndCheckDseSchema(false);
    }

    /**
     * The dump has no custom index, so the mode changes nothing in the load itself (DSE index classes are
     * covered by {@code Dse6IndexClassesTest}); this run also switches those keyspaces to NetworkTopologyStrategy.
     */
    @Test
    public void dseSchemaLoadsInDse6Mode() throws Throwable
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        // the DDL of the checks (ALTER TABLE, and ALTER KEYSPACE refused for its strategy) would meet the schema freeze of the mode
        try (WithProperties allowDdl = new WithProperties().set(CassandraRelevantProperties.INTERNODE_COMPATIBILITY_MODE_ALLOW_DDL, true))
        {
            loadAndCheckDseSchema(true);
        }
    }

    @Test
    public void userKeyspaceWithLocalStrategyIsStillRefused()
    {
        for (InternodeCompatibilityMode mode : InternodeCompatibilityMode.values())
        {
            DatabaseDescriptor.setInternodeCompatibilityMode(mode);
            // keyspace names are case-sensitive: only DSE's exact names are accepted
            for (String name : new String[]{ "user_local_ks", "DSE_SYSTEM_LOCAL", "dse_system_local2" })
            {
                DistributedSchema before = Schema.instance.getDistributedSchemaBlocking();
                Keyspaces after = before.getKeyspaces().withAddedOrUpdated(KeyspaceMetadata.create(name, KeyspaceParams.local()));
                SchemaTransformationResult result = new SchemaTransformationResult(before,
                                                                                   new DistributedSchema(after, before.getVersion()),
                                                                                   Keyspaces.diff(before.getKeyspaces(), after));
                assertThatThrownBy(() -> Schema.instance.mergeAndUpdateVersion(result, false))
                .as("%s in %s", name, mode)
                .isInstanceOf(AssertionError.class)
                .hasMessage("LocalStrategy should not be used");
                assertThat(Schema.instance.getKeyspaceMetadata(name)).as("%s in %s", name, mode).isNull();
            }
        }
    }

    @Test
    public void dseLocalKeyspaceCannotBeCreatedWithLocalStrategyThroughCql()
    {
        // accepting what DSE left on disk does not open LocalStrategy to DDL
        assertThatThrownBy(() -> schemaChange("CREATE KEYSPACE dse_system_local WITH replication = {'class': 'LocalStrategy'}"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessage("Unable to use given strategy class: LocalStrategy is reserved for internal use.");
    }

    private void loadAndCheckDseSchema(boolean ntsSwitched) throws Throwable
    {
        // precondition, the NONE case: with no DSE keyspace loaded yet the filtered lists are the unfiltered ones, in order
        assertThat(Schema.instance.getNonLocalStrategyKeyspaces()).isSameAs(Schema.instance.distributedKeyspaces());
        assertThat(List.copyOf(Schema.instance.getNonLocalStrategyKeyspaces().names())).isEqualTo(List.copyOf(Schema.instance.distributedKeyspaces().names()));
        assertThat(Iterables.transform(Keyspace.nonLocalStrategy(), Keyspace::getName)).containsExactlyElementsOf(Iterables.transform(Keyspace.distributed(), Keyspace::getName));

        DseSchema dse = DseSchema.parse(new String(Files.readAllBytes(DseFixtures.describeSchema(DseFixtures.DseVersion.DSE_6_9_26)), StandardCharsets.UTF_8));
        assertThat(dse.replication.keySet()).containsExactlyInAnyOrderElementsOf(EXPECTED_STRATEGIES.keySet());
        String localDc = DatabaseDescriptor.getLocalDataCenter();
        if (ntsSwitched)
            NTS_SWITCHED_KEYSPACES.forEach(ks -> dse.replication.put(ks, ImmutableMap.of("class", NetworkTopologyStrategy.class.getName(), localDc, "1")));

        // an upgraded node has only DSE's rows for the replicated system keyspaces: remove the ones this software
        // wrote (at its fixed generation timestamps). DSE's rows are written at the current time here; the timestamps
        // DSE really uses for them are not collected (they decide whether this software's fixed-timestamp updates of
        // those keyspaces win on disk after the upgrade)
        for (String keyspace : SchemaConstants.REPLICATED_SYSTEM_KEYSPACE_NAMES)
        {
            for (String schemaTable : SchemaKeyspaceTables.ALL)
                executeOnceInternal(String.format("DELETE FROM system_schema.%s USING TIMESTAMP %d WHERE keyspace_name = ?", schemaTable, LATEST_REPLICATED_SYSTEM_KEYSPACE_GENERATION), keyspace);
            // what is left can only be DSE's own rows, written by a previous run at the current time
            List<String> left = new ArrayList<>();
            executeOnceInternal("SELECT table_name FROM system_schema.tables WHERE keyspace_name = ?", keyspace).forEach(row -> left.add(row.getString("table_name")));
            assertThat(left).as(keyspace).isSubsetOf(Iterables.transform(dse.tables.get(keyspace), t -> t.name));
        }
        dse.writeSystemSchemaRows();
        // what CassandraDaemon.setup does at startup
        Schema.instance.loadFromDisk();
        // then, in DSE_6 mode, the check of the DSE features this software does not support: the schema of a DSE node of
        // the Cassandra workload has no trace of them (DSE core's keyspaces, solr_admin included: DSE core creates it on
        // every node); with NONE nothing is checked
        assertThat(Schema.instance.getKeyspaceMetadata("solr_admin")).isNotNull();
        Dse6NonCoreTraces.resetStatus();
        try
        {
            Dse6NonCoreTraces.verifyAtStartup();
            assertThat(Dse6NonCoreTraces.status()).isEqualTo(DatabaseDescriptor.isDse6CompatibilityMode() ? "none" : null);
        }
        finally
        {
            Dse6NonCoreTraces.resetStatus();
        }
        // exactly DSE's tables, without those this software adds to the replicated system keyspaces
        for (String keyspace : SchemaConstants.REPLICATED_SYSTEM_KEYSPACE_NAMES)
            assertThat(Schema.instance.getKeyspaceMetadata(keyspace).tables.stream().map(t -> t.name))
            .as(keyspace).containsExactlyInAnyOrderElementsOf(Iterables.transform(dse.tables.get(keyspace), t -> t.name));

        for (Map.Entry<String, Class<?>> expected : EXPECTED_STRATEGIES.entrySet())
        {
            String name = expected.getKey();
            Class<?> strategy = ntsSwitched && NTS_SWITCHED_KEYSPACES.contains(name) ? NetworkTopologyStrategy.class : expected.getValue();
            KeyspaceMetadata ksm = Schema.instance.getKeyspaceMetadata(name);
            assertThat(ksm).as(name).isNotNull();
            assertThat(ksm.params.replication.klass).as(name).isEqualTo(strategy);
            assertThat(Keyspace.open(name).getReplicationStrategy()).as(name).isInstanceOf(strategy);
            for (Map.Entry<String, List<String[]>> type : dse.typeFields.getOrDefault(name, Map.of()).entrySet())
            {
                UserType loaded = ksm.types.getNullable(ByteBufferUtil.bytes(type.getKey()));
                assertThat(loaded).as("%s.%s", name, type.getKey()).isNotNull();
                assertThat(loaded.fieldNames()).as("%s.%s", name, type.getKey()).hasSize(type.getValue().size());
                for (int i = 0; i < type.getValue().size(); i++)
                {
                    assertThat(loaded.fieldNameAsString(i)).as("%s.%s field %d", name, type.getKey(), i).isEqualTo(type.getValue().get(i)[0]);
                    // cqlsh spells a nested frozen type out ("frozen<set<frozen<t>>>"), this software does not: compare the
                    // parsed types (the row was written from the same string, so this checks the load, not DSE's spelling)
                    assertThat(loaded.fieldType(i)).as("%s.%s field %d", name, type.getKey(), i)
                                                   .isEqualTo(CQLTypeParser.parse(name, type.getValue().get(i)[1], ksm.types));
                }
            }
            for (TableMetadata table : dse.tables.getOrDefault(name, List.of()))
            {
                TableMetadata loaded = ksm.getTableOrViewNullable(table.name);
                assertThat(loaded).as(table.toString()).isNotNull();
                assertThat(loaded.id).as(table.toString()).isEqualTo(table.id);
                // every option DSE wrote, nodesync aside, loads as this software's CQL parser reads the same DDL
                // (a consistency check, like the column checks below: the rows were written from that parse; the option
                // values themselves are checked against the fixture below for a few tables)
                assertThat(loaded.params).as(table.toString()).isEqualTo(table.params);
                assertThat(loaded.columns()).as(table.toString()).hasSameSizeAs(table.columns());
                for (ColumnMetadata column : table.columns())
                {
                    ColumnMetadata loadedColumn = loaded.getColumn(column.name);
                    assertThat(loadedColumn).as("%s.%s", table, column.name).isNotNull();
                    assertThat(loadedColumn.kind).as("%s.%s", table, column.name).isEqualTo(column.kind);
                    assertThat(loadedColumn.type.asCQL3Type().toString()).as("%s.%s", table, column.name).isEqualTo(column.type.asCQL3Type().toString());
                    assertThat(loadedColumn.clusteringOrder()).as("%s.%s", table, column.name).isEqualTo(column.clusteringOrder());
                }
                assertThat(Keyspace.open(name).getColumnFamilyStores().stream().map(cfs -> cfs.name)).as(table.toString()).contains(table.name);
            }
        }
        // DSE's option values, as the fixture has them; the option this software does not have (nodesync) is ignored
        TableParams solrResources = Schema.instance.getTableMetadata("dse_system_local", "solr_resources").params;
        assertThat(solrResources.gcGraceSeconds).isEqualTo(7776000);
        assertThat(solrResources.memtableFlushPeriodInMs).isEqualTo(3600000);
        assertThat(solrResources.speculativeRetry).isEqualTo(new PercentileSpeculativeRetryPolicy(99.0));
        assertThat(solrResources.additionalWritePolicy).isEqualTo(new PercentileSpeculativeRetryPolicy(99.0));
        assertThat(solrResources.readRepair).isEqualTo(ReadRepairStrategy.BLOCKING);
        assertThat(solrResources.caching).isEqualTo(CachingParams.CACHE_KEYS);
        assertThat(solrResources.compaction.klass()).isEqualTo(SizeTieredCompactionStrategy.class);
        assertThat(solrResources.compaction.options()).containsEntry("min_threshold", "4").containsEntry("max_threshold", "32");
        assertThat(solrResources.compression.getSstableCompressor()).isInstanceOf(LZ4Compressor.class);
        assertThat(solrResources.compression.chunkLength()).isEqualTo(64 * 1024);
        assertThat(solrResources.bloomFilterFpChance).isEqualTo(0.01);
        assertThat(solrResources.crcCheckChance).isEqualTo(1.0);
        assertThat(solrResources.minIndexInterval).isEqualTo(128);
        assertThat(solrResources.maxIndexInterval).isEqualTo(2048);
        TableParams testTable = Schema.instance.getTableMetadata("test_ks", "test_table").params;
        assertThat(testTable.gcGraceSeconds).isEqualTo(864000);
        assertThat(testTable.memtableFlushPeriodInMs).isZero();
        TableParams slowLog = Schema.instance.getTableMetadata("dse_perf", "node_slow_log").params;
        assertThat(slowLog.compaction.klass()).isEqualTo(TimeWindowCompactionStrategy.class);
        assertThat(Schema.instance.getTableMetadata("system_distributed", "nodesync_user_validations").params.defaultTimeToLive).isEqualTo(86400);
        UntypedResultSet nodesync = executeOnceInternal("SELECT nodesync FROM system_schema.tables WHERE keyspace_name = 'dse_system_local' AND table_name = 'solr_resources'");
        assertThat(nodesync.one().getFrozenTextMap("nodesync")).isEqualTo(ImmutableMap.of("enabled", "true", "incremental", "true"));

        // the local keyspaces hold data like any other table, on this node only, through the client path as well
        sessionNet().execute(new SimpleStatement("INSERT INTO dse_system_local.solr_resources (core_name, resource_name, resource_value) VALUES ('c', 'r', 0x01)")
                             .setConsistencyLevel(ConsistencyLevel.QUORUM));
        Row written = sessionNet().execute(new SimpleStatement("SELECT resource_name FROM dse_system_local.solr_resources WHERE core_name = 'c'")
                                           .setConsistencyLevel(ConsistencyLevel.LOCAL_ONE)).one();
        assertThat(written.getString("resource_name")).isEqualTo("r");

        // they are part of the distributed schema and of its version (this software digests every keyspace but
        // system and system_schema; the patch does not change that), but own no ranges: not streamed, repaired,
        // moved or counted in ownership
        assertThat(Schema.instance.distributedKeyspaces().names()).contains("dse_system_local", "dse_insights_local");
        assertThat(Schema.instance.getNonLocalStrategyKeyspaces().names())
        .doesNotContain("dse_system_local", "dse_insights_local")
        .contains("dse_system", "dse_insights", "solr_admin", "system_backups", "dse_leases", "dse_perf", "dse_security", "test_ks",
                  "system_auth", "system_distributed", "system_traces");
        assertThat(Iterables.transform(Keyspace.nonLocalStrategy(), Keyspace::getName)).doesNotContain("dse_system_local", "dse_insights_local");
        assertThat(StorageService.instance.getNonLocalStrategyKeyspaces()).doesNotContain("dse_system_local", "dse_insights_local").contains("dse_system");
        // drain flushes them all the same
        assertThat(Iterables.transform(Keyspace.distributed(), Keyspace::getName)).contains("dse_system_local", "dse_insights_local");
        assertThatThrownBy(() -> StorageService.instance.effectiveOwnership("dse_system_local"))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Ownership values for keyspaces with LocalStrategy are meaningless");
        // with DSE's EverywhereStrategy and LocalStrategy keyspaces among the user keyspaces, ownership without a keyspace
        // is refused, as in HCD 1.2 (DSE 6.9.26 itself reports it in nodetool status)
        assertThatThrownBy(() -> StorageService.instance.effectiveOwnership(null))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Non-system keyspaces don't have the same replication settings, effective ownership information is meaningless");

        // DDL on them: tables can be altered (and the change is part of the schema version); the keyspace cannot, since
        // ALTER KEYSPACE keeps the LocalStrategy that CQL reserves for internal use
        UUID versionBefore = Schema.instance.getVersion();
        schemaChange("ALTER TABLE dse_system_local.solr_resources WITH comment = 'altered'");
        assertThat(Schema.instance.getTableMetadata("dse_system_local", "solr_resources").params.comment).isEqualTo("altered");
        assertThat(Schema.instance.getVersion()).isNotEqualTo(versionBefore);
        assertThatThrownBy(() -> schemaChange("ALTER KEYSPACE dse_system_local WITH durable_writes = false"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessage("Unable to use given strategy class: LocalStrategy is reserved for internal use.");

        // the next startup step: in DSE_6 mode, system_auth, system_traces and system_distributed keep DSE's definitions
        // and a DSE node pulling this node's schema gets nothing; the additions wait for the mode to be dropped
        if (DatabaseDescriptor.isDse6CompatibilityMode())
        {
            checkSetUpKeepsDseDefinitions(dse);
            checkSchemaPullAnswers(dse);
            DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.NONE);
        }
        // with NONE (the first start after the mode is dropped, for the DSE_6 run), the startup step brings them to this
        // software's definitions, adding the tables and columns DSE does not have and keeping DSE's own tables; it
        // changes them once. (The schema version cannot show it here: it is digested from system_schema, where the
        // partition tombstones of the load hide what the step writes at its fixed timestamps.)
        Map<String, KeyspaceMetadata> beforeAdditions = replicatedSystemKeyspaces();
        StorageService.instance.setUpDistributedSystemKeyspaces();
        assertThat(Schema.instance.getKeyspaceMetadata(SchemaConstants.DISTRIBUTED_KEYSPACE_NAME).tables.stream().map(t -> t.name))
        .contains("nodesync_status", "nodesync_user_validations", "backups", "backup_task_state", "restore_task_state")
        .containsAll(SystemDistributedKeyspace.TABLE_NAMES);
        assertThat(Schema.instance.getKeyspaceMetadata(SchemaConstants.AUTH_KEYSPACE_NAME).tables.stream().map(t -> t.name))
        .contains(AuthKeyspace.ROLES, AuthKeyspace.RESOURCE_ROLE_INDEX, AuthKeyspace.NETWORK_PERMISSIONS);
        assertThat(Schema.instance.getTableMetadata(SchemaConstants.TRACE_KEYSPACE_NAME, TraceKeyspace.SESSIONS).getColumn(ColumnIdentifier.getInterned("coordinator_port", true))).isNotNull();
        Map<String, KeyspaceMetadata> afterAdditions = replicatedSystemKeyspaces();
        for (String keyspace : SchemaConstants.REPLICATED_SYSTEM_KEYSPACE_NAMES)
            assertThat(afterAdditions.get(keyspace)).as(keyspace).isNotEqualTo(beforeAdditions.get(keyspace));
        StorageService.instance.setUpDistributedSystemKeyspaces();
        assertThat(replicatedSystemKeyspaces()).isEqualTo(afterAdditions);
        for (String keyspace : SchemaConstants.REPLICATED_SYSTEM_KEYSPACE_NAMES)
            assertThat(Schema.instance.getKeyspaceMetadata(keyspace).params.replication.klass).as(keyspace)
                                                                                          .isEqualTo(ntsSwitched ? NetworkTopologyStrategy.class : SimpleStrategy.class);
        assertThat(Schema.instance.getKeyspaceMetadata("dse_system_local").params.replication.klass).isEqualTo(LocalStrategy.class);
    }

    /** The replicated system keyspaces as this node holds them now, by name. */
    private static Map<String, KeyspaceMetadata> replicatedSystemKeyspaces()
    {
        Map<String, KeyspaceMetadata> keyspaces = new HashMap<>();
        for (String keyspace : SchemaConstants.REPLICATED_SYSTEM_KEYSPACE_NAMES)
            keyspaces.put(keyspace, Schema.instance.getKeyspaceMetadata(keyspace));
        return keyspaces;
    }

    /**
     * In DSE_6 mode the startup step that brings the replicated system keyspaces to this software's definitions leaves
     * them as DSE wrote them; what it defers is what the NONE start adds.
     */
    private void checkSetUpKeepsDseDefinitions(DseSchema dse) throws Throwable
    {
        Map<String, KeyspaceMetadata> loaded = replicatedSystemKeyspaces();

        // run twice, as for a node started with join_ring: false that joins later: what is deferred is logged once per keyspace
        withLogAppender(StorageService.class, appender -> {
            StorageService.instance.setUpDistributedSystemKeyspaces();
            StorageService.instance.setUpDistributedSystemKeyspaces();
            for (String keyspace : SchemaConstants.REPLICATED_SYSTEM_KEYSPACE_NAMES)
                assertThat(appender.list.stream().filter(e -> e.getFormattedMessage().startsWith("internode_compatibility_mode DSE_6: " + keyspace +
                                                                                                    " keeps the definition it was loaded with")))
                .as(keyspace).hasSize(1);
        });

        assertThat(replicatedSystemKeyspaces()).isEqualTo(loaded);
        for (String keyspace : SchemaConstants.REPLICATED_SYSTEM_KEYSPACE_NAMES)
            assertThat(Schema.instance.getKeyspaceMetadata(keyspace).tables.stream().map(t -> t.name))
            .as(keyspace).containsExactlyInAnyOrderElementsOf(Iterables.transform(dse.tables.get(keyspace), t -> t.name));
        // DSE's tracing tables have no port columns
        assertThat(Schema.instance.getTableMetadata(SchemaConstants.TRACE_KEYSPACE_NAME, TraceKeyspace.SESSIONS).getColumn(ColumnIdentifier.getInterned("coordinator_port", true))).isNull();

        // the additions deferred to the NONE start, as the startup step logs them
        assertThat(DseSystemKeyspaces.deferredAdditions(TraceKeyspace.metadata(), Schema.instance.getKeyspaceMetadata(SchemaConstants.TRACE_KEYSPACE_NAME)))
        .containsExactlyInAnyOrder("system_traces.sessions.coordinator_port", "system_traces.events.source_port");
        assertThat(DseSystemKeyspaces.deferredAdditions(SystemDistributedKeyspace.metadata(), Schema.instance.getKeyspaceMetadata(SchemaConstants.DISTRIBUTED_KEYSPACE_NAME)))
        .containsExactlyInAnyOrder("system_distributed.partition_denylist",
                                   "system_distributed.repair_history.coordinator_port",
                                   "system_distributed.repair_history.participants_v2");
        // with the storage compatibility mode DSE_6 requires (the CIDR and identity tables need 5.0)
        StorageCompatibilityMode storageMode = DatabaseDescriptor.getStorageCompatibilityMode();
        try
        {
            DatabaseDescriptor.setStorageCompatibilityMode(StorageCompatibilityMode.HCD_1);
            assertThat(DseSystemKeyspaces.deferredAdditions(AuthKeyspace.metadata(), Schema.instance.getKeyspaceMetadata(SchemaConstants.AUTH_KEYSPACE_NAME)))
            .containsExactlyInAnyOrder("system_auth.resource_role_permissons_index", "system_auth.network_permissions");
        }
        finally
        {
            DatabaseDescriptor.setStorageCompatibilityMode(storageMode);
        }
    }

    /**
     * In DSE_6 mode, the answer to a schema pull: nothing for a peer speaking the DSE dialect (a count of zero on the wire at
     * version 11), and this node's schema for another node of this software, whatever the tables it holds.
     */
    private static void checkSchemaPullAnswers(DseSchema dse) throws Exception
    {
        DefaultSchemaUpdateHandler handler = (DefaultSchemaUpdateHandler) Schema.instance.updateHandler;
        InetAddressAndPort dsePeer = InetAddressAndPort.getByName("127.0.0.250");
        InetAddressAndPort upgradedPeer = InetAddressAndPort.getByName("127.0.0.251");
        long answeredWithoutSchema = DseCompatRefusals.count(DefaultSchemaUpdateHandler.SCHEMA_PULL_WITHOUT_SCHEMA);
        try
        {
            MessagingService.instance().versions.set(dsePeer, MessagingService.VERSION_3014);
            MessagingService.instance().versions.set(upgradedPeer, MessagingService.current_version);
            assertThat(DsePeersRecord.peers()).containsExactly(dsePeer);

            // the answer as it goes on the wire at version 11, through the serializer of the response verb: a count of
            // zero and nothing else (the 3.0 layout has no compatibility flag)
            Message<NoPayload> pull = Message.builder(Verb.SCHEMA_PULL_REQ, NoPayload.noPayload).from(dsePeer).build();
            Message<Collection<Mutation>> answer = pull.responseWith(handler.schemaMutationsForPull(dsePeer));
            assertThat(answer.verb()).isEqualTo(Verb.SCHEMA_PULL_RSP);
            assertThat(answer.payload).isEmpty();
            try (DataOutputBuffer out = new DataOutputBuffer())
            {
                answer.verb().serializer().serialize(answer.payload, out, MessagingService.VERSION_3014);
                assertThat(out.toByteArray()).containsExactly(0, 0, 0, 0);
            }
            assertThat(DseCompatRefusals.count(DefaultSchemaUpdateHandler.SCHEMA_PULL_WITHOUT_SCHEMA)).isEqualTo(answeredWithoutSchema + 1);

            // another node of this software gets the schema, DSE's keyspaces included
            Map<String, Set<String>> tables = tablesIn(handler.schemaMutationsForPull(upgradedPeer));
            assertThat(tables.get("test_ks")).containsExactly("test_table");
            assertThat(tables.get(SchemaConstants.AUTH_KEYSPACE_NAME)).containsAll(Iterables.transform(dse.tables.get(SchemaConstants.AUTH_KEYSPACE_NAME), t -> t.name));
            assertThat(DseCompatRefusals.count(DefaultSchemaUpdateHandler.SCHEMA_PULL_WITHOUT_SCHEMA)).isEqualTo(answeredWithoutSchema + 1);
        }
        finally
        {
            MessagingService.instance().versions.reset(dsePeer);
            MessagingService.instance().versions.reset(upgradedPeer);
            // the peer at 11 went on (and off) the record of DSE 6 peers: nothing may be left of it for the next test
            DsePeersRecord.awaitPersisted();
        }
        assertThat(DsePeersRecord.peers()).isEmpty();
        assertThat(DsePeersRecord.persistedPeers()).isEmpty();
    }

    /** The table names in the {@code system_schema.tables} rows of schema mutations, by keyspace. */
    private static Map<String, Set<String>> tablesIn(Collection<Mutation> mutations)
    {
        Map<String, Set<String>> tables = new HashMap<>();
        for (Mutation mutation : mutations)
            for (PartitionUpdate update : mutation.getPartitionUpdates())
                if (update.metadata().name.equals(SchemaKeyspaceTables.TABLES))
                    for (org.apache.cassandra.db.rows.Row row : update.rows())
                        tables.computeIfAbsent(UTF8Type.instance.compose(update.partitionKey().getKey()), k -> new HashSet<>())
                              .add(UTF8Type.instance.compose(row.clustering().bufferAt(0)));
        return tables;
    }

    /**
     * DSE's schema, from cqlsh's description of it, as DSE stores it in {@code system_schema}: tables with DSE's option
     * values verbatim (including {@code nodesync} and DSE's spellings such as {@code '99PERCENTILE'}) and the columns
     * DSE writes for every table (as DSE 6.9.26 writes them), types as field name and type strings. Two parts are
     * assumptions, not observed on DSE: the replication class is stored fully qualified
     * ({@code org.apache.cassandra.locator.<class>}, as cqlsh prints only the short name), and the {@code type} of a
     * column is written as this software renders it (the {@code system_schema.columns} rows observed on DSE have simple
     * and one-level types only, the dump also nested frozen ones). The local system keyspaces ({@code system},
     * {@code system_schema}) are skipped: this software defines them itself and never loads their rows.
     */
    private static final class DseSchema
    {
        final Map<String, Map<String, String>> replication = new LinkedHashMap<>();
        final Map<String, Boolean> durableWrites = new HashMap<>();
        final Map<String, Map<String, List<String[]>>> typeFields = new LinkedHashMap<>();
        final Map<String, List<TableMetadata>> tables = new LinkedHashMap<>();
        final Map<TableMetadata, Map<String, String>> rawOptions = new HashMap<>();

        static DseSchema parse(String describe)
        {
            DseSchema schema = new DseSchema();
            String withoutComments = describe.replaceAll("(?s)/\\*.*?\\*/", "").replaceAll("(?m)^//.*$", "");
            for (String statement : withoutComments.split(";\\s*\\n"))
            {
                statement = statement.trim();
                if (statement.isEmpty())
                    continue;
                Matcher keyspace = KEYSPACE.matcher(statement);
                Matcher type = TYPE.matcher(statement);
                Matcher table = TABLE.matcher(statement);
                if (keyspace.lookingAt())
                    schema.addKeyspace(keyspace.group(1), keyspace.group(2), Boolean.parseBoolean(keyspace.group(3)));
                else if (type.lookingAt())
                    schema.addType(type.group(1), type.group(2), type.group(3));
                else if (table.lookingAt())
                    schema.addTable(table.group(1), table.group(2), statement);
                else
                    throw new AssertionError("unexpected statement in the DSE schema dump: " + statement);
            }
            return schema;
        }

        private static boolean skipped(String keyspace)
        {
            return SchemaConstants.isLocalSystemKeyspace(keyspace);
        }

        private void addKeyspace(String name, String replicationLiteral, boolean durable)
        {
            if (skipped(name))
                return;
            Map<String, String> options = new LinkedHashMap<>();
            Matcher entry = MAP_ENTRY.matcher(replicationLiteral);
            while (entry.find())
            {
                String value = entry.group(2);
                // cqlsh prints the short class name; DSE stores the fully qualified one
                if (entry.group(1).equals("class") && !value.contains("."))
                    value = "org.apache.cassandra.locator." + value;
                options.put(entry.group(1), value);
            }
            replication.put(name, options);
            durableWrites.put(name, durable);
        }

        private void addType(String keyspace, String name, String body)
        {
            if (skipped(keyspace))
                return;
            List<String[]> fields = new ArrayList<>();
            for (String line : body.split(",\\s*\\n"))
            {
                String field = line.trim();
                int space = field.indexOf(' ');
                fields.add(new String[]{ field.substring(0, space).replace("\"", ""), field.substring(space + 1).trim() });
            }
            typeFields.computeIfAbsent(keyspace, k -> new LinkedHashMap<>()).put(name, fields);
        }

        private void addTable(String keyspace, String name, String statement)
        {
            if (skipped(keyspace))
                return;
            TableMetadata existing = Schema.instance.getTableMetadata(keyspace, name);
            TableId id = existing != null ? existing.id : TableId.unsafeDeterministic(keyspace, name);
            TableMetadata table = CreateTableStatement.parse(statement, keyspace, types(keyspace)).id(id).build();
            Map<String, String> options = new LinkedHashMap<>();
            Matcher option = TABLE_OPTION.matcher(statement);
            while (option.find())
                options.put(option.group(1), option.group(2).trim());
            assertThat(options).as("options of %s.%s", keyspace, name).containsKeys("nodesync", "speculative_retry", "caching", "compaction");
            // cqlsh prints the compaction and compression classes as DSE stores them, fully qualified
            assertThat(options.get("compaction")).as("compaction of %s.%s", keyspace, name).contains("'class': 'org.apache.cassandra.db.compaction.");
            assertThat(options.get("compression")).as("compression of %s.%s", keyspace, name).contains("'class': 'org.apache.cassandra.io.compress.");
            tables.computeIfAbsent(keyspace, k -> new ArrayList<>()).add(table);
            rawOptions.put(table, options);
        }

        private Types types(String keyspace)
        {
            Types.RawBuilder builder = Types.rawBuilder(keyspace);
            typeFields.getOrDefault(keyspace, Map.of()).forEach((name, fields) -> {
                List<String> names = new ArrayList<>();
                List<String> types = new ArrayList<>();
                fields.forEach(f -> { names.add(f[0]); types.add(f[1]); });
                builder.add(name, names, types);
            });
            return builder.build();
        }

        void writeSystemSchemaRows()
        {
            replication.forEach((keyspace, options) ->
                                executeOnceInternal("INSERT INTO system_schema.keyspaces (keyspace_name, durable_writes, replication) VALUES (?, ?, ?)",
                                                    keyspace, durableWrites.get(keyspace), options));
            typeFields.forEach((keyspace, types) -> types.forEach((name, fields) -> {
                List<String> names = new ArrayList<>();
                List<String> fieldTypes = new ArrayList<>();
                fields.forEach(f -> { names.add(f[0]); fieldTypes.add(f[1]); });
                executeOnceInternal("INSERT INTO system_schema.types (keyspace_name, type_name, field_names, field_types) VALUES (?, ?, ?, ?)",
                                    keyspace, name, names, fieldTypes);
            }));
            tables.forEach((keyspace, list) -> list.forEach(table -> {
                Map<String, String> options = rawOptions.get(table);
                String query = String.format("INSERT INTO system_schema.tables (keyspace_name, table_name, id, flags, %s, %s) VALUES (?, ?, ?, ?, %s, %s)",
                                             DSE_CONSTANT_TABLE_COLUMNS, String.join(", ", options.keySet()),
                                             DSE_CONSTANT_TABLE_VALUES, String.join(", ", options.values()));
                executeOnceInternal(query, keyspace, table.name, table.id.asUUID(), TableMetadata.Flag.toStringSet(table.flags));
                for (ColumnMetadata column : table.columns())
                    executeOnceInternal("INSERT INTO system_schema.columns (keyspace_name, table_name, column_name, clustering_order, column_name_bytes, kind, position, type, required_for_liveness) " +
                                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, false)",
                                        keyspace, table.name, column.name.toString(), column.clusteringOrder().toString().toLowerCase(),
                                        column.name.bytes, column.kind.toString().toLowerCase(), column.position(), column.type.asCQL3Type().toString());
            }));
        }
    }
}
