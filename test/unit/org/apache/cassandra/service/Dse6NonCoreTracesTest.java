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

package org.apache.cassandra.service;

import java.io.OutputStream;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.InternodeCompatibilityMode;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.statements.schema.IndexTarget;
import org.apache.cassandra.db.Dse51SystemTables;
import org.apache.cassandra.db.Dse51SystemTablesRecords;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.dsecompat.Dse51Fixtures;
import org.apache.cassandra.dsecompat.DseFixtures;
import org.apache.cassandra.dsecompat.DseFixtures.DseVersion;
import org.apache.cassandra.exceptions.StartupException;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.Dse51GossipStates;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.HeartBeatState;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.LocalStrategy;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.nodes.CC4UpgradeNodesPersistence;
import org.apache.cassandra.nodes.DseNodeWorkloads;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.Indexes;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.SchemaTestUtil;
import org.apache.cassandra.schema.SystemDistributedKeyspace;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.Tables;
import org.apache.cassandra.service.Dse6NonCoreTraces.Kind;
import org.apache.cassandra.service.Dse6NonCoreTraces.Traces;
import org.apache.cassandra.utils.FBUtilities;
import org.msgpack.jackson.dataformat.MessagePackFactory;

import static org.apache.cassandra.config.CassandraRelevantProperties.INTERNODE_COMPATIBILITY_MODE_IGNORE_DSE_PEER_WORKLOADS;
import static org.apache.cassandra.cql3.QueryProcessor.executeOnceInternal;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The startup check of {@code internode_compatibility_mode: DSE_6} that refuses any trace of the DSE features this
 * software does not support ({@link Dse6NonCoreTraces}): each kind of trace, the DSE core keyspaces that are not traces,
 * the workloads of DSE's {@code nodes/local} and {@code nodes/peers} (the fixtures written by DSE, and copies of them with
 * other workloads), the override of the entries of {@code nodes/peers}, and {@code NONE}, which reads nothing. In
 * {@code DSE_5_1} the traces DSE 5.1.51 leaves: the workloads it stored and gossips (those of the Search and Graph
 * node of its fixtures), and the rows of {@code system_schema} read before the schema is loaded.
 */
public class Dse6NonCoreTracesTest extends CQLTester
{
    private static final String OVERRIDE_KEY = INTERNODE_COMPATIBILITY_MODE_IGNORE_DSE_PEER_WORKLOADS.getKey();
    private static final String SOLR_INDEX = "com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex";
    private static final String NO_TRACE = "DSE 6 compatibility mode: no trace of DSE Search, Graph, Analytics, DSEFS, CFS or Advanced Replication found";
    private static final String REMEDY_PREFIX = ". Roll this node back to DSE, remove them there (";
    /** The start of the remedies when a compaction strategy, which an ALTER removes, is refused with other traces. */
    private static final String ALTER_REMEDY_PREFIX = ". Roll this node back to DSE, remove or alter them there (";
    /** The start of the remedies when compaction strategies alone are refused. */
    private static final String ONLY_ALTER_REMEDY_PREFIX = ". Roll this node back to DSE, alter them there (";
    private static final String REMEDY_SUFFIX = ") and upgrade it again";
    private static final String PEER_ADVICE = ". An entry of nodes/peers is stale when that node has since been restarted as a Cassandra-only " +
                                              "node or has left the cluster: if every entry listed from nodes/peers is stale, start with -D" +
                                              OVERRIDE_KEY + "=true instead (it warns at every start; this software never rewrites " +
                                              "nodes/peers, so keep the option until the flag drop)";
    private static final String FOUND = "internode_compatibility_mode DSE_6 refused: found traces of DSE features that this software does " +
                                        "not support; only DSE core (the Cassandra workload) can be upgraded. ";

    private static final String FOUND_DSE51 = "internode_compatibility_mode DSE_5_1 refused: found traces of DSE " +
                                              "features that this software does not support; only DSE core (the " +
                                              "Cassandra workload) can be upgraded. ";
    private static final String NO_TRACE_DSE51 = "DSE 5.1 compatibility mode: no trace of DSE Search, Graph, " +
                                                 "Analytics, DSEFS, CFS, Advanced Replication, tiered storage or " +
                                                 "in-memory tables found, nor a compaction strategy this software " +
                                                 "no longer has";
    /** The Search and Graph node of the DSE 5.1.51 fixtures. */
    private static final String SEARCH_GRAPH_NODE = "127.0.51.21";
    /** What DSE 5.1.51 stored for the Search and Graph node, as the refusals list it. */
    private static final String SEARCH_GRAPH_WORKLOADS = "workloads [Cassandra, Graph, Search], graph true, " +
                                                         "workload Search";

    private InternodeCompatibilityMode modeBefore;
    private File metadataBefore;
    private Path root;

    @Before
    public void setUp() throws Exception
    {
        modeBefore = DatabaseDescriptor.getInternodeCompatibilityMode();
        metadataBefore = DatabaseDescriptor.getMetadataDirectory();
        root = Files.createTempDirectory("dse6noncore");
        DatabaseDescriptor.setMetadataDirectory(new File(Files.createDirectories(root.resolve("metadata"))));
        Dse6NonCoreTraces.resetStatus();
    }

    @After
    public void tearDown()
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(modeBefore);
        DatabaseDescriptor.setMetadataDirectory(metadataBefore);
        new File(root).deleteRecursive();
        Dse6NonCoreTraces.resetStatus();
    }

    // ---- helpers ----

    private interface CheckedRunnable
    {
        void run() throws Exception;
    }

    /** Runs {@code action} and returns the messages {@link Dse6NonCoreTraces} logged at {@code level} (every level when null) meanwhile. */
    private List<String> logsOf(Level level, CheckedRunnable action) throws Throwable
    {
        List<String> messages = new ArrayList<>();
        withLogAppender(Dse6NonCoreTraces.class, appender -> {
            action.run();
            appender.list.stream().filter(e -> level == null || e.getLevel() == level).map(ILoggingEvent::getFormattedMessage).forEach(messages::add);
        });
        return messages;
    }

    private static Traces keyspaces(String... names)
    {
        Map<String, String> keyspaces = new TreeMap<>();
        for (String name : names)
            keyspaces.put(name, null);
        Traces traces = new Traces();
        Dse6NonCoreTraces.addKeyspaceTraces(traces, keyspaces);
        return traces;
    }

    private static String refusal(Traces traces, boolean override)
    {
        try
        {
            Dse6NonCoreTraces.verify(traces, override);
            return null;
        }
        catch (StartupException e)
        {
            assertThat(e.returnCode).isEqualTo(StartupException.ERR_WRONG_CONFIG);
            return e.getMessage();
        }
    }

    private static List<String> descriptions(Traces traces)
    {
        return traces.all.stream().map(t -> t.feature + " " + t.description).collect(Collectors.toList());
    }

    private static ObjectMapper msgpack()
    {
        MessagePackFactory factory = new MessagePackFactory();
        factory.disable(JsonGenerator.Feature.AUTO_CLOSE_TARGET);
        return new ObjectMapper(factory); // checkstyle: permit this instantiation
    }

    /** Installs the nodes files of {@code version} in the metadata directory, then applies {@code edit} to the fields of nodes/local. */
    @SuppressWarnings("unchecked")
    private Path editNodesLocal(DseVersion version, Consumer<Map<String, Object>> edit) throws Exception
    {
        Path nodes = DseFixtures.installNodesDirectory(DatabaseDescriptor.getMetadataDirectory().toPath(), version, false);
        Map<String, Object> local = msgpack().readValue(Files.readAllBytes(nodes.resolve("local")), LinkedHashMap.class);
        edit.accept(local);
        write(nodes.resolve("local"), local);
        return nodes;
    }

    /** Installs the nodes files of {@code version} in the metadata directory, then applies {@code edit} to the fields of the first entry of nodes/peers. */
    @SuppressWarnings("unchecked")
    private Path editFirstPeer(DseVersion version, Consumer<Map<String, Object>> edit) throws Exception
    {
        Path nodes = DseFixtures.installNodesDirectory(DatabaseDescriptor.getMetadataDirectory().toPath(), version, false);
        List<Map<String, Object>> peers = msgpack().readValue(Files.readAllBytes(nodes.resolve("peers")), List.class);
        edit.accept(peers.get(0));
        write(nodes.resolve("peers"), peers);
        return nodes;
    }

    private static void write(Path path, Object value) throws Exception
    {
        try (OutputStream out = Files.newOutputStream(path))
        {
            msgpack().writeValue(out, value);
        }
    }

    // ---- keyspaces ----

    @Test
    public void everyKeyspaceOfAnUnsupportedFeatureIsATrace()
    {
        Traces traces = keyspaces("HiveMetaStore", "cfs", "cfs_archive", "dse_advrep", "dse_analytics", "dsefs");
        assertThat(descriptions(traces)).containsExactly("ANALYTICS keyspace HiveMetaStore",
                                                         "ANALYTICS keyspace cfs",
                                                         "ANALYTICS keyspace cfs_archive",
                                                         "ADVANCED_REPLICATION keyspace dse_advrep",
                                                         "ANALYTICS keyspace dse_analytics",
                                                         "ANALYTICS keyspace dsefs");
        // each on its own refuses the start, empty or not (the check does not look into them)
        for (String keyspace : Dse6NonCoreTraces.NON_CORE_KEYSPACES.keySet())
            assertThat(refusal(keyspaces(keyspace), false)).as(keyspace).contains("keyspace " + keyspace + '.');

        assertThat(refusal(traces, false))
        .isEqualTo(FOUND + "DSE Analytics, DSEFS and CFS: keyspace HiveMetaStore, keyspace cfs, keyspace cfs_archive, keyspace " +
                   "dse_analytics, keyspace dsefs; DSE Advanced Replication: keyspace dse_advrep" + REMEDY_PREFIX +
                   "migrate the data of the Analytics, DSEFS and CFS keyspaces elsewhere if it is needed and drop them, empty or not; " +
                   "stop Advanced Replication, migrate what its keyspace holds if it is needed and drop it, empty or not" + REMEDY_SUFFIX);
    }

    @Test
    public void theKeyspacesOfDseCoreAreNotTraces() throws Throwable
    {
        // DSE core's keyspaces, solr_admin (created by DSE core on every node, whatever its workload), the system keyspaces,
        // and names that differ from the traces by case or suffix only (keyspace names are case-sensitive)
        Traces traces = keyspaces("dse_system", "dse_system_local", "dse_security", "dse_perf", "dse_leases", "dse_insights",
                                  "dse_insights_local", "dse_audit", "solr_admin", "system", "system_schema", "system_auth",
                                  "system_distributed", "system_traces", "system_backups", "system_views", "system_virtual_schema",
                                  "hivemetastore", "DSE_ADVREP", "dsefs2", "my_cfs");
        assertThat(traces.all).isEmpty();
        assertThat(logsOf(Level.INFO, () -> Dse6NonCoreTraces.verify(traces, false))).containsExactly(NO_TRACE);
        assertThat(Dse6NonCoreTraces.status()).isEqualTo("none");
    }

    @Test
    public void aKeyspaceWithAGraphEngineIsATrace()
    {
        Map<String, String> keyspaces = new TreeMap<>();
        keyspaces.put("core_graph", "Core");
        keyspaces.put("classic_graph", "Classic");
        keyspaces.put("plain", null);
        Traces traces = new Traces();
        Dse6NonCoreTraces.addKeyspaceTraces(traces, keyspaces);
        assertThat(descriptions(traces)).containsExactly("GRAPH keyspace classic_graph (graph_engine Classic)",
                                                         "GRAPH keyspace core_graph (graph_engine Core)");
        assertThat(refusal(traces, false))
        .isEqualTo(FOUND + "DSE Graph: keyspace classic_graph (graph_engine Classic), keyspace core_graph (graph_engine Core)" +
                   REMEDY_PREFIX + "migrate the data of the graph keyspaces elsewhere and drop them" + REMEDY_SUFFIX);
    }

    // ---- DSE Search ----

    @Test
    public void anIndexOfADseSearchClassIsATrace()
    {
        assertThat(Dse6NonCoreTraces.isSearchIndexClass("com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex")).isTrue();
        assertThat(Dse6NonCoreTraces.isSearchIndexClass("com.datastax.bdp.search.solr.ThriftSolrSecondaryIndex")).isTrue();
        assertThat(Dse6NonCoreTraces.isSearchIndexClass("com.datastax.bdp.cassandra.index.solr.Cql3SolrSecondaryIndex")).isTrue();
        assertThat(Dse6NonCoreTraces.isSearchIndexClass("com.datastax.bdp.cassandra.index.solr.SolrSecondaryIndex")).isTrue();
        assertThat(Dse6NonCoreTraces.isSearchIndexClass("org.apache.cassandra.index.sai.StorageAttachedIndex")).isFalse();
        assertThat(Dse6NonCoreTraces.isSearchIndexClass("com.datastax.bdp.index.StorageAttachedIndex")).isFalse();
        assertThat(Dse6NonCoreTraces.isSearchIndexClass(null)).isFalse();

        TableMetadata table = TableMetadata.builder("ks", "t").addPartitionKeyColumn("k", Int32Type.instance)
                                           .addRegularColumn("v", UTF8Type.instance)
                                           .indexes(Indexes.of(custom("t_solr_query_index", SOLR_INDEX, "solr_query"),
                                                               custom("thrift_index", "com.datastax.bdp.search.solr.ThriftSolrSecondaryIndex", "v"),
                                                               // DSE 6.8's name of SAI is an alias of it, not a trace
                                                               custom("sai_index", "com.datastax.bdp.index.StorageAttachedIndex", "v"),
                                                               custom("other_index", "com.example.MissingIndex", "v"),
                                                               IndexMetadata.fromSchemaMetadata("regular_index", IndexMetadata.Kind.COMPOSITES,
                                                                                                Map.of(IndexTarget.TARGET_OPTION_NAME, "v"))))
                                           .build();
        Traces traces = new Traces();
        Dse6NonCoreTraces.addSearchIndexTraces(traces, List.of(KeyspaceMetadata.create("ks", KeyspaceParams.simple(1), Tables.of(table))));
        assertThat(descriptions(traces)).containsExactlyInAnyOrder("SEARCH index ks.t.t_solr_query_index (" + SOLR_INDEX + ')',
                                                                   "SEARCH index ks.t.thrift_index (com.datastax.bdp.search.solr.ThriftSolrSecondaryIndex)");
    }

    private static IndexMetadata custom(String name, String className, String target)
    {
        return IndexMetadata.fromSchemaMetadata(name, IndexMetadata.Kind.CUSTOM,
                                                Map.of(IndexTarget.CUSTOM_INDEX_OPTION_NAME, className, IndexTarget.TARGET_OPTION_NAME, target));
    }

    @Test
    public void aSolrDataDirectoryIsATraceEvenEmpty() throws Exception
    {
        File first = new File(Files.createDirectories(root.resolve("data1")));
        File second = new File(Files.createDirectories(root.resolve("data2")));
        // a plain file of that name is not DSE Search's directory
        Files.createFile(root.resolve("data1").resolve("solr.data"));
        Traces none = new Traces();
        Dse6NonCoreTraces.addSolrDataTraces(none, new File[]{ first, second });
        assertThat(none.all).isEmpty();

        Files.createDirectories(root.resolve("data2").resolve("solr.data"));
        Traces traces = new Traces();
        Dse6NonCoreTraces.addSolrDataTraces(traces, new File[]{ first, second });
        String directory = root.resolve("data2").resolve("solr.data").toString();
        assertThat(descriptions(traces)).containsExactly("SEARCH directory " + directory);
        assertThat(refusal(traces, false))
        .isEqualTo(FOUND + "DSE Search: directory " + directory + REMEDY_PREFIX + "delete the solr.data directories once no search index is left" + REMEDY_SUFFIX);
    }

    // ---- workloads ----

    @Test
    public void theWorkloadsDseWroteForACassandraNodeAreNotTraces() throws Throwable
    {
        for (DseVersion version : DseVersion.values())
        {
            DseFixtures.installNodesDirectory(DatabaseDescriptor.getMetadataDirectory().toPath(), version, false);
            DseNodeWorkloads local = CC4UpgradeNodesPersistence.localWorkloadsInNodesDirectory();
            assertThat(local.workloads).as(version.dir).containsExactly("Cassandra");
            assertThat(local.graph).as(version.dir).isFalse();
            assertThat(local.workload).as(version.dir).isEqualTo("Cassandra");
            assertThat(Dse6NonCoreTraces.nonCore(local)).as(version.dir).isEmpty();
            List<DseNodeWorkloads> peers = CC4UpgradeNodesPersistence.peerWorkloadsInNodesDirectory();
            assertThat(peers).as(version.dir).isNotEmpty();
            assertThat(peers).as(version.dir).allMatch(p -> p.peer != null && p.workloads.equals(List.of("Cassandra")) && Dse6NonCoreTraces.nonCore(p).isEmpty());
            Traces traces = new Traces();
            Dse6NonCoreTraces.addWorkloadTraces(traces, local, peers);
            assertThat(traces.all).as(version.dir).isEmpty();
        }
        // a 6.9 node without peers writes an empty map
        DseFixtures.installNodesDirectory(DatabaseDescriptor.getMetadataDirectory().toPath(), DseVersion.DSE_6_9_26, true);
        assertThat(CC4UpgradeNodesPersistence.peerWorkloadsInNodesDirectory()).isEmpty();
        // and a node that never ran DSE has no nodes directory
        new File(DatabaseDescriptor.getMetadataDirectory().toPath().resolve("nodes")).deleteRecursive();
        assertThat(CC4UpgradeNodesPersistence.localWorkloadsInNodesDirectory()).isNull();
        assertThat(CC4UpgradeNodesPersistence.peerWorkloadsInNodesDirectory()).isEmpty();
    }

    @Test
    public void workloadsOtherThanCassandraInNodesLocalAreTraces() throws Exception
    {
        editNodesLocal(DseVersion.DSE_6_9_26, local -> local.put("workloads", List.of("Search", "Cassandra")));
        assertThat(localTraces()).containsExactly("WORKLOADS this node (nodes/local): workloads [Search, Cassandra]");

        editNodesLocal(DseVersion.DSE_6_8_65, local -> local.put("workloads", List.of("Analytics")));
        assertThat(localTraces()).containsExactly("WORKLOADS this node (nodes/local): workloads [Analytics]");

        editNodesLocal(DseVersion.DSE_6_9_26, local -> local.put("graph", true));
        assertThat(localTraces()).containsExactly("WORKLOADS this node (nodes/local): graph true");

        // the legacy single string alone
        editNodesLocal(DseVersion.DSE_6_9_26, local -> local.put("workload", "SearchAnalytics"));
        assertThat(localTraces()).containsExactly("WORKLOADS this node (nodes/local): workload SearchAnalytics");

        editNodesLocal(DseVersion.DSE_6_9_26, local -> {
            local.put("workloads", List.of("Graph", "Cassandra"));
            local.put("graph", true);
            local.put("workload", "Graph");
        });
        List<String> all = localTraces();
        assertThat(all).containsExactly("WORKLOADS this node (nodes/local): workloads [Graph, Cassandra], graph true, workload Graph");

        Traces traces = new Traces();
        Dse6NonCoreTraces.addWorkloadTraces(traces, CC4UpgradeNodesPersistence.localWorkloadsInNodesDirectory(), List.of());
        // the override covers the entries of nodes/peers only
        assertThat(refusal(traces, true))
        .isEqualTo(FOUND + "DSE workloads other than Cassandra: this node (nodes/local): workloads [Graph, Cassandra], graph true, workload Graph" +
                   REMEDY_PREFIX + "restart this node as a plain Cassandra node (dse cassandra without -s, -k or -g; SOLR_ENABLED, " +
                   "SPARK_ENABLED and GRAPH_ENABLED set to 0 in /etc/default/dse for a package install)" + REMEDY_SUFFIX);
    }

    private static List<String> localTraces()
    {
        Traces traces = new Traces();
        Dse6NonCoreTraces.addWorkloadTraces(traces, CC4UpgradeNodesPersistence.localWorkloadsInNodesDirectory(),
                                            CC4UpgradeNodesPersistence.peerWorkloadsInNodesDirectory());
        return descriptions(traces);
    }

    @Test
    public void workloadsOtherThanCassandraInAPeerEntryAreTracesTheOverrideIgnores() throws Throwable
    {
        editFirstPeer(DseVersion.DSE_6_9_26, peer -> {
            peer.put("workloads", List.of("Search", "Cassandra"));
            peer.put("workload", "Search");
        });
        Traces traces = new Traces();
        Dse6NonCoreTraces.addWorkloadTraces(traces, CC4UpgradeNodesPersistence.localWorkloadsInNodesDirectory(),
                                            CC4UpgradeNodesPersistence.peerWorkloadsInNodesDirectory());
        String entry = "peer 127.0.0.2 (nodes/peers): workloads [Search, Cassandra], workload Search";
        assertThat(descriptions(traces)).containsExactly("WORKLOADS " + entry);
        assertThat(refusal(traces, false))
        .isEqualTo(FOUND + "DSE workloads other than Cassandra: " + entry + REMEDY_PREFIX + "restart the peers listed as plain " +
                   "Cassandra nodes while this node runs, so that its nodes/peers records their new workloads" + REMEDY_SUFFIX + PEER_ADVICE);

        // the override: a warning that lists the entries ignored, the start goes on
        List<String> warnings = logsOf(Level.WARN, () -> Dse6NonCoreTraces.verify(traces, true));
        assertThat(warnings).containsExactly("The DSE workloads other than Cassandra recorded for peers in nodes/peers are ignored (" +
                                             OVERRIDE_KEY + "=true): " + entry + "; this software never rewrites nodes/peers, and " +
                                             "supports none of these workloads: remove the option unless each of these entries is " +
                                             "stale (the node was since restarted as a Cassandra-only node, or has left the cluster)");
        assertThat(Dse6NonCoreTraces.status()).isEqualTo("none but nodes/peers ignored (" + OVERRIDE_KEY + "=true): " + entry);

        editFirstPeer(DseVersion.DSE_6_9_26, peer -> peer.put("graph", true));
        assertThat(localTraces()).containsExactly("WORKLOADS peer 127.0.0.2 (nodes/peers): graph true");
    }

    @Test
    public void theOverrideWarnsEvenWhenNothingIsIgnored() throws Throwable
    {
        List<String> warnings = logsOf(Level.WARN, () -> Dse6NonCoreTraces.verify(new Traces(), true));
        assertThat(warnings).containsExactly(OVERRIDE_KEY + "=true is set: a DSE workload other than Cassandra recorded for a peer in " +
                                             "nodes/peers would not refuse the start of this node (none is recorded)");
        assertThat(Dse6NonCoreTraces.status()).isEqualTo("none (" + OVERRIDE_KEY + "=true is set)");
        // without it, no warning
        assertThat(logsOf(Level.WARN, () -> Dse6NonCoreTraces.verify(new Traces(), false))).isEmpty();
    }

    @Test
    public void theOverrideDoesNotCoverTheOtherTraces() throws Throwable
    {
        Traces traces = keyspaces("dse_advrep");
        traces.add(Kind.PEER_ENTRY, "peer 10.0.0.2 (nodes/peers): workloads [Analytics]");
        String[] refused = new String[1];
        List<String> warnings = logsOf(Level.WARN, () -> refused[0] = refusal(traces, true));
        // refused on the keyspace alone, the peer entry is only warned about
        assertThat(refused[0]).isEqualTo(FOUND + "DSE Advanced Replication: keyspace dse_advrep" + REMEDY_PREFIX + "stop Advanced " +
                                         "Replication, migrate what its keyspace holds if it is needed and drop it, empty or not" + REMEDY_SUFFIX);
        assertThat(warnings).hasSize(1);
        assertThat(warnings.get(0)).contains("peer 10.0.0.2 (nodes/peers): workloads [Analytics]");
    }

    @Test
    public void oneRefusalListsEveryTraceGroupedByFeature() throws Exception
    {
        Traces traces = new Traces();
        Dse6NonCoreTraces.addWorkloadTraces(traces,
                                            new DseNodeWorkloads(null, List.of("Search", "Cassandra"), false, "Search"),
                                            List.of(new DseNodeWorkloads(InetAddress.getByName("10.0.0.2"), List.of("Cassandra"), false, "Cassandra"),
                                                    new DseNodeWorkloads(InetAddress.getByName("10.0.0.3"), List.of("Analytics", "Cassandra"), true, "Analytics"),
                                                    new DseNodeWorkloads(null, List.of("Graph"), null, null)));
        Map<String, String> keyspaces = new TreeMap<>();
        keyspaces.put("dse_analytics", null);
        keyspaces.put("dse_advrep", null);
        keyspaces.put("g", "Core");
        keyspaces.put("solr_admin", null);
        keyspaces.put("dse_system", null);
        Dse6NonCoreTraces.addKeyspaceTraces(traces, keyspaces);
        traces.add(Kind.SEARCH_INDEX, "index ks.t.t_solr_query_index (" + SOLR_INDEX + ')');
        traces.add(Kind.SOLR_DATA_DIRECTORY, "directory /var/lib/cassandra/data/solr.data");

        assertThat(refusal(traces, false))
        .isEqualTo(FOUND + "DSE workloads other than Cassandra: this node (nodes/local): workloads [Search, Cassandra], workload Search, " +
                   "peer 10.0.0.3 (nodes/peers): workloads [Analytics, Cassandra], graph true, workload Analytics, peer (no address) " +
                   "(nodes/peers): workloads [Graph]; DSE Search: index ks.t.t_solr_query_index (" + SOLR_INDEX + "), directory " +
                   "/var/lib/cassandra/data/solr.data; DSE Graph: keyspace g (graph_engine Core); DSE Analytics, DSEFS and CFS: keyspace " +
                   "dse_analytics; DSE Advanced Replication: keyspace dse_advrep" + REMEDY_PREFIX +
                   "restart this node as a plain Cassandra node (dse cassandra without -s, -k or -g; SOLR_ENABLED, SPARK_ENABLED and " +
                   "GRAPH_ENABLED set to 0 in /etc/default/dse for a package install); restart the peers listed as plain Cassandra " +
                   "nodes while this node runs, so that its nodes/peers records their new workloads; drop the search indexes (DROP " +
                   "SEARCH INDEX ON <keyspace>.<table>); delete the solr.data directories once no search index is left; migrate the " +
                   "data of the graph keyspaces elsewhere and drop them; migrate the data of the Analytics, DSEFS and CFS keyspaces " +
                   "elsewhere if it is needed and drop them, empty or not; stop Advanced Replication, migrate what its keyspace holds " +
                   "if it is needed and drop it, empty or not" + REMEDY_SUFFIX + PEER_ADVICE);
    }

    @Test
    public void everyTraceIsListed()
    {
        Traces traces = new Traces();
        List<String> entries = new ArrayList<>();
        for (int i = 0; i < 40; i++)
        {
            entries.add("peer 10.0.0." + i + " (nodes/peers): workloads [Search]");
            traces.add(Kind.PEER_ENTRY, entries.get(i));
        }
        assertThat(refusal(traces, false)).isEqualTo(FOUND + "DSE workloads other than Cassandra: " + String.join(", ", entries) +
                                                     REMEDY_PREFIX + "restart the peers listed as plain Cassandra nodes while this " +
                                                     "node runs, so that its nodes/peers records their new workloads" + REMEDY_SUFFIX +
                                                     PEER_ADVICE);
    }

    @Test
    public void anUnknownWorkloadIsNotATrace() throws Exception
    {
        // DSE's placeholder for a workload it does not know yet, as it may keep it for a peer that gossips no DSE workload
        // (a node already upgraded to this software): not a feature, and not a trace
        Traces traces = new Traces();
        Dse6NonCoreTraces.addWorkloadTraces(traces,
                                            new DseNodeWorkloads(null, List.of("Unknown"), null, "Unknown"),
                                            List.of(new DseNodeWorkloads(InetAddress.getByName("10.0.0.2"), List.of("Unknown"), false, "Unknown"),
                                                    new DseNodeWorkloads(InetAddress.getByName("10.0.0.3"), List.of(), null, null),
                                                    new DseNodeWorkloads(InetAddress.getByName("10.0.0.4"), null, null, null),
                                                    new DseNodeWorkloads(InetAddress.getByName("10.0.0.5"), List.of("Cassandra", "Unknown"), false, "Cassandra")));
        assertThat(traces.all).isEmpty();
        // next to another workload, the other one is the trace
        Dse6NonCoreTraces.addWorkloadTraces(traces, null, List.of(new DseNodeWorkloads(InetAddress.getByName("10.0.0.6"), List.of("Unknown", "Search"), false, "Unknown")));
        assertThat(descriptions(traces)).containsExactly("WORKLOADS peer 10.0.0.6 (nodes/peers): workloads [Unknown, Search]");
    }

    // ---- the whole check ----

    @Test
    public void theWholeCheckRefusesAKeyspaceAGraphEngineAndAPeerEntry() throws Throwable
    {
        // created without the mode: in DSE_6 mode the schema is frozen
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.NONE);
        schemaChange("CREATE KEYSPACE IF NOT EXISTS dse_advrep WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
        String graph = createKeyspace("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
        try
        {
            // DSE stores graph_engine in system_schema.keyspaces, which this software reads but never writes
            executeOnceInternal("UPDATE system_schema.keyspaces SET graph_engine = 'Core' WHERE keyspace_name = ?", graph);

            // an empty solr.data in the first data directory of this node
            File solrData = new File(DatabaseDescriptor.getAllDataFileLocations()[0], "solr.data");
            solrData.createDirectoriesIfNotExists();
            try
            {
                DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
                // a check that passed before: its status goes with the refusal
                Dse6NonCoreTraces.verify(new Traces(), false);
                assertThat(Dse6NonCoreTraces.status()).isEqualTo("none");
                editFirstPeer(DseVersion.DSE_6_9_26, peer -> peer.put("workloads", List.of("Graph")));
                assertThatThrownBy(Dse6NonCoreTraces::verifyAtStartup)
                .isInstanceOfSatisfying(StartupException.class, e -> assertThat(e.returnCode).isEqualTo(StartupException.ERR_WRONG_CONFIG))
                .hasMessageStartingWith(FOUND + "DSE workloads other than Cassandra: peer 127.0.0.2 (nodes/peers): workloads [Graph]; DSE Search: " +
                                        "directory " + solrData + "; DSE Graph: keyspace " + graph + " (graph_engine Core); DSE Advanced " +
                                        "Replication: keyspace dse_advrep. ");
                assertThat(Dse6NonCoreTraces.status()).isNull();

                // the override leaves the peer entry out of the refusal only
                try (WithProperties override = new WithProperties().set(INTERNODE_COMPATIBILITY_MODE_IGNORE_DSE_PEER_WORKLOADS, true))
                {
                    assertThatThrownBy(Dse6NonCoreTraces::verifyAtStartup)
                    .hasMessageStartingWith(FOUND + "DSE Search: directory " + solrData + "; DSE Graph: keyspace " + graph +
                                            " (graph_engine Core); DSE Advanced Replication: keyspace dse_advrep. ");
                }
            }
            finally
            {
                solrData.deleteRecursive();
            }

            // once the keyspaces are gone (and the graph engine with its keyspace), only the peer entry is left
            DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.NONE);
            schemaChange("DROP KEYSPACE dse_advrep");
            executeOnceInternal("UPDATE system_schema.keyspaces SET graph_engine = null WHERE keyspace_name = ?", graph);
            DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
            assertThatThrownBy(Dse6NonCoreTraces::verifyAtStartup)
            .hasMessage(FOUND + "DSE workloads other than Cassandra: peer 127.0.0.2 (nodes/peers): workloads [Graph]" + REMEDY_PREFIX +
                        "restart the peers listed as plain Cassandra nodes while this node runs, so that its nodes/peers records " +
                        "their new workloads" + REMEDY_SUFFIX + PEER_ADVICE);
            try (WithProperties override = new WithProperties().set(INTERNODE_COMPATIBILITY_MODE_IGNORE_DSE_PEER_WORKLOADS, true))
            {
                assertThat(logsOf(Level.INFO, Dse6NonCoreTraces::verifyAtStartup))
                .containsExactly(NO_TRACE + " apart from what of nodes/peers is ignored");
            }
            assertThat(Dse6NonCoreTraces.status()).isEqualTo("none but nodes/peers ignored (" + OVERRIDE_KEY +
                                                             "=true): peer 127.0.0.2 (nodes/peers): workloads [Graph]");

            // with the fixture as DSE wrote it, the node starts
            DseFixtures.installNodesDirectory(DatabaseDescriptor.getMetadataDirectory().toPath(), DseVersion.DSE_6_9_26, false);
            assertThat(logsOf(Level.INFO, Dse6NonCoreTraces::verifyAtStartup)).containsExactly(NO_TRACE);
            assertThat(Dse6NonCoreTraces.status()).isEqualTo("none");
        }
        finally
        {
            DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.NONE);
            schemaChange("DROP KEYSPACE IF EXISTS dse_advrep");
        }
    }

    @Test
    public void unreadableNodesFilesAreListedWithTheOtherTraces() throws Throwable
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.NONE);
        schemaChange("CREATE KEYSPACE IF NOT EXISTS dse_advrep WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
        try
        {
            DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
            Path nodes = DseFixtures.installNodesDirectory(DatabaseDescriptor.getMetadataDirectory().toPath(), DseVersion.DSE_6_9_26, false);
            Files.write(nodes.resolve("peers"), new byte[]{ (byte) 0x91, (byte) 0x85, 0x01 });
            String readable = "Make nodes/peers readable: the files DSE wrote in the metadata directory, left in place in DSE_6 mode (restore " +
                              "them from the copy of the nodes directory made before the upgrade if they are damaged); -D" + OVERRIDE_KEY + "=true leaves nodes/peers unchecked";
            // the collection goes on after the unreadable file: one refusal lists the keyspace too
            assertThatThrownBy(Dse6NonCoreTraces::verifyAtStartup)
            .isInstanceOfSatisfying(StartupException.class, e -> assertThat(e.returnCode).isEqualTo(StartupException.ERR_WRONG_DISK_STATE))
            .hasMessageStartingWith(FOUND + "DSE Advanced Replication: keyspace dse_advrep; DSE files that cannot be read: nodes/peers " +
                                    "(java.lang.RuntimeException: Failed to read the DSE workloads from the node metadata file " + nodes.resolve("peers") +
                                    "), so the workloads of the peers are unknown" + REMEDY_PREFIX + "stop Advanced Replication, " +
                                    "migrate what its keyspace holds if it is needed and drop it, empty or not" + REMEDY_SUFFIX + ". " + readable)
            .hasCauseInstanceOf(RuntimeException.class);

            // the override leaves nodes/peers unchecked, with a warning of its own, but not the keyspace
            try (WithProperties override = new WithProperties().set(INTERNODE_COMPATIBILITY_MODE_IGNORE_DSE_PEER_WORKLOADS, true))
            {
                String[] refused = new String[1];
                List<ILoggingEvent> warnings = new ArrayList<>();
                withLogAppender(Dse6NonCoreTraces.class, appender -> {
                    try
                    {
                        Dse6NonCoreTraces.verifyAtStartup();
                    }
                    catch (StartupException e)
                    {
                        refused[0] = e.getMessage();
                    }
                    appender.list.stream().filter(e -> e.getLevel() == Level.WARN).forEach(warnings::add);
                });
                assertThat(refused[0]).isEqualTo(FOUND + "DSE Advanced Replication: keyspace dse_advrep" + REMEDY_PREFIX + "stop Advanced " +
                                                 "Replication, migrate what its keyspace holds if it is needed and drop it, empty or not" + REMEDY_SUFFIX);
                assertThat(warnings).hasSize(1);
                assertThat(warnings.get(0).getFormattedMessage())
                .isEqualTo("DSE's nodes/peers cannot be read and is left unchecked (" + OVERRIDE_KEY + "=true): a DSE workload other than " +
                           "Cassandra recorded for a peer would not refuse the start of this node");
                // with the exception that says why
                assertThat(warnings.get(0).getThrowableProxy()).isNotNull();
                assertThat(warnings.get(0).getThrowableProxy().getMessage()).isEqualTo("Failed to read the DSE workloads from the node metadata file " + nodes.resolve("peers"));

                DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.NONE);
                schemaChange("DROP KEYSPACE dse_advrep");
                DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
                assertThat(logsOf(Level.INFO, Dse6NonCoreTraces::verifyAtStartup)).containsExactly(NO_TRACE + " apart from what of nodes/peers is ignored");
                assertThat(Dse6NonCoreTraces.status()).isEqualTo("none but nodes/peers ignored (" + OVERRIDE_KEY + "=true): nodes/peers cannot " +
                                                                 "be read (java.lang.RuntimeException: Failed to read the DSE workloads from the " +
                                                                 "node metadata file " + nodes.resolve("peers") + ")");

                // nodes/local is this node's own record: the override does not cover it
                Files.write(nodes.resolve("local"), new byte[]{ (byte) 0x85, 0x01 });
                assertThatThrownBy(Dse6NonCoreTraces::verifyAtStartup)
                .isInstanceOfSatisfying(StartupException.class, e -> assertThat(e.returnCode).isEqualTo(StartupException.ERR_WRONG_DISK_STATE))
                .hasMessage("internode_compatibility_mode DSE_6 refused: cannot check for traces of DSE features that this software does " +
                            "not support; only DSE core (the Cassandra workload) can be upgraded. DSE files that cannot be read: nodes/local " +
                            "(java.lang.RuntimeException: Failed to read the DSE workloads from the node metadata file " + nodes.resolve("local") +
                            "), so the workloads of this node are unknown. Make nodes/local readable: the files DSE wrote in the metadata directory, left in place in DSE_6 mode " +
                            "(restore them from the copy of the nodes directory made before the upgrade if they are damaged)");
            }
        }
        finally
        {
            DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.NONE);
            schemaChange("DROP KEYSPACE IF EXISTS dse_advrep");
        }
    }

    @Test
    public void noneModeReadsAndChecksNothing() throws Throwable
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.NONE);
        schemaChange("CREATE KEYSPACE IF NOT EXISTS dse_analytics WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
        try
        {
            // a Search workload in nodes/local, an unreadable nodes/peers, a keyspace of DSE Analytics and a solr.data
            // directory would each refuse the start in DSE_6 mode
            Path nodes = editNodesLocal(DseVersion.DSE_6_9_26, local -> local.put("workloads", List.of("Search")));
            Files.write(nodes.resolve("peers"), new byte[]{ (byte) 0x91, (byte) 0x85, 0x01 });
            File solrData = new File(DatabaseDescriptor.getAllDataFileLocations()[0], "solr.data");
            solrData.createDirectoriesIfNotExists();
            try
            {
                // nothing is logged, at any level, and nothing is recorded
                assertThat(logsOf(null, Dse6NonCoreTraces::verifyAtStartup)).isEmpty();
                assertThat(Dse6NonCoreTraces.status()).isNull();
            }
            finally
            {
                solrData.deleteRecursive();
            }
        }
        finally
        {
            schemaChange("DROP KEYSPACE IF EXISTS dse_analytics");
        }
    }

    @Test
    public void theSchemaOfThisTestNodeHasNoTrace() throws Throwable
    {
        // the keyspaces this software creates, and no nodes directory: the check passes in DSE_6 mode
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        assertThat(logsOf(Level.INFO, Dse6NonCoreTraces::verifyAtStartup)).containsExactly(NO_TRACE);
        assertThat(Dse6NonCoreTraces.status()).isEqualTo("none");
    }

    // ---- DSE 5.1 ----

    /**
     * The values DSE 5.1.51 stored for the node started with Search and Graph, as its {@code system.local} holds them
     * ({@code select-local-peers.txt} of the fixtures, checked here), recorded as this node records them at its first
     * start in {@code DSE_5_1}, for the node itself ({@code peer} null) or for a peer.
     */
    private static Dse51SystemTables.StoredColumns searchGraphColumns(InetAddress peer) throws Exception
    {
        List<String> select = Files.readAllLines(DseFixtures.dse51().searchGraphSystemSelect());
        assertThat(select).anyMatch(line -> line.matches(" graph +\\| True"))
                          .anyMatch(line -> line.matches(" workload +\\| Search"))
                          .anyMatch(line -> line.matches(" workloads +\\| \\{'Cassandra', 'Graph', 'Search'}"));
        return Dse51SystemTablesRecords.workloads(peer, List.of("Cassandra", "Graph", "Search"), "Search", true,
                                                  Dse51Fixtures.DSE_VERSION);
    }

    /** The values DSE 5.1.51 stored for a node of the Cassandra workload alone, as the three-node cluster has them. */
    private static Dse51SystemTables.StoredColumns cassandraColumns()
    {
        return Dse51SystemTablesRecords.workloads(null, List.of("Cassandra"), "Cassandra", false,
                                                  Dse51Fixtures.DSE_VERSION);
    }

    /**
     * Loads the record of the values of DSE 5.1's columns into {@code Dse51SystemTables.stored()}, as a start in
     * DSE_5_1 does.
     */
    private static void loadStored(Dse51SystemTables.StoredColumns... rows) throws Exception
    {
        Dse51SystemTablesRecords.write(rows);
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_5_1);
        Dse51SystemTables.loadOrReadAtStart();
    }

    /** Empties {@code Dse51SystemTables.stored()} again, as a start outside DSE_5_1 does, and deletes the record. */
    private static void clearStored() throws Exception
    {
        InternodeCompatibilityMode mode = DatabaseDescriptor.getInternodeCompatibilityMode();
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.NONE);
        Dse51SystemTables.loadOrReadAtStart();
        DatabaseDescriptor.setInternodeCompatibilityMode(mode);
        Files.deleteIfExists(Dse51SystemTables.recordPath());
    }

    /**
     * The workloads DSE 5.1.51 stored for the Search and Graph node: in its own {@code system.local} they refuse its
     * start; in the {@code system.peers} of another node they refuse that node's start too, unless the override
     * ignores the entries of the peers, with a warning. The values are those of the fixture, read once at the first
     * start in the mode and kept, as the texts say.
     */
    @Test
    public void dse51TheWorkloadsDse51StoredAreTraces() throws Throwable
    {
        try
        {
            loadStored(searchGraphColumns(null));
            assertThatThrownBy(Dse6NonCoreTraces::verifyDse51AtStartup)
            .isInstanceOfSatisfying(StartupException.class,
                                    e -> assertThat(e.returnCode).isEqualTo(StartupException.ERR_WRONG_CONFIG))
            .hasMessage(FOUND_DSE51 + "DSE workloads other than Cassandra: this node (system.local): " +
                        SEARCH_GRAPH_WORKLOADS + REMEDY_PREFIX + "restart this node as a plain Cassandra node (dse " +
                        "cassandra without -s, -k, -c or -g; for a package install, every workload of " +
                        "/etc/default/dse set to 0)" + REMEDY_SUFFIX + ". " + rollbackDeletes());
            assertThat(Dse6NonCoreTraces.status()).isNull();
            // the next start in the mode loads the record of the first one, with the same workloads, until the
            // record is deleted (the rollback deletes it with the marker of the upgrade): then they are read again
            // (here from the system tables of this test node, which hold none)
            Dse51SystemTables.loadOrReadAtStart();
            assertThatThrownBy(Dse6NonCoreTraces::verifyDse51AtStartup).hasMessageContaining(SEARCH_GRAPH_WORKLOADS);
            Files.delete(Dse51SystemTables.recordPath());
            Dse51SystemTables.loadOrReadAtStart();
            assertThat(logsOf(Level.INFO, Dse6NonCoreTraces::verifyDse51AtStartup)).containsExactly(NO_TRACE_DSE51);

            loadStored(cassandraColumns(), searchGraphColumns(InetAddress.getByName(SEARCH_GRAPH_NODE)));
            String entry = "peer " + SEARCH_GRAPH_NODE + " (system.peers): " + SEARCH_GRAPH_WORKLOADS;
            assertThatThrownBy(Dse6NonCoreTraces::verifyDse51AtStartup)
            .hasMessage(FOUND_DSE51 + "DSE workloads other than Cassandra: " + entry + REMEDY_PREFIX + "restart the " +
                        "peers listed as plain Cassandra nodes while this node runs DSE 5.1, so that its " +
                        "system.peers records their new workloads" + REMEDY_SUFFIX + ". " + rollbackDeletes() +
                        ". An entry of system.peers is " +
                        "stale when that node has since been restarted as a Cassandra-only node or has left the " +
                        "cluster: if every entry listed from system.peers is stale, start with -D" + OVERRIDE_KEY +
                        "=true instead (it warns at every start; this node keeps the values it read at its first " +
                        "start in DSE_5_1 mode, so keep the option until the flag drop)");
            try (WithProperties override = new WithProperties()
                                           .set(INTERNODE_COMPATIBILITY_MODE_IGNORE_DSE_PEER_WORKLOADS, true))
            {
                List<String> logged = logsOf(null, Dse6NonCoreTraces::verifyDse51AtStartup);
                assertThat(logged).containsExactly("The DSE workloads other than Cassandra recorded for peers in " +
                                                   "system.peers are ignored (" + OVERRIDE_KEY + "=true): " + entry +
                                                   "; this node keeps the values it read at its first start in " +
                                                   "DSE_5_1 mode, and supports none of these workloads: remove the " +
                                                   "option unless each of these entries is stale (the node was since " +
                                                   "restarted as a Cassandra-only node, or has left the cluster)",
                                                   NO_TRACE_DSE51 + " apart from what of system.peers is ignored");
                assertThat(Dse6NonCoreTraces.status())
                .isEqualTo("none but system.peers ignored (" + OVERRIDE_KEY + "=true): " + entry);
            }

            // the Cassandra workload alone, as the three-node cluster of the fixtures stored it: no trace
            loadStored(cassandraColumns());
            assertThat(logsOf(Level.INFO, Dse6NonCoreTraces::verifyDse51AtStartup)).containsExactly(NO_TRACE_DSE51);
            assertThat(Dse6NonCoreTraces.status()).isEqualTo("none");
        }
        finally
        {
            clearStored();
        }
    }

    /**
     * The traces DSE 5.1 leaves in the schema, read from the rows of {@code system_schema} as stored, before the schema
     * is loaded: the keyspaces of DSE 6's list, {@code spark_system} and {@code solr_admin} (which only DSE Search
     * creates on DSE 5.1), the three keyspaces of a graph, an index of a DSE Search class, a {@code solr.data}
     * directory and the compaction classes of DSE 5.1's tiered storage and in-memory tables, which this software
     * cannot load; the rows of the index and of the compaction are written directly to {@code system_schema}, as DSE
     * wrote them, since this software would refuse their statements. Without {@code solr_admin}, its remedy is left
     * out.
     */
    @Test
    public void dse51TheTracesInTheSchemaRowsAreRefused() throws Throwable
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.NONE);
        String simple = " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}";
        String graph = createKeyspace("CREATE KEYSPACE %s" + simple);
        List<String> created = List.of("\"HiveMetaStore\"", "cfs", "cfs_archive", "dsefs", "spark_system", "dse_advrep",
                                       "solr_admin", graph + "_system", graph + "_pvt");
        for (String keyspace : created)
            schemaChange("CREATE KEYSPACE IF NOT EXISTS " + keyspace + simple);
        String tiered = createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        String inMemory = createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        String tieredClass = "org.apache.cassandra.db.compaction.TieredCompactionStrategy";
        File solrData = new File(DatabaseDescriptor.getAllDataFileLocations()[0], "solr.data");
        solrData.createDirectoriesIfNotExists();
        try
        {
            executeOnceInternal("UPDATE system_schema.tables SET compaction = {'class': '" + tieredClass + "', " +
                                "'config': 'strategy1'} WHERE keyspace_name = ? AND table_name = ?", KEYSPACE, tiered);
            executeOnceInternal("UPDATE system_schema.tables SET compaction = {'class': 'MemoryOnlyStrategy'} " +
                                "WHERE keyspace_name = ? AND table_name = ?", KEYSPACE, inMemory);
            executeOnceInternal("INSERT INTO system_schema.indexes (keyspace_name, table_name, index_name, kind, " +
                                "options) VALUES (?, ?, ?, 'CUSTOM', ?)", KEYSPACE, tiered, "solr_query_index",
                                Map.of(IndexTarget.CUSTOM_INDEX_OPTION_NAME, SOLR_INDEX, "target", "solr_query"));
            DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_5_1);
            String index = "index " + KEYSPACE + '.' + tiered + ".solr_query_index (" + SOLR_INDEX + ")";
            String graphs = "DSE Graph: keyspace " + graph + " (a keyspace of the graph " + graph + "), keyspace " +
                            graph + "_pvt (a keyspace of the graph " + graph + "), keyspace " + graph + "_system (a " +
                            "keyspace of the graph " + graph + "); DSE Analytics, DSEFS and CFS: keyspace " +
                            "HiveMetaStore, keyspace cfs, keyspace cfs_archive, keyspace dsefs, keyspace " +
                            "spark_system; DSE Advanced Replication: keyspace dse_advrep; DSE tiered storage and " +
                            "in-memory tables: table " +
                            KEYSPACE + '.' + tiered + " (compaction " + tieredClass + "), table " + KEYSPACE + '.' +
                            inMemory + " (compaction MemoryOnlyStrategy)";
            String otherRemedies = "migrate the data of the graph keyspaces elsewhere and drop them; migrate the " +
                                   "data of the Analytics, DSEFS and CFS keyspaces elsewhere if it is needed and " +
                                   "drop them, empty or not; stop Advanced Replication, migrate what its keyspace " +
                                   "holds if it is needed and drop it, empty or not; alter the tables and " +
                                   "materialized views listed to a compaction strategy of Apache Cassandra (ALTER " +
                                   "TABLE or ALTER MATERIALIZED VIEW ... WITH compaction)";
            assertThatThrownBy(Dse6NonCoreTraces::verifyDse51AtStartup)
            .hasMessage(FOUND_DSE51 + "DSE Search: keyspace solr_admin, " + index + ", directory " + solrData + "; " +
                        graphs + ALTER_REMEDY_PREFIX + "drop the search indexes (DROP SEARCH INDEX ON " +
                        "<keyspace>.<table>); delete the solr.data directories once no search index is left; drop " +
                        "the keyspace solr_admin once no node runs DSE Search; " + otherRemedies + REMEDY_SUFFIX);
            // the markers of the preflight tool, fed the same rows, find the same traces, grouped the same way
            assertThat(offlineMarkers()).isEqualTo("DSE Search: keyspace solr_admin, " + index + ", directory " +
                                                   solrData + "; " + graphs);

            // without solr_admin, the search index is no reason to drop it
            DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.NONE);
            schemaChange("DROP KEYSPACE solr_admin");
            DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_5_1);
            assertThatThrownBy(Dse6NonCoreTraces::verifyDse51AtStartup)
            .hasMessage(FOUND_DSE51 + "DSE Search: " + index + ", directory " + solrData + "; " + graphs +
                        ALTER_REMEDY_PREFIX + "drop the search indexes (DROP SEARCH INDEX ON <keyspace>.<table>); " +
                        "delete the solr.data directories once no search index is left; " + otherRemedies +
                        REMEDY_SUFFIX);

            // the same keyspaces and rows are not read with NONE or DSE_6, whose check reads the loaded schema
            for (InternodeCompatibilityMode mode : List.of(InternodeCompatibilityMode.NONE,
                                                           InternodeCompatibilityMode.DSE_6))
            {
                DatabaseDescriptor.setInternodeCompatibilityMode(mode);
                assertThat(logsOf(null, Dse6NonCoreTraces::verifyDse51AtStartup)).as("%s", mode).isEmpty();
            }
        }
        finally
        {
            solrData.deleteRecursive();
            DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.NONE);
            executeOnceInternal("DELETE FROM system_schema.indexes WHERE keyspace_name = ? AND table_name = ? AND " +
                                "index_name = ?", KEYSPACE, tiered, "solr_query_index");
            schemaChange("DROP TABLE " + KEYSPACE + '.' + tiered);
            schemaChange("DROP TABLE " + KEYSPACE + '.' + inMemory);
            for (String keyspace : created)
                schemaChange("DROP KEYSPACE IF EXISTS " + keyspace);
        }
    }

    /**
     * The traces {@link Dse6NonCoreTraces.Dse51Markers} finds in the rows of {@code system_schema} of this node and its
     * data directories, fed one by one as the preflight tool feeds what it read offline, joined as the refusal of the
     * start check joins its groups.
     */
    private static String offlineMarkers()
    {
        Dse6NonCoreTraces.Dse51Markers markers = new Dse6NonCoreTraces.Dse51Markers();
        Map<String, String> keyspaces = new TreeMap<>();
        String keyspaceRows = "SELECT keyspace_name, replication FROM system_schema.keyspaces";
        for (UntypedResultSet.Row row : executeOnceInternal(keyspaceRows))
            keyspaces.put(row.getString("keyspace_name"), row.getFrozenTextMap("replication").get("class"));
        markers.keyspaces(keyspaces);
        for (UntypedResultSet.Row row : executeOnceInternal("SELECT * FROM system_schema.indexes"))
            markers.index(row.getString("keyspace_name"), row.getString("table_name"), row.getString("index_name"),
                          row.getFrozenTextMap("options").get(IndexTarget.CUSTOM_INDEX_OPTION_NAME));
        for (UntypedResultSet.Row row : executeOnceInternal("SELECT * FROM system_schema.tables"))
            markers.table(row.getString("keyspace_name"), row.getString("table_name"),
                          row.getFrozenTextMap("compaction").get("class"));
        for (UntypedResultSet.Row row : executeOnceInternal("SELECT * FROM system_schema.views"))
            markers.view(row.getString("keyspace_name"), row.getString("view_name"),
                         row.getFrozenTextMap("compaction").get("class"));
        markers.solrData(DatabaseDescriptor.getAllDataFileLocations());
        return markers.byFeature()
                      .entrySet()
                      .stream()
                      .map(e -> e.getKey().displayName() + ": " + String.join(", ", e.getValue()))
                      .collect(Collectors.joining("; "));
    }

    /**
     * A table and a materialized view compacted by {@code DateTieredCompactionStrategy}, which DSE 5.1.51 has and this
     * software no longer has, are refused in a group of their own with the remedy, before the schema is loaded; the
     * compaction of the materialized views is read for the DSE classes too. The rows are written directly to
     * {@code system_schema}, as DSE wrote them. With {@code NONE} and {@code DSE_6} nothing of it is read: the check of
     * {@code DSE_6} ({@link Dse6NonCoreTraces#verifyAtStartup}) reads the loaded schema only, not these rows.
     */
    @Test
    public void dse51TablesAndViewsOnACompactionStrategyThisSoftwareNoLongerHasAreRefused() throws Throwable
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.NONE);
        // the drop of a view records it in system_distributed, which this test node does not create
        String distributed = SchemaConstants.DISTRIBUTED_KEYSPACE_NAME;
        boolean distributedAdded = Schema.instance.getKeyspaceMetadata(distributed) == null;
        if (distributedAdded)
            SchemaTestUtil.addOrUpdateKeyspace(SystemDistributedKeyspace.metadata(), true);
        String table = createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        // the build of the view is not waited for: its rows do not matter here
        String view = createViewAsync("CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE k IS NOT NULL AND v IS " +
                                      "NOT NULL PRIMARY KEY (v, k)");
        String dateTiered = "org.apache.cassandra.db.compaction.DateTieredCompactionStrategy";
        String removedRemedy = "alter the tables and materialized views compacted by DateTieredCompactionStrategy to " +
                               "TimeWindowCompactionStrategy (ALTER TABLE or ALTER MATERIALIZED VIEW ... WITH " +
                               "compaction = {'class': 'TimeWindowCompactionStrategy', ...})";
        try
        {
            executeOnceInternal("UPDATE system_schema.tables SET compaction = {'class': '" + dateTiered + "'} " +
                                "WHERE keyspace_name = ? AND table_name = ?", KEYSPACE, table);
            executeOnceInternal("UPDATE system_schema.views SET compaction = " +
                                "{'class': 'DateTieredCompactionStrategy'} WHERE keyspace_name = ? AND view_name = ?",
                                KEYSPACE, view);
            DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_5_1);
            String removed = "tables or materialized views on a compaction strategy this software no longer has: " +
                             "table " + KEYSPACE + '.' + table + " (compaction " + dateTiered + "), materialized " +
                             "view " + KEYSPACE + '.' + view + " (compaction DateTieredCompactionStrategy)";
            assertThatThrownBy(Dse6NonCoreTraces::verifyDse51AtStartup)
            .isInstanceOf(StartupException.class)
            .hasMessage("internode_compatibility_mode DSE_5_1 refused: found " + removed + ONLY_ALTER_REMEDY_PREFIX +
                        removedRemedy + REMEDY_SUFFIX)
            .satisfies(e -> assertThat(((StartupException) e).returnCode).isEqualTo(StartupException.ERR_WRONG_CONFIG));
            assertThat(Dse6NonCoreTraces.status()).isNull();

            // a view on a DSE class of tiered storage or in-memory tables is refused as a table is, the strategy this
            // software no longer has in its group, after the DSE features
            DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.NONE);
            executeOnceInternal("UPDATE system_schema.tables SET compaction = {'class': 'MemoryOnlyStrategy'} " +
                                "WHERE keyspace_name = ? AND table_name = ?", KEYSPACE, table);
            String tieredClass = "org.apache.cassandra.db.compaction.TieredCompactionStrategy";
            executeOnceInternal("UPDATE system_schema.views SET compaction = {'class': '" + tieredClass + "'} " +
                                "WHERE keyspace_name = ? AND view_name = ?", KEYSPACE, view);
            DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_5_1);
            String dseRemedy = "alter the tables and materialized views listed to a compaction strategy of Apache " +
                               "Cassandra (ALTER TABLE or ALTER MATERIALIZED VIEW ... WITH compaction)";
            assertThatThrownBy(Dse6NonCoreTraces::verifyDse51AtStartup)
            .hasMessage(FOUND_DSE51 + "DSE tiered storage and in-memory tables: table " + KEYSPACE + '.' + table +
                        " (compaction MemoryOnlyStrategy), materialized view " + KEYSPACE + '.' + view +
                        " (compaction " + tieredClass + ')' + ONLY_ALTER_REMEDY_PREFIX + dseRemedy + REMEDY_SUFFIX);

            DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.NONE);
            executeOnceInternal("UPDATE system_schema.views SET compaction = {'class': 'MemoryOnlyStrategy'} " +
                                "WHERE keyspace_name = ? AND view_name = ?", KEYSPACE, view);
            executeOnceInternal("UPDATE system_schema.tables SET compaction = {'class': '" + dateTiered + "'} " +
                                "WHERE keyspace_name = ? AND table_name = ?", KEYSPACE, table);
            DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_5_1);
            assertThatThrownBy(Dse6NonCoreTraces::verifyDse51AtStartup)
            .hasMessage(FOUND_DSE51 + "DSE tiered storage and in-memory tables: materialized view " + KEYSPACE + '.' +
                        view + " (compaction MemoryOnlyStrategy). Also found tables or materialized views on a " +
                        "compaction strategy this software no longer has: table " + KEYSPACE + '.' + table +
                        " (compaction " + dateTiered + ')' + ONLY_ALTER_REMEDY_PREFIX + dseRemedy + "; " +
                        removedRemedy +
                        REMEDY_SUFFIX);
            // the markers of the preflight tool, fed the same rows, find the same traces in the same groups
            assertThat(offlineMarkers()).isEqualTo("DSE tiered storage and in-memory tables: materialized view " +
                                                   KEYSPACE + '.' + view + " (compaction MemoryOnlyStrategy); a " +
                                                   "compaction strategy this software no longer has: table " +
                                                   KEYSPACE + '.' + table + " (compaction " + dateTiered + ')');

            // the rows are not read with NONE or DSE_6
            for (InternodeCompatibilityMode mode : List.of(InternodeCompatibilityMode.NONE,
                                                           InternodeCompatibilityMode.DSE_6))
            {
                DatabaseDescriptor.setInternodeCompatibilityMode(mode);
                assertThat(logsOf(null, Dse6NonCoreTraces::verifyDse51AtStartup)).as("%s", mode).isEmpty();
            }
        }
        finally
        {
            DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.NONE);
            Dse6NonCoreTraces.resetStatus();
            // dropped here, so that no row the test changed is left behind for the next test
            dropView(view);
            schemaChange("DROP TABLE " + KEYSPACE + '.' + table);
            if (distributedAdded)
                SchemaTestUtil.dropKeyspaceIfExist(distributed, true);
        }
    }

    /** The compaction classes the check reads, by their long and their short names, and those it does not refuse. */
    @Test
    public void dse51CompactionTracesByClass()
    {
        Dse6NonCoreTraces.Traces traces = new Dse6NonCoreTraces.Traces();
        Dse6NonCoreTraces.addCompactionTrace(traces, "table ks.a", "DateTieredCompactionStrategy");
        Dse6NonCoreTraces.addCompactionTrace(traces, "materialized view ks.b",
                                             "org.apache.cassandra.db.compaction.DateTieredCompactionStrategy");
        Dse6NonCoreTraces.addCompactionTrace(traces, "table ks.c", "TieredCompactionStrategy");
        Dse6NonCoreTraces.addCompactionTrace(traces, "table ks.d", "TimeWindowCompactionStrategy");
        Dse6NonCoreTraces.addCompactionTrace(traces, "table ks.e", "SizeTieredCompactionStrategy");
        Dse6NonCoreTraces.addCompactionTrace(traces, "table ks.f", null);
        assertThat(traces.all).extracting(t -> t.kind + " " + t.description)
                              .containsExactly("REMOVED_COMPACTION table ks.a (compaction " +
                                               "DateTieredCompactionStrategy)",
                                               "REMOVED_COMPACTION materialized view ks.b (compaction " +
                                               "org.apache.cassandra.db.compaction.DateTieredCompactionStrategy)",
                                               "TABLE_COMPACTION table ks.c (compaction TieredCompactionStrategy)");
    }

    /**
     * The keyspaces of a graph of DSE 5.1: the three keyspaces of a graph, {@code <graph>}, {@code <graph>_system} and
     * {@code <graph>_pvt}, and a {@code <graph>_pvt} of {@code LocalStrategy} even alone; a pair of user keyspaces
     * such as {@code orders} and {@code orders_system}, or a keyspace with one of those suffixes alone (a
     * {@code _pvt} of {@code LocalStrategy} apart), is no trace.
     */
    @Test
    public void dse51TheKeyspacesOfAGraph()
    {
        Map<String, String> keyspaces = new TreeMap<>();
        keyspaces.put("g", SimpleStrategy.class.getName());
        keyspaces.put("g_system", NetworkTopologyStrategy.class.getName());
        keyspaces.put("g_pvt", LocalStrategy.class.getName());
        keyspaces.put("alone_pvt", LocalStrategy.class.getName());
        keyspaces.put("orders", SimpleStrategy.class.getName());
        keyspaces.put("orders_system", SimpleStrategy.class.getName());
        keyspaces.put("history_pvt", SimpleStrategy.class.getName());
        keyspaces.put("_pvt", SimpleStrategy.class.getName());
        // a graph whose name ends with a suffix of DSE 5.1's Graph
        keyspaces.put("x_system", SimpleStrategy.class.getName());
        keyspaces.put("x_system_system", SimpleStrategy.class.getName());
        keyspaces.put("x_system_pvt", LocalStrategy.class.getName());
        Traces traces = new Traces();
        Dse6NonCoreTraces.addDse51KeyspaceTraces(traces, keyspaces);
        assertThat(descriptions(traces))
        .containsExactly("GRAPH keyspace alone_pvt (LocalStrategy, as DSE Graph creates it)",
                         "GRAPH keyspace g (a keyspace of the graph g)",
                         "GRAPH keyspace g_pvt (a keyspace of the graph g)",
                         "GRAPH keyspace g_system (a keyspace of the graph g)",
                         "GRAPH keyspace x_system (a keyspace of the graph x_system)",
                         "GRAPH keyspace x_system_pvt (a keyspace of the graph x_system)",
                         "GRAPH keyspace x_system_system (a keyspace of the graph x_system)");
    }

    /**
     * The DSE state the Search and Graph node of the fixtures gossips ({@code gossipinfo.txt}) refuses the start once
     * the node has joined, with what removes it on that peer; the one of node 1 of the three-node cluster, the
     * Cassandra workload, is no trace.
     */
    @Test
    public void dse51TheWorkloadsADse51PeerGossipsAreTraces() throws Throwable
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_5_1);
        InetAddressAndPort searchGraphNode = InetAddressAndPort.getByName(SEARCH_GRAPH_NODE);
        DseNodeWorkloads searchGraph = Dse51GossipStates.gossipedWorkloads(searchGraphNode, searchGraphState());
        assertThatThrownBy(() -> Dse6NonCoreTraces.verifyGossipedWorkloads(List.of(searchGraph)))
        .isInstanceOfSatisfying(StartupException.class,
                                e -> assertThat(e.returnCode).isEqualTo(StartupException.ERR_WRONG_CONFIG))
        .hasMessage(FOUND_DSE51 + "DSE workloads other than Cassandra: peer " + SEARCH_GRAPH_NODE + " (gossip): " +
                    SEARCH_GRAPH_WORKLOADS + ". Restart the DSE 5.1 peers listed from gossip as plain Cassandra " +
                    "nodes (dse cassandra without -s, -k, -c or -g), then start this node again");

        EndpointState node1 = gossipedState(DseFixtures.dse51().gossipInfo(1), "127.0.51.11");
        DseNodeWorkloads cassandra = Dse51GossipStates.gossipedWorkloads(InetAddressAndPort.getByName("127.0.51.11"),
                                                                         node1);
        assertThat(logsOf(Level.INFO, () -> Dse6NonCoreTraces.verifyGossipedWorkloads(List.of(cassandra))))
        .containsExactly("DSE 5.1 compatibility mode: no DSE 5.1 peer gossips a DSE workload other than Cassandra " +
                         "(1 DSE 5.1 peer(s) gossiping their DSE state)");
        // with NONE nothing is read
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.NONE);
        assertThat(logsOf(null, Dse6NonCoreTraces::verifyDse51GossipedWorkloads)).isEmpty();
    }

    /**
     * Which peers of gossip the check of {@code DSE_5_1} reads, at the start and periodically: those gossiping a DSE
     * 5.1 messaging version and a DSE state, not this node, nor a peer that left the ring, nor a node of this software;
     * the periodic check logs at ERROR and records what it finds, and refuses nothing.
     */
    @Test
    public void dse51TheGossipOfTheDse51PeersIsReadAtTheStartAndPeriodically() throws Throwable
    {
        InetAddressAndPort searchGraph = InetAddressAndPort.getByName(SEARCH_GRAPH_NODE);
        InetAddressAndPort left = InetAddressAndPort.getByName("127.0.51.22");
        InetAddressAndPort unknown = InetAddressAndPort.getByName("127.0.51.23");
        InetAddressAndPort ours = InetAddressAndPort.getByName("127.0.51.24");
        InetAddressAndPort self = FBUtilities.getBroadcastAddressAndPort();
        EndpointState leftState = searchGraphState();
        leftState.addApplicationState(ApplicationState.STATUS_WITH_PORT,
                                      VersionedValue.unsafeMakeVersionedValue(VersionedValue.STATUS_LEFT + ",0,0", 10));
        EndpointState unknownState = new EndpointState(new HeartBeatState(1, 1));
        unknownState.addApplicationState(ApplicationState.NET_VERSION,
                                         VersionedValue.unsafeMakeVersionedValue("11", 1));
        EndpointState oursState = searchGraphState();
        String ourVersion = String.valueOf(MessagingService.current_version);
        oursState.addApplicationState(ApplicationState.NET_VERSION,
                                      VersionedValue.unsafeMakeVersionedValue(ourVersion, 20));
        EndpointState selfState = Gossiper.instance.endpointStateMap.get(self);
        Map<InetAddressAndPort, EndpointState> added = Map.of(searchGraph, searchGraphState(), left, leftState,
                                                              unknown, unknownState, ours, oursState);
        Gossiper.instance.endpointStateMap.putAll(added);
        Gossiper.instance.endpointStateMap.put(self, searchGraphState());
        try
        {
            DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_5_1);
            assertThat(Dse6NonCoreTraces.gossipedWorkloads()).extracting(w -> w.peer)
                                                              .containsExactly(searchGraph.getAddress());
            assertThatThrownBy(Dse6NonCoreTraces::verifyDse51GossipedWorkloads)
            .hasMessageContaining("peer " + SEARCH_GRAPH_NODE + " (gossip): " + SEARCH_GRAPH_WORKLOADS);

            String trace = "peer " + SEARCH_GRAPH_NODE + " (gossip): " + SEARCH_GRAPH_WORKLOADS;
            List<String> errors = logsOf(Level.ERROR,
                                         () -> assertThat(Dse6NonCoreTraces.recheckDse51GossipedWorkloads())
                                               .containsExactly(trace));
            assertThat(errors).containsExactly("The periodic check found DSE 5.1 peers gossiping DSE workloads other " +
                                               "than Cassandra, which this software does not support: peer " +
                                               SEARCH_GRAPH_NODE + " (gossip): " + SEARCH_GRAPH_WORKLOADS + "; " +
                                               "restart them as plain Cassandra nodes (dse cassandra without -s, -k, " +
                                               "-c or -g): this node refuses its next start while they run them");
            assertThat(Dse6NonCoreTraces.status())
            .isEqualTo("found by the periodic check in gossip: peer " + SEARCH_GRAPH_NODE + " (gossip): " +
                       SEARCH_GRAPH_WORKLOADS);

            // added to the status of the startup check, and forgotten once the peer gossips no such workload
            Dse6NonCoreTraces.verify(new Traces(), false);
            assertThat(Dse6NonCoreTraces.status()).isEqualTo("none; found by the periodic check in gossip: " + trace);
            Gossiper.instance.endpointStateMap.remove(searchGraph);
            assertThat(Dse6NonCoreTraces.recheckDse51GossipedWorkloads()).isEmpty();
            assertThat(Dse6NonCoreTraces.status()).isEqualTo("none");

            // in the other modes nothing is read or logged
            Gossiper.instance.endpointStateMap.put(searchGraph, searchGraphState());
            for (InternodeCompatibilityMode mode : List.of(InternodeCompatibilityMode.NONE,
                                                           InternodeCompatibilityMode.DSE_6))
            {
                DatabaseDescriptor.setInternodeCompatibilityMode(mode);
                assertThat(logsOf(null, () -> assertThat(Dse6NonCoreTraces.recheckDse51GossipedWorkloads()).isEmpty()))
                .as("%s", mode).isEmpty();
            }
        }
        finally
        {
            for (InetAddressAndPort peer : added.keySet())
                Gossiper.instance.endpointStateMap.remove(peer);
            if (selfState == null)
                Gossiper.instance.endpointStateMap.remove(self);
            else
                Gossiper.instance.endpointStateMap.put(self, selfState);
        }
    }

    /**
     * What the refusals of the workloads DSE 5.1 stored say of the record of the upgrade: the rollback must delete it
     * with the marker.
     */
    private static String rollbackDeletes()
    {
        return "The rollback must delete " + DseUpgradeMarker.path(InternodeCompatibilityMode.DSE_5_1) + " and " +
               Dse51SystemTables.recordPath() + ", which hold what this node read of system.local and system.peers " +
               "at its first start in DSE_5_1 mode, or the next start reads the same workloads again";
    }

    /** The gossip states of the Search and Graph node of the fixtures: its DSE state and its {@code NET_VERSION}. */
    private static EndpointState searchGraphState() throws Exception
    {
        EndpointState state = gossipedState(DseFixtures.dse51().searchGraphGossipInfo(), SEARCH_GRAPH_NODE);
        assertThat(state.getApplicationState(ApplicationState.NET_VERSION).value).isEqualTo("11");
        return state;
    }

    /**
     * The DSE state, at ordinal 9, and the {@code NET_VERSION} that {@code endpoint} gossips in a gossipinfo file of
     * the DSE 5.1.51 fixtures.
     */
    private static EndpointState gossipedState(Path gossipInfo, String endpoint) throws Exception
    {
        EndpointState state = new EndpointState(new HeartBeatState(1, 1));
        for (DseFixtures.GossipState s : DseFixtures.dse51().gossipStates(gossipInfo, '/' + endpoint))
        {
            for (ApplicationState kept : List.of(ApplicationState.X_11_PADDING, ApplicationState.NET_VERSION))
            {
                if (s.name.equals(kept.name()))
                    state.addApplicationState(kept, VersionedValue.unsafeMakeVersionedValue(s.value, s.version));
            }
        }
        assertThat(state.getApplicationState(ApplicationState.X_11_PADDING)).as(endpoint).isNotNull();
        return state;
    }
}
