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
import org.apache.cassandra.cql3.statements.schema.IndexTarget;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.dsecompat.DseFixtures;
import org.apache.cassandra.dsecompat.DseFixtures.DseVersion;
import org.apache.cassandra.exceptions.StartupException;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.nodes.CC4UpgradeNodesPersistence;
import org.apache.cassandra.nodes.DseNodeWorkloads;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.Indexes;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.Tables;
import org.apache.cassandra.service.Dse6NonCoreTraces.Kind;
import org.apache.cassandra.service.Dse6NonCoreTraces.Traces;
import org.msgpack.jackson.dataformat.MessagePackFactory;

import static org.apache.cassandra.config.CassandraRelevantProperties.INTERNODE_COMPATIBILITY_MODE_IGNORE_DSE_PEER_WORKLOADS;
import static org.apache.cassandra.cql3.QueryProcessor.executeOnceInternal;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The startup check of {@code internode_compatibility_mode: DSE_6} that refuses any trace of the DSE features this
 * software does not support ({@link Dse6NonCoreTraces}): each kind of trace, the DSE core keyspaces that are not traces,
 * the workloads of DSE's {@code nodes/local} and {@code nodes/peers} (the fixtures written by DSE, and copies of them with
 * other workloads), the override of the entries of {@code nodes/peers}, and {@code NONE}, which reads nothing.
 */
public class Dse6NonCoreTracesTest extends CQLTester
{
    private static final String OVERRIDE_KEY = INTERNODE_COMPATIBILITY_MODE_IGNORE_DSE_PEER_WORKLOADS.getKey();
    private static final String SOLR_INDEX = "com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex";
    private static final String NO_TRACE = "DSE 6 compatibility mode: no trace of DSE Search, Graph, Analytics, DSEFS, CFS or Advanced Replication found";
    private static final String REMEDY_PREFIX = ". Roll this node back to DSE, remove them there (";
    private static final String REMEDY_SUFFIX = ") and upgrade it again";
    private static final String PEER_ADVICE = ". An entry of nodes/peers is stale when that node has since been restarted as a Cassandra-only " +
                                              "node or has left the cluster: if every entry listed from nodes/peers is stale, start with -D" +
                                              OVERRIDE_KEY + "=true instead (it warns at every start; this software never rewrites " +
                                              "nodes/peers, so keep the option until the flag drop)";
    private static final String FOUND = "internode_compatibility_mode DSE_6 refused: found traces of DSE features that this software does " +
                                        "not support; only DSE core (the Cassandra workload) can be upgraded. ";

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
}
