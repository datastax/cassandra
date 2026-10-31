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

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.statements.schema.IndexTarget;
import org.apache.cassandra.exceptions.StartupException;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.nodes.CC4UpgradeNodesPersistence;
import org.apache.cassandra.nodes.DseNodeWorkloads;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.SchemaKeyspaceTables;
import org.apache.cassandra.schema.TableMetadata;

import static org.apache.cassandra.config.CassandraRelevantProperties.INTERNODE_COMPATIBILITY_MODE_IGNORE_DSE_PEER_WORKLOADS;

/**
 * In {@code internode_compatibility_mode: DSE_6}, refuses to start while the node finds any trace of the DSE features
 * this software does not support and never will: DSE Search (Solr), DSE Graph, DSE Analytics (Spark, Hive), DSEFS and
 * CFS, and DSE Advanced Replication. Only DSE core, the Cassandra workload, is upgraded. The traces, even empty
 * ones:
 * <ul>
 *     <li>a DSE workload other than {@code Cassandra} in DSE's {@code nodes/local} (this node) or {@code nodes/peers}
 *     (its peers, as DSE last recorded them): a {@code workloads} set with a workload other than {@code Cassandra}
 *     and {@code Unknown}, {@code graph = true}, or a legacy {@code workload} string other than {@code Cassandra} and
 *     {@code Unknown}. {@code Unknown} is DSE's placeholder for a workload it does not know yet, not a feature: it is
 *     not a trace, so that the entry DSE may keep for a peer already upgraded to this software (which gossips no DSE
 *     workload) cannot block the next node of the upgrade; a node that ran another workload names it in its own
 *     {@code nodes/local}, which it checks when it is upgraded;</li>
 *     <li>DSE Search: an index of a DSE Search class ({@code com.datastax.bdp.search.solr.*}, and the
 *     {@code com.datastax.bdp.cassandra.index.solr.*} names this software also knows as DSE indexes, see
 *     {@code CreateIndexStatement.DSE_INDEXES}), a {@code solr.data} directory in a data directory;</li>
 *     <li>DSE Graph: a keyspace whose {@code graph_engine} in {@code system_schema.keyspaces} is not null;</li>
 *     <li>DSE Analytics, DSEFS and CFS: the keyspaces {@code dse_analytics}, {@code HiveMetaStore}, {@code dsefs},
 *     {@code cfs} and {@code cfs_archive};</li>
 *     <li>DSE Advanced Replication: the keyspace {@code dse_advrep}.</li>
 * </ul>
 * Not traces: DSE core's own keyspaces ({@code dse_system}, {@code dse_system_local}, {@code dse_security},
 * {@code dse_perf}, {@code dse_leases}, {@code dse_insights}, {@code dse_insights_local}, {@code dse_audit}), the
 * {@code system*} keyspaces, and {@code solr_admin}: DSE core creates it on every node, whatever its workload (it is
 * in the schema of a single-node Cassandra-workload DSE 6.9.26 cluster, see
 * {@code test/data/dse-compat/schema/dse-6.9.26/describe-schema.txt}; DSE 6.9's {@code SolrKeyspace} is in
 * {@code dse-core}), so its presence tells nothing about DSE Search. What {@code dse.yaml} relocates is not seen,
 * since this software does not read {@code dse.yaml}: a DSEFS keyspace renamed by {@code dsefs_options.keyspace_name},
 * a Solr data directory moved by {@code solr_data_dir}, the log directory of Advanced Replication
 * ({@code advanced_replication_options.advanced_replication_directory}); the DSE Search indexes, the workloads and
 * {@code dse_advrep} still are.
 * <p>
 * One refusal lists every trace found, grouped by feature, with what removes each on DSE; a DSE file that cannot be
 * read is listed as well, and the collection goes on. The entries of {@code nodes/peers} may be stale: DSE records a
 * peer's workloads when that peer gossips them, and this software never rewrites the file, so the entry of a node
 * restarted as a Cassandra-only node while this node was down keeps its old workloads.
 * {@code cassandra.internode_compatibility_mode.ignore_dse_peer_workloads=true} ignores those entries only (and an
 * unreadable {@code nodes/peers}), with a warning on every start; every other trace describes this node's own disk or
 * the schema of the whole cluster, is never stale, and has no override. An absent {@code nodes/local} or
 * {@code nodes/peers} holds no workload to check: it is not a trace, and is not reported. With {@code NONE} nothing
 * is read or checked.
 * <p>
 * The check runs at startup once the schema is loaded from disk and before any keyspace is opened, so that no index
 * of a DSE Search class is instantiated ({@code SecondaryIndexManager} refuses one in this mode as well). As for the
 * other startup refusals of the mode, the node has written its first files by then (the import of DSE's node metadata,
 * its local metadata, commit log segments), so a refused node goes back to DSE by a rollback, which the refusal
 * names.
 */
public final class Dse6NonCoreTraces
{
    private static final Logger logger = LoggerFactory.getLogger(Dse6NonCoreTraces.class);

    static final String REFUSED = "internode_compatibility_mode DSE_6 refused: ";
    /** The directory DSE Search keeps its index files in, in the first data directory by default. */
    public static final String SOLR_DATA = "solr.data";
    static final String THIS_NODE = "this node (nodes/local)";
    /** DSE's workload of a node it knows nothing about yet. */
    static final String UNKNOWN_WORKLOAD = "Unknown";

    /** The DSE features this software does not support, in the order a refusal lists them. */
    enum Feature
    {
        WORKLOADS("DSE workloads other than Cassandra"),
        SEARCH("DSE Search"),
        GRAPH("DSE Graph"),
        ANALYTICS("DSE Analytics, DSEFS and CFS"),
        ADVANCED_REPLICATION("DSE Advanced Replication"),
        /** not a feature: the DSE files the check could not read, so it cannot tell whether they hold traces */
        UNREADABLE("DSE files that cannot be read");

        final String displayName;

        Feature(String displayName)
        {
            this.displayName = displayName;
        }
    }

    /** Where a trace was found, which decides what removes it. */
    enum Kind
    {
        /** DSE's {@code nodes/local} */
        WORKLOADS_OF_THIS_NODE(Feature.WORKLOADS),
        /** DSE's {@code nodes/local}, which cannot be read */
        UNREADABLE_NODES_LOCAL(Feature.UNREADABLE),
        /** an entry of DSE's {@code nodes/peers}, which the override ignores */
        PEER_ENTRY(Feature.WORKLOADS),
        /** DSE's {@code nodes/peers}, which cannot be read; the override ignores it too */
        UNREADABLE_NODES_PEERS(Feature.UNREADABLE),
        /** a keyspace of {@link #NON_CORE_KEYSPACES}, whose feature is the one of the keyspace */
        KEYSPACE(null),
        GRAPH_KEYSPACE(Feature.GRAPH),
        SEARCH_INDEX(Feature.SEARCH),
        SOLR_DATA_DIRECTORY(Feature.SEARCH);

        /** the feature of every trace of this kind, null when it depends on the trace */
        @Nullable
        final Feature feature;

        Kind(@Nullable Feature feature)
        {
            this.feature = feature;
        }

        boolean coveredByOverride()
        {
            return this == PEER_ENTRY || this == UNREADABLE_NODES_PEERS;
        }
    }

    /** The keyspaces of the features this software does not support; names are case-sensitive. */
    static final Map<String, Feature> NON_CORE_KEYSPACES = ImmutableMap.<String, Feature>builder()
                                                                       .put("dse_analytics", Feature.ANALYTICS)
                                                                       .put("HiveMetaStore", Feature.ANALYTICS)
                                                                       .put("dsefs", Feature.ANALYTICS)
                                                                       .put("cfs", Feature.ANALYTICS)
                                                                       .put("cfs_archive", Feature.ANALYTICS)
                                                                       .put("dse_advrep", Feature.ADVANCED_REPLICATION)
                                                                       .build();

    /**
     * The packages of the DSE Search index classes: DSE 6's, and the one of the other DSE Search class
     * names this software lists as DSE indexes ({@code CreateIndexStatement.DSE_INDEXES}).
     */
    private static final List<String> SEARCH_INDEX_PACKAGES = ImmutableList.of("com.datastax.bdp.search.solr.",
                                                                               "com.datastax.bdp.cassandra.index.solr.");

    /** One trace. */
    static final class Trace
    {
        final Feature feature;
        final Kind kind;
        final String description;
        /** why a DSE file cannot be read, for the traces of an unreadable file */
        @Nullable
        final Throwable cause;

        Trace(Feature feature, Kind kind, String description, @Nullable Throwable cause)
        {
            this.feature = feature;
            this.kind = kind;
            this.description = description;
            this.cause = cause;
        }

        @Override
        public String toString()
        {
            return description;
        }
    }

    /** The traces found, in the order found. */
    @VisibleForTesting
    static final class Traces
    {
        final List<Trace> all = new ArrayList<>();

        /** A trace of a kind whose feature does not depend on the trace. */
        void add(Kind kind, String description)
        {
            assert kind.feature != null : kind;
            all.add(new Trace(kind.feature, kind, description, null));
        }

        void addKeyspace(Feature feature, String description)
        {
            all.add(new Trace(feature, Kind.KEYSPACE, description, null));
        }

        void addUnreadable(Kind kind, String description, Throwable cause)
        {
            assert kind == Kind.UNREADABLE_NODES_LOCAL || kind == Kind.UNREADABLE_NODES_PEERS : kind;
            all.add(new Trace(kind.feature, kind, description, cause));
        }
    }

    /** What the last check in this process found, for {@code nodetool compatstatus}; null before it, after a refusal and with {@code NONE}. */
    private static volatile String status;

    private Dse6NonCoreTraces()
    {
    }

    /** What the last check found (no trace, and what of {@code nodes/peers} it ignored); null before the check, after a refusal and with {@code NONE}. */
    public static String status()
    {
        return status;
    }

    @VisibleForTesting
    public static void resetStatus()
    {
        status = null;
    }

    /**
     * The DSE feature a keyspace belongs to when it is one of the keyspaces of the features this software does not
     * support (DSE Analytics, DSEFS and CFS, or Advanced Replication), as the refusal names it; null for any other
     * keyspace. Names are case-sensitive.
     */
    @Nullable
    public static String nonCoreKeyspaceFeature(String keyspace)
    {
        Feature feature = NON_CORE_KEYSPACES.get(keyspace);
        return feature == null ? null : feature.displayName;
    }

    /** Whether {@code className} is the class of a DSE Search index. */
    public static boolean isSearchIndexClass(String className)
    {
        return className != null && SEARCH_INDEX_PACKAGES.stream().anyMatch(className::startsWith);
    }

    /**
     * The refusal of an index of a DSE Search class in {@code DSE_6} mode by {@code SecondaryIndexManager} and
     * {@code IndexMetadata.validate}: the startup check refuses such a schema first, so it is only met by an index that
     * gets past it, or by a {@code CREATE CUSTOM INDEX} of such a class while the schema freeze of the mode is lifted.
     */
    public static String searchIndexRefusal(String keyspace, String table, String index, String className)
    {
        return REFUSED + "the schema contains the DSE Search index " + keyspace + '.' + table + '.' + index + " (" + className +
               "); DSE Search is not supported by this software. Roll this node back to DSE, drop the search index " +
               "there (DROP SEARCH INDEX ON " + keyspace + '.' + table + ") and upgrade it again";
    }

    /** Runs the check in {@code DSE_6}; does nothing with {@code NONE}. */
    public static void verifyAtStartup() throws StartupException
    {
        if (!DatabaseDescriptor.isDse6CompatibilityMode())
            return;
        Traces traces = new Traces();

        DseNodeWorkloads local = null;
        try
        {
            local = CC4UpgradeNodesPersistence.localWorkloadsInNodesDirectory();
        }
        catch (RuntimeException e)
        {
            traces.addUnreadable(Kind.UNREADABLE_NODES_LOCAL, "nodes/local (" + e + "), so the workloads of this node are unknown", e);
        }
        List<DseNodeWorkloads> peers = List.of();
        try
        {
            peers = CC4UpgradeNodesPersistence.peerWorkloadsInNodesDirectory();
        }
        catch (RuntimeException e)
        {
            traces.addUnreadable(Kind.UNREADABLE_NODES_PEERS, "nodes/peers (" + e + "), so the workloads of the peers are unknown", e);
        }
        addWorkloadTraces(traces, local, peers);
        addKeyspaceTraces(traces, keyspacesWithGraphEngine());
        addSearchIndexTraces(traces, Schema.instance.getKeyspaces().stream().map(Schema.instance::getKeyspaceMetadata).collect(Collectors.toList()));
        addSolrDataTraces(traces, DatabaseDescriptor.getAllDataFileLocations());
        verify(traces, INTERNODE_COMPATIBILITY_MODE_IGNORE_DSE_PEER_WORKLOADS.getBoolean());
    }

    /** Every keyspace of {@code system_schema.keyspaces} with its {@code graph_engine}, null when not set, sorted by name. */
    private static Map<String, String> keyspacesWithGraphEngine()
    {
        Map<String, String> keyspaces = new TreeMap<>();
        String query = String.format("SELECT keyspace_name, graph_engine FROM %s.%s", SchemaConstants.SCHEMA_KEYSPACE_NAME, SchemaKeyspaceTables.KEYSPACES);
        for (UntypedResultSet.Row row : QueryProcessor.executeInternal(query))
            keyspaces.put(row.getString("keyspace_name"), row.has("graph_engine") ? row.getString("graph_engine") : null);
        return keyspaces;
    }

    /** The workload traces of this node's {@code nodes/local} (null when absent) and of the entries of {@code nodes/peers}. */
    @VisibleForTesting
    static void addWorkloadTraces(Traces traces, @Nullable DseNodeWorkloads local, List<DseNodeWorkloads> peers)
    {
        if (local != null && !nonCore(local).isEmpty())
            traces.add(Kind.WORKLOADS_OF_THIS_NODE, THIS_NODE + ": " + String.join(", ", nonCore(local)));
        for (DseNodeWorkloads peer : peers)
        {
            List<String> nonCore = nonCore(peer);
            if (!nonCore.isEmpty())
                traces.add(Kind.PEER_ENTRY, "peer " + (peer.peer == null ? "(no address)" : peer.peer.getHostAddress()) + " (nodes/peers): " + String.join(", ", nonCore));
        }
    }

    /**
     * What {@code fields} record beyond the Cassandra workload, e.g. {@code [workloads [Search, Cassandra], graph true]};
     * empty when the node ran the Cassandra workload alone, or when DSE did not know its workload ({@code Unknown}).
     */
    public static List<String> nonCore(DseNodeWorkloads fields)
    {
        List<String> found = new ArrayList<>(3);
        if (fields.workloads.stream().anyMatch(w -> !DseNodeWorkloads.CASSANDRA.equals(w) && !UNKNOWN_WORKLOAD.equals(w)))
            found.add("workloads " + fields.workloads);
        if (Boolean.TRUE.equals(fields.graph))
            found.add("graph true");
        if (fields.workload != null && !DseNodeWorkloads.CASSANDRA.equals(fields.workload) && !UNKNOWN_WORKLOAD.equals(fields.workload))
            found.add("workload " + fields.workload);
        return found;
    }

    /** The keyspace traces among {@code keyspaces}, every keyspace of the schema with its {@code graph_engine} (null when not set). */
    @VisibleForTesting
    static void addKeyspaceTraces(Traces traces, Map<String, String> keyspaces)
    {
        for (Map.Entry<String, String> keyspace : keyspaces.entrySet())
        {
            String name = keyspace.getKey();
            Feature feature = NON_CORE_KEYSPACES.get(name);
            if (feature != null)
                traces.addKeyspace(feature, "keyspace " + name);
            if (keyspace.getValue() != null)
                traces.add(Kind.GRAPH_KEYSPACE, "keyspace " + name + " (graph_engine " + keyspace.getValue() + ')');
        }
    }

    /** The indexes of a DSE Search class in the tables of {@code keyspaces}. */
    @VisibleForTesting
    static void addSearchIndexTraces(Traces traces, Iterable<KeyspaceMetadata> keyspaces)
    {
        for (KeyspaceMetadata keyspace : keyspaces)
        {
            if (keyspace == null)
                continue;
            for (TableMetadata table : keyspace.tables)
            {
                for (IndexMetadata index : table.indexes)
                {
                    if (!index.isCustom() || index.options == null || index.options.get(IndexTarget.CUSTOM_INDEX_OPTION_NAME) == null)
                        continue;
                    String className = index.getIndexClassName();
                    if (isSearchIndexClass(className))
                        traces.add(Kind.SEARCH_INDEX, "index " + table.keyspace + '.' + table.name + '.' + index.name + " (" + className + ')');
                }
            }
        }
    }

    /** The {@code solr.data} directories of the data directories. */
    @VisibleForTesting
    static void addSolrDataTraces(Traces traces, File[] dataDirectories)
    {
        for (File dataDirectory : dataDirectories)
        {
            File solrData = new File(dataDirectory, SOLR_DATA);
            if (solrData.isDirectory())
                traces.add(Kind.SOLR_DATA_DIRECTORY, "directory " + solrData);
        }
    }

    /**
     * Refuses the start when {@code traces} holds a trace the override does not cover, logs what of {@code nodes/peers}
     * the override ignores, and records what was found.
     */
    @VisibleForTesting
    static void verify(Traces traces, boolean override) throws StartupException
    {
        List<Trace> ignoredEntries = new ArrayList<>();
        Trace unreadablePeers = null;
        List<Trace> refused = new ArrayList<>();
        for (Trace trace : traces.all)
        {
            if (!override || !trace.kind.coveredByOverride())
                refused.add(trace);
            else if (trace.kind == Kind.UNREADABLE_NODES_PEERS)
                unreadablePeers = trace;
            else
                ignoredEntries.add(trace);
        }

        // the override is reported on every start, also when it changes nothing
        String key = INTERNODE_COMPATIBILITY_MODE_IGNORE_DSE_PEER_WORKLOADS.getKey();
        if (unreadablePeers != null)
            logger.warn("DSE's nodes/peers cannot be read and is left unchecked ({}=true): a DSE workload other than Cassandra recorded " +
                        "for a peer would not refuse the start of this node", key, unreadablePeers.cause);
        else if (override && ignoredEntries.isEmpty())
            logger.warn("{}=true is set: a DSE workload other than Cassandra recorded for a peer in nodes/peers would not refuse the " +
                        "start of this node (none is recorded)", key);
        if (!ignoredEntries.isEmpty())
            logger.warn("The DSE workloads other than Cassandra recorded for peers in nodes/peers are ignored ({}=true): {}; this " +
                        "software never rewrites nodes/peers, and supports none of these workloads: remove the option unless each of " +
                        "these entries is stale (the node was since restarted as a Cassandra-only node, or has left the cluster)",
                        key, join(ignoredEntries));

        if (!refused.isEmpty())
        {
            status = null;
            boolean unreadable = refused.stream().anyMatch(t -> t.cause != null);
            Throwable cause = refused.stream().map(t -> t.cause).filter(c -> c != null).findFirst().orElse(null);
            throw new StartupException(unreadable ? StartupException.ERR_WRONG_DISK_STATE : StartupException.ERR_WRONG_CONFIG, refusal(refused), cause);
        }

        String found = "DSE 6 compatibility mode: no trace of DSE Search, Graph, Analytics, DSEFS, CFS or Advanced Replication found";
        if (unreadablePeers == null && ignoredEntries.isEmpty())
        {
            logger.info(found);
            status = "none" + (override ? " (" + key + "=true is set)" : "");
            return;
        }
        logger.info(found + " apart from what of nodes/peers is ignored");
        List<String> ignored = new ArrayList<>();
        if (unreadablePeers != null)
            ignored.add("nodes/peers cannot be read (" + unreadablePeers.cause + ')');
        if (!ignoredEntries.isEmpty())
            ignored.add(join(ignoredEntries));
        status = "none but nodes/peers ignored (" + key + "=true): " + String.join("; ", ignored);
    }

    /** The refusal: every trace per feature, what removes each on DSE, and the override of the entries of {@code nodes/peers}. */
    private static String refusal(List<Trace> refused)
    {
        Map<Feature, List<Trace>> byFeature = new EnumMap<>(Feature.class);
        Set<Kind> kinds = EnumSet.noneOf(Kind.class);
        for (Trace trace : refused)
        {
            byFeature.computeIfAbsent(trace.feature, f -> new ArrayList<>()).add(trace);
            kinds.add(trace.kind);
        }
        List<String> groups = new ArrayList<>();
        for (Map.Entry<Feature, List<Trace>> e : byFeature.entrySet())
            groups.add(e.getKey().displayName + ": " + join(e.getValue()));

        List<String> remedies = new ArrayList<>();
        if (kinds.contains(Kind.WORKLOADS_OF_THIS_NODE))
            remedies.add("restart this node as a plain Cassandra node (dse cassandra without -s, -k or -g; SOLR_ENABLED, " +
                         "SPARK_ENABLED and GRAPH_ENABLED set to 0 in /etc/default/dse for a package install)");
        if (kinds.contains(Kind.PEER_ENTRY))
            remedies.add("restart the peers listed as plain Cassandra nodes while this node runs, so that its nodes/peers records " +
                         "their new workloads");
        if (kinds.contains(Kind.SEARCH_INDEX))
            remedies.add("drop the search indexes (DROP SEARCH INDEX ON <keyspace>.<table>)");
        if (kinds.contains(Kind.SOLR_DATA_DIRECTORY))
            remedies.add("delete the " + SOLR_DATA + " directories once no search index is left");
        if (byFeature.containsKey(Feature.GRAPH))
            remedies.add("migrate the data of the graph keyspaces elsewhere and drop them");
        if (byFeature.containsKey(Feature.ANALYTICS))
            remedies.add("migrate the data of the Analytics, DSEFS and CFS keyspaces elsewhere if it is needed and drop them, empty or not");
        if (byFeature.containsKey(Feature.ADVANCED_REPLICATION))
            remedies.add("stop Advanced Replication, migrate what its keyspace holds if it is needed and drop it, empty or not");

        List<String> sentences = new ArrayList<>();
        // a file that cannot be read is no trace: it only keeps the check from telling whether there is one
        boolean onlyUnreadable = byFeature.keySet().equals(EnumSet.of(Feature.UNREADABLE));
        sentences.add((onlyUnreadable ? "cannot check for traces of DSE features that this software does not support"
                                      : "found traces of DSE features that this software does not support") +
                      "; only DSE core (the Cassandra workload) can be upgraded. " + String.join("; ", groups));
        if (!remedies.isEmpty())
            sentences.add("Roll this node back to DSE, remove them there (" + String.join("; ", remedies) + ") and upgrade it again");
        boolean localUnreadable = kinds.contains(Kind.UNREADABLE_NODES_LOCAL);
        boolean peersUnreadable = kinds.contains(Kind.UNREADABLE_NODES_PEERS);
        if (localUnreadable || peersUnreadable)
            sentences.add("Make " + (localUnreadable && peersUnreadable ? "nodes/local and nodes/peers" : localUnreadable ? "nodes/local" : "nodes/peers") +
                          " readable: the files DSE wrote in the metadata directory, left in place in DSE_6 mode (restore them from the " +
                          "copy of the nodes directory made before the upgrade if they are damaged)" +
                          (peersUnreadable ? "; -D" + INTERNODE_COMPATIBILITY_MODE_IGNORE_DSE_PEER_WORKLOADS.getKey() + "=true leaves nodes/peers unchecked" : ""));
        if (kinds.contains(Kind.PEER_ENTRY))
            sentences.add("An entry of nodes/peers is stale when that node has since been restarted as a Cassandra-only node or has left " +
                          "the cluster: if every entry listed from nodes/peers is stale, start with -D" +
                          INTERNODE_COMPATIBILITY_MODE_IGNORE_DSE_PEER_WORKLOADS.getKey() + "=true instead (it warns at every start; " +
                          "this software never rewrites nodes/peers, so keep the option until the flag drop)");
        return REFUSED + String.join(". ", sentences);
    }

    private static String join(List<Trace> traces)
    {
        return traces.stream().map(Trace::toString).collect(Collectors.joining(", "));
    }
}
