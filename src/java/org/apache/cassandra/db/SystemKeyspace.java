/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.db;

import java.io.IOError;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.TabularData;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;
import org.apache.commons.lang3.ObjectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.compaction.CompactionHistoryTabularData;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.db.marshal.TupleType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.SSTableId;
import org.apache.cassandra.io.sstable.SequenceBasedSSTableId;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.metrics.RestorableMeter;
import org.apache.cassandra.metrics.TopPartitionTracker;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.nodes.INodeInfo;
import org.apache.cassandra.nodes.IPeerInfo;
import org.apache.cassandra.nodes.Nodes;
import org.apache.cassandra.nodes.TruncationRecord;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.Tables;
import org.apache.cassandra.schema.Types;
import org.apache.cassandra.schema.UserFunctions;
import org.apache.cassandra.schema.Views;
import org.apache.cassandra.service.paxos.Ballot;
import org.apache.cassandra.sensors.Context;
import org.apache.cassandra.sensors.RequestSensors;
import org.apache.cassandra.sensors.RequestTracker;
import org.apache.cassandra.sensors.Type;
import org.apache.cassandra.service.paxos.Commit;
import org.apache.cassandra.service.paxos.Commit.Accepted;
import org.apache.cassandra.service.paxos.Commit.AcceptedWithTTL;
import org.apache.cassandra.service.paxos.Commit.Committed;
import org.apache.cassandra.service.paxos.PaxosRepairHistory;
import org.apache.cassandra.service.paxos.PaxosState;
import org.apache.cassandra.service.paxos.uncommitted.PaxosRows;
import org.apache.cassandra.service.paxos.uncommitted.PaxosUncommittedIndex;
import org.apache.cassandra.streaming.StreamOperation;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.CassandraVersion;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MD5Digest;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.concurrent.Future;

import static java.lang.String.format;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static org.apache.cassandra.config.CassandraRelevantProperties.PERSIST_PREPARED_STATEMENTS;
import static org.apache.cassandra.config.CassandraRelevantProperties.UNSAFE_SYSTEM;
import static org.apache.cassandra.config.Config.PaxosStatePurging.legacy;
import static org.apache.cassandra.config.DatabaseDescriptor.paxosStatePurging;
import static org.apache.cassandra.cql3.QueryProcessor.executeInternal;
import static org.apache.cassandra.cql3.QueryProcessor.executeInternalWithNowInSec;
import static org.apache.cassandra.cql3.QueryProcessor.executeOnceInternal;
import static org.apache.cassandra.service.paxos.Commit.latest;
import static org.apache.cassandra.utils.CassandraVersion.NULL_VERSION;
import static org.apache.cassandra.utils.CassandraVersion.UNREADABLE_VERSION;
import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;
import static org.apache.cassandra.utils.FBUtilities.now;

public final class SystemKeyspace
{
    private SystemKeyspace()
    {
    }

    private static final Logger logger = LoggerFactory.getLogger(SystemKeyspace.class);

    public static final CassandraVersion CURRENT_VERSION = new CassandraVersion(FBUtilities.getReleaseVersionString());

    public static final String BATCHES = "batches";
    public static final String PAXOS = "paxos";
    public static final String PAXOS_REPAIR_HISTORY = "paxos_repair_history";
    public static final String PAXOS_REPAIR_STATE = "_paxos_repair_state";
    public static final String BUILT_INDEXES = "IndexInfo";
    public static final String LOCAL = "local";
    public static final String PEERS_V2 = "peers_v2";
    public static final String PEER_EVENTS_V2 = "peer_events_v2";
    public static final String COMPACTION_HISTORY = "compaction_history";
    public static final String SSTABLE_ACTIVITY_V2 = "sstable_activity_v2"; // v2 has modified generation column type (v1 - int, v2 - text), see CASSANDRA-17048
    public static final String TABLE_ESTIMATES = "table_estimates";
    public static final String TABLE_ESTIMATES_TYPE_PRIMARY = "primary";
    public static final String TABLE_ESTIMATES_TYPE_LOCAL_PRIMARY = "local_primary";
    public static final String AVAILABLE_RANGES_V2 = "available_ranges_v2";
    public static final String TRANSFERRED_RANGES_V2 = "transferred_ranges_v2";
    public static final String VIEW_BUILDS_IN_PROGRESS = "view_builds_in_progress";
    public static final String BUILT_VIEWS = "built_views";
    public static final String PREPARED_STATEMENTS = "prepared_statements";
    public static final String REPAIRS = "repairs";
    public static final String TOP_PARTITIONS = "top_partitions";

    /**
     * By default the system keyspace tables should be stored in a single data directory to allow the server
     * to handle more gracefully disk failures. Some tables through can be split accross multiple directories
     * as the server can continue operating even if those tables lost some data.
     */
    public static final Set<String> TABLES_SPLIT_ACROSS_MULTIPLE_DISKS = ImmutableSet.of(BATCHES,
                                                                                         PAXOS,
                                                                                         COMPACTION_HISTORY,
                                                                                         PREPARED_STATEMENTS,
                                                                                         REPAIRS);

    /** @deprecated See CASSANDRA-7544 */
    @Deprecated(since = "4.0") public static final String LEGACY_PEERS = "peers";
    /** @deprecated See CASSANDRA-7544 */
    @Deprecated(since = "4.0") public static final String LEGACY_PEER_EVENTS = "peer_events";
    /** @deprecated See CASSANDRA-7544 */
    @Deprecated(since = "4.0") public static final String LEGACY_TRANSFERRED_RANGES = "transferred_ranges";
    /** @deprecated See CASSANDRA-14404 */
    @Deprecated(since = "4.0") public static final String LEGACY_AVAILABLE_RANGES = "available_ranges";
    /** @deprecated See CASSANDRA-15637 */
    @Deprecated(since = "4.0") public static final String LEGACY_SIZE_ESTIMATES = "size_estimates";
    /** @deprecated See CASSANDRA-17048 */
    @Deprecated(since = "4.1") public static final String LEGACY_SSTABLE_ACTIVITY = "sstable_activity";

    // Names of all tables that could have been a part of a system keyspace. Useful for pre-flight checks.
    // For details, see CASSANDRA-17777
    public static final Set<String> ALL_TABLE_NAMES = ImmutableSet.of(
        BATCHES, PAXOS, PAXOS_REPAIR_HISTORY, PAXOS_REPAIR_STATE, BUILT_INDEXES, LOCAL, PEERS_V2, PEER_EVENTS_V2,
        COMPACTION_HISTORY, SSTABLE_ACTIVITY_V2, TABLE_ESTIMATES, TABLE_ESTIMATES_TYPE_PRIMARY,
        TABLE_ESTIMATES_TYPE_LOCAL_PRIMARY, AVAILABLE_RANGES_V2, TRANSFERRED_RANGES_V2, VIEW_BUILDS_IN_PROGRESS,
        BUILT_VIEWS, PREPARED_STATEMENTS, REPAIRS, TOP_PARTITIONS, LEGACY_PEERS, LEGACY_PEER_EVENTS,
        LEGACY_TRANSFERRED_RANGES, LEGACY_AVAILABLE_RANGES, LEGACY_SIZE_ESTIMATES, LEGACY_SSTABLE_ACTIVITY);

    public static final Set<String> TABLE_NAMES = ImmutableSet.of(
        BATCHES, PAXOS, PAXOS_REPAIR_HISTORY, BUILT_INDEXES, LOCAL, PEERS_V2, PEER_EVENTS_V2, 
        COMPACTION_HISTORY, SSTABLE_ACTIVITY_V2, TABLE_ESTIMATES, AVAILABLE_RANGES_V2, TRANSFERRED_RANGES_V2, VIEW_BUILDS_IN_PROGRESS, 
        BUILT_VIEWS, PREPARED_STATEMENTS, REPAIRS, TOP_PARTITIONS, LEGACY_PEERS, LEGACY_PEER_EVENTS, 
        LEGACY_TRANSFERRED_RANGES, LEGACY_AVAILABLE_RANGES, LEGACY_SIZE_ESTIMATES, LEGACY_SSTABLE_ACTIVITY);

    public static final TableMetadata Batches =
        parse(BATCHES,
              "batches awaiting replay",
              "CREATE TABLE %s ("
              + "id timeuuid,"
              + "mutations list<blob>,"
              + "version int,"
              + "PRIMARY KEY ((id)))")
              .partitioner(new LocalPartitioner(TimeUUIDType.instance))
              .compaction(CompactionParams.stcs(singletonMap("min_threshold", "2")))
              .build();

    private static final TableMetadata Paxos =
        parse(PAXOS,
                "in-progress paxos proposals",
                "CREATE TABLE %s ("
                + "row_key blob,"
                + "cf_id UUID,"
                + "in_progress_ballot timeuuid,"
                + "in_progress_read_ballot timeuuid,"
                + "most_recent_commit blob,"
                + "most_recent_commit_at timeuuid,"
                + "most_recent_commit_version int,"
                + "proposal blob,"
                + "proposal_ballot timeuuid,"
                + "proposal_version int,"
                + "PRIMARY KEY ((row_key), cf_id))")
                .compaction(CompactionParams.lcs(emptyMap()))
                .indexes(PaxosUncommittedIndex.indexes())
                .build();
    private static final Context PaxosContext = Context.from(Paxos);

    private static final TableMetadata BuiltIndexes =
        parse(BUILT_INDEXES,
              "built column indexes",
              "CREATE TABLE \"%s\" ("
              + "table_name text," // table_name here is the name of the keyspace - don't be fooled
              + "index_name text,"
              + "value blob," // Table used to be compact in previous versions
              + "PRIMARY KEY ((table_name), index_name)) ")
              .build();

    private static final TableMetadata PaxosRepairHistoryTable =
        parse(PAXOS_REPAIR_HISTORY,
                "paxos repair history",
                "CREATE TABLE %s ("
                + "keyspace_name text,"
                + "table_name text,"
                + "points frozen<list<tuple<blob, timeuuid>>>, "
                + "PRIMARY KEY (keyspace_name, table_name))"
                + "WITH COMMENT='Last successful paxos repairs by range'")
        .build();

    // Used by CNDB
    public static final TableMetadata Local =
        parse(LOCAL,
                "information about the local node",
                "CREATE TABLE %s ("
                + "key text,"
                + "bootstrapped text,"
                + "broadcast_address inet,"
                + "broadcast_port int,"
                + "cluster_name text,"
                + "cql_version text,"
                + "data_center text,"
                + "gossip_generation int,"
                + "host_id uuid,"
                + "listen_address inet,"
                + "listen_port int,"
                + "native_protocol_version text,"
                + "partitioner text,"
                + "rack text,"
                + "release_version text,"
                + "rpc_address inet,"
                + "rpc_port int,"
                + "schema_version uuid,"
                + "tokens set<varchar>,"
                + "truncated_at map<uuid, blob>,"
                + "PRIMARY KEY ((key)))"
                ).recordDeprecatedSystemColumn("thrift_version", UTF8Type.instance)
                .build();

    // Used by CNDB
    public static final TableMetadata PeersV2 =
        parse(PEERS_V2,
                "information about known peers in the cluster",
                "CREATE TABLE %s ("
                + "peer inet,"
                + "peer_port int,"
                + "data_center text,"
                + "host_id uuid,"
                + "preferred_ip inet,"
                + "preferred_port int,"
                + "rack text,"
                + "release_version text,"
                + "native_address inet,"
                + "native_port int,"
                + "schema_version uuid,"
                + "tokens set<varchar>,"
                + "PRIMARY KEY ((peer), peer_port))")
                .build();

    private static final TableMetadata PeerEventsV2 =
        parse(PEER_EVENTS_V2,
                "events related to peers",
                "CREATE TABLE %s ("
                + "peer inet,"
                + "peer_port int,"
                + "hints_dropped map<timeuuid, int>,"
                + "PRIMARY KEY ((peer), peer_port))")
                .build();

    private static final TableMetadata CompactionHistory =
        parse(COMPACTION_HISTORY,
                "week-long compaction history",
                "CREATE TABLE %s ("
                + "id timeuuid,"
                + "bytes_in bigint,"
                + "bytes_out bigint,"
                + "columnfamily_name text,"
                + "compacted_at timestamp,"
                + "keyspace_name text,"
                + "rows_merged map<int, bigint>," // Note that we currently store partitions, not rows!
                + "compaction_properties frozen<map<text, text>>,"
                + "PRIMARY KEY ((id)))")
                .defaultTimeToLive((int) TimeUnit.DAYS.toSeconds(7))
                .build();

    private static final TableMetadata LegacySSTableActivity =
        parse(LEGACY_SSTABLE_ACTIVITY,
                "historic sstable read rates",
                "CREATE TABLE %s ("
                + "keyspace_name text,"
                + "columnfamily_name text,"
                + "generation int,"
                + "rate_120m double,"
                + "rate_15m double,"
                + "PRIMARY KEY ((keyspace_name, columnfamily_name, generation)))")
                .build();

    private static final TableMetadata SSTableActivity =
        parse(SSTABLE_ACTIVITY_V2,
                "historic sstable read rates",
                "CREATE TABLE %s ("
                + "keyspace_name text,"
                + "table_name text,"
                + "id text,"
                + "rate_120m double,"
                + "rate_15m double,"
                + "PRIMARY KEY ((keyspace_name, table_name, id)))")
                .build();

    /** @deprecated See CASSANDRA-15637 */
    @Deprecated(since = "4.0")
    private static final TableMetadata LegacySizeEstimates =
        parse(LEGACY_SIZE_ESTIMATES,
              "per-table primary range size estimates, table is deprecated in favor of " + TABLE_ESTIMATES,
                "CREATE TABLE %s ("
                + "keyspace_name text,"
                + "table_name text,"
                + "range_start text,"
                + "range_end text,"
                + "mean_partition_size bigint,"
                + "partitions_count bigint,"
                + "PRIMARY KEY ((keyspace_name), table_name, range_start, range_end))")
                .build();

    private static final TableMetadata TableEstimates =
        parse(TABLE_ESTIMATES,
              "per-table range size estimates",
              "CREATE TABLE %s ("
               + "keyspace_name text,"
               + "table_name text,"
               + "range_type text,"
               + "range_start text,"
               + "range_end text,"
               + "mean_partition_size bigint,"
               + "partitions_count bigint,"
               + "PRIMARY KEY ((keyspace_name), table_name, range_type, range_start, range_end))")
               .build();

    private static final TableMetadata AvailableRangesV2 =
    parse(AVAILABLE_RANGES_V2,
          "available keyspace/ranges during bootstrap/replace that are ready to be served",
          "CREATE TABLE %s ("
          + "keyspace_name text,"
          + "full_ranges set<blob>,"
          + "transient_ranges set<blob>,"
          + "PRIMARY KEY ((keyspace_name)))")
    .build();

    private static final TableMetadata TransferredRangesV2 =
        parse(TRANSFERRED_RANGES_V2,
                "record of transferred ranges for streaming operation",
                "CREATE TABLE %s ("
                + "operation text,"
                + "peer inet,"
                + "peer_port int,"
                + "keyspace_name text,"
                + "ranges set<blob>,"
                + "PRIMARY KEY ((operation, keyspace_name), peer, peer_port))")
                .build();

    private static final TableMetadata ViewBuildsInProgress =
        parse(VIEW_BUILDS_IN_PROGRESS,
              "views builds current progress",
              "CREATE TABLE %s ("
              + "keyspace_name text,"
              + "view_name text,"
              + "start_token varchar,"
              + "end_token varchar,"
              + "last_token varchar,"
              + "keys_built bigint,"
              + "PRIMARY KEY ((keyspace_name), view_name, start_token, end_token))")
              .build();

    private static final TableMetadata BuiltViews =
        parse(BUILT_VIEWS,
                "built views",
                "CREATE TABLE %s ("
                + "keyspace_name text,"
                + "view_name text,"
                + "status_replicated boolean,"
                + "PRIMARY KEY ((keyspace_name), view_name))")
                .build();

    private static final TableMetadata TopPartitions =
        parse(TOP_PARTITIONS,
                "Stores the top partitions",
                "CREATE TABLE  %s ("
                + "keyspace_name text,"
                + "table_name text,"
                + "top_type text,"
                + "top frozen<list<tuple<text, bigint>>>,"
                + "last_update timestamp,"
                + "PRIMARY KEY (keyspace_name, table_name, top_type))")
                .build();


    private static final TableMetadata PreparedStatements =
        parse(PREPARED_STATEMENTS,
                "prepared statements",
                "CREATE TABLE %s ("
                + "prepared_id blob,"
                + "logged_keyspace text,"
                + "query_string text,"
                + "PRIMARY KEY ((prepared_id)))")
                .build();

    private static final TableMetadata Repairs =
        parse(REPAIRS,
          "repairs",
          "CREATE TABLE %s ("
          + "parent_id timeuuid, "
          + "started_at timestamp, "
          + "last_update timestamp, "
          + "repaired_at timestamp, "
          + "state int, "
          + "coordinator inet, "
          + "coordinator_port int,"
          + "participants set<inet>,"
          + "participants_wp set<text>,"
          + "ranges set<blob>, "
          + "cfids set<uuid>, "
          + "PRIMARY KEY (parent_id))").build();

    /** @deprecated See CASSANDRA-7544 */
    @Deprecated(since = "4.0")
    // Used by CNDB
    public static final TableMetadata LegacyPeers =
        parse(LEGACY_PEERS,
            "information about known peers in the cluster",
            "CREATE TABLE %s ("
            + "peer inet,"
            + "data_center text,"
            + "host_id uuid,"
            + "preferred_ip inet,"
            + "rack text,"
            + "release_version text,"
            + "rpc_address inet,"
            + "schema_version uuid,"
            + "tokens set<varchar>,"
            + "PRIMARY KEY ((peer)))")
            .build();

    /** @deprecated See CASSANDRA-7544 */
    @Deprecated(since = "4.0")
    private static final TableMetadata LegacyPeerEvents =
        parse(LEGACY_PEER_EVENTS,
            "events related to peers",
            "CREATE TABLE %s ("
            + "peer inet,"
            + "hints_dropped map<timeuuid, int>,"
            + "PRIMARY KEY ((peer)))")
            .build();

    /** @deprecated See CASSANDRA-7544 */
    @Deprecated(since = "4.0")
    private static final TableMetadata LegacyTransferredRanges =
        parse(LEGACY_TRANSFERRED_RANGES,
            "record of transferred ranges for streaming operation",
            "CREATE TABLE %s ("
            + "operation text,"
            + "peer inet,"
            + "keyspace_name text,"
            + "ranges set<blob>,"
            + "PRIMARY KEY ((operation, keyspace_name), peer))")
            .build();

    /** @deprecated See CASSANDRA-14404 */
    @Deprecated(since = "4.0")
    private static final TableMetadata LegacyAvailableRanges =
        parse(LEGACY_AVAILABLE_RANGES,
              "available keyspace/ranges during bootstrap/replace that are ready to be served",
              "CREATE TABLE %s ("
              + "keyspace_name text,"
              + "ranges set<blob>,"
              + "PRIMARY KEY ((keyspace_name)))")
        .build();

    private static TableMetadata.Builder parse(String table, String description, String cql)
    {
        return CreateTableStatement.parse(format(cql, table), SchemaConstants.SYSTEM_KEYSPACE_NAME)
                                   .id(TableId.forSystemTable(SchemaConstants.SYSTEM_KEYSPACE_NAME, table))
                                   .gcGraceSeconds(0)
                                   .memtableFlushPeriod((int) TimeUnit.HOURS.toMillis(1))
                                   .comment(description);
    }

    public static KeyspaceMetadata metadata()
    {
        return KeyspaceMetadata.create(SchemaConstants.SYSTEM_KEYSPACE_NAME, KeyspaceParams.local(), tables(), Views.none(), Types.none(), UserFunctions.none());
    }

    private static Tables tables()
    {
        return Tables.of(BuiltIndexes,
                         Batches,
                         Paxos,
                         PaxosRepairHistoryTable,
                         Local,
                         PeersV2,
                         LegacyPeers,
                         PeerEventsV2,
                         LegacyPeerEvents,
                         CompactionHistory,
                         LegacySSTableActivity,
                         SSTableActivity,
                         LegacySizeEstimates,
                         TableEstimates,
                         AvailableRangesV2,
                         LegacyAvailableRanges,
                         TransferredRangesV2,
                         LegacyTransferredRanges,
                         ViewBuildsInProgress,
                         BuiltViews,
                         PreparedStatements,
                         Repairs,
                         TopPartitions);
    }

    public enum BootstrapState
    {
        NEEDS_BOOTSTRAP,
        COMPLETED,
        IN_PROGRESS,
        DECOMMISSIONED
    }

    public static void persistLocalMetadata()
    {
        persistLocalMetadata(UUID::randomUUID);
    }

    @VisibleForTesting
    public static void persistLocalMetadata(Supplier<UUID> nodeIdSupplier)
    {
        Nodes.local().update(info -> {
            info.setClusterName(DatabaseDescriptor.getClusterName());
            info.setReleaseVersion(SystemKeyspace.CURRENT_VERSION);
            info.setCqlVersion(QueryProcessor.CQL_VERSION);
            info.setNativeProtocolVersion(ProtocolVersion.CURRENT);
            info.setBroadcastAddressAndPort(FBUtilities.getBroadcastAddressAndPort());
            info.setDataCenter(DatabaseDescriptor.getEndpointSnitch().getLocalDatacenter());
            info.setRack(DatabaseDescriptor.getEndpointSnitch().getLocalRack());
            info.setPartitionerClass(DatabaseDescriptor.getPartitioner().getClass());
            info.setNativeTransportAddressAndPort(InetAddressAndPort.getByAddressOverrideDefaults(DatabaseDescriptor.getRpcAddress(), DatabaseDescriptor.getNativeTransportPort()));
            info.setBroadcastAddressAndPort(FBUtilities.getBroadcastAddressAndPort());
            info.setListenAddressAndPort(FBUtilities.getLocalAddressAndPort());
            return info;
        }, true, true);

        // We should store host ID as soon as possible in the system.local table and flush that table to disk so that
        // we can be sure that those changes are stored in sstable and not in the commit log (see CASSANDRA-18153).
        // It is very unlikely that when upgrading the host id is not flushed to disk, but if that's the case, we limit
        // this change only to the new installations or the user should just flush system.local table.
        if (!CommitLog.instance.hasFilesToReplay())
            SystemKeyspace.getOrInitializeLocalHostId(nodeIdSupplier);
    }

    public static void updateCompactionHistory(TimeUUID taskId,
                                               String ksname,
                                               String cfname,
                                               long compactedAt,
                                               long bytesIn,
                                               long bytesOut,
                                               Map<Integer, Long> partitionsMerged,
                                               Map<String, String> compactionProperties)
    {
        // don't write anything when the history table itself is compacted, since that would in turn cause new compactions
        if (ksname.equals("system") && cfname.equals(COMPACTION_HISTORY))
            return;
        // For historical reasons (pre 3.0 refactor) we call the final field rows_merged but we actually store partitions!
        String req = "INSERT INTO system.%s (id, keyspace_name, columnfamily_name, compacted_at, bytes_in, bytes_out, rows_merged, compaction_properties) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
        executeInternal(format(req, COMPACTION_HISTORY),
                        taskId,
                        ksname,
                        cfname,
                        ByteBufferUtil.bytes(compactedAt),
                        bytesIn,
                        bytesOut,
                        partitionsMerged,
                        compactionProperties);
    }

    public static TabularData getCompactionHistory() throws OpenDataException
    {
        UntypedResultSet queryResultSet = executeInternal(format("SELECT * from system.%s", COMPACTION_HISTORY));
        return CompactionHistoryTabularData.from(queryResultSet);
    }

    public static boolean isViewBuilt(String keyspaceName, String viewName)
    {
        String req = "SELECT view_name FROM %s.\"%s\" WHERE keyspace_name=? AND view_name=?";
        UntypedResultSet result = executeInternal(format(req, SchemaConstants.SYSTEM_KEYSPACE_NAME, BUILT_VIEWS), keyspaceName, viewName);
        return !result.isEmpty();
    }

    public static boolean isViewStatusReplicated(String keyspaceName, String viewName)
    {
        String req = "SELECT status_replicated FROM %s.\"%s\" WHERE keyspace_name=? AND view_name=?";
        UntypedResultSet result = executeInternal(format(req, SchemaConstants.SYSTEM_KEYSPACE_NAME, BUILT_VIEWS), keyspaceName, viewName);

        if (result.isEmpty())
            return false;
        UntypedResultSet.Row row = result.one();
        return row.has("status_replicated") && row.getBoolean("status_replicated");
    }

    public static void setViewBuilt(String keyspaceName, String viewName, boolean replicated)
    {
        if (isViewBuilt(keyspaceName, viewName) && isViewStatusReplicated(keyspaceName, viewName) == replicated)
            return;

        String req = "INSERT INTO %s.\"%s\" (keyspace_name, view_name, status_replicated) VALUES (?, ?, ?)";
        executeInternal(format(req, SchemaConstants.SYSTEM_KEYSPACE_NAME, BUILT_VIEWS), keyspaceName, viewName, replicated);
        forceBlockingFlush(BUILT_VIEWS);
    }

    public static void setViewRemoved(String keyspaceName, String viewName)
    {
        String buildReq = "DELETE FROM %s.%s WHERE keyspace_name = ? AND view_name = ?";
        executeInternal(String.format(buildReq, SchemaConstants.SYSTEM_KEYSPACE_NAME, VIEW_BUILDS_IN_PROGRESS), keyspaceName, viewName);

        String builtReq = "DELETE FROM %s.\"%s\" WHERE keyspace_name = ? AND view_name = ? IF EXISTS";
        executeInternal(String.format(builtReq, SchemaConstants.SYSTEM_KEYSPACE_NAME, BUILT_VIEWS), keyspaceName, viewName);
        forceBlockingFlush(VIEW_BUILDS_IN_PROGRESS, BUILT_VIEWS);
    }

    public static void finishViewBuildStatus(String ksname, String viewName)
    {
        // We flush the view built first, because if we fail now, we'll restart at the last place we checkpointed
        // view build.
        // If we flush the delete first, we'll have to restart from the beginning.
        // Also, if writing to the built_view succeeds, but the view_builds_in_progress deletion fails, we will be able
        // to skip the view build next boot.
        setViewBuilt(ksname, viewName, false);
        executeInternal(String.format("DELETE FROM system.%s WHERE keyspace_name = ? AND view_name = ?", VIEW_BUILDS_IN_PROGRESS), ksname, viewName);
        forceBlockingFlush(VIEW_BUILDS_IN_PROGRESS);
    }

    public static void setViewBuiltReplicated(String ksname, String viewName)
    {
        setViewBuilt(ksname, viewName, true);
    }

    public static void updateViewBuildStatus(String ksname, String viewName, Range<Token> range, Token lastToken, long keysBuilt)
    {
        String req = "INSERT INTO system.%s (keyspace_name, view_name, start_token, end_token, last_token, keys_built) VALUES (?, ?, ?, ?, ?, ?)";
        Token.TokenFactory factory = ViewBuildsInProgress.partitioner.getTokenFactory();
        executeInternal(format(req, VIEW_BUILDS_IN_PROGRESS),
                        ksname,
                        viewName,
                        factory.toString(range.left),
                        factory.toString(range.right),
                        factory.toString(lastToken),
                        keysBuilt);
    }

    public static Map<Range<Token>, Pair<Token, Long>> getViewBuildStatus(String ksname, String viewName)
    {
        String req = "SELECT start_token, end_token, last_token, keys_built FROM system.%s WHERE keyspace_name = ? AND view_name = ?";
        Token.TokenFactory factory = ViewBuildsInProgress.partitioner.getTokenFactory();
        UntypedResultSet rs = executeInternal(format(req, VIEW_BUILDS_IN_PROGRESS), ksname, viewName);

        if (rs == null || rs.isEmpty())
            return Collections.emptyMap();

        Map<Range<Token>, Pair<Token, Long>> status = new HashMap<>();
        for (UntypedResultSet.Row row : rs)
        {
            Token start = factory.fromString(row.getString("start_token"));
            Token end = factory.fromString(row.getString("end_token"));
            Range<Token> range = new Range<>(start, end);

            Token lastToken = row.has("last_token") ? factory.fromString(row.getString("last_token")) : null;
            long keysBuilt = row.has("keys_built") ? row.getLong("keys_built") : 0;

            status.put(range, Pair.create(lastToken, keysBuilt));
        }
        return status;
    }

    public static void saveTruncationRecord(TableId tableId, long truncatedAt, CommitLogPosition position)
    {
        Nodes.local().update(info -> info.addTruncationRecord(tableId.asUUID(), new TruncationRecord(position, truncatedAt)), true);
    }

    /**
     * This method is used to remove information about truncation time for specified column family
     */
    public static void removeTruncationRecord(TableId id)
    {
        Nodes.local().update(info -> info.removeTruncationRecord(id.asUUID()), true);
    }

    public static CommitLogPosition getTruncatedPosition(TableId id)
    {
        TruncationRecord record = Nodes.local().get().getTruncationRecords().get(id.asUUID());
        return record != null ? record.position : null;
    }

    public static long getTruncatedAt(TableId id)
    {
        TruncationRecord record = Nodes.local().get().getTruncationRecords().get(id.asUUID());
        return record != null ? record.truncatedAt : Long.MIN_VALUE;
    }

    /**
     * Record tokens being used by another node
     */
    public static void updateTokens(InetAddressAndPort ep, Collection<Token> tokens)
    {
        if (ep.equals(FBUtilities.getBroadcastAddressAndPort()))
            return;

        Nodes.peers().update(ep, peer -> peer.setTokens(tokens), false);
    }

    public static boolean updatePreferredIP(InetAddressAndPort ep, InetAddressAndPort preferredIP)
    {
        if (preferredIP.equals(getPreferredIP(ep)))
            return false;

        Nodes.peers().update(ep, info -> info.setPreferredAddressAndPort(preferredIP), true);
        return true;
    }

    public static void updatePeerNativeAddress(InetAddressAndPort ep, InetAddressAndPort address)
    {
        if (ep.equals(FBUtilities.getBroadcastAddressAndPort()))
            return;

        Nodes.peers().update(ep, info -> info.setNativeTransportAddressAndPort(address), false);
    }


    public static synchronized void updateHintsDropped(InetAddressAndPort ep, TimeUUID timePeriod, int value)
    {
        // with 30 day TTL
        String req = "UPDATE system.%s USING TTL 2592000 SET hints_dropped[ ? ] = ? WHERE peer = ?";
        executeInternal(String.format(req, LEGACY_PEER_EVENTS), timePeriod, value, ep.getAddress());
        req = "UPDATE system.%s USING TTL 2592000 SET hints_dropped[ ? ] = ? WHERE peer = ? AND peer_port = ?";
        executeInternal(String.format(req, PEER_EVENTS_V2), timePeriod, value, ep.getAddress(), ep.getPort());
    }

    public static void updateSchemaVersion(UUID version)
    {
        Nodes.local().update(info -> info.setSchemaVersion(version), false);
    }

    /**
     * Remove stored tokens being used by another node
     */
    public static void removeEndpoint(InetAddressAndPort ep)
    {
        Nodes.peers().remove(ep, true, false);
    }

    /**
     * This method is used to update the System Keyspace with the new tokens for this node
     */
    public static void updateTokens(Collection<Token> tokens)
    {
        assert !tokens.isEmpty() : "removeEndpoint should be used instead";
        Nodes.getInstance().getLocal().update(info -> info.setTokens(tokens), true);
    }

    public static void forceBlockingFlush(String ...cfnames)
    {
        if (!UNSAFE_SYSTEM.getBoolean())
        {
            List<Future<CommitLogPosition>> futures = new ArrayList<>();

            for (String cfname : cfnames)
            {
                futures.add(Keyspace.open(SchemaConstants.SYSTEM_KEYSPACE_NAME)
                                    .getColumnFamilyStore(cfname)
                                    .forceFlush(ColumnFamilyStore.FlushReason.INTERNALLY_FORCED));
            }
            FBUtilities.waitOnFutures(futures);
        }
    }

    /**
     * Return a map of stored tokens to IP addresses
     *
     */
    public static SetMultimap<InetAddressAndPort, Token> loadTokens()
    {
        SetMultimap<InetAddressAndPort, Token> tokenMap = HashMultimap.create();
        Nodes.peers().get().filter(IPeerInfo::isExisting).forEach(info -> tokenMap.putAll(info.getPeerAddressAndPort(), info.getTokens()));
        return tokenMap;
    }

    /**
     * Return a map of store host_ids to IP addresses
     *
     */
    public static Map<InetAddressAndPort, UUID> loadHostIds()
    {
        return Nodes.peers().get().filter(IPeerInfo::isExisting).collect(Collectors.toMap(IPeerInfo::getPeerAddressAndPort, INodeInfo::getHostId));
    }

    /**
     * Get preferred IP for given endpoint if it is known. Otherwise this returns given endpoint itself.
     *
     * @param ep endpoint address to check
     * @return Preferred IP for given endpoint if present, otherwise returns given ep
     */
    public static InetAddressAndPort getPreferredIP(InetAddressAndPort ep)
    {
        IPeerInfo info = Nodes.peers().get(ep);
        if (info != null && info.getPreferredAddressAndPort() != null && info.isExisting())
            return info.getPreferredAddressAndPort();
        else
            return ep;
    }

    /**
     * Return a map of IP addresses containing a map of dc and rack info
     */
    public static Map<InetAddressAndPort, Map<String, String>> loadDcRackInfo()
    {
        return Nodes.peers()
                    .get()
                    .filter(p -> p.getDataCenter() != null && p.getRack() != null && p.isExisting())
                    .collect(Collectors.toMap(IPeerInfo::getPeerAddressAndPort, p -> {
                        Map<String, String> dcRack = new HashMap<>();
                        dcRack.put("data_center", p.getDataCenter());
                        dcRack.put("rack", p.getRack());
                        return dcRack;
                    }));
    }

    /**
     * Get release version for given endpoint.
     * If release version is unknown, then this returns null.
     *
     * @param ep endpoint address to check
     * @return Release version or null if version is unknown.
     */
    public static CassandraVersion getReleaseVersion(InetAddressAndPort ep)
    {
        if (FBUtilities.getBroadcastAddressAndPort().equals(ep))
            return CURRENT_VERSION;
        else
        {
            IPeerInfo peer = Nodes.peers().get(ep);
            return peer != null && peer.isExisting() ? peer.getReleaseVersion() : null;
        }
    }

    /**
     * One of three things will happen if you try to read the system keyspace:
     * 1. files are present and you can read them: great
     * 2. no files are there: great (new node is assumed)
     * 3. files are present but you can't read them: bad
     * @throws ConfigurationException
     */
    public static void checkHealth() throws ConfigurationException
    {
        Keyspace keyspace;
        try
        {
            keyspace = Keyspace.open(SchemaConstants.SYSTEM_KEYSPACE_NAME);
        }
        catch (AssertionError err)
        {
            // this happens when a user switches from OPP to RP.
            ConfigurationException ex = new ConfigurationException("Could not read system keyspace!");
            ex.initCause(err);
            throw ex;
        }
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(LOCAL);

        String savedClusterName = Nodes.local().get().getClusterName();
        if (savedClusterName == null) {
            // this is a brand new node
            if (!cfs.getLiveSSTables().isEmpty())
                throw new ConfigurationException("Found system keyspace files, but they couldn't be loaded!");

            // no system files.  this is a new node.
            return;
        }
        if (!DatabaseDescriptor.getClusterName().equals(savedClusterName))
            throw new ConfigurationException("Saved cluster name " + savedClusterName + " != configured name " + DatabaseDescriptor.getClusterName());
    }

    public static Collection<Token> getSavedTokens()
    {
        return Nodes.local().get().getTokens();
    }

    public static int incrementAndGetGeneration()
    {
        // gossip generation is specific to Gossip thus it is not handled by Nodes.Local
        String req = "SELECT gossip_generation FROM system.%s WHERE key='%s'";
        UntypedResultSet result = executeInternal(format(req, LOCAL, LOCAL));

        int generation;
        if (result.isEmpty() || !result.one().has("gossip_generation"))
        {
            // seconds-since-epoch isn't a foolproof new generation
            // (where foolproof is "guaranteed to be larger than the last one seen at this ip address"),
            // but it's as close as sanely possible
            generation = (int) (currentTimeMillis() / 1000);
        }
        else
        {
            // Other nodes will ignore gossip messages about a node that have a lower generation than previously seen.
            final int storedGeneration = result.one().getInt("gossip_generation") + 1;
            final int now = (int) (currentTimeMillis() / 1000);
            if (storedGeneration >= now)
            {
                logger.warn("Using stored Gossip Generation {} as it is greater than current system time {}.  See CASSANDRA-3654 if you experience problems",
                            storedGeneration, now);
                generation = storedGeneration;
            }
            else
            {
                generation = now;
            }
        }

        req = "INSERT INTO system.%s (key, gossip_generation) VALUES ('%s', ?)";
        executeInternal(format(req, LOCAL, LOCAL), generation);
        forceBlockingFlush(LOCAL);

        return generation;
    }

    public static BootstrapState getBootstrapState()
    {
        return ObjectUtils.firstNonNull(Nodes.local().get().getBootstrapState(), BootstrapState.NEEDS_BOOTSTRAP);
    }

    public static boolean bootstrapComplete()
    {
        return getBootstrapState() == BootstrapState.COMPLETED;
    }

    public static boolean bootstrapInProgress()
    {
        return getBootstrapState() == BootstrapState.IN_PROGRESS;
    }

    public static boolean wasDecommissioned()
    {
        return getBootstrapState() == BootstrapState.DECOMMISSIONED;
    }

    public static void setBootstrapState(BootstrapState state)
    {
        Nodes.local().update(info -> info.setBootstrapState(state), true);
    }

    public static boolean isIndexBuilt(String keyspaceName, String indexName)
    {
        String req = "SELECT index_name FROM %s.\"%s\" WHERE table_name=? AND index_name=?";
        UntypedResultSet result = executeInternal(format(req, SchemaConstants.SYSTEM_KEYSPACE_NAME, BUILT_INDEXES), keyspaceName, indexName);
        return !result.isEmpty();
    }

    public static void setIndexBuilt(String keyspaceName, String indexName)
    {
        String req = "INSERT INTO %s.\"%s\" (table_name, index_name) VALUES (?, ?) IF NOT EXISTS;";
        executeInternal(String.format(req, SchemaConstants.SYSTEM_KEYSPACE_NAME, BUILT_INDEXES), keyspaceName, indexName);
        forceBlockingFlush(BUILT_INDEXES);
    }

    public static void setIndexRemoved(String keyspaceName, String indexName)
    {
        String req = "DELETE FROM %s.\"%s\" WHERE table_name = ? AND index_name = ? IF EXISTS";
        executeInternal(String.format(req, SchemaConstants.SYSTEM_KEYSPACE_NAME, BUILT_INDEXES), keyspaceName, indexName);
        forceBlockingFlush(BUILT_INDEXES);
    }

    public static List<String> getBuiltIndexes(String keyspaceName, Set<String> indexNames)
    {
        List<String> names = new ArrayList<>(indexNames);
        String req = "SELECT index_name from %s.\"%s\" WHERE table_name=? AND index_name IN ?";
        UntypedResultSet results = executeInternal(format(req, SchemaConstants.SYSTEM_KEYSPACE_NAME, BUILT_INDEXES), keyspaceName, names);
        return StreamSupport.stream(results.spliterator(), false)
                            .map(r -> r.getString("index_name"))
                            .collect(Collectors.toList());
    }

    /**
     * Read the host ID from the system keyspace.
     */
    public static UUID getLocalHostId()
    {
        return Nodes.local().get().getHostId();
    }

    /**
     * Read the host ID from the system keyspace, creating (and storing) one if
     * none exists.
     */
    public static synchronized UUID getOrInitializeLocalHostId()
    {
        return getOrInitializeLocalHostId(UUID::randomUUID);
    }

    private static synchronized UUID getOrInitializeLocalHostId(Supplier<UUID> nodeIdSupplier)
    {
        UUID hostId = getLocalHostId();
        if (hostId != null)
            return hostId;

        // ID not found, generate a new one, persist, and then return it.
        hostId = nodeIdSupplier.get();
        logger.warn("No host ID found, created {} (Note: This should happen exactly once per node).", hostId);
        return setLocalHostId(hostId);
    }

    /**
     * Sets the local host ID explicitly.  Should only be called outside of SystemTable when replacing a node.
     */
    public static UUID setLocalHostId(UUID hostId)
    {
        return Nodes.local().update(info -> info.setHostId(hostId), false).getHostId();
    }

    /**
     * Gets the schema version or null if missing
     */
    public static UUID getSchemaVersion()
    {
        return Nodes.local().get().getSchemaVersion();
    }

    /**
     * Gets the stored rack for the local node, or null if none have been set yet.
     */
    public static String getRack()
    {
        return Nodes.local().get().getRack();
    }

    /**
     * Gets the stored data center for the local node, or null if none have been set yet.
     */
    public static String getDatacenter()
    {
        return Nodes.local().get().getDataCenter();
    }

    /**
     * Load the current paxos state for the table and key
     *
     * NOTE: nowInSec is typically provided as zero, and should not be assumed to be definitive, as the cache may apply different nowInSec filters
     */
    public static PaxosState.Snapshot loadPaxosState(DecoratedKey partitionKey, TableMetadata metadata, long nowInSec)
    {
        // Track bytes read from the Paxos system table for the commit that initiated Paxos
        registerPaxosSensor(Type.READ_BYTES);

        String cql = "SELECT * FROM system." + PAXOS + " WHERE row_key = ? AND cf_id = ?";
        List<Row> results = QueryProcessor.executeInternalRawWithNow(nowInSec, cql, partitionKey.getKey(), metadata.id.asUUID()).get(partitionKey);

        // transfer bytes read off of Paxos system table to the user table for the commit that initiated Paxos
        transferPaxosSensorBytes(metadata, Type.READ_BYTES);

        if (results == null || results.isEmpty())
        {
            Committed noneCommitted = Committed.none(partitionKey, metadata);
            return new PaxosState.Snapshot(Ballot.none(), Ballot.none(), null, noneCommitted);
        }

        long purgeBefore = 0;
        long overrideTtlSeconds = 0;
        switch (paxosStatePurging())
        {
            default: throw new AssertionError();
            case legacy:
            case gc_grace:
                overrideTtlSeconds = metadata.params.gcGraceSeconds;
                if (nowInSec > 0)
                    purgeBefore = TimeUnit.SECONDS.toMicros(nowInSec - overrideTtlSeconds);
                break;

            case repaired:
                ColumnFamilyStore cfs = Keyspace.openAndGetStoreIfExists(metadata);
                if (cfs != null)
                {
                    long paxosPurgeGraceMicros = DatabaseDescriptor.getPaxosPurgeGrace(MICROSECONDS);
                    purgeBefore = cfs.getPaxosRepairLowBound(partitionKey).uuidTimestamp() - paxosPurgeGraceMicros;
                }
        }


        Row row = results.get(0);

        Ballot promisedWrite = PaxosRows.getWritePromise(row);
        if (promisedWrite.uuidTimestamp() < purgeBefore) promisedWrite = Ballot.none();
        Ballot promised = latest(promisedWrite, PaxosRows.getPromise(row));
        if (promised.uuidTimestamp() < purgeBefore) promised = Ballot.none();

        // either we have both a recently accepted ballot and update or we have neither
        Accepted accepted = PaxosRows.getAccepted(row, purgeBefore, overrideTtlSeconds);
        Committed committed = PaxosRows.getCommitted(metadata, partitionKey, row, purgeBefore, overrideTtlSeconds);
        // fix a race with TTL/deletion resolution, where TTL expires after equal deletion is inserted; TTL wins the resolution, and is read using an old ballot's nowInSec
        if (accepted != null && !accepted.isAfter(committed))
            accepted = null;

        return new PaxosState.Snapshot(promised, promisedWrite, accepted, committed);
    }

    public static int legacyPaxosTtlSec(TableMetadata metadata)
    {
        // keep paxos state around for at least 3h
        return Math.max(3 * 3600, metadata.params.gcGraceSeconds);
    }

    public static void savePaxosWritePromise(DecoratedKey key, TableMetadata metadata, Ballot ballot)
    {
        if (paxosStatePurging() == legacy)
        {
            String cql = "UPDATE system." + PAXOS + " USING TIMESTAMP ? AND TTL ? SET in_progress_ballot = ? WHERE row_key = ? AND cf_id = ?";
            trackPaxosBytes(metadata, () -> executeInternal(cql,
                            ballot.unixMicros(),
                            legacyPaxosTtlSec(metadata),
                            ballot,
                            key.getKey(),
                            metadata.id.asUUID()));
        }
        else
        {
            String cql = "UPDATE system." + PAXOS + " USING TIMESTAMP ? SET in_progress_ballot = ? WHERE row_key = ? AND cf_id = ?";
            trackPaxosBytes(metadata, () -> executeInternal(cql,
                            ballot.unixMicros(),
                            ballot,
                            key.getKey(),
                            metadata.id.asUUID()));
        }
    }

    public static void savePaxosReadPromise(DecoratedKey key, TableMetadata metadata, Ballot ballot)
    {
        if (paxosStatePurging() == legacy)
        {
            String cql = "UPDATE system." + PAXOS + " USING TIMESTAMP ? AND TTL ? SET in_progress_read_ballot = ? WHERE row_key = ? AND cf_id = ?";
            trackPaxosBytes(metadata, () -> executeInternal(cql,
                            ballot.unixMicros(),
                            legacyPaxosTtlSec(metadata),
                            ballot,
                            key.getKey(),
                            metadata.id.asUUID()));
        }
        else
        {
            String cql = "UPDATE system." + PAXOS + " USING TIMESTAMP ? SET in_progress_read_ballot = ? WHERE row_key = ? AND cf_id = ?";
            trackPaxosBytes(metadata, () -> executeInternal(cql,
                            ballot.unixMicros(),
                            ballot,
                            key.getKey(),
                            metadata.id.asUUID()));
        }
    }

    public static void savePaxosProposal(Commit proposal)
    {
        if (proposal instanceof AcceptedWithTTL)
        {
            long localDeletionTime = ((Commit.AcceptedWithTTL) proposal).localDeletionTime;
            int ttlInSec = legacyPaxosTtlSec(proposal.update.metadata());
            long nowInSec = localDeletionTime - ttlInSec;
            String cql = "UPDATE system." + PAXOS + " USING TIMESTAMP ? AND TTL ? SET proposal_ballot = ?, proposal = ?, proposal_version = ? WHERE row_key = ? AND cf_id = ?";
            trackPaxosBytes(proposal, () -> executeInternalWithNowInSec(cql,
                                        nowInSec,
                                        proposal.ballot.unixMicros(),
                                        ttlInSec,
                                        proposal.ballot,
                                        PartitionUpdate.toBytes(proposal.update, MessagingService.current_version),
                                        MessagingService.current_version,
                                        proposal.update.partitionKey().getKey(),
                                        proposal.update.metadata().id.asUUID()));
        }
        else
        {
            String cql = "UPDATE system." + PAXOS + " USING TIMESTAMP ? SET proposal_ballot = ?, proposal = ?, proposal_version = ? WHERE row_key = ? AND cf_id = ?";
            trackPaxosBytes(proposal, () -> executeInternal(cql,
                            proposal.ballot.unixMicros(),
                            proposal.ballot,
                            PartitionUpdate.toBytes(proposal.update, MessagingService.current_version),
                            MessagingService.current_version,
                            proposal.update.partitionKey().getKey(),
                            proposal.update.metadata().id.asUUID()));
        }
    }

    public static void savePaxosCommit(Commit commit)
    {
        // We always erase the last proposal (with the commit timestamp to no erase more recent proposal in case the commit is old)
        // even though that's really just an optimization  since SP.beginAndRepairPaxos will exclude accepted proposal older than the mrc.
        if (commit instanceof Commit.CommittedWithTTL)
        {
            long localDeletionTime = ((Commit.CommittedWithTTL) commit).localDeletionTime;
            int ttlInSec = legacyPaxosTtlSec(commit.update.metadata());
            long nowInSec = localDeletionTime - ttlInSec;
            String cql = "UPDATE system." + PAXOS + " USING TIMESTAMP ? AND TTL ? SET proposal_ballot = null, proposal = null, proposal_version = null, most_recent_commit_at = ?, most_recent_commit = ?, most_recent_commit_version = ? WHERE row_key = ? AND cf_id = ?";
            trackPaxosBytes(commit, () -> executeInternalWithNowInSec(cql,
                            nowInSec,
                            commit.ballot.unixMicros(),
                            ttlInSec,
                            commit.ballot,
                            PartitionUpdate.toBytes(commit.update, MessagingService.current_version),
                            MessagingService.current_version,
                            commit.update.partitionKey().getKey(),
                            commit.update.metadata().id.asUUID()));
        }
        else
        {
            String cql = "UPDATE system." + PAXOS + " USING TIMESTAMP ? SET proposal_ballot = null, proposal = null, proposal_version = null, most_recent_commit_at = ?, most_recent_commit = ?, most_recent_commit_version = ? WHERE row_key = ? AND cf_id = ?";
            trackPaxosBytes(commit, () -> executeInternal(cql,
                            commit.ballot.unixMicros(),
                            commit.ballot,
                            PartitionUpdate.toBytes(commit.update, MessagingService.current_version),
                            MessagingService.current_version,
                            commit.update.partitionKey().getKey(),
                            commit.update.metadata().id.asUUID()));
        }
    }

    @VisibleForTesting
    public static void savePaxosRepairHistory(String keyspace, String table, PaxosRepairHistory history, boolean flush)
    {
        String cql = "INSERT INTO system.%s (keyspace_name, table_name, points) VALUES (?, ?, ?)";
        executeInternal(String.format(cql, PAXOS_REPAIR_HISTORY), keyspace, table, history.toTupleBufferList());
        if (flush)
            flushPaxosRepairHistory();
    }

    public static void flushPaxosRepairHistory()
    {
        Schema.instance.getColumnFamilyStoreInstance(PaxosRepairHistoryTable.id)
                       .forceBlockingFlush(ColumnFamilyStore.FlushReason.INTERNALLY_FORCED);
    }

    public static PaxosRepairHistory loadPaxosRepairHistory(String keyspace, String table)
    {
        if (SchemaConstants.LOCAL_SYSTEM_KEYSPACE_NAMES.contains(keyspace))
            return PaxosRepairHistory.EMPTY;

        UntypedResultSet results = executeInternal(String.format("SELECT * FROM system.%s WHERE keyspace_name=? AND table_name=?", PAXOS_REPAIR_HISTORY), keyspace, table);
        if (results.isEmpty())
            return PaxosRepairHistory.EMPTY;

        UntypedResultSet.Row row = Iterables.getOnlyElement(results);
        List<ByteBuffer> points = row.getList("points", BytesType.instance);

        return PaxosRepairHistory.fromTupleBufferList(points);
    }

    /**
     * Decorates a paxos comit consumer with methods to track bytes written to the Paxos system table under the context of the user table that initiated Paxos.
     */
    private static void trackPaxosBytes(TableMetadata metadata, Runnable paxosCommitConsumer)
    {
        // Track bytes written to the Paxos system table for the commit that initiated Paxos
        registerPaxosSensor(Type.WRITE_BYTES);
        paxosCommitConsumer.run();
        // transfer bytes written to the Paxos system table to the user table for the commit that initiated Paxos
        transferPaxosSensorBytes(metadata, Type.WRITE_BYTES);
    }

    /**
     * Decorates a paxos comit consumer with methods to track bytes written to the Paxos system table under the context of the user table that initiated Paxos.
     */
    private static void trackPaxosBytes(Commit commit, Runnable paxosCommitConsumer)
    {
        // Track bytes written to the Paxos system table for the commit that initiated Paxos
        registerPaxosSensor(Type.WRITE_BYTES);
        paxosCommitConsumer.run();
        // transfer bytes written to the Paxos system table to the user table for the commit that initiated Paxos
        transferPaxosSensorBytes(commit.update.metadata(), Type.WRITE_BYTES);
    }

    private static void registerPaxosSensor(Type type)
    {
        RequestSensors sensors = RequestTracker.instance.get();
        if (sensors != null)
        {
            sensors.registerSensor(PaxosContext, type);
        }
    }

    /**
     * Populates sensor values of a given {@link Type} associated with the user commit that initiated Paxos.
     */
    private static void transferPaxosSensorBytes(TableMetadata targetSensorMetadata, Type type)
    {
        RequestSensors sensors = RequestTracker.instance.get();
        if (sensors != null)
            sensors.getSensor(PaxosContext, type).ifPresent(paxosSensor -> {
                sensors.incrementSensor(Context.from(targetSensorMetadata), type, paxosSensor.getValue());
                sensors.syncAllSensors();
            });
    }

    /**
     * Returns a RestorableMeter tracking the average read rate of a particular SSTable, restoring the last-seen rate
     * from values in system.sstable_activity if present.
     * @param keyspace the keyspace the sstable belongs to
     * @param table the table the sstable belongs to
     * @param id the generation id for the sstable
     */
    public static RestorableMeter getSSTableReadMeter(String keyspace, String table, SSTableId id)
    {
        UntypedResultSet results = readSSTableActivity(keyspace, table, id);

        if (results.isEmpty())
            return RestorableMeter.createWithDefaultRates();

        UntypedResultSet.Row row = results.one();
        double m15rate = row.getDouble("rate_15m");
        double m120rate = row.getDouble("rate_120m");
        return RestorableMeter.builder().withM15Rate(m15rate).withM120Rate(m120rate).build();
    }

    @VisibleForTesting
    public static UntypedResultSet readSSTableActivity(String keyspace, String table, SSTableId id)
    {
        String cql = "SELECT * FROM system.%s WHERE keyspace_name=? and table_name=? and id=?";
        return executeInternal(format(cql, SSTABLE_ACTIVITY_V2), keyspace, table, id.toString());
    }

    /**
     * Writes the current read rates for a given SSTable to system.sstable_activity
     */
    public static void persistSSTableReadMeter(String keyspace, String table, SSTableId id, RestorableMeter meter)
    {
        // Store values with a one-day TTL to handle corner cases where cleanup might not occur
        String cql = "INSERT INTO system.%s (keyspace_name, table_name, id, rate_15m, rate_120m) VALUES (?, ?, ?, ?, ?) USING TTL 864000";
        executeInternal(format(cql, SSTABLE_ACTIVITY_V2),
                        keyspace,
                        table,
                        id.toString(),
                        meter.fifteenMinuteRate(),
                        meter.twoHourRate());

        if (!DatabaseDescriptor.isUUIDSSTableIdentifiersEnabled() && id instanceof SequenceBasedSSTableId)
        {
            // we do this in order to make it possible to downgrade until we switch in cassandra.yaml to UUID based ids
            // see the discussion on CASSANDRA-17048
            cql = "INSERT INTO system.%s (keyspace_name, columnfamily_name, generation, rate_15m, rate_120m) VALUES (?, ?, ?, ?, ?) USING TTL 864000";
            executeInternal(format(cql, LEGACY_SSTABLE_ACTIVITY),
                            keyspace,
                            table,
                            ((SequenceBasedSSTableId) id).generation,
                            meter.fifteenMinuteRate(),
                            meter.twoHourRate());
        }
    }

    /**
     * Clears persisted read rates from system.sstable_activity for SSTables that have been deleted.
     */
    public static void clearSSTableReadMeter(String keyspace, String table, SSTableId id)
    {
        String cql = "DELETE FROM system.%s WHERE keyspace_name=? AND table_name=? and id=?";
        executeInternal(format(cql, SSTABLE_ACTIVITY_V2), keyspace, table, id.toString());
        if (!DatabaseDescriptor.isUUIDSSTableIdentifiersEnabled() && id instanceof SequenceBasedSSTableId)
        {
            // we do this in order to make it possible to downgrade until we switch in cassandra.yaml to UUID based ids
            // see the discussion on CASSANDRA-17048
            cql = "DELETE FROM system.%s WHERE keyspace_name=? AND columnfamily_name=? and generation=?";
            executeInternal(format(cql, LEGACY_SSTABLE_ACTIVITY), keyspace, table, ((SequenceBasedSSTableId) id).generation);
        }
    }

    /**
     * Writes the current partition count and size estimates into SIZE_ESTIMATES_CF
     */
    public static void updateSizeEstimates(String keyspace, String table, Map<Range<Token>, Pair<Long, Long>> estimates)
    {
        long timestamp = FBUtilities.timestampMicros();
        long nowInSec = FBUtilities.nowInSeconds();
        PartitionUpdate.Builder update = PartitionUpdate.builder(LegacySizeEstimates, UTF8Type.instance.decompose(keyspace), LegacySizeEstimates.regularAndStaticColumns(), estimates.size());
        // delete all previous values with a single range tombstone.
        update.add(new RangeTombstone(Slice.make(LegacySizeEstimates.comparator, table), DeletionTime.build(timestamp - 1, nowInSec)));

        // add a CQL row for each primary token range.
        for (Map.Entry<Range<Token>, Pair<Long, Long>> entry : estimates.entrySet())
        {
            Range<Token> range = entry.getKey();
            Pair<Long, Long> values = entry.getValue();
            update.add(Rows.simpleBuilder(LegacySizeEstimates, table, range.left.toString(), range.right.toString())
                           .timestamp(timestamp)
                           .add("partitions_count", values.left)
                           .add("mean_partition_size", values.right)
                           .build());
        }
        new Mutation(update.build()).apply();
    }

    /**
     * Writes the current partition count and size estimates into table_estimates
     */
    public static void updateTableEstimates(String keyspace, String table, String type, Map<Range<Token>, Pair<Long, Long>> estimates)
    {
        long timestamp = FBUtilities.timestampMicros();
        long nowInSec = FBUtilities.nowInSeconds();
        PartitionUpdate.Builder update = PartitionUpdate.builder(TableEstimates, UTF8Type.instance.decompose(keyspace), TableEstimates.regularAndStaticColumns(), estimates.size());

        // delete all previous values with a single range tombstone.
        update.add(new RangeTombstone(Slice.make(TableEstimates.comparator, table, type), DeletionTime.build(timestamp - 1, nowInSec)));

        // add a CQL row for each primary token range.
        for (Map.Entry<Range<Token>, Pair<Long, Long>> entry : estimates.entrySet())
        {
            Range<Token> range = entry.getKey();
            Pair<Long, Long> values = entry.getValue();
            update.add(Rows.simpleBuilder(TableEstimates, table, type, range.left.toString(), range.right.toString())
                           .timestamp(timestamp)
                           .add("partitions_count", values.left)
                           .add("mean_partition_size", values.right)
                           .build());
        }

        new Mutation(update.build()).apply();
    }


    /**
     * Clears size estimates for a table (on table drop)
     */
    public static void clearEstimates(String keyspace, String table)
    {
        String cqlFormat = "DELETE FROM %s WHERE keyspace_name = ? AND table_name = ?";
        String cql = format(cqlFormat, LegacySizeEstimates.toString());
        executeInternal(cql, keyspace, table);
        cql = String.format(cqlFormat, TableEstimates.toString());
        executeInternal(cql, keyspace, table);
    }

    /**
     * truncates size_estimates and table_estimates tables
     */
    public static void clearAllEstimates()
    {
        for (String table : Arrays.asList(LEGACY_SIZE_ESTIMATES, TABLE_ESTIMATES))
        {
            ColumnFamilyStore cfs = Keyspace.open(SchemaConstants.SYSTEM_KEYSPACE_NAME).getColumnFamilyStore(table);
            cfs.truncateBlockingWithoutSnapshot();
        }
    }

    public static synchronized void updateAvailableRanges(String keyspace, Collection<Range<Token>> completedFullRanges, Collection<Range<Token>> completedTransientRanges)
    {
        String cql = "UPDATE system.%s SET full_ranges = full_ranges + ?, transient_ranges = transient_ranges + ? WHERE keyspace_name = ?";
        executeInternal(format(cql, AVAILABLE_RANGES_V2),
                        completedFullRanges.stream().map(SystemKeyspace::rangeToBytes).collect(Collectors.toSet()),
                        completedTransientRanges.stream().map(SystemKeyspace::rangeToBytes).collect(Collectors.toSet()),
                        keyspace);
    }

    /**
     * List of the streamed ranges, where transientness is encoded based on the source, where range was streamed from.
     */
    public static synchronized AvailableRanges getAvailableRanges(String keyspace, IPartitioner partitioner)
    {
        String query = "SELECT * FROM system.%s WHERE keyspace_name=?";
        UntypedResultSet rs = executeInternal(format(query, AVAILABLE_RANGES_V2), keyspace);

        ImmutableSet.Builder<Range<Token>> full = new ImmutableSet.Builder<>();
        ImmutableSet.Builder<Range<Token>> trans = new ImmutableSet.Builder<>();
        for (UntypedResultSet.Row row : rs)
        {
            Optional.ofNullable(row.getSet("full_ranges", BytesType.instance))
                    .ifPresent(full_ranges -> full_ranges.stream()
                            .map(buf -> byteBufferToRange(buf, partitioner))
                            .forEach(full::add));
            Optional.ofNullable(row.getSet("transient_ranges", BytesType.instance))
                    .ifPresent(transient_ranges -> transient_ranges.stream()
                            .map(buf -> byteBufferToRange(buf, partitioner))
                            .forEach(trans::add));
        }
        return new AvailableRanges(full.build(), trans.build());
    }

    public static class AvailableRanges
    {
        public Set<Range<Token>> full;
        public Set<Range<Token>> trans;

        private AvailableRanges(Set<Range<Token>> full, Set<Range<Token>> trans)
        {
            this.full = full;
            this.trans = trans;
        }
    }

    public static void resetAvailableStreamedRanges()
    {
        ColumnFamilyStore availableRanges = Keyspace.open(SchemaConstants.SYSTEM_KEYSPACE_NAME).getColumnFamilyStore(AVAILABLE_RANGES_V2);
        availableRanges.truncateBlockingWithoutSnapshot();
    }

    public static void resetAvailableStreamedRangesForKeyspace(String keyspace)
    {
        String cql = "DELETE FROM %s.%s WHERE keyspace_name = ?";
        executeInternal(format(cql, SchemaConstants.SYSTEM_KEYSPACE_NAME, AVAILABLE_RANGES_V2), keyspace);
    }

    public static synchronized void updateTransferredRanges(StreamOperation streamOperation,
                                                         InetAddressAndPort peer,
                                                         String keyspace,
                                                         Collection<Range<Token>> streamedRanges)
    {
        String cql = "UPDATE system.%s SET ranges = ranges + ? WHERE operation = ? AND peer = ? AND keyspace_name = ?";
        Set<ByteBuffer> rangesToUpdate = new HashSet<>(streamedRanges.size());
        for (Range<Token> range : streamedRanges)
        {
            rangesToUpdate.add(rangeToBytes(range));
        }
        executeInternal(format(cql, LEGACY_TRANSFERRED_RANGES), rangesToUpdate, streamOperation.getDescription(), peer.getAddress(), keyspace);
        cql = "UPDATE system.%s SET ranges = ranges + ? WHERE operation = ? AND peer = ? AND peer_port = ? AND keyspace_name = ?";
        executeInternal(String.format(cql, TRANSFERRED_RANGES_V2), rangesToUpdate, streamOperation.getDescription(), peer.getAddress(), peer.getPort(), keyspace);
    }

    public static synchronized Map<InetAddressAndPort, Set<Range<Token>>> getTransferredRanges(String description, String keyspace, IPartitioner partitioner)
    {
        Map<InetAddressAndPort, Set<Range<Token>>> result = new HashMap<>();
        String query = "SELECT * FROM system.%s WHERE operation = ? AND keyspace_name = ?";
        UntypedResultSet rs = executeInternal(String.format(query, TRANSFERRED_RANGES_V2), description, keyspace);
        for (UntypedResultSet.Row row : rs)
        {
            InetAddress peerAddress = row.getInetAddress("peer");
            int port = row.getInt("peer_port");
            InetAddressAndPort peer = InetAddressAndPort.getByAddressOverrideDefaults(peerAddress, port);
            Set<ByteBuffer> rawRanges = row.getSet("ranges", BytesType.instance);
            Set<Range<Token>> ranges = Sets.newHashSetWithExpectedSize(rawRanges.size());
            for (ByteBuffer rawRange : rawRanges)
            {
                ranges.add(byteBufferToRange(rawRange, partitioner));
            }
            result.put(peer, ranges);
        }
        return ImmutableMap.copyOf(result);
    }

    /**
     * Compare the release version in the system.local table with the one included in the distro.
     * If they don't match, snapshot all tables in the system and schema keyspaces. This is intended
     * to be called at startup to create a backup of the system tables during an upgrade
     *
     * @throws IOException
     */
    public static void snapshotOnVersionChange() throws IOException
    {
        String previous = getPreviousVersionString();
        String next = FBUtilities.getReleaseVersionString();

        FBUtilities.setPreviousReleaseVersionString(previous);

        // if we're restarting after an upgrade, snapshot the system and schema keyspaces
        if (!previous.equals(NULL_VERSION.toString()) && !previous.equals(next))

        {
            logger.info("Detected version upgrade from {} to {}, snapshotting system keyspaces", previous, next);
            String snapshotName = Keyspace.getTimestampedSnapshotName(format("upgrade-%s-%s",
                                                                             previous,
                                                                             next));

            Instant creationTime = now();
            for (String keyspace : SchemaConstants.LOCAL_SYSTEM_KEYSPACE_NAMES)
                Keyspace.open(keyspace).snapshot(snapshotName, null, false, null, null, creationTime);
        }
    }

    /**
     * Try to determine what the previous version, if any, was installed on this node.
     * Primary source of truth is the release version in system.local. If the previous
     * version cannot be determined by looking there then either:
     * * the node never had a C* install before
     * * the node was at very old version (pre 1.2), which did not include system.local
     *
     * @return either a version read from the system.local table or one of two special values
     * indicating either no previous version (SystemUpgrade.NULL_VERSION) or an unreadable,
     * legacy version (SystemUpgrade.UNREADABLE_VERSION).
     */
    private static String getPreviousVersionString()
    {
        CassandraVersion version = Nodes.local().get().getReleaseVersion();
        if (version == null)
        {
            // it isn't inconceivable that one might try to upgrade a node straight from <= 1.1 to whatever
            // the current version is. If we couldn't read a previous version from system.local we check for
            // the existence of the legacy system.Versions table. We don't actually attempt to read a version
            // from there, but it informs us that this isn't a completely new node.
            for (File dataDirectory : Directories.getKSChildDirectories(SchemaConstants.SYSTEM_KEYSPACE_NAME))
            {
                if (dataDirectory.name().equals("Versions") && dataDirectory.tryList().length > 0)
                {
                    logger.trace("Found unreadable versions info in pre 1.2 system.Versions table");
                    return UNREADABLE_VERSION.toString();
                }
            }

            // no previous version information found, we can assume that this is a new node
            return NULL_VERSION.toString();
        }
        // report back whatever we found in the system table
        return version.toString();
    }

    @VisibleForTesting
    public static Set<Range<Token>> rawRangesToRangeSet(Set<ByteBuffer> rawRanges, IPartitioner partitioner)
    {
        return rawRanges.stream().map(buf -> byteBufferToRange(buf, partitioner)).collect(Collectors.toSet());
    }

    @VisibleForTesting
    public static ByteBuffer rangeToBytes(Range<Token> range)
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            // The format with which token ranges are serialized in the system tables is the pre-3.0 serialization
            // formot for ranges, so we should maintain that for now. And while we don't really support pre-3.0
            // messaging versions, we know AbstractBounds.Serializer still support it _exactly_ for this use case, so we
            // pass 0 as the version to trigger that legacy code.
            // In the future, it might be worth switching to a stable text format for the ranges to 1) save that and 2)
            // be more user friendly (the serialization format we currently use is pretty custom).
            Range.tokenSerializer.serialize(range, out, 0);
            return out.buffer();
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
    }

    @SuppressWarnings("unchecked")
    private static Range<Token> byteBufferToRange(ByteBuffer rawRange, IPartitioner partitioner)
    {
        try
        {
            // See rangeToBytes above for why version is 0.
            return (Range<Token>) Range.tokenSerializer.deserialize(ByteStreams.newDataInput(ByteBufferUtil.getArray(rawRange)),
                                                                    partitioner,
                                                                    0);
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
    }

    public static void writePreparedStatement(String loggedKeyspace, MD5Digest key, String cql)
    {
        if (PERSIST_PREPARED_STATEMENTS.getBoolean())
        {
            executeInternal(format("INSERT INTO %s (logged_keyspace, prepared_id, query_string) VALUES (?, ?, ?)",
                                   PreparedStatements.toString()),
                            loggedKeyspace, key.byteBuffer(), cql);
            logger.debug("stored prepared statement for logged keyspace '{}': '{}'", loggedKeyspace, cql);
        }
        else
            logger.debug("not persisting prepared statement for logged keyspace '{}': '{}'", loggedKeyspace, cql);
    }

    public static void removePreparedStatement(MD5Digest key)
    {
        executeInternal(format("DELETE FROM %s WHERE prepared_id = ?", PreparedStatements.toString()),
                        key.byteBuffer());
    }

    public static void resetPreparedStatements()
    {
        ColumnFamilyStore preparedStatements = Keyspace.open(SchemaConstants.SYSTEM_KEYSPACE_NAME).getColumnFamilyStore(PREPARED_STATEMENTS);
        preparedStatements.truncateBlockingWithoutSnapshot();
    }

    public static int loadPreparedStatements(TriFunction<MD5Digest, String, String, Boolean> onLoaded)
    {
        String query = String.format("SELECT prepared_id, logged_keyspace, query_string FROM %s.%s", SchemaConstants.SYSTEM_KEYSPACE_NAME, PREPARED_STATEMENTS);
        UntypedResultSet resultSet = executeOnceInternal(query);
        int counter = 0;
        for (UntypedResultSet.Row row : resultSet)
        {
            if (onLoaded.accept(MD5Digest.wrap(row.getByteArray("prepared_id")),
                                row.getString("query_string"),
                                row.has("logged_keyspace") ? row.getString("logged_keyspace") : null))
                counter++;
        }
        return counter;
    }

    public static int loadPreparedStatement(MD5Digest digest, TriFunction<MD5Digest, String, String, Boolean> onLoaded)
    {
        String query = String.format("SELECT prepared_id, logged_keyspace, query_string FROM %s.%s WHERE prepared_id = ?", SchemaConstants.SYSTEM_KEYSPACE_NAME, PREPARED_STATEMENTS);
        UntypedResultSet resultSet = executeOnceInternal(query, digest.byteBuffer());
        int counter = 0;
        for (UntypedResultSet.Row row : resultSet)
        {
            if (onLoaded.accept(MD5Digest.wrap(row.getByteArray("prepared_id")),
                                row.getString("query_string"),
                                row.has("logged_keyspace") ? row.getString("logged_keyspace") : null))
                counter++;
        }
        return counter;
    }

    public static interface TriFunction<A, B, C, D> {
        D accept(A var1, B var2, C var3);
    }

    public static void saveTopPartitions(TableMetadata metadata, String topType, Collection<TopPartitionTracker.TopPartition> topPartitions, long lastUpdate)
    {
        String cql = String.format("INSERT INTO %s.%s (keyspace_name, table_name, top_type, top, last_update) values (?, ?, ?, ?, ?)", SchemaConstants.SYSTEM_KEYSPACE_NAME, TOP_PARTITIONS);
        List<ByteBuffer> tupleList = new ArrayList<>(topPartitions.size());
        topPartitions.forEach(tp -> {
            String key = metadata.partitionKeyType.getString(tp.key.getKey());
            tupleList.add(TupleType.buildValue(new ByteBuffer[] { UTF8Type.instance.decompose(key),
                                                                  LongType.instance.decompose(tp.value)}));
        });
        executeInternal(cql, metadata.keyspace, metadata.name, topType, tupleList, Date.from(Instant.ofEpochMilli(lastUpdate)));
    }

    public static TopPartitionTracker.StoredTopPartitions getTopPartitions(TableMetadata metadata, String topType)
    {
        try
        {
            String cql = String.format("SELECT top, last_update FROM %s.%s WHERE keyspace_name = ? and table_name = ? and top_type = ?", SchemaConstants.SYSTEM_KEYSPACE_NAME, TOP_PARTITIONS);
            UntypedResultSet res = executeInternal(cql, metadata.keyspace, metadata.name, topType);
            if (res == null || res.isEmpty())
                return TopPartitionTracker.StoredTopPartitions.EMPTY;
            UntypedResultSet.Row row = res.one();
            long lastUpdated = row.getLong("last_update");
            List<ByteBuffer> top = row.getList("top", BytesType.instance);
            if (top == null || top.isEmpty())
                return TopPartitionTracker.StoredTopPartitions.EMPTY;

            List<TopPartitionTracker.TopPartition> topPartitions = new ArrayList<>(top.size());
            TupleType tupleType = new TupleType(ImmutableList.of(UTF8Type.instance, LongType.instance));
            for (ByteBuffer bb : top)
            {
                ByteBuffer[] components = tupleType.split(ByteBufferAccessor.instance, bb);
                String keyStr = UTF8Type.instance.compose(components[0]);
                long value = LongType.instance.compose(components[1]);
                topPartitions.add(new TopPartitionTracker.TopPartition(metadata.partitioner.decorateKey(metadata.partitionKeyType.fromString(keyStr)), value));
            }
            return new TopPartitionTracker.StoredTopPartitions(topPartitions, lastUpdated);
        }
        catch (Exception e)
        {
            logger.warn("Could not load stored top {} partitions for {}.{}", topType, metadata.keyspace, metadata.name, e);
            return TopPartitionTracker.StoredTopPartitions.EMPTY;
        }
    }
}
