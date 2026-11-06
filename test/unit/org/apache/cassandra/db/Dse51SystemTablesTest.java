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

package org.apache.cassandra.db;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.google.common.collect.Iterables;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.InternodeCompatibilityMode;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.InetAddressType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.exceptions.StartupException;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.sstable.format.bti.BtiFormat;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.sstable.metadata.MetadataComponent;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.nodes.DseNodeWorkloads;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.DroppedColumn;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.service.DefaultFSErrorHandler;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMKiller;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.KillerForTests;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;

import static org.apache.cassandra.db.Dse51SystemTables.RECORD_HEADER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The sstables of {@code system.local} and {@code system.peers} that DataStax Enterprise 5.1 writes name the five
 * columns it adds to those tables: a flushed one those a row of its memtable wrote, a compacted one all five. They
 * are read here in every mode, with those cells skipped, and their values are read in {@code DSE_5_1} by
 * {@link Dse51SystemTables#read}.
 * <p>
 * No sstable written by DSE 5.1 is among the fixtures yet: the sstables here are written by this software (big format)
 * from DSE 5.1's definition of the table, the table of this software with the five columns
 * ({@link Dse51SystemTables#withDse51Columns}), and opened with the definition of this software, as a start of this
 * software opens those of a former DSE 5.1 node. Their header is the one DSE 5.1.51 writes, by its source: the columns
 * with their types, the compacted one built from the whole table definition.
 */
@RunWith(BMUnitRunner.class)
public class Dse51SystemTablesTest extends CQLTester
{
    private static final String PEERS = SchemaConstants.SYSTEM_KEYSPACE_NAME + '.' + SystemKeyspace.LEGACY_PEERS;
    private static final String PEERS_V2 = SchemaConstants.SYSTEM_KEYSPACE_NAME + '.' + SystemKeyspace.PEERS_V2;
    private static final String RELEASE = "3.11.3.5151";
    private static final String DSE_VERSION = "5.1.51";

    /** The id of the sstable whose read the rule of {@code anFSErrorStopsTheStart...} fails; null for none. */
    public static volatile String failingSSTable;

    /** The sstables of {@code system.local} added by a test, removed after it. */
    private final List<SSTableReader> localSSTables = new ArrayList<>();

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Before
    public void setUp() throws Throwable
    {
        Files.deleteIfExists(Dse51SystemTables.recordPath());
        // no compaction merges the sstables a test adds before it reads them
        local().disableAutoCompaction();
        peers().disableAutoCompaction();
    }

    @After
    public void cleanUp() throws Throwable
    {
        Files.deleteIfExists(Dse51SystemTables.recordPath());
        execute("TRUNCATE " + PEERS);
        execute("TRUNCATE " + PEERS_V2);
        ColumnFamilyStore local = local();
        for (SSTableReader sstable : localSSTables)
        {
            try (LifecycleTransaction txn = local.getTracker().tryModify(sstable, OperationType.UNKNOWN))
            {
                txn.obsoleteOriginals();
                txn.finish();
            }
        }
        localSSTables.clear();
        local().enableAutoCompaction();
        peers().enableAutoCompaction();
    }

    @Test
    public void theFiveColumnsAndTheirTypes()
    {
        assertThat(Dse51SystemTables.COLUMNS.keySet()).containsExactly("workload", "workloads", "graph",
                                                                       "dse_version", "server_id");
        assertThat(Dse51SystemTables.COLUMNS.get("workload")).isEqualTo(UTF8Type.instance);
        assertThat(Dse51SystemTables.COLUMNS.get("workloads")).isEqualTo(SetType.getInstance(UTF8Type.instance, false));
        assertThat(Dse51SystemTables.COLUMNS.get("workloads").isMultiCell()).isFalse();
        assertThat(Dse51SystemTables.COLUMNS.get("graph")).isEqualTo(BooleanType.instance);
        assertThat(Dse51SystemTables.COLUMNS.get("dse_version")).isEqualTo(UTF8Type.instance);
        assertThat(Dse51SystemTables.COLUMNS.get("server_id")).isEqualTo(UTF8Type.instance);
    }

    /**
     * Both tables record the five as dropped regular columns of their types, dropped after any timestamp; no other
     * table does, and neither table defines them.
     */
    @Test
    public void bothTablesRecordTheFiveAsDroppedColumns()
    {
        for (TableMetadata table : List.of(SystemKeyspace.Local, SystemKeyspace.LegacyPeers))
        {
            assertThat(Dse51SystemTables.hasDse51Columns(table)).isTrue();
            for (Map.Entry<String, AbstractType<?>> column : Dse51SystemTables.COLUMNS.entrySet())
            {
                ByteBuffer name = UTF8Type.instance.decompose(column.getKey());
                assertThat(table.getColumn(name)).as("%s in %s", column.getKey(), table).isNull();
                DroppedColumn dropped = table.droppedColumns.get(name);
                assertThat(dropped).as("%s in %s", column.getKey(), table).isNotNull();
                assertThat(dropped.droppedTime).isEqualTo(Long.MAX_VALUE);
                assertThat(dropped.column.kind).isEqualTo(ColumnMetadata.Kind.REGULAR);
                assertThat(dropped.column.type).isEqualTo(column.getValue());
                assertThat(Dse51SystemTables.isDse51Column(table, name)).isTrue();
            }
            assertThat(Dse51SystemTables.isDse51Column(table, UTF8Type.instance.decompose("rpc_address"))).isFalse();
        }
        // thrift_version stays recorded
        assertThat(SystemKeyspace.Local.droppedColumns).containsKey(UTF8Type.instance.decompose("thrift_version"));
        TableMetadata peersV2 = Schema.instance.getTableMetadata(SchemaConstants.SYSTEM_KEYSPACE_NAME,
                                                                 SystemKeyspace.PEERS_V2);
        assertThat(Dse51SystemTables.hasDse51Columns(peersV2)).isFalse();
        assertThat(peersV2.droppedColumns).isEmpty();
        assertThat(Dse51SystemTables.isDse51Column(peersV2, UTF8Type.instance.decompose("dse_version"))).isFalse();
    }

    /** The definition with the five columns: the same table (its id), the five as regular columns, none dropped. */
    @Test
    public void theDefinitionWithTheFiveColumns()
    {
        TableMetadata withDse51Columns = Dse51SystemTables.withDse51Columns(SystemKeyspace.Local);
        assertThat(withDse51Columns.id).isEqualTo(SystemKeyspace.Local.id);
        for (Map.Entry<String, AbstractType<?>> column : Dse51SystemTables.COLUMNS.entrySet())
        {
            ColumnMetadata defined = withDse51Columns.getColumn(UTF8Type.instance.decompose(column.getKey()));
            assertThat(defined).isNotNull();
            assertThat(defined.kind).isEqualTo(ColumnMetadata.Kind.REGULAR);
            assertThat(defined.type).isEqualTo(column.getValue());
        }
        assertThat(withDse51Columns.droppedColumns.keySet())
        .containsExactly(UTF8Type.instance.decompose("thrift_version"));
        assertThat(Dse51SystemTables.withDse51Columns(SystemKeyspace.LegacyPeers).droppedColumns).isEmpty();
    }

    /**
     * A compacted sstable of {@code system.peers}, its header naming all five columns, is read in every mode: the
     * other columns of its rows as written, the five skipped; then migrated to {@code peers_v2}, with its release.
     */
    @Test
    public void aCompactedPeersSSTableNamingTheFiveIsReadInEveryModeAndMigrated() throws Throwable
    {
        InetAddress peer = InetAddress.getByName("10.0.51.2");
        UUID hostId = UUID.randomUUID();
        TableMetadata withDse51Columns = Dse51SystemTables.withDse51Columns(SystemKeyspace.LegacyPeers);
        PartitionUpdate update = new RowUpdateBuilder(withDse51Columns, FBUtilities.timestampMicros(), peer)
                                 .add("data_center", "dc1")
                                 .add("rack", "r1")
                                 .add("host_id", hostId)
                                 .add("release_version", RELEASE)
                                 .add("tokens", Set.of("42"))
                                 .add("workload", "Cassandra")
                                 .add("workloads", Set.of("Cassandra"))
                                 .add("graph", false)
                                 .add("dse_version", DSE_VERSION)
                                 .add("server_id", "02-42-AC-11-00-02")
                                 .buildUpdate();
        SSTableReader sstable = addSSTable(peers(), withDse51Columns, withDse51Columns.regularAndStaticColumns(),
                                           List.of(update));
        assertThat(headerColumns(sstable)).containsAll(Dse51SystemTables.COLUMNS.keySet());
        assertThat(droppedColumnsOf(sstable)).containsExactlyInAnyOrderElementsOf(Dse51SystemTables.COLUMNS.keySet());

        for (InternodeCompatibilityMode mode : InternodeCompatibilityMode.values())
        {
            withMode(mode, () -> {
                UntypedResultSet rows = execute("SELECT * FROM " + PEERS + " WHERE peer = ?", peer);
                assertThat(rows.size()).as("rows in %s", mode).isEqualTo(1);
                UntypedResultSet.Row row = rows.one();
                assertThat(row.getString("data_center")).isEqualTo("dc1");
                assertThat(row.getString("rack")).isEqualTo("r1");
                assertThat(row.getUUID("host_id")).isEqualTo(hostId);
                assertThat(row.getString("release_version")).isEqualTo(RELEASE);
                assertThat(row.getSet("tokens", UTF8Type.instance)).containsExactly("42");
                assertThat(row.getColumns().stream().map(c -> c.name.toString()))
                .doesNotContainAnyElementsOf(Dse51SystemTables.COLUMNS.keySet());
            });
        }

        SystemKeyspaceMigrator41.migratePeers();
        UntypedResultSet migrated = execute("SELECT * FROM " + PEERS_V2 + " WHERE peer = ?", peer);
        assertThat(migrated.size()).isEqualTo(1);
        UntypedResultSet.Row row = migrated.one();
        assertThat(row.getInt("peer_port")).isEqualTo(DatabaseDescriptor.getStoragePort());
        assertThat(row.getString("data_center")).isEqualTo("dc1");
        assertThat(row.getUUID("host_id")).isEqualTo(hostId);
        assertThat(row.getString("release_version")).isEqualTo(RELEASE);
        assertThat(row.getSet("tokens", UTF8Type.instance)).containsExactly("42");
    }

    /**
     * A flushed sstable names only the columns some row wrote: here {@code dse_version} and {@code workloads}, as a
     * memtable holding only some writes of DSE's persister. Read in every mode as the compacted one.
     */
    @Test
    public void aFlushedPeersSSTableNamingSomeOfTheFiveIsReadInEveryMode() throws Throwable
    {
        InetAddress peer = InetAddress.getByName("10.0.51.3");
        TableMetadata withDse51Columns = Dse51SystemTables.withDse51Columns(SystemKeyspace.LegacyPeers);
        PartitionUpdate update = new RowUpdateBuilder(withDse51Columns, FBUtilities.timestampMicros(), peer)
                                 .add("data_center", "dc1")
                                 .add("workloads", Set.of("Cassandra"))
                                 .add("dse_version", DSE_VERSION)
                                 .buildUpdate();
        SSTableReader sstable = addSSTable(peers(), withDse51Columns, update.columns(), List.of(update));
        assertThat(headerColumns(sstable)).contains("dse_version", "workloads")
                                          .doesNotContain("workload", "graph", "server_id");

        for (InternodeCompatibilityMode mode : InternodeCompatibilityMode.values())
            withMode(mode, () -> assertRows(execute("SELECT peer, data_center FROM " + PEERS + " WHERE peer = ?", peer),
                                            row(peer, "dc1")));
    }

    /** The row {@code local} of {@code system.local} as DSE 5.1 writes it, its five columns skipped in every mode. */
    @Test
    public void aLocalSSTableNamingTheFiveIsReadInEveryMode() throws Throwable
    {
        localSSTable(1L);
        for (InternodeCompatibilityMode mode : InternodeCompatibilityMode.values())
        {
            withMode(mode, () -> {
                UntypedResultSet rows = execute("SELECT * FROM system.local WHERE key = 'local'");
                assertThat(rows.size()).isEqualTo(1);
                assertThat(rows.one().getString("cluster_name")).isEqualTo(DatabaseDescriptor.getClusterName());
                assertThat(rows.one().getColumns().stream().map(c -> c.name.toString()))
                .doesNotContainAnyElementsOf(Dse51SystemTables.COLUMNS.keySet());
            });
        }
    }

    /**
     * {@link Dse51SystemTables#read} returns the values DSE 5.1 wrote, merged across the sstables as a read merges
     * them: the newest {@code dse_version}, the set {@code workloads} as its last write left it, nothing for a peer
     * whose partition a later sstable deletes, nothing for a row that holds none of the five.
     */
    @Test
    public void theValuesAreReadFromTheSSTablesMerged() throws Throwable
    {
        InetAddress search = InetAddress.getByName("10.0.51.2");
        InetAddress removed = InetAddress.getByName("10.0.51.3");
        InetAddress plain = InetAddress.getByName("10.0.51.4");
        TableMetadata withDse51Columns = Dse51SystemTables.withDse51Columns(SystemKeyspace.LegacyPeers);
        long timestamp = FBUtilities.timestampMicros();
        List<PartitionUpdate> older = List.of(new RowUpdateBuilder(withDse51Columns, timestamp, search)
                                              .add("workloads", Set.of("Cassandra"))
                                              .add("workload", "Cassandra")
                                              .add("dse_version", "5.1.50")
                                              .buildUpdate(),
                                              new RowUpdateBuilder(withDse51Columns, timestamp, removed)
                                              .add("workloads", Set.of("Cassandra"))
                                              .add("dse_version", DSE_VERSION)
                                              .buildUpdate(),
                                              new RowUpdateBuilder(withDse51Columns, timestamp, plain)
                                              .add("data_center", "dc1")
                                              .buildUpdate());
        ByteBuffer removedKey = InetAddressType.instance.decompose(removed);
        List<PartitionUpdate> newer = List.of(new RowUpdateBuilder(withDse51Columns, timestamp + 10, search)
                                              .add("workloads", Set.of("Search"))
                                              .add("workload", "Search")
                                              .add("graph", true)
                                              .add("dse_version", DSE_VERSION)
                                              .buildUpdate(),
                                              PartitionUpdate.fullPartitionDelete(withDse51Columns, removedKey,
                                                                                  timestamp + 10,
                                                                                  FBUtilities.nowInSeconds()));
        addSSTable(peers(), withDse51Columns, withDse51Columns.regularAndStaticColumns(), older);
        addSSTable(peers(), withDse51Columns, withDse51Columns.regularAndStaticColumns(), newer);

        List<Dse51SystemTables.StoredColumns> stored = Dse51SystemTables.read(peers());
        assertThat(stored).singleElement().satisfies(columns -> {
            assertThat(columns.peer).isEqualTo(search);
            assertThat(columns.workloads).containsExactly("Search");
            assertThat(columns.workload).isEqualTo("Search");
            assertThat(columns.graph).isTrue();
            assertThat(columns.dseVersion).isEqualTo(DSE_VERSION);
        });
        assertRows(execute("SELECT peer, data_center FROM " + PEERS + " WHERE peer = ?", plain), row(plain, "dc1"));
    }

    /** The row of {@code system.local} is read as the node's own, without a peer. */
    @Test
    public void theValuesOfTheLocalRowAreRead() throws Throwable
    {
        localSSTable(1L);
        assertThat(Dse51SystemTables.read(local())).singleElement().satisfies(columns -> {
            assertThat(columns.peer).isNull();
            assertThat(columns.workloads).containsExactly("Analytics", "Cassandra");
            assertThat(columns.workload).isEqualTo("Analytics");
            assertThat(columns.graph).isFalse();
            assertThat(columns.dseVersion).isEqualTo(DSE_VERSION);
            assertThat(columns.toString()).isEqualTo("local (workloads [Analytics, Cassandra], workload Analytics, " +
                                                     "graph false, dse_version 5.1.51)");
        });
    }

    /**
     * In {@code DSE_5_1}, the first start reads the values with an INFO line per row and records them; a later start
     * loads them from the record, even once the sstables no longer hold them. In the other modes nothing is read,
     * recorded or logged.
     */
    @Test
    public void theStartReadsAndRecordsTheValuesInDse51ModeOnly() throws Throwable
    {
        InetAddress peer = InetAddress.getByName("10.0.51.2");
        TableMetadata withDse51Columns = Dse51SystemTables.withDse51Columns(SystemKeyspace.LegacyPeers);
        addSSTable(peers(), withDse51Columns, withDse51Columns.regularAndStaticColumns(),
                   List.of(new RowUpdateBuilder(withDse51Columns, FBUtilities.timestampMicros(), peer)
                           .add("workloads", Set.of("Cassandra"))
                           .add("dse_version", DSE_VERSION)
                           .buildUpdate()));
        for (InternodeCompatibilityMode mode : List.of(InternodeCompatibilityMode.NONE,
                                                       InternodeCompatibilityMode.DSE_6))
        {
            List<String> lines = loadOrReadAtStartCollecting(mode, Level.INFO);
            assertThat(lines).as("INFO lines in %s", mode).isEmpty();
            assertThat(Dse51SystemTables.stored()).as("values read in %s", mode).isEmpty();
            assertThat(Dse51SystemTables.recordPath()).as("record in %s", mode).doesNotExist();
        }

        Path record = Dse51SystemTables.recordPath();
        List<String> lines = loadOrReadAtStartCollecting(InternodeCompatibilityMode.DSE_5_1, Level.INFO);
        String row = "DSE 5.1 compatibility mode: DSE 5.1's columns of system.local and system.peers for 10.0.51.2 " +
                     "(workloads [Cassandra], workload absent, graph absent, dse_version 5.1.51)";
        assertThat(lines).containsExactly("DSE 5.1 compatibility mode: recorded the values of DSE 5.1's columns of " +
                                          "system.local and system.peers in " + record,
                                          summary("read from their sstables", 1),
                                          row);
        assertStoredPeer(peer);
        assertThat(Files.readAllLines(record)).containsExactly(RECORD_HEADER, "10.0.51.2\t5.1.51\t-\t-\tCassandra");

        // a compaction of this software rewrote the rows without the values: the record still has them
        execute("TRUNCATE " + PEERS);
        assertThat(loadOrReadAtStartCollecting(InternodeCompatibilityMode.DSE_5_1, Level.INFO))
        .containsExactly(summary("loaded from " + record, 1));
        assertStoredPeer(peer);
        assertThat(loadOrReadAtStartCollecting(InternodeCompatibilityMode.DSE_5_1, Level.DEBUG)).containsExactly(row);
    }

    /** A first start that finds no value records that too, so that a later start does not read the sstables again. */
    @Test
    public void aStartThatFindsNoValueRecordsIt() throws Throwable
    {
        assertThat(loadOrReadAtStartCollecting(InternodeCompatibilityMode.DSE_5_1, Level.INFO))
        .endsWith(summary("read from their sstables", 0));
        assertThat(Dse51SystemTables.stored()).isEmpty();
        assertThat(Files.readAllLines(Dse51SystemTables.recordPath())).containsExactly(RECORD_HEADER);

        localSSTable(1L);
        loadOrReadAtStartCollecting(InternodeCompatibilityMode.DSE_5_1, Level.INFO);
        assertThat(Dse51SystemTables.stored()).isEmpty();
    }

    /**
     * A record that cannot be read is renamed aside (its name followed by the suffix and the time) with a WARN
     * carrying the reason, the values read again from the sstables and recorded again.
     */
    @Test
    public void anUnreadableRecordIsRenamedAsideAndTheValuesReadAgain() throws Throwable
    {
        localSSTable(1L);
        Path record = Dse51SystemTables.recordPath();
        Files.write(record, List.of("not a record"));
        List<ILoggingEvent> warnings = loadOrReadAtStartCollectingEvents(InternodeCompatibilityMode.DSE_5_1,
                                                                         Level.WARN);
        Path aside = asideOf(record);
        try
        {
            assertThat(warnings).singleElement().satisfies(event -> {
                assertThat(event.getFormattedMessage())
                .isEqualTo("DSE 5.1 compatibility mode: the record " + record + " of the values of DSE 5.1's " +
                           "columns of system.local and system.peers cannot be read, and is renamed to " + aside +
                           "; the values are read again from the sstables of those tables, which a compaction may " +
                           "have rewritten without some of them");
                assertThat(event.getThrowableProxy().getMessage())
                .startsWith("not a record of the values of DSE 5.1's columns");
            });
            assertThat(Files.readAllLines(aside)).containsExactly("not a record");
            assertThat(Dse51SystemTables.stored()).singleElement()
                                                  .satisfies(columns -> assertThat(columns.peer).isNull());
            assertThat(Files.readAllLines(record)).hasSize(2).first().isEqualTo(RECORD_HEADER);
        }
        finally
        {
            Files.deleteIfExists(aside);
        }
    }

    /**
     * A record that can neither be read nor renamed aside (here its directory, a metadata directory of this test, is
     * not writable) is reported with an ERROR, and with no WARN claiming the rename: the values are read from the
     * sstables and not recorded.
     */
    @Test
    public void anUnreadableRecordThatCannotBeRenamedIsReportedWithAnError() throws Throwable
    {
        localSSTable(1L);
        File metadata = DatabaseDescriptor.getMetadataDirectory();
        Path directory = temporaryFolder.newFolder().toPath();
        DatabaseDescriptor.setMetadataDirectory(new File(directory));
        try
        {
            Path record = Dse51SystemTables.recordPath();
            Files.write(record, List.of("not a record"));
            assertThat(directory.toFile().setWritable(false)).isTrue();
            // not where the user can write anywhere, as root
            Assume.assumeFalse(Files.isWritable(directory));
            List<ILoggingEvent> events = loadOrReadAtStartCollectingEvents(InternodeCompatibilityMode.DSE_5_1, null);
            assertThat(events.stream().filter(event -> event.getLevel() == Level.WARN)).isEmpty();
            assertThat(events.stream().filter(event -> event.getLevel() == Level.ERROR)).singleElement()
                                                                                         .satisfies(event -> {
                assertThat(event.getFormattedMessage())
                .startsWith("DSE 5.1 compatibility mode: the record " + record + " of the values of DSE 5.1's " +
                            "columns of system.local and system.peers cannot be read, nor renamed to " + record +
                            Dse51SystemTables.UNREADABLE_SUFFIX + '-')
                .endsWith("; the values cannot be recorded until it is removed by hand, and every start reads them " +
                          "again from the sstables of those tables, which a compaction of this software rewrites " +
                          "without them. Remove it and restart the node now, before any compaction of system.local " +
                          "and system.peers");
                assertThat(event.getThrowableProxy().getSuppressed()).hasSize(1);
            });
            assertThat(Dse51SystemTables.stored()).singleElement()
                                                  .satisfies(columns -> assertThat(columns.peer).isNull());
            assertThat(Files.readAllLines(record)).containsExactly("not a record");
        }
        finally
        {
            assertThat(directory.toFile().setWritable(true)).isTrue();
            DatabaseDescriptor.setMetadataDirectory(metadata);
        }
    }

    /**
     * A record of an earlier upgrade that cannot be deleted (here a non-empty directory) is reported with an ERROR,
     * and the start is refused; the next start, once the record is gone, reads and records the values.
     */
    @Test
    public void aRecordOfAnEarlierUpgradeThatCannotBeDeletedRefusesTheStart() throws Throwable
    {
        localSSTable(1L);
        Path record = Files.createDirectories(Dse51SystemTables.recordPath());
        Path inTheWay = Files.write(record.resolve("in-the-way"), new byte[0]);
        try
        {
            Logger logger = (Logger) LoggerFactory.getLogger(Dse51SystemTables.class);
            ListAppender<ILoggingEvent> appender = new ListAppender<>();
            appender.start();
            logger.addAppender(appender);
            try
            {
                assertThat(Dse51SystemTables.deleteRecordOfAnEarlierUpgrade()).isFalse();
            }
            finally
            {
                logger.detachAppender(appender);
                appender.stop();
            }
            assertThat(appender.list).singleElement().satisfies(event -> {
                assertThat(event.getLevel()).isEqualTo(Level.ERROR);
                assertThat(event.getFormattedMessage())
                .isEqualTo("DSE 5.1 compatibility mode: unable to delete " + record + ", the record of the values of " +
                           "DSE 5.1's columns of system.local and system.peers of an earlier upgrade; the DSE 5.1 " +
                           "upgrade marker is not written and the start is refused. Delete it by hand and start the " +
                           "node again");
            });
            assertThatThrownBy(() -> withMode(InternodeCompatibilityMode.DSE_5_1, Dse51SystemTables::loadOrReadAtStart))
            .isInstanceOf(StartupException.class)
            .hasMessage("cannot start in DSE_5_1 compatibility mode: " + record + ", the record of the values of " +
                        "DSE 5.1's columns of system.local and system.peers of an earlier upgrade, cannot be deleted " +
                        "(the error is logged above); delete it by hand and start the node again");
            assertThat(record).isDirectory();
        }
        finally
        {
            Files.deleteIfExists(inTheWay);
            Files.deleteIfExists(record);
            // clears the refusal of the start, whatever failed above
            Dse51SystemTables.deleteRecordOfAnEarlierUpgrade();
        }
        // the next start: the deletion succeeds, the values are read and recorded
        assertThat(Dse51SystemTables.deleteRecordOfAnEarlierUpgrade()).isTrue();
        assertThat(loadOrReadAtStartCollecting(InternodeCompatibilityMode.DSE_5_1, Level.INFO))
        .contains(summary("read from their sstables", 1));
        assertThat(record).isRegularFile();
    }

    /**
     * A record that cannot be written is reported with an ERROR carrying the reason, the values being those read;
     * nothing is left behind, and the next start reads the sstables again.
     */
    @Test
    public void aRecordThatCannotBeWrittenIsReportedWithAnError() throws Throwable
    {
        localSSTable(1L);
        Path record = Dse51SystemTables.recordPath();
        Path temporary = Files.createDirectories(record.resolveSibling(record.getFileName() + ".tmp"));
        Files.write(temporary.resolve("in-the-way"), new byte[0]);
        try
        {
            List<ILoggingEvent> errors = loadOrReadAtStartCollectingEvents(InternodeCompatibilityMode.DSE_5_1,
                                                                           Level.ERROR);
            assertThat(errors).singleElement().satisfies(event -> {
                assertThat(event.getFormattedMessage())
                .isEqualTo("DSE 5.1 compatibility mode: the values of DSE 5.1's columns of system.local and " +
                           "system.peers cannot be recorded in " + record + "; a later start reads them again from " +
                           "the sstables of those tables, which a compaction of this software rewrites without " +
                           "them. Remove the cause reported below (for example make the metadata directory " +
                           "writable, or remove " + record + ".tmp) and restart the node now, before any " +
                           "compaction of system.local and system.peers");
                // the failure to open the temporary file, the failure to delete it suppressed
                // the message of the exception depends on the operating system; it names the file
                assertThat(event.getThrowableProxy().getClassName()).isEqualTo(FileSystemException.class.getName());
                assertThat(event.getThrowableProxy().getMessage()).contains(record + ".tmp");
                assertThat(event.getThrowableProxy().getSuppressed()).singleElement()
                .satisfies(suppressed -> assertThat(suppressed.getClassName())
                                         .isEqualTo(DirectoryNotEmptyException.class.getName()));
            });
            assertThat(Dse51SystemTables.stored()).singleElement()
                                                  .satisfies(columns -> assertThat(columns.peer).isNull());
            assertThat(record).doesNotExist();
        }
        finally
        {
            Files.deleteIfExists(temporary.resolve("in-the-way"));
            Files.deleteIfExists(temporary);
        }
    }

    /** A value that cannot be written as a field of the record is reported with its own ERROR. */
    @Test
    public void aValueThatCannotBeRecordedIsReportedWithAnError() throws Throwable
    {
        TableMetadata withDse51Columns = Dse51SystemTables.withDse51Columns(SystemKeyspace.LegacyPeers);
        addSSTable(peers(), withDse51Columns, withDse51Columns.regularAndStaticColumns(),
                   List.of(new RowUpdateBuilder(withDse51Columns, FBUtilities.timestampMicros(),
                                                InetAddress.getByName("10.0.51.2"))
                           .add("workload", "a\tb")
                           .buildUpdate()));
        Path record = Dse51SystemTables.recordPath();
        List<ILoggingEvent> errors = loadOrReadAtStartCollectingEvents(InternodeCompatibilityMode.DSE_5_1, Level.ERROR);
        assertThat(errors.stream().map(ILoggingEvent::getFormattedMessage))
        .containsExactly("DSE 5.1 compatibility mode: the values of DSE 5.1's columns of system.local and " +
                         "system.peers cannot be recorded in " + record + ": the value 'a\tb' cannot be recorded: it " +
                         "is empty or -, or holds a tab, a line break or a comma; a later start reads them again " +
                         "from the sstables of those tables, which a compaction of this software rewrites without " +
                         "them");
        assertThat(record).doesNotExist();
        assertThat(Dse51SystemTables.stored()).singleElement()
                                              .satisfies(columns -> assertThat(columns.workload).isEqualTo("a\tb"));
    }

    /**
     * The record of an earlier upgrade is deleted, with an INFO line, and a temporary file of it; the copies of
     * unreadable records are kept. Nothing happens without a record.
     */
    @Test
    public void theRecordOfAnEarlierUpgradeIsDeleted() throws Throwable
    {
        Path record = Dse51SystemTables.recordPath();
        Dse51SystemTables.write(record, List.of());
        Path temporary = Files.write(record.resolveSibling(record.getFileName() + ".tmp"), new byte[0]);
        Path unreadable = Files.write(record.resolveSibling(record.getFileName() +
                                                            Dse51SystemTables.UNREADABLE_SUFFIX + "-1"), new byte[0]);
        Logger logger = (Logger) LoggerFactory.getLogger(Dse51SystemTables.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
        try
        {
            assertThat(Dse51SystemTables.deleteRecordOfAnEarlierUpgrade()).isTrue();
            assertThat(record).doesNotExist();
            assertThat(temporary).doesNotExist();
            assertThat(unreadable).exists();
            assertThat(Dse51SystemTables.deleteRecordOfAnEarlierUpgrade()).isTrue();
        }
        finally
        {
            logger.detachAppender(appender);
            appender.stop();
            Files.deleteIfExists(unreadable);
        }
        assertThat(appender.list.stream().map(ILoggingEvent::getFormattedMessage))
        .containsExactly("DSE 5.1 compatibility mode: deleted " + record + ", the record of the values of DSE 5.1's " +
                         "columns of system.local and system.peers of an earlier upgrade");
    }

    /**
     * The record is read back as written, every field absent or present; a record not in that layout is refused, and
     * so is a value that cannot be written as a field.
     */
    @Test
    public void theRecordIsReadBackAsWritten() throws Throwable
    {
        Path record = temporaryFolder.newFolder().toPath().resolve(Dse51SystemTables.RECORD_FILE_NAME);
        InetAddress peer = InetAddress.getByName("10.0.51.2");
        List<Dse51SystemTables.StoredColumns> written =
            List.of(new Dse51SystemTables.StoredColumns(null, List.of("Cassandra", "Graph", "Search"), "Search",
                                                        true, DSE_VERSION),
                    new Dse51SystemTables.StoredColumns(peer, List.of(), null, null, null));
        Dse51SystemTables.write(record, written);
        assertThat(Files.readAllLines(record)).containsExactly(RECORD_HEADER,
                                                               "local\t5.1.51\tSearch\ttrue\tCassandra,Graph,Search",
                                                               "10.0.51.2\t-\t-\t-\t-");
        assertThat(Dse51SystemTables.load(record).stream().map(Object::toString))
        .containsExactlyElementsOf(written.stream().map(Object::toString).collect(Collectors.toList()));

        Files.write(record, List.of("something else"));
        assertThatThrownBy(() -> Dse51SystemTables.load(record))
        .hasMessage("not a record of the values of DSE 5.1's columns: its first line is not '" + RECORD_HEADER + "'");
        Files.write(record, List.of(RECORD_HEADER, "local\t5.1.51"));
        assertThatThrownBy(() -> Dse51SystemTables.load(record))
        .hasMessage("line 'local\t5.1.51' does not have five fields separated by tabs");
        Files.write(record, List.of(RECORD_HEADER, "local\t5.1.51\t-\tyes\t-"));
        assertThatThrownBy(() -> Dse51SystemTables.load(record))
        .hasMessage("line 'local\t5.1.51\t-\tyes\t-' has a graph field that is not true, false or -");
        Files.write(record, List.of(RECORD_HEADER, "local\t-\t-\t-\tCassandra,"));
        assertThatThrownBy(() -> Dse51SystemTables.load(record))
        .hasMessage("line 'local\t-\t-\t-\tCassandra,' has an empty element in its workloads field");
        // the workloads are sorted, as a set of text is
        Files.write(record, List.of(RECORD_HEADER, "local\t-\t-\t-\tSearch,Cassandra"));
        assertThat(Dse51SystemTables.load(record)).singleElement()
                                                  .satisfies(row -> assertThat(row.workloads)
                                                                    .containsExactly("Cassandra", "Search"));

        for (String value : List.of("", "-", "a\tb", "a\nb", "a,b"))
        {
            List<Dse51SystemTables.StoredColumns> refused =
                List.of(new Dse51SystemTables.StoredColumns(peer, List.of(value), null, null, null));
            assertThatThrownBy(() -> Dse51SystemTables.write(record, refused))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("the value '" + value + "' cannot be recorded: it is empty or -, or holds a tab, a line " +
                        "break or a comma");
        }
        assertThat(record.resolveSibling(record.getFileName() + ".tmp")).doesNotExist();
    }

    /** The workload fields of a row, as the checks of the DSE workloads read them. */
    @Test
    public void theWorkloadFieldsOfARow() throws Throwable
    {
        InetAddress peer = InetAddress.getByName("10.0.51.2");
        DseNodeWorkloads workloads = new Dse51SystemTables.StoredColumns(peer, List.of("Cassandra", "Search"), "Search",
                                                                         true, DSE_VERSION).nodeWorkloads();
        assertThat(workloads.peer).isEqualTo(peer);
        assertThat(workloads.workloads).containsExactly("Cassandra", "Search");
        assertThat(workloads.workload).isEqualTo("Search");
        assertThat(workloads.graph).isTrue();
        DseNodeWorkloads local = new Dse51SystemTables.StoredColumns(null, List.of(), null, null, DSE_VERSION)
                                 .nodeWorkloads();
        assertThat(local.peer).isNull();
        assertThat(local.workloads).isEmpty();
        assertThat(local.workload).isNull();
        assertThat(local.graph).isNull();
    }

    /** An expired value is not read; the values of the others are. */
    @Test
    public void anExpiredValueIsNotRead() throws Throwable
    {
        InetAddress peer = InetAddress.getByName("10.0.51.2");
        TableMetadata withDse51Columns = Dse51SystemTables.withDse51Columns(SystemKeyspace.LegacyPeers);
        long timestamp = FBUtilities.timestampMicros();
        long expiredAt = FBUtilities.nowInSeconds() - 10;
        addSSTable(peers(), withDse51Columns, withDse51Columns.regularAndStaticColumns(),
                   List.of(new RowUpdateBuilder(withDse51Columns, expiredAt, timestamp, 1, peer)
                           .add("dse_version", DSE_VERSION)
                           .buildUpdate()));
        addSSTable(peers(), withDse51Columns, withDse51Columns.regularAndStaticColumns(),
                   List.of(new RowUpdateBuilder(withDse51Columns, timestamp, peer)
                           .add("workload", "Cassandra")
                           .buildUpdate()));
        assertThat(Dse51SystemTables.read(peers())).singleElement().satisfies(columns -> {
            assertThat(columns.workload).isEqualTo("Cassandra");
            assertThat(columns.dseVersion).isNull();
        });
    }

    /** A flushed sstable that names none of the five (a memtable without a write of DSE's persister) holds none. */
    @Test
    public void aFlushedPeersSSTableNamingNoneOfTheFiveHoldsNoValue() throws Throwable
    {
        InetAddress peer = InetAddress.getByName("10.0.51.4");
        TableMetadata withDse51Columns = Dse51SystemTables.withDse51Columns(SystemKeyspace.LegacyPeers);
        PartitionUpdate update = new RowUpdateBuilder(withDse51Columns, FBUtilities.timestampMicros(), peer)
                                 .add("data_center", "dc1")
                                 .add("release_version", RELEASE)
                                 .buildUpdate();
        SSTableReader sstable = addSSTable(peers(), withDse51Columns, update.columns(), List.of(update));
        assertThat(headerColumns(sstable)).doesNotContainAnyElementsOf(Dse51SystemTables.COLUMNS.keySet());
        for (InternodeCompatibilityMode mode : InternodeCompatibilityMode.values())
            withMode(mode, () -> assertRows(execute("SELECT peer, data_center FROM " + PEERS + " WHERE peer = ?", peer),
                                            row(peer, "dc1")));
        assertThat(Dse51SystemTables.read(peers())).isEmpty();
    }

    /**
     * An sstable that cannot be read (here its statistics, truncated) is skipped with a WARN, and a partition whose
     * values cannot be decoded too; the values of the others are read. A start with that sstable skipped records
     * nothing; once it is gone, a start records the values without the partition that cannot be decoded. An sstable
     * of another format than big, which DSE 5.1 does not write, holds none.
     */
    @Test
    public void anUnreadableSSTableOrValueIsSkippedWithAWarnAndANonBigSSTableHoldsNone() throws Throwable
    {
        InetAddress readable = InetAddress.getByName("10.0.51.2");
        InetAddress corrupted = InetAddress.getByName("10.0.51.3");
        InetAddress malformed = InetAddress.getByName("10.0.51.5");
        TableMetadata withDse51Columns = Dse51SystemTables.withDse51Columns(SystemKeyspace.LegacyPeers);
        long timestamp = FBUtilities.timestampMicros();
        addSSTable(peers(), withDse51Columns, withDse51Columns.regularAndStaticColumns(),
                   List.of(new RowUpdateBuilder(withDse51Columns, timestamp, readable)
                           .add("dse_version", DSE_VERSION)
                           .buildUpdate()));
        SSTableReader broken = addSSTable(peers(), withDse51Columns, withDse51Columns.regularAndStaticColumns(),
                                          List.of(new RowUpdateBuilder(withDse51Columns, timestamp, corrupted)
                                                  .add("dse_version", DSE_VERSION)
                                                  .buildUpdate()));
        ColumnMetadata workloads = withDse51Columns.getColumn(UTF8Type.instance.decompose("workloads"));
        // a set of nine elements, cut after one byte
        ByteBuffer notASet = ByteBuffer.wrap(new byte[]{ 0, 0, 0, 9, 1 });
        Row malformedRow = BTreeRow.singleCellRow(Clustering.EMPTY, BufferCell.live(workloads, timestamp, notASet));
        ByteBuffer malformedPeer = InetAddressType.instance.decompose(malformed);
        DecoratedKey malformedKey = withDse51Columns.partitioner.decorateKey(malformedPeer);
        addSSTable(peers(), withDse51Columns, withDse51Columns.regularAndStaticColumns(),
                   List.of(PartitionUpdate.singleRowUpdate(withDse51Columns, malformedKey, malformedRow)));
        Path statistics = broken.descriptor.fileFor(SSTableFormat.Components.STATS).toPath();
        try (FileChannel channel = FileChannel.open(statistics, StandardOpenOption.WRITE))
        {
            channel.truncate(4);
        }
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        Logger logger = (Logger) LoggerFactory.getLogger(Dse51SystemTables.class);
        logger.addAppender(appender);
        try
        {
            assertThat(Dse51SystemTables.read(peers()))
            .singleElement()
            .satisfies(columns -> assertThat(columns.peer).isEqualTo(readable));
        }
        finally
        {
            logger.detachAppender(appender);
            appender.stop();
        }
        assertThat(appender.list).allSatisfy(event -> {
            assertThat(event.getLevel()).isEqualTo(Level.WARN);
            assertThat(event.getThrowableProxy()).isNotNull();
        });
        String sstableWarning = "DSE 5.1 compatibility mode: the values of DSE 5.1's columns of system.peers cannot " +
                                "be read from " + broken.descriptor + ", which is skipped; the values read from the " +
                                "other sstables may be older than those it holds, or belong to a peer it deletes";
        String partitionWarning = "DSE 5.1 compatibility mode: the values of DSE 5.1's columns of system.peers in " +
                                  "the partition 10.0.51.5 cannot be decoded, and are left out, also of the record " +
                                  "of a start in the mode";
        // the truncated statistics are not a disk error, only a failure to read them; the malformed value either
        assertThat(appender.list.stream().map(event -> event.getThrowableProxy().getClassName()))
        .doesNotContain(CorruptSSTableException.class.getName());
        assertThat(appender.list.stream().map(ILoggingEvent::getFormattedMessage))
        .containsExactlyInAnyOrder(sstableWarning, partitionWarning);
        // a start does not record values read with an sstable skipped: the next one, the cause removed, reads again
        List<String> warnings = loadOrReadAtStartCollecting(InternodeCompatibilityMode.DSE_5_1, Level.WARN);
        String notRecordedWarning = "DSE 5.1 compatibility mode: the values of DSE 5.1's columns of system.local and " +
                                    "system.peers are not recorded, since some could not be read (" +
                                    broken.descriptor + "): the next start reads them again. Remove the cause " +
                                    "reported above and restart the node now, before any compaction of " +
                                    "system.local and system.peers";
        assertThat(warnings).containsExactlyInAnyOrder(sstableWarning, partitionWarning, notRecordedWarning);
        assertThat(warnings).last().isEqualTo(notRecordedWarning);
        assertThat(Dse51SystemTables.stored()).singleElement()
                                              .satisfies(columns -> assertThat(columns.peer).isEqualTo(readable));
        assertThat(Dse51SystemTables.recordPath()).doesNotExist();
        try (LifecycleTransaction txn = peers().getTracker().tryModify(broken, OperationType.UNKNOWN))
        {
            txn.obsoleteOriginals();
            txn.finish();
        }
        // the partition that cannot be decoded, a lasting fault, is only left out of the record
        List<ILoggingEvent> events = loadOrReadAtStartCollectingEvents(InternodeCompatibilityMode.DSE_5_1, null);
        assertThat(events.stream()
                         .filter(event -> event.getLevel() == Level.WARN)
                         .map(ILoggingEvent::getFormattedMessage))
        .containsExactly(partitionWarning);
        assertThat(events.stream().map(ILoggingEvent::getFormattedMessage))
        .contains("DSE 5.1 compatibility mode: recorded the values of DSE 5.1's columns of system.local and " +
                  "system.peers in " + Dse51SystemTables.recordPath());
        assertThat(Dse51SystemTables.stored()).singleElement()
                                              .satisfies(columns -> assertThat(columns.peer).isEqualTo(readable));
        assertThat(Files.readAllLines(Dse51SystemTables.recordPath()))
        .containsExactly(RECORD_HEADER, "10.0.51.2\t5.1.51\t-\t-\t-");

        SSTableReader bti = addSSTable(peers(), withDse51Columns, withDse51Columns.regularAndStaticColumns(),
                                       List.of(new RowUpdateBuilder(withDse51Columns, timestamp, corrupted)
                                               .add("dse_version", DSE_VERSION)
                                               .buildUpdate()),
                                       BtiFormat.getInstance());
        assertThat(Dse51SystemTables.partitions(bti, withDse51Columns)).isEmpty();
    }

    /**
     * A corrupt chunk of the data of an sstable is a disk error: with {@code disk_failure_policy: stop} it stops the
     * start (the JVM is killed; here a killer that only records it), besides the WARN carrying the
     * {@code CorruptSSTableException}.
     */
    @Test
    public void aCorruptSSTableStopsTheStartWithTheDiskFailurePolicyStop() throws Throwable
    {
        TableMetadata withDse51Columns = Dse51SystemTables.withDse51Columns(SystemKeyspace.LegacyPeers);
        SSTableReader sstable = addSSTable(peers(), withDse51Columns, withDse51Columns.regularAndStaticColumns(),
                                           List.of(new RowUpdateBuilder(withDse51Columns, FBUtilities.timestampMicros(),
                                                                        InetAddress.getByName("10.0.51.2"))
                                                   .add("dse_version", DSE_VERSION)
                                                   .buildUpdate()));
        assertThat(sstable.compression).isTrue();
        try (FileChannel channel = FileChannel.open(sstable.getDataFile().toPath(), StandardOpenOption.WRITE))
        {
            channel.write(ByteBuffer.wrap(new byte[]{ 1, 2, 3, 4 }), 0);
        }
        Config.DiskFailurePolicy policy = DatabaseDescriptor.getDiskFailurePolicy();
        DatabaseDescriptor.setDiskFailurePolicy(Config.DiskFailurePolicy.stop);
        FileUtils.setFSErrorHandler(new DefaultFSErrorHandler());
        KillerForTests killer = new KillerForTests();
        JVMKiller previousKiller = JVMStabilityInspector.replaceKiller(killer);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        Logger logger = (Logger) LoggerFactory.getLogger(Dse51SystemTables.class);
        logger.addAppender(appender);
        try
        {
            assertThat(Dse51SystemTables.read(peers())).isEmpty();
            assertThat(killer.wasKilled()).isTrue();
        }
        finally
        {
            logger.detachAppender(appender);
            appender.stop();
            JVMStabilityInspector.replaceKiller(previousKiller);
            FileUtils.setFSErrorHandler(null);
            DatabaseDescriptor.setDiskFailurePolicy(policy);
        }
        assertThat(appender.list).singleElement().satisfies(event -> {
            assertThat(event.getLevel()).isEqualTo(Level.WARN);
            assertThat(event.getThrowableProxy().getClassName()).isEqualTo(CorruptSSTableException.class.getName());
        });
        try (LifecycleTransaction txn = peers().getTracker().tryModify(sstable, OperationType.UNKNOWN))
        {
            txn.obsoleteOriginals();
            txn.finish();
        }
    }

    /**
     * An {@code FSError} reading an sstable (injected here) is a disk error: with {@code disk_failure_policy: stop} it
     * stops the start (the JVM is killed; here a killer that only records it), after the WARN carrying it; the other
     * sstables are read. The error handler then also stops the transports of this test server, which uses none.
     */
    @Test
    @BMRule(name = "FailTheReadOfAnSSTable",
            targetClass = "org.apache.cassandra.db.Dse51SystemTables",
            targetMethod = "partitions",
            targetLocation = "AT ENTRY",
            condition = "$1.descriptor.id.toString()" +
                        ".equals(org.apache.cassandra.db.Dse51SystemTablesTest.failingSSTable)",
            action = "throw new org.apache.cassandra.io.FSReadError(\"injected\", " +
                     "new java.lang.Throwable(\"injected\"))")
    public void anFSErrorStopsTheStartWithTheDiskFailurePolicyStop() throws Throwable
    {
        TableMetadata withDse51Columns = Dse51SystemTables.withDse51Columns(SystemKeyspace.LegacyPeers);
        InetAddress readable = InetAddress.getByName("10.0.51.2");
        addSSTable(peers(), withDse51Columns, withDse51Columns.regularAndStaticColumns(),
                   List.of(new RowUpdateBuilder(withDse51Columns, FBUtilities.timestampMicros(), readable)
                           .add("dse_version", DSE_VERSION)
                           .buildUpdate()));
        SSTableReader failing = addSSTable(peers(), withDse51Columns, withDse51Columns.regularAndStaticColumns(),
                                           List.of(new RowUpdateBuilder(withDse51Columns, FBUtilities.timestampMicros(),
                                                                        InetAddress.getByName("10.0.51.3"))
                                                   .add("dse_version", DSE_VERSION)
                                                   .buildUpdate()));
        Config.DiskFailurePolicy policy = DatabaseDescriptor.getDiskFailurePolicy();
        DatabaseDescriptor.setDiskFailurePolicy(Config.DiskFailurePolicy.stop);
        FileUtils.setFSErrorHandler(new DefaultFSErrorHandler());
        KillerForTests killer = new KillerForTests();
        JVMKiller previousKiller = JVMStabilityInspector.replaceKiller(killer);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        Logger logger = (Logger) LoggerFactory.getLogger(Dse51SystemTables.class);
        logger.addAppender(appender);
        failingSSTable = failing.descriptor.id.toString();
        try
        {
            assertThat(Dse51SystemTables.read(peers()))
            .singleElement()
            .satisfies(columns -> assertThat(columns.peer).isEqualTo(readable));
            assertThat(killer.wasKilled()).isTrue();
        }
        finally
        {
            failingSSTable = null;
            logger.detachAppender(appender);
            appender.stop();
            JVMStabilityInspector.replaceKiller(previousKiller);
            FileUtils.setFSErrorHandler(null);
            DatabaseDescriptor.setDiskFailurePolicy(policy);
        }
        assertThat(appender.list).singleElement().satisfies(event -> {
            assertThat(event.getLevel()).isEqualTo(Level.WARN);
            assertThat(event.getFormattedMessage()).contains(failing.descriptor.toString());
            assertThat(event.getThrowableProxy().getClassName()).isEqualTo(FSReadError.class.getName());
        });
    }

    /**
     * The CQL of both tables with their internals ({@code DESCRIBE ... WITH INTERNALS}) lists the five as dropped
     * column records, as it lists {@code thrift_version} for {@code system.local}; without the internals it does not.
     */
    @Test
    public void theCqlWithInternalsListsTheFiveAsDropped()
    {
        String droppedAt = " USING TIMESTAMP " + Long.MAX_VALUE;
        List<String> records = List.of("DROPPED COLUMN RECORD workload text" + droppedAt,
                                       "DROPPED COLUMN RECORD workloads frozen<set<text>>" + droppedAt,
                                       "DROPPED COLUMN RECORD graph boolean" + droppedAt,
                                       "DROPPED COLUMN RECORD dse_version text" + droppedAt,
                                       "DROPPED COLUMN RECORD server_id text" + droppedAt);
        for (TableMetadata table : List.of(SystemKeyspace.Local, SystemKeyspace.LegacyPeers))
        {
            assertThat(table.toCqlString(true, false)).contains(records);
            assertThat(table.toCqlString(false, false)).doesNotContain("DROPPED COLUMN RECORD");
        }
        assertThat(SystemKeyspace.Local.toCqlString(true, false))
        .contains("DROPPED COLUMN RECORD thrift_version text USING TIMESTAMP " + Long.MAX_VALUE);
    }

    /**
     * A compaction of this software rewrites the rows without the values of the five columns, which are then gone; the
     * other columns are kept. The header of the sstable it writes still names the five, as dropped columns: it is the
     * union of the headers of its input sstables, as for any dropped column.
     */
    @Test
    public void aCompactionOfThisSoftwareRewritesTheRowsWithoutTheValuesOfTheFiveColumns() throws Throwable
    {
        InetAddress peer = InetAddress.getByName("10.0.51.2");
        TableMetadata withDse51Columns = Dse51SystemTables.withDse51Columns(SystemKeyspace.LegacyPeers);
        long timestamp = FBUtilities.timestampMicros();
        addSSTable(peers(), withDse51Columns, withDse51Columns.regularAndStaticColumns(),
                   List.of(new RowUpdateBuilder(withDse51Columns, timestamp, peer)
                           .add("data_center", "dc1")
                           .add("dse_version", DSE_VERSION)
                           .buildUpdate()));
        addSSTable(peers(), withDse51Columns, withDse51Columns.regularAndStaticColumns(),
                   List.of(new RowUpdateBuilder(withDse51Columns, timestamp + 1, peer)
                           .add("workloads", Set.of("Cassandra"))
                           .buildUpdate()));
        assertThat(Dse51SystemTables.read(peers())).hasSize(1);

        peers().forceMajorCompaction();

        assertThat(peers().getLiveSSTables()).singleElement().satisfies(sstable -> {
            assertThat(droppedColumnsOf(sstable))
            .containsExactlyInAnyOrderElementsOf(Dse51SystemTables.COLUMNS.keySet());
            assertThat(sstable.descriptor.version.isLatestVersion()).isTrue();
        });
        assertThat(Dse51SystemTables.read(peers())).isEmpty();
        assertRows(execute("SELECT peer, data_center FROM " + PEERS + " WHERE peer = ?", peer), row(peer, "dc1"));
    }

    /** The row {@code local} with the five columns, at {@code timestamp}, in a new sstable of {@code system.local}. */
    private void localSSTable(long timestamp)
    {
        TableMetadata withDse51Columns = Dse51SystemTables.withDse51Columns(SystemKeyspace.Local);
        PartitionUpdate update = new RowUpdateBuilder(withDse51Columns, timestamp, SystemKeyspace.LOCAL)
                                 .add("workload", "Analytics")
                                 .add("workloads", Set.of("Analytics", "Cassandra"))
                                 .add("graph", false)
                                 .add("dse_version", DSE_VERSION)
                                 .add("server_id", "02-42-AC-11-00-02")
                                 .buildUpdate();
        localSSTables.add(addSSTable(local(), withDse51Columns, update.columns(), List.of(update)));
    }

    private static void assertStoredPeer(InetAddress peer)
    {
        assertThat(Dse51SystemTables.stored()).singleElement().satisfies(columns -> {
            assertThat(columns.peer).isEqualTo(peer);
            assertThat(columns.workloads).containsExactly("Cassandra");
            assertThat(columns.workload).isNull();
            assertThat(columns.graph).isNull();
            assertThat(columns.dseVersion).isEqualTo(DSE_VERSION);
        });
    }

    /** The summary line of {@link Dse51SystemTables#loadOrReadAtStart}. */
    private static String summary(String from, int rows)
    {
        return "DSE 5.1 compatibility mode: values of DSE 5.1's columns " +
               "[workload, workloads, graph, dse_version] of system.local and system.peers " + from +
               "; rows holding a value: " + rows;
    }

    /** The record renamed aside, in the directory of {@code record}. */
    private static Path asideOf(Path record) throws IOException
    {
        try (Stream<Path> files = Files.list(record.getParent()))
        {
            String prefix = record.getFileName() + Dse51SystemTables.UNREADABLE_SUFFIX + '-';
            return Iterables.getOnlyElement(files.filter(file -> file.getFileName().toString().startsWith(prefix))
                                                 .collect(Collectors.toList()));
        }
    }

    private static List<String> loadOrReadAtStartCollecting(InternodeCompatibilityMode mode, Level level)
    throws Throwable
    {
        return loadOrReadAtStartCollectingEvents(mode, level).stream()
                                                             .map(ILoggingEvent::getFormattedMessage)
                                                             .collect(Collectors.toList());
    }

    /** The events {@link Dse51SystemTables#loadOrReadAtStart} logs in {@code mode} at {@code level}, or all if null. */
    private static List<ILoggingEvent> loadOrReadAtStartCollectingEvents(InternodeCompatibilityMode mode, Level level)
    throws Throwable
    {
        Logger logger = (Logger) LoggerFactory.getLogger(Dse51SystemTables.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
        try
        {
            withMode(mode, Dse51SystemTables::loadOrReadAtStart);
            return appender.list.stream()
                                .filter(event -> level == null || event.getLevel() == level)
                                .collect(Collectors.toList());
        }
        finally
        {
            logger.detachAppender(appender);
            appender.stop();
        }
    }

    /**
     * Writes {@code updates} with {@code written} (big format, a header naming {@code columns}) into a new sstable of
     * {@code cfs}, opened with the definition of {@code cfs} and added to its live sstables.
     */
    private static SSTableReader addSSTable(ColumnFamilyStore cfs, TableMetadata written,
                                            RegularAndStaticColumns columns, List<PartitionUpdate> updates)
    {
        return addSSTable(cfs, written, columns, updates, BigFormat.getInstance());
    }

    /** As {@link #addSSTable(ColumnFamilyStore, TableMetadata, RegularAndStaticColumns, List)}, in {@code format}. */
    private static SSTableReader addSSTable(ColumnFamilyStore cfs, TableMetadata written,
                                            RegularAndStaticColumns columns, List<PartitionUpdate> updates,
                                            SSTableFormat<?, ?> format)
    {
        File directory = cfs.getDirectories().getDirectoryForNewSSTables();
        Descriptor descriptor = cfs.newSSTableDescriptor(directory, format);
        TableMetadataRef ref = TableMetadataRef.forOfflineTools(written);
        SerializationHeader header = new SerializationHeader(true, written, columns, EncodingStats.NO_STATS);
        List<PartitionUpdate> sorted = new ArrayList<>(updates);
        sorted.sort(Comparator.comparing(PartitionUpdate::partitionKey));
        try (LifecycleTransaction txn = LifecycleTransaction.offline(OperationType.WRITE, ref);
             SSTableWriter writer = descriptor.getFormat().getWriterFactory()
                                              .builder(descriptor)
                                              .setTableMetadataRef(ref)
                                              .setKeyCount(sorted.size())
                                              .setSerializationHeader(header)
                                              .setMetadataCollector(new MetadataCollector(written.comparator))
                                              .addDefaultComponents(Collections.emptySet())
                                              .build(txn, null))
        {
            for (PartitionUpdate update : sorted)
                writer.append(update.unfilteredIterator());
            writer.finish(false, null);
            txn.finish();
        }
        SSTableReader sstable = SSTableReader.open(cfs, descriptor);
        cfs.addSSTable(sstable);
        return sstable;
    }

    /** The names of the regular columns in the header of {@code sstable}, as written. */
    private static Set<String> headerColumns(SSTableReader sstable) throws Exception
    {
        MetadataComponent component = sstable.descriptor.getMetadataSerializer()
                                                        .deserialize(sstable.descriptor, MetadataType.HEADER);
        return ((SerializationHeader.Component) component).getRegularColumns()
                                                          .keySet()
                                                          .stream()
                                                          .map(UTF8Type.instance::compose)
                                                          .collect(Collectors.toSet());
    }

    /** The names of the columns {@code sstable} is read with as dropped columns. */
    private static List<String> droppedColumnsOf(SSTableReader sstable)
    {
        return sstable.header.columns().regulars.stream()
                                                .filter(ColumnMetadata::isDropped)
                                                .map(column -> column.name.toString())
                                                .collect(Collectors.toList());
    }

    private static ColumnFamilyStore peers()
    {
        return Keyspace.open(SchemaConstants.SYSTEM_KEYSPACE_NAME).getColumnFamilyStore(SystemKeyspace.LEGACY_PEERS);
    }

    private static ColumnFamilyStore local()
    {
        return Keyspace.open(SchemaConstants.SYSTEM_KEYSPACE_NAME).getColumnFamilyStore(SystemKeyspace.LOCAL);
    }

    private interface Body
    {
        void run() throws Throwable;
    }

    private static void withMode(InternodeCompatibilityMode mode, Body body) throws Throwable
    {
        InternodeCompatibilityMode previous = DatabaseDescriptor.getInternodeCompatibilityMode();
        DatabaseDescriptor.setInternodeCompatibilityMode(mode);
        try
        {
            body.run();
        }
        finally
        {
            DatabaseDescriptor.setInternodeCompatibilityMode(previous);
        }
    }
}
