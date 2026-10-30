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

package org.apache.cassandra.tools.dsepreflight;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.fasterxml.jackson.databind.ObjectReader;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.db.marshal.TupleType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.db.rows.DeserializationHelper;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.dsecompat.DseFixtures;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableTxnWriter;
import org.apache.cassandra.io.sstable.SequenceBasedSSTableId;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.TOCComponent;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.sstable.format.bti.BtiFormat;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.service.paxos.Ballot;
import org.apache.cassandra.tools.dsepreflight.PaxosRollbackReport.Action;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JsonUtils;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.config.CassandraRelevantProperties.TRIE_INDEX_FORMAT_VERSION;
import static org.apache.cassandra.tools.dsepreflight.DseNodeFixture.row;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The Paxos rollback mode of {@code dsepreflight} ({@code --paxos-rollback}), run through its command line on a DSE
 * 6.8.65 node laid out from the fixtures ({@link DseNodeFixture}) as the rollback finds it: test tables added to its
 * schema, {@code system.paxos} sstables written with the rows Paxos v1 writes (a promise, an accepted proposal, a commit
 * that nulls the proposal, each with the timestamp of its ballot) in HCD 2.0's layout in the data directory, and in
 * DSE's layout in the {@code pre-hcd-<timestamp>} snapshot: the rows DSE 6.8.65 left on its test cluster
 * ({@link DseFixtures#paxosRows()}), and the cells of the sstables of the dump of a rollback
 * ({@link DseFixtures#paxosRollbackDump()}), written back cell by cell, in the {@code bti} format version {@code bb} DSE
 * 6.8 writes; and the sstable of {@code system.paxos} DSE 6.9.26 wrote itself ({@link DseFixtures#dse6926PaxosSSTableDirectory()}).
 */
public class PaxosRollbackTest
{
    private static final String TAG = "pre-hcd-20261003T155007Z";
    private static final TableId PAXOS_ID = TableId.fromString("b7b7f0c2-fd0a-3410-8c05-3ef614bb7c2d");
    private static final String PAXOS_DIRECTORY = "paxos-b7b7f0c2fd0a34108c053ef614bb7c2d";
    private static final int TTL = DseFixtures.PAXOS_DSE_TTL_SECONDS;
    /** The offset between the epochs of time UUIDs (1582-10-15) and of Unix, in 100 ns units. */
    private static final long UUID_EPOCH_OFFSET = 0x01b21dd213814000L;
    private static final int HCD_VERSION = MessagingService.VERSION_40;
    private static final List<String> KEYSPACES_KEY = List.of("keyspace_name");
    private static final List<String> TABLES_KEY = List.of("table_name");
    private static final List<String> COLUMNS_KEY = List.of("table_name", "column_name");
    private static final List<String> TYPES_KEY = List.of("type_name");
    private static final ObjectReader REPORT_READER = JsonUtils.JSON_OBJECT_MAPPER.readerFor(PaxosRollbackReport.class);
    private static final String SIMPLE = "org.apache.cassandra.locator.SimpleStrategy";
    private static final String NTS = "org.apache.cassandra.locator.NetworkTopologyStrategy";
    /** The keyspace of the table of DSE 6.9.26's sstable of system.paxos ({@code lab.lwt} on its test cluster). */
    private static final String DSE_6_9_26_KEYSPACE = "dse_6926";
    /** When DSE 6.9.26's sstable of system.paxos is read directly: 2026-10-03T12:00:00Z, before its cells expire. */
    private static final long DSE_6_9_26_READ_AT = 1791028800L;

    /** {@code rf3.px}: the id of {@code m5.px} in the rollback dump, so that the rows of the dump belong to it. */
    private static final UUID RF3 = UUID.fromString("0f59f853-bf41-11f1-9b7d-b57cc343566d");
    private static final UUID RF2 = UUID.fromString("11111111-bf41-11f1-9b7d-b57cc343566d");
    private static final UUID RF1 = UUID.fromString("22222222-bf41-11f1-9b7d-b57cc343566d");
    private static final UUID COMPOSITE = UUID.fromString("33333333-bf41-11f1-9b7d-b57cc343566d");
    private static final UUID TUPLE = UUID.fromString("44444444-bf41-11f1-9b7d-b57cc343566d");
    private static final UUID LIST = UUID.fromString("55555555-bf41-11f1-9b7d-b57cc343566d");
    private static final UUID UDT = UUID.fromString("66666666-bf41-11f1-9b7d-b57cc343566d");
    private static final UUID VARINT = UUID.fromString("77777777-bf41-11f1-9b7d-b57cc343566d");
    private static final UUID MULTI = UUID.fromString("88888888-bf41-11f1-9b7d-b57cc343566d");
    private static final UUID MULTI_LOCAL1 = UUID.fromString("99999999-bf41-11f1-9b7d-b57cc343566d");
    private static final UUID SIMPLE_MULTI = UUID.fromString("aaaaaaaa-bf41-11f1-9b7d-b57cc343566d");
    private static final UUID DC1_ONLY = UUID.fromString("bbbbbbbb-bf41-11f1-9b7d-b57cc343566d");
    private static final UUID TEXT = UUID.fromString("cccccccc-bf41-11f1-9b7d-b57cc343566d");
    private static final UUID LOCAL = UUID.fromString("dddddddd-bf41-11f1-9b7d-b57cc343566d");
    private static final UUID UNKNOWN_CLASS = UUID.fromString("eeeeeeee-bf41-11f1-9b7d-b57cc343566d");
    private static final UUID EVERYWHERE = UUID.fromString("ffffffff-bf41-11f1-9b7d-b57cc343566d");
    private static final UUID NO_REPLICA = UUID.fromString("12121212-bf41-11f1-9b7d-b57cc343566d");

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private int generation = 1;
    private final long base = timeUuidTimestamp(FBUtilities.timestampMicros() - TimeUnit.HOURS.toMicros(1));

    @BeforeClass
    public static void initialise()
    {
        // the sstables are written in DSE 6's trie version, as DSE 6.8 and an upgraded node in HCD_1 storage mode write them
        try (WithProperties properties = new WithProperties().set(TRIE_INDEX_FORMAT_VERSION, "bb"))
        {
            DatabaseDescriptor.clientInitialization();
            assertThat(BtiFormat.getInstance().getLatestVersion().version).isEqualTo("bb");
        }
    }

    // ---- the runs ----

    /** One run of the tool. */
    private static final class Run
    {
        final int exitCode;
        final String out;
        final String err;
        final PaxosRollbackReport report;

        Run(int exitCode, String out, String err, PaxosRollbackReport report)
        {
            this.exitCode = exitCode;
            this.out = out;
            this.err = err;
            this.report = report;
        }

        PaxosRollbackReport.Partition partition(UUID cfId, String rowKeyHex)
        {
            return report.partitions.stream().filter(p -> p.cfId.equals(cfId.toString()) && p.rowKey.equals(rowKeyHex)).findFirst()
                                    .orElseThrow(() -> new AssertionError("no partition " + cfId + " 0x" + rowKeyHex + " in\n" + out));
        }

        List<PaxosRollbackReport.Partition> partitions(UUID cfId)
        {
            return report.partitions.stream().filter(p -> p.cfId.equals(cfId.toString())).collect(Collectors.toList());
        }

        /** The reads of a partition as {@code <consistency> <action>}. */
        List<String> reads(UUID cfId, ByteBuffer rowKey)
        {
            return partition(cfId, hex(rowKey)).reads.stream().map(r -> r.consistency + " " + r.action).collect(Collectors.toList());
        }
    }

    private Run run(DseNodeFixture node, String... extra) throws IOException
    {
        return run(node, temporaryFolder.getRoot().toPath().resolve("paxos-" + UUID.randomUUID() + ".json"), extra);
    }

    private static Run run(DseNodeFixture node, Path json, String... extra) throws IOException
    {
        List<String> args = new ArrayList<>(Arrays.asList(node.args("--paxos-rollback", "--json", json.toString())));
        args.addAll(Arrays.asList(extra));
        Run run = run(args.toArray(new String[0]));
        if (!Files.exists(json))
            return new Run(run.exitCode, run.out, run.err, null);
        byte[] report = Files.readAllBytes(json);
        DsePreflightTest.assertNoPlanDecisionIds(new String(report, StandardCharsets.UTF_8));
        return new Run(run.exitCode, run.out, run.err, REPORT_READER.readValue(report));
    }

    /** A run of the tool with exactly these arguments. */
    private static Run run(String[] args)
    {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        int exitCode;
        try (PrintStream outStream = new PrintStream(out, true, StandardCharsets.UTF_8);
             PrintStream errStream = new PrintStream(err, true, StandardCharsets.UTF_8))
        {
            exitCode = DsePreflight.run(args, outStream, errStream);
        }
        DsePreflightTest.assertNoPlanDecisionIds(out.toString(StandardCharsets.UTF_8), err.toString(StandardCharsets.UTF_8));
        return new Run(exitCode, out.toString(StandardCharsets.UTF_8), err.toString(StandardCharsets.UTF_8), null);
    }

    private static Run assertExit(Run run, int exitCode)
    {
        assertThat(run.exitCode).as("exit code; output:\n%s%s", run.out, run.err).isEqualTo(exitCode);
        return run;
    }

    private static PaxosRollbackReport.Read only(PaxosRollbackReport.Partition partition)
    {
        assertThat(partition.reads).hasSize(1);
        return partition.reads.get(0);
    }

    // ---- the node ----

    /** The DSE 6.8.65 node of the fixtures with the test tables, a snapshot of its schema, and no Paxos state yet. */
    private DseNodeFixture node() throws IOException
    {
        // on its real path: a problem names the real path of a directory reached through a symbolic link
        DseNodeFixture node = DseNodeFixture.create(temporaryFolder.newFolder().toPath().toRealPath());
        addKeyspace(node, "rf3", simple(3));
        addKeyspace(node, "rf2", simple(2));
        addKeyspace(node, "rf1", simple(1));
        addKeyspace(node, "multi", Map.of("class", NTS, "dc1", "3", "dc2", "3"));
        addKeyspace(node, "multi_local1", Map.of("class", NTS, "dc1", "1", "dc2", "2"));
        addKeyspace(node, "simple_multi", simple(3));
        addKeyspace(node, "dc1_only", Map.of("class", NTS, "dc1", "3"));
        addKeyspace(node, "local_ks", Map.of("class", "org.apache.cassandra.locator.LocalStrategy"));
        addKeyspace(node, "unknown_class", Map.of("class", "com.example.ShardedStrategy", "shards", "4"));
        addKeyspace(node, "everywhere", Map.of("class", "org.apache.cassandra.locator.EverywhereStrategy"));
        addKeyspace(node, "no_replica", Map.of("class", NTS, "dc1", "0"));
        node.writeRows("system_schema", "types", KEYSPACES_KEY, TYPES_KEY,
                       row("rf3", "addr").with("field_names", List.of("street", "zip")).with("field_types", List.of("text", "int")));
        addTable(node, "rf3", "px", RF3, List.of(column("id", "int")));
        addTable(node, "rf2", "t", RF2, List.of(column("id", "int")));
        addTable(node, "rf1", "t", RF1, List.of(column("id", "int")));
        addTable(node, "rf3", "composite", COMPOSITE, List.of(column("a", "text"), column("b", "bigint")));
        addTable(node, "rf3", "tuple_key", TUPLE, List.of(column("k", "frozen<tuple<int, text>>")));
        addTable(node, "rf3", "list_key", LIST, List.of(column("k", "frozen<list<int>>")));
        addTable(node, "rf3", "udt_key", UDT, List.of(column("u", "frozen<addr>"), column("n", "int")));
        addTable(node, "rf3", "varint_key", VARINT, List.of(column("k", "varint")));
        addTable(node, "rf3", "text_key", TEXT, List.of(column("k", "text")));
        addTable(node, "multi", "t", MULTI, List.of(column("id", "int")));
        addTable(node, "multi_local1", "t", MULTI_LOCAL1, List.of(column("id", "int")));
        addTable(node, "simple_multi", "t", SIMPLE_MULTI, List.of(column("id", "int")));
        addTable(node, "dc1_only", "t", DC1_ONLY, List.of(column("id", "int")));
        addTable(node, "local_ks", "t", LOCAL, List.of(column("id", "int")));
        addTable(node, "unknown_class", "t", UNKNOWN_CLASS, List.of(column("id", "int")));
        addTable(node, "everywhere", "t", EVERYWHERE, List.of(column("id", "int")));
        addTable(node, "no_replica", "t", NO_REPLICA, List.of(column("id", "int")));
        snapshotSchema(node);
        return node;
    }

    /** {@link #node()} with an empty snapshot of system.paxos: DSE had no Paxos state when the snapshot was taken. */
    private DseNodeFixture nodeWithEmptyPaxosSnapshot() throws IOException
    {
        DseNodeFixture node = node();
        Files.createDirectories(paxosSnapshot(node));
        return node;
    }

    private static Map<String, String> simple(int replicationFactor)
    {
        return Map.of("class", SIMPLE, "replication_factor", Integer.toString(replicationFactor));
    }

    private static void addKeyspace(DseNodeFixture node, String keyspace, Map<String, String> replication) throws IOException
    {
        node.writeRows("system_schema", "keyspaces", KEYSPACES_KEY, List.of(), row(keyspace).with("durable_writes", true).with("replication", replication));
    }

    /** A partition key column of a test table. */
    private static final class KeyColumn
    {
        final String name;
        final String type;

        KeyColumn(String name, String type)
        {
            this.name = name;
            this.type = type;
        }
    }

    private static KeyColumn column(String name, String type)
    {
        return new KeyColumn(name, type);
    }

    /** Adds a table with the partition key columns given, a clustering column {@code ck} and a regular column {@code v}. */
    private static void addTable(DseNodeFixture node, String keyspace, String table, UUID id, List<KeyColumn> partitionKey) throws IOException
    {
        node.writeRows("system_schema", "tables", KEYSPACES_KEY, TABLES_KEY, row(keyspace, table).with("id", id).with("flags", Collections.singleton("compound")));
        List<DseNodeFixture.SchemaRow> columns = new ArrayList<>();
        for (int i = 0; i < partitionKey.size(); i++)
            columns.add(columnRow(keyspace, table, partitionKey.get(i).name, "partition_key", i, partitionKey.get(i).type));
        columns.add(columnRow(keyspace, table, "ck", "clustering", 0, "int"));
        columns.add(columnRow(keyspace, table, "v", "regular", -1, "int"));
        node.writeRows("system_schema", "columns", KEYSPACES_KEY, COLUMNS_KEY, columns.toArray(new DseNodeFixture.SchemaRow[0]));
    }

    private static DseNodeFixture.SchemaRow columnRow(String keyspace, String table, String name, String kind, int position, String type)
    {
        return row(keyspace, table, name).with("clustering_order", "clustering".equals(kind) ? "asc" : "none")
                                         .with("column_name_bytes", ByteBufferUtil.bytes(name))
                                         .with("kind", kind)
                                         .with("position", position)
                                         .with("type", type);
    }

    /** Copies the sstables of the schema tables into their snapshot {@link #TAG}, as {@code nodetool snapshot} links them. */
    private static void snapshotSchema(DseNodeFixture node) throws IOException
    {
        try (Stream<Path> tables = Files.list(node.data.resolve("system_schema")))
        {
            for (Path table : (Iterable<Path>) tables::iterator)
            {
                Path snapshot = Files.createDirectories(table.resolve("snapshots").resolve(TAG));
                try (Stream<Path> files = Files.list(table))
                {
                    for (Path file : (Iterable<Path>) files::iterator)
                        if (Files.isRegularFile(file))
                            Files.copy(file, snapshot.resolve(file.getFileName()), StandardCopyOption.REPLACE_EXISTING);
                }
            }
        }
    }

    private static Path paxosDirectory(DseNodeFixture node)
    {
        return node.data.resolve("system").resolve(PAXOS_DIRECTORY);
    }

    private static Path paxosSnapshot(DseNodeFixture node)
    {
        return paxosDirectory(node).resolve("snapshots").resolve(TAG);
    }

    // ---- system.paxos rows ----

    /** system.paxos as HCD 2.0 has it ({@code in_progress_read_ballot} included) or as DSE 6.8 has it. */
    private static TableMetadata paxosMetadata(boolean hcd)
    {
        TableMetadata.Builder builder = TableMetadata.builder("system", "paxos", PAXOS_ID)
                                                     .partitioner(Murmur3Partitioner.instance)
                                                     .addPartitionKeyColumn("row_key", BytesType.instance)
                                                     .addClusteringColumn("cf_id", UUIDType.instance)
                                                     .addRegularColumn("in_progress_ballot", TimeUUIDType.instance);
        if (hcd)
            builder.addRegularColumn("in_progress_read_ballot", TimeUUIDType.instance);
        return builder.addRegularColumn("most_recent_commit", BytesType.instance)
                      .addRegularColumn("most_recent_commit_at", TimeUUIDType.instance)
                      .addRegularColumn("most_recent_commit_version", Int32Type.instance)
                      .addRegularColumn("proposal", BytesType.instance)
                      .addRegularColumn("proposal_ballot", TimeUUIDType.instance)
                      .addRegularColumn("proposal_version", Int32Type.instance)
                      .build();
    }

    /** The 60-bit timestamp of a time UUID at {@code unixMicros}. */
    private static long timeUuidTimestamp(long unixMicros)
    {
        return unixMicros * 10 + UUID_EPOCH_OFFSET;
    }

    /** A version 1 UUID with the 60-bit timestamp given. */
    private static UUID timeUuid(long timestamp)
    {
        long msb = ((timestamp & 0xFFFFFFFFL) << 32) | (((timestamp >>> 32) & 0xFFFFL) << 16) | 0x1000L | ((timestamp >>> 48) & 0x0FFFL);
        UUID uuid = new UUID(msb, 0x9a8b7c6d5e4f3021L);
        assertThat(uuid.timestamp()).isEqualTo(timestamp);
        return uuid;
    }

    private static long micros(UUID ballot)
    {
        return (ballot.timestamp() - UUID_EPOCH_OFFSET) / 10;
    }

    /**
     * The cells of one write of a row of system.paxos, at the timestamp given, with the TTL given (none when null); a null
     * value deletes the cell.
     */
    private static PartitionUpdate cells(TableMetadata paxos, ByteBuffer rowKey, UUID cfId, long timestamp, Integer ttl, Object... columnsAndValues)
    {
        Map<String, Object> values = new LinkedHashMap<>();
        for (int i = 0; i < columnsAndValues.length; i += 2)
        {
            Object value = columnsAndValues[i + 1];
            values.put((String) columnsAndValues[i], value instanceof UUID ? ByteBufferUtil.bytes((UUID) value) : value);
        }
        PartitionUpdate.SimpleBuilder builder = PartitionUpdate.simpleBuilder(paxos, rowKey).timestamp(timestamp);
        if (ttl != null)
            builder.ttl(ttl);
        Row.SimpleBuilder row = builder.row(ByteBufferUtil.bytes(cfId));
        values.forEach(row::add);
        return builder.build();
    }

    private static PartitionUpdate cells(TableMetadata paxos, ByteBuffer rowKey, UUID cfId, long timestamp, Object... columnsAndValues)
    {
        return cells(paxos, rowKey, cfId, timestamp, TTL, columnsAndValues);
    }

    /** A promise, as Paxos v1 saves it. */
    private static PartitionUpdate promise(TableMetadata paxos, ByteBuffer rowKey, UUID cfId, UUID ballot)
    {
        return cells(paxos, rowKey, cfId, micros(ballot), "in_progress_ballot", ballot);
    }

    /** An accepted proposal, as Paxos v1 saves it. */
    private static PartitionUpdate accept(TableMetadata paxos, ByteBuffer rowKey, UUID cfId, UUID ballot, ByteBuffer update, int version)
    {
        return cells(paxos, rowKey, cfId, micros(ballot), "proposal_ballot", ballot, "proposal", update, "proposal_version", version);
    }

    /** A commit, as Paxos v1 saves it: the proposal is deleted. */
    private static PartitionUpdate commit(TableMetadata paxos, ByteBuffer rowKey, UUID cfId, UUID ballot, ByteBuffer update, int version)
    {
        return cells(paxos, rowKey, cfId, micros(ballot), "proposal_ballot", null, "proposal", null, "proposal_version", null,
                     "most_recent_commit_at", ballot, "most_recent_commit", update, "most_recent_commit_version", version);
    }

    /** An empty proposal of HCD 2.0 for the table {@code cfId}: what serial reads propose. */
    private PartitionUpdate emptyProposal(TableMetadata paxos, UUID cfId, ByteBuffer rowKey)
    {
        return accept(paxos, rowKey, cfId, timeUuid(base), emptyUpdate(cfId, rowKey, HCD_VERSION), HCD_VERSION);
    }

    /** An update of the table {@code cfId} with no row, serialized at {@code version}. */
    private static ByteBuffer emptyUpdate(UUID cfId, ByteBuffer rowKey, int version)
    {
        TableMetadata table = TableMetadata.builder("ks", "t", TableId.fromUUID(cfId)).partitioner(Murmur3Partitioner.instance)
                                           .addPartitionKeyColumn("k", BytesType.instance).build();
        return PartitionUpdate.toBytes(PartitionUpdate.emptyUpdate(table, table.partitioner.decorateKey(rowKey)), version);
    }

    /** An update of {@code rf3.px} setting {@code v}, serialized at {@code version}. */
    private static ByteBuffer update(int id, int value, int version)
    {
        TableMetadata table = TableMetadata.builder("rf3", "px", TableId.fromUUID(RF3)).partitioner(Murmur3Partitioner.instance)
                                           .addPartitionKeyColumn("id", Int32Type.instance).addClusteringColumn("ck", Int32Type.instance)
                                           .addRegularColumn("v", Int32Type.instance).build();
        PartitionUpdate.SimpleBuilder builder = PartitionUpdate.simpleBuilder(table, id).timestamp(FBUtilities.timestampMicros());
        builder.row(1).add("v", value);
        return PartitionUpdate.toBytes(builder.build(), version);
    }

    /** Writes the updates, merged by partition, as one sstable of system.paxos in {@code directory}; returns its descriptor. */
    private Descriptor writeSSTable(Path directory, TableMetadata paxos, SSTableFormat<?, ?> format, List<PartitionUpdate> updates) throws IOException
    {
        Files.createDirectories(directory);
        Map<ByteBuffer, List<PartitionUpdate>> byKey = new LinkedHashMap<>();
        for (PartitionUpdate update : updates)
            byKey.computeIfAbsent(update.partitionKey().getKey(), k -> new ArrayList<>()).add(update);
        List<PartitionUpdate> merged = new ArrayList<>();
        byKey.values().forEach(list -> merged.add(PartitionUpdate.merge(list)));
        merged.sort((a, b) -> a.partitionKey().compareTo(b.partitionKey()));
        Descriptor descriptor = new Descriptor(new File(directory), "system", "paxos", new SequenceBasedSSTableId(generation++), format);
        SerializationHeader header = new SerializationHeader(true, paxos, paxos.regularAndStaticColumns(), EncodingStats.NO_STATS);
        try (SSTableTxnWriter writer = SSTableTxnWriter.create(TableMetadataRef.forOfflineTools(paxos), descriptor, merged.size(), 0, null, false, header,
                                                               Collections.emptyList(), null))
        {
            for (PartitionUpdate update : merged)
                writer.append(update.unfilteredIterator());
            writer.finish(false, null);
        }
        return descriptor;
    }

    private static ByteBuffer intKey(int id)
    {
        return ByteBufferUtil.bytes(id);
    }

    private static String hex(ByteBuffer bytes)
    {
        return ByteBufferUtil.bytesToHex(bytes);
    }

    // ---- the open rounds ----

    @Test
    public void listsTheOpenRoundsOfHcdSSTablesAndWritesTheSerialReads() throws IOException
    {
        DseNodeFixture node = nodeWithEmptyPaxosSnapshot();
        TableMetadata paxos = paxosMetadata(true);
        int version = HCD_VERSION;
        UUID c1 = timeUuid(base);
        UUID p2 = timeUuid(base + 20);
        // the commit's time_low is near its end, the later proposal's has wrapped: the text of the proposal sorts first
        long wrap = (base & ~0xFFFFFFFFL) | 0xFFFFFF00L;
        UUID early = timeUuid(wrap);
        UUID late = timeUuid(wrap + 0x200);
        assertThat(late.toString()).isLessThan(early.toString());
        assertThat(late.timestamp()).isGreaterThan(early.timestamp());

        List<PartitionUpdate> updates = new ArrayList<>();
        // 1: committed (the commit deletes the proposal): not open
        updates.add(promise(paxos, intKey(1), RF3, c1));
        updates.add(accept(paxos, intKey(1), RF3, c1, update(1, 10, version), version));
        updates.add(commit(paxos, intKey(1), RF3, timeUuid(base + 1), update(1, 10, version), version));
        // 2: a non-empty proposal accepted after the last commit: open
        updates.add(commit(paxos, intKey(2), RF3, c1, update(2, 1, version), version));
        updates.add(promise(paxos, intKey(2), RF3, p2));
        updates.add(accept(paxos, intKey(2), RF3, p2, update(2, 2, version), version));
        // 3: an empty proposal and no commit (a serial read on a partition never written by an LWT): open
        updates.add(promise(paxos, intKey(3), RF3, p2));
        updates.add(accept(paxos, intKey(3), RF3, p2, emptyUpdate(RF3, intKey(3), version), version));
        // 4: a promise only: not open
        updates.add(commit(paxos, intKey(4), RF3, c1, update(4, 1, version), version));
        updates.add(promise(paxos, intKey(4), RF3, p2));
        // 5: the proposal is later than the commit although its text sorts before it: open
        updates.add(commit(paxos, intKey(5), RF3, early, update(5, 1, version), version));
        updates.add(cells(paxos, intKey(5), RF3, micros(late), "proposal_ballot", late, "proposal", update(5, 2, version), "proposal_version", version));
        // 6: the proposal is earlier than the commit although its text sorts after it: not open
        updates.add(cells(paxos, intKey(6), RF3, micros(early), "proposal_ballot", early, "proposal", update(6, 2, version), "proposal_version", version));
        updates.add(cells(paxos, intKey(6), RF3, micros(late), "most_recent_commit_at", late, "most_recent_commit", update(6, 1, version),
                          "most_recent_commit_version", version));
        // 7: proposal and commit at the same ballot: not open
        updates.add(cells(paxos, intKey(7), RF3, micros(p2), "proposal_ballot", p2, "proposal", update(7, 2, version), "proposal_version", version,
                          "most_recent_commit_at", p2, "most_recent_commit", update(7, 2, version), "most_recent_commit_version", version));
        writeSSTable(paxosDirectory(node), paxos, BtiFormat.getInstance(), updates);

        Path cql = temporaryFolder.getRoot().toPath().resolve("reads.cql");
        Map<String, String> before = node.fingerprint();
        Run run = assertExit(run(node, "--cql", cql.toString()), DsePreflight.EXIT_READY);
        assertThat(node.fingerprint()).as("the node's files after the run").isEqualTo(before);
        assertThat(run.report.result).isEqualTo(PaxosRollbackReport.READS_LISTED);
        assertThat(run.report.kind).isEqualTo(PaxosRollbackReport.KIND);
        assertThat(run.report.dataCenter).isEqualTo("dc1");
        assertThat(run.report.singleDataCenter).isTrue();
        assertThat(run.report.snapshot).isEqualTo(TAG);
        assertThat(run.report.problems).isEmpty();
        assertThat(run.report.schemas).containsExactly("data directories", "snapshot " + TAG);
        assertThat(run.report.rowsRead).isEqualTo(7);
        assertThat(run.report.sstables).hasSize(1);
        assertThat(run.report.sstables.get(0).source).isEqualTo("data directory");
        assertThat(run.report.sstables.get(0).format).isEqualTo("bti bb");
        assertThat(run.report.sstables.get(0).openRows).isEqualTo(3);
        assertThat(run.report.partitions).extracting(p -> p.rowKey).containsExactly(hex(intKey(2)), hex(intKey(3)), hex(intKey(5)));

        PaxosRollbackReport.Partition two = run.partition(RF3, hex(intKey(2)));
        assertThat(two.keyspace).isEqualTo("rf3");
        assertThat(two.table).isEqualTo("px");
        assertThat(two.where).isEqualTo("id = 2");
        assertThat(two.select).isEqualTo("SELECT id FROM rf3.px WHERE id = 2 LIMIT 1;");
        assertThat(only(two).consistency).isEqualTo("SERIAL");
        assertThat(only(two).action).isEqualTo(Action.READ);
        assertThat(only(two).reason).isEqualTo("keyspace rf3: SimpleStrategy, replication factor 3; every replica is in this node's data center, so SERIAL and " +
                                               "LOCAL_SERIAL are the same quorum: total replication factor 3");
        assertThat(two.openRows).hasSize(1);
        PaxosRollbackReport.OpenRow row = two.openRows.get(0);
        assertThat(row.source).isEqualTo("data directory");
        assertThat(row.writtenBy).isEqualTo("HCD 2.0");
        assertThat(row.version).isEqualTo(version);
        assertThat(row.emptyProposal).isFalse();
        assertThat(row.note).isNull();
        assertThat(row.proposalBallot).isEqualTo(p2.toString());
        assertThat(row.mostRecentCommitAt).isEqualTo(c1.toString());

        PaxosRollbackReport.OpenRow three = run.partition(RF3, hex(intKey(3))).openRows.get(0);
        assertThat(three.emptyProposal).isTrue();
        assertThat(three.mostRecentCommitAt).isNull();
        assertThat(run.partition(RF3, hex(intKey(5))).openRows.get(0).proposalBallot).isEqualTo(late.toString());

        String serial = PaxosRollback.cql(run.report, PaxosRollback.SERIAL);
        assertThat(new String(Files.readAllBytes(cql), StandardCharsets.UTF_8)).isEqualTo(serial);
        assertThat(serial).endsWith("CONSISTENCY SERIAL;\n" +
                                    "SELECT id FROM rf3.px WHERE id = 2 LIMIT 1;\n" +
                                    "SELECT id FROM rf3.px WHERE id = 3 LIMIT 1;\n" +
                                    "SELECT id FROM rf3.px WHERE id = 5 LIMIT 1;\n")
                          .contains("Run it through another node (a DSE node while one exists)")
                          .contains("Do not start DSE before every read has succeeded.");
        Path local = temporaryFolder.getRoot().toPath().resolve("reads-local-serial.cql");
        assertThat(new String(Files.readAllBytes(local), StandardCharsets.UTF_8)).contains("Run it through a node of data center dc1 (this node's) other than this node")
                                                                                 .endsWith("-- No read is needed at LOCAL_SERIAL.\n")
                                                                                 .doesNotContain("CONSISTENCY");
        assertThat(run.report.cqlFile).isEqualTo(cql.toAbsolutePath().toString());
        assertThat(run.report.cqlLocalSerialFile).isEqualTo(local.toAbsolutePath().toString());
        assertThat(run.out).contains("Precondition, not verified by the tool: HCD 2.0 stopped after a successful drain")
                           .contains("Result: READS_LISTED (3 partitions with an open round; reads listed: 3 at SERIAL, 0 at LOCAL_SERIAL; 0 need no read, " +
                                     "0 cannot be protected, 0 need their serial consistency, 0 to check, 0 to handle by hand)")
                           .contains("  rf3.px\n    READ at SERIAL: keyspace rf3: SimpleStrategy, replication factor 3; ")
                           .contains("    id = 3: READ at SERIAL\n")
                           .contains("open in data directory, " + new File(run.report.sstables.get(0).path).name() +
                                     ", written by HCD 2.0 (version " + version + "): empty proposal " + p2 + ", no commit")
                           .contains("cqlsh input written to " + cql.toAbsolutePath() + " (SERIAL, through any other node) and " + local.toAbsolutePath());
    }

    /** The cells of the sstables of the rollback dump, by sstable ({@code bb-7}, {@code bb-8}, {@code bb-3}). */
    private static Map<String, List<PartitionUpdate>> rollbackDumpSSTables() throws IOException
    {
        Map<String, List<PartitionUpdate>> sstables = new LinkedHashMap<>();
        TableMetadata hcd = paxosMetadata(true);
        TableMetadata dse = paxosMetadata(false);
        Pattern section = Pattern.compile("^### .*/(bb-\\d+)-bti-Data\\.db$");
        Pattern rowLine = Pattern.compile("^\\[([0-9a-f]+)]@\\d+ Row\\[info=\\[ts=EMPTY] ]: ([0-9a-f-]+) \\| (.*)$");
        Pattern cellPattern = Pattern.compile("\\[(\\w+)=(\\S+) ts=(\\d+)(?: ttl=(\\d+))?(?: ldt=\\d+)?]");
        String current = null;
        int cellCount = 0;
        for (String line : Files.readAllLines(DseFixtures.paxosRollbackDump()))
        {
            Matcher m = section.matcher(line);
            if (m.matches())
            {
                current = m.group(1);
                sstables.put(current, new ArrayList<>());
                continue;
            }
            m = rowLine.matcher(line);
            if (!m.matches())
                continue;
            ByteBuffer key = ByteBufferUtil.hexToBytes(m.group(1));
            UUID cfId = UUID.fromString(m.group(2));
            Matcher cell = cellPattern.matcher(m.group(3));
            while (cell.find())
            {
                String name = cell.group(1);
                String text = cell.group(2);
                Object value = "<tombstone>".equals(text) ? null
                                                          : name.endsWith("_version") ? (Object) Integer.valueOf(text)
                                                          : name.endsWith("ballot") || name.endsWith("_at") ? (Object) UUID.fromString(text)
                                                          : ByteBufferUtil.hexToBytes(text);
                Integer ttl = cell.group(4) == null ? null : Integer.valueOf(cell.group(4));
                sstables.get(current).add(cells("bb-3".equals(current) ? dse : hcd, key, cfId, Long.parseLong(cell.group(3)), ttl, name, value));
                cellCount++;
            }
        }
        assertThat(sstables.keySet()).containsExactly("bb-7", "bb-8", "bb-3");
        assertThat(cellCount).isEqualTo(15 * 7 + 5 + 3 * 7 + 2 * 7 + 5);
        return sstables;
    }

    @Test
    public void readsTheRowsOfTheRollbackDumpCellByCell() throws IOException
    {
        DseNodeFixture node = node();
        Map<String, List<PartitionUpdate>> sstables = rollbackDumpSSTables();
        // before the restore: bb-7 and bb-8 in the data directory, DSE's bb-3 in the snapshot
        writeSSTable(paxosDirectory(node), paxosMetadata(true), BtiFormat.getInstance(), sstables.get("bb-7"));
        writeSSTable(paxosDirectory(node), paxosMetadata(true), BtiFormat.getInstance(), sstables.get("bb-8"));
        writeSSTable(paxosSnapshot(node), paxosMetadata(false), BtiFormat.getInstance(), sstables.get("bb-3"));

        Map<String, String> before = node.fingerprint();
        Run run = assertExit(run(node), DsePreflight.EXIT_READY);
        assertThat(node.fingerprint()).as("the node's files after the run").isEqualTo(before);
        assertThat(run.report.sstables).extracting(s -> s.source + " " + s.rows + " " + s.openRows)
                                       .containsExactly("data directory 16 15", "data directory 3 0", "snapshot " + TAG + " 3 2");
        assertRollbackDumpPartitions(run, "data directory", false);
    }

    @Test
    public void readsTheQuarantineOfTheRollbackDumpAfterTheRestore() throws IOException
    {
        DseNodeFixture node = node();
        Map<String, List<PartitionUpdate>> sstables = rollbackDumpSSTables();
        // after the restore, as the dump shows it: DSE's bb-3 in the snapshot and back in the data directory, HCD 2.0's
        // bb-7 and bb-8 in a quarantine
        Descriptor dse = writeSSTable(paxosSnapshot(node), paxosMetadata(false), BtiFormat.getInstance(), sstables.get("bb-3"));
        try (Stream<Path> files = Files.list(paxosSnapshot(node)))
        {
            for (Path file : (Iterable<Path>) files::iterator)
                Files.copy(file, paxosDirectory(node).resolve(file.getFileName()));
        }
        Path quarantine = node.root.resolve("hcd-written-quarantine").resolve("system").resolve(PAXOS_DIRECTORY);
        writeSSTable(quarantine, paxosMetadata(true), BtiFormat.getInstance(), sstables.get("bb-7"));
        writeSSTable(quarantine, paxosMetadata(true), BtiFormat.getInstance(), sstables.get("bb-8"));

        // without the quarantine, the rows HCD 2.0 wrote are missing: a problem
        Run restored = assertExit(run(node), DsePreflight.EXIT_BLOCKED);
        assertThat(restored.report.problems).containsExactly(RESTORED_PROBLEM);
        assertThat(restored.report.partitions).extracting(p -> p.where).containsExactly("id = 10", "id = 11");
        assertThat(PaxosRollback.cql(restored.report, PaxosRollback.SERIAL)).startsWith("-- INCOMPLETE: the report lists 1 problem. See the report before running " +
                                                                                        "this file.\n-- SERIAL reads completing");

        Run run = assertExit(run(node, "--paxos-dir", quarantine.toString()), DsePreflight.EXIT_READY);
        assertThat(run.report.problems).isEmpty();
        assertThat(run.report.sstables).extracting(s -> s.source + " " + new File(s.path).name() + " " + s.rows + " " + s.openRows)
                                       .containsExactly("data directory " + dse.baseFile().name() + " 3 2",
                                                        "snapshot " + TAG + ' ' + dse.baseFile().name() + " 3 2",
                                                        "--paxos-dir " + quarantine + " bb-2-bti 16 15",
                                                        "--paxos-dir " + quarantine + " bb-3-bti 3 0");
        assertRollbackDumpPartitions(run, "--paxos-dir " + quarantine, true);
        assertThat(run.report.directories).extracting(d -> d.source + ' ' + d.path + ' ' + d.sstables + ' ' + d.rows)
                                          .containsExactly("data directory " + paxosDirectory(node) + " 1 3",
                                                           "snapshot " + TAG + ' ' + paxosSnapshot(node) + " 1 3",
                                                           "--paxos-dir " + quarantine + ' ' + quarantine + " 2 19");
        assertThat(run.out).contains("system.paxos directories read:\n" +
                                     "  data directory: " + paxosDirectory(node) + " (1 sstable, 3 rows)\n" +
                                     "  snapshot " + TAG + ": " + paxosSnapshot(node) + " (1 sstable, 3 rows)\n" +
                                     "  --paxos-dir " + quarantine + ": " + quarantine + " (2 sstables, 19 rows)\n" +
                                     "system.paxos sstables read, each on its own (25 rows):\n");
    }

    /**
     * A layout seen in a rollback: a {@code --paxos-dir} naming the quarantine, or its system directory, instead of the table
     * directory of system.paxos in it read nothing, and the run used to succeed with nothing to read although HCD 2.0's
     * open rounds were in the table directory.
     */
    @Test
    public void reportsAPaxosDirectoryThatHoldsNoPaxosSSTable() throws IOException
    {
        DseNodeFixture node = node();
        Map<String, List<PartitionUpdate>> sstables = rollbackDumpSSTables();
        writeSSTable(paxosSnapshot(node), paxosMetadata(false), BtiFormat.getInstance(), sstables.get("bb-3"));
        try (Stream<Path> files = Files.list(paxosSnapshot(node)))
        {
            for (Path file : (Iterable<Path>) files::iterator)
                Files.copy(file, paxosDirectory(node).resolve(file.getFileName()));
        }
        Path root = node.root.resolve("hcd-written-quarantine").resolve("20261003T163700Z");
        Path system = root.resolve("system");
        Path quarantine = system.resolve(PAXOS_DIRECTORY);
        writeSSTable(quarantine, paxosMetadata(true), BtiFormat.getInstance(), sstables.get("bb-7"));
        writeSSTable(quarantine, paxosMetadata(true), BtiFormat.getInstance(), sstables.get("bb-8"));
        // another table moved by the restore next to system.paxos
        Files.createDirectories(system.resolve("batches-919a4bc57a333573b03e13fc3f68b465"));

        String expected = PAXOS_DIRECTORY_EXPECTED;
        String notDescended = " (the tool reads the directories given, not their subdirectories)";
        String suggested = "; it holds the directory " + quarantine + " (2 sstables): pass " + quarantine + " instead" + notDescended;

        // the quarantine itself, and its system directory: a problem naming the table directory to pass, which is not read
        for (Path parent : List.of(root, system))
        {
            Run run = assertExit(run(node, "--paxos-dir", parent.toString()), DsePreflight.EXIT_BLOCKED);
            assertThat(run.report.result).isEqualTo(PaxosRollbackReport.NEEDS_ATTENTION);
            assertThat(run.report.problems).containsExactly("--paxos-dir " + parent + " holds no system.paxos sstable (no *-Data.db file): nothing was read " +
                                                            "from it. --paxos-dir" + expected + suggested);
            assertThat(run.report.sstables).extracting(s -> s.source).containsExactly("data directory", "snapshot " + TAG);
            assertThat(run.report.directories).extracting(d -> d.source + ' ' + d.sstables + ' ' + d.rows)
                                              .containsExactly("data directory 1 3", "snapshot " + TAG + " 1 3", "--paxos-dir " + parent + " 0 0");
            assertThat(run.out).contains("  --paxos-dir " + parent + ": " + parent + " (0 sstables, 0 rows)\n")
                               .contains("  PROBLEM: --paxos-dir " + parent + " holds no system.paxos sstable");
        }

        // the quarantine with its table directory also given: the table directory is read, the quarantine is still a problem
        Run alsoGiven = assertExit(run(node, "--paxos-dir", root.toString(), "--paxos-dir", quarantine.toString()), DsePreflight.EXIT_BLOCKED);
        assertThat(alsoGiven.report.problems).containsExactly("--paxos-dir " + root + " holds no system.paxos sstable (no *-Data.db file): nothing was read " +
                                                              "from it. --paxos-dir" + expected + "; it holds the directory " + quarantine + " (2 sstables, " +
                                                              "already read as --paxos-dir " + quarantine + "): drop this --paxos-dir" + notDescended);
        assertRollbackDumpPartitions(alsoGiven, "--paxos-dir " + quarantine, true);
        assertThat(alsoGiven.report.directories).extracting(d -> d.source + ' ' + d.sstables + ' ' + d.rows)
                                                .containsExactly("data directory 1 3", "snapshot " + TAG + " 1 3", "--paxos-dir " + root + " 0 0",
                                                                 "--paxos-dir " + quarantine + " 2 19");

        // the system directory of the data directory: its table directory is already read
        Path dataSystem = paxosDirectory(node).getParent();
        Run liveSystem = assertExit(run(node, "--paxos-dir", dataSystem.toString()), DsePreflight.EXIT_BLOCKED);
        assertThat(liveSystem.report.problems).containsExactly("--paxos-dir " + dataSystem + " holds no system.paxos sstable (no *-Data.db file): nothing was " +
                                                               "read from it. --paxos-dir" + expected + "; it holds the directory " + paxosDirectory(node) +
                                                               " (1 sstable, already read as data directory): drop this --paxos-dir" + notDescended);

        // an empty directory, and an empty table directory of system.paxos, named as since Cassandra 2.1 or before
        Path empty = Files.createDirectories(node.root.resolve("empty"));
        Run emptyRun = assertExit(run(node, "--paxos-dir", empty.toString()), DsePreflight.EXIT_BLOCKED);
        assertThat(emptyRun.report.problems).containsExactly("--paxos-dir " + empty + " holds no system.paxos sstable (no *-Data.db file): nothing was read " +
                                                             "from it. --paxos-dir" + expected);
        Path otherQuarantine = node.root.resolve("other-quarantine");
        for (String name : List.of(PAXOS_DIRECTORY, "paxos"))
        {
            Path emptyTable = Files.createDirectories(otherQuarantine.resolve("system").resolve(name));
            // given as is and with a trailing ".": recognised by its normalized path
            for (Path given : List.of(emptyTable, emptyTable.resolve(".")))
            {
                Run emptyTableRun = assertExit(run(node, "--paxos-dir", given.toString()), DsePreflight.EXIT_BLOCKED);
                assertThat(emptyTableRun.report.problems).containsExactly("--paxos-dir " + given + " holds no system.paxos sstable (no *-Data.db file): " +
                                                                          "nothing was read from it. --paxos-dir" + expected +
                                                                          EMPTY_PAXOS_DIRECTORY_ACCEPTED);
            }
        }
        // a quarantine whose table directories of system.paxos are all empty
        Run allEmpty = assertExit(run(node, "--paxos-dir", otherQuarantine.toString()), DsePreflight.EXIT_BLOCKED);
        assertThat(allEmpty.report.problems).containsExactly("--paxos-dir " + otherQuarantine + " holds no system.paxos sstable (no *-Data.db file): nothing " +
                                                             "was read from it. --paxos-dir" + expected + "; it holds the directories " +
                                                             otherQuarantine.resolve("system").resolve("paxos") + " (0 sstables), " +
                                                             otherQuarantine.resolve("system").resolve(PAXOS_DIRECTORY) + " (0 sstables), none with an sstable: " +
                                                             "check where the restore moved HCD 2.0's sstables" + notDescended);

        // the only table directory of a quarantine, empty: passing it leads to the explanation
        Path emptyQuarantine = node.root.resolve("empty-quarantine");
        Path emptyQuarantineTable = Files.createDirectories(emptyQuarantine.resolve("system").resolve(PAXOS_DIRECTORY));
        Run onlyEmpty = assertExit(run(node, "--paxos-dir", emptyQuarantine.toString()), DsePreflight.EXIT_BLOCKED);
        assertThat(onlyEmpty.report.problems).containsExactly("--paxos-dir " + emptyQuarantine + " holds no system.paxos sstable (no *-Data.db file): nothing " +
                                                              "was read from it. --paxos-dir" + expected + "; it holds the directory " + emptyQuarantineTable +
                                                              " (0 sstables): pass " + emptyQuarantineTable + " instead" + notDescended);
        assertThat(assertExit(run(node, "--paxos-dir", emptyQuarantineTable.toString()), DsePreflight.EXIT_BLOCKED).report.problems)
            .containsExactly("--paxos-dir " + emptyQuarantineTable + " holds no system.paxos sstable (no *-Data.db file): nothing was read from it. " +
                             "--paxos-dir" + expected + EMPTY_PAXOS_DIRECTORY_ACCEPTED);

        // a system directory holding the table directory, with one sstable: passing it reads the sstable
        Path keyspaceLevel = node.root.resolve("keyspace-level").resolve("system");
        Path keyspaceLevelTable = keyspaceLevel.resolve(PAXOS_DIRECTORY);
        writeSSTable(keyspaceLevelTable, paxosMetadata(true), BtiFormat.getInstance(), sstables.get("bb-8"));
        Run direct = assertExit(run(node, "--paxos-dir", keyspaceLevel.toString()), DsePreflight.EXIT_BLOCKED);
        assertThat(direct.report.problems).containsExactly("--paxos-dir " + keyspaceLevel + " holds no system.paxos sstable (no *-Data.db file): nothing was " +
                                                           "read from it. --paxos-dir" + expected + "; it holds the directory " + keyspaceLevelTable +
                                                           " (1 sstable): pass " + keyspaceLevelTable + " instead" + notDescended);
        Run directFollowed = assertExit(run(node, "--paxos-dir", keyspaceLevelTable.toString()), DsePreflight.EXIT_READY);
        assertThat(directFollowed.report.directories).extracting(d -> d.source + ' ' + d.sstables + ' ' + d.rows)
                                                     .containsExactly("data directory 1 3", "snapshot " + TAG + " 1 3", "--paxos-dir " + keyspaceLevelTable + " 1 3");

        // a table directory of system.paxos whose parent is not a system directory: its sstables are not those of
        // system.paxos for the tool, and it is not named from its parent
        Path notSystem = node.root.resolve("not_system");
        Path notSystemTable = notSystem.resolve(PAXOS_DIRECTORY);
        Descriptor notSystemSSTable = writeSSTable(notSystemTable, paxosMetadata(true), BtiFormat.getInstance(), sstables.get("bb-8"));
        assertThat(assertExit(run(node, "--paxos-dir", notSystem.toString()), DsePreflight.EXIT_BLOCKED).report.problems)
            .containsExactly("--paxos-dir " + notSystem + " holds no system.paxos sstable (no *-Data.db file): nothing was read from it. --paxos-dir" + expected);
        assertThat(assertExit(run(node, "--paxos-dir", notSystemTable.toString()), DsePreflight.EXIT_BLOCKED).report.problems)
            .containsExactly(notSystemSSTable.baseFile() + ": not an sstable of system.paxos: its directory names not_system.paxos (the directory of an " +
                             "sstable of system.paxos is laid out as <...>/system/paxos-<id>)",
                             "--paxos-dir " + notSystemTable + " holds no system.paxos sstable (1 sstable of another table): nothing was read from it. " +
                             "--paxos-dir" + expected + "; it is named as one, but its parent is not a system directory: the tool reads the sstables " +
                             "of system.paxos only from a directory laid out as <...>/system/paxos-<id>");

        // a directory whose sstables cannot be listed: a line saying so, and only the problem of the listing
        Path unlisted = Files.createDirectories(node.root.resolve("unlisted").resolve("system").resolve(PAXOS_DIRECTORY));
        Files.write(unlisted.resolve("bb_txn_dsenodesync_" + TimeUUID.Generator.nextTimeUUID() + ".log"), "ADD:[x,0,1][2]\n".getBytes(StandardCharsets.UTF_8));
        Run unlistedRun = assertExit(run(node, "--paxos-dir", unlisted.toString()), DsePreflight.EXIT_BLOCKED);
        assertThat(unlistedRun.report.problems).hasSize(1);
        assertThat(unlistedRun.report.problems.get(0)).startsWith("cannot list the sstables of " + unlisted + " (")
                                                     .endsWith("): check them by hand with sstabledump on a copy of their files");
        PaxosRollbackReport.Directory unlistedDirectory = unlistedRun.report.directories.get(2);
        assertThat(unlistedDirectory.source).isEqualTo("--paxos-dir " + unlisted);
        assertThat(unlistedDirectory.sstables).isNull();
        assertThat(unlistedDirectory.otherTableSSTables).isNull();
        assertThat(unlistedDirectory.rows).isNull();
        String listing = unlistedRun.report.problems.get(0);
        assertThat(unlistedDirectory.error).isEqualTo("cannot be listed: " + listing.substring(("cannot list the sstables of " + unlisted + " (").length(),
                                                                                                  listing.length() - "): check them by hand with sstabledump on a copy of their files".length()));
        assertThat(unlistedRun.out).contains("  --paxos-dir " + unlisted + ": " + unlisted + " (" + unlistedDirectory.error + ")\n");
        // its parent: the table directory is named as one to pass
        Path unlistedRoot = node.root.resolve("unlisted");
        Run unlistedParent = assertExit(run(node, "--paxos-dir", unlistedRoot.toString()), DsePreflight.EXIT_BLOCKED);
        assertThat(unlistedParent.report.problems).containsExactly("--paxos-dir " + unlistedRoot + " holds no system.paxos sstable (no *-Data.db file): " +
                                                                   "nothing was read from it. --paxos-dir" + expected + "; it holds the directory " + unlisted +
                                                                   " (its sstables cannot be listed): pass " + unlisted + " instead" + notDescended);
        // given with it: named as given, not as read
        Run unlistedBoth = assertExit(run(node, "--paxos-dir", unlistedRoot.toString(), "--paxos-dir", unlisted.toString()), DsePreflight.EXIT_BLOCKED);
        assertThat(unlistedBoth.report.problems).hasSize(2);
        assertThat(unlistedBoth.report.problems.get(0)).isEqualTo("--paxos-dir " + unlistedRoot + " holds no system.paxos sstable (no *-Data.db file): " +
                                                                  "nothing was read from it. --paxos-dir" + expected + "; it holds the directory " + unlisted +
                                                                  " (its sstables cannot be listed, already named as --paxos-dir " + unlisted + "): drop this " +
                                                                  "--paxos-dir" + notDescended);
        assertThat(unlistedBoth.report.problems.get(1)).startsWith("cannot list the sstables of " + unlisted + " (");
        // an empty table directory next to it: it is named as one to pass
        Path nextToUnlisted = Files.createDirectories(unlisted.resolveSibling("paxos-00000000000000000000000000000002"));
        Run unlistedSibling = assertExit(run(node, "--paxos-dir", nextToUnlisted.toString()), DsePreflight.EXIT_BLOCKED);
        assertThat(unlistedSibling.report.problems).containsExactly("--paxos-dir " + nextToUnlisted + " holds no system.paxos sstable (no *-Data.db file): " +
                                                                    "nothing was read from it. --paxos-dir" + expected + "; it is laid out as one but holds " +
                                                                    "none, and next to it is the directory " + unlisted + " (its sstables cannot be " +
                                                                    "listed): pass " + unlisted + " instead");
        Run unlistedFollowed = assertExit(run(node, "--paxos-dir", unlisted.toString()), DsePreflight.EXIT_BLOCKED);
        assertThat(unlistedFollowed.report.problems).hasSize(1);
        assertThat(unlistedFollowed.report.problems.get(0)).startsWith("cannot list the sstables of " + unlisted + " (");

        // several table directories of system.paxos: each one is named
        Path otherId = Files.createDirectories(system.resolve("paxos-00000000000000000000000000000001"));
        Run several = assertExit(run(node, "--paxos-dir", system.toString()), DsePreflight.EXIT_BLOCKED);
        assertThat(several.report.problems).containsExactly("--paxos-dir " + system + " holds no system.paxos sstable (no *-Data.db file): nothing was read " +
                                                            "from it. --paxos-dir" + expected + "; it holds the directories " + otherId + " (0 sstables), " +
                                                            quarantine + " (2 sstables): pass " + quarantine + " instead" + notDescended);
        // the system directory given with a trailing ".": the directories to pass are named under its normalized path, and
        // a table directory given with a "." in its path is read as the table directory it is
        Path systemDot = system.resolve(".");
        Run severalDot = assertExit(run(node, "--paxos-dir", systemDot.toString()), DsePreflight.EXIT_BLOCKED);
        assertThat(severalDot.report.problems).containsExactly("--paxos-dir " + systemDot + " holds no system.paxos sstable (no *-Data.db file): nothing was " +
                                                               "read from it. --paxos-dir" + expected + "; it holds the directories " + otherId + " (0 sstables), " +
                                                               quarantine + " (2 sstables): pass " + quarantine + " instead" + notDescended);
        assertThat(severalDot.report.directories.get(2).path).isEqualTo(system.toString());
        Run dotted = assertExit(run(node, "--paxos-dir", systemDot.resolve(PAXOS_DIRECTORY).toString()), DsePreflight.EXIT_READY);
        assertThat(dotted.report.problems).isEmpty();
        assertRollbackDumpPartitions(dotted, "--paxos-dir " + systemDot.resolve(PAXOS_DIRECTORY), true);
        // a symbolic link to the system directory under another name: its table directories would not be read as those of
        // system.paxos through it, so none is named
        Path systemLink = Files.createSymbolicLink(node.root.resolve("system-link"), system);
        assertThat(assertExit(run(node, "--paxos-dir", systemLink.toString()), DsePreflight.EXIT_BLOCKED).report.problems)
            .containsExactly("--paxos-dir " + systemLink + " holds no system.paxos sstable (no *-Data.db file): nothing was read from it. --paxos-dir" + expected +
                             "; it is reached through a symbolic link, and its real path is " + system.toRealPath() + ": the layout is read from the path given");
        // a ".." after a symbolic link: the path names another directory than without it, refused; first where the
        // normalized path does not exist, then where it is another table directory of system.paxos with an sstable
        Path jump = Files.createSymbolicLink(node.root.resolve("jump"), system);
        Path jumped = jump.resolve("..").resolve("system").resolve(PAXOS_DIRECTORY);
        Run jumpedRun = assertExit(run(node, "--paxos-dir", jumped.toString()), DsePreflight.EXIT_BLOCKED);
        assertThat(jumpedRun.report.problems).hasSize(2);
        assertThat(jumpedRun.report.problems.get(0)).startsWith("--paxos-dir " + jumped + " cannot be compared with the directory its path names without " +
                                                                "\".\" and \"..\" (" + jumped.normalize() + "): java.nio.file.NoSuchFileException: ")
                                                    .endsWith(": nothing was read from it; give the directory without \".\" and \"..\"");
        assertThat(jumpedRun.report.problems.get(1)).isEqualTo(RESTORED_PROBLEM);
        Descriptor elsewhere = writeSSTable(jumped.normalize(), paxosMetadata(true), BtiFormat.getInstance(), sstables.get("bb-8"));
        Run jumpedOther = assertExit(run(node, "--paxos-dir", jumped.toString()), DsePreflight.EXIT_BLOCKED);
        assertThat(jumpedOther.report.problems).containsExactly("--paxos-dir " + jumped + " is not the directory its path names without \".\" and \"..\" (" +
                                                                jumped.normalize() + "), as a symbolic link comes before a \"..\": nothing was read from " +
                                                                "it; give the directory without \".\" and \"..\"", RESTORED_PROBLEM);
        assertThat(jumpedOther.report.directories).extracting(d -> d.source).containsExactly("data directory", "snapshot " + TAG);
        assertThat(jumpedOther.report.sstables).extracting(sstable -> sstable.path).doesNotContain(elsewhere.baseFile().toString());

        // the one with sstables already given: drop the parent
        Run severalGiven = assertExit(run(node, "--paxos-dir", system.toString(), "--paxos-dir", quarantine.toString()), DsePreflight.EXIT_BLOCKED);
        assertThat(severalGiven.report.problems).containsExactly("--paxos-dir " + system + " holds no system.paxos sstable (no *-Data.db file): nothing was " +
                                                                 "read from it. --paxos-dir" + expected + "; it holds the directories " + otherId + " (0 sstables), " +
                                                                 quarantine + " (2 sstables, already read as --paxos-dir " + quarantine + "): drop this " +
                                                                 "--paxos-dir" + notDescended);
        // the empty table directory next to the one with sstables: that one is named, to pass or as already read
        Run sibling = assertExit(run(node, "--paxos-dir", otherId.toString()), DsePreflight.EXIT_BLOCKED);
        assertThat(sibling.report.problems).containsExactly("--paxos-dir " + otherId + " holds no system.paxos sstable (no *-Data.db file): nothing was read " +
                                                            "from it. --paxos-dir" + expected + "; it is laid out as one but holds none, and next to it is the " +
                                                            "directory " + quarantine + " (2 sstables): pass " + quarantine + " instead");
        assertThat(assertExit(run(node, "--paxos-dir", quarantine.toString()), DsePreflight.EXIT_READY).report.problems).isEmpty();
        Run siblingRead = assertExit(run(node, "--paxos-dir", quarantine.toString(), "--paxos-dir", otherId.toString()), DsePreflight.EXIT_BLOCKED);
        assertThat(siblingRead.report.problems).containsExactly("--paxos-dir " + otherId + " holds no system.paxos sstable (no *-Data.db file): nothing was " +
                                                                "read from it. --paxos-dir" + expected + "; it is laid out as one but holds none, and next to " +
                                                                "it is the directory " + quarantine + " (2 sstables, already read as --paxos-dir " +
                                                                quarantine + "): drop this --paxos-dir");
        // the table directory of another table next to it: the same
        Path batches = system.resolve("batches-919a4bc57a333573b03e13fc3f68b465");
        Run otherTable = assertExit(run(node, "--paxos-dir", batches.toString()), DsePreflight.EXIT_BLOCKED);
        assertThat(otherTable.report.problems).containsExactly("--paxos-dir " + batches + " holds no system.paxos sstable (no *-Data.db file): nothing was " +
                                                               "read from it. --paxos-dir" + expected + "; it is the table directory of another table, and " +
                                                               "next to it is the directory " + quarantine + " (2 sstables): pass " + quarantine +
                                                               " instead");

        // a directory already read, as a data directory, as the snapshot or as another --paxos-dir: read once, a problem; the
        // comparison with the snapshot is made as without it
        Run dataAgain = assertExit(run(node, "--paxos-dir", paxosDirectory(node).toString()), DsePreflight.EXIT_BLOCKED);
        assertThat(dataAgain.report.problems).containsExactly("--paxos-dir " + paxosDirectory(node) + " is the directory already read as data directory, " +
                                                              "read once: drop this --paxos-dir", RESTORED_PROBLEM);
        assertThat(dataAgain.report.directories).extracting(d -> d.source).containsExactly("data directory", "snapshot " + TAG);
        Run snapshotAgain = assertExit(run(node, "--paxos-dir", paxosSnapshot(node).toString()), DsePreflight.EXIT_BLOCKED);
        assertThat(snapshotAgain.report.problems).containsExactly("--paxos-dir " + paxosSnapshot(node) + " is the directory already read as snapshot " + TAG +
                                                                  ", read once: drop this --paxos-dir", RESTORED_PROBLEM);
        Path link = Files.createSymbolicLink(node.root.resolve("quarantine-link"), quarantine);
        Run linked = assertExit(run(node, "--paxos-dir", quarantine.toString(), "--paxos-dir", link.toString()), DsePreflight.EXIT_BLOCKED);
        assertThat(linked.report.problems).containsExactly("--paxos-dir " + link + " is the directory already read as --paxos-dir " + quarantine +
                                                           ", read once: drop this --paxos-dir");
        Run literal = assertExit(run(node, "--paxos-dir", quarantine.toString(), "--paxos-dir", quarantine.toString()), DsePreflight.EXIT_BLOCKED);
        assertThat(literal.report.problems).containsExactly("--paxos-dir " + quarantine + " is the directory already read as --paxos-dir " + quarantine +
                                                            ", read once: drop this --paxos-dir");
        Path sameQuarantine = system.resolve("..").resolve("system").resolve(PAXOS_DIRECTORY);
        Run twice = assertExit(run(node, "--paxos-dir", quarantine.toString(), "--paxos-dir", sameQuarantine.toString()), DsePreflight.EXIT_BLOCKED);
        assertThat(twice.report.problems).containsExactly("--paxos-dir " + sameQuarantine + " is the directory already read as --paxos-dir " + quarantine +
                                                          ", read once: drop this --paxos-dir");
        assertRollbackDumpPartitions(twice, "--paxos-dir " + quarantine, true);

        // the table directory: read, no problem
        Run right = assertExit(run(node, "--paxos-dir", quarantine.toString()), DsePreflight.EXIT_READY);
        assertThat(right.report.problems).isEmpty();
        assertRollbackDumpPartitions(right, "--paxos-dir " + quarantine, true);

        // two with sstables, none given: both named
        writeSSTable(otherId, paxosMetadata(true), BtiFormat.getInstance(), sstables.get("bb-8"));
        Run twoToPass = assertExit(run(node, "--paxos-dir", system.toString()), DsePreflight.EXIT_BLOCKED);
        assertThat(twoToPass.report.problems).containsExactly("--paxos-dir " + system + " holds no system.paxos sstable (no *-Data.db file): nothing was read " +
                                                              "from it. --paxos-dir" + expected + "; it holds the directories " + otherId + " (1 sstable), " +
                                                              quarantine + " (2 sstables): pass instead each one of " + otherId + ", " + quarantine + " the " +
                                                              "restore moved HCD 2.0's sstables to" + notDescended);
        assertThat(assertExit(run(node, "--paxos-dir", otherId.toString(), "--paxos-dir", quarantine.toString()), DsePreflight.EXIT_READY).report.problems)
            .isEmpty();
        // an empty table directory next to both
        Path third = Files.createDirectories(system.resolve("paxos-00000000000000000000000000000003"));
        Run twoSiblings = assertExit(run(node, "--paxos-dir", third.toString()), DsePreflight.EXIT_BLOCKED);
        assertThat(twoSiblings.report.problems).containsExactly("--paxos-dir " + third + " holds no system.paxos sstable (no *-Data.db file): nothing was " +
                                                                "read from it. --paxos-dir" + expected + "; it is laid out as one but holds none, and next to " +
                                                                "it are the directories " + otherId + " (1 sstable), " + quarantine + " (2 " +
                                                                "sstables): pass instead each one of " + otherId + ", " + quarantine + " the " +
                                                                "restore moved HCD 2.0's sstables to");
        Run twoSiblingsFollowed = assertExit(run(node, "--paxos-dir", otherId.toString(), "--paxos-dir", quarantine.toString()),
                                             DsePreflight.EXIT_READY);
        assertThat(twoSiblingsFollowed.report.problems).isEmpty();

        // the table directory and an empty one: the first is read, the second is a problem and has its line
        Run both = assertExit(run(node, "--paxos-dir", quarantine.toString(), "--paxos-dir", empty.toString()), DsePreflight.EXIT_BLOCKED);
        assertThat(both.report.problems).containsExactly(emptyRun.report.problems.get(0));
        assertRollbackDumpPartitions(both, "--paxos-dir " + quarantine, true);
        assertThat(both.report.directories).extracting(d -> d.source + ' ' + d.sstables + ' ' + d.rows)
                                           .containsExactly("data directory 1 3", "snapshot " + TAG + " 1 3", "--paxos-dir " + quarantine + " 2 19",
                                                            "--paxos-dir " + empty + " 0 0");
        assertThat(both.out).contains("  --paxos-dir " + empty + ": " + empty + " (0 sstables, 0 rows)\n");
    }

    /** What a --paxos-dir that holds no sstable of system.paxos was expected to be, in its problem. */
    private static final String PAXOS_DIRECTORY_EXPECTED = " takes the table directory of system.paxos the restore moved HCD 2.0's sstables to, laid out as " +
                                                           "<...>/system/paxos-<id> and holding their *-Data.db files, such as <quarantine>/system/paxos-<id>";
    /**
     * What the problem of an empty table directory of system.paxos given with --paxos-dir, laid out as one, says on a node
     * with one data directory when no directory given holds an sstable of system.paxos.
     */
    private static final String EMPTY_PAXOS_DIRECTORY_ACCEPTED = "; it is laid out as one: check that it is the directory the restore moved the sstables " +
                                                                 "of system.paxos to. The restore moves all of them there, so it is empty only when the " +
                                                                 "node had none at the drain (no LWT under HCD 2.0, and none of DSE's left): then there is " +
                                                                 "no Paxos state to read, and this problem needs no other action when it is the only " +
                                                                 "problem in the report";
    /** The same on a node with two data directories. */
    private static final String EMPTY_PAXOS_DIRECTORY_ACCEPTED_OF_TWO = "; it is laid out as one: check that it is the directory the restore moved the " +
                                                                        "sstables of system.paxos to. The restore moves there all those of its data " +
                                                                        "directory, so it is empty only when that data directory had none at the drain. " +
                                                                        "When none of the 2 data directories had any (no LWT under HCD 2.0, and none of " +
                                                                        "DSE's left), there is no Paxos state to read: pass the quarantined " +
                                                                        "system/paxos-<id> of each of them; then this problem needs no other action when " +
                                                                        "the only other problem in the report is the same problem (an empty quarantined " +
                                                                        "system/paxos-<id>) for the other data directory";
    private static final String RESTORED_PROBLEM = "every system.paxos sstable of the data directories is also in the snapshot " + TAG + " (by name and size; " +
                                                   "also when the data directories hold none): system looks already restored, so the system.paxos sstables HCD 2.0 " +
                                                   "wrote would not be read. If the restore ran, pass --paxos-dir with each directory they were moved to (such as " +
                                                   "<quarantine>/system/paxos-<id>); if it did not (HCD 2.0 wrote no system.paxos sstable on this node: no LWT and no " +
                                                   "compaction of system.paxos), pass --not-restored";
    private static final String PARTLY_RESTORED_PROBLEM = "the system.paxos sstables of the data directories are some of those of the snapshot " + TAG +
                                                          " (by name and size; also when the data directories hold none), and the others are missing: a " +
                                                          "restore of system in progress or interrupted (the system.paxos sstables HCD 2.0 wrote would not be " +
                                                          "read: finish the restore and pass --paxos-dir with each directory they were moved to, such as " +
                                                          "<quarantine>/system/paxos-<id>), or snapshot sstables HCD 2.0 dropped because all their cells had " +
                                                          "expired, without writing any of its own (then pass --not-restored)";

    @Test
    public void asksWhetherSystemIsRestoredWhenTheDataDirectoryHoldsOnlyTheSnapshotsPaxosState() throws IOException
    {
        // HCD 2.0 wrote no system.paxos sstable (no LWT, no compaction): the data directory holds DSE's sstable as the snapshot
        // does, which is also what it holds after the restore: the operator says which
        DseNodeFixture node = node();
        Map<String, List<PartitionUpdate>> sstables = rollbackDumpSSTables();
        writeSSTable(paxosSnapshot(node), paxosMetadata(false), BtiFormat.getInstance(), sstables.get("bb-3"));
        try (Stream<Path> files = Files.list(paxosSnapshot(node)))
        {
            for (Path file : (Iterable<Path>) files::iterator)
                Files.copy(file, paxosDirectory(node).resolve(file.getFileName()));
        }
        Run asked = assertExit(run(node), DsePreflight.EXIT_BLOCKED);
        assertThat(asked.report.problems).containsExactly(RESTORED_PROBLEM);
        Run stated = assertExit(run(node, "--not-restored"), DsePreflight.EXIT_READY);
        assertThat(stated.report.problems).isEmpty();
        assertThat(stated.report.notRestored).isTrue();
        assertThat(asked.report.notRestored).isFalse();
        assertThat(stated.out).contains("--not-restored: the operator states that system has not been restored yet.\n");
        assertThat(asked.out).doesNotContain("--not-restored:");
        assertThat(assertExit(run(node, "--not-restored", "--paxos-dir", paxosDirectory(node).toString()), DsePreflight.EXIT_ERROR).err)
            .contains("--not-restored and --paxos-dir contradict each other");
        assertThat(stated.report.partitions).extracting(p -> p.where).containsExactly("id = 10", "id = 11");

        // no system.paxos sstable in the data directory nor in the snapshot: the same question
        DseNodeFixture empty = nodeWithEmptyPaxosSnapshot();
        assertThat(assertExit(run(empty), DsePreflight.EXIT_BLOCKED).report.problems).containsExactly(RESTORED_PROBLEM);
        assertThat(assertExit(run(empty, "--not-restored"), DsePreflight.EXIT_READY).report.result).isEqualTo(PaxosRollbackReport.NOTHING_TO_READ);

        // an sstable of the data directory with the name of the snapshot's but another size: HCD 2.0's, not restored
        DseNodeFixture other = node();
        generation = 1;
        writeSSTable(paxosSnapshot(other), paxosMetadata(false), BtiFormat.getInstance(), sstables.get("bb-3"));
        generation = 1;
        Descriptor hcd = writeSSTable(paxosDirectory(other), paxosMetadata(true), BtiFormat.getInstance(), sstables.get("bb-7"));
        Path snapshotData = paxosSnapshot(other).resolve(hcd.fileFor(BtiFormat.Components.DATA).name());
        assertThat(Files.size(snapshotData)).isNotEqualTo(Files.size(hcd.fileFor(BtiFormat.Components.DATA).toPath()));
        Run differs = assertExit(run(other), DsePreflight.EXIT_READY);
        assertThat(differs.report.sstables).extracting(sstable -> new File(sstable.path).name()).containsExactly("bb-1-bti", "bb-1-bti");
        assertThat(differs.report.problems).isEmpty();

        // the snapshot's sstables, only some of them or none, in the data directory: a restore in progress, or snapshot
        // sstables HCD 2.0 dropped as fully expired: the operator says which
        DseNodeFixture partial = node();
        writeSSTable(paxosSnapshot(partial), paxosMetadata(false), BtiFormat.getInstance(), sstables.get("bb-3"));
        Descriptor second = writeSSTable(paxosSnapshot(partial), paxosMetadata(false), BtiFormat.getInstance(), sstables.get("bb-8"));
        assertThat(assertExit(run(partial), DsePreflight.EXIT_BLOCKED).report.problems).containsExactly(PARTLY_RESTORED_PROBLEM);
        assertThat(assertExit(run(partial, "--not-restored"), DsePreflight.EXIT_READY).report.problems).isEmpty();
        try (Stream<Path> files = Files.list(paxosSnapshot(partial)))
        {
            for (Path file : (Iterable<Path>) files::iterator)
                if (file.getFileName().toString().startsWith(second.baseFile().name() + '-'))
                    Files.copy(file, paxosDirectory(partial).resolve(file.getFileName()));
        }
        Run subset = assertExit(run(partial), DsePreflight.EXIT_BLOCKED);
        assertThat(subset.report.problems).containsExactly(PARTLY_RESTORED_PROBLEM);
        assertThat(subset.report.sstables).extracting(sstable -> sstable.source).containsExactly("data directory", "snapshot " + TAG, "snapshot " + TAG);
        assertThat(assertExit(run(partial, "--not-restored"), DsePreflight.EXIT_READY).report.problems).isEmpty();
    }

    @Test
    public void comparesThePaxosStateOfEveryDataDirectory() throws IOException
    {
        // the system keyspace in the first data directory, the snapshot of system.paxos in the second
        DseNodeFixture node = node();
        Path second = node.root.resolve("data2");
        Path secondPaxos = second.resolve("system").resolve(PAXOS_DIRECTORY);
        Descriptor descriptor = writeSSTable(secondPaxos.resolve("snapshots").resolve(TAG), paxosMetadata(false), BtiFormat.getInstance(), rollbackDumpSSTables().get("bb-3"));
        try (Stream<Path> files = Files.list(secondPaxos.resolve("snapshots").resolve(TAG)))
        {
            for (Path file : (Iterable<Path>) files::iterator)
                Files.copy(file, Files.createDirectories(paxosDirectory(node)).resolve(file.getFileName()));
        }
        Run run = assertExit(run(node, "--data-dir", node.data.toString(), "--data-dir", second.toString()), DsePreflight.EXIT_BLOCKED);
        assertThat(run.report.problems).containsExactly(RESTORED_PROBLEM);
        assertThat(run.report.sstables).extracting(s -> s.source + ' ' + new File(s.path).name())
                                       .containsExactly("data directory " + descriptor.baseFile().name(), "snapshot " + TAG + ' ' + descriptor.baseFile().name());
        // a line for every directory of system.paxos read, the one of the second data directory without sstables included
        assertThat(run.report.directories).extracting(d -> d.source + ' ' + d.path + ' ' + d.sstables + ' ' + d.rows)
                                          .containsExactly("data directory " + paxosDirectory(node) + " 1 3",
                                                           "data directory " + secondPaxos + " 0 0",
                                                           "snapshot " + TAG + ' ' + secondPaxos.resolve("snapshots").resolve(TAG) + " 1 3");
        assertThat(run.out).contains("  data directory: " + secondPaxos + " (0 sstables, 0 rows)\n");

        // an sstable HCD 2.0 wrote in the second data directory only: not restored, and read
        Descriptor hcd = writeSSTable(secondPaxos, paxosMetadata(true), BtiFormat.getInstance(), rollbackDumpSSTables().get("bb-7"));
        Run notRestored = assertExit(run(node, "--data-dir", node.data.toString(), "--data-dir", second.toString()), DsePreflight.EXIT_READY);
        assertThat(notRestored.report.problems).isEmpty();
        assertThat(notRestored.report.sstables).extracting(s -> s.path).contains(hcd.baseFile().toString());
        assertThat(notRestored.report.partitions).hasSize(15);
    }

    /**
     * The quarantined {@code system/paxos-<id>} of the data directories, given with {@code --paxos-dir}. All empty: each
     * has the same problem, which needs no other action when the number passed is the number of data directories, and
     * which names the number passed otherwise (one of two, two of three, three of two, two of one); the problems keep the
     * order of the directories given before a problem found later in the scan. One empty and another holding sstables:
     * the empty one is the wrong directory to pass (the rollback passes only the directories that hold {@code *-Data.db}
     * files, or each one when none does), in whichever order they are given, also when its quarantine, the system
     * directory of its quarantine or a symbolic link to its quarantine is given instead, and also when two directories
     * given hold sstables.
     */
    @Test
    public void reportsTheEmptyPaxosDirectoriesOfSeveralDataDirectories() throws IOException
    {
        DseNodeFixture node = node();
        Map<String, List<PartitionUpdate>> sstables = rollbackDumpSSTables();
        // system restored in both data directories: DSE's bb-3 back in the first, nothing in the second
        writeSSTable(paxosSnapshot(node), paxosMetadata(false), BtiFormat.getInstance(), sstables.get("bb-3"));
        try (Stream<Path> files = Files.list(paxosSnapshot(node)))
        {
            for (Path file : (Iterable<Path>) files::iterator)
                Files.copy(file, paxosDirectory(node).resolve(file.getFileName()));
        }
        Path second = node.root.resolve("data2");
        Files.createDirectories(second.resolve("system").resolve(PAXOS_DIRECTORY));
        Path firstRoot = node.root.resolve("quarantine1");
        Path firstQuarantine = Files.createDirectories(firstRoot.resolve("system").resolve(PAXOS_DIRECTORY));
        Path secondQuarantine = Files.createDirectories(node.root.resolve("quarantine2").resolve("system").resolve(PAXOS_DIRECTORY));
        List<String> dataDirectories = List.of("--data-dir", node.data.toString(), "--data-dir", second.toString());
        String firstEmpty = "--paxos-dir " + firstQuarantine + " holds no system.paxos sstable (no *-Data.db file): nothing was read from it. --paxos-dir" +
                            PAXOS_DIRECTORY_EXPECTED;
        String secondEmpty = "--paxos-dir " + secondQuarantine + " holds no system.paxos sstable (no *-Data.db file): nothing was read from it. --paxos-dir" +
                             PAXOS_DIRECTORY_EXPECTED;

        // both empty: one problem for each, saying it needs no other action when the other one is the same problem
        Run bothEmpty = assertExit(run(node, dataDirectories, firstQuarantine, secondQuarantine), DsePreflight.EXIT_BLOCKED);
        assertThat(bothEmpty.report.problems).containsExactly(firstEmpty + EMPTY_PAXOS_DIRECTORY_ACCEPTED_OF_TWO,
                                                              secondEmpty + EMPTY_PAXOS_DIRECTORY_ACCEPTED_OF_TWO);
        assertThat(bothEmpty.report.partitions).extracting(p -> p.where).containsExactly("id = 10", "id = 11");
        assertThat(bothEmpty.report.directories).extracting(d -> d.source + ' ' + d.sstables)
                                                .containsExactly("data directory 1", "snapshot " + TAG + " 1", "data directory 0",
                                                                 "--paxos-dir " + firstQuarantine + " 0", "--paxos-dir " + secondQuarantine + " 0");
        // only one of them: too few for the data directories
        Run onlyFirst = assertExit(run(node, dataDirectories, firstQuarantine), DsePreflight.EXIT_BLOCKED);
        assertThat(onlyFirst.report.problems).containsExactly(firstEmpty + EMPTY_PAXOS_DIRECTORY_ACCEPTED_OF_TWO + "; 1 empty system/paxos-<id> is passed " +
                                                              "for the 2 data directories: pass only that of each data directory");
        // both of them on a node with three data directories: too few as well
        Path third = node.root.resolve("data3");
        Files.createDirectories(third.resolve("system").resolve(PAXOS_DIRECTORY));
        String ofThree = "; it is laid out as one: check that it is the directory the restore moved the sstables of system.paxos to. The restore moves " +
                         "there all those of its data directory, so it is empty only when that data directory had none at the drain. When none of the 3 " +
                         "data directories had any (no LWT under HCD 2.0, and none of DSE's left), there is no Paxos state to read: pass the quarantined " +
                         "system/paxos-<id> of each of them; then this problem needs no other action when the only other problems in the report are the " +
                         "same problem (an empty quarantined system/paxos-<id>) for each of the other 2 data directories; 2 empty system/paxos-<id> are " +
                         "passed for the 3 data directories: pass only that of each data directory";
        List<String> threeDataDirectories = new ArrayList<>(dataDirectories);
        threeDataDirectories.addAll(List.of("--data-dir", third.toString()));
        Run ofThreeRun = assertExit(run(node, threeDataDirectories, firstQuarantine, secondQuarantine), DsePreflight.EXIT_BLOCKED);
        assertThat(ofThreeRun.report.problems).containsExactly(firstEmpty + ofThree, secondEmpty + ofThree);
        // three of them for two data directories, such as a stale quarantine of an earlier restore: too many
        Path staleQuarantine = Files.createDirectories(node.root.resolve("quarantine-stale").resolve("system").resolve(PAXOS_DIRECTORY));
        String staleEmpty = "--paxos-dir " + staleQuarantine + " holds no system.paxos sstable (no *-Data.db file): nothing was read from it. --paxos-dir" +
                            PAXOS_DIRECTORY_EXPECTED;
        String tooMany = EMPTY_PAXOS_DIRECTORY_ACCEPTED_OF_TWO + "; 3 empty system/paxos-<id> are passed for the 2 data directories: pass only that of " +
                         "each data directory";
        Run tooManyRun = assertExit(run(node, dataDirectories, firstQuarantine, secondQuarantine, staleQuarantine), DsePreflight.EXIT_BLOCKED);
        assertThat(tooManyRun.report.problems).containsExactly(firstEmpty + tooMany, secondEmpty + tooMany, staleEmpty + tooMany);
        // two of them on a node with one data directory: too many as well
        String twoForOne = EMPTY_PAXOS_DIRECTORY_ACCEPTED + "; 2 empty system/paxos-<id> are passed for the 1 data directory: pass only that of the data " +
                           "directory";
        Run twoForOneRun = assertExit(run(node, List.of(), firstQuarantine, staleQuarantine), DsePreflight.EXIT_BLOCKED);
        assertThat(twoForOneRun.report.problems).containsExactly(firstEmpty + twoForOne, staleEmpty + twoForOne);

        // on a node with one data directory, the problem keeps its place before those found later in the scan: here those
        // of a directory given after it that holds an sstable of another table
        Path other = node.root.resolve("elsewhere").resolve("system").resolve("batches-919a4bc57a333573b03e13fc3f68b465");
        Descriptor otherSSTable = writeSSTable(other, paxosMetadata(true), BtiFormat.getInstance(), sstables.get("bb-8"));
        Run ordered = assertExit(run(node, List.of(), firstQuarantine, other), DsePreflight.EXIT_BLOCKED);
        assertThat(ordered.report.problems).containsExactly(firstEmpty + EMPTY_PAXOS_DIRECTORY_ACCEPTED,
                                                            otherSSTable.baseFile() + ": not an sstable of system.paxos: its directory names system.batches " +
                                                            "(the directory of an sstable of system.paxos is laid out as <...>/system/paxos-<id>)",
                                                            "--paxos-dir " + other + " holds no system.paxos sstable (1 sstable of another table): nothing " +
                                                            "was read from it. --paxos-dir" + PAXOS_DIRECTORY_EXPECTED);

        // HCD 2.0's sstables in the second only: the first is named as the one to drop, given before or after the second
        writeSSTable(secondQuarantine, paxosMetadata(true), BtiFormat.getInstance(), sstables.get("bb-7"));
        writeSSTable(secondQuarantine, paxosMetadata(true), BtiFormat.getInstance(), sstables.get("bb-8"));
        String passOnly = " sstables of system.paxos: drop this --paxos-dir (pass only the directories that hold sstables of system.paxos)";
        String drop = "; it is laid out as one but holds none, while --paxos-dir " + secondQuarantine + " holds" + passOnly;
        for (List<Path> order : List.of(List.of(firstQuarantine, secondQuarantine), List.of(secondQuarantine, firstQuarantine)))
        {
            Run oneEmpty = assertExit(run(node, dataDirectories, order.toArray(new Path[0])), DsePreflight.EXIT_BLOCKED);
            assertThat(oneEmpty.report.problems).containsExactly(firstEmpty + drop);
            assertRollbackDumpPartitions(oneEmpty, "--paxos-dir " + secondQuarantine, true);
        }
        // its quarantine instead: dropped too, not named as the one to pass
        Run quarantineGiven = assertExit(run(node, dataDirectories, firstRoot, secondQuarantine), DsePreflight.EXIT_BLOCKED);
        assertThat(quarantineGiven.report.problems).containsExactly("--paxos-dir " + firstRoot + " holds no system.paxos sstable (no *-Data.db file): " +
                                                                    "nothing was read from it. --paxos-dir" + PAXOS_DIRECTORY_EXPECTED + "; it holds the " +
                                                                    "directory " + firstQuarantine + " (0 sstables), none with an sstable, while --paxos-dir " +
                                                                    secondQuarantine + " holds sstables of system.paxos: drop this --paxos-dir (the tool " +
                                                                    "reads the directories given, not their subdirectories)");
        // the system directory of its quarantine instead: dropped too
        Path firstSystem = firstRoot.resolve("system");
        Run systemGiven = assertExit(run(node, dataDirectories, firstSystem, secondQuarantine), DsePreflight.EXIT_BLOCKED);
        assertThat(systemGiven.report.problems).containsExactly("--paxos-dir " + firstSystem + " holds no system.paxos sstable (no *-Data.db file): " +
                                                                "nothing was read from it. --paxos-dir" + PAXOS_DIRECTORY_EXPECTED + "; it holds the " +
                                                                "directory " + firstQuarantine + " (0 sstables), none with an sstable, while --paxos-dir " +
                                                                secondQuarantine + " holds sstables of system.paxos: drop this --paxos-dir (the tool " +
                                                                "reads the directories given, not their subdirectories)");
        // reached through a symbolic link to its quarantine: dropped, with its real path
        Path linked = Files.createSymbolicLink(node.root.resolve("quarantine1-link"), firstRoot).resolve("system").resolve(PAXOS_DIRECTORY);
        Run linkGiven = assertExit(run(node, dataDirectories, linked, secondQuarantine), DsePreflight.EXIT_BLOCKED);
        assertThat(linkGiven.report.problems).containsExactly("--paxos-dir " + linked + " holds no system.paxos sstable (no *-Data.db file): nothing was " +
                                                              "read from it. --paxos-dir" + PAXOS_DIRECTORY_EXPECTED + drop + "; it is reached through a " +
                                                              "symbolic link, and its real path is " + firstQuarantine.toRealPath() + ": the layout is " +
                                                              "read from the path given");
        // the second alone, as the rollback passes it: nothing left to decide
        Run followed = assertExit(run(node, dataDirectories, secondQuarantine), DsePreflight.EXIT_READY);
        assertThat(followed.report.problems).isEmpty();
        assertRollbackDumpPartitions(followed, "--paxos-dir " + secondQuarantine, true);

        // a third one holding sstables of system.paxos too: both are named
        Path thirdQuarantine = node.root.resolve("quarantine3").resolve("system").resolve(PAXOS_DIRECTORY);
        writeSSTable(thirdQuarantine, paxosMetadata(true), BtiFormat.getInstance(), sstables.get("bb-8"));
        Run twoHold = assertExit(run(node, dataDirectories, firstQuarantine, secondQuarantine, thirdQuarantine), DsePreflight.EXIT_BLOCKED);
        assertThat(twoHold.report.problems).containsExactly(firstEmpty + "; it is laid out as one but holds none, while --paxos-dir " + secondQuarantine +
                                                            ", --paxos-dir " + thirdQuarantine + " hold" + passOnly);
    }

    /** A run of the tool on {@code node} with these options, then a {@code --paxos-dir} for each of {@code paxosDirectories}. */
    private Run run(DseNodeFixture node, List<String> options, Path... paxosDirectories) throws IOException
    {
        List<String> extra = new ArrayList<>(options);
        for (Path directory : paxosDirectories)
            extra.addAll(List.of("--paxos-dir", directory.toString()));
        return run(node, extra.toArray(new String[0]));
    }

    /**
     * The 15 open partitions of the rollback dump, those of HCD 2.0 read from {@code hcdSource}, DSE's {@code bb-3} also in the
     * data directory when {@code restored}.
     */
    private static void assertRollbackDumpPartitions(Run run, String hcdSource, boolean restored)
    {
        // ids 10 and 11 (DSE), 20 to 22 (HCD 2.0, non-empty), 100 to 109 (HCD 2.0, empty); not id 12 (no proposal ballot)
        List<Integer> ids = new ArrayList<>(List.of(10, 11, 20, 21, 22));
        for (int id = 100; id < 110; id++)
            ids.add(id);
        assertThat(run.partitions(RF3)).extracting(p -> p.where).containsExactlyElementsOf(ids.stream().map(id -> "id = " + id).collect(Collectors.toList()));
        for (int id : ids)
        {
            PaxosRollbackReport.Partition partition = run.partition(RF3, hex(intKey(id)));
            assertThat(only(partition).action).isEqualTo(Action.READ);
            List<PaxosRollbackReport.OpenRow> rows = partition.openRows;
            if (id < 20)
            {
                // open in DSE's sstable of the snapshot (and of its restored copy) and in HCD 2.0's compaction of it, written by DSE
                List<String> expected = new ArrayList<>(List.of(hcdSource + " DSE 1024 false", "snapshot " + TAG + " DSE 1024 false"));
                if (restored)
                    expected.add("data directory DSE 1024 false");
                assertThat(rows).extracting(r -> r.source + " " + r.writtenBy + " " + r.version + " " + r.emptyProposal)
                                .containsExactlyInAnyOrderElementsOf(expected);
            }
            else
            {
                assertThat(rows).extracting(r -> r.source + " " + r.writtenBy + " " + r.version + " " + r.emptyProposal)
                                .containsExactly(hcdSource + " HCD 2.0 12 " + (id >= 100));
            }
        }
        PaxosRollbackReport.OpenRow ten = run.partition(RF3, hex(intKey(10))).openRows.stream().filter(r -> r.source.startsWith("snapshot")).findFirst().get();
        assertThat(ten.proposalBallot).isEqualTo("d7191240-bf41-11f1-b58b-b5a15fd1fbe1");
        assertThat(ten.mostRecentCommitAt).isEqualTo("d1c368e0-bf41-11f1-9af8-aaf816ecb511");
        assertThat(PaxosRollback.cql(run.report, PaxosRollback.SERIAL)).contains("SELECT id FROM rf3.px WHERE id = 22 LIMIT 1;\n")
                                                                       .doesNotContain("id = 12 ");
    }

    @Test
    public void readsEachSSTableOnItsOwnAndTheSnapshotWithTheDse6865PaxosRows() throws IOException
    {
        DseNodeFixture node = node();
        List<DseFixtures.PaxosRow> rows = DseFixtures.paxosRows();
        addKeyspace(node, "bbtest", simple(3));
        addTable(node, "bbtest", "t", DseFixtures.PAXOS_COLLECTION_TABLE_ID, List.of(column("id", "int")));
        snapshotSchema(node);

        // the snapshot: DSE's rows written back as DSE wrote them, in DSE's layout of system.paxos
        TableMetadata dsePaxos = paxosMetadata(false);
        List<PartitionUpdate> dse = new ArrayList<>();
        for (DseFixtures.PaxosRow row : rows)
        {
            dse.add(cells(dsePaxos, row.rowKey, row.cfId, micros(row.mostRecentCommitAt), "most_recent_commit_at", row.mostRecentCommitAt,
                          "most_recent_commit", row.mostRecentCommit, "most_recent_commit_version", row.mostRecentCommitVersion));
            if (row.inProgressBallot != null)
                dse.add(promise(dsePaxos, row.rowKey, row.cfId, row.inProgressBallot));
            if (row.proposalBallot != null)
                dse.add(accept(dsePaxos, row.rowKey, row.cfId, row.proposalBallot, row.proposal, row.proposalVersion));
            else if (row.proposalVersion != null)
                dse.add(cells(dsePaxos, row.rowKey, row.cfId, micros(row.mostRecentCommitAt), "proposal_version", row.proposalVersion));
        }
        writeSSTable(paxosSnapshot(node), dsePaxos, BtiFormat.getInstance(), dse);

        // the data directory: a proposal in one sstable, its commit in a later one: each sstable on its own lists the proposal
        TableMetadata paxos = paxosMetadata(true);
        UUID proposal = timeUuid(base);
        writeSSTable(paxosDirectory(node), paxos, BtiFormat.getInstance(), List.of(accept(paxos, intKey(8), RF3, proposal, update(8, 1, HCD_VERSION), HCD_VERSION)));
        writeSSTable(paxosDirectory(node), paxos, BtiFormat.getInstance(),
                     List.of(commit(paxos, intKey(8), RF3, timeUuid(base + 10), update(8, 1, HCD_VERSION), HCD_VERSION)));

        Map<String, String> before = node.fingerprint();
        Run run = assertExit(run(node), DsePreflight.EXIT_READY);
        assertThat(node.fingerprint()).as("the node's files after the run").isEqualTo(before);
        assertThat(run.report.sstables).extracting(s -> s.source).containsExactly("data directory", "data directory", "snapshot " + TAG);
        assertThat(run.report.rowsRead).isEqualTo(2 + rows.size());

        // the DSE 6.8.65 rows: the 100 rows of lab.lwt and the 9 conditional batches of bbtest.t are committed; the 9 empty
        // proposals the serial reads of the upgraded node left after an empty commit are open
        List<DseFixtures.PaxosRow> expectedOpen = rows.stream()
                                                      .filter(r -> r.proposalBallot != null &&
                                                                   Ballot.fromUuid(r.proposalBallot).uuidTimestamp() > Ballot.fromUuid(r.mostRecentCommitAt).uuidTimestamp())
                                                      .collect(Collectors.toList());
        assertThat(expectedOpen).hasSize(9).allMatch(r -> r.cfId.equals(DseFixtures.PAXOS_COLLECTION_TABLE_ID));
        List<PaxosRollbackReport.Partition> dseOpen = run.partitions(DseFixtures.PAXOS_COLLECTION_TABLE_ID);
        assertThat(dseOpen).extracting(p -> p.rowKey).containsExactlyInAnyOrderElementsOf(expectedOpen.stream().map(r -> hex(r.rowKey)).collect(Collectors.toList()));
        for (PaxosRollbackReport.Partition partition : dseOpen)
        {
            assertThat(partition.keyspace).isEqualTo("bbtest");
            assertThat(partition.where).isEqualTo("id = " + ByteBufferUtil.hexToBytes(partition.rowKey).getInt());
            assertThat(only(partition).action).isEqualTo(Action.READ);
            assertThat(partition.openRows).hasSize(1);
            PaxosRollbackReport.OpenRow open = partition.openRows.get(0);
            assertThat(open.source).isEqualTo("snapshot " + TAG);
            assertThat(open.writtenBy).isEqualTo("DSE");
            assertThat(open.version).isEqualTo(DseFixtures.PAXOS_DSE_6_8_65_VERSION);
            assertThat(open.emptyProposal).isTrue();
        }
        assertThat(run.partitions(DseFixtures.PAXOS_LWT_TABLE_ID)).isEmpty();

        PaxosRollbackReport.Partition eight = run.partition(RF3, hex(intKey(8)));
        assertThat(eight.openRows).hasSize(1);
        assertThat(eight.openRows.get(0).emptyProposal).isFalse();
        assertThat(eight.openRows.get(0).mostRecentCommitAt).isNull();
    }

    /**
     * The {@code system.paxos} sstable DSE 6.9.26 left on the first node of its test cluster ({@link DseFixtures#dse6926PaxosSSTableDirectory()}),
     * in the snapshot as DSE left it: read as DSE's, it holds committed rows only, so no round of it is open (whether its
     * cells, which expire in October 2026, are still live or not). Next to it in the snapshot, two rounds DSE 6.9 left open,
     * tagged 1536 as DSE 6.9 tags them: a proposal accepted and never committed, made of the bytes of one of those commits,
     * and an empty proposal (a round that changed nothing). Decoded with DSE's layout DSE_68, the first is not empty and the
     * second is (read with this software's own layout, both look not empty); both are listed as DSE's and read at the
     * table's serial consistency.
     */
    @Test
    public void readsTheDse6926PaxosSSTableInTheSnapshot() throws IOException
    {
        // the sstables this test writes into the snapshot must not take DSE's generation 4
        generation = 5;
        DseNodeFixture node = node();
        addKeyspace(node, DSE_6_9_26_KEYSPACE, Map.of("class", NTS, "dc1", "3"));
        addTable(node, DSE_6_9_26_KEYSPACE, "lwt", DseFixtures.PAXOS_DSE_6_9_26_LWT_TABLE_ID, List.of(column("id", "int")));
        snapshotSchema(node);

        Files.createDirectories(paxosSnapshot(node));
        for (Path component : DseFixtures.dse6926PaxosSSTableComponents())
            Files.copy(component, paxosSnapshot(node).resolve(component.getFileName()));
        Map<Integer, ByteBuffer> commits = dse6926Commits(paxosSnapshot(node));
        assertThat(commits).hasSize(DseFixtures.PAXOS_DSE_6_9_26_ROWS).containsKeys(0, 99, 9001, 9002, 9003);

        // the open rounds, at later ballots than the commits: the commit of 9001 accepted again and not committed, and an
        // empty proposal for 9002
        TableMetadata dsePaxos = paxosMetadata(false);
        UUID cfId = DseFixtures.PAXOS_DSE_6_9_26_LWT_TABLE_ID;
        UUID proposal = timeUuid(base);
        UUID emptyProposal = timeUuid(base + 10);
        // with the maximum timestamp Long.MIN_VALUE, as the first node of the DSE 6.8.65 test cluster wrote its empty commits
        // (the bytes of the fixture paxos/dse-6.8.65): read with this software's own layout its bytes never look empty,
        // whatever the date
        ByteBuffer empty = dse68EmptyUpdate(cfId, intKey(9002), Long.MIN_VALUE);
        // empty at DSE's layout; at this software's own, the bytes of the maximum timestamp would be read as the key and the flags
        assertThat(PartitionUpdate.PartitionUpdateSerializer.isEmpty(empty.duplicate(), DeserializationHelper.Flag.LOCAL, null, MessagingService.VERSION_DSE_68)).isTrue();
        assertThat(PartitionUpdate.PartitionUpdateSerializer.isEmpty(empty.duplicate(), DeserializationHelper.Flag.LOCAL, null, HCD_VERSION)).isFalse();
        assertThat(PartitionUpdate.PartitionUpdateSerializer.isEmpty(commits.get(9001).duplicate(), DeserializationHelper.Flag.LOCAL, null, MessagingService.VERSION_DSE_68)).isFalse();
        assertThat(PartitionUpdate.PartitionUpdateSerializer.isEmpty(commits.get(9001).duplicate(), DeserializationHelper.Flag.LOCAL, null, HCD_VERSION)).isFalse();
        writeSSTable(paxosSnapshot(node), dsePaxos, BtiFormat.getInstance(),
                     List.of(promise(dsePaxos, intKey(9001), cfId, proposal),
                             accept(dsePaxos, intKey(9001), cfId, proposal, commits.get(9001), DseFixtures.PAXOS_DSE_6_9_26_VERSION),
                             promise(dsePaxos, intKey(9002), cfId, emptyProposal),
                             accept(dsePaxos, intKey(9002), cfId, emptyProposal, empty, DseFixtures.PAXOS_DSE_6_9_26_VERSION)));
        // the data directory: HCD 2.0 committed a round of another table after the upgrade, so system is not restored yet
        TableMetadata paxos = paxosMetadata(true);
        writeSSTable(paxosDirectory(node), paxos, BtiFormat.getInstance(),
                     List.of(commit(paxos, intKey(8), RF3, timeUuid(base + 10), update(8, 1, HCD_VERSION), HCD_VERSION)));

        Map<String, String> before = node.fingerprint();
        Run run = assertExit(run(node), DsePreflight.EXIT_READY);
        assertThat(node.fingerprint()).as("the node's files after the run").isEqualTo(before);
        assertThat(run.report.problems).isEmpty();
        assertThat(run.report.sstables).extracting(s -> s.source).containsExactlyInAnyOrder("data directory", "snapshot " + TAG, "snapshot " + TAG);
        PaxosRollbackReport.SSTable dse = run.report.sstables.stream().filter(s -> s.path.endsWith("bb-4-bti")).findFirst()
                                                             .orElseThrow(() -> new AssertionError("DSE's sstable not read:\n" + run.out));
        assertThat(dse.source).isEqualTo("snapshot " + TAG);
        assertThat(dse.error).isNull();
        assertThat(dse.openRows).isZero();
        // the tool counts the rows live at the real clock: all of them before the first cell expires, none after the last
        long now = FBUtilities.nowInSeconds();
        if (now < DseFixtures.PAXOS_DSE_6_9_26_FIRST_EXPIRY - 60)
            assertThat(dse.rows).isEqualTo(DseFixtures.PAXOS_DSE_6_9_26_ROWS);
        else if (now > DseFixtures.PAXOS_DSE_6_9_26_LAST_EXPIRY + 60)
            assertThat(dse.rows).isZero();
        else
            assertThat(dse.rows).isBetween(0L, (long) DseFixtures.PAXOS_DSE_6_9_26_ROWS);
        assertThat(run.report.rowsRead).isEqualTo(3 + dse.rows);

        assertThat(run.partitions(cfId)).extracting(p -> p.rowKey).containsExactlyInAnyOrder(hex(intKey(9001)), hex(intKey(9002)));
        for (int id : new int[]{ 9001, 9002 })
        {
            PaxosRollbackReport.Partition open = run.partition(cfId, hex(intKey(id)));
            assertThat(open.keyspace).isEqualTo(DSE_6_9_26_KEYSPACE);
            assertThat(open.where).isEqualTo("id = " + id);
            assertThat(open.notes).isEmpty();
            assertThat(only(open).consistency).isEqualTo(PaxosRollback.SERIAL);
            assertThat(only(open).action).isEqualTo(Action.READ);
            assertThat(open.openRows).hasSize(1);
            PaxosRollbackReport.OpenRow row = open.openRows.get(0);
            assertThat(row.source).isEqualTo("snapshot " + TAG);
            assertThat(row.writtenBy).isEqualTo("DSE");
            assertThat(row.version).isEqualTo(DseFixtures.PAXOS_DSE_6_9_26_VERSION);
            assertThat(row.emptyProposal).isEqualTo(id == 9002);
            assertThat(row.note).isNull();
        }
        assertThat(run.partitions(RF3)).isEmpty();
    }

    /**
     * An update of the table {@code cfId} with no row, in the layout of DSE's WRITES version DSE_68 that DSE 6.8 and 6.9
     * store in system.paxos: this software's own layout of it (identical for an empty update) with DSE's maximum timestamp
     * after the table id, which this software cannot write ({@code PartitionUpdate.serializer} refuses DSE_68).
     */
    private static ByteBuffer dse68EmptyUpdate(UUID cfId, ByteBuffer rowKey, long maxTimestamp)
    {
        ByteBuffer own = emptyUpdate(cfId, rowKey, HCD_VERSION);
        ByteBuffer tableId = own.duplicate();
        tableId.limit(tableId.position() + 16);
        ByteBuffer rest = own.duplicate();
        rest.position(rest.position() + 16);
        ByteBuffer dse = ByteBuffer.allocate(own.remaining() + Long.BYTES);
        dse.put(tableId).putLong(maxTimestamp).put(rest).flip();
        return dse;
    }

    /**
     * The {@code most_recent_commit} of each key of DSE 6.9.26's sstable of system.paxos in {@code directory}, read at
     * {@link #DSE_6_9_26_READ_AT}, before its cells expire.
     */
    private static Map<Integer, ByteBuffer> dse6926Commits(Path directory) throws IOException
    {
        TableMetadata dsePaxos = paxosMetadata(false);
        Descriptor descriptor = Descriptor.fromFileWithComponent(new File(directory.resolve("bb-4-bti-Data.db")), false).left;
        SSTableReader reader = SSTableReader.open(null, descriptor, TOCComponent.loadTOC(descriptor, false), TableMetadataRef.forOfflineTools(dsePaxos), false, true);
        Map<Integer, ByteBuffer> commits = new LinkedHashMap<>();
        try (PartitionIterator partitions = UnfilteredPartitionIterators.filter(reader.getScanner(), DSE_6_9_26_READ_AT))
        {
            while (partitions.hasNext())
            {
                try (RowIterator partition = partitions.next())
                {
                    Row row = partition.next();
                    assertThat(partition.hasNext()).isFalse();
                    commits.put(ByteBufferUtil.toInt(partition.partitionKey().getKey()),
                                row.getCell(dsePaxos.getColumn(ByteBufferUtil.bytes("most_recent_commit"))).buffer());
                }
            }
        }
        finally
        {
            reader.selfRef().release();
        }
        return commits;
    }

    @Test
    public void listsWhatItCannotTellConservatively() throws IOException
    {
        DseNodeFixture node = nodeWithEmptyPaxosSnapshot();
        TableMetadata paxos = paxosMetadata(true);
        UUID commit = timeUuid(base + 100);
        writeSSTable(paxosDirectory(node), paxos, BtiFormat.getInstance(), List.of(
            // a proposal ballot that is not a time UUID, older or not
            cells(paxos, intKey(1), RF3, micros(commit), "proposal_ballot", UUID.fromString("0b6a5b3e-1c2d-4e5f-8a9b-0c1d2e3f4a5b"), "proposal",
                  update(1, 1, HCD_VERSION), "proposal_version", HCD_VERSION, "most_recent_commit_at", commit, "most_recent_commit", update(1, 1, HCD_VERSION),
                  "most_recent_commit_version", HCD_VERSION),
            // a version in DSE's range that is none of DSE 6's
            accept(paxos, intKey(2), RF3, timeUuid(base), update(2, 1, HCD_VERSION), 2048),
            // bytes that are no update
            accept(paxos, intKey(3), RF3, timeUuid(base), ByteBuffer.wrap(new byte[]{ 1, 2, 3 }), HCD_VERSION),
            // a proposal ballot without proposal nor version
            cells(paxos, intKey(4), RF3, micros(commit), "proposal_ballot", commit),
            // a commit ballot that is not a time UUID
            cells(paxos, intKey(5), RF3, micros(commit), "proposal_ballot", commit, "proposal", update(5, 1, HCD_VERSION), "proposal_version", HCD_VERSION,
                  "most_recent_commit_at", UUID.fromString("1b6a5b3e-1c2d-4e5f-8a9b-0c1d2e3f4a5b"), "most_recent_commit", update(5, 1, HCD_VERSION),
                  "most_recent_commit_version", HCD_VERSION)));

        Run run = assertExit(run(node), DsePreflight.EXIT_READY);
        assertThat(run.partition(RF3, hex(intKey(1))).openRows.get(0).note)
            .isEqualTo("a ballot is not a time UUID: listed, since the order of the proposal and the commit cannot be told");
        PaxosRollbackReport.OpenRow unknownVersion = run.partition(RF3, hex(intKey(2))).openRows.get(0);
        assertThat(unknownVersion.writtenBy).isEqualTo("DSE");
        assertThat(unknownVersion.emptyProposal).isNull();
        assertThat(unknownVersion.note).isEqualTo("proposal_version 2048 is not a DSE 6 version: whether the proposal is empty is unknown");
        PaxosRollbackReport.OpenRow undecodable = run.partition(RF3, hex(intKey(3))).openRows.get(0);
        assertThat(undecodable.emptyProposal).isNull();
        assertThat(undecodable.note).startsWith("the proposal cannot be decoded (").endsWith("): whether it is empty is unknown");
        PaxosRollbackReport.OpenRow missing = run.partition(RF3, hex(intKey(4))).openRows.get(0);
        assertThat(missing.writtenBy).isEqualTo("unknown");
        assertThat(missing.note).isEqualTo("the proposal or its version is missing: listed conservatively, Paxos ignores such a proposal");
        assertThat(run.partition(RF3, hex(intKey(5))).openRows.get(0).note)
            .isEqualTo("a ballot is not a time UUID: listed, since the order of the proposal and the commit cannot be told");
        assertThat(run.report.partitions).hasSize(5).allMatch(p -> only(p).action == Action.READ);
    }

    @Test
    public void rendersCompositeCollectionTupleAndUdtKeys() throws IOException
    {
        DseNodeFixture node = nodeWithEmptyPaxosSnapshot();
        TableMetadata paxos = paxosMetadata(true);
        ByteBuffer composite = CompositeType.build(ByteBufferAccessor.instance, UTF8Type.instance.decompose("it's"), LongType.instance.decompose(7L));
        ByteBuffer tuple = TupleType.buildValue(Int32Type.instance.decompose(1), UTF8Type.instance.decompose("one"));
        ByteBuffer list = ListType.getInstance(Int32Type.instance, false).decompose(List.of(1, 2));
        ByteBuffer udt = CompositeType.build(ByteBufferAccessor.instance, TupleType.buildValue(UTF8Type.instance.decompose("Main St"), Int32Type.instance.decompose(5)),
                                             Int32Type.instance.decompose(9));
        // a varint not in its shortest form: its literal parses to other bytes, so it is written as a blob
        ByteBuffer varint = ByteBuffer.wrap(new byte[]{ 0, 1 });
        // bytes that are not a list: no literal, no blob form for a non-native type
        ByteBuffer badList = ByteBuffer.wrap(new byte[]{ 0, 0, 0, 9 });
        // not UTF-8: blobAsText would refuse it too
        ByteBuffer badText = ByteBuffer.wrap(new byte[]{ (byte) 0xC3, (byte) 0x28 });
        // not a composite of two components
        ByteBuffer badComposite = ByteBuffer.wrap(new byte[]{ 0, 9, 1 });
        UUID unknownTable = UUID.fromString("abababab-bf41-11f1-9b7d-b57cc343566d");
        List<PartitionUpdate> updates = new ArrayList<>();
        for (Object[] entry : new Object[][]{ { composite, COMPOSITE }, { tuple, TUPLE }, { list, LIST }, { udt, UDT }, { varint, VARINT },
                                              { badList, LIST }, { badText, TEXT }, { badComposite, COMPOSITE }, { intKey(1), unknownTable } })
            updates.add(emptyProposal(paxos, (UUID) entry[1], (ByteBuffer) entry[0]));
        writeSSTable(paxosDirectory(node), paxos, BtiFormat.getInstance(), updates);

        Run run = assertExit(run(node), DsePreflight.EXIT_BLOCKED);
        assertThat(run.report.result).isEqualTo(PaxosRollbackReport.NEEDS_ATTENTION);
        assertThat(run.partition(COMPOSITE, hex(composite)).select).isEqualTo("SELECT a, b FROM rf3.composite WHERE a = 'it''s' AND b = 7 LIMIT 1;");
        assertThat(run.partition(TUPLE, hex(tuple)).where).isEqualTo("k = (1, 'one')");
        assertThat(run.partition(LIST, hex(list)).where).isEqualTo("k = [1, 2]");
        assertThat(run.partition(UDT, hex(udt)).where).isEqualTo("u = {street: 'Main St', zip: 5} AND n = 9");
        PaxosRollbackReport.Partition blob = run.partition(VARINT, hex(varint));
        assertThat(blob.where).isEqualTo("k = blobAsVarint(0x0001)");
        assertThat(blob.notes).containsExactly("the value of k (varint) is written as a blob: it does not render as a varint literal");
        assertThat(only(blob).action).isEqualTo(Action.READ);

        PaxosRollbackReport.Partition notAList = run.partition(LIST, hex(badList));
        assertThat(notAList.select).isNull();
        assertThat(only(notAList).action).isEqualTo(Action.MANUAL);
        assertThat(only(notAList).reason).isEqualTo("the value 0x00000009 of partition key column k (frozen<list<int>>) of rf3.list_key cannot be written as a CQL " +
                                                    "literal; read the partition of row_key 0x00000009 by hand");
        PaxosRollbackReport.Partition notText = run.partition(TEXT, hex(badText));
        assertThat(notText.where).isNull();
        assertThat(only(notText).action).isEqualTo(Action.MANUAL);
        assertThat(only(notText).reason).startsWith("the value 0xc328 of partition key column k (text) of rf3.text_key cannot be written as a CQL literal");
        PaxosRollbackReport.Partition notComposite = run.partition(COMPOSITE, hex(badComposite));
        assertThat(only(notComposite).action).isEqualTo(Action.MANUAL);
        assertThat(only(notComposite).reason).isEqualTo("row_key 0x000901 is not a composite of the 2 partition key columns of rf3.composite; read the partition by hand");
        PaxosRollbackReport.Partition unknown = run.partition(unknownTable, hex(intKey(1)));
        assertThat(unknown.keyspace).isNull();
        assertThat(only(unknown).action).isEqualTo(Action.MANUAL);
        assertThat(only(unknown).reason).startsWith("table " + unknownTable + " is in no schema read (data directories, snapshot " + TAG + "): if it was dropped");

        assertThat(PaxosRollback.cql(run.report, PaxosRollback.SERIAL)).contains("SELECT k FROM rf3.varint_key WHERE k = blobAsVarint(0x0001) LIMIT 1;")
                                                                       .contains("SELECT u, n FROM rf3.udt_key WHERE u = {street: 'Main St', zip: 5} AND n = 9 LIMIT 1;")
                                                                       .doesNotContain("0x00000009")
                                                                       .doesNotContain("text_key");
    }

    // ---- the replication rules ----

    @Test
    public void appliesTheReplicationRules() throws IOException
    {
        DseNodeFixture node = nodeWithEmptyPaxosSnapshot();
        TableMetadata paxos = paxosMetadata(true);
        List<PartitionUpdate> updates = new ArrayList<>();
        for (UUID cfId : List.of(RF3, RF2, RF1, DC1_ONLY, LOCAL, EVERYWHERE))
            updates.add(emptyProposal(paxos, cfId, intKey(1)));
        writeSSTable(paxosDirectory(node), paxos, BtiFormat.getInstance(), updates);

        Run run = assertExit(run(node), DsePreflight.EXIT_BLOCKED);
        assertThat(run.report.result).isEqualTo(PaxosRollbackReport.NEEDS_ATTENTION);
        assertThat(run.reads(RF3, intKey(1))).containsExactly("SERIAL READ");
        PaxosRollbackReport.Read rf2 = only(run.partition(RF2, hex(intKey(1))));
        assertThat(rf2.action).isEqualTo(Action.SKIPPED);
        assertThat(rf2.reason).endsWith("total replication factor 2: every quorum includes all the other replicas");
        PaxosRollbackReport.Read rf1 = only(run.partition(RF1, hex(intKey(1))));
        assertThat(rf1.action).isEqualTo(Action.CANNOT_PROTECT);
        assertThat(rf1.reason).endsWith("total replication factor 1: this node is the whole quorum, so the Paxos state of the upgrade is lost at the rollback " +
                                        "and an open proposal of the snapshot can replay over newer data");
        assertThat(run.reads(DC1_ONLY, intKey(1))).containsExactly("SERIAL READ");
        PaxosRollbackReport.Read local = only(run.partition(LOCAL, hex(intKey(1))));
        assertThat(local.action).isEqualTo(Action.SKIPPED);
        assertThat(local.reason).endsWith("the keyspace is local to each node, no other node holds the partition");
        PaxosRollbackReport.Read everywhere = only(run.partition(EVERYWHERE, hex(intKey(1))));
        assertThat(everywhere.action).isEqualTo(Action.READ);
        assertThat(everywhere.reason).startsWith("keyspace everywhere: EverywhereStrategy (3 nodes in the nodes files, 3 in data center dc1)")
                                     .endsWith("total replication factor 3");
        assertThat(run.out).contains("CANNOT BE PROTECTED at SERIAL")
                           .contains("WARNING: some open rounds cannot be protected")
                           .contains("2 need no read, 1 cannot be protected, 0 need their serial consistency, 0 to check, 0 to handle by hand");
        assertThat(PaxosRollback.cql(run.report, PaxosRollback.SERIAL)).contains("SELECT id FROM rf3.px WHERE id = 1 LIMIT 1;")
                                                                       .contains("SELECT id FROM everywhere.t WHERE id = 1 LIMIT 1;")
                                                                       .doesNotContain("rf2.t").doesNotContain("rf1.t").doesNotContain("local_ks");
    }

    @Test
    public void needsTheSerialConsistencyWhereTheQuorumsDiffer() throws IOException
    {
        DseNodeFixture node = nodeWithEmptyPaxosSnapshot();
        node.editPeers(peers -> peers.get(0).put("data_center", "dc2"));
        TableMetadata paxos = paxosMetadata(true);
        List<PartitionUpdate> updates = new ArrayList<>();
        for (UUID cfId : List.of(MULTI, MULTI_LOCAL1, SIMPLE_MULTI, DC1_ONLY, UNKNOWN_CLASS))
            updates.add(emptyProposal(paxos, cfId, intKey(1)));
        writeSSTable(paxosDirectory(node), paxos, BtiFormat.getInstance(), updates);

        // a cluster of two data centers: no read without the serial consistency of the keyspaces whose replicas are in both
        Run run = assertExit(run(node), DsePreflight.EXIT_BLOCKED);
        assertThat(run.report.singleDataCenter).isFalse();
        assertThat(run.reads(MULTI, intKey(1))).containsExactly("null CHOOSE");
        assertThat(only(run.partition(MULTI, hex(intKey(1)))).reason)
            .isEqualTo("keyspace multi: NetworkTopologyStrategy {dc1=3, dc2=3}: SERIAL and LOCAL_SERIAL may be different quorums, and a serial read at the " +
                       "consistency the table is not written at can complete an old proposal over a newer committed value: no read is listed; pass " +
                       "--serial-consistency multi=SERIAL or multi=LOCAL_SERIAL, the serial consistency of the application");
        assertThat(run.reads(MULTI_LOCAL1, intKey(1))).containsExactly("null CHOOSE");
        assertThat(run.reads(SIMPLE_MULTI, intKey(1))).containsExactly("null CHOOSE");
        assertThat(only(run.partition(SIMPLE_MULTI, hex(intKey(1)))).reason).contains("SimpleStrategy places replicas regardless of data centers");
        assertThat(run.reads(DC1_ONLY, intKey(1))).containsExactly("SERIAL READ");
        assertThat(run.reads(UNKNOWN_CLASS, intKey(1))).containsExactly("null CHOOSE");
        assertThat(only(run.partition(UNKNOWN_CLASS, hex(intKey(1)))).reason).contains("(the replication class com.example.ShardedStrategy is not one the tool knows)");
        assertThat(PaxosRollback.cql(run.report, PaxosRollback.SERIAL)).startsWith("-- INCOMPLETE: 4 partitions need a decision (--serial-consistency, or by hand). " +
                                                                                    "See the report before running this file.\n")
                                                                        .endsWith("CONSISTENCY SERIAL;\nSELECT id FROM dc1_only.t WHERE id = 1 LIMIT 1;\n");
        assertThat(PaxosRollback.cql(run.report, PaxosRollback.LOCAL_SERIAL)).startsWith("-- INCOMPLETE: 4 partitions need a decision (--serial-consistency, or " +
                                                                                         "by hand). See the report before running this file.\n")
                                                                             .endsWith("-- No read is needed at LOCAL_SERIAL.\n");

        // the operator names them: one keyspace at LOCAL_SERIAL, every other at SERIAL
        Path cql = temporaryFolder.getRoot().toPath().resolve("multi");
        Run known = assertExit(run(node, "--serial-consistency", "multi=local_serial", "--serial-consistency", "*=SERIAL", "--cql", cql.toString()),
                               DsePreflight.EXIT_BLOCKED);
        assertThat(known.reads(MULTI, intKey(1))).containsExactly("LOCAL_SERIAL READ");
        assertThat(only(known.partition(MULTI, hex(intKey(1)))).reason).contains("--serial-consistency multi=LOCAL_SERIAL").endsWith("replication factor in data center dc1 3");
        assertThat(known.reads(MULTI_LOCAL1, intKey(1))).containsExactly("SERIAL READ");
        assertThat(only(known.partition(MULTI_LOCAL1, hex(intKey(1)))).reason).contains("--serial-consistency *=SERIAL");
        assertThat(known.reads(SIMPLE_MULTI, intKey(1))).containsExactly("SERIAL READ");
        assertThat(known.reads(DC1_ONLY, intKey(1))).containsExactly("SERIAL READ");
        assertThat(known.reads(UNKNOWN_CLASS, intKey(1))).containsExactly("SERIAL CHECK");
        assertThat(new String(Files.readAllBytes(cql), StandardCharsets.UTF_8)).startsWith("-- INCOMPLETE: 1 partition has a read to check by hand. See the report " +
                                                                                           "before running this file.\n")
                                                                               .endsWith("CONSISTENCY SERIAL;\n" +
                                                                                         "SELECT id FROM dc1_only.t WHERE id = 1 LIMIT 1;\n" +
                                                                                         "SELECT id FROM multi_local1.t WHERE id = 1 LIMIT 1;\n" +
                                                                                         "SELECT id FROM simple_multi.t WHERE id = 1 LIMIT 1;\n" +
                                                                                         "-- CHECK by hand that this read is needed and enough (see the report):\n" +
                                                                                         "SELECT id FROM unknown_class.t WHERE id = 1 LIMIT 1;\n");
        assertThat(new String(Files.readAllBytes(cql.resolveSibling("multi-local-serial.cql")), StandardCharsets.UTF_8))
            .startsWith("-- NOTE: this file leaves nothing out; the report needs attention for the other serial consistency.\n")
            .contains("Run it through a node of data center dc1 (this node's) other than this node, never through a node of another\n-- data center")
            .endsWith("CONSISTENCY LOCAL_SERIAL;\nSELECT id FROM multi.t WHERE id = 1 LIMIT 1;\n");

        // at LOCAL_SERIAL, a local replication factor of 1 cannot be protected, and one of 2 needs no read
        Run local = assertExit(run(node, "--serial-consistency", "*=LOCAL_SERIAL"), DsePreflight.EXIT_BLOCKED);
        assertThat(local.reads(MULTI_LOCAL1, intKey(1))).containsExactly("LOCAL_SERIAL CANNOT_PROTECT");
        assertThat(local.reads(SIMPLE_MULTI, intKey(1))).containsExactly("LOCAL_SERIAL CHECK");
    }

    @Test
    public void checksTheDataCenter() throws IOException
    {
        DseNodeFixture node = nodeWithEmptyPaxosSnapshot();
        node.editPeers(peers -> peers.get(0).put("data_center", "dc2"));
        TableMetadata paxos = paxosMetadata(true);
        writeSSTable(paxosDirectory(node), paxos, BtiFormat.getInstance(), List.of(emptyProposal(paxos, DC1_ONLY, intKey(1)), emptyProposal(paxos, MULTI, intKey(1)),
                                                                                    emptyProposal(paxos, NO_REPLICA, intKey(1))));

        // a data center that contradicts nodes/local: a problem, and the replication that puts no replica there is too
        Run wrong = assertExit(run(node, "--dc", "dc2", "--serial-consistency", "*=LOCAL_SERIAL"), DsePreflight.EXIT_BLOCKED);
        assertThat(wrong.report.dataCenter).isEqualTo("dc2");
        assertThat(wrong.report.dataCenterSource).isEqualTo("--dc");
        assertThat(wrong.report.problems).anyMatch(p -> p.startsWith("--dc dc2 but ") && p.endsWith("/nodes/local records data center dc1: check --dc"));
        PaxosRollbackReport.Read none = only(wrong.partition(DC1_ONLY, hex(intKey(1))));
        assertThat(none.action).isEqualTo(Action.MANUAL);
        assertThat(none.consistency).isNull();
        assertThat(none.reason).isEqualTo("keyspace dc1_only: NetworkTopologyStrategy {dc1=3}: the replication places no replica in data center dc2 (--dc), yet " +
                                          "this node holds Paxos state of it: the replication (the schema, or the nodes files for EverywhereStrategy) or this " +
                                          "node's data center is wrong; no read is listed, check them");
        assertThat(wrong.reads(MULTI, intKey(1))).containsExactly("LOCAL_SERIAL READ");
        // the same at SERIAL, and without --serial-consistency
        assertThat(assertExit(run(node, "--dc", "dc2", "--serial-consistency", "*=SERIAL"), DsePreflight.EXIT_BLOCKED).reads(DC1_ONLY, intKey(1)))
            .containsExactly("null MANUAL");
        assertThat(assertExit(run(node, "--dc", "dc2"), DsePreflight.EXIT_BLOCKED).reads(DC1_ONLY, intKey(1))).containsExactly("null MANUAL");
        assertThat(only(wrong.partition(NO_REPLICA, hex(intKey(1)))).reason)
            .isEqualTo("keyspace no_replica: NetworkTopologyStrategy {dc1=0}: the replication places no replica of the keyspace, yet this node holds Paxos state " +
                       "of it: the replication (the schema, or the nodes files for EverywhereStrategy) or this node's data center is wrong; no read is listed, " +
                       "check them");

        // a data center nothing knows
        Run unknown = assertExit(run(node, "--dc", "dc9", "--serial-consistency", "*=SERIAL"), DsePreflight.EXIT_BLOCKED);
        assertThat(unknown.report.problems).anyMatch(p -> p.startsWith("data center dc9 (--dc) is neither in the nodes files nor in the replication of any keyspace"));

        // a keyspace named with --serial-consistency that is in no schema
        Run typo = assertExit(run(node, "--serial-consistency", "mutli=SERIAL", "--serial-consistency", "*=SERIAL"), DsePreflight.EXIT_BLOCKED);
        assertThat(typo.report.problems).containsExactly("--serial-consistency names keyspace mutli, which is in no schema read: check its name");
        assertThat(typo.reads(DC1_ONLY, intKey(1))).containsExactly("SERIAL READ");
    }

    @Test
    public void takesTheDataCenterFromTheRackdcFileWithoutNodesFiles() throws IOException
    {
        DseNodeFixture node = nodeWithEmptyPaxosSnapshot();
        Files.delete(node.metadata.resolve("nodes").resolve("local"));
        TableMetadata paxos = paxosMetadata(true);
        writeSSTable(paxosDirectory(node), paxos, BtiFormat.getInstance(), List.of(emptyProposal(paxos, RF3, intKey(1)), emptyProposal(paxos, DC1_ONLY, intKey(1)),
                                                                                    emptyProposal(paxos, EVERYWHERE, intKey(1))));

        Run run = assertExit(run(node), DsePreflight.EXIT_BLOCKED);
        assertThat(run.report.dataCenter).isEqualTo("dc1");
        assertThat(run.report.dataCenterSource).isEqualTo("cassandra-rackdc.properties");
        assertThat(run.report.singleDataCenter).isNull();
        assertThat(run.reads(RF3, intKey(1))).containsExactly("null CHOOSE");
        assertThat(run.reads(DC1_ONLY, intKey(1))).containsExactly("SERIAL READ");
        assertThat(run.reads(EVERYWHERE, intKey(1))).containsExactly("null CHOOSE");
        assertThat(only(run.partition(EVERYWHERE, hex(intKey(1)))).reason).contains("EverywhereStrategy places a replica on every node, and the nodes files do not tell");

        // an absent nodes/peers tells nothing about the other nodes either
        DseNodeFixture noPeers = nodeWithEmptyPaxosSnapshot();
        Files.delete(noPeers.metadata.resolve("nodes").resolve("peers"));
        writeSSTable(paxosDirectory(noPeers), paxos, BtiFormat.getInstance(), List.of(emptyProposal(paxos, RF3, intKey(1))));
        Run peersAbsent = assertExit(run(noPeers), DsePreflight.EXIT_BLOCKED);
        assertThat(peersAbsent.report.dataCenterSource).endsWith("/nodes/local");
        assertThat(peersAbsent.report.singleDataCenter).isNull();
        assertThat(peersAbsent.reads(RF3, intKey(1))).containsExactly("null CHOOSE");

        // no data center at all
        Files.delete(node.conf.resolve("cassandra-rackdc.properties"));
        Run unknown = assertExit(run(node, "--serial-consistency", "*=LOCAL_SERIAL"), DsePreflight.EXIT_BLOCKED);
        assertThat(unknown.report.dataCenter).isNull();
        assertThat(only(unknown.partition(DC1_ONLY, hex(intKey(1)))).reason).contains("this node's data center is unknown, pass --dc: the read is listed");
        assertThat(unknown.reads(DC1_ONLY, intKey(1))).containsExactly("LOCAL_SERIAL CHECK");
        assertThat(PaxosRollback.cql(unknown.report, PaxosRollback.LOCAL_SERIAL)).contains("a node of data center <this node's data center>");
    }

    // ---- inputs ----

    @Test
    public void readsTheSchemaOfTheSnapshotAlone() throws IOException
    {
        DseNodeFixture node = nodeWithEmptyPaxosSnapshot();
        // the schema sstables of the data directory are gone: the snapshot's describe the tables
        try (Stream<Path> tables = Files.list(node.data.resolve("system_schema")))
        {
            for (Path table : (Iterable<Path>) tables::iterator)
                try (Stream<Path> files = Files.list(table))
                {
                    for (Path file : (Iterable<Path>) files::iterator)
                        if (Files.isRegularFile(file))
                            Files.delete(file);
                }
        }
        TableMetadata paxos = paxosMetadata(true);
        Path quarantine = node.root.resolve("quarantine").resolve("system").resolve(PAXOS_DIRECTORY);
        Descriptor descriptor = writeSSTable(quarantine, paxos, BtiFormat.getInstance(),
                                             List.of(accept(paxos, intKey(4), RF3, timeUuid(base), update(4, 1, HCD_VERSION), HCD_VERSION)));
        // without the components a reader may write when they are missing, which a read-only run must not write
        Files.delete(descriptor.fileFor(BtiFormat.Components.FILTER).toPath());
        Files.delete(descriptor.fileFor(BtiFormat.Components.TOC).toPath());

        Map<String, String> before = node.fingerprint();
        Run run = assertExit(run(node, "--paxos-dir", quarantine.toString()), DsePreflight.EXIT_READY);
        assertThat(node.fingerprint()).as("the node's files after the run").isEqualTo(before);
        assertThat(run.report.schemas).containsExactly("snapshot " + TAG);
        assertThat(run.report.problems).isEmpty();
        assertThat(run.report.sstables).hasSize(1);
        assertThat(run.report.sstables.get(0).source).isEqualTo("--paxos-dir " + quarantine);
        assertThat(run.partition(RF3, hex(intKey(4))).where).isEqualTo("id = 4");
    }

    /**
     * An sstable of the big format is never opened, as opening it loads its summary, which deletes a Summary.db it cannot
     * load: a system.paxos sstable of the big format is a problem of the report, a schema of the big format is not read,
     * and the files of the node stay as they are. Neither DSE 6 nor HCD 2.0 in DSE_6 mode writes the big format, and a
     * node being rolled back cannot rewrite it, so no text names upgradesstables.
     */
    @Test
    public void opensNoSSTableOfTheBigFormatAndLeavesItsFilesAsTheyAre() throws IOException
    {
        DseNodeFixture node = nodeWithEmptyPaxosSnapshot();
        TableMetadata paxos = paxosMetadata(true);
        Path quarantine = node.root.resolve("quarantine").resolve("system").resolve(PAXOS_DIRECTORY);
        Descriptor big = writeSSTable(quarantine, paxos, BigFormat.getInstance(),
                                      List.of(accept(paxos, intKey(4), RF3, timeUuid(base), update(4, 1, HCD_VERSION), HCD_VERSION)));
        Path summary = big.fileFor(BigFormat.Components.SUMMARY).toPath();
        unloadableSummary(summary);
        assertThat(summaryLeftByOpening(big, paxos, summary)).as("whether opening a copy of the sstable leaves its Summary.db").isFalse();

        Map<String, String> before = node.fingerprint();
        Run run = assertExit(run(node, "--paxos-dir", quarantine.toString()), DsePreflight.EXIT_BLOCKED);
        assertThat(node.fingerprint()).as("the node's files after the run").isEqualTo(before);
        String refused = big.baseFile() + " is in the big format, which this tool does not read: opening it could delete or rewrite some of its files";
        assertThat(run.report.problems).containsExactly("cannot read " + big.baseFile() + " (java.io.IOException: " + refused + "): check it by hand with " +
                                                        "sstabledump on a copy of its files");
        assertThat(run.report.sstables).hasSize(1);
        assertThat(run.report.sstables.get(0).format).startsWith("big ");
        assertThat(run.report.sstables.get(0).error).isEqualTo("java.io.IOException: " + refused);
        assertThat(run.out + run.err).doesNotContain("upgradesstables");

        // a schema of the big format in the data directory: the snapshot's describes the tables alone
        node.writeRows(BigFormat.getInstance(), "system_schema", "keyspaces", KEYSPACES_KEY, List.of(),
                       row("big_written").with("durable_writes", true).with("replication", simple(3)));
        unloadableSummary(onlyBigSummary(node.data.resolve("system_schema")));
        Map<String, String> beforeSchema = node.fingerprint();
        Run schema = assertExit(run(node, "--not-restored"), DsePreflight.EXIT_BLOCKED);
        assertThat(node.fingerprint()).as("the node's files after the run").isEqualTo(beforeSchema);
        assertThat(schema.report.schemas).containsExactly("snapshot " + TAG);
        assertThat(schema.report.problems).hasSize(1);
        assertThat(schema.report.problems.get(0)).startsWith("cannot read system_schema of the data directories (java.io.IOException: ")
                                                 .contains(" is in the big format, which this tool does not read: opening it could delete or rewrite some of its files")
                                                 .endsWith("): the tables are taken from the schema of the snapshot " + TAG + " alone, and a partition of a table " +
                                                           "not in it is listed to handle by hand; this problem needs no other action when no partition is listed to handle by hand");
        assertThat(schema.out + schema.err).doesNotContain("upgradesstables");

        // the same sstable in the snapshot too, as the snapshot links it: no schema can be read
        snapshotSchema(node);
        Map<String, String> beforeNone = node.fingerprint();
        Run none = assertExit(run(node), DsePreflight.EXIT_ERROR);
        assertThat(node.fingerprint()).as("the node's files after the run").isEqualTo(beforeNone);
        assertThat(none.err).contains("cannot read system_schema in the data directories ")
                            .contains("cannot read system_schema of the data directories (java.io.IOException: ")
                            .contains("cannot read system_schema of the snapshot " + TAG + " (java.io.IOException: ")
                            .contains(" is in the big format, which this tool does not read")
                            .doesNotContain("upgradesstables");
    }

    /** Makes a Summary.db of the big format one that cannot be loaded: its minimum index interval is not the table's. */
    private static void unloadableSummary(Path summary) throws IOException
    {
        byte[] bytes = Files.readAllBytes(summary);
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        buffer.putInt(0, buffer.getInt(0) + 1);
        Files.write(summary, bytes);
    }

    /** Opens a copy of a big sstable offline, as the reader would, and returns whether the copy of its Summary.db is left. */
    private boolean summaryLeftByOpening(Descriptor descriptor, TableMetadata metadata, Path summary) throws IOException
    {
        Path copy = Files.createDirectories(temporaryFolder.newFolder().toPath().resolve("system").resolve(PAXOS_DIRECTORY));
        try (Stream<Path> files = Files.list(summary.getParent()))
        {
            for (Path file : (Iterable<Path>) files::iterator)
                if (file.getFileName().toString().startsWith(descriptor.baseFile().name()))
                    Files.copy(file, copy.resolve(file.getFileName()));
        }
        Descriptor copied = new Descriptor(new File(copy), descriptor.ksname, descriptor.cfname, descriptor.id, descriptor.version.format);
        SSTableReader reader = SSTableReader.open(null, copied, TOCComponent.loadTOC(copied, false), TableMetadataRef.forOfflineTools(metadata), false, true);
        reader.selfRef().release();
        return Files.exists(copy.resolve(summary.getFileName()));
    }

    /** The one Summary.db of the big format in the table directories of a keyspace directory, snapshots left out. */
    private static Path onlyBigSummary(Path keyspaceDirectory) throws IOException
    {
        try (Stream<Path> files = Files.walk(keyspaceDirectory, 2))
        {
            List<Path> summaries = files.filter(f -> f.getFileName().toString().endsWith("-big-Summary.db")).collect(Collectors.toList());
            assertThat(summaries).hasSize(1);
            return summaries.get(0);
        }
    }

    /**
     * A data directory without system.paxos is a problem even when another one has it: absent, its system directory not
     * listable, or without a directory of system.paxos, its open rounds would be missed.
     */
    @Test
    public void reportsEveryDataDirectoryWithoutPaxosState() throws IOException
    {
        DseNodeFixture node = nodeWithEmptyPaxosSnapshot();
        writeSSTable(paxosDirectory(node), paxosMetadata(true), BtiFormat.getInstance(), List.of(emptyProposal(paxosMetadata(true), RF3, intKey(1))));
        Path absent = node.root.resolve("absent");
        Path noSystem = Files.createDirectories(node.root.resolve("no-system"));
        Path noPaxos = Files.createDirectories(node.root.resolve("no-paxos").resolve("system").resolve("batches-919a4bc57a333573b03e13fc3f68b465")).getParent().getParent();
        Map<String, String> before = node.fingerprint();
        Run run = assertExit(run(node, "--data-dir", node.data.toString(), "--data-dir", absent.toString(), "--data-dir", noSystem.toString(),
                                 "--data-dir", noPaxos.toString()), DsePreflight.EXIT_BLOCKED);
        assertThat(node.fingerprint()).as("the node's files after the run").isEqualTo(before);
        assertThat(run.report.result).isEqualTo(PaxosRollbackReport.NEEDS_ATTENTION);
        assertThat(run.report.problems).containsExactly("no directory of system.paxos (system/paxos-<id>) in the data directories " + absent + " (not a directory), " +
                                                        noSystem + " (no system directory), " + noPaxos + " (no system/paxos-<id>): the Paxos state of those " +
                                                        "data directories was not read; check the data directories (--data-dir)");
        assertThat(run.reads(RF3, intKey(1))).containsExactly("SERIAL READ");

        // one of them
        Run one = assertExit(run(node, "--data-dir", node.data.toString(), "--data-dir", absent.toString()), DsePreflight.EXIT_BLOCKED);
        assertThat(one.report.problems).containsExactly("no directory of system.paxos (system/paxos-<id>) in the data directory " + absent + " (not a directory): " +
                                                        "the Paxos state of that data directory was not read; check the data directories (--data-dir)");
    }

    /** A data directory whose system directory cannot be listed is a problem too; not run where permissions do not apply (root). */
    @Test
    public void reportsADataDirectoryThatCannotBeListed() throws IOException
    {
        DseNodeFixture node = nodeWithEmptyPaxosSnapshot();
        writeSSTable(paxosDirectory(node), paxosMetadata(true), BtiFormat.getInstance(), List.of(emptyProposal(paxosMetadata(true), RF3, intKey(1))));
        Path system = Files.createDirectories(node.root.resolve("unlistable").resolve("system"));
        Map<String, String> before = node.fingerprint();
        system.toFile().setReadable(false);
        try
        {
            Assume.assumeFalse("the directory is still readable", Files.isReadable(system));
            Run run = assertExit(run(node, "--data-dir", node.data.toString(), "--data-dir", system.getParent().toString()), DsePreflight.EXIT_BLOCKED);
            assertThat(run.report.problems).containsExactly("no directory of system.paxos (system/paxos-<id>) in the data directory " + system.getParent() +
                                                            " (its system directory cannot be listed): the Paxos state of that data directory was not read; " +
                                                            "check the data directories (--data-dir)");
        }
        finally
        {
            system.toFile().setReadable(true);
        }
        assertThat(node.fingerprint()).as("the node's files after the run").isEqualTo(before);
    }

    /** An unreadable cassandra-rackdc.properties is a problem when nothing else names this node's data center. */
    @Test
    public void reportsAnUnreadableRackdcFileWhenTheDataCenterIsUnknown() throws IOException
    {
        DseNodeFixture node = nodeWithEmptyPaxosSnapshot();
        Path nodes = node.metadata.resolve("nodes");
        Files.delete(nodes.resolve("local"));
        Files.delete(nodes.resolve("peers"));
        Path rackdc = node.root.resolve("absent.properties");
        Run run = assertExit(run(node, "--rackdc", rackdc.toString(), "--not-restored"), DsePreflight.EXIT_BLOCKED);
        assertThat(run.report.dataCenter).isNull();
        assertThat(run.report.problems).hasSize(1);
        assertThat(run.report.problems.get(0)).startsWith("cannot read cassandra-rackdc.properties " + rackdc + " (")
                                              .endsWith("), and the nodes files name no data center for this node: pass --dc, or --rackdc <file>");
        // the data center given: no problem
        Run given = assertExit(run(node, "--rackdc", rackdc.toString(), "--not-restored", "--dc", "dc1"), DsePreflight.EXIT_READY);
        assertThat(given.report.problems).isEmpty();
    }

    /** An unreadable cassandra.yaml is a problem when DSE's default directories are assumed in its place. */
    @Test
    public void reportsAnUnreadableCassandraYaml() throws IOException
    {
        DseNodeFixture node = nodeWithEmptyPaxosSnapshot();
        writeSSTable(paxosDirectory(node), paxosMetadata(true), BtiFormat.getInstance(), List.of(emptyProposal(paxosMetadata(true), RF3, intKey(1))));
        Path yaml = node.root.resolve("absent.yaml");
        // with every directory given, nothing is assumed
        Run given = assertExit(run(node, "--cassandra-yaml", yaml.toString(), "--data-dir", node.data.toString(), "--metadata-dir", node.metadata.toString()),
                               DsePreflight.EXIT_READY);
        assertThat(given.report.problems).isEmpty();
        // the default directories of DSE are those of the machine: the runs that assume them only where they do not exist
        Assume.assumeFalse("/var/lib/cassandra exists", Files.exists(Path.of("/var/lib/cassandra")));
        Run run = assertExit(run(node, "--cassandra-yaml", yaml.toString(), "--data-dir", node.data.toString(), "--dc", "dc1"), DsePreflight.EXIT_BLOCKED);
        assertThat(run.report.problems).hasSize(1);
        assertThat(run.report.problems.get(0)).startsWith("cannot read cassandra.yaml " + yaml + " (")
                                              .endsWith("): DSE's defaults are assumed (metadata_directory: /var/lib/cassandra/metadata), which may not be " +
                                                        "this node's directories: pass --conf-dir or --cassandra-yaml, or --data-dir and --metadata-dir");
        // the default data directory holds no snapshot: the usage error names the unreadable cassandra.yaml too
        Run noSnapshot = assertExit(run(node, "--cassandra-yaml", yaml.toString(), "--metadata-dir", node.metadata.toString()), DsePreflight.EXIT_ERROR);
        assertThat(noSnapshot.err).contains("no pre-hcd-<timestamp> snapshot of the system or system_schema tables in the data directories [/var/lib/cassandra/data]")
                                  .contains(" (problems found before: cannot read cassandra.yaml " + yaml + " (")
                                  .contains("DSE's defaults are assumed (data_file_directories: /var/lib/cassandra/data)");
    }

    /**
     * A proposal no later than its commit is open again for DSE once the commit has expired, when the commit expires first:
     * listed when that happens within the margin of the run; a proposal whose version is below 4.0 is written by an unknown
     * software and not decoded.
     */
    @Test
    public void listsAProposalWhoseCommitExpiresFirst() throws IOException
    {
        DseNodeFixture node = nodeWithEmptyPaxosSnapshot();
        TableMetadata paxos = paxosMetadata(true);
        UUID proposal = timeUuid(base);
        UUID commit = timeUuid(base + 10);
        int soon = (int) TimeUnit.HOURS.toSeconds(3);
        List<PartitionUpdate> updates = new ArrayList<>();
        // 1: the commit expires in 3 hours, the proposal in 10 days: listed
        updates.add(cells(paxos, intKey(1), RF3, micros(proposal), TTL, "proposal_ballot", proposal, "proposal", update(1, 1, HCD_VERSION), "proposal_version", HCD_VERSION));
        updates.add(cells(paxos, intKey(1), RF3, micros(commit), soon, "most_recent_commit_at", commit, "most_recent_commit", update(1, 2, HCD_VERSION),
                          "most_recent_commit_version", HCD_VERSION));
        // 2: the commit expires in 10 days, the proposal in 3 hours: not listed
        updates.add(cells(paxos, intKey(2), RF3, micros(proposal), soon, "proposal_ballot", proposal, "proposal", update(2, 1, HCD_VERSION), "proposal_version", HCD_VERSION));
        updates.add(cells(paxos, intKey(2), RF3, micros(commit), TTL, "most_recent_commit_at", commit, "most_recent_commit", update(2, 2, HCD_VERSION),
                          "most_recent_commit_version", HCD_VERSION));
        // 3: the commit expires first, but beyond the margin: not listed
        int later = (int) (PaxosRollback.COMMIT_EXPIRY_MARGIN_SECONDS + TimeUnit.HOURS.toSeconds(1));
        updates.add(cells(paxos, intKey(3), RF3, micros(proposal), TTL, "proposal_ballot", proposal, "proposal", update(3, 1, HCD_VERSION), "proposal_version", HCD_VERSION));
        updates.add(cells(paxos, intKey(3), RF3, micros(commit), later, "most_recent_commit_at", commit, "most_recent_commit", update(3, 2, HCD_VERSION),
                          "most_recent_commit_version", HCD_VERSION));
        // 4: the commit never expires: not listed
        updates.add(cells(paxos, intKey(4), RF3, micros(proposal), TTL, "proposal_ballot", proposal, "proposal", update(4, 1, HCD_VERSION), "proposal_version", HCD_VERSION));
        updates.add(cells(paxos, intKey(4), RF3, micros(commit), (Integer) null, "most_recent_commit_at", commit, "most_recent_commit", update(4, 2, HCD_VERSION),
                          "most_recent_commit_version", HCD_VERSION));
        // 5: an open proposal of a version below 4.0, after a commit of HCD 2.0
        updates.add(cells(paxos, intKey(5), RF3, micros(proposal), TTL, "most_recent_commit_at", proposal, "most_recent_commit", update(5, 1, HCD_VERSION),
                          "most_recent_commit_version", HCD_VERSION));
        updates.add(cells(paxos, intKey(5), RF3, micros(commit), TTL, "proposal_ballot", commit, "proposal", update(5, 2, HCD_VERSION), "proposal_version", 11));
        writeSSTable(paxosDirectory(node), paxos, BtiFormat.getInstance(), updates);

        Run run = assertExit(run(node), DsePreflight.EXIT_READY);
        assertThat(run.report.partitions).extracting(p -> p.where).containsExactly("id = 1", "id = 5");
        PaxosRollbackReport.OpenRow expiring = run.partition(RF3, hex(intKey(1))).openRows.get(0);
        assertThat(expiring.note).startsWith("the commit, later than the proposal, expires at ")
                                 .endsWith(", within 24 hours of this run and before the proposal: listed, since DSE would read the proposal alone once the commit " +
                                           "has expired");
        assertThat(expiring.writtenBy).isEqualTo("HCD 2.0");
        PaxosRollbackReport.OpenRow old = run.partition(RF3, hex(intKey(5))).openRows.get(0);
        assertThat(old.writtenBy).isEqualTo("unknown");
        assertThat(old.emptyProposal).isNull();
        assertThat(old.note).isEqualTo("proposal_version 11 is below 4.0, neither DSE 6 nor HCD 2.0: whether the proposal is empty is unknown");
        String validity = "Run the tool again if DSE does not start before " + run.report.validUntil + " (within 24 hours of the run): the Paxos state is " +
                          "read as of the run.";
        assertThat(Instant.parse(run.report.validUntil)).isAfter(FBUtilities.now().plus(23, ChronoUnit.HOURS)).isBefore(FBUtilities.now().plus(25, ChronoUnit.HOURS));
        assertThat(run.out).contains(validity);
        assertThat(PaxosRollback.cql(run.report, PaxosRollback.SERIAL)).contains("\n-- " + validity + "\n");
        assertThat(PaxosRollback.cql(run.report, PaxosRollback.LOCAL_SERIAL)).contains("\n-- " + validity + "\n");
    }

    @Test
    public void reportsTheSSTablesItCannotRead() throws IOException
    {
        DseNodeFixture node = nodeWithEmptyPaxosSnapshot();
        TableMetadata paxos = paxosMetadata(true);
        Descriptor corrupt = writeSSTable(paxosDirectory(node), paxos, BtiFormat.getInstance(), List.of(emptyProposal(paxos, RF3, intKey(1))));
        Path data = corrupt.fileFor(BtiFormat.Components.DATA).toPath();
        byte[] garbage = new byte[(int) Files.size(data)];
        Arrays.fill(garbage, (byte) 0x5a);
        Files.write(data, garbage);
        // an sstable of another table given with --paxos-dir
        Path other = node.root.resolve("elsewhere").resolve("system").resolve("batches-919a4bc57a333573b03e13fc3f68b465");
        writeSSTable(other, paxos, BtiFormat.getInstance(), List.of(emptyProposal(paxos, RF3, intKey(2))));
        writeSSTable(other, paxos, BtiFormat.getInstance(), List.of(emptyProposal(paxos, RF3, intKey(3))));

        Run run = assertExit(run(node, "--paxos-dir", other.toString()), DsePreflight.EXIT_BLOCKED);
        assertThat(run.report.result).isEqualTo(PaxosRollbackReport.NEEDS_ATTENTION);
        assertThat(run.report.sstables).extracting(s -> s.error == null).containsExactly(false, false, false);
        assertThat(run.report.problems).anyMatch(p -> p.startsWith("cannot read " + corrupt.baseFile()) && p.endsWith("check it by hand with sstabledump on a copy of its files"));
        assertThat(run.report.problems).anyMatch(p -> p.endsWith("not an sstable of system.paxos: its directory names system.batches (the directory of an sstable of " +
                                                                 "system.paxos is laid out as <...>/system/paxos-<id>)"));
        assertThat(run.out).contains("  PROBLEM: cannot read ");
        // the --paxos-dir holds an sstable, but none of system.paxos
        assertThat(run.report.problems).hasSize(4)
                                       .filteredOn(p -> p.startsWith("--paxos-dir "))
                                       .containsExactly("--paxos-dir " + other + " holds no system.paxos sstable (2 sstables of other tables): nothing was " +
                                                        "read from it. --paxos-dir" + PAXOS_DIRECTORY_EXPECTED);
        assertThat(run.report.directories).extracting(d -> d.source + ' ' + d.sstables + ' ' + d.otherTableSSTables + ' ' + d.rows)
                                          .containsExactly("data directory 1 0 0", "snapshot " + TAG + " 0 0 0", "--paxos-dir " + other + " 2 2 0");
        assertThat(run.out).contains("  --paxos-dir " + other + ": " + other + " (2 sstables, 2 of other tables, 0 rows)\n");

        // one sstable of another table
        Path single = node.root.resolve("elsewhere2").resolve("system").resolve("batches-919a4bc57a333573b03e13fc3f68b465");
        Descriptor singleSSTable = writeSSTable(single, paxos, BtiFormat.getInstance(), List.of(emptyProposal(paxos, RF3, intKey(4))));
        Run singleRun = assertExit(run(node, "--paxos-dir", single.toString()), DsePreflight.EXIT_BLOCKED);
        assertThat(singleRun.report.problems).hasSize(3);
        assertThat(singleRun.report.problems.get(0)).startsWith("cannot read " + corrupt.baseFile() + " (");
        assertThat(singleRun.report.problems.subList(1, 3))
            .containsExactly(singleSSTable.baseFile() + ": not an sstable of system.paxos: its directory names system.batches (the directory of an sstable " +
                             "of system.paxos is laid out as <...>/system/paxos-<id>)",
                             "--paxos-dir " + single + " holds no system.paxos sstable (1 sstable of another table): nothing was read from it. " +
                             "--paxos-dir" + PAXOS_DIRECTORY_EXPECTED);
        assertThat(singleRun.out).contains("  --paxos-dir " + single + ": " + single + " (1 sstable, 1 of another table, 0 rows)\n");
    }

    @Test
    public void recognisesTheLayoutOfTheRootAndOfTheDirectoriesRightUnderIt()
    {
        // the root has no name and no parent, a directory right under it has a parent without a name: no failure, no layout
        File root = new File(Path.of("/"));
        assertThat(PaxosRollback.nameOf(root)).isEmpty();
        assertThat(PaxosRollback.inSystemDirectory(root)).isFalse();
        assertThat(PaxosRollback.isTableDirectory(root, "paxos")).isFalse();
        assertThat(PaxosRollback.inSystemDirectory(new File(Path.of("/quarantine")))).isFalse();
        assertThat(PaxosRollback.inSystemDirectory(new File(Path.of("/system/" + PAXOS_DIRECTORY)))).isTrue();
        assertThat(PaxosRollback.isTableDirectory(new File(Path.of("/system/" + PAXOS_DIRECTORY)), "paxos")).isTrue();
        assertThat(PaxosRollback.nameOf(new File(Path.of("/quarantine")))).isEqualTo("quarantine");
    }

    @Test
    public void noReadIsNeededWhereEveryQuorumIncludesTheOtherReplicas() throws IOException
    {
        DseNodeFixture node = nodeWithEmptyPaxosSnapshot();
        TableMetadata paxos = paxosMetadata(true);
        writeSSTable(paxosDirectory(node), paxos, BtiFormat.getInstance(), List.of(emptyProposal(paxos, RF2, intKey(1)), emptyProposal(paxos, LOCAL, intKey(1))));
        Run run = assertExit(run(node), DsePreflight.EXIT_READY);
        assertThat(run.report.result).isEqualTo(PaxosRollbackReport.NO_READ_NEEDED);
        assertThat(run.report.partitions).hasSize(2).allMatch(p -> only(p).action == Action.SKIPPED);
        assertThat(run.out).contains("Result: NO_READ_NEEDED (2 partitions with an open round; reads listed: 0 at SERIAL, 0 at LOCAL_SERIAL; 2 need no read");
    }

    @Test
    public void nothingToReadWithoutAnOpenRound() throws IOException
    {
        DseNodeFixture node = nodeWithEmptyPaxosSnapshot();
        Run run = assertExit(run(node, "--not-restored"), DsePreflight.EXIT_READY);
        assertThat(run.report.result).isEqualTo(PaxosRollbackReport.NOTHING_TO_READ);
        assertThat(run.report.partitions).isEmpty();
        assertThat(run.report.cqlFile).isNull();
        assertThat(run.report.cqlLocalSerialFile).isNull();
        assertThat(run.out).contains("-- No read is needed at SERIAL.").contains("-- No read is needed at LOCAL_SERIAL.");
        // an empty snapshot of system.paxos is DSE without Paxos state when it was taken: listed, not a problem
        assertThat(run.out).contains("system.paxos directories read:\n" +
                                     "  data directory: " + paxosDirectory(node) + " (0 sstables, 0 rows)\n" +
                                     "  snapshot " + TAG + ": " + paxosSnapshot(node) + " (0 sstables, 0 rows)\n" +
                                     "system.paxos sstables read, each on its own (0 rows):\n  none\n");
    }

    @Test
    public void reportsTheRequiredInputsThatAreMissing() throws IOException
    {
        // the snapshot of the schema but none of system.paxos
        DseNodeFixture node = node();
        Files.createDirectories(paxosDirectory(node));
        Run noSnapshot = assertExit(run(node), DsePreflight.EXIT_BLOCKED);
        assertThat(noSnapshot.report.problems).containsExactly("the snapshot " + TAG + " has no directory of system.paxos (system/paxos-<id>/snapshots/" + TAG +
                                                               "): its Paxos state was not read; check that the snapshot is complete before the restore");

        // no system.paxos at all in the data directories
        DseNodeFixture empty = node();
        Run noPaxos = assertExit(run(empty), DsePreflight.EXIT_BLOCKED);
        assertThat(noPaxos.report.problems).hasSize(1);
        assertThat(noPaxos.report.problems.get(0)).startsWith("no directory of system.paxos (system/paxos-<id>) in the data directories")
                                                  .endsWith(": nothing was read; check the data directories (--data-dir)");
        assertThat(noPaxos.out).contains("system.paxos directories read:\n  none\n");
    }

    @Test
    public void refusesWhatIsNotARollbackOfThisNode() throws IOException
    {
        DseNodeFixture node = node();
        // no pre-hcd snapshot at all
        Path moved = temporaryFolder.newFolder("moved").toPath();
        try (Stream<Path> tables = Files.list(node.data.resolve("system_schema")))
        {
            for (Path table : (Iterable<Path>) tables::iterator)
                Files.move(table.resolve("snapshots"), moved.resolve(table.getFileName()));
        }
        Run none = assertExit(run(node), DsePreflight.EXIT_ERROR);
        assertThat(none.err).contains("no pre-hcd-<timestamp> snapshot of the system or system_schema tables in the data directories")
                            .contains("pass --snapshot <tag>, the snapshot taken before the upgrade, or --data-dir if the data directories " +
                                      "are not those of DSE's cassandra.yaml");

        // two of them
        Files.createDirectories(paxosSnapshot(node));
        Files.createDirectories(paxosDirectory(node).resolve("snapshots").resolve("pre-hcd-20261004T000000Z"));
        Run two = assertExit(run(node), DsePreflight.EXIT_ERROR);
        assertThat(two.err).contains("several pre-hcd-<timestamp> snapshots in the data directories: " + TAG + ", pre-hcd-20261004T000000Z");
        Run chosen = assertExit(run(node, "--snapshot", TAG, "--not-restored"), DsePreflight.EXIT_READY);
        assertThat(chosen.report.schemas).containsExactly("data directories");
        for (String path : List.of("..", ".", "../" + TAG, ""))
            assertThat(assertExit(run(node, "--snapshot", path), DsePreflight.EXIT_ERROR).err)
                .as(path).contains("--snapshot takes the tag of a snapshot, not a path: " + path);
        assertThat(assertExit(run(node, "--snapshot", "pre-hcd-none"), DsePreflight.EXIT_ERROR).err)
            .contains("no snapshot pre-hcd-none of the system or system_schema tables");

        // options of the mode without it, and the reverse
        assertThat(assertExit(run(node.args("--cql", "x.cql")), DsePreflight.EXIT_ERROR).err).contains("--cql applies only to --paxos-rollback");
        assertThat(assertExit(run(node.args("--not-restored")), DsePreflight.EXIT_ERROR).err).contains("--not-restored applies only to --paxos-rollback");
        assertThat(assertExit(run(node, "--aggregate", node.root.toString()), DsePreflight.EXIT_ERROR).err).contains("--aggregate does not apply to --paxos-rollback");
        assertThat(assertExit(run(node, "--serial-consistency", "rf3"), DsePreflight.EXIT_ERROR).err)
            .contains("--serial-consistency needs <keyspace>=SERIAL or <keyspace>=LOCAL_SERIAL: rf3");
        assertThat(assertExit(run(node, "--serial-consistency", "rf3=QUORUM"), DsePreflight.EXIT_ERROR).err)
            .contains("--serial-consistency needs <keyspace>=SERIAL or <keyspace>=LOCAL_SERIAL: rf3=QUORUM");
        assertThat(assertExit(run(node, "--serial-consistency", "=SERIAL"), DsePreflight.EXIT_ERROR).err)
            .contains("--serial-consistency needs <keyspace>=SERIAL or <keyspace>=LOCAL_SERIAL: =SERIAL");
        assertThat(assertExit(run(node, "--serial-consistency", " =SERIAL"), DsePreflight.EXIT_ERROR).err)
            .contains("--serial-consistency needs <keyspace>=SERIAL or <keyspace>=LOCAL_SERIAL:  =SERIAL");
        assertThat(assertExit(run(node, "--commitlog-dir", node.root.toString()), DsePreflight.EXIT_ERROR).err)
            .contains("--commitlog-dir does not apply to --paxos-rollback");
        assertThat(assertExit(run(node, "--hints-dir", node.root.toString()), DsePreflight.EXIT_ERROR).err)
            .contains("--hints-dir does not apply to --paxos-rollback");
        assertThat(assertExit(run(node, "--serial-consistency", "rf3=SERIAL", "--serial-consistency", "rf3=LOCAL_SERIAL"), DsePreflight.EXIT_ERROR).err)
            .contains("--serial-consistency names keyspace rf3 more than once");
        assertThat(assertExit(run(node, "--paxos-dir", node.root.resolve("absent").toString()), DsePreflight.EXIT_ERROR).err)
            .contains("--paxos-dir needs a directory of system.paxos sstables");
    }

    @Test
    public void theAggregateModeSkipsPaxosRollbackReports() throws IOException
    {
        DseNodeFixture node = nodeWithEmptyPaxosSnapshot();
        Path reports = temporaryFolder.newFolder("reports").toPath();
        assertExit(run(node, reports.resolve("paxos.json"), "--not-restored"), DsePreflight.EXIT_READY);
        Path aggregateJson = temporaryFolder.getRoot().toPath().resolve("aggregate.json");
        Run aggregate = run(new String[]{ "--aggregate", reports.toString(), "--json", aggregateJson.toString() });
        assertThat(aggregate.out).contains("[PAXOS_ROLLBACK_REPORT_SKIPPED] Paxos rollback reports (--paxos-rollback) in the directory, skipped: paxos.json")
                                 .doesNotContain("UNREADABLE_REPORT");
        // an INFO finding, and the report is not counted as a node's: no node report is left
        ClusterAggregate.AggregateReport result = JsonUtils.JSON_OBJECT_MAPPER.readerFor(ClusterAggregate.AggregateReport.class).readValue(aggregateJson.toFile());
        assertThat(result.findings).filteredOn(f -> f.code.equals("PAXOS_ROLLBACK_REPORT_SKIPPED"))
                                   .singleElement().satisfies(f -> assertThat(f.severity).isEqualTo(Finding.Severity.INFO));
        assertThat(result.nodes).isEmpty();
        assertThat(result.reportsRead).isZero();
        assertThat(aggregate.exitCode).isEqualTo(DsePreflight.EXIT_BLOCKED);
    }
}
