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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.zip.CRC32;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.msgpack.jackson.dataformat.MessagePackFactory;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.dsecompat.DseFixtures;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableTxnWriter;
import org.apache.cassandra.io.sstable.SequenceBasedSSTableId;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.bti.BtiFormat;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.FBUtilities;

/**
 * A DSE 6.8.65 node laid out in a temporary directory from the fixtures of {@code test/data/dse-compat}: the
 * configuration of {@link DseFixtures#confDirectory()} (its {@code /var/lib/cassandra} directories moved under the
 * temporary directory), the DSE installation's {@code ds_version.txt}, a data directory with the {@code system_schema}
 * and {@code system_auth.role_permissions} sstables of {@link DseFixtures#schemaDataDirectory()}, a metadata directory
 * with the {@code nodes} files given, and empty hints and commit log directories. The copy is modified by the tests to
 * build a node with one forbidden feature: configuration edits, {@code nodes/local} and {@code nodes/peers} edits (the
 * msgpack maps DSE wrote, decoded and encoded again), and schema rows written as additional {@code bb} sstables next to
 * DSE's, read and merged with them as the tool reads DSE's own (so a row written later wins, and a deletion shadows).
 */
final class DseNodeFixture
{
    static final String DSE_STORAGE = "/var/lib/cassandra";
    /** The generation of the first sstable a test writes: above every generation DSE wrote in the fixture. */
    private static final int FIRST_GENERATION = 900000;

    final Path root;
    final Path conf;
    final Path dseHome;
    final Path data;
    final Path metadata;
    final Path hints;
    final Path commitLog;
    private int generation = FIRST_GENERATION;

    private DseNodeFixture(Path root)
    {
        this.root = root;
        this.conf = root.resolve("conf");
        this.dseHome = root.resolve("dse");
        Path storage = root.resolve("var");
        this.data = storage.resolve("data");
        this.metadata = storage.resolve("metadata");
        this.hints = storage.resolve("hints");
        this.commitLog = storage.resolve("commitlog");
    }

    /** The DSE 6.8.65 node of the fixtures in {@code root}, with the {@code local} and {@code peers} files of {@code nodesSource}. */
    static DseNodeFixture create(Path root, Path nodesSource) throws IOException
    {
        DseNodeFixture node = new DseNodeFixture(root);
        Files.createDirectories(node.conf);
        Files.createDirectories(node.dseHome);
        Files.createDirectories(node.hints);
        Files.createDirectories(node.commitLog);
        Path source = DseFixtures.confDirectory();
        String yaml = new String(Files.readAllBytes(source.resolve("cassandra.yaml")), StandardCharsets.UTF_8);
        Files.write(node.conf.resolve("cassandra.yaml"), yaml.replace(DSE_STORAGE, root.resolve("var").toString()).getBytes(StandardCharsets.UTF_8));
        Files.copy(source.resolve("dse.yaml"), node.conf.resolve("dse.yaml"));
        Files.copy(source.resolve("cassandra-rackdc.properties"), node.conf.resolve("cassandra-rackdc.properties"));
        Files.copy(source.resolve("ds_version.txt"), node.dseHome.resolve("ds_version.txt"));
        copyTree(DseFixtures.schemaDataDirectory(), node.data);
        Path nodes = Files.createDirectories(node.metadata.resolve("nodes"));
        Files.copy(nodesSource.resolve("local"), nodes.resolve("local"));
        Files.copy(nodesSource.resolve("peers"), nodes.resolve("peers"));
        return node;
    }

    /**
     * The first node of the cluster the fixtures were taken from, 192.168.57.11 in rack {@code rack1}, as it was before
     * any node was upgraded: the nodes files of {@code nodes/dse-6.8.65} (both peers on DSE), with the configuration of
     * the second node moved to the first node's address and rack (the nodes of that cluster have the same configuration
     * but for these).
     */
    static DseNodeFixture create(Path root) throws IOException
    {
        Path nodes = Files.createDirectories(root.resolve("nodes-source"));
        Files.copy(DseFixtures.nodesLocal(DseFixtures.DseVersion.DSE_6_8_65), nodes.resolve("local"));
        Files.copy(DseFixtures.nodesPeers(DseFixtures.DseVersion.DSE_6_8_65), nodes.resolve("peers"));
        DseNodeFixture node = create(root.resolve("node"), nodes);
        replace(node.conf.resolve("cassandra.yaml"), "192.168.57.12", "192.168.57.11");
        replace(node.conf.resolve("cassandra-rackdc.properties"), "rack=rack2", "rack=rack1");
        return node;
    }

    private static void replace(Path file, String from, String to) throws IOException
    {
        String content = new String(Files.readAllBytes(file), StandardCharsets.UTF_8);
        if (!content.contains(from))
            throw new IllegalStateException(file + " does not contain " + from);
        Files.write(file, content.replace(from, to).getBytes(StandardCharsets.UTF_8));
    }

    /** The arguments pointing the tool at this node, followed by {@code extra}. */
    String[] args(String... extra)
    {
        List<String> args = new ArrayList<>(Arrays.asList("--conf-dir", conf.toString(),
                                                          "--dse-yaml", conf.resolve("dse.yaml").toString(),
                                                          "--dse-home", dseHome.toString()));
        args.addAll(Arrays.asList(extra));
        return args.toArray(new String[0]);
    }

    // ---- configuration ----

    /** Edits {@code cassandra.yaml} or {@code dse.yaml} as a YAML map. */
    void editYaml(String file, Consumer<Map<String, Object>> edit) throws IOException
    {
        Path path = conf.resolve(file);
        Map<String, Object> yaml;
        try (InputStream in = Files.newInputStream(path))
        {
            yaml = new Yaml(new SafeConstructor(new LoaderOptions())).load(in);
        }
        edit.accept(yaml);
        DumperOptions options = new DumperOptions();
        options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
        try (Writer out = Files.newBufferedWriter(path, StandardCharsets.UTF_8))
        {
            new Yaml(options).dump(yaml, out);
        }
    }

    /** A YAML sub-map of {@code yaml}, created when absent. */
    @SuppressWarnings("unchecked")
    static Map<String, Object> section(Map<String, Object> yaml, String key)
    {
        return (Map<String, Object>) yaml.computeIfAbsent(key, k -> new LinkedHashMap<>());
    }

    // ---- nodes/local and nodes/peers ----

    void editLocal(Consumer<Map<String, Object>> edit) throws IOException
    {
        Path local = metadata.resolve("nodes").resolve("local");
        Map<String, Object> map = msgpack().readValue(Files.readAllBytes(local), new TypeReference<LinkedHashMap<String, Object>>() {});
        edit.accept(map);
        writeMsgpack(local, map);
    }

    void editPeers(Consumer<List<Map<String, Object>>> edit) throws IOException
    {
        Path peers = metadata.resolve("nodes").resolve("peers");
        List<Map<String, Object>> list = msgpack().readValue(Files.readAllBytes(peers), new TypeReference<List<Map<String, Object>>>() {});
        edit.accept(list);
        writeMsgpack(peers, list);
    }

    private static ObjectMapper msgpack()
    {
        MessagePackFactory factory = new MessagePackFactory();
        factory.disable(JsonGenerator.Feature.AUTO_CLOSE_TARGET);
        return new ObjectMapper(factory); // checkstyle: permit this instantiation
    }

    private static void writeMsgpack(Path path, Object value) throws IOException
    {
        try (OutputStream out = Files.newOutputStream(path))
        {
            msgpack().writeValue(out, value);
        }
    }

    // ---- schema rows ----

    /** One row to write: the partition key values, the clustering values and the other columns by name. */
    static final class SchemaRow
    {
        final Object[] partitionKey;
        final Object[] clustering;
        final Map<String, Object> columns = new LinkedHashMap<>();

        SchemaRow(Object[] partitionKey, Object[] clustering)
        {
            this.partitionKey = partitionKey;
            this.clustering = clustering;
        }

        SchemaRow with(String column, Object value)
        {
            columns.put(column, value);
            return this;
        }
    }

    static SchemaRow row(Object partitionKey, Object... clustering)
    {
        return new SchemaRow(new Object[]{ partitionKey }, clustering);
    }

    /**
     * Writes {@code rows} to {@code keyspace.table} as one more sstable of its directory, with the columns the
     * serialization headers of DSE's sstables of the table describe (the primary key columns named as given).
     */
    void writeRows(String keyspace, String table, List<String> partitionKey, List<String> clustering, SchemaRow... rows) throws IOException
    {
        writeRows(BtiFormat.getInstance(), keyspace, table, partitionKey, clustering, rows);
    }

    /** {@link #writeRows(String, String, List, List, SchemaRow...)} in the sstable format given. */
    void writeRows(SSTableFormat<?, ?> format, String keyspace, String table, List<String> partitionKey, List<String> clustering, SchemaRow... rows) throws IOException
    {
        TableMetadata metadata = DseTableReader.metadataFromSSTables(List.of(new File(data)), keyspace, table, partitionKey, clustering);
        long timestamp = FBUtilities.timestampMicros();
        Map<String, PartitionUpdate.SimpleBuilder> partitions = new TreeMap<>();
        for (SchemaRow row : rows)
        {
            PartitionUpdate.SimpleBuilder partition = partitions.computeIfAbsent(Arrays.toString(row.partitionKey),
                                                                                 k -> PartitionUpdate.simpleBuilder(metadata, row.partitionKey).timestamp(timestamp));
            Row.SimpleBuilder builder = partition.row(row.clustering);
            row.columns.forEach(builder::add);
        }
        List<PartitionUpdate> updates = new ArrayList<>();
        partitions.values().forEach(p -> updates.add(p.build()));
        write(format, keyspace, table, metadata, updates);
    }

    /** Deletes the partitions of {@code keyspace.table} with the keys given, in one more sstable. */
    void deletePartitions(String keyspace, String table, List<String> partitionKey, List<String> clustering, Object... keys) throws IOException
    {
        TableMetadata metadata = DseTableReader.metadataFromSSTables(List.of(new File(data)), keyspace, table, partitionKey, clustering);
        long timestamp = FBUtilities.timestampMicros();
        List<PartitionUpdate> updates = new ArrayList<>();
        for (Object key : keys)
            updates.add(PartitionUpdate.simpleBuilder(metadata, key).timestamp(timestamp).delete().build());
        write(BtiFormat.getInstance(), keyspace, table, metadata, updates);
    }

    /**
     * Starts writing {@code row} to {@code keyspace.table} as one more sstable, as a flush of DSE in progress: the writer
     * has appended it but is neither finished nor aborted, so the transaction log lists the sstable as new and its
     * {@code TOC.txt} is not written yet. The caller aborts it.
     */
    SSTableTxnWriter startWriting(String keyspace, String table, List<String> partitionKey, List<String> clustering, SchemaRow row) throws IOException
    {
        TableMetadata metadata = DseTableReader.metadataFromSSTables(List.of(new File(data)), keyspace, table, partitionKey, clustering);
        PartitionUpdate.SimpleBuilder partition = PartitionUpdate.simpleBuilder(metadata, row.partitionKey).timestamp(FBUtilities.timestampMicros());
        Row.SimpleBuilder builder = partition.row(row.clustering);
        row.columns.forEach(builder::add);
        SSTableTxnWriter writer = writer(BtiFormat.getInstance(), keyspace, table, metadata, 1);
        writer.append(partition.build().unfilteredIterator());
        return writer;
    }

    private SSTableTxnWriter writer(SSTableFormat<?, ?> format, String keyspace, String table, TableMetadata metadata, int keys) throws IOException
    {
        Path directory;
        try (Stream<Path> tables = Files.list(data.resolve(keyspace)))
        {
            directory = tables.filter(p -> DseTableReader.isDirectoryOf(p.getFileName().toString(), table)).findFirst()
                              .orElseThrow(() -> new IllegalStateException("no directory of " + keyspace + '.' + table));
        }
        Descriptor descriptor = new Descriptor(new File(directory), keyspace, table, new SequenceBasedSSTableId(generation++), format);
        SerializationHeader header = new SerializationHeader(true, metadata, metadata.regularAndStaticColumns(), EncodingStats.NO_STATS);
        return SSTableTxnWriter.create(TableMetadataRef.forOfflineTools(metadata), descriptor, keys, 0, null, false, header, Collections.emptyList(), null);
    }

    private void write(SSTableFormat<?, ?> format, String keyspace, String table, TableMetadata metadata, List<PartitionUpdate> updates) throws IOException
    {
        updates.sort((a, b) -> a.partitionKey().compareTo(b.partitionKey()));
        try (SSTableTxnWriter writer = writer(format, keyspace, table, metadata, updates.size()))
        {
            for (PartitionUpdate update : updates)
                writer.append(update.unfilteredIterator());
            writer.finish(false, null);
        }
    }

    // ---- read-only check ----

    /** Every file under the node, with its size, modification time and CRC32: equal before and after a read-only run. */
    Map<String, String> fingerprint() throws IOException
    {
        Map<String, String> files = new TreeMap<>();
        try (Stream<Path> paths = Files.walk(root))
        {
            for (Path path : (Iterable<Path>) paths::iterator)
            {
                if (Files.isDirectory(path))
                {
                    files.put(root.relativize(path) + "/", "directory " + Files.getLastModifiedTime(path));
                    continue;
                }
                CRC32 crc = new CRC32();
                crc.update(Files.readAllBytes(path));
                files.put(root.relativize(path).toString(), Files.size(path) + " " + Files.getLastModifiedTime(path) + ' ' + crc.getValue());
            }
        }
        return files;
    }

    private static void copyTree(Path source, Path target) throws IOException
    {
        try (Stream<Path> paths = Files.walk(source))
        {
            for (Path path : (Iterable<Path>) paths::iterator)
            {
                Path destination = target.resolve(source.relativize(path).toString());
                if (Files.isDirectory(path))
                    Files.createDirectories(destination);
                else
                    Files.copy(path, destination, StandardCopyOption.REPLACE_EXISTING);
            }
        }
    }
}
