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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Stream;

import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.dsecompat.Dse51Fixtures;
import org.apache.cassandra.dsecompat.DseFixtures;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableTxnWriter;
import org.apache.cassandra.io.sstable.SequenceBasedSSTableId;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Node 1 of the three-node DSE 5.1.51 cluster of the fixtures ({@link Dse51Fixtures}), laid out in a temporary
 * directory: its configuration as it ran ({@code conf/dse-5.1.51/running/}, the cut paths of its directories put back
 * under the temporary directory) or as DSE installs it ({@code installed/}, its {@code /var/lib/cassandra} directories
 * moved there); a data directory with its {@code system.local} and {@code system.peers} sstables (compacted or
 * flushed), its {@code system_schema} and {@code system_auth.role_permissions} sstables; its hints file and its commit
 * log segment; an empty metadata directory. Not captured, generated here: the {@code ds_version.txt} of the
 * installation ({@code DSVersion=5.1.51}), and the schema rows a test adds or deletes ({@link #writeRows},
 * {@link #deleteRows}), written as big {@code me} sstables next to DSE's by this software in the DSE 5.1
 * compatibility mode of a client.
 */
final class Dse51NodeFixture
{
    /** The placeholder the capture cut the node's directories to in {@code running/cassandra.yaml}. */
    static final String NODE_DIRECTORY = "<node directory>";
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

    private Dse51NodeFixture(Path root)
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

    /** Node 1 with its configuration as it ran, and its compacted {@code system.local} and {@code system.peers}. */
    static Dse51NodeFixture create(Path root) throws IOException
    {
        return create(root, true, DseFixtures.dse51().systemSSTables(true));
    }

    /**
     * Node 1 with its configuration as it ran ({@code running} true) or as installed, and the {@code system.local} and
     * {@code system.peers} table directories of {@code systemTables}.
     */
    static Dse51NodeFixture create(Path root, boolean running, Path systemTables) throws IOException
    {
        Dse51Fixtures fixtures = DseFixtures.dse51();
        Dse51NodeFixture node = new Dse51NodeFixture(root);
        Files.createDirectories(node.conf);
        Files.createDirectories(node.dseHome);
        Files.createDirectories(node.hints);
        Files.createDirectories(node.metadata);
        Path source = fixtures.confDirectory(running);
        String yaml = new String(Files.readAllBytes(source.resolve(NodeLayout.CASSANDRA_YAML)), StandardCharsets.UTF_8);
        String storage = root.resolve("var").toString();
        yaml = running ? yaml.replace(NODE_DIRECTORY, storage) : yaml.replace(DseNodeFixture.DSE_STORAGE, storage);
        Files.write(node.conf.resolve(NodeLayout.CASSANDRA_YAML), yaml.getBytes(StandardCharsets.UTF_8));
        for (String file : List.of(NodeLayout.DSE_YAML, NodeLayout.RACKDC, NodeLayout.JVM_OPTIONS))
            Files.copy(source.resolve(file), node.conf.resolve(file));
        byte[] dsVersion = ("DSVersion=" + Dse51Fixtures.DSE_VERSION + '\n').getBytes(StandardCharsets.UTF_8);
        Files.write(node.dseHome.resolve("ds_version.txt"), dsVersion);
        copyTree(systemTables, node.data.resolve("system"));
        copyTree(fixtures.schemaDataDirectory(), node.data);
        Files.copy(fixtures.hintsFile(), node.hints.resolve(fixtures.hintsFile().getFileName()));
        fixtures.unpackCommitLogSegments(fixtures.commitLogSegmentGz().getParent(), node.commitLog);
        return node;
    }

    /** The arguments pointing the tool at this node, followed by {@code extra}. */
    String[] args(String... extra)
    {
        List<String> args = new ArrayList<>(Arrays.asList("--conf-dir", conf.toString(),
                                                          "--dse-yaml", conf.resolve(NodeLayout.DSE_YAML).toString(),
                                                          "--dse-home", dseHome.toString(),
                                                          "--metadata-dir", metadata.toString()));
        args.addAll(Arrays.asList(extra));
        return args.toArray(new String[0]);
    }

    /**
     * The arguments of the rollback check of this node, followed by {@code extra}: those of {@link #args} but
     * {@code --dse-yaml}, which the check does not read and so refuses.
     */
    String[] rollbackCheckArgs(String... extra)
    {
        List<String> args = new ArrayList<>(Arrays.asList(args("--rollback-check")));
        int dseYaml = args.indexOf("--dse-yaml");
        if (dseYaml < 0)
            throw new IllegalStateException("no --dse-yaml in " + args);
        args.remove(dseYaml + 1);
        args.remove(dseYaml);
        args.addAll(Arrays.asList(extra));
        return args.toArray(new String[0]);
    }

    /** Replaces {@code from} with {@code to} in a file of the configuration directory, which must hold it. */
    void replace(String file, String from, String to) throws IOException
    {
        Path path = conf.resolve(file);
        String content = new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
        if (!content.contains(from))
            throw new IllegalStateException(path + " does not contain " + from);
        Files.write(path, content.replace(from, to).getBytes(StandardCharsets.UTF_8));
    }

    /** Appends {@code lines} to a file of the configuration directory. */
    void append(String file, String... lines) throws IOException
    {
        Path path = conf.resolve(file);
        String content = new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
        Files.write(path, (content + String.join("\n", lines) + '\n').getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Writes {@code rows} to {@code keyspace.table} as one more big sstable of its directory, of the version the
     * format writes (big {@code me} in a client in the DSE 5.1 compatibility mode), with the columns the serialization
     * headers of DSE's sstables of the table describe.
     */
    void writeRows(String keyspace, String table, List<String> partitionKey, List<String> clustering,
                   DseNodeFixture.SchemaRow... rows) throws IOException
    {
        try (SSTableTxnWriter writer = writer(keyspace, table, partitionKey, clustering, false, rows))
        {
            writer.finish(false, null);
        }
    }

    /**
     * Writes the deletion of each of {@code rows} (their primary keys; their columns are not read) to
     * {@code keyspace.table} as one more big sstable, as {@link #writeRows} writes rows: the rows DSE wrote are
     * shadowed, as after a {@code DROP} or an {@code ALTER} on DSE.
     */
    void deleteRows(String keyspace, String table, List<String> partitionKey, List<String> clustering,
                    DseNodeFixture.SchemaRow... rows) throws IOException
    {
        try (SSTableTxnWriter writer = writer(keyspace, table, partitionKey, clustering, true, rows))
        {
            writer.finish(false, null);
        }
    }

    /**
     * Starts writing {@code rows} to {@code keyspace.table} as one more big sstable, as a flush of DSE 5.1 in progress:
     * the writer has appended them but is neither finished nor aborted, so the transaction log lists the sstable as
     * new. The caller closes it, which aborts the transaction.
     */
    SSTableTxnWriter startWriting(String keyspace, String table, List<String> partitionKey, List<String> clustering,
                                  DseNodeFixture.SchemaRow... rows) throws IOException
    {
        return writer(keyspace, table, partitionKey, clustering, false, rows);
    }

    private SSTableTxnWriter writer(String keyspace, String table, List<String> partitionKey, List<String> clustering,
                                    boolean deletions, DseNodeFixture.SchemaRow... rows) throws IOException
    {
        TableMetadata metadata = DseTableReader.metadataFromSSTables(List.of(new File(data)), keyspace, table,
                                                                     partitionKey, clustering);
        Path directory;
        try (Stream<Path> tables = Files.list(data.resolve(keyspace)))
        {
            directory = tables.filter(p -> DseTableReader.isDirectoryOf(p.getFileName().toString(), table))
                              .findFirst()
                              .orElseThrow(() -> new IllegalStateException("no directory of " + keyspace + '.' +
                                                                           table));
        }
        long timestamp = newerThanEveryCell(directory);
        Map<String, PartitionUpdate.SimpleBuilder> partitions = new TreeMap<>();
        for (DseNodeFixture.SchemaRow row : rows)
        {
            PartitionUpdate.SimpleBuilder partition =
                partitions.computeIfAbsent(Arrays.toString(row.partitionKey),
                                           k -> PartitionUpdate.simpleBuilder(metadata, row.partitionKey)
                                                               .timestamp(timestamp));
            Row.SimpleBuilder builder = partition.row(row.clustering);
            if (deletions)
                builder.delete();
            else
                row.columns.forEach(builder::add);
        }
        List<PartitionUpdate> updates = new ArrayList<>();
        partitions.values().forEach(p -> updates.add(p.build()));
        updates.sort((a, b) -> a.partitionKey().compareTo(b.partitionKey()));
        Descriptor descriptor = new Descriptor(new File(directory), keyspace, table,
                                               new SequenceBasedSSTableId(generation++), BigFormat.getInstance());
        SerializationHeader header = new SerializationHeader(true, metadata, metadata.regularAndStaticColumns(),
                                                             EncodingStats.NO_STATS);
        SSTableTxnWriter writer = SSTableTxnWriter.create(TableMetadataRef.forOfflineTools(metadata), descriptor,
                                                          updates.size(), 0, null, false, header,
                                                          Collections.emptyList(), null);
        try
        {
            for (PartitionUpdate update : updates)
                writer.append(update.unfilteredIterator());
            return writer;
        }
        catch (RuntimeException e)
        {
            writer.abort();
            throw e;
        }
    }

    /**
     * A write timestamp above that of every cell of the sstables of a table directory (the maximum timestamp of their
     * statistics, plus one), so that what a test writes or deletes shadows what DSE wrote whatever the clock of the
     * machine; this machine's clock for a directory without an sstable.
     */
    private static long newerThanEveryCell(Path directory) throws IOException
    {
        long newest = Long.MIN_VALUE;
        try (Stream<Path> files = Files.list(directory))
        {
            for (Path file : (Iterable<Path>) files::iterator)
            {
                if (!file.getFileName().toString().endsWith("-Data.db"))
                    continue;
                Descriptor descriptor = Descriptor.fromFile(new File(file));
                StatsMetadata stats = (StatsMetadata) descriptor.getMetadataSerializer()
                                                                .deserialize(descriptor, MetadataType.STATS);
                newest = Math.max(newest, stats.maxTimestamp);
            }
        }
        return newest == Long.MIN_VALUE ? FBUtilities.timestampMicros() : newest + 1;
    }

    /** Every file under the node, with its size, modification time and CRC32: unchanged by a read-only run. */
    Map<String, String> fingerprint() throws IOException
    {
        return DseNodeFixture.fingerprint(root);
    }

    static void copyTree(Path source, Path target) throws IOException
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
