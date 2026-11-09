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

import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.cassandra.db.Dse51SystemTables;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dsecompat.DseFixtures;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.PathUtils;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The rows of {@code system_schema} DSE 5.1.51 wrote, from the sstables of the DSE 5.1.51 schema fixture
 * ({@code DseFixtures.dse51().schemaDataDirectory()}: node 1 after the restrictions and the failed
 * {@code UNRESTRICT ROWS} on a view of its README), read with this software's definitions of {@code system_schema},
 * for the tests that apply them to the schema tables of a test node.
 */
public final class Dse51SchemaRows
{
    /** The keyspace of the fixture's user tables, among them {@code rlac} and its view {@code rlac_by_v}. */
    public static final String KEYSPACE = "test_ks";

    private Dse51SchemaRows()
    {
    }

    /**
     * The partitions of the table {@code table} of {@code system_schema} the fixture holds, one list per sstable, in
     * the order of their generations: a later sstable may hold newer cells of the same partition. Their keys are
     * decorated with {@code Murmur3Partitioner}, DSE's.
     */
    public static List<List<PartitionUpdate>> partitions(String table) throws IOException
    {
        Path root = DseFixtures.dse51().schemaDataDirectory().resolve(SchemaConstants.SCHEMA_KEYSPACE_NAME);
        Path directory;
        try (Stream<Path> directories = Files.list(root))
        {
            List<Path> found = directories.filter(p -> p.getFileName().toString().matches(table + "-[0-9a-f]{32}"))
                                          .collect(Collectors.toList());
            assertThat(found).as("table directories of %s in %s", table, root).hasSize(1);
            directory = found.get(0);
        }
        // this software's definition, with the partitioner DSE 5.1.51 wrote the keys with, whatever the test node's
        TableMetadata metadata = Keyspace.open(SchemaConstants.SCHEMA_KEYSPACE_NAME)
                                         .getColumnFamilyStore(table)
                                         .metadata()
                                         .unbuild()
                                         .partitioner(Murmur3Partitioner.instance)
                                         .build();
        // opened from a copy, so that nothing is ever written next to the fixture
        Path temp = Files.createTempDirectory("dse51-schema");
        PathUtils.deleteRecursiveOnExit(temp);
        Path copy = Files.createDirectories(temp.resolve(SchemaConstants.SCHEMA_KEYSPACE_NAME)
                                                           .resolve(directory.getFileName()));
        List<Path> dataFiles = new ArrayList<>();
        try (Stream<Path> files = Files.list(directory))
        {
            for (Path file : files.collect(Collectors.toList()))
            {
                Path copied = Files.copy(file, copy.resolve(file.getFileName()));
                if (copied.getFileName().toString().endsWith("-Data.db"))
                    dataFiles.add(copied);
            }
        }
        dataFiles.sort(Comparator.comparingInt(file -> Integer.parseInt(file.getFileName().toString().split("-")[1])));
        List<List<PartitionUpdate>> partitions = new ArrayList<>();
        for (Path dataFile : dataFiles)
        {
            SSTableReader sstable = SSTableReader.openNoValidation(null, Descriptor.fromFile(new File(dataFile)),
                                                                   TableMetadataRef.forOfflineTools(metadata));
            try
            {
                partitions.add(Dse51SystemTables.partitions(sstable, metadata));
            }
            finally
            {
                sstable.selfRef().release();
            }
        }
        return partitions;
    }

    /**
     * Applies to the schema tables of this node the rows the fixture holds for {@code keyspace} in every table of
     * {@code system_schema} it has, as DSE 5.1.51 wrote them but for their timestamps: those of each sstable are made
     * one and the same, later than now and than those of the sstables before it, so that the rows are applied again
     * after a test deleted them, and the newer cells of a later sstable still win. The schema in memory is left as it
     * is. The test node must use {@code Murmur3Partitioner}, as {@code CQLTester} does.
     */
    public static void apply(String keyspace) throws IOException
    {
        Path root = DseFixtures.dse51().schemaDataDirectory().resolve(SchemaConstants.SCHEMA_KEYSPACE_NAME);
        List<String> tables;
        try (Stream<Path> directories = Files.list(root))
        {
            tables = directories.map(p -> p.getFileName().toString().replaceFirst("-[0-9a-f]{32}$", ""))
                                .sorted()
                                .collect(Collectors.toList());
        }
        long timestamp = FBUtilities.timestampMicros();
        for (String table : tables)
        {
            for (List<PartitionUpdate> sstable : partitions(table))
            {
                timestamp++;
                for (PartitionUpdate partition : sstable)
                    if (keyspaceOf(partition).equals(keyspace))
                        new Mutation(partition.withUpdatedTimestamps(timestamp)).applyUnsafe();
            }
        }
    }

    /** The name of the keyspace a partition of {@code system_schema} belongs to, its partition key. */
    public static String keyspaceOf(PartitionUpdate partition)
    {
        try
        {
            return ByteBufferUtil.string(partition.partitionKey().getKey());
        }
        catch (CharacterCodingException e)
        {
            throw new AssertionError(e);
        }
    }
}
