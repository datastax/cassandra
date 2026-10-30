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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableFormat.Components;
import org.apache.cassandra.io.sstable.format.StatsComponent;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.FBUtilities;

/**
 * The tables DSE left in a node's data directories, offline: whether a directory is one of a table's, and the table as
 * the sstables of committed transactions ({@link LifecycleTransaction#getFiles}) describe it in their serialization
 * headers (the union of their columns, with the types DSE wrote, so that a column DSE has and this software does not is
 * described too), the primary key columns named by the caller. Only the statistics component of each sstable is read.
 */
final class DseTableReader
{
    /** The table id at the end of the name of a table directory. */
    private static final Pattern TABLE_ID = Pattern.compile("[0-9a-f]{32}");

    private DseTableReader()
    {
    }

    /** The descriptors of the sstables of committed transactions in one table directory. */
    private static List<Descriptor> sstablesIn(File directory)
    {
        List<Descriptor> descriptors = new ArrayList<>();
        // a transaction log that cannot be read fails, as the startup of HCD 2.0 would on the same log (the scan of the data
        // directories, which is advisory, falls back instead)
        List<File> dataFiles = LifecycleTransaction.getFiles(directory.toPath(),
                                                             (file, type) -> type == Directories.FileType.FINAL && file.name().endsWith("-" + Components.DATA.name),
                                                             Directories.OnTxnErr.THROW);
        for (File dataFile : dataFiles)
            descriptors.add(Descriptor.fromFileWithComponent(dataFile, false).left);
        return descriptors;
    }

    /**
     * The table as the serialization headers of its sstables in the data directories describe it.
     *
     * @throws IllegalStateException when the data directories hold no sstable of the table
     */
    @VisibleForTesting
    static TableMetadata metadataFromSSTables(List<File> dataDirectories, String keyspace, String table, List<String> partitionKey, List<String> clustering) throws IOException
    {
        List<Descriptor> descriptors = new ArrayList<>();
        descriptors(dataDirectories, keyspace, table, descriptors);
        if (descriptors.isEmpty())
            throw new IllegalStateException("no sstable of " + keyspace + '.' + table + " in " + dataDirectories);
        return metadataFromHeaders(keyspace, table, descriptors, partitionKey, clustering);
    }

    /**
     * Adds the descriptors of the sstables of committed transactions of {@code keyspace.table} in the data directories to
     * {@code descriptors}.
     */
    private static void descriptors(List<File> dataDirectories, String keyspace, String table, List<Descriptor> descriptors)
    {
        for (File dataDirectory : dataDirectories)
        {
            File keyspaceDirectory = new File(dataDirectory, keyspace);
            if (!keyspaceDirectory.isDirectory())
                continue;
            for (File tableDirectory : keyspaceDirectory.listUnchecked(f -> f.isDirectory() && (isDirectoryOf(f.name(), table) || f.name().equals(table))))
                descriptors.addAll(sstablesIn(tableDirectory));
        }
    }

    /**
     * Whether a directory named {@code name} is one of the directories of {@code table} named with its id,
     * {@code <table>-<table id>}; a table created before Cassandra 2.1 has a directory named {@code <table>}.
     */
    static boolean isDirectoryOf(String name, String table)
    {
        return name.length() == table.length() + 33 && name.startsWith(table + '-') && TABLE_ID.matcher(name.substring(table.length() + 1)).matches();
    }

    /**
     * The table as the serialization headers of its sstables describe it: the key and clustering types of the first,
     * and the static and regular columns of all of them (the first type met for a name), with the partitioner the
     * first was written with.
     */
    private static TableMetadata metadataFromHeaders(String keyspace, String table, List<Descriptor> descriptors, List<String> partitionKey, List<String> clustering) throws IOException
    {
        Map<ByteBuffer, AbstractType<?>> statics = new LinkedHashMap<>();
        Map<ByteBuffer, AbstractType<?>> regulars = new LinkedHashMap<>();
        SerializationHeader.Component first = null;
        IPartitioner partitioner = null;
        for (Descriptor descriptor : descriptors)
        {
            StatsComponent stats = StatsComponent.load(descriptor, MetadataType.VALIDATION, MetadataType.HEADER);
            SerializationHeader.Component header = stats.serializationHeader();
            if (first == null)
            {
                first = header;
                partitioner = FBUtilities.newPartitioner(descriptor);
            }
            header.getStaticColumns().forEach(statics::putIfAbsent);
            header.getRegularColumns().forEach(regulars::putIfAbsent);
        }

        TableMetadata.Builder builder = TableMetadata.builder(keyspace, table).partitioner(partitioner);
        AbstractType<?> keyType = first.getKeyType();
        if (partitionKey.size() > 1 && keyType instanceof CompositeType)
        {
            List<AbstractType<?>> components = keyType.subTypes();
            for (int i = 0; i < components.size(); i++)
                builder.addPartitionKeyColumn(i < partitionKey.size() ? partitionKey.get(i) : "key" + i, components.get(i));
        }
        else
        {
            builder.addPartitionKeyColumn(partitionKey.get(0), keyType);
        }
        List<AbstractType<?>> clusteringTypes = first.getClusteringTypes();
        for (int i = 0; i < clusteringTypes.size(); i++)
            builder.addClusteringColumn(i < clustering.size() ? clustering.get(i) : "clustering" + i, clusteringTypes.get(i));
        statics.forEach((name, type) -> builder.addStaticColumn(ColumnIdentifier.getInterned(UTF8Type.instance.getString(name), true), type));
        regulars.forEach((name, type) -> builder.addRegularColumn(ColumnIdentifier.getInterned(UTF8Type.instance.getString(name), true), type));
        return builder.build();
    }
}
