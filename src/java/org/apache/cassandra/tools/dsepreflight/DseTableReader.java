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
import java.util.Set;
import java.util.function.Consumer;
import java.util.regex.Pattern;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableFormat.Components;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.StatsComponent;
import org.apache.cassandra.io.sstable.format.TOCComponent;
import org.apache.cassandra.io.sstable.format.bti.BtiFormat;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Reads the rows of one table from the sstables DSE left in a node's data directories, offline and read-only: the
 * sstables of committed transactions only ({@link LifecycleTransaction#getFiles}), opened without validation, which
 * neither rebuilds nor saves any component. The table is described by the serialization headers of its sstables (the
 * union of their columns, with the types DSE wrote), so that a column DSE has and this software does not, such as the
 * {@code restricted} set of DSE's {@code system_auth.role_permissions}, is read too; the primary key columns are named
 * by the caller. The sstables are merged as a node merges them, so rows deleted later (a keyspace DSE dropped, for
 * instance) are not returned.
 */
final class DseTableReader
{
    /** The table id at the end of the name of a table directory. */
    private static final Pattern TABLE_ID = Pattern.compile("[0-9a-f]{32}");

    private DseTableReader()
    {
    }

    /** The rows of a table, and the sstables they were read from. */
    static final class TableRows
    {
        final List<UntypedResultSet.Row> rows;
        final List<String> sstables;
        /** whether a directory of the table exists in any data directory */
        final boolean found;

        TableRows(List<UntypedResultSet.Row> rows, List<String> sstables, boolean found)
        {
            this.rows = rows;
            this.sstables = sstables;
            this.found = found;
        }
    }

    /**
     * Reads every live row of {@code keyspace.table} from the data directories.
     *
     * @param partitionKey the names of the partition key columns
     * @param clustering the names of the clustering columns; an sstable with more clustering columns names the others
     *                   {@code clustering<n>}
     * @throws IOException when a directory or an sstable cannot be read, or an sstable is not in a trie format
     * @throws RuntimeException when an sstable cannot be decoded
     */
    static TableRows read(List<File> dataDirectories, String keyspace, String table, List<String> partitionKey, List<String> clustering) throws IOException
    {
        List<Descriptor> descriptors = new ArrayList<>();
        boolean found = descriptors(dataDirectories, keyspace, table, descriptors);
        List<String> names = new ArrayList<>(descriptors.size());
        for (Descriptor descriptor : descriptors)
            names.add(descriptor.baseFile().toString());
        if (descriptors.isEmpty())
            return new TableRows(List.of(), names, found);
        // only the trie formats DSE 6 writes are opened: opening an sstable of the big format loads its summary, which
        // deletes a Summary.db it cannot load and may rewrite TOC.txt (the scan of the data directories reports such
        // sstables, which DSE must rewrite with upgradesstables)
        for (Descriptor descriptor : descriptors)
            if (!(descriptor.version.format instanceof BtiFormat))
                throw new IOException(descriptor.baseFile() + " is in the " + descriptor.version.format.name() + " format, which this tool does not " +
                                      "read: run nodetool upgradesstables on DSE first");

        TableMetadata metadata = metadataFromHeaders(keyspace, table, descriptors, partitionKey, clustering);
        List<UntypedResultSet.Row> rows = new ArrayList<>();
        rows(descriptors, metadata, rows::add);
        return new TableRows(rows, names, found);
    }

    /** The descriptors of the sstables of committed transactions in one table directory. */
    private static List<Descriptor> sstablesIn(File directory)
    {
        List<Descriptor> descriptors = new ArrayList<>();
        // a transaction log that cannot be read fails the read: the schema must be exact, and the startup of HCD 2.0 would
        // fail on the same log (the scan of the data directories, which is advisory, falls back instead)
        List<File> dataFiles = LifecycleTransaction.getFiles(directory.toPath(),
                                                             (file, type) -> type == Directories.FileType.FINAL && file.name().endsWith("-" + Components.DATA.name),
                                                             Directories.OnTxnErr.THROW);
        for (File dataFile : dataFiles)
            descriptors.add(Descriptor.fromFileWithComponent(dataFile, false).left);
        return descriptors;
    }

    /** Passes the live rows of the sstables given, merged as a node merges them, to {@code consumer}. */
    private static void rows(List<Descriptor> descriptors, TableMetadata metadata, Consumer<UntypedResultSet.Row> consumer) throws IOException
    {
        List<SSTableReader> readers = new ArrayList<>(descriptors.size());
        List<UnfilteredPartitionIterator> scanners = new ArrayList<>(descriptors.size());
        try
        {
            for (Descriptor descriptor : descriptors)
            {
                // the components are given, so that opening the sstable never writes a TOC.txt for one that has none (as
                // loading it without them would; the scan of the data directories reports such sstables), and it is opened
                // offline without validation, which rebuilds nothing
                Set<Component> components = descriptor.fileFor(Components.TOC).exists() ? TOCComponent.loadTOC(descriptor, false)
                                                                                         : descriptor.discoverComponents();
                readers.add(SSTableReader.open(null, descriptor, components, TableMetadataRef.forOfflineTools(metadata), false, true));
            }
            for (SSTableReader reader : readers)
                scanners.add(reader.getScanner());
            // from here the merge owns the scanners, and closes them
            try (UnfilteredPartitionIterator merged = UnfilteredPartitionIterators.merge(new ArrayList<>(scanners), UnfilteredPartitionIterators.MergeListener.NOOP))
            {
                scanners.clear();
                PartitionIterator partitions = UnfilteredPartitionIterators.filter(merged, FBUtilities.nowInSeconds());
                while (partitions.hasNext())
                {
                    try (RowIterator partition = partitions.next())
                    {
                        while (partition.hasNext())
                        {
                            Row row = partition.next();
                            consumer.accept(UntypedResultSet.Row.fromInternalRow(metadata, partition.partitionKey(), row));
                        }
                    }
                }
            }
        }
        finally
        {
            for (UnfilteredPartitionIterator scanner : scanners)
                FileUtils.closeQuietly(scanner);
            for (SSTableReader reader : readers)
                reader.selfRef().release();
        }
    }

    /**
     * The table as {@link #read} describes it from the serialization headers of its sstables.
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
     * {@code descriptors}; returns whether a directory of the table exists.
     */
    private static boolean descriptors(List<File> dataDirectories, String keyspace, String table, List<Descriptor> descriptors)
    {
        boolean found = false;
        for (File dataDirectory : dataDirectories)
        {
            File keyspaceDirectory = new File(dataDirectory, keyspace);
            if (!keyspaceDirectory.isDirectory())
                continue;
            for (File tableDirectory : keyspaceDirectory.listUnchecked(f -> f.isDirectory() && (isDirectoryOf(f.name(), table) || f.name().equals(table))))
            {
                found = true;
                descriptors.addAll(sstablesIn(tableDirectory));
            }
        }
        return found;
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
