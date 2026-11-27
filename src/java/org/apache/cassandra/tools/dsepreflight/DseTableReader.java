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
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.regex.Pattern;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.sstable.format.bti.BtiFormat;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Reads the rows of one table from a node's sstables, offline and read-only: those of the data directories or of a
 * snapshot of the table, or one sstable on its own ({@link #readSSTable}), written by DSE or by HCD 2.0. Only the
 * sstables of committed transactions are read ({@link LifecycleTransaction#getFiles}), only in the trie format, and they
 * are opened without validation, which neither rebuilds nor saves any component; the big sstables of a DSE 5.1 node
 * are read from a scratch copy instead ({@link #readBig}). The table is described by the
 * serialization headers of its sstables (the union of their columns, with the types they were written with), so that a
 * column DSE has and this software does not, such as the {@code restricted} set of DSE's
 * {@code system_auth.role_permissions}, is read too; the primary key columns are named by the caller. The sstables read
 * together are merged as a node merges them, so rows deleted later (a keyspace DSE dropped, for instance) are not
 * returned.
 */
final class DseTableReader
{
    private static final Logger logger = LoggerFactory.getLogger(DseTableReader.class);

    /** The table id at the end of the name of a table directory. */
    private static final Pattern TABLE_ID = Pattern.compile("[0-9a-f]{32}");

    /** The prefix of the name of a scratch directory of {@link #readBig}. */
    static final String SCRATCH_PREFIX = "dsepreflight-";
    /** Where {@link #readBig} makes its scratch directories: the temporary directory when null; set by tests. */
    @VisibleForTesting
    @Nullable
    static volatile Path scratchParent;

    /** The components a scratch copy of a big sstable copies; {@link #LINKED} are linked instead. */
    private static final List<Component> COPIED = List.of(Components.TOC, Components.STATS,
                                                          BigFormat.Components.SUMMARY, Components.FILTER,
                                                          Components.COMPRESSION_INFO, Components.DIGEST,
                                                          Components.CRC);
    /** The components a scratch copy of a big sstable links symbolically: the large ones, never written on opening. */
    private static final List<Component> LINKED = List.of(Components.DATA, BigFormat.Components.PRIMARY_INDEX);

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
        return read(dataDirectories, null, keyspace, table, partitionKey, clustering);
    }

    /**
     * Reads every live row of {@code keyspace.table} from the data directories, or, when {@code snapshot} is not null,
     * from the snapshot of that name of the table ({@code <table directory>/snapshots/<snapshot>}) instead; the table is
     * {@link TableRows#found} when a directory of it (of the snapshot) exists.
     *
     * @see #read(List, String, String, List, List)
     */
    static TableRows read(List<File> dataDirectories, @Nullable String snapshot, String keyspace, String table, List<String> partitionKey, List<String> clustering) throws IOException
    {
        List<Descriptor> descriptors = new ArrayList<>();
        boolean found = descriptors(dataDirectories, snapshot, keyspace, table, descriptors);
        List<String> names = new ArrayList<>(descriptors.size());
        for (Descriptor descriptor : descriptors)
            names.add(descriptor.baseFile().toString());
        if (descriptors.isEmpty())
            return new TableRows(List.of(), names, found);
        for (Descriptor descriptor : descriptors)
            checkTrieFormat(descriptor);

        TableMetadata metadata = metadataFromHeaders(keyspace, table, descriptors, partitionKey, clustering);
        List<UntypedResultSet.Row> rows = new ArrayList<>();
        rows(descriptors, metadata, (row, cells) -> rows.add(row));
        return new TableRows(rows, names, found);
    }

    /**
     * Reads every live row of {@code keyspace.table} from the data directories of a DSE 5.1 node, whose sstables are in
     * the big format, as {@link #read(List, String, String, List, List)} reads those of the trie format. Opening a
     * big sstable can delete its {@code Summary.db} and write its {@code TOC.txt}, even offline, so each one is opened
     * from a scratch copy in a new temporary directory: its small components copied, its {@code Data.db} and
     * {@code Index.db} linked symbolically, never written; the copy is deleted once the rows are read. The node's files
     * are only read.
     *
     * @throws IOException when a directory or an sstable cannot be read, an sstable is not in the big format, or the
     *                     scratch copy cannot be made
     * @throws RuntimeException when an sstable cannot be decoded
     */
    static TableRows readBig(List<File> dataDirectories, String keyspace, String table, List<String> partitionKey,
                             List<String> clustering) throws IOException
    {
        return readBig(dataDirectories, null, keyspace, table, partitionKey, clustering);
    }

    /**
     * Reads every live row of {@code keyspace.table} from the data directories of a DSE 5.1 node, or, when
     * {@code snapshot} is not null, from the snapshot of that name of the table, as
     * {@link #read(List, String, String, String, List, List)} chooses them, from scratch copies as
     * {@link #readBig(List, String, String, List, List)} reads them.
     *
     * @see #readBig(List, String, String, List, List)
     */
    static TableRows readBig(List<File> dataDirectories, @Nullable String snapshot, String keyspace, String table,
                             List<String> partitionKey, List<String> clustering) throws IOException
    {
        List<Descriptor> descriptors = new ArrayList<>();
        boolean found = descriptors(dataDirectories, snapshot, keyspace, table, descriptors);
        List<String> names = new ArrayList<>(descriptors.size());
        for (Descriptor descriptor : descriptors)
            names.add(descriptor.baseFile().toString());
        if (descriptors.isEmpty())
            return new TableRows(List.of(), names, found);
        for (Descriptor descriptor : descriptors)
            checkBigFormat(descriptor);
        Path scratch = scratchDirectory();
        try
        {
            List<Descriptor> copies = new ArrayList<>(descriptors.size());
            for (int i = 0; i < descriptors.size(); i++)
                copies.add(scratchCopy(descriptors.get(i), scratch.resolve(Integer.toString(i))));
            TableMetadata metadata = metadataFromHeaders(keyspace, table, copies, partitionKey, clustering);
            List<UntypedResultSet.Row> rows = new ArrayList<>();
            rows(copies, metadata, (row, cells) -> rows.add(row));
            return new TableRows(rows, names, found);
        }
        finally
        {
            deleteScratch(scratch);
        }
    }

    /**
     * Passes every live row of one big sstable of a DSE 5.1 node on its own, as {@link #readSSTable} passes those of a
     * trie sstable, read from a scratch copy as {@link #readBig(List, String, String, List, List)} reads them.
     *
     * @throws IOException when the sstable cannot be read, is not in the big format, or the scratch copy cannot be made
     * @throws RuntimeException when the sstable cannot be decoded
     */
    static void readBigSSTable(Descriptor descriptor, List<String> partitionKey, List<String> clustering,
                               BiConsumer<UntypedResultSet.Row, Row> consumer) throws IOException
    {
        checkBigFormat(descriptor);
        Path scratch = scratchDirectory();
        try
        {
            Descriptor copy = scratchCopy(descriptor, scratch.resolve("0"));
            TableMetadata metadata = metadataFromHeaders(descriptor.ksname, descriptor.cfname, List.of(copy),
                                                         partitionKey, clustering);
            rows(List.of(copy), metadata, consumer);
        }
        finally
        {
            deleteScratch(scratch);
        }
    }

    /** Refuses an sstable that is not in the big format, before anything of it is copied or opened. */
    private static void checkBigFormat(Descriptor descriptor) throws IOException
    {
        if (!BigFormat.is(descriptor.version.format))
            throw new IOException(descriptor.baseFile() + " is in the " + descriptor.version.format.name() +
                                  " format, which DSE 5.1 does not write: this tool reads only the big format on " +
                                  "a DSE 5.1 node");
    }

    /** A new scratch directory of {@link #readBig}, in {@link #scratchParent} when set. */
    private static Path scratchDirectory() throws IOException
    {
        return scratchParent == null ? Files.createTempDirectory(SCRATCH_PREFIX)
                                     : Files.createTempDirectory(scratchParent, SCRATCH_PREFIX);
    }

    /**
     * Deletes a scratch directory of {@link #readBig}: its copies, and its links themselves (a link is deleted, never
     * followed, so the node's files it points to stay). What cannot be deleted stays in the temporary directory, which
     * holds no file of the node, and is logged at WARN; a run killed while it reads leaves its scratch directory too.
     */
    private static void deleteScratch(Path scratch)
    {
        try
        {
            Files.walkFileTree(scratch, new SimpleFileVisitor<>()
            {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attributes) throws IOException
                {
                    Files.delete(file);
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult postVisitDirectory(Path directory, IOException e) throws IOException
                {
                    Files.delete(directory);
                    return FileVisitResult.CONTINUE;
                }
            });
        }
        catch (IOException | RuntimeException e)
        {
            // what is left holds copies of components of the node's system tables: said, for the operator to delete
            logger.warn("Could not delete the scratch copy {} of a DSE 5.1 sstable ({}); delete it by hand", scratch,
                        e.toString());
        }
    }

    /**
     * A copy of the big sstable {@code descriptor} in {@code directory} (created), named as the original, its
     * {@link #COPIED} components copied and its {@link #LINKED} ones linked to the original files; the components it
     * does not have are left out.
     */
    private static Descriptor scratchCopy(Descriptor descriptor, Path directory) throws IOException
    {
        Path tableDirectory = Files.createDirectories(directory.resolve(descriptor.ksname)
                                                               .resolve(descriptor.directory.name()));
        Descriptor copy = new Descriptor(descriptor.version, new File(tableDirectory), descriptor.ksname,
                                         descriptor.cfname, descriptor.id);
        for (Component component : COPIED)
        {
            Path source = descriptor.fileFor(component).toPath();
            if (Files.exists(source))
                Files.copy(source, copy.fileFor(component).toPath());
        }
        for (Component component : LINKED)
        {
            Path source = descriptor.fileFor(component).toPath();
            if (Files.exists(source))
                Files.createSymbolicLink(copy.fileFor(component).toPath(), source.toAbsolutePath());
        }
        return copy;
    }

    /**
     * Passes every live row of one sstable on its own, described by its own serialization header, to {@code consumer}, one
     * at a time, with its live cells (for their timestamps and expiry): the rows a node reading that sstable alone would
     * return, never all held in memory.
     *
     * @throws IOException when the sstable cannot be read, or is not in a trie format
     * @throws RuntimeException when the sstable cannot be decoded
     */
    static void readSSTable(Descriptor descriptor, List<String> partitionKey, List<String> clustering, BiConsumer<UntypedResultSet.Row, Row> consumer) throws IOException
    {
        checkTrieFormat(descriptor);
        TableMetadata metadata = metadataFromHeaders(descriptor.ksname, descriptor.cfname, List.of(descriptor), partitionKey, clustering);
        rows(List.of(descriptor), metadata, consumer);
    }

    /**
     * Refuses an sstable that is not in a trie format, before anything of it is opened: opening an sstable of the big
     * format loads its summary, which deletes a Summary.db it cannot load and may rewrite or create TOC.txt, even offline.
     * Neither DSE 6 nor HCD 2.0 in {@code DSE_6} mode writes the big format, and the node inspection reports the big
     * sstables left by an earlier release as blocking.
     */
    private static void checkTrieFormat(Descriptor descriptor) throws IOException
    {
        if (!(descriptor.version.format instanceof BtiFormat))
            throw new IOException(descriptor.baseFile() + " is in the " + descriptor.version.format.name() + " format, which this tool does not read: " +
                                  "opening it could delete or rewrite some of its files");
    }

    /** The descriptors of the sstables of committed transactions in one directory: a table directory, or a snapshot of one. */
    static List<Descriptor> sstablesIn(File directory)
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

    /** Passes the live rows of the sstables given, merged as a node merges them, to {@code consumer}, each with its live cells. */
    private static void rows(List<Descriptor> descriptors, TableMetadata metadata, BiConsumer<UntypedResultSet.Row, Row> consumer) throws IOException
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
                            consumer.accept(UntypedResultSet.Row.fromInternalRow(metadata, partition.partitionKey(), row), row);
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
        descriptors(dataDirectories, null, keyspace, table, descriptors);
        if (descriptors.isEmpty())
            throw new IllegalStateException("no sstable of " + keyspace + '.' + table + " in " + dataDirectories);
        return metadataFromHeaders(keyspace, table, descriptors, partitionKey, clustering);
    }

    /**
     * Adds the descriptors of the sstables of committed transactions of {@code keyspace.table} in the data directories
     * (in its snapshot {@code snapshot}, when not null) to {@code descriptors}; returns whether a directory of the table
     * (of the snapshot) exists.
     */
    private static boolean descriptors(List<File> dataDirectories, @Nullable String snapshot, String keyspace, String table, List<Descriptor> descriptors)
    {
        boolean found = false;
        for (File dataDirectory : dataDirectories)
        {
            File keyspaceDirectory = new File(dataDirectory, keyspace);
            if (!keyspaceDirectory.isDirectory())
                continue;
            for (File tableDirectory : keyspaceDirectory.listUnchecked(f -> f.isDirectory() && (isDirectoryOf(f.name(), table) || f.name().equals(table))))
            {
                File directory = snapshot == null ? tableDirectory : new File(new File(tableDirectory, Directories.SNAPSHOT_SUBDIR), snapshot);
                if (!directory.isDirectory())
                    continue;
                found = true;
                descriptors.addAll(sstablesIn(directory));
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

    /** Whether a directory named {@code name} is the directory of some table named with its id, {@code <table>-<table id>}. */
    static boolean isTableDirectoryName(String name)
    {
        return name.length() > 33 && isDirectoryOf(name, name.substring(0, name.length() - 33));
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
