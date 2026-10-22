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

package org.apache.cassandra.io.sstable.format.bti;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.cache.ChunkCache;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.WrappingUnfilteredRowIterator;
import org.apache.cassandra.io.compress.EncryptorTest;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.SSTableReadsListener;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.PageAware;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.concurrent.Ref;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * {@link BtiTableWriter#resetAndTruncate()} after a failed append (as in {@code SSTableRewriter.tryAppend}) rewrites
 * the chunks containing the mark. A reader opened early before that may have cached such a chunk, and the chunk cache
 * must not serve that stale content to the readers opened afterwards, in particular the final one; likewise for the
 * partial chunks cached by the early opens after a truncation.
 */
@RunWith(Parameterized.class)
public class BtiTableWriterResetAndTruncateTest extends CQLTester
{
    private static final String ENCRYPTION = " WITH compression = {" +
                                             "'class' : 'Encryptor', " +
                                             "'cipher_algorithm' : 'AES/CBC/PKCS5Padding', " +
                                             "'secret_key_strength' : 128, " +
                                             "'key_provider' : '" + EncryptorTest.KeyProviderFactoryStub.class.getName() + "'}";

    @Parameterized.Parameter
    public boolean encrypted;

    @Parameterized.Parameters(name = "encrypted={0}")
    public static Collection<Object[]> parameters()
    {
        return List.of(new Object[]{ false }, new Object[]{ true });
    }

    private SSTableFormat<?, ?> savedFormat;
    private Config.DiskAccessMode savedIndexAccessMode;
    private int savedColumnIndexSizeInKiB;

    @Before
    public void saveConfig()
    {
        savedFormat = DatabaseDescriptor.getSelectedSSTableFormat();
        savedIndexAccessMode = DatabaseDescriptor.getIndexAccessMode();
        savedColumnIndexSizeInKiB = DatabaseDescriptor.getColumnIndexSizeInKiB();
        DatabaseDescriptor.setSelectedSSTableFormat(BtiFormat.getInstance());
        // read plain index files through the chunk cache (encrypted ones always are)
        DatabaseDescriptor.setIndexAccessMode(Config.DiskAccessMode.standard);
        // index every row, so that every partition gets a row index
        DatabaseDescriptor.setColumnIndexSizeInKiB(0);
    }

    @After
    public void restoreConfig()
    {
        Ref.setOnLeak(null);
        DatabaseDescriptor.setSelectedSSTableFormat(savedFormat);
        DatabaseDescriptor.setIndexAccessMode(savedIndexAccessMode);
        DatabaseDescriptor.setColumnIndexSizeInKiB(savedColumnIndexSizeInKiB);
    }

    /**
     * A reader that read the row index before the truncation (simulated by opening the row index from the writer's
     * own file handle builder, as {@link BtiTableWriter#openEarly} does) cached the chunk containing the mark.
     */
    @Test
    public void testFinalReaderDoesNotSeeChunksCachedBeforeTruncation() throws Throwable
    {
        assertNotNull("chunk cache required", ChunkCache.instance);
        TableMetadata metadata = createTestTable();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();

        List<DecoratedKey> keys = sortedKeys(metadata, 12);
        List<DecoratedKey> written = keys.subList(0, 10);
        DecoratedKey discarded = keys.get(10);
        DecoratedKey rewritten = keys.get(11);

        SSTableReader sstable;
        // closing the writer aborts it if the test fails before finish()
        try (LifecycleTransaction txn = LifecycleTransaction.offline(OperationType.WRITE, cfs.metadata);
             BtiTableWriter writer = createWriter(cfs, txn, keys.size()))
        {
            for (DecoratedKey key : written)
                writer.append(partition(metadata, key, 20, "a"));

            writer.mark();
            // the discarded partition writes some of its row index trie before the failure (see clustering()), which
            // the sync below puts on disk, so that the truncation does truncate the file instead of resetting the buffer
            appendFailing(writer, partition(metadata, discarded, 30_000, "discarded"), 20_000);

            // What an early open does: a reader opened on what was flushed so far caches the chunk containing the
            // mark, now holding the start of the discarded partition's row index.
            BtiTableWriter.IndexWriter indexWriter = writer.indexWriter();
            indexWriter.rowIndexWriter.sync();
            try (FileHandle rowIndex = indexWriter.openRowIndexHandle())
            {
                readAll(rowIndex);
            }

            long flushedBeforeTruncation = indexWriter.rowIndexWriter.getLastFlushOffset();
            writer.resetAndTruncate();
            assertTrue("The row index was not truncated", indexWriter.rowIndexWriter.getLastFlushOffset() < flushedBeforeTruncation);
            writer.append(partition(metadata, rewritten, 25, "rewritten"));

            sstable = writer.finish(true, null);
            txn.finish();
        }

        try
        {
            assertPartition(sstable, rewritten, 25, "rewritten");
            assertPartition(sstable, written.get(9), 20, "a");
            assertEquals(0, read(sstable, discarded, Slices.ALL).size());
        }
        finally
        {
            sstable.selfRef().release();
        }
    }

    /**
     * Drives the early opens through {@link BtiTableWriter#openEarly} with natural (buffer-full) flushes. A reader
     * opened before a failed append reads its data and row index; after the truncation the flushes of a plain index
     * writer are no longer aligned to chunks, so a reader opened early at such a flush boundary caches a partial last
     * chunk. The readers opened after them must not be served any of those chunks.
     */
    @Test
    public void testFinalReaderDoesNotSeePartialChunksCachedAfterTruncation() throws Throwable
    {
        assertNotNull("chunk cache required", ChunkCache.instance);
        TableMetadata metadata = createTestTable();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();

        int count = 100_000;
        List<DecoratedKey> keys = sortedKeys(metadata, count);

        SSTableReader sstable;
        AtomicReference<SSTableReader> before = new AtomicReference<>();
        AtomicReference<SSTableReader> after = new AtomicReference<>();
        int next = 0;
        int discarded;
        try (LifecycleTransaction txn = LifecycleTransaction.offline(OperationType.WRITE, cfs.metadata);
             BtiTableWriter writer = createWriter(cfs, txn, count))
        {
            BtiTableWriter.IndexWriter indexWriter = writer.indexWriter();
            writer.setMaxDataAge(System.currentTimeMillis());

            for (; next < 10; next++)
                writer.append(partition(metadata, keys.get(next), 3, "a"));

            // an early open before the failure, whose reader reads (and caches) all its data and row index
            writer.openEarly(before::set);
            while (before.get() == null && next < count / 2)
                writer.append(partition(metadata, keys.get(next++), 3, "a"));
            assertNotNull("The first early open never completed", before.get());
            readAllData(before.get());
            readAll(rowIndexFile(before.get()));

            // a failed append, as in SSTableRewriter.tryAppend: the row index of the discarded partition grows larger
            // than the writer's buffer before the failure, so the truncation does truncate the file instead of
            // resetting the buffer
            writer.mark();
            discarded = next;
            appendFailing(writer, partition(metadata, keys.get(next++), 30_000, "discarded"), 20_000);
            long flushedBeforeTruncation = indexWriter.rowIndexWriter.getLastFlushOffset();
            writer.resetAndTruncate();
            assertTrue("The row index was not truncated", indexWriter.rowIndexWriter.getLastFlushOffset() < flushedBeforeTruncation);

            // an early open once all files have been flushed past what it covers, as SSTableRewriter does
            writer.openEarly(after::set);
            while (after.get() == null && next < count - 1000)
                writer.append(partition(metadata, keys.get(next++), 3, "a"));
            assertNotNull("The second early open never completed", after.get());
            FileHandle earlyRowIndex = rowIndexFile(after.get());
            long boundary = earlyRowIndex.dataLength();
            if (!encrypted)
                assertTrue("Expected an unaligned flush boundary after the truncation, got " + boundary, boundary % PageAware.PAGE_SIZE != 0);

            // the early reader reads its whole row index, caching the partial chunk at the flush boundary
            readAll(earlyRowIndex);

            for (int i = 0; i < 1000; i++)
                writer.append(partition(metadata, keys.get(next++), 3, "a"));
            sstable = writer.finish(true, null);
            txn.finish();
        }
        finally
        {
            if (before.get() != null)
                before.get().selfRef().release();
            if (after.get() != null)
                after.get().selfRef().release();
        }

        try
        {
            for (int i = 0; i < next; i++)
            {
                if (i == discarded)
                    assertEquals(0, read(sstable, keys.get(i), Slices.ALL).size());
                else
                    assertPartition(sstable, keys.get(i), 3, "a");
            }
        }
        finally
        {
            sstable.selfRef().release();
        }
    }

    /**
     * A failure while opening an early reader must release the partial partition index built for it. The failure is
     * induced by the unset max data age, whose precondition fails right after openInternal acquired the partition
     * index, so this covers that leak specifically.
     */
    @Test
    public void testFailedEarlyOpenReleasesPartitionIndex() throws Throwable
    {
        List<Object> leaks = new CopyOnWriteArrayList<>();
        Ref.setOnLeak(leaks::add);
        TableMetadata metadata = createTestTable();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();

        int count = 100_000;
        List<DecoratedKey> keys = sortedKeys(metadata, count);
        try (LifecycleTransaction txn = LifecycleTransaction.offline(OperationType.WRITE, cfs.metadata);
             BtiTableWriter writer = createWriter(cfs, txn, count))
        {
            for (int next = 0; next < 10; next++)
                writer.append(partition(metadata, keys.get(next), 3, "a"));

            // the max data age is not set, which makes the construction of the early reader fail
            writer.openEarly(reader -> fail("The early open should have failed"));
            Throwable failure = null;
            for (int next = 10; next < count && failure == null; next++)
            {
                try
                {
                    writer.append(partition(metadata, keys.get(next), 3, "a"));
                }
                catch (Throwable t)
                {
                    failure = t;
                }
            }
            assertNotNull("The early open was never attempted", failure);
            assertTrue("Unexpected failure " + failure, thrownThrough(failure, BtiTableWriter.class.getName(), "openInternal"));
        }

        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (leaks.isEmpty() && System.nanoTime() < deadline)
        {
            System.gc();
            Thread.sleep(100);
        }
        assertEquals("Leaked references: " + leaks, 0, leaks.size());
    }

    private TableMetadata createTestTable()
    {
        createTable("CREATE TABLE %s (pk int, ck int, v text, PRIMARY KEY (pk, ck))" + (encrypted ? ENCRYPTION : ""));
        disableCompaction();
        return getCurrentColumnFamilyStore().metadata();
    }

    private static List<DecoratedKey> sortedKeys(TableMetadata metadata, int count)
    {
        List<DecoratedKey> keys = new ArrayList<>(count);
        for (int pk = 0; pk < count; pk++)
            keys.add(metadata.partitioner.decorateKey(Int32Type.instance.decompose(pk)));
        keys.sort(DecoratedKey::compareTo);
        return keys;
    }

    private static boolean thrownThrough(Throwable t, String className, String method)
    {
        for (Throwable c = t; c != null; c = c.getCause())
        {
            for (StackTraceElement frame : c.getStackTrace())
            {
                if (frame.getClassName().equals(className) && frame.getMethodName().equals(method))
                    return true;
            }
        }
        return false;
    }

    private static final class SimulatedFailure extends RuntimeException
    {
        SimulatedFailure()
        {
            super("simulated failure of a partition append");
        }
    }

    /**
     * Appends the partition through an iterator that fails after the given number of rows, as a compaction whose
     * input turns out to be corrupted does.
     */
    private static void appendFailing(BtiTableWriter writer, UnfilteredRowIterator partition, int failAfter)
    {
        UnfilteredRowIterator failing = new WrappingUnfilteredRowIterator()
        {
            private int returned;

            @Override
            public UnfilteredRowIterator wrapped()
            {
                return partition;
            }

            @Override
            public boolean hasNext()
            {
                return partition.hasNext();
            }

            @Override
            public Unfiltered next()
            {
                if (returned++ == failAfter)
                    throw new SimulatedFailure();
                return partition.next();
            }

            @Override
            public void close()
            {
                partition.close();
            }
        };
        try
        {
            writer.append(failing);
            fail("The append should have failed");
        }
        catch (SimulatedFailure expected)
        {
            // as expected
        }
    }

    /**
     * The row index file handle of a BTI reader. With {@code sharedCopy == false}, {@code unbuildTo} hands out the
     * reader's own handles and takes no new references.
     */
    private static FileHandle rowIndexFile(SSTableReader reader)
    {
        return ((BtiTableReader) reader).unbuildTo(new BtiTableReader.Builder(reader.descriptor), false).getRowIndexFile();
    }

    /**
     * Reads (and so caches) every chunk of the file visible through the handle. Chunk by chunk, as the positions of an
     * encrypted index skip the end of each chunk.
     */
    private static void readAll(FileHandle handle) throws IOException
    {
        try (RandomAccessReader reader = handle.createReader())
        {
            for (long position = 0; position < reader.length(); position += PageAware.PAGE_SIZE)
            {
                reader.seek(position);
                reader.readByte();
            }
        }
    }

    private static void readAllData(SSTableReader sstable)
    {
        try (ISSTableScanner scanner = sstable.getScanner())
        {
            while (scanner.hasNext())
            {
                try (UnfilteredRowIterator partition = scanner.next())
                {
                    while (partition.hasNext())
                        partition.next();
                }
            }
        }
    }

    private static void assertPartition(SSTableReader sstable, DecoratedKey key, int rows, String prefix)
    {
        TableMetadata metadata = sstable.metadata();
        ClusteringComparator comparator = metadata.comparator;
        ColumnMetadata v = metadata.getColumn(ColumnIdentifier.getInterned("v", false));
        for (int i = 0; i < rows; i++)
        {
            // a slice from the middle of the partition walks its row index
            int ck = clustering(i);
            List<Unfiltered> read = read(sstable, key, Slices.with(comparator, Slice.make(comparator, ck)));
            assertEquals("rows for ck=" + ck, 1, read.size());
            Row row = (Row) read.get(0);
            assertEquals(ck, (int) Int32Type.instance.compose(row.clustering().bufferAt(0)));
            assertEquals(prefix + i, UTF8Type.instance.compose(row.getCell(v).buffer()));
        }
    }

    private static List<Unfiltered> read(SSTableReader sstable, DecoratedKey key, Slices slices)
    {
        List<Unfiltered> result = new ArrayList<>();
        try (UnfilteredRowIterator iterator = sstable.rowIterator(key, slices, ColumnFilter.all(sstable.metadata()), false,
                                                                  SSTableReadsListener.NOOP_LISTENER))
        {
            while (iterator.hasNext())
                result.add(iterator.next());
        }
        return result;
    }

    private static UnfilteredRowIterator partition(TableMetadata metadata, DecoratedKey key, int rows, String prefix)
    {
        PartitionUpdate.SimpleBuilder builder = PartitionUpdate.simpleBuilder(metadata, Int32Type.instance.compose(key.getKey()));
        for (int i = 0; i < rows; i++)
            builder.row(clustering(i)).add("v", prefix + i);
        return builder.build().unfilteredIterator();
    }

    /**
     * The clustering of the i-th row. The row index trie of a partition is kept in memory until the subtrees of its
     * nodes are complete and larger than a page: spreading the clusterings over distinct first bytes, 2000 rows each,
     * makes the trie of a large partition reach the file (and the writer's buffer fill) while the partition is written.
     */
    private static int clustering(int i)
    {
        return ((i / 2000) << 24) | (i % 2000);
    }

    private static BtiTableWriter createWriter(ColumnFamilyStore cfs, LifecycleTransaction txn, long keyCount)
    {
        Descriptor desc = cfs.newSSTableDescriptor(cfs.getDirectories().getDirectoryForNewSSTables(), BtiFormat.getInstance());
        return (BtiTableWriter) desc.getFormat().getWriterFactory().builder(desc)
                                    .setTableMetadataRef(cfs.metadata)
                                    .setKeyCount(keyCount)
                                    .setSerializationHeader(new SerializationHeader(true, cfs.metadata(),
                                                                                    cfs.metadata().regularAndStaticColumns(),
                                                                                    EncodingStats.NO_STATS))
                                    .setMetadataCollector(new MetadataCollector(cfs.metadata().comparator))
                                    .addDefaultComponents(cfs.indexManager.listIndexGroups())
                                    .build(txn, cfs);
    }
}
