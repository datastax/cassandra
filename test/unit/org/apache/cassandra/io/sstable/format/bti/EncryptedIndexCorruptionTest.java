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

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.function.ThrowingRunnable;

import org.apache.cassandra.cache.ChunkCache;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.io.compress.CorruptBlockException;
import org.apache.cassandra.io.compress.EncryptedSequentialWriter;
import org.apache.cassandra.io.compress.EncryptorTest;
import org.apache.cassandra.io.sstable.AbstractSSTableIterator;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.SSTableReadsListener;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.tries.ReverseValueIterator;
import org.apache.cassandra.io.util.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * A corrupted page of an encrypted BTI partition or row index must fail the read with a CorruptSSTableException and
 * mark the sstable suspect, like corruption of a plain index does, so that e.g. compaction stops selecting it.
 * Corruption of a plain index, which is not checksummed, must not leak what the failed read held.
 */
public class EncryptedIndexCorruptionTest extends CQLTester
{
    private static final String ENCRYPTION = " WITH compression = {" +
                                             "'class' : 'Encryptor', " +
                                             "'cipher_algorithm' : 'AES/CBC/PKCS5Padding', " +
                                             "'secret_key_strength' : 128, " +
                                             "'key_provider' : '" + EncryptorTest.KeyProviderFactoryStub.class.getName() + "'}";

    private SSTableFormat<?, ?> savedFormat;
    private Config.DiskFailurePolicy savedPolicy;
    private int savedColumnIndexSizeInKiB;
    private Config.DiskAccessMode savedIndexAccessMode;

    @Before
    public void saveConfig()
    {
        savedFormat = DatabaseDescriptor.getSelectedSSTableFormat();
        savedPolicy = DatabaseDescriptor.getDiskFailurePolicy();
        savedColumnIndexSizeInKiB = DatabaseDescriptor.getColumnIndexSizeInKiB();
        savedIndexAccessMode = DatabaseDescriptor.getIndexAccessMode();
        DatabaseDescriptor.setSelectedSSTableFormat(BtiFormat.getInstance());
        DatabaseDescriptor.setDiskFailurePolicy(Config.DiskFailurePolicy.ignore);
    }

    @After
    public void restoreConfig()
    {
        DatabaseDescriptor.setSelectedSSTableFormat(savedFormat);
        DatabaseDescriptor.setDiskFailurePolicy(savedPolicy);
        DatabaseDescriptor.setColumnIndexSizeInKiB(savedColumnIndexSizeInKiB);
        DatabaseDescriptor.setIndexAccessMode(savedIndexAccessMode);
    }

    @Test
    public void testCorruptEncryptedPartitionIndexMarksSuspect() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, v text, PRIMARY KEY (pk, ck))" + ENCRYPTION);
        disableCompaction();

        for (int pk = 0; pk < 100; pk++)
            execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, 0, "value" + pk);
        flush();

        SSTableReader sstable = singleEncryptedSSTable();
        assertRows(execute("SELECT v FROM %s WHERE pk = ?", 42), row("value42"));

        // The partition index of such a small table fits in one chunk, which contains the trie root that every
        // lookup starts from: corrupt it and drop it from the chunk cache.
        File partitionIndex = sstable.descriptor.fileFor(BtiFormat.Components.PARTITION_INDEX);
        long length = partitionIndex.length();
        assertEquals("Unexpected partition index size", EncryptedSequentialWriter.CHUNK_SIZE, length);
        flipByte(partitionIndex, length / 2);

        DecoratedKey key = sstable.decorateKey(Int32Type.instance.decompose(42));

        // point read: BtiTableReader.getExactPosition
        assertCorruptRead("SELECT v FROM %s WHERE pk = ?", 42);
        assertSuspectAndReset(sstable, "point read");

        // BtiTableReader.getRowIndexEntry with a GT/GE operator
        assertCorrupt(() -> sstable.getPosition(key, SSTableReader.Operator.GE));
        assertSuspectAndReset(sstable, "getPosition(GE)");

        // BtiTableReader.getApproximatePosition
        assertCorrupt(() -> sstable.getApproximatePositionsForBounds(new Bounds<PartitionPosition>(key, key)));
        assertSuspectAndReset(sstable, "getApproximatePositionsForBounds");

        // a range read goes through the sstable scanner, which marks the sstable suspect itself
        assertCorruptRead("SELECT v FROM %s WHERE token(pk) >= token(?) LIMIT 1", 42);
        assertSuspectAndReset(sstable, "range read");

        // restore the file so that the table can be cleaned up normally
        flipByte(partitionIndex, length / 2);
        assertRows(execute("SELECT v FROM %s WHERE pk = ?", 42), row("value42"));
    }

    private static final int ROWS = 2000;

    /**
     * Writes a partition with a row index spanning several chunks and returns its sstable after checking that the
     * trie root (and the partition's row index header written just before it) is in the last chunk.
     */
    private SSTableReader writeIndexedPartition() throws Throwable
    {
        // index every row, so that the partition gets a row index spanning multiple chunks
        DatabaseDescriptor.setColumnIndexSizeInKiB(0);
        createTable("CREATE TABLE %s (pk int, ck int, v text, PRIMARY KEY (pk, ck))" + ENCRYPTION);
        disableCompaction();

        for (int ck = 0; ck < ROWS; ck++)
            execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", 1, ck, "value" + ck);
        flush();

        SSTableReader sstable = singleEncryptedSSTable();
        File rowIndex = sstable.descriptor.fileFor(BtiFormat.Components.ROW_INDEX);
        long chunks = rowIndex.length() / EncryptedSequentialWriter.CHUNK_SIZE;
        assertTrue("Expected a row index spanning several chunks, got " + rowIndex.length(), chunks >= 3);

        TrieIndexEntry entry = ((BtiTableReader) sstable).getExactPosition(sstable.decorateKey(Int32Type.instance.decompose(1)),
                                                                           SSTableReadsListener.NOOP_LISTENER,
                                                                           false);
        assertTrue(entry != null && entry.isIndexed());
        assertEquals("The row index trie root must be in the last chunk", chunks - 1, entry.indexTrieRoot / EncryptedSequentialWriter.CHUNK_SIZE);
        return sstable;
    }

    private static void flipBytesInChunks(File file, long fromChunk, long toChunkExclusive) throws Exception
    {
        for (long chunk = fromChunk; chunk < toChunkExclusive; chunk++)
            flipByte(file, chunk * EncryptedSequentialWriter.CHUNK_SIZE + 100);
    }

    @Test
    public void testCorruptEncryptedRowIndexMarksSuspect() throws Throwable
    {
        SSTableReader sstable = writeIndexedPartition();
        assertRows(execute("SELECT v FROM %s WHERE pk = ? AND ck >= ? LIMIT 1", 1, 0), row("value0"));

        // The partition lookup in BtiTableReader reads the row index header and the trie root in the last chunk; the
        // nodes a slice from the middle of the partition descends to are in earlier chunks, read by the sstable
        // iterator: corrupt all but the last chunk.
        File rowIndex = sstable.descriptor.fileFor(BtiFormat.Components.ROW_INDEX);
        long chunks = rowIndex.length() / EncryptedSequentialWriter.CHUNK_SIZE;
        flipBytesInChunks(rowIndex, 0, chunks - 1);

        Throwable t = assertCorruptRead("SELECT v FROM %s WHERE pk = ? AND ck >= ? LIMIT 1", 1, ROWS / 2);
        assertThrownThrough(t, AbstractSSTableIterator.class);
        assertSuspectAndReset(sstable, "slice read");

        flipBytesInChunks(rowIndex, 0, chunks - 1);
        assertRows(execute("SELECT v FROM %s WHERE pk = ? AND ck >= ? LIMIT 1", 1, ROWS / 2), row("value" + ROWS / 2));
    }

    /**
     * A reversed query with two slices whose second row index walk fails on a corrupted chunk must not release the
     * first slice's (already closed) row index walker a second time, which would drop a reference to a cached chunk
     * that is not held and leave the chunk unusable.
     */
    @Test
    public void testCorruptEncryptedRowIndexReversedSlices() throws Throwable
    {
        assertNotNull("chunk cache required to detect a double release", ChunkCache.instance);
        SSTableReader sstable = writeIndexedPartition();
        DecoratedKey key = sstable.decorateKey(Int32Type.instance.decompose(1));

        // Find a corrupted chunk and two clusterings low < high such that the reversed row index walk towards low
        // fails and the one towards high does not: a reversed iteration over both processes the high slice first,
        // through intact chunks, then fails creating the walker for the low one.
        File rowIndex = sstable.descriptor.fileFor(BtiFormat.Components.ROW_INDEX);
        long chunks = rowIndex.length() / EncryptedSequentialWriter.CHUNK_SIZE;
        long corruptChunk = -1;
        int low = -1;
        int high = -1;
        for (long chunk = 0; chunk < chunks - 1 && corruptChunk < 0; chunk++)
        {
            flipBytesInChunks(rowIndex, chunk, chunk + 1);
            int firstFailing = -1;
            for (int ck = 0; ck < ROWS && high < 0; ck += 50)
            {
                int clustering = ck;
                boolean failing = fails(() -> readReversed(sstable, key, clustering));
                if (failing && firstFailing < 0)
                    firstFailing = ck;
                else if (!failing && firstFailing >= 0)
                    high = ck;
            }
            sstable.unmarkSuspect();
            if (high >= 0)
            {
                corruptChunk = chunk;
                low = firstFailing;
            }
            else
            {
                flipBytesInChunks(rowIndex, chunk, chunk + 1);
            }
        }
        assertTrue("No row index chunk breaks the walk towards a clustering but not towards a higher one", corruptChunk >= 0);

        int lowClustering = low;
        int highClustering = high;
        Throwable t = assertCorrupt(() -> readReversed(sstable, key, lowClustering, highClustering));
        // the failure happens when moving to the second slice, after the first one was read
        assertThrownThrough(t, SSTableReversedIterator.class, "setForSlice");
        assertThrownThrough(t, AbstractSSTableIterator.class, "slice");
        // ... while constructing the new row index walker, which is what used to leave the closed one in place
        assertThrownThrough(t, ReverseValueIterator.class, "<init>");
        assertSuspectAndReset(sstable, "reversed slices read");

        // The chunks the first walker held are still cached; they must still be usable (a double release would have
        // dropped their reference count to zero while they are in the cache).
        assertEquals(1, readReversed(sstable, key, high));
        assertRows(execute("SELECT v FROM %s WHERE pk = ? AND ck = ?", 1, high), row("value" + high));

        flipBytesInChunks(rowIndex, corruptChunk, corruptChunk + 1);
        assertEquals(2, readReversed(sstable, key, low, high));
        assertFalse(sstable.isMarkedSuspect());
    }

    /**
     * The row index of a plain table is not checksummed, so a walker reading garbage fails with whatever unchecked
     * exception the garbage leads to (as can a data file read with a crc_check_chance below 1). When that happens
     * while the sstable iterator is constructed, the caller never gets the iterator to close: the constructor must
     * release the data file it opened and the row index reader itself.
     */
    @Test
    public void testUncheckedRowIndexFailureInConstructorReleasesResources() throws Throwable
    {
        assertNotNull("chunk cache required to detect the leak", ChunkCache.instance);
        DatabaseDescriptor.setColumnIndexSizeInKiB(0);
        // read the row index through the chunk cache too, to see whether its reader is released
        DatabaseDescriptor.setIndexAccessMode(Config.DiskAccessMode.standard);
        // the static column makes the iterator constructor open the data file to read the static row
        createTable("CREATE TABLE %s (pk int, ck int, s int static, v text, PRIMARY KEY (pk, ck))");
        disableCompaction();
        execute("INSERT INTO %s (pk, s) VALUES (?, ?)", 1, 42);
        // the plain row index is more compact than the encrypted one: write more rows to span several chunks
        int rows = 3 * ROWS;
        for (int ck = 0; ck < rows; ck++)
            execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", 1, ck, "value" + ck);
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        assertTrue(sstable instanceof BtiTableReader);
        DecoratedKey key = sstable.decorateKey(Int32Type.instance.decompose(1));
        File dataFile = sstable.descriptor.fileFor(SSTableFormat.Components.DATA);
        File rowIndex = sstable.descriptor.fileFor(BtiFormat.Components.ROW_INDEX);
        assertEquals(1, readForward(sstable, key, rows / 2));
        assertEquals(0, ChunkCache.instance.chunksInUse(dataFile));
        assertTrue("The row index must be read through the chunk cache", ChunkCache.instance.sizeOfFile(rowIndex) > 0);
        assertEquals(0, ChunkCache.instance.chunksInUse(rowIndex));

        // Overwrite all but the last chunk (which holds the trie root and the partition header) with garbage and
        // find a slice whose row index walk fails with an unchecked exception while constructing the iterator.
        long chunks = rowIndex.length() / EncryptedSequentialWriter.CHUNK_SIZE;
        assertTrue("Expected a row index spanning several chunks, got " + rowIndex.length(), chunks >= 3);
        byte[] saved = overwrite(rowIndex, 0, (chunks - 1) * EncryptedSequentialWriter.CHUNK_SIZE, (byte) 0xFF);

        Throwable failure = null;
        for (int ck = 0; ck < rows && failure == null; ck += 25)
        {
            try
            {
                readForward(sstable, key, ck);
            }
            catch (Throwable t)
            {
                if (findCause(t, CorruptSSTableException.class) == null && thrownThrough(t, AbstractSSTableIterator.class, "<init>"))
                    failure = t;
            }
        }
        try
        {
            assertNotNull("No slice read failed with an unchecked exception in the iterator constructor", failure);
            assertEquals("The iterator construction failed with " + failure + " and leaked the data file",
                         0, ChunkCache.instance.chunksInUse(dataFile));
            assertEquals("The iterator construction failed with " + failure + " and leaked the row index reader",
                         0, ChunkCache.instance.chunksInUse(rowIndex));
            // an unchecked exception does not by itself say the sstable is corrupted
            assertFalse(sstable.isMarkedSuspect());
        }
        finally
        {
            overwrite(rowIndex, 0, saved);
        }
        assertEquals(1, readForward(sstable, key, rows / 2));
    }

    private static int readForward(SSTableReader sstable, DecoratedKey key, int clustering)
    {
        ClusteringComparator comparator = sstable.metadata().comparator;
        int count = 0;
        Slices slices = Slices.with(comparator, Slice.make(comparator, clustering));
        try (UnfilteredRowIterator iterator = sstable.rowIterator(key, slices, ColumnFilter.all(sstable.metadata()), false,
                                                                  SSTableReadsListener.NOOP_LISTENER))
        {
            while (iterator.hasNext())
            {
                iterator.next();
                count++;
            }
        }
        return count;
    }

    /**
     * Overwrites the given range of the file with the given byte, drops the file's chunks from the chunk cache and
     * returns the previous content.
     */
    private static byte[] overwrite(File file, long position, long length, byte value) throws Exception
    {
        byte[] previous = new byte[(int) length];
        try (FileChannel channel = FileChannel.open(file.toPath(), StandardOpenOption.READ))
        {
            assertEquals(length, channel.read(ByteBuffer.wrap(previous), position));
        }
        byte[] garbage = new byte[(int) length];
        Arrays.fill(garbage, value);
        overwrite(file, position, garbage);
        return previous;
    }

    private static void overwrite(File file, long position, byte[] content) throws Exception
    {
        try (FileChannel channel = FileChannel.open(file.toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE))
        {
            assertEquals(content.length, channel.write(ByteBuffer.wrap(content), position));
            channel.force(true);
        }
        if (ChunkCache.instance != null)
            ChunkCache.instance.invalidateFileNow(file);
    }

    /**
     * Iterates in reverse order over the rows with the given clusterings, one slice each, and returns the row count.
     */
    private static int readReversed(SSTableReader sstable, DecoratedKey key, int... clusterings)
    {
        ClusteringComparator comparator = sstable.metadata().comparator;
        Slices.Builder slices = new Slices.Builder(comparator);
        for (int clustering : clusterings)
            slices.add(Slice.make(comparator, clustering));
        int count = 0;
        try (UnfilteredRowIterator iterator = sstable.rowIterator(key, slices.build(), ColumnFilter.all(sstable.metadata()), true, SSTableReadsListener.NOOP_LISTENER))
        {
            while (iterator.hasNext())
            {
                iterator.next();
                count++;
            }
        }
        return count;
    }

    private static boolean fails(ThrowingRunnable operation)
    {
        try
        {
            operation.run();
            return false;
        }
        catch (Throwable t)
        {
            return findCause(t, CorruptSSTableException.class) != null;
        }
    }

    private SSTableReader singleEncryptedSSTable()
    {
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        Set<SSTableReader> sstables = cfs.getLiveSSTables();
        assertEquals(1, sstables.size());
        SSTableReader sstable = sstables.iterator().next();
        assertTrue("Expected a BTI sstable, got " + sstable.descriptor, sstable instanceof BtiTableReader);
        assertTrue(sstable.descriptor.version.indicesAreEncrypted());
        assertFalse(sstable.isMarkedSuspect());
        return sstable;
    }

    private static void assertSuspectAndReset(SSTableReader sstable, String operation)
    {
        assertTrue("The sstable must be marked suspect after " + operation, sstable.isMarkedSuspect());
        sstable.unmarkSuspect();
    }

    private Throwable assertCorruptRead(String query, Object... values)
    {
        return assertCorrupt(() -> execute(query, values));
    }

    private static Throwable assertCorrupt(ThrowingRunnable operation)
    {
        try
        {
            operation.run();
        }
        catch (Throwable t)
        {
            Throwable corrupt = findCause(t, CorruptSSTableException.class);
            assertTrue("Expected a CorruptSSTableException in the cause chain of " + t, corrupt != null);
            assertTrue("Expected a CorruptBlockException cause, got " + corrupt.getCause(),
                       findCause(corrupt, CorruptBlockException.class) != null);
            return t;
        }
        throw new AssertionError("Expected the read of a corrupted encrypted index to fail");
    }

    private static void assertThrownThrough(Throwable t, Class<?> type)
    {
        assertThrownThrough(t, type, null);
    }

    /**
     * Asserts that the stack trace of the throwable or of one of its causes has a frame of the given class (or of
     * one of its nested classes) and, if given, method.
     */
    private static void assertThrownThrough(Throwable t, Class<?> type, String method)
    {
        if (!thrownThrough(t, type, method))
            throw new AssertionError("Expected a " + type.getSimpleName() + (method == null ? "" : '.' + method) +
                                     " frame in the stack trace of " + t);
    }

    private static boolean thrownThrough(Throwable t, Class<?> type, String method)
    {
        for (Throwable c = t; c != null; c = c.getCause())
        {
            for (StackTraceElement frame : c.getStackTrace())
            {
                String className = frame.getClassName();
                if ((className.equals(type.getName()) || className.startsWith(type.getName() + '$'))
                    && (method == null || frame.getMethodName().equals(method)))
                    return true;
            }
        }
        return false;
    }

    private static Throwable findCause(Throwable t, Class<? extends Throwable> type)
    {
        for (Throwable c = t; c != null; c = c.getCause())
        {
            if (type.isInstance(c))
                return c;
        }
        return null;
    }

    /**
     * Flips a byte of the file and drops the file's chunks from the chunk cache, including those cached by the
     * handles that are already open.
     */
    private static void flipByte(File file, long position) throws Exception
    {
        try (FileChannel channel = FileChannel.open(file.toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE))
        {
            ByteBuffer buf = ByteBuffer.allocate(1);
            assertEquals(1, channel.read(buf, position));
            buf.put(0, (byte) (buf.get(0) ^ 0x5A));
            buf.rewind();
            assertEquals(1, channel.write(buf, position));
            channel.force(true);
        }
        if (ChunkCache.instance != null)
            ChunkCache.instance.invalidateFileNow(file);
    }
}
