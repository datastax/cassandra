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

package org.apache.cassandra.io.util;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.zip.CRC32;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.cache.ChunkCache;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.crypto.IKeyProvider;
import org.apache.cassandra.crypto.IKeyProviderFactory;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.compress.CorruptBlockException;
import org.apache.cassandra.io.compress.EncryptedSequentialWriter;
import org.apache.cassandra.io.compress.EncryptionConfig;
import org.apache.cassandra.io.compress.Encryptor;
import org.apache.cassandra.io.compress.EncryptorTest;
import org.apache.cassandra.io.compress.OutOfPlaceEncryptor;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.schema.CompressionParams;

import static org.apache.cassandra.io.compress.EncryptedSequentialWriter.CHUNK_SIZE;
import static org.apache.cassandra.io.compress.EncryptedSequentialWriter.FOOTER_LENGTH;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * Verifies that encryption-only files (as used for the BTI partition and row indexes of encrypted tables) are read
 * through the chunk cache when the {@link FileHandle.Builder} is given one, and that the cached reader returns the
 * same content and positions (including the skipping of the unusable tail of every chunk) as an uncached one; that
 * handles with a length override share the cached chunks; and that corrupted chunks (checksum mismatch, decryption
 * failure with the wrong key, invalid length footer, truncated chunk) are reported as a CorruptSSTableException
 * caused by a CorruptBlockException and are not left in the cache.
 */
@RunWith(Parameterized.class)
public class EncryptedChunkReaderCacheTest
{
    private static final int CHUNKS_TO_WRITE = 8;

    @Parameterized.Parameter(0)
    public boolean mmapped;

    @Parameterized.Parameter(1)
    public boolean outOfPlace;

    private CompressionParams compressionParams;
    private CompressionMetadata compressionMetadata;
    private File file;
    private long[] writtenPositions; // where each byte was written, with chunk-end positions moved to the next chunk
    private byte[] writtenValues;
    private int maxBytesInPage;

    @Parameterized.Parameters(name = "mmapped={0} outOfPlace={1}")
    public static Collection<Object[]> parameters()
    {
        return Arrays.asList(new Object[][]{
            { false, false },
            { false, true },
            { true, false },
            { true, true },
        });
    }

    @BeforeClass
    public static void setupDD()
    {
        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.enableChunkCache(512);
        CassandraRelevantProperties.CHUNKCACHE_ASYNC_CLEANUP.setBoolean(false); // keep the cache sizes stable after operations
    }

    /**
     * Key provider returning a key different from the all-zero one of {@link EncryptorTest.KeyProviderFactoryStub}.
     */
    public static class OtherKeyProviderFactoryStub implements IKeyProviderFactory
    {
        @Override
        public IKeyProvider getKeyProvider(Map<String, String> options)
        {
            return new IKeyProvider()
            {
                @Override
                public SecretKey getSecretKey(String cipherName, int keyStrength)
                {
                    byte[] key = new byte[keyStrength / 8];
                    Arrays.fill(key, (byte) 0x3C);
                    return new SecretKeySpec(key, cipherName.replaceAll("/.*", ""));
                }
            };
        }

        @Override
        public Set<String> supportedOptions()
        {
            return Collections.emptySet();
        }
    }

    private CompressionParams encryptionParams(Class<? extends IKeyProviderFactory> keyProviderFactory)
    {
        Map<String, String> opts = new HashMap<>();
        opts.put(EncryptionConfig.KEY_PROVIDER, keyProviderFactory.getName());
        opts.put(EncryptionConfig.CIPHER_ALGORITHM, "AES/CBC/PKCS5Padding");
        opts.put(EncryptionConfig.SECRET_KEY_STRENGTH, Integer.toString(128));
        opts.put(CompressionParams.CLASS, outOfPlace ? OutOfPlaceEncryptor.class.getName()
                                                     : Encryptor.class.getName());
        return CompressionParams.fromMap(opts);
    }

    @Before
    public void setUp() throws IOException
    {
        compressionParams = encryptionParams(EncryptorTest.KeyProviderFactoryStub.class);
        compressionMetadata = CompressionMetadata.encryptedOnly(compressionParams);
        assertEquals(!outOfPlace, compressionParams.getSstableCompressor().encryptionOnly().canDecompressInPlace());
        maxBytesInPage = EncryptedSequentialWriter.maxBytesInPage(compressionParams.getSstableCompressor().encryptionOnly());

        assertNotNull("The chunk cache must be enabled for this test", ChunkCache.instance);
        ChunkCache.instance.clear();
        file = FileUtils.createTempFile("encrypted-chunk-cache", ".db");
        file.deleteOnExit();

        int bytesToWrite = maxBytesInPage * CHUNKS_TO_WRITE - 17; // leave the last chunk partially filled
        writtenPositions = new long[bytesToWrite];
        writtenValues = new byte[bytesToWrite];
        try (EncryptedSequentialWriter writer = new EncryptedSequentialWriter(file,
                                                                              SequentialWriterOption.newBuilder().finishOnClose(false).build(),
                                                                              compressionParams.getSstableCompressor().encryptionOnly()))
        {
            for (int i = 0; i < bytesToWrite; ++i)
            {
                // When the current chunk is full the writer reports the chunk's usable end as its position, and
                // the byte lands at the start of the next chunk (see EncryptedChunkReader.adjustPosition).
                long position = writer.position();
                writtenPositions[i] = (position & (CHUNK_SIZE - 1)) < maxBytesInPage ? position
                                                                                      : position - maxBytesInPage + CHUNK_SIZE;
                // the value depends on the chunk too, so that a chunk served for another cannot go unnoticed
                writtenValues[i] = (byte) (position ^ ((position >>> 8) * 7) ^ ((position >>> 12) * 31));
                writer.writeByte(writtenValues[i]);
            }
            writer.finish();
        }
        assertEquals(0, file.length() % CHUNK_SIZE);
        assertEquals(CHUNKS_TO_WRITE, file.length() / CHUNK_SIZE);
    }

    @After
    public void tearDown()
    {
        if (compressionMetadata != null)
            compressionMetadata.close();
        if (ChunkCache.instance != null)
            ChunkCache.instance.clear();
        if (file != null)
        {
            if (ChunkCache.instance != null)
                ChunkCache.instance.invalidateFile(file);
            file.tryDelete();
        }
    }

    private FileHandle.Builder handleBuilder(ChunkCache chunkCache)
    {
        return handleBuilder(chunkCache, compressionMetadata);
    }

    private FileHandle.Builder handleBuilder(ChunkCache chunkCache, CompressionMetadata metadata)
    {
        return new FileHandle.Builder(file).bufferSize(PageAware.PAGE_SIZE)
                                           .mmapped(mmapped)
                                           .withChunkCache(chunkCache)
                                           .encryptionOnly()
                                           .withCompressionMetadata(metadata);
    }

    /**
     * Reads the first byte of the given chunk through a new reader, expecting a CorruptSSTableException caused by a
     * CorruptBlockException, which is returned.
     */
    private static CorruptBlockException assertCorruptChunk(FileHandle fh, int chunk)
    {
        CorruptSSTableException e = assertThrows(CorruptSSTableException.class, () -> {
            try (RandomAccessReader reader = fh.createReader())
            {
                reader.seek((long) chunk * CHUNK_SIZE);
                reader.readByte();
            }
        });
        assertTrue("Unexpected cause " + e.getCause(), e.getCause() instanceof CorruptBlockException);
        return (CorruptBlockException) e.getCause();
    }

    /**
     * Encrypted data read with the wrong key passes the checksum (which covers the ciphertext) but fails decryption:
     * the decryption IOException must surface as a CorruptSSTableException caused by a CorruptBlockException, and the
     * chunk must not be cached.
     */
    @Test
    public void testDecryptionFailureThroughCache() throws IOException
    {
        CompressionParams otherKeyParams = encryptionParams(OtherKeyProviderFactoryStub.class);
        assertEquals(maxBytesInPage, EncryptedSequentialWriter.maxBytesInPage(otherKeyParams.getSstableCompressor().encryptionOnly()));
        try (CompressionMetadata otherKeyMetadata = CompressionMetadata.encryptedOnly(otherKeyParams);
             FileHandle fh = handleBuilder(ChunkCache.instance, otherKeyMetadata).complete())
        {
            assertFalse(fh.rebuffererFactory() instanceof EncryptedChunkReader);
            // With a wrong key, a padding check can accidentally pass (garbage is then returned); this happens with a
            // probability of about 1/256 per chunk, so check that at least one chunk fails and examine all failures.
            int failures = 0;
            for (int chunk = 0; chunk < CHUNKS_TO_WRITE; ++chunk)
            {
                long missesBefore = misses();
                CorruptBlockException cbe;
                try (RandomAccessReader reader = fh.createReader())
                {
                    reader.seek((long) chunk * CHUNK_SIZE);
                    reader.readByte();
                    continue;
                }
                catch (CorruptSSTableException e)
                {
                    assertTrue("Unexpected cause " + e.getCause(), e.getCause() instanceof CorruptBlockException);
                    cbe = (CorruptBlockException) e.getCause();
                }
                ++failures;
                assertEquals(1, misses() - missesBefore);
                assertTrue("Expected the decryption IOException as cause, got " + cbe.getCause(), cbe.getCause() instanceof IOException);

                // the failed chunk was not cached: reading it again loads (and fails) again
                missesBefore = misses();
                assertCorruptChunk(fh, chunk);
                assertEquals(1, misses() - missesBefore);
            }
            assertTrue("Expected decryption with the wrong key to fail", failures > 0);
        }
    }

    /**
     * The checksum is always verified for encryption-only files (CompressionParams.shouldCheckCrc() is true whenever
     * the params are enabled), so an invalid length in the chunk footer can only be read if the checksum was
     * (re)computed over it; simulate that and check that the length is validated.
     */
    @Test
    public void testInvalidLengthFooterWithValidChecksum() throws IOException
    {
        assertTrue(compressionParams.shouldCheckCrc());
        int chunk = 1;
        long chunkStart = (long) chunk * CHUNK_SIZE;
        try (FileChannel channel = FileChannel.open(file.toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE))
        {
            ByteBuffer buf = ByteBuffer.allocate(CHUNK_SIZE);
            assertEquals(CHUNK_SIZE, channel.read(buf, chunkStart));
            buf.putInt(CHUNK_SIZE - FOOTER_LENGTH, Integer.MAX_VALUE);
            CRC32 crc = new CRC32();
            buf.position(0).limit(CHUNK_SIZE - 4);
            crc.update(buf);
            buf.limit(CHUNK_SIZE);
            buf.putInt(CHUNK_SIZE - 4, (int) crc.getValue());
            buf.position(0);
            assertEquals(CHUNK_SIZE, channel.write(buf, chunkStart));
            channel.force(true);
        }

        try (FileHandle fh = handleBuilder(ChunkCache.instance).complete())
        {
            long missesBefore = misses();
            assertCorruptChunk(fh, chunk);
            assertCorruptChunk(fh, chunk);
            assertEquals("The corrupted chunk must not be cached", 2, misses() - missesBefore);
        }
    }

    /**
     * A chunk cut short on disk (e.g. a truncated file) must be reported as a corrupted block.
     */
    @Test
    public void testTruncatedChunk() throws IOException
    {
        int chunk = CHUNKS_TO_WRITE - 1;
        try (FileChannel channel = FileChannel.open(file.toPath(), StandardOpenOption.WRITE))
        {
            channel.truncate((long) chunk * CHUNK_SIZE + CHUNK_SIZE / 2);
        }

        try (FileHandle fh = handleBuilder(ChunkCache.instance).complete())
        {
            try (RandomAccessReader reader = fh.createReader())
            {
                reader.seek(0);
                assertEquals(writtenValues[0], reader.readByte());
            }
            long missesBefore = misses();
            assertCorruptChunk(fh, chunk);
            assertCorruptChunk(fh, chunk);
            assertEquals("The truncated chunk must not be cached", 2, misses() - missesBefore);
        }
    }

    private static long misses()
    {
        return ChunkCache.instance.metrics.misses();
    }

    private void readAndVerifyAll(RandomAccessReader reader) throws IOException
    {
        readAndVerifyUpTo(reader, Long.MAX_VALUE);
    }

    private void readAndVerifyUpTo(RandomAccessReader reader, long end) throws IOException
    {
        reader.seek(0);
        for (int i = 0; i < writtenPositions.length && writtenPositions[i] < end; ++i)
        {
            byte b = reader.readByte();
            assertEquals("Byte " + i + " written at position " + writtenPositions[i], writtenValues[i], b);
        }
    }

    @Test
    public void testReadsGoThroughChunkCache() throws IOException
    {
        assertEquals(0, ChunkCache.instance.sizeOfFile(file));
        long chunks = file.length() / CHUNK_SIZE;

        try (FileHandle fh = handleBuilder(ChunkCache.instance).complete())
        {
            assertFalse("Expected the encrypted reader to be wrapped by the chunk cache, got " + fh.rebuffererFactory(),
                        fh.rebuffererFactory() instanceof EncryptedChunkReader);
            assertEquals(CHUNK_SIZE, fh.rebuffererFactory().chunkSize());

            long missesBefore = misses();
            try (RandomAccessReader reader = fh.createReader())
            {
                readAndVerifyAll(reader);
            }
            assertEquals(chunks, ChunkCache.instance.sizeOfFile(file));
            assertEquals(chunks, misses() - missesBefore);

            // a second reader over the same handle is served from the cache
            missesBefore = misses();
            try (RandomAccessReader reader = fh.createReader())
            {
                readAndVerifyAll(reader);
            }
            assertEquals(chunks, ChunkCache.instance.sizeOfFile(file));
            assertEquals(0, misses() - missesBefore);

            // the cache wrapper invalidates single chunks on behalf of the encrypted reader
            fh.rebuffererFactory().invalidateIfCached(CHUNK_SIZE + 5);
            assertEquals(chunks - 1, ChunkCache.instance.sizeOfFile(file));
            missesBefore = misses();
            try (RandomAccessReader reader = fh.createReader())
            {
                readAndVerifyAll(reader);
            }
            assertEquals(chunks, ChunkCache.instance.sizeOfFile(file));
            assertEquals(1, misses() - missesBefore);
        }

        // a new handle on the same file reuses the cached chunks
        long missesBefore = misses();
        try (FileHandle fh = handleBuilder(ChunkCache.instance).complete();
             RandomAccessReader reader = fh.createReader())
        {
            readAndVerifyAll(reader);
        }
        assertEquals(chunks, ChunkCache.instance.sizeOfFile(file));
        assertEquals(0, misses() - missesBefore);
    }

    /**
     * Early-open handles on an index file use a length override (the writer's last content position); their cached
     * chunks are keyed on the file only, so a later handle with a larger (or no) length override reuses them.
     */
    @Test
    public void testLengthOverrideHandleSharesCachedChunks() throws IOException
    {
        int overrideChunks = CHUNKS_TO_WRITE / 2;
        // what EncryptedSequentialWriter.updateFileHandle sets after the first overrideChunks chunks are flushed
        long lengthOverride = (long) (overrideChunks - 1) * CHUNK_SIZE + maxBytesInPage;
        long chunks = file.length() / CHUNK_SIZE;

        long missesBefore = misses();
        try (FileHandle fh = handleBuilder(ChunkCache.instance).withLengthOverride(lengthOverride).complete();
             RandomAccessReader reader = fh.createReader())
        {
            assertEquals(lengthOverride, reader.length());
            readAndVerifyUpTo(reader, lengthOverride);
        }
        assertEquals(overrideChunks, ChunkCache.instance.sizeOfFile(file));
        assertEquals(overrideChunks, misses() - missesBefore);

        missesBefore = misses();
        try (FileHandle fh = handleBuilder(ChunkCache.instance).complete();
             RandomAccessReader reader = fh.createReader())
        {
            readAndVerifyAll(reader);
        }
        assertEquals(chunks, ChunkCache.instance.sizeOfFile(file));
        // only the chunks beyond the override were loaded
        assertEquals(chunks - overrideChunks, misses() - missesBefore);
    }

    /**
     * A chunk failing its checksum must surface as a CorruptSSTableException (so that the disk failure policy applies)
     * whose cause is the CorruptBlockException, also when read through the chunk cache, and must not be left cached.
     */
    @Test
    public void testCorruptChunkThroughCache() throws IOException
    {
        int corruptChunk = 2;
        long corruptPosition = (long) corruptChunk * CHUNK_SIZE + 100;
        byte original = readFileByte(corruptPosition);
        writeFileByte(corruptPosition, (byte) (original ^ 0x5A));

        try (FileHandle fh = handleBuilder(ChunkCache.instance).complete())
        {
            try (RandomAccessReader reader = fh.createReader())
            {
                reader.seek(0);
                assertEquals(writtenValues[0], reader.readByte());
                assertEquals(1, ChunkCache.instance.sizeOfFile(file));

                CorruptSSTableException e = assertThrows(CorruptSSTableException.class, () -> {
                    reader.seek((long) corruptChunk * CHUNK_SIZE);
                    reader.readByte();
                });
                assertTrue("Unexpected cause " + e.getCause(), e.getCause() instanceof CorruptBlockException);
                // Like CompressedChunkReader.Standard, the standard reader also drops the file from the file system
                // caches, which makes ChunkCache forget the file id (handles already open keep using the old id,
                // so their cached chunks are still found by them but no longer counted by sizeOfFile).
                assertEquals(mmapped ? 1 : 0, ChunkCache.instance.sizeOfFile(file));
            }

            // repair the file; the failed chunk was not cached, so it is loaded again (the only miss besides the
            // chunks not read yet) and now succeeds
            writeFileByte(corruptPosition, original);
            long chunks = file.length() / CHUNK_SIZE;
            long missesBefore = misses();
            try (RandomAccessReader reader = fh.createReader())
            {
                readAndVerifyAll(reader);
            }
            assertEquals(chunks - 1, misses() - missesBefore);
            if (mmapped)
                assertEquals(chunks, ChunkCache.instance.sizeOfFile(file));
        }
    }

    private byte readFileByte(long position) throws IOException
    {
        try (FileChannel channel = FileChannel.open(file.toPath(), StandardOpenOption.READ))
        {
            ByteBuffer buf = ByteBuffer.allocate(1);
            assertEquals(1, channel.read(buf, position));
            return buf.get(0);
        }
    }

    private void writeFileByte(long position, byte value) throws IOException
    {
        try (FileChannel channel = FileChannel.open(file.toPath(), StandardOpenOption.WRITE))
        {
            assertEquals(1, channel.write(ByteBuffer.wrap(new byte[]{ value }), position));
            channel.force(true);
        }
    }

    @Test
    public void testNothingCachedWithoutChunkCache() throws IOException
    {
        try (FileHandle fh = handleBuilder(null).complete();
             RandomAccessReader reader = fh.createReader())
        {
            assertTrue(fh.rebuffererFactory() instanceof EncryptedChunkReader);
            readAndVerifyAll(reader);
        }
        assertEquals(0, ChunkCache.instance.sizeOfFile(file));
    }

    @Test
    public void testPositionsAcrossChunkEndsMatchUncachedReader() throws IOException
    {
        try (FileHandle cachedHandle = handleBuilder(ChunkCache.instance).complete();
             FileHandle plainHandle = handleBuilder(null).complete();
             RandomAccessReader cached = cachedHandle.createReader();
             RandomAccessReader plain = plainHandle.createReader())
        {
            assertFalse("Expected the encrypted reader to be wrapped by the chunk cache, got " + cachedHandle.rebuffererFactory(),
                        cachedHandle.rebuffererFactory() instanceof EncryptedChunkReader);
            assertTrue(plainHandle.rebuffererFactory() instanceof EncryptedChunkReader);
            assertEquals(plain.length(), cached.length());

            for (int readSize : new int[]{ 1, 7, 33, maxBytesInPage - 1, maxBytesInPage, maxBytesInPage + 50, 2 * maxBytesInPage + 3 })
            {
                for (int chunk = 0; chunk < CHUNKS_TO_WRITE - 1; ++chunk)
                {
                    long chunkEnd = (long) chunk * CHUNK_SIZE + maxBytesInPage;
                    for (long seekTarget = chunkEnd - 40; seekTarget <= chunkEnd + 3; ++seekTarget)
                    {
                        // positions from chunkEnd on are in the unusable tail of the chunk; the readers move them by
                        // the size of that tail, i.e. chunkEnd maps to the start of the next chunk
                        long effectivePosition = seekTarget < chunkEnd ? seekTarget : seekTarget - maxBytesInPage + CHUNK_SIZE;
                        int index = Arrays.binarySearch(writtenPositions, effectivePosition);
                        assertTrue(String.format("%x maps to %x, which was not written", seekTarget, effectivePosition), index >= 0);
                        // Stay within the written data: the reader's length() is imprecise for a partially-filled
                        // last chunk (see EncryptedSequentialWriter), so skipping past its content is not supported.
                        if (index + readSize >= writtenPositions.length)
                            continue;
                        String context = String.format("seek to %x, read/skip %d bytes", seekTarget, readSize);

                        byte[] expected = new byte[readSize];
                        byte[] actual = new byte[readSize];
                        plain.seek(seekTarget);
                        cached.seek(seekTarget);
                        int expectedRead = plain.read(expected, 0, readSize);
                        int actualRead = cached.read(actual, 0, readSize);
                        assertEquals("Bytes read " + context, expectedRead, actualRead);
                        assertArrayEquals("Content " + context, expected, actual);
                        for (int j = 0; j < readSize; ++j)
                            assertEquals("Byte " + j + ' ' + context, writtenValues[index + j], actual[j]);
                        assertEquals("Position after read " + context, plain.getFilePointer(), cached.getFilePointer());
                        assertEquals("Next byte after read " + context, nextByte(plain), nextByte(cached));

                        plain.seek(seekTarget);
                        cached.seek(seekTarget);
                        int expectedSkipped = plain.skipBytes(readSize);
                        int actualSkipped = cached.skipBytes(readSize);
                        assertEquals("Bytes skipped " + context, expectedSkipped, actualSkipped);
                        assertEquals("Position after skip " + context, plain.getFilePointer(), cached.getFilePointer());
                        assertEquals("Next byte after skip " + context, nextByte(plain), nextByte(cached));
                    }
                }
            }

            // verify the content through the cached reader against the written pattern as well
            readAndVerifyAll(cached);
        }
        assertEquals(file.length() / CHUNK_SIZE, ChunkCache.instance.sizeOfFile(file));
    }

    private static int nextByte(RandomAccessReader reader) throws IOException
    {
        try
        {
            return reader.readByte() & 0xFF;
        }
        catch (EOFException e)
        {
            return Integer.MAX_VALUE;
        }
    }
}
