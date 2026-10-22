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
import java.nio.ByteOrder;
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
import org.junit.Assume;
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
import org.apache.cassandra.io.compress.ShortPageEncryptor;
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
    private static final int LAST_CHUNK_UNWRITTEN_BYTES = 17; // the last chunk is partially filled

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
        return encryptionParams(keyProviderFactory, outOfPlace ? OutOfPlaceEncryptor.class : Encryptor.class);
    }

    private static CompressionParams encryptionParams(Class<? extends IKeyProviderFactory> keyProviderFactory,
                                                      Class<? extends Encryptor> encryptorClass)
    {
        Map<String, String> opts = new HashMap<>();
        opts.put(EncryptionConfig.KEY_PROVIDER, keyProviderFactory.getName());
        opts.put(EncryptionConfig.CIPHER_ALGORITHM, "AES/CBC/PKCS5Padding");
        opts.put(EncryptionConfig.SECRET_KEY_STRENGTH, Integer.toString(128));
        opts.put(CompressionParams.CLASS, encryptorClass.getName());
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

        int bytesToWrite = maxBytesInPage * CHUNKS_TO_WRITE - LAST_CHUNK_UNWRITTEN_BYTES;
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
        return handleBuilder(file, chunkCache, metadata);
    }

    private FileHandle.Builder handleBuilder(File file, ChunkCache chunkCache, CompressionMetadata metadata)
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
     * The last chunk is not read when the handle is built, with or without a length override (without one, the length
     * is the usable end of the last chunk, see EncryptedChunkReader.defaultLength): a corrupted last chunk (here
     * failing its checksum) is only reported when it is read, as a CorruptSSTableException caused by a
     * CorruptBlockException.
     */
    @Test
    public void testCorruptLastChunkFailsOnRead() throws IOException
    {
        int lastChunk = CHUNKS_TO_WRITE - 1;
        long corruptPosition = (long) lastChunk * CHUNK_SIZE + 100;
        writeFileByte(corruptPosition, (byte) (readFileByte(corruptPosition) ^ 0x5A));

        for (ChunkCache chunkCache : new ChunkCache[]{ ChunkCache.instance, null })
        {
            try (FileHandle fh = handleBuilder(chunkCache).complete())
            {
                assertEquals(file.length() - CHUNK_SIZE + maxBytesInPage, fh.dataLength());
                assertCorruptChunk(fh, lastChunk);
            }

            try (FileHandle fh = handleBuilder(chunkCache).withLengthOverride(dataEnd()).complete())
            {
                assertCorruptChunk(fh, lastChunk);
            }
        }
    }

    /**
     * A chunk that passes its checksum and decrypts, but holds more content than a chunk can (simulated by reading with
     * an encryptor that reports less room in a chunk), is reported as corrupted when it is read, also the last chunk
     * (which building the handle does not read).
     */
    @Test
    public void testChunkWithTooMuchContentIsCorrupt() throws IOException
    {
        Assume.assumeFalse("ShortPageEncryptor decrypts in place: the out-of-place runs would repeat the others", outOfPlace);
        CompressionParams shortPageParams = encryptionParams(EncryptorTest.KeyProviderFactoryStub.class, ShortPageEncryptor.class);
        int shortMaxBytesInPage = EncryptedSequentialWriter.maxBytesInPage(shortPageParams.getSstableCompressor().encryptionOnly());
        assertEquals(maxBytesInPage - ShortPageEncryptor.SHORTER_BY, shortMaxBytesInPage);
        // the last chunk holds too much content too
        assertTrue(maxBytesInPage - LAST_CHUNK_UNWRITTEN_BYTES > shortMaxBytesInPage);

        try (CompressionMetadata shortPageMetadata = CompressionMetadata.encryptedOnly(shortPageParams))
        {
            for (ChunkCache chunkCache : new ChunkCache[]{ ChunkCache.instance, null })
            {
                try (FileHandle fh = handleBuilder(chunkCache, shortPageMetadata).complete())
                {
                    assertEquals(file.length() - CHUNK_SIZE + shortMaxBytesInPage, fh.dataLength());
                    assertCorruptChunk(fh, 1);
                    assertCorruptChunk(fh, CHUNKS_TO_WRITE - 1);
                }

                try (FileHandle fh = handleBuilder(chunkCache, shortPageMetadata).withLengthOverride(3L * CHUNK_SIZE).complete())
                {
                    assertCorruptChunk(fh, 1);
                }
            }
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

    /**
     * @return the position after the last byte written, in the padded last chunk
     */
    private long dataEnd()
    {
        return writtenPositions[writtenPositions.length - 1] + 1;
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
                        // Stay within the written data: the reader's length() is only an upper bound for a padded
                        // last chunk (see testPaddingBeforeLength), so reading or skipping past its content fails.
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
                        long positionAfterRead = plain.getFilePointer();
                        assertEquals("Position after read " + context, positionAfterRead, cached.getFilePointer());
                        int nextByteAfterRead = nextByte(plain);
                        assertEquals("Next byte after read " + context, nextByteAfterRead, nextByte(cached));

                        plain.seek(seekTarget);
                        cached.seek(seekTarget);
                        assertEquals("Bytes skipped " + context, readSize, plain.skipBytes(readSize));
                        assertEquals("Bytes skipped " + context, readSize, cached.skipBytes(readSize));
                        // a skip must leave the position a read of the same bytes leaves, also when the bytes end at
                        // the usable end of a chunk
                        assertEquals("Position after skip " + context, positionAfterRead, plain.getFilePointer());
                        assertEquals("Position after skip " + context, positionAfterRead, cached.getFilePointer());
                        assertEquals("Next byte after skip " + context, nextByteAfterRead, nextByte(plain));
                        assertEquals("Next byte after skip " + context, nextByteAfterRead, nextByte(cached));
                    }
                }
            }

            // verify the content through the cached reader against the written pattern as well
            readAndVerifyAll(cached);
        }
        assertEquals(file.length() / CHUNK_SIZE, ChunkCache.instance.sizeOfFile(file));
    }

    /**
     * Without a length override, length() is the usable end of the last chunk (the start of its hole): seeking there
     * must give a clean EOF rather than an attempt to read a chunk at the physical end of the file.
     */
    @Test
    public void testSeekToLengthIsEOF() throws IOException
    {
        for (ChunkCache chunkCache : new ChunkCache[]{ ChunkCache.instance, null })
        {
            try (FileHandle fh = handleBuilder(chunkCache).complete();
                 RandomAccessReader reader = fh.createReader())
            {
                long length = reader.length();
                assertEquals(file.length() - (CHUNK_SIZE - maxBytesInPage), length);
                assertEquals(length, fh.dataLength());

                reader.seek(length);
                assertEquals(length, reader.getFilePointer());
                assertTrue(reader.isEOF());
                assertEquals(0, reader.bytesRemaining());
                assertThrows(EOFException.class, reader::readByte);
                assertEquals(0, reader.skipBytes(1));
                assertEquals(length, reader.getFilePointer());
            }
        }
    }

    /**
     * Skips leaving the current buffer must stop at length(), also when it is a length override in the middle of a
     * chunk (early-open readers), and must report the number of bytes skipped not counting the holes crossed.
     */
    @Test
    public void testSkipStopsAtLength() throws IOException
    {
        long dataEnd = dataEnd();
        long lastChunkStart = dataEnd - (dataEnd & (CHUNK_SIZE - 1));
        long lengthOverride = dataEnd - 100;
        assertTrue(lengthOverride > lastChunkStart);

        for (ChunkCache chunkCache : new ChunkCache[]{ ChunkCache.instance, null })
        {
            try (FileHandle fh = handleBuilder(chunkCache).withLengthOverride(lengthOverride).complete();
                 RandomAccessReader reader = fh.createReader())
            {
                assertEquals(lengthOverride, reader.length());

                // across holes: 10 bytes before the hole of the previous chunk, then the data of the last chunk
                long start = lastChunkStart - CHUNK_SIZE + maxBytesInPage - 10;
                int expectedSkipped = (int) (10 + lengthOverride - lastChunkStart);
                reader.seek(start);
                assertEquals(expectedSkipped, reader.skipBytes(3 * maxBytesInPage));
                assertEquals(lengthOverride, reader.getFilePointer());
                assertTrue(reader.isEOF());
                assertEquals(0, reader.bytesRemaining());
                assertEquals(0, reader.skipBytes(1));

                reader.seek(start);
                assertEquals(expectedSkipped, reader.skipBytes(expectedSkipped));
                assertEquals(lengthOverride, reader.getFilePointer());

                reader.seek(start);
                assertThrows(EOFException.class, () -> reader.skipBytesFully(expectedSkipped + 1));
                assertEquals(lengthOverride, reader.getFilePointer());
            }
        }
    }

    /**
     * With a length override in the middle of a chunk, the chunk is read (and cached) in full, but the reader stops at
     * length(): reads and skips end there, and seeking past it is an error.
     */
    @Test
    public void testReadsStopAtMidChunkLengthOverride() throws IOException
    {
        long lengthOverride = 3L * CHUNK_SIZE + 100;
        for (ChunkCache chunkCache : new ChunkCache[]{ ChunkCache.instance, null })
        {
            try (FileHandle fh = handleBuilder(chunkCache).withLengthOverride(lengthOverride).complete();
                 RandomAccessReader reader = fh.createReader())
            {
                reader.seek(lengthOverride - 10);
                assertEquals(10, reader.bytesRemaining());
                assertEquals(10, reader.read(new byte[20], 0, 20));
                assertEquals(lengthOverride, reader.getFilePointer());
                assertTrue(reader.isEOF());
                assertEquals(0, reader.bytesRemaining());
                assertEquals(0, reader.available());
                assertEquals(-1, reader.read());

                reader.seek(lengthOverride - 10);
                assertThrows(EOFException.class, () -> reader.readFully(new byte[11]));

                reader.seek(lengthOverride - 10);
                assertEquals(10, reader.skipBytes(20));
                assertEquals(lengthOverride, reader.getFilePointer());

                reader.seek(lengthOverride - 10);
                assertThrows(IllegalArgumentException.class, () -> reader.seek(lengthOverride + 1));
            }
        }
    }

    /**
     * Handles of the same file with different lengths (e.g. early-open handles) share the cached chunks, but each
     * reader stops at its own length: a chunk cached by a longer handle is cut at a shorter handle's length in the
     * middle of it, and a chunk cached by a shorter handle is read in full by a longer one.
     */
    @Test
    public void testHandlesWithDifferentLengthsShareChunks() throws IOException
    {
        long shortLength = 3L * CHUNK_SIZE + 100;
        for (boolean shortFirst : new boolean[]{ true, false })
        {
            ChunkCache.instance.invalidateFileNow(file);
            long missesBefore = misses();
            // the long length is the end of the data, as recorded by the writer for the handles it opens early
            try (FileHandle shortHandle = handleBuilder(ChunkCache.instance).withLengthOverride(shortLength).complete();
                 FileHandle longHandle = handleBuilder(ChunkCache.instance).withLengthOverride(dataEnd()).complete();
                 RandomAccessReader shortReader = shortHandle.createReader();
                 RandomAccessReader longReader = longHandle.createReader())
            {
                assertEquals(shortLength, shortReader.length());
                assertEquals(dataEnd(), longReader.length());
                for (RandomAccessReader reader : shortFirst ? new RandomAccessReader[]{ shortReader, longReader }
                                                            : new RandomAccessReader[]{ longReader, shortReader })
                {
                    long length = reader.length();
                    String context = "length " + length + (shortFirst ? ", short first" : ", long first");
                    reader.seek(0);
                    assertEquals(context, writtenIndex(length), reader.bytesRemaining());
                    readAndVerifyUpTo(reader, length);
                    assertEquals(context, length, reader.getFilePointer());
                    assertTrue(context, reader.isEOF());
                    assertEquals(context, -1, reader.read());
                }
            }
            // every chunk was loaded once, by the reader that needed it first
            assertEquals(CHUNKS_TO_WRITE, misses() - missesBefore);
        }
    }

    /**
     * A skip whose last byte would be the first byte of the chunk at a length override (the previous chunk being full)
     * stops one byte short, where reading the bytes before the override leaves the pointer: at the start of the
     * previous chunk's hole, from which the next read gives EOF.
     */
    @Test
    public void testSkipOverHoleToChunkStartLength() throws IOException
    {
        long lengthOverride = 4L * CHUNK_SIZE;
        long holeStart = lengthOverride - CHUNK_SIZE + maxBytesInPage;
        long start = holeStart - 10;
        for (ChunkCache chunkCache : new ChunkCache[]{ ChunkCache.instance, null })
        {
            try (FileHandle fh = handleBuilder(chunkCache).withLengthOverride(lengthOverride).complete();
                 RandomAccessReader reader = fh.createReader())
            {
                reader.seek(start);
                reader.readFully(new byte[10]);
                assertEquals(holeStart, reader.getFilePointer());
                assertTrue(reader.isEOF());
                assertEquals(0, reader.bytesRemaining());
                assertThrows(EOFException.class, reader::readByte);

                reader.seek(start);
                assertEquals(10, reader.skipBytes(11));
                assertEquals(holeStart, reader.getFilePointer());
                assertEquals(0, reader.skipBytes(1));
                assertThrows(EOFException.class, reader::readByte);

                reader.seek(start);
                assertThrows(EOFException.class, () -> reader.skipBytesFully(11));
                assertEquals(holeStart, reader.getFilePointer());

                reader.seek(start);
                assertEquals(10, reader.skipBytes(10));
                assertEquals(holeStart, reader.getFilePointer());
            }
        }
    }

    /**
     * A chunk padded before length() has less content than its usable size, which only decrypting the chunk reveals:
     * a chunk in the middle of the file (see EncryptedSequentialWriter.padToPageBoundary, used by the page-aware trie
     * writers), or the last chunk of a file opened without a length override, whose length is the usable end of the
     * last chunk. Both behave the same: bytesRemaining() and isEOF() count the padding, reads stop at the end of the
     * content and report EOF (an EOFException) without moving on to the next chunk or to length(), and seeking or
     * skipping into the padding is an IllegalArgumentException. Seeking or skipping to length() after a padded last
     * chunk gives a clean EOF; with a length override at the end of the content, the last chunk reads to EOF.
     */
    @Test
    public void testPaddingBeforeLength() throws IOException
    {
        File padded = FileUtils.createTempFile("encrypted-padded", ".db");
        try
        {
            try (EncryptedSequentialWriter writer = new EncryptedSequentialWriter(padded,
                                                                                  SequentialWriterOption.newBuilder().finishOnClose(false).build(),
                                                                                  compressionParams.getSstableCompressor().encryptionOnly()))
            {
                for (int i = 0; i < 100; ++i)
                    writer.writeByte(i);
                writer.padToPageBoundary();
                assertEquals(CHUNK_SIZE, writer.position());
                for (int i = 0; i < 100; ++i)
                    writer.writeByte(100 + i);
                writer.finish();
            }

            for (ChunkCache chunkCache : new ChunkCache[]{ ChunkCache.instance, null })
            {
                try (FileHandle fh = handleBuilder(padded, chunkCache, compressionMetadata).complete();
                     RandomAccessReader reader = fh.createReader())
                {
                    long length = reader.length();
                    assertEquals(CHUNK_SIZE + maxBytesInPage, length);
                    assertEquals(2 * maxBytesInPage, reader.bytesRemaining());

                    // the padded chunk in the middle of the file, then the padded last chunk
                    assertPaddedChunk(reader, 0, 0, maxBytesInPage, padded);
                    assertPaddedChunk(reader, CHUNK_SIZE, 100, 0, padded);

                    // a skip to length() counts the padding as skipped, like a seek there
                    reader.seek(CHUNK_SIZE + 90);
                    assertEquals(maxBytesInPage - 90, reader.skipBytes(maxBytesInPage));
                    assertEquals(length, reader.getFilePointer());
                    assertTrue(reader.isEOF());
                    assertEquals(-1, reader.read());
                    reader.seek(length);
                    assertTrue(reader.isEOF());
                    assertThrows(EOFException.class, reader::readByte);
                }

                // with the length the writer records for the handles it opens early, the end of the content
                try (FileHandle fh = handleBuilder(padded, chunkCache, compressionMetadata).withLengthOverride(CHUNK_SIZE + 100).complete();
                     RandomAccessReader reader = fh.createReader())
                {
                    assertEquals(maxBytesInPage + 100, reader.bytesRemaining());
                    assertPaddedChunk(reader, 0, 0, 100, padded);
                    reader.seek(CHUNK_SIZE);
                    for (int i = 0; i < 100; ++i)
                        assertEquals((byte) (100 + i), reader.readByte());
                    assertTrue(reader.isEOF());
                    assertEquals(-1, reader.read());
                }
            }
        }
        finally
        {
            ChunkCache.instance.invalidateFile(padded);
            padded.tryDelete();
        }
    }

    /**
     * Checks a chunk of the {@code padded} file starting at {@code chunkStart} whose content is 100 bytes valued from
     * {@code firstValue}, followed by padding up to its usable end, and then by {@code remainingAfter} bytes before
     * length() (see testPaddingBeforeLength).
     */
    private void assertPaddedChunk(RandomAccessReader reader, long chunkStart, int firstValue, long remainingAfter,
                                   File padded) throws IOException
    {
        int content = 100;
        long contentEnd = chunkStart + content;
        String context = "chunk at " + chunkStart;

        reader.seek(chunkStart + 50);
        assertEquals(context, maxBytesInPage - 50 + remainingAfter, reader.bytesRemaining());
        byte[] buf = new byte[60];
        assertEquals(context, 50, reader.read(buf, 0, 60));
        for (int i = 0; i < 50; ++i)
            assertEquals(context, (byte) (firstValue + 50 + i), buf[i]);
        assertEquals(context, contentEnd, reader.getFilePointer());
        // the padding is counted, but cannot be read
        assertEquals(context, maxBytesInPage - content + remainingAfter, reader.bytesRemaining());
        assertFalse(context, reader.isEOF());
        assertEquals(context, -1, reader.read());
        assertThrows(context, EOFException.class, reader::readByte);
        assertEquals(context, contentEnd, reader.getFilePointer());
        reader.seek(chunkStart + 50);
        assertThrows(context, EOFException.class, () -> reader.readFully(new byte[60]));

        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> reader.seek(contentEnd + 50));
        assertTrue(e.getMessage(), e.getMessage().contains(padded.path()) && e.getMessage().contains("past the end of the data"));
        reader.seek(chunkStart + 50);
        assertThrows(context, IllegalArgumentException.class, () -> reader.skipBytes(60));
    }

    /**
     * The writer pads a chunk after encrypting it, so decrypting a padded chunk fills only the start of the buffer it
     * is read into, and the rest of the buffer keeps what it held before: when decrypting from a mapped region (or out
     * of place), that is whatever a recycled pooled buffer held, e.g. another chunk. These bytes cannot be read,
     * because the limit of the buffer is the decrypted length: check that the chunk reader sets it, that the
     * rebufferer reusing its buffer, the chunk cache (whose buffers are recycled through its pool), a wrapping
     * rebufferer and the reader keep it, and that absolute reads past it (as made by the trie walkers) fail.
     */
    @Test
    public void testPaddedChunkLimitHidesStaleBytes() throws IOException
    {
        int content = 100; // in the last chunk
        long lastChunk = CHUNK_SIZE;
        File padded = FileUtils.createTempFile("encrypted-padded", ".db");
        try
        {
            try (EncryptedSequentialWriter writer = new EncryptedSequentialWriter(padded,
                                                                                  SequentialWriterOption.newBuilder().finishOnClose(false).build(),
                                                                                  compressionParams.getSstableCompressor().encryptionOnly()))
            {
                for (int i = 0; i < maxBytesInPage + content; ++i)
                    writer.writeByte(paddedFileByte(i));
                writer.finish();
            }
            assertEquals(2 * CHUNK_SIZE, padded.length());

            try (FileHandle fh = handleBuilder(padded, null, compressionMetadata).complete())
            {
                // the chunk reader, into a buffer full of other bytes
                ChunkReader chunkReader = (ChunkReader) fh.rebuffererFactory();
                ByteBuffer buffer = chunkReader.preferredBufferType().allocate(CHUNK_SIZE);
                while (buffer.hasRemaining())
                    buffer.put((byte) 0xA5);
                chunkReader.readChunk(lastChunk, buffer);
                assertEquals(0, buffer.position());
                assertPaddedChunkLimit(buffer, content, "chunk reader");

                // the rebufferer used without the chunk cache, which reads every chunk into the same buffer
                Rebufferer rebufferer = fh.rebuffererFactory().instantiateRebufferer(false);
                try
                {
                    Rebufferer.BufferHolder holder = rebufferer.rebuffer(0);
                    assertEquals(maxBytesInPage, holder.buffer().limit());
                    holder.release();
                    assertPaddedChunkLimit(rebufferer.rebuffer(lastChunk + 10), lastChunk, content, "rebufferer");
                }
                finally
                {
                    rebufferer.closeReader();
                }

                // a wrapping rebufferer
                Rebufferer tailOverriding = new TailOverridingRebufferer(fh.rebuffererFactory().instantiateRebufferer(false),
                                                                         2 * CHUNK_SIZE, ByteBuffer.allocate(10));
                try
                {
                    assertPaddedChunkLimit(tailOverriding.rebuffer(lastChunk), lastChunk, content, "wrapping rebufferer");
                }
                finally
                {
                    tailOverriding.closeReader();
                }
            }

            // the chunk cache, after releasing a chunk to its pool so that the padded chunk is likely to reuse its buffer
            try (FileHandle fh = handleBuilder(padded, ChunkCache.instance, compressionMetadata).complete())
            {
                Rebufferer rebufferer = fh.rebuffererFactory().instantiateRebufferer(false);
                Rebufferer.BufferHolder holder = rebufferer.rebuffer(0);
                assertEquals(maxBytesInPage, holder.buffer().limit());
                holder.release();
                ChunkCache.instance.invalidateFileNow(padded);
                long missesBefore = misses();
                assertPaddedChunkLimit(rebufferer.rebuffer(lastChunk), lastChunk, content, "chunk cache");
                assertEquals(1, misses() - missesBefore);
                // served from the cache
                assertPaddedChunkLimit(rebufferer.rebuffer(lastChunk + 10), lastChunk, content, "cached chunk");
                assertEquals(1, misses() - missesBefore);
            }

            // the reader, without a length override (the usable end of the chunk, which allows positions in the
            // padding) and with one at the end of the data
            for (ChunkCache chunkCache : new ChunkCache[]{ ChunkCache.instance, null })
            {
                for (long lengthOverride : new long[]{ -1, lastChunk + content })
                {
                    FileHandle.Builder builder = handleBuilder(padded, chunkCache, compressionMetadata);
                    if (lengthOverride >= 0)
                        builder.withLengthOverride(lengthOverride);
                    try (FileHandle fh = builder.complete();
                         RandomAccessReader reader = fh.createReader())
                    {
                        String context = "reader, length " + reader.length() + (chunkCache == null ? "" : ", cached");
                        reader.seek(lastChunk + content - 10);
                        byte[] read = new byte[20];
                        assertEquals(context, 10, reader.read(read, 0, read.length));
                        for (int i = 0; i < 10; ++i)
                            assertEquals(context, paddedFileByte(maxBytesInPage + content - 10 + i), read[i]);
                        assertEquals(context, -1, reader.read());
                        assertThrows(context, IllegalArgumentException.class, () -> reader.seek(lastChunk + content + 1));
                    }
                }
            }
        }
        finally
        {
            ChunkCache.instance.invalidateFile(padded);
            padded.tryDelete();
        }
    }

    /**
     * The byte written at the given index of the file of testPaddedChunkLimitHidesStaleBytes, never 0.
     */
    private static byte paddedFileByte(int index)
    {
        return (byte) (0x80 | index);
    }

    private void assertPaddedChunkLimit(Rebufferer.BufferHolder holder, long chunkStart, int content, String context)
    {
        try
        {
            assertEquals(context, chunkStart, holder.offset());
            assertPaddedChunkLimit(holder.buffer(), content, context);
        }
        finally
        {
            holder.release();
        }
    }

    private void assertPaddedChunkLimit(ByteBuffer buffer, int content, String context)
    {
        assertEquals(context, content, buffer.limit());
        for (int i = 0; i < content; ++i)
            assertEquals(context, paddedFileByte(maxBytesInPage + i), buffer.get(i));
        assertThrows(context, IndexOutOfBoundsException.class, () -> buffer.get(content));
        assertThrows(context, IndexOutOfBoundsException.class, () -> buffer.getInt(content - 3));
        assertEquals(context, content, buffer.duplicate().limit());
        assertEquals(context, content, buffer.slice().capacity());
    }

    /**
     * A failed read (here of a corrupted chunk) releases the reader's buffer, whose memory may then be reused (by the
     * chunk cache, or by the failed read itself: the standard reader decrypts in place, into the buffer that the
     * rebufferer used without the chunk cache reuses): the reader must not read from it afterwards, e.g. after a seek
     * back into its range. The reader stays at the position it failed to load, and a read there fails again rather
     * than returning other data.
     * <p>
     * Reading the released buffer only returns wrong data deterministically with the parameters mmapped=false and
     * outOfPlace=false, without the chunk cache: the failed in-place read has overwritten that buffer with the
     * ciphertext of the corrupted chunk. In the other cases the released buffer still holds the previous chunk (the
     * mmapped and out-of-place readers fail on the checksum before writing to the buffer, and the cached chunk is
     * not reused this soon).
     */
    @Test
    public void testFailedReadDoesNotExposeReleasedBuffer() throws IOException
    {
        int corruptChunk = 2;
        long corruptPosition = (long) corruptChunk * CHUNK_SIZE + 100;
        writeFileByte(corruptPosition, (byte) (readFileByte(corruptPosition) ^ 0x5A));

        for (ChunkCache chunkCache : new ChunkCache[]{ ChunkCache.instance, null })
        {
            try (FileHandle fh = handleBuilder(chunkCache).complete();
                 RandomAccessReader reader = fh.createReader())
            {
                String context = chunkCache == null ? "uncached" : "cached";
                reader.seek(100);
                assertEquals(context, writtenValues[100], reader.readByte());
                long corruptChunkStart = (long) corruptChunk * CHUNK_SIZE;
                assertThrows(context, CorruptSSTableException.class, () -> reader.seek(corruptChunkStart));
                assertEquals(context, corruptChunkStart, reader.getFilePointer());
                assertThrows(context, CorruptSSTableException.class, reader::readByte);
                assertEquals(context, corruptChunkStart, reader.getFilePointer());

                reader.seek(200);
                byte[] read = new byte[100];
                reader.readFully(read);
                assertArrayEquals(context, Arrays.copyOfRange(writtenValues, 200, 300), read);
            }
        }
    }

    /**
     * A skip ending at the start of a hole (the usable end of a full chunk) loads the chunk ending there, like a read
     * of its last byte, and the next read loads the next chunk; a skip ending at a length() that is the start of a hole
     * loads nothing, and neither does the EOF that follows.
     */
    @Test
    public void testSkipToHoleStartLoadsChunksLikeRead() throws IOException
    {
        long lengthOverride = 3L * CHUNK_SIZE + maxBytesInPage;
        long start = CHUNK_SIZE + maxBytesInPage - 10;
        long holeStart = 2L * CHUNK_SIZE + maxBytesInPage;
        for (ChunkCache chunkCache : new ChunkCache[]{ ChunkCache.instance, null })
        {
            try (FileHandle fh = handleBuilder(chunkCache).withLengthOverride(lengthOverride).complete())
            {
                RandomAccessReaderTest.CountingRebufferer rebufferer = new RandomAccessReaderTest.CountingRebufferer(fh.rebuffererFactory().instantiateRebufferer(false));
                try (RandomAccessReader reader = new RandomAccessReader(rebufferer, ByteOrder.BIG_ENDIAN, Rebufferer.EMPTY))
                {
                    reader.seek(start);
                    rebufferer.rebuffers = 0;
                    assertEquals(10 + maxBytesInPage, reader.skipBytes(10 + maxBytesInPage));
                    assertEquals(holeStart, reader.getFilePointer());
                    assertEquals(maxBytesInPage, reader.bytesRemaining());
                    assertEquals(1, rebufferer.rebuffers);
                    assertEquals(writtenValues[writtenIndex(holeStart)], reader.readByte());
                    assertEquals(3L * CHUNK_SIZE + 1, reader.getFilePointer());
                    assertEquals(2, rebufferer.rebuffers);

                    reader.seek(start);
                    rebufferer.rebuffers = 0;
                    assertEquals(10 + 2 * maxBytesInPage, reader.skipBytes(10 + 2 * maxBytesInPage));
                    assertEquals(lengthOverride, reader.getFilePointer());
                    assertTrue(reader.isEOF());
                    assertThrows(EOFException.class, reader::readByte);
                    assertEquals(0, rebufferer.rebuffers);
                }
            }
        }
    }

    /**
     * isEOF(), bytesRemaining() and available() count the bytes that can be read up to length(), not the holes: check
     * them, and that the bytes counted can be read and are followed by EOF, at the start, in the middle of a chunk, at
     * a hole start (reached by a read), at the start of the last chunk and at length(), for lengths at a chunk start,
     * at a hole start and in the middle of a chunk, and without a length override. Without a length override length()
     * is the usable end of the padded last chunk, and the padding is counted although it cannot be read: reads stop at
     * the end of the data, as in any chunk padded before length() (see testPaddingBeforeLength).
     */
    @Test
    public void testBytesRemainingMatchesReadableBytes() throws IOException
    {
        for (long lengthOverride : new long[]{ 4L * CHUNK_SIZE, 3L * CHUNK_SIZE + maxBytesInPage, 3L * CHUNK_SIZE + 100, -1 })
        {
            for (ChunkCache chunkCache : new ChunkCache[]{ ChunkCache.instance, null })
            {
                FileHandle.Builder builder = handleBuilder(chunkCache);
                if (lengthOverride >= 0)
                    builder.withLengthOverride(lengthOverride);
                try (FileHandle fh = builder.complete();
                     RandomAccessReader reader = fh.createReader())
                {
                    long length = reader.length();
                    long padding = lengthOverride >= 0 ? 0 : length - dataEnd();
                    if (lengthOverride < 0)
                        assertEquals(LAST_CHUNK_UNWRITTEN_BYTES, padding);
                    long lastByte = length - 1;
                    long lastChunkStart = lastByte - (lastByte & (CHUNK_SIZE - 1));
                    String context = "length " + length + (chunkCache == null ? "" : ", cached");

                    for (long position : new long[]{ 0, 100, CHUNK_SIZE + 100, lastChunkStart, lastChunkStart + 50 })
                    {
                        reader.seek(position);
                        assertRemainingIsReadable(reader, length - padding, padding, context + ", seek to " + position);
                    }

                    // at the hole starts up to the end of the data (including one just before a chunk-start length,
                    // and a hole-start length), where reading the last bytes of a chunk leaves the pointer
                    for (long holeStart = maxBytesInPage; holeStart <= length - padding; holeStart += CHUNK_SIZE)
                    {
                        reader.seek(holeStart - 5);
                        reader.readFully(new byte[5]);
                        assertEquals(holeStart, reader.getFilePointer());
                        assertRemainingIsReadable(reader, length - padding, padding, context + ", read to " + holeStart);
                    }

                    reader.seek(length);
                    assertRemainingIsReadable(reader, length, 0, context + ", seek to length");
                }
            }
        }
    }

    /**
     * Checks isEOF(), bytesRemaining() and available() at the reader's position against the number of bytes written
     * between it and {@code readableEnd}, plus the given unreadable padding counted by bytesRemaining(); then checks
     * that these bytes can be read, and are followed by EOF.
     */
    private void assertRemainingIsReadable(RandomAccessReader reader, long readableEnd, long padding,
                                           String context) throws IOException
    {
        long position = reader.getFilePointer();
        context += String.format(" (position %x)", position);
        int readable = writtenIndex(readableEnd) - writtenIndex(position);
        assertEquals("Bytes remaining, " + context, readable + padding, reader.bytesRemaining());
        assertEquals("Available, " + context, readable + padding, reader.available());
        assertEquals("EOF, " + context, readable + padding == 0, reader.isEOF());

        reader.readFully(new byte[readable]);
        assertEquals("Bytes remaining after read, " + context, padding, reader.bytesRemaining());
        assertEquals("EOF after read, " + context, padding == 0, reader.isEOF());
        assertEquals("Next read, " + context, -1, reader.read());
        assertThrows("Next read, " + context, EOFException.class, reader::readByte);
    }

    /**
     * @return the index of the first byte written at or after the given position
     */
    private int writtenIndex(long position)
    {
        int index = Arrays.binarySearch(writtenPositions, position);
        return index >= 0 ? index : -index - 1;
    }

    private void truncateFile(long length) throws IOException
    {
        try (FileChannel channel = FileChannel.open(file.toPath(), StandardOpenOption.WRITE))
        {
            channel.truncate(length);
        }
        assertEquals(length, file.length());
        ChunkCache.instance.invalidateFile(file); // drop the chunks cached before the truncation
    }

    /**
     * Without a length override, a file ending with a full chunk (i.e. right after a hole, like every file written by
     * EncryptedSequentialWriter) has the usable end of its last chunk as length; an empty file has length 0.
     */
    @Test
    public void testDefaultLengthOfFileEndingAfterHole() throws IOException
    {
        truncateFile(2L * CHUNK_SIZE);
        for (ChunkCache chunkCache : new ChunkCache[]{ ChunkCache.instance, null })
        {
            try (FileHandle fh = handleBuilder(chunkCache).complete();
                 RandomAccessReader reader = fh.createReader())
            {
                long length = CHUNK_SIZE + maxBytesInPage;
                assertEquals(length, fh.dataLength());
                assertEquals(length, reader.length());
                readAndVerifyUpTo(reader, length);
                assertEquals(length, reader.getFilePointer());
                assertThrows(EOFException.class, reader::readByte);
            }
        }

        truncateFile(0);
        for (ChunkCache chunkCache : new ChunkCache[]{ ChunkCache.instance, null })
        {
            try (FileHandle fh = handleBuilder(chunkCache).complete();
                 RandomAccessReader reader = fh.createReader())
            {
                assertEquals(0, fh.dataLength());
                assertEquals(0, reader.length());
                assertTrue(reader.isEOF());
                assertThrows(EOFException.class, reader::readByte);
            }
        }
    }

    /**
     * Without a length override, a (truncated) file ending in the usable part of a chunk, or exactly at its hole start,
     * keeps its length, and reading the incomplete chunk reports it as corrupted.
     */
    @Test
    public void testDefaultLengthOfFileEndingInUsablePart() throws IOException
    {
        // decreasing lengths, as truncation cannot extend the file
        for (long fileLength : new long[]{ CHUNK_SIZE + maxBytesInPage, CHUNK_SIZE + 100, CHUNK_SIZE - maxBytesInPage - 1 })
        {
            truncateFile(fileLength);
            for (ChunkCache chunkCache : new ChunkCache[]{ ChunkCache.instance, null })
            {
                try (FileHandle fh = handleBuilder(chunkCache).complete())
                {
                    assertEquals(fileLength, fh.dataLength());
                    if (fileLength > CHUNK_SIZE)
                    {
                        try (RandomAccessReader reader = fh.createReader())
                        {
                            assertEquals(fileLength, reader.length());
                            readAndVerifyUpTo(reader, maxBytesInPage);
                        }
                    }
                    assertCorruptChunk(fh, (int) (fileLength / CHUNK_SIZE));
                }
            }
        }
    }

    /**
     * Without a length override, a (truncated) file ending inside the hole of a chunk has no valid length and is
     * reported as corrupted when opened.
     */
    @Test
    public void testDefaultLengthOfFileEndingInHoleIsCorrupt() throws IOException
    {
        // decreasing lengths, as truncation cannot extend the file
        for (long fileLength : new long[]{ 2L * CHUNK_SIZE - 1, CHUNK_SIZE + maxBytesInPage + 1 })
        {
            truncateFile(fileLength);
            for (ChunkCache chunkCache : new ChunkCache[]{ ChunkCache.instance, null })
            {
                CorruptSSTableException e = assertThrows(CorruptSSTableException.class, () -> handleBuilder(chunkCache).complete());
                assertTrue("Unexpected cause " + e.getCause(), e.getCause() instanceof CorruptBlockException);
            }
        }
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
