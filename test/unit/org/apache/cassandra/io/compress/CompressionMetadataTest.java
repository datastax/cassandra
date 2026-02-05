/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.io.compress;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.compress.CompressionMetadata.Chunk;
import org.apache.cassandra.io.sstable.format.SSTableReader.PartitionPositionBounds;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.SliceDescriptor;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.utils.units.SizeUnit;

import static java.util.Arrays.asList;
import static org.apache.cassandra.io.compress.CompressionChunkOffsetCache.getCacheSizeInBytes;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class CompressionMetadataTest
{
    @BeforeClass
    public static void init()
    {
        DatabaseDescriptor.toolInitialization();
    }

    @After
    public void tearDown()
    {
        CompressionChunkOffsetCache.resetCache();
        CassandraRelevantProperties.COMPRESSION_CHUNK_OFFSETS_BLOCK_CACHE_SIZE.reset();
        CassandraRelevantProperties.COMPRESSION_CHUNK_OFFSETS_FACTORY.reset();
    }

    private File generateMetaDataFile(long dataLength, long... offsets) throws IOException
    {
        Path path = Files.createTempFile("compression_metadata", ".db");
        CompressionParams params = CompressionParams.snappy(16);
        try (CompressionMetadata.Writer writer = CompressionMetadata.Writer.open(params, new File(path)))
        {
            for (long offset : offsets)
                writer.addOffset(offset);

            writer.finalizeLength(dataLength, offsets.length);
            writer.doPrepare();
            Throwable t = writer.doCommit(null);
            if (t != null)
                throw new IOException(t);
        }
        return new File(path);
    }

    private CompressionMetadata createMetadata(long dataLength, long compressedFileLength, long... offsets) throws IOException
    {
        File f = generateMetaDataFile(dataLength, offsets);
        return new CompressionMetadata(f, compressedFileLength, true);
    }

    private void checkMetadata(CompressionMetadata metadata, long expectedDataLength, long expectedCompressedFileLimit, long expectedMemorySize)
    {
        assertThat(metadata.dataLength).isEqualTo(expectedDataLength);
        assertThat(metadata.compressedFileLength).isEqualTo(expectedCompressedFileLimit);
        assertThat(metadata.chunkLength()).isEqualTo(16);
        assertThat(metadata.parameters.chunkLength()).isEqualTo(16);
        assertThat(metadata.parameters.getSstableCompressor().getClass()).isEqualTo(SnappyCompressor.class);

        // FIXME
//        // if it's configured to use in-memory
//        if (CompressionChunkOffsetsFactory.getInMemoryLimitInBytes(100000) > 0)
//            assertThat(metadata.offHeapSize()).isEqualTo(expectedMemorySize);
//        // otherwise on-disk
//        else
//            assertThat(metadata.offHeapSize()).isEqualTo(0);
    }

    private void assertChunks(CompressionMetadata metadata, long from, long to, long expectedOffset, long expectedLength)
    {
        for (long offset = from; offset < to; offset++)
        {
            Chunk chunk = metadata.chunkFor(offset);
            assertThat(chunk.offset).isEqualTo(expectedOffset);
            assertThat(chunk.length).isEqualTo(expectedLength);
        }
    }

    private void checkSlice(File f, long compressedSize, long from, long to, Consumer<CompressionMetadata> check)
    {
        try (CompressionMetadata md = new CompressionMetadata(f, compressedSize, true, new SliceDescriptor(from, to, 16), false))
        {
            check.accept(md);
        }
    }

    @Test
    public void testLargeFileAccessInMemory() throws IOException
    {
        CassandraRelevantProperties.COMPRESSION_CHUNK_OFFSETS_BLOCK_CACHE_SIZE.setString("0B");
        testLargeFileAccess();
    }

    @Test
    public void testLargeFileAccessBlockCache() throws IOException
    {
        CassandraRelevantProperties.COMPRESSION_CHUNK_OFFSETS_BLOCK_CACHE_SIZE.setString("100MiB");
        testLargeFileAccess();
    }

    private void testLargeFileAccess() throws IOException
    {
        int chunkCount = 1000_000;
        int chunkLength = 16;
        int checksumLength = 4; // payload size is 12 = chunkLength - checksumLength
        long[] offsets = new long[chunkCount];
        long offset = 0;
        for (int i = 0; i < chunkCount; i++)
        {
            offsets[i] = offset;
            offset += chunkLength;
        }

        File largeFile = generateMetaDataFile((long) chunkCount * chunkLength, offsets);
        long compressedFileLimit = offset;

        CassandraRelevantProperties.COMPRESSION_CHUNK_OFFSETS_BLOCK_CACHE_SIZE.setString("0B");
        CompressionMetadata metadataWithInMemory = new CompressionMetadata(largeFile, compressedFileLimit, true);
        checkMetadata(metadataWithInMemory, (long) chunkCount * chunkLength, compressedFileLimit, chunkCount * 8L);

        CassandraRelevantProperties.COMPRESSION_CHUNK_OFFSETS_BLOCK_CACHE_SIZE.setString("100MiB");
        CompressionMetadata metadataWithBlockCache = new CompressionMetadata(largeFile, compressedFileLimit, true);
        checkMetadata(metadataWithBlockCache, (long) chunkCount * chunkLength, compressedFileLimit, 0);
        try
        {
            for (int i = 0; i < chunkCount; )
            {
                long pos = (long) i * chunkLength;
                Chunk chunkWithInMemory = metadataWithInMemory.chunkFor(pos);
                assertThat(chunkWithInMemory.offset).isEqualTo((long) i * chunkLength);
                assertThat(chunkWithInMemory.length).isEqualTo(chunkLength - checksumLength);

                Chunk chunkWithBlockCache = metadataWithBlockCache.chunkFor(pos);
                assertThat(chunkWithBlockCache.offset).isEqualTo((long) i * chunkLength);
                assertThat(chunkWithBlockCache.length).isEqualTo(chunkLength - checksumLength);

                i += ThreadLocalRandom.current().nextInt(100) + 1;
            }
        }
        finally
        {
            metadataWithInMemory.close();
            metadataWithBlockCache.close();
        }
    }

    @Test
    public void chunkForWithBlockCache() throws IOException
    {
        CassandraRelevantProperties.COMPRESSION_CHUNK_OFFSETS_BLOCK_CACHE_SIZE.setString("100MiB");
        chunkFor();

        // verify cache is invalidated on closing compression metadata
        assertThat(CompressionChunkOffsetCache.get().size()).isEqualTo(0);
    }

    @Test
    public void chunkForInMemory() throws IOException
    {
        CassandraRelevantProperties.COMPRESSION_CHUNK_OFFSETS_BLOCK_CACHE_SIZE.setString("0B");
        chunkFor();
    }

    private void chunkFor() throws IOException
    {
        try (CompressionMetadata lessThanOneChunk = createMetadata(10, 7, 0))
        {
            checkMetadata(lessThanOneChunk, 10, 7, 8);
            assertChunks(lessThanOneChunk, 0, 10, 0, 3);
        }

        try (CompressionMetadata oneChunk = createMetadata(16, 9, 0))
        {
            checkMetadata(oneChunk, 16, 9, 8);
            assertChunks(oneChunk, 0, 16, 0, 5);
        }

        try (CompressionMetadata moreThanOneChunk = createMetadata(20, 15, 0, 9))
        {
            checkMetadata(moreThanOneChunk, 20, 15, 16);
            assertChunks(moreThanOneChunk, 0, 16, 0, 5);
            assertChunks(moreThanOneChunk, 16, 20, 9, 2);
        }

        try (CompressionMetadata extraEmptyChunk = createMetadata(20, 17, 0, 9, 15))
        {
            checkMetadata(extraEmptyChunk, 20, 15, 16);
            assertChunks(extraEmptyChunk, 0, 16, 0, 5);
            assertChunks(extraEmptyChunk, 16, 20, 9, 2);
        }

        // chunks of lengths: 3, 5, 7, 11, 7, 5 (+ 4 bytes CRC for each chunk)
        File manyChunksFile = generateMetaDataFile(90, 0, 7, 16, 27, 42, 53);

        // full slice, we should load all 6 chunks
        checkSlice(manyChunksFile, 62, 0L, 90L, md -> {
            checkMetadata(md, 90, 62, 6 * 8);
            assertChunks(md, 0, 16, 0, 3);
            assertChunks(md, 16, 32, 7, 5);
            assertChunks(md, 32, 48, 16, 7);
            assertChunks(md, 48, 64, 27, 11);
            assertChunks(md, 64, 80, 42, 7);
            assertChunks(md, 80, 96, 53, 5);

            assertThat(md.getTotalSizeForSections(asList(new PartitionPositionBounds(0, 90)))).isEqualTo(62);
            assertThat(md.getChunksForSections(asList(new PartitionPositionBounds(0, 90)))).containsExactly(new Chunk(0, 3), new Chunk(7, 5), new Chunk(16, 7), new Chunk(27, 11), new Chunk(42, 7), new Chunk(53, 5));

            assertThat(md.getTotalSizeForSections(asList(new PartitionPositionBounds(20, 40), new PartitionPositionBounds(50, 70)))).isEqualTo(46);
            assertThat(md.getChunksForSections(asList(new PartitionPositionBounds(20, 40), new PartitionPositionBounds(50, 70)))).containsExactly(new Chunk(7, 5), new Chunk(16, 7), new Chunk(27, 11), new Chunk(42, 7));
        });

        // slice starting at 20, we should skip first chunk
        checkSlice(manyChunksFile, 55, 20L, 90L, md -> {
            checkMetadata(md, 74, 55, 5 * 8);
            assertChunks(md, 20, 32, 7, 5);
            assertChunks(md, 32, 48, 16, 7);
            assertChunks(md, 48, 64, 27, 11);
            assertChunks(md, 64, 80, 42, 7);
            assertChunks(md, 80, 90, 53, 5);

            assertThat(md.getTotalSizeForSections(asList(new PartitionPositionBounds(20, 90)))).isEqualTo(55);
            assertThat(md.getChunksForSections(asList(new PartitionPositionBounds(20, 90)))).containsExactly(new Chunk(7, 5), new Chunk(16, 7), new Chunk(27, 11), new Chunk(42, 7), new Chunk(53, 5));

            assertThat(md.getTotalSizeForSections(asList(new PartitionPositionBounds(30, 40), new PartitionPositionBounds(50, 60)))).isEqualTo(35);
            assertThat(md.getChunksForSections(asList(new PartitionPositionBounds(30, 40), new PartitionPositionBounds(50, 60)))).containsExactly(new Chunk(7, 5), new Chunk(16, 7), new Chunk(27, 11));
        });

        // slice ending at 70, we should skip last chunk
        checkSlice(manyChunksFile, 53, 0L, 70L, md -> {
            checkMetadata(md, 70, 53, 5 * 8);
            assertChunks(md, 0, 16, 0, 3);
            assertChunks(md, 16, 32, 7, 5);
            assertChunks(md, 32, 48, 16, 7);
            assertChunks(md, 48, 64, 27, 11);
            assertChunks(md, 64, 70, 42, 7);

            assertThat(md.getTotalSizeForSections(asList(new PartitionPositionBounds(0, 70)))).isEqualTo(53);
            assertThat(md.getChunksForSections(asList(new PartitionPositionBounds(0, 70)))).containsExactly(new Chunk(0, 3), new Chunk(7, 5), new Chunk(16, 7), new Chunk(27, 11), new Chunk(42, 7));

            assertThat(md.getTotalSizeForSections(asList(new PartitionPositionBounds(30, 40), new PartitionPositionBounds(50, 60)))).isEqualTo(35);
            assertThat(md.getChunksForSections(asList(new PartitionPositionBounds(30, 40), new PartitionPositionBounds(50, 60)))).containsExactly(new Chunk(7, 5), new Chunk(16, 7), new Chunk(27, 11));
        });

        // slice starting at 20 and ending at 70, we should skip first and last chunk
        checkSlice(manyChunksFile, 46, 20L, 70L, md -> {
            checkMetadata(md, 54, 46, 4 * 8);
            assertChunks(md, 20, 32, 7, 5);
            assertChunks(md, 32, 48, 16, 7);
            assertChunks(md, 48, 64, 27, 11);
            assertChunks(md, 64, 70, 42, 7);

            assertThat(md.getTotalSizeForSections(asList(new PartitionPositionBounds(20, 70)))).isEqualTo(46);
            assertThat(md.getChunksForSections(asList(new PartitionPositionBounds(20, 70)))).containsExactly(new Chunk(7, 5), new Chunk(16, 7), new Chunk(27, 11), new Chunk(42, 7));

            assertThat(md.getTotalSizeForSections(asList(new PartitionPositionBounds(30, 40), new PartitionPositionBounds(50, 60)))).isEqualTo(35);
            assertThat(md.getChunksForSections(asList(new PartitionPositionBounds(30, 40), new PartitionPositionBounds(50, 60)))).containsExactly(new Chunk(7, 5), new Chunk(16, 7), new Chunk(27, 11));
        });
    }

    @Test
    public void testConcurrentAccessInMemory() throws Exception
    {
        CassandraRelevantProperties.COMPRESSION_CHUNK_OFFSETS_BLOCK_CACHE_SIZE.setString("0B");
        testConcurrentAccess();
    }

    @Test
    public void testConcurrentAccessBlockCache() throws Exception
    {
        CassandraRelevantProperties.COMPRESSION_CHUNK_OFFSETS_BLOCK_CACHE_SIZE.setString("100MiB");
        testConcurrentAccess();

        // verify cache is invalidated on closing compression metadata
        assertThat(CompressionChunkOffsetCache.get().size()).isEqualTo(0);
    }

    private void testConcurrentAccess() throws Exception
    {
        int chunkCount = 100_000;
        int chunkLength = 16;
        int checksumLength = 4; // payload size is 12 = chunkLength - checksumLength
        long[] offsets = new long[chunkCount];
        long offset = 0;
        for (int i = 0; i < chunkCount; i++)
        {
            offsets[i] = offset;
            offset += chunkLength;
        }

        File metadataFile = generateMetaDataFile((long) chunkCount * chunkLength, offsets);
        long compressedFileLength = offset;
        try (CompressionMetadata metadata = new CompressionMetadata(metadataFile, compressedFileLength, true))
        {
            int numThreads = 16;
            int operationsPerThread = 10000;
            ExecutorService executor = Executors.newFixedThreadPool(numThreads);
            List<Callable<Void>> tasks = new ArrayList<>(numThreads);

            for (int i = 0; i < numThreads; i++)
            {
                tasks.add(() -> {
                    for (int j = 0; j < operationsPerThread; j++)
                    {
                        long position = ThreadLocalRandom.current().nextLong(metadata.dataLength);
                        int chunkIndex = (int) (position / chunkLength);
                        long expectedOffset = (long) chunkIndex * chunkLength;
                        int expectedLength = chunkLength - checksumLength;

                        Chunk chunk = metadata.chunkFor(position);

                        assertThat(chunk.offset).isEqualTo(expectedOffset);
                        assertThat(chunk.length).isEqualTo(expectedLength);
                    }
                    return null;
                });
            }

            List<Future<Void>> futures = executor.invokeAll(tasks);
            for (Future<Void> future : futures)
            {
                future.get(); // This will throw an exception if the task failed
            }
            executor.shutdown();
        }
    }

    @Test
    public void testCacheSize()
    {
        CassandraRelevantProperties.COMPRESSION_CHUNK_OFFSETS_BLOCK_CACHE_SIZE.setString("0B");
        assertThat(getCacheSizeInBytes(1000)).isEqualTo(0);
        CassandraRelevantProperties.COMPRESSION_CHUNK_OFFSETS_BLOCK_CACHE_SIZE.setString("10GiB");
        assertThat(getCacheSizeInBytes(1000)).isEqualTo(SizeUnit.GIGABYTES.toBytes(10));
        CassandraRelevantProperties.COMPRESSION_CHUNK_OFFSETS_BLOCK_CACHE_SIZE.setString("10MiB");
        assertThat(getCacheSizeInBytes(1000)).isEqualTo(SizeUnit.MEGABYTES.toBytes(10));

        CassandraRelevantProperties.COMPRESSION_CHUNK_OFFSETS_BLOCK_CACHE_SIZE.setString("auto");
        assertThat(getCacheSizeInBytes(1000)).isEqualTo(1000);
        CassandraRelevantProperties.COMPRESSION_CHUNK_OFFSETS_BLOCK_CACHE_SIZE.setString("auto@1");
        assertThat(getCacheSizeInBytes(1000)).isEqualTo(1000);
        CassandraRelevantProperties.COMPRESSION_CHUNK_OFFSETS_BLOCK_CACHE_SIZE.setString("auto@0.1");
        assertThat(getCacheSizeInBytes(1000)).isEqualTo(100);
    }

    @Test
    public void testCompressionMetadataReadError() throws Exception
    {
        CassandraRelevantProperties.COMPRESSION_CHUNK_OFFSETS_BLOCK_CACHE_SIZE.setString("0B");

        File f = generateMetaDataFile(64, 0, 16, 32, 48);
        long sizeBefore = f.length();
        // corrupt the file to force error while opening
        try (FileChannel channel = FileChannel.open(f.toPath(), StandardOpenOption.WRITE))
        {
            // EOF
            channel.truncate(sizeBefore - Long.BYTES);
        }

        long before = CompressionMetadata.nativeMemoryAllocated();

        assertThatThrownBy(() -> new CompressionMetadata(f, 64, true)).isInstanceOf(RuntimeException.class);
        assertThat(CompressionMetadata.nativeMemoryAllocated()).isEqualTo(before);
    }
}
