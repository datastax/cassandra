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
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.compress.CompressionMetadata.Chunk;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.schema.CompressionParams;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class CompressionMetadataCacheTest
{
    private boolean originalCacheEnabled;
    private int originalCacheSize;
    private int originalBlockSize;

    @BeforeClass
    public static void setupDD()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    private File metadataFile;

    @Before
    public void setUp() throws IOException
    {
        // Save original config
        originalCacheEnabled = DatabaseDescriptor.getCompressionMetadataCacheEnabled();
        originalCacheSize = DatabaseDescriptor.getCompressionMetadataCacheSizeMB();
        originalBlockSize = DatabaseDescriptor.getCompressionMetadataCacheBlockSize();

        // Enable cache for testing
        DatabaseDescriptor.setCompressionMetadataCacheEnabled(true);
        DatabaseDescriptor.setCompressionMetadataCacheSizeMB(10); // Small cache for testing
        DatabaseDescriptor.setCompressionMetadataCacheBlockSize(64); // Small blocks for testing

        // Create test metadata file
        metadataFile = generateMetadataFile(1024 * 1024, 100); // 1MB data, 100 chunks

        // Clear cache
        CompressionMetadataCache.instance.clear();
    }

    @After
    public void tearDown()
    {
        // Restore original config
        DatabaseDescriptor.setCompressionMetadataCacheEnabled(originalCacheEnabled);
        DatabaseDescriptor.setCompressionMetadataCacheSizeMB(originalCacheSize);
        DatabaseDescriptor.setCompressionMetadataCacheBlockSize(originalBlockSize);

        // Clean up test file
        if (metadataFile != null && metadataFile.exists())
            metadataFile.delete();

        // Clear cache
        CompressionMetadataCache.instance.clear();
    }

    private File generateMetadataFile(long dataLength, int numChunks) throws IOException
    {
        // Create temp directory for test files
        Path tempDir = Files.createTempDirectory("compression_metadata_cache_test");

        // Use proper SSTable naming: na-1-big-CompressionInfo.db
        File metadataFile = new File(tempDir.resolve("na-1-big-CompressionInfo.db"));
        CompressionParams params = CompressionParams.snappy(16384); // 16KB chunks

        long compressedFileSize;
        try (CompressionMetadata.Writer writer = CompressionMetadata.Writer.open(params, metadataFile))
        {
            long offset = 0;
            for (int i = 0; i < numChunks; i++)
            {
                writer.addOffset(offset);
                offset += 8192 + (i % 100); // Varying compressed sizes
            }
            compressedFileSize = offset;

            writer.finalizeLength(dataLength, numChunks);
            writer.doPrepare();
            Throwable t = writer.doCommit(null);
            if (t != null)
                throw new IOException(t);
        }

        // Create corresponding data file with correct size
        File dataFile = new File(tempDir.resolve("na-1-big-Data.db"));
        try (RandomAccessFile raf = new RandomAccessFile(dataFile.toJavaIOFile(), "rw"))
        {
            raf.setLength(compressedFileSize);
        }

        return metadataFile;
    }

    @Test
    public void testCacheEnabled()
    {
        assertThat(CompressionMetadataCache.instance.isEnabled()).isTrue();
    }

    @Test
    public void testGetChunk() throws IOException
    {
        Path path = metadataFile.toPath();

        // Get first chunk
        Chunk chunk0 = CompressionMetadataCache.instance.getChunk(path, 0);
        assertThat(chunk0).isNotNull();
        assertThat(chunk0.offset).isEqualTo(0);

        // Get another chunk in same block
        Chunk chunk1 = CompressionMetadataCache.instance.getChunk(path, 1);
        assertThat(chunk1).isNotNull();
        assertThat(chunk1.offset).isGreaterThan(0);

        // Verify metrics show hits
        CompressionMetadataCacheMetrics metrics = CompressionMetadataCache.instance.getMetricsForTesting();
        assertThat(metrics.hits()).isGreaterThan(0);
    }

    @Test
    public void testCacheHitRate() throws IOException
    {
        Path path = metadataFile.toPath();
        CompressionMetadataCacheMetrics metrics = CompressionMetadataCache.instance.getMetricsForTesting();

        long initialHits = metrics.hits();
        long initialMisses = metrics.misses();

        // First access - should be cache miss
        CompressionMetadataCache.instance.getChunk(path, 0);
        assertThat(metrics.misses()).isEqualTo(initialMisses + 1);

        // Second access to same block - should be cache hit
        CompressionMetadataCache.instance.getChunk(path, 1);
        assertThat(metrics.hits()).isGreaterThan(initialHits);

        // Verify hit rate
        assertThat(metrics.hitRate()).isGreaterThan(0.0);
    }

    @Test
    public void testBlockBoundaries() throws IOException
    {
        Path path = metadataFile.toPath();
        int blockSize = DatabaseDescriptor.getCompressionMetadataCacheBlockSize();

        // Access chunk at block boundary
        Chunk chunk = CompressionMetadataCache.instance.getChunk(path, blockSize - 1);
        assertThat(chunk).isNotNull();

        // Access chunk in next block
        Chunk nextChunk = CompressionMetadataCache.instance.getChunk(path, blockSize);
        assertThat(nextChunk).isNotNull();
        assertThat(nextChunk.offset).isGreaterThan(chunk.offset);
    }

    @Test
    public void testConcurrentAccess() throws Exception
    {
        Path path = metadataFile.toPath();
        int numThreads = 10;
        int accessesPerThread = 100;

        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(numThreads);

        List<Future<?>> futures = new ArrayList<>();

        for (int t = 0; t < numThreads; t++)
        {
            final int threadId = t;
            futures.add(executor.submit(() -> {
                try
                {
                    startLatch.await();
                    for (int i = 0; i < accessesPerThread; i++)
                    {
                        int chunkIndex = (threadId * accessesPerThread + i) % 100;
                        Chunk chunk = CompressionMetadataCache.instance.getChunk(path, chunkIndex);
                        assertThat(chunk).isNotNull();
                    }
                }
                catch (Exception e)
                {
                    throw new RuntimeException(e);
                }
                finally
                {
                    doneLatch.countDown();
                }
            }));
        }

        // Start all threads at once
        startLatch.countDown();

        // Wait for completion
        assertThat(doneLatch.await(30, TimeUnit.SECONDS)).isTrue();

        // Verify no exceptions
        for (Future<?> future : futures)
        {
            future.get();
        }

        executor.shutdown();

        // Verify metrics
        CompressionMetadataCacheMetrics metrics = CompressionMetadataCache.instance.getMetricsForTesting();
        assertThat(metrics.requests()).isEqualTo(numThreads * accessesPerThread);
        assertThat(metrics.hitRate()).isGreaterThan(0.0);
    }

    @Test
    public void testCacheEviction() throws IOException
    {
        // Create a larger metadata file with many chunks to generate enough blocks
        // Cache is 10MB (set in setUp), with block size 64 and 512 bytes per block
        // We need > 20480 blocks to exceed 10MB cache
        // 1,310,720 chunks / 64 = 20,480 blocks = exactly 10MB
        // Use 1,500,000 chunks to create ~23,437 blocks = ~12MB to force evictions
        File largeMetadataFile = generateMetadataFile(1500 * 1024 * 1024, 1500000);

        try
        {
            CompressionMetadataCache.instance.clear();

            Path path = largeMetadataFile.toPath();
            CompressionMetadataCacheMetrics metrics = CompressionMetadataCache.instance.getMetricsForTesting();

            long initialEvictions = metrics.evictions();

            // Access many different blocks to trigger evictions
            // Access chunks across many blocks: 0, 64, 128, 192, etc.
            // With 1,500,000 chunks and block size 64, we have 23,437 complete blocks
            for (int i = 0; i < 23437; i++)
            {
                CompressionMetadataCache.instance.getChunk(path, i * 64);
            }

            // Verify evictions occurred
            assertThat(metrics.evictions()).isGreaterThan(initialEvictions);
        }
        finally
        {
            largeMetadataFile.delete();
        }
    }

    @Test
    public void testMetricsReset()
    {
        CompressionMetadataCacheMetrics metrics = CompressionMetadataCache.instance.getMetricsForTesting();

        // Record some activity
        metrics.recordHits(10);
        metrics.recordMisses(5);

        assertThat(metrics.hits()).isEqualTo(10);
        assertThat(metrics.misses()).isEqualTo(5);

        // Reset
        metrics.reset();

        assertThat(metrics.hits()).isEqualTo(0);
        assertThat(metrics.misses()).isEqualTo(0);
    }

    @Test
    public void testCacheStats()
    {
        CompressionMetadataCache.CacheStats stats = CompressionMetadataCache.instance.getStats();
        assertThat(stats).isNotNull();
        assertThat(stats.size).isGreaterThanOrEqualTo(0);
    }

    @Test
    public void testInvalidChunkIndex() throws IOException
    {
        Path path = metadataFile.toPath();

        // Try to access chunk beyond file bounds
        assertThatThrownBy(() -> CompressionMetadataCache.instance.getChunk(path, 10000))
            .isInstanceOf(Exception.class);
    }

    @Test
    public void testConfigValidation()
    {
        // Test cache size validation
        assertThatThrownBy(() -> DatabaseDescriptor.setCompressionMetadataCacheSizeMB(0))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("must be positive");

        assertThatThrownBy(() -> DatabaseDescriptor.setCompressionMetadataCacheSizeMB(-1))
            .isInstanceOf(IllegalArgumentException.class);

        // Test block size validation
        assertThatThrownBy(() -> DatabaseDescriptor.setCompressionMetadataCacheBlockSize(32))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("must be between 64 and 8192");

        assertThatThrownBy(() -> DatabaseDescriptor.setCompressionMetadataCacheBlockSize(10000))
            .isInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(() -> DatabaseDescriptor.setCompressionMetadataCacheBlockSize(100))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("must be a power of 2");
    }

    @Test
    public void testCacheDisabled() throws IOException
    {
        try
        {
            CompressionMetadataCache.instance.enable(false);

            assertThat(CompressionMetadataCache.instance.isEnabled()).isFalse();

            Path path = metadataFile.toPath();
            assertThatThrownBy(() -> CompressionMetadataCache.instance.getChunk(path, 0))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("not enabled");
        }
        finally
        {
            CompressionMetadataCache.instance.enable(true);
        }
    }

    @Test
    public void testLoadLatencyMetrics() throws IOException
    {
        Path path = metadataFile.toPath();
        CompressionMetadataCacheMetrics metrics = CompressionMetadataCache.instance.getMetricsForTesting();

        long initialLoadSuccesses = metrics.loadSuccesses();

        // Trigger cache miss (load from disk)
        CompressionMetadataCache.instance.getChunk(path, 0);

        assertThat(metrics.loadSuccesses()).isGreaterThan(initialLoadSuccesses);
        assertThat(metrics.averageLoadPenalty()).isGreaterThan(0.0);
    }
}
