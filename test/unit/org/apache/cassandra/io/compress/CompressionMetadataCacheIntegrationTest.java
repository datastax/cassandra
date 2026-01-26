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

import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.io.sstable.format.SSTableReader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Integration tests for CompressionMetadataCache with real SSTables.
 * Tests cache behavior during flush, compaction, and read operations.
 */
public class CompressionMetadataCacheIntegrationTest extends CQLTester
{
    private boolean originalCacheEnabled;
    private int originalCacheSize;
    private int originalBlockSize;

    @BeforeClass
    public static void setUpClass()
    {
        CQLTester.setUpClass();
    }

    @Before
    public void setUp()
    {
        // Save original config
        originalCacheEnabled = DatabaseDescriptor.getCompressionMetadataCacheEnabled();
        originalCacheSize = DatabaseDescriptor.getCompressionMetadataCacheSizeMB();
        originalBlockSize = DatabaseDescriptor.getCompressionMetadataCacheBlockSize();

        // Enable cache for testing
        DatabaseDescriptor.setCompressionMetadataCacheEnabled(true);
        DatabaseDescriptor.setCompressionMetadataCacheSizeMB(100);
        DatabaseDescriptor.setCompressionMetadataCacheBlockSize(1024);

        // Enable cache runtime (singleton was initialized with cache disabled)
        CompressionMetadataCache.instance.enable(true);
    }

    @After
    public void tearDown()
    {
        // Restore original config
        DatabaseDescriptor.setCompressionMetadataCacheEnabled(originalCacheEnabled);
        DatabaseDescriptor.setCompressionMetadataCacheSizeMB(originalCacheSize);
        DatabaseDescriptor.setCompressionMetadataCacheBlockSize(originalBlockSize);

        // Restore cache state
        CompressionMetadataCache.instance.enable(originalCacheEnabled);
    }

    @Test
    public void testCacheWithCompressedSSTable() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v text) WITH compression = {'class': 'LZ4Compressor'}");
        
        // Insert data and flush
        for (int i = 0; i < 1000; i++)
        {
            execute("INSERT INTO %s (k, v) VALUES (?, ?)", i, "value" + i);
        }
        flush();

        ColumnFamilyStore store = getCurrentColumnFamilyStore();
        Set<SSTableReader> sstables = store.getLiveSSTables();
        
        assertEquals(1, sstables.size());
        
        SSTableReader sstable = sstables.iterator().next();
        CompressionMetadata metadata = sstable.getCompressionMetadata();
        
        assertNotNull(metadata);
        assertTrue(metadata instanceof CachedCompressionMetadata);
        assertTrue(metadata.hasOffsets());
        
        // Verify cache metrics show activity
        CompressionMetadataCacheMetrics metrics = CompressionMetadataCache.instance.getMetricsForTesting();
        assertTrue(metrics.requests() > 0);
    }

    @Test
    public void testCacheDisabledFallback() throws Throwable
    {
        // Disable cache
        DatabaseDescriptor.setCompressionMetadataCacheEnabled(false);
        CompressionMetadataCache.instance.clear();
        
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v text) WITH compression = {'class': 'LZ4Compressor'}");
        
        for (int i = 0; i < 100; i++)
        {
            execute("INSERT INTO %s (k, v) VALUES (?, ?)", i, "value" + i);
        }
        flush();

        ColumnFamilyStore store = getCurrentColumnFamilyStore();
        Set<SSTableReader> sstables = store.getLiveSSTables();
        
        assertEquals(1, sstables.size());
        
        SSTableReader sstable = sstables.iterator().next();
        CompressionMetadata metadata = sstable.getCompressionMetadata();
        
        assertNotNull(metadata);
        // Should fall back to regular CompressionMetadata
        assertFalse(metadata instanceof CachedCompressionMetadata);
    }

    @Test
    public void testCacheHitRateDuringReads() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v text) WITH compression = {'class': 'LZ4Compressor'}");
        
        // Insert and flush
        for (int i = 0; i < 1000; i++)
        {
            execute("INSERT INTO %s (k, v) VALUES (?, ?)", i, "value" + i);
        }
        flush();

        CompressionMetadataCacheMetrics metrics = CompressionMetadataCache.instance.getMetricsForTesting();
        long initialHits = metrics.hits();
        long initialMisses = metrics.misses();
        
        // Perform reads to trigger cache access
        for (int i = 0; i < 100; i++)
        {
            execute("SELECT * FROM %s WHERE k = ?", i);
        }
        
        // Verify cache was used
        assertTrue(metrics.requests() > initialHits + initialMisses);
        assertTrue(metrics.hitRate() > 0.0);
    }

    @Test
    public void testCacheWithMultipleSSTables() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v text) WITH compression = {'class': 'LZ4Compressor'}");
        
        // Create multiple SSTables
        for (int batch = 0; batch < 3; batch++)
        {
            for (int i = batch * 100; i < (batch + 1) * 100; i++)
            {
                execute("INSERT INTO %s (k, v) VALUES (?, ?)", i, "value" + i);
            }
            flush();
        }

        ColumnFamilyStore store = getCurrentColumnFamilyStore();
        Set<SSTableReader> sstables = store.getLiveSSTables();
        
        assertEquals(3, sstables.size());
        
        // Verify all SSTables use cached metadata
        for (SSTableReader sstable : sstables)
        {
            CompressionMetadata metadata = sstable.getCompressionMetadata();
            assertNotNull(metadata);
            assertTrue(metadata instanceof CachedCompressionMetadata);
        }
        
        // Verify cache has entries for multiple files
        assertTrue(CompressionMetadataCache.instance.size() > 0);
    }

    @Test
    public void testCacheAfterCompaction() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v text) WITH compression = {'class': 'LZ4Compressor'}");
        
        // Create multiple SSTables
        for (int batch = 0; batch < 3; batch++)
        {
            for (int i = batch * 100; i < (batch + 1) * 100; i++)
            {
                execute("INSERT INTO %s (k, v) VALUES (?, ?)", i, "value" + i);
            }
            flush();
        }

        ColumnFamilyStore store = getCurrentColumnFamilyStore();
        assertEquals(3, store.getLiveSSTables().size());
        
        // Compact
        compact();
        
        // Should have single SSTable after compaction
        Set<SSTableReader> sstables = store.getLiveSSTables();
        assertEquals(1, sstables.size());
        
        SSTableReader sstable = sstables.iterator().next();
        CompressionMetadata metadata = sstable.getCompressionMetadata();
        
        assertNotNull(metadata);
        assertTrue(metadata instanceof CachedCompressionMetadata);
        
        // Verify cache still works after compaction
        CompressionMetadataCacheMetrics metrics = CompressionMetadataCache.instance.getMetricsForTesting();
        assertTrue(metrics.requests() > 0);
    }

    @Test
    public void testCacheMemoryBounds() throws Throwable
    {
        // Set small cache size to test eviction
        DatabaseDescriptor.setCompressionMetadataCacheSizeMB(10);
        CompressionMetadataCache.instance.clear();
        
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v text) WITH compression = {'class': 'LZ4Compressor'}");
        
        // Create many SSTables to trigger evictions
        for (int batch = 0; batch < 20; batch++)
        {
            for (int i = batch * 50; i < (batch + 1) * 50; i++)
            {
                execute("INSERT INTO %s (k, v) VALUES (?, ?)", i, "value" + i);
            }
            flush();
        }

        CompressionMetadataCacheMetrics metrics = CompressionMetadataCache.instance.getMetricsForTesting();
        
        // Verify evictions occurred
        assertTrue(metrics.evictions() > 0);
        
        // Verify cache size is bounded
        long weightBytes = CompressionMetadataCache.instance.weightBytes();
        long maxBytes = 10 * 1024 * 1024; // 10 MB
        assertTrue(weightBytes <= maxBytes);
    }

    @Test
    public void testCacheWithDifferentCompressors() throws Throwable
    {
        // Test with LZ4
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v text) WITH compression = {'class': 'LZ4Compressor'}");
        
        for (int i = 0; i < 100; i++)
        {
            execute("INSERT INTO %s (k, v) VALUES (?, ?)", i, "value" + i);
        }
        flush();

        ColumnFamilyStore store = getCurrentColumnFamilyStore();
        SSTableReader sstable = store.getLiveSSTables().iterator().next();
        
        assertTrue(sstable.getCompressionMetadata() instanceof CachedCompressionMetadata);
        assertTrue(sstable.getCompressionMetadata().parameters.getSstableCompressor() instanceof LZ4Compressor);
        
        // Test with Snappy
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v text) WITH compression = {'class': 'SnappyCompressor'}");
        
        for (int i = 0; i < 100; i++)
        {
            execute("INSERT INTO %s (k, v) VALUES (?, ?)", i, "value" + i);
        }
        flush();

        store = getCurrentColumnFamilyStore();
        sstable = store.getLiveSSTables().iterator().next();
        
        assertTrue(sstable.getCompressionMetadata() instanceof CachedCompressionMetadata);
        assertTrue(sstable.getCompressionMetadata().parameters.getSstableCompressor() instanceof SnappyCompressor);
    }

    @Test
    public void testCacheMetricsAccuracy() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v text) WITH compression = {'class': 'LZ4Compressor'}");
        
        CompressionMetadataCacheMetrics metrics = CompressionMetadataCache.instance.getMetricsForTesting();
        metrics.reset();
        
        // Insert and flush
        for (int i = 0; i < 500; i++)
        {
            execute("INSERT INTO %s (k, v) VALUES (?, ?)", i, "value" + i);
        }
        flush();

        long requestsBefore = metrics.requests();
        
        // Perform reads
        for (int i = 0; i < 100; i++)
        {
            execute("SELECT * FROM %s WHERE k = ?", i);
        }
        
        long requestsAfter = metrics.requests();
        
        // Verify metrics increased
        assertTrue(requestsAfter > requestsBefore);
        assertTrue(metrics.hits() > 0);
        assertTrue(metrics.hitRate() > 0.0);
        assertTrue(metrics.averageLoadPenalty() > 0.0);
    }
}
