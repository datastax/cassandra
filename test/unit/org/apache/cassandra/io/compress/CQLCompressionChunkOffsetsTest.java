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

import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.awaitility.Awaitility;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CQLCompressionChunkOffsetsTest extends CQLTester
{
    @BeforeClass
    public static void setUpClass()
    {
        CQLTester.setUpClass();
    }

    @Before
    public void resetConfigs()
    {
        CassandraRelevantProperties.COMPRESSION_CHUNK_OFFSETS_BLOCK_CACHE_SIZE.reset();
        CompressionChunkOffsetCache.resetCache();
    }

    @Test
    public void testNativeMemoryTrackingLifecycleWithInMemory() throws Throwable
    {
        CassandraRelevantProperties.COMPRESSION_CHUNK_OFFSETS_BLOCK_CACHE_SIZE.setString("0MiB");

        createTable("CREATE TABLE %s (k int PRIMARY KEY, v text) WITH compression = {'sstable_compression': 'LZ4Compressor', 'chunk_length_in_kb': '1'};");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();

        long initialMemory = CompressionMetadata.nativeMemoryAllocated();

        populateAndFlush(0, 128);
        assertEquals(1, cfs.getLiveSSTables().size());

        SSTableReader first = cfs.getLiveSSTables().iterator().next();
        long firstOffHeap = first.getCompressionMetadata().offHeapSize();
        assertTrue(firstOffHeap > 0);
        assertEquals(initialMemory + firstOffHeap, CompressionMetadata.nativeMemoryAllocated());

        populateAndFlush(128, 256);
        assertEquals(2, cfs.getLiveSSTables().size());

        SSTableReader second = cfs.getLiveSSTables().stream().filter(s -> s.descriptor != first.descriptor)
                                  .findFirst().orElseThrow(() -> new AssertionError("Expected flushed sstable"));
        long secondOffheap = second.getCompressionMetadata().offHeapSize();
        assertTrue(secondOffheap > 0);
        assertEquals(initialMemory + firstOffHeap + secondOffheap, CompressionMetadata.nativeMemoryAllocated());

        // force compaction
        compact();
        assertEquals(1, cfs.getLiveSSTables().size());

        SSTableReader compacted = cfs.getLiveSSTables().iterator().next();
        long compactedOffheap = compacted.getCompressionMetadata().offHeapSize();
        assertTrue(compactedOffheap > 0);
        Awaitility.await("Memory released after compaction")
                  .atMost(10, TimeUnit.SECONDS)
                  .untilAsserted(() -> {
                      assertEquals(initialMemory + compactedOffheap, CompressionMetadata.nativeMemoryAllocated());
                  });
    }

    @Test
    public void testNativeMemoryTrackingLifecycleWithBlockCache() throws Throwable
    {
        CassandraRelevantProperties.COMPRESSION_CHUNK_OFFSETS_BLOCK_CACHE_SIZE.setString("1000MiB");

        createTable("CREATE TABLE %s (k int PRIMARY KEY, v text) WITH compression = {'sstable_compression': 'LZ4Compressor', 'chunk_length_in_kb': '1'};");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();

        long initialMemory = CompressionMetadata.nativeMemoryAllocated();

        populateAndFlush(0, 128);
        assertEquals(1, cfs.getLiveSSTables().size());

        SSTableReader first = cfs.getLiveSSTables().iterator().next();
        long firstOffHeap = first.getCompressionMetadata().offHeapSize();
        assertEquals(firstOffHeap, 0);

        // should include memory from allocated block cache buffer
        long memoryAfterFirstFlush = CompressionMetadata.nativeMemoryAllocated();
        assertTrue(memoryAfterFirstFlush > initialMemory);

        populateAndFlush(128, 256);
        assertEquals(2, cfs.getLiveSSTables().size());

        SSTableReader second = cfs.getLiveSSTables().stream().filter(s -> s.descriptor != first.descriptor)
                                  .findFirst().orElseThrow(() -> new AssertionError("Expected flushed sstable"));
        long secondOffheap = second.getCompressionMetadata().offHeapSize();
        assertEquals(secondOffheap, 0);

        long memoryAfterSecondFlush = CompressionMetadata.nativeMemoryAllocated();
        assertTrue(memoryAfterSecondFlush > memoryAfterFirstFlush);

        // force compaction
        compact();
        assertEquals(1, cfs.getLiveSSTables().size());

        SSTableReader compacted = cfs.getLiveSSTables().iterator().next();
        long compactedOffheap = compacted.getCompressionMetadata().offHeapSize();
        assertEquals(compactedOffheap, 0);
        Awaitility.await("Memory released after compaction")
                  .atMost(10, TimeUnit.SECONDS)
                  .untilAsserted(() -> {
                      assertTrue(CompressionMetadata.nativeMemoryAllocated() <= memoryAfterSecondFlush);
                  });
    }

    private void populateAndFlush(int start, int end)
    {
        for (int i = start; i < end; i++)
            execute("INSERT INTO %s (k, v) values (?, ?)", i, String.format("value_%d_%08000d", i, i));
        flush();
    }
}
