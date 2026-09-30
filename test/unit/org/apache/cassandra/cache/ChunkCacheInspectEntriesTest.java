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

package org.apache.cassandra.cache;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.metrics.ChunkCacheMetrics;
import org.apache.cassandra.utils.memory.BufferPool;
import org.apache.cassandra.utils.memory.BufferPools;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class ChunkCacheInspectEntriesTest
{
    private static final Logger logger = LoggerFactory.getLogger(ChunkCacheInspectEntriesTest.class);

    private ChunkCache cache;

    @Parameterized.Parameter(0)
    public ChunkCache.InspectEntriesOrder order;

    @Parameterized.Parameter(1)
    public int numFiles;

    @Parameterized.Parameter(2)
    public int limit;

    @Parameterized.Parameter(3)
    public String testName;

    @Parameterized.Parameters(name = "{3}: order={0}, numFiles={1}, limit={2}")
    public static Collection<Object[]> parameters()
    {
        return Arrays.asList(new Object[][]{
            {ChunkCache.InspectEntriesOrder.HOTTEST, 3, 10, "testInspectHotEntriesWithMultipleFiles"},
            {ChunkCache.InspectEntriesOrder.COLDEST, 2, 10, "testInspectColdEntriesWithMultipleFiles"},
            {ChunkCache.InspectEntriesOrder.HOTTEST, 5, 2, "testInspectHotEntriesWithLimit"},
            {ChunkCache.InspectEntriesOrder.HOTTEST, 1, 0, "testInspectHotEntriesWithZeroLimit"}
        });
    }

    @BeforeClass
    public static void setupDD()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @org.junit.Before
    public void setUp()
    {
        BufferPool pool = BufferPools.forChunkCache();
        cache = new ChunkCache(pool, 512, ChunkCacheMetrics::create);
    }

    @Test
    public void testInspectEntries() throws IOException
    {
        logger.info("Starting test: {} with order={}, numFiles={}, limit={}", testName, order, numFiles, limit);
        
        assertEquals(0, cache.size());

        List<File> files = new ArrayList<>();
        List<FileHandle> handles = new ArrayList<>();
        List<RandomAccessReader> readers = new ArrayList<>();

        try
        {
            // Create and populate cache with numFiles
            logger.debug("Creating {} test files and populating cache", numFiles);
            for (int i = 0; i < numFiles; i++)
            {
                File file = FileUtils.createTempFile("test" + i, null);
                file.deleteOnExit();
                Files.write(file.toPath(), new byte[RandomAccessReader.DEFAULT_BUFFER_SIZE]);
                files.add(file);

                FileHandle.Builder builder = new FileHandle.Builder(file).withChunkCache(cache);
                FileHandle handle = builder.complete();
                handles.add(handle);

                RandomAccessReader reader = handle.createReader();
                readers.add(reader);
                reader.reBuffer();
                logger.trace("Created and cached file {}: {}", i, file.path());
            }

            assertEquals(numFiles, cache.size());
            logger.debug("Cache populated with {} entries", numFiles);

            Set<File> expectedFiles = new HashSet<>(files);

            // Inspect entries
            logger.debug("Inspecting cache entries with limit={} and order={}", limit, order);
            List<ChunkCache.ChunkCacheInspectionEntry> entries = new ArrayList<>();
            cache.inspectEntries(limit, order, entries::add);

            // Verify count respects limit
            int expectedCount = Math.min(limit, numFiles);
            assertEquals(expectedCount, entries.size());
            logger.debug("Retrieved {} entries from cache (expected: {})", entries.size(), expectedCount);

            // Verify entries have valid data and match files we put in cache
            for (ChunkCache.ChunkCacheInspectionEntry entry : entries)
            {
                assertNotNull("File should not be null", entry.file);
                assertTrue("File should be one we added to cache", expectedFiles.contains(entry.file));
                assertTrue("Position should be non-negative", entry.position >= 0);
                assertTrue("Size should be positive", entry.size > 0);
                logger.trace("Verified entry: file={}, position={}, size={}", entry.file.path(), entry.position, entry.size);
            }

            // Verify all files are represented when limit >= numFiles
            if (limit >= numFiles)
            {
                Set<File> observedFiles = entries.stream()
                                                 .map(e -> e.file)
                                                 .collect(Collectors.toSet());
                assertEquals("All cached files should appear in results", expectedFiles, observedFiles);
                logger.debug("Verified all {} files are represented in results", numFiles);
            }
            
            logger.info("Test completed successfully: {}", testName);
        }
        finally
        {
            // Clean up resources
            logger.debug("Cleaning up test resources");
            for (RandomAccessReader reader : readers)
                reader.close();
            for (FileHandle handle : handles)
                handle.close();
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testInspectEntriesWhenCacheDisabled()
    {
        logger.info("Testing inspect entries with disabled cache for order={}", order);
        BufferPool pool = BufferPools.forChunkCache();
        ChunkCache disabledCache = new ChunkCache(pool, 0, ChunkCacheMetrics::create);

        logger.debug("Attempting to inspect entries on disabled cache - expecting IllegalStateException");
        disabledCache.inspectEntries(10, order, e -> {
        });
    }

    @Test
    public void testInspectEntriesWithEmptyCache()
    {
        logger.info("Testing inspect entries with empty cache for order={}", order);
        assertEquals(0, cache.size());
        logger.debug("Cache verified empty");

        List<ChunkCache.ChunkCacheInspectionEntry> entries = new ArrayList<>();

        // Should not throw when cache is empty
        logger.debug("Inspecting empty cache with order={}", order);
        cache.inspectEntries(10, order, entries::add);

        assertEquals(0, entries.size());
        logger.info("Verified empty cache returns 0 entries");
    }

}
