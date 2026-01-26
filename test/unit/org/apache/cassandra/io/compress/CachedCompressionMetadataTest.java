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
import java.util.Arrays;
import java.util.Collection;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.compress.CompressionMetadata.Chunk;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.SliceDescriptor;
import org.apache.cassandra.schema.CompressionParams;

import static org.assertj.core.api.Assertions.assertThat;

public class CachedCompressionMetadataTest
{
    private boolean originalCacheEnabled;
    private File dataFile;
    private File metadataFile;

    @BeforeClass
    public static void setupDD()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Before
    public void setUp() throws IOException
    {
        // Save and enable cache
        originalCacheEnabled = DatabaseDescriptor.getCompressionMetadataCacheEnabled();
        DatabaseDescriptor.setCompressionMetadataCacheEnabled(true);
        DatabaseDescriptor.setCompressionMetadataCacheSizeMB(10);
        DatabaseDescriptor.setCompressionMetadataCacheBlockSize(64);

        // Create test files with valid SSTable naming convention
        // Format: {version}-{generation}-{format}-Component.db
        Path tempDir = Files.createTempDirectory("cached_compression_test");
        dataFile = new File(tempDir.resolve("na-1-big-Data.db"));

        // Create metadata file using Writer and calculate compressed file size
        CompressionParams params = CompressionParams.snappy(16384);
        metadataFile = new File(tempDir.resolve("na-1-big-CompressionInfo.db"));

        long compressedFileSize;
        try (CompressionMetadata.Writer writer = CompressionMetadata.Writer.open(params, metadataFile))
        {
            long offset = 0;
            for (int i = 0; i < 100; i++)
            {
                writer.addOffset(offset);
                offset += 8192 + (i % 100);
            }
            // The final offset represents the total compressed file size
            compressedFileSize = offset;

            writer.finalizeLength(1024 * 1024, 100);
            writer.doPrepare();
            Throwable t = writer.doCommit(null);
            if (t != null)
                throw new IOException(t);
        }

        // Write data to the Data.db file to match the compressed file size
        // This ensures dataFilePath.length() returns the correct value
        dataFile.createFileIfNotExists();
        try (java.io.RandomAccessFile raf = new java.io.RandomAccessFile(dataFile.toJavaIOFile(), "rw"))
        {
            raf.setLength(compressedFileSize);
        }

        CompressionMetadataCache.instance.clear();
    }

    @After
    public void tearDown()
    {
        DatabaseDescriptor.setCompressionMetadataCacheEnabled(originalCacheEnabled);
        
        if (dataFile != null && dataFile.exists())
            dataFile.delete();
        if (metadataFile != null && metadataFile.exists())
            metadataFile.delete();
            
        CompressionMetadataCache.instance.clear();
    }

    @Test
    public void testCreateWithCacheEnabled() throws IOException
    {
        CompressionMetadata metadata = CachedCompressionMetadata.create(
            dataFile, SliceDescriptor.NONE, false);
        
        assertThat(metadata).isInstanceOf(CachedCompressionMetadata.class);
        assertThat(metadata.parameters.chunkLength()).isEqualTo(16384);
        assertThat(metadata.hasOffsets()).isTrue();
    }

    @Test
    public void testCreateWithCacheDisabled() throws IOException
    {
        try
        {
            CompressionMetadataCache.instance.enable(false);

            CompressionMetadata metadata = CachedCompressionMetadata.create(
                dataFile, SliceDescriptor.NONE, false);

            // Should fall back to regular CompressionMetadata
            assertThat(metadata).isNotInstanceOf(CachedCompressionMetadata.class);
        }
        finally
        {
            // Re-enable for other tests
            CompressionMetadataCache.instance.enable(true);
        }
    }

    @Test
    public void testCreateWithSkipOffsets() throws IOException
    {
        CompressionMetadata metadata = CachedCompressionMetadata.create(
            dataFile, SliceDescriptor.NONE, true);
        
        // Should fall back to regular CompressionMetadata when skipOffsets=true
        assertThat(metadata).isNotInstanceOf(CachedCompressionMetadata.class);
    }

    @Test
    public void testChunkFor() throws IOException
    {
        CachedCompressionMetadata metadata = (CachedCompressionMetadata) 
            CachedCompressionMetadata.create(dataFile, SliceDescriptor.NONE, false);
        
        // Test first chunk
        Chunk chunk0 = metadata.chunkFor(0);
        assertThat(chunk0).isNotNull();
        assertThat(chunk0.offset).isEqualTo(0);
        
        // Test another chunk
        Chunk chunk1 = metadata.chunkFor(16384);
        assertThat(chunk1).isNotNull();
        assertThat(chunk1.offset).isGreaterThan(0);
        
        // Verify chunks are different
        assertThat(chunk1.offset).isNotEqualTo(chunk0.offset);
    }

    @Test
    public void testGetChunksForSections() throws IOException
    {
        CachedCompressionMetadata metadata = (CachedCompressionMetadata) 
            CachedCompressionMetadata.create(dataFile, SliceDescriptor.NONE, false);
        
        // Create test sections
        SSTableReader.PartitionPositionBounds section1 = 
            new SSTableReader.PartitionPositionBounds(0, 32768);
        SSTableReader.PartitionPositionBounds section2 = 
            new SSTableReader.PartitionPositionBounds(65536, 98304);
        
        Collection<SSTableReader.PartitionPositionBounds> sections = 
            Arrays.asList(section1, section2);
        
        Chunk[] chunks = metadata.getChunksForSections(sections);
        
        assertThat(chunks).isNotNull();
        assertThat(chunks.length).isGreaterThan(0);
        
        // Verify chunks are in order
        for (int i = 1; i < chunks.length; i++)
        {
            assertThat(chunks[i].offset).isGreaterThanOrEqualTo(chunks[i-1].offset);
        }
    }

    @Test
    public void testOffHeapSize()
    {
        CompressionMetadata metadata = CachedCompressionMetadata.create(
            dataFile, SliceDescriptor.NONE, false);
        
        // CachedCompressionMetadata should report 0 off-heap size
        // because memory is managed by the cache
        assertThat(metadata.offHeapSize()).isEqualTo(0);
    }

    @Test
    public void testClose()
    {
        CompressionMetadata metadata = CachedCompressionMetadata.create(
            dataFile, SliceDescriptor.NONE, false);
        
        // Close should not throw
        metadata.close();
        
        // Can close multiple times
        metadata.close();
    }

    @Test
    public void testHasOffsets()
    {
        CachedCompressionMetadata metadata = (CachedCompressionMetadata) 
            CachedCompressionMetadata.create(dataFile, SliceDescriptor.NONE, false);
        
        // Should always return true since cache provides offsets
        assertThat(metadata.hasOffsets()).isTrue();
    }

    @Test
    public void testCompressionParameters()
    {
        CompressionMetadata metadata = CachedCompressionMetadata.create(
            dataFile, SliceDescriptor.NONE, false);
        
        assertThat(metadata.parameters).isNotNull();
        assertThat(metadata.parameters.chunkLength()).isEqualTo(16384);
        assertThat(metadata.parameters.getSstableCompressor()).isInstanceOf(SnappyCompressor.class);
    }

    @Test
    public void testDataLength()
    {
        CompressionMetadata metadata = CachedCompressionMetadata.create(
            dataFile, SliceDescriptor.NONE, false);
        
        assertThat(metadata.dataLength).isEqualTo(1024 * 1024);
    }

    @Test
    public void testCompressedFileLength()
    {
        CompressionMetadata metadata = CachedCompressionMetadata.create(
            dataFile, SliceDescriptor.NONE, false);
        
        assertThat(metadata.compressedFileLength).isGreaterThan(0);
    }

    @Test
    public void testChunkLength()
    {
        CompressionMetadata metadata = CachedCompressionMetadata.create(
            dataFile, SliceDescriptor.NONE, false);
        
        assertThat(metadata.chunkLength()).isEqualTo(16384);
    }

    @Test
    public void testMultipleInstances() throws IOException
    {
        // Create multiple instances for same file
        CompressionMetadata metadata1 = CachedCompressionMetadata.create(
            dataFile, SliceDescriptor.NONE, false);
        CompressionMetadata metadata2 = CachedCompressionMetadata.create(
            dataFile, SliceDescriptor.NONE, false);
        
        // Both should work correctly
        Chunk chunk1 = metadata1.chunkFor(0);
        Chunk chunk2 = metadata2.chunkFor(0);
        
        assertThat(chunk1.offset).isEqualTo(chunk2.offset);
        assertThat(chunk1.length).isEqualTo(chunk2.length);
        
        // Close both
        metadata1.close();
        metadata2.close();
    }

    @Test
    public void testCacheHitOnRepeatedAccess() throws IOException
    {
        CachedCompressionMetadata metadata = (CachedCompressionMetadata) 
            CachedCompressionMetadata.create(dataFile, SliceDescriptor.NONE, false);
        
        CompressionMetadataCacheMetrics metrics = 
            CompressionMetadataCache.instance.getMetricsForTesting();
        
        long initialHits = metrics.hits();
        
        // First access - cache miss
        metadata.chunkFor(0);
        
        // Second access to same block - cache hit
        metadata.chunkFor(16384);
        
        assertThat(metrics.hits()).isGreaterThan(initialHits);
    }
}
