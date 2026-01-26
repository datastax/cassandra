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
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileInputStreamPlus;
import org.apache.cassandra.io.util.SliceDescriptor;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.utils.concurrent.Ref;

/**
 * Cached implementation of CompressionMetadata that uses CompressionMetadataCache
 * for chunk offset lookups instead of loading all offsets into memory.
 * 
 * This implementation is always used when the cache is enabled, following the
 * "always-cache" approach for predictable memory usage.
 */
public class CachedCompressionMetadata extends CompressionMetadata
{
    private static final Logger logger = LoggerFactory.getLogger(CachedCompressionMetadata.class);

    private final CompressionMetadataCache cache;
    private final int chunkLengthBits;

    /**
     * Create a CachedCompressionMetadata instance.
     * This constructor reads only the metadata header, not the chunk offsets.
     */
    private CachedCompressionMetadata(File indexFilePath,
                                     long compressedLength,
                                     CompressionParams parameters,
                                     long dataLength,
                                     int chunkLengthBits)
    {
        // Call parent constructor with null offsets (we'll use cache instead)
        super(indexFilePath, parameters, dataLength, compressedLength);
        this.cache = CompressionMetadataCache.instance;
        this.chunkLengthBits = chunkLengthBits;
    }

    /**
     * Factory method to create CachedCompressionMetadata.
     * This is the entry point that will be called from FileHandle.Builder.
     *
     * @param dataFilePath Path to the compressed data file
     * @param sliceDescriptor Slice descriptor for partial reads
     * @param skipOffsets Whether to skip loading offsets (for encryption-only mode)
     * @return CachedCompressionMetadata instance
     */
    public static CompressionMetadata create(File dataFilePath, SliceDescriptor sliceDescriptor, boolean skipOffsets)
    {
        // If cache is not enabled, fall back to regular CompressionMetadata
        if (!CompressionMetadataCache.instance.isEnabled())
        {
            logger.info("Cache not enabled, using regular CompressionMetadata for {}", dataFilePath);
            return CompressionMetadata.read(dataFilePath, sliceDescriptor, skipOffsets);
        }

        // If skipOffsets is true (encryption-only mode), use regular implementation
        if (skipOffsets)
        {
            logger.info("Skip offsets requested, using regular CompressionMetadata for {}", dataFilePath);
            return CompressionMetadata.read(dataFilePath, sliceDescriptor, skipOffsets);
        }

        try
        {
            logger.info("Creating CachedCompressionMetadata for {}", dataFilePath);
            return createCached(dataFilePath, sliceDescriptor);
        }
        catch (Exception e)
        {
            logger.warn("Failed to create CachedCompressionMetadata for {}, falling back to regular", dataFilePath, e);
            return CompressionMetadata.read(dataFilePath, sliceDescriptor, skipOffsets);
        }
    }

    /**
     * Create a CachedCompressionMetadata by reading only the header from the metadata file.
     */
    private static CachedCompressionMetadata createCached(File dataFilePath, SliceDescriptor sliceDescriptor) throws IOException
    {
        // Use Descriptor API to get compression info file path
        Descriptor descriptor = Descriptor.fromFilename(dataFilePath);
        File indexFilePath = descriptor.fileFor(Component.COMPRESSION_INFO);
        
        try (FileInputStreamPlus stream = indexFilePath.newInputStream())
        {
            // Read compression parameters from header
            String compressorName = stream.readUTF();
            int optionCount = stream.readInt();
            Map<String, String> options = new HashMap<>(optionCount);
            for (int i = 0; i < optionCount; i++)
            {
                String key = stream.readUTF();
                String value = stream.readUTF();
                options.put(key, value);
            }
            
            int chunkLength = stream.readInt();
            int maxCompressedSize = stream.readInt();
            long dataLength = stream.readLong();
            
            // Create compression parameters
            CompressionParams parameters;
            try
            {
                parameters = new CompressionParams(compressorName, chunkLength, maxCompressedSize, options);
            }
            catch (ConfigurationException e)
            {
                throw new RuntimeException("Cannot create CompressionParams for stored parameters", e);
            }
            
            // Calculate chunk length bits
            assert Integer.bitCount(chunkLength) == 1;
            int chunkLengthBits = Integer.numberOfTrailingZeros(chunkLength);
            
            // Handle slice descriptor
            long uncompressedLength = sliceDescriptor.exists() 
                ? sliceDescriptor.dataEnd - sliceDescriptor.sliceStart 
                : dataLength;
            
            long compressedLength = dataFilePath.length();
            
            logger.debug("Created CachedCompressionMetadata for {} (dataLength={}, chunkLength={})",
                        dataFilePath, uncompressedLength, chunkLength);
            
            return new CachedCompressionMetadata(indexFilePath, compressedLength, parameters, 
                                                uncompressedLength, chunkLengthBits);
        }
        catch (IOException e)
        {
            throw new CorruptSSTableException(e, indexFilePath);
        }
    }

    @Override
    public boolean hasOffsets()
    {
        // Return true because we can provide offsets (via cache)
        // This allows assertions in hot path methods (chunkFor, getChunksForSections) to pass
        return true;
    }

    @Override
    public Chunk chunkFor(long uncompressedDataPosition)
    {
        try
        {
            int chunkIndex = (int) (uncompressedDataPosition >> chunkLengthBits);
            return cache.getChunk(indexFilePath.toPath(), chunkIndex);
        }
        catch (IOException e)
        {
            throw new CorruptSSTableException(e, indexFilePath);
        }
    }

    @Override
    int chunkIndex(long uncompressedDataPosition)
    {
        return (int) (uncompressedDataPosition >> chunkLengthBits);
    }

    @Override
    public long getTotalSizeForSections(Collection<SSTableReader.PartitionPositionBounds> sections)
    {
        assert hasOffsets();

        // Calculate total size by fetching chunks from cache
        // This is used during streaming to calculate transfer size
        long size = 0;
        int lastIncludedChunkIdx = -1;

        try
        {
            for (SSTableReader.PartitionPositionBounds section : sections)
            {
                int sectionStartIdx = Math.max(chunkIndex(section.lowerPosition), lastIncludedChunkIdx + 1);
                int sectionEndIdx = chunkIndex(section.upperPosition - 1);

                for (int idx = sectionStartIdx; idx <= sectionEndIdx; idx++)
                {
                    Chunk chunk = cache.getChunk(indexFilePath.toPath(), idx);
                    size += chunk.length + 4; // +4 for checksum
                }
                lastIncludedChunkIdx = sectionEndIdx;
            }
            return size;
        }
        catch (IOException e)
        {
            throw new CorruptSSTableException(e, indexFilePath);
        }
    }

    @Override
    public Chunk[] getChunksForSections(Collection<SSTableReader.PartitionPositionBounds> sections)
    {
        assert hasOffsets();

        // Collect all chunk indices first - use TreeSet to deduplicate and sort
        SortedSet<Integer> chunkIndices = new TreeSet<>();
        for (SSTableReader.PartitionPositionBounds section : sections)
        {
            int sectionStartIdx = chunkIndex(section.lowerPosition);
            int sectionEndIdx = chunkIndex(section.upperPosition - 1); // we need to include the last byte of the section but not the upper position (which is excluded)

            for (int idx = sectionStartIdx; idx <= sectionEndIdx; idx++)
                chunkIndices.add(idx);
        }

        // Batch fetch chunks from cache
        // Note: getChunks() expects sorted indices and returns chunks in the same order
        // Since chunk indices are monotonically increasing, chunk offsets are also monotonically increasing
        // Therefore, no additional sorting is needed
        try
        {
            int[] indices = new int[chunkIndices.size()];
            int i = 0;
            for (Integer idx : chunkIndices)
                indices[i++] = idx;

            return cache.getChunks(indexFilePath.toPath(), indices);
        }
        catch (IOException e)
        {
            throw new CorruptSSTableException(e, indexFilePath);
        }
    }

    @Override
    public long offHeapSize()
    {
        // Cached metadata doesn't keep offsets in memory per-SSTable
        // Memory is managed by the cache and shared across SSTables
        return 0;
    }

    @Override
    public void addTo(Ref.IdentityCollection identities)
    {
        // No-op: cached metadata doesn't manage per-SSTable off-heap memory
        // Memory is managed by the cache and shared across SSTables
    }

    @Override
    public void close()
    {
        // Don't call super.close() - it assumes hasOffsets() == true means chunkOffsets != null
        // We override hasOffsets() to return true (semantic: "can provide offsets via cache")
        // but chunkOffsets is null (we don't keep offsets in memory per-SSTable)
        // Cache entries are shared across SSTables and managed by LRU eviction
        // No per-SSTable resources to clean up
    }
}
