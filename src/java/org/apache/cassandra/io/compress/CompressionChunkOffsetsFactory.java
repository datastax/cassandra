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

import java.io.EOFException;
import java.io.IOException;
import java.util.Locale;

import com.google.common.base.Preconditions;

import io.netty.util.internal.PlatformDependent;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.Memory;
import org.apache.cassandra.io.util.TrackedDataInputPlus;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.config.CassandraRelevantProperties.COMPRESSION_CHUNK_OFFSETS_FACTORY;
import static org.apache.cassandra.config.CassandraRelevantProperties.COMPRESSION_CHUNK_OFFSETS_TYPE;
import static org.apache.cassandra.io.compress.CompressionMetadata.NATIVE_MEMORY_USAGE;

/**
 * Factory for {@link CompressionChunkOffsets} implementations.
 * <p>
 * The implementation is selected by {@link CassandraRelevantProperties#COMPRESSION_CHUNK_OFFSETS_TYPE}:
 * </p>
 * <ul>
 *     <li>{@code in_memory} (default): use {@link CompressionChunkOffsets.InMemory}, loading all offsets into a single
 *     Memory.LongArray.</li>
 *     <li>{@code mmap}: use {@link CompressionChunkOffsets.Mmap}, which memory-maps the offsets section of the
 *     compression info file. Gives close to in-memory performance while letting the OS reclaim pages under memory
 *     pressure. Requires the file to be fully present on local disk.</li>
 *     <li>{@code block_cache}: use {@link CompressionChunkOffsets.BlockCache} sized by
 *     {@link CassandraRelevantProperties#COMPRESSION_CHUNK_OFFSETS_BLOCK_CACHE_SIZE}.</li>
 * </ul>
 */
public interface CompressionChunkOffsetsFactory
{
    CompressionChunkOffsetsFactory instance = COMPRESSION_CHUNK_OFFSETS_FACTORY.isPresent()
                                              ? FBUtilities.construct(COMPRESSION_CHUNK_OFFSETS_FACTORY.getString(), "Compression Chunk Offsets Factory")
                                              : new CompressionChunkOffsetsFactory() {};

    enum Type
    {
        IN_MEMORY, MMAP, BLOCK_CACHE
    }

    /**
     * @return the configured compression chunk offsets type.
     * @throws ConfigurationException if the configured value is not a recognised type.
     */
    static Type type()
    {
        String value = COMPRESSION_CHUNK_OFFSETS_TYPE.getString();
        try
        {
            return Type.valueOf(value.trim().toUpperCase(Locale.ROOT));
        }
        catch (IllegalArgumentException e)
        {
            throw new ConfigurationException(String.format("Invalid value '%s' for %s. Valid values are: in_memory, mmap, block_cache",
                                                           value, COMPRESSION_CHUNK_OFFSETS_TYPE.getKey()));
        }
    }

    /**
     * Create a {@link CompressionChunkOffsets} implementation from the provided compression metadata stream.
     *
     * @param indexFilePath        path to the compression info file
     * @param input                stream positioned at the start of chunk offsets
     * @param offsetsStart         position in the metadata file where chunk offsets begin
     * @param startIndex           first chunk index to include (inclusive)
     * @param endIndex             last chunk index to include (exclusive)
     * @param chunkCount           total number of chunks in the file
     * @param compressedFileLength compressed data file length
     * @param readerType           read-time or write-time access mode
     * @return a chunk offsets implementation
     */
    default CompressionChunkOffsets getInstance(File indexFilePath, TrackedDataInputPlus input, long offsetsStart,
                                                int startIndex, int endIndex,
                                                int chunkCount, long compressedFileLength,
                                                CompressionMetadataReaderType readerType) throws IOException
    {
        if (chunkCount == 0)
            return new CompressionChunkOffsets.Empty();

        switch (type())
        {
            case MMAP:
                return new CompressionChunkOffsets.Mmap(indexFilePath, offsetsStart, startIndex, endIndex - startIndex,
                                                        endIndex, chunkCount, compressedFileLength, readerType);
            case BLOCK_CACHE:
                CompressionChunkOffsetCache cache = CompressionChunkOffsetCache.get();
                return new CompressionChunkOffsets.BlockCache(indexFilePath, offsetsStart, startIndex, endIndex - startIndex,
                                                              endIndex, chunkCount, compressedFileLength, readerType, cache);
            case IN_MEMORY:
            default:
                return createInMemoryOffsets(indexFilePath, input, startIndex, endIndex, chunkCount, compressedFileLength);
        }
    }

    /**
     * Creates a {@link CompressionChunkOffsets} for metadata when opening the CompressionMetadata.Writer for early-open
     * SSTableReader or final SSTableReader.
     * <p>
     * Note that compression info file is written when completing the sstable writing. During early-open, we expect
     * the compression info file is not present; in that case this method always uses in-memory offsets.
     * <p>
     * When it's called after completing sstable writing with compression info, in-memory limit admission is applied and
     * the writer's in-memory offsets may be released if on-disk offsets are selected.
     * </p>
     * @param indexFilePath path to the compression info file
     * @param memoryChunkOffsets in-memory offsets buffer built by the writer
     * @param offsetsStart position of the offsets table in the compression info file
     * @param startIndex first chunk index to include (inclusive)
     * @param endIndex last chunk index to include (exclusive)
     * @param chunkCount total number of chunks in the file
     * @param compressedFileLength compressed data file length
     * @param isCompressionInfoWritten whether the compression info file is fully written
     * @return a chunk offsets implementation for use in early-open or final SSTableReader
     */
    default CompressionChunkOffsets getInstanceOnWriterComplete(File indexFilePath, Memory.LongArray memoryChunkOffsets,
                                                                long offsetsStart, int startIndex, int endIndex, int chunkCount,
                                                                long compressedFileLength, boolean isCompressionInfoWritten) throws IOException
    {
        if (chunkCount == 0)
            return new CompressionChunkOffsets.Empty();

        // When CompressionMetadata.Writer completes, the opened CompressionMetadata is used into the final SSTableReader.
        // This is considered READ_TIME as sstable has completed writing and the file will be used during READ_TIME
        CompressionMetadataReaderType readerType = CompressionMetadataReaderType.READ_TIME;

        // During early open, compression info file is not written yet, so use in-memory offsets
        if (!isCompressionInfoWritten)
        {
            NATIVE_MEMORY_USAGE.addAndGet(memoryChunkOffsets.memoryUsed());
            return new CompressionChunkOffsets.InMemory(memoryChunkOffsets, compressedFileLength);
        }

        switch (type())
        {
            case MMAP:
                // Release writer's in-memory offsets since the offsets are now read from the mapped file
                memoryChunkOffsets.close();
                return new CompressionChunkOffsets.Mmap(indexFilePath, offsetsStart, startIndex, endIndex - startIndex,
                                                        endIndex, chunkCount, compressedFileLength, readerType);
            case BLOCK_CACHE:
                CompressionChunkOffsetCache cache = CompressionChunkOffsetCache.get();
                // Release writer's in-memory offsets since we'll use block-cached implementation.
                memoryChunkOffsets.close();
                return new CompressionChunkOffsets.BlockCache(indexFilePath, offsetsStart, startIndex, endIndex - startIndex,
                                                              endIndex, chunkCount, compressedFileLength, readerType, cache);
            case IN_MEMORY:
            default:
                NATIVE_MEMORY_USAGE.addAndGet(memoryChunkOffsets.memoryUsed());
                return new CompressionChunkOffsets.InMemory(memoryChunkOffsets, compressedFileLength);
        }
    }



    static CompressionChunkOffsets createInMemoryOffsets(File indexFilePath, TrackedDataInputPlus input,
                                                         int startIndex, int endIndex, int chunkCount,
                                                         long compressedFileLength)
    {
        Preconditions.checkState(startIndex < chunkCount, "The start index %s has to be < chunk count %s", startIndex, chunkCount);
        Preconditions.checkState(endIndex <= chunkCount, "The end index %s has to be <= chunk count %s", endIndex, chunkCount);
        Preconditions.checkState(startIndex <= endIndex, "The start index %s has to be < end index %s", startIndex, endIndex);

        int chunksToRead = endIndex - startIndex;
        if (chunksToRead == 0)
            return new CompressionChunkOffsets.Empty();

        Memory.LongArray offsets = new Memory.LongArray(chunksToRead);
        long i = 0;
        try
        {
            input.skipBytes(startIndex * 8);
            long lastOffset;
            for (i = 0; i < chunksToRead; i++)
            {
                lastOffset = input.readLong();
                offsets.set(i, lastOffset);
            }

            // We adjust the compressed file length to store the position after the last chunk just to be able to
            // calculate the offset of the chunk next to the last one (in order to calculate the length of the last chunk).
            // Obvously, we could use the compressed file length for that purpose but unfortunately, sometimes there is
            // an empty chunk added to the end of the file thus we cannot rely on the file length.
            lastOffset = endIndex < chunkCount ? input.readLong() - offsets.get(0) : compressedFileLength;
            NATIVE_MEMORY_USAGE.getAndAdd(offsets.memoryUsed());
            return new CompressionChunkOffsets.InMemory(offsets, lastOffset);
        }
        catch (EOFException e)
        {
            offsets.close();
            String msg = String.format("Corrupted Index File %s: read %d but expected at least %d chunks.", input, i, chunksToRead);
            throw new CorruptSSTableException(new IOException(msg, e), indexFilePath.toPath());
        }
        catch (IOException e)
        {
            offsets.close();
            throw new FSReadError(e, indexFilePath.toPath());
        }
        catch (RuntimeException e)
        {
            offsets.close();
            throw e;
        }
    }
}
