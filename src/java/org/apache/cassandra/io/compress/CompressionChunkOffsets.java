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

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;

import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.Memory;
import org.apache.cassandra.io.util.TrackedDataInputPlus;
import org.apache.cassandra.utils.concurrent.Ref;

import static org.apache.cassandra.io.compress.CompressionMetadata.NATIVE_MEMORY_USAGE;

public interface CompressionChunkOffsets extends AutoCloseable
{
    enum Type
    {
        IN_MEMORY,
        ON_DISK,
        ON_DISK_WITH_CACHE;
    }

    long get(int index);

    int size();

    long memoryUsed();

    void addTo(Ref.IdentityCollection identities);

    long compressedFileLength();

    void close();

    static CompressionChunkOffsets getInstance(File indexFilePath, TrackedDataInputPlus input, int startIndex, int endIndex,
                                               long compressedFileLength, Type type, CompressionMetadataReaderType readerType)
    {
        switch (type)
        {
            case IN_MEMORY:          return createInMemoryOffsets(indexFilePath, input, startIndex, endIndex, compressedFileLength);
            case ON_DISK:            return createOnDiskOffsets(indexFilePath, input, startIndex, endIndex, compressedFileLength, readerType, null);
            case ON_DISK_WITH_CACHE: return createOnDiskOffsets(indexFilePath, input, startIndex, endIndex, compressedFileLength, readerType, CompressionChunkOffsetCache.get());
            default: throw new UnsupportedOperationException(type + " is not supported");
        }
    }

    static CompressionChunkOffsets createOnDiskOffsets(File indexFilePath,
                                                       TrackedDataInputPlus stream,
                                                       int startIndex,
                                                       int endIndex,
                                                       long compressedFileLength,
                                                       CompressionMetadataReaderType readerType,
                                                       @Nullable CompressionChunkOffsetCache cache)
    {

        final int chunkCount;
        try
        {
            chunkCount = stream.readInt();
            if (chunkCount < 0)
                throw new IOException("Compressed file with 0 chunks encountered: " + startIndex);
        }
        catch (IOException e)
        {
            throw new FSReadError(e, indexFilePath);
        }

        long offsetsStart = stream.getBytesRead();

        Preconditions.checkState(chunkCount >= 0, "Negative chunk count %s for %s", chunkCount, indexFilePath);
        Preconditions.checkState(startIndex < chunkCount || chunkCount == 0,
                                 "The start index %s has to be < chunk count %s", startIndex, chunkCount);
        Preconditions.checkState(endIndex <= chunkCount,
                                 "The end index %s has to be <= chunk count %s", endIndex, chunkCount);
        Preconditions.checkState(startIndex <= endIndex,
                                 "The start index %s has to be < end index %s", startIndex, endIndex);
        int chunksToRead = Math.max(0, endIndex - startIndex);
        if (chunksToRead == 0)
        {
            return new Empty();
        }
        else
        {
            // We adjust the compressed file length to store the position after the last chunk just to be able to
            // calculate the offset of the chunk next to the last one (in order to calculate the length of the last chunk).
            // Obvously, we could use the compressed file length for that purpose but unfortunately, sometimes there is
            // an empty chunk added to the end of the file thus we cannot rely on the file length.
            long lastOffset = endIndex < chunkCount
                              ? CompressionChunkOffsetCache.get().getOffset(indexFilePath, offsetsStart, endIndex, chunkCount, readerType)
                                - CompressionChunkOffsetCache.get().getOffset(indexFilePath, offsetsStart, startIndex, chunkCount, readerType)
                              : compressedFileLength;
            return new CompressionChunkOffsets.OnDiskChunkOffsets(indexFilePath,
                                                                  offsetsStart,
                                                                  startIndex,
                                                                  chunksToRead,
                                                                  chunkCount,
                                                                  lastOffset,
                                                                  readerType,
                                                                  cache);
        }
    }

    static CompressionChunkOffsets createInMemoryOffsets(File indexFilePath, TrackedDataInputPlus input, int startIndex, int endIndex, long compressedFileLength)
    {

        final Memory.LongArray offsets;
        final int chunkCount;
        try
        {
            chunkCount = input.readInt();
            if (chunkCount < 0)
                throw new IOException("Compressed file with 0 chunks encountered: " + input);
        }
        catch (IOException e)
        {
            throw new FSReadError(e, indexFilePath.toPath());
        }

        if (chunkCount == 0)
            return new Empty();

        Preconditions.checkState(startIndex < chunkCount, "The start index %s has to be < chunk count %s", startIndex, chunkCount);
        Preconditions.checkState(endIndex <= chunkCount, "The end index %s has to be <= chunk count %s", endIndex, chunkCount);
        Preconditions.checkState(startIndex <= endIndex, "The start index %s has to be < end index %s", startIndex, endIndex);

        int chunksToRead = endIndex - startIndex;

        if (chunksToRead == 0)
            return new Empty();

        offsets = new Memory.LongArray(chunksToRead);
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

            lastOffset = endIndex < chunkCount ? input.readLong() - offsets.get(0) : compressedFileLength;
            NATIVE_MEMORY_USAGE.addAndGet(offsets.memoryUsed());
            return new InMemoryCompressionChunkOffsets(offsets, lastOffset);
        }
        catch (EOFException e)
        {
            offsets.close();
            String msg = String.format("Corrupted Index File %s: read %d but expected at least %d chunks.",
                                       input, i, chunksToRead);
            throw new CorruptSSTableException(new IOException(msg, e), indexFilePath.toPath());
        }
        catch (IOException e)
        {
            offsets.close();
            throw new FSReadError(e, indexFilePath.toPath());
        }
    }

    class Empty implements CompressionChunkOffsets
    {
        public long get(int index)
        {
            throw new IndexOutOfBoundsException("Chunk index " + index + " out of bounds for empty offsets");
        }

        public int size()
        {
            return 0;
        }

        public long memoryUsed()
        {
            return 0;
        }

        public void addTo(Ref.IdentityCollection identities)
        {
        }

        @Override
        public long compressedFileLength()
        {
            return 0;
        }

        public void close()
        {
        }
    }

    class InMemoryCompressionChunkOffsets implements CompressionChunkOffsets
    {
        private final Memory.LongArray offsets;
        private final long compressedOffsetLength;

        public InMemoryCompressionChunkOffsets(Memory.LongArray offsets, long compressedOffsetLength)
        {
            this.offsets = offsets;
            this.compressedOffsetLength = compressedOffsetLength;
        }

        public long get(int index)
        {
            return offsets.get(index);
        }

        public int size()
        {
            return Math.toIntExact(offsets.size());
        }

        public long memoryUsed()
        {
            return offsets.memoryUsed();
        }

        public void addTo(Ref.IdentityCollection identities)
        {
            if (offsets.memory != null)
                identities.add(offsets.memory);
        }

        @Override
        public long compressedFileLength()
        {
            return compressedOffsetLength;
        }

        public void close()
        {
            offsets.close();
        }
    }

    class OnDiskChunkOffsets implements CompressionChunkOffsets
    {
        private final File file;
        private final long offsetsStart;
        private final int baseChunkIndex;
        private final int size;
        private final int chunkCount;
        private final CompressionMetadataReaderType readerType;
        private final long compressedFileLength;
        private final CompressionChunkOffsetCache cache;

        public OnDiskChunkOffsets(File file,
                                  long offsetsStart,
                                  int baseChunkIndex,
                                  int size,
                                  int chunkCount,
                                  long compressedFileLength,
                                  CompressionMetadataReaderType readerType,
                                  @Nullable CompressionChunkOffsetCache cache)
        {
            this.file = file;
            this.offsetsStart = offsetsStart;
            this.baseChunkIndex = baseChunkIndex;
            this.size = size;
            this.chunkCount = chunkCount;
            this.readerType = readerType;
            this.compressedFileLength = compressedFileLength;
            this.cache = cache;
        }

        public long get(int index)
        {
            int absoluteIndex = baseChunkIndex + index;
            // TODO use the cache if provided in constructor, otherwise load from on-disk data
            return CompressionChunkOffsetCache.get().getOffset(file, offsetsStart, absoluteIndex, chunkCount, readerType);
        }

        public int size()
        {
            return size;
        }

        public long memoryUsed()
        {
            return 0;
        }

        public void addTo(Ref.IdentityCollection identities)
        {
            // TODO maybe clear from cache
        }

        @Override
        public long compressedFileLength()
        {
            return compressedFileLength;
        }

        public void close()
        {
        }
    }
}
