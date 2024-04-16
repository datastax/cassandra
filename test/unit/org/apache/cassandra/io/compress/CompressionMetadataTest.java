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
import java.util.function.Consumer;

import org.junit.Test;

import org.apache.cassandra.io.compress.CompressionMetadata.Chunk;
import org.apache.cassandra.io.sstable.format.SSTableReader.PartitionPositionBounds;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.SliceDescriptor;
import org.apache.cassandra.schema.CompressionParams;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

public class CompressionMetadataTest
{
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

    private void checkMetadata(CompressionMetadata metadata, long expectedDataLength, long expectedCompressedFileLimit, long expectedOffHeapSize)
    {
        assertThat(metadata.dataLength).isEqualTo(expectedDataLength);
        assertThat(metadata.compressedFileLength).isEqualTo(expectedCompressedFileLimit);
        assertThat(metadata.chunkLength()).isEqualTo(16);
        assertThat(metadata.parameters.chunkLength()).isEqualTo(16);
        assertThat(metadata.parameters.getSstableCompressor().getClass()).isEqualTo(SnappyCompressor.class);
        assertThat(metadata.offHeapSize()).isEqualTo(expectedOffHeapSize);
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
        try (CompressionMetadata md = new CompressionMetadata(f, compressedSize, true, new SliceDescriptor(from, to, 16)))
        {
            check.accept(md);
        }
    }

    @Test
    public void chunkFor() throws IOException
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
}