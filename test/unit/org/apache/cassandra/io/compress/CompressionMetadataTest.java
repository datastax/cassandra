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

import org.junit.Test;

import org.apache.cassandra.io.util.File;
import org.apache.cassandra.schema.CompressionParams;

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
            CompressionMetadata.Chunk chunk = metadata.chunkFor(offset);
            assertThat(chunk.offset).isEqualTo(expectedOffset);
            assertThat(chunk.length).isEqualTo(expectedLength);
        }
    }

    @Test
    public void chunkFor() throws IOException
    {
        CompressionMetadata lessThanOneChunk = createMetadata(10, 7, 0);
        checkMetadata(lessThanOneChunk, 10, 7, 8);
        assertChunks(lessThanOneChunk, 0, 10, 0, 3);

        CompressionMetadata oneChunk = createMetadata(16, 9, 0);
        checkMetadata(oneChunk, 16, 9, 8);
        assertChunks(oneChunk, 0, 16, 0, 5);

        CompressionMetadata moreThanOneChunk = createMetadata(20, 15, 0, 9);
        checkMetadata(moreThanOneChunk, 20, 15, 16);
        assertChunks(moreThanOneChunk, 0, 16, 0, 5);
        assertChunks(moreThanOneChunk, 16, 20, 9, 2);

        CompressionMetadata extraEmptyChunk = createMetadata(20, 17, 0, 9, 15);
        checkMetadata(extraEmptyChunk, 20, 15, 16);
        assertChunks(extraEmptyChunk, 0, 16, 0, 5);
        assertChunks(extraEmptyChunk, 16, 20, 9, 2);
    }
}