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

package org.apache.cassandra.test.microbench;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.io.compress.CompressionChunkOffsets;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.schema.CompressionParams;

@BenchmarkMode({Mode.Throughput, Mode.AverageTime})
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 1, time = 15)
@Measurement(iterations = 3, time = 15)
@Fork(value = 2, jvmArgsAppend = { "-Xms6G", "-Xmx6G"})
@Threads(4)
public class CompressionChunkOffsetsBench
{
    @State(Scope.Benchmark)
    public static class BenchmarkState
    {
        @Param({ "IN_MEMORY", "ON_DISK"})
        public String mode;

        @Param({ "1000000"})
        public int chunkCount;

        @Param({ "0", "256"})
        public int cacheSizeMb;

        private static final int CHUNK_LENGTH = 16;
        private static final int COMPRESSED_LEN = 12;
        private static final int CHECKSUM_LEN = 4;
        private static final int OFFSETS_TO_READ = 1024 * 512;

        private CompressionMetadata metadata;
        private File metadataFile;
        private long[] positions;
        private int positionIdx;

        @Setup(Level.Trial)
        public void setup() throws Exception
        {
            DatabaseDescriptor.toolInitialization();
            CassandraRelevantProperties.COMPRESSION_CHUNK_OFFSETS_ON_DISK_CACHE_SIZE.setString(cacheSizeMb + "MiB");
            CassandraRelevantProperties.COMPRESSION_CHUNK_OFFSETS_TYPE.setString(mode);

            metadataFile = createMetadataFile();
            long compressedFileLength = computeCompressedFileLength();
            metadata = new CompressionMetadata(metadataFile, compressedFileLength, true);
            positions = buildPositions();
            positionIdx = 0;
        }

        @TearDown(Level.Trial)
        public void teardown() throws Exception
        {
            if (metadata != null)
                metadata.close();

            CassandraRelevantProperties.COMPRESSION_CHUNK_OFFSETS_ON_DISK_CACHE_SIZE.reset();
            CassandraRelevantProperties.COMPRESSION_CHUNK_OFFSETS_FACTORY.reset();
            CassandraRelevantProperties.COMPRESSION_CHUNK_OFFSETS_TYPE.reset();
        }

        @Setup(Level.Iteration)
        public void prewarmCache()
        {
            if (!(CompressionChunkOffsets.Type.ON_DISK.toString().equals(mode) && cacheSizeMb > 0))
                return;

            for (long position : positions)
                metadata.chunkFor(position);
        }

        private File createMetadataFile() throws IOException
        {
            Path path = Files.createTempFile("compression_metadata_bench", ".db");
            CompressionParams params = CompressionParams.snappy(CHUNK_LENGTH);
            try (CompressionMetadata.Writer writer = CompressionMetadata.Writer.open(params, new File(path)))
            {
                long offset = 0;
                for (int i = 0; i < chunkCount; i++)
                {
                    writer.addOffset(offset);
                    // mock offset
                    offset += COMPRESSED_LEN + CHECKSUM_LEN;
                }
                writer.finalizeLength((long) chunkCount * CHUNK_LENGTH, chunkCount);
                writer.prepareToCommit();
                Throwable t = writer.commit(null);
                if (t != null)
                    throw new IOException(t);
            }
            return new File(path);
        }

        private long computeCompressedFileLength()
        {
            return (long) chunkCount * (COMPRESSED_LEN + CHECKSUM_LEN);
        }

        private long[] buildPositions()
        {
            long[] values = new long[OFFSETS_TO_READ];
            long maxPosition = (long) chunkCount * CHUNK_LENGTH;
            long step = Math.max(1L, maxPosition / OFFSETS_TO_READ);
            long position = 0;
            for (int i = 0; i < OFFSETS_TO_READ; i++)
            {
                values[i] = position;
                position = Math.min(maxPosition - 1, position + step);
            }
            return values;
        }

        long nextPosition()
        {
            // loop through positions
            int idx = positionIdx++ & (OFFSETS_TO_READ - 1);
            return positions[idx];
        }
    }

    @Benchmark
    public void chunkForRandom(BenchmarkState state, Blackhole blackhole)
    {
//        Benchmark                                    (chunkCount)              (mode)   Mode  Cnt    Score    Error   Units
//        CompressionChunkOffsetsBench.chunkForRandom       1000000           IN_MEMORY  thrpt    6  201.053 ± 58.986  ops/us
//        CompressionChunkOffsetsBench.chunkForRandom       1000000             ON_DISK  thrpt    6    1.091 ±  0.141  ops/us
//        CompressionChunkOffsetsBench.chunkForRandom       1000000  ON_DISK_WITH_CACHE  thrpt    6    5.926 ±  1.009  ops/us
//        CompressionChunkOffsetsBench.chunkForRandom       1000000           IN_MEMORY   avgt    6    0.020 ±  0.005   us/op
//        CompressionChunkOffsetsBench.chunkForRandom       1000000             ON_DISK   avgt    6    3.767 ±  0.286   us/op
//        CompressionChunkOffsetsBench.chunkForRandom       1000000  ON_DISK_WITH_CACHE   avgt    6    0.705 ±  0.183   us/op
        CompressionMetadata.Chunk chunk = state.metadata.chunkFor(state.nextPosition());
        blackhole.consume(chunk.offset);
        blackhole.consume(chunk.length);
    }
}
