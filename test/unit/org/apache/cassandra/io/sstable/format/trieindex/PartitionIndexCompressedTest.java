/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.io.sstable.format.trieindex;


import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.io.compress.CompressedSequentialWriter;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.io.util.SequentialWriterOption;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.schema.TableMetadata;

/**
 * Verify the index/page-aware infrastructure also works with compression. This is not used anywhere
 * (superseded by EncryptedSequentialWriter/ChunkReader).
 */
@RunWith(Parameterized.class)
public class PartitionIndexCompressedTest extends PartitionIndexTest
{
    class JumpingCompressedFile extends CompressedSequentialWriter
    {
        long[] cutoffs;
        long[] offsets;

        JumpingCompressedFile(File file, SequentialWriterOption option, long... cutoffsAndOffsets)
        {
            super(file, new File(file.toPath() + ".offsets"), null,
                  option, CompressionParams.lz4(4096, 4096),
                  new MetadataCollector(TableMetadata.minimal("k", "s").comparator));
            assert (cutoffsAndOffsets.length & 1) == 0;
            cutoffs = new long[cutoffsAndOffsets.length / 2];
            offsets = new long[cutoffs.length];
            for (int i = 0; i < cutoffs.length; ++i)
            {
                cutoffs[i] = cutoffsAndOffsets[i * 2];
                offsets[i] = cutoffsAndOffsets[i * 2 + 1];
            }
        }

        @Override
        public long position()
        {
            return jumped(super.position(), cutoffs, offsets);
        }
    }


    @Override
    protected SequentialWriter makeWriter(File file)
    {

        return new CompressedSequentialWriter(file,
                new File(file.toPath() + ".offsets"),
                null,
                SequentialWriterOption
                        .newBuilder()
                        .finishOnClose(false)
                        .build(),
                CompressionParams.lz4(4096, 4096),
                new MetadataCollector(TableMetadata.minimal("k", "s").comparator));
    }

    @Override
    public SequentialWriter makeJumpingWriter(File file, long[] cutoffsAndOffsets)
    {
        return new JumpingCompressedFile(file, SequentialWriterOption.newBuilder().finishOnClose(true).build(), cutoffsAndOffsets);
    }
}
