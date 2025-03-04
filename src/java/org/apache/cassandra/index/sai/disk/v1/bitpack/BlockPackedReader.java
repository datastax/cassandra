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
package org.apache.cassandra.index.sai.disk.v1.bitpack;

import java.io.IOException;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.index.sai.disk.io.IndexInputReader;
import org.apache.cassandra.index.sai.disk.v1.LongArray;
import org.apache.cassandra.index.sai.utils.IndexFileUtils;
import org.apache.cassandra.index.sai.utils.SAICodecUtils;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.lucene.index.CorruptIndexException;

import static org.apache.cassandra.index.sai.utils.SAICodecUtils.checkBlockSize;
import static org.apache.cassandra.index.sai.utils.SAICodecUtils.numBlocks;
import static org.apache.cassandra.index.sai.utils.SAICodecUtils.readVLong;
import static org.apache.lucene.util.BitUtil.zigZagDecode;

/**
 * Provides non-blocking, random access to a stream written with {@link BlockPackedWriter}.
 */
public class BlockPackedReader implements LongArray.Factory
{
    private final FileHandle file;
    private final int blockShift;
    private final int blockMask;
    private final long valueCount;
    private final byte[] blockBitsPerValue;
    private final long[] blockOffsets;
    private final long[] minValues;

    @SuppressWarnings("resource")
    public BlockPackedReader(FileHandle file, NumericValuesMeta meta) throws IOException
    {
        this.file = file;

        this.valueCount = meta.valueCount;

        blockShift = checkBlockSize(meta.blockSize, AbstractBlockPackedWriter.MIN_BLOCK_SIZE, AbstractBlockPackedWriter.MAX_BLOCK_SIZE);
        blockMask = meta.blockSize - 1;
        final int numBlocks = numBlocks(valueCount, meta.blockSize);
        blockBitsPerValue = new byte[numBlocks];
        blockOffsets = new long[numBlocks];
        minValues = new long[numBlocks];

        try (final IndexInputReader in = IndexInputReader.create(this.file.createReader()))
        {
            SAICodecUtils.validate(in);
            in.seek(meta.blockMetaOffset);

            for (int i = 0; i < numBlocks; ++i)
            {
                final int token = in.readByte() & 0xFF;
                final int bitsPerValue = token >>> AbstractBlockPackedWriter.BPV_SHIFT;
                if (bitsPerValue > 64)
                {
                    throw new CorruptIndexException(String.format("Block %d is corrupted. Bits per value should be no more than 64 and is %d.", i, bitsPerValue), in);
                }
                if ((token & AbstractBlockPackedWriter.MIN_VALUE_EQUALS_0) == 0)
                {
                    long val = zigZagDecode(1L + readVLong(in));
                    minValues[i] = val;
                }
                else
                {
                    minValues[i] = 0L;
                }

                blockBitsPerValue[i] = (byte) bitsPerValue;

                if (bitsPerValue > 0)
                {
                    blockOffsets[i] = in.readVLong();
                }
                else
                {
                    blockOffsets[i] = -1;
                }
            }
        }
    }

    @VisibleForTesting
    @Override
    public LongArray open()
    {
        var indexInput = IndexFileUtils.instance.openInput(file);
        return new AbstractBlockPackedReader(indexInput, blockBitsPerValue, blockShift, blockMask, 0, valueCount)
        {
            @Override
            protected long blockOffsetAt(int block)
            {
                return blockOffsets[block];
            }

            @Override
            long delta(int block, int idx)
            {
                return minValues[block];
            }

            @Override
            public void close() throws IOException
            {
                indexInput.close();
            }
        };
    }
}
