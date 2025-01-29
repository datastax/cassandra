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
package org.apache.cassandra.index.sai.disk.v1.postings;


import java.io.Closeable;
import java.io.IOException;
import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.agrona.collections.LongArrayList;
import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.disk.format.IndexComponentType;
import org.apache.cassandra.index.sai.disk.io.IndexOutput;
import org.apache.cassandra.index.sai.disk.format.IndexComponents;
import org.apache.cassandra.index.sai.disk.io.IndexOutputWriter;
import org.apache.cassandra.index.sai.disk.oldlucene.DirectWriterAdapter;
import org.apache.cassandra.index.sai.disk.oldlucene.LuceneCompat;
import org.apache.cassandra.index.sai.disk.oldlucene.ResettableByteBuffersIndexOutput;
import org.apache.cassandra.index.sai.utils.SAICodecUtils;
import org.apache.lucene.store.DataOutput;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Math.max;
import static java.lang.Math.min;


/**
 * Encodes, compresses and writes postings lists to disk.
 *
 * All row IDs in the posting list are delta encoded, then deltas are divided into blocks for compression.
 * <p>
 * In packed blocks, longs are encoded with the same bit width (FoR compression). The block size (i.e. number of
 * longs inside block) is fixed (currently 128). Additionally blocks that are all the same value are encoded in an
 * optimized way.
 * </p>
 * <p>
 * In VLong blocks, longs are compressed with {@link DataOutput#writeVLong}. The block size is variable.
 * </p>
 *
 * <p>
 * Packed blocks are favoured, meaning when the postings are long enough, {@link PostingsWriter} will try
 * to encode most data as a packed block. Take a term with 259 row IDs as an example, the first 256 IDs are encoded
 * as two packed blocks, while the remaining 3 are encoded as one VLong block.
 * </p>
 * <p>
 * Each posting list ends with a meta section and a skip table, that are written right after all postings blocks. Skip
 * interval is the same as block size, and each skip entry points to the end of each block.  Skip table consist of
 * block offsets and maximum rowids of each block, compressed as two FoR blocks.
 * </p>
 *
 * Visual representation of the disk format:
 * <pre>
 *
 * +========+========================+=====+==============+===============+============+=====+========================+========+
 * | HEADER | POSTINGS LIST (TERM 1)                                                   | ... | POSTINGS LIST (TERM N) | FOOTER |
 * +========+========================+=====+==============+===============+============+=====+========================+========+
 *          | FOR BLOCK (1)          | ... | FOR BLOCK (N)| BLOCK SUMMARY              |
 *          +------------------------+-----+--------------+---------------+------------+
 *                                                        | BLOCK SIZE    |            |
 *                                                        | LIST SIZE     | SKIP TABLE |
 *                                                        +---------------+------------+
 *                                                                        | BLOCKS POS.|
 *                                                                        | MAX ROWIDS |
 *                                                                        +------------+
 *
 *  </pre>
 */
@NotThreadSafe
public class PostingsWriter implements Closeable
{
    protected static final Logger logger = LoggerFactory.getLogger(PostingsWriter.class);

    // import static org.apache.lucene.codecs.lucene50.Lucene50PostingsFormat.BLOCK_SIZE;
    private final static int BLOCK_ENTRIES = 128;

    private static final String POSTINGS_MUST_BE_SORTED_ERROR_MSG = "Postings must be sorted ascending, got [%s] after [%s]";

    private final IndexOutput dataOutput;
    private final int blockEntries;
    private final long[] deltaBuffer;
    private final int[] freqBuffer; // frequency is capped at 255
    private final LongArrayList blockOffsets = new LongArrayList();
    private final LongArrayList blockMaxIDs = new LongArrayList();
    private final ResettableByteBuffersIndexOutput inMemoryOutput;

    private final long startOffset;

    private int bufferUpto;
    private long lastSegmentRowId;
    // This number is the count of row ids written to the postings for this segment. Because a segment row id can be in
    // multiple postings list for the segment, this number could exceed Integer.MAX_VALUE, so we use a long.
    private long totalPostings;

    public PostingsWriter(IndexComponents.ForWrite components) throws IOException
    {
        this(components, BLOCK_ENTRIES);
    }

    public PostingsWriter(IndexOutput dataOutput) throws IOException
    {
        this(dataOutput, BLOCK_ENTRIES);
    }

    @VisibleForTesting
    PostingsWriter(IndexComponents.ForWrite components, int blockEntries) throws IOException
    {
        this(components.addOrGet(IndexComponentType.POSTING_LISTS).openOutput(true), blockEntries);
    }

    private PostingsWriter(IndexOutput dataOutput, int blockEntries) throws IOException
    {
        assert dataOutput instanceof IndexOutputWriter;
        logger.debug("Creating postings writer for output {}", dataOutput);
        this.blockEntries = blockEntries;
        this.dataOutput = dataOutput;
        startOffset = dataOutput.getFilePointer();
        deltaBuffer = new long[blockEntries];
        freqBuffer = new int[blockEntries];
        inMemoryOutput = LuceneCompat.getResettableByteBuffersIndexOutput(dataOutput.order(), 1024, "blockOffsets");
        SAICodecUtils.writeHeader(dataOutput);
    }

    /**
     * @return current file pointer
     */
    public long getFilePointer()
    {
        return dataOutput.getFilePointer();
    }

    /**
     * @return file pointer where index structure begins (before header)
     */
    public long getStartOffset()
    {
        return startOffset;
    }

    /**
     * write footer to the postings
     */
    public void complete() throws IOException
    {
        SAICodecUtils.writeFooter(dataOutput);
    }

    @Override
    public void close() throws IOException
    {
        dataOutput.close();
    }

    /**
     * Encodes, compresses and flushes given posting list to disk.
     *
     * @param postings posting list to write to disk
     *
     * @return file offset to the summary block of this posting list
     */
    public long write(PostingList postings) throws IOException
    {
        checkArgument(postings != null, "Expected non-null posting list.");
        checkArgument(postings.size() > 0, "Expected non-empty posting list.");

        resetBlockCounters();
        blockOffsets.clear();
        blockMaxIDs.clear();

        int segmentRowId;
        // When postings list are merged, we don't know exact size, just an upper bound.
        // We need to count how many postings we added to the block ourselves.
        int size = 0;
        while ((segmentRowId = postings.nextPosting()) != PostingList.END_OF_STREAM)
        {
            writePosting(segmentRowId, postings.frequency());
            size++;
            totalPostings++;
        }
        if (size == 0)
            return -1;

        finish();

        final long summaryOffset = dataOutput.getFilePointer();
        writeSummary(size);
        return summaryOffset;
    }

    public long getTotalPostings()
    {
        return totalPostings;
    }

    private void writePosting(long segmentRowId, int freq) throws IOException {
        if (!(segmentRowId >= lastSegmentRowId || lastSegmentRowId == 0))
            throw new IllegalArgumentException(String.format(POSTINGS_MUST_BE_SORTED_ERROR_MSG, segmentRowId, lastSegmentRowId));

        assert freq > 0;
        final long delta = segmentRowId - lastSegmentRowId;
        deltaBuffer[bufferUpto] = delta;
        freqBuffer[bufferUpto] = min(freq, 255);
        bufferUpto++;

        if (bufferUpto == blockEntries) {
            addBlockToSkipTable(segmentRowId);
            writePostingsBlock(bufferUpto);
            resetBlockCounters();
        }
        lastSegmentRowId = segmentRowId;
    }

    private void finish() throws IOException
    {
        if (bufferUpto > 0)
        {
            addBlockToSkipTable(lastSegmentRowId);

            writePostingsBlock(bufferUpto);
        }
    }

    private void resetBlockCounters()
    {
        bufferUpto = 0;
        lastSegmentRowId = 0;
    }

    private void addBlockToSkipTable(long maxSegmentRowID)
    {
        blockOffsets.add(dataOutput.getFilePointer());
        blockMaxIDs.add(maxSegmentRowID);
    }

    private void writeSummary(int exactSize) throws IOException
    {
        dataOutput.writeVInt(blockEntries);
        dataOutput.writeVInt(exactSize);
        writeSkipTable();
    }

    private void writeSkipTable() throws IOException
    {
        assert blockOffsets.size() == blockMaxIDs.size();
        dataOutput.writeVInt(blockOffsets.size());

        // compressing offsets in memory first, to know the exact length (with padding)
        inMemoryOutput.reset();

        writeSortedFoRBlock(blockOffsets, inMemoryOutput);
        dataOutput.writeVLong(inMemoryOutput.getFilePointer());
        inMemoryOutput.copyTo(dataOutput);
        writeSortedFoRBlock(blockMaxIDs, dataOutput);
    }

    private void writePostingsBlock(int entries) throws IOException {
        // Find max value to determine bits needed
        long maxValue = 0;
        for (int i = 0; i < entries; i++) {
            maxValue = max(maxValue, deltaBuffer[i]);
            maxValue = max(maxValue, freqBuffer[i]);
        }
        
        // Use the maximum bits needed for either value type
        final int bitsPerValue = maxValue == 0 ? 0 : LuceneCompat.directWriterUnsignedBitsRequired(dataOutput.order(), maxValue);
        
        dataOutput.writeByte((byte) bitsPerValue);
        if (bitsPerValue > 0) {
            // Write interleaved [delta][freq] pairs
            final DirectWriterAdapter writer = LuceneCompat.directWriterGetInstance(dataOutput.order(), dataOutput, entries * 2, bitsPerValue);
            for (int i = 0; i < entries; ++i) {
                writer.add(deltaBuffer[i]);
                writer.add(freqBuffer[i]);
            }
            writer.finish();
        }
    }

    private void writeSortedFoRBlock(LongArrayList values, IndexOutput output) throws IOException
    {
        final long maxValue = values.getLong(values.size() - 1);

        assert values.size() > 0;
        final int bitsPerValue = maxValue == 0 ? 0 : LuceneCompat.directWriterUnsignedBitsRequired(output.order(), maxValue);
        output.writeByte((byte) bitsPerValue);
        if (bitsPerValue > 0)
        {
            final DirectWriterAdapter writer = LuceneCompat.directWriterGetInstance(output.order(), output, values.size(), bitsPerValue);
            for (int i = 0; i < values.size(); ++i)
            {
                writer.add(values.getLong(i));
            }
            writer.finish();
        }
    }
}
