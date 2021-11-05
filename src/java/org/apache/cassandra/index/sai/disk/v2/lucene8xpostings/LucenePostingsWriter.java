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
package org.apache.cassandra.index.sai.disk.v2.lucene8xpostings;


import java.io.Closeable;
import java.io.IOException;
import javax.annotation.concurrent.NotThreadSafe;

import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.utils.SAICodecUtils;
import org.apache.lucene.store.IndexOutput;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.cassandra.index.sai.disk.v2.lucene8xpostings.LuceneSkipWriter.MAX_SKIP_LEVELS;

@NotThreadSafe
public class LucenePostingsWriter implements Closeable
{
    private final static String POSTINGS_MUST_BE_SORTED_ERROR_MSG = "Postings must be sorted ascending, got [%s] after [%s]";

    private final IndexOutput dataOutput;
    private final int blockSize;
    private final long[] rowIdDeltaBuffer;
    private final LuceneSkipWriter skipWriter;
    private final ForDeltaUtil forDeltaUtil;

    private final long startOffset;

    private long block;

    private int bufferUpto;
    private long lastSegmentRowId;
    private long totalPostings;

    public LucenePostingsWriter(IndexOutput dataOutput, int blockSize, long maxDoc) throws IOException
    {
        this.blockSize = blockSize;
        this.dataOutput = dataOutput;
        startOffset = dataOutput.getFilePointer();
        rowIdDeltaBuffer = new long[blockSize];
        SAICodecUtils.writeHeader(dataOutput);

        final ForUtil forUtil = new ForUtil();
        forDeltaUtil = new ForDeltaUtil(forUtil);

        skipWriter = new LuceneSkipWriter(blockSize,
                                          8,
                                          MAX_SKIP_LEVELS,
                                          maxDoc,
                                          dataOutput);
    }

    /**
     * @return current file pointer
     */
    public long getFilePointer()
    {
        return dataOutput.getFilePointer();
    }

    /**
     * @return file pointer where index structure begins
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

        totalPostings = 0;
        block = 0;
        resetBlockCounters();
        // reset the skip writer
        this.skipWriter.resetSkip();

        long postingsFP = dataOutput.getFilePointer();

        long segmentRowId;
        // When postings list are merged, we don't know exact size, just an upper bound.
        // We need to count how many postings we added to the block ourselves.
        long size = 0;
        while ((segmentRowId = postings.nextPosting()) != PostingList.END_OF_STREAM)
        {
            writePosting(segmentRowId);
            size++;
            totalPostings++;
        }
        if (size == 0)
            return -1;

        finish();

        long mainFP = dataOutput.getFilePointer();

        int skipOffset = (int)(mainFP - postingsFP);

        dataOutput.writeVLong(size);
        dataOutput.writeVInt(skipOffset);

        skipWriter.writeSkip(dataOutput);

        return mainFP;
    }

//    public long getTotalPostings()
//    {
//        return totalPostings;
//    }

    private void writePosting(long segmentRowId) throws IOException
    {
        if (!(segmentRowId >= lastSegmentRowId || lastSegmentRowId == 0))
            throw new IllegalArgumentException(String.format(POSTINGS_MUST_BE_SORTED_ERROR_MSG, segmentRowId, lastSegmentRowId));

        if (block > 0 && bufferUpto == 0)
        {
            skipWriter.bufferSkip(segmentRowId, totalPostings);
        }

        final long delta = segmentRowId - lastSegmentRowId;

        rowIdDeltaBuffer[bufferUpto++] = delta;

        if (bufferUpto == blockSize)
        {
            writePostingsBlock();
            resetBlockCounters();
        }
        lastSegmentRowId = segmentRowId;
    }

    private void finish() throws IOException
    {
        if (bufferUpto > 0)
        {
            writePostingsBlock();
        }
    }

    private void resetBlockCounters()
    {
        bufferUpto = 0;
        lastSegmentRowId = 0;
    }

    private void writePostingsBlock() throws IOException
    {
        assert bufferUpto > 0;

        forDeltaUtil.encodeDeltas(rowIdDeltaBuffer, dataOutput);
        block++;
    }
}
