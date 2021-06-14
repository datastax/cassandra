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
package org.apache.cassandra.index.sai.disk.v1;


import java.io.IOException;
import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.index.sai.disk.ReversePostingList;
import org.apache.cassandra.index.sai.metrics.QueryEventListener;
import org.apache.cassandra.index.sai.utils.LongArray;
import org.apache.cassandra.index.sai.utils.SeekingRandomAccessInput;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.IndexInput;


/**
 * Reads, decompresses and decodes postings lists written by {@link PostingsWriter}.
 *
 * Holds exactly one postings block in memory at a time. Does binary search over skip table to find a postings block to
 * load.
 */
@NotThreadSafe
public class ReversePostingsReader implements ReversePostingList
{
    protected final IndexInput input;
    private final int blockSize;
    private final long numPostings;
    private final LongArray blockOffsets;
    private final LongArray blockMaxValues;
    private final SeekingRandomAccessInput seekingInput;
    private final QueryEventListener.PostingListEventListener listener;

    private final PostingsReader.BlocksSummary summary;

    private int postingsBlockIdx;
    private int blockIdx; // position in block
    private long actualSegmentRowId;

    private long currentPosition;
    private DirectReaders.Reader currentFORValues;
    private long postingsDecoded = 0;

    @VisibleForTesting
    ReversePostingsReader(IndexInput input, long summaryOffset, QueryEventListener.PostingListEventListener listener) throws IOException
    {
        this(input, new PostingsReader.BlocksSummary(input, summaryOffset, () -> {}), listener);
    }

    @VisibleForTesting
    public ReversePostingsReader(IndexInput input, PostingsReader.BlocksSummary summary, QueryEventListener.PostingListEventListener listener) throws IOException
    {
        this.input = input;
        this.seekingInput = new SeekingRandomAccessInput(input);
        this.blockOffsets = summary.offsets;
        this.blockSize = summary.blockSize;
        this.numPostings = summary.numPostings;
        this.blockMaxValues = summary.maxValues;
        this.listener = listener;

        this.summary = summary;

        this.postingsBlockIdx = (int)summary.maxValues.length(); // make sure the initial postingsBlockIdx is 1 greater than the actual max index
        //                                                          so the assert in lastPosInBlock works

        lastPosInBlock((int)summary.maxValues.length() - 1);
    }

    @Override
    public long getOrdinal()
    {
        if (postingsBlockIdx == 0 && blockIdx == 0)
        {
            return 0;
        }
        long ordinal = ( (postingsBlockIdx) * blockSize) + blockIdx;
        return ordinal;
    }

    @Override
    public void close() throws IOException
    {
        listener.postingDecoded(postingsDecoded);
        try
        {
            input.close();
        }
        finally
        {
            summary.close();
        }
    }

    @Override
    public long size()
    {
        return numPostings;
    }

    @Override
    public long advance(long targetRowID) throws IOException
    {
        listener.onAdvance();
        int block = binarySearchBlock(targetRowID);

        if (block < 0)
        {
            block = -block;
        }

        if (postingsBlockIdx == block)
        {
            // we're in the same block, just iterate through
            return slowAdvance(targetRowID);
        }
        // Even if there was an exact match, block might contain duplicates.
        // We iterate to the target token from the beginning.
        lastPosInBlock(block);
        return slowAdvance(targetRowID);
    }

    private long slowAdvance(final long targetRowID) throws IOException
    {
        while (true)
        {
            final long segmentRowId = peekNext();

            if (segmentRowId == REVERSE_END_OF_STREAM) return segmentRowId;

            minusOnePosition(segmentRowId);

            if (segmentRowId <= targetRowID)
            {
                return segmentRowId;
            }
        }
    }

    private int binarySearchBlock(final long targetRowID)
    {
        // TODO: the variable names are opposite?
        int low = postingsBlockIdx - 1;
        int high = 0;

        // in current block
//        if (low <= high && targetRowID <= blockMaxValues.get(low))
//            return low;

        while (low >= high)
        {
            int mid = low + ((high - low) >> 1) ;

            long midVal = blockMaxValues.get(mid);

            if (midVal > targetRowID)
            {
                low = mid - 1;
            }
            else if (midVal < targetRowID)
            {
                high = mid + 1;
            }
            else
            {
                // target found, but we need to check for duplicates
                if (mid > 0 && (mid + 1) < blockMaxValues.length() && blockMaxValues.get(mid + 1L) == targetRowID)
                {
                    // there are duplicates, pivot right
                    high = mid + 1;
                }
                else
                {
                    // no duplicates
                    return mid;
                }
            }
        }
        return -(low + 1);  // target not found
    }

    private boolean lastPosInBlockCalled = false;

    public void lastPosInBlock(int block) throws IOException
    {
        // blockMaxValues is integer only
        actualSegmentRowId = blockMaxValues.get(block);

        assert block < postingsBlockIdx : "block="+block+" postingsBlockIdx="+postingsBlockIdx;

        postingsBlockIdx = block;
        lastPosInBlockCalled = true;

        reBuffer();
    }

    @Override
    public long nextPosting() throws IOException
    {
        final long next = peekNext();
        if (next != ReversePostingList.REVERSE_END_OF_STREAM)
        {
            minusOnePosition(next);
        }
        return next;
    }

    @VisibleForTesting
    int getBlockSize()
    {
        return blockSize;
    }

    private long peekNext() throws IOException
    {
        try
        {
            if (blockIdx <= 0 && postingsBlockIdx <= 0)
            {
                return ReversePostingList.REVERSE_END_OF_STREAM;
            }
            if (blockIdx == -1)
            {
                postingsBlockIdx--;
                reBuffer();
            }
            // when the row id is the max or last in the block, there's no delta, so return the row id as is
            if (blockIdx == blockSize && lastPosInBlockCalled)
            {
                return actualSegmentRowId;
            }
            else if (blockIdx == blockSize)
            {
                blockIdx--;
            }
            return actualSegmentRowId - nextRowID();
        }
        finally
        {
            lastPosInBlockCalled = false;
        }
    }

    private int nextRowID()
    {
        // currentFORValues is null when the all the values in the block are the same
        if (currentFORValues == null)
        {
            return 0;
        }
        else
        {
            final long id = currentFORValues.get(seekingInput, currentPosition, blockIdx);
            postingsDecoded++;
            return Math.toIntExact(id);
        }
    }

    private void minusOnePosition(long nextRowID)
    {
        actualSegmentRowId = nextRowID;
        blockIdx--;
    }

    private void reBuffer() throws IOException
    {
        final long pointer = blockOffsets.get(postingsBlockIdx);

        input.seek(pointer);

        readFoRBlock(input);

        if (postingsBlockIdx == blockOffsets.length() - 1)
        {
            blockIdx = (int) (numPostings % blockSize);
            if (blockIdx == 0)
            {
                blockIdx = blockSize;
            }
        }
        else
        {
            blockIdx = blockSize;
        }
    }

    private void readFoRBlock(IndexInput in) throws IOException
    {
        final byte bitsPerValue = in.readByte();

        currentPosition = in.getFilePointer();

        if (bitsPerValue == 0)
        {
            // currentFORValues is null when the all the values in the block are the same
            currentFORValues = null;
            return;
        }
        else if (bitsPerValue > 64)
        {
            throw new CorruptIndexException(
                    String.format("Postings list #%s block is corrupted. Bits per value should be no more than 64 and is %d.", postingsBlockIdx, bitsPerValue), input);
        }
        currentFORValues = DirectReaders.getReaderForBitsPerValue(bitsPerValue);
    }
}
