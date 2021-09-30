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
package org.apache.cassandra.index.sai.disk.v2.postings;


import java.io.IOException;
import java.util.Arrays;
import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.index.sai.disk.OrdinalPostingList;
import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.v1.LongArray;
import org.apache.cassandra.index.sai.metrics.QueryEventListener;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.SeekingRandomAccessInput;
import org.apache.cassandra.index.sai.utils.SharedIndexInput;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.lucene.codecs.lucene84.PForEncoder;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.IndexInput;

import static org.apache.lucene.codecs.lucene84.Lucene84PostingsFormat.BLOCK_SIZE;


/**
 * Reads, decompresses and decodes postings lists written by {@link PostingsWriter}.
 *
 * Holds exactly one postings block in memory at a time. Does binary search over skip table to find a postings block to
 * load.
 */
@NotThreadSafe
public class PForDeltaPostingsReader implements OrdinalPostingList
{
    protected final IndexInput input;
    private final int blockSize;
    private final long numPostings;
    private final LongArray blockOffsets;
    private final LongArray blockMaxValues;
    private final SeekingRandomAccessInput seekingInput;
    private final QueryEventListener.PostingListEventListener listener;

    // TODO: Expose more things through the summary, now that it's an actual field?
    private final PostingsReader.BlocksSummary summary;
    private final PForEncoder forEncoder = new PForEncoder();

    private int postingsBlockIdx;
    private int blockIdx; // position in block
    private long totalPostingsRead;
    private long actualSegmentRowId;

    private long currentPosition;

    private final long[] postingsBuffer = new long[BLOCK_SIZE];

    private long[] currentPostings;
    private long postingsDecoded = 0;

    @VisibleForTesting
    public PForDeltaPostingsReader(SharedIndexInput sharedInput, long summaryOffset, QueryEventListener.PostingListEventListener listener) throws IOException
    {
        this(sharedInput, new PostingsReader.BlocksSummary(sharedInput.sharedCopy(), summaryOffset), listener);
    }

    @VisibleForTesting
    public PForDeltaPostingsReader(IndexInput input,
                                   PostingsReader.BlocksSummary summary,
                                   QueryEventListener.PostingListEventListener listener) throws IOException
    {
        this.input = input;
        this.seekingInput = new SeekingRandomAccessInput(input);
        this.blockOffsets = summary.offsets;
        this.blockSize = summary.blockSize;
        this.numPostings = summary.numPostings;
        this.blockMaxValues = summary.maxValues;
        this.listener = listener;

        this.summary = summary;

        reBuffer();
    }

    @Override
    public long getOrdinal()
    {
        return totalPostingsRead;
    }

    interface InputCloser
    {
        void close() throws IOException;
    }

    @Override
    public void close() throws IOException
    {
        listener.postingDecoded(postingsDecoded);
        FileUtils.closeQuietly(input);
        summary.close();
    }

    @Override
    public long size()
    {
        return numPostings;
    }

    /**
     * Advances to the first row ID beyond the current that is greater than or equal to the
     * target, and returns that row ID. Exhausts the iterator and returns {@link #END_OF_STREAM} if
     * the target is greater than the highest row ID.
     *
     * Does binary search over the skip table to find the next block to load into memory.
     *
     * Note: Callers must use the return value of this method before calling {@link #nextPosting()}, as calling
     * that method will return the next posting, not the one to which we have just advanced.
     *
     * @param targetRowId target rowId to advance to
     *
     * @return first segment row ID which is >= the target row ID or {@link PostingList#END_OF_STREAM} if one does not exist
     */
    @Override
    public long advance(long targetRowId) throws IOException
    {
        listener.onAdvance();
        int block = binarySearchBlock(targetRowId);

        if (block < 0)
        {
            block = -block - 1;
        }

        if (postingsBlockIdx == block + 1)
        {
            // we're in the same block, just iterate through
            return slowAdvance(targetRowId);
        }
        assert block > 0;
        // Even if there was an exact match, block might contain duplicates.
        // We iterate to the target token from the beginning.
        lastPosInBlock(block - 1);
        return slowAdvance(targetRowId);
    }

    private long slowAdvance(long targetRowId) throws IOException
    {
        while (totalPostingsRead < numPostings)
        {
            final long segmentRowId = peekNext();

            advanceOnePosition(segmentRowId);

            if (segmentRowId >= targetRowId)
                return segmentRowId;
        }
        return END_OF_STREAM;
    }

    private int binarySearchBlock(long targetRowId) throws IOException
    {
        int low = postingsBlockIdx - 1;
        int high = Math.toIntExact(blockMaxValues.length()) - 1;

        // in current block
        if (low <= high && targetRowId <= blockMaxValues.get(low))
            return low;

        while (low <= high)
        {
            int mid = low + ((high - low) >> 1) ;

            long midVal = blockMaxValues.get(mid);
            if (midVal < targetRowId)
            {
                low = mid + 1;
            }
            else if (midVal > targetRowId)
            {
                high = mid - 1;
            }
            else
            {
                // target found, but we need to check for duplicates
                if (mid > 0 && blockMaxValues.get(mid - 1) == targetRowId)
                {
                    // there are duplicates, pivot left
                    high = mid - 1;
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

    private void lastPosInBlock(int block)
    {
        // blockMaxValues is integer only
        actualSegmentRowId = blockMaxValues.get(block);
        //upper bound, since we might've advanced to the last block, but upper bound is enough
        totalPostingsRead += (blockSize - blockIdx) + (block - postingsBlockIdx + 1) * blockSize;

        postingsBlockIdx = block + 1;
        blockIdx = blockSize;
    }

    @Override
    public long nextPosting() throws IOException
    {
        final long next = peekNext();
        if (next != END_OF_STREAM)
        {
            advanceOnePosition(next);
        }
//        System.out.println(this.getClass().getSimpleName()+ "@" + this.hashCode() + ".nextPosting = " + next);

        return next;
    }

    @VisibleForTesting
    int getBlockSize()
    {
        return blockSize;
    }

    private long peekNext() throws IOException
    {
        if (totalPostingsRead >= numPostings)
        {
            return END_OF_STREAM;
        }
        if (blockIdx == blockSize)
        {
            reBuffer();
        }

        return actualSegmentRowId + nextRowID();
    }

    private int nextRowID()
    {
        // currentFORValues is null when the all the values in the block are the same
        if (currentPostings == null)
        {
            return 0;
        }
        else
        {
            final long id = this.currentPostings[blockIdx];
            //final long id = currentFORValues.get(seekingInput, currentPosition, blockIdx);
            postingsDecoded++;
            return Math.toIntExact(id);
        }
    }

    private void advanceOnePosition(long nextRowID)
    {
        actualSegmentRowId = nextRowID;
        totalPostingsRead++;
        blockIdx++;
    }

    private void reBuffer() throws IOException
    {
        final long pointer = blockOffsets.get(postingsBlockIdx);

        input.seek(pointer);

        final long left = numPostings - totalPostingsRead;
        assert left > 0 : "numPostings="+numPostings+" totalPostingsRead="+totalPostingsRead;

        readFoRBlock(input);

        postingsBlockIdx++;
        blockIdx = 0;
    }

    private void readFoRBlock(IndexInput in) throws IOException
    {
        final byte bitsPerValue = in.readByte();

        currentPosition = in.getFilePointer();

        if (bitsPerValue == 0)
        {
            // currentFORValues is null when the all the values in the block are the same
            currentPostings = null;
            return;
        }
        else if (bitsPerValue > 64)
        {
            throw new CorruptIndexException(
                    String.format("Postings list #%s block is corrupted. Bits per value should be no more than 64 and is %d.", postingsBlockIdx, bitsPerValue), input);
        }
        this.currentPostings = this.postingsBuffer;
        Arrays.fill(currentPostings, 0);
        this.forEncoder.decodePFoR(currentPostings, in);
    }
}
