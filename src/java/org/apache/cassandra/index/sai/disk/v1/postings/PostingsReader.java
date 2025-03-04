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


import java.io.IOException;
import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.disk.io.IndexInput;
import org.apache.cassandra.index.sai.disk.io.IndexInputReader;
import org.apache.cassandra.index.sai.disk.oldlucene.LuceneCompat;
import org.apache.cassandra.index.sai.disk.v1.LongArray;
import org.apache.cassandra.index.sai.metrics.QueryEventListener;
import org.apache.cassandra.index.sai.utils.SeekingRandomAccessInput;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.LongValues;


/**
 * Reads, decompresses and decodes postings lists written by {@link PostingsWriter}.
 *
 * Holds exactly one postings block in memory at a time. Does binary search over skip table to find a postings block to
 * load.
 */
@NotThreadSafe
public class PostingsReader implements OrdinalPostingList
{
    private static final Logger logger = LoggerFactory.getLogger(PostingsReader.class);

    protected final IndexInput input;
    protected final InputCloser runOnClose;
    private final int blockEntries;
    private final int numPostings;
    private final LongArray blockOffsets;
    private final LongArray blockMaxValues;
    private final SeekingRandomAccessInput seekingInput;
    private final QueryEventListener.PostingListEventListener listener;

    // TODO: Expose more things through the summary, now that it's an actual field?
    private final BlocksSummary summary;

    private int postingsBlockIdx;
    private int blockIdx; // position in block
    private int totalPostingsRead;
    private int actualSegmentRowId;

    private long currentPosition;
    private LongValues currentFORValues;
    private int postingsDecoded = 0;
    private int currentFrequency = Integer.MIN_VALUE;
    private final boolean readFrequencies;

    @VisibleForTesting
    public PostingsReader(IndexInput input, long summaryOffset, QueryEventListener.PostingListEventListener listener) throws IOException
    {
        this(input, new BlocksSummary(input, summaryOffset, InputCloser.NOOP), listener);
    }

    public PostingsReader(IndexInput input, BlocksSummary summary, QueryEventListener.PostingListEventListener listener) throws IOException
    {
        this(input, summary, false, listener, () -> {
            try
            {
                input.close();
            }
            finally
            {
                summary.close();
            }
        });
    }

    public PostingsReader(IndexInput input, BlocksSummary summary, boolean readFrequencies, QueryEventListener.PostingListEventListener listener) throws IOException
    {
        this(input, summary, readFrequencies, listener, () -> {
            try
            {
                input.close();
            }
            finally
            {
                summary.close();
            }
        });
    }

    public PostingsReader(IndexInput input, BlocksSummary summary, boolean readFrequencies, QueryEventListener.PostingListEventListener listener, InputCloser runOnClose) throws IOException
    {
        assert input instanceof IndexInputReader;
        logger.trace("Opening postings reader for {}", input);
        this.readFrequencies = readFrequencies;
        this.input = input;
        this.seekingInput = new SeekingRandomAccessInput(input);
        this.blockOffsets = summary.offsets;
        this.blockEntries = summary.blockEntries;
        this.numPostings = summary.numPostings;
        this.blockMaxValues = summary.maxValues;
        this.listener = listener;

        this.summary = summary;
        this.runOnClose = runOnClose;

        reBuffer();
    }

    @Override
    public int getOrdinal()
    {
        return totalPostingsRead;
    }

    public interface InputCloser
    {
        InputCloser NOOP = () -> {};
        void close() throws IOException;
    }

    public static class BlocksSummary
    {
        final int blockEntries;
        final int numPostings;
        final LongArray offsets;
        final LongArray maxValues;

        private final InputCloser runOnClose;

        public BlocksSummary(IndexInput input, long offset) throws IOException
        {
            this(input, offset, input::close);
        }

        public BlocksSummary(IndexInput input, long offset, InputCloser runOnClose) throws IOException
        {
            this.runOnClose = runOnClose;

            input.seek(offset);
            this.blockEntries = input.readVInt();
            // This is the count of row ids in a single posting list. For now, a segment cannot have more than
            // Integer.MAX_VALUE row ids, so it is safe to use an int here.
            this.numPostings = input.readVInt();

            final SeekingRandomAccessInput randomAccessInput = new SeekingRandomAccessInput(input);
            final int numBlocks = input.readVInt();
            final long maxBlockValuesLength = input.readVLong();
            final long maxBlockValuesOffset = input.getFilePointer() + maxBlockValuesLength;

            final byte offsetBitsPerValue = input.readByte();
            if (offsetBitsPerValue > 64)
            {
                String message = String.format("Postings list header is corrupted: Bits per value for block offsets must be no more than 64 and is %d.", offsetBitsPerValue);
                throw new CorruptIndexException(message, input);
            }
            this.offsets = new LongArrayReader(randomAccessInput, offsetBitsPerValue == 0 ? LongValues.ZEROES : LuceneCompat.directReaderGetInstance(randomAccessInput, offsetBitsPerValue, input.getFilePointer()), numBlocks);

            input.seek(maxBlockValuesOffset);
            final byte valuesBitsPerValue = input.readByte();
            if (valuesBitsPerValue > 64)
            {
                String message = String.format("Postings list header is corrupted: Bits per value for values samples must be no more than 64 and is %d.", valuesBitsPerValue);
                throw new CorruptIndexException(message, input);
            }
            this.maxValues = new LongArrayReader(randomAccessInput, valuesBitsPerValue == 0 ? LongValues.ZEROES : LuceneCompat.directReaderGetInstance(randomAccessInput, valuesBitsPerValue, input.getFilePointer()), numBlocks);
        }

        void close() throws IOException
        {
            runOnClose.close();
        }

        private static class LongArrayReader implements LongArray
        {
            private final RandomAccessInput input;
            private final LongValues reader;
            private final int length;

            private LongArrayReader(RandomAccessInput input, LongValues reader, int length)
            {
                this.input = input;
                this.reader = reader;
                this.length = length;
            }

            @Override
            public long ceilingRowId(long value)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public long indexOf(long targetToken)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public long get(long idx)
            {
                return reader.get(idx);
            }

            @Override
            public long length()
            {
                return length;
            }
        }
    }

    @Override
    public void close() throws IOException
    {
        listener.postingDecoded(postingsDecoded);
        runOnClose.close();
    }

    @Override
    public int size()
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
     * @param targetRowID target row ID to advance to
     *
     * @return first segment row ID which is >= the target row ID or {@link PostingList#END_OF_STREAM} if one does not exist
     */
    @Override
    public int advance(int targetRowID) throws IOException
    {
        listener.onAdvance();
        int block = binarySearchBlock(targetRowID);

        if (block < 0)
        {
            block = -block - 1;
        }

        if (postingsBlockIdx == block + 1)
        {
            // we're in the same block, just iterate through
            return slowAdvance(targetRowID);
        }
        assert block > 0;
        // Even if there was an exact match, block might contain duplicates.
        // We iterate to the target token from the beginning.
        lastPosInBlock(block - 1);
        return slowAdvance(targetRowID);
    }

    private int slowAdvance(int targetRowID) throws IOException
    {
        while (totalPostingsRead < numPostings)
        {
            int segmentRowId = peekNext();

            advanceOnePosition(segmentRowId);

            if (segmentRowId >= targetRowID)
            {
                return segmentRowId;
            }
        }
        return END_OF_STREAM;
    }

    private int binarySearchBlock(long targetRowID)
    {
        int low = postingsBlockIdx - 1;
        int high = Math.toIntExact(blockMaxValues.length()) - 1;

        // in current block
        if (low <= high && targetRowID <= blockMaxValues.get(low))
            return low;

        while (low <= high)
        {
            int mid = low + ((high - low) >> 1) ;

            long midVal = blockMaxValues.get(mid);

            if (midVal < targetRowID)
            {
                low = mid + 1;
            }
            else if (midVal > targetRowID)
            {
                high = mid - 1;
            }
            else
            {
                // target found, but we need to check for duplicates
                if (mid > 0 && blockMaxValues.get(mid - 1L) == targetRowID)
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
        actualSegmentRowId = Math.toIntExact(blockMaxValues.get(block));
        //upper bound, since we might've advanced to the last block, but upper bound is enough
        totalPostingsRead += (blockEntries - blockIdx) + (block - postingsBlockIdx + 1) * blockEntries;

        postingsBlockIdx = block + 1;
        blockIdx = blockEntries;
    }

    @Override
    public int nextPosting() throws IOException
    {
        final int next = peekNext();
        if (next != END_OF_STREAM)
        {
            advanceOnePosition(next);
        }
        return next;
    }

    @VisibleForTesting
    int getBlockEntries()
    {
        return blockEntries;
    }

    private int peekNext() throws IOException
    {
        if (totalPostingsRead >= numPostings)
        {
            return END_OF_STREAM;
        }
        if (blockIdx == blockEntries)
        {
            reBuffer();
        }

        return actualSegmentRowId + nextRowDelta();
    }

    private int nextRowDelta()
    {
        if (currentFORValues == null)
        {
            currentFrequency = Integer.MIN_VALUE;
            return 0;
        }

        long offset = readFrequencies ? 2L * blockIdx : blockIdx;
        long id = currentFORValues.get(offset);
        if (readFrequencies)
            currentFrequency = Math.toIntExact(currentFORValues.get(offset + 1));
        postingsDecoded++;
        return Math.toIntExact(id);
    }

    private void advanceOnePosition(int nextRowID)
    {
        actualSegmentRowId = nextRowID;
        totalPostingsRead++;
        blockIdx++;
    }

    private void reBuffer() throws IOException
    {
        final long pointer = blockOffsets.get(postingsBlockIdx);
        if (pointer < 4) {
            // the first 4 bytes must be CODEC_MAGIC
            throw new CorruptIndexException(String.format("Invalid block offset %d for postings block idx %d", pointer, postingsBlockIdx), input);
        }

        input.seek(pointer);

        final long left = numPostings - totalPostingsRead;
        assert left > 0;

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
            // If bitsPerValue is 0 then all the values in the block are the same
            currentFORValues = LongValues.ZEROES;
            return;
        }
        else if (bitsPerValue > 64)
        {
            throw new CorruptIndexException(
                    String.format("Postings list #%s block is corrupted. Bits per value should be no more than 64 and is %d.", postingsBlockIdx, bitsPerValue), input);
        }
        currentFORValues = LuceneCompat.directReaderGetInstance(seekingInput, bitsPerValue, currentPosition);
    }

    @Override
    public int frequency() {
        return currentFrequency;
    }
}
