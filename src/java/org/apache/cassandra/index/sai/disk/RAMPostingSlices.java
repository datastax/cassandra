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
package org.apache.cassandra.index.sai.disk;

import java.io.IOException;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.ByteBlockPool;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.mutable.MutableValueInt;

/**
 * Encodes postings as variable integers into "slices" of byte blocks for efficient memory usage.
 */
class RAMPostingSlices
{
    static final int DEFAULT_TERM_DICT_SIZE = 1024;

    /** Pool of byte blocks storing the actual posting data */
    private final ByteBlockPool postingsPool;
    /** true if we're also writing term frequencies for an analyzed index */
    private final boolean includeFrequencies;

    /** The starting positions of postings for each term.  Term id = index in array. */
    private int[] postingStarts = new int[DEFAULT_TERM_DICT_SIZE];
    /** The current write positions for each term's postings.  Term id = index in array. */
    private int[] postingUptos = new int[DEFAULT_TERM_DICT_SIZE];
    /** The number of postings for each term.  Term id = index in array. */
    private int[] sizes = new int[DEFAULT_TERM_DICT_SIZE];

    RAMPostingSlices(Counter memoryUsage, boolean includeFrequencies)
    {
        postingsPool = new ByteBlockPool(new ByteBlockPool.DirectTrackingAllocator(memoryUsage));
        this.includeFrequencies = includeFrequencies;
    }

    /**
     * Creates and returns a PostingList for the given term ID.
     */
    PostingList postingList(int termID, final ByteSliceReader reader, long maxSegmentRowID)
    {
        initReader(reader, termID);

        final MutableValueInt lastSegmentRowId = new MutableValueInt();

        return new PostingList()
        {
            int frequency = Integer.MIN_VALUE;

            @Override
            public int nextPosting() throws IOException
            {
                if (reader.eof())
                {
                    frequency = Integer.MIN_VALUE;
                    return PostingList.END_OF_STREAM;
                }
                else
                {
                    lastSegmentRowId.value += reader.readVInt();
                    if (includeFrequencies)
                        frequency = reader.readVInt();
                    return lastSegmentRowId.value;
                }
            }

            @Override
            public int frequency()
            {
                if (!includeFrequencies)
                    return 1;
                if (frequency <= 0)
                    throw new IllegalStateException("frequency() called before nextPosting()");
                return frequency;
            }

            @Override
            public int size()
            {
                return sizes[termID];
            }

            @Override
            public int advance(int targetRowID)
            {
                throw new UnsupportedOperationException();
            }
        };
    }

    /**
     * Initializes a ByteSliceReader for reading postings for a specific term.
     */
    void initReader(ByteSliceReader reader, int termID)
    {
        final int upto = postingUptos[termID];
        reader.init(postingsPool, postingStarts[termID], upto);
    }

    /**
     * Creates a new slice for storing postings for a given term ID.
     * Grows the internal arrays if necessary and allocates a new block
     * if the current block cannot accommodate a new slice.
     */
    void createNewSlice(int termID)
    {
        if (termID >= postingStarts.length - 1)
        {
            postingStarts = ArrayUtil.grow(postingStarts, termID + 1);
            postingUptos = ArrayUtil.grow(postingUptos, termID + 1);
            sizes = ArrayUtil.grow(sizes, termID + 1);
        }

        // the slice will not fit in the current block, create a new block
        if ((ByteBlockPool.BYTE_BLOCK_SIZE - postingsPool.byteUpto) < ByteBlockPool.FIRST_LEVEL_SIZE)
        {
            postingsPool.nextBuffer();
        }

        final int upto = postingsPool.newSlice(ByteBlockPool.FIRST_LEVEL_SIZE);
        postingStarts[termID] = upto + postingsPool.byteOffset;
        postingUptos[termID] = upto + postingsPool.byteOffset;
    }

    void writePosting(int termID, int deltaRowId, int frequency)
    {
        assert termID >= 0 : termID;
        assert deltaRowId >= 0 : deltaRowId;
        writeVInt(termID, deltaRowId);

        if (includeFrequencies)
        {
            assert frequency > 0 : frequency;
            writeVInt(termID, frequency);
        }

        sizes[termID]++;
    }

    /**
     * Writes a variable-length integer to the posting list for a given term.
     * The integer is encoded using a variable-length encoding scheme where each
     * byte uses 7 bits for the value and 1 bit to indicate if more bytes follow.
     */
    private void writeVInt(int termID, int i)
    {
        while ((i & ~0x7F) != 0)
        {
            writeByte(termID, (byte) ((i & 0x7f) | 0x80));
            i >>>= 7;
        }
        writeByte(termID, (byte) i);
    }

    /**
     * Writes a single byte to the posting list for a given term.
     * If the current slice is full, it automatically allocates a new slice.
     */
    private void writeByte(int termID, byte b)
    {
        int upto = postingUptos[termID];
        byte[] block = postingsPool.buffers[upto >> ByteBlockPool.BYTE_BLOCK_SHIFT];
        assert block != null;
        int offset = upto & ByteBlockPool.BYTE_BLOCK_MASK;
        if (block[offset] != 0)
        {
            // End of slice; allocate a new one
            offset = postingsPool.allocSlice(block, offset);
            block = postingsPool.buffer;
            postingUptos[termID] = offset + postingsPool.byteOffset;
        }
        block[offset] = b;
        postingUptos[termID]++;
    }
}
