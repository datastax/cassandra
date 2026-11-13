/*
 * Copyright IBM Corp.
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
package org.apache.cassandra.index.sai.disk.v1.postings;

import java.io.IOException;
import javax.annotation.concurrent.NotThreadSafe;

import org.apache.cassandra.index.sai.disk.io.IndexInput;
import org.apache.cassandra.index.sai.disk.oldlucene.LuceneCompat;
import org.apache.cassandra.index.sai.utils.SeekingRandomAccessInput;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.util.LongValues;

/**
 * Reads whole posting lists written by {@link PostingsWriter} into a {@link SortedRowIdsBuilder}.
 * <p>
 * One instance serves any number of posting lists of the same file. Unlike a {@link PostingsReader} it keeps no
 * state per posting list, which makes it suitable for the queries that visit a very large number of terms.
 */
@NotThreadSafe
public class EagerPostingsReader
{
    private final IndexInput input;
    private final SeekingRandomAccessInput seekingInput;
    private final boolean readFrequencies;
    private long postingsDecoded;

    /**
     * @param input the input over the postings file; it is not closed by this reader, and it can be shared with
     * other readers as long as they seek before reading, as {@link PostingsReader} does
     * @param readFrequencies whether the posting lists were written with the frequencies
     */
    public EagerPostingsReader(IndexInput input, boolean readFrequencies)
    {
        this.input = input;
        this.seekingInput = new SeekingRandomAccessInput(input);
        this.readFrequencies = readFrequencies;
    }

    /**
     * Adds to {@code rowIds} all the row ids of the posting list whose summary is at the given offset.
     */
    public void readAll(long summaryOffset, SortedRowIdsBuilder rowIds) throws IOException
    {
        input.seek(summaryOffset);
        int blockEntries = input.readVInt();
        int numPostings = input.readVInt();
        if (numPostings == 0)
            return;

        int numBlocks = input.readVInt();
        if (blockEntries <= 0 || numPostings < 0 || numBlocks != (numPostings + blockEntries - 1) / blockEntries)
            throw new CorruptIndexException(String.format("Postings list header is corrupted: %d blocks for %d postings in blocks of %d.",
                                                          numBlocks, numPostings, blockEntries), input);
        input.readVLong(); // length of the block offsets

        byte offsetBitsPerValue = input.readByte();
        if (offsetBitsPerValue > 64)
            throw new CorruptIndexException(String.format("Postings list header is corrupted: Bits per value for block offsets must be no more than 64 and is %d.", offsetBitsPerValue), input);
        LongValues blockOffsets = offsetBitsPerValue == 0
                                  ? LongValues.ZEROES
                                  : LuceneCompat.directReaderGetInstance(seekingInput, offsetBitsPerValue, input.getFilePointer());

        // the row ids are stored as deltas, the first one from zero, and a block continues from the previous one
        int rowId = 0;
        int remaining = numPostings;
        for (int block = 0; block < numBlocks; block++)
        {
            long blockOffset = blockOffsets.get(block);
            if (blockOffset < 4) // the first 4 bytes must be CODEC_MAGIC
                throw new CorruptIndexException(String.format("Invalid block offset %d for postings block idx %d", blockOffset, block), input);

            input.seek(blockOffset);
            byte bitsPerValue = input.readByte();
            if (bitsPerValue > 64)
                throw new CorruptIndexException(String.format("Postings list #%s block is corrupted. Bits per value should be no more than 64 and is %d.", block, bitsPerValue), input);

            // no bits means that all the deltas of the block are zero
            LongValues deltas = bitsPerValue == 0
                                ? LongValues.ZEROES
                                : LuceneCompat.directReaderGetInstance(seekingInput, bitsPerValue, input.getFilePointer());
            int entries = Math.min(remaining, blockEntries);
            for (int i = 0; i < entries; i++)
            {
                rowId += Math.toIntExact(deltas.get(readFrequencies ? 2L * i : i));
                rowIds.add(rowId);
            }
            remaining -= entries;
        }
        postingsDecoded += numPostings;
    }

    /**
     * @return the number of row ids read so far
     */
    public long postingsDecoded()
    {
        return postingsDecoded;
    }
}
