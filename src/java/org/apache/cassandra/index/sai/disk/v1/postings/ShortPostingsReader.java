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
 * Reads whole posting lists written by {@link PostingsWriter}, when they are short, into a
 * {@link SortedRowIdsBuilder}.
 * <p>
 * One instance serves any number of posting lists of the same file. Unlike a {@link PostingsReader} it keeps no
 * state per posting list, which makes it suitable for the queries that visit a very large number of terms with
 * a few rows each.
 */
@NotThreadSafe
public class ShortPostingsReader
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
    public ShortPostingsReader(IndexInput input, boolean readFrequencies)
    {
        this.input = input;
        this.seekingInput = new SeekingRandomAccessInput(input);
        this.readFrequencies = readFrequencies;
    }

    /**
     * Adds to {@code rowIds} the row ids of the posting list whose summary is at the given offset, if the list has
     * at most {@code maxPostings} row ids.
     *
     * @return {@code false} if the posting list is longer than that, in which case nothing is added
     */
    public boolean readIfShort(long summaryOffset, int maxPostings, SortedRowIdsBuilder rowIds) throws IOException
    {
        input.seek(summaryOffset);
        int blockEntries = input.readVInt();
        int numPostings = input.readVInt();
        // only the lists made of one block are read here, as that block holds all the row ids
        if (numPostings > maxPostings || numPostings > blockEntries)
            return false;
        if (numPostings == 0)
            return true;

        int numBlocks = input.readVInt();
        if (numBlocks != 1)
            throw new CorruptIndexException(String.format("Postings list header is corrupted: %d blocks for %d postings in blocks of %d.",
                                                          numBlocks, numPostings, blockEntries), input);
        input.readVLong(); // length of the block offsets

        byte offsetBitsPerValue = input.readByte();
        if (offsetBitsPerValue > 64)
            throw new CorruptIndexException(String.format("Postings list header is corrupted: Bits per value for block offsets must be no more than 64 and is %d.", offsetBitsPerValue), input);
        long blockOffset = offsetBitsPerValue == 0
                           ? 0
                           : LuceneCompat.directReaderGetInstance(seekingInput, offsetBitsPerValue, input.getFilePointer()).get(0);
        if (blockOffset < 4) // the first 4 bytes must be CODEC_MAGIC
            throw new CorruptIndexException(String.format("Invalid block offset %d for the postings block", blockOffset), input);

        input.seek(blockOffset);
        byte bitsPerValue = input.readByte();
        if (bitsPerValue > 64)
            throw new CorruptIndexException(String.format("Postings list block is corrupted. Bits per value should be no more than 64 and is %d.", bitsPerValue), input);

        // the row ids are stored as deltas, the first one from zero; no bits means that all the deltas are zero
        LongValues deltas = bitsPerValue == 0
                            ? LongValues.ZEROES
                            : LuceneCompat.directReaderGetInstance(seekingInput, bitsPerValue, input.getFilePointer());
        int rowId = 0;
        for (int i = 0; i < numPostings; i++)
        {
            rowId += Math.toIntExact(deltas.get(readFrequencies ? 2L * i : i));
            rowIds.add(rowId);
        }
        postingsDecoded += numPostings;
        return true;
    }

    /**
     * @return the number of row ids read so far
     */
    public long postingsDecoded()
    {
        return postingsDecoded;
    }
}
