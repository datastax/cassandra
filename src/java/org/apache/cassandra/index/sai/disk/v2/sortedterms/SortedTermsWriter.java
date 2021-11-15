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

package org.apache.cassandra.index.sai.disk.v2.sortedterms;

import java.io.IOException;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.base.Preconditions;

import org.apache.cassandra.index.sai.disk.io.IndexOutputWriter;
import org.apache.cassandra.index.sai.disk.io.RAMIndexOutput;
import org.apache.cassandra.io.tries.IncrementalDeepTrieWriterPageAware;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.packed.DirectMonotonicWriter;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedLongValues;


import static org.apache.cassandra.index.sai.disk.v1.trie.TrieTermsDictionaryReader.trieSerializer;

/**
 * Writes an ordered sequence of terms for use with {@link SortedTermsReader}.
 * <p>
 * Terms must be added in lexicographical ascending order.
 * Terms can be of varying lengths.
 *
 * <p>
 * Important implementation note: SAI blocked packed readers are slow,
 * and Lucene MonotonicBlockPackedReader is slow.  Using them
 * will cause this class to slow considerably.
 *
 * For documentation of the underlying on-disk data structures, see the package documentation.
 *
 * @see SortedTermsReader
 * @see org.apache.cassandra.index.sai.disk.v2.sortedterms
 */
@NotThreadSafe
public class SortedTermsWriter
{
    // The TERMS_DICT_ constants allow for quickly determining the id of the current block based on a point id
    // or to check if we are exactly at the beginning of the block.
    // Terms data are organized in blocks of (2 ^ TERMS_DICT_BLOCK_SHIFT) terms.
    // The blocks should not be too small because they allow prefix compression of
    // the terms except the first term in a block.
    // The blocks should not be too large because we can't just ranfomly jump to the term inside the block,
    // but we have to iterate through all the terms from the start of the block.
    static final int TERMS_DICT_BLOCK_SHIFT = 4;
    static final int TERMS_DICT_BLOCK_SIZE = 1 << TERMS_DICT_BLOCK_SHIFT;
    static final int TERMS_DICT_BLOCK_MASK = TERMS_DICT_BLOCK_SIZE - 1;

    static final int DIRECT_MONOTONIC_BLOCK_SHIFT = 16;

    private final IncrementalDeepTrieWriterPageAware<Long> trieWriter;
    private final IndexOutput termsOutput;
    private final IndexOutput offsetsOutput;

    private BytesRefBuilder prevTerm = new BytesRefBuilder();
    private BytesRefBuilder tempTerm = new BytesRefBuilder();

    private final long bytesStartFP;
    private final PackedLongValues.Builder offsetsBuilder;

    private int maxLength = -1;
    private long pointId = 0;

    /**
     * Creates a new writer.
     * <p>
     * It does not own the components, so you must close the components by yourself
     * after you're done with the writer.
     *
     * @param termsData where to write the prefix-compressed terms data
     * @param termsDataBlockOffsets  where to write the offsets of each block of terms data
     * @param trieWriter where to write the trie that maps the terms to point ids
     */
    public SortedTermsWriter(@Nonnull IndexOutput termsData,
                             @Nonnull IndexOutput termsDataBlockOffsets,
                             @Nonnull IndexOutputWriter trieWriter)
    {
        this.trieWriter = new IncrementalDeepTrieWriterPageAware<>(trieSerializer, trieWriter.asSequentialWriter());
        this.termsOutput = termsData;
        this.bytesStartFP = termsData.getFilePointer();
        this.offsetsOutput = termsDataBlockOffsets;

        offsetsBuilder = PackedLongValues.deltaPackedBuilder(PackedInts.COMPACT);
    }

    /**
     * Appends a term at the end of the sequence.
     * Terms must be added in lexicographic order.
     *
     * @throws IOException if write to disk fails
     * @throws IllegalArgumentException if the term is not greater than the previous added term
     */
    public void add(final @Nonnull ByteComparable term) throws IOException
    {
        tempTerm.clear();
        copyBytes(term, tempTerm);

        final BytesRef termRef = tempTerm.get();
        final BytesRef prevTermRef = this.prevTerm.get();

        Preconditions.checkArgument(prevTermRef.length == 0 || prevTermRef.compareTo(termRef) < 0,
                                    "Terms must be added in lexicographic ascending order.");
        writeTermData(termRef);
        writeTermToTrie(term);

        maxLength = Math.max(maxLength, termRef.length);
        swapTempWithPrevious();
        pointId++;
    }

    private void writeTermToTrie(ByteComparable term) throws IOException
    {
        trieWriter.add(term, pointId);
    }

    private void writeTermData(BytesRef term) throws IOException
    {
        if ((pointId & TERMS_DICT_BLOCK_MASK) == 0)
        {
            offsetsBuilder.add(termsOutput.getFilePointer() - bytesStartFP);

            termsOutput.writeVInt(term.length);
            termsOutput.writeBytes(term.bytes, term.offset, term.length);
        }
        else
        {
            final int prefixLength = StringHelper.bytesDifference(prevTerm.get(), term);
            final int suffixLength = term.length - prefixLength;
            assert suffixLength > 0: "terms must be unique";

            termsOutput.writeByte((byte) (Math.min(prefixLength, 15) | (Math.min(15, suffixLength - 1) << 4)));
            if (prefixLength >= 15)
                termsOutput.writeVInt(prefixLength - 15);
            if (suffixLength >= 16)
                termsOutput.writeVInt(suffixLength - 16);

            termsOutput.writeBytes(term.bytes, term.offset + prefixLength, term.length - prefixLength);
        }
    }

    /**
     * Flushes any in-memory buffers to the output streams.
     * Does not close the output streams.
     * No more writes are allowed.
     */
    public @Nonnull SortedTermsMeta finish() throws IOException
    {
        RAMIndexOutput meta = new RAMIndexOutput("SortedTermsMeta");
        long blockCount = writeOffsets(meta);

        final long trieFP = this.trieWriter.complete();
        return new SortedTermsMeta(trieFP, pointId, maxLength, meta.getBytes(), blockCount);
    }


    /**
     * Writes the block offsets earlier added to the <code>offsetBuilder</code> to <code>offsetsOutput</code>.
     *
     * @return the number of offsets (== number of blocks)
     */
    private long writeOffsets(RAMIndexOutput meta) throws IOException
    {
        PackedLongValues offsets = offsetsBuilder.build();

        // DirectMonotonicReader is much faster than MonotonicBlockPackedReader
        DirectMonotonicWriter offsetsWriter =
            DirectMonotonicWriter.getInstance(meta, offsetsOutput, offsets.size(), DIRECT_MONOTONIC_BLOCK_SHIFT);

        for (int x = 0; x < offsets.size(); x++)
            offsetsWriter.add(offsets.get(x));

        offsetsWriter.finish();
        return offsets.size();
    }

    /**
     * Copies bytes from source to dest.
     */
    private void copyBytes(ByteComparable source, BytesRefBuilder dest)
    {
        ByteSource byteSource = source.asComparableBytes(ByteComparable.Version.OSS41);
        int val;
        while ((val = byteSource.next()) != ByteSource.END_OF_STREAM)
            dest.append((byte) val);
    }

    /**
     * Swaps <code>this.temp</code> with <code>this.previous</code>.
     * It is faster to swap the pointers instead of copying the data.
     */
    private void swapTempWithPrevious()
    {
        BytesRefBuilder temp = this.tempTerm;
        this.tempTerm = this.prevTerm;
        this.prevTerm = temp;
    }

}
