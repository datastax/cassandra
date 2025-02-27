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

import java.nio.ByteBuffer;
import java.util.List;
import java.util.NoSuchElementException;

import com.google.common.annotations.VisibleForTesting;

import org.agrona.collections.Int2IntHashMap;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.ByteBlockPool;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;
import org.apache.lucene.util.Counter;

/**
 * Indexes strings into an on-heap inverted index to be flushed in an SSTable attached index later.
 * For flushing use the PostingTerms interface.
 */
public class RAMStringIndexer
{
    @VisibleForTesting
    public static int MAX_BLOCK_BYTE_POOL_SIZE = Integer.MAX_VALUE;
    private final BytesRefHash termsHash;
    private final RAMPostingSlices slices;
    // counters need to be separate so that we can trigger flushes if either ByteBlockPool hits maximum size
    private final Counter termsBytesUsed;
    private final Counter slicesBytesUsed;

    private int[] lastSegmentRowID = new int[RAMPostingSlices.DEFAULT_TERM_DICT_SIZE];

    private final boolean writeFrequencies;
    private final Int2IntHashMap docLengths = new Int2IntHashMap(Integer.MIN_VALUE);

    public RAMStringIndexer(boolean writeFrequencies)
    {
        this.writeFrequencies = writeFrequencies;
        termsBytesUsed = Counter.newCounter();
        slicesBytesUsed = Counter.newCounter();

        ByteBlockPool termsPool = new ByteBlockPool(new ByteBlockPool.DirectTrackingAllocator(termsBytesUsed));

        termsHash = new BytesRefHash(termsPool);

        slices = new RAMPostingSlices(slicesBytesUsed, writeFrequencies);
    }

    public long estimatedBytesUsed()
    {
        return termsBytesUsed.get() + slicesBytesUsed.get();
    }

    public boolean requiresFlush()
    {
        // ByteBlockPool can't handle more than Integer.MAX_VALUE bytes. These are allocated in fixed-size chunks,
        // and additions are guaranteed to be smaller than the chunks. This means that the last chunk allocation will
        // be triggered by an addition, and the rest of the space in the final chunk will be wasted, as the bytesUsed
        // counters track block allocation, not the size of additions. This means that we can't pass this check and then
        // fail to add a term.
        return termsBytesUsed.get() >= MAX_BLOCK_BYTE_POOL_SIZE || slicesBytesUsed.get() >= MAX_BLOCK_BYTE_POOL_SIZE;
    }

    public boolean isEmpty()
    {
        return docLengths.isEmpty();
    }

    public Int2IntHashMap getDocLengths()
    {
        return docLengths;
    }

    /**
     * EXPENSIVE OPERATION due to sorting the terms, only call once.
     */
    // TODO: assert or throw and exception if getTermsWithPostings is called > 1
    public TermsIterator getTermsWithPostings(ByteBuffer minTerm, ByteBuffer maxTerm, ByteComparable.Version byteComparableVersion)
    {
        final int[] sortedTermIDs = termsHash.sort();

        final int valueCount = termsHash.size();
        final ByteSliceReader sliceReader = new ByteSliceReader();

        return new TermsIterator()
        {
            private int ordUpto = 0;
            private final BytesRef br = new BytesRef();

            @Override
            public ByteBuffer getMinTerm()
            {
                return minTerm;
            }

            @Override
            public ByteBuffer getMaxTerm()
            {
                return maxTerm;
            }

            public void close() {}

            @Override
            public PostingList postings()
            {
                int termID = sortedTermIDs[ordUpto - 1];
                final int maxSegmentRowId = lastSegmentRowID[termID];
                return slices.postingList(termID, sliceReader, maxSegmentRowId);
            }

            @Override
            public boolean hasNext() {
                return ordUpto < valueCount;
            }

            @Override
            public ByteComparable next()
            {
                if (!hasNext())
                    throw new NoSuchElementException();

                termsHash.get(sortedTermIDs[ordUpto], br);
                ordUpto++;
                return asByteComparable(br.bytes, br.offset, br.length);
            }

            private ByteComparable asByteComparable(byte[] bytes, int offset, int length)
            {
                // The bytes were encoded when they were inserted into the termsHash.
                return ByteComparable.preencoded(byteComparableVersion, bytes, offset, length);
            }
        };
    }

    /**
     * @return bytes allocated.  may be zero if the (term, row) pair is a duplicate
     */
    public long addAll(List<BytesRef> terms, int segmentRowId)
    {
        long startBytes = estimatedBytesUsed();
        Int2IntHashMap frequencies = new Int2IntHashMap(Integer.MIN_VALUE);
        Int2IntHashMap deltas = new Int2IntHashMap(Integer.MIN_VALUE);

        for (BytesRef term : terms)
        {
            int termID = termsHash.add(term);
            boolean firstOccurrence = termID >= 0;

            if (firstOccurrence)
            {
                // first time seeing this term in any row, create the term's first slice !
                slices.createNewSlice(termID);
                // grow the termID -> last segment array if necessary
                if (termID >= lastSegmentRowID.length - 1)
                    lastSegmentRowID = ArrayUtil.grow(lastSegmentRowID, termID + 1);
                if (writeFrequencies)
                    frequencies.put(termID, 1);
            }
            else
            {
                termID = (-termID) - 1;
                // compaction should call this method only with increasing segmentRowIds
                assert segmentRowId >= lastSegmentRowID[termID];
                // increment frequency
                if (writeFrequencies)
                    frequencies.put(termID, frequencies.getOrDefault(termID, 0) + 1);
                // Skip computing a delta if we've already seen this term in this row
                if (segmentRowId == lastSegmentRowID[termID])
                    continue;
            }

            // Compute the delta from the last time this term was seen, to this row
            int delta = segmentRowId - lastSegmentRowID[termID];
            // sanity check that we're advancing the row id, i.e. no duplicate entries.
            assert firstOccurrence || delta > 0;
            deltas.put(termID, delta);
            lastSegmentRowID[termID] = segmentRowId;
        }

        // add the postings now that we know the frequencies
        deltas.forEachInt((termID, delta) -> {
            slices.writePosting(termID, delta, frequencies.get(termID));
        });

        docLengths.put(segmentRowId, terms.size());

        return estimatedBytesUsed() - startBytes;
    }
}
