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

package org.apache.cassandra.index.sai.disk.vector;

import java.io.IOException;
import java.util.PrimitiveIterator;
import java.util.function.Supplier;

import io.github.jbellis.jvector.util.BitSet;
import io.github.jbellis.jvector.util.Bits;
import io.github.jbellis.jvector.util.SparseBits;
import org.apache.cassandra.index.sai.disk.v5.V5VectorPostingsWriter;
import org.apache.cassandra.index.sai.utils.SingletonIntIterator;

public interface OnDiskOrdinalsMap extends AutoCloseable
{
    /** maps from vector ordinals returned by index search to rowids in the sstable */
    RowIdsView getRowIdsView();

    default Bits ignoringDeleted(Bits acceptBits) {
        return acceptBits;
    }

    /** maps from rowids to their associated ordinals, for setting up the ordinals-to-accept in a restricted search */
    OrdinalsView getOrdinalsView();

    void close();

    V5VectorPostingsWriter.Structure getStructure();

    class OneToOneRowIdsView implements RowIdsView {

        @Override
        public PrimitiveIterator.OfInt getSegmentRowIdsMatching(int vectorOrdinal) throws IOException
        {
            return new SingletonIntIterator(vectorOrdinal);
        }

        @Override
        public void close()
        {
            // noop
        }
    }

    class EmptyRowIdsView implements RowIdsView
    {
        @Override
        public PrimitiveIterator.OfInt getSegmentRowIdsMatching(int vectorOrdinal) throws IOException
        {
            return new PrimitiveIterator.OfInt()
            {
                @Override
                public int nextInt()
                {
                    throw new IllegalStateException();
                }

                @Override
                public boolean hasNext()
                {
                    return false;
                }
            };
        }

        @Override
        public void close()
        {
            // noop
        }
    }

    /**
     * An OrdinalsView that always returns -1 for all rowIds. This is used when the segment has no postings, which
     * can happen if all the graph's ordinals are in the deletedOrdinals set.
     */
    class EmptyOrdinalsView implements OrdinalsView
    {
        @Override
        public int getOrdinalForRowId(int rowId) throws IOException
        {
            return -1;
        }

        @Override
        public void forEachOrdinalInRange(int startRowId, int endRowId, OrdinalConsumer consumer) throws IOException
        {
            // noop
        }

        @Override
        public Bits buildOrdinalBits(int startRowId, int endRowId, Supplier<SparseBits> bitsSupplier) throws IOException
        {
            // Get an empty bitset
            return bitsSupplier.get();
        }

        @Override
        public void close()
        {
            // noop
        }
    }

    /** Bits matching the given range, inclusively. */
    class MatchRangeBits extends BitSet
    {
        final int lowerBound;
        final int upperBound;

        public MatchRangeBits(int lowerBound, int upperBound) {
            // bitset is empty if lowerBound > upperBound
            this.lowerBound = lowerBound;
            this.upperBound = upperBound;
        }

        @Override
        public boolean get(int index) {
            return lowerBound <= index && index <= upperBound;
        }

        @Override
        public int length() {
            if (lowerBound > upperBound)
                return 0;
            return upperBound - lowerBound + 1;
        }

        @Override
        public void set(int i)
        {
            throw new UnsupportedOperationException("not supported");
        }

        @Override
        public boolean getAndSet(int i)
        {
            throw new UnsupportedOperationException("not supported");
        }

        @Override
        public void clear(int i)
        {
            throw new UnsupportedOperationException("not supported");
        }

        @Override
        public void clear(int i, int i1)
        {
            throw new UnsupportedOperationException("not supported");
        }

        @Override
        public int cardinality()
        {
            return length();
        }

        @Override
        public int approximateCardinality()
        {
            return length();
        }

        @Override
        public int prevSetBit(int i)
        {
            throw new UnsupportedOperationException("not supported");
        }

        @Override
        public int nextSetBit(int i)
        {
            throw new UnsupportedOperationException("not supported");
        }

        @Override
        public long ramBytesUsed()
        {
            return 2 * Integer.BYTES;
        }
    }

    class OneToOneOrdinalsView implements OrdinalsView
    {
        // The number of ordinals in the segment. If we see a rowId greater than or equal to this, we know it's not in
        // the graph.
        private final int size;

        public OneToOneOrdinalsView(int size)
        {
            this.size = size;
        }

        @Override
        public int getOrdinalForRowId(int rowId) throws IOException
        {
            if (rowId >= size)
                return -1;
            return rowId;
        }

        @Override
        public void forEachOrdinalInRange(int startRowId, int endRowId, OrdinalConsumer consumer) throws IOException
        {
            // risk of overflow
            assert endRowId < Integer.MAX_VALUE : "endRowId must be less than Integer.MAX_VALUE";
            assert endRowId >= startRowId : "endRowId must be greater than or equal to startRowId";

            int start = Math.max(startRowId, 0);
            int end = Math.min(endRowId + 1, size);
            for (int rowId = start; rowId < end; rowId++)
                consumer.accept(rowId, rowId);
        }

        @Override
        public BitSet buildOrdinalBits(int startRowId, int endRowId, Supplier<SparseBits> unused) throws IOException
        {
            int start = Math.max(startRowId, 0);
            int end = Math.min(endRowId, size - 1);

            return new MatchRangeBits(start, end);
        }

        @Override
        public void close()
        {
            // noop
        }
    }
}
