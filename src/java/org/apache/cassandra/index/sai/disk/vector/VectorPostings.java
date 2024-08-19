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

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.ToIntFunction;

import com.google.common.base.Preconditions;

import io.github.jbellis.jvector.util.RamUsageEstimator;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.hash.serialization.BytesReader;
import net.openhft.chronicle.hash.serialization.BytesWriter;
import org.agrona.collections.IntArrayList;

public class VectorPostings<T>
{
    // we expect that the overwhelmingly most common cardinality will be 1, so optimize for reads using COWAL
    final CopyOnWriteArrayList<T> postings;
    volatile int ordinal = -1;

    private volatile IntArrayList rowIds; // initially null; gets filled in on flush by computeRowIds

    public VectorPostings(T firstKey)
    {
        postings = new CopyOnWriteArrayList<>(List.of(firstKey));
    }

    public VectorPostings(List<T> raw)
    {
        postings = new CopyOnWriteArrayList<>(raw);
    }

    /**
     * Split out from constructor only to make dealing with concurrent inserts easier for CassandraOnHeapGraph.
     * Should be called at most once per instance.
     */
    public void setOrdinal(int ordinal)
    {
        assert this.ordinal == -1 : String.format("ordinal already set to %d; attempted to set to %d", this.ordinal, ordinal);
        this.ordinal = ordinal;
    }

    public boolean add(T key)
    {
        for (T existing : postings)
            if (existing.equals(key))
                return false;
        postings.add(key);
        return true;
    }

    /**
     * @return true if current ordinal is removed by partition/range deletion.
     * Must be called after computeRowIds.
     */
    public boolean shouldAppendDeletedOrdinal()
    {
        return !postings.isEmpty() && (rowIds != null && rowIds.isEmpty());
    }

    /**
     * Compute the rowIds corresponding to the < T > keys in this postings list.
     */
    public void computeRowIds(ToIntFunction<T> postingTransformer)
    {
        Preconditions.checkState(rowIds == null);

        IntArrayList ids = new IntArrayList(postings.size(), -1);
        for (T key : postings)
        {
            int rowId = postingTransformer.applyAsInt(key);
            // partition deletion and range deletion won't trigger index update. There is no row id for given key during flush
            if (rowId >= 0)
                ids.add(rowId);
        }

        rowIds = ids;
    }

    /**
     * @return rowIds corresponding to the < T > keys in this postings list.
     * Must be called after computeRowIds.
     */
    public IntArrayList getRowIds()
    {
        Preconditions.checkNotNull(rowIds);
        return rowIds;
    }

    public void remove(T key)
    {
        postings.remove(key);
    }

    public long ramBytesUsed()
    {
        return emptyBytesUsed() + postings.size() * bytesPerPosting();
    }

    public static long emptyBytesUsed()
    {
        long REF_BYTES = RamUsageEstimator.NUM_BYTES_OBJECT_REF;
        long AH_BYTES = RamUsageEstimator.NUM_BYTES_ARRAY_HEADER;
        return Integer.BYTES + REF_BYTES + AH_BYTES;
    }

    // we can't do this exactly without reflection, because keys could be Integer or PrimaryKey.
    // PK is larger, so we'll take that and return an upper bound.
    // we already count the float[] vector in vectorValues, so leave it out here
    public long bytesPerPosting()
    {
        long REF_BYTES = RamUsageEstimator.NUM_BYTES_OBJECT_REF;
        return REF_BYTES
               + 2 * Long.BYTES // hashes in PreHashedDecoratedKey
               + REF_BYTES; // key ByteBuffer, this is used elsewhere so we don't take the deep size
    }

    public int size()
    {
        return postings.size();
    }

    public List<T> getPostings()
    {
        return postings;
    }

    public boolean isEmpty()
    {
        return postings.isEmpty();
    }

    public int getOrdinal()
    {
        assert ordinal >= 0 : "ordinal not set";
        return ordinal;
    }

    public static class CompactionVectorPostings extends VectorPostings<Integer> {
        public CompactionVectorPostings(int ordinal, List<Integer> raw)
        {
            super(raw);
            this.ordinal = ordinal;
        }

        public CompactionVectorPostings(int ordinal, int firstKey)
        {
            super(firstKey);
            this.ordinal = ordinal;
        }

        @Override
        public void setOrdinal(int ordinal)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public IntArrayList getRowIds()
        {
            var L = new IntArrayList(size(), -1);
            for (var i : postings)
                L.addInt(i);
            return L;
        }

        // CVP always contains int keys, so we don't have to be pessimistic on size like super does
        @Override
        public long bytesPerPosting()
        {
            long REF_BYTES = RamUsageEstimator.NUM_BYTES_OBJECT_REF;
            return REF_BYTES + Integer.BYTES;
        }
    }

    static class Marshaller implements BytesReader<CompactionVectorPostings>, BytesWriter<CompactionVectorPostings>
    {
        @Override
        public void write(Bytes out, CompactionVectorPostings postings) {
            out.writeInt(postings.ordinal);
            out.writeInt(postings.size());
            for (Integer posting : postings.getPostings()) {
                out.writeInt(posting);
            }
        }

        @Override
        public CompactionVectorPostings read(Bytes in, CompactionVectorPostings using) {
            int ordinal = in.readInt();
            int size = in.readInt();
            assert size >= 0 : size;
            CompactionVectorPostings cvp;
            if (size == 1) {
                cvp = new CompactionVectorPostings(ordinal, in.readInt());
            }
            else
            {
                var postingsList = new IntArrayList(size, -1);
                for (int i = 0; i < size; i++)
                {
                    postingsList.add(in.readInt());
                }
                cvp = new CompactionVectorPostings(ordinal, postingsList);
            }
            return cvp;
        }
    }
}
