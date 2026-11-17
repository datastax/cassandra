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

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

import io.github.jbellis.jvector.util.DenseIntMap;
import io.github.jbellis.jvector.util.RamUsageEstimator;
import io.github.jbellis.jvector.vector.types.VectorFloat;

/**
 * A live index's vectors by ordinal, each read from the memtable's stored copy (see {@link MemtableVector}), so the
 * index holds no full-precision copy of its own once each write completes. Each read returns a new vector.
 */
public class MemtableVectorValues implements RamAwareVectorValues
{
    private final int dimension;
    private final DenseIntMap<MemtableVector> values = new DenseIntMap<>(1024);
    // heap held by incoming vectors not yet attached to the memtable's stored copies
    private final AtomicLong incomingBytes = new AtomicLong();

    public MemtableVectorValues(int dimension)
    {
        this.dimension = dimension;
    }

    /**
     * @return heap bytes the new entry holds: its reference, the memtable vector, and the incoming vector it keeps
     * until {@link #attach} releases it
     */
    public long add(int ordinal, MemtableVector vector)
    {
        if (!values.compareAndPut(ordinal, null, vector))
            throw new IllegalStateException("Vector already exists for ordinal " + ordinal);
        long incoming = vector.ramBytesUsed() - MemtableVector.SHALLOW_SIZE;
        incomingBytes.addAndGet(incoming);
        return RamUsageEstimator.NUM_BYTES_OBJECT_REF + MemtableVector.SHALLOW_SIZE + incoming;
    }

    /**
     * Points {@code vector} at the memtable's stored copy {@code value} (see {@link MemtableVector#attach}).
     *
     * @return the heap bytes released, for the caller to return to the memtable's pool
     */
    public long attach(MemtableVector vector, ByteBuffer value)
    {
        long released = vector.attach(value);
        if (released > 0)
            incomingBytes.addAndGet(-released);
        return released;
    }

    @Override
    public int size()
    {
        return values.size();
    }

    @Override
    public int dimension()
    {
        return dimension;
    }

    @Override
    public VectorFloat<?> getVector(int i)
    {
        MemtableVector vector = values.get(i);
        return vector == null ? null : vector.vector();
    }

    @Override
    public void getVectorInto(int node, VectorFloat<?> destination, int offset)
    {
        // reranking reads each candidate into a scratch vector, so reading in place allocates nothing
        values.get(node).readInto(destination, offset);
    }

    @Override
    public boolean isValueShared()
    {
        return false;
    }

    @Override
    public MemtableVectorValues copy()
    {
        // every read returns a new vector, so this can be shared
        return this;
    }

    /** The number of vectors that still hold their own copy instead of reading the memtable's stored cell. */
    public int vectorsHoldingCopies()
    {
        int count = 0;
        for (int i = 0; i < values.size(); i++)
        {
            MemtableVector vector = values.get(i);
            if (vector != null && vector.isPending())
                count++;
        }
        return count;
    }

    @Override
    public long ramBytesUsed()
    {
        return 2L * RamUsageEstimator.NUM_BYTES_OBJECT_REF
               + RamEstimation.denseIntMapRamUsed(values.size())
               + values.size() * (RamUsageEstimator.NUM_BYTES_OBJECT_REF + MemtableVector.SHALLOW_SIZE)
               + incomingBytes.get();
    }
}
