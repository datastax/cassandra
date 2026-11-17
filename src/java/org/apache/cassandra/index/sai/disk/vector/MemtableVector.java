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
import java.nio.ByteOrder;

import io.github.jbellis.jvector.disk.IndexWriter;
import io.github.jbellis.jvector.util.RamUsageEstimator;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.FloatArray;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;
import org.apache.cassandra.utils.memory.MemoryUtil;

/**
 * A live index's vector that the memtable holds: the incoming vector until the write that added it completes, then
 * the memtable's stored copy of the cell, read in place. A live index keeps one of these per distinct vector instead
 * of its own full-precision copy, so each live vector is held once.
 * <p>
 * The stored copy stays valid until the memtable is discarded: memtable values are freed only with the memtable, and
 * the memtable is discarded only after its flush and after every read that started before. Cells hold vectors as
 * big-endian floats, so reads convert them; callers that score vectors take {@link #vector()}, never this object.
 */
public final class MemtableVector implements VectorFloat<float[]>, FloatArray
{
    private static final VectorTypeSupport vts = VectorizationProvider.getInstance().getVectorTypeSupport();
    /** Heap bytes of one memtable vector once its stored copy is attached. */
    static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(MemtableVector.class);

    // a thread's reusable view of native memory, so reads of stored vectors allocate nothing
    private static final ThreadLocal<ByteBuffer> nativeView = ThreadLocal.withInitial(() -> MemoryUtil.getHollowDirectByteBuffer(ByteOrder.BIG_ENDIAN));

    private final int dimension;
    // orders vectors in the live index's postings map, so that most comparisons do not read the vectors
    private final long hash;
    // the incoming vector until the stored copy is attached; null after
    private volatile VectorFloat<?> pending;
    // the stored copy: a native address, or a buffer when the memtable keeps values on the heap
    private volatile long address;
    private volatile ByteBuffer stored;

    public MemtableVector(VectorFloat<?> incoming)
    {
        this.dimension = incoming.length();
        this.pending = incoming;
        long h = dimension;
        for (int i = 0; i < dimension; i++)
            h = (h ^ Float.floatToIntBits(incoming.get(i))) * 0x9E3779B97F4A7C15L;
        this.hash = h ^ (h >>> 31);
    }

    /**
     * Orders memtable vectors by hash, then by content, so equal vectors compare equal: the same order whatever
     * holds each vector, incoming or stored. Only vectors with the same hash, almost always equal ones, are read.
     */
    public static int compare(MemtableVector a, MemtableVector b)
    {
        int byHash = Long.compare(a.hash, b.hash);
        if (byHash != 0 || a == b)
            return byHash;
        for (int i = 0; i < a.dimension; i++)
        {
            int c = Integer.compare(Float.floatToIntBits(a.get(i)), Float.floatToIntBits(b.get(i)));
            if (c != 0)
                return c;
        }
        return Integer.compare(a.dimension, b.dimension);
    }

    /** Copies this vector into {@code destination} starting at {@code offset}, allocating nothing. */
    public void readInto(VectorFloat<?> destination, int offset)
    {
        VectorFloat<?> incoming = pending;
        if (incoming != null)
        {
            destination.copyFrom(incoming, 0, offset, dimension);
            return;
        }
        ByteBuffer source = storedView();
        for (int i = 0; i < dimension; i++)
            destination.set(offset + i, source.getFloat(i * Float.BYTES));
    }

    /** A big-endian view of the stored copy, positioned at its first float; a thread's reused view for native memory. */
    private ByteBuffer storedView()
    {
        if (address == 0)
            return stored.duplicate().order(ByteOrder.BIG_ENDIAN).slice().order(ByteOrder.BIG_ENDIAN);
        ByteBuffer view = nativeView.get();
        MemoryUtil.setDirectByteBuffer(view, address, dimension * Float.BYTES);
        return view;
    }

    /**
     * Reads this vector from the memtable's stored copy from now on, if {@code value} holds the same floats, and
     * releases the incoming vector.
     *
     * @return the heap bytes of the incoming vector this released; 0 when it was already released, or when
     * {@code value} is not the stored copy and this keeps its own
     */
    public synchronized long attach(ByteBuffer value)
    {
        VectorFloat<?> incoming = pending;
        if (incoming == null)
            return 0;
        if (value == null || value.remaining() != dimension * Float.BYTES)
            return 0;
        var floats = value.duplicate().order(ByteOrder.BIG_ENDIAN).asFloatBuffer();
        for (int i = 0; i < dimension; i++)
            if (Float.floatToIntBits(floats.get(i)) != Float.floatToIntBits(incoming.get(i)))
                return 0;
        if (value.isDirect())
            address = MemoryUtil.getAddress(value) + value.position();
        else
            stored = value.duplicate();
        pending = null; // published after the stored copy, so a reader that sees null finds it
        return incoming.ramBytesUsed();
    }

    /** Whether this still holds the incoming vector, its own copy, rather than reading the memtable's. */
    public boolean isPending()
    {
        return pending != null;
    }

    /** This vector as a scoring vector: the incoming vector, or a new one read from the memtable's stored copy. */
    public VectorFloat<?> vector()
    {
        VectorFloat<?> incoming = pending;
        return incoming != null ? incoming : vts.createFloatVector(array());
    }

    @Override
    public float[] array()
    {
        VectorFloat<?> incoming = pending;
        if (incoming instanceof FloatArray)
            return ((FloatArray) incoming).array();
        float[] result = new float[dimension];
        if (incoming != null)
        {
            for (int i = 0; i < dimension; i++)
                result[i] = incoming.get(i);
            return result;
        }
        ByteBuffer source = storedView();
        for (int i = 0; i < dimension; i++)
            result[i] = source.getFloat(i * Float.BYTES);
        return result;
    }

    @Override
    public float[] get()
    {
        return array();
    }

    @Override
    public int length()
    {
        return dimension;
    }

    @Override
    public float get(int i)
    {
        VectorFloat<?> incoming = pending;
        if (incoming != null)
            return incoming.get(i);
        return storedView().getFloat(i * Float.BYTES);
    }

    @Override
    public VectorFloat<float[]> copy()
    {
        return new MemtableVector(vector());
    }

    @Override
    public long ramBytesUsed()
    {
        VectorFloat<?> incoming = pending;
        return SHALLOW_SIZE + (incoming == null ? 0 : incoming.ramBytesUsed());
    }

    @Override
    public void writeTo(IndexWriter indexWriter)
    {
        throw new UnsupportedOperationException("A memtable vector is read through vector()");
    }

    @Override
    public void copyFrom(VectorFloat<?> src, int srcOffset, int destOffset, int length)
    {
        throw new UnsupportedOperationException("A memtable vector is read-only");
    }

    @Override
    public void set(int i, float value)
    {
        throw new UnsupportedOperationException("A memtable vector is read-only");
    }

    @Override
    public void zero()
    {
        throw new UnsupportedOperationException("A memtable vector is read-only");
    }
}
