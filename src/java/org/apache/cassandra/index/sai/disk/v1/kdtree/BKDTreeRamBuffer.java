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
package org.apache.cassandra.index.sai.disk.v1.kdtree;

import java.io.IOException;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import org.apache.cassandra.index.sai.disk.oldlucene.MutablePointValues;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.ByteBlockPool;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedLongValues;

/**
 * On-heap buffer for point values that provides a sortable view of itself as {@link MutablePointValues}.
 */
public class BKDTreeRamBuffer implements Accountable
{
    @VisibleForTesting
    public static int MAX_BLOCK_BYTE_POOL_SIZE = Integer.MAX_VALUE;
    // This counter should not be used to track any other allocations, as we use it to prevent block pool overflow
    private final Counter blockBytesUsed;
    private final ByteBlockPool bytes;
    private final int pointDimensionCount, pointNumBytes;
    private final int packedBytesLength;
    private final byte[] packedValue;
    private final PackedLongValues.Builder docIDsBuilder;
    private int numPoints;
    private int numRows;
    private int lastSegmentRowID = -1;
    private boolean closed = false;

    public BKDTreeRamBuffer(int pointDimensionCount, int pointNumBytes)
    {
        this.blockBytesUsed = Counter.newCounter();
        this.pointDimensionCount = pointDimensionCount;
        this.pointNumBytes = pointNumBytes;

        this.bytes = new ByteBlockPool(new ByteBlockPool.DirectTrackingAllocator(blockBytesUsed));

        packedValue = new byte[pointDimensionCount * pointNumBytes];
        packedBytesLength = pointDimensionCount * pointNumBytes;

        docIDsBuilder = PackedLongValues.deltaPackedBuilder(PackedInts.COMPACT);
    }

    @Override
    public long ramBytesUsed()
    {
        return docIDsBuilder.ramBytesUsed() + blockBytesUsed.get();
    }

    public boolean requiresFlush()
    {
        // ByteBlockPool can't handle more than Integer.MAX_VALUE bytes. These are allocated in fixed-size chunks,
        // and additions are guaranteed to be smaller than the chunks. This means that the last chunk allocation will
        // be triggered by an addition, and the rest of the space in the final chunk will be wasted, as the bytesUsed
        // counters track block allocation, not the size of additions. This means that we can't pass this check and then
        // fail to add a term.
        return blockBytesUsed.get() >= MAX_BLOCK_BYTE_POOL_SIZE;
    }

    public int numRows()
    {
        return numRows;
    }

    public long addPackedValue(int segmentRowId, BytesRef value)
    {
        ensureOpen();
        
        if (value.length != packedBytesLength)
        {
            throw new IllegalArgumentException("The value has length=" + value.length + " but should be " + pointDimensionCount * pointNumBytes);
        }

        long startingBlockBytesUsed = blockBytesUsed.get();
        long startingDocIDsBytesUsed = docIDsBuilder.ramBytesUsed();

        docIDsBuilder.add(segmentRowId);
        bytes.append(value);

        if (segmentRowId != lastSegmentRowID)
        {
            numRows++;
            lastSegmentRowID = segmentRowId;
        }

        numPoints++;

        long docIDsAllocatedBytes = docIDsBuilder.ramBytesUsed() - startingDocIDsBytesUsed;
        long blockAllocatedBytes = blockBytesUsed.get() - startingBlockBytesUsed;
        
        return docIDsAllocatedBytes + blockAllocatedBytes;
    }

    public MutableOneDimPointValues asPointValues()
    {
        ensureOpen();
        // building packed longs is destructive
        closed = true;
        final PackedLongValues docIDs = docIDsBuilder.build();
        return new MutableOneDimPointValues()
        {
            final int[] ords = new int[numPoints];

            {
                for (int i = 0; i < numPoints; ++i)
                {
                    ords[i] = i;
                }
            }

            @Override
            public void getValue(int i, BytesRef packedValue)
            {
                final long offset = (long) packedBytesLength * (long) ords[i];
                packedValue.length = packedBytesLength;
                bytes.setRawBytesRef(packedValue, offset);
            }

            @Override
            public byte getByteAt(int i, int k)
            {
                byte[] a = new byte[1];
                final long offset = (long) packedBytesLength * (long) ords[i] + (long) k;
                bytes.readBytes(offset, a, 0, 1);
                return a[0];
            }

            @Override
            public int getDocID(int i)
            {
                return Math.toIntExact(docIDs.get(ords[i]));
            }

            @Override
            public void swap(int i, int j)
            {
                int tmp = ords[i];
                ords[i] = ords[j];
                ords[j] = tmp;
            }

            @Override
            public void intersect(IntersectVisitor visitor) throws IOException
            {
                final BytesRef scratch = new BytesRef();
                for (int i = 0; i < numPoints; i++)
                {
                    getValue(i, scratch);
                    assert scratch.length == packedValue.length;
                    System.arraycopy(scratch.bytes, scratch.offset, packedValue, 0, packedBytesLength);
                    visitor.visit(getDocID(i), packedValue);
                }
            }

            @Override
            public int getNumDimensions()
            {
                return pointDimensionCount;
            }

            @Override
            public int getBytesPerDimension()
            {
                return pointNumBytes;
            }

            @Override
            public long size()
            {
                return numPoints;
            }

            @Override
            public int getDocCount()
            {
                return numRows;
            }
        };
    }

    private void ensureOpen()
    {
        Preconditions.checkState(!closed, "Expected open buffer.");
    }
}
