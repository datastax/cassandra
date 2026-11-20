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

package org.apache.cassandra.index.sai.utils;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.sai.disk.v1.SegmentMetadata;
import org.apache.cassandra.io.sstable.format.SSTableReader;

public class RangeUtil
{
    private static final Token.KeyBound MIN_KEY_BOUND = DatabaseDescriptor.getPartitioner().getMinimumToken().minKeyBound();

    public static boolean coversFullRing(AbstractBounds<PartitionPosition> keyRange)
    {
        return keyRange.left.equals(MIN_KEY_BOUND) && keyRange.right.equals(MIN_KEY_BOUND);
    }

    /**
     * Check if the provided {@link SSTableReader} intersects with the provided key range.
     * @param reader SSTableReader
     * @param keyRange key range
     * @return true the key range intersects with the min/max key bounds
     */
    public static boolean intersects(SSTableReader reader, AbstractBounds<PartitionPosition> keyRange)
    {
        return intersects(reader.first.getToken().minKeyBound(), reader.last.getToken().maxKeyBound(), keyRange);
    }

    /**
     * Check if the min/max key bounds intersects with the keyRange
     * @param minKeyBound min key bound
     * @param maxKeyBound max key bound
     * @param keyRange key range
     * @return true the key range intersects with the min/max key bounds
     */
    public static boolean intersects(Token.KeyBound minKeyBound, Token.KeyBound maxKeyBound, AbstractBounds<PartitionPosition> keyRange)
    {
        if (keyRange instanceof Range && ((Range<?>)keyRange).isWrapAround())
            return keyRange.contains(minKeyBound) || keyRange.contains(maxKeyBound);

        int cmp = keyRange.right.compareTo(minKeyBound);
        // if right is minimum, it means right is the max token and bigger than maxKey.
        // if right bound is less than minKeyBound, no intersection
        if (!keyRange.right.isMinimum() && (!keyRange.inclusiveRight() && cmp == 0 || cmp < 0))
            return false;

        cmp = keyRange.left.compareTo(maxKeyBound);
        // if left bound is bigger than maxKeyBound, no intersection
        return (keyRange.isStartInclusive() || cmp != 0) && cmp <= 0;
    }


    /**
     * Estimates how many of the given {@code rows}, assumed to be spread uniformly over the whole ring,
     * fall into the {@code keyRange}. Does no I/O, it only uses ring arithmetic.
     *
     * @throws UnsupportedOperationException if the partitioner doesn't support token arithmetic
     */
    public static long estimateRowsInRange(long rows, AbstractBounds<PartitionPosition> keyRange)
    {
        if (rows <= 0)
            return 0;
        if (coversFullRing(keyRange))
            return rows;
        return Math.round(rows * getRingFraction(keyRange));
    }

    /**
     * Estimates how many rows of the given index segment fall into the {@code keyRange}, assuming the rows are spread
     * uniformly over the tokens between the min and the max key of the segment.
     * Does no I/O, it only uses ring arithmetic.
     */
    public static long estimateRowsInRange(SegmentMetadata segment, AbstractBounds<PartitionPosition> keyRange)
    {
        long segmentRows = 1 + segment.maxSSTableRowId - segment.minSSTableRowId;
        return estimateRowsInRange(segmentRows, segment.minKey.token(), segment.maxKey.token(), keyRange);
    }

    /**
     * Estimates how many rows of the given sstable fall into the {@code keyRange}, assuming the rows are spread
     * uniformly over the tokens between the first and the last token of the sstable.
     * Does no I/O, it only uses ring arithmetic.
     */
    public static long estimateRowsInRange(SSTableReader sstable, AbstractBounds<PartitionPosition> keyRange)
    {
        return estimateRowsInRange(sstable.getTotalRows(), sstable.first.getToken(), sstable.last.getToken(), keyRange);
    }

    /**
     * Estimates how many of the given {@code rows}, assumed to be spread uniformly over tokens between
     * {@code minToken} and {@code maxToken} (both inclusive), fall into the {@code keyRange}.
     * Does no I/O, it only uses ring arithmetic.
     *
     * @throws UnsupportedOperationException if the partitioner doesn't support token arithmetic
     */
    @SuppressWarnings("unchecked")
    private static long estimateRowsInRange(long rows, Token minToken, Token maxToken, AbstractBounds<PartitionPosition> keyRange)
    {
        if (rows <= 0)
            return 0;
        if (coversFullRing(keyRange))
            return rows;

        if (keyRange instanceof Range && ((Range<PartitionPosition>) keyRange).isTrulyWrapAround())
        {
            long sum = 0;
            for (Range<PartitionPosition> unwrapped : ((Range<PartitionPosition>) keyRange).unwrap())
                sum += estimateRowsInRange(rows, minToken, maxToken, unwrapped);
            return Math.min(rows, sum);
        }

        Token lo = keyRange.left.getToken();
        if (lo.compareTo(minToken) < 0)
            lo = minToken;
        // a minimum right bound denotes the end of the ring
        Token hi = keyRange.right.isMinimum() ? maxToken : keyRange.right.getToken();
        if (hi.compareTo(maxToken) > 0)
            hi = maxToken;
        if (lo.compareTo(hi) > 0)
            return 0;

        double spanFraction = minToken.size(maxToken.nextValidToken());
        double overlapFraction = lo.size(hi.nextValidToken());
        return Math.round(rows * Math.min(1.0, overlapFraction / spanFraction));
    }

    public static double getRingFraction(AbstractBounds<PartitionPosition> keyRange)
    {
        return keyRange.left.getToken().size(keyRange.right.getToken().nextValidToken());
    }
}
