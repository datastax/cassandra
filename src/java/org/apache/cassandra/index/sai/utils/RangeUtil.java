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

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.IPartitioner;
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
     * Returns the average number of rows per partition, never less than one.
     * If the counts are unknown (not positive), it assumes one row per partition.
     */
    private static double avgRowsPerPartition(long rows, long partitions)
    {
        return rows > 0 && partitions > 0 ? Math.max(1.0, (double) rows / partitions) : 1.0;
    }

    /**
     * Estimates how many of the given {@code rows} in the given number of {@code partitions}, assumed to be spread
     * uniformly over the whole ring, fall into the {@code keyRange}. Does no I/O, it only uses ring arithmetic.
     * <p>
     * A key range that selects a single partition covers a negligible fraction of the ring, so the ring-based
     * estimate would round down to zero. We assume that any key range matches at least one partition,
     * therefore the estimate is never lower than the average number of rows per partition.
     *
     * @throws UnsupportedOperationException if the partitioner doesn't support token arithmetic
     */
    @VisibleForTesting
    static long estimateRowsInRange(long rows, long partitions, AbstractBounds<PartitionPosition> keyRange)
    {
        IPartitioner partitioner = keyRange.left.getPartitioner();
        Token minToken = partitioner.getMinimumToken();
        Token maxToken = partitioner.getMaximumToken();
        return estimateRowsInRange(rows, avgRowsPerPartition(rows, partitions), minToken, maxToken, keyRange);
    }

    /**
     * Estimates how many rows of the given memtable fall into the {@code keyRange}, assuming the rows are spread
     * uniformly over the whole ring.
     * Does no I/O, it only uses ring arithmetic.
     * The estimate is never lower than the average partition size.
     */
    public static long estimateRowsInRange(Memtable memtable, AbstractBounds<PartitionPosition> keyRange)
    {
        if (memtable.isClean())
            return 0;

        long rows = Memtable.estimateRowCount(memtable);
        long partitions = memtable.partitionCount();
        return estimateRowsInRange(rows,
                                   avgRowsPerPartition(rows, partitions),
                                   memtable.minPartitionKey().getToken(),
                                   memtable.maxPartitionKey().getToken(),
                                   keyRange);
    }

    /**
     * Estimates how many rows of the given index segment fall into the {@code keyRange}, assuming the rows are spread
     * uniformly over the tokens between the min and the max key of the segment.
     * Does no I/O, it only uses ring arithmetic.
     * The estimate for a key range that overlaps the segment is never lower than the average partition size
     * in the sstable the segment belongs to.
     *
     * @param sstable the sstable the segment belongs to
     * @param segment the index segment of the sstable
     */
    public static long estimateRowsInRange(SSTableReader sstable, SegmentMetadata segment, AbstractBounds<PartitionPosition> keyRange)
    {
        long segmentRows = 1 + segment.maxSSTableRowId - segment.minSSTableRowId;
        return estimateRowsInRange(segmentRows,
                                   avgRowsPerPartition(sstable.getTotalRows(), sstable.estimatedKeys()),
                                   segment.minKey.token(),
                                   segment.maxKey.token(),
                                   keyRange);
    }

    /**
     * Estimates how many rows of the given sstable fall into the {@code keyRange}, assuming the rows are spread
     * uniformly over the tokens between the first and the last token of the sstable.
     * Does no I/O, it only uses ring arithmetic.
     * The estimate for a key range that overlaps the sstable is never lower than the average partition size.
     */
    public static long estimateRowsInRange(SSTableReader sstable, AbstractBounds<PartitionPosition> keyRange)
    {
        return estimateRowsInRange(sstable.getTotalRows(),
                                   avgRowsPerPartition(sstable.getTotalRows(), sstable.estimatedKeys()),
                                   sstable.first.getToken(),
                                   sstable.last.getToken(),
                                   keyRange);
    }

    /**
     * Estimates how many of the given {@code rows}, assumed to be spread uniformly over tokens between
     * {@code minToken} and {@code maxToken} (both inclusive), fall into the {@code keyRange}.
     * Does no I/O, it only uses ring arithmetic.
     * <p>
     * If the key range overlaps with the token span, at least one partition is assumed to match, so the estimate
     * is never lower than {@code avgRowsPerPartition}.
     * <p>
     * If the partitioner does not support token arithmetic, returns {@code rows} if
     * the key range overlaps with the token span and 0 otherwise.
     */
    private static long estimateRowsInRange(long rows,
                                            double avgRowsPerPartition,
                                            Token minToken,
                                            Token maxToken,
                                            AbstractBounds<PartitionPosition> keyRange)
    {
        if (rows <= 0)
            return 0;
        if (coversFullRing(keyRange))
            return rows;

        if (keyRange instanceof Range && ((Range<PartitionPosition>) keyRange).isTrulyWrapAround())
        {
            long sum = 0;
            for (Range<PartitionPosition> unwrapped : ((Range<PartitionPosition>) keyRange).unwrap())
                sum += estimateRowsInRange(rows, avgRowsPerPartition, minToken, maxToken, unwrapped);
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

        try
        {
            double spanFraction = minToken.size(maxToken.nextValidToken());
            double overlapFraction = lo.size(hi.nextValidToken());
            return clampToPartition(rows, avgRowsPerPartition, rows * Math.min(1.0, overlapFraction / spanFraction));
        }
        catch (UnsupportedOperationException e)
        {
            // partitioners like ByteOrderedPartitioner, OrderPreservingPartitioner and LocalPartitioner
            // don't support token arithmetic
            return rows;
        }
    }

    /** Rounds the estimate, but never goes below the size of one average partition nor above the total row count. */
    private static long clampToPartition(long rows, double avgRowsPerPartition, double estimate)
    {
        return Math.min(rows, Math.round(Math.max(avgRowsPerPartition, estimate)));
    }
}
