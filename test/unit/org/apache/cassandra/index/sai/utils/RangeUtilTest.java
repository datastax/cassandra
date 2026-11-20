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

package org.apache.cassandra.index.sai.utils;

import java.lang.reflect.Field;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Murmur3Partitioner.LongToken;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.sai.disk.v1.SegmentMetadata;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RangeUtilTest
{
    private static final long ROWS = 1000;

    @BeforeClass
    public static void setupClass()
    {
        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
    }

    private static AbstractBounds<PartitionPosition> range(Token left, Token right)
    {
        return new Range<>(left.minKeyBound(), right.maxKeyBound());
    }

    private static AbstractBounds<PartitionPosition> range(long left, long right)
    {
        return range(token(left), token(right));
    }

    private static Token token(long value)
    {
        return new LongToken(value);
    }

    @Test
    public void fullRing()
    {
        Token min = Murmur3Partitioner.instance.getMinimumToken();
        assertEquals(1000, RangeUtil.estimateRowsInRange(1000, range(min, min)));
    }

    @Test
    public void halfOfRing()
    {
        assertEquals(500, RangeUtil.estimateRowsInRange(1000, range(token(0), Murmur3Partitioner.instance.getMaximumToken())));
        assertEquals(500, RangeUtil.estimateRowsInRange(1000, range(Murmur3Partitioner.instance.getMinimumToken(), token(-1))));
    }

    @Test
    public void quarterOfRing()
    {
        assertEquals(250, RangeUtil.estimateRowsInRange(1000, range(token(0), token(Long.MAX_VALUE / 2))));
    }

    @Test
    public void wrapAroundRange()
    {
        // from 3/4 of the ring to 1/4 of the ring: half of the ring in total
        long left = Long.MAX_VALUE / 2;
        long right = Long.MIN_VALUE / 2;
        assertEquals(500, RangeUtil.estimateRowsInRange(1000, range(token(left), token(right))));
    }

    @Test
    public void rangeWithMinimumRightBoundReachesEndOfRing()
    {
        Token min = Murmur3Partitioner.instance.getMinimumToken();
        AbstractBounds<PartitionPosition> toTheEnd = new Range<>(token(0).minKeyBound(), min.minKeyBound());
        assertEquals(500, RangeUtil.estimateRowsInRange(1000, toTheEnd));
    }

    @Test
    public void emptyRange()
    {
        assertEquals(0, RangeUtil.estimateRowsInRange(0, range(token(0), token(100))));
    }

    /** The fields we need to set on the mocks are final, so they can't be stubbed. */
    private static void setField(Class<?> declaringClass, Object target, String name, Object value) throws ReflectiveOperationException
    {
        Field field = declaringClass.getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }

    /** An sstable with 1000 rows, spread over the tokens [minToken, maxToken]. */
    private static SSTableReader sstable(long minToken, long maxToken) throws ReflectiveOperationException
    {
        SSTableReader sstable = mock(SSTableReader.class);
        when(sstable.getTotalRows()).thenReturn(ROWS);
        setField(SSTableReader.class, sstable, "first", new BufferDecoratedKey(token(minToken), ByteBufferUtil.EMPTY_BYTE_BUFFER));
        setField(SSTableReader.class, sstable, "last", new BufferDecoratedKey(token(maxToken), ByteBufferUtil.EMPTY_BYTE_BUFFER));
        return sstable;
    }

    /** A segment with 1000 rows, spread over the tokens [minToken, maxToken]. */
    private static SegmentMetadata segment(long minToken, long maxToken) throws ReflectiveOperationException
    {
        PrimaryKey minKey = mock(PrimaryKey.class);
        when(minKey.token()).thenReturn(token(minToken));
        PrimaryKey maxKey = mock(PrimaryKey.class);
        when(maxKey.token()).thenReturn(token(maxToken));

        SegmentMetadata segment = mock(SegmentMetadata.class);
        setField(SegmentMetadata.class, segment, "minSSTableRowId", 100L);
        setField(SegmentMetadata.class, segment, "maxSSTableRowId", 100L + ROWS - 1);
        setField(SegmentMetadata.class, segment, "minKey", minKey);
        setField(SegmentMetadata.class, segment, "maxKey", maxKey);
        return segment;
    }

    @Test
    public void sstableFullRing() throws ReflectiveOperationException
    {
        Token min = Murmur3Partitioner.instance.getMinimumToken();
        assertEquals(1000, RangeUtil.estimateRowsInRange(sstable(0, Long.MAX_VALUE), range(min, min)));
        assertEquals(1000, RangeUtil.estimateRowsInRange(sstable(-1000, 1000), range(min, min)));
    }

    @Test
    public void keyRangeCoveringWholeSSTable() throws ReflectiveOperationException
    {
        assertEquals(1000, RangeUtil.estimateRowsInRange(sstable(0, Long.MAX_VALUE), range(-1000, Long.MAX_VALUE)));
        assertEquals(1000, RangeUtil.estimateRowsInRange(sstable(-1000, 1000), range(-2000, 2000)));
        assertEquals(1000, RangeUtil.estimateRowsInRange(sstable(Long.MIN_VALUE / 2, Long.MAX_VALUE / 2), range(Long.MIN_VALUE / 2, Long.MAX_VALUE / 2)));
    }

    @Test
    public void keyRangeInsideSSTable() throws ReflectiveOperationException
    {
        // a quarter of the ring is a half of the span of the sstable
        assertEquals(500, RangeUtil.estimateRowsInRange(sstable(0, Long.MAX_VALUE), range(0, Long.MAX_VALUE / 2)));
        assertEquals(500, RangeUtil.estimateRowsInRange(sstable(0, 1_000_000), range(250_000, 750_000)));
        assertEquals(500, RangeUtil.estimateRowsInRange(sstable(Long.MIN_VALUE / 2, Long.MAX_VALUE / 2), range(Long.MIN_VALUE / 4, Long.MAX_VALUE / 4)));
        assertEquals(100, RangeUtil.estimateRowsInRange(sstable(-1000, 1000), range(-100, 100)));
    }

    @Test
    public void keyRangePartiallyOverlappingSSTable() throws ReflectiveOperationException
    {
        // only the [MAX_VALUE / 2, MAX_VALUE] part of the range overlaps with the sstable
        assertEquals(500, RangeUtil.estimateRowsInRange(sstable(0, Long.MAX_VALUE), range(Long.MAX_VALUE / 2, Long.MAX_VALUE)));
        assertEquals(500, RangeUtil.estimateRowsInRange(sstable(0, 1_000_000), range(-500_000, 500_000)));
        assertEquals(500, RangeUtil.estimateRowsInRange(sstable(0, 1_000_000), range(500_000, 1_500_000)));
    }

    @Test
    public void keyRangeNotOverlappingSSTable() throws ReflectiveOperationException
    {
        assertEquals(0, RangeUtil.estimateRowsInRange(sstable(0, Long.MAX_VALUE), range(-1000, -1)));
        assertEquals(0, RangeUtil.estimateRowsInRange(sstable(0, 1_000_000), range(2_000_000, 3_000_000)));
        assertEquals(0, RangeUtil.estimateRowsInRange(sstable(Long.MIN_VALUE / 2, Long.MIN_VALUE / 4), range(0, Long.MAX_VALUE / 2)));
    }

    @Test
    public void keyRangeToEndOfRingOverlappingSSTable() throws ReflectiveOperationException
    {
        // a minimum right bound denotes the end of the ring
        Token min = Murmur3Partitioner.instance.getMinimumToken();
        assertEquals(500, RangeUtil.estimateRowsInRange(sstable(0, 1_000_000), new Range<>(token(500_000).minKeyBound(), min.minKeyBound())));
        assertEquals(1000, RangeUtil.estimateRowsInRange(sstable(0, 1_000_000), new Range<>(token(-1).minKeyBound(), min.minKeyBound())));
    }

    @Test
    public void sstableWrapAroundRange() throws ReflectiveOperationException
    {
        // the wrapped range is (MAX_VALUE / 2, MIN_VALUE] + (MIN_VALUE, -1]; only the first part overlaps the sstable
        assertEquals(500, RangeUtil.estimateRowsInRange(sstable(0, Long.MAX_VALUE), range(Long.MAX_VALUE / 2, -1)));
        // both parts of the wrapped range overlap the sstable: 250 rows on each side
        assertEquals(500, RangeUtil.estimateRowsInRange(sstable(-1000, 1000), range(500, -500)));
        // the sstable is inside the gap of the wrapped range
        assertEquals(0, RangeUtil.estimateRowsInRange(sstable(-1000, 1000), range(2000, -2000)));
    }

    @Test
    public void segmentFullRing() throws ReflectiveOperationException
    {
        Token min = Murmur3Partitioner.instance.getMinimumToken();
        assertEquals(1000, RangeUtil.estimateRowsInRange(segment(0, Long.MAX_VALUE), range(min, min)));
        assertEquals(1000, RangeUtil.estimateRowsInRange(segment(-1000, 1000), range(min, min)));
    }

    @Test
    public void keyRangeCoveringWholeSegment() throws ReflectiveOperationException
    {
        assertEquals(1000, RangeUtil.estimateRowsInRange(segment(0, Long.MAX_VALUE), range(-1000, Long.MAX_VALUE)));
        assertEquals(1000, RangeUtil.estimateRowsInRange(segment(-1000, 1000), range(-2000, 2000)));
        assertEquals(1000, RangeUtil.estimateRowsInRange(segment(Long.MIN_VALUE / 2, Long.MAX_VALUE / 2), range(Long.MIN_VALUE / 2, Long.MAX_VALUE / 2)));
    }

    @Test
    public void keyRangeInsideSegment() throws ReflectiveOperationException
    {
        assertEquals(500, RangeUtil.estimateRowsInRange(segment(0, Long.MAX_VALUE), range(0, Long.MAX_VALUE / 2)));
        assertEquals(500, RangeUtil.estimateRowsInRange(segment(0, 1_000_000), range(250_000, 750_000)));
        assertEquals(500, RangeUtil.estimateRowsInRange(segment(Long.MIN_VALUE / 2, Long.MAX_VALUE / 2), range(Long.MIN_VALUE / 4, Long.MAX_VALUE / 4)));
        assertEquals(100, RangeUtil.estimateRowsInRange(segment(-1000, 1000), range(-100, 100)));
    }

    @Test
    public void keyRangePartiallyOverlappingSegment() throws ReflectiveOperationException
    {
        assertEquals(500, RangeUtil.estimateRowsInRange(segment(0, Long.MAX_VALUE), range(Long.MAX_VALUE / 2, Long.MAX_VALUE)));
        assertEquals(500, RangeUtil.estimateRowsInRange(segment(0, 1_000_000), range(-500_000, 500_000)));
        assertEquals(500, RangeUtil.estimateRowsInRange(segment(0, 1_000_000), range(500_000, 1_500_000)));
    }

    @Test
    public void keyRangeNotOverlappingSegment() throws ReflectiveOperationException
    {
        assertEquals(0, RangeUtil.estimateRowsInRange(segment(0, Long.MAX_VALUE), range(-1000, -1)));
        assertEquals(0, RangeUtil.estimateRowsInRange(segment(0, 1_000_000), range(2_000_000, 3_000_000)));
        assertEquals(0, RangeUtil.estimateRowsInRange(segment(Long.MIN_VALUE / 2, Long.MIN_VALUE / 4), range(0, Long.MAX_VALUE / 2)));
    }

    @Test
    public void keyRangeToEndOfRingOverlappingSegment() throws ReflectiveOperationException
    {
        // a minimum right bound denotes the end of the ring
        Token min = Murmur3Partitioner.instance.getMinimumToken();
        assertEquals(500, RangeUtil.estimateRowsInRange(segment(0, 1_000_000), new Range<>(token(500_000).minKeyBound(), min.minKeyBound())));
        assertEquals(1000, RangeUtil.estimateRowsInRange(segment(0, 1_000_000), new Range<>(token(-1).minKeyBound(), min.minKeyBound())));
    }

    @Test
    public void segmentWrapAroundRange() throws ReflectiveOperationException
    {
        assertEquals(500, RangeUtil.estimateRowsInRange(segment(0, Long.MAX_VALUE), range(Long.MAX_VALUE / 2, -1)));
        assertEquals(500, RangeUtil.estimateRowsInRange(segment(-1000, 1000), range(500, -500)));
        assertEquals(0, RangeUtil.estimateRowsInRange(segment(-1000, 1000), range(2000, -2000)));
    }
}
