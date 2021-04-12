/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package org.apache.cassandra.serializers;

import java.nio.ByteBuffer;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import com.datastax.bdp.test.categories.UnitTest;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.apache.cassandra.db.marshal.datetime.DateRange;
import org.apache.cassandra.db.marshal.datetime.DateRange.DateRangeBound.Precision;

import static org.apache.cassandra.db.marshal.datetime.DateRange.DateRangeBuilder.dateRange;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(JUnitParamsRunner.class)
@Category(UnitTest.class)
public class DateRangeSerializerTest
{
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    @Parameters(method = "dateRanges")
    public void testSerializeRoundTrip(DateRange source)
    {
        ByteBuffer serialized = DateRangeSerializer.instance.serialize(source);

        // For UDT or tuple type buffer contains whole cell payload, and codec can't rely on absolute byte addressing
        ByteBuffer payload = ByteBuffer.allocate(5 + serialized.capacity());
        // put serialized date range in between other data
        payload.putInt(44).put(serialized).put((byte) 1);
        payload.position(4);

        DateRange actual = DateRangeSerializer.instance.deserialize(payload);

        assertEquals(source, actual);
        //provided ByteBuffer should never be consumed by read operations that modify its current position
        assertEquals(4, payload.position());
    }

    @Test
    public void testNullValueSerializeRoundTrip()
    {
        ByteBuffer serialized = DateRangeSerializer.instance.serialize(null);
        assertEquals(0, serialized.capacity());
        assertNull(DateRangeSerializer.instance.deserialize(serialized));
    }

    @Test
    public void testDeserializeInvalidLengthInput()
    {
        expectedException.expect(AssertionError.class);
        DateRangeSerializer.instance.deserialize(ByteBuffer.allocate(5));
    }

    @Test
    public void testDeserializeUnsupportedHeader()
    {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Unknown date range type");
        DateRangeSerializer.instance.deserialize(ByteBuffer.allocate(1).put(0, (byte) 0x15));
    }

    @SuppressWarnings("unused")
    private Object[] dateRanges()
    {
        return new Object[]{
                new Object[]{
                        // 2015-12-03T10:15:30 TO 2016-01-01T00:00:01.001Z
                        dateRange()
                                .withLowerBound("2015-12-03T10:15:30.000Z", Precision.SECOND)
                                .withUpperBound("2016-01-01T00:00:01.001Z", Precision.MILLISECOND)
                                .build()
                },
                new Object[]{
                        // 1998-01-01 TO *
                        dateRange()
                                .withLowerBound("1998-01-01T00:00:00.000Z", Precision.DAY)
                                .withUnboundedUpperBound()
                                .build()
                },
                new Object[]{
                        // * TO 1951-01-02T01
                        dateRange()
                                .withUnboundedLowerBound()
                                .withUpperBound("1951-01-02T01:00:00.003Z", Precision.HOUR)
                                .build()
                },
                new Object[]{
                        // *
                        dateRange()
                                .withUnboundedLowerBound()
                                .build()
                },
                new Object[]{
                        // [* TO *]
                        dateRange()
                                .withUnboundedLowerBound()
                                .withUnboundedUpperBound()
                                .build()
                },
                new Object[]{
                        // 1966
                        dateRange()
                                .withLowerBound("1966-03-03T03:30:30.030Z", Precision.YEAR)
                                .build(),
                }
        };
    }
}
