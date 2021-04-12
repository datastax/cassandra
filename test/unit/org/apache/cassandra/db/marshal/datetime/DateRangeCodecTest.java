/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package org.apache.cassandra.db.marshal.datetime;

import java.nio.ByteBuffer;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.datastax.bdp.test.categories.UnitTest;
import com.datastax.driver.core.ProtocolVersion;
import org.apache.cassandra.db.marshal.datetime.DateRange.DateRangeBound.Precision;

import static org.junit.Assert.assertEquals;

@Category(UnitTest.class)
public class DateRangeCodecTest
{
    private final DateRangeCodec codec = DateRangeCodec.instance;

    @Test
    public void testSerializeRoundTrip()
    {
        DateRange expected = DateRange.DateRangeBuilder.dateRange()
                .withLowerBound("2015-12-03T10:15:30.00Z", Precision.SECOND)
                .withUpperBound("2016-01-01T00:00:01.00Z", Precision.MILLISECOND)
                .build();

        ByteBuffer serialized = codec.serialize(expected, ProtocolVersion.NEWEST_SUPPORTED);

        // For UDT or tuple type buffer contains whole cell payload, and codec can't rely on absolute byte addressing
        ByteBuffer payload = ByteBuffer.allocate(5 + serialized.capacity());
        // put serialized date range in between other data
        payload.putInt(44).put(serialized).put((byte) 1);
        payload.position(4);

        DateRange actual = codec.deserialize(payload, ProtocolVersion.NEWEST_SUPPORTED);

        assertEquals(expected, actual);
        //provided ByteBuffer should never be consumed by read operations that modify its current position
        assertEquals(4, payload.position());
    }
}
