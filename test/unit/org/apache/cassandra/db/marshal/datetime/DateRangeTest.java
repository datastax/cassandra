/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package org.apache.cassandra.db.marshal.datetime;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.datastax.bdp.test.categories.UnitTest;
import org.apache.cassandra.db.marshal.datetime.DateRange.DateRangeBound;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

@Category(UnitTest.class)
public class DateRangeTest
{
    @Test
    public void testDateRangeEquality()
    {
        DateRange first = DateRange.DateRangeBuilder.dateRange()
                .withLowerBound("2015-12-03T10:15:30.00Z", DateRangeBound.Precision.SECOND)
                .withUpperBound("2016-01-01T00:00:01.00Z", DateRangeBound.Precision.MILLISECOND)
                .build();
        DateRange second = DateRange.DateRangeBuilder.dateRange()
                // millis are off, but precision is higher so we skip them
                .withLowerBound("2015-12-03T10:15:30.01Z", DateRangeBound.Precision.SECOND)
                .withUpperBound("2016-01-01T00:00:01.00Z", DateRangeBound.Precision.MILLISECOND)
                .build();
        DateRange third = DateRange.DateRangeBuilder.dateRange()
                .withLowerBound("2015-12-03T10:15:30.00Z", DateRangeBound.Precision.MILLISECOND)
                .withUpperBound("2016-01-01T00:00:01.00Z", DateRangeBound.Precision.MILLISECOND)
                .build();

        assertEquals(first, second);
        assertEquals(first, first);
        assertEquals(first.hashCode(), second.hashCode());
        assertEquals(first.hashCode(), first.hashCode());
        assertEquals(first.formatToSolrString(), second.formatToSolrString());
        assertNotEquals(first, third);
        assertNotEquals(first.hashCode(), third.hashCode());
    }
}
