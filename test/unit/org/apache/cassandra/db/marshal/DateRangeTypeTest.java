/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package org.apache.cassandra.db.marshal;

import java.nio.ByteBuffer;
import java.text.ParseException;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import com.datastax.bdp.test.categories.UnitTest;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.apache.cassandra.db.marshal.datetime.DateRange;
import org.apache.cassandra.db.marshal.datetime.DateRange.DateRangeBound.Precision;
import org.apache.cassandra.transport.ProtocolVersion;

import static org.apache.cassandra.db.marshal.datetime.DateRange.DateRangeBuilder.dateRange;
import static org.junit.Assert.assertEquals;

@RunWith(JUnitParamsRunner.class)
@Category(UnitTest.class)
public class DateRangeTypeTest
{
    private final DateRangeType dateRangeType = DateRangeType.instance;

    @Test
    @Parameters(method = "testData")
    public void shouldFormatDateRangeAsJson(DateRange source, String expected) throws ParseException
    {
        ByteBuffer bytes = dateRangeType.decompose(source);
        String actualJson = dateRangeType.toJSONString(bytes, ProtocolVersion.CURRENT);
        assertEquals('"' + expected + '"', actualJson);
    }

    @Test
    @Parameters(method = "testData")
    public void shouldCreateProperDateRangeFromString(DateRange expected, String source)
    {
        ByteBuffer dateRangeBytes = dateRangeType.fromString(source);
        DateRange actual = dateRangeType.getSerializer().deserialize(dateRangeBytes);
        assertEquals(expected, actual);
    }

    @SuppressWarnings("unused")
    private Object[] testData()
    {
        return new Object[]{
                new Object[]{
                        dateRange()
                                .withLowerBound("1950-01-01T00:00:00.000Z", Precision.YEAR)
                                .withUnboundedUpperBound()
                                .build(),
                        "[1950 TO *]"
                },
                new Object[]{
                        dateRange()
                                .withLowerBound("1998-01-01T00:00:00.000Z", Precision.MILLISECOND)
                                .withUpperBound("1999-02-01T00:00:00.000Z", Precision.DAY)
                                .build(),
                        "[1998-01-01T00:00:00.000Z TO 1999-02-01]"
                },
                new Object[]{
                        dateRange()
                                .withLowerBound("1930-12-03T01:01:01.003Z", Precision.DAY)
                                .withUpperBound("1951-01-02T00:00:00.003Z", Precision.MILLISECOND)
                                .build(),
                        "[1930-12-03 TO 1951-01-02T00:00:00.003Z]"
                },
                new Object[]{
                        dateRange()
                                .withUnboundedLowerBound()
                                .withUpperBound("2014-01-02T00:00:00.003Z", Precision.YEAR)
                                .build(),
                        "[* TO 2014]"
                },
                new Object[]{
                        dateRange()
                                .withUnboundedLowerBound()
                                .withUnboundedUpperBound()
                                .build(),
                        "[* TO *]"
                },
                new Object[]{
                        dateRange()
                                .withLowerBound("1966-03-03T03:30:30.030Z", Precision.YEAR)
                                .build(),
                        "1966"
                },
                new Object[]{
                        dateRange()
                                .withLowerBound("1700-01-01T00:00:00.000Z", Precision.MILLISECOND)
                                .build(),
                        "1700-01-01T00:00:00.000Z"
                },
                new Object[]{
                        dateRange()
                                .withLowerBound("-0009-01-01T00:00:00.000Z", Precision.YEAR)
                                .build(),
                        "-0009"
                },
        };
    }
}
