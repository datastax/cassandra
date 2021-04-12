/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package org.apache.cassandra.db.marshal.datetime;

import java.text.ParseException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import com.datastax.bdp.test.categories.UnitTest;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.apache.cassandra.db.marshal.datetime.DateRange.DateRangeBound.Precision;
import org.apache.cassandra.serializers.DateRangeSerializer;

import static org.apache.cassandra.db.marshal.datetime.DateRange.DateRangeBuilder.dateRange;
import static org.junit.Assert.assertEquals;

@RunWith(JUnitParamsRunner.class)
@Category(UnitTest.class)
public class DateRangeUtilTest
{
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    @Parameters(method = "testData")
    public void shouldParseAndFormatSolrDateRangeFormat(String source, DateRange expectedParsedSource) throws ParseException
    {
        DateRange parsedSource = DateRangeUtil.parseDateRange(source);

        assertEquals(expectedParsedSource, parsedSource);
        assertEquals(source, parsedSource.formatToSolrString());
    }

    @Test
    @Parameters(method = "testData")
    public void shouldSerializeAndDeserializeDateRange(@SuppressWarnings("unused") String source, DateRange dateRange)
    {
        DateRange parsed = DateRangeSerializer.instance.deserialize(DateRangeSerializer.instance.serialize(dateRange));
        assertEquals(dateRange, parsed);
    }

    @Test
    public void shouldNotParseDateRangeWithWrongDateOrder() throws ParseException
    {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Wrong order: 2010 TO 2009");
        DateRangeUtil.parseDateRange("[2010 TO 2009]");
    }

    @Test
    public void shouldRoundUpperBoundToTheGivenPrecision()
    {
        ZonedDateTime timestamp = ZonedDateTime.ofInstant(Instant.parse("2011-02-03T04:05:16.789Z"), ZoneOffset.UTC);
        assertEquals("2011-02-03T04:05:16.789Z", DateRangeUtil.roundUpperBoundTimestampToPrecision(timestamp, Precision.MILLISECOND).toInstant().toString());
        assertEquals("2011-02-03T04:05:16.999Z", DateRangeUtil.roundUpperBoundTimestampToPrecision(timestamp, Precision.SECOND).toInstant().toString());
        assertEquals("2011-02-03T04:05:59.999Z", DateRangeUtil.roundUpperBoundTimestampToPrecision(timestamp, Precision.MINUTE).toInstant().toString());
        assertEquals("2011-02-03T04:59:59.999Z", DateRangeUtil.roundUpperBoundTimestampToPrecision(timestamp, Precision.HOUR).toInstant().toString());
        assertEquals("2011-02-03T23:59:59.999Z", DateRangeUtil.roundUpperBoundTimestampToPrecision(timestamp, Precision.DAY).toInstant().toString());
        assertEquals("2011-02-28T23:59:59.999Z", DateRangeUtil.roundUpperBoundTimestampToPrecision(timestamp, Precision.MONTH).toInstant().toString());
        assertEquals("2011-12-31T23:59:59.999Z", DateRangeUtil.roundUpperBoundTimestampToPrecision(timestamp, Precision.YEAR).toInstant().toString());
    }

    @Test
    public void shouldRoundLowerBoundToTheGivenPrecision()
    {
        ZonedDateTime timestamp = ZonedDateTime.ofInstant(Instant.parse("2011-02-03T04:05:16.789Z"), ZoneOffset.UTC);
        assertEquals("2011-02-03T04:05:16.789Z", DateRangeUtil.roundLowerBoundTimestampToPrecision(timestamp, Precision.MILLISECOND).toInstant().toString());
        assertEquals("2011-02-03T04:05:16Z", DateRangeUtil.roundLowerBoundTimestampToPrecision(timestamp, Precision.SECOND).toInstant().toString());
        assertEquals("2011-02-03T04:05:00Z", DateRangeUtil.roundLowerBoundTimestampToPrecision(timestamp, Precision.MINUTE).toInstant().toString());
        assertEquals("2011-02-03T04:00:00Z", DateRangeUtil.roundLowerBoundTimestampToPrecision(timestamp, Precision.HOUR).toInstant().toString());
        assertEquals("2011-02-03T00:00:00Z", DateRangeUtil.roundLowerBoundTimestampToPrecision(timestamp, Precision.DAY).toInstant().toString());
        assertEquals("2011-02-01T00:00:00Z", DateRangeUtil.roundLowerBoundTimestampToPrecision(timestamp, Precision.MONTH).toInstant().toString());
        assertEquals("2011-01-01T00:00:00Z", DateRangeUtil.roundLowerBoundTimestampToPrecision(timestamp, Precision.YEAR).toInstant().toString());
    }

    @SuppressWarnings("unused")
    private Object[] testData()
    {
        return new Object[]{
                new Object[]{
                        "[2011-01 TO 2015]",
                        dateRange()
                                .withLowerBound("2011-01-01T00:00:00.000Z", Precision.MONTH)
                                .withUpperBound("2015-12-31T23:59:59.999Z", Precision.YEAR)
                                .build()
                },
                new Object[]{
                        "[2010-01-02 TO 2015-05-05T13]",
                        dateRange()
                                .withLowerBound("2010-01-02T00:00:00.000Z", Precision.DAY)
                                .withUpperBound("2015-05-05T13:59:59.999Z", Precision.HOUR)
                                .build()
                },
                new Object[]{
                        "[1973-06-30T13:57:28.123Z TO 1999-05-05T14:14:59]",
                        dateRange()
                                .withLowerBound("1973-06-30T13:57:28.123Z", Precision.MILLISECOND)
                                .withUpperBound("1999-05-05T14:14:59.999Z", Precision.SECOND)
                                .build()
                },
                // leap year
                new Object[]{
                        "[2010-01-01T15 TO 2016-02]",
                        dateRange()
                                .withLowerBound("2010-01-01T15:00:00.000Z", Precision.HOUR)
                                .withUpperBound("2016-02-29T23:59:59.999Z", Precision.MONTH)
                                .build()
                },
                // pre-epoch
                new Object[]{
                        "[1500 TO 1501]",
                        dateRange()
                                .withLowerBound("1500-01-01T00:00:00.000Z", Precision.YEAR)
                                .withUpperBound("1501-12-31T23:59:59.999Z", Precision.YEAR)
                                .build()
                },
                // AD/BC era boundary
                new Object[]{
                        "[0001-01-01 TO 0001-01-01]",
                        dateRange()
                                .withLowerBound("0001-01-01T00:00:00.000Z", Precision.DAY)
                                .withUpperBound("0001-01-01T00:00:00.000Z", Precision.DAY)
                                .build()
                },
                new Object[]{
                        "[0001-01-01 TO 0001-01-02]",
                        dateRange()
                                .withLowerBound("0001-01-01T00:00:00.000Z", Precision.DAY)
                                .withUpperBound("0001-01-02T23:59:59.999Z", Precision.DAY)
                                .build()
                },
                new Object[]{
                        "[0000-01-01 TO 0000-01-01]",
                        dateRange()
                                .withLowerBound("0000-01-01T00:00:00.000Z", Precision.DAY)
                                .withUpperBound("0000-01-01T00:00:00.000Z", Precision.DAY)
                                .build()
                },
                new Object[]{
                        "[0000-01-01 TO 0000-01-02]",
                        dateRange()
                                .withLowerBound("0000-01-01T00:00:00.000Z", Precision.DAY)
                                .withUpperBound("0000-01-02T23:59:59.999Z", Precision.DAY)
                                .build()
                },
                new Object[]{
                        "[-0001-01-01 TO -0001-01-01]",
                        dateRange()
                                .withLowerBound("-0001-01-01T00:00:00.000Z", Precision.DAY)
                                .withUpperBound("-0001-01-01T00:00:00.000Z", Precision.DAY)
                                .build()
                },
                new Object[]{
                        "[-0001-01-01 TO -0001-01-02]",
                        dateRange()
                                .withLowerBound("-0001-01-01T00:00:00.000Z", Precision.DAY)
                                .withUpperBound("-0001-01-02T23:59:59.999Z", Precision.DAY)
                                .build()
                },
                // unbounded
                new Object[]{
                        "[* TO 2014-12-01]",
                        dateRange()
                                .withUnboundedLowerBound()
                                .withUpperBound("2014-12-01T23:59:59.999Z", Precision.DAY)
                                .build()
                },
                new Object[]{
                        "[1999 TO *]",
                        dateRange()
                                .withLowerBound("1999-01-01T00:00:00Z", Precision.YEAR)
                                .withUnboundedUpperBound()
                                .build()
                },
                new Object[]{
                        "[* TO *]",
                        dateRange()
                                .withUnboundedLowerBound()
                                .withUnboundedUpperBound()
                                .build()
                },
                new Object[]{
                        "*",
                        dateRange()
                                .withUnboundedLowerBound()
                                .build()
                },
                // unit shapes
                new Object[]{
                        "-0009",
                        dateRange()
                                .withLowerBound("-0009-01-01T00:00:00.000Z", Precision.YEAR)
                                .build()
                },
                new Object[]{
                        "2000-11",
                        dateRange()
                                .withLowerBound("2000-11-01T00:00:00.000Z", Precision.MONTH)
                                .build()
                }
        };
    }
}
