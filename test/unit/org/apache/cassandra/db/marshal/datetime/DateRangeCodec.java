/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package org.apache.cassandra.db.marshal.datetime;

import java.nio.ByteBuffer;
import java.text.ParseException;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;
import org.apache.cassandra.db.marshal.DateRangeType;
import org.apache.cassandra.serializers.DateRangeSerializer;

/**
 * {@link TypeCodec} that maps binary representation of {@link DateRangeType} to {@link DateRange}.
 */
public class DateRangeCodec extends TypeCodec<DateRange>
{
    public static final DateRangeCodec instance = new DateRangeCodec();

    private DateRangeCodec()
    {
        super(DataType.custom(DateRangeType.class.getName()), DateRange.class);
    }

    @Override
    public ByteBuffer serialize(DateRange dateRange, ProtocolVersion protocolVersion) throws InvalidTypeException
    {
        if (dateRange == null)
        {
            return null;
        }
        return DateRangeSerializer.instance.serialize(dateRange);
    }

    @Override
    public DateRange deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) throws InvalidTypeException
    {
        if (bytes == null || bytes.remaining() == 0)
        {
            return null;
        }
        return DateRangeSerializer.instance.deserialize(bytes);
    }

    @Override
    public DateRange parse(String value) throws InvalidTypeException
    {
        if (value == null || value.isEmpty() || value.equalsIgnoreCase("NULL"))
        {
            return null;
        }
        try
        {
            return DateRangeUtil.parseDateRange(value);
        }
        catch (ParseException e)
        {
            throw new IllegalArgumentException(String.format("Invalid date range literal: '%s'", value), e);
        }
    }

    @Override
    public String format(DateRange dateRange) throws InvalidTypeException
    {
        return dateRange == null ? "NULL" : dateRange.formatToSolrString();
    }
}
