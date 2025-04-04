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
package org.apache.cassandra.serializers;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.apache.cassandra.db.marshal.ValueAccessor;
import org.apache.cassandra.utils.ByteBufferUtil;

import static java.time.ZoneOffset.UTC;
import static java.time.format.ResolverStyle.STRICT;

// For byte-order comparability, we shift by Integer.MIN_VALUE and treat the data as an unsigned integer ranging from
// min date to max date w/epoch sitting in the center @ 2^31
public class SimpleDateSerializer extends TypeSerializer<Integer>
{
    private static final DateTimeFormatter formatter =
            DateTimeFormatter.ISO_LOCAL_DATE.withZone(UTC).withResolverStyle(STRICT);
    private static final long minSupportedDateMillis = TimeUnit.DAYS.toMillis(Integer.MIN_VALUE);
    private static final long maxSupportedDateMillis = TimeUnit.DAYS.toMillis(Integer.MAX_VALUE);
    private static final long maxSupportedDays = (long)Math.pow(2,32) - 1;
    private static final long byteOrderShift = (long)Math.pow(2,31) * 2;

    private static final Pattern rawPattern = Pattern.compile("^-?\\d+$");
    public static final SimpleDateSerializer instance = new SimpleDateSerializer();

    public <V> Integer deserialize(V value, ValueAccessor<V> accessor)
    {
        return accessor.isEmpty(value) ? null : accessor.toInt(value);
    }

    public ByteBuffer serialize(Integer value)
    {
        return value == null ? ByteBufferUtil.EMPTY_BYTE_BUFFER : ByteBufferUtil.bytes(value);
    }

    public static int dateStringToDays(String source) throws MarshalException
    {
        // Raw day value in unsigned int form, epoch @ 2^31
        if (rawPattern.matcher(source).matches())
        {
            return parseRaw(source);
        }

        // Attempt to parse as date string
        try
        {
            LocalDate parsed = formatter.parse(source, LocalDate::from);
            long millis = parsed.atStartOfDay(UTC).toInstant().toEpochMilli();
            return timeInMillisToDay(source, millis);
        }
        catch (DateTimeParseException| ArithmeticException e1)
        {
            throw new MarshalException(String.format("Unable to coerce '%s' to a formatted date (long)", source), e1);
        }
    }

    private static int parseRaw(String source) {
        try
        {
            long result = Long.parseLong(source);

            if (result < 0 || result > maxSupportedDays)
                throw new NumberFormatException("Input out of bounds: " + source);

            // Shift > epoch days into negative portion of Integer result for byte order comparability
            if (result >= Integer.MAX_VALUE)
                result -= byteOrderShift;

            return (int) result;
        }
        catch (NumberFormatException | DateTimeParseException e)
        {
            throw new MarshalException(String.format("Unable to make unsigned int (for date) from: '%s'", source), e);
        }
    }

    public static int timeInMillisToDay(long millis)
    {
        return timeInMillisToDay(null, millis);
    }

    private static int timeInMillisToDay(String source, long millis)
    {
        if (millis < minSupportedDateMillis)
        {
            throw new MarshalException(String.format("Input date %s is less than min supported date %s",
                                                     null == source ? ZonedDateTime.ofInstant(Instant.ofEpochMilli(millis), UTC).toLocalDate() : source,
                                                     ZonedDateTime.ofInstant(Instant.ofEpochMilli(minSupportedDateMillis), UTC).toLocalDate()));
        }
        if (millis > maxSupportedDateMillis)
        {
            throw new MarshalException(String.format("Input date %s is greater than max supported date %s",
                                                     null == source ? ZonedDateTime.ofInstant(Instant.ofEpochMilli(millis), UTC).toLocalDate() : source,
                                                     ZonedDateTime.ofInstant(Instant.ofEpochMilli(maxSupportedDateMillis), UTC).toLocalDate()));
        }
        return (int) (Duration.ofMillis(millis).toDays() - Integer.MIN_VALUE);
    }

    public static long dayToTimeInMillis(int days)
    {
        return Duration.ofDays(days + Integer.MIN_VALUE).toMillis();
    }

    public <V> void validate(V value, ValueAccessor<V> accessor) throws MarshalException
    {
        if (accessor.size(value) != 4)
            throw new MarshalException(String.format("Expected 4 byte long for date (%d)", accessor.size(value)));
    }

    public String toString(Integer value)
    {
        if (value == null)
            return "";

        return Instant.ofEpochMilli(dayToTimeInMillis(value)).atZone(UTC).format(formatter);
    }

    public Class<Integer> getType()
    {
        return Integer.class;
    }
}
