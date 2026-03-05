/*
 * Copyright DataStax, Inc.
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

import java.math.BigDecimal;
import java.math.BigInteger;

import org.junit.Test;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.ByteType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.DurationType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.InetAddressType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.ShortType;
import org.apache.cassandra.db.marshal.SimpleDateType;
import org.apache.cassandra.db.marshal.TimeType;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

public class CqlDataGeneratorTest
{
    private final CqlDataGenerator.Factory factory = new CqlDataGenerator.Factory();

    @Test
    public void testAscii()
    {
        testGenerator(factory.ascii(5, 20), AsciiType.instance);
    }

    @Test
    public void testUtf8()
    {
        testGenerator(factory.utf8(5, 20), UTF8Type.instance);
    }

    @Test
    public void testBoolean()
    {
        testGenerator(factory.bool(), BooleanType.instance);
    }

    @Test
    public void testTinyint()
    {
        testGenerator(factory.tinyint(), ByteType.instance);
    }

    @Test
    public void testBlob()
    {
        testGenerator(factory.blob(16, 64), BytesType.instance);
    }

    @Test
    public void testSmallint()
    {
        testGenerator(factory.smallint(Short.MIN_VALUE, Short.MAX_VALUE), ShortType.instance);
    }

    @Test
    public void testInt()
    {
        testGenerator(factory.cint(0, 100), Int32Type.instance);
    }

    @Test
    public void testBigint()
    {
        testGenerator(factory.bigint(0L, 100L), LongType.instance);
    }

    @Test
    public void testVarint()
    {
        testGenerator(factory.varint(BigInteger.valueOf(Long.MIN_VALUE), BigInteger.valueOf(Long.MAX_VALUE)), IntegerType.instance);
    }

    @Test
    public void testFloat()
    {
        testGenerator(factory.cfloat(-1000.0f, 1000.0f), FloatType.instance);
    }

    @Test
    public void testDouble()
    {
        testGenerator(factory.cdouble(-1000.0, 1000.0), DoubleType.instance);
    }

    @Test
    public void testDecimal()
    {
        testGenerator(factory.decimal(BigDecimal.valueOf(-1000), BigDecimal.valueOf(1000)), DecimalType.instance);
    }

    @Test
    public void testDecimal2()
    {
        var generator = factory.decimal(BigDecimal.valueOf(-1000), BigDecimal.valueOf(1000));
        for (int i = 0; i < 10000; i++)
        {
            String cqlValue = generator.generateCqlValue();
            BigDecimal value = new BigDecimal(cqlValue);
            assertThat(value).isBetween(BigDecimal.valueOf(-1000), BigDecimal.valueOf(1000));
        }
    }

    @Test
    public void testInet()
    {
        testGenerator(factory.inet(), InetAddressType.instance);
    }

    @Test
    public void testUuid()
    {
        testGenerator(factory.uuid(), UUIDType.instance);
    }

    @Test
    public void testTimeuuid()
    {
        testGenerator(factory.timeuuid(), TimeUUIDType.instance);
    }

    @Test
    public void testTimestamp()
    {
        testGenerator(factory.timestamp(), TimestampType.instance);
    }

    @Test
    public void testTime()
    {
        testGenerator(factory.time(0, 1000), TimeType.instance);
    }

    @Test
    public void testDate()
    {
        testGenerator(factory.date(), SimpleDateType.instance);
    }

    @Test
    public void testDuration()
    {
        testGenerator(factory.duration(0, 1000), DurationType.instance);
    }

    private <T> void testGenerator(CqlDataGenerator generator, AbstractType<T> type)
    {
        for (int i = 0; i < 10000; i++)
        {
            String maybeQuotedValue = generator.generateCqlValue();
            String rawValue = (maybeQuotedValue.startsWith("'") && maybeQuotedValue.endsWith("'"))
                              ? maybeQuotedValue.substring(1, maybeQuotedValue.length() - 1)
                              : maybeQuotedValue;
            assertThatCode(() -> type.getSerializer().validate(type.fromString(rawValue)))
                .doesNotThrowAnyException();
        }
    }
}
