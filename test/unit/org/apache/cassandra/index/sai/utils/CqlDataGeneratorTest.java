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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;

import org.junit.Test;

import org.apache.cassandra.cql3.CQL3Type;
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
import org.checkerframework.checker.nullness.qual.NonNull;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class CqlDataGeneratorTest
{
    private final CqlDataGenerator.Factory factory = new CqlDataGenerator.Factory();

    @Test
    public void testAscii()
    {
        testGenerator(factory.ascii(0, 20), AsciiType.instance);
    }

    @Test
    public void testUtf8()
    {
        testGenerator(factory.utf8(0, 20), UTF8Type.instance);
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
        testGenerator(factory.blob(0, 64), BytesType.instance);
    }

    @Test
    public void testSmallint()
    {
        short min = Short.MIN_VALUE;
        short max = Short.MAX_VALUE;
        testGenerator(factory.smallint(min, max), ShortType.instance, min, max);
    }

    @Test
    public void testInt()
    {
        int min = 0;
        int max = 100;
        testGenerator(factory.cint(min, max), Int32Type.instance, min, max);
    }

    @Test
    public void testBigint()
    {
        long min = 0L;
        long max = 100L;
        testGenerator(factory.bigint(min, max), LongType.instance, min, max);
    }

    @Test
    public void testVarint()
    {
        BigInteger min1 = BigInteger.valueOf(Long.MIN_VALUE);
        BigInteger max1 = BigInteger.valueOf(Long.MAX_VALUE);
        testGenerator(factory.varint(min1, max1), IntegerType.instance, min1, max1);

        BigInteger min2 = BigInteger.valueOf(-7);
        BigInteger max2 = BigInteger.valueOf(7);
        testGenerator(factory.varint(min2, max2), IntegerType.instance, min2, max2);
    }

    @Test
    public void testFloat()
    {
        float min = -1000.0f;
        float max = 1000.0f;
        testGenerator(factory.cfloat(min, max), FloatType.instance, min, max);
    }

    @Test
    public void testDouble()
    {
        double min = -1000.0;
        double max = 1000.0;
        testGenerator(factory.cdouble(min, max), DoubleType.instance, min, max);
    }

    @Test
    public void testDecimal()
    {
        BigDecimal min = BigDecimal.valueOf(-1000);
        BigDecimal max = BigDecimal.valueOf(1000);
        testGenerator(factory.decimal(min, max), DecimalType.instance, min, max);
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
        long min = 0;
        long max = 1000;
        testGenerator(factory.time(min, max), TimeType.instance, min, max);
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
            String rawValue = generateUnquotedCqlValue(generator);
            type.fromString(rawValue);
        }
    }

    /**
     * Test generator with bounds validation using Comparable types.
     * Uses the Cassandra type to parse the CQL string into a ByteBuffer and then compose it into a Java object.
     *
     * @param generator the CQL data generator
     * @param type the Cassandra type
     * @param min the minimum expected value (inclusive)
     * @param max the maximum expected value (inclusive)
     */
    private <T extends Comparable<T>> void testGenerator(CqlDataGenerator generator,
                                                          AbstractType<T> type,
                                                          T min,
                                                          T max)
    {
        for (int i = 0; i < 10000; i++)
        {
            String rawValue = generateUnquotedCqlValue(generator);
            ByteBuffer valueBytes = type.fromString(rawValue);
            T value = type.compose(valueBytes);
            assertThat(value).isBetween(min, max);
        }
    }

    private static @NonNull String generateUnquotedCqlValue(CqlDataGenerator generator)
    {
        String maybeQuotedValue = generator.generateCqlValue();
        return (maybeQuotedValue.startsWith("'") && maybeQuotedValue.endsWith("'"))
               ? maybeQuotedValue.substring(1, maybeQuotedValue.length() - 1)
               : maybeQuotedValue;
    }

    @Test
    public void testExhaustiveness()
    {
        for (CQL3Type.Native type : CQL3Type.Native.values())
        {
            String name = type.name();
            if (type == CQL3Type.Native.COUNTER || type == CQL3Type.Native.EMPTY)
            {
                assertThatThrownBy(() -> factory.forType(name))
                .hasMessageContaining("Unsupported type: " + name);
            }
            else
            {
                assertThat(factory.forType(name)).isNotNull();
            }
        }
    }
}
