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
import java.net.InetAddress;
import java.util.Arrays;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.cassandra.cql3.Duration;
import org.apache.cassandra.utils.Hex;
import org.apache.cassandra.utils.RandomPlus;
import org.apache.cassandra.utils.TimeUUID;

public interface CqlDataGenerator
{
    /**
     * Generates a random value and returns it as a CQL literal string (e.g. 'abc', 123, 0xdeadbeef)
     */
    String generateCqlValue();

    /**
     * Factory for creating {@link CqlDataGenerator}s for different CQL types
     */
    class Factory
    {
        private final RandomPlus random;

        public Factory()
        {
            this.random = new RandomPlus();
        }

        public Factory(RandomPlus random)
        {
            this.random = random;
        }

        public CqlDataGenerator ascii(int minLength, int maxLength)
        {
            return new AsciiGenerator(random, minLength, maxLength);
        }

        public CqlDataGenerator utf8(int minLength, int maxLength)
        {
            return new UTF8Generator(random, minLength, maxLength);
        }

        public CqlDataGenerator bool()
        {
            return new BooleanGenerator(random);
        }

        public CqlDataGenerator tinyint()
        {
            return new ByteGenerator(random);
        }

        public CqlDataGenerator blob(int minLength, int maxLength)
        {
            return new BytesGenerator(random, minLength, maxLength);
        }

        public CqlDataGenerator smallint(short min, short max)
        {
            return new ShortGenerator(random, min, max);
        }

        public CqlDataGenerator cint(int min, int max)
        {
            return new IntGenerator(random, min, max);
        }

        public CqlDataGenerator bigint(long min, long max)
        {
            return new LongGenerator(random, min, max);
        }

        public CqlDataGenerator varint(BigInteger min, BigInteger max)
        {
            return new BigIntegerGenerator(random, min, max);
        }

        public CqlDataGenerator cfloat(float min, float max)
        {
            return new FloatGenerator(random, min, max);
        }

        public CqlDataGenerator cdouble(double min, double max)
        {
            return new DoubleGenerator(random, min, max);
        }

        public CqlDataGenerator decimal(BigDecimal min, BigDecimal max)
        {
            return new DecimalGenerator(random, min, max);
        }

        public CqlDataGenerator inet()
        {
            return new InetAddressGenerator(random);
        }

        public CqlDataGenerator uuid()
        {
            return new UUIDGenerator(random);
        }

        public CqlDataGenerator timeuuid()
        {
            return new TimeUUIDGenerator(random);
        }

        public CqlDataGenerator timestamp()
        {
            return new TimestampGenerator(random);
        }

        public CqlDataGenerator time(long min, long max)
        {
            return new TimeGenerator(random, min, max);
        }

        public CqlDataGenerator date()
        {
            return new DateGenerator(random);
        }

        public CqlDataGenerator duration(long minSeconds, long maxSeconds)
        {
            return new DurationGenerator(random, minSeconds, maxSeconds);
        }

        public CqlDataGenerator forType(String cqlTypeName)
        {
            // The type can come with a modifier like `static` e.g. `int static`, so drop any modifiers after space.
            String baseType = cqlTypeName.split(" ")[0];
            switch (baseType.toLowerCase())
            {
                case "ascii":
                    return ascii(5, 20);
                case "text":
                case "varchar":
                    return utf8(5, 20);
                case "boolean":
                    return bool();
                case "tinyint":
                    return tinyint();
                case "blob":
                    return blob(16, 64);
                case "smallint":
                    return smallint(Short.MIN_VALUE, Short.MAX_VALUE);
                case "int":
                    return cint(Integer.MIN_VALUE, Integer.MAX_VALUE);
                case "bigint":
                    return bigint(Long.MIN_VALUE, Long.MAX_VALUE);
                case "varint":
                    return varint(BigInteger.valueOf(Long.MIN_VALUE), BigInteger.valueOf(Long.MAX_VALUE));
                case "float":
                    return cfloat(-1000.0f, 1000.0f);
                case "double":
                    return cdouble(-1000.0, 1000.0);
                case "decimal":
                    return decimal(BigDecimal.valueOf(-1000), BigDecimal.valueOf(1000));
                case "inet":
                    return inet();
                case "uuid":
                    return uuid();
                case "timeuuid":
                    return timeuuid();
                case "timestamp":
                    return timestamp();
                case "time":
                    return time(0, 24 * Duration.NANOS_PER_HOUR);
                case "date":
                    return date();
                case "duration":
                    return duration(0, 1000);
                default:
                    throw new IllegalArgumentException("Unsupported type: " + cqlTypeName);
            }
        }
    }

    abstract class AbstractCqlDataGenerator implements CqlDataGenerator
    {
        final RandomPlus random;

        public AbstractCqlDataGenerator(RandomPlus random)
        {
            this.random = random;
        }
    }

    class AsciiGenerator extends AbstractCqlDataGenerator
    {
        private final int minLength;
        private final int maxLength;

        public AsciiGenerator(RandomPlus random, int minLength, int maxLength)
        {
            super(random);
            this.minLength = minLength;
            this.maxLength = maxLength;
        }

        @Override
        public String generateCqlValue()
        {
            int length = random.nextInt(minLength, maxLength + 1);
            return String.format("'%s'", random.nextAlphanumeric(length));
        }
    }

    class UTF8Generator extends AbstractCqlDataGenerator
    {
        private final int minLength;
        private final int maxLength;

        public UTF8Generator(RandomPlus random, int minLength, int maxLength)
        {
            super(random);
            this.minLength = minLength;
            this.maxLength = maxLength;
        }

        @Override
        public String generateCqlValue()
        {
            int length = random.nextInt(minLength, maxLength + 1);
            return String.format("'%s'", random.nextAlphanumeric(length));
        }
    }

    class BooleanGenerator extends AbstractCqlDataGenerator
    {
        public BooleanGenerator(RandomPlus random)
        {
            super(random);
        }

        @Override
        public String generateCqlValue()
        {
            return String.valueOf(random.nextBoolean());
        }
    }

    class ByteGenerator extends AbstractCqlDataGenerator
    {
        public ByteGenerator(RandomPlus random)
        {
            super(random);
        }

        @Override
        public String generateCqlValue()
        {
            return String.valueOf((byte) random.nextInt());
        }
    }

    class BytesGenerator extends AbstractCqlDataGenerator
    {
        private final int minLength;
        private final int maxLength;

        public BytesGenerator(RandomPlus random, int minLength, int maxLength)
        {
            super(random);
            this.minLength = minLength;
            this.maxLength = maxLength;
        }

        @Override
        public String generateCqlValue()
        {
            return Hex.bytesToHex(random.nextBytes(minLength, maxLength));
        }
    }

    class ShortGenerator extends AbstractCqlDataGenerator
    {
        private final short min;
        private final short max;

        public ShortGenerator(RandomPlus random, short min, short max)
        {
            super(random);
            this.min = min;
            this.max = max;
        }

        @Override
        public String generateCqlValue()
        {
            return String.valueOf((short) random.nextInt(min, max));
        }
    }

    class IntGenerator extends AbstractCqlDataGenerator
    {
        private final int min;
        private final int max;

        public IntGenerator(RandomPlus random, int min, int max)
        {
            super(random);
            this.min = min;
            this.max = max;
        }

        @Override
        public String generateCqlValue()
        {
            return String.valueOf(random.nextInt(min, max));
        }
    }

    class LongGenerator extends AbstractCqlDataGenerator
    {
        private final long min;
        private final long max;

        public LongGenerator(RandomPlus random, long min, long max)
        {
            super(random);
            this.min = min;
            this.max = max;
        }

        @Override
        public String generateCqlValue()
        {
            return String.valueOf(random.nextLong(min, max));
        }
    }

    class BigIntegerGenerator extends AbstractCqlDataGenerator
    {
        private final BigInteger min;
        private final BigInteger max;

        public BigIntegerGenerator(RandomPlus random, BigInteger min, BigInteger max)
        {
            super(random);
            this.min = min;
            this.max = max;
        }

        @Override
        public String generateCqlValue()
        {
            return random.nextBigInteger(min, max).toString();
        }
    }

    class FloatGenerator extends AbstractCqlDataGenerator
    {
        private final float min;
        private final float max;

        public FloatGenerator(RandomPlus random, float min, float max)
        {
            super(random);
            this.min = min;
            this.max = max;
        }

        @Override
        public String generateCqlValue()
        {
            return String.valueOf(random.nextFloat(min, max));
        }
    }

    class DoubleGenerator extends AbstractCqlDataGenerator
    {
        private final double min;
        private final double max;

        public DoubleGenerator(RandomPlus random, double min, double max)
        {
            super(random);
            this.min = min;
            this.max = max;
        }

        @Override
        public String generateCqlValue()
        {
            return String.valueOf(random.nextDouble(min, max));
        }
    }

    class DecimalGenerator extends AbstractCqlDataGenerator
    {
        private final BigDecimal min;
        private final BigDecimal max;

        public DecimalGenerator(RandomPlus random, BigDecimal min, BigDecimal max)
        {
            super(random);
            this.min = min;
            this.max = max;
        }

        @Override
        public String generateCqlValue()
        {
            return random.nextBigDecimal(min, max).toString();
        }
    }

    class InetAddressGenerator extends AbstractCqlDataGenerator
    {
        public InetAddressGenerator(RandomPlus random)
        {
            super(random);
        }

        @Override
        public String generateCqlValue()
        {
            try
            {
                InetAddress address = InetAddress.getByAddress(new byte[]{
                    (byte) random.nextInt(1, 256),
                    (byte) random.nextInt(0, 256),
                    (byte) random.nextInt(0, 256),
                    (byte) random.nextInt(1, 256)
                });
                return String.format("'%s'", address.getHostAddress());
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }
    }

    class UUIDGenerator extends AbstractCqlDataGenerator
    {
        public UUIDGenerator(RandomPlus random)
        {
            super(random);
        }

        @Override
        public String generateCqlValue()
        {
            return new UUID(random.nextLong(), random.nextLong()).toString();
        }
    }

    class TimeUUIDGenerator extends AbstractCqlDataGenerator
    {
        public TimeUUIDGenerator(RandomPlus random)
        {
            super(random);
        }

        @Override
        public String generateCqlValue()
        {
            return TimeUUID.Generator.atUnixMillis(random.nextInt()).toString();
        }
    }

    class TimestampGenerator extends AbstractCqlDataGenerator
    {
        public TimestampGenerator(RandomPlus random)
        {
            super(random);
        }

        @Override
        public String generateCqlValue()
        {
            return String.valueOf(random.nextLong());
        }
    }

    class TimeGenerator extends AbstractCqlDataGenerator
    {
        private final long min;
        private final long max;

        public TimeGenerator(RandomPlus random, long min, long max)
        {
            super(random);
            this.min = min;
            this.max = max;
        }

        @Override
        public String generateCqlValue()
        {
            return String.valueOf(random.nextLong(min, max));
        }
    }

    class DateGenerator extends AbstractCqlDataGenerator
    {
        public DateGenerator(RandomPlus random)
        {
            super(random);
        }

        @Override
        public String generateCqlValue()
        {
            return String.format("'%d-%02d-%02d'",
                                 random.nextInt(1970, 2070),
                                 random.nextInt(1, 13),
                                 random.nextInt(1, 29));
        }
    }

    class DurationGenerator extends AbstractCqlDataGenerator
    {
        private final long minMilliseconds;
        private final long maxMilliseconds;

        public DurationGenerator(RandomPlus random, long minSeconds, long maxSeconds)
        {
            super(random);
            this.minMilliseconds = minSeconds * 1000;
            this.maxMilliseconds = maxSeconds * 1000;
        }

        @Override
        public String generateCqlValue()
        {
            return String.format("%dms", random.nextLong(minMilliseconds, maxMilliseconds));
        }
    }

    class RowGenerator
    {
        final CqlDataGenerator[] columnValueGenerators;

        public RowGenerator(CqlDataGenerator[] columnValueGenerators)
        {
            this.columnValueGenerators = columnValueGenerators;
        }

        public String generateRow()
        {
            return Arrays.stream(columnValueGenerators)
                         .map(CqlDataGenerator::generateCqlValue)
                         .collect(Collectors.joining(","));
        }
    }
}
