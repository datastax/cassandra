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
import java.net.InetAddress;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.cassandra.utils.Hex;
import org.apache.cassandra.utils.UUIDGen;

public interface CqlDataGenerator
{
    /**
     * Generates a random value and returns it as a CQL literal string (e.g. 'abc', 123, 0xdeadbeef)
     */
    String generateCqlValue();

    /**
     * Factory for creating RandomDataGenerators for different CQL types
     */
    public class Factory
    {
        private final RandomPlus random;

        public Factory(Random random)
        {
            this.random = new RandomPlus(random);
        }

        public Factory()
        {
            this(ThreadLocalRandom.current());
        }

        private String randomAlphanumeric(int length)
        {
            return random.getWrappedRandom().ints(48, 123)
                         .filter(i -> (i <= 57 || i >= 65) && (i <= 90 || i >= 97))
                         .limit(length)
                         .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                         .toString();
        }

        public CqlDataGenerator ascii(int minLength, int maxLength)
        {
            return new AsciiGenerator(this, random, minLength, maxLength);
        }

        public CqlDataGenerator utf8(int minLength, int maxLength)
        {
            return new UTF8Generator(this, random, minLength, maxLength);
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
            return new SimpleDateGenerator(random);
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
                    return time(0, 24 * 60 * 60 * 1000000000L -1);
                case "date":
                    return date();
                case "duration":
                    return duration(0, 1000);
                default:
                    throw new IllegalArgumentException("Unsupported type: " + cqlTypeName);
            }
        }
    }


    class AsciiGenerator implements CqlDataGenerator
    {
        private final Factory factory;
        private final RandomPlus random;
        private final int minLength;
        private final int maxLength;

        public AsciiGenerator(Factory factory, RandomPlus random, int minLength, int maxLength)
        {
            this.factory = factory;
            this.random = random;
            this.minLength = minLength;
            this.maxLength = maxLength;
        }

        @Override
        public String generateCqlValue()
        {

            int length = random.nextInt(minLength, maxLength + 1);
            return String.format("'%s'", factory.randomAlphanumeric(length));
        }
    }

    class UTF8Generator implements CqlDataGenerator
    {
        private final Factory factory;
        private final RandomPlus random;
        private final int minLength;
        private final int maxLength;

        public UTF8Generator(Factory factory, RandomPlus random, int minLength, int maxLength)
        {
            this.factory = factory;
            this.random = random;
            this.minLength = minLength;
            this.maxLength = maxLength;
        }

        @Override
        public String generateCqlValue()
        {
            int length = random.nextInt(minLength, maxLength + 1);
            return String.format("'%s'", factory.randomAlphanumeric(length));
        }
    }

    class BooleanGenerator implements CqlDataGenerator
    {
        private final RandomPlus random;

        public BooleanGenerator(RandomPlus random)
        {
            this.random = random;
        }

        @Override
        public String generateCqlValue()
        {
            return String.valueOf(random.nextBoolean());
        }
    }

    class ByteGenerator implements CqlDataGenerator
    {
        private final RandomPlus random;

        public ByteGenerator(RandomPlus random)
        {
            this.random = random;
        }

        @Override
        public String generateCqlValue()
        {
            return String.valueOf((byte) random.nextInt());
        }
    }

    class BytesGenerator implements CqlDataGenerator
    {
        private final RandomPlus random;
        private final int minLength;
        private final int maxLength;

        public BytesGenerator(RandomPlus random, int minLength, int maxLength)
        {
            this.random = random;
            this.minLength = minLength;
            this.maxLength = maxLength;
        }

        @Override
        public String generateCqlValue()
        {
            return Hex.bytesToHex(random.nextBytes(minLength, maxLength));
        }
    }

    class ShortGenerator implements CqlDataGenerator
    {
        private final RandomPlus random;
        private final short min;
        private final short max;

        public ShortGenerator(RandomPlus random, short min, short max)
        {
            this.random = random;
            this.min = min;
            this.max = max;
        }

        @Override
        public String generateCqlValue()
        {
            return String.valueOf((short) random.nextInt(min, max));
        }
    }

    class IntGenerator implements CqlDataGenerator
    {
        private final RandomPlus random;
        private final int min;
        private final int max;

        public IntGenerator(RandomPlus random, int min, int max)
        {
            this.random = random;
            this.min = min;
            this.max = max;
        }

        @Override
        public String generateCqlValue()
        {
            return String.valueOf(random.nextInt(min, max));
        }
    }

    class LongGenerator implements CqlDataGenerator
    {
        private final RandomPlus random;
        private final long min;
        private final long max;

        public LongGenerator(RandomPlus random, long min, long max)
        {
            this.random = random;
            this.min = min;
            this.max = max;
        }

        @Override
        public String generateCqlValue()
        {
            return String.valueOf(random.nextLong(min, max));
        }
    }

    class BigIntegerGenerator implements CqlDataGenerator
    {
        private final RandomPlus random;
        private final BigInteger min;
        private final BigInteger max;

        public BigIntegerGenerator(RandomPlus random, BigInteger min, BigInteger max)
        {
            this.random = random;
            this.min = min;
            this.max = max;
        }

        @Override
        public String generateCqlValue()
        {
            return random.nextBigInteger(min, max).toString();
        }
    }

    class FloatGenerator implements CqlDataGenerator
    {
        private final RandomPlus random;
        private final float min;
        private final float max;

        public FloatGenerator(RandomPlus random, float min, float max)
        {
            this.random = random;
            this.min = min;
            this.max = max;
        }

        @Override
        public String generateCqlValue()
        {
            return String.valueOf(random.nextFloat(min, max));
        }
    }

    class DoubleGenerator implements CqlDataGenerator
    {
        private final RandomPlus random;
        private final double min;
        private final double max;

        public DoubleGenerator(RandomPlus random, double min, double max)
        {
            this.random = random;
            this.min = min;
            this.max = max;
        }

        @Override
        public String generateCqlValue()
        {
            return String.valueOf(random.nextDouble(min, max));
        }
    }

    class DecimalGenerator implements CqlDataGenerator
    {
        private final RandomPlus random;
        private final BigDecimal min;
        private final BigDecimal max;

        public DecimalGenerator(RandomPlus random, BigDecimal min, BigDecimal max)
        {
            this.random = random;
            this.min = min;
            this.max = max;
        }

        @Override
        public String generateCqlValue()
        {
            return random.nextBigDecimal(min, max).toString();
        }
    }

    class InetAddressGenerator implements CqlDataGenerator
    {
        private final RandomPlus random;

        public InetAddressGenerator(RandomPlus random)
        {
            this.random = random;
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

    class UUIDGenerator implements CqlDataGenerator
    {
        private final RandomPlus random;

        public UUIDGenerator(RandomPlus random)
        {
            this.random = random;
        }

        @Override
        public String generateCqlValue()
        {
            return new UUID(random.nextLong(), random.nextLong()).toString();
        }
    }

    class TimeUUIDGenerator implements CqlDataGenerator
    {
        private final RandomPlus random;

        public TimeUUIDGenerator(RandomPlus random)
        {
            this.random = random;
        }

        @Override
        public String generateCqlValue()
        {
            return UUIDGen.getTimeUUID(random.nextInt()).toString();
        }
    }

    class TimestampGenerator implements CqlDataGenerator
    {
        private final RandomPlus random;

        public TimestampGenerator(RandomPlus random)
        {
            this.random = random;
        }

        @Override
        public String generateCqlValue()
        {
            return String.valueOf(random.nextLong());
        }
    }

    class TimeGenerator implements CqlDataGenerator
    {
        private final RandomPlus random;
        private final long min;
        private final long max;

        public TimeGenerator(RandomPlus random, long min, long max)
        {
            this.random = random;
            this.min = min;
            this.max = max;
        }

        @Override
        public String generateCqlValue()
        {
            return String.valueOf(random.nextLong(min, max));
        }
    }

    class SimpleDateGenerator implements CqlDataGenerator
    {
        private final RandomPlus random;

        public SimpleDateGenerator(RandomPlus random)
        {
            this.random = random;
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

    class DurationGenerator implements CqlDataGenerator
    {
        private final RandomPlus random;
        private final long minMilliseconds;
        private final long maxMilliseconds;

        public DurationGenerator(RandomPlus random, long minSeconds, long maxSeconds)
        {
            this.random = random;
            this.minMilliseconds = minSeconds * 1000;
            this.maxMilliseconds = maxSeconds * 1000;
        }

        @Override
        public String generateCqlValue()
        {
            return String.format("%dms", random.nextLong(minMilliseconds, maxMilliseconds));
        }
    }

    public static class RowGenerator
    {
        final CqlDataGenerator[] columnValueGenerators;

        public RowGenerator(CqlDataGenerator[] columnValueGenerators)
        {
            this.columnValueGenerators = columnValueGenerators;
        }

        public String generateRow()
        {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < columnValueGenerators.length; i++)
            {
                if (i > 0)
                    sb.append(", ");
                sb.append(columnValueGenerators[i].generateCqlValue());
            }
            return sb.toString();
        }
    }
}
