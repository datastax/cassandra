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

import java.util.Random;
import com.google.common.base.Preconditions;
import java.math.BigInteger;
import java.math.BigDecimal;

public class RandomPlus
{
    private final Random random;

    public RandomPlus(Random random)
    {
        this.random = random;
    }

    public boolean nextBoolean()
    {
        return random.nextBoolean();
    }

    public int nextInt()
    {
        return random.nextInt();
    }

    public int nextInt(int min, int max)
    {
        return (int) nextDouble(min, max);
    }

    public long nextLong()
    {
        return random.nextLong();
    }

    public long nextLong(long min, long max)
    {
        return (long) nextDouble(min, max);
    }

    public byte[] nextBytes(int minLength, int maxLength)
    {
        int length = nextInt(minLength, maxLength);
        byte[] bytes = new byte[length];
        random.nextBytes(bytes);
        return bytes;
    }

    public double nextDouble()
    {
        return random.nextDouble();
    }

    public double nextDouble(double min, double max)
    {
        Preconditions.checkArgument(min < max, "max must be greater than min");
        return min + (max - min) * random.nextDouble();
    }

    public float nextFloat(float min, float max)
    {
        Preconditions.checkArgument(min < max, "max must be greater than min");
        return min + (max - min) * random.nextFloat();
    }

    public Random getWrappedRandom()
    {
        return random;
    }

    public byte[] nextBytesArray(int minLength, int maxLength)
    {
        int length = nextInt(minLength, maxLength);
        byte[] bytes = new byte[length];
        random.nextBytes(bytes);
        return bytes;
    }

    public BigInteger nextBigInteger(BigInteger min, BigInteger max)
    {
        Preconditions.checkArgument(min.compareTo(max) < 0, "max must be greater than min");
        BigInteger range = max.subtract(min);
        int len = max.bitLength();
        BigInteger res = new BigInteger(len, random);
        if (res.compareTo(min) < 0)
            res = res.add(min);
        if (res.compareTo(range) >= 0)
            res = res.mod(range).add(min);
        return res;
    }

    public BigDecimal nextBigDecimal(BigDecimal min, BigDecimal max)
    {
        return new BigDecimal(nextDouble(min.doubleValue(), max.doubleValue()));
    }
}
