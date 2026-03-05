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

package org.apache.cassandra.utils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Random;

import com.google.common.base.Preconditions;

/**
 * Extension of {@link Random} with additional utility methods for generating random values
 * within specified ranges for various numeric types.
 */
public class RandomPlus extends Random
{
    public RandomPlus()
    {
        super();
    }

    public RandomPlus(long seed)
    {
        super(seed);
    }

    public int nextInt(int min, int max)
    {
        return (int) nextDouble(min, max);
    }

    public long nextLong(long min, long max)
    {
        return (long) nextDouble(min, max);
    }

    public byte[] nextBytes(int minLength, int maxLength)
    {
        int length = nextInt(minLength, maxLength);
        byte[] bytes = new byte[length];
        nextBytes(bytes);
        return bytes;
    }

    public double nextDouble(double min, double max)
    {
        Preconditions.checkArgument(min < max, "max must be greater than min");
        return min + (max - min) * nextDouble();
    }

    public float nextFloat(float min, float max)
    {
        Preconditions.checkArgument(min < max, "max must be greater than min");
        return min + (max - min) * nextFloat();
    }

    public BigInteger nextBigInteger(BigInteger min, BigInteger max)
    {
        Preconditions.checkArgument(min.compareTo(max) < 0, "max must be greater than min");
        BigInteger range = max.subtract(min);
        int len = range.bitLength();
        BigInteger res;
        do
        {
            res = new BigInteger(len, this).add(min);
        }
        while (res.compareTo(max) >= 0);
        return res;
    }

    public BigDecimal nextBigDecimal(BigDecimal min, BigDecimal max)
    {
        return BigDecimal.valueOf(nextDouble(min.doubleValue(), max.doubleValue()));
    }

    public String nextAlphanumeric(int length)
    {
        return ints(48, 123)
               .filter(i -> (i <= 57 || i >= 65) && (i <= 90 || i >= 97))
               .limit(length)
               .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
               .toString();
    }
}
