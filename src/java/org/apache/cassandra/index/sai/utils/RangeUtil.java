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

package org.apache.cassandra.index.sai.utils;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.format.SSTableReader;

public class RangeUtil
{
    private static final Token.KeyBound MIN_KEY_BOUND = DatabaseDescriptor.getPartitioner().getMinimumToken().minKeyBound();

    public static boolean coversFullRing(AbstractBounds<PartitionPosition> keyRange)
    {
        return keyRange.left.equals(MIN_KEY_BOUND) && keyRange.right.equals(MIN_KEY_BOUND);
    }

    /**
     * Check if the provided {@link SSTableReader} intersects with the provided key range.
     * @param reader SSTableReader
     * @param keyRange key range
     * @return true the key range intersects with the min/max key bounds
     */
    public static boolean intersects(SSTableReader reader, AbstractBounds<PartitionPosition> keyRange)
    {
        return intersects(reader.first.getToken().minKeyBound(), reader.last.getToken().maxKeyBound(), keyRange);
    }

    /**
     * Check if the min/max key bounds intersects with the keyRange
     * @param minKeyBound min key bound
     * @param maxKeyBound max key bound
     * @param keyRange key range
     * @return true the key range intersects with the min/max key bounds
     */
    public static boolean intersects(Token.KeyBound minKeyBound, Token.KeyBound maxKeyBound, AbstractBounds<PartitionPosition> keyRange)
    {
        if (keyRange instanceof Range && ((Range<?>)keyRange).isWrapAround())
            return keyRange.contains(minKeyBound) || keyRange.contains(maxKeyBound);

        int cmp = keyRange.right.compareTo(minKeyBound);
        // if right is minimum, it means right is the max token and bigger than maxKey.
        // if right bound is less than minKeyBound, no intersection
        if (!keyRange.right.isMinimum() && (!keyRange.inclusiveRight() && cmp == 0 || cmp < 0))
            return false;

        cmp = keyRange.left.compareTo(maxKeyBound);
        // if left bound is bigger than maxKeyBound, no intersection
        return (keyRange.isStartInclusive() || cmp != 0) && cmp <= 0;
    }
}
