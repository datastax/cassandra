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
package org.apache.cassandra.index.sai.memory;

import java.io.IOException;
import java.util.SortedSet;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.RangeIterator;

/**
 * A {@link RangeIterator} that filters the returned {@link PrimaryKey}s based on the provided keyRange
 */
public class FilteringKeyRangeIterator extends RangeIterator
{
    private final AbstractBounds<PartitionPosition> keyRange;
    private final PeekingIterator<PrimaryKey> source;

    public FilteringKeyRangeIterator(SortedSet<PrimaryKey> keys, AbstractBounds<PartitionPosition> keyRange)
    {
        super(keys.first(), keys.last(), keys.size());
        this.keyRange = keyRange;
        this.source = Iterators.peekingIterator(keys.iterator());
    }

    @Override
    protected PrimaryKey computeNext()
    {
        while (source.hasNext())
        {
            PrimaryKey key = source.next();
            if (keyRange.contains(key.partitionKey()))
                return key;
        }
        return endOfData();
    }

    @Override
    protected void performSkipTo(PrimaryKey nextKey)
    {
        while (source.hasNext())
        {
            if (source.peek().compareTo(nextKey) >= 0)
                break;
            // Consume key
            source.next();
        }
    }

    @Override
    public void close() throws IOException
    {
    }
}
