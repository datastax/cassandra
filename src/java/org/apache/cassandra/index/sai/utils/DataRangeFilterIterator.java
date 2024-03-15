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

import java.util.Iterator;
import java.util.List;

import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.io.util.FileUtils;

/**
 * An iterator that filters the input iterator based on the provided list of {@link DataRange}.
 * The {@link DataRange} list must be ordered in ascending order of the partition key.
 */
public class DataRangeFilterIterator extends RangeIterator
{
    private final Iterator<AbstractBounds<PartitionPosition>> keyRanges;
    private AbstractBounds<PartitionPosition> currentKeyRange;
    private final PrimaryKey.Factory primaryKeyFactory;
    private final RangeIterator input;

    public DataRangeFilterIterator(List<DataRange> dataRanges, PrimaryKey.Factory primaryKeyFactory, RangeIterator input)
    {
        super(input);
        assert dataRanges != null && !dataRanges.isEmpty();
        this.keyRanges = dataRanges.stream().map(DataRange::keyRange).iterator();
        this.currentKeyRange = keyRanges.next();
        this.primaryKeyFactory = primaryKeyFactory;
        this.input = input;
    }

    /**
     * Returns the next available key contained by one of the keyRanges.
     * If the next key falls out of the current key range, it skips to the next key range, and so on.
     * If no more keys or no more ranges are available, returns endOfData().
     */
    @Override
    protected PrimaryKey computeNext()
    {
        if (!input.hasNext())
            return endOfData();

        PrimaryKey key = input.next();
        while (!(currentKeyRange.contains(key.partitionKey())))
        {
            if (!currentKeyRange.right.isMinimum() && currentKeyRange.right.compareTo(key.partitionKey()) <= 0)
            {
                // We enter this block when currentKeyRange does not contain the current key,
                // and the current key is greater than the currentKeyRange's right boundary.
                if (!keyRanges.hasNext())
                    return endOfData();
                currentKeyRange = keyRanges.next();
            }
            else
            {
                // the following condition may be false if currentKeyRange.left is not inclusive,
                // and key == currentKeyRange.left; in this case we should not try to skipTo the beginning
                // of the range because that would be requesting the key to go backwards
                // (in some implementations, skipTo can go backwards, and we don't want that)
                if (currentKeyRange.left.getToken().compareTo(key.token()) > 0)
                {
                    // key before the current range, so let's move the key forward
                    input.skipTo(primaryKeyFactory.createTokenOnly(currentKeyRange.left.getToken()));
                }
                if (!input.hasNext())
                    return endOfData();
                key = input.next();
            }
        }
        return key;
    }

    @Override
    public void performSkipTo(PrimaryKey nextToken)
    {
        input.skipTo(nextToken);
    }

    @Override
    public void close()
    {
        FileUtils.closeQuietly(input);
    }
}
