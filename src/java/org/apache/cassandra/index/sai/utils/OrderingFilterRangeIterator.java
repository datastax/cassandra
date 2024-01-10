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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import javax.annotation.concurrent.NotThreadSafe;

import org.apache.cassandra.index.sai.SSTableIndex;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.Pair;

/**
 * An iterator that consumes a chunk of {@link PrimaryKey}s from the {@link RangeIterator}, passes them to the
 * {@link Function} to filter the chunk of {@link PrimaryKey}s and then pass the results to next consumer.
 * The PKs are currently returned in {@link PrimaryKey} order, but that contract may change.
 */
@NotThreadSafe
public class OrderingFilterRangeIterator implements Iterator<Pair<List<OrderIterator>, Set<SSTableIndex>>>, AutoCloseable
{
    private final RangeIterator input;
    private final int chunkSize;
    private final Function<List<PrimaryKey>, Pair<List<OrderIterator>, Set<SSTableIndex>>> nextRangeFunction;

    public OrderingFilterRangeIterator(RangeIterator input, int chunkSize, Function<List<PrimaryKey>, Pair<List<OrderIterator>, Set<SSTableIndex>>> nextRangeFunction)
    {
        this.input = input;
        this.chunkSize = chunkSize;
        this.nextRangeFunction = nextRangeFunction;
    }

    @Override
    public boolean hasNext()
    {
        return input.hasNext();
    }

    @Override
    public Pair<List<OrderIterator>, Set<SSTableIndex>> next()
    {
        List<PrimaryKey> nextKeys = new ArrayList<>(chunkSize);
        do
        {
            nextKeys.add(input.next());
        }
        while (nextKeys.size() < chunkSize && input.hasNext());
        return nextRangeFunction.apply(nextKeys);
    }

    public void close() {
        FileUtils.closeQuietly(input);
    }
}
