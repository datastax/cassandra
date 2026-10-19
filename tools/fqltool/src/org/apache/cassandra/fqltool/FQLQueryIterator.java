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

package org.apache.cassandra.fqltool;

import java.util.Arrays;
import java.util.Comparator;

import net.openhft.chronicle.queue.ExcerptTailer;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.Merger;

public class FQLQueryIterator implements CloseableIterator<FQLQuery>
{
    // use a priority queue to be able to sort the head of the query logs in memory
    private final Merger<FQLQuery, Void, Void> pq;
    private final ExcerptTailer tailer;
    private final FQLQueryReader reader;

    /**
     * Create an iterator over the FQLQueries in tailer
     *
     * Reads up to readAhead queries in to memory to be able to sort them (the files are mostly sorted already)
     */
    public FQLQueryIterator(ExcerptTailer tailer, int readAhead)
    {
        assert readAhead > 0 : "readAhead needs to be > 0";
        reader = new FQLQueryReader();
        this.tailer = tailer;
        pq = new Merger<>(Arrays.asList(new Void[readAhead]), // create read-ahead many null sources
                          avoid -> readNext(),                // calling our readNext for each of them
                          avoid -> {},
                          Comparator.naturalOrder(),
                          null);
    }

    @Override
    public boolean hasNext()
    {
        return pq.hasNext();
    }

    @Override
    public FQLQuery next()
    {
        return pq.nonReducingNext();
    }

    private FQLQuery readNext()
    {
        if (tailer.readDocument(reader))
            return reader.getQuery();
        return null;
    }

    @Override
    public void close()
    {
        // nothing to do
    }
}

