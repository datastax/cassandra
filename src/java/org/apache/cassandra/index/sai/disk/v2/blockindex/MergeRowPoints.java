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

package org.apache.cassandra.index.sai.disk.v2.blockindex;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.apache.cassandra.io.util.FileUtils;
import org.apache.lucene.util.PriorityQueue;

import static org.apache.cassandra.index.sai.disk.v2.blockindex.BlockIndexWriter.RowPoint;
import static org.apache.cassandra.index.sai.disk.v2.blockindex.BlockIndexWriter.RowPointIterator;


public class MergeRowPoints implements RowPointIterator
{
    final MergeQueue queue;
    final List<Closeable> toCloseList;
    final RowPoint state = new RowPoint();

    public MergeRowPoints(List<RowPointIterator> iterators, Closeable... toClose) throws IOException
    {
        toCloseList = new ArrayList<>(iterators);
        Arrays.stream(toClose).forEach(c -> toCloseList.add(c));
        queue = new MergeQueue(iterators.size());
        for (RowPointIterator it : iterators)
        {
            it.next(); // init iterator
            queue.add(it);
        }
    }

    @Override
    public RowPoint current()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public RowPoint next() throws IOException
    {
        if (queue.size() != 0)
        {
            final RowPointIterator iterator = queue.top();

            final RowPoint topState = iterator.current();

            state.rowID = topState.rowID;
            state.pointID = topState.pointID;

            final RowPoint nextState = iterator.next();

            if (nextState != null)
            {
                queue.updateTop();
            }
            else
            {
                queue.pop();
            }
            return state;
        }
        else
        {
            // queue is exhausted
            return null;
        }
    }

    @Override
    public void close() throws IOException
    {
        FileUtils.close(toCloseList);
    }

    static final Comparator<RowPoint> comparator = (a, b) -> {
        int cmp1 = Long.compare(a.rowID, b.rowID);
        if (cmp1 == 0)
        {
            return Long.compare(a.pointID, b.pointID);
        }
        else
        {
            return cmp1;
        }
    };

    private static class MergeQueue extends PriorityQueue<RowPointIterator>
    {
        public MergeQueue(int maxSize)
        {
            super(maxSize);
        }

        @Override
        public boolean lessThan(RowPointIterator a, RowPointIterator b)
        {
            assert a != b;

            int cmp = comparator.compare(a.current(), b.current());

            if (cmp < 0)
            {
                return true;
            }
            else return false;
        }
    }
}
