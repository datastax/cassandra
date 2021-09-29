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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.cassandra.io.util.FileUtils;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.PriorityQueue;

import static org.apache.cassandra.index.sai.disk.v2.blockindex.BlockIndexReader.IndexIterator;
import static org.apache.cassandra.index.sai.disk.v2.blockindex.BlockIndexReader.IndexState;


public class MergeIndexIterators implements IndexIterator
{
    final MergeQueue queue;
    final List<IndexIterator> toClose;
    final IndexState state = new IndexState();
    final BytesRefBuilder builder = new BytesRefBuilder();

    public MergeIndexIterators(List<IndexIterator> iterators) throws IOException
    {
        toClose = new ArrayList<>(iterators);
        queue = new MergeQueue(iterators.size());
        for (IndexIterator it : iterators)
        {
            it.next(); // init iterator
            queue.add(it);
        }
    }

    @Override
    public IndexState current()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public IndexState next() throws IOException
    {
        if (queue.size() != 0)
        {
            final IndexIterator iterator = queue.top();

            final IndexState topState = iterator.current();

            builder.clear();
            builder.append(topState.term);
            state.term = builder.get();
            state.rowid = topState.rowid;

            final IndexState nextState = iterator.next();

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
        FileUtils.close(toClose);
    }

    static final Comparator<IndexState> comparator = (a, b) -> {
        int cmp1 = a.term.compareTo(b.term);
        if (cmp1 == 0)
        {
            return Long.compare(a.rowid, b.rowid);
        }
        else
        {
            return cmp1;
        }
    };

    private static class MergeQueue extends PriorityQueue<IndexIterator>
    {
        public MergeQueue(int maxSize)
        {
            super(maxSize);
        }

        @Override
        public boolean lessThan(IndexIterator a, IndexIterator b)
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
