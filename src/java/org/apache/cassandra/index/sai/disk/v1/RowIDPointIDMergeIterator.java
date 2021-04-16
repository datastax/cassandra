/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.index.sai.disk.v1;


import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.lucene.util.PriorityQueue;

public final class RowIDPointIDMergeIterator implements Iterator<BKDReader.RowIDAndPointID>, Closeable
{
    private final TermMergeQueue queue;
    final SubIterator[] top;
    final BKDReader.RowIDAndPointIDIterator[] iterators;
    private final boolean removeDuplicates = true;
    private int numTop;
    private BKDReader.RowIDAndPointID current;

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public RowIDPointIDMergeIterator(BKDReader.RowIDAndPointIDIterator... iterators)
    {
        this.iterators = iterators;
        queue = new TermMergeQueue(iterators.length);
        top = new SubIterator[iterators.length];
        int index = 0;
        for (Iterator<BKDReader.RowIDAndPointID> iterator : iterators)
        {
            if (iterator.hasNext())
            {
                SubIterator sub = new SubIterator();
                sub.current = iterator.next();
                sub.iterator = iterator;
                sub.index = index++;
                queue.add(sub);
            }
        }
    }

    @Override
    public void close() throws IOException
    {
       for (BKDReader.RowIDAndPointIDIterator it : iterators)
       {
           it.close();
       }
    }

    @Override
    public boolean hasNext()
    {
        if (queue.size() > 0)
        {
            return true;
        }

        for (int i = 0; i < numTop; i++)
        {
            if (top[i].iterator.hasNext())
            {
                return true;
            }
        }
        return false;
    }

    public int getNumTop()
    {
        return numTop;
    }

    @Override
    public BKDReader.RowIDAndPointID next()
    {
        // restore queue
        pushTop();

        // gather equal top elements
        if (queue.size() > 0)
        {
            pullTop();
        }
        else
        {
            current = null;
        }
        if (current == null)
        {
            throw new NoSuchElementException();
        }
        return current;
    }

    @Override
    public void remove()
    {
        throw new UnsupportedOperationException();
    }

    private void pullTop()
    {
        assert numTop == 0;
        top[numTop++] = queue.pop();
        if (removeDuplicates)
        {
            // extract all subs from the queue that have the same top element
            while (queue.size() != 0 && queue.top().current.compareTo(top[0].current) == 0)
            {
                top[numTop++] = queue.pop();
            }
        }
        current = top[0].current;
    }

    private void pushTop()
    {
        // call next() on each top, and put back into queue
        for (int i = 0; i < numTop; i++)
        {
            if (top[i].iterator.hasNext())
            {
                top[i].current = top[i].iterator.next();
                queue.add(top[i]);
            }
            else
            {
                // no more elements
                top[i].current = null;
            }
        }
        numTop = 0;
    }

    public static class SubIterator
    {
        Iterator<BKDReader.RowIDAndPointID> iterator;
        BKDReader.RowIDAndPointID current;
        int index;
    }

    private static class TermMergeQueue extends PriorityQueue<SubIterator>
    {

        TermMergeQueue(int size)
        {
            super(size);
        }

        @Override
        protected boolean lessThan(SubIterator a, SubIterator b)
        {
            final int cmp = a.current.compareTo(b.current);
            if (cmp != 0)
            {
                return cmp < 0;
            }
            else
            {
                return a.index < b.index;
            }
        }
    }
}
