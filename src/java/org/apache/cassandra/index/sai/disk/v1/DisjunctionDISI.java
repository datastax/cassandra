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
import java.util.List;
import java.util.PriorityQueue;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;

import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.io.util.FileUtils;

/**
 * Disjunction posting list over sub posting lists.
 *
 * Copied and modified from Lucene 7.5
 */
@NotThreadSafe
public class DisjunctionDISI implements PostingList
{
    private final DisiPriorityQueue subIterators;
    private final long cost;
    private final Closeable onClose;

    private DisjunctionDISI(@Nonnull DisiPriorityQueue subIterators, Closeable onClose)
    {
        this.subIterators = subIterators;
        this.onClose = onClose;
        long cost = 0;
        for (DisiWrapper w : subIterators)
        {
            cost += w.cost;
        }
        this.cost = cost;
    }

    @Override
    public void close() throws IOException
    {
        if (onClose != null)
        {
            FileUtils.close(onClose);
        }
    }

    public static @Nonnull PostingList create(@Nonnull PriorityQueue<PeekablePostingList> postings)
    {
        return create(postings, null);
    }

    public static @Nonnull PostingList create(@Nonnull PriorityQueue<PeekablePostingList> postings, Closeable onClose)
    {
        DisiPriorityQueue queue = new DisiPriorityQueue(postings.size());
        for (PostingList list : postings)
        {
            queue.add(new DisiWrapper(list));
        }
        return new DisjunctionDISI(queue, onClose);
    }

    public static @Nonnull PostingList create(@Nonnull List<PostingList> postings, Closeable onClose)
    {
        DisiPriorityQueue queue = new DisiPriorityQueue(postings.size());
        for (PostingList list : postings)
        {
            queue.add(new DisiWrapper(list));
        }
        return new DisjunctionDISI(queue, onClose);
    }

    @Override
    public long size()
    {
        return cost;
    }

    @Override
    public long nextPosting() throws IOException
    {
        DisiWrapper top = subIterators.top();
        final long doc = top.doc;
        do
        {
            top.doc = top.iterator.nextPosting();
            top = subIterators.updateTop();
        } while (top.doc == doc);

        return top.doc;
    }

    @Override
    public long advance(long target) throws IOException
    {
        DisiWrapper top = subIterators.top();
        do
        {
            top.doc = top.iterator.advance(target);
            top = subIterators.updateTop();
        } while (top.doc < target);

        return top.doc;
    }
}


