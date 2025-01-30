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
package org.apache.cassandra.index.sai.disk.v1.postings;

import java.io.IOException;
import java.util.ArrayList;
import java.util.PriorityQueue;
import javax.annotation.concurrent.NotThreadSafe;

import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.io.util.FileUtils;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Merges multiple {@link PostingList} which individually contain unique items into a single list.
 */
@NotThreadSafe
public class MergePostingList implements PostingList
{
    final ArrayList<PeekablePostingList> postingLists;
    // (Intersection code just calls advance(long), so don't create this until we need it)
    PriorityQueue<PeekablePostingList> pq;
    final int size;
    private int lastRowId = -1;

    private MergePostingList(ArrayList<PeekablePostingList> postingLists)
    {
        checkArgument(!postingLists.isEmpty());
        this.postingLists = postingLists;
        long totalPostings = 0;
        for (PostingList postingList : postingLists)
        {
            totalPostings += postingList.size();
        }
        // We could technically "overflow" integer if enough row ids are duplicated in the source posting lists.
        // The size does not affect correctness, so just use integer max if that happens.
        this.size = (int) Math.min(totalPostings, Integer.MAX_VALUE);
    }

    public static PostingList merge(ArrayList<PeekablePostingList> postings)
    {
        if (postings.isEmpty())
            return PostingList.EMPTY;

        if (postings.size() == 1)
            return postings.get(0);

        return new MergePostingList(postings);
    }

    @Override
    public int nextPosting() throws IOException
    {
        // lazily create PQ if we haven't already
        if (pq == null)
        {
            // elements could be removed in advance() even thouh postingLists started as non-empty
            if (postingLists.isEmpty())
                return PostingList.END_OF_STREAM;

            // Leverage PQ's O(N) heapify time complexity
            pq = new PriorityQueue<>(postingLists);
        }

        while (!pq.isEmpty())
        {
            // remove the list with the next rowid, then add it back in the correct order
            // for the one it has after that
            PeekablePostingList head = pq.poll();
            int next = head.nextPosting();

            if (next == END_OF_STREAM)
            {
                // skip current posting list
            }
            else if (next > lastRowId)
            {
                // row we haven't seen before
                lastRowId = next;
                pq.add(head);
                return next;
            }
            else if (next == lastRowId)
            {
                // we've already seen this one, keep going
                pq.add(head);
            }
        }

        return PostingList.END_OF_STREAM;
    }

    @SuppressWarnings("resource")
    @Override
    public int advance(int targetRowID) throws IOException
    {
        // clean out obsolete child lists, and remember the smallest row seen in case
        // we can use it for the fast path
        int nextRowId = PostingList.END_OF_STREAM;
        PostingList nextPostingList = null;
        for (int i = postingLists.size() - 1; i >= 0; i--) // index backwards to simplify the remove() case
        {
            var peekable = postingLists.get(i);
            int peeked = peekable.advanceWithoutConsuming(targetRowID);

            // clean out obsolete child lists
            if (peeked == PostingList.END_OF_STREAM)
            {
                postingLists.remove(i);
                continue;
            }

            if (lastRowId <= peeked && peeked < nextRowId) {
                nextRowId = peeked;
                nextPostingList = peekable;
            }
        }

        if (lastRowId == targetRowID) {
            // we're asking for the next row, past the current row, which an arbitrary
            // number of our child posting lists may be pointing to.  In this case we
            // need to let the PQ do its work to figure out what the correct next row AFTER
            // this one is.
            return nextPosting();
        }

        // fast path -- no PQ
        pq = null; // we're invalidating the pq's assumptions, so force a rebuild if we need it again
        if (nextPostingList == null)
            return PostingList.END_OF_STREAM;
        lastRowId = nextPostingList.nextPosting();
        return lastRowId;
    }

    @Override
    public int size()
    {
        return size;
    }

    @Override
    public void close() throws IOException
    {
        FileUtils.close(postingLists);
    }
}
