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
import java.util.List;
import javax.annotation.concurrent.NotThreadSafe;

import org.apache.cassandra.index.sai.disk.PostingList;

import static java.lang.Math.max;

/**
 * Performs intersection operations on multiple PostingLists, returning only postings
 * that appear in all inputs.
 */
@NotThreadSafe
public class IntersectingPostingList implements PostingList
{
    private final List<PostingList> postingLists;
    private final int size;

    // currentRowIds state is effectively local to findNextIntersection, but we keep it
    // around as a field to avoid repeated allocations there
    private final int[] currentRowIds;

    private IntersectingPostingList(List<PostingList> postingLists)
    {
        assert !postingLists.isEmpty();
        this.postingLists = postingLists;
        this.size = postingLists.stream()
                                .mapToInt(PostingList::size)
                                .min()
                                .orElse(0);
        this.currentRowIds = new int[postingLists.size()];
    }

    public static PostingList intersect(List<PostingList> postingLists)
    {
        if (postingLists.size() == 1)
            return postingLists.get(0);

        if (postingLists.stream().anyMatch(PostingList::isEmpty))
            return PostingList.EMPTY;

        return new IntersectingPostingList(postingLists);
    }

    @Override
    public int nextPosting() throws IOException
    {
        return findNextIntersection(Integer.MIN_VALUE, false);
    }

    @Override
    public int advance(int targetRowID) throws IOException
    {
        assert targetRowID >= 0 : targetRowID;
        return findNextIntersection(targetRowID, true);
    }

    private int findNextIntersection(int targetRowID, boolean isAdvance) throws IOException
    {
        // Initialize currentRowIds from the underlying posting lists
        for (int i = 0; i < postingLists.size(); i++)
        {
            currentRowIds[i] = isAdvance
                               ? postingLists.get(i).advance(targetRowID)
                               : postingLists.get(i).nextPosting();

            if (currentRowIds[i] == END_OF_STREAM)
                return END_OF_STREAM;
        }

        while (true)
        {
            // Find the maximum row ID among all posting lists
            int maxRowId = targetRowID;
            for (int rowId : currentRowIds)
                maxRowId = max(maxRowId, rowId);

            // Advance any posting list that's behind the maximum
            boolean allMatch = true;
            for (int i = 0; i < postingLists.size(); i++)
            {
                if (currentRowIds[i] < maxRowId)
                {
                    currentRowIds[i] = postingLists.get(i).advance(maxRowId);
                    if (currentRowIds[i] == END_OF_STREAM)
                        return END_OF_STREAM;
                    allMatch = false;
                }
            }

            // If all posting lists have the same row ID, we've found an intersection
            if (allMatch)
                return maxRowId;

            // Otherwise, continue searching with the new maximum as target
            targetRowID = maxRowId;
        }
    }

    @Override
    public int size()
    {
        return size;
    }

    @Override
    public void close() throws IOException
    {
        for (PostingList list : postingLists)
            list.close();
    }
}


