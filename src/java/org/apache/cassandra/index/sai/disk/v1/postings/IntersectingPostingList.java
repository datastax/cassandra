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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.concurrent.NotThreadSafe;

import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.io.util.FileUtils;

/**
 * Performs intersection operations on multiple PostingLists, returning only postings
 * that appear in all inputs.
 */
@NotThreadSafe
public class IntersectingPostingList implements PostingList
{
    private final Map<ByteBuffer, PostingList> postingsByTerm;
    private final List<PostingList> postingLists; // so we can access by ordinal in intersection code
    private final int size;

    private IntersectingPostingList(Map<ByteBuffer, PostingList> postingsByTerm)
    {
        if (postingsByTerm.isEmpty())
            throw new AssertionError();
        this.postingsByTerm = postingsByTerm;
        this.postingLists = new ArrayList<>(postingsByTerm.values());
        this.size = postingLists.stream()
                                .mapToInt(PostingList::size)
                                .min()
                                .orElse(0);
    }

    /**
     * @return the intersection of the provided term-posting list mappings
     */
    public static IntersectingPostingList intersect(Map<ByteBuffer, PostingList> postingsByTerm)
    {
        // TODO optimize cases where
        // - we have a single postinglist
        // - any posting list is empty (intersection also empty)
        return new IntersectingPostingList(postingsByTerm);
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

    @Override
    public int frequency()
    {
        // call frequencies() instead
        throw new UnsupportedOperationException();
    }

    public Map<ByteBuffer, Integer> frequencies()
    {
        Map<ByteBuffer, Integer> result = new HashMap<>();
        for (Map.Entry<ByteBuffer, PostingList> entry : postingsByTerm.entrySet())
            result.put(entry.getKey(), entry.getValue().frequency());
        return result;
    }

    private int findNextIntersection(int targetRowID, boolean isAdvance) throws IOException
    {
        int maxRowId = targetRowID;
        int maxRowIdIndex = -1;

        // Scan through all posting lists looking for a common row ID
        for (int i = 0; i < postingLists.size(); i++)
        {
            // don't advance the sublist in which we found our current max
            if (i == maxRowIdIndex)
                continue;

            // Advance this sublist to the current max, special casing the first one as needed
            PostingList list = postingLists.get(i);
            int rowId = (isAdvance || maxRowIdIndex >= 0)
                        ? list.advance(maxRowId)
                        : list.nextPosting();
            if (rowId == END_OF_STREAM)
                return END_OF_STREAM;

            // Update maxRowId + index if we find a larger value, or this was the first sublist evaluated
            if (rowId > maxRowId || maxRowIdIndex < 0)
            {
                maxRowId = rowId;
                maxRowIdIndex = i;
                i = -1; // restart the scan with new maxRowId
            }
        }

        // Once we complete a full scan without finding a larger rowId, we've found an intersection
        return maxRowId;
    }

    @Override
    public int size()
    {
        return size;
    }

    @Override
    public void close()
    {
        for (PostingList list : postingLists)
            FileUtils.closeQuietly(list);
    }
}


