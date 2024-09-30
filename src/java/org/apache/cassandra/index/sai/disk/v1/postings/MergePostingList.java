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
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.IntMerger;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Merges multiple {@link PostingList} which individually contain unique items into a single list.
 */
@NotThreadSafe
public class MergePostingList extends IntMerger<PostingList> implements PostingList
{
    final int size;

    private MergePostingList(List<? extends PostingList> postingLists)
    {
        super(postingLists, PostingList.class);
        checkArgument(!postingLists.isEmpty());
        long totalPostings = 0;
        for (PostingList postingList : postingLists)
            totalPostings += postingList.size();

        // We could technically "overflow" integer if enough row ids are duplicated in the source posting lists.
        // The size does not affect correctness, so just use integer max if that happens.
        this.size = (int) Math.min(totalPostings, Integer.MAX_VALUE);
    }

    public static PostingList merge(List<PostingList> postings)
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
        return advance();
    }

    @Override
    public int advance(int targetRowID) throws IOException
    {
        return skipTo(targetRowID);
    }

    @Override
    public int size()
    {
        return size;
    }

    @Override
    public void close()
    {
        applyToAllSources(FileUtils::closeQuietly);
    }

    @Override
    public int advanceSource(PostingList s) throws IOException
    {
        return s.nextPosting();
    }

    @Override
    protected int skipSource(PostingList s, int targetPosition) throws IOException
    {
        return s.advance(targetPosition);
    }
}
