/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.index.sai.disk.v1.postings;

import java.io.IOException;

import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.io.util.FileUtils;

public class ComplementPostingList implements PostingList
{
    private final PostingList source;
    private final int lastSourceRowId;
    private int nextRowId;
    private int nextSourceRowId = -1;

    /**
     * A posting list that complements the provided posting list within the specified range.
     *
     * @param minSegmentRowId inclusive minimum row id
     * @param maxSegmentRowId exclusive maximum row id
     * @param source posting list to complement
     */
    public ComplementPostingList(int minSegmentRowId, int maxSegmentRowId, PostingList source)
    {
        this.nextRowId = minSegmentRowId;
        this.lastSourceRowId = maxSegmentRowId;
        this.source = source == null ? PostingList.EMPTY : source;
    }

    @Override
    public int nextPosting() throws IOException
    {
        if (nextSourceRowId == -1)
            nextSourceRowId = source.nextPosting();

        // Move both pointers forward
        while (nextSourceRowId == nextRowId)
        {
            nextRowId++;
            nextSourceRowId = source.nextPosting();
        }

        if (nextRowId > lastSourceRowId)
            return END_OF_STREAM;

        return nextRowId++;
    }

    @Override
    public int size()
    {
        return (lastSourceRowId - nextRowId) - source.size() + 1;
    }

    @Override
    public int advance(int targetRowID) throws IOException
    {
        nextRowId = targetRowID;
        nextSourceRowId = source.advance(targetRowID);
        return nextPosting();
    }

    @Override
    public void close()
    {
        FileUtils.closeQuietly(source);
    }
}
