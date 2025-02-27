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
import java.util.function.ToIntFunction;

import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.lucene.util.LongHeap;

/**
 * A posting list for ANN search results.  Transforms results from similarity order to rowId order.
 */
public class ReorderingPostingList implements PostingList
{
    private final LongHeap segmentRowIds;
    private final int size;

    public <T> ReorderingPostingList(CloseableIterator<T> source, ToIntFunction<T> rowIdTransformer)
    {
        segmentRowIds = new LongHeap(32);
        int n = 0;
        try (source)
        {
            while (source.hasNext())
            {
                segmentRowIds.push(rowIdTransformer.applyAsInt(source.next()));
                n++;
            }
        }
        this.size = n;
    }

    @Override
    public int nextPosting() throws IOException
    {
        if (segmentRowIds.size() == 0)
            return PostingList.END_OF_STREAM;
        return (int) segmentRowIds.pop();
    }

    @Override
    public int size()
    {
        return size;
    }

    @Override
    public int advance(int targetRowID) throws IOException
    {
        int rowId;
        do
        {
            rowId = nextPosting();
        } while (rowId < targetRowID);
        return rowId;
    }
}
