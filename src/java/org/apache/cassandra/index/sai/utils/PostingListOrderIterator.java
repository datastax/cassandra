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

package org.apache.cassandra.index.sai.utils;

import org.apache.cassandra.index.sai.disk.IndexSearcherContext;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.io.util.FileUtils;

/**
 * An iterator over scored primary keys ordered by the score descending (maybe that could change?)
 * Not skippable.
 */
public class PostingListOrderIterator extends OrderIterator
{
    private final PrimaryKeyMap primaryKeyMap;
    private final ScoredRowIdIterator scoredRowIdIterator;
    private final IndexSearcherContext searcherContext;

    public PostingListOrderIterator(ScoredRowIdIterator scoredRowIdIterator, PrimaryKeyMap primaryKeyMap, IndexSearcherContext context)
    {
        this.scoredRowIdIterator = scoredRowIdIterator;
        this.primaryKeyMap = primaryKeyMap;
        this.searcherContext = context;
    }

    @Override
    public void close()
    {
        FileUtils.closeQuietly(primaryKeyMap);
        FileUtils.closeQuietly(scoredRowIdIterator);
    }

    @Override
    protected ScoredPrimaryKey computeNext()
    {
        if (!scoredRowIdIterator.hasNext())
        {
            System.out.println("PostingListOrderIterator.computeNext() scoredRowIdIterator.hasNext() is false");
            return endOfData();
        }
        var scoredRowId = scoredRowIdIterator.next();
        var primaryKey = primaryKeyMap.primaryKeyFromRowId(searcherContext.getSegmentRowIdOffset() + scoredRowId.segmentRowId);
        return new ScoredPrimaryKey(primaryKey, scoredRowId.score);
    }
}
