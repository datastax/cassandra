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

import java.util.NoSuchElementException;

import org.apache.cassandra.index.sai.disk.IndexSearcherContext;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.vector.ScoredRowId;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.CloseableIterator;

/**
 * An iterator over scored primary keys ordered by the score descending
 * Not skippable.
 */
public class ScoredRowIdPrimaryKeyMapIterator implements CloseableIterator<ScoredPrimaryKey>
{
    private final PrimaryKeyMap primaryKeyMap;
    private final CloseableIterator<ScoredRowId> scoredRowIdIterator;
    private final IndexSearcherContext searcherContext;

    public ScoredRowIdPrimaryKeyMapIterator(CloseableIterator<ScoredRowId> scoredRowIdIterator,
                                            PrimaryKeyMap primaryKeyMap,
                                            IndexSearcherContext context)
    {
        this.scoredRowIdIterator = scoredRowIdIterator;
        this.primaryKeyMap = primaryKeyMap;
        this.searcherContext = context;
    }

    @Override
    public boolean hasNext()
    {
        return scoredRowIdIterator.hasNext();
    }

    @Override
    public ScoredPrimaryKey next()
    {
        if (!hasNext())
            throw new NoSuchElementException();
        var scoredRowId = scoredRowIdIterator.next();
        var primaryKey = primaryKeyMap.primaryKeyFromRowId(searcherContext.getSegmentRowIdOffset() + scoredRowId.getSegmentRowId());
        return new ScoredPrimaryKey(primaryKey, scoredRowId.getScore());
    }

    @Override
    public void close()
    {
        FileUtils.closeQuietly(primaryKeyMap);
        FileUtils.closeQuietly(scoredRowIdIterator);
    }
}
