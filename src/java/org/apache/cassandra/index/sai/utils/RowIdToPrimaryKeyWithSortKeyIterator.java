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

import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.disk.IndexSearcherContext;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.io.sstable.SSTableId;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.CloseableIterator;

/**
 * An iterator over scored primary keys ordered by the score descending
 * Not skippable.
 */
public class RowIdToPrimaryKeyWithSortKeyIterator extends AbstractIterator<PrimaryKeyWithSortKey>
{
    private final IndexContext indexContext;
    private final SSTableId<?> sstableId;
    private final PrimaryKeyMap primaryKeyMap;
    private final CloseableIterator<? extends RowIdWithMeta> scoredRowIdIterator;
    private final IndexSearcherContext searcherContext;

    public RowIdToPrimaryKeyWithSortKeyIterator(IndexContext indexContext,
                                                SSTableId<?> sstableId,
                                                CloseableIterator<? extends RowIdWithMeta> scoredRowIdIterator,
                                                PrimaryKeyMap primaryKeyMap,
                                                IndexSearcherContext context)
    {
        this.indexContext = indexContext;
        this.sstableId = sstableId;
        this.scoredRowIdIterator = scoredRowIdIterator;
        this.primaryKeyMap = primaryKeyMap;
        this.searcherContext = context;
    }

    @Override
    protected PrimaryKeyWithSortKey computeNext()
    {
        if (!scoredRowIdIterator.hasNext())
            return endOfData();
        var rowIdWithMeta = scoredRowIdIterator.next();
        return rowIdWithMeta.buildPrimaryKeyWithSortKey(indexContext, sstableId, primaryKeyMap, searcherContext.getSegmentRowIdOffset());
    }

    @Override
    public void close()
    {
        FileUtils.closeQuietly(primaryKeyMap);
        FileUtils.closeQuietly(scoredRowIdIterator);
    }
}
