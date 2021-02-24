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
package org.apache.cassandra.index.sai.disk;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.sai.SSTableContext;
import org.apache.cassandra.index.sai.SSTableIndex;
import org.apache.cassandra.index.sai.SSTableQueryContext;
import org.apache.cassandra.index.sai.disk.io.IndexComponents;
import org.apache.cassandra.index.sai.disk.v1.PrimaryKeyMap;
import org.apache.cassandra.index.sai.metrics.ColumnQueryMetrics;
import org.apache.cassandra.index.sai.metrics.QueryEventListener;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.LongArray;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.RangeIterator;
import org.apache.cassandra.index.sai.utils.TypeUtil;

/**
 * Abstract reader for individual segments of an on-disk index.
 *
 * Accepts shared resources (token/offset file readers), and uses them to perform lookups against on-disk data
 * structures.
 */
public abstract class IndexSearcher implements Closeable
{
    private final PrimaryKeyMap primaryKeyMap;
    final SSTableIndex.PerIndexFiles indexFiles;

    final SegmentMetadata metadata;

    final IndexComponents indexComponents;

    IndexSearcher(Segment segment) throws IOException
    {
        this.indexComponents = segment.indexFiles.components();
        this.primaryKeyMap = segment.primaryKeyMap;
        this.indexFiles = segment.indexFiles;
        this.metadata = segment.metadata;
    }

    public static IndexSearcher open(boolean isString, Segment segment, ColumnQueryMetrics listener) throws IOException
    {
        return isString ? open(segment, (QueryEventListener.TrieIndexEventListener) listener)
                        : open(segment, (QueryEventListener.BKDIndexEventListener) listener);
    }

    public static InvertedIndexSearcher open(Segment segment, QueryEventListener.TrieIndexEventListener listener) throws IOException
    {
        return new InvertedIndexSearcher(segment, listener);
    }

    public static KDTreeIndexSearcher open(Segment segment, QueryEventListener.BKDIndexEventListener listener) throws IOException
    {
        return new KDTreeIndexSearcher(segment, listener);
    }

    /**
     * @return number of per-index open files attached to a sstable
     */
    public static int openPerIndexFiles(AbstractType<?> columnType)
    {
        return TypeUtil.isLiteral(columnType) ? InvertedIndexSearcher.openPerIndexFiles() : KDTreeIndexSearcher.openPerIndexFiles();
    }

    /**
     * @return memory usage of underlying on-disk data structure
     */
    public abstract long indexFileCacheSize();

    /**
     * Search on-disk index synchronously.
     *
     * @param expression to filter on disk index
     * @param queryContext to track per sstable cache and per query metrics
     *
     * @return {@link RangeIterator} that matches given expression
     */
    public abstract List<RangeIterator> search(Expression expression, SSTableQueryContext queryContext);

    List<RangeIterator> toIterators(List<PostingList.PeekablePostingList> postingLists, SSTableQueryContext queryContext)
    {
        if (postingLists == null || postingLists.isEmpty())
            return Collections.EMPTY_LIST;

        List<RangeIterator> iterators = new ArrayList<>();

        for (PostingList.PeekablePostingList postingList : postingLists)
        {
            SearcherContext searcherContext = new SearcherContext(queryContext, postingList);
            if (!searcherContext.noOverlap)
                iterators.add(new PostingListRangeIterator(searcherContext, indexComponents));
        }

        return iterators;
    }

    public class SearcherContext
    {
        PrimaryKey minimumKey;
        PrimaryKey maximumKey;
        long maxPartitionOffset;
        boolean noOverlap;
        final SSTableQueryContext context;
        final PostingList.PeekablePostingList postingList;
        final PrimaryKeyMap primaryKeyMap;

        SearcherContext(SSTableQueryContext context, PostingList.PeekablePostingList postingList)
        {
            this.context = context;
            this.postingList = postingList;
            this.primaryKeyMap = IndexSearcher.this.primaryKeyMap.copyOf();

            minimumKey = this.primaryKeyMap.primaryKeyFromRowId(postingList.peek());

            // use segment's metadata for the range iterator, may not be accurate, but should not matter to performance.
            maximumKey = metadata.maxKey;

            maxPartitionOffset = Long.MAX_VALUE;
        }

        long count()
        {
            return postingList.size();
        }
    }
}
