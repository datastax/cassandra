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
package org.apache.cassandra.index.sai.disk.v1;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;

import org.slf4j.Logger;

import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.SSTableContext;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.v2.V2VectorIndexSearcher;
import org.apache.cassandra.index.sai.disk.v3.V3OnDiskFormat;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.plan.Orderer;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.RangeIterator;
import org.apache.cassandra.index.sai.utils.PrimaryKeyWithSortKey;
import org.apache.cassandra.index.sai.utils.RangeUtil;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.CloseableIterator;

/**
 * Each segment represents an on-disk index structure (kdtree/terms/postings) flushed by memory limit or token boundaries,
 * or max segment rowId limit, because of lucene's limitation on 2B(Integer.MAX_VALUE). It also helps to reduce resource
 * consumption for read requests as only segments that intersect with read request data range need to be loaded.
 */
public class Segment implements Closeable
{
    private static final Logger logger = org.slf4j.LoggerFactory.getLogger(Segment.class);

    private final Token.KeyBound minKeyBound;
    private final Token.KeyBound maxKeyBound;

    // per sstable
    final PrimaryKeyMap.Factory primaryKeyMapFactory;
    // per-index
    public final PerIndexFiles indexFiles;
    // per-segment
    public final SegmentMetadata metadata;
    public final SSTableContext sstableContext;

    private final IndexSearcher index;

    public Segment(IndexContext indexContext, SSTableContext sstableContext, PerIndexFiles indexFiles, SegmentMetadata metadata) throws IOException
    {
        this.minKeyBound = metadata.minKey.token().minKeyBound();
        this.maxKeyBound = metadata.maxKey.token().maxKeyBound();

        this.sstableContext = sstableContext;
        this.primaryKeyMapFactory = sstableContext.primaryKeyMapFactory();
        this.indexFiles = indexFiles;
        this.metadata = metadata;

        var version = indexFiles.usedPerIndexComponents().version();
        IndexSearcher searcher = version.onDiskFormat().newIndexSearcher(sstableContext, indexContext, indexFiles, metadata);
        logger.info("Opened searcher {} for segment {}:{} for index [{}] on column [{}] at version {}",
                    searcher.getClass().getSimpleName(),
                    sstableContext.descriptor(),
                    metadata.segmentRowIdOffset,
                    indexContext.getIndexName(),
                    indexContext.getColumnName(),
                    version);
        this.index = searcher;
    }

    @VisibleForTesting
    public Segment(PrimaryKeyMap.Factory primaryKeyMapFactory,
                   PerIndexFiles indexFiles,
                   SegmentMetadata metadata,
                   AbstractType<?> columnType)
    {
        this.primaryKeyMapFactory = primaryKeyMapFactory;
        this.indexFiles = indexFiles;
        this.metadata = metadata;
        this.minKeyBound = null;
        this.maxKeyBound = null;
        this.index = null;
        this.sstableContext = null;
    }

    @VisibleForTesting
    public Segment(Token minKey, Token maxKey)
    {
        this.primaryKeyMapFactory = null;
        this.indexFiles = null;
        this.metadata = null;
        this.minKeyBound = minKey.minKeyBound();
        this.maxKeyBound = maxKey.maxKeyBound();
        this.index = null;
        this.sstableContext = null;
    }

    /**
     * @return true if current segment intersects with query key range
     */
    public boolean intersects(AbstractBounds<PartitionPosition> keyRange)
    {
        return RangeUtil.intersects(minKeyBound, maxKeyBound, keyRange);
    }

    public long indexFileCacheSize()
    {
        return index == null ? 0 : index.indexFileCacheSize();
    }

    /**
     * Search on-disk index synchronously
     *
     * @param expression to filter on disk index
     * @param keyRange   key range specific in read command, used by ANN index
     * @param context    to track per sstable cache and per query metrics
     * @param defer      create the iterator in a deferred state
     * @param limit      the num of rows to returned, used by ANN index
     * @return range iterator of {@link PrimaryKey} that matches given expression
     */
    public RangeIterator search(Expression expression, AbstractBounds<PartitionPosition> keyRange, QueryContext context, boolean defer, int limit) throws IOException
    {
        return index.search(expression, keyRange, context, defer, limit);
    }

    /**
     * Order the on-disk index synchronously and produce an iterator in score order
     *
     * @param orderer    to filter on disk index
     * @param keyRange   key range specific in read command, used by ANN index
     * @param context    to track per sstable cache and per query metrics
     * @param limit      the num of rows to returned, used by ANN index
     * @return an iterator of {@link PrimaryKeyWithSortKey} in score order
     */
    public CloseableIterator<PrimaryKeyWithSortKey> orderBy(Orderer orderer, Expression slice, AbstractBounds<PartitionPosition> keyRange, QueryContext context, int limit) throws IOException
    {
        return index.orderBy(orderer, slice, keyRange, context, limit);
    }

    public IndexSearcher getIndexSearcher()
    {
        return index;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Segment segment = (Segment) o;
        return Objects.equal(metadata, segment.metadata);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(metadata);
    }

    public CloseableIterator<PrimaryKeyWithSortKey> orderResultsBy(QueryContext context, List<PrimaryKey> keys, Orderer orderer, int limit) throws IOException
    {
        return index.orderResultsBy(sstableContext.sstable, context, keys, orderer, limit);
    }

    @Override
    public void close()
    {
        FileUtils.closeQuietly(index);
    }

    @Override
    public String toString()
    {
        return String.format("Segment{metadata=%s}", metadata);
    }

    /**
     * Estimate how many nodes the index will visit to find the top `limit` results
     * given the number of candidates that match other predicates and taking into
     * account the size of the index itself.  (The smaller
     * the number of candidates, the more nodes we expect to visit just to find
     * results that are in that set.)
     */
    public int estimateAnnNodesVisited(int limit, int candidates)
    {
        IndexSearcher searcher = getIndexSearcher();
        assert searcher instanceof V2VectorIndexSearcher : searcher;
        return ((V2VectorIndexSearcher) searcher).estimateNodesVisited(limit, candidates);
    }

    /**
     * Returns a modified LIMIT (top k) to use with the ANN index that is proportional
     * to the number of rows in this segment, relative to the total rows in the sstable.
     */
    public int proportionalAnnLimit(int limit, long totalRows)
    {
        if (!V3OnDiskFormat.REDUCE_TOPK_ACROSS_SSTABLES)
            return limit;

        // Note: it is tempting to think that we should max out results for the first segment
        // since that's where we're establishing our rerank floor.  This *does* reduce the number
        // of calls to resume, but it's 10-15% slower overall, so don't do it.
        // if (context.getAnnRerankFloor() == 0 && V3OnDiskFormat.ENABLE_RERANK_FLOOR)
        //    return limit;

        // We expect the number of top results found in each segment to be proportional to its number of rows.
        // (We don't pad this number more because resuming a search if we guess too low is very very inexpensive.)
        long segmentRows = 1 + metadata.maxSSTableRowId - metadata.minSSTableRowId;
        int proportionalLimit = (int) Math.ceil(limit * ((double) segmentRows / totalRows));
        assert proportionalLimit >= 1 : proportionalLimit;
        return proportionalLimit;
    }
}
