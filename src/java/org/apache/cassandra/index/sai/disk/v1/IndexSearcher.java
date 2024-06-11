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
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.disk.IndexSearcherContext;
import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.disk.PostingListRangeIterator;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.plan.Orderer;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.PrimaryKeyWithByteComparable;
import org.apache.cassandra.index.sai.utils.PrimaryKeyWithSortKey;
import org.apache.cassandra.index.sai.utils.PriorityQueueIterator;
import org.apache.cassandra.index.sai.utils.RangeIterator;
import org.apache.cassandra.index.sai.utils.RowIdWithMeta;
import org.apache.cassandra.index.sai.utils.RowIdToPrimaryKeyWithSortKeyIterator;
import org.apache.cassandra.index.sai.utils.SegmentOrdering;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReadsListener;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

/**
 * Abstract reader for individual segments of an on-disk index.
 *
 * Accepts shared resources (token/offset file readers), and uses them to perform lookups against on-disk data
 * structures.
 */
public abstract class IndexSearcher implements Closeable, SegmentOrdering
{
    protected final PrimaryKeyMap.Factory primaryKeyMapFactory;
    final PerIndexFiles indexFiles;
    protected final SegmentMetadata metadata;
    final IndexDescriptor indexDescriptor;
    protected final IndexContext indexContext;

    private static final SSTableReadsListener NOOP_LISTENER = new SSTableReadsListener() {};

    private final ColumnFilter columnFilter;

    protected IndexSearcher(PrimaryKeyMap.Factory primaryKeyMapFactory,
                            PerIndexFiles perIndexFiles,
                            SegmentMetadata segmentMetadata,
                            IndexDescriptor indexDescriptor,
                            IndexContext indexContext)
    {
        this.primaryKeyMapFactory = primaryKeyMapFactory;
        this.indexFiles = perIndexFiles;
        this.metadata = segmentMetadata;
        this.indexDescriptor = indexDescriptor;
        this.indexContext = indexContext;
        columnFilter = ColumnFilter.selection(RegularAndStaticColumns.of(indexContext.getDefinition()));
    }

    /**
     * @return memory usage of underlying on-disk data structure
     */
    public abstract long indexFileCacheSize();

    /**
     * Search on-disk index synchronously
     *
     * @param expression   to filter on disk index
     * @param keyRange     key range specific in read command, used by ANN index
     * @param queryContext to track per sstable cache and per query metrics
     * @param defer        create the iterator in a deferred state
     * @param limit        the num of rows to returned, used by ANN index
     * @return {@link RangeIterator} that matches given expression
     */
    public abstract RangeIterator search(Expression expression, AbstractBounds<PartitionPosition> keyRange, QueryContext queryContext, boolean defer, int limit) throws IOException;

    /**
     * Order the on-disk index synchronously and produce an iterator in score order
     *
     * @param orderer      the object containing the ordering logic
     * @param keyRange     key range specific in read command, used by ANN index
     * @param queryContext to track per sstable cache and per query metrics
     * @param limit        the num of rows to returned, used by ANN index
     * @return an iterator of {@link PrimaryKeyWithSortKey} in score order
     */
    abstract public CloseableIterator<? extends PrimaryKeyWithSortKey> orderBy(Orderer orderer, AbstractBounds<PartitionPosition> keyRange, QueryContext queryContext, int limit) throws IOException;


    @Override
    public CloseableIterator<? extends PrimaryKeyWithSortKey> orderResultsBy(SSTableReader reader, QueryContext context, List<PrimaryKey> keys, Orderer orderer, int limit) throws IOException
    {
        Comparator<PrimaryKeyWithSortKey> comparator = orderer.isAscending()
                                                       ? Comparator.naturalOrder()
                                                       : Comparator.reverseOrder();
        var pq = new PriorityQueue<>(comparator);
        for (var key : keys)
        {
            var slices = Slices.with(indexContext.comparator(), Slice.make(key.clustering()));
            // TODO if we end up needing to read the row still, is it better to store offset and use reader.unfilteredAt?
            try (var iter = reader.iterator(key.partitionKey(), slices, columnFilter, false, NOOP_LISTENER))
            {
                if (iter.hasNext())
                {
                    var row = (Row) iter.next();
                    assert !iter.hasNext();
                    var cell = row.getCell(indexContext.getDefinition());
                    if (cell == null)
                        continue;
                    // We encode the bytes to make sure they compare correctly.
                    var byteComparable = encode(cell.buffer());
                    pq.add(new PrimaryKeyWithByteComparable(indexContext, indexDescriptor.descriptor.id, key, byteComparable));
                }
            }
        }
        return new PriorityQueueIterator<>(pq);
    }

    private ByteComparable encode(ByteBuffer input)
    {
        return indexContext.isLiteral() ? ByteComparable.fixedLength(input)
                                        : v -> TypeUtil.asComparableBytes(input, indexContext.getValidator(), v);
    }

    protected RangeIterator toPrimaryKeyIterator(PostingList postingList, QueryContext queryContext) throws IOException
    {
        if (postingList == null || postingList.size() == 0)
            return RangeIterator.empty();

        IndexSearcherContext searcherContext = new IndexSearcherContext(metadata.minKey,
                                                                        metadata.maxKey,
                                                                        metadata.minSSTableRowId,
                                                                        metadata.maxSSTableRowId,
                                                                        metadata.segmentRowIdOffset,
                                                                        queryContext,
                                                                        postingList.peekable());
        return new PostingListRangeIterator(indexContext, primaryKeyMapFactory.newPerSSTablePrimaryKeyMap(), searcherContext);
    }

    protected CloseableIterator<? extends PrimaryKeyWithSortKey> toMetaSortedIterator(CloseableIterator<? extends RowIdWithMeta> rowIdIterator, QueryContext queryContext) throws IOException
    {
        if (rowIdIterator == null || !rowIdIterator.hasNext())
            return CloseableIterator.emptyIterator();

        IndexSearcherContext searcherContext = new IndexSearcherContext(metadata.minKey,
                                                                        metadata.maxKey,
                                                                        metadata.minSSTableRowId,
                                                                        metadata.maxSSTableRowId,
                                                                        metadata.segmentRowIdOffset,
                                                                        queryContext,
                                                                        null);
        return new RowIdToPrimaryKeyWithSortKeyIterator(indexContext,
                                                        indexDescriptor.descriptor.id,
                                                        rowIdIterator,
                                                        primaryKeyMapFactory.newPerSSTablePrimaryKeyMap(),
                                                        searcherContext);
    }
}
