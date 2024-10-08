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

import java.io.IOException;
import java.lang.invoke.MethodHandles;

import com.google.common.base.MoreObjects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.format.IndexComponentType;
import org.apache.cassandra.index.sai.disk.v1.kdtree.BKDReader;
import org.apache.cassandra.index.sai.metrics.MulticastQueryEventListeners;
import org.apache.cassandra.index.sai.metrics.QueryEventListener;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.plan.Orderer;
import org.apache.cassandra.index.sai.utils.AbstractIterator;
import org.apache.cassandra.index.sai.utils.PrimaryKeyWithSortKey;
import org.apache.cassandra.index.sai.utils.RangeIterator;
import org.apache.cassandra.index.sai.utils.RowIdWithByteComparable;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static org.apache.cassandra.index.sai.disk.v1.kdtree.BKDQueries.bkdQueryFrom;

/**
 * Executes {@link Expression}s against the kd-tree for an individual index segment.
 */
public class KDTreeIndexSearcher extends IndexSearcher
{
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final BKDReader bkdReader;
    private final QueryEventListener.BKDIndexEventListener perColumnEventListener;

    KDTreeIndexSearcher(PrimaryKeyMap.Factory primaryKeyMapFactory,
                        PerIndexFiles perIndexFiles,
                        SegmentMetadata segmentMetadata,
                        IndexContext indexContext) throws IOException
    {
        super(primaryKeyMapFactory, perIndexFiles, segmentMetadata, indexContext);

        final long bkdPosition = metadata.getIndexRoot(IndexComponentType.KD_TREE);
        assert bkdPosition >= 0;
        final long postingsPosition = metadata.getIndexRoot(IndexComponentType.KD_TREE_POSTING_LISTS);
        assert postingsPosition >= 0;

        bkdReader = new BKDReader(indexContext,
                                  indexFiles.kdtree(),
                                  bkdPosition,
                                  indexFiles.kdtreePostingLists(),
                                  postingsPosition);
        perColumnEventListener = (QueryEventListener.BKDIndexEventListener)indexContext.getColumnQueryMetrics();
    }

    @Override
    public long indexFileCacheSize()
    {
        return bkdReader.memoryUsage();
    }

    @Override
    public RangeIterator search(Expression exp, AbstractBounds<PartitionPosition> keyRange, QueryContext context, boolean defer, int limit) throws IOException
    {
        PostingList postingList = searchPosting(exp, context);
        return toPrimaryKeyIterator(postingList, context);
    }

    private PostingList searchPosting(Expression exp, QueryContext context)
    {
        if (logger.isTraceEnabled())
            logger.trace(indexContext.logMessage("Searching on expression '{}'..."), exp);

        if (exp.getOp().isEqualityOrRange())
        {
            final BKDReader.IntersectVisitor query = bkdQueryFrom(exp, bkdReader.getNumDimensions(), bkdReader.getBytesPerDimension());
            QueryEventListener.BKDIndexEventListener listener = MulticastQueryEventListeners.of(context, perColumnEventListener);
            return bkdReader.intersect(query, listener, context);
        }
        else
        {
            throw new IllegalArgumentException(indexContext.logMessage("Unsupported expression during index query: " + exp));
        }
    }

    public CloseableIterator<PrimaryKeyWithSortKey> orderBy(Orderer orderer, Expression slice, AbstractBounds<PartitionPosition> keyRange, QueryContext queryContext, int limit) throws IOException
    {
        var query = slice != null && slice.getOp().isEqualityOrRange()
                    ? bkdQueryFrom(slice, bkdReader.getNumDimensions(), bkdReader.getBytesPerDimension())
                    : null;
        var direction = orderer.isAscending() ? BKDReader.Direction.FORWARD : BKDReader.Direction.BACKWARD;
        var iter = new RowIdIterator(bkdReader.iteratorState(direction, query));
        return toMetaSortedIterator(iter, queryContext);
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                          .add("indexContext", indexContext)
                          .add("count", bkdReader.getPointCount())
                          .add("numDimensions", bkdReader.getNumDimensions())
                          .add("bytesPerDimension", bkdReader.getBytesPerDimension())
                          .toString();
    }

    @Override
    public void close()
    {
        bkdReader.close();
    }

    private static class RowIdIterator extends AbstractIterator<RowIdWithByteComparable> implements CloseableIterator<RowIdWithByteComparable>
    {
        private final BKDReader.IteratorState iterator;
        RowIdIterator(BKDReader.IteratorState iterator)
        {
            this.iterator = iterator;
        }

        @Override
        public RowIdWithByteComparable computeNext()
        {
            if (!iterator.hasNext())
                return endOfData();

            var segmentRowId = iterator.next();
            // We have to copy scratch to prevent it from being overwritten by the next call to computeNext()
            var indexValue = new byte[iterator.scratch.length];
            System.arraycopy(iterator.scratch, 0, indexValue, 0, iterator.scratch.length);
            // We store the indexValue in an already encoded format, so we use the preencoded method here
            // to avoid re-encoding it.
            return new RowIdWithByteComparable(Math.toIntExact(segmentRowId),
                                               ByteComparable.preencoded(TypeUtil.BYTE_COMPARABLE_VERSION,
                                                                         indexValue));
        }

        @Override
        public void close()
        {
            FileUtils.closeQuietly(iterator);
        }
    }
}
