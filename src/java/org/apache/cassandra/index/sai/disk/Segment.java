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
import java.util.List;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;

import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.sai.ColumnContext;
import org.apache.cassandra.index.sai.SSTableContext;
import org.apache.cassandra.index.sai.SSTableIndex;
import org.apache.cassandra.index.sai.SSTableQueryContext;
import org.apache.cassandra.index.sai.disk.v1.PrimaryKeyMap;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.LongArray;
import org.apache.cassandra.index.sai.utils.RangeIterator;
import org.apache.cassandra.io.util.FileUtils;

/**
 * Each segment represents an on-disk index structure (kdtree/terms/postings) flushed by memory limit or token boundaries,
 * or max segment rowId limit, because of lucene's limitation on 2B(Integer.MAX_VALUE). It also helps to reduce resource
 * consumption for read requests as only segments that intersect with read request data range need to be loaded.
 */
public class Segment implements Closeable
{
    private final Token minKey;
    private final Token.KeyBound minKeyBound;
    private final Token maxKey;
    private final Token.KeyBound maxKeyBound;

    // per sstable
    public final PrimaryKeyMap primaryKeyMap;
    // per-index
    public final SSTableIndex.PerIndexFiles indexFiles;
    // per-segment
    public final SegmentMetadata metadata;

    private final IndexSearcher index;
    private final AbstractType<?> columnType;

    public Segment(ColumnContext columnContext, SSTableContext sstableContext, SSTableIndex.PerIndexFiles indexFiles, SegmentMetadata metadata) throws IOException
    {
        this.minKey = metadata.minKey.partitionKey().getToken();
        this.minKeyBound = minKey.minKeyBound();
        this.maxKey = metadata.maxKey.partitionKey().getToken();
        this.maxKeyBound = maxKey.maxKeyBound();

        this.primaryKeyMap = sstableContext.primaryKeyMap;
        this.indexFiles = indexFiles;
        this.metadata = metadata;
        this.columnType = columnContext.getValidator();

        this.index = IndexSearcher.open(columnContext.isLiteral(), this, columnContext.getColumnQueryMetrics());
    }

    @VisibleForTesting
    public Segment(PrimaryKeyMap primaryKeyMap, SSTableIndex.PerIndexFiles indexFiles, SegmentMetadata metadata, AbstractType<?> columnType)
    {
        this.primaryKeyMap = primaryKeyMap;
        this.indexFiles = indexFiles;
        this.metadata = metadata;
        this.columnType = columnType;
        this.minKey = null;
        this.minKeyBound = null;
        this.maxKey = null;
        this.maxKeyBound = null;
        this.index = null;
    }

    @VisibleForTesting
    public Segment(Token minKey, Token maxKey)
    {
        this.primaryKeyMap = null;
        this.indexFiles = null;
        this.metadata = null;
        this.minKey = minKey;
        this.minKeyBound = minKey.minKeyBound();
        this.maxKey = maxKey;
        this.maxKeyBound = maxKey.maxKeyBound();
        this.columnType = null;
        this.index = null;
    }

    /**
     * @return true if current segment intersects with query key range
     */
    public boolean intersects(AbstractBounds<PartitionPosition> keyRange)
    {
        if (keyRange instanceof Range && ((Range<?>)keyRange).isWrapAround())
            return keyRange.contains(minKeyBound) || keyRange.contains(maxKeyBound);

        int cmp = keyRange.right.getToken().compareTo(minKey);
        // if right is minimum, it means right is the max token and bigger than maxKey.
        // if right bound is less than minKey, no intersection
        if (!keyRange.right.isMinimum() && (!keyRange.inclusiveRight() && cmp == 0 || cmp < 0))
            return false;

        cmp = keyRange.left.getToken().compareTo(maxKey);
        // if left bound is bigger than maxKey, no intersection
        if (!keyRange.isStartInclusive() && cmp == 0 || cmp > 0)
            return false;

        return true;
    }

    public long indexFileCacheSize()
    {
        return index == null ? 0 : index.indexFileCacheSize();
    }

    /**
     * Search on-disk index synchronously
     *
     * @param expression to filter on disk index
     * @param context to track per sstable cache and per query metrics
     * @return range iterator that matches given expression
     */
    public List<RangeIterator> search(Expression expression, SSTableQueryContext context) throws IOException
    {
        return index.search(expression, context);
    }

    public AbstractType<?> getColumnType()
    {
        return columnType;
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
}
