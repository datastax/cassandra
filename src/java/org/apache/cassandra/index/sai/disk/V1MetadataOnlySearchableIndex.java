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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.virtual.SimpleDataSet;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.SSTableContext;
import org.apache.cassandra.index.sai.disk.format.IndexComponents;
import org.apache.cassandra.index.sai.disk.v1.MetadataSource;
import org.apache.cassandra.index.sai.disk.v1.PerIndexFiles;
import org.apache.cassandra.index.sai.disk.v1.Segment;
import org.apache.cassandra.index.sai.disk.v1.SegmentMetadata;
import org.apache.cassandra.index.sai.iterators.KeyRangeIterator;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.plan.Orderer;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.PrimaryKeyWithSortKey;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.Throwables;

/**
 * An index that eagerly loads segment metadata and nothing else. It is currently only used for vector indexes to
 * read PQ files during compaction.
 */
public class V1MetadataOnlySearchableIndex implements SearchableIndex
{
    private final List<SegmentMetadata> metadatas;
    private final DecoratedKey minKey;
    private final DecoratedKey maxKey; // in token order
    private final ByteBuffer minTerm;
    private final ByteBuffer maxTerm;
    private final long minSSTableRowId, maxSSTableRowId;
    private final long numRows;
    private PerIndexFiles indexFiles;

    public V1MetadataOnlySearchableIndex(SSTableContext sstableContext, IndexComponents.ForRead perIndexComponents)
    {
        var indexContext = perIndexComponents.context();
        try
        {
            this.indexFiles = new PerIndexFiles(perIndexComponents);

            final MetadataSource source = MetadataSource.loadMetadata(perIndexComponents);

            // We skip loading the terms distribution becuase this class doesn't use them for now.
            metadatas = SegmentMetadata.load(source, indexContext, false);

            this.minKey = metadatas.get(0).minKey.partitionKey();
            this.maxKey = metadatas.get(metadatas.size() - 1).maxKey.partitionKey();

            var version = perIndexComponents.version();
            this.minTerm = metadatas.stream().map(m -> m.minTerm).min(TypeUtil.comparator(indexContext.getValidator(), version)).orElse(null);
            this.maxTerm = metadatas.stream().map(m -> m.maxTerm).max(TypeUtil.comparator(indexContext.getValidator(), version)).orElse(null);

            this.numRows = metadatas.stream().mapToLong(m -> m.numRows).sum();

            this.minSSTableRowId = metadatas.get(0).minSSTableRowId;
            this.maxSSTableRowId = metadatas.get(metadatas.size() - 1).maxSSTableRowId;
        }
        catch (Throwable t)
        {
            FileUtils.closeQuietly(indexFiles);
            FileUtils.closeQuietly(sstableContext);
            throw Throwables.unchecked(t);
        }
    }

    @Override
    public long indexFileCacheSize()
    {
        // TODO what is the right value here?
        return 0;
    }

    @Override
    public long getRowCount()
    {
        return numRows;
    }

    @Override
    public long minSSTableRowId()
    {
        return minSSTableRowId;
    }
    @Override
    public long maxSSTableRowId()
    {
        return maxSSTableRowId;
    }

    @Override
    public ByteBuffer minTerm()
    {
        return minTerm;
    }

    @Override
    public ByteBuffer maxTerm()
    {
        return maxTerm;
    }

    @Override
    public DecoratedKey minKey()
    {
        return minKey;
    }

    @Override
    public DecoratedKey maxKey()
    {
        return maxKey;
    }

    @Override
    public KeyRangeIterator search(Expression expression,
                                   AbstractBounds<PartitionPosition> keyRange,
                                   QueryContext context,
                                   boolean defer,
                                   int limit) throws IOException
    {
        // This index is not meant for searching, only for accessing metadata and index files
        throw new UnsupportedOperationException();
    }

    @Override
    public List<CloseableIterator<PrimaryKeyWithSortKey>> orderBy(Orderer orderer,
                                                                  Expression slice,
                                                                  AbstractBounds<PartitionPosition> keyRange,
                                                                  QueryContext context,
                                                                  int limit,
                                                                  long totalRows) throws IOException
    {
        // This index is not meant for searching, only for accessing metadata and index files
        throw new UnsupportedOperationException();
    }

    @Override
    public List<Segment> getSegments()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<SegmentMetadata> getSegmentMetadatas()
    {
        return metadatas;
    }

    public PerIndexFiles indexFiles()
    {
        return indexFiles;
    }

    @Override
    public void populateSystemView(SimpleDataSet dataSet, SSTableReader sstable)
    {
        // TODO what is valid here?
    }

    @Override
    public long estimateMatchingRowsCount(Expression predicate, AbstractBounds<PartitionPosition> keyRange)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws IOException
    {
        FileUtils.closeQuietly(indexFiles);
    }

    @Override
    public List<CloseableIterator<PrimaryKeyWithSortKey>> orderResultsBy(QueryContext context, List<PrimaryKey> keys, Orderer orderer, int limit, long totalRows) throws IOException
    {
        throw new UnsupportedOperationException();
    }
}

