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

package org.apache.cassandra.index.sai.disk.v1dse;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import com.google.common.collect.ImmutableList;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.virtual.SimpleDataSet;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.SSTableContext;
import org.apache.cassandra.index.sai.disk.SSTableIndex;
import org.apache.cassandra.index.sai.disk.v1dse.segment.Segment;
import org.apache.cassandra.index.sai.disk.v1dse.segment.SegmentMetadata;
import org.apache.cassandra.index.sai.iterators.KeyRangeIterator;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.Throwables;

public class DSELegacySSTableIndex extends SSTableIndex
{
    private final ImmutableList<Segment> segments;
    private final List<SegmentMetadata> metadatas;
    private final AbstractBounds<PartitionPosition> bounds;
    private final ByteBuffer minTerm;
    private final ByteBuffer maxTerm;
    private final long minSSTableRowId, maxSSTableRowId;
    private final long numRows;

    private PerColumnIndexFiles indexFiles;

    public DSELegacySSTableIndex(SSTableContext sstableContext, IndexContext indexContext)
    {
        super(sstableContext, indexContext);

        try
        {
            this.indexFiles = new PerColumnIndexFiles(sstableContext.indexDescriptor, indexContext);

            ImmutableList.Builder<Segment> segmentsBuilder = ImmutableList.builder();

            final MetadataSource source = MetadataSource.loadColumnMetadata(sstableContext.indexDescriptor, indexContext);

            metadatas = SegmentMetadata.load(source, sstableContext.indexDescriptor.primaryKeyFactory);

            for (SegmentMetadata metadata : metadatas)
            {
                segmentsBuilder.add(new Segment(indexContext, sstableContext, indexFiles, metadata));
            }

            segments = segmentsBuilder.build();
            assert !segments.isEmpty();

            DecoratedKey minKey = metadatas.get(0).minKey.partitionKey();
            DecoratedKey maxKey = metadatas.get(metadatas.size() - 1).maxKey.partitionKey();

            this.bounds = AbstractBounds.bounds(minKey, true, maxKey, true);

            this.minTerm = metadatas.stream().map(m -> m.minTerm).min(TypeUtil.comparator(indexContext.getValidator())).orElse(null);
            this.maxTerm = metadatas.stream().map(m -> m.maxTerm).max(TypeUtil.comparator(indexContext.getValidator())).orElse(null);

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
        return 0;
    }

    @Override
    public long getRowCount()
    {
        return 0;
    }

    @Override
    public long minSSTableRowId()
    {
        return 0;
    }

    @Override
    public long maxSSTableRowId()
    {
        return 0;
    }

    @Override
    public ByteBuffer minTerm()
    {
        return null;
    }

    @Override
    public ByteBuffer maxTerm()
    {
        return null;
    }

    @Override
    public AbstractBounds<PartitionPosition> bounds()
    {
        return null;
    }

    @Override
    public List<KeyRangeIterator> search(Expression expression, AbstractBounds<PartitionPosition> keyRange, QueryContext context) throws IOException
    {
        return null;
    }

    @Override
    public void populateSegmentView(SimpleDataSet dataSet)
    {

    }

    @Override
    protected void internalRelease()
    {

    }
}
