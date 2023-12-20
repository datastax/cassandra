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

package org.apache.cassandra.index.sai.disk.v2cc;

import java.io.IOException;
import java.nio.ByteBuffer;
import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v2cc.bitpack.BlockPackedReader;
import org.apache.cassandra.index.sai.disk.v2cc.bitpack.NumericValuesMeta;
import org.apache.cassandra.index.sai.disk.v2cc.sortedterms.SortedTermsMeta;
import org.apache.cassandra.index.sai.disk.v2cc.sortedterms.SortedTermsReader;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;

/**
 * A row-aware {@link PrimaryKeyMap}
 *
 * This uses the following on-disk structures:
 * <ul>
 *     <li>Block-packed structure for rowId to token lookups using {@link BlockPackedReader}.
 *     Uses component {@link IndexComponent#TOKEN_VALUES} </li>
 *     <li>A sorted-terms structure for rowId to {@link PrimaryKey} and {@link PrimaryKey} to rowId lookups using
 *     {@link SortedTermsReader}. Uses components {@link IndexComponent#PRIMARY_KEY_TRIE}, {@link IndexComponent#PRIMARY_KEY_BLOCKS},
 *     {@link IndexComponent#PRIMARY_KEY_BLOCK_OFFSETS}</li>
 * </ul>
 *
 * While the {@link RowAwarePrimaryKeyMapFactory} is threadsafe, individual instances of the {@link RowAwarePrimaryKeyMap}
 * are not.
 */
@NotThreadSafe
public class RowAwarePrimaryKeyMap implements PrimaryKeyMap
{
    @ThreadSafe
    public static class RowAwarePrimaryKeyMapFactory implements Factory
    {
        private final LongArray.Factory tokenReaderFactory;
        private final SortedTermsReader sortedTermsReader;
        private final FileHandle token;
        private final FileHandle termsDataBlockOffsets;
        private final FileHandle termsData;
        private final FileHandle termsTrie;
        private final IPartitioner partitioner;
        private final ClusteringComparator clusteringComparator;
        private final PrimaryKey.Factory primaryKeyFactory;

        public RowAwarePrimaryKeyMapFactory(IndexDescriptor indexDescriptor, SSTableReader sstable)
        {
            this.token = indexDescriptor.createPerSSTableFileHandle(IndexComponent.TOKEN_VALUES, this::close);
            this.termsDataBlockOffsets = indexDescriptor.createPerSSTableFileHandle(IndexComponent.PRIMARY_KEY_BLOCK_OFFSETS, this::close);
            this.termsData = indexDescriptor.createPerSSTableFileHandle(IndexComponent.PRIMARY_KEY_BLOCKS, this::close);
            this.termsTrie = indexDescriptor.createPerSSTableFileHandle(IndexComponent.PRIMARY_KEY_TRIE, this::close);
            try
            {
                MetadataSource metadataSource = MetadataSource.loadGroupMetadata(indexDescriptor);
                NumericValuesMeta tokensMeta = new NumericValuesMeta(metadataSource.get(indexDescriptor.componentName(IndexComponent.TOKEN_VALUES)));
                SortedTermsMeta sortedTermsMeta = new SortedTermsMeta(metadataSource.get(indexDescriptor.componentName(IndexComponent.PRIMARY_KEY_BLOCKS)));
                NumericValuesMeta blockOffsetsMeta = new NumericValuesMeta(metadataSource.get(indexDescriptor.componentName(IndexComponent.PRIMARY_KEY_BLOCK_OFFSETS)));

                this.tokenReaderFactory = new BlockPackedReader(token, tokensMeta);
                this.sortedTermsReader = new SortedTermsReader(termsData, termsDataBlockOffsets, termsTrie, sortedTermsMeta, blockOffsetsMeta);
                this.partitioner = sstable.metadata().partitioner;
                this.primaryKeyFactory = indexDescriptor.primaryKeyFactory;
                this.clusteringComparator = indexDescriptor.clusteringComparator;
            }
            catch (Throwable t)
            {
                throw Throwables.unchecked(t);
            }
        }

        @Override
        public PrimaryKeyMap newPerSSTablePrimaryKeyMap() throws IOException
        {
            final LongArray rowIdToToken = new LongArray.DeferredLongArray(() -> tokenReaderFactory.open());
            return new RowAwarePrimaryKeyMap(rowIdToToken,
                                             sortedTermsReader,
                                             sortedTermsReader.openCursor(),
                                             partitioner,
                                             primaryKeyFactory,
                                             clusteringComparator);
        }

        @Override
        public void close()
        {
            FileUtils.closeQuietly(token, termsData, termsDataBlockOffsets, termsTrie);
        }
    }

    private final LongArray rowIdToToken;
    private final SortedTermsReader sortedTermsReader;
    private final SortedTermsReader.Cursor cursor;
    private final IPartitioner partitioner;
    private final PrimaryKey.Factory primaryKeyFactory;
    private final ClusteringComparator clusteringComparator;

    private RowAwarePrimaryKeyMap(LongArray rowIdToToken,
                                  SortedTermsReader sortedTermsReader,
                                  SortedTermsReader.Cursor cursor,
                                  IPartitioner partitioner,
                                  PrimaryKey.Factory primaryKeyFactory,
                                  ClusteringComparator clusteringComparator)
    {
        this.rowIdToToken = rowIdToToken;
        this.sortedTermsReader = sortedTermsReader;
        this.cursor = cursor;
        this.partitioner = partitioner;
        this.primaryKeyFactory = primaryKeyFactory;
        this.clusteringComparator = clusteringComparator;
    }

    @Override
    public PrimaryKey primaryKeyFromRowId(long sstableRowId)
    {
        return loadPrimaryKey(sstableRowId);
    }

    @Override
    public long rowIdFromPrimaryKey(PrimaryKey key)
    {
        return sortedTermsReader.getPointId(key);
    }

    @Override
    public void close()
    {
        FileUtils.closeQuietly(cursor, rowIdToToken);
    }

    private PrimaryKey loadPrimaryKey(long sstableRowId)
    {
        try
        {
            cursor.seekToPointId(sstableRowId);
            ByteSource.Peekable peekable = cursor.term().asPeekableBytes(ByteComparable.Version.OSS41);

            Token token = partitioner.getTokenFactory().fromComparableBytes(ByteSourceInverse.nextComponentSource(peekable),
                                                                            ByteComparable.Version.OSS41);
            byte[] keyBytes = ByteSourceInverse.getUnescapedBytes(ByteSourceInverse.nextComponentSource(peekable));

            if (keyBytes == null)
                return primaryKeyFactory.create(token);

            DecoratedKey partitionKey = new BufferDecoratedKey(token, ByteBuffer.wrap(keyBytes));

            if (clusteringComparator.size() == 0)
                return primaryKeyFactory.create(partitionKey);
            else
                return primaryKeyFactory.create(partitionKey,
                                                clusteringComparator.clusteringFromByteComparable(ByteBufferAccessor.instance,
                                                                                                  v -> ByteSourceInverse.nextComponentSource(peekable)));
        }
        catch (IOException e)
        {
            throw Throwables.cleaned(e);
        }
    }
}
