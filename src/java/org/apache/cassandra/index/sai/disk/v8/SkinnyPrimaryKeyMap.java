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

package org.apache.cassandra.index.sai.disk.v8;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.format.IndexComponentType;
import org.apache.cassandra.index.sai.disk.format.IndexComponents;
import org.apache.cassandra.index.sai.disk.v1.LongArray;
import org.apache.cassandra.index.sai.disk.v1.MetadataSource;
import org.apache.cassandra.index.sai.disk.v1.bitpack.BlockPackedReader;
import org.apache.cassandra.index.sai.disk.v1.bitpack.MonotonicBlockPackedReader;
import org.apache.cassandra.index.sai.disk.v1.bitpack.NumericValuesMeta;
import org.apache.cassandra.index.sai.disk.v2.PrimaryKeyWithSource;
import org.apache.cassandra.index.sai.disk.v8.keystore.KeyLookup;
import org.apache.cassandra.index.sai.disk.v8.keystore.KeyLookupMeta;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.io.sstable.SSTableId;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;

/**
 * A {@link PrimaryKeyMap} for skinny tables (those with no clustering columns).
 * <p>
 * This uses the following on-disk structures:
 * <ul>
 *     <li>A block-packed structure for rowId to token lookups using {@link BlockPackedReader}.
 *     Uses the {@link IndexComponentType#TOKEN_VALUES} component</li>
 *     <li>A monotonic block packed structure for rowId to partitionId lookups using {@link MonotonicBlockPackedReader}.
 *     Uses the {@link IndexComponentType#PARTITION_SIZES} component</li>
 *     <li>A key store for rowId to {@link PrimaryKey} and {@link PrimaryKey} to rowId lookups using
 *     {@link KeyLookup}. Uses the {@link IndexComponentType#PARTITION_KEY_BLOCKS} and
 *     {@link IndexComponentType#PARTITION_KEY_BLOCK_OFFSETS} components</li>
 * </ul>
 * <p>
 * While the {@link Factory} is threadsafe, individual instances of the {@link SkinnyPrimaryKeyMap}
 * are not.
 */
@NotThreadSafe
public class SkinnyPrimaryKeyMap implements PrimaryKeyMap
{
    @ThreadSafe
    public static class Factory implements PrimaryKeyMap.Factory
    {
        private final IndexComponents.ForRead perSSTableComponents;
        private final FileHandle tokensFile;
        protected final SSTableId<?> sstableId;
        protected final boolean hasStaticColumns;

        protected final MetadataSource metadataSource;
        protected final LongArray.Factory tokenReaderFactory;
        protected final LongArray.Factory partitionReaderFactory;
        protected final KeyLookup partitionKeyReader;
        protected final IPartitioner partitioner;
        protected final RowAwarePrimaryKeyFactory primaryKeyFactory;

        // private final FileHandle tokensFile;
        private final FileHandle partitionsFile;
        private final FileHandle partitionKeyBlockOffsetsFile;
        private final FileHandle partitionKeyBlocksFile;

        public Factory(IndexComponents.ForRead perSSTableComponents, RowAwarePrimaryKeyFactory primaryKeyFactory, SSTableReader sstable)
        {
            try
            {
                this.perSSTableComponents = perSSTableComponents;
                metadataSource = MetadataSource.loadMetadata(perSSTableComponents);
                NumericValuesMeta tokensMeta = new NumericValuesMeta(metadataSource.get(perSSTableComponents.get(IndexComponentType.TOKEN_VALUES)));
                this.tokensFile = perSSTableComponents.get(IndexComponentType.TOKEN_VALUES).createFileHandle(this::close);
                this.tokenReaderFactory = new BlockPackedReader(tokensFile, tokensMeta);
                NumericValuesMeta partitionsMeta = new NumericValuesMeta(metadataSource.get(perSSTableComponents.get(IndexComponentType.PARTITION_SIZES)));
                this.partitionsFile = perSSTableComponents.get(IndexComponentType.PARTITION_SIZES).createFileHandle(this::close);
                this.partitionReaderFactory = new MonotonicBlockPackedReader(partitionsFile, partitionsMeta);
                NumericValuesMeta partitionKeyBlockOffsetsMeta = new NumericValuesMeta(metadataSource.get(perSSTableComponents.get(IndexComponentType.PARTITION_KEY_BLOCK_OFFSETS)));
                KeyLookupMeta partitionKeysMeta = new KeyLookupMeta(metadataSource.get(perSSTableComponents.get(IndexComponentType.PARTITION_KEY_BLOCKS)));
                this.partitionKeyBlocksFile = perSSTableComponents.get(IndexComponentType.PARTITION_KEY_BLOCKS).createFileHandle(this::close);
                this.partitionKeyBlockOffsetsFile = perSSTableComponents.get(IndexComponentType.PARTITION_KEY_BLOCK_OFFSETS).createFileHandle(this::close);
                this.partitionKeyReader = new KeyLookup(partitionKeyBlocksFile, partitionKeyBlockOffsetsFile, partitionKeysMeta, partitionKeyBlockOffsetsMeta);
                this.partitioner = sstable.metadata().partitioner;
                this.primaryKeyFactory = primaryKeyFactory;
                this.sstableId = sstable.getId();
                this.hasStaticColumns = sstable.metadata().hasStaticColumns();
            }
            catch (Throwable t)
            {
                throw Throwables.unchecked(t);
            }
        }

        @Override
        @SuppressWarnings({ "resource", "RedundantSuppression" })
        public PrimaryKeyMap newPerSSTablePrimaryKeyMap()
        {
            LongArray rowIdToToken = new LongArray.DeferredLongArray(tokenReaderFactory::open);
            LongArray rowIdToPartitionId = new LongArray.DeferredLongArray(partitionReaderFactory::open);
            try
            {
                return new SkinnyPrimaryKeyMap(rowIdToToken,
                                               rowIdToPartitionId,
                                               partitionKeyReader.openCursor(),
                                               partitioner,
                                               primaryKeyFactory,
                                               sstableId,
                                               hasStaticColumns);
            }
            catch (IOException e)
            {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public void close()
        {
            FileUtils.closeQuietly(Arrays.asList(tokensFile, partitionsFile, partitionKeyBlocksFile, partitionKeyBlockOffsetsFile));
        }
    }

    protected final LongArray tokenArray;
    protected final LongArray partitionArray;
    protected final KeyLookup.Cursor partitionKeyCursor;
    protected final IPartitioner partitioner;
    protected final RowAwarePrimaryKeyFactory primaryKeyFactory;
    protected final SSTableId<?> sstableId;
    private final boolean hasStaticColumns;

    protected SkinnyPrimaryKeyMap(LongArray tokenArray,
                                  LongArray partitionArray,
                                  KeyLookup.Cursor partitionKeyCursor,
                                  IPartitioner partitioner,
                                  RowAwarePrimaryKeyFactory primaryKeyFactory,
                                  SSTableId<?> sstableId,
                                  boolean hasStaticColumns)
    {
        this.tokenArray = tokenArray;
        this.partitionArray = partitionArray;
        this.partitionKeyCursor = partitionKeyCursor;
        this.partitioner = partitioner;
        this.primaryKeyFactory = primaryKeyFactory;
        this.sstableId = sstableId;
        this.hasStaticColumns = hasStaticColumns;
    }

    @Override
    public SSTableId<?> getSSTableId()
    {
        return sstableId;
    }

    @Override
    public long count()
    {
        return tokenArray.length();
    }

    @Override
    public PrimaryKey primaryKeyFromRowId(long sstableRowId)
    {
        long token = tokenArray.get(sstableRowId);
        return primaryKeyFactory.createDeferred(partitioner.getTokenFactory().fromLongValue(token), () -> supplier(sstableRowId));
    }

    @Override
    public PrimaryKey primaryKeyFromRowId(long sstableRowId, PrimaryKey lowerBound, PrimaryKey upperBound)
    {
        return hasStaticColumns ? primaryKeyFromRowId(sstableRowId)
                                : primaryKeyFactory.createWithSource(this, sstableRowId, lowerBound, upperBound);
    }

    @Override
    public long exactRowIdOrInvertedCeiling(PrimaryKey key)
    {
        if (key instanceof PrimaryKeyWithSource)
        {
            var pkws = (PrimaryKeyWithSource) key;
            if (pkws.getSourceSstableId().equals(sstableId))
                return pkws.getSourceRowId();
        }
        long rowId = tokenArray.indexOf(key.token().getLongValue());
        if (key.isTokenOnly() || rowId < 0)
            return rowId;
        // The first index might not have been the correct match in the case of token collisions.
        return tokenCeilingCollisionDetection(key, rowId);
    }

    @Override
    public long ceiling(PrimaryKey key)
    {
        if (key instanceof PrimaryKeyWithSource)
        {
            var pkws = (PrimaryKeyWithSource) key;
            if (pkws.getSourceSstableId().equals(sstableId))
                return pkws.getSourceRowId();
        }
        long rowId = tokenArray.ceilingRowId(key.token().getLongValue());
        if (key.isTokenOnly() || rowId < 0)
            return rowId;
        // The first index might not have been the correct match in the case of token collisions.
        return tokenCeilingCollisionDetection(key, rowId);
    }

    @Override
    public long floor(PrimaryKey key)
    {
        if (key instanceof PrimaryKeyWithSource)
        {
            var pkws = (PrimaryKeyWithSource) key;
            if (pkws.getSourceSstableId().equals(sstableId))
                return pkws.getSourceRowId();
        }
        long rowId = tokenArray.floorRowId(key.token().getLongValue());
        if (key.isTokenOnly() || rowId < 0)
            return rowId;
        // The first index might not have been the correct match in the case of token collisions.
        return tokenFloorCollisionDetection(key, rowId);
    }

    @Override
    public void close()
    {
        FileUtils.closeQuietly(Arrays.asList(partitionKeyCursor, tokenArray, partitionArray));
    }

    // Look for token collision by if the adjacent token in the token array matches the
    // current token. If we find a collision, we need to compare the partition key instead.
    protected long tokenCeilingCollisionDetection(PrimaryKey primaryKey, long rowId)
    {
        // Look for collisions while we haven't reached the end of the tokens and the tokens don't collide
        while (rowId + 1 < tokenArray.length() && primaryKey.token().getLongValue() == tokenArray.get(rowId + 1))
        {
            // If we had a collision, then see if the partition key for this row is >= to the lookup partition key
            if (readPartitionKey(rowId).compareTo(primaryKey.partitionKey()) >= 0)
                return rowId;

            rowId++;
        }
        // Note: We would normally expect to get here without going into the while loop
        return rowId;
    }

    protected long tokenFloorCollisionDetection(PrimaryKey primaryKey, long rowId)
    {
        // Look for collisions while we haven't reached the end of the tokens and the tokens don't collide
        while (rowId - 1 >= 0 && primaryKey.token().getLongValue() == tokenArray.get(rowId - 1))
        {
            // If we had a collision, then see if the partition key for this row is >= to the lookup partition key
            if (readPartitionKey(rowId).compareTo(primaryKey.partitionKey()) <= 0)
                return rowId;

            rowId--;
        }
        // Note: We would normally expect to get here without going into the while loop
        return rowId;
    }

    protected PrimaryKey supplier(long sstableRowId)
    {
        return primaryKeyFactory.create(readPartitionKey(sstableRowId), Clustering.EMPTY);
    }

    protected DecoratedKey readPartitionKey(long sstableRowId)
    {
        long partitionId = partitionArray.get(sstableRowId);
        ByteSource.Peekable peekable = ByteSource.peekable(partitionKeyCursor.seekToPointId(partitionId).asComparableBytes(TypeUtil.BYTE_COMPARABLE_VERSION));

        byte[] keyBytes = ByteSourceInverse.getUnescapedBytes(peekable);

        assert keyBytes != null : "Primary key from map did not contain a partition key";

        ByteBuffer decoratedKey = ByteBuffer.wrap(keyBytes);
        return new BufferDecoratedKey(partitioner.getToken(decoratedKey), decoratedKey);
    }
}
