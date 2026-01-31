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

package org.apache.cassandra.index.sai.disk.v2;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.format.IndexComponentType;
import org.apache.cassandra.index.sai.disk.format.IndexComponents;
import org.apache.cassandra.index.sai.disk.v1.LongArray;
import org.apache.cassandra.index.sai.disk.v1.bitpack.NumericValuesMeta;
import org.apache.cassandra.index.sai.disk.v2.keystore.KeyLookup;
import org.apache.cassandra.index.sai.disk.v2.keystore.KeyLookupMeta;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.io.sstable.SSTableId;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

/**
 * An extension of the {@link SkinnyPrimaryKeyMap} for wide tables (those with clustering columns).
 * <p>
 * This used the following additional on-disk structures to the {@link SkinnyPrimaryKeyMap}
 * <ul>
 *     <li>A key store for rowId to {@link Clustering} and {@link Clustering} to rowId lookups using
 *     {@link KeyLookup}. Uses the {@link org.apache.cassandra.index.sai.disk.format.IndexComponentType#CLUSTERING_KEY_BLOCKS} and
 *     {@link org.apache.cassandra.index.sai.disk.format.IndexComponentType#CLUSTERING_KEY_BLOCK_OFFSETS} components</li>
 * </ul>
 * While the {@link Factory} is threadsafe, individual instances of the {@link WidePrimaryKeyMap}
 * are not.
 */
@NotThreadSafe
public class WidePrimaryKeyMap extends SkinnyPrimaryKeyMap
{
    private final ClusteringComparator clusteringComparator;
    private final KeyLookup.Cursor clusteringKeyCursor;

    private WidePrimaryKeyMap(LongArray tokenArray,
                              LongArray partitionArray, KeyLookup.Cursor partitionKeyCursor,
                              KeyLookup.Cursor clusteringKeyCursor, IPartitioner partitioner, RowAwarePrimaryKeyFactory primaryKeyFactory,
                              ClusteringComparator clusteringComparator, SSTableId<?> sstableId, boolean hasStaticColumns)
    {
        super(tokenArray, partitionArray, partitionKeyCursor, partitioner, primaryKeyFactory,
              sstableId, hasStaticColumns);

        this.clusteringComparator = clusteringComparator;
        this.clusteringKeyCursor = clusteringKeyCursor;
    }

    /**
     * Returns a row Id for a {@link PrimaryKey}. If there is no such term, returns the `-(next row id) - 1` where `next row id` is the row id of the next greatest {@link PrimaryKey} in the map.
     *
     * @param key the {@link PrimaryKey} to lookup
     * @return a row id
     */
    @Override
    public long exactRowIdOrInvertedCeiling(PrimaryKey key)
    {
        if (key instanceof PrimaryKeyWithSource)
        {
            PrimaryKeyWithSource pkws = (PrimaryKeyWithSource) key;
            if (pkws.getSourceSstableId().equals(sstableId))
                return pkws.getSourceRowId();
        }

        // Find the partition using the token array for initial lookup
        long rowId = tokenArray.indexOf(key.token().getLongValue());
        if (key.isTokenOnly() || rowId < 0)
            return rowId;
        // If we have skipped a token (shouldn't happen with indexOf, but check for safety)
        if (tokenArray.get(rowId) != key.token().getLongValue())
            return rowId;

        // Handle token collisions by comparing partition keys using partitionKeyCursor
        rowId = tokenCeilingCollisionDetection(key, rowId);

        // Now search within the partition for the clustering key
        long clusteringRowId = clusteringKeyCursor.clusteredSeekToKey(
        clusteringComparator.asByteComparable(key.clustering()), rowId, startOfNextPartition(rowId));

        // clusteredSeekToKey returns the ceiling (next greater or equal key) or -1 if not found
        if (clusteringRowId < 0)
            return Long.MIN_VALUE;
        assert clusteringRowId < tokenArray.length();
        // Check if this is an exact match by comparing the clustering key
        Clustering<?> foundClustering = readClusteringKey(clusteringRowId);
        int cmp = clusteringComparator.compare(foundClustering, key.clustering());
        if (cmp == 0)
            return clusteringRowId;
        else
            return -clusteringRowId - 1;
    }

    /**
     * Returns the row ID of the smallest primary key greater than or equal to the given key.
     * Returns -1 if no such key exists (i.e., the given key is greater than all keys in the map).
     * <p>
     * For wide tables, this method leverages {@link #exactRowIdOrInvertedCeiling(PrimaryKey)}
     * and converts the inverted ceiling format to a regular ceiling, except for token-only keys
     * where we need special handling to avoid token ring wraparound issues.
     *
     * @param key the primary key to find the ceiling for
     * @return the row ID of the ceiling key, or -1 if no ceiling exists
     */
    @Override
    public long ceiling(PrimaryKey key)
    {
        long rowId = exactRowIdOrInvertedCeiling(key);
        if (rowId >= 0)
            return rowId;
        else if (rowId == Long.MIN_VALUE)
            return -1;
        else
            return -rowId - 1;
    }

    /**
     * Returns the row ID of the greatest primary key less than or equal to the given key.
     * Returns -1 if no such key exists (i.e., the given key is less than all keys in the map).
     * <p>
     * For wide tables, this method handles both token-only keys and full primary keys with clustering.
     * For token-only keys, it returns the last row of the matching partition if found.
     *
     * @param key the primary key to find the floor for
     * @return the row ID of the floor key, or -1 if no floor exists
     */
    @Override
    public long floor(PrimaryKey key)
    {
        if (key instanceof PrimaryKeyWithSource)
        {
            PrimaryKeyWithSource pkws = (PrimaryKeyWithSource) key;
            if (pkws.getSourceSstableId().equals(sstableId))
                return pkws.getSourceRowId();
        }

        long rowId = exactRowIdOrInvertedCeiling(key);

        if (rowId >= 0)
        {
            // If the key is a prefix (token-only or partition-only), 
            // the floor is the *greatest* row ID associated with this prefix.
            if (key.isTokenOnly())
                return startOfNextPartition(rowId) - 1;

            return rowId;
        }

        if (rowId == Long.MIN_VALUE)
            return tokenArray.length() - 1;

        // rowId is -(ceiling) - 1. The floor is the row immediately before the ceiling.
        return -rowId - 2;
    }

    @Override
    public void close()
    {
        super.close();
        FileUtils.closeQuietly(clusteringKeyCursor);
    }

    @Override
    protected PrimaryKey supplier(long sstableRowId)
    {
        return primaryKeyFactory.create(readPartitionKey(sstableRowId), readClusteringKey(sstableRowId));
    }

    private Clustering<?> readClusteringKey(long sstableRowId)
    {
        ByteSource.Peekable peekable = ByteSource.peekable(clusteringKeyCursor.seekToPointId(sstableRowId)
                                                                              .asComparableBytes(TypeUtil.BYTE_COMPARABLE_VERSION));

        Clustering<?> clustering = clusteringComparator.clusteringFromByteComparable(ByteBufferAccessor.instance, v -> peekable);

        if (clustering == null)
            clustering = Clustering.EMPTY;

        return clustering;
    }

    // Returns the rowId of the next partition or the number of rows if supplied rowId is in the last partition
    private long startOfNextPartition(long rowId)
    {
        long partitionId = partitionArray.get(rowId);
        long nextPartitionRowId = partitionArray.ceilingRowId(++partitionId);
        if (nextPartitionRowId == -1)
            nextPartitionRowId = partitionArray.length();
        return nextPartitionRowId;
    }

    @ThreadSafe
    public static class Factory extends SkinnyPrimaryKeyMap.Factory
    {
        private final IndexComponents.ForRead perSSTableComponents;
        private final ClusteringComparator clusteringComparator;
        private final KeyLookup clusteringKeyReader;
        private final FileHandle clusteringKeyBlockOffsetsFile;
        private final FileHandle clustingingKeyBlocksFile;

        public Factory(IndexComponents.ForRead perSSTableComponents, RowAwarePrimaryKeyFactory primaryKeyFactory,
                       SSTableReader sstable)
        {
            super(perSSTableComponents, primaryKeyFactory, sstable);

            try
            {
                this.perSSTableComponents = perSSTableComponents;
                this.clusteringKeyBlockOffsetsFile =
                perSSTableComponents.get(IndexComponentType.CLUSTERING_KEY_BLOCK_OFFSETS).createFileHandle(this::close);
                this.clustingingKeyBlocksFile =
                perSSTableComponents.get(IndexComponentType.CLUSTERING_KEY_BLOCKS).createFileHandle(this::close);
                this.clusteringComparator = sstable.metadata().comparator;
                NumericValuesMeta clusteringKeyBlockOffsetsMeta = new NumericValuesMeta(
                metadataSource.get(perSSTableComponents.get(IndexComponentType.CLUSTERING_KEY_BLOCK_OFFSETS)));
                KeyLookupMeta clusteringKeyMeta = new KeyLookupMeta(
                metadataSource.get(perSSTableComponents.get(IndexComponentType.CLUSTERING_KEY_BLOCKS)));
                this.clusteringKeyReader = new KeyLookup(clustingingKeyBlocksFile, clusteringKeyBlockOffsetsFile,
                                                         clusteringKeyMeta, clusteringKeyBlockOffsetsMeta);
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
            LongArray partitionIdToToken = new LongArray.DeferredLongArray(partitionReaderFactory::open);
            try
            {

                return new WidePrimaryKeyMap(rowIdToToken, partitionIdToToken, partitionKeyReader.openCursor(),
                                             clusteringKeyReader.openCursor(), partitioner, primaryKeyFactory, clusteringComparator, sstableId,
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
            super.close();
            FileUtils.closeQuietly(Arrays.asList(clustingingKeyBlocksFile, clusteringKeyBlockOffsetsFile));
        }
    }
}
