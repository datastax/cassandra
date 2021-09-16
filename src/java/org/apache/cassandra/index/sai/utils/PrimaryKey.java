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
package org.apache.cassandra.index.sai.utils;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.sai.disk.format.IndexFeatureSet;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.disk.v1.LongArray;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;

/**
 * The primary key of a row, composed by the partition key and the clustering key.
 */
public class PrimaryKey implements Comparable<PrimaryKey>
{
    private static final ClusteringComparator EMPTY_COMPARATOR = new ClusteringComparator();

    public enum Kind
    {
        TOKEN,
        PARTITION,
        MAPPED,
        UNMAPPED,
    }

    private Kind kind;
    private DecoratedKey partitionKey;
    private Clustering clustering;
    private ClusteringComparator clusteringComparator;
    private long sstableRowId;

    public static class PrimaryKeyFactory
    {
        private final IPartitioner partitioner;
        private final ClusteringComparator comparator;
        private final IndexFeatureSet indexFeatureSet;

        PrimaryKeyFactory(IPartitioner partitioner, ClusteringComparator comparator, IndexFeatureSet indexFeatureSet)
        {
            this.partitioner = partitioner;
            this.comparator = comparator;
            this.indexFeatureSet = indexFeatureSet;
        }

        public PrimaryKey createKey(DecoratedKey partitionKey, Clustering clustering, long sstableRowId)
        {
            return new PrimaryKey(partitionKey,
                                  indexFeatureSet.isRowAware() ? clustering : Clustering.EMPTY,
                                  indexFeatureSet.isRowAware() ? comparator : EMPTY_COMPARATOR,
                                  sstableRowId);
        }

        public PrimaryKey createKey(DecoratedKey partitionKey, Clustering clustering)
        {
            return new PrimaryKey(partitionKey,
                                  indexFeatureSet.isRowAware() ? clustering : Clustering.EMPTY,
                                  indexFeatureSet.isRowAware() ? comparator : EMPTY_COMPARATOR,
                                  -1);
        }

        public PrimaryKey createKey(ByteComparable comparable, long sstableRowId)
        {
            return createKeyFromPeekable(comparable.asPeekableBytes(ByteComparable.Version.OSS41), sstableRowId);
        }

        public PrimaryKey createKey(byte[] bytes)
        {
            return createKeyFromPeekable(ByteSource.peekable(ByteSource.fixedLength(bytes)), -1);
        }

        public PrimaryKey createKey(byte[] bytes, long sstableRowId)
        {
            return createKeyFromPeekable(ByteSource.peekable(ByteSource.fixedLength(bytes)), sstableRowId);
        }

        public PrimaryKey createKey(DecoratedKey key)
        {
            return new PrimaryKey(key);
        }

        public PrimaryKey createKey(Token token)
        {
            return new PrimaryKey(token);
        }

        public PrimaryKey createKey(Token token, long sstableRowId)
        {
            return new PrimaryKey(token, sstableRowId);
        }

        private PrimaryKey createKeyFromPeekable(ByteSource.Peekable peekable, long sstableRowId)
        {
            Token token = partitioner.getTokenFactory().fromComparableBytes(ByteSourceInverse.nextComponentSource(peekable), ByteComparable.Version.OSS41);
            byte[] keyBytes = ByteSourceInverse.getUnescapedBytes(ByteSourceInverse.nextComponentSource(peekable));
            DecoratedKey key =  new BufferDecoratedKey(token, ByteBuffer.wrap(keyBytes));

            if (!indexFeatureSet.isRowAware() || (comparator.size() == 0) || (peekable.peek() == ByteSource.TERMINATOR))
                return new PrimaryKey(key, sstableRowId);

            ByteBuffer[] values = new ByteBuffer[comparator.size()];

            byte[] clusteringKeyBytes;
            int index = 0;

            while ((clusteringKeyBytes = ByteSourceInverse.getUnescapedBytes(ByteSourceInverse.nextComponentSource(peekable))) != null)
            {
                values[index++] = ByteBuffer.wrap(clusteringKeyBytes);
                if (peekable.peek() == ByteSource.TERMINATOR)
                    break;
            }

            Clustering clustering = Clustering.make(values);

            return new PrimaryKey(key, clustering, comparator, sstableRowId);
        }
    }

    public static PrimaryKeyFactory factory(TableMetadata tableMetadata, IndexFeatureSet indexFeatureSet)
    {
        return new PrimaryKeyFactory(tableMetadata.partitioner, tableMetadata.comparator, indexFeatureSet);
    }

    @VisibleForTesting
    public static PrimaryKeyFactory factory(IPartitioner partitioner, ClusteringComparator comparator, IndexFeatureSet indexFeatureSet)
    {
        return new PrimaryKeyFactory(partitioner, comparator, indexFeatureSet);
    }

    @VisibleForTesting
    public static PrimaryKeyFactory factory()
    {
        return new PrimaryKeyFactory(Murmur3Partitioner.instance, EMPTY_COMPARATOR, Version.LATEST.onDiskFormat().indexFeatureSet());
    }

    private PrimaryKey(DecoratedKey partitionKey, Clustering clustering, ClusteringComparator comparator, long sstableRowId)
    {
        this(sstableRowId >= 0 ? Kind.MAPPED : Kind.UNMAPPED, partitionKey, clustering, comparator, sstableRowId);
    }

    private PrimaryKey(Token token)
    {
        this(Kind.TOKEN, new BufferDecoratedKey(token, ByteBufferUtil.EMPTY_BYTE_BUFFER), Clustering.EMPTY, EMPTY_COMPARATOR, -1);
    }

    private PrimaryKey(Token token, long sstableRowId)
    {
        this(Kind.MAPPED, new BufferDecoratedKey(token, ByteBufferUtil.EMPTY_BYTE_BUFFER), Clustering.EMPTY, EMPTY_COMPARATOR, sstableRowId);
    }

    private PrimaryKey(DecoratedKey partitionKey)
    {
        this(Kind.PARTITION, partitionKey, Clustering.EMPTY, EMPTY_COMPARATOR, -1);
    }

    private PrimaryKey(DecoratedKey partitionKey, long sstableRowId)
    {
        this(Kind.PARTITION, partitionKey, Clustering.EMPTY, EMPTY_COMPARATOR, sstableRowId);
    }

    private PrimaryKey(Kind kind, DecoratedKey partitionKey, Clustering clustering, ClusteringComparator clusteringComparator, long sstableRowId)
    {
        this.kind = kind;
        this.partitionKey = partitionKey;
        this.clustering = clustering;
        this.clusteringComparator = clusteringComparator;
        this.sstableRowId = sstableRowId;
    }

    public int size()
    {
        return partitionKey.getKey().remaining() + clustering.dataSize();
    }

    public byte[] asBytes()
    {
        ByteSource source = asComparableBytes(ByteComparable.Version.OSS41);

        return ByteSourceInverse.readBytes(source);
    }

    public ByteSource asComparableBytes(ByteComparable.Version version)
    {
        ByteSource[] sources = new ByteSource[clustering.size() + 2];
        sources[0] = partitionKey.getToken().asComparableBytes(version);
        sources[1] = ByteSource.of(partitionKey.getKey(), version);
        for (int index = 0; index < clustering.size(); index++)
        {
            sources[index + 2] = ByteSource.of(clustering.bufferAt(index), version);
        }
        return ByteSource.withTerminator(version == ByteComparable.Version.LEGACY
                                         ? ByteSource.END_OF_STREAM
                                         : ByteSource.TERMINATOR,
                                         sources);
    }

    public DecoratedKey partitionKey()
    {
        return partitionKey;
    }

    public Clustering clustering()
    {
        assert clustering != null && kind != Kind.TOKEN;
        return clustering;
    }

    public boolean hasEmptyClustering()
    {
        return clustering == null || clustering.isEmpty();
    }

    public ClusteringComparator clusteringComparator()
    {
        assert clusteringComparator != null && kind != Kind.TOKEN;
        return clusteringComparator;
    }

    public long sstableRowId(LongArray tokenToRowId)
    {
        return tokenToRowId.findTokenRowID(partitionKey.getToken().getLongValue());
    }

    public long sstableRowId()
    {
        assert kind == Kind.MAPPED : "Should be MAPPED to read sstableRowId but was " + kind;
        return sstableRowId;
    }

    @Override
    public int compareTo(PrimaryKey o)
    {
        if (kind == Kind.TOKEN || o.kind == Kind.TOKEN)
            return partitionKey.getToken().compareTo(o.partitionKey.getToken());
        int cmp = partitionKey.compareTo(o.partitionKey);
        if (cmp != 0 || kind == Kind.PARTITION || o.kind == Kind.PARTITION || (clustering.isEmpty() && o.clustering.isEmpty()))
            return cmp;
        return clusteringComparator.equals(o.clusteringComparator) ? clusteringComparator.compare(clustering, o.clustering) : 0;
    }

    @Override
    public String toString()
    {
        return String.format("PrimaryKey: { partition : %s, clustering: %s:%s, sstableRowId: %s} ",
                             partitionKey,
                             clustering.kind(),
                             String.join(",", Arrays.stream(clustering.getBufferArray())
                                                    .map(ByteBufferUtil::bytesToHex)
                                                    .collect(Collectors.toList())),
                             sstableRowId);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(partitionKey, clustering, clusteringComparator);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof PrimaryKey)
            return compareTo((PrimaryKey)obj) == 0;
        return false;
    }
}
