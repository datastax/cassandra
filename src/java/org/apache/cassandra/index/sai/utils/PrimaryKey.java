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

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.sai.disk.format.IndexFeatureSet;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.disk.v1.LongArray;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

/**
 * The primary key of a row, composed by the partition key and the clustering key.
 */
public class PrimaryKey implements Comparable<PrimaryKey>
{
    private static final ClusteringComparator EMPTY_COMPARATOR = new ClusteringComparator();

    public static final Serializer serializer = new Serializer();

    public enum Kind
    {
        TOKEN(true, false, false),
        TOKEN_MAPPED(true, false, true),
        PARTITION(false, true, false),
        PARTITION_MAPPED(false, true, true),
        MAPPED(false, false, true),
        UNMAPPED(false, false, false);

        final boolean token;
        final boolean partition;
        final boolean mapped;

        private static final Kind[] allKinds = Kind.values();

        static Kind fromOrdinal(int ordinal)
        {
            return allKinds[ordinal];
        }

        Kind(boolean token, boolean partition, boolean mapped)
        {
            this.token = token;
            this.partition = partition;
            this.mapped = mapped;
        }
    }

    private Kind kind;
    private Token token;
    private DecoratedKey partitionKey;
    private Clustering clustering;
    private ClusteringComparator clusteringComparator;
    private long sstableRowId;
    private Supplier<DecoratedKey> decoratedKeySupplier;

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

        public PrimaryKey createKey(DataInputPlus input, long sstableRowId) throws IOException
        {
            return serializer.deserialize(input, 0, partitioner, comparator, sstableRowId);

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
            return new PrimaryKey(token, sstableRowId, () -> new BufferDecoratedKey(token, ByteBufferUtil.EMPTY_BYTE_BUFFER));
        }

        public PrimaryKey createKey(Token token, long sstableRowId, Supplier<DecoratedKey> decoratedKeySupplier)
        {
            return new PrimaryKey(token, sstableRowId, decoratedKeySupplier);
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
        this(sstableRowId >= 0 ? Kind.MAPPED : Kind.UNMAPPED, null, partitionKey, clustering, comparator, sstableRowId, null);
    }

    private PrimaryKey(Token token)
    {
        this(Kind.TOKEN, token, null, Clustering.EMPTY, EMPTY_COMPARATOR, -1, () -> new BufferDecoratedKey(token, ByteBufferUtil.EMPTY_BYTE_BUFFER));
    }

    private PrimaryKey(Token token, long sstableRowId, Supplier<DecoratedKey> decoratedKeySupplier)
    {
        this(Kind.TOKEN_MAPPED, token, null, Clustering.EMPTY, EMPTY_COMPARATOR, sstableRowId, decoratedKeySupplier);
    }

    private PrimaryKey(DecoratedKey partitionKey)
    {
        this(Kind.PARTITION, null, partitionKey, Clustering.EMPTY, EMPTY_COMPARATOR, -1, null);
    }

    private PrimaryKey(Kind kind,
                       Token token,
                       DecoratedKey partitionKey,
                       Clustering clustering,
                       ClusteringComparator clusteringComparator,
                       long sstableRowId,
                       Supplier<DecoratedKey> decoratedKeySupplier)
    {
        this.kind = kind;
        this.token = token;
        this.partitionKey = partitionKey;
        this.clustering = clustering;
        this.clusteringComparator = clusteringComparator;
        this.sstableRowId = sstableRowId;
        this.decoratedKeySupplier = decoratedKeySupplier;
    }

    public int size()
    {
        return partitionKey.getKey().remaining() + clustering.dataSize();
    }

    public ByteSource asComparableBytes(ByteComparable.Version version)
    {
        if (kind.token)
            return token.asComparableBytes(version);
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

    public Token token()
    {
        return kind.token ? token : partitionKey.getToken();
    }

    public DecoratedKey partitionKey()
    {
        return partitionKey == null ? decoratedKeySupplier.get() : partitionKey;
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
        assert kind.mapped : "Should be MAPPED to read sstableRowId but was " + kind;
        return sstableRowId;
    }

    @Override
    public int compareTo(PrimaryKey o)
    {
        if (kind.token || o.kind.token)
            return token().compareTo(o.token());
        int cmp = partitionKey().compareTo(o.partitionKey());
        if (cmp != 0 || kind.partition || o.kind.partition || (clustering.isEmpty() && o.clustering.isEmpty()))
            return cmp;
        return clusteringComparator.equals(o.clusteringComparator) ? clusteringComparator.compare(clustering, o.clustering) : 0;
    }

    @Override
    public String toString()
    {
        return String.format("PrimaryKey: { token : %s, partition : %s, clustering: %s:%s, sstableRowId: %s} ",
                             token,
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

    public static class Serializer
    {
        private static final ClusteringPrefix.Kind[] ALL_CLUSTERING_KINDS = ClusteringPrefix.Kind.values();

        public void serialize(DataOutputPlus output, int version, PrimaryKey primaryKey) throws IOException
        {
            assert !primaryKey.kind.token : "TOKEN only primary keys can't be serialized";
            output.writeByte(primaryKey.kind.ordinal());
            PartitionPosition.serializer.serialize(primaryKey.partitionKey, output, version);
            if (!primaryKey.kind.partition)
            {
                output.writeByte(primaryKey.clustering.kind().ordinal());
                if (primaryKey.clustering.kind() != ClusteringPrefix.Kind.STATIC_CLUSTERING)
                    Clustering.serializer.serialize(primaryKey.clustering, output, version, primaryKey.clusteringComparator.subtypes());
            }
        }

        public PrimaryKey deserialize(DataInputPlus input,
                                      int version,
                                      IPartitioner partitioner,
                                      ClusteringComparator clusteringComparator,
                                      long sstableRowId) throws IOException
        {
            Kind partitionKind = Kind.fromOrdinal(input.readByte());
            DecoratedKey partitionKey = (DecoratedKey)PartitionPosition.serializer.deserialize(input, partitioner, version);
            if (partitionKind.partition)
                return new PrimaryKey(partitionKey);
            ClusteringPrefix.Kind kind = ALL_CLUSTERING_KINDS[input.readByte()];
            Clustering clustering = kind == ClusteringPrefix.Kind.STATIC_CLUSTERING ? Clustering.STATIC_CLUSTERING
                                                                                    : Clustering.serializer.deserialize(input,
                                                                                                                        version,
                                                                                                                        clusteringComparator.subtypes());
            return new PrimaryKey(partitionKey, clustering, clusteringComparator, sstableRowId);
        }
    }
}
