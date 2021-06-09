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
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
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

    private AtomicLong mutableCounter = new AtomicLong();


    public enum Kind
    {
        TOKEN,
        MAPPED,
        UNMAPPED,
    }

    private Kind kind;
    private DecoratedKey partitionKey;
    private Clustering clustering;
    private ClusteringComparator clusteringComparator;
    private long sstableRowId;
    private boolean mutable = false;
    private long mutableId;

    public static class PrimaryKeyFactory
    {
        private final IPartitioner partitioner;
        private final ClusteringComparator comparator;
        private final PrimaryKey mutablePrimaryKey;
        private final boolean mutable;

        PrimaryKeyFactory(IPartitioner partitioner, ClusteringComparator comparator, boolean mutable)
        {
            this.partitioner = partitioner;
            this.comparator = comparator;
            this.mutablePrimaryKey = new PrimaryKey();
            this.mutable = mutable;
        }

        public PrimaryKeyFactory copyOf()
        {
            return new PrimaryKeyFactory(partitioner, comparator, mutable);
        }

        public PrimaryKey createKey(DecoratedKey partitionKey, Clustering clustering, long sstableRowId)
        {
            return makeMutableKey(partitionKey, clustering, sstableRowId);
        }

        public PrimaryKey createKey(DecoratedKey partitionKey, Clustering clustering)
        {
            return makeMutableKey(partitionKey, clustering, -1);
        }

        public PrimaryKey createKey(ByteComparable comparable, long sstableRowId)
        {
            return createKeyFromPeekable(comparable.asPeekableBytes(ByteComparable.Version.OSS41), sstableRowId);
        }

        public PrimaryKey createKey(byte[] bytes)
        {
            return createKeyFromPeekable(ByteSource.peekable(ByteSource.fixedLength(bytes)), -1);
        }

        @VisibleForTesting
        public PrimaryKey createKey(DecoratedKey key)
        {
            return new PrimaryKey(key, Clustering.EMPTY, EMPTY_COMPARATOR);
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

            if ((comparator.size() == 0) || peekable.peek() == ByteSource.TERMINATOR)
                return new PrimaryKey(key, Clustering.EMPTY, comparator);

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

            return makeMutableKey(key, clustering, sstableRowId);
        }

        private PrimaryKey makeMutableKey(DecoratedKey partitionKey, Clustering clustering, long sstableRowId)
        {
//            if (mutable)
//            {
//                mutablePrimaryKey.kind = sstableRowId >= 0 ? Kind.MAPPED : Kind.UNMAPPED;
//                mutablePrimaryKey.partitionKey = partitionKey;
//                mutablePrimaryKey.clustering = clustering;
//                mutablePrimaryKey.clusteringComparator = comparator;
//                mutablePrimaryKey.sstableRowId = sstableRowId;
//                return mutablePrimaryKey;
//            }
            return new PrimaryKey(partitionKey, clustering, comparator, sstableRowId);
        }
    }

    public static PrimaryKeyFactory immutableFactory(TableMetadata tableMetadata)
    {
        return new PrimaryKeyFactory(tableMetadata.partitioner, tableMetadata.comparator, false);
    }

    public static PrimaryKeyFactory factory(TableMetadata tableMetadata)
    {
        return new PrimaryKeyFactory(tableMetadata.partitioner, tableMetadata.comparator, true);
    }

    @VisibleForTesting
    public static PrimaryKeyFactory factory(IPartitioner partitioner, ClusteringComparator comparator)
    {
        return new PrimaryKeyFactory(partitioner, comparator, false);
    }

    public static PrimaryKeyFactory factory()
    {
        return new PrimaryKeyFactory(Murmur3Partitioner.instance, EMPTY_COMPARATOR, false);
    }

    private PrimaryKey()
    {
        mutable = true;
        mutableId = mutableCounter.getAndIncrement();
    }

    private PrimaryKey(DecoratedKey partitionKey, Clustering clustering, ClusteringComparator comparator)
    {
        this(partitionKey, clustering, comparator, -1);
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

    @Override
    public String toString()
    {
        return String.format("PrimaryKey: { partition : %s, clustering: %s, sstableRowId: %s} ",
                             partitionKey,
                             String.join(",", Arrays.stream(clustering.getBufferArray())
                                                    .map(ByteBufferUtil::bytesToHex)
                                                    .collect(Collectors.toList())),
                             sstableRowId);
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
//        assert this.mutable && o.mutable && this.mutableId == o.mutableId : "Trying to compare the mutable primary key for a mapped value";
        if (kind == Kind.TOKEN || o.kind == Kind.TOKEN)
            return partitionKey.getToken().compareTo(o.partitionKey.getToken());
        int cmp = partitionKey.compareTo(o.partitionKey);
        if (clustering.isEmpty() || o.clustering.isEmpty())
            return cmp;
        return (cmp != 0) ? cmp : clusteringComparator().compare(clustering, o.clustering);
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
