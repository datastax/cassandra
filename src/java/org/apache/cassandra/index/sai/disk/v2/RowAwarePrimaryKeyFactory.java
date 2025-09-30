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

import java.util.Arrays;
import java.util.Objects;
import java.util.function.LongFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import io.github.jbellis.jvector.util.RamUsageEstimator;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

/**
 * A row-aware {@link PrimaryKey.Factory}. This creates {@link PrimaryKey} instances that are
 * sortable by {@link DecoratedKey} and {@link Clustering}.
 */
public class RowAwarePrimaryKeyFactory implements PrimaryKey.Factory
{
    private final ClusteringComparator clusteringComparator;
    private final boolean hasEmptyClustering;


    public RowAwarePrimaryKeyFactory(ClusteringComparator clusteringComparator)
    {
        this.clusteringComparator = clusteringComparator;
        this.hasEmptyClustering = clusteringComparator.size() == 0;
    }

    @Override
    public PrimaryKey createTokenOnly(Token token)
    {
        return new RowAwarePrimaryKey(token, null, null);
    }

    @Override
    public PrimaryKey createDeferred(long sstableRowId, LongFunction<Token> rowIdToToken, LongFunction<PrimaryKey> rowIdToPrimaryKey)
    {
        return new DeferredRowAwarePrimaryKey(sstableRowId, rowIdToToken, rowIdToPrimaryKey);
    }

    @Override
    public PrimaryKey create(DecoratedKey partitionKey, Clustering clustering)
    {
        return new RowAwarePrimaryKey(partitionKey.getToken(), partitionKey, clustering);
    }

    private class RowAwarePrimaryKey implements PrimaryKey
    {
        private final Token token;
        private final DecoratedKey partitionKey;
        private final Clustering clustering;

        private RowAwarePrimaryKey(Token token, DecoratedKey partitionKey, Clustering clustering)
        {
            this.token = token;
            this.partitionKey = partitionKey;
            this.clustering = clustering;
        }

        @Override
        public RowAwarePrimaryKey forStaticRow()
        {
            return new RowAwarePrimaryKey(token, partitionKey, Clustering.STATIC_CLUSTERING, primaryKeySupplier);
        }

        @Override
        public Token token()
        {
            return token;
        }

        @Override
        public DecoratedKey partitionKey()
        {
            return partitionKey;
        }

        @Override
        public Clustering clustering()
        {
            return clustering;
        }

        @Override
        public ByteSource asComparableBytes(ByteComparable.Version version)
        {
            return asComparableBytes(version == ByteComparable.Version.LEGACY ? ByteSource.END_OF_STREAM : ByteSource.TERMINATOR, version, false);
        }

        @Override
        public ByteSource asComparableBytesMinPrefix(ByteComparable.Version version)
        {
            return asComparableBytes(ByteSource.LT_NEXT_COMPONENT, version, true);
        }

        @Override
        public ByteSource asComparableBytesMaxPrefix(ByteComparable.Version version)
        {
            return asComparableBytes(ByteSource.GT_NEXT_COMPONENT, version, true);
        }

        private ByteSource asComparableBytes(int terminator, ByteComparable.Version version, boolean isPrefix)
        {
            ByteSource tokenComparable = token.asComparableBytes(version);
            ByteSource keyComparable = partitionKey == null ? null
                                                            : ByteSource.of(partitionKey.getKey(), version);

            // It is important that the ClusteringComparator.asBytesComparable method is used
            // to maintain the correct clustering sort order
            ByteSource clusteringComparable = clusteringComparator.size() == 0 ||
                                              clustering == null ||
                                              clustering.isEmpty() ? null
                                                                   : clusteringComparator.asByteComparable(clustering)
                                                                                         .asComparableBytes(version);

            // prefix doesn't include null components
            if (isPrefix)
            {
                if (keyComparable == null)
                    return ByteSource.withTerminator(terminator, tokenComparable);
                else if (clusteringComparable == null)
                    return ByteSource.withTerminator(terminator, tokenComparable, keyComparable);
            }
            return ByteSource.withTerminator(terminator, tokenComparable, keyComparable, clusteringComparable);
        }

        @Override
        public int compareTo(PrimaryKey o)
        {
            int cmp = token().compareTo(o.token());

            // If the tokens don't match then we don't need to compare any more of the key.
            // Otherwise if this key has no deferred loader and it's partition key is null
            // or the other partition key is null then one or both of the keys
            // are token only so we can only compare tokens
            if ((cmp != 0) || partitionKey == null || o.partitionKey() == null)
                return cmp;

            // Next compare the partition keys. If they are not equal or
            // this is a single row partition key or there are no
            // clusterings then we can return the result of this without
            // needing to compare the clusterings
            cmp = partitionKey().compareTo(o.partitionKey());
            if (cmp != 0 || hasEmptyClustering() || o.hasEmptyClustering())
                return cmp;
            return clusteringComparator.compare(clustering(), o.clustering());
        }

        @Override
        public int hashCode()
        {
            if (hasEmptyClustering)
                return Objects.hash(token);
            return Objects.hash(token, clustering());
        }

        @Override
        public boolean equals(Object obj)
        {
            if (obj instanceof PrimaryKey)
                return compareTo((PrimaryKey)obj) == 0;
            return false;
        }

        @Override
        public String toString()
        {
            return String.format("RowAwarePrimaryKey: { token: %s, partition: %s, clustering: %s:%s} ",
                                 token,
                                 partitionKey,
                                 clustering == null ? null : clustering.kind(),
                                 clustering == null ? null :String.join(",", Arrays.stream(clustering.getBufferArray())
                                                                                   .map(ByteBufferUtil::bytesToHex)
                                                                                   .collect(Collectors.toList())));
        }

        @Override
        public long ramBytesUsed()
        {
            // Object header + 4 references (token, partitionKey, clustering, primaryKeySupplier) + implicit outer reference
            long size = RamUsageEstimator.NUM_BYTES_OBJECT_HEADER +
                       5L * RamUsageEstimator.NUM_BYTES_OBJECT_REF;

            if (token != null)
                size += token.getHeapSize();
            if (partitionKey != null)
                size += RamUsageEstimator.NUM_BYTES_OBJECT_HEADER +
                       2L * RamUsageEstimator.NUM_BYTES_OBJECT_REF + // token and key references
                       2L * Long.BYTES;
            // We don't count clustering size here as it's managed elsewhere
            return size;
        }
    }

    private static class DeferredRowAwarePrimaryKey implements PrimaryKey
    {
        private final long sstableRowId;
        private final LongFunction<Token> rowIdToToken;
        private final LongFunction<PrimaryKey> primaryKeySupplier;

        private Token token = null;
        private PrimaryKey primaryKey = null;

        private DeferredRowAwarePrimaryKey(long sstableRowId, LongFunction<Token> rowIdToToken, LongFunction<PrimaryKey> primaryKeySupplier)
        {
            this.sstableRowId = sstableRowId;
            this.rowIdToToken = rowIdToToken;
            this.primaryKeySupplier = primaryKeySupplier;
        }

        @Override
        public Token token()
        {
            if (token == null)
                token = rowIdToToken.apply(sstableRowId);
            return token;
        }

        private PrimaryKey primaryKey()
        {
            // Any caller of this method needs the fully resolved key. We need to make sure that the key is loaded
            // before returning a byte comparable representation. If we don't we won't get a correct
            // comparison because we potentially won't be using the partition key
            // and clustering for the lookup
            if (primaryKey == null)
                primaryKey = primaryKeySupplier.apply(sstableRowId);
            return primaryKey;
        }

        @Override
        public DecoratedKey partitionKey()
        {
            return primaryKey().partitionKey();
        }

        @Override
        public Clustering clustering()
        {
            return primaryKey().clustering();
        }

        @Override
        public ByteSource asComparableBytes(ByteComparable.Version version)
        {
            return primaryKey().asComparableBytes(version);
        }

        @Override
        public ByteSource asComparableBytesMinPrefix(ByteComparable.Version version)
        {
            return primaryKey().asComparableBytesMinPrefix(version);
        }

        @Override
        public ByteSource asComparableBytesMaxPrefix(ByteComparable.Version version)
        {
            return primaryKey().asComparableBytesMaxPrefix(version);
        }

        @Override
        public int compareTo(PrimaryKey o)
        {
            int cmp = token().compareTo(o.token());
            if (cmp != 0)
                return cmp;
            return primaryKey().compareTo(o);
        }

        @Override
        public int hashCode()
        {
            return primaryKey().hashCode();
        }

        @Override
        public boolean equals(Object obj)
        {
            return primaryKey().equals(obj);
        }

        @Override
        public String toString()
        {
            return primaryKey().toString();
        }

        @Override
        public long ramBytesUsed()
        {
            // Object header + 3 references (sstableRowId, rowIdToToken, primaryKeySupplier) + implicit outer reference
            return RamUsageEstimator.NUM_BYTES_OBJECT_HEADER +
                   4L * RamUsageEstimator.NUM_BYTES_OBJECT_REF +
                   Long.BYTES;
        }
    }

}
