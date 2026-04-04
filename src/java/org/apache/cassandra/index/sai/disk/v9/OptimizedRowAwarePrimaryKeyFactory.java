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

package org.apache.cassandra.index.sai.disk.v9;

import java.util.function.Supplier;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.sai.disk.v2.RowAwarePrimaryKeyFactory;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

public class OptimizedRowAwarePrimaryKeyFactory extends RowAwarePrimaryKeyFactory
{
    public OptimizedRowAwarePrimaryKeyFactory(ClusteringComparator clusteringComparator)
    {
        super(clusteringComparator);
    }

    @Override
    public PrimaryKey createDeferred(Token token, Supplier<PrimaryKey> primaryKeySupplier)
    {
        return new OptimizedRowAwarePrimaryKeyFactory.RowAwarePrimaryKey(token, null, null, primaryKeySupplier);
    }

    @Override
    public PrimaryKey create(DecoratedKey partitionKey, Clustering clustering)
    {
        return new OptimizedRowAwarePrimaryKeyFactory.RowAwarePrimaryKey(partitionKey.getToken(), partitionKey, clustering, null);
    }

    private class RowAwarePrimaryKey extends RowAwarePrimaryKeyFactory.RowAwarePrimaryKey
    {
        private RowAwarePrimaryKey(Token token, DecoratedKey partitionKey, Clustering clustering, Supplier<PrimaryKey> primaryKeySupplier)
        {
            super(token, partitionKey, clustering, primaryKeySupplier);
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
            // We need to make sure that the key is loaded before returning a
            // byte comparable representation. If we don't we won't get a correct
            // comparison because we potentially won't be using the partition key
            // and clustering for the lookup
            loadDeferred();

            ByteSource keyComparable = ByteSource.of(partitionKey.getKey(), version);

            // It is important that the ClusteringComparator.asBytesComparable method is used
            // to maintain the correct clustering sort order
            ByteSource clusteringComparable = clusteringComparator.size() == 0 ||
                                              clustering == null ||
                                              clustering.isEmpty() ? null
                                                                   : clusteringComparator.asByteComparable(clustering)
                                                                                         .asComparableBytes(version);

            // prefix doesn't include null components
            if (isPrefix && clusteringComparable == null)
                return ByteSource.withTerminator(terminator, keyComparable);
            else
                return ByteSource.withTerminator(terminator, keyComparable, clusteringComparable);
        }
    }
}
