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

/**
 * Factory for creating row aware primary keys, which does not use token as prefix in comparison.
 * Thus, it compares by partition key and then clustering key. It is used to store partition keys
 * and clustering keys separately, greatly reducing the disk size.
 */
public class ClusteredRowAwarePrimaryKeyFactory extends RowAwarePrimaryKeyFactory
{
    public ClusteredRowAwarePrimaryKeyFactory(ClusteringComparator clusteringComparator)
    {
        super(clusteringComparator);
    }

    @Override
    public PrimaryKey createDeferred(Token token, Supplier<PrimaryKey> primaryKeySupplier)
    {
        return new RowAwarePrimaryKey(token, null, null, primaryKeySupplier);
    }

    @Override
    public PrimaryKey create(DecoratedKey partitionKey, Clustering<?> clustering)
    {
        return new RowAwarePrimaryKey(partitionKey.getToken(), partitionKey, clustering, null);
    }

    private class RowAwarePrimaryKey extends RowAwarePrimaryKeyFactory.RowAwarePrimaryKey
    {
        private RowAwarePrimaryKey(Token token, DecoratedKey partitionKey, Clustering<?> clustering, Supplier<PrimaryKey> primaryKeySupplier)
        {
            super(token, partitionKey, clustering, primaryKeySupplier);
        }

        @Override
        protected ByteSource asComparableBytes(int terminator, ByteComparable.Version version, boolean isPrefix)
        {
            return ByteSource.withTerminator(terminator, buildComparableSources(version, isPrefix, false));
        }

        @Override
        public int compareTo(PrimaryKey o)
        {
            if (o.isTokenOnly())
                return token().compareTo(o.token());

            // Compare the partition keys. If they are not equal or
            // this is a single row partition key or there are no
            // clusterings, then return the result of this without
            // needing to compare the clusterings.
            int cmp = partitionKey().compareTo(o.partitionKey());
            if (cmp != 0 || !hasClustering() || !o.hasClustering())
                return cmp;
            return clusteringComparator.compare(clustering(), o.clustering());
        }
    }
}
