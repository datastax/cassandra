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
package org.apache.cassandra.index.internal.composites;

import java.nio.ByteBuffer;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.index.internal.CassandraIndex;
import org.apache.cassandra.index.internal.IndexEntry;
import org.apache.cassandra.schema.IndexMetadata;

/**
 * Common superclass for indexes that capture collection keys, including
 * indexes on such keys themselves.
 *
 * A cell indexed by this index will have the general form:
 *   ck_0 ... ck_n c_name [col_elt] : v
 * where ck_i are the cluster keys, c_name the CQL3 column name, col_elt the
 * collection element that we want to index (which may or may not be there depending
 * on whether c_name is the collection we're indexing), and v the cell value.
 *
 * Such a cell is indexed if c_name is the indexed collection (in which case we are guaranteed to have
 * col_elt). The index entry can be viewed in the following way:
 *   - the row key is determined by subclasses of this type.
 *   - the cell name will be 'rk ck_0 ... ck_n' where rk is the row key of the initial cell.
 */
public abstract class CollectionKeyIndexBase extends CassandraIndex
{
    public CollectionKeyIndexBase(ColumnFamilyStore baseCfs, IndexMetadata indexDef)
    {
        super(baseCfs, indexDef);
    }

    public <T> ClusteringBuilder buildIndexClusteringPrefix(ByteBuffer partitionKey,
                                                            ClusteringPrefix<T> prefix,
                                                            CellPath path)
    {
        ClusteringBuilder builder = ClusteringBuilder.create(getIndexComparator());
        builder.add(partitionKey);

        // When indexing a static column, prefix will be empty but only the
        // partition key is needed at query time.
        for (int i = 0; i < prefix.size(); i++)
            builder.add(prefix.get(i), prefix.accessor());

        return builder;
    }

    public IndexEntry decodeEntry(DecoratedKey indexedValue,
                                  Row indexEntry)
    {
        Clustering<?> clustering = indexEntry.clustering();

        Clustering<?> indexedEntryClustering = null;
        if (getIndexedColumn().isStatic())
            indexedEntryClustering = Clustering.STATIC_CLUSTERING;
        else
        {
            int count = 1 + baseCfs.metadata().clusteringColumns().size();
            ClusteringBuilder builder = ClusteringBuilder.create(baseCfs.getComparator());
            for (int i = 0; i < count - 1; i++)
                builder.add(clustering, i + 1);
            indexedEntryClustering = builder.build();
        }

        return new IndexEntry(indexedValue,
                              clustering,
                              indexEntry.primaryKeyLivenessInfo().timestamp(),
                              clustering.bufferAt(0),
                              indexedEntryClustering);
    }
}
