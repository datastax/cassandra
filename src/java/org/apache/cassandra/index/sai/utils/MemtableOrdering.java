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

import java.util.List;

import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.plan.Orderer;
import org.apache.cassandra.utils.CloseableIterator;

/***
 * Analogue of SegmentOrdering, but for memtables.
 */
public interface MemtableOrdering
{

    /**
     * Order the index based on the given expression.
     *
     * @param queryContext - the query context
     * @param orderer      - the expression to order by
     * @param slice    - the expression to restrict index search by
     * @param keyRange     - the key range to search
     * @param limit        - can be used to inform the search, but should not be used to prematurely limit the iterator
     * @return an iterator over the results in score order.
     */
    List<CloseableIterator<PrimaryKeyWithSortKey>> orderBy(QueryContext queryContext,
                                                           Orderer orderer,
                                                           Expression slice,
                                                           AbstractBounds<PartitionPosition> keyRange,
                                                           int limit);

    /**
     * Order the given list of {@link PrimaryKey} results corresponding to the given expression.
     * Returns an iterator over the results in score order.
     *
     * Assumes that the given  spans the same rows as the implementing index's segment.
     */
    CloseableIterator<PrimaryKeyWithSortKey> orderResultsBy(QueryContext context, List<PrimaryKey> keys, Orderer orderer, int limit);
}
