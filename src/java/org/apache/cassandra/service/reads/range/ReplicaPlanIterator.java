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

package org.apache.cassandra.service.reads.range;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.locator.LocalStrategy;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.locator.ReplicaPlans;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.utils.Pair;

class ReplicaPlanIterator extends AbstractReplicaPlanIterator
{
    private final Keyspace keyspace;
    private final ConsistencyLevel consistency;
    private final Index.QueryPlan indexQueryPlan;
    @VisibleForTesting
    final Iterator<? extends AbstractBounds<PartitionPosition>> ranges;
    private final int rangeCount;

    ReplicaPlanIterator(AbstractBounds<PartitionPosition> keyRange,
                        Index.QueryPlan indexQueryPlan,
                        Keyspace keyspace,
                        ConsistencyLevel consistency)
    {
        this.indexQueryPlan = indexQueryPlan;
        this.keyspace = keyspace;
        this.consistency = consistency;


        List<? extends AbstractBounds<PartitionPosition>> l = keyspace.getReplicationStrategy() instanceof LocalStrategy
                                                              ? keyRange.unwrap()
                                                              : getRestrictedRanges(keyspace.getReplicationStrategy().getTokenMetadata(), keyRange);
        this.ranges = l.iterator();
        this.rangeCount = l.size();
    }

    /**
     * @return the number of {@link ReplicaPlan.ForRangeRead}s in this iterator
     */
    @Override
    public int size()
    {
        return rangeCount;
    }

    @Override
    protected ReplicaPlan.ForRangeRead computeNext()
    {
        if (!ranges.hasNext())
            return endOfData();

        return ReplicaPlans.forRangeRead(keyspace, indexQueryPlan, consistency, ranges.next(), 1);
    }
}
