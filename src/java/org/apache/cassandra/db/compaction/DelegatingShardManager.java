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

package org.apache.cassandra.db.compaction;

import java.util.function.IntFunction;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;

/**
 * A shard manager that delegates to a token generator for determining shard boundaries.
 */
public class DelegatingShardManager implements ShardManager
{
    private final IntFunction<Token[]> tokenGenerator;
    private CompactionRealm realm;

    public DelegatingShardManager(IntFunction<Token[]> tokenGenerator, CompactionRealm realm)
    {
        this.tokenGenerator = tokenGenerator;
        this.realm = realm;
    }

    @Override
    public double rangeSpanned(Range<Token> tableRange)
    {
        return tableRange.left.size(tableRange.right);
    }

    @Override
    public double localSpaceCoverage()
    {
        // This manager is global, so it owns the whole range.
        return 1;
    }

    @Override
    public double shardSetCoverage()
    {
        // For now there are no disks defined, so this is the same as localSpaceCoverage
        return 1;
    }

    @Override
    public double minimumPerPartitionSpan()
    {
        return localSpaceCoverage() / Math.max(1, realm.estimatedPartitionCount());
    }

    @Override
    public ShardTracker boundaries(int shardCount)
    {
        var tokens = tokenGenerator.apply(shardCount);
        return new SimpleShardTracker(tokens);
    }
}
