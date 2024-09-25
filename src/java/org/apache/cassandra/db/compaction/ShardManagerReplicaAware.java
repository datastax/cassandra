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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.dht.tokenallocator.IsolatedTokenAllocator;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.TokenMetadata;

/**
 * A {@link ShardManager} implementation that aligns UCS and replica shards to limit the amount of sstables that are
 * partially owned by replicas. It takes an {@link AbstractReplicationStrategy} as input and uses it to determine
 * current and future replica token boundaries to use as sharding split points to ensure that for current and
 * future states of the cluster, the generated sstable shard ranges will not span multiple nodes for sufficiently high
 * levels of compaction.
 * <p>
 * If more compaction requires more shards than the already allocated tokens can satisfy, use the
 * {@link org.apache.cassandra.dht.tokenallocator.TokenAllocator} to allocate more tokens and then use those tokens
 * as split points. This implementation relies on the fact that token allocation is deterministic after the first
 * token has been selected.
 */
public class ShardManagerReplicaAware implements ShardManager
{
    private static final Logger logger = LoggerFactory.getLogger(ShardManagerReplicaAware.class);
    public static final Token[] EMPTY_TOKENS = new Token[0];
    private final AbstractReplicationStrategy rs;
    private final TokenMetadata tokenMetadata;
    private final IPartitioner partitioner;
    private final ConcurrentHashMap<Integer, Token[]> splitPointCache;

    public ShardManagerReplicaAware(AbstractReplicationStrategy rs)
    {
        this.rs = rs;
        // Clone the map to ensure it has a consistent view of the tokenMetadata. UCS creates a new instance of the
        // ShardManagerTokenAware class when the token metadata changes.
        this.tokenMetadata = rs.getTokenMetadata().cloneOnlyTokenMap();
        this.splitPointCache = new ConcurrentHashMap<>();
        this.partitioner = tokenMetadata.partitioner;
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
    public ShardTracker boundaries(int shardCount)
    {
        try
        {
            var splitPoints = splitPointCache.computeIfAbsent(shardCount, this::computeBoundaries);
            return new ReplicaAlignedShardTracker(splitPoints);
        }
        catch (Throwable t)
        {
            logger.error("Error creating shard boundaries", t);
            throw t;
        }
    }

    private Token[] computeBoundaries(int shardCount)
    {
        logger.debug("Creating shard boundaries for {} shards", shardCount);
        // Because sstables do not wrap around, we need shardCount - 1 splits.
        var splitPointCount = shardCount - 1;
        // Copy array list. The current token allocation logic doesn't consider our copy of tokenMetadata, so
        // modifying the sorted tokens here won't give us much benefit.
        var sortedTokensList = new ArrayList<>(tokenMetadata.sortedTokens());
        if (splitPointCount > sortedTokensList.size())
        {
            // Not enough tokens, allocate them.
            int additionalSplits = splitPointCount - sortedTokensList.size();
            var newTokens = IsolatedTokenAllocator.allocateTokens(additionalSplits, rs);
            sortedTokensList.addAll(newTokens);
            sortedTokensList.sort(Token::compareTo);
        }
        var sortedTokens = sortedTokensList.toArray(EMPTY_TOKENS);
        // Short circuit on equal and on count 1.
        if (sortedTokens.length == splitPointCount)
            return sortedTokens;
        if (splitPointCount == 0)
            return EMPTY_TOKENS;

        // Get the ideal split points and then map them to their nearest neighbor.
        var evenSplitPoints = computeUniformSplitPoints(splitPointCount);
        var nodeAlignedSplitPoints = new Token[splitPointCount];

        // UCS requires that the splitting points for a given density are also splitting points for
        // all higher densities, so we pick from among the existing tokens.
        int pos = 0;
        for (int i = 0; i < evenSplitPoints.length; i++)
        {
            int min = pos;
            int max = sortedTokens.length - evenSplitPoints.length + i;
            Token value = evenSplitPoints[i];
            pos = Arrays.binarySearch(sortedTokens, min, max, value);
            if (pos < 0)
                pos = -pos - 1;

            if (pos == min)
            {
                // No left neighbor, so choose the right neighbor
                nodeAlignedSplitPoints[i] = sortedTokens[pos];
                pos++;
            }
            else if (pos == max)
            {
                // No right neighbor, so choose the left neighbor
                // This also means that for all greater indexes we don't have a choice.
                for (; i < evenSplitPoints.length; ++i)
                    nodeAlignedSplitPoints[i] = sortedTokens[pos++ - 1];
            }
            else
            {
                // Check the neighbors
                Token leftNeighbor = sortedTokens[pos - 1];
                Token rightNeighbor = sortedTokens[pos];

                // Choose the nearest neighbor. By convention, prefer left if value is midpoint, but don't
                // choose the same token twice.
                if (leftNeighbor.size(value) <= value.size(rightNeighbor))
                {
                    nodeAlignedSplitPoints[i] = leftNeighbor;
                    // No need to bump pos because we decremented it to find the right split token.
                }
                else
                {
                    nodeAlignedSplitPoints[i] = rightNeighbor;
                    pos++;
                }
            }
        }

        return nodeAlignedSplitPoints;
    }


    private Token[] computeUniformSplitPoints(int splitPointCount)
    {
        // Want the shard count here to get the right ratio.
        var rangeStep = 1.0 / (splitPointCount + 1);
        var tokens = new Token[splitPointCount];
        for (int i = 0; i < splitPointCount; i++)
        {
            // Multiply the step by the index + 1 to get the ratio to the left of the minimum token.
            var ratioToLeft = rangeStep * (i + 1);
            tokens[i] = partitioner.split(partitioner.getMinimumToken(), partitioner.getMaximumToken(), ratioToLeft);
        }
        return tokens;
    }

    private class ReplicaAlignedShardTracker implements ShardTracker
    {
        private final Token minToken;
        private final Token[] sortedTokens;
        private int nextShardIndex = 0;
        private Token currentEnd;

        ReplicaAlignedShardTracker(Token[] sortedTokens)
        {
            this.sortedTokens = sortedTokens;
            this.minToken = partitioner.getMinimumToken();
            this.currentEnd = nextShardIndex < sortedTokens.length ? sortedTokens[nextShardIndex] : null;
        }

        @Override
        public Token shardStart()
        {
            return nextShardIndex == 0 ? minToken : sortedTokens[nextShardIndex - 1];
        }

        @Nullable
        @Override
        public Token shardEnd()
        {
            return nextShardIndex < sortedTokens.length ? sortedTokens[nextShardIndex] : null;
        }

        @Override
        public Range<Token> shardSpan()
        {
            return new Range<>(shardStart(), end());
        }

        @Override
        public double shardSpanSize()
        {
            // No weight applied because weighting is a local range property.
            return shardStart().size(end());
        }

        /**
         * Non-nullable implementation of {@link ShardTracker#shardEnd()}
         * @return
         */
        private Token end()
        {
            Token end = shardEnd();
            return end != null ? end : minToken;
        }

        @Override
        public boolean advanceTo(Token nextToken)
        {
            if (currentEnd == null || nextToken.compareTo(currentEnd) <= 0)
                return false;
            do
            {
                nextShardIndex++;
                currentEnd = shardEnd();
                if (currentEnd == null)
                    break;
            } while (nextToken.compareTo(currentEnd) > 0);
            return true;
        }

        @Override
        public int count()
        {
            return sortedTokens.length + 1;
        }

        @Override
        public double fractionInShard(Range<Token> targetSpan)
        {
            Range<Token> shardSpan = shardSpan();
            Range<Token> covered = targetSpan.intersectionNonWrapping(shardSpan);
            if (covered == null)
                return 0;
            if (covered == targetSpan)
                return 1;
            double inShardSize = covered.left.size(covered.right);
            double totalSize = targetSpan.left.size(targetSpan.right);
            return inShardSize / totalSize;
        }

        @Override
        public double rangeSpanned(PartitionPosition first, PartitionPosition last)
        {
            // Ignore local range owndership for initial implementation.
            return first.getToken().size(last.getToken());
        }

        @Override
        public int shardIndex()
        {
            return nextShardIndex - 1;
        }

        @Override
        public long shardAdjustedKeyCount(Set<SSTableReader> sstables)
        {
            // Not sure if this needs a custom implementation yet
            return ShardTracker.super.shardAdjustedKeyCount(sstables);
        }

        @Override
        public void applyTokenSpaceCoverage(SSTableWriter writer)
        {
            // Not sure if this needs a custom implementation yet
            ShardTracker.super.applyTokenSpaceCoverage(writer);
        }
    }
}
