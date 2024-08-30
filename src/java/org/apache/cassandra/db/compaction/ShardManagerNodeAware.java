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

import java.util.Arrays;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.dht.tokenallocator.TokenAllocation;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.TokenMetadata;

/**
 * A shard manager implementation that accepts token-allocator-generated-tokens and splits along them to ensure that
 * current and future states of the cluster will have sstables within shards, not across them, for sufficiently high
 * levels of compaction, which allows nodes to trivially own complete sstables for sufficiently high levels of
 * compaction.
 *
 * If there are not yet enough tokens allocated, use the {@link org.apache.cassandra.dht.tokenallocator.TokenAllocator}
 * to allocate more tokens to split along. The key to this implementation is utilizing the same algorithm to allocate
 * tokens to nodes and to split ranges for higher levels of compaction.
 */
    // I haven't figured out yet whether the interesting part of this class is the fact that we use the token allocator
    // to find higher level splits or if it is the node awareness. Is it possible to remove the node awareness and keep
    // the allocator's logic or do we need both?
// TODO should we extend ShardManagerDiskAware?

public class ShardManagerNodeAware implements ShardManager
{
    public static final Token[] TOKENS = new Token[0];
    private final AbstractReplicationStrategy rs;
    private final TokenMetadata tokenMetadata;

    public ShardManagerNodeAware(AbstractReplicationStrategy rs)
    {
        this.rs = rs;
        this.tokenMetadata = rs.getTokenMetadata();
    }

    @Override
    public double rangeSpanned(Range<Token> tableRange)
    {
        return tableRange.left.size(tableRange.right);
    }

    @Override
    public double localSpaceCoverage()
    {
        // At the moment, this is global, so it covers the whole range. Might not be right though.
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
        var splitPointCount = shardCount - 1;
        // TODO is it safe to get tokens here and then endpoints later without synchronization?
        var sortedTokens = tokenMetadata.sortedTokens();
        if (splitPointCount > sortedTokens.size())
        {
            // Need to allocate tokens within node boundaries.
            var endpoints = tokenMetadata.getAllEndpoints();
            double addititionalSplits = splitPointCount - sortedTokens.size();
            var splitPointsPerNode = (int) Math.ceil(addititionalSplits / endpoints.size());
            // Compute additional tokens since we don't have enough
            for (var endpoint : endpoints)
                sortedTokens.addAll(TokenAllocation.allocateTokens(tokenMetadata, rs, endpoint, splitPointsPerNode));
            // Sort again since we added new tokens.
            sortedTokens.sort(Token::compareTo);
        }
        var splitPoints = findTokenAlignedSplitPoints(sortedTokens.toArray(TOKENS), shardCount);
        return new NodeAlignedShardTracker(shardCount, splitPoints);
    }

    private Token[] findTokenAlignedSplitPoints(Token[] sortedTokens, int shardCount)
    {
        // Short circuit on equal
        if (sortedTokens.length == shardCount - 1)
            return sortedTokens;
        var evenSplitPoints = computeUniformSplitPoints(tokenMetadata.partitioner, shardCount);
        var nodeAlignedSplitPoints = new Token[shardCount - 1];

        // UCS requires that the splitting points for a given density are also splitting points for
        // all higher densities, so we pick from among the existing tokens.
        int pos = 0;
        for (int i = 0; i < evenSplitPoints.length; i++) {
            Token value = evenSplitPoints[i];
            pos = Arrays.binarySearch(sortedTokens, pos, evenSplitPoints.length, value);

            if (pos >= 0)
            {
                // Exact match found
                nodeAlignedSplitPoints[i] = sortedTokens[pos];
                pos++;
            }
            else
            {
                // pos is -(insertion point) - 1, so calculate the insertion point
                pos = -pos - 1;

                // Check the neighbors
                Token leftNeighbor = sortedTokens[pos - 1];
                Token rightNeighbor = sortedTokens[pos];

                // Choose the nearest neighbor. By convention, prefer left if value is midpoint.
                if (value.size(leftNeighbor) <= value.size(rightNeighbor))
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


    private Token[] computeUniformSplitPoints(IPartitioner partitioner, int shardCount)
    {
        var tokenCount = shardCount - 1;
        var tokens = new Token[tokenCount];
        for (int i = 0; i < tokenCount; i++)
        {
            var ratio = ((double) i) / shardCount;
            tokens[i] = partitioner.split(partitioner.getMinimumToken(), partitioner.getMaximumToken(), ratio);
        }
        return tokens;
    }

    private class NodeAlignedShardTracker implements ShardTracker
    {
        private final int shardCount;
        private final Token[] sortedTokens;
        private int index = 0;

        NodeAlignedShardTracker(int shardCount, Token[] sortedTokens)
        {
            this.shardCount = shardCount;
            this.sortedTokens = sortedTokens;
        }

        @Override
        public Token shardStart()
        {
            return sortedTokens[index];
        }

        @Nullable
        @Override
        public Token shardEnd()
        {
            return index + 1 < sortedTokens.length ? sortedTokens[index + 1] : null;
        }

        @Override
        public Range<Token> shardSpan()
        {
            return new Range<>(shardStart(), end());
        }

        @Override
        public double shardSpanSize()
        {
            var start = sortedTokens[index];
            // TODO should this be weighted? I think not because we use the token allocator to get the splits and that
            //  currently removes our ability to know the weight, but want to check.
            return start.size(end());
        }

        /**
         * Non-nullable implementation of {@link ShardTracker#shardEnd()}
         * @return
         */
        private Token end()
        {
            var end = shardEnd();
            return end != null ? end : sortedTokens[index].minValue();
        }

        @Override
        public boolean advanceTo(Token nextToken)
        {
            var currentEnd = shardEnd();
            if (currentEnd == null || nextToken.compareTo(currentEnd) <= 0)
                return false;
            do
            {
                index++;
                currentEnd = shardEnd();
                if (currentEnd == null)
                    break;
            } while (nextToken.compareTo(currentEnd) > 0);
            return true;
        }

        @Override
        public int count()
        {
            return shardCount;
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
            // TODO confirm this is okay without a weigth and without reference to the sortedLocalRange list
            double inShardSize = covered.left.size(covered.right);
            double totalSize = targetSpan.left.size(targetSpan.right);
            return inShardSize / totalSize;
        }

        @Override
        public double rangeSpanned(PartitionPosition first, PartitionPosition last)
        {
            // TODO how do we take local range ownership into account here? The ShardManagerNodeAware is doing that for
            // us, but it seems that this node aware version is possibly off base.
            return ShardManagerNodeAware.this.rangeSpanned(first, last);
        }

        @Override
        public int shardIndex()
        {
            return index;
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
