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

import java.util.Set;
import javax.annotation.Nullable;

import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;

/**
 * A shard tracker that uses the provided tokens as a complete list of split points. The first token is typically
 * the minimum token.
 */
class SimpleShardTracker implements ShardTracker
{
    private final Token[] sortedTokens;
    private int index;
    private Token currentEnd;

    SimpleShardTracker(Token[] sortedTokens)
    {
        assert sortedTokens.length > 0;
        assert sortedTokens[0].isMinimum();
        this.sortedTokens = sortedTokens;
        this.index = 0;
        this.currentEnd = shardEnd();
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
        // No weight applied because weighting is a local range property.
        return shardStart().size(end());
    }

    /**
     * Non-nullable implementation of {@link ShardTracker#shardEnd()}. Returns the first token if the current shard
     * is the last shard.
     * @return the end token of the current shard
     */
    private Token end()
    {
        Token end = shardEnd();
        return end != null ? end : sortedTokens[0];
    }

    @Override
    public boolean advanceTo(Token nextToken)
    {
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
        return sortedTokens.length;
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
