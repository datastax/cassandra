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

import org.junit.Test;

import org.apache.cassandra.db.SortedLocalRanges;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.mockito.Mockito;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

public class DelegatingShardManagerTest
{
    final IPartitioner partitioner = Murmur3Partitioner.instance;

    @Test
    public void testWrappingShardManagerNoDisks()
    {
        CompactionRealm realm = Mockito.mock(CompactionRealm.class);
        when(realm.getPartitioner()).thenReturn(partitioner);
        when(realm.estimatedPartitionCount()).thenReturn(1L << 16);
        SortedLocalRanges localRanges = SortedLocalRanges.forTestingFull(realm);
        ShardManager delegate = new ShardManagerNoDisks(localRanges);

        DelegatingShardManager wrapper = new DelegatingShardManager((x) -> consumeTokens(delegate.boundaries(x)), realm);

        var range = new Range<>(partitioner.getMinimumToken(), partitioner.getMinimumToken());
        assertEquals(1, wrapper.rangeSpanned(range), 0);
        assertEquals(1, wrapper.localSpaceCoverage(), 0);
        assertEquals(1, wrapper.shardSetCoverage(), 0);
        assertEquals(1D / (1L << 16), wrapper.minimumPerPartitionSpan(), 0);

        // We expect the same shards because the wrapper delegates.
        for (int i = 1; i < 512; i++)
        {
            var wrapperTokenTracker = wrapper.boundaries(i);

            // Make assertion about the first shard's fraction of the whole token range. This assertion relies
            // on the fact that the delegate shard manager splits the space evenly for the given number of tokens.
            var fractionInShard = wrapperTokenTracker.fractionInShard(range);
            assertEquals(1d / i, fractionInShard, 0.001);

            Token[] actualTokens = consumeTokens(wrapperTokenTracker);
            Token[] expectedTokens = consumeTokens(delegate.boundaries(i));
            assertArrayEquals(actualTokens, expectedTokens);
        }
    }

    private Token[] consumeTokens(ShardTracker iterator)
    {
        Token[] actualTokens = new Token[iterator.count()];
        actualTokens[iterator.shardIndex()] = iterator.shardStart();
        for (Token end = iterator.shardEnd(); end != null; end = iterator.shardEnd())
        {
            assertFalse(iterator.advanceTo(end));
            assertTrue(iterator.advanceTo(end.nextValidToken()));
            actualTokens[iterator.shardIndex()] = (end);
        }
        return actualTokens;
    }
}
