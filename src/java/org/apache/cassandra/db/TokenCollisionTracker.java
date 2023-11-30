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

package org.apache.cassandra.db;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.agrona.collections.LongHashSet;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.FilterFactory;
import org.apache.cassandra.utils.IFilter;

public class TokenCollisionTracker implements ITokenCollisionTracker
{
    private static final Logger logger = LoggerFactory.getLogger(TokenCollisionTracker.class);

    private final LongHashSet tokenCollisions;
    private final IFilter allTokens;

    public TokenCollisionTracker(long estimatedEntries)
    {
        tokenCollisions = new LongHashSet();
        // 10 buckets per element gives us < 1% false positive rate; see BloomCalculations.probs
        allTokens = FilterFactory.getFilter(estimatedEntries, 10);
    }

    public static TokenCollisionTracker build(Collection<SSTableReader> sstables, Function<ByteBuffer, Token> tokenProvider)
    {
        var nRows = sstables.stream().mapToLong(SSTableReader::estimatedKeys).sum();
        var tracker = new TokenCollisionTracker(nRows);
        for (var sstable: sstables)
        {
            try (var it = sstable.allKeysIterator())
            {
                do
                {
                    // VSTODO this is inefficient since we only care about the token, not the DK
                    Token token = tokenProvider.apply(it.key());
                    tracker.addToken(token);
                } while (it.advance());
            }
            catch (IOException e)
            {
                throw new UncheckedIOException(e);
            }
        }
        logger.info("For {} rows across {} SSTables, token collision count (including BF false positives) is {}.",
                    nRows,
                    sstables.size(),
                    tracker.getCollisionCount());
        return tracker;
    }

    private int getCollisionCount()
    {
        return tokenCollisions.size();
    }

    private void addToken(Token token)
    {
        var key = new TokenFilterKey(token);
        if (allTokens.isPresent(key))
        {
            // VSTODO we could massively reduce our false positive rate by checking individual sstable BFs here
            // as well, when the global BF says we have a collision.
            tokenCollisions.add(token.getLongValue());
        }
        allTokens.add(key);
    }

    @Override
    public boolean isUnique(Token token)
    {
        return !tokenCollisions.contains(token.getLongValue());
    }

    private static class TokenFilterKey implements IFilter.FilterKey
    {
        private final Token token;

        public TokenFilterKey(Token token)
        {
            this.token = token;
        }

        @Override
        public void filterHash(long[] dest)
        {
            var m = token.getLongValue();
            dest[0] = m >> 32;
            dest[1] = (int) m;
        }
    }
}
