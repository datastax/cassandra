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

package org.apache.cassandra.index.sai.disk.v1.trie;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.cassandra.db.tries.Direction;
import org.apache.cassandra.db.tries.PatternMatcher;
import org.apache.cassandra.db.tries.TriePattern;
import org.apache.cassandra.io.tries.PatternValueIterator;
import org.apache.cassandra.io.util.Rebufferer;
import org.apache.cassandra.io.util.SizedInts;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

/**
 * Page-aware reader of the terms of a trie terms dictionary written by {@link TrieTermsDictionaryWriter} whose keys
 * match a pattern, in ascending or descending order. The terms trie is walked with the pattern, which skips the
 * branches that cannot match (see {@link PatternValueIterator}).
 * <p>
 * The pattern is matched against the stored bytes of the terms, which only makes sense when they are the raw bytes of
 * the indexed values, i.e. for non-composite literal types (see {@code OnDiskFormat#encodeForTrie}).
 */
@NotThreadSafe
public class PatternTermsDictionaryReader extends PatternValueIterator<PatternTermsDictionaryReader> implements TermsDictionaryIterator
{
    /**
     * @param ascending whether to list the terms in ascending or descending order
     * @param collecting whether to collect the keys of the terms, needed by {@link #next()}
     */
    public PatternTermsDictionaryReader(Rebufferer rebufferer,
                                        long root,
                                        TriePattern pattern,
                                        boolean ascending,
                                        boolean collecting,
                                        ByteComparable.Version version)
    {
        super(rebufferer, root, PatternMatcher.create(pattern), ascending ? Direction.FORWARD : Direction.REVERSE, collecting, version);
    }

    @Override
    public long nextAsLong()
    {
        return nextValueAsLong(this::getCurrentPayload, TrieTermsDictionaryReader.NOT_FOUND);
    }

    @Override
    public boolean hasNext()
    {
        return super.hasNext();
    }

    @Override
    public Pair<ByteComparable, Long> next()
    {
        return nextValue(() -> Pair.create(collectedKey(), getCurrentPayload()));
    }

    private long getCurrentPayload()
    {
        int bytes = payloadFlags();
        return bytes == 0 ? TrieTermsDictionaryReader.NOT_FOUND : SizedInts.read(buf, payloadPosition(), bytes);
    }
}
