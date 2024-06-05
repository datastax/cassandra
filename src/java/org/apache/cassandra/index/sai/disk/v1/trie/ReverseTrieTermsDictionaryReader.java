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

import java.nio.ByteBuffer;
import java.util.Iterator;

import org.apache.cassandra.io.tries.ReverseValueIterator;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.Rebufferer;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.SizedInts;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

/**
 * Page-aware reverse iterator reader for a trie terms dictionary written by {@link TrieTermsDictionaryWriter}.
 */
public class ReverseTrieTermsDictionaryReader extends ReverseValueIterator<ReverseTrieTermsDictionaryReader> implements Iterator<Pair<ByteComparable, Long>>
{
    public ReverseTrieTermsDictionaryReader(Rebufferer rebufferer, long root)
    {
        super(rebufferer, root, true);
    }

    @Override
    public boolean hasNext()
    {
        return super.hasNext();
    }

    @Override
    public Pair<ByteComparable, Long> next()
    {
        return nextValue(this::getKeyAndPayload);
    }

    private Pair<ByteComparable, Long> getKeyAndPayload()
    {
        return Pair.create(collectedKey(), getPayload(buf, payloadPosition(), payloadFlags()));
    }

    private static long getPayload(ByteBuffer contents, int payloadPos, int bytes)
    {
        return SizedInts.read(contents, payloadPos, bytes);
    }
}
