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

package org.apache.cassandra.db.tries;

import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

/**
 * Prefixed trie. Represents the content of the given trie with the prefix prepended to all keys.
 */
public class PrefixedTrie<T> extends Trie<T>
{
    final ByteComparable prefix;
    final Trie<T> trie;

    public PrefixedTrie(ByteComparable prefix, Trie<T> trie)
    {
        this.prefix = prefix;
        this.trie = trie;
    }

    @Override
    protected Trie.Cursor<T> cursor(Direction direction)
    {
        return new Cursor<>(prefix.asComparableBytes(Trie.BYTE_COMPARABLE_VERSION), trie.cursor(direction));
    }

    private static class Cursor<T> implements Trie.Cursor<T>
    {
        final Trie.Cursor<T> source;
        ByteSource prefixBytes;
        int nextByte;
        int currentByte;
        int depthOfPrefix;

        Cursor(ByteSource prefix, Trie.Cursor<T> source)
        {
            this.source = source;
            prefixBytes = prefix;
            currentByte = -1;
            nextByte = prefixBytes.next();
            depthOfPrefix = 0;
        }

        int addPrefixDepthAndCheckDone(int depthInBranch)
        {
            currentByte = source.incomingTransition();
            if (depthInBranch < 0)
                depthOfPrefix = 0;

            return depthInBranch + depthOfPrefix;
        }

        boolean prefixDone()
        {
            return nextByte == ByteSource.END_OF_STREAM;
        }

        @Override
        public int depth()
        {
            if (prefixDone())
                return source.depth() + depthOfPrefix;
            else
                return depthOfPrefix;
        }

        @Override
        public int incomingTransition()
        {
            return currentByte;
        }

        @Override
        public int advance()
        {
            if (prefixDone())
                return addPrefixDepthAndCheckDone(source.advance());

            ++depthOfPrefix;
            currentByte = nextByte;
            nextByte = prefixBytes.next();
            return depthOfPrefix;
        }

        @Override
        public int advanceMultiple(Trie.TransitionsReceiver receiver)
        {
            if (prefixDone())
                return addPrefixDepthAndCheckDone(source.advanceMultiple(receiver));

            while (!prefixDone())
            {
                receiver.addPathByte(currentByte);
                ++depthOfPrefix;
                currentByte = nextByte;
                nextByte = prefixBytes.next();
            }
            return depthOfPrefix;
        }

        @Override
        public int skipTo(int skipDepth, int skipTransition)
        {
            // regardless if we exhausted prefix, if caller asks for depth <= prefix depth, we're done.
            if (skipDepth <= depthOfPrefix)
                return exhausted();
            if (prefixDone())
                return addPrefixDepthAndCheckDone(source.skipTo(skipDepth - depthOfPrefix, skipTransition));
            if (skipDepth == depthOfPrefix + 1 && source.direction().gt(skipTransition, nextByte))
                return exhausted();
            return advance();
        }

        private int exhausted()
        {
            currentByte = -1;
            nextByte = ByteSource.END_OF_STREAM;
            depthOfPrefix = -1;
            return depthOfPrefix;
        }

        public Direction direction()
        {
            return source.direction();
        }

        @Override
        public T content()
        {
            return prefixDone() ? source.content() : null;
        }

        @Override
        public Trie<T> tailTrie()
        {
            if (prefixDone())
                return source.tailTrie();
            else
            {
                if (!(prefixBytes instanceof ByteSource.Duplicatable))
                    prefixBytes = ByteSource.duplicatable(prefixBytes);
                ByteSource.Duplicatable duplicatableSource = (ByteSource.Duplicatable) prefixBytes;

                return new PrefixedTrie<>(v -> duplicatableSource.duplicate(), source.tailTrie());
            }
        }
    }
}
