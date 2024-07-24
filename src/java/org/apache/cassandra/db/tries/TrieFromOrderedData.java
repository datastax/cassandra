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

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.function.Function;

public abstract class TrieFromOrderedData<I, O> extends Trie<O>
{

    // If used in reverse direction, input must be prefix-free!
    @Override
    protected Trie.Cursor<O> cursor(Direction direction)
    {
        Iterator<I> iterator = getIterator(direction);
        if (!iterator.hasNext())
            return Trie.<O>empty().cursor(direction);
        return new Cursor(direction, iterator);
    }

    abstract public Iterator<I> getIterator(Direction direction);
    abstract public ByteComparable keyFor(I entry);
    abstract public O contentFor(I entry);

    class Cursor implements Trie.Cursor<O>
    {
        final Direction direction;
        final Iterator<I> iterator;

        I currentEntry;
        ByteSource currentKey;
        byte[] bytes;
        int depth;
        int next;

        Cursor(Direction direction, Iterator<I> iterator)
        {
            this.direction = direction;
            this.iterator = iterator;
            depth = 0;
            bytes = new byte[232];
            // hasNext is checked by cursor() above
            next = getNextEntryAndItsFirstByte();
        }

        @Override
        public int depth()
        {
            return depth;
        }

        @Override
        public int incomingTransition()
        {
            return depth > 0 ? getByte(depth - 1) : -1;
        }

        @Override
        public O content()
        {
            return atCompletedEntry() ? contentFor(currentEntry) : null;
        }

        @Override
        public Direction direction()
        {
            return direction;
        }

        private void setByte(int depth, int value)
        {
            if (depth >= bytes.length)
                bytes = Arrays.copyOf(bytes, bytes.length * 2);
            bytes[depth] = (byte) value;
        }

        private int getByte(int depth)
        {
            return bytes[depth] & 0xFF;
        }

        private boolean atCompletedEntry()
        {
            return next == ByteSource.END_OF_STREAM;
        }

        private int descendWith(int depth, int next)
        {
            setByte(depth, next);
            this.next = currentKey.next();
            return this.depth = depth + 1;
        }

        private int exhausted()
        {
            return depth = -1;
        }

        @Override
        public int advance()
        {
            if (!atCompletedEntry())
                return descendWith(depth, next);
            else
                return advanceToNextEntry();
        }

        private int advanceToNextEntry()
        {
            if (iterator.hasNext())
            {
                int n = getNextEntryAndItsFirstByte();
                int d = 0;
                while (d < depth && n == getByte(d))
                {
                    ++d;
                    n = currentKey.next();
                }
                assert d == depth || direction.gt(n, getByte(d)) : "Invalid key order";
                return descendWith(d, n);
            }
            else
                return exhausted();
        }

        private int getNextEntryAndItsFirstByte()
        {
            currentEntry = iterator.next();
            currentKey = keyFor(currentEntry).asComparableBytes(Trie.BYTE_COMPARABLE_VERSION);
            return currentKey.next();
        }

        @Override
        public int advanceMultiple(TransitionsReceiver receiver)
        {
            if (atCompletedEntry())
                return advanceToNextEntry();

            int c = next;
            int n = currentKey.next();
            int d = depth;
            while (n != ByteSource.END_OF_STREAM)
            {
                setByte(d, c);
                if (receiver != null)
                    receiver.addPathByte(c);
                c = n;
                n = currentKey.next();
                ++d;
            }
            setByte(d, c);
            next = n;
            return depth = d + 1;
        }

        @Override
        public int skipTo(int skipDepth, int skipTransition)
        {
            assert skipDepth <= depth + 1 : "Invalid skip request";
            if (skipDepth > depth && direction.le(skipTransition, next))
                return advance();

            setByte(skipDepth - 1, skipTransition);

            while (true)
            {
                if (!iterator.hasNext())
                    return exhausted();

                int n = getNextEntryAndItsFirstByte();
                int d = 0;
                while (true)
                {
                    if (d == skipDepth)
                    {
                        next = n;
                        return depth = d;
                    }

                    int target = getByte(d);
                    if (direction.gt(n, target))
                        return descendWith(d, n);

                    if (direction.lt(n, target))
                        break;  // skip this entry, it is before the target position

                    ++d;
                    n = currentKey.next();
                }
            }
        }

        @Override
        public Trie<O> tailTrie()
        {
            throw new UnsupportedOperationException();
        }
    }

    public static class FromMap<K, V, O> extends TrieFromOrderedData<Map.Entry<K, V>, O>
    {
        final NavigableMap<K, V> map;
        final Function<K, ByteComparable> keyTransformer;
        final Function<V, O> contentTransformer;

        public FromMap(NavigableMap<K, V> map, Function<K, ByteComparable> keyTransformer, Function<V, O> contentTransformer)
        {
            this.map = map;
            this.keyTransformer = keyTransformer;
            this.contentTransformer = contentTransformer;
        }

        @Override
        public Iterator<Map.Entry<K, V>> getIterator(Direction direction)
        {
            return (direction.isForward() ? map : map.descendingMap()).entrySet().iterator();
        }

        @Override
        public ByteComparable keyFor(Map.Entry<K, V> entry)
        {
            return keyTransformer.apply(entry.getKey());
        }

        @Override
        public O contentFor(Map.Entry<K, V> entry)
        {
            return contentTransformer.apply(entry.getValue());
        }
    }
}
