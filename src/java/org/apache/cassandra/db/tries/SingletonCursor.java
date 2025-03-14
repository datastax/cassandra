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

/// Trie cursor for a singleton trie, mapping a given key to a value.
class SingletonCursor<T> implements Cursor<T>
{
    private final Direction direction;
    private ByteSource src;
    private final ByteComparable.Version byteComparableVersion;
    private final T value;
    private int currentDepth = 0;
    private int currentTransition = -1;
    private int nextTransition;


    public SingletonCursor(Direction direction, ByteSource src, ByteComparable.Version byteComparableVersion, T value)
    {
        this.src = src;
        this.direction = direction;
        this.byteComparableVersion = byteComparableVersion;
        this.value = value;
        this.nextTransition = src.next();
    }

    @Override
    public int advance()
    {
        currentTransition = nextTransition;
        if (currentTransition != ByteSource.END_OF_STREAM)
        {
            nextTransition = src.next();
            return ++currentDepth;
        }
        else
        {
            return done();
        }
    }

    @Override
    public int advanceMultiple(TransitionsReceiver receiver)
    {
        if (nextTransition == ByteSource.END_OF_STREAM)
            return done();
        int current = nextTransition;
        int depth = currentDepth;
        int next = src.next();
        while (next != ByteSource.END_OF_STREAM)
        {
            if (receiver != null)
                receiver.addPathByte(current);
            current = next;
            next = src.next();
            ++depth;
        }
        currentTransition = current;
        nextTransition = next;
        return currentDepth = ++depth;
    }

    @Override
    public int skipTo(int skipDepth, int skipTransition)
    {
        if (skipDepth <= currentDepth)
        {
            assert skipDepth < currentDepth || direction.gt(skipTransition, currentTransition);
            return done();  // no alternatives
        }
        if (direction.gt(skipTransition, nextTransition))
            return done();   // request is skipping over our path

        return advance();
    }

    private int done()
    {
        currentTransition = -1;
        return currentDepth = -1;
    }

    @Override
    public int depth()
    {
        return currentDepth;
    }

    @Override
    public T content()
    {
        return nextTransition == ByteSource.END_OF_STREAM ? value : null;
    }

    @Override
    public int incomingTransition()
    {
        return currentTransition;
    }

    @Override
    public Direction direction()
    {
        return direction;
    }

    @Override
    public ByteComparable.Version byteComparableVersion()
    {
        return byteComparableVersion;
    }

    @Override
    public SingletonCursor tailCursor(Direction dir)
    {
        if (!(src instanceof ByteSource.Duplicatable))
            src = ByteSource.duplicatable(src);
        ByteSource.Duplicatable duplicatableSource = (ByteSource.Duplicatable) src;

        return new SingletonCursor(dir, duplicatableSource.duplicate(), byteComparableVersion, value);
    }
}
