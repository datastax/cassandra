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

/// Prefixed cursor. Prepends the given prefix to all keys of the supplied cursor.
abstract class PrefixedCursor<T, C extends Cursor<T>> implements Cursor<T>
{
    final C tail;
    ByteSource prefixBytes;
    int nextPrefixByte;
    long currentPosition;
    long depthAdjustment;

    PrefixedCursor(ByteComparable prefix, C tail)
    {
        this(prefix.asComparableBytes(tail.byteComparableVersion()), tail);
    }

    PrefixedCursor(ByteSource prefix, C tail)
    {
        this(prefix.next(), prefix, tail);
    }

    PrefixedCursor(int firstPrefixByte, ByteSource prefix, C tail)
    {
        this.tail = tail;
        prefixBytes = prefix;
        tail.assertFresh();
        currentPosition = tail.encodedPosition(); // initial position with the correct direction
        nextPrefixByte = firstPrefixByte;
    }

    long completeAdvanceInTail(long positionInTail)
    {
        if (Cursor.isExhausted(positionInTail))
            return exhausted();

        currentPosition = positionInTail + depthAdjustment;
        return currentPosition;
    }

    boolean prefixDone()
    {
        return nextPrefixByte == ByteSource.END_OF_STREAM;
    }

    @Override
    public long encodedPosition()
    {
        return currentPosition;
    }

    @Override
    public long advance()
    {
        if (prefixDone())
            return completeAdvanceInTail(tail.advance());

        currentPosition = Cursor.positionForDescentWithByte(currentPosition, nextPrefixByte);
        nextPrefixByte = prefixBytes.next();
        checkPrefixDone();
        return currentPosition;
    }

    @Override
    public long advanceMultiple(TransitionsReceiver receiver)
    {
        if (prefixDone())
            return completeAdvanceInTail(tail.advanceMultiple(receiver));

        long pos = currentPosition;
        int incomingTransition = nextPrefixByte;
        int nextPrefixByte = prefixBytes.next();

        while (nextPrefixByte != ByteSource.END_OF_STREAM)
        {
            receiver.addPathByte(incomingTransition);
            pos += DEPTH_ADJUSTMENT_ONE;
            incomingTransition = nextPrefixByte;
            nextPrefixByte = prefixBytes.next();
        }
        currentPosition = Cursor.positionForDescentWithByte(pos, incomingTransition);
        checkPrefixDone();
        // We can't continue with a tail advance, because there may be content or other features at the tail's root.
        return currentPosition;
    }

    @Override
    public long skipTo(long encodedSkipPosition)
    {
        if (prefixDone())
            return completeAdvanceInTail(tail.skipTo(encodedSkipPosition - depthAdjustment));

        long nextPosition = Cursor.positionForDescentWithByte(currentPosition, nextPrefixByte);
        if (Cursor.compare(encodedSkipPosition, nextPosition) > 0)
            return exhausted();
        assert Cursor.depth(encodedSkipPosition) == Cursor.depth(nextPosition)
            : "Invalid advance request to " + Cursor.toString(encodedSkipPosition) +
              " to cursor at " + Cursor.toString(currentPosition);
        nextPrefixByte = prefixBytes.next();
        currentPosition = nextPosition;
        checkPrefixDone();
        return currentPosition;
    }

    private void checkPrefixDone()
    {
        if (nextPrefixByte == ByteSource.END_OF_STREAM)
            depthAdjustment = Cursor.depthCorrectionValue(currentPosition);
    }

    private long exhausted()
    {
        currentPosition = Cursor.exhaustedPosition(currentPosition);
        return currentPosition;
    }

    public ByteComparable.Version byteComparableVersion()
    {
        return tail.byteComparableVersion();
    }

    @Override
    public T content()
    {
        return prefixDone() ? tail.content() : null;
    }

    ByteSource.Duplicatable duplicateSource()
    {
        if (!(prefixBytes instanceof ByteSource.Duplicatable))
            prefixBytes = ByteSource.duplicatable(prefixBytes);
        ByteSource.Duplicatable duplicatableSource = (ByteSource.Duplicatable) prefixBytes;
        return duplicatableSource.duplicate();
    }

    static class Plain<T> extends PrefixedCursor<T, Cursor<T>> implements Cursor<T>
    {
        Plain(ByteComparable prefix, Cursor<T> tail)
        {
            super(prefix, tail);
        }

        Plain(int firstPrefixByte, ByteSource prefix, Cursor<T> source)
        {
            super(firstPrefixByte, prefix, source);
        }

        @Override
        public Cursor<T> tailCursor(Direction direction)
        {
            assert !Cursor.isExhausted(currentPosition) : "tailTrie called on exhausted cursor";

            if (prefixDone())
                return tail.tailCursor(direction);
            else
                return new Plain<>(nextPrefixByte, duplicateSource(), tail.tailCursor(direction));
        }
    }

    static class Range<S extends RangeState<S>> extends PrefixedCursor<S, RangeCursor<S>> implements RangeCursor<S>
    {
        Range(ByteComparable prefix, RangeCursor<S> tail)
        {
            super(prefix, tail);
        }

        Range(int firstPrefixByte, ByteSource prefix, RangeCursor<S> source)
        {
            super(firstPrefixByte, prefix, source);
        }

        @Override
        public S state()
        {
            if (prefixDone())
                return tail.state();
            return null;
        }

        @Override
        public RangeCursor<S> tailCursor(Direction direction)
        {
            assert !Cursor.isExhausted(currentPosition) : "tailTrie called on exhausted cursor";

            if (prefixDone())
                return tail.tailCursor(direction);
            else
                return new Range<>(nextPrefixByte, duplicateSource(), tail.tailCursor(direction));
        }
    }

    static class DeletionAware<T, D extends RangeState<D>>
    extends PrefixedCursor<T, DeletionAwareCursor<T, D>> implements DeletionAwareCursor<T, D>
    {
        DeletionAware(ByteComparable prefix, DeletionAwareCursor<T, D> tail)
        {
            super(prefix, tail);
        }

        DeletionAware(int firstPrefixByte, ByteSource prefix, DeletionAwareCursor<T, D> tail)
        {
            super(firstPrefixByte, prefix, tail);
        }

        @Override
        public RangeCursor<D> deletionBranchCursor(Direction direction)
        {
            return prefixDone() ? tail.deletionBranchCursor(direction) : null;
        }

        @Override
        public DeletionAwareCursor<T, D> tailCursor(Direction direction)
        {
            assert !Cursor.isExhausted(currentPosition) : "tailTrie called on exhausted cursor";

            if (prefixDone())
                return tail.tailCursor(direction);
            else
            {
                return new DeletionAware<>(nextPrefixByte, duplicateSource(), tail.tailCursor(direction));
            }
        }
    }
}
