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

import java.util.Arrays;

import com.google.common.base.Preconditions;

import org.agrona.DirectBuffer;
import org.apache.cassandra.utils.Hex;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

public interface VerificationCursor
{
    int EXHAUSTED_DEPTH = -1;
    int EXHAUSTED_TRANSITION = -1;
    int INITIAL_TRANSITION = -1;

    /// Verifies:
    /// - `advance` does advance, `depth <= prevDepth + 1` and transition is higher than previous at the same depth
    ///   (this requires path tracking)
    /// - `skipTo` is not called with earlier or equal position (including lower levels)
    /// - `maybeSkipTo` is not called with earlier position that can't be identified with depth/incomingTransition only
    ///   (i.e. seeks to lower depth with an incoming transition that lower than the previous at that depth)
    /// - exhausted state is `depth = -1, incomingTransition = -1`
    /// - start state is `depth = 0, incomingTransition = -1`
    class Plain<T, C extends Cursor<T>> implements Cursor<T>, Cursor.TransitionsReceiver
    {
        final Direction direction;
        final C source;
        final int minDepth;
        int returnedDepth;
        int returnedTransition;
        byte[] path;

        Cursor.TransitionsReceiver chainedReceiver = null;
        boolean advanceMultipleCalledReceiver;

        Plain(C cursor, int minDepth, int expectedDepth, int expectedTransition)
        {
            this.direction = cursor.direction();
            this.source = cursor;
            this.minDepth = minDepth;
            this.returnedDepth = expectedDepth;
            this.returnedTransition = expectedTransition;
            this.path = new byte[16];
            Preconditions.checkState(source.depth() == expectedDepth && source.incomingTransition() == expectedTransition,
                                     "Invalid initial depth %s with incoming transition %s (must be %s, %s)",
                                     source.depth(), source.incomingTransition(),
                                     expectedDepth, expectedTransition);
        }

        @Override
        public int depth()
        {
            Preconditions.checkState(returnedDepth == source.depth(),
                                     "Depth changed without advance: %s -> %s", returnedDepth, source.depth());
            return returnedDepth;
        }

        @Override
        public int incomingTransition()
        {
            Preconditions.checkState(returnedTransition == source.incomingTransition(),
                                     "Transition changed without advance: %s -> %s", returnedTransition, source.incomingTransition());
            return source.incomingTransition();
        }

        @Override
        public T content()
        {
            return source.content();
        }

        @Override
        public Direction direction()
        {
            return direction;
        }

        @Override
        public ByteComparable.Version byteComparableVersion()
        {
            return source.byteComparableVersion();
        }

        @Override
        public int advance()
        {
            return verify(source.advance());
        }

        @Override
        public int advanceMultiple(Cursor.TransitionsReceiver receiver)
        {
            advanceMultipleCalledReceiver = false;
            chainedReceiver = receiver;
            int depth = source.advanceMultiple(this);
            chainedReceiver = null;
            Preconditions.checkState(!advanceMultipleCalledReceiver || depth == returnedDepth + 1,
                                     "advanceMultiple returned depth %s did not match depth %s after added characters",
                                     depth, returnedDepth + 1);
            return verify(depth);
        }

        @Override
        public int skipTo(int skipDepth, int skipTransition)
        {
            verifySkipRequest(skipDepth, skipTransition);
            return verify(source.skipTo(skipDepth, skipTransition));
        }

        private void verifySkipRequest(int skipDepth, int skipTransition)
        {
            Preconditions.checkState(skipDepth <= returnedDepth + 1,
                                     "Skip descends more than one level: %s -> %s",
                                     returnedDepth,
                                     skipDepth);
            if (skipDepth <= returnedDepth && skipDepth > minDepth)
                Preconditions.checkState(direction.lt(getByte(skipDepth), skipTransition),
                                         "Skip goes backwards to %s at depth %s where it already visited %s",
                                         skipTransition, skipDepth, getByte(skipDepth));

        }

        private int verify(int depth)
        {
            Preconditions.checkState(depth <= returnedDepth + 1,
                                     "Cursor advanced more than one level: %s -> %s",
                                     returnedDepth,
                                     depth);
            Preconditions.checkState(depth < 0 || depth > minDepth,
                                     "Cursor ascended to depth %s beyond its minimum depth %s",
                                     depth, minDepth);
            final int transition = source.incomingTransition();
            if (depth < 0)
            {
                Preconditions.checkState(depth == EXHAUSTED_DEPTH && transition == EXHAUSTED_TRANSITION,
                                         "Cursor exhausted state should be %s, %s but was %s, %s",
                                         EXHAUSTED_DEPTH, EXHAUSTED_TRANSITION,
                                         depth, transition);
            }
            else if (depth <= returnedDepth)
            {
                Preconditions.checkState(direction.lt(getByte(depth), transition),
                                         "Cursor went backwards to %s at depth %s where it already visited %s",
                                         transition, depth, getByte(depth));
            }
            returnedDepth = depth;
            returnedTransition = transition;
            if (depth >= 0)
                addByte(returnedTransition, depth);
            return depth;
        }

        @Override
        @SuppressWarnings("unchecked")
        public Plain<T, C> tailCursor(Direction direction)
        {
            return new Plain<>((C) source.tailCursor(direction), 0, 0, INITIAL_TRANSITION);
        }


        @Override
        public void addPathByte(int nextByte)
        {
            addByte(nextByte, ++returnedDepth);
            returnedTransition = nextByte;
            if (chainedReceiver != null)
                chainedReceiver.addPathByte(nextByte);
        }

        private void addByte(int nextByte, int depth)
        {
            int index = depth - minDepth - 1;
            if (index >= path.length)
                path = Arrays.copyOf(path, path.length * 2);
            path[index] = (byte) nextByte;
        }

        private int getByte(int depth)
        {
            return path[depth - minDepth - 1] & 0xFF;
        }

        @Override
        public void addPathBytes(DirectBuffer buffer, int pos, int count)
        {
            for (int i = 0; i < count; ++i)
                addByte(buffer.getByte(pos + i), returnedDepth + 1 + i);
            returnedDepth += count;
            returnedTransition = buffer.getByte(pos + count - 1) & 0xFF;
            if (chainedReceiver != null)
                chainedReceiver.addPathBytes(buffer, pos, count);
        }

        @Override
        public String toString()
        {
            StringBuilder builder = new StringBuilder();
            builder.append(source.getClass().getTypeName()
                                 .replace(source.getClass().getPackageName() + '.', ""));
            if (returnedDepth < 0)
            {
                builder.append(" exhausted");
            }
            else
            {
                builder.append(" at ");
                builder.append(Hex.bytesToHex(path, 0, returnedDepth - minDepth));
            }
            return builder.toString();
        }
    }

    class TrieSet extends Plain<TrieSetCursor.RangeState, TrieSetCursor> implements TrieSetCursor
    {
        boolean currentPrecedingIncluded;
        boolean nextPrecedingIncluded;

        TrieSet(TrieSetCursor source)
        {
            this(source, 0, 0, INITIAL_TRANSITION);
        }

        TrieSet(TrieSetCursor source, int minDepth, int expectedDepth, int expectedTransition)
        {
            super(source, minDepth, expectedDepth, expectedTransition);
            // start state can be non-null for sets
            currentPrecedingIncluded = source.precedingIncluded();
            Preconditions.checkNotNull(currentPrecedingIncluded, "Covering state for trie sets must not be null");
            nextPrecedingIncluded = source.content() != null ? source.content().precedingIncluded(direction.opposite()) : currentPrecedingIncluded;
        }

        void verifyEndState()
        {
            // end state can be non-null for sets
        }

        @Override
        public TrieSetCursor.RangeState state()
        {
            return Preconditions.checkNotNull(source.state());
        }

        @Override
        public TrieSet tailCursor(Direction direction)
        {
            return new TrieSet(source.tailCursor(direction), 0, 0, INITIAL_TRANSITION);
        }

        @Override
        public int advance()
        {
            currentPrecedingIncluded = nextPrecedingIncluded;
            return verifyState(super.advance());
        }

        @Override
        public int advanceMultiple(TransitionsReceiver receiver)
        {
            currentPrecedingIncluded = nextPrecedingIncluded;
            return verifyState(super.advanceMultiple(receiver));
        }

        @Override
        public int skipTo(int skipDepth, int skipTransition)
        {
            return verifySkipState(super.skipTo(skipDepth, skipTransition));
        }

        @Override
        public boolean precedingIncluded()
        {
            Preconditions.checkState(currentPrecedingIncluded == source.precedingIncluded(),
                                     "Covering state changed without advance: %s -> %s. %s",
                                     currentPrecedingIncluded, source.precedingIncluded(),
                                     currentPrecedingIncluded == source.precedingIncluded()
                                     ? "The values are equal but different object. This is not permitted for performance reasons."
                                     : "");
            // == above is correct, we do not want covering state to be recreated unless some change happened to the cursor
            return currentPrecedingIncluded;
        }

        private int verifyState(int depth)
        {
            boolean precedingIncluded = source.precedingIncluded();
            Preconditions.checkNotNull(precedingIncluded, "Covering state for trie sets must not be null");
            Preconditions.checkState(currentPrecedingIncluded == precedingIncluded,
                                     "Unexpected change to covering state: %s -> %s",
                                     currentPrecedingIncluded, precedingIncluded);
            currentPrecedingIncluded = precedingIncluded;

            RangeState content = source.content();
            if (content != null)
            {
                Preconditions.checkState(currentPrecedingIncluded == content.precedingIncluded(direction),
                                         "Range end %s does not close covering state %s",
                                         content.precedingIncluded(direction), currentPrecedingIncluded);
                nextPrecedingIncluded = content.precedingIncluded(direction.opposite());
            }

            if (depth < 0)
                verifyEndState();
            return depth;
        }

        private int verifySkipState(int depth)
        {
            // The covering state information is invalidated by a skip.
            currentPrecedingIncluded = source.precedingIncluded();
            Preconditions.checkNotNull(currentPrecedingIncluded, "Covering state for trie sets must not be null");
            nextPrecedingIncluded = currentPrecedingIncluded;
            return verifyState(depth);
        }
    }
}
