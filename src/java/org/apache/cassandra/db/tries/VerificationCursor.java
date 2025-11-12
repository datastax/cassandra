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
import java.util.Objects;

import com.google.common.base.Preconditions;

import org.agrona.DirectBuffer;
import org.apache.cassandra.utils.Hex;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

public interface VerificationCursor
{
    /// Verifies:
    /// - `advance` does advance, `depth <= prevDepth + 1` and transition is higher than previous at the same depth
    ///   (this requires path tracking)
    /// - `skipTo` is not called with earlier or equal position (including lower levels)
    /// - `maybeSkipTo` is not called with earlier position that can't be identified with depth/incomingTransition only
    ///   (i.e. seeks to lower depth with an incoming transition that lower than the previous at that depth)
    /// - exhausted state matches `Cursor.exhaustedPosition(direction)`
    /// - start state matches `Cursor.rootPosition(direction)`
    class Plain<T, C extends Cursor<T>> implements Cursor<T>, Cursor.TransitionsReceiver
    {
        static
        {
            try
            {
                assert false;
                throw new IllegalStateException("Assertions need to be turned on for verification cursors.");
            }
            catch (AssertionError e)
            {
                // correct path
            }
        }

        final Direction direction;
        final C source;
        long returnedPosition;
        byte[] path;

        Cursor.TransitionsReceiver chainedReceiver = null;
        boolean advanceMultipleCalledReceiver;

        Plain(C cursor)
        {
            this.direction = cursor.direction();
            this.source = cursor;
            this.returnedPosition = Cursor.rootPosition(direction);
            this.path = new byte[16];
            long reportedPosition = source.encodedPosition();
            assert Cursor.direction(reportedPosition) == direction :
                String.format("Invalid direction bit %d in root position %s (%016x)\n%s",
                              (reportedPosition >> DIRECTION_BIT) & 1,
                              Cursor.toString(reportedPosition),
                              reportedPosition,
                              this);
            assert Cursor.compare(reportedPosition, returnedPosition) == 0 :
                String.format("Invalid initial position %s (must be %s)\n%s",
                              Cursor.toString(reportedPosition),
                              Cursor.toString(returnedPosition),
                              this);
        }

        @Override
        public long encodedPosition()
        {
            assert Cursor.compare(source.encodedPosition(), returnedPosition) == 0 :
                String.format("Position changed without advance: %s -> %s\n%s",
                              Cursor.toString(returnedPosition),
                              Cursor.toString(source.encodedPosition()),
                              this);
            return returnedPosition;
        }

        @Override
        public T content()
        {
            return source.content();
        }

        @Override
        public ByteComparable.Version byteComparableVersion()
        {
            return source.byteComparableVersion();
        }

        @Override
        public long advance()
        {
            return verify(source.advance());
        }

        @Override
        public long advanceMultiple(Cursor.TransitionsReceiver receiver)
        {
            advanceMultipleCalledReceiver = false;
            chainedReceiver = receiver;
            // Note: if the code below calls the receiver (us), returnedPosition will be adjusted to reflect descent.
            long position = source.advanceMultiple(this);
            chainedReceiver = null;
            int depth = Cursor.depth(position);
            int prevDepth = Cursor.depth(returnedPosition);
            assert !advanceMultipleCalledReceiver || depth == prevDepth + 1 :
                String.format("advanceMultiple returned depth %s did not match depth %s after added characters\n%s",
                              depth,
                              prevDepth + 1,
                              this);
            return verify(position);
        }

        @Override
        public long skipTo(long encodedSkipPosition)
        {
            verifySkipRequest(encodedSkipPosition);
            return verify(source.skipTo(encodedSkipPosition));
        }

        private void verifySkipRequest(long encodedSkipPosition)
        {
            int skipDepth = Cursor.depth(encodedSkipPosition);
            int currDepth = Cursor.depth(returnedPosition);
            assert skipDepth <= currDepth + 1 :
                String.format("Skip descends more than one level: %s -> %s\n%s",
                              Cursor.toString(returnedPosition),
                              Cursor.toString(encodedSkipPosition),
                              this);
            int skipTransition = Cursor.undecodedTransition(encodedSkipPosition);
            if (skipDepth <= currDepth && skipDepth > 0)
                assert (getByte(skipDepth) ^ direction.select(0x00, 0xFF)) < skipTransition :
                    String.format("Skip goes backwards to %s where it already visited byte %s\n%s",
                                  Cursor.toString(encodedSkipPosition),
                                  getByte(skipDepth),
                                  this);

        }

        private long verify(long newPosition)
        {
            int newDepth = Cursor.depth(newPosition);
            int oldDepth = Cursor.depth(returnedPosition);
            assert newDepth <= oldDepth + 1 :
                String.format("Cursor advanced more than one level: %s -> %s\n%s",
                              Cursor.toString(returnedPosition),
                              Cursor.toString(newPosition),
                              this);
            assert Cursor.direction(newPosition) == direction :
                String.format("Invalid direction bit %d in position %s (%016x)\n%s",
                              (newPosition >> DIRECTION_BIT) & 1,
                              Cursor.toString(newPosition),
                              newPosition,
                              this);

            if (Cursor.isExhausted(newPosition))
            {
                assert Cursor.compare(newPosition, Cursor.exhaustedPosition(direction)) == 0 :
                    String.format("Cursor exhausted state should be %s but was %s\n%s",
                                  Cursor.toString(Cursor.exhaustedPosition(direction)),
                                  Cursor.toString(newPosition),
                                  this);
            }
            else
            {
                if (newDepth <= oldDepth)
                {
                    assert (getByte(newDepth) ^ direction.select(0x00, 0xFF))
                           < Cursor.undecodedTransition(newPosition) :
                        String.format("Cursor went backwards to %s where it already visited byte %s\n%s",
                                      Cursor.toString(newPosition),
                                      getByte(newDepth),
                                      this);
                }
                int undecodedTransition = Cursor.undecodedTransition(newPosition);
                assert undecodedTransition >= 0 && undecodedTransition <= 0xFF :
                    String.format("Cursor returned invalid incoming transition with state %s (%016x)\n%s",
                                  Cursor.toString(newPosition),
                                  newPosition,
                                  this);
                addByte(newPosition);
            }
            returnedPosition = newPosition;
            return newPosition;
        }

        @Override
        @SuppressWarnings("unchecked")
        public Plain<T, C> tailCursor(Direction direction)
        {
            return new Plain<>((C) source.tailCursor(direction));
        }


        @Override
        public void addPathByte(int nextByte)
        {
            advanceMultipleCalledReceiver = true;
            returnedPosition = Cursor.positionForDescentWithByte(returnedPosition, nextByte);
            addByte(returnedPosition);

            if (chainedReceiver != null)
                chainedReceiver.addPathByte(nextByte);
        }

        private void addByte(long asPosition)
        {
            addByte(Cursor.incomingTransition(asPosition), Cursor.depth(asPosition));
        }

        private void addByte(int nextByteEncoded, int depth)
        {
            int index = depth - 1;
            if (index >= path.length)
                path = Arrays.copyOf(path, path.length * 2);
            path[index] = (byte) nextByteEncoded;
        }

        private int getByte(int depth)
        {
            return path[depth - 1] & 0xFF;
        }

        @Override
        public void addPathBytes(DirectBuffer buffer, int pos, int count)
        {
            advanceMultipleCalledReceiver = true;
            for (int i = 0; i < count; ++i)
            {
                int nextByte = buffer.getByte(pos + i) & 0xFF;
                returnedPosition = Cursor.positionForDescentWithByte(returnedPosition, nextByte);
                addByte(returnedPosition);
            }
            if (chainedReceiver != null)
                chainedReceiver.addPathBytes(buffer, pos, count);
        }

        @Override
        public String toString()
        {
            StringBuilder builder = new StringBuilder();
            builder.append(source.getClass().getTypeName()
                                 .replace(source.getClass().getPackageName() + '.', ""));
            if (Cursor.isExhausted(returnedPosition))
            {
                builder.append(" exhausted");
            }
            else
            {
                builder.append(" at ");
                builder.append(Hex.bytesToHex(path, 0, Cursor.depth(returnedPosition)));
            }
            return builder.toString();
        }
    }

    abstract class WithRanges<S extends RangeState<S>, C extends RangeCursor<S>>
    extends Plain<S, C>
    implements RangeCursor<S>
    {
        S currentPrecedingState = null;
        S nextPrecedingState = null;
        int maxNextDepth = Integer.MAX_VALUE;

        WithRanges(C source)
        {
            super(source);
            // start state can be non-null for sets
            currentPrecedingState = verifyCoveringStateProperties(source.precedingState());
            final S content = source.content();
            nextPrecedingState = content != null ? verifyBoundaryStateProperties(content).precedingState(direction.opposite())
                                                 : currentPrecedingState;
        }

        void verifyEndState()
        {
            // end state can be non-null for sets
        }

        @Override
        public long advance()
        {
            currentPrecedingState = nextPrecedingState;
            checkIfDescentShouldBeForbidden();
            return verifyState(super.advance());
        }

        @Override
        public long advanceMultiple(TransitionsReceiver receiver)
        {
            currentPrecedingState = nextPrecedingState;
            checkIfDescentShouldBeForbidden();
            return verifyState(super.advanceMultiple(receiver));
        }

        @Override
        public long skipTo(long encodedSkipPosition)
        {
            checkIfDescentShouldBeForbidden();
            return verifySkipState(super.skipTo(encodedSkipPosition));
        }

        private void checkIfDescentShouldBeForbidden()
        {
            maxNextDepth = source.content() != null ? Cursor.depth(returnedPosition) : Integer.MAX_VALUE;
        }

        @Override
        public S precedingState()
        {
            assert currentPrecedingState == source.precedingState() ||
                   currentPrecedingState != null && currentPrecedingState.equals(source.precedingState()) :
                String.format("Preceding state changed without advance: %s -> %s.\n%s",
                              currentPrecedingState, source.precedingState(),
                              this);
            return currentPrecedingState;
        }

        @Override
        public S state()
        {
            return source.state();
        }

        boolean agree(S left, S right)
        {
            return Objects.equals(left, right);
        }

        private long verifyState(long position)
        {
            S precedingState = source.precedingState();
            boolean equal = agree(currentPrecedingState, precedingState);
            assert equal : String.format("Unexpected change to covering state: %s -> %s\n%s",
                                         currentPrecedingState, precedingState, this);
            assert Cursor.depth(position) <= maxNextDepth :
                String.format("Cursor descended after reporting an included branch\n%s",
                              this);
            currentPrecedingState = precedingState;

            S content = source.content();
            if (content != null)
            {
                assert agree(currentPrecedingState, content.precedingState(direction)) :
                    String.format("Range end %s does not close covering state %s\n%s",
                                  content.precedingState(direction), currentPrecedingState, this);
                verifyBoundaryStateProperties(content);
                nextPrecedingState = content.precedingState(direction.opposite());
            }

            if (Cursor.isExhausted(position))
                verifyEndState();
            return position;
        }

        private long verifySkipState(long depth)
        {
            // The covering state information is invalidated by a skip.
            currentPrecedingState = verifyCoveringStateProperties(source.precedingState());
            nextPrecedingState = currentPrecedingState;
            return verifyState(depth);
        }

        S verifyCoveringStateProperties(S state)
        {
            if (state == null)
                return null;
            assert !state.isBoundary() :
                String.format("Boundary state %s was returned where a covering state was expected\n%s",
                              state,
                              this);
            final S precedingState = state.precedingState(Direction.FORWARD);
            final S succeedingState = state.precedingState(Direction.REVERSE);
            assert precedingState == state && succeedingState == state :
                String.format("State %s must return itself its preceding and succeeding state (returned %s/%s)\n%s",
                              state,
                              precedingState,
                              succeedingState,
                              this);
            return state;
        }

        S verifyBoundaryStateProperties(S state)
        {
            if (state == null)
                return null;
            assert state.isBoundary() :
                String.format("Covering state %s was returned where a boundary state was expected\n%s",
                              state,
                              this);
            final S precedingState = state.precedingState(Direction.FORWARD);
            final S succeedingState = state.precedingState(Direction.REVERSE);
            verifyCoveringStateProperties(precedingState);
            verifyCoveringStateProperties(succeedingState);
            return state;
        }


        @Override
        public abstract WithRanges<S, C> tailCursor(Direction direction);

        @Override
        public String toString()
        {
            return super.toString() + " state " + state();
        }
    }


    class Range<S extends RangeState<S>> extends WithRanges<S, RangeCursor<S>> implements RangeCursor<S>
    {
        Range(RangeCursor<S> source)
        {
            super(source);
            assert currentPrecedingState == null :
                String.format("Initial preceding state %s should be null for range cursor\n%s",
                              currentPrecedingState, this);
        }

        @Override
        void verifyEndState()
        {
            assert currentPrecedingState == null :
                String.format("End state %s should be null for range cursor\n%s",
                              currentPrecedingState, this);
        }

        @Override
        public Range<S> tailCursor(Direction direction)
        {
            return new Range<>(source.tailCursor(direction));
        }
    }

    class TrieSet extends WithRanges<TrieSetCursor.RangeState, TrieSetCursor> implements TrieSetCursor
    {
        TrieSet(TrieSetCursor source)
        {
            super(source);
            // start and end state can be non-null for sets
        }

        @Override
        public TrieSetCursor.RangeState state()
        {
            return Preconditions.checkNotNull(source.state());
        }

        @Override
        public TrieSet tailCursor(Direction direction)
        {
            return new TrieSet(source.tailCursor(direction));
        }
    }

    class DeletionAware<T, D extends RangeState<D>>
    extends VerificationCursor.Plain<T, DeletionAwareCursor<T, D>>
    implements DeletionAwareCursor<T, D>
    {
        int deletionBranchDepth;

        DeletionAware(DeletionAwareCursor<T, D> source)
        {
            super(source);
            this.deletionBranchDepth = -1;
            verifyDeletionBranch(0);
        }

        @Override
        public long advance()
        {
            return verifyDeletionBranch(super.advance());
        }

        @Override
        public long advanceMultiple(TransitionsReceiver receiver)
        {
            return verifyDeletionBranch(super.advanceMultiple(receiver));
        }

        @Override
        public long skipTo(long encodedSkipPosition)
        {
            return verifyDeletionBranch(super.skipTo(encodedSkipPosition));
        }

        @Override
        public RangeCursor<D> deletionBranchCursor(Direction direction)
        {
            // deletionBranch is already verified
            final RangeCursor<D> deletionBranch = source.deletionBranchCursor(direction);
            if (deletionBranch == null)
                return null;
            return new Range<>(deletionBranch);
        }

        long verifyDeletionBranch(long position)
        {
            int depth = Cursor.depth(position);
            if (depth <= deletionBranchDepth)
                deletionBranchDepth = -1;

            var deletionBranch = source.deletionBranchCursor(direction);
            if (deletionBranch != null)
            {
                assert deletionBranchDepth == -1 :
                    String.format("Deletion branch at position %s covered by another deletion branch at parent depth %s\n%s",
                                  Cursor.toString(position),
                                  deletionBranchDepth,
                                  this);
                assert Cursor.compare(deletionBranch.encodedPosition(), Cursor.rootPosition(direction)) == 0 :
                    String.format("Invalid deletion branch initial position %s\n%s",
                                  Cursor.toString(deletionBranch.encodedPosition()),
                                  this);
                assert deletionBranch.precedingState() == null :
                    String.format("Deletion branch starts with active deletion %s\n%s",
                                  deletionBranch.precedingState(),
                                  this);
                deletionBranch.skipTo(Cursor.exhaustedPosition(direction));
                assert deletionBranch.precedingState() == null :
                    String.format("Deletion branch ends with active deletion %s\n%s",
                                  deletionBranch.precedingState(),
                                  this);
                deletionBranchDepth = Cursor.depth(position);
            }
            return position;
        }

        @Override
        public DeletionAware<T, D> tailCursor(Direction direction)
        {
            return new DeletionAware<>(source.tailCursor(direction));
        }
    }
}
