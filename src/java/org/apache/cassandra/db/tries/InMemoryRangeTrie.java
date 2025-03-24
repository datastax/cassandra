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

import java.util.function.Predicate;

import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.concurrent.OpOrder;

public class InMemoryRangeTrie<S extends RangeState<S>> extends InMemoryBaseTrie<S> implements RangeTrie<S>
{
    // constants for space calculations
    private static final long EMPTY_SIZE_ON_HEAP;
    private static final long EMPTY_SIZE_OFF_HEAP;
    static
    {
        // Measuring the empty size of long-lived tries, because these are the ones for which we want to track size.
        InMemoryBaseTrie<?> empty = new InMemoryRangeTrie<>(ByteComparable.Version.OSS50, BufferType.ON_HEAP, ExpectedLifetime.LONG, null);
        EMPTY_SIZE_ON_HEAP = ObjectSizes.measureDeep(empty);
        empty = new InMemoryRangeTrie<>(ByteComparable.Version.OSS50, BufferType.OFF_HEAP, ExpectedLifetime.LONG, null);
        EMPTY_SIZE_OFF_HEAP = ObjectSizes.measureDeep(empty);
    }

    InMemoryRangeTrie(ByteComparable.Version byteComparableVersion, BufferType bufferType, ExpectedLifetime lifetime, OpOrder opOrder)
    {
        super(byteComparableVersion, bufferType, lifetime, opOrder);
    }

    public static <S extends RangeState<S>> InMemoryRangeTrie<S> shortLived(ByteComparable.Version byteComparableVersion)
    {
        return new InMemoryRangeTrie<>(byteComparableVersion, BufferType.ON_HEAP, ExpectedLifetime.SHORT, null);
    }

    public static <S extends RangeState<S>> InMemoryRangeTrie<S> shortLived(ByteComparable.Version byteComparableVersion, BufferType bufferType)
    {
        return new InMemoryRangeTrie<>(byteComparableVersion, bufferType, ExpectedLifetime.SHORT, null);
    }

    public static <S extends RangeState<S>> InMemoryRangeTrie<S> longLived(ByteComparable.Version byteComparableVersion, OpOrder opOrder)
    {
        return longLived(byteComparableVersion, BufferType.OFF_HEAP, opOrder);
    }

    public static <S extends RangeState<S>> InMemoryRangeTrie<S> longLived(ByteComparable.Version byteComparableVersion, BufferType bufferType, OpOrder opOrder)
    {
        return new InMemoryRangeTrie<>(byteComparableVersion, bufferType, ExpectedLifetime.LONG, opOrder);
    }

    public InMemoryRangeCursor makeCursor(Direction direction)
    {
        return new InMemoryRangeCursor(direction, root, 0, -1);
    }

    protected long emptySizeOnHeap()
    {
        return bufferType == BufferType.ON_HEAP ? EMPTY_SIZE_ON_HEAP : EMPTY_SIZE_OFF_HEAP;
    }

    class InMemoryRangeCursor extends InMemoryCursor implements RangeCursor<S>
    {
        boolean activeIsSet;
        S activeRange;  // only non-null if activeIsSet
        S prevContent;  // can only be non-null if activeIsSet

        InMemoryRangeCursor(Direction direction, int root, int depth, int incomingTransition)
        {
            super(direction, root, depth, incomingTransition);
            activeIsSet = true;
            activeRange = null;
            prevContent = null;
        }

        @Override
        public int advance()
        {
            return updateActiveAndReturn(super.advance());
        }

        @Override
        public int advanceMultiple(TransitionsReceiver receiver)
        {
            return updateActiveAndReturn(super.advanceMultiple(receiver));
        }

        @Override
        public int skipTo(int skipDepth, int skipTransition)
        {
            activeIsSet = false;    // since we are skipping, we have no idea where we will end up
            activeRange = null;
            prevContent = null;
            return updateActiveAndReturn(super.skipTo(skipDepth, skipTransition));
        }

        @Override
        public S state()
        {
            if (!activeIsSet)
                setActiveState();
            return activeRange;
        }

        private int updateActiveAndReturn(int depth)
        {
            if (depth >= 0)
            {
                // Always check if we are seeing new content; if we do, that's an easy state update.
                S content = content();
                if (content != null)
                {
                    activeRange = content;
                    prevContent = content;
                    activeIsSet = true;
                }
                else if (prevContent != null)
                {
                    // If the previous state was exact, its right side is what we now have.
                    activeRange = prevContent.precedingState(direction.opposite());
                    prevContent = null;
                    assert activeIsSet;
                }
                // otherwise the active state is either not set or still valid.
            }
            else
            {
                // exhausted
                activeIsSet = true;
                activeRange = null;
                prevContent = null;
            }
            return depth;
        }

        private void setActiveState()
        {
            assert content() == null;
            S nearestContent = getNearestContent();
            // Note: the nearest content may change between the time we fetch it and when we reach that node, e.g.
            // if someone deletes aa-cd where there existed an abc-acd deletion, and we fetched the latter while at "a".
            // This, though, should only be possible of the preceding state of the nearest content is null.
            activeRange = nearestContent != null ? nearestContent.precedingState(direction) : null;
            prevContent = null;
            activeIsSet = true;
        }

        private S getNearestContent()
        {
            // Walk a copy of this cursor (non-range because we are only not doing anything smart with it) to find the
            // nearest child content in the direction of the cursor.
            return new InMemoryCursor(direction, currentNode, 0, -1).advanceToContent(null);
        }

        @Override
        public InMemoryRangeCursor tailCursor(Direction direction)
        {
            InMemoryRangeCursor cursor = new InMemoryRangeCursor(direction, currentFullNode, 0, -1);
            if (activeIsSet)
            {
                // Copy the state we have already compiled to the child cursor.
                cursor.activeIsSet = true;
                cursor.activeRange = activeRange;
            }
            else
                cursor.activeIsSet = false;

            return cursor;
        }
    }

    static class Mutation<M extends RangeState<M>, U extends RangeState<U>> extends InMemoryBaseTrie.Mutation<M, U, RangeCursor<U>>
    {
        Mutation(UpsertTransformerWithKeyProducer<M, U> transformer, Predicate<NodeFeatures<U>> needsForcedCopy, RangeCursor<U> source, InMemoryRangeTrie<M>.ApplyState state)
        {
            super(transformer, needsForcedCopy, source, state);
        }

        @Override
        void apply() throws TrieSpaceExhaustedException
        {
            applyRanges();
            assert state.currentDepth == 0 : "Unexpected change to applyState. Concurrent trie modification?";
        }

        void applyContent(M existingState, U mutationState) throws TrieSpaceExhaustedException
        {
            M combined = transformer.apply(existingState, mutationState, state);
            if (combined != null)
                combined = combined.isBoundary() ? combined : null;
            state.setContent(combined, // can be null
                             state.currentDepth >= forcedCopyDepth); // this is called at the start of processing
        }


        void applyRanges() throws TrieSpaceExhaustedException
        {
            // While activeDeletion is not set, follow the mutation trie.
            // When a deletion is found, get existing covering state, combine and apply/store.
            // Get rightSideAsCovering and walk the full existing trie to apply, advancing mutation cursor in parallel
            // until we see another entry in mutation trie.
            // Repeat until mutation trie is exhausted.
            int depth = state.currentDepth;
            int prevAscendDepth = state.setAscendLimit(depth);
            while (true)
            {
                if (depth < forcedCopyDepth)
                    forcedCopyDepth = needsForcedCopy.test(this) ? depth : Integer.MAX_VALUE;

                U content = mutationCursor.content();
                if (content != null)
                {
                    final M existingCoveringState = getExistingCoveringState();
                    applyContent(existingCoveringState, content);
                    U mutationCoveringState = content.precedingState(Direction.REVERSE);
                    // Several cases:
                    // - New deletion is point deletion: Apply it and move on to next mutation branch.
                    // - New deletion starts range and there is no existing or it beats the existing: Walk both tries in
                    //   parallel to apply deletion and adjust on any change.
                    // - New deletion starts range and existing beats it: We still have to walk both tries in parallel,
                    //   because existing deletion may end before the newly introduced one, and we want to apply that when
                    //   it does.
                    if (mutationCoveringState != null)
                        applyDeletionRange(rightSideAsCovering(existingCoveringState), mutationCoveringState);
                }

                depth = mutationCursor.advance();
                // Descend but do not modify anything yet.
                if (!state.advanceTo(depth, mutationCursor.incomingTransition(), forcedCopyDepth))
                    break;
                assert depth == state.currentDepth : "Unexpected change to applyState. Concurrent trie modification?";
            }
            state.setAscendLimit(prevAscendDepth);
        }

        void applyDeletionRange(M existingCoveringState,
                                   U mutationCoveringState)
        throws TrieSpaceExhaustedException
        {
            boolean atMutation = true;
            int depth = mutationCursor.depth();
            int transition = mutationCursor.incomingTransition();
            // We are walking both tries in parallel.
            while (true)
            {
                if (atMutation)
                {
                    depth = mutationCursor.advance();
                    transition = mutationCursor.incomingTransition();

                    assert depth > 0 : "Unbounded range in mutation trie, state " + mutationCoveringState + " active when exhausted.";
                    if (depth < forcedCopyDepth)
                        forcedCopyDepth = needsForcedCopy.test(this) ? depth : Integer.MAX_VALUE;
                }
                atMutation = !state.advanceToNextExistingOr(depth, transition, forcedCopyDepth);

                M existingContent = state.getContent();
                U mutationContent = atMutation ? mutationCursor.content() : null;
                if (existingContent != null || mutationContent != null)
                {
                    if (existingContent == null)
                        existingContent = existingCoveringState;
                    if (mutationContent == null)
                        mutationContent = mutationCoveringState;
                    applyContent(existingContent, mutationContent);
                    mutationCoveringState = mutationContent.precedingState(Direction.REVERSE);
                    existingCoveringState = rightSideAsCovering(existingContent);
                    if (mutationCoveringState == null)
                    {
                        assert atMutation; // mutation covering state can only change when mutation content is present
                        return; // mutation deletion range was closed, we can continue normal mutation cursor iteration
                    }
                }
            }
        }

        static <S extends RangeState<S>> S rightSideAsCovering(S rangeState)
        {
            if (rangeState == null)
                return null;
            return rangeState.precedingState(Direction.REVERSE);
        }

        M getExistingCoveringState()
        {
            // If the current node has content, use it.
            M existingCoveringState = state.getContent();
            if (existingCoveringState != null)
                return existingCoveringState;

            // Otherwise, we must have a descendant that will have the active state as its preceding.
            existingCoveringState = state.getNearestContent();
            if (existingCoveringState != null)
                return existingCoveringState.precedingState(Direction.FORWARD);

            return null;
        }
    }


    /// Modify this trie to apply the mutation given in the form of a trie. Any content in the mutation will be resolved
    /// with the given function before being placed in this trie (even if there's no pre-existing content in this trie).
    /// @param mutation the mutation to be applied, given in the form of a trie. Note that its content can be of type
    /// different than the element type for this memtable trie.
    /// @param transformer a function applied to the potentially pre-existing value for the given key, and the new
    /// value. Applied even if there's no pre-existing value in the memtable trie.
    /// @param needsForcedCopy a predicate which decides when to fully copy a branch to provide atomicity guarantees to
    /// concurrent readers. See NodeFeatures for details.
    public <U extends RangeState<U>> void apply(RangeTrie<U> mutation,
                                                final UpsertTransformerWithKeyProducer<S, U> transformer,
                                                Predicate<NodeFeatures<U>> needsForcedCopy) throws TrieSpaceExhaustedException
    {
        try
        {
            Mutation<S, U> m = new Mutation<>(transformer,
                                              needsForcedCopy,
                                              mutation.cursor(Direction.FORWARD),
                                              applyState.start());
            m.apply();
            m.complete();
            completeMutation();
        }
        catch (Throwable t)
        {
            abortMutation();
            throw t;
        }
    }

    /// Modify this trie to apply the mutation given in the form of a trie. Any content in the mutation will be resolved
    /// with the given function before being placed in this trie (even if there's no pre-existing content in this trie).
    /// @param mutation the mutation to be applied, given in the form of a trie. Note that its content can be of type
    /// different than the element type for this memtable trie.
    /// @param transformer a function applied to the potentially pre-existing value for the given key, and the new
    /// value. Applied even if there's no pre-existing value in the memtable trie.
    /// @param needsForcedCopy a predicate which decides when to fully copy a branch to provide atomicity guarantees to
    /// concurrent readers. See NodeFeatures for details.
    public <U extends RangeState<U>> void apply(RangeTrie<U> mutation,
                                                final UpsertTransformer<S, U> transformer,
                                                Predicate<NodeFeatures<U>> needsForcedCopy) throws TrieSpaceExhaustedException
    {
        apply(mutation, (UpsertTransformerWithKeyProducer<S, U>) transformer, needsForcedCopy);
    }
}
