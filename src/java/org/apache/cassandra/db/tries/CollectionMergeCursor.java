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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.IntFunction;

import org.apache.cassandra.utils.bytecomparable.ByteComparable;

/// A merged view of multiple tries.
///
/// This is accomplished by walking the cursors in parallel; the merged cursor takes the position and features of the
/// smallest and advances with it; when multiple cursors are equal, all of them are advanced. The ordered view of the
/// cursors is maintained using a custom binary min-heap, built for efficiently reforming the heap when the top elements
/// are advanced.
///
/// Crucial for the efficiency of this is the fact that when they are advanced like this, we can compare cursors'
/// positions by their `depth` descending and then `incomingTransition` ascending.
/// See [Trie.md](./Trie.md) for further details.
///
/// The merge cursor is a variation of the idea of a merge iterator with one key observation: because we advance
/// the source iterators together, we can compare them just by depth and incoming transition.
///
/// The most straightforward way to implement merging of iterators is to use a `PriorityQueue`,
/// `poll` it to find the next item to consume, then `add` the iterator back after advancing.
/// This is not very efficient as `poll` and `add` in all cases require at least
/// `log(size)` comparisons and swaps (usually more than `2*log(size)`) per consumed item, even
/// if the input is suitable for fast iteration.
///
/// The implementation below makes use of the fact that replacing the top element in a binary heap can be
/// done much more efficiently than separately removing it and placing it back, especially in the cases where
/// the top iterator is to be used again very soon (e.g. when there are large sections of the output where
/// only a limited number of input iterators overlap, which is normally the case in many practically useful
/// situations, e.g. levelled compaction).
///
/// The implementation builds and maintains a binary heap of sources (stored in an array), where we do not
/// add items after the initial construction. Instead we advance the smallest element (which is at the top
/// of the heap) and push it down to find its place for its new position. Should this source be exhausted,
/// we swap it with the last source in the heap and proceed by pushing that down in the heap.
///
/// In the case where we have multiple sources with matching positions, the merging algorithm
/// must be able to merge all equal values. To achieve this `content` walks the heap to
/// find all equal cursors without advancing them, and separately `advance` advances
/// all equal sources and restores the heap structure.
///
/// The latter is done equivalently to the process of initial construction of a min-heap using back-to-front
/// heapification as done in the classic heapsort algorithm. It only needs to heapify subheaps whose top item
/// is advanced (i.e. one whose position matches the current), and we can do that recursively from
/// bottom to top. Should a source be exhausted when advancing, it can be thrown away by swapping in the last
/// source in the heap (note: we must be careful to advance that source too if required).
///
/// To make it easier to advance efficienty in single-sourced branches of tries, we extract the current smallest
/// cursor (the head) separately, and start any advance with comparing that to the heap's first. When the smallest
/// cursor remains the same (e.g. in branches coming from a single source) this makes it possible to advance with
/// just one comparison instead of two at the expense of increasing the number by one in the general case.
///
/// Note: This is a simplification of the MergeIterator code from CASSANDRA-8915, without the leading ordered
/// section and equalParent flag since comparisons of cursor positions are cheap.
abstract class CollectionMergeCursor<T, C extends Cursor<T>> implements Cursor<T>
{
    final Trie.CollectionMergeResolver<T> resolver;
    final Direction direction;

    /// The smallest cursor, tracked separately to improve performance in single-source sections of the trie.
    C head;

    /// Binary heap of the remaining cursors. The smallest element is at position 0.
    /// Every element `i` is smaller than or equal to its two children, i.e.
    /// ```heap[i] <= heap[i*2 + 1] && heap[i] <= heap[i*2 + 2]```
    final C[] heap;

    /// A list used to collect contents during [#content()] calls.
    final List<T> contents;
    /// Whether content has already been collected for this position.
    boolean contentCollected;
    /// The collected content.
    T collectedContent;

    <I> CollectionMergeCursor(Trie.CollectionMergeResolver<T> resolver, Direction direction, Collection<I> inputs, IntFunction<C[]> cursorArrayConstructor, BiFunction<I, Direction, C> extractor)
    {
        this.resolver = resolver;
        this.direction = direction;
        int count = inputs.size();
        // Get cursors for all inputs. Put one of them in head and the rest in the heap.
        heap = cursorArrayConstructor.apply(count - 1);
        contents = new ArrayList<>(count);
        int i = -1;
        for (I src : inputs)
        {
            C cursor = extractor.apply(src, direction);
            assert cursor.depth() == 0;
            if (i >= 0)
                heap[i] = cursor;
            else
                head = cursor;
            ++i;
        }
        // The cursors are all currently positioned on the root and thus in valid heap order.
    }

    /// Interface for internal operations that can be applied to selected top elements of the heap.
    interface HeapOp<T, C extends Cursor<T>>
    {
        void apply(CollectionMergeCursor<T, C> self, C cursor, int index);

        default boolean shouldContinueWithChild(C child, C head)
        {
            return equalCursor(child, head);
        }
    }

    /// Apply a non-interfering operation, i.e. one that does not change the cursor state, to all inputs in the heap
    /// that satisfy the [HeapOp#shouldContinueWithChild] condition (by default, being equal to the head).
    /// For interfering operations like advancing the cursors, use [#advanceSelectedAndRestoreHeap(AdvancingHeapOp)].
    void applyToSelectedInHeap(HeapOp<T, C> action)
    {
        applyToSelectedElementsInHeap(action, 0);
    }

    /// Interface for internal advancing operations that can be applied to the heap cursors. This interface provides
    /// the code to restore the heap structure after advancing the cursors.
    interface AdvancingHeapOp<T, C extends Cursor<T>> extends HeapOp<T, C>
    {
        void apply(C cursor);

        default void apply(CollectionMergeCursor<T, C> self, C cursor, int index)
        {
            // Apply the operation, which should advance the position of the element.
            apply(cursor);

            // This method is called on the back path of the recursion. At this point the heaps at both children are
            // advanced and well-formed.
            // Place current node in its proper position.
            self.heapifyDown(cursor, index);
            // The heap rooted at index is now advanced and well-formed.
        }
    }


    /// Advance the state of all inputs in the heap that satisfy the [#shouldContinueWithChild] condition
    /// (by default, being equal to the head) and restore the heap invariant.
    void advanceSelectedAndRestoreHeap(AdvancingHeapOp<T, C> action)
    {
        applyToSelectedElementsInHeap(action, 0);
    }

    /// Apply an operation to all elements on the heap that satisfy, recursively through the heap hierarchy, the
    /// `shouldContinueWithChild` condition (being equal to the head by default). Descends recursively in the
    /// heap structure to all selected children and applies the operation on the way back.
    ///
    /// This operation can be something that does not change the cursor state (see [#content]) or an operation
    /// that advances the cursor to a new state, wrapped in a [AdvancingHeapOp] ([#advance] or
    /// [#skipTo]). The latter interface takes care of pushing elements down in the heap after advancing
    /// and restores the subheap state on return from each level of the recursion.
    private void applyToSelectedElementsInHeap(HeapOp<T, C> action, int index)
    {
        if (index >= heap.length)
            return;
        C item = heap[index];
        if (!action.shouldContinueWithChild(item, head))
            return;

        // If the children are at the same position, they also need advancing and their subheap
        // invariant to be restored.
        applyToSelectedElementsInHeap(action, index * 2 + 1);
        applyToSelectedElementsInHeap(action, index * 2 + 2);

        // Apply the action. This is done on the reverse direction to give the action a chance to form proper
        // subheaps and combine them on processing the parent.
        action.apply(this, item, index);
    }

    void applyToAllOnHeap(HeapOp<T, C> action)
    {
        for (int i = 0; i < heap.length; i++)
            action.apply(this, heap[i], i);
    }

    /// Push the given state down in the heap from the given index until it finds its proper place among
    /// the subheap rooted at that position.
    private void heapifyDown(C item, int index)
    {
        while (true)
        {
            int next = index * 2 + 1;
            if (next >= heap.length)
                break;
            // Select the smaller of the two children to push down to.
            if (next + 1 < heap.length && greaterCursor(direction, heap[next], heap[next + 1]))
                ++next;
            // If the child is greater or equal, the invariant has been restored.
            if (!greaterCursor(direction, item, heap[next]))
                break;
            heap[index] = heap[next];
            index = next;
        }
        heap[index] = item;
    }

    /// Check if the head is greater than the top element in the heap, and if so, swap them and push down the new
    /// top until its proper place.
    ///
    /// @param headDepth the depth of the head cursor (as returned by e.g. advance).
    /// @return the new head element's depth
    private int maybeSwapHead(int headDepth)
    {
        int heap0Depth = heap[0].depth();
        if (headDepth > heap0Depth ||
            (headDepth == heap0Depth && direction.le(head.incomingTransition(), heap[0].incomingTransition())))
            return headDepth;   // head is still smallest

        // otherwise we need to swap heap and heap[0]
        C newHeap0 = head;
        head = heap[0];
        heapifyDown(newHeap0, 0);
        return heap0Depth;
    }

    boolean branchHasMultipleSources()
    {
        return equalCursor(heap[0], head);
    }

    boolean isExhausted()
    {
        return head.depth() < 0;
    }

    @Override
    public int advance()
    {
        contentCollected = false;
        return doAdvance();
    }

    private int doAdvance()
    {
        advanceSelectedAndRestoreHeap(Cursor::advance);
        return maybeSwapHead(head.advance());
    }

    @Override
    public int advanceMultiple(TransitionsReceiver receiver)
    {
        contentCollected = false;
        // If the current position is present in just one cursor, we can safely descend multiple levels within
        // its branch as no one of the other tries has content for it.
        if (branchHasMultipleSources())
            return doAdvance();   // More than one source at current position, do single-step advance.

        // If there are no children, i.e. the cursor ascends, we have to check if it's become larger than some
        // other candidate.
        return maybeSwapHead(head.advanceMultiple(receiver));
    }

    @Override
    public int skipTo(int skipDepth, int skipTransition)
    {
        // We need to advance all cursors that stand before the requested position.
        // If a child cursor does not need to advance as it is greater than the skip position, neither of the ones
        // below it in the heap hierarchy do as they can't have an earlier position.
        class SkipTo implements AdvancingHeapOp<T, C>
        {
            @Override
            public boolean shouldContinueWithChild(C child, C head)
            {
                // When the requested position descends, the inplicit prefix bytes are those of the head cursor,
                // and thus we need to check against that if it is a match.
                if (equalCursor(child, head))
                    return true;
                // Otherwise we can compare the child's position against a cursor advanced as requested, and need
                // to skip only if it would be before it.
                int childDepth = child.depth();
                return childDepth > skipDepth ||
                       childDepth == skipDepth && direction.lt(child.incomingTransition(), skipTransition);
            }

            @Override
            public void apply(C cursor)
            {
                cursor.skipTo(skipDepth, skipTransition);
            }
        }

        contentCollected = false;
        applyToSelectedElementsInHeap(new SkipTo(), 0);
        return maybeSwapHead(head.skipTo(skipDepth, skipTransition));
    }

    @Override
    public int depth()
    {
        return head.depth();
    }

    @Override
    public int incomingTransition()
    {
        return head.incomingTransition();
    }

    @Override
    public Direction direction()
    {
        return direction;
    }

    @Override
    public ByteComparable.Version byteComparableVersion()
    {
        return head.byteComparableVersion();
    }

    T maybeCollectContent()
    {
        if (!contentCollected)
        {
            collectedContent = isExhausted() ? null : collectContent();
            contentCollected = true;
        }
        return collectedContent;
    }

    T collectContent()
    {
        applyToSelectedInHeap(CollectionMergeCursor::collectContent);
        collectContent(head, -1);
        return resolveContent();
    }

    T resolveContent()
    {
        T toReturn;
        switch (contents.size())
        {
            case 0:
                toReturn = null;
                break;
            case 1:
                toReturn = contents.get(0);
                break;
            default:
                toReturn = resolver.resolve(contents);
                break;
        }
        contents.clear();
        return toReturn;
    }

    void collectContent(C item, int index)
    {
        T itemContent = getContent(item);
        if (itemContent != null)
            contents.add(itemContent);
    }

    abstract T getContent(C item);

    /// Compare the positions of two cursors. One is before the other when
    /// - its depth is greater, or
    /// - its depth is equal, and the incoming transition is smaller.
    static <T> boolean greaterCursor(Direction direction, Cursor<T> c1, Cursor<T> c2)
    {
        int c1depth = c1.depth();
        int c2depth = c2.depth();
        if (c1depth != c2depth)
            return c1depth < c2depth;
        return direction.lt(c2.incomingTransition(), c1.incomingTransition());
    }

    static <T> boolean equalCursor(Cursor<T> c1, Cursor<T> c2)
    {
        return c1.depth() == c2.depth() && c1.incomingTransition() == c2.incomingTransition();
    }

    static class Plain<T> extends CollectionMergeCursor<T, Cursor<T>> implements Cursor<T>
    {
        public <I> Plain(Trie.CollectionMergeResolver<T> resolver, Direction direction, Collection<I> inputs, BiFunction<I, Direction, Cursor<T>> extractor)
        {
            super(resolver, direction, inputs, Cursor[]::new, extractor);
        }

        @Override
        public T content()
        {
            return maybeCollectContent();
        }

        @Override
        T getContent(Cursor<T> item)
        {
            return item.content();
        }

        @Override
        public Cursor<T> tailCursor(Direction dir)
        {
            if (!branchHasMultipleSources())
                return head.tailCursor(dir);

            List<Cursor<T>> inputs = new ArrayList<>(heap.length + 1);
            inputs.add(head);
            applyToSelectedInHeap((self, cursor, index) -> inputs.add(cursor));

            return new Plain<>(resolver, dir, inputs, Cursor::tailCursor);
        }
    }

    static class Range<S extends RangeState<S>> extends CollectionMergeCursor<S, RangeCursor<S>> implements RangeCursor<S>
    {
        <I> Range(Trie.CollectionMergeResolver<S> resolver,
                  Direction direction,
                  Collection<I> inputs,
                  BiFunction<I, Direction, RangeCursor<S>> extractor)
        {
            super(resolver, direction, inputs, RangeCursor[]::new, extractor);
        }

        @Override
        public S state()
        {
            return maybeCollectContent();
        }

        @Override
        S collectContent()
        {
            applyToAllOnHeap(CollectionMergeCursor::collectContent);
            collectContent(head, -1);
            return resolveContent();
        }

        @Override
        S getContent(RangeCursor<S> item)
        {
            return equalCursor(item, head) ? item.state() : item.precedingState();
        }

        @Override
        public RangeCursor<S> tailCursor(Direction direction)
        {
            List<RangeCursor<S>> inputs = new ArrayList<>(heap.length);
            inputs.add(head);
            applyToAllOnHeap((self, cursor, index) ->
                             {
                                 if (equalCursor(head, cursor))
                                     inputs.add(cursor);
                                 else if (cursor.precedingState() != null)
                                     inputs.add(cursor.precedingStateCursor(direction));
                             });

            if (inputs.size() == 1)
            {
                assert head == inputs.get(0);
                return head.tailCursor(direction);
            }

            return new Range<>(resolver, direction, inputs, RangeCursor::tailCursor);
        }
    }

}
