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

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import com.google.common.collect.ImmutableList;

import org.agrona.DirectBuffer;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

/**
 * Base class for tries.
 * <p>
 * Normal users of tries will only use the public methods, which provide various transformations of the trie, conversion
 * of its content to other formats (e.g. iterable of values), and several forms of processing.
 * <p>
 * For any unimplemented data extraction operations one can build on the {@link TrieEntriesWalker} (for-each processing)
 * and {@link TrieEntriesIterator} (to iterator) base classes, which provide the necessary mechanisms to handle walking
 * the trie.
 * <p>
 * The internal representation of tries using this interface is defined in the {@link Cursor} interface.
 * <p>
 * Cursors are a method of presenting the internal structure of a trie without representing nodes as objects, which is
 * still useful for performing the basic operations on tries (iteration, slicing/intersection and merging). A cursor
 * will list the nodes of a trie in order, together with information about the path that was taken to reach them.
 * <p>
 * To begin traversal over a trie, one must retrieve a cursor by calling {@link #cursor}. Because cursors are
 * stateful, the traversal must always proceed from one thread. Should concurrent reads be required, separate calls to
 * {@link #cursor} must be made. Any modification that has completed before the construction of a cursor must be
 * visible, but any later concurrent modifications may be presented fully, partially or not at all; this also means that
 * if multiple are made, the cursor may see any part of any subset of them.
 * <p>
 * Note: This model only supports depth-first traversals. We do not currently have a need for breadth-first walks.
 * <p>
 * See Trie.md for further description of the trie representation model.
 *
 * @param <T> The content type of the trie.
 */
public abstract class Trie<T>
{
    /**
     * A trie cursor.
     * <p>
     * This is the internal representation of the trie, which enables efficient walks and basic operations (merge,
     * slice) on tries.
     * <p>
     * The cursor represents the state of a walk over the nodes of trie. It provides three main features:<ul>
     * <li>the current {@code depth} or descend-depth in the trie;</li>
     * <li>the {@code incomingTransition}, i.e. the byte that was used to reach the current point;</li>
     * <li>the {@code content} associated with the current node,</li>
     * </ul>
     * and provides methods for advancing to the next position.  This is enough information to extract all paths, and
     * also to easily compare cursors over different tries that are advanced together. Advancing is always done in
     * order; if one imagines the set of nodes in the trie with their associated paths, a cursor may only advance from a
     * node with a lexicographically smaller path to one with bigger. The {@code advance} operation moves to the immediate
     * next, it is also possible to skip over some items e.g. all children of the current node ({@code skipChildren}).
     * <p>
     * Moving to the immediate next position in the lexicographic order is accomplished by:<ul>
     * <li>if the current node has children, moving to its first child;</li>
     * <li>otherwise, ascend the parent chain and return the next child of the closest parent that still has any.</li>
     * </ul>
     * As long as the trie is not exhausted, advancing always takes one step down, from the current node, or from a node
     * on the parent chain. By comparing the new depth (which {@code advance} also returns) with the one before the advance,
     * one can tell if the former was the case (if {@code newDepth == oldDepth + 1}) and how many steps up we had to take
     * ({@code oldDepth + 1 - newDepth}). When following a path down, the cursor will stop on all prefixes.
     * <p>
     * When it is created the cursor is placed on the root node with {@code depth() = 0}, {@code incomingTransition() = -1}.
     * Since tries can have mappings for empty, content() can possibly be non-null. The cursor is exhausted when it
     * returns a depth of -1 (the operations that advance a cursor return the depth, and {@code depth()} will also
     * return -1 if queried afterwards). It is not allowed for a cursor to start in exhausted state; once a cursor is
     * exhausted, calling any of the advance methods or {@code tailTrie} is an error.
     * <p>
     * For example, the following trie:<br/>
     * <pre>
     *  t
     *   r
     *    e
     *     e *
     *    i
     *     e *
     *     p *
     *  w
     *   i
     *    n  *
     * </pre>
     * has nodes reachable with the paths<br/>
     * &nbsp; "", t, tr, tre, tree*, tri, trie*, trip*, w, wi, win*<br/>
     * and the cursor will list them with the following {@code (depth, incomingTransition)} pairs:<br/>
     * &nbsp; (0, -1), (1, t), (2, r), (3, e), (4, e)*, (3, i), (4, e)*, (4, p)*, (1, w), (2, i), (3, n)*
     * <p>
     * Because we exhaust transitions on bigger depths before we go the next transition on the smaller ones, when
     * cursors are advanced together their positions can be easily compared using only the {@code depth} and
     * {@code incomingTransition}:<ul>
     * <li>one that is higher in depth is before one that is lower;</li>
     * <li>for equal depths, the one with smaller incomingTransition is first.</li>
     * </ul>
     * If we consider walking the trie above in parallel with this:<br/>
     * <pre>
     *  t
     *   r
     *    i
     *     c
     *      k *
     *  u
     *   p *
     * </pre>
     * the combined iteration will proceed as follows:<pre>
     *  (0, -1)+  (0, -1)+          cursors equal, advance both
     *  (1, t)+   (1, t)+   t       cursors equal, advance both
     *  (2, r)+   (2, r)+   tr      cursors equal, advance both
     *  (3, e)+ < (3, i)    tre     cursors not equal, advance smaller (3 = 3, e < i)
     *  (4, e)+ < (3, i)    tree*   cursors not equal, advance smaller (4 > 3)
     *  (3, i)+   (3, i)+   tri     cursors equal, advance both
     *  (4, e)  > (4, c)+   tric    cursors not equal, advance smaller (4 = 4, e > c)
     *  (4, e)  > (5, k)+   trick*  cursors not equal, advance smaller (4 < 5)
     *  (4, e)+ < (1, u)    trie*   cursors not equal, advance smaller (4 > 1)
     *  (4, p)+ < (1, u)    trip*   cursors not equal, advance smaller (4 > 1)
     *  (1, w)  > (1, u)+   u       cursors not equal, advance smaller (1 = 1, w > u)
     *  (1, w)  > (2, p)+   up*     cursors not equal, advance smaller (1 < 2)
     *  (1, w)+ < (-1, -1)  w       cursors not equal, advance smaller (1 > -1)
     *  (2, i)+ < (-1, -1)  wi      cursors not equal, advance smaller (2 > -1)
     *  (3, n)+ < (-1, -1)  win*    cursors not equal, advance smaller (3 > -1)
     *  (-1, -1)  (-1, -1)          both exhasted
     *  </pre>
     * <p>
     * Cursors are created with a direction (forward or reverse), which specifies the order in which a node's children
     * are iterated (smaller first or larger first). Note that entries returned in reverse direction are in
     * lexicographic order for the inverted alphabet, which is not the same as being presented in reverse. For example,
     * a cursor for a trie containing "ab", "abc" and "cba", will visit the nodes in order "cba", "ab", "abc", i.e.
     * prefixes will still be reported before their descendants.
     */
    protected interface Cursor<T>
    {

        /**
         * @return the current descend-depth; 0, if the cursor has just been created and is positioned on the root,
         *         and -1, if the trie has been exhausted.
         */
        int depth();

        /**
         * @return the last transition taken; if positioned on the root, return -1
         */
        int incomingTransition();

        /**
         * @return the content associated with the current node. This may be non-null for any presented node, including
         *         the root.
         */
        T content();

        /**
         * Returns the direction in which this cursor is progressing.
         */
        Direction direction();

        /**
         * Returns the byte-comparable version that this trie uses.
         */
        ByteComparable.Version byteComparableVersion();

        /**
         * Advance one position to the node whose associated path is next lexicographically.
         * This can be either:<ul>
         * <li>descending one level to the first child of the current node,
         * <li>ascending to the closest parent that has remaining children, and then descending one level to its next
         *   child.
         * </ul>
         * It is an error to call this after the trie has already been exhausted (i.e. when depth() == -1);
         * for performance reasons we won't always check this.
         *
         * @return depth (can be prev+1 or <=prev), -1 means that the trie is exhausted
         */
        int advance();

        /**
         * Advance, descending multiple levels if the cursor can do this for the current position without extra work
         * (e.g. when positioned on a chain node in a memtable trie). If the current node does not have children this
         * is exactly the same as advance(), otherwise it may take multiple steps down (but will not necessarily, even
         * if they exist).
         * <p>
         * Note that if any positions are skipped, their content must be null.
         * <p>
         * This is an optional optimization; the default implementation falls back to calling advance.
         * <p>
         * It is an error to call this after the trie has already been exhausted (i.e. when depth() == -1);
         * for performance reasons we won't always check this.
         *
         * @param receiver object that will receive all transitions taken except the last;
         *                 on ascend, or if only one step down was taken, it will not receive any
         * @return the new depth, -1 if the trie is exhausted
         */
        default int advanceMultiple(TransitionsReceiver receiver)
        {
            return advance();
        }

        /**
         * Advance all the way to the next node with non-null content.
         * <p>
         * It is an error to call this after the trie has already been exhausted (i.e. when depth() == -1);
         * for performance reasons we won't always check this.
         *
         * @param receiver object that will receive all taken transitions
         * @return the content, null if the trie is exhausted
         */
        default T advanceToContent(ResettingTransitionsReceiver receiver)
        {
            int prevDepth = depth();
            while (true)
            {
                int currDepth = advanceMultiple(receiver);
                if (currDepth <= 0)
                    return null;
                if (receiver != null)
                {
                    if (currDepth <= prevDepth)
                        receiver.resetPathLength(currDepth - 1);
                    receiver.addPathByte(incomingTransition());
                }
                T content = content();
                if (content != null)
                    return content;
                prevDepth = currDepth;
            }
        }

        /**
         * Advance to the specified depth and incoming transition or the first valid position that is after the specified
         * position. The inputs must be something that could be returned by a single call to {@link #advance} (i.e.
         * {@code depth} must be <= current depth + 1, and {@code incomingTransition} must be higher than what the
         * current state saw at the requested depth.
         *
         * @return the new depth, always <= previous depth; -1 if the trie is exhausted
         */
        int skipTo(int skipDepth, int skipTransition);

        /**
         * Descend into the cursor with the given path.
         *
         * @return True if the descent is positioned at the end of the given path, false if the trie did not have a path
         * for it. In the latter case the cursor is positioned at the first node that follows the given key in iteration
         * order.
         */
        default boolean descendAlong(ByteSource bytes)
        {
            int next = bytes.next();
            int depth = depth();
            while (next != ByteSource.END_OF_STREAM)
            {
                if (skipTo(++depth, next) != depth || incomingTransition() != next)
                    return false;
                next = bytes.next();
            }
            return true;
        }

        /**
         * Returns a tail trie, i.e. a trie whose root is the current position. Walking a tail trie will list all
         * descendants of the current position with depth adjusted by the current depth.
         * <p>
         * It is an error to call tailTrie on an exhausted cursor.
         */
        Trie<T> tailTrie();
    }

    protected abstract Cursor<T> cursor(Direction direction);

    /**
     * Used by {@link Cursor#advanceMultiple} to feed the transitions taken.
     */
    protected interface TransitionsReceiver
    {
        /** Add a single byte to the path. */
        void addPathByte(int nextByte);
        /** Add the count bytes from position pos in the given buffer. */
        void addPathBytes(DirectBuffer buffer, int pos, int count);
    }

    /**
     * Used by {@link Cursor#advanceToContent} to track the transitions and backtracking taken.
     */
    protected interface ResettingTransitionsReceiver extends TransitionsReceiver
    {
        /** Delete all bytes beyond the given length. */
        void resetPathLength(int newLength);
    }

    /**
     * A push interface for walking over the trie. Builds upon TransitionsReceiver to be given the bytes of the
     * path, and adds methods called on encountering content and completion.
     * See {@link TrieDumper} for an example of how this can be used, and {@link TrieEntriesWalker} as a base class
     * for other common usages.
     */
    protected interface Walker<T, R> extends ResettingTransitionsReceiver
    {
        /** Called when content is found. */
        void content(T content);

        /** Called at the completion of the walk. */
        R complete();
    }

    /**
     * Adapter interface providing the methods a {@link Walker} to a {@link Consumer}, so that the latter can be used
     * with {@link #process}.
     *
     * This enables calls like
     *     trie.forEachEntry(x -> System.out.println(x));
     * to be mapped directly to a single call to {@link #process} without extra allocations.
     */
    public interface ValueConsumer<T> extends Consumer<T>, Walker<T, Void>
    {
        @Override
        default void content(T content)
        {
            accept(content);
        }

        @Override
        default Void complete()
        {
            return null;
        }

        @Override
        default void resetPathLength(int newDepth)
        {
            // not tracking path
        }

        @Override
        default void addPathByte(int nextByte)
        {
            // not tracking path
        }

        @Override
        default void addPathBytes(DirectBuffer buffer, int pos, int count)
        {
            // not tracking path
        }
    }

    /**
     * Call the given consumer on all content values in the trie in order.
     */
    public void forEachValue(ValueConsumer<T> consumer)
    {
        process(consumer, Direction.FORWARD);
    }

    /**
     * Call the given consumer on all (path, content) pairs with non-null content in the trie in order.
     */
    public void forEachEntry(BiConsumer<ByteComparable.Preencoded, T> consumer)
    {
        forEachEntry(Direction.FORWARD, consumer);
    }

    /**
     * Call the given consumer on all (path, content) pairs with non-null content in the trie in order.
     */
    public void forEachEntry(Direction direction, BiConsumer<ByteComparable.Preencoded, T> consumer)
    {
        Cursor<T> cursor = cursor(direction);
        process(new TrieEntriesWalker.WithConsumer<T>(consumer, cursor.byteComparableVersion()), cursor);
        // Note: we can't do the ValueConsumer trick here, because the implementation requires state and cannot be
        // implemented with default methods alone.
    }

    /**
     * Process the trie using the given Walker.
     */
    public <R> R process(Walker<T, R> walker, Direction direction)
    {
        return process(walker, cursor(direction));
    }

    static <T, R> R process(Walker<T, R> walker, Cursor<T> cursor)
    {
        assert cursor.depth() == 0 : "The provided cursor has already been advanced.";
        T content = cursor.content();   // handle content on the root node
        if (content == null)
            content = cursor.advanceToContent(walker);

        while (content != null)
        {
            walker.content(content);
            content = cursor.advanceToContent(walker);
        }
        return walker.complete();
    }


    /**
     * Process the trie using the given ValueConsumer, skipping all branches below the top content-bearing node.
     */
    public Void forEachValueSkippingBranches(Direction direction, ValueConsumer<T> consumer)
    {
        return processSkippingBranches(consumer, cursor(direction));
    }

    /**
     * Call the given consumer on all (path, content) pairs with non-null content in the trie in order, skipping all
     * branches below the top content-bearing node.
     */
    public void forEachEntrySkippingBranches(Direction direction, BiConsumer<ByteComparable.Preencoded, T> consumer)
    {
        Cursor<T> cursor = cursor(direction);
        processSkippingBranches(new TrieEntriesWalker.WithConsumer<T>(consumer, cursor.byteComparableVersion()), cursor);
        // Note: we can't do the ValueConsumer trick here, because the implementation requires state and cannot be
        // implemented with default methods alone.
    }

    /**
     * Process the trie using the given Walker, skipping all branches below the top content-bearing node.
     */
    public <R> R processSkippingBranches(Walker<T, R> walker, Direction direction)
    {
        return processSkippingBranches(walker, cursor(direction));
    }

    static <T, R> R processSkippingBranches(Walker<T, R> walker, Cursor<T> cursor)
    {
        assert cursor.depth() == 0 : "The provided cursor has already been advanced.";
        T content = cursor.content();   // handle content on the root node
        if (content != null)
        {
            walker.content(content);
            return walker.complete();
        }
        content = cursor.advanceToContent(walker);

        while (content != null)
        {
            walker.content(content);
            if (cursor.skipTo(cursor.depth(), cursor.incomingTransition() + cursor.direction().increase) < 0)
                break;
            walker.resetPathLength(cursor.depth() - 1);
            walker.addPathByte(cursor.incomingTransition());
            content = cursor.content();
            if (content == null)
                content = cursor.advanceToContent(walker);
        }
        return walker.complete();
    }

    /**
     * Map-like get by key.
     */
    public T get(ByteComparable key)
    {
        Cursor<T> cursor = cursor(Direction.FORWARD);
        if (cursor.descendAlong(key.asComparableBytes(cursor.byteComparableVersion())))
            return cursor.content();
        else
            return null;
    }

    /**
     * Constuct a textual representation of the trie.
     */
    public String dump()
    {
        return dump(Object::toString);
    }

    /**
     * Constuct a textual representation of the trie using the given content-to-string mapper.
     */
    public String dump(Function<T, String> contentToString)
    {
        return process(new TrieDumper<>(contentToString), Direction.FORWARD);
    }

    /**
     * Returns a singleton trie mapping the given byte path to content.
     */
    public static <T> Trie<T> singleton(ByteComparable b, ByteComparable.Version byteComparableVersion, T v)
    {
        return new SingletonTrie<>(b, byteComparableVersion, v);
    }

    /**
     * Returns a view of the subtrie containing everything in this trie whose keys fall between the given boundaries.
     * The view is live, i.e. any write to the source will be reflected in the subtrie.
     *
     * This method will throw an assertion error if the bounds provided are not correctly ordered, including with
     * respect to the `includeLeft` and `includeRight` constraints (i.e. subtrie(x, false, x, false) is an invalid call
     * but subtrie(x, true, x, false) is inefficient but fine for an empty subtrie).
     *
     * @param left the left bound for the returned subtrie. If {@code null}, the resulting subtrie is not left-bounded.
     * @param includeLeft whether {@code left} is an inclusive bound of not.
     * @param right the right bound for the returned subtrie. If {@code null}, the resulting subtrie is not right-bounded.
     * @param includeRight whether {@code right} is an inclusive bound of not.
     * @return a view of the subtrie containing all the keys of this trie falling between {@code left} (inclusively if
     * {@code includeLeft}) and {@code right} (inclusively if {@code includeRight}).
     */
    public Trie<T> subtrie(ByteComparable left, boolean includeLeft, ByteComparable right, boolean includeRight)
    {
        if (left == null && right == null)
            return this;
        return new SlicedTrie<>(this, left, includeLeft, right, includeRight);
    }

    /**
     * Returns a view of the subtrie containing everything in this trie whose keys fall between the given boundaries.
     * The view is live, i.e. any write to the source will be reflected in the subtrie.
     *
     * @param left the left bound for the returned subtrie, inclusive. If {@code null}, the resulting subtrie is not
     *             left-bounded.
     * @param right the right bound for the returned subtrie, exclusive. If {@code null}, the resulting subtrie is not
     *              right-bounded.
     * @return a view of the subtrie containing all the keys of this trie falling between {@code left} inclusively and
     * {@code right} exclusively.
     */
    public Trie<T> subtrie(ByteComparable left, ByteComparable right)
    {
        return subtrie(left, true, right, false);
    }

    /**
     * Returns the ordered entry set of this trie's content as an iterable.
     */
    public Iterable<Map.Entry<ByteComparable.Preencoded, T>> entrySet()
    {
        return this::entryIterator;
    }

    /**
     * Returns the ordered entry set of this trie's content as an iterable.
     */
    public Iterable<Map.Entry<ByteComparable.Preencoded, T>> entrySet(Direction direction)
    {
        return () -> entryIterator(direction);
    }

    /**
     * Returns the ordered entry set of this trie's content in an iterator.
     */
    public Iterator<Map.Entry<ByteComparable.Preencoded, T>> entryIterator()
    {
        return entryIterator(Direction.FORWARD);
    }

    /**
     * Returns the ordered entry set of this trie's content in an iterator.
     */
    public Iterator<Map.Entry<ByteComparable.Preencoded, T>> entryIterator(Direction direction)
    {
        return new TrieEntriesIterator.AsEntries<>(cursor(direction));
    }

    /**
     * Returns the ordered entry set of this trie's content in an iterable, filtered by the given type.
     */
    public <U extends T> Iterable<Map.Entry<ByteComparable.Preencoded, U>> filteredEntrySet(Class<U> clazz)
    {
        return filteredEntrySet(Direction.FORWARD, clazz);
    }

    /**
     * Returns the ordered entry set of this trie's content in an iterable, filtered by the given type.
     */
    public <U extends T> Iterable<Map.Entry<ByteComparable.Preencoded, U>> filteredEntrySet(Direction direction, Class<U> clazz)
    {
        return () -> filteredEntryIterator(direction, clazz);
    }

    /**
     * Returns the ordered entry set of this trie's content in an iterator, filtered by the given type.
     */
    public <U extends T> Iterator<Map.Entry<ByteComparable.Preencoded, U>> filteredEntryIterator(Direction direction, Class<U> clazz)
    {
        return new TrieEntriesIterator.AsEntriesFilteredByType<>(cursor(direction), clazz);
    }

    /**
     * Returns the ordered set of values of this trie as an iterable.
     */
    public Iterable<T> values()
    {
        return this::valueIterator;
    }

    /**
     * Returns the ordered set of values of this trie as an iterable.
     */
    public Iterable<T> values(Direction direction)
    {
        return direction.isForward() ? this::valueIterator : this::reverseValueIterator;
    }

    /**
     * Returns the ordered set of values of this trie in an iterator.
     */
    public Iterator<T> valueIterator()
    {
        return valueIterator(Direction.FORWARD);
    }

    /**
     * Returns the inversely ordered set of values of this trie in an iterator.
     */
    public Iterator<T> reverseValueIterator()
    {
        return valueIterator(Direction.REVERSE);
    }

    /**
     * Returns the ordered set of values of this trie in an iterator.
     */
    public Iterator<T> valueIterator(Direction direction)
    {
        return new TrieValuesIterator<>(cursor(direction));
    }

    /**
     * Returns the ordered set of values of this trie in an iterable, filtered by the given type.
     */
    public <U extends T> Iterable<U> filteredValues(Class<U> clazz)
    {
        return filteredValues(Direction.FORWARD, clazz);
    }

    /**
     * Returns the ordered set of values of this trie in an iterable, filtered by the given type.
     */
    public <U extends T> Iterable<U> filteredValues(Direction direction, Class<U> clazz)
    {
        return () -> filteredValuesIterator(direction, clazz);
    }

    /**
     * Returns the ordered set of values of this trie in an iterator, filtered by the given type.
     */
    public <U extends T> Iterator<U> filteredValuesIterator(Direction direction, Class<U> clazz)
    {
        return new TrieValuesIterator.FilteredByType<>(cursor(direction), clazz);
    }

    /**
     * Returns the values in any order. For some tries this is much faster than the ordered iterable.
     */
    public Iterable<T> valuesUnordered()
    {
        return values();
    }

    /**
     * Resolver of content of merged nodes, used for two-source merges (i.e. mergeWith).
     */
    public interface MergeResolver<T>
    {
        // Note: No guarantees about argument order.
        // E.g. during t1.mergeWith(t2, resolver), resolver may be called with t1 or t2's items as first argument.
        T resolve(T b1, T b2);
    }

    /**
     * Constructs a view of the merge of this trie with the given one. The view is live, i.e. any write to any of the
     * sources will be reflected in the merged view.
     *
     * If there is content for a given key in both sources, the resolver will be called to obtain the combination.
     * (The resolver will not be called if there's content from only one source.)
     */
    public Trie<T> mergeWith(Trie<T> other, MergeResolver<T> resolver)
    {
        return new MergeTrie<>(resolver, this, other);
    }

    /**
     * Resolver of content of merged nodes.
     *
     * The resolver's methods are only called if more than one of the merged nodes contain content, and the
     * order in which the arguments are given is not defined. Only present non-null values will be included in the
     * collection passed to the resolving methods.
     *
     * Can also be used as a two-source resolver.
     */
    public interface CollectionMergeResolver<T> extends MergeResolver<T>
    {
        T resolve(Collection<T> contents);

        @Override
        default T resolve(T c1, T c2)
        {
            return resolve(ImmutableList.of(c1, c2));
        }
    }

    private static final CollectionMergeResolver<Object> THROWING_RESOLVER = new CollectionMergeResolver<Object>()
    {
        @Override
        public Object resolve(Collection<Object> contents)
        {
            throw error();
        }

        private AssertionError error()
        {
            throw new AssertionError("Entries must be distinct.");
        }
    };

    /**
     * Returns a resolver that throws whenever more than one of the merged nodes contains content.
     * Can be used to merge tries that are known to have distinct content paths.
     */
    @SuppressWarnings("unchecked")
    public static <T> CollectionMergeResolver<T> throwingResolver()
    {
        return (CollectionMergeResolver<T>) THROWING_RESOLVER;
    }

    /**
     * Constructs a view of the merge of multiple tries. The view is live, i.e. any write to any of the
     * sources will be reflected in the merged view.
     *
     * If there is content for a given key in more than one sources, the resolver will be called to obtain the
     * combination. (The resolver will not be called if there's content from only one source.)
     */
    public static <T> Trie<T> merge(Collection<? extends Trie<T>> sources, CollectionMergeResolver<T> resolver)
    {
        switch (sources.size())
        {
        case 0:
            throw new AssertionError();
        case 1:
            return sources.iterator().next();
        case 2:
        {
            Iterator<? extends Trie<T>> it = sources.iterator();
            Trie<T> t1 = it.next();
            Trie<T> t2 = it.next();
            return t1.mergeWith(t2, resolver);
        }
        default:
            return new CollectionMergeTrie<>(sources, resolver);
        }
    }

    /**
     * Constructs a view of the merge of multiple tries, where each source must have distinct keys. The view is live,
     * i.e. any write to any of the sources will be reflected in the merged view.
     *
     * If there is content for a given key in more than one sources, the merge will throw an assertion error.
     */
    public static <T> Trie<T> mergeDistinct(Collection<? extends Trie<T>> sources)
    {
        switch (sources.size())
        {
        case 0:
            throw new AssertionError();
        case 1:
            return sources.iterator().next();
        case 2:
        {
            Iterator<? extends Trie<T>> it = sources.iterator();
            Trie<T> t1 = it.next();
            Trie<T> t2 = it.next();
            return new MergeTrie.Distinct<>(t1, t2);
        }
        default:
            return new CollectionMergeTrie.Distinct<>(sources);
        }
    }

    /**
     * Returns a Trie that is a view of this one, where the given prefix is prepended before the root.
     */
    public Trie<T> prefixedBy(ByteComparable prefix)
    {
        return new PrefixedTrie(prefix, this);
    }

    /**
     * Returns an entry set containing all tail tree constructed at the points that contain content of
     * the given type.
     */
    public Iterable<Map.Entry<ByteComparable.Preencoded, Trie<T>>> tailTries(Direction direction, Class<? extends T> clazz)
    {
        return () -> new TrieTailsIterator.AsEntries<>(cursor(direction), clazz);
    }

    /**
     * Returns a trie that corresponds to the branch of this trie rooted at the given prefix.
     * <p>
     * The result will include the same values as {@code subtrie(prefix, nextBranch(prefix))}, but the keys in the
     * resulting trie will not include the prefix. In other words,
     *   {@code tailTrie(prefix).prefixedBy(prefix) = subtrie(prefix, nextBranch(prefix))}
     * where nextBranch stands for the key adjusted by adding one at the last position.
     */
    public Trie<T> tailTrie(ByteComparable prefix)
    {
        Cursor<T> c = cursor(Direction.FORWARD);
        if (c.descendAlong(prefix.asComparableBytes(c.byteComparableVersion())))
            return c.tailTrie();
        else
            return null;
    }

    public static <T> Trie<T> empty(ByteComparable.Version byteComparableVersion)
    {
        return new Trie<T>()
        {
            public Cursor<T> cursor(Direction dir)
            {
                return new EmptyCursor<>(dir, byteComparableVersion);
            }
        };
    }

    static class EmptyCursor<T> implements Cursor<T>
    {
        private final Direction direction;
        private final ByteComparable.Version byteComparableVersion;
        int depth;

        public EmptyCursor(Direction direction, ByteComparable.Version byteComparableVersion)
        {
            this.direction = direction;
            this.byteComparableVersion = byteComparableVersion;
            depth = 0;
        }

        public int advance()
        {
            return depth = -1;
        }

        public int skipTo(int skipDepth, int skipTransition)
        {
            return depth = -1;
        }

        public ByteComparable.Version byteComparableVersion()
        {
            if (byteComparableVersion != null)
                return byteComparableVersion;
            throw new AssertionError();
        }

        @Override
        public Trie<T> tailTrie()
        {
            assert depth == 0 : "tailTrie called on exhausted cursor";
            return empty(byteComparableVersion);
        }

        public int depth()
        {
            return depth;
        }

        public T content()
        {
            return null;
        }

        @Override
        public Direction direction()
        {
            return direction;
        }

        public int incomingTransition()
        {
            return -1;
        }
    }
}
