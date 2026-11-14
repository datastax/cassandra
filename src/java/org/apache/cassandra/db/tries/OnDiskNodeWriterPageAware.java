/*
 * Copyright IBM Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.db.tries;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.TreeSet;

import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.Hex;

/// Lays the nodes of a trie out so that they make good use of the pages of the output: a reader that descends through
/// the trie should have to fetch as few pages as possible, so a node and the branch below it should share a page
/// whenever they can.
///
/// To do this, completed nodes are not written but kept in memory as [Buffered] nodes hanging from their parents, for
/// as long as the branch they form is no bigger than a page. When a node completes and the branch below it has
/// outgrown a page, its children are laid out ([#layout]): each child, with all of its branch, is written to the page
/// the output is currently in if it fits in what is left of it, largest first, and the output is padded to the next
/// page when none does. A branch that is written this way is never split across pages. The parent stays in memory with
/// its children's positions, to be packed in turn with its own siblings.
///
/// A node written after its children have been laid out no longer has its first child immediately before it, where
/// the chain, sparse and bitmap node types expect to find it. Such a node is preceded by a relay node (see
/// [OnDiskWriteNodeType.WithRelay]) that gives the position of the first child.
///
/// ### Nested tries
///
/// The alternate branch of a node, i.e. the deletion branch of a node of a deletion-aware trie, is treated as one more
/// child of the node: it stays in memory with it, counts towards the size of its branch, is written before the node's
/// other children when the branch is written as a whole, and takes part in the layout when it is not. A deletion
/// branch thus shares a page with the node it belongs to whenever the node's branch fits in one.
///
/// For this the node writer of the nested tries ([#nested]) does not write a trie out when its walk completes, but
/// holds on to its root until [#attachBranch] moves it into a node of the main trie. The nodes of the nested tries
/// are [Buffered] nodes like all others, and everything that sizes, lays out and writes nodes treats them alike: the
/// only thing that differs between the two tries is the type of their content, which a buffered node holds in
/// serialized form. Like any other, a nested trie is laid out while it is being walked if it outgrows a page.
///
/// ### Sizes
///
/// The pointers of a node are stored as distances and take as many bytes as the furthest one needs, so the size of a
/// node that has children or an alternate branch already on disk depends on where the node itself is going to be
/// written. The size of a buffered branch is therefore a function of the position it would start at. This class
/// computes it exactly, by following the steps the write will take, and caches it in each node together with the
/// range of start positions the value holds for ([#sizeAt]); the decisions that determine the encoding of a node are
/// made by the same method ([OnDiskWriteNodeType#selectChildrenType]) for sizing and for writing.
///
/// To know the size of a node's content, it is serialized when the node is completed, and it is the serialized form
/// that is kept in memory until the node is written.
///
/// ### Limits
///
/// A node that is by itself larger than a page (a large payload, or a wide node when pages are small) cannot be kept
/// within one and is written across page boundaries, starting on a new page if that saves crossing one. This is only
/// possible if the positions of the output are continuous across page boundaries: a node is read by stepping
/// backwards from its position, which cannot work over a gap. Plain files satisfy this; an output that does not,
/// like [org.apache.cassandra.io.compress.EncryptedSequentialWriter], cannot hold such a node, and writing one to it
/// fails.
///
/// The choice of what to place in a page is greedy and can waste space when the children of a node are of very
/// different sizes: a child that takes nearly a whole page has to start a new one, and everything that remains of
/// the page it was to follow is padding. A long branch with one small child at each level is the worst case.
///
/// The amount of data held in memory is bounded by the page size: each node on the trie writer's path can hold up to 256
/// completed children and an alternate branch, each forming a branch no bigger than a page.
///
/// Writing and sizing a buffered branch are recursive; to keep the stack they need bounded regardless of the page
/// size and the shape of the trie, a branch whose depth of buffered nodes exceeds [#MAX_BUFFERED_DEPTH] is laid out
/// in the same way as one that has outgrown a page.
public final class OnDiskNodeWriterPageAware<T> extends OnDiskNodeWriter<T>
{
    /// The maximum number of levels of buffered nodes in a branch.
    static final int MAX_BUFFERED_DEPTH = 128;

    private final int maxBytesPerPage;
    private final boolean foldChains;
    /// True if this writes tries nested in another, which are not written out when finished, see [#finish].
    private final boolean isNested;

    /// The node last given to [#complete], waiting to be attached to its parent or to be finished as the root. For a
    /// nested writer, this is also where the root of a finished trie waits for [#attachBranch].
    private Buffered lastCompleted;

    // Scratch space for passing the children of a node to the encoders.
    private long[] scratchPositions = new long[4];
    private byte[] scratchTransitions = new byte[4];

    /// A prepared reused TreeSet for ordering nodes in a layout.
    private final TreeSet<Buffered> reusableLayoutSet = new TreeSet<>(LARGEST_LAST);
    /// A node the size of the remaining space in the page, to search the children that fit in it.
    private final Buffered spaceLeft = new Buffered();

    /// Where the content of a completed node is serialized.
    private final DataOutputBuffer scratch = new DataOutputBuffer();

    /// Writes content that is already serialized.
    private static final OnDiskTrieWriter.DataSerializer<byte[]> SERIALIZED = (out, bytes) -> out.write(bytes);

    public OnDiskNodeWriterPageAware(DataOutputPlus out, OnDiskTrieWriter.DataSerializer<T> serializer)
    {
        this(out, serializer, true);
    }

    /// @param foldChains whether to apply the chain folding described in [#complete]. Disabling it makes the node
    ///                   writer produce exactly the nodes [OnDiskNodeWriterImmediate] writes, which the tests use to
    ///                   compare the two.
    OnDiskNodeWriterPageAware(DataOutputPlus out, OnDiskTrieWriter.DataSerializer<T> serializer, boolean foldChains)
    {
        this(out, serializer, foldChains, false);
    }

    private OnDiskNodeWriterPageAware(DataOutputPlus out, OnDiskTrieWriter.DataSerializer<T> serializer, boolean foldChains, boolean isNested)
    {
        super(out, serializer);
        this.maxBytesPerPage = out.maxBytesInPage();
        this.foldChains = foldChains;
        this.isNested = isNested;
        spaceLeft.transition = Integer.MAX_VALUE;   // goes after all nodes of the same size in LARGEST_LAST
    }

    /// A node on the trie writer's path, with the children completed below it so far.
    static final class PathNode<T> extends Node<T>
    {
        private static final Buffered[] NO_CHILDREN = new Buffered[0];

        /// Grown on demand; a node can have up to 256 children, but the vast majority have very few.
        Buffered[] children = NO_CHILDREN;
        int childCount = 0;
        /// The root of the node's alternate branch, null if it has none.
        Buffered alternateBranch;

        void addChild(Buffered child)
        {
            if (childCount == children.length)
                children = Arrays.copyOf(children, Math.max(4, childCount * 2));
            children[childCount++] = child;
        }

        /// Returns the children collected so far, or null if there are none, and leaves the node without children.
        Buffered[] takeChildren()
        {
            if (childCount == 0)
                return null;
            Buffered[] taken = Arrays.copyOf(children, childCount);
            // clear the used part of the array to avoid retaining the completed branches
            Arrays.fill(children, 0, childCount, null);
            childCount = 0;
            return taken;
        }
    }

    /// A completed node. It is either buffered, in which case it holds everything needed to write it and any of its
    /// children may themselves be buffered or written, or written, in which case only its position remains.
    ///
    /// The nodes of a trie and of the tries nested in it are linked together and handled alike, so this is not tied to
    /// a type of content; a node holds its content as the bytes it is to be written as.
    static final class Buffered
    {
        /// The transition that leads to this node from its parent. Children of a node have distinct transitions; an
        /// alternate branch is given one above them all.
        int transition;
        /// True if the transition from the parent is to be moved to the start of `chain`, see [#complete].
        boolean folded;
        /// The transitions after the parent's that lead to this node, written as chain nodes after it. Null if none.
        byte[] chain;
        /// The children of this node in descending order of their transitions, or null if it has none.
        Buffered[] children;
        /// The root of the alternate branch of this node, or null if it has none.
        Buffered alternateBranch;
        /// The serialized content for the node's two content slots, null where there is none.
        byte[] descentContent;
        byte[] ascentContent;
        /// The size of the payload node the content will be written as, 0 if there is none. This does not include the
        /// pointer to the alternate branch.
        int payloadSize;

        /// The position of the node, i.e. where its serialization ends, or 0 if it has not been written yet.
        long position = 0;

        /// The size of what is yet to be written of the branch rooted at this node, i.e. this node and all unwritten
        /// nodes below it, when the first of its bytes is written at any position in `[sizeValidFrom, sizeValidUntil)`.
        /// `sizeValidFrom` appears unnecessary, but we size children in their write order, while layout may move some
        /// ahead, which can result in them becoming smaller.
        long size;
        long sizeValidFrom = 0;
        long sizeValidUntil = Long.MIN_VALUE;
        /// The number of levels of unwritten nodes in the branch. Set together with `size`.
        int depth;

        boolean isWritten()
        {
            return position > 0;
        }

        @Override
        public String toString()
        {
            if (isWritten())
                return String.format("Written at: %x", position);
            String res = String.format("%02x%s ", transition, folded ? "(folded)" : "");
            if (chain != null)
                res += Hex.bytesToHex(chain) + " ";
            if (descentContent != null)
                res += "D[" + Hex.bytesToHex(descentContent) + "] ";
            if (ascentContent != null)
                res += "A[" + Hex.bytesToHex(ascentContent) + "] ";
            if (alternateBranch != null)
                res += String.format("alt: %x ", alternateBranch.position);
            if (children != null)
                for (Buffered child : children)
                    res += String.format("%02x: %x ", child.transition, child.position);
            return res;
        }
    }

    /// The transition given to the alternate branch of a node to place it among the node's children.
    private static final int ALTERNATE_BRANCH_TRANSITION = 256;

    /// Orders the nodes to lay out by size. (Note: we never modify a node or its size while it is in the layout set.)
    /// Siblings of the same size are told apart by their transitions, with the
    /// highest placed last so that it is the first one taken: this is the order in which children are written when
    /// they are not laid out, which puts the earlier children of a node closer to it.
    private static final Comparator<Buffered> LARGEST_LAST = (l, r) ->
    {
        int c = Long.compare(l.size, r.size);
        return c != 0 ? c : Integer.compare(l.transition, r.transition);
    };

    @Override
    Node<T> newNode()
    {
        return new PathNode<>();
    }

    /// Turns the completed path node into a buffered one, and lays its branch out if it has grown too big to be
    /// buffered.
    ///
    /// If the node has a single child that is still buffered and starts with a chain, the transition to that child is
    /// folded into the child's chain. This saves the single-transition chain node that the node would otherwise be
    /// written with, as well as a step for the readers. (See [OnDiskNodeWriterImmediate] for why it does not do the
    /// same.)
    ///
    /// A node that has nothing of its own, i.e. no content and no alternate branch, and a single child that is still
    /// buffered is not a node at all, only a stretch of the path to that child. The trie writer makes one when a
    /// branch that leads to no content is walked after the child (e.g. in an intersection, or a view that drops
    /// content). Such a node is not buffered: the child takes its place, with the transitions that lead to the node
    /// and the one from the node to the child added to the front of its chain. Besides the bytes this saves, it is
    /// what guarantees that a relay never leads to another relay, which readers reject: if the folding above were
    /// applied here and the child then written away from the node, a relay would be all there is to the node, and
    /// thus what a relay in front of the node's parent would lead to.
    @Override
    void complete(Node<T> pathNode, byte[] path, int chainStart, int chainEnd) throws IOException
    {
        PathNode<T> source = (PathNode<T>) pathNode;
        Buffered node = new Buffered();
        node.children = source.takeChildren();
        node.alternateBranch = source.alternateBranch;
        node.descentContent = serialize(source.descentContent);
        node.ascentContent = serialize(source.ascentContent);
        node.payloadSize = OnDiskWriteNodeType.sizePayload(node.descentContent != null ? node.descentContent.length : -1,
                                                           node.ascentContent != null ? node.ascentContent.length : -1,
                                                           node.alternateBranch != null,
                                                           node.children != null);
        assert node.children != null || node.payloadSize > 0;
        source.alternateBranch = null;
        source.descentContent = null;
        source.ascentContent = null;

        Buffered onlyBufferedChild = null;
        if (foldChains && node.children != null && node.children.length == 1 && !node.children[0].isWritten())
            onlyBufferedChild = node.children[0];

        if (onlyBufferedChild != null && node.payloadSize == 0)
        {
            // Nothing but a part of the path to the child, which takes the place of the node.
            node = onlyBufferedChild;
            node.chain = joinChains(path, chainStart, chainEnd, node.transition, node.chain);
            invalidateSize(node);
        }
        else
        {
            if (chainEnd > chainStart)
                node.chain = Arrays.copyOfRange(path, chainStart, chainEnd);

            if (onlyBufferedChild != null && OnDiskWriteNodeType.isExpandableChain(onlyBufferedChild.chain))
            {
                onlyBufferedChild.folded = true;
                invalidateSize(onlyBufferedChild);
            }
        }

        sizeAt(node, out.position());
        if (needsLayout(node))
            layout(node, reusableLayoutSet);

        lastCompleted = node;
    }

    /// The chain of a child that takes the place of its parent: the transitions `path[chainStart, chainEnd)` that
    /// lead to the parent, the transition from the parent to the child, and the child's own chain, if it has one.
    private static byte[] joinChains(byte[] path, int chainStart, int chainEnd, int transition, byte[] childChain)
    {
        int parentLength = chainEnd - chainStart;
        int childLength = childChain != null ? childChain.length : 0;
        byte[] chain = new byte[parentLength + 1 + childLength];
        System.arraycopy(path, chainStart, chain, 0, parentLength);
        chain[parentLength] = (byte) transition;
        if (childLength > 0)
            System.arraycopy(childChain, 0, chain, parentLength + 1, childLength);
        return chain;
    }

    /// The serialized form of the given content, null if there is none.
    private byte[] serialize(T content) throws IOException
    {
        if (content == null)
            return null;
        scratch.clear();
        serializer.serialize(scratch, content);
        return scratch.toByteArray();
    }

    @Override
    void attachCompleted(Node<T> parent, int transition)
    {
        lastCompleted.transition = transition;
        ((PathNode<T>) parent).addChild(lastCompleted);
        lastCompleted = null;
    }

    /// Writes what remains of the trie, unless this is a nested writer.
    ///
    /// The root may already have been written when it was completed, if it is larger than a page by itself. Otherwise
    /// what remains of the trie is no bigger than a page, and is kept in one by moving to a new page if it does not fit
    /// the current one.
    ///
    /// A nested writer leaves the trie for the node it is going to be attached to, which will place it together with
    /// the rest of its branch.
    @Override
    long finish() throws IOException
    {
        Buffered root = lastCompleted;
        if (isNested)
            return root.position; // 0 if not yet written, a valid position otherwise

        lastCompleted = null;
        int bytesLeft = out.bytesLeftInPage();
        while (!root.isWritten())
        {
            long size = sizeAt(root, out.position());
            if (size <= bytesLeft)
                writeBranch(root, size, bytesLeft);
            else if (size <= maxBytesPerPage)
            {
                out.padToPageBoundary();
                bytesLeft = maxBytesPerPage;
            }
            else
            {
                // the distances to written nodes have grown with the padding, and with them the root
                layout(root, reusableLayoutSet);
                bytesLeft = out.bytesLeftInPage();
            }
        }
        assert root.position == out.position();
        return root.position;
    }

    @Override
    <B> OnDiskNodeWriter<B> nested(OnDiskTrieWriter.DataSerializer<B> serializer)
    {
        return new OnDiskNodeWriterPageAware<>(out, serializer, foldChains, true);
    }

    @Override
    void attachBranch(Node<T> node, OnDiskNodeWriter<?> nested)
    {
        OnDiskNodeWriterPageAware<?> nestedWriter = (OnDiskNodeWriterPageAware<?>) nested;
        assert nestedWriter.isNested;
        Buffered root = nestedWriter.lastCompleted;
        assert root != null : "No trie to attach";
        nestedWriter.lastCompleted = null;

        PathNode<T> pathNode = (PathNode<T>) node;
        assert pathNode.alternateBranch == null;
        root.transition = ALTERNATE_BRANCH_TRANSITION;
        pathNode.alternateBranch = root;
    }

    @Override
    void reset()
    {
        lastCompleted = null;
    }

    // Sizing

    /// The number of bytes that writing the branch rooted at `node` will take if the first of them is written at the
    /// position `start`, where a branch is a node with all of the nodes below it that are still unwritten. This is
    /// what [#writeBranch] writes; it does not include any padding.
    ///
    /// The value is cached in the node for as long as the question is about the same range of positions and nothing
    /// in the branch has been written; code that writes part of a branch must call [#invalidateSize].
    private long sizeAt(Buffered node, long start)
    {
        if (start >= node.sizeValidFrom && start < node.sizeValidUntil)
            return node.size;
        return computeSize(node, start);
    }

    private static void invalidateSize(Buffered node)
    {
        node.sizeValidUntil = Long.MIN_VALUE;
    }

    private boolean needsLayout(Buffered node)
    {
        return node.size > maxBytesPerPage || node.depth > MAX_BUFFERED_DEPTH;
    }

    /// Calculates [#sizeAt] by following what [#writeBranch] does.
    ///
    /// The result holds for the given start position, and remains the same for later start positions for as long as
    /// no pointer in the branch needs a different number of bytes. The distances that grow with the start position
    /// are the ones to nodes that are already written, so a branch that has none and no alternate branches, which is
    /// the case for the vast majority, has a size that is valid everywhere.
    private long computeSize(Buffered node, long start)
    {
        node.size = 0;
        node.sizeValidUntil = Long.MAX_VALUE;
        node.depth = 1;
        // Note: this size is probably also valid for earlier positions; in particular, setting `start` here under the
        // sizing call of a node that ends up being larger that the page limit means that the node will likely be
        // re-sized again during the `layout` call.
        // However, the above only happens if there was no previous sizing that would cover the write position, i.e.
        // if the pointer sizes do change, which is not likely to happen that often, especially for a large trie.
        node.sizeValidFrom = start;

        // The alternate branch is written first if it has not been written yet.
        long alternatePos = sizeAlternateBranch(node, start);

        // Unwritten children are written next, in order, with the node that addresses them.
        sizeChildren(node, start);

        // The payload, with the pointer to the alternate branch follows.
        node.size += node.payloadSize + sizeAlternateBranchPointer(node, start, alternatePos);

        // And finally the leading chain.
        if (node.chain != null)
            node.size += OnDiskWriteNodeType.sizeChain(node.chain.length) + (node.folded ? 1 : 0);

        return node.size;
    }

    private void sizeChildren(Buffered node, long start)
    {
        Buffered[] children = node.children;
        if (children != null)
        {
            int count = children.length;
            long furthest = Long.MAX_VALUE;
            long first = 0;
            boolean anyWritten = false;
            for (int i = 0; i < count; ++i)
            {
                Buffered child = children[i];
                long childPos = child.position;
                if (childPos <= 0)
                {
                    long childSize = sizeAt(child, start + node.size);
                    // The child's size holds while the child starts before its limit; it starts `size` bytes after us.
                    node.sizeValidUntil = Math.min(node.sizeValidUntil, child.sizeValidUntil - node.size);
                    node.size += childSize;
                    childPos = start + node.size;
                    node.depth = Math.max(node.depth, child.depth + 1);
                }
                else
                    anyWritten = true;

                if (i < count - 1)
                    furthest = Math.min(furthest, childPos);
                else
                    first = childPos;
            }

            // Size the node that lists the children.
            long base = start + node.size;
            long furthestDelta = base - furthest;
            long firstDelta = base - first;
            OnDiskWriteNodeType nodeType = OnDiskWriteNodeType.selectChildrenType(furthestDelta, firstDelta, count, children[0].folded);
            node.size += nodeType.sizeChildren(count);
            if (anyWritten)
                node.sizeValidUntil = Math.min(node.sizeValidUntil, addValidFor(start, nodeType.sizeValidFor(furthestDelta, firstDelta, children[count - 1].isWritten())));
        }
    }

    private long sizeAlternateBranch(Buffered node, long start)
    {
        Buffered alternateBranch = node.alternateBranch;
        if (alternateBranch == null)
            return 0;

        long alternatePos = alternateBranch.position;
        if (alternatePos > 0)
            return alternatePos;

        node.size += sizeAt(alternateBranch, start);
        node.sizeValidUntil = Math.min(node.sizeValidUntil, alternateBranch.sizeValidUntil);
        node.depth = Math.max(node.depth, alternateBranch.depth + 1);
        return start + node.size;
    }

    private static long sizeAlternateBranchPointer(Buffered node, long start, long alternatePos)
    {
        if (alternatePos <= 0)
            return 0;

        // The branch is referenced by its distance from the payload, which is what comes next.
        long delta = start + node.size - alternatePos;
        // If the branch is to be written with us, the distance is the size of what is written between the two and
        // does not change with our position.
        if (node.alternateBranch.isWritten())
            node.sizeValidUntil = Math.min(node.sizeValidUntil, addValidFor(start, delta == 0 ? 1 : positionsWithSameBytes(delta)));
        return OnDiskWriteNodeType.sizeAlternateBranchPointer(delta);
    }

    /// The number of consecutive values, starting with the given one, that need the same number of bytes.
    static long positionsWithSameBytes(long value)
    {
        int bytes = OnDiskBaseTrieWriter.bytesFor(value);
        return bytes == 8 ? Long.MAX_VALUE : (1L << (bytes * 8)) - value;
    }

    /// The end of a range of `sizeValidFor` positions that begins at `start`.
    private static long addValidFor(long start, long validFor)
    {
        return validFor == Long.MAX_VALUE ? Long.MAX_VALUE : start + validFor;
    }

    // Layout

    /// Called for a node whose branch cannot remain buffered as it is. Writes enough of the branch for what remains
    /// of it to be suitable for buffering again: on return the node is either written, or forms with what is still
    /// unwritten below it a branch that is no larger than a page when placed at the current position.
    ///
    /// The alternate branch of the node is laid out together with its children.
    private void layout(Buffered node, TreeSet<Buffered> layoutSetToReuse) throws IOException
    {
        Buffered[] children = node.children;
        Buffered alternateBranch = node.alternateBranch;
        if (children != null || alternateBranch != null)
        {
            // The branch may be too big only because some of the children have themselves outgrown a page, which
            // can happen when the distance to their written descendants has grown since they were buffered. If so,
            // lay those out and see if this is sufficient, to keep as much as possible of the branch together.
            boolean laidOutChild = layoutIfOutgrown(alternateBranch, layoutSetToReuse);
            if (children != null)
                for (Buffered child : children)
                    laidOutChild |= layoutIfOutgrown(child, layoutSetToReuse);

            if (laidOutChild)
            {
                invalidateSize(node);
                sizeAt(node, out.position());
                if (!needsLayout(node))
                    return;
            }

            // Write all the children, each with all of its branch in one page.
            layoutChildren(layoutSetToReuse, alternateBranch, children);
            invalidateSize(node);
        }

        // With all children placed the node can normally wait to be laid out with its own siblings. If it is larger
        // than a page by itself, there is no point in holding on to it.
        long nodeSize = sizeAt(node, out.position());
        if (nodeSize > maxBytesPerPage)
            writeStartingNewPageIfBetter(node, nodeSize);
    }

    private void layoutChildren(TreeSet<Buffered> pending, Buffered alternateBranch, Buffered[] children) throws IOException
    {
        if (pending == null)
            pending = new TreeSet<>(LARGEST_LAST);

        queue(pending, alternateBranch);
        if (children != null)
            for (Buffered child : children)
                queue(pending, child);

        while (!pending.isEmpty())
        {
            // Take the largest child that fits what remains of the page. If none does, move to a new page
            // and take the largest of all.
            int bytesLeft = out.bytesLeftInPage();
            spaceLeft.size = bytesLeft;
            Buffered child = pending.floor(spaceLeft);
            if (child == null)
            {
                out.padToPageBoundary();
                bytesLeft = maxBytesPerPage;
                child = pending.last();
            }
            pending.remove(child);

            // The child was sized when it was queued, and that size may no longer hold where we are now.
            long size = sizeAt(child, out.position());
            if (size <= bytesLeft)
                writeBranch(child, size, bytesLeft);
            else
            {
                // If the child has now outgrown a page, lay it out and let what remains of it, if anything,
                // wait its turn with the rest. Otherwise it will have to be tried again in a page with more room.
                if (size > maxBytesPerPage)
                    layout(child, null); // pending is already in use, make a new set
                queue(pending, child);
            }
        }
    }

    /// Adds the given child to the set of nodes to lay out, if it is still unwritten.
    private void queue(TreeSet<Buffered> pending, Buffered child)
    {
        if (child == null || child.isWritten())
            return;
        sizeAt(child, out.position());
        boolean added = pending.add(child);
        assert added;
    }

    /// Lays out the given child if its branch is larger than a page when placed at the current position.
    private boolean layoutIfOutgrown(Buffered child, TreeSet<Buffered> layoutSetToReuse) throws IOException
    {
        if (child == null || child.isWritten() || sizeAt(child, out.position()) <= maxBytesPerPage)
            return false;
        layout(child, layoutSetToReuse);
        return true;
    }

    /// Writes a node that is larger than a page, with whatever is still unwritten below it. It will have to cross
    /// page boundaries; it is started on a new page if using up the rest of the current one would make it cross one
    /// more boundary than necessary.
    private void writeStartingNewPageIfBetter(Buffered node, long size) throws IOException
    {
        int bytesLeft = out.bytesLeftInPage();
        long inLastPage = (size - 1) % maxBytesPerPage + 1;
        if (bytesLeft < inLastPage)
        {
            out.padToPageBoundary();
            bytesLeft = maxBytesPerPage;
            size = sizeAt(node, out.position());
        }
        writeBranch(node, size, bytesLeft);
    }

    // Writing

    /// Writes the branch rooted at the given node at the current position, where `size` is its [#sizeAt] for that
    /// position and `bytesLeft` the space that remains in the page.
    private void writeBranch(Buffered node, long size, int bytesLeft) throws IOException
    {
        long start = out.position();
        writeBranch(node);
        long written = out.position() - start;
        // What is written within a page must take exactly the size that was calculated for it. What crosses page
        // boundaries, i.e. a node that is larger than a page, can also be off if the output's positions are not
        // continuous there. Such a node cannot be read back, so do not let it pass.
        if (written != size && size > bytesLeft)
            throw new IOException(String.format("A node expected to take %d bytes at position %x took %d. " +
                                                "A node larger than a page (%d bytes) can only be written to an " +
                                                "output whose positions are continuous across page boundaries.",
                                                size, start, written, maxBytesPerPage));
        assert written == size
            : String.format("Branch expected to take %d bytes at %x took %d", size, start, written);
    }

    /// Writes the alternate branch and all children of the node that are not written yet, recursively and in order,
    /// and then the node itself.
    private void writeBranch(Buffered node) throws IOException
    {
        Buffered alternateBranch = node.alternateBranch;
        if (alternateBranch != null && !alternateBranch.isWritten())
            writeBranch(alternateBranch);

        Buffered[] children = node.children;
        if (children != null)
            for (Buffered child : children)
                if (!child.isWritten())
                    writeBranch(child);

        writeNode(node);
    }

    /// Writes a node whose children and alternate branch are all written, and releases everything but its position.
    private void writeNode(Buffered node) throws IOException
    {
        Buffered[] children = node.children;
        boolean hasChildren = children != null;
        if (hasChildren)
        {
            int count = children.length;
            if (scratchPositions.length < count)
            {
                scratchPositions = new long[256];
                scratchTransitions = new byte[256];
            }
            long[] positions = scratchPositions;
            byte[] transitions = scratchTransitions;

            long base = out.position();
            long furthest = Long.MAX_VALUE;
            for (int i = 0; i < count; ++i)
            {
                Buffered child = children[i];
                assert child.isWritten() && child.position <= base;
                positions[i] = child.position;
                transitions[i] = (byte) child.transition;
                if (i < count - 1)
                    furthest = Math.min(furthest, child.position);
            }

            boolean folded = children[0].folded;
            // Nothing is written to list a folded child, so a relay to it must have the payload after it (see complete).
            assert !folded || node.payloadSize > 0 : "Folded the only child of a node without payload";
            OnDiskWriteNodeType nodeType = OnDiskWriteNodeType.selectChildrenType(base - furthest, base - positions[count - 1], count, folded);
            nodeType.writeChildren(out, positions, transitions, count, base);
        }

        Buffered alternateBranch = node.alternateBranch;
        assert alternateBranch == null || alternateBranch.isWritten();
        if (node.payloadSize > 0)
            OnDiskWriteNodeType.writePayload(out,
                                             SERIALIZED,
                                             node.descentContent,
                                             node.ascentContent,
                                             alternateBranch != null ? alternateBranch.position : 0,
                                             hasChildren);

        if (node.chain != null)
        {
            if (node.folded)
                OnDiskWriteNodeType.writeChainFolded(out, node.chain, 0, node.chain.length, node.transition);
            else
                OnDiskWriteNodeType.writeChain(out, node.chain, 0, node.chain.length);
        }

        node.position = out.position();
        node.children = null;
        node.alternateBranch = null;
        node.chain = null;
        node.descentContent = null;
        node.ascentContent = null;
    }
}
