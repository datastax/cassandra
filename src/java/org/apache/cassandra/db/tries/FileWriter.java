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

import java.io.IOException;
import java.util.Arrays;
import java.util.NavigableSet;
import java.util.TreeSet;

import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.utils.Hex;
import org.apache.cassandra.utils.vint.VIntCoding;

/// Written bottom-to-top, i.e. children first, with negative deltas for every pointer. Everything except value is
/// written reversed
/// Types:
/// - leaf up to 63 bytes
///   `[data bytes] 00nnnnnn`
/// - generic content; child immediately before
///   p - has child
///   d - has descent content
///   a - has ascent content
///   `[ascent data bytes] [varint-encoded ascent data length] [descent data bytes] [varint-encoded descent data length] 11110dap`
/// - chain up to 64 bytes; child immediately before
///   `[byte n-1] ... [byte 1] [byte 0] 01nnnnnn`
/// - sparse up to 25 children; last child immediately before
///   bb - bytes per pointer - 1
///   nnnnn - child count - 2
///   `[child n-2] ... [child 1] [child 0] [byte n-1] ... [byte 1][byte 0]1nnnnnbb`
/// - bitmap
///   `[child n-2] ... [child 1] [child 0] [32-byte bitmap] 11100bbb`
/// - dense full
///   `[child 255] ... [child 1] [child 0] 11101bbb`
/// - relay (epsilon transition)
///   `[child] 11111bbb`

public class FileWriter<T> extends TriePathReconstructor implements Cursor.Walker<T, DataOutputPlus>
{

    interface DataSerializer<T>
    {
        int serializedSize(T value);
        int serialize(DataOutputPlus out, T value) throws IOException;
    }

    final DataOutputPlus out;
    final DataSerializer<T> dataSerializer;
    final boolean swapAscentAndDescentSides;
    final int maxBytesPerPage;

    InProgressNode<T>[] nodesOnPath = new InProgressNode[32];
    int lastNodeOnPath = -1;
    boolean onAscentPath = false;

    TreeSet<Node<T>> reusableTreeSet = new TreeSet<>();
    Node<T> reusableBoundaryNode = Node.make(0, null, null, null, null);
    NavigableSet<Node<T>> reusableHeadSet = reusableTreeSet.headSet(reusableBoundaryNode, true);

    public FileWriter(DataOutputPlus out, DataSerializer<T> dataSerializer, boolean swapAscentAndDescentSides)
    {
        this.out = out;
        this.dataSerializer = dataSerializer;
        this.swapAscentAndDescentSides = swapAscentAndDescentSides;
        this.maxBytesPerPage = out.maxBytesInPage();
    }

    // on way down, only collect path (base class)
    // if there's content, make a node and add to nodesOnPath

    @Override
    public void content(T content)
    {
        if (lastNodeOnPath >= 0)
        {
            InProgressNode<T> lastNode = nodesOnPath[lastNodeOnPath];
            if (lastNode.depth == keyPos)
            {
                assert onAscentPath;
                lastNode.ascentPathContent = content;
                return;
            }
            assert lastNode.depth < keyPos;
        }

        InProgressNode<T> node = addNewNode(keyPos);
        if (onAscentPath)
            node.ascentPathContent = content;
        else
            node.descentPathContent = content;
    }

    private InProgressNode<T> addNewNode(int depth)
    {
        if (++lastNodeOnPath >= nodesOnPath.length)
            nodesOnPath = Arrays.copyOf(nodesOnPath, lastNodeOnPath * 2);

        InProgressNode<T> node = nodesOnPath[lastNodeOnPath];
        if (node == null)
            nodesOnPath[lastNodeOnPath] = node = new InProgressNode<>();

        node.depth = depth;
        return node;
    }

    @Override
    public void addPathByte(int nextByte)
    {
        super.addPathByte(nextByte);
        onAscentPath = false;
    }

    public void ascendTo(long newEncodedPosition) throws IOException
    {
        // ascend until closest node on path (if there's content, it will be current depth)
        //   if ascend depth reached, exit
        if (lastNodeOnPath < 0)
        {
            // fully empty trie; file is left empty, root is at 0
            return;
        }

        // throw away any path that did not result in content
        InProgressNode<T> node = nodesOnPath[lastNodeOnPath];
        assert node.depth <= keyPos;
        keyPos = node.depth;

        int newLength = Math.max(Cursor.depth(newEncodedPosition) - 1, -1);
        // If we are returning to an ascent-path position that doesn't advance, prepare to add content to the node at
        // this depth rather than add a new child to parent.
        if (Cursor.isOnReturnPath(newEncodedPosition) && (newLength == -1 || (keyBytes[newLength] & 0xFF) == Cursor.incomingTransition(newEncodedPosition)))
            ++newLength;

        // repeat:
        while (newLength < node.depth)
        {
            --lastNodeOnPath;
            InProgressNode<T> next = lastNodeOnPath >= 0 ? nodesOnPath[lastNodeOnPath] : null;
            //   the path to reach this node includes the key bytes until there's node on path or we reach ascent depth - 1
            int stopDepth = (next != null ? Math.max(next.depth, newLength) : newLength);
            //   complete node, possibly write page
            Node<T> completedNode = completeNode(node, stopDepth);

            --keyPos;

            //   if reached exhausted, child is root
            if (keyPos < 0)
            {
                writeNodeRecursively(completedNode);
                return; // the current file position is the root position
            }

            //   if reached ascend depth, add node on path, add child and exit
            if (next == null || next.depth < keyPos)
                next = addNewNode(keyPos);

            //   add child with last byte and pointer to child
            next.addChild(completedNode);

            node = next;
        }
    }

    private Node<T> completeNode(InProgressNode<T> node, int stopDepth) throws IOException
    {
        byte[] otherBytes = keyPos > stopDepth + 1 ? Arrays.copyOfRange(keyBytes, stopDepth + 1, keyPos) : null;
        keyPos = stopDepth + 1;
        Node<T> completed = node.complete(stopDepth >= 0 ? keyBytes[stopDepth] & 0xFF : 0, otherBytes, swapAscentAndDescentSides);
        long position = out.position();
        long branchSize = completed.prepareBranchSize(dataSerializer, position);
        if (branchSize > maxBytesPerPage)
            layoutChildren(completed, branchSize, position);
        return completed;
    }

    private long layoutChildren(Node<T> completed, long branchSize, long position) throws IOException
    {
        if (completed.children == null)
        {
            // This node is large because it has a large payload. Just write it now.
            // TODO: Maybe deal with this better?
            return writeWithPadding(completed, branchSize);
        }

        TreeSet<Node<T>> orderedChildren = reusableTreeSet;
        for (Node<T> child : completed.children)
        {
            if (child.writtenFilePos >= 0)
                continue;

            child.getBranchSize(dataSerializer, position); // update currentBranchSize
            orderedChildren.add(child);
        }

        if (orderedChildren.isEmpty())
        {
            // This node itself has become too big, likely due to combination of pointer sizes and data. Write it now.
            return writeWithPadding(completed, branchSize);
        }

        // First make sure we don't have a child that itself has become larger than a page because of growing distance
        // to children, and lay its children out instead.
        boolean hadChildLargerThanAPage = false;
        while (!orderedChildren.isEmpty() && orderedChildren.last().currentBranchSize > maxBytesPerPage)
        {
            hadChildLargerThanAPage = true;
            Node<T> last = orderedChildren.pollLast();
            position = layoutChildren(last, last.getBranchSize(dataSerializer, position), position);
        }
        if (hadChildLargerThanAPage)
        {
            // update size to reflect written children
            branchSize = completed.prepareBranchSize(dataSerializer, position);
            if (branchSize <= maxBytesPerPage)
                return position; // This branch now fits a page, no need to do anything further
        }

        Node<T> boundary = reusableBoundaryNode;
        // Note that this map's methods reflect changes in boundary on each pollLast call.
        NavigableSet<Node<T>> fitting = reusableHeadSet;
        while (!orderedChildren.isEmpty())
        {
            boundary.currentBranchSize = out.bytesLeftInPage();
            Node<T> node = fitting.pollLast();
            if (node == null)
            {
                position = out.padToPageBoundary();
                continue;
            }

            // size may have grown because the position advanced
            long nodeSize = node.getBranchSize(dataSerializer, position);
            if (nodeSize <= boundary.currentBranchSize)
            {
                position = writeNodeRecursively(node); // most common path
            }
            else if (nodeSize <= maxBytesPerPage)
            {
                // Size changed. Put the node back and retry selection.
                orderedChildren.add(node);
            }
            else
            {
                // This node became bigger than a page because of other sibling moving the writing position.
                // This should be extremely rare and can't be easily avoided.
                position = layoutChildren(node, nodeSize, position);
                if (node.writtenFilePos < 0)
                {
                    nodeSize = node.getBranchSize(dataSerializer, position);
                    // if leading node can fit, put it with its branch
                    if (nodeSize <= out.bytesLeftInPage())
                        position = writeNode(node); // no need for recursive call, children are already laid out
                    else
                        orderedChildren.add(node);
                }
                // else we're done, the parent was also written
            }
        }

        // update the node size to reflect written children
        branchSize = completed.prepareBranchSize(dataSerializer, position);
        if (branchSize > maxBytesPerPage)  // if the node itself is large, place it now
            position = writeWithPadding(completed, branchSize);
        return position;
    }

    private long writeWithPadding(Node<T> completed, long branchSize) throws IOException
    {
        // If using the remainder of the page splits the node in one more page than necessary, advance to a new page.
        if (out.bytesLeftInPage() < branchSize % maxBytesPerPage)
            out.padToPageBoundary();

        return writeNodeRecursively(completed);
    }

    @Override
    public void onReturnPath()
    {
        onAscentPath = true;
    }

    public DataOutputPlus complete()
    {
        return out;
    }

    static void writeReversedSized(DataOutputPlus out, long value, int bytes) throws IOException
    {
        // since data is read back-to-front, big endian means writing the top byte last
        out.writeMostSignificantBytes(Long.reverseBytes(value), bytes);
    }

    static void writeReversedVint(DataOutputPlus out, long value) throws IOException
    {
        assert value >= 0;
        if (value < 128)
            out.writeByte((int) value);
        else
        {
            int size = VIntCoding.computeUnsignedVIntSize(value);
            if (size < 9)
            {
                int extraBytes = size - 1;
                long mask = (long) VIntCoding.encodeExtraBytesToRead(extraBytes) << (extraBytes << 3);
                writeReversedSized(out, value | mask, size);
            }
            else if (size == 9)
            {
                // The leading byte goes last, like the one writeReversedSized puts in the top byte
                // of the shorter encodings: the reader takes the byte immediately before the value
                // as the leading one.
                out.writeLong(Long.reverseBytes(value));
                out.write((byte) 0xFF);
            }
            else
            {
                throw new AssertionError();
            }
        }
    }

    private long writeNode(Node<T> node) throws IOException
    {
        boolean hasChildren = node.children != null;
        assert hasChildren || node.ascentPathContent != null || node.descentPathContent != null;

        if (hasChildren)    // TODO: if child contains a chain and we are making a new one, join them
            writeChildrenOfNode(node);

        if (node.ascentPathContent != null || node.descentPathContent != null)
            OnDiskWriteNodeType.writePayload(out, dataSerializer, node.descentPathContent, node.ascentPathContent, hasChildren);

        if (node.otherTransitions != null)
            OnDiskWriteNodeType.writeChain(out, node.otherTransitions);

        return node.finalizeWithPos(out.position());
    }

    private void writeChildrenOfNode(Node<T> node) throws IOException
    {
        int size = node.childCount();
        long basePos = out.position();
        long furthestChild = Long.MAX_VALUE;
        for (Node<T> child : node.children)
            furthestChild = Math.min(furthestChild, child.writtenFilePos);
        assert furthestChild >= 0 && furthestChild <= basePos;
        int bytesPerPointer = bytesFor(basePos - furthestChild);
        OnDiskWriteNodeType type = OnDiskWriteNodeType.selectChildrenType(bytesPerPointer, size);
        type.writeChildren(out, node.children, basePos, bytesPerPointer);
    }

    private long writeNodeRecursively(Node<T> node) throws IOException
    {
        long expectedPos = out.position() + node.currentBranchSize;
        if (node.children != null)
        {
            for (Node<T> child : node.children)
                if (child.writtenFilePos < 0)
                    writeNodeRecursively(child);
        }

        long pos = writeNode(node);
        // The evaluation can be slightly wrong, especially in the presence of relays (because we may use a smaller
        // size for them or skip them altogether) but can't be smaller than what we actually end up with.
        assert pos <= expectedPos;
        return pos;
    }

    static int bytesFor(long delta)
    {
        return 8 - Long.numberOfLeadingZeros(delta | 1L) / 8; // at least 1
    }

    static class Node<T> implements Comparable<Node<T>>
    {
        // Values must be cleared when the node is written
        int firstTransition;
        byte[] otherTransitions;
        Node[] children;
        T descentPathContent;
        T ascentPathContent;

        long writtenFilePos;

        long currentBranchSize;
        long branchSizeValidUntilPosition;
        // TODO: data items longer than (half a) page must be placed separately

        public static <T> Node<T> make(int firstTransition, byte[] otherTransitions, Node[] children, T descentPathContent, T ascentPathContent)
        {
            if (children != null && children.length == 1 && children[0].otherTransitions != null)
            {
                // If we have only one child, and that child starts with a chain prefix, put our transition in its chain.
                children[0] = children[0].copyWithAttachedPrefixByte();
            }
            // Use 0 for "valid until" to force preparation on first `getBranchSize`
            return new Node<>(firstTransition, otherTransitions, children, descentPathContent, ascentPathContent, 0, 0);
        }

        private Node(int firstTransition, byte[] otherTransitions, Node[] children, T descentPathContent, T ascentPathContent, long currentBranchSize, long branchSizeValidUntilPosition)
        {
            this.firstTransition = firstTransition;
            this.otherTransitions = otherTransitions;
            this.children = children;
            this.descentPathContent = descentPathContent;
            this.ascentPathContent = ascentPathContent;
            this.branchSizeValidUntilPosition = branchSizeValidUntilPosition;
            this.currentBranchSize = currentBranchSize;
            this.writtenFilePos = -1;
        }

        private Node<T> copyWithAttachedPrefixByte()
        {
            assert writtenFilePos < 0;
            assert firstTransition != -1;
            return new Node<>(-1, insertFirst(otherTransitions, firstTransition), children, descentPathContent, ascentPathContent, currentBranchSize + 1, branchSizeValidUntilPosition);
        }

        private static byte[] insertFirst(byte[] transitions, int firstByte)
        {
            byte[] copy = new byte[transitions.length + 1];
            copy[0] = (byte) firstByte;
            System.arraycopy(transitions, 0, copy, 1, transitions.length);
            return copy;
        }

        private long addUnwrittenChildSizes(DataSerializer<T> serializer, long runningPos)
        {
            for (Node<T> child : children)
            {
                if (child.writtenFilePos >= 0)
                    continue;

                if (runningPos > child.branchSizeValidUntilPosition)
                    child.prepareBranchSize(serializer, runningPos);

                branchSizeValidUntilPosition = Math.min(branchSizeValidUntilPosition, child.branchSizeValidUntilPosition);
                long childSize = child.currentBranchSize;
                currentBranchSize += childSize;
                runningPos += childSize;
            }
            return runningPos;
        }

        private void addWrittenChildPointerSizes(long unwrittenStart, long basePos)
        {
            long furthestWrittenChild = Long.MAX_VALUE;
            for (Node<T> child : children)
            {
                long childPos = child.writtenFilePos;
                if (childPos < 0)
                    continue;
                furthestWrittenChild = Math.min(furthestWrittenChild, childPos);
            }

            long furthestUnwrittenChild = unwrittenStart + children[0].currentBranchSize; // we don't need to seek inside the first child's branch
            long furthestChild = Math.min(furthestWrittenChild, furthestUnwrittenChild);

            int bytes = bytesFor(basePos - furthestChild);
            long validUntil = bytes == 8 ? Long.MAX_VALUE : (furthestWrittenChild + (1L << (bytes * 8)));
            branchSizeValidUntilPosition = Math.min(branchSizeValidUntilPosition, validUntil);

            OnDiskWriteNodeType type = OnDiskWriteNodeType.selectChildrenType(bytes, children.length);
            long childrenSize = type.sizeChildren(bytes, children);

            currentBranchSize += childrenSize;
        }

        long prepareBranchSize(DataSerializer<T> serializer, long positionForSizeCalculations)
        {
            currentBranchSize = 0;
            branchSizeValidUntilPosition = Long.MAX_VALUE;

            if (children != null)
            {
                long basePos = addUnwrittenChildSizes(serializer, positionForSizeCalculations);
                addWrittenChildPointerSizes(positionForSizeCalculations, basePos);
            }

            if (descentPathContent != null || ascentPathContent != null)
                currentBranchSize += OnDiskWriteNodeType.sizePayload(serializer, descentPathContent, ascentPathContent, children != null);

            if (otherTransitions != null)
                currentBranchSize += OnDiskWriteNodeType.sizeChain(otherTransitions);

            return currentBranchSize;
        }

        long getBranchSize(DataSerializer<T> serializer, long positionedAt)
        {
            if (positionedAt >= branchSizeValidUntilPosition)
                prepareBranchSize(serializer, positionedAt);
            return currentBranchSize;
        }

        long finalizeWithPos(long writtenFilePos)
        {
            this.writtenFilePos = writtenFilePos;
            this.otherTransitions = null;
            this.children = null;
            this.descentPathContent = null;
            this.ascentPathContent = null;
            this.currentBranchSize = 0;
            this.branchSizeValidUntilPosition = Long.MAX_VALUE;
            return writtenFilePos;
        }

        public int childCount()
        {
            return children != null ? children.length : 0;
        }

        public int childTransition(int i)
        {
            return children[i].firstTransition;
        }

        public long child(int i)
        {
            return children[i].writtenFilePos;
        }

        @Override
        public String toString()
        {
            if (writtenFilePos >= 0)
                return String.format("Written at: %x", writtenFilePos);
            String res = "";

            if (otherTransitions != null)
                res += Hex.bytesToHex(otherTransitions) + " ";

            if (descentPathContent != null)
                res += "D[" + descentPathContent + "] ";
            if (ascentPathContent != null)
                res += "A[" + ascentPathContent + "] ";

            if (children != null)
            {
                for (int i = 0; i < children.length; ++i)
                    res += String.format("%02x: %x ", children[i].firstTransition, children[i].writtenFilePos);
            }

            return res;
        }

        @Override
        public int compareTo(Node<T> other)
        {
            int cmp = Long.compare(currentBranchSize, other.currentBranchSize);
            if (cmp != 0)
                return cmp;
            return -Integer.compare(firstTransition, other.firstTransition);
        }
    }

    // reusable
    static class InProgressNode<T>
    {
        private static final Node[] NO_CHILDREN = new Node[0];

        int depth;

        T descentPathContent;
        T ascentPathContent;
        /// Grown on demand by [#addChild]; a node can have up to 256 children, but the vast majority have very few.
        private Node[] children = NO_CHILDREN;
        private int childCount = 0;

        private Node<T> complete(int firstTransition, byte[] otherTransitions, boolean swapAscentAndDescentSides)
        {
            Node<T> completed = Node.make(firstTransition, otherTransitions, childCount > 0 ? Arrays.copyOf(children, childCount) : null,
                                          swapAscentAndDescentSides ? ascentPathContent : descentPathContent,
                                          swapAscentAndDescentSides ? descentPathContent : ascentPathContent);
            reset();
            return completed;
        }

        void reset()
        {
            // clear the used part of the array to avoid retaining the completed branches; the entries above
            // childCount are already null because every reset restores this state
            Arrays.fill(children, 0, childCount, null);
            childCount = 0;
            descentPathContent = null;
            ascentPathContent = null;
        }

        void addChild(Node target)
        {
            if (childCount == children.length)
                children = Arrays.copyOf(children, Math.max(4, childCount * 2));
            children[childCount++] = target;
        }

        @Override
        public String toString()
        {
            String res = "";
            if (descentPathContent != null)
                res += "D[" + descentPathContent + "] ";
            if (ascentPathContent != null)
                res += "A[" + ascentPathContent + "] ";

            for (int i = 0; i < childCount; ++i)
                res += String.format("%02x: %x ", children[i].firstTransition, children[i].writtenFilePos);

            return res;
        }
    }

    public static <T> File write(BaseTrie<T, ?, ?> trie, boolean isOrdered, DataSerializer<T> serializer, File file) throws IOException
    {
        try (SequentialWriter writer = new SequentialWriter(file))
        {
            FileWriter<T> fw = new FileWriter<>(writer, serializer, isOrdered);

            Cursor<T> c = trie.cursor(Direction.REVERSE);
            T content = c.content();   // handle content on the root node
            if (content != null)
                fw.content(content);

            long prevPosition = c.encodedPosition();
            while (true)
            {
                long currPosition = c.advanceMultiple(fw);

                if (Cursor.ascended(currPosition, prevPosition))
                {
                    // write the nodes that have been completed
                    fw.ascendTo(currPosition);

                    if (Cursor.isExhausted(currPosition))
                    {
                        writer.finish();
                        return file;
                    }

                    // update key tracker
                    int depth = Cursor.depth(currPosition);
                    if (depth > 0)
                    {
                        fw.resetPathLength(depth - 1);
                        fw.addPathByte(Cursor.incomingTransition(currPosition));
                    }
                }
                else
                    fw.addPathByte(Cursor.incomingTransition(currPosition));

                if (Cursor.isOnReturnPath(currPosition))
                    fw.onReturnPath();

                content = c.content();
                if (content != null)
                    fw.content(content);

                prevPosition = currPosition;
            }
        }
    }
}
