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

import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.utils.vint.VIntCoding;

/// Written bottom-to-top, i.e. children first, with negative deltas for every pointer. It also walks the trie in
/// reverse order to write the first child closest to the parent.
///
/// Every node is written as soon as the walk leaves it: the only state kept is the path to the current position and,
/// for each node on it, the content seen so far and the positions of the children already written. Everything except
/// value is written reversed. See [OnDiskTrie.md](./OnDiskTrie.md) for details.
public class OnDiskTrieWriter<T> extends TriePathReconstructor implements Cursor.Walker<T, DataOutputPlus>
{
    interface DataSerializer<T>
    {
        /// Writes `value` to `out` and returns the number of bytes written.
        int serialize(DataOutputPlus out, T value) throws IOException;
    }

    final DataOutputPlus out;
    final DataSerializer<T> dataSerializer;
    final boolean swapAscentAndDescentSides;

    Node<T>[] nodesOnPath = new Node[32];
    int lastNodeOnPath = -1;
    boolean onAscentPath = false;

    public OnDiskTrieWriter(DataOutputPlus out, DataSerializer<T> dataSerializer, boolean swapAscentAndDescentSides)
    {
        this.out = out;
        this.dataSerializer = dataSerializer;
        this.swapAscentAndDescentSides = swapAscentAndDescentSides;
    }

    // On the way down we only collect the path (base class). A node is created when content is found or when a
    // child completes, so a path that leads to nothing leaves nothing behind.

    @Override
    public void content(T content)
    {
        Node<T> node = nodeAtCurrentPosition();
        if (onAscentPath)
        {
            assert node.ascentPathContent == null;
            node.ascentPathContent = content;
        }
        else
        {
            assert node.descentPathContent == null;
            node.descentPathContent = content;
        }
    }

    /// Attach a payload to the ascent-side content slot of the node at the current position, whatever path the walk
    /// is on. [OnDiskDeletionAwareTrieWriter] uses this for the deletion-branch pointer: the slot a range trie fills with
    /// return-path content is free in a trie whose content is descent-side only, which is the same reuse
    /// [InMemoryTrie] makes of a prefix node's alternate branch pointer.
    public void ascentContent(T content)
    {
        Node<T> node = nodeAtCurrentPosition();
        assert node.ascentPathContent == null;
        node.ascentPathContent = content;
    }

    private Node<T> nodeAtCurrentPosition()
    {
        if (lastNodeOnPath >= 0)
        {
            Node<T> lastNode = nodesOnPath[lastNodeOnPath];
            if (lastNode.depth == keyPos)
                return lastNode;
            assert lastNode.depth < keyPos;
        }
        return addNewNode(keyPos);
    }

    private Node<T> addNewNode(int depth)
    {
        if (++lastNodeOnPath >= nodesOnPath.length)
            nodesOnPath = Arrays.copyOf(nodesOnPath, lastNodeOnPath * 2);

        Node<T> node = nodesOnPath[lastNodeOnPath];
        if (node == null)
            nodesOnPath[lastNodeOnPath] = node = new Node<>();

        node.depth = depth;
        return node;
    }

    @Override
    public void addPathByte(int nextByte)
    {
        super.addPathByte(nextByte);
        onAscentPath = false;
    }

    /// Write out every node the walk has finished with, i.e. every node on the path deeper than the position the
    /// cursor ascended to, deepest first. Each node is written, then the chain of transitions between it and the
    /// next shallower node on the path (or the ascent depth, whichever is deeper), and the resulting position is
    /// recorded as a child of that parent, which is created if the path had no node at that depth.
    public void ascendTo(long newEncodedPosition) throws IOException
    {
        if (lastNodeOnPath < 0)
        {
            // fully empty trie; file is left empty, root is at 0
            return;
        }

        // throw away any path that did not result in content
        Node<T> node = nodesOnPath[lastNodeOnPath];
        assert node.depth <= keyPos;
        keyPos = node.depth;

        int newLength = Math.max(Cursor.depth(newEncodedPosition) - 1, -1);
        // If we are returning to an ascent-path position that doesn't advance, prepare to add content to the node at
        // this depth rather than add a new child to parent.
        if (Cursor.isOnReturnPath(newEncodedPosition) && (newLength == -1 || (keyBytes[newLength] & 0xFF) == Cursor.incomingTransition(newEncodedPosition)))
            ++newLength;

        while (newLength < node.depth)
        {
            writeAndRecycleNode(node);
            --lastNodeOnPath;
            Node<T> next = lastNodeOnPath >= 0 ? nodesOnPath[lastNodeOnPath] : null;
            // the path to reach this node includes the key bytes until there's a node on path or we reach ascent depth - 1
            int stopDepth = (next != null ? Math.max(next.depth, newLength) : newLength);
            popAndWriteChain(stopDepth + 1);

            --keyPos;

            // if reached exhausted, the node just written is the root
            if (keyPos < 0)
                return; // the current file position is the root position

            // if reached ascend depth, add node on path, add child and exit
            if (next == null || next.depth < keyPos)
                next = addNewNode(keyPos);

            // add child with last byte and pointer to child
            next.addChild(keyBytes[keyPos] & 0xFF, out.position());

            node = next;
        }
    }

    /// Write the transitions `keyBytes[stopDepth, keyPos)` as chain nodes, deepest transition first, in pieces of at
    /// most [OnDiskWriteNodeType#MAX_CHAIN_LENGTH_INCLUSIVE] with the deepest piece the full one, and leave `keyPos`
    /// at `stopDepth`.
    private void popAndWriteChain(int stopDepth) throws IOException
    {
        while (keyPos > stopDepth)
        {
            int written = 0;
            while (keyPos > stopDepth && written < OnDiskWriteNodeType.MAX_CHAIN_LENGTH_INCLUSIVE)
            {
                out.writeByte(keyBytes[--keyPos]);
                ++written;
            }
            out.writeByte(OnDiskWriteNodeType.CHAIN.bits | (written - 1));
        }
    }

    private void writeAndRecycleNode(Node<T> node) throws IOException
    {
        boolean hasChildren = node.childCount() > 0;
        T descentContent = swapAscentAndDescentSides ? node.ascentPathContent : node.descentPathContent;
        T ascentContent = swapAscentAndDescentSides ? node.descentPathContent : node.ascentPathContent;
        boolean hasContent = descentContent != null || ascentContent != null;
        assert hasChildren || hasContent;

        if (hasChildren)
            writeChildrenOfNode(node);

        if (hasContent)
            OnDiskWriteNodeType.writePayload(out, dataSerializer, descentContent, ascentContent, hasChildren);

        node.reset();
    }

    private void writeChildrenOfNode(Node<T> node) throws IOException
    {
        int count = node.childCount();
        long basePos = out.position();
        // Children are recorded in the order they were written, so the first is the furthest away and the last is
        // immediately before this node (delta 0, which the sparse, bitmap and chain types leave implicit).
        if (node.child(count - 1) != basePos)
            throw new IllegalStateException("Last written child at " + node.child(count - 1) +
                                            " does not immediately precede its parent at " + basePos);
        long furthestChild = node.child(0);
        assert furthestChild >= 0 && furthestChild <= basePos;
        int bytesPerPointer = bytesFor(basePos - furthestChild);
        OnDiskWriteNodeType type = OnDiskWriteNodeType.selectChildrenType(bytesPerPointer, count);
        type.writeChildren(out, node, basePos, bytesPerPointer);
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

    static int bytesFor(long delta)
    {
        return 8 - Long.numberOfLeadingZeros(delta | 1L) / 8; // at least 1
    }

    /// A node on the current path: its content, and the children that have been completed and written so far. One
    /// instance per depth is reused for the whole write.
    static class Node<T>
    {
        private static final long[] NO_POSITIONS = new long[0];
        private static final byte[] NO_TRANSITIONS = new byte[0];

        int depth;

        T descentPathContent;
        T ascentPathContent;
        /// Positions (the end of each child's serialization) and transition bytes of the children written so far, in
        /// the order they were written. Grown on demand: a node can have up to 256 children, but the vast majority
        /// have very few.
        private long[] childPositions = NO_POSITIONS;
        private byte[] childTransitions = NO_TRANSITIONS;
        private int childCount = 0;

        int childCount()
        {
            return childCount;
        }

        int childTransition(int index)
        {
            return childTransitions[index] & 0xFF;
        }

        long child(int index)
        {
            return childPositions[index];
        }

        void addChild(int transition, long position)
        {
            if (childCount == childPositions.length)
            {
                int newLength = Math.max(4, childCount * 2);
                childPositions = Arrays.copyOf(childPositions, newLength);
                childTransitions = Arrays.copyOf(childTransitions, newLength);
            }
            childPositions[childCount] = position;
            childTransitions[childCount] = (byte) transition;
            ++childCount;
        }

        void reset()
        {
            childCount = 0;
            descentPathContent = null;
            ascentPathContent = null;
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
                res += String.format("%02x: %x ", childTransition(i), child(i));

            return res;
        }
    }

    /// Serialize a trie into `out`. Returns the position of the root, which is the position of `out` when this
    /// returns, or -1 if the trie is empty; a non-empty trie always writes at least one byte.
    public static <T> long write(BaseTrie<T, ?, ?> trie, boolean isOrdered, DataSerializer<T> serializer, DataOutputPlus out) throws IOException
    {
        OnDiskTrieWriter<T> fw = new OnDiskTrieWriter<>(out, serializer, isOrdered);
        long startPosition = out.position();

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
                    // An empty trie leaves the stream untouched, and the position there is not a node.
                    long position = out.position();
                    return position > startPosition ? position : -1;
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

    public static <T> File write(BaseTrie<T, ?, ?> trie, boolean isOrdered, DataSerializer<T> serializer, File file) throws IOException
    {
        try (SequentialWriter writer = new SequentialWriter(file))
        {
            write(trie, isOrdered, serializer, writer);
            writer.finish();
            return file;
        }
    }
}
