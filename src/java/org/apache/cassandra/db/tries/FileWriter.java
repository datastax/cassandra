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
import java.util.BitSet;

import org.agrona.DirectBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.SequentialWriter;
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
///   `[ascent data bytes] [varint-encoded ascent data length] [descent data bytes] [varint-encoded descent data length] 01000adp`
/// - link (epsilon transition)
///   `[varint-encoded delta] 01000000`
/// - chain up to 64 bytes; child immediately before
///   `[byte n-1] ... [byte 1] [byte 0] 10nnnnnn`
/// - sparse up to 25 children; last child immediately before
///   bb - bytes per pointer - 1
///   nnnn - child count - 2
///   `[byte n-1][child n-2][byte n-2] ... [child 1][byte 1][child 0][byte 0]11bbnnnn`
/// - sparse generic
///   `[byte n-1][child n-2][byte n-2] ... [child 1][byte 1][child 0][byte 0][child count n]01001bbb`
/// - sparse bitmap
///   `[child n-2] ... [child 1] [child 0] [32-byte bitmap] 01011bbb`
/// - dense range
///   `[child n-2] ... [child 1] [child 0] [to] [from] 01110bbb`
/// - dense full
///   `[child 254] ... [child 1] [child 0] 01111bbb`

public class FileWriter<T> extends TriePathReconstructor implements Cursor.Walker<T, DataOutputPlus>
{
    static final int MAX_LEAF_LENGTH_INCLUSIVE = 63;
    static final int BITS_LEAF = 0b00000000;    // Must be 00 so leaf nodecode reads correctly as varint

    static final int MAX_CHAIN_LENGTH_INCLUSIVE = 64;
    static final int BITS_CHAIN = 0b01000000;

    // sparse: 0x80 - 0xE0 (96 positions) 1lllllbb, lllll is count - 2 and must be < 24, bb is bytes per pointer
    static final int MAX_SPARSE_LENGTH_INCLUSIVE = 25;
    static final int MAX_SPARSE_BYTES = 4;
    static final int BITS_SPARSE = 0b10000000;
    static final int SHIFT_SPARSE_LENGTH = 2;

    static final int BITS_PREFIX = 0b11100000;
    static final int PREFIX_HAS_CHILD = 0b00000001;
    static final int PREFIX_HAS_ASCENT_CONTENT = 0b00000010;
    static final int PREFIX_HAS_DESCENT_CONTENT = 0b00000100;

    static final int BITS_BITMAP = 0b11101000;
    static final int BITS_DENSE = 0b11110000;

    static final int BITS_RESERVED = 0b11111000;

    interface DataSerializer<T>
    {
        int serialize(DataOutputPlus out, T value) throws IOException;
    }

    final DataOutputPlus out;
    final DataSerializer<T> dataSerializer;
    final boolean swapAscentAndDescentSides;

    Node<T>[] nodesOnPath = new Node[32];
    int lastNodeOnPath = -1;
    boolean onAscentPath = false;

    public FileWriter(DataOutputPlus out, DataSerializer<T> dataSerializer, boolean swapAscentAndDescentSides)
    {
        this.out = out;
        this.dataSerializer = dataSerializer;
        this.swapAscentAndDescentSides = swapAscentAndDescentSides;
    }

    // on way down, only collect path (base class)
    // if there's content, make a node and add to nodesOnPath

    @Override
    public void content(T content)
    {
        if (lastNodeOnPath >= 0)
        {
            Node<T> lastNode = nodesOnPath[lastNodeOnPath];
            if (lastNode.depth == keyPos)
            {
                assert onAscentPath;
                lastNode.ascentPathContent = content;
                return;
            }
            assert lastNode.depth < keyPos;
        }

        Node<T> node = addNewNode(keyPos);
        if (onAscentPath)
            node.ascentPathContent = content;
        else
            node.descentPathContent = content;
    }

    private Node<T> addNewNode(int depth)
    {
        if (++lastNodeOnPath >= nodesOnPath.length)
            nodesOnPath = Arrays.copyOf(nodesOnPath, lastNodeOnPath * 2);

        Node<T> node = nodesOnPath[lastNodeOnPath];
        if (node == null)
            nodesOnPath[lastNodeOnPath] = node = new Node<T>();

        node.depth = depth;
        node.reset();
        return node;
    }

    @Override
    public void addPathByte(int nextByte)
    {
        super.addPathByte(nextByte);
        onAscentPath = false;
    }

    public void ascendTo(long newEncodedPosition)
    {
        // ascend until closest node on path (if there's content, it will be current depth)
        //   if ascend depth reached, exit
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

        try
        {
            // repeat:
            while (newLength < node.depth)
            {
                //   write node
                writeAndRecycleNode(node);
                --lastNodeOnPath;
                Node<T> next = lastNodeOnPath >= 0 ? nodesOnPath[lastNodeOnPath] : null;
                //   until there's node on path or we reach ascent depth - 1, write a byte, form chain and set child to be the chain
                int stopDepth = (next != null ? Math.max(next.depth, newLength) : newLength);
                popAndWriteChain(stopDepth + 1);

                --keyPos;

                //   if reached exhausted, child is root
                if (keyPos < 0)
                    return; // the current file position is the root position

                //   if reached ascend depth, add node on path, add child and exit
                if (next == null || next.depth < keyPos)
                    next = addNewNode(keyPos);

                //   add child with last byte and pointer to child
                next.addChild(keyBytes[keyPos], out.position());

                node = next;
            }
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    private void popAndWriteChain(int stopDepth) throws IOException
    {
        while (keyPos > stopDepth)
        {
            int written = 0;
            while (keyPos > stopDepth && written < MAX_CHAIN_LENGTH_INCLUSIVE)
            {
                out.writeByte(keyBytes[--keyPos]);
                ++written;
            }
            out.writeByte(BITS_CHAIN | (written - 1));
        }
    }

    @Override
    public void onReturnPath()
    {
        onAscentPath = true;
    }

    @Override
    public DataOutputPlus complete()
    {
        return out;
    }

    private void writeLeaf(T data) throws IOException
    {
        int length = dataSerializer.serialize(out, data);
        if (length <= MAX_LEAF_LENGTH_INCLUSIVE)
            out.writeByte(BITS_LEAF | length);
        else
        {
            writeReversedVint(length);
            // data-only prefix
            out.writeByte(BITS_PREFIX | PREFIX_HAS_DESCENT_CONTENT);
        }
    }

    private void writeContentPrefix(T descentPath, T ascentPath, boolean hasChildren) throws IOException
    {
        int code = BITS_PREFIX;
        if (hasChildren)
            code |= PREFIX_HAS_CHILD;
        if (ascentPath != null)
        {
            writeContentWithSize(ascentPath);
            code |= PREFIX_HAS_ASCENT_CONTENT;
        }
        if (descentPath != null)
        {
            writeContentWithSize(descentPath);
            code |= PREFIX_HAS_DESCENT_CONTENT;
        }
        out.writeByte(code);
    }

    private void writeContentWithSize(T content) throws IOException
    {
        int length = dataSerializer.serialize(out, content);
        writeReversedVint(length);
    }

    private void writeReversedSized(long value, int bytes) throws IOException
    {
        // since data is read back-to-front, big endian means writing the top byte last
        out.writeMostSignificantBytes(Long.reverseBytes(value), bytes);
    }

    private void writeReversedVint(long value) throws IOException
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
                writeReversedSized(value | mask, size);
            }
            else if (size == 9)
            {
                out.write((byte) 0xFF);
                out.writeLong(Long.reverseBytes(value));
            }
            else
            {
                throw new AssertionError();
            }
        }
    }

    private void writeAndRecycleNode(Node<T> node) throws IOException
    {
        boolean hasChildren = node.childCount() > 0;
        if (swapAscentAndDescentSides)
        {
            T t = node.ascentPathContent; node.ascentPathContent = node.descentPathContent; node.descentPathContent = t;
        }

        if (!hasChildren && node.ascentPathContent == null)
            writeLeaf(node.descentPathContent);
        else
        {
            boolean hasContent = node.descentPathContent != null || node.ascentPathContent != null;
            assert hasContent || node.childCount() > 1;
            if (hasChildren)
                writeChildrenOfNode(node);

            if (hasContent)
                writeContentPrefix(node.descentPathContent, node.ascentPathContent, hasChildren);
        }

        node.reset();
    }

    private void writeChildrenOfNode(Node<T> node) throws IOException
    {
        int size = node.childCount();
        if (size == 1)
        {
            out.writeByte(node.childTransition(0));
            out.writeByte(BITS_CHAIN | 0);
            return;
        }

        long basePos = out.position();
        assert node.child(size - 1) == basePos;
        long maxDiff = basePos - node.child(0);
        int bytesPerPointer = 8 - Long.numberOfLeadingZeros(maxDiff | 1L) / 8; // at least 1
        if (size <= MAX_SPARSE_LENGTH_INCLUSIVE && bytesPerPointer <= MAX_SPARSE_BYTES)
            writeSparse(node, basePos, bytesPerPointer);
        else if (size < 256 - 32 / bytesPerPointer)
            writeBitmap(node, basePos, bytesPerPointer);
        else
            writeDense(node, basePos);
    }

    private void writeSparse(Node<T> node, long basePos, int bytesPerPointer) throws IOException
    {
        int size = node.childCount();
        // last pointer is implicitly 0
        assert node.child(size - 1) == basePos;
        for (int i = size - 2; i >= 0; --i)
            writeReversedSized(basePos - node.child(i), bytesPerPointer);
        for (int i = size - 1; i >= 0; --i)
            out.writeByte(node.childTransition(i));
        out.writeByte(BITS_SPARSE | ((size - 2) << SHIFT_SPARSE_LENGTH) | (bytesPerPointer - 1));
    }

    private void writeBitmap(Node<T> node, long basePos, int bytesPerPointer) throws IOException
    {
        int size = node.childCount();
        // last pointer is implicitly 0
        assert node.child(size - 1) == basePos;
        for (int i = size - 2; i >= 0; --i)
            writeReversedSized(basePos - node.child(i), bytesPerPointer);
        BitSet bits = new BitSet(256);
        for (int i = 0; i < size; ++i)
            bits.set(node.childTransition(i));
        long[] bitsAsLong = bits.toLongArray();
        for (int i = 3; i >= 0; --i)
            out.writeLong(bitsAsLong[i]);   // lowest-order bytes ends up last
        out.writeByte(BITS_BITMAP | (bytesPerPointer - 1));
    }

    private void writeDense(Node<T> node, long basePos) throws IOException
    {
        int size = node.childCount();
        long maxDiff = basePos - node.child(0) + 1; // accommodate null, written as -1 (all 0xFF)
        int bytesPerPointer = 8 - Long.numberOfLeadingZeros(maxDiff | 1L) / 8; // at least 1
        // last pointer is not implicit here
        int index = size - 1;
        for (int i = 0; i <= 255; ++i)
        {
            if (index >= 0 && i == (node.childTransition(index)))
                writeReversedSized(basePos - node.child(index--), bytesPerPointer);
            else
                writeReversedSized(-1L, bytesPerPointer);
        }
        assert index == -1;
        out.writeByte(BITS_DENSE | (bytesPerPointer - 1));
    }

    // reusable
    static class Node<T>
    {
        int depth;

        T descentPathContent;
        T ascentPathContent;
        private long[] children = new long[256];
        private byte[] childTransitions = new byte[256];
        private int childCount = 0;

        int childTransition(int index)
        {
            return childTransitions[index] & 0xFF;
        }

        long child(int index)
        {
            return children[index];
        }

        int childCount()
        {
            return childCount;
        }

        void reset()
        {
            childCount = 0;
            descentPathContent = null;
            ascentPathContent = null;
        }

        void addChild(int transition, long target)
        {
            children[childCount] = target;
            childTransitions[childCount] = (byte) transition;
            ++childCount;
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

    public static <T> File write(Trie<T> trie, boolean isOrdered, DataSerializer<T> serializer, File file)
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
