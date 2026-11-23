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

import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.vint.VIntCoding;

/// Written bottom-to-top, i.e. children first, with negative deltas for every pointer. It also walks the trie in
/// reverse order to write the first child closest to the parent. See [OnDiskTrie.md](./OnDiskTrie.md) for details.
///
/// This is what the on-disk trie writers have in common, the walking half of the construction: the path, the nodes on
/// it with the content seen so far, and [#step], one step of the walk. When the walk leaves a node it is handed over,
/// together with the chain of transitions that leads to it, to an [OnDiskNodeWriter], which decides when and where
/// the node is written. Which node writer is plugged in determines the layout of the file:
/// [OnDiskNodeWriter.Type#IMMEDIATE] writes each node at once, [OnDiskNodeWriter.Type#PAGE_AWARE] packs them into the
/// pages of the output.
///
/// [OnDiskTrieWriter] writes plain and range tries, and [OnDiskDeletionAwareTrieWriter] deletion-aware ones; each
/// drives its own walk, and decides which of a node's two content slots each item it is given goes in.
abstract class OnDiskBaseTrieWriter<T> extends TriePathReconstructor
{
    final OnDiskNodeWriter<T> nodeWriter;
    /// What the nodes' content slots hold; see [OnDiskCursor.ContentStyle].
    final OnDiskCursor.ContentStyle contentStyle;
    /// True for the ordered styles. The walk is in reverse, so what they present before a branch in forward walks
    /// arrives on the return path and goes in the descent-side slot, and the other way round.
    final boolean swapAscentAndDescentSides;

    OnDiskNodeWriter.Node<T>[] nodesOnPath = new OnDiskNodeWriter.Node[32];
    int lastNodeOnPath = -1;

    /// True if the walk has completed and the trie was not empty. An empty trie writes nothing and has no root.
    boolean hasRoot = false;
    /// The position of the root of the trie whose walk has completed, 0 if there is none or it is not known yet. The
    /// latter is the case for a trie written through a nested node writer, which is placed by the node writer of the
    /// trie it is attached to, possibly some time after its walk completes.
    long rootPosition = 0;

    OnDiskBaseTrieWriter(OnDiskNodeWriter<T> nodeWriter, OnDiskCursor.ContentStyle contentStyle)
    {
        this.nodeWriter = nodeWriter;
        this.contentStyle = contentStyle;
        this.swapAscentAndDescentSides = contentStyle == OnDiskCursor.ContentStyle.ORDERED
                                         || contentStyle == OnDiskCursor.ContentStyle.RANGE;
    }

    // On the way down we only collect the path, in TriePathReconstructor. A node is created when content or an
    // alternate branch is found, or when a child completes, so a path that leads to nothing leaves nothing behind.

    /// Prepare for the walk of a trie. A writer can write several tries, one after the other.
    void start()
    {
        // A finished walk has given every node on its path to the node writer, but leaves the path's length as it
        // ended.
        assert lastNodeOnPath == -1 : "Previous walk did not finish";
        resetPathLength(0);
        hasRoot = false;
        rootPosition = 0;
        nodeWriter.reset();
    }

    /// Attach a payload presented on the descent path to the node at the current position. It goes in the node's
    /// descent-side content slot, unless this writer swaps the sides.
    void descentContent(T content)
    {
        OnDiskNodeWriter.Node<T> node = nodeAtCurrentPosition();
        if (swapAscentAndDescentSides)
        {
            assert node.ascentContent == null;
            node.ascentContent = content;
        }
        else
        {
            assert node.descentContent == null;
            node.descentContent = content;
        }
    }

    /// Attach a payload presented on the return path to the node at the current position. It goes in the node's
    /// ascent-side content slot, unless this writer swaps the sides.
    void ascentContent(T content)
    {
        OnDiskNodeWriter.Node<T> node = nodeAtCurrentPosition();
        if (swapAscentAndDescentSides)
        {
            assert node.descentContent == null;
            node.descentContent = content;
        }
        else
        {
            assert node.ascentContent == null;
            node.ascentContent = content;
        }
    }

    /// Make the trie that `branchWriter` has just completed the alternate branch of the node at the current position.
    /// `branchWriter` must be writing through a node writer [nested][OnDiskNodeWriter#nested] in this writer's, and
    /// the trie it wrote must not be empty (see [#hasRoot]).
    ///
    /// The pointer to the branch takes the ascent-side content slot of the node: the slot a range trie fills with
    /// return-path content is free in a trie whose content is descent-side only, which is the same reuse
    /// [InMemoryTrie] makes of a prefix node's alternate branch pointer. [OnDiskDeletionAwareTrieWriter] uses this for
    /// the deletion branches.
    ///
    /// The node must thus not have, or later be given, content for its ascent-side slot. The branch must be walked
    /// and attached when the walk first reaches the node, before any of the node's children is completed (see
    /// [OnDiskNodeWriter#attachBranch]).
    void alternateBranch(OnDiskBaseTrieWriter<?> branchWriter)
    {
        assert branchWriter.hasRoot;
        OnDiskNodeWriter.Node<T> node = nodeAtCurrentPosition();
        assert node.ascentContent == null;
        nodeWriter.attachBranch(node, branchWriter.nodeWriter);
    }

    private OnDiskNodeWriter.Node<T> nodeAtCurrentPosition()
    {
        if (lastNodeOnPath >= 0)
        {
            OnDiskNodeWriter.Node<T> lastNode = nodesOnPath[lastNodeOnPath];
            if (lastNode.depth == keyPos)
                return lastNode;
            assert lastNode.depth < keyPos;
        }
        return addNewNode(keyPos);
    }

    private OnDiskNodeWriter.Node<T> addNewNode(int depth)
    {
        if (++lastNodeOnPath >= nodesOnPath.length)
            nodesOnPath = Arrays.copyOf(nodesOnPath, lastNodeOnPath * 2);

        OnDiskNodeWriter.Node<T> node = nodesOnPath[lastNodeOnPath];
        if (node == null)
            nodesOnPath[lastNodeOnPath] = node = nodeWriter.newNode();

        node.depth = depth;
        return node;
    }

    /// Complete every node the walk has finished with, i.e. every node on the path deeper than the position the
    /// cursor ascended to, deepest first. Each node is given to the node writer together with the chain of transitions
    /// between it and the next shallower node on the path (or the ascent depth, whichever is deeper), and is then
    /// attached as a child of that parent, which is created if the path had no node at that depth.
    private void ascendTo(long newEncodedPosition) throws IOException
    {
        if (lastNodeOnPath < 0)
        {
            // fully empty trie; nothing is written and there is no root
            return;
        }

        // throw away any path that did not result in content
        OnDiskNodeWriter.Node<T> node = nodesOnPath[lastNodeOnPath];
        assert node.depth <= keyPos;
        keyPos = node.depth;

        int newLength = Math.max(Cursor.depth(newEncodedPosition) - 1, -1);
        // If we are returning to an ascent-path position that doesn't advance, prepare to add content to the node at
        // this depth rather than add a new child to parent.
        if (Cursor.isOnReturnPath(newEncodedPosition)
            && (newLength == -1 ||
                (keyBytes[newLength] & 0xFF) == Cursor.incomingTransition(newEncodedPosition)))
            ++newLength;

        while (newLength < node.depth)
        {
            --lastNodeOnPath;
            OnDiskNodeWriter.Node<T> next = lastNodeOnPath >= 0 ? nodesOnPath[lastNodeOnPath] : null;
            // The path to reach this node includes the key bytes until there's a node on path or we reach ascent
            // depth - 1: the transition out of that parent, followed by a chain of single-child transitions.
            int stopDepth = (next != null ? Math.max(next.depth, newLength) : newLength);
            nodeWriter.complete(node, keyBytes, stopDepth + 1, keyPos);
            keyPos = stopDepth;

            // if reached exhausted, the node just completed is the root
            if (keyPos < 0)
            {
                rootPosition = nodeWriter.finish();
                hasRoot = true;
                return;
            }

            // if reached ascend depth, add node on path. Note that this reuses the node that was just completed.
            if (next == null || next.depth < keyPos)
                next = addNewNode(keyPos);

            // add child with last byte and pointer to child
            nodeWriter.attachCompleted(next, keyBytes[keyPos] & 0xFF);

            node = next;
        }
    }

    /// Advance the cursor from `prevPosition` with this as the receiver of the transitions it takes, complete the
    /// nodes it ascended out of, and bring the path up to the new position. Returns the new position.
    ///
    /// [Cursor#process] cannot drive a writer: it never reports ascents, and nodes are only completed on ascent.
    long step(Cursor<?> c, long prevPosition) throws IOException
    {
        long currPosition = c.advanceMultiple(this);

        if (Cursor.ascended(currPosition, prevPosition))
        {
            // hand over the nodes that have been completed
            ascendTo(currPosition);

            if (Cursor.isExhausted(currPosition))
                return currPosition;

            // update key tracker
            int depth = Cursor.depth(currPosition);
            if (depth > 0)
            {
                resetPathLength(depth - 1);
                addPathByte(Cursor.incomingTransition(currPosition));
            }
            else
                resetPathLength(0);
        }
        else
            addPathByte(Cursor.incomingTransition(currPosition));

        return currPosition;
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
            else
            {
                // The leading byte goes last, like the one writeReversedSized puts in the top byte
                // of the shorter encodings: the reader takes the byte immediately before the value
                // as the leading one.
                out.writeLong(Long.reverseBytes(value));
                out.write((byte) 0xFF);
            }
        }
    }

    static int bytesFor(long delta)
    {
        return 8 - Integer.divideUnsigned(Long.numberOfLeadingZeros(delta | 1L) , 8); // at least 1
    }
}
