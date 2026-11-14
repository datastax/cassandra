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

/// Writes every node as soon as the walk leaves it: the node itself, then the chain of transitions that leads to it.
///
/// Nothing is kept but the positions of the children of the nodes on the trie writer's path, and nothing is allocated per
/// node. Because a node is always written directly after the last of its children, the first child of a node
/// immediately precedes it and the furthest is the one completed first, so this needs neither a relay nor a search
/// for the furthest child. The pages of the output are ignored.
///
/// A nested trie is written like any other, at the time it is walked: its root is known when its walk completes, and
/// attaching it to a node only records that position. Because the node must be written directly after the last of its
/// children, the nested trie has to be walked before the children are.
///
/// Unlike [OnDiskNodeWriterPageAware], this does not fold the transition to a node's only child into the chain that
/// starts the child (see [OnDiskNodeWriterPageAware#complete]): by the time a node is found to have no other
/// children, the chain of its child is already in the output, complete with the node code that carries its length.
/// This could be done by leaving the code of a node's leading chain for the parent to write, but the added
/// complexity is not worth the byte it saves.
public final class OnDiskNodeWriterImmediate<T> extends OnDiskNodeWriter<T>
{
    /// The root of the trie last written, to be given to the node the trie is attached to if this is a nested writer.
    /// 0 if there is none.
    private long lastRoot = 0;

    public OnDiskNodeWriterImmediate(DataOutputPlus out, OnDiskTrieWriter.DataSerializer<T> serializer)
    {
        super(out, serializer);
    }

    /// A node on the current path, with the children that have been completed and written so far. One instance per
    /// level is reused for the whole write.
    static final class PathNode<T> extends Node<T>
    {
        private static final long[] NO_POSITIONS = new long[0];
        private static final byte[] NO_TRANSITIONS = new byte[0];

        /// Positions (the end of each child's serialization) and transition bytes of the children written so far, in
        /// the order they were written. Grown on demand: a node can have up to 256 children, but the vast majority
        /// have very few.
        long[] childPositions = NO_POSITIONS;
        byte[] childTransitions = NO_TRANSITIONS;
        int childCount = 0;
        /// The position of the root of the node's alternate branch, 0 if it has none.
        long alternateBranch = 0;

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
            descentContent = null;
            ascentContent = null;
            alternateBranch = 0;
        }

        @Override
        public String toString()
        {
            String res = "";
            if (descentContent != null)
                res += "D[" + descentContent + "] ";
            if (ascentContent != null)
                res += "A[" + ascentContent + "] ";
            if (alternateBranch > 0)
                res += String.format("alt: %x ", alternateBranch);

            for (int i = 0; i < childCount; ++i)
                res += String.format("%02x: %x ", childTransitions[i] & 0xFF, childPositions[i]);

            return res;
        }
    }

    @Override
    Node<T> newNode()
    {
        return new PathNode<>();
    }

    @Override
    void complete(Node<T> pathNode, byte[] path, int chainStart, int chainEnd) throws IOException
    {
        PathNode<T> node = (PathNode<T>) pathNode;
        boolean hasChildren = node.childCount > 0;
        boolean hasPayload = node.descentContent != null || node.ascentContent != null || node.alternateBranch > 0;
        assert hasChildren || hasPayload;

        if (hasChildren)
            writeChildren(node);

        if (hasPayload)
            OnDiskWriteNodeType.writePayload(out, serializer, node.descentContent, node.ascentContent, node.alternateBranch, hasChildren);

        node.reset();

        OnDiskWriteNodeType.writeChain(out, path, chainStart, chainEnd);
    }

    private void writeChildren(PathNode<T> node) throws IOException
    {
        int count = node.childCount;
        long basePos = out.position();
        // Children are recorded in the order they were written, so the first is the furthest away and the last is
        // immediately before this node (delta 0, which the sparse, bitmap and chain types leave implicit).
        if (node.childPositions[count - 1] != basePos)
            throw new IllegalStateException("Last written child at " + node.childPositions[count - 1] +
                                            " does not immediately precede its parent at " + basePos);
        long furthestDelta = basePos - node.childPositions[0];
        assert furthestDelta >= 0;
        OnDiskWriteNodeType type = OnDiskWriteNodeType.selectChildrenType(furthestDelta, count);
        type.writeChildren(out, node.childPositions, node.childTransitions, count, basePos);
    }

    @Override
    void attachCompleted(Node<T> parent, int transition)
    {
        // The node just completed, together with the chain that leads to it, is the last thing written.
        ((PathNode<T>) parent).addChild(transition, out.position());
    }

    @Override
    long finish()
    {
        return lastRoot = out.position();
    }

    @Override
    <B> OnDiskNodeWriter<B> nested(OnDiskTrieWriter.DataSerializer<B> serializer)
    {
        return new OnDiskNodeWriterImmediate<>(out, serializer);
    }

    @Override
    void attachBranch(Node<T> node, OnDiskNodeWriter<?> nested)
    {
        // The nested trie is already written; all that remains is to note where.
        OnDiskNodeWriterImmediate<?> nestedWriter = (OnDiskNodeWriterImmediate<?>) nested;
        long root = nestedWriter.lastRoot;
        assert root > 0 : "No trie to attach";
        nestedWriter.lastRoot = 0;

        PathNode<T> pathNode = (PathNode<T>) node;
        assert pathNode.alternateBranch == 0;
        assert pathNode.childCount == 0 : "Alternate branch written after the children of its node";
        pathNode.alternateBranch = root;
    }

    @Override
    void reset()
    {
        lastRoot = 0;
    }
}
