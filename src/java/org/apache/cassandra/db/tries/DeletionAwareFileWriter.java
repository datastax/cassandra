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

import org.apache.cassandra.io.util.DataOutputPlus;

/// Serializes a [DeletionAwareTrie] using the [FileWriter] on-disk trie format.
///
/// ## Why the deletion branch lives in the payload
///
/// [OnDiskWriteNodeType] has no room for a deletion branch: the node code is
/// `nodeCode >> 3`, all 32 type codes are assigned (LEAF 0-7, CHAIN 8-15, SPARSE 16-27,
/// BITMAP 28, DENSE 29, PREFIX 30, RELAY 31), and PREFIX's three flag bits are all in
/// use (`HAS_CHILD`, `HAS_ASCENT_CONTENT`, `HAS_DESCENT_CONTENT`).
///
/// Rather than steal encoding space from a format that is also the basis of the on-disk
/// table format, the deletion branch is recorded in the node's **payload**, which is
/// pluggable per use case via [FileWriter.DataSerializer]. The node encoding is
/// untouched, so this cannot conflict with changes to the shared format, and the choice
/// is reversible: if node-level support is added later, only the codec below changes.
///
/// ## Payload layout
///
/// Every payload written by this class is:
///
/// ```
///   unsigned vint : deletionBranchRoot + 1   (0 when the node has no deletion branch)
///   unsigned vint : contentLength            (0 when the node has no live content)
///   bytes         : content, as written by the caller's serializer
/// ```
///
/// ## Ordering
///
/// [FileWriter] writes nodes bottom-up as the walk ascends, so every pointer it emits
/// points backwards. [DeletionAwareCursor#process] visits a node's content, then its
/// deletion branch, then descends into its children, and the node itself is written
/// after its children. Serializing the deletion branch when the walk leaves it therefore
/// places those bytes before both the children and the node that references them, and
/// the recorded root is a valid backward pointer.
///
/// A branch trie is complete in itself, so its root is the position reached once it has
/// been fully written — the same convention [OnDiskTrie#open] uses when it takes the
/// file length as the root.
/// Note this *holds* a [FileWriter] rather than extending it: the underlying writer is a
/// `Walker<Payload<T>>` while this is a `Walker<T>`, and Java does not permit inheriting the
/// same interface with two different type arguments.
public class DeletionAwareFileWriter<T, D extends RangeState<D>>
implements DeletionAwareCursor.DeletionAwareWalker<T, D, DataOutputPlus>
{
    /// A node's live content together with the position of its deletion branch, if any.
    static class Payload<T>
    {
        final T content;                 // null if the node only carries a deletion branch
        final long deletionBranchRoot;   // -1 if the node has no deletion branch

        Payload(T content, long deletionBranchRoot)
        {
            this.content = content;
            this.deletionBranchRoot = deletionBranchRoot;
        }
    }

    /// Wraps the caller's content serializer with the deletion-branch pointer.
    private static class PayloadSerializer<T> implements FileWriter.DataSerializer<Payload<T>>
    {
        final FileWriter.DataSerializer<T> contentSerializer;

        PayloadSerializer(FileWriter.DataSerializer<T> contentSerializer)
        {
            this.contentSerializer = contentSerializer;
        }

        @Override
        public int serializedSize(Payload<T> value)
        {
            int contentSize = value.content != null ? contentSerializer.serializedSize(value.content) : 0;
            return vintSize(value.deletionBranchRoot + 1) + vintSize(contentSize) + contentSize;
        }

        @Override
        public int serialize(DataOutputPlus out, Payload<T> value) throws IOException
        {
            int contentSize = value.content != null ? contentSerializer.serializedSize(value.content) : 0;
            out.writeUnsignedVInt(value.deletionBranchRoot + 1);
            out.writeUnsignedVInt(contentSize);
            if (contentSize > 0)
                contentSerializer.serialize(out, value.content);
            return serializedSize(value);
        }

        private static int vintSize(long value)
        {
            int size = 1;
            while ((value >>>= 7) != 0)
                ++size;
            return size;
        }
    }

    final DataOutputPlus out;
    final FileWriter.DataSerializer<D> deletionSerializer;
    final boolean swapAscentAndDescentSides;

    /// The writer for the data trie itself.
    final FileWriter<Payload<T>> inner;

    /// Non-null only while the walk is inside a deletion branch.
    private FileWriter<D> branchWriter;
    /// Depth of the node the current deletion branch hangs from, to rebase the nested walk.
    private int branchDepthAdjustment;

    /// Content seen at the current position, held until we know whether this node also has
    /// a deletion branch. [DeletionAwareCursor#process] reports content before the branch.
    private T pendingContent;
    private long pendingBranchRoot = -1;
    private boolean hasPending;

    public DeletionAwareFileWriter(DataOutputPlus out,
                                   FileWriter.DataSerializer<T> contentSerializer,
                                   FileWriter.DataSerializer<D> deletionSerializer,
                                   boolean swapAscentAndDescentSides)
    {
        this.out = out;
        this.deletionSerializer = deletionSerializer;
        this.swapAscentAndDescentSides = swapAscentAndDescentSides;
        this.inner = new FileWriter<>(out, new PayloadSerializer<>(contentSerializer), swapAscentAndDescentSides);
    }

    /// Write out every node the walk has finished with. Called by the driving loop on ascent,
    /// mirroring [FileWriter#ascendTo].
    public void ascendTo(long newEncodedPosition)
    {
        flushPending();
        inner.ascendTo(newEncodedPosition);
    }

    // ---- walker callbacks -------------------------------------------------------------

    @Override
    public void content(T content)
    {
        flushPending();
        pendingContent = content;
        hasPending = true;
    }

    @Override
    public boolean enterDeletionsBranch()
    {
        // Start a nested trie in the same stream. Its bytes land before the node that will
        // reference them, so the recorded root is a backward pointer like every other.
        branchWriter = new FileWriter<>(out, deletionSerializer, swapAscentAndDescentSides);
        branchDepthAdjustment = inner.keyPos;
        return true;
    }

    @Override
    public void deletionMarker(D marker)
    {
        branchWriter.content(marker);
    }

    @Override
    public void exitDeletionsBranch()
    {
        branchWriter.ascendTo(Cursor.encode(0, 0, Direction.FORWARD));
        branchWriter.complete();
        pendingBranchRoot = out.position();
        hasPending = true;
        branchWriter = null;
        branchDepthAdjustment = 0;
    }

    // ---- path tracking ----------------------------------------------------------------
    // While inside a deletion branch these describe the nested trie, not the outer one.

    @Override
    public void addPathByte(int nextByte)
    {
        if (branchWriter != null)
            branchWriter.addPathByte(nextByte);
        else
            inner.addPathByte(nextByte);
    }

    @Override
    public void addPathBytes(org.agrona.DirectBuffer buffer, int pos, int count)
    {
        if (branchWriter != null)
            branchWriter.addPathBytes(buffer, pos, count);
        else
            inner.addPathBytes(buffer, pos, count);
    }

    @Override
    public void resetPathLength(int newLength)
    {
        if (branchWriter != null)
            branchWriter.resetPathLength(newLength);
        else
            inner.resetPathLength(newLength);
    }

    @Override
    public void onReturnPath()
    {
        if (branchWriter != null)
            branchWriter.onReturnPath();
        else
            inner.onReturnPath();
    }

    @Override
    public DataOutputPlus complete()
    {
        flushPending();
        return inner.complete();
    }

    /// Hand the accumulated content and deletion-branch pointer to the underlying writer as
    /// one payload. Called once the walk moves on from the position they belong to.
    private void flushPending()
    {
        if (!hasPending)
            return;
        hasPending = false;
        T content = pendingContent;
        long branchRoot = pendingBranchRoot;
        pendingContent = null;
        pendingBranchRoot = -1;
        inner.content(new Payload<>(content, branchRoot));
    }
}
