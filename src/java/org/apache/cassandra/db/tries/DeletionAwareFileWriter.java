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
///   bytes         : content, as written by the caller's serializer (may be empty)
/// ```
///
/// The content carries no length of its own: the node encoding already length-prefixes the
/// whole payload, so the reader derives the content length from the payload length it is
/// given minus the bytes consumed by the vint. Overhead over a plain trie is therefore one
/// byte per payload for any branch root below 128.
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
implements Cursor.Walker<T, DataOutputPlus>
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
            return vintSize(value.deletionBranchRoot + 1) + contentSize;
        }

        @Override
        public int serialize(DataOutputPlus out, Payload<T> value) throws IOException
        {
            // No length is written for the content: the node encoding already length-prefixes the
            // whole payload (see OnDiskWriteNodeType.PREFIX, which writes serialize()'s return value
            // as a reversed vint). The reader recovers the content length by subtracting the bytes
            // consumed by the branch vint from the payload length it is handed.
            out.writeUnsignedVInt(value.deletionBranchRoot + 1);
            if (value.content != null)
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
    /// Stream position when the current deletion branch was entered, to tell a branch that wrote
    /// nothing from one that did.
    private long branchStartPosition;


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
        inner.ascendTo(newEncodedPosition);
    }

    // ---- walker callbacks -------------------------------------------------------------

    @Override
    public void content(T content)
    {
        inner.content(new Payload<>(content, -1));
    }

    /// Not [DeletionAwareCursor.DeletionAwareWalker]: that contract reports a branch's markers but
    /// never its ascents, and [FileWriter] only emits nodes on ascent. The driver below walks both
    /// levels explicitly instead, so these are plain methods rather than interface overrides.
    public boolean enterDeletionsBranch()
    {
        // Start a nested trie in the same stream. Its bytes land before the node that will
        // reference them, so the recorded root is a backward pointer like every other.
        //
        // Always written ordered, independently of the data trie: a deletion branch is a range
        // trie, it is read back through OnDiskCursor.Range, and that always reads with
        // isOrdered = true. Writing it unordered loses the ascent-path stops and the read-back
        // positions disagree on ON_RETURN_PATH_BIT.
        branchWriter = new FileWriter<>(out, deletionSerializer, true);
        branchStartPosition = out.position();
        branchDepthAdjustment = inner.keyPos;
        return true;
    }

    public void deletionMarker(D marker)
    {
        branchWriter.content(marker);
    }

    /// @return the position of the branch just written, to be recorded in the node's payload, or
    ///         -1 if the branch turned out to have no content.
    public long exitDeletionsBranch()
    {
        branchWriter.complete();
        // A trie with no content leaves the stream untouched (see FileWriter.ascendTo), so the
        // position is that of whatever node was written last, or 0 if nothing has been written
        // yet. Report "no branch" rather than a root that is not a node: a recorded root makes
        // the reader set MAY_HAVE_DELETION_BRANCH_BIT and decode that byte as a node code.
        long position = out.position();
        long root = position > branchStartPosition ? position : -1;
        branchWriter = null;
        branchDepthAdjustment = 0;
        return root;
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
        return inner.complete();
    }

    /// Serialize a deletion-aware trie.
    ///
    /// [DeletionAwareCursor#process] cannot drive this: it never calls `ascendTo`, and [FileWriter]
    /// only emits nodes on ascent — which is why [FileWriter#write] has its own loop. The same
    /// applies to a deletion branch, so both levels are walked explicitly here.
    ///
    /// Writes in [Direction#REVERSE] like [FileWriter#write], so the root ends up last and the trie
    /// is read from the end of the stream.
    public static <T, D extends RangeState<D>> void write(DeletionAwareTrie<T, D> trie,
                                                          boolean isOrdered,
                                                          FileWriter.DataSerializer<T> contentSerializer,
                                                          FileWriter.DataSerializer<D> deletionSerializer,
                                                          DataOutputPlus out)
    {
        DeletionAwareFileWriter<T, D> fw =
            new DeletionAwareFileWriter<>(out, contentSerializer, deletionSerializer, isOrdered);
        DeletionAwareCursor<T, D> c = trie.cursor(Direction.REVERSE);

        emitAt(fw, c);
        long prevPosition = c.encodedPosition();
        while (true)
        {
            long currPosition = c.advanceMultiple(fw);
            if (Cursor.ascended(currPosition, prevPosition))
            {
                fw.ascendTo(currPosition);
                if (Cursor.isExhausted(currPosition))
                    break;

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

            emitAt(fw, c);
            prevPosition = currPosition;
        }
        fw.complete();
    }

    /// Report the content and, if present, the whole deletion branch at the cursor's position.
    private static <T, D extends RangeState<D>> void emitAt(DeletionAwareFileWriter<T, D> fw,
                                                            DeletionAwareCursor<T, D> c)
    {
        long position = c.encodedPosition();

        T content = Cursor.content(c, position);

        long branchRoot = -1;
        if ((position & Cursor.MAY_HAVE_DELETION_BRANCH_BIT) != 0)
        {
            RangeCursor<D> branch = c.deletionBranchCursor(Direction.REVERSE);
            if (branch != null)
            {
                fw.enterDeletionsBranch();
                writeBranch(fw.branchWriter, branch);
                branchRoot = fw.exitDeletionsBranch();
            }
        }

        // Emit once both halves are known. This must happen while the walk is still at this
        // position: FileWriter.content asserts on the path state, so it cannot be deferred past
        // the next ascent. Writing the branch first is still correct — content() only attaches
        // the payload to the in-progress node, whose bytes are emitted later on ascent, so the
        // recorded root remains a backward pointer.
        if (content != null || branchRoot >= 0)
            fw.inner.content(new Payload<>(content, branchRoot));
    }

    /// The same ascent-driven loop as above, over one deletion branch, feeding the nested writer.
    private static <D extends RangeState<D>> void writeBranch(FileWriter<D> bw, RangeCursor<D> c)
    {
        D content = c.content();
        if (content != null)
            bw.content(content);

        long prevPosition = c.encodedPosition();
        while (true)
        {
            long currPosition = c.advanceMultiple(bw);
            if (Cursor.ascended(currPosition, prevPosition))
            {
                bw.ascendTo(currPosition);
                if (Cursor.isExhausted(currPosition))
                    return;

                int depth = Cursor.depth(currPosition);
                if (depth > 0)
                {
                    bw.resetPathLength(depth - 1);
                    bw.addPathByte(Cursor.incomingTransition(currPosition));
                }
            }
            else
                bw.addPathByte(Cursor.incomingTransition(currPosition));

            if (Cursor.isOnReturnPath(currPosition))
                bw.onReturnPath();

            content = c.content();
            if (content != null)
                bw.content(content);
            prevPosition = currPosition;
        }
    }

}
