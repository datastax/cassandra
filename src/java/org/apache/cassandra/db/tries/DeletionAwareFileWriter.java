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
/// ## Where the deletion branch pointer lives
///
/// [OnDiskWriteNodeType] has no room for a deletion branch: the node code is
/// `nodeCode >> 3`, all 32 type codes are assigned (LEAF 0-7, CHAIN 8-15, SPARSE 16-27,
/// BITMAP 28, DENSE 29, PREFIX 30, RELAY 31), and PREFIX's three flag bits are all in
/// use (`HAS_CHILD`, `HAS_ASCENT_CONTENT`, `HAS_DESCENT_CONTENT`).
///
/// A node does, however, have a spare payload slot. A generic-content node carries content on both the
/// descent and the ascent (return) path, and the data trie of a deletion-aware trie only ever presents
/// content on descent (see the `presentContentOnDescentPath` argument
/// [InMemoryDeletionAwareTrie] passes to its base) — so nothing ever fills its ascent-side slot. The
/// deletion branch's root position goes there, as a minimal-width reversed integer whose length the
/// node's own length prefix carries. This is the same reuse the in-memory trie makes of a prefix node's
/// secondary pointer, which holds return-path content in a range trie and the alternate branch in a
/// deletion-aware one (see `PREFIX_ALTERNATE_OFFSET` and the prefix node section of `InMemoryTrie.md`).
///
/// A payload with no deletion branch is therefore byte-for-byte what a plain trie would write for the
/// same content, and the node code's descent- and ascent-content bits are exactly
/// [Cursor#MAY_HAVE_CONTENT_BIT] and [Cursor#MAY_HAVE_DELETION_BRANCH_BIT], so the reader produces both
/// flags without decoding anything.
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
/// `Walker<Slot<T>>` while this is a `Walker<T>`, and Java does not permit inheriting the
/// same interface with two different type arguments.
public class DeletionAwareFileWriter<T, D extends RangeState<D>>
implements Cursor.Walker<T, DataOutputPlus>
{
    /// The contents of one of a node's two payload slots: live content on the descent side, or the
    /// position of the node's deletion branch on the ascent side. Exactly one of the two is set.
    static class Slot<T>
    {
        final T content;
        final long deletionBranchRoot;   // -1 unless this is an ascent-side slot

        private Slot(T content, long deletionBranchRoot)
        {
            this.content = content;
            this.deletionBranchRoot = deletionBranchRoot;
        }

        static <T> Slot<T> content(T content)
        {
            return new Slot<>(content, -1);
        }

        static <T> Slot<T> branch(long deletionBranchRoot)
        {
            return new Slot<>(null, deletionBranchRoot);
        }
    }

    /// Writes whichever of the two kinds of slot it is handed: the caller's content, or the branch pointer as a
    /// minimal-width reversed integer, whose length the node's own length prefix records.
    private static class SlotSerializer<T> implements FileWriter.DataSerializer<Slot<T>>
    {
        final FileWriter.DataSerializer<T> contentSerializer;

        SlotSerializer(FileWriter.DataSerializer<T> contentSerializer)
        {
            this.contentSerializer = contentSerializer;
        }

        @Override
        public int serializedSize(Slot<T> value)
        {
            return value.content != null ? contentSerializer.serializedSize(value.content)
                                         : FileWriter.bytesFor(value.deletionBranchRoot);
        }

        @Override
        public int serialize(DataOutputPlus out, Slot<T> value) throws IOException
        {
            if (value.content != null)
                return contentSerializer.serialize(out, value.content);

            int bytes = FileWriter.bytesFor(value.deletionBranchRoot);
            FileWriter.writeReversedSized(out, value.deletionBranchRoot, bytes);
            return bytes;
        }
    }

    final DataOutputPlus out;
    final FileWriter.DataSerializer<D> deletionSerializer;

    /// The writer for the data trie itself.
    final FileWriter<Slot<T>> inner;

    /// Non-null only while the walk is inside a deletion branch.
    private FileWriter<D> branchWriter;
    /// Depth of the node the current deletion branch hangs from, to rebase the nested walk.
    private int branchDepthAdjustment;
    /// Stream position when the current deletion branch was entered, to tell a branch that wrote
    /// nothing from one that did.
    private long branchStartPosition;


    public DeletionAwareFileWriter(DataOutputPlus out,
                                   FileWriter.DataSerializer<T> contentSerializer,
                                   FileWriter.DataSerializer<D> deletionSerializer)
    {
        this.out = out;
        this.deletionSerializer = deletionSerializer;
        // Never ordered: OnDiskDeletionAwareTrie always reads the data trie with isOrdered = false, and an ordered
        // write would swap the two content slots in FileWriter.InProgressNode.complete, putting the branch pointer
        // where the reader expects the live content.
        this.inner = new FileWriter<>(out, new SlotSerializer<>(contentSerializer), false);
    }

    /// Write out every node the walk has finished with. Called by the driving loop on ascent,
    /// mirroring [FileWriter#ascendTo].
    public void ascendTo(long newEncodedPosition) throws IOException
    {
        inner.ascendTo(newEncodedPosition);
    }

    // ---- walker callbacks -------------------------------------------------------------

    @Override
    public void content(T content)
    {
        inner.content(Slot.content(content));
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

    /// @return the position of the branch just written, to be recorded in the node's ascent-side slot, or
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
                                                          FileWriter.DataSerializer<T> contentSerializer,
                                                          FileWriter.DataSerializer<D> deletionSerializer,
                                                          DataOutputPlus out) throws IOException
    {
        DeletionAwareFileWriter<T, D> fw =
            new DeletionAwareFileWriter<>(out, contentSerializer, deletionSerializer);
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
                                                            DeletionAwareCursor<T, D> c) throws IOException
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
        // position: the calls below attach to the node the walk is on, so they cannot be deferred
        // past the next ascent. Writing the branch first is still correct — these only attach the
        // slots to the in-progress node, whose bytes are emitted later on ascent, so the recorded
        // root remains a backward pointer.
        if (content != null)
            fw.inner.content(Slot.content(content));
        if (branchRoot >= 0)
            fw.inner.ascentContent(Slot.branch(branchRoot));
    }

    /// The same ascent-driven loop as above, over one deletion branch, feeding the nested writer.
    private static <D extends RangeState<D>> void writeBranch(FileWriter<D> bw, RangeCursor<D> c) throws IOException
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
