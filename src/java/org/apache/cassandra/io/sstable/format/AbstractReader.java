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

package org.apache.cassandra.io.sstable.format;

import java.io.IOException;

import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.UnfilteredDeserializer;
import org.apache.cassandra.db.rows.RangeTombstoneBoundMarker;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.SerializationHelper;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.schema.TableMetadata;

public abstract class AbstractReader implements SSTableReader.PartitionReader
{
    protected final TableMetadata metadata;

    protected final Slices slices;

    private final boolean shouldCloseFile;

    public FileDataInput file;

    public UnfilteredDeserializer deserializer;

    // The start of the current slice. This will be null as soon as we know we've passed that bound.
    protected ClusteringBound start = ClusteringBound.BOTTOM;
    // The end of the current slice. Will never be null.
    protected ClusteringBound end = ClusteringBound.TOP;

    private Slice pendingSlice;

    // Records the currently open range tombstone (if any)
    public DeletionTime openMarker = null;

    protected long filePos = -1;

    Stage stage = Stage.NEEDS_SLICE;

    /** +1 for forward iteration, -1 for reverse */
    private final int direction;

    private int currentSlice;

    protected AbstractReader(SSTableReader sstable,
                             Slices slices,
                             FileDataInput file,
                             boolean shouldCloseFile,
                             SerializationHelper helper,
                             boolean reversed)
    {
        assert file != null;
        this.metadata = sstable.metadata();
        this.slices = slices;
        this.file = file;
        this.shouldCloseFile = shouldCloseFile;
        this.direction = reversed ? -1 : +1;
        this.deserializer = UnfilteredDeserializer.create(metadata, file, sstable.header, helper);
        this.currentSlice = reversed ? slices.size() : -1;
    }

    public Unfiltered next()
    throws IOException
    {
        Unfiltered next;

        while (true)
        {
            switch (stage)
            {
            case NEEDS_SLICE:
                currentSlice += direction;
                if (currentSlice < 0 || currentSlice >= slices.size())
                    return null;
                assert pendingSlice == null;
                pendingSlice = slices.get(currentSlice);
                stage = Stage.NEEDS_SET_FOR_SLICE;
                // fall through
            case NEEDS_SET_FOR_SLICE:
                // The call below is expected to reset the file pointer, or do nothing. In either case, if it throws
                // we shouldn't seek on retry.
                filePos = -1;
                if (!setForSlice(pendingSlice))
                {
                    pendingSlice = null;
                    stage = Stage.NEEDS_SLICE;
                    continue;
                }

                pendingSlice = null;
                stage = Stage.NEEDS_PRE_SLICE;
                // fall through
            case NEEDS_PRE_SLICE:
                do
                {
                    filePos = file.getSeekPosition();
                }
                while (!preSliceStep());
                // Note: If this succeeded, the deserializer is prepared and the file pointer is in the middle of a row
                // which passes the in-slice test. We should not save the current file position; if retry is needed for
                // next item, it will correctly start at the beginning of the row for which we applied preSliceStep last.
                stage = Stage.NEEDS_SLICE_PREP;
                // fall through

            case NEEDS_SLICE_PREP:
                while (!slicePrepStep())
                {
                    filePos = file.getSeekPosition();
                }

                stage = Stage.READY;
                next = sliceStartMarker();
                if (next != null)
                    return next;
                // fall through
            case READY:
                next = nextInSlice();
                filePos = file.getSeekPosition();
                if (next != null)
                    return next;
                stage = Stage.NEEDS_BLOCK;
                // fall through
            case NEEDS_BLOCK:
                if (!advanceBlock())
                {
                    stage = Stage.NEEDS_SLICE;
                    next = sliceEndMarker();
                    if (next != null)
                        return next;
                    continue;
                }
                stage = Stage.NEEDS_PRE_BLOCK;
                // fall through
            case NEEDS_PRE_BLOCK:
                do
                {
                    filePos = file.getSeekPosition();
                }
                while (!preBlockStep());

                stage = Stage.NEEDS_BLOCK_PREP;
                // fall through
            case NEEDS_BLOCK_PREP:
                while (!blockPrepStep())
                {
                    filePos = file.getSeekPosition();
                }

                stage = stage.READY;
            }
        }
    }

    /**
     * Resets the state to the last known finished item
     * This is needed to handle async retries - due to missing data in the chunk cache.
     * Classes that override this must call the base class too.
     */
    public void resetReaderState() throws IOException
    {
        if (filePos != -1)
            seekToPosition(filePos);
    }

    public void seekToPosition(long position) throws IOException
    {
        file.seek(position);
        deserializer.clearState();
    }

    protected DeletionTime updateOpenMarker(RangeTombstoneMarker marker)
    {
        // Note that we always read index blocks in forward order so this method is always called in forward order
        return openMarker = marker.isOpen(false) ? marker.openDeletionTime(false) : null;
    }

    public static RangeTombstoneBoundMarker markerFrom(ClusteringBound where, DeletionTime deletion)
    {
        if (deletion == null)
            return null;
        assert where != null;
        return new RangeTombstoneBoundMarker(where, deletion);
    }

    public void close() throws IOException
    {
        if (shouldCloseFile && file != null)
            file.close();
    }

    protected boolean skipSmallerRow(ClusteringBound bound) throws IOException
    {
        assert bound != null;
        // Note that the following comparison is strict. The reason is that the only cases
        // where it can be == is if the "next" is a RT start marker (either a '[' of a ')[' boundary),
        // and if we had a non-strict inequality and an open RT marker before this, we would issue
        // the open marker first, and then return then next later, which would send in the
        // stream both '[' (or '(') and then ')[' for the same clustering value, which is wrong.
        // By using a strict inequality, we avoid that problem (if we do get ')[' for the same
        // clustering value than the slice, we'll simply record it in 'openMarker').
        if (!deserializer.hasNext() || deserializer.compareNextTo(bound) > 0)
            return true;

        if (deserializer.nextIsRow())
            deserializer.skipNext();
        else
            updateOpenMarker((RangeTombstoneMarker)deserializer.readNext());
        return false;
    }


    // Compute the next element to return, assuming we're in the middle to the slice
    // and the next element is either in the slice, or just after it. Returns null
    // if we're done with the slice.
    protected Unfiltered readUnfiltered() throws IOException
    {
        assert end != null;
        while (true)
        {
            if (!deserializer.hasNext())
                return null;
            // We use the same reasoning as in handlePreSliceData regarding the strictness of the inequality below.
            // We want to exclude deserialized unfiltered equal to end, because 1) we won't miss any rows since those
            // wouldn't be equal to a slice bound and 2) an end bound can be equal to a start bound
            // (EXCL_END(x) == INCL_START(x) for instance) and in that case we don't want to return start bound because
            // it's fundamentally excluded. And if the bound is an end (for a range tombstone), it means it's exactly
            // our slice end, but in that case we will properly close the range tombstone anyway as part of our "close
            // an open marker" code
            if (deserializer.compareNextTo(end) >= 0)
            {   // make sure we are not in the middle of an unfiltered when returning null because leaving
                // the file pointer in the middle is not allowed as it would break a retry
                deserializer.rewind();
                return null;
            }

            Unfiltered next = deserializer.readNext();
            // We may get empty row for the same reason expressed on UnfilteredSerializer.deserializeOne.
            // This should be rare (i.e. does not warrant saving state).
            if (next.isEmpty())
                continue;

            if (next.kind() == Unfiltered.Kind.RANGE_TOMBSTONE_MARKER)
                updateOpenMarker((RangeTombstoneMarker) next);
            return next;
        }
    }

    /**
     * Stages in the reading progression.
     * Because we can get an interruption (NotInCacheException) during each of the stages, we need to save the kind of
     * processing we were doing before that happened to continue without redoing too much of the work.
     */
    enum Stage
    {
        /** A slice is needed before we can start any work. Separate stage to avoid double increase if setForSlice throws. */
        NEEDS_SLICE,
        /** We have received a slice and now we need to read the index to find where to start filtering content. */
        NEEDS_SET_FOR_SLICE,
        /** We have found the section of the file that should contain the data, but we still need to filter non-matching items. */
        NEEDS_PRE_SLICE,
        /** Positioned at start of slice, do any slice preparation (reverse readers). */
        NEEDS_SLICE_PREP,
        /** No further pre-processing is needed before we can issue rows. */
        READY,
        /** nextInSlice completed, will try getting new block (reverse indexed readers). */
        NEEDS_BLOCK,
        /** got new block, filter non-matching (reverse indexed readers). */
        NEEDS_PRE_BLOCK,
        /** got new block and filtered, prepare block (reverse indexed readers). */
        NEEDS_BLOCK_PREP
    }

    public boolean setForSlice(Slice slice) throws IOException
    {
        start = slice.start();
        end = slice.end();
        return true;
    }

    // Skip all data that comes before the currently set slice.
    // Return true if complete, false if another preSliceStep is needed.
    protected boolean preSliceStep() throws IOException
    {
        return skipSmallerRow(start);
    }

    /**
     * Do a slice preparation step. Return true if done.
     */
    protected boolean slicePrepStep() throws IOException
    {
        return true;
    }

    /**
     * Returns the start marker that should be issued as the beginning of the slice, or null if none. This marker is
     * usually ready immediately after the PRE_SLICE stage completes.
     * This call is not allowed to read any further data / throw a NotInCacheException.
     */
    protected abstract RangeTombstoneMarker sliceStartMarker();

    /** Return the next Unfiltered in the current slice, or null if completed. */
    protected abstract Unfiltered nextInSlice() throws IOException;

    /**
     * Returns the end marker that should be issued as the ending of the slice, or null if none. This marker is
     * the closing of anything that it active at the point where the slice ends and is known when the READY stage completes.
     * This call is not allowed to read any further data / throw a NotInCacheException.
     */
    protected abstract RangeTombstoneMarker sliceEndMarker();

    /**
     * Try to advance to the next block in the same slice. Used by reversed readers where slice can conver multiple
     * blocks. Return false if there are no further blocks in the slice.
     */
    protected boolean advanceBlock() throws IOException
    {
        return false;
    }

    /**
     * Do a pre-slice advance for new block. Return true if done.
     */
    protected boolean preBlockStep() throws IOException
    {
        throw new IllegalStateException("Should be overridden if advanceBlock is.");
    }

    /**
     * Do a block preparation step. Return true if done.
     */
    protected boolean blockPrepStep() throws IOException
    {
        throw new IllegalStateException("Should be overridden if advanceBlock is.");
    }

    @Override
    public String toString()
    {
        long filePos = this.filePos;
        return String.format("SSTable reader class: %s, position: %d, direction: %d, slice: %d, stage: %s",
                this.getClass().getName(), filePos == -1 ? 0 : filePos, direction, currentSlice, stage);
    }
}
