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

package org.apache.cassandra.io.sstable.format.trieindex;

import java.io.IOException;

import com.carrotsearch.hppc.LongStack;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.SerializationHelper;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.FileDataInput;

/**
 * Reverse iteration is performed by going through an index block (or the whole partition if not indexed) forwards
 * and storing the positions of each entry that falls within the slice in a stack. Reverse iteration then pops out
 * positions and reads the entries.
 *
 * Note: The earlier version of this was constructing an in-memory view of the block instead, which gives better
 * performance on bigger queries and index blocks (due to not having to read disk again). With the lower
 * granularity of the tries it makes better sense to store as little as possible as the beginning of the block
 * should very rarely be in other page/chunk cache locations. This has the benefit of being able to answer small
 * queries (esp. LIMIT 1) faster and with less GC churn.
 */
class ReverseReader extends AbstractReader
{
    LongStack rowOffsets = new LongStack();
    DeletionTime sliceOpenMarker, sliceCloseMarker;
    boolean foundLessThan;
    long startPos = -1;

    ReverseReader(SSTableReader sstable,
                  Slices slices,
                  FileDataInput file,
                  boolean shouldCloseFile,
                  SerializationHelper helper)
    {
        super(sstable, slices, file, shouldCloseFile, helper, true);
    }

    // Prepare for the given slice. This does not throw, and isn't restartable
    public boolean setForSlice(Slice slice) throws IOException
    {
        // read full row and filter
        if (startPos == -1)
            startPos = file.getSeekPosition();
        else
            seekToPosition(startPos);

        assert rowOffsets.isEmpty();

        return super.setForSlice(slice);
    }

    protected RangeTombstoneMarker sliceStartMarker()
    {
        return markerFrom(end, sliceCloseMarker);
    }

    protected Unfiltered nextInSlice() throws IOException
    {
        Unfiltered toReturn;

        while (!rowOffsets.isEmpty())
        {
            seekToPosition(rowOffsets.peek());
            boolean hasNext = deserializer.hasNext();
            assert hasNext;
            toReturn = deserializer.readNext();
            rowOffsets.pop();
            // We may get empty row for the same reason expressed on UnfilteredSerializer.deserializeOne.
            if (!toReturn.isEmpty())
                return toReturn;
        }
        return null;
    }

    protected RangeTombstoneMarker sliceEndMarker()
    {
        return markerFrom(start, sliceOpenMarker);
    }

    protected boolean preSliceStep() throws IOException
    {
        return preStep(start);
    }

    protected boolean slicePrepStep() throws IOException
    {
        return prepStep(end);
    }

    boolean preStep(ClusteringBound start) throws IOException
    {
        assert filePos == file.getSeekPosition();
        if (skipSmallerRow(start))
        {
            sliceOpenMarker = openMarker;
            return true;
        }
        else
        {
            foundLessThan = true;
            return false;
        }
    }

    boolean prepStep(ClusteringBound end) throws IOException
    {
        // filePos could be in the middle of a row when prepSteps finish
        if (skipSmallerRow(end))
        {
            sliceCloseMarker = openMarker;
            return true;
        }
        else
        {
            rowOffsets.push(filePos);
            return false;
        }
    }
}
