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

import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.rows.SerializationHelper;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.Rebufferer;

class ReverseIndexedReader extends ReverseReader
{
    private RowIndexReverseIterator indexReader;
    final TrieIndexEntry indexEntry;
    final FileHandle rowIndexFile;
    long basePosition;
    long currentBlockStart;
    long currentBlockEnd;
    RowIndexReader.IndexInfo currentIndexInfo;
    Rebufferer.ReaderConstraint rc;

    public ReverseIndexedReader(TrieIndexSSTableReader sstable,
                                TrieIndexEntry indexEntry,
                                Slices slices,
                                FileDataInput file,
                                boolean shouldCloseFile,
                                SerializationHelper helper,
                                Rebufferer.ReaderConstraint rc)
    {
        super(sstable, slices, file, shouldCloseFile, helper);
        basePosition = indexEntry.position;
        this.indexEntry = indexEntry;
        this.rc = rc;
        this.rowIndexFile = sstable.rowIndexFile;
    }

    @Override
    public void close() throws IOException
    {
        try
        {
            // This can't be null as setForSlice is always called.
            indexReader.close();
        }
        finally
        {
            super.close();
        }
    }

    @Override
    public boolean setForSlice(Slice slice) throws IOException
    {
        if (currentIndexInfo == null)
        {
            start = slice.start();
            end = slice.end();

            foundLessThan = false;
            ClusteringComparator comparator = metadata.comparator;
            if (indexReader != null)
            {
                indexReader.close();
                indexReader = null; // set to null so we don't double close if constructor below throws
            }
            indexReader = new RowIndexReverseIterator(rowIndexFile,
                                                      indexEntry,
                                                      comparator.asByteComparable(end),
                                                      rc);
            sliceOpenMarker = null;
            currentIndexInfo = indexReader.nextIndexInfo(); // this shouldn't throw (constructor calculated it)
            if (currentIndexInfo == null)
                return false; // no content
        }

        // this can throw; we save currentIndexInfo so we don't redo index seek if that happens
        return gotoIndexBlock();
    }

    boolean gotoIndexBlock() throws IOException
    {
        assert rowOffsets.isEmpty();

        openMarker = currentIndexInfo.openDeletion;
        currentBlockStart = basePosition + currentIndexInfo.offset;
        seekToPosition(currentBlockStart);
        currentIndexInfo = null;    // completed successfully
        return true;
    }

    @Override
    protected boolean advanceBlock() throws IOException
    {
        if (foundLessThan)
            return false;

        if (currentIndexInfo == null)
        {
            currentBlockEnd = currentBlockStart;
            currentIndexInfo = indexReader.nextIndexInfo();
            if (currentIndexInfo == null)
                return false; // no content
        }

        return gotoIndexBlock();
    }

    @Override
    protected boolean preBlockStep() throws IOException
    {
        return filePos >= currentBlockEnd || preStep(start);
    }

    @Override
    protected boolean blockPrepStep() throws IOException
    {
        return filePos >= currentBlockEnd || prepStep(ClusteringBound.TOP);
    }

    // make block pre/prep done as slice pre/prep
}
