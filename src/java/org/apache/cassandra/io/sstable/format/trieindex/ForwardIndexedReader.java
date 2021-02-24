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

import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.rows.SerializationHelper;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.Rebufferer;

class ForwardIndexedReader extends ForwardReader
{
    private final RowIndexReader indexReader;
    long basePosition;

    ForwardIndexedReader(TrieIndexSSTableReader sstable,
                         TrieIndexEntry indexEntry,
                         Slices slices,
                         FileDataInput file,
                         boolean shouldCloseFile,
                         SerializationHelper helper,
                         Rebufferer.ReaderConstraint rc)
    {
        super(sstable, slices, file, shouldCloseFile, helper);
        basePosition = indexEntry.position;
        indexReader = new RowIndexReader(sstable.rowIndexFile, indexEntry, rc);
    }

    @Override
    public void close() throws IOException
    {
        try
        {
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
        super.setForSlice(slice);

        RowIndexReader.IndexInfo indexInfo = indexReader.separatorFloor(metadata.comparator.asByteComparable(slice.start()));
        assert indexInfo != null;
        long position = basePosition + indexInfo.offset;
        if (filePos == -1 || filePos < position) // Don't go back if the index points to the block we are currently on
        {
            openMarker = indexInfo.openDeletion;
            seekToPosition(position);
        }
        return true;
        // Otherwise we are already in the relevant index block, there is no point to go back to its beginning.
    }
}
