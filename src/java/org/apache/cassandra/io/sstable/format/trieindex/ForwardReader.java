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

import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.SerializationHelper;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.FileDataInput;

class ForwardReader extends AbstractReader
{

    ForwardReader(SSTableReader sstable,
                  Slices slices,
                  FileDataInput file,
                  boolean shouldCloseFile,
                  SerializationHelper helper)
    {
        super(sstable, slices, file, shouldCloseFile, helper, false);
    }

    protected RangeTombstoneMarker sliceStartMarker()
    {
        // We've reached the beginning of our queried slice. If we have an open marker
        // we should return that first.
        return markerFrom(start, openMarker);
    }

    // Compute the next element to return, assuming we're in the middle to the slice
    // and the next element is either in the slice, or just after it. Returns null
    // if we're done with the slice.
    protected Unfiltered nextInSlice() throws IOException
    {
        return readUnfiltered();
    }

    protected RangeTombstoneMarker sliceEndMarker()
    {
        return markerFrom(end, openMarker);
    }
}
