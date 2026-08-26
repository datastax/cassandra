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

import org.apache.cassandra.io.util.ChannelProxy;
import org.apache.cassandra.io.util.MmapRebufferer;
import org.apache.cassandra.io.util.MmappedRegions;
import org.apache.cassandra.io.util.Rebufferer;
import org.apache.cassandra.utils.Closeable;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static org.apache.cassandra.io.util.RandomAccessReader.DEFAULT_BUFFER_SIZE;

public abstract class OnDiskBaseTrie<T, C extends Cursor<T>, Q extends BaseTrie<T, C, Q>> implements BaseTrie<T, C, Q>, Closeable
{
    final Rebufferer rebufferer;
    final OnDiskCursor.DataDeserializer<T> deserializer;
    final ByteComparable.Version byteComparableVersion;
    final long root;

    public OnDiskBaseTrie(Rebufferer rebufferer, OnDiskCursor.DataDeserializer<T> deserializer, ByteComparable.Version byteComparableVersion, long root)
    {
        this.rebufferer = rebufferer;
        this.deserializer = deserializer;
        this.byteComparableVersion = byteComparableVersion;
        this.root = root;
    }

    public Rebufferer rebufferer()
    {
        return rebufferer;
    }

    /// Open a rebufferer over the whole of the given channel, to be shared by all the cursors of a file-backed trie.
    ///
    /// A trie hands the same rebufferer to every cursor it makes, and a cursor keeps the data it was given until it
    /// has to move off it. More than one cursor is live at a time in normal use: a deletion branch cursor is taken
    /// while its parent is still walking, and merges and slices hold several. The rebufferer a
    /// [org.apache.cassandra.io.util.ChunkReader] instantiates owns a single buffer and hands out duplicates of it,
    /// so any trie longer than one chunk would have its cursors overwriting each other's data. Cursors are not
    /// closeable, so they cannot be given a buffer each either. Memory mapping gives every cursor a view of the same
    /// immutable mapping instead, and [MmapRebufferer] is documented to be shared between readers.
    static Rebufferer mapWholeFile(ChannelProxy channel)
    {
        long length = channel.size();
        MmappedRegions regions = length > 0 ? MmappedRegions.map(channel, length, DEFAULT_BUFFER_SIZE, 0, false)
                                            : MmappedRegions.empty(channel);
        return new MmapRebufferer(channel, length, regions);
    }

    interface RebuffererAccess
    {
        Rebufferer rebufferer();
    }

    protected interface WithoutChannel extends RebuffererAccess, Closeable
    {
        @Override
        default void close()
        {
            // Cursors aren't closeable. User must control that they are no longer in use.
            rebufferer().closeReader();
        }
    }

    protected interface WithOwnChannel extends RebuffererAccess, Closeable
    {
        @Override
        default void close()
        {
            Rebufferer rebufferer = rebufferer();
            try
            {
                rebufferer.closeReader();
            }
            finally
            {
                try
                {
                    rebufferer.close();
                }
                finally
                {
                    rebufferer.channel().close();
                }
            }
        }
    }
}
