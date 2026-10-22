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

import javax.annotation.Nullable;

import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.util.ChannelProxy;
import org.apache.cassandra.io.util.RebuffererFactory;
import org.apache.cassandra.io.util.SimpleChunkReader;
import org.apache.cassandra.utils.Closeable;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static org.apache.cassandra.io.util.RandomAccessReader.DEFAULT_BUFFER_SIZE;

public abstract class OnDiskBaseTrie<T, C extends Cursor<T>, Q extends BaseTrie<T, C, Q>>
implements BaseTrie<T, C, Q>, Closeable
{
    final RebuffererFactory rebuffererFactory;
    final OnDiskCursor.DataDeserializer<T> deserializer;
    final ByteComparable.Version byteComparableVersion;
    final long root;
    /// The channel the trie was opened over, which [#close] closes along with [#rebuffererFactory], or null when the
    /// trie reads a buffer someone else owns -- a commit-log record or a message payload -- and there is nothing to
    /// release: the cursors give their rebufferers back as they are closed.
    @Nullable
    final ChannelProxy ownedChannel;

    /// A negative `root` stands for the end of the data, which is where the writer puts the root of a trie that takes
    /// up the whole buffer or file.
    OnDiskBaseTrie(RebuffererFactory rebuffererFactory, OnDiskCursor.DataDeserializer<T> deserializer, ByteComparable.Version byteComparableVersion, long root, @Nullable ChannelProxy ownedChannel)
    {
        this.rebuffererFactory = rebuffererFactory;
        this.deserializer = deserializer;
        this.byteComparableVersion = byteComparableVersion;
        this.root = root >= 0 ? root : rebuffererFactory.fileLength();
        this.ownedChannel = ownedChannel;
    }

    /// The trie's buffer source. Each cursor takes a rebufferer of its own from it; see [OnDiskCursor#rebufferer].
    public RebuffererFactory rebuffererFactory()
    {
        return rebuffererFactory;
    }

    /// Open a factory over the given channel, which hands each cursor of a file-backed trie a rebufferer of its own.
    ///
    /// A cursor keeps the data it was given until it has to move off it, and more than one cursor is live at a time
    /// in normal use: a deletion branch cursor is taken while its parent is still walking, and merges and slices hold
    /// several. The rebufferer a [org.apache.cassandra.io.util.ChunkReader] instantiates owns a single buffer and
    /// hands out duplicates of it, so cursors sharing one overwrite each other's data as soon as the trie is longer
    /// than a chunk. The chunk reader itself is thread-safe and is what the cursors share instead; each takes a
    /// buffer from it on construction and returns it on [Cursor#close].
    static RebuffererFactory openChunkReader(ChannelProxy channel)
    {
        return new SimpleChunkReader(channel, -1, BufferType.OFF_HEAP, DEFAULT_BUFFER_SIZE);
    }

    /// Close the factory, then the channel it holds a shared copy of, if the trie owns one. The cursors have given
    /// their rebufferers back as they were closed; the caller must have stopped reading by now.
    @Override
    public void close()
    {
        if (ownedChannel == null)
            return;

        try
        {
            rebuffererFactory.close();
        }
        finally
        {
            ownedChannel.close();
        }
    }
}
