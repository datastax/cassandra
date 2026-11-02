/*
 * Copyright IBM Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.db.tries;

import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.util.ChannelProxy;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.RebuffererFactory;
import org.apache.cassandra.io.util.SimpleChunkReader;
import org.apache.cassandra.utils.Closeable;

import static org.apache.cassandra.db.tries.TrieUtil.VERSION;
import static org.apache.cassandra.io.util.RandomAccessReader.DEFAULT_BUFFER_SIZE;

/// On-disk tries read from a file, for tests. The on-disk tries read bytes someone else owns; these open the file
/// themselves, and close what they opened when they are closed. Each takes up the whole file, with its root at the end.
public class FileTries
{
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

    static <T> Plain<T> open(File file, OnDiskCursor.DataDeserializer<T> deserializer, boolean isOrdered)
    {
        ChannelProxy channel = new ChannelProxy(file);
        try
        {
            return new Plain<>(openChunkReader(channel), channel, deserializer, isOrdered);
        }
        catch (Throwable t)
        {
            channel.close();
            throw t;
        }
    }

    static <S extends RangeState<S>> Range<S> openRange(File file, OnDiskCursor.DataDeserializer<S> deserializer)
    {
        ChannelProxy channel = new ChannelProxy(file);
        try
        {
            return new Range<>(openChunkReader(channel), channel, deserializer);
        }
        catch (Throwable t)
        {
            channel.close();
            throw t;
        }
    }

    static <T, D extends RangeState<D>> DeletionAware<T, D> openDeletionAware(File file,
                                                                             OnDiskCursor.DataDeserializer<T> contentDeserializer,
                                                                             OnDiskCursor.DataDeserializer<D> deletionDeserializer)
    {
        ChannelProxy channel = new ChannelProxy(file);
        try
        {
            return new DeletionAware<>(openChunkReader(channel), channel, contentDeserializer, deletionDeserializer);
        }
        catch (Throwable t)
        {
            channel.close();
            throw t;
        }
    }

    /// Close the factory, then the channel, which the factory reads through but does not close. The cursors have given
    /// their rebufferers back as they were closed; the caller must have stopped reading by now.
    private static void close(RebuffererFactory rebuffererFactory, ChannelProxy channel)
    {
        try
        {
            rebuffererFactory.close();
        }
        finally
        {
            channel.close();
        }
    }

    /// An [OnDiskTrie] that owns the factory it reads through and the channel under it.
    static class Plain<T> extends OnDiskTrie<T> implements Closeable
    {
        final ChannelProxy channel;

        Plain(RebuffererFactory rebuffererFactory, ChannelProxy channel, OnDiskCursor.DataDeserializer<T> deserializer, boolean isOrdered)
        {
            super(rebuffererFactory, deserializer, VERSION, isOrdered, -1);
            this.channel = channel;
        }

        @Override
        public void close()
        {
            FileTries.close(rebuffererFactory, channel);
        }
    }

    /// An [OnDiskRangeTrie] that owns the factory it reads through and the channel under it.
    static class Range<S extends RangeState<S>> extends OnDiskRangeTrie<S> implements Closeable
    {
        final ChannelProxy channel;

        Range(RebuffererFactory rebuffererFactory, ChannelProxy channel, OnDiskCursor.DataDeserializer<S> deserializer)
        {
            super(rebuffererFactory, deserializer, VERSION, -1);
            this.channel = channel;
        }

        @Override
        public void close()
        {
            FileTries.close(rebuffererFactory, channel);
        }
    }

    /// An [OnDiskDeletionAwareTrie] that owns the factory it reads through and the channel under it.
    static class DeletionAware<T, D extends RangeState<D>> extends OnDiskDeletionAwareTrie<T, D> implements Closeable
    {
        final ChannelProxy channel;

        DeletionAware(RebuffererFactory rebuffererFactory,
                      ChannelProxy channel,
                      OnDiskCursor.DataDeserializer<T> contentDeserializer,
                      OnDiskCursor.DataDeserializer<D> deletionDeserializer)
        {
            super(rebuffererFactory, contentDeserializer, deletionDeserializer, VERSION, -1);
            this.channel = channel;
        }

        @Override
        public void close()
        {
            FileTries.close(rebuffererFactory, channel);
        }
    }
}
