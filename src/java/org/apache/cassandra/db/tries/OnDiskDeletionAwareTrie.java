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

import java.nio.ByteBuffer;
import java.util.Set;

import org.apache.cassandra.io.util.ByteBufferRebufferer;
import org.apache.cassandra.io.util.ChannelProxy;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.Rebufferer;
import org.apache.cassandra.io.util.RebuffererFactory;
import org.apache.cassandra.utils.Closeable;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

/// Reads a [DeletionAwareTrie] written by [DeletionAwareFileWriter].
///
/// The data trie is read as an ordinary on-disk trie of the caller's content, walked with
/// [OnDiskCursor#alternateInAscentSlot] set so that the ascent-side content slot is understood as the
/// position of the node's deletion branch. A deletion branch is itself a complete range trie in the same
/// stream, so [Cursor#deletionBranchCursor] is just an [OnDiskCursor.Range] opened at that position.
///
/// See [DeletionAwareFileWriter] for why the branch position lives in that slot rather than in the node
/// encoding.
public class OnDiskDeletionAwareTrie<T, D extends RangeState<D>>
implements DeletionAwareTrie<T, D>, OnDiskCursor.RebuffererSource, Closeable
{
    final RebuffererFactory rebuffererFactory;
    final Set<Rebufferer> outstandingRebufferers;
    final OnDiskCursor.DataDeserializer<T> contentDeserializer;
    final OnDiskCursor.DataDeserializer<D> deletionDeserializer;
    final ByteComparable.Version byteComparableVersion;
    final long root;
    private final boolean ownsChannel;
    private final ChannelProxy channel;

    OnDiskDeletionAwareTrie(RebuffererFactory rebuffererFactory,
                            OnDiskCursor.DataDeserializer<T> contentDeserializer,
                            OnDiskCursor.DataDeserializer<D> deletionDeserializer,
                            ByteComparable.Version byteComparableVersion,
                            long root,
                            ChannelProxy channel)
    {
        this.rebuffererFactory = rebuffererFactory;
        this.outstandingRebufferers = OnDiskCursor.RebuffererSource.trackerFor(rebuffererFactory);
        this.contentDeserializer = contentDeserializer;
        this.deletionDeserializer = deletionDeserializer;
        this.byteComparableVersion = byteComparableVersion;
        this.root = root;
        this.channel = channel;
        this.ownsChannel = channel != null;
    }

    @Override
    public DeletionAwareCursor<T, D> makeCursor(Direction direction)
    {
        if (root == 0)
            return new DeletionAwareCursor.Empty<>(direction, byteComparableVersion);
        return new OnDiskDeletionAwareCursor<>(this, direction, root);
    }

    public ByteComparable.Version byteComparableVersion()
    {
        return byteComparableVersion;
    }

    @Override
    public RebuffererFactory rebuffererFactory()
    {
        return rebuffererFactory;
    }

    @Override
    public Set<Rebufferer> outstandingRebufferers()
    {
        return outstandingRebufferers;
    }

    /// Mirrors [OnDiskBaseTrie.WithOwnChannel]: whatever the cursors still hold is released first, then the factory,
    /// then the channel, which the factory holds a shared copy of. The caller must have stopped reading by now.
    @Override
    public void close()
    {
        releaseOutstandingRebufferers();
        if (!ownsChannel)
            return;

        try
        {
            rebuffererFactory.close();
        }
        finally
        {
            channel.close();
        }
    }

    /// Open a trie that is already fully in memory — a commit-log record or a message payload —
    /// rather than a file. The buffer must outlive every cursor taken from the result, and is not
    /// released by [#close].
    ///
    /// `root` is the position the trie's root node ends at; pass -1 when the trie occupies the
    /// whole buffer, since the writer emits the root last.
    public static <T, D extends RangeState<D>> OnDiskDeletionAwareTrie<T, D> open(ByteBuffer buffer,
                                                                                  OnDiskCursor.DataDeserializer<T> contentDeserializer,
                                                                                  OnDiskCursor.DataDeserializer<D> deletionDeserializer,
                                                                                  ByteComparable.Version version,
                                                                                  long root)
    {
        ByteBufferRebufferer rebufferer = new ByteBufferRebufferer(buffer);
        return new OnDiskDeletionAwareTrie<>(rebufferer, contentDeserializer, deletionDeserializer, version,
                                             root >= 0 ? root : rebufferer.fileLength(), null);
    }

    public static <T, D extends RangeState<D>> OnDiskDeletionAwareTrie<T, D> open(File file,
                                                                                  OnDiskCursor.DataDeserializer<T> contentDeserializer,
                                                                                  OnDiskCursor.DataDeserializer<D> deletionDeserializer,
                                                                                  ByteComparable.Version version,
                                                                                  long root)
    {
        ChannelProxy channel = new ChannelProxy(file);
        try
        {
            RebuffererFactory rebuffererFactory = OnDiskBaseTrie.openChunkReader(channel);
            return new OnDiskDeletionAwareTrie<>(rebuffererFactory, contentDeserializer, deletionDeserializer, version,
                                                 root >= 0 ? root : rebuffererFactory.fileLength(), channel);
        }
        catch (Throwable t)
        {
            channel.close();
            throw t;
        }
    }

    /// Presents the data trie as a deletion-aware cursor.
    ///
    /// A node's descent- and ascent-content bits are exactly [Cursor#MAY_HAVE_CONTENT_BIT] and
    /// [Cursor#MAY_HAVE_DELETION_BRANCH_BIT], so the underlying cursor already reports both flags and the
    /// positions it returns are passed through unchanged. The branch itself is only read when the caller
    /// asks for it.
    static class OnDiskDeletionAwareCursor<T, D extends RangeState<D>> implements DeletionAwareCursor<T, D>
    {
        final OnDiskDeletionAwareTrie<T, D> trie;
        final OnDiskCursor<T> source;

        OnDiskDeletionAwareCursor(OnDiskDeletionAwareTrie<T, D> trie, Direction direction, long root)
        {
            this.trie = trie;
            this.source = new OnDiskCursor<>(trie.contentDeserializer, trie,
                                             trie.byteComparableVersion, direction, false, true, root);
        }

        OnDiskDeletionAwareCursor(OnDiskDeletionAwareTrie<T, D> trie, Direction direction, long rootPostCodePos, int rootNodeCode)
        {
            this.trie = trie;
            this.source = new OnDiskCursor<>(trie.contentDeserializer, trie,
                                             trie.byteComparableVersion, direction, false, true, rootPostCodePos, rootNodeCode);
        }

        @Override
        public RangeCursor<D> deletionBranchCursor(Direction direction)
        {
            long root = source.alternateBranch();
            if (root < 0)
                return null;
            return new OnDiskCursor.Range<>(trie.deletionDeserializer, trie,
                                            trie.byteComparableVersion, direction, root);
        }

        @Override
        public T content()
        {
            return source.content();
        }

        @Override
        public long encodedPosition()
        {
            return source.encodedPosition();
        }

        @Override
        public ByteComparable.Version byteComparableVersion()
        {
            return source.byteComparableVersion();
        }

        @Override
        public long advance()
        {
            return source.advance();
        }

        @Override
        public long advanceMultiple(TransitionsReceiver receiver)
        {
            return source.advanceMultiple(receiver);
        }

        @Override
        public long skipTo(long encodedSkipPosition)
        {
            return source.skipTo(encodedSkipPosition);
        }

        @Override
        public DeletionAwareCursor<T, D> tailCursor(Direction direction)
        {
            return new OnDiskDeletionAwareCursor<>(trie, direction, source.currentFullNodePostCodePos, source.currentFullNodeCode);
        }

        @Override
        public void close()
        {
            source.close();
        }
    }
}
