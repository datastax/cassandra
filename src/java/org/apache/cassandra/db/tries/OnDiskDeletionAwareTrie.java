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

import javax.annotation.Nullable;

import org.apache.cassandra.io.util.ByteBufferRebufferer;
import org.apache.cassandra.io.util.ChannelProxy;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.RebuffererFactory;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

/// Reads a [DeletionAwareTrie] written by [OnDiskDeletionAwareTrieWriter].
///
/// The data trie is read as an ordinary on-disk trie of the caller's content, walked with
/// [OnDiskCursor#alternateInAscentSlot] set so that the ascent-side content slot is understood as the
/// position of the node's deletion branch. A deletion branch is itself a complete range trie in the same
/// stream, so [Cursor#deletionBranchCursor] is just an [OnDiskCursor.Range] opened at that position.
///
/// See [OnDiskDeletionAwareTrieWriter] for why the branch position lives in that slot rather than in the node
/// encoding.
public class OnDiskDeletionAwareTrie<T, D extends RangeState<D>>
extends OnDiskBaseTrie<T, DeletionAwareCursor<T, D>, DeletionAwareTrie<T, D>>
implements DeletionAwareTrie<T, D>
{
    final OnDiskCursor.DataDeserializer<D> deletionDeserializer;

    OnDiskDeletionAwareTrie(RebuffererFactory rebuffererFactory,
                            OnDiskCursor.DataDeserializer<T> contentDeserializer,
                            OnDiskCursor.DataDeserializer<D> deletionDeserializer,
                            ByteComparable.Version byteComparableVersion,
                            long root,
                            @Nullable ChannelProxy ownedChannel)
    {
        super(rebuffererFactory, contentDeserializer, byteComparableVersion, root, ownedChannel);
        this.deletionDeserializer = deletionDeserializer;
    }

    @Override
    public DeletionAwareCursor<T, D> makeCursor(Direction direction)
    {
        if (root == 0)
            return new DeletionAwareCursor.Empty<>(direction, byteComparableVersion);
        return new OnDiskDeletionAwareCursor<>(deserializer, deletionDeserializer, rebuffererFactory,
                                               byteComparableVersion, direction, root);
    }

    public ByteComparable.Version byteComparableVersion()
    {
        return byteComparableVersion;
    }

    /// Open a trie that is already fully in memory — a commit-log record or a message payload —
    /// rather than a file. The buffer must outlive every cursor taken from the result, and is not
    /// released by [#close].
    ///
    /// `root` is the position the trie's root node ends at, as [OnDiskDeletionAwareTrieWriter#write] returns it. A
    /// negative value stands for the end of the buffer, which is where the root is when the trie takes up the whole
    /// buffer, since the writer emits the root last.
    public static <T, D extends RangeState<D>> OnDiskDeletionAwareTrie<T, D> open(ByteBuffer buffer,
                                                                                  OnDiskCursor.DataDeserializer<T> contentDeserializer,
                                                                                  OnDiskCursor.DataDeserializer<D> deletionDeserializer,
                                                                                  ByteComparable.Version version,
                                                                                  long root)
    {
        return new OnDiskDeletionAwareTrie<>(new ByteBufferRebufferer(buffer), contentDeserializer, deletionDeserializer,
                                             version, root, null);
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
            return new OnDiskDeletionAwareTrie<>(openChunkReader(channel), contentDeserializer, deletionDeserializer,
                                                 version, root, channel);
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
    /// [Cursor#MAY_HAVE_DELETION_BRANCH_BIT], so the plain cursor already reports both flags and its positions are
    /// the deletion-aware ones. The branch itself is only read when the caller asks for it.
    static class OnDiskDeletionAwareCursor<T, D extends RangeState<D>> extends OnDiskCursor<T> implements DeletionAwareCursor<T, D>
    {
        final OnDiskCursor.DataDeserializer<D> deletionDeserializer;

        OnDiskDeletionAwareCursor(OnDiskCursor.DataDeserializer<T> contentDeserializer,
                                  OnDiskCursor.DataDeserializer<D> deletionDeserializer,
                                  RebuffererFactory rebuffererFactory,
                                  ByteComparable.Version byteComparableVersion,
                                  Direction direction,
                                  long root)
        {
            super(contentDeserializer, rebuffererFactory, byteComparableVersion, direction, false, true, root);
            this.deletionDeserializer = deletionDeserializer;
        }

        OnDiskDeletionAwareCursor(OnDiskCursor.DataDeserializer<T> contentDeserializer,
                                  OnDiskCursor.DataDeserializer<D> deletionDeserializer,
                                  RebuffererFactory rebuffererFactory,
                                  ByteComparable.Version byteComparableVersion,
                                  Direction direction,
                                  long rootPostCodePos,
                                  int rootNodeCode)
        {
            super(contentDeserializer, rebuffererFactory, byteComparableVersion, direction, false, true, rootPostCodePos, rootNodeCode, null);
            this.deletionDeserializer = deletionDeserializer;
        }

        @Override
        public RangeCursor<D> deletionBranchCursor(Direction direction)
        {
            long root = alternateBranch();
            if (root < 0)
                return null;
            return new OnDiskCursor.Range<>(deletionDeserializer, rebuffererFactory, byteComparableVersion, direction, root);
        }

        @Override
        public DeletionAwareCursor<T, D> tailCursor(Direction direction)
        {
            return new OnDiskDeletionAwareCursor<>(rdr.deserializer, deletionDeserializer, rebuffererFactory, byteComparableVersion,
                                                   direction, currentFullNodePostCodePos, currentFullNodeCode);
        }
    }
}
