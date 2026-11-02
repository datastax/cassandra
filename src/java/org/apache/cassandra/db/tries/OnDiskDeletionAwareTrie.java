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

import org.apache.cassandra.io.util.ByteBufferRebufferer;
import org.apache.cassandra.io.util.RebuffererFactory;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

/// Reads a [DeletionAwareTrie] written by [OnDiskDeletionAwareTrieWriter].
///
/// The data trie is read as an ordinary on-disk trie of the caller's content, walked with
/// [OnDiskCursor#alternateInAscentSlot] set so that the ascent-side content slot is understood as the
/// position of the node's deletion branch. A deletion branch is itself a complete range trie in the same
/// stream, so [Cursor#deletionBranchCursor] is just an [OnDiskRangeCursor] opened at that position.
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
                            long root)
    {
        super(rebuffererFactory, contentDeserializer, byteComparableVersion, root);
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

    /// The length of the buffer or file the trie is read from.
    public long sizeInBytes()
    {
        return rebuffererFactory.fileLength();
    }

    /// Open a trie that is already fully in memory — a commit-log record or a message payload —
    /// rather than a file. The buffer must outlive every cursor taken from the result.
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
                                             version, root);
    }
}
