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

import org.apache.cassandra.io.util.RebuffererFactory;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

/// Presents the data trie of an [OnDiskDeletionAwareTrie] as a deletion-aware cursor.
///
/// A node's descent- and ascent-content bits are exactly [Cursor#MAY_HAVE_CONTENT_BIT] and
/// [Cursor#MAY_HAVE_DELETION_BRANCH_BIT], so the plain cursor already reports both flags and its positions are
/// the deletion-aware ones. The branch itself is only read when the caller asks for it.
class OnDiskDeletionAwareCursor<T, D extends RangeState<D>> extends OnDiskCursor<T> implements DeletionAwareCursor<T, D>
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
        super(contentDeserializer, rebuffererFactory, byteComparableVersion, direction, false, true, rootPostCodePos, rootNodeCode);
        this.deletionDeserializer = deletionDeserializer;
    }

    @Override
    public RangeCursor<D> deletionBranchCursor(Direction direction)
    {
        long root = alternateBranch();
        if (root < 0)
            return null;
        return new OnDiskRangeCursor<>(deletionDeserializer, rebuffererFactory, byteComparableVersion, direction, root);
    }

    @Override
    public DeletionAwareCursor<T, D> tailCursor(Direction direction)
    {
        return new OnDiskDeletionAwareCursor<>(rdr.deserializer, deletionDeserializer, rebuffererFactory, byteComparableVersion,
                                               direction, currentFullNodePostCodePos, currentFullNodeCode);
    }
}
