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

/// A cursor over an on-disk range trie, from its root: the trie of an [OnDiskRangeTrie], or a deletion branch of an
/// [OnDiskDeletionAwareTrie]. It presents every node's content as it is written, its root's included; a tail of it
/// is an [OnDiskRangeTailCursor].
class OnDiskRangeCursor<S extends RangeState<S>> extends OnDiskBaseRangeCursor<S>
{
    OnDiskRangeCursor(DataDeserializer<S> deserializer, RebuffererFactory rebuffererFactory, ByteComparable.Version byteComparableVersion, Direction direction, long root)
    {
        super(deserializer, rebuffererFactory, byteComparableVersion, direction, root);
        initActiveState();
    }
}
