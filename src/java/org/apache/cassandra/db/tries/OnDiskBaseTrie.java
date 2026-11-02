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

import org.apache.cassandra.io.util.RebuffererFactory;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

/// The part the on-disk tries have in common: a view over the bytes a [RebuffererFactory] reads. The trie owns
/// nothing: whoever provides the buffer or file must keep it available for as long as cursors are taken from the trie.
public abstract class OnDiskBaseTrie<T, C extends Cursor<T>, Q extends BaseTrie<T, C, Q>>
implements BaseTrie<T, C, Q>
{
    final RebuffererFactory rebuffererFactory;
    final OnDiskCursor.DataDeserializer<T> deserializer;
    final ByteComparable.Version byteComparableVersion;
    final long root;

    /// A negative `root` stands for the end of the data, which is where the writer puts the root of a trie that takes
    /// up the whole buffer or file.
    OnDiskBaseTrie(RebuffererFactory rebuffererFactory, OnDiskCursor.DataDeserializer<T> deserializer, ByteComparable.Version byteComparableVersion, long root)
    {
        this.rebuffererFactory = rebuffererFactory;
        this.deserializer = deserializer;
        this.byteComparableVersion = byteComparableVersion;
        this.root = root >= 0 ? root : rebuffererFactory.fileLength();
    }

    /// The trie's buffer source. Each cursor takes a rebufferer of its own from it; see [OnDiskCursor#rebufferer].
    public RebuffererFactory rebuffererFactory()
    {
        return rebuffererFactory;
    }
}
