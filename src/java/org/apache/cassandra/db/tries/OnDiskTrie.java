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
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.Rebufferer;
import org.apache.cassandra.utils.Closeable;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

public abstract class OnDiskTrie<T> extends OnDiskBaseTrie<T, Cursor<T>, Trie<T>> implements Trie<T>, Closeable
{
    final boolean isOrdered;

    public OnDiskTrie(Rebufferer rebufferer, OnDiskCursor.DataDeserializer<T> deserializer, ByteComparable.Version byteComparableVersion, boolean isOrdered, long root)
    {
        super(rebufferer, deserializer, byteComparableVersion, root);
        this.isOrdered = isOrdered;
    }

    @Override
    public Cursor<T> makeCursor(Direction direction)
    {
        if (root != 0)
            return new OnDiskCursor<>(deserializer, rebufferer, byteComparableVersion, direction, isOrdered, root);
        else
            return new Cursor.Empty<>(direction, byteComparableVersion);
    }

    static class WithoutChannel<T> extends OnDiskTrie<T> implements OnDiskBaseTrie.WithoutChannel
    {
        public WithoutChannel(Rebufferer rebufferer, OnDiskCursor.DataDeserializer<T> deserializer, ByteComparable.Version byteComparableVersion, boolean isOrdered, long root)
        {
            super(rebufferer, deserializer, byteComparableVersion, isOrdered, root);
        }
    }

    static class WithOwnChannel<T> extends OnDiskTrie<T> implements OnDiskBaseTrie.WithOwnChannel
    {
        public WithOwnChannel(Rebufferer rebufferer, OnDiskCursor.DataDeserializer<T> deserializer, ByteComparable.Version byteComparableVersion, boolean isOrdered, long root)
        {
            super(rebufferer, deserializer, byteComparableVersion, isOrdered, root);
        }
    }

    public static <T> OnDiskTrie<T> open(File file, OnDiskCursor.DataDeserializer<T> deserializer, ByteComparable.Version version, boolean isOrdered, long root)
    {
        ChannelProxy channel = new ChannelProxy(file);
        try
        {
            Rebufferer rebufferer = mapWholeFile(channel);
            return new WithOwnChannel(rebufferer, deserializer, version, isOrdered, root >= 0 ? root : rebufferer.fileLength());
        }
        catch (Throwable t)
        {
            channel.close();
            throw t;
        }
    }
}
