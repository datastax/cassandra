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

public class OnDiskRangeTrie<S extends RangeState<S>> extends OnDiskBaseTrie<S, RangeCursor<S>, RangeTrie<S>> implements RangeTrie<S>
{
    OnDiskRangeTrie(RebuffererFactory rebuffererFactory, OnDiskCursor.DataDeserializer<S> deserializer, ByteComparable.Version byteComparableVersion, long root, @Nullable ChannelProxy ownedChannel)
    {
        super(rebuffererFactory, deserializer, byteComparableVersion, root, ownedChannel);
    }

    @Override
    public RangeCursor<S> makeCursor(Direction direction)
    {
        if (root != 0)
            return new OnDiskCursor.Range<>(deserializer, rebuffererFactory, byteComparableVersion, direction, root);
        else
            return new RangeCursor.Empty<>(null, byteComparableVersion, direction);
    }

    public static <S extends RangeState<S>> OnDiskRangeTrie<S> open(File file, OnDiskCursor.DataDeserializer<S> deserializer, ByteComparable.Version version, long root)
    {
        ChannelProxy channel = new ChannelProxy(file);
        try
        {
            return new OnDiskRangeTrie<>(openChunkReader(channel), deserializer, version, root, channel);
        }
        catch (Throwable t)
        {
            channel.close();
            throw t;
        }
    }

    /// Open a trie that is already fully in memory rather than a file. The buffer must outlive every cursor taken from
    /// the result, and is not released by [#close].
    public static <S extends RangeState<S>> OnDiskRangeTrie<S> open(ByteBuffer buffer, OnDiskCursor.DataDeserializer<S> deserializer, ByteComparable.Version version, long root)
    {
        return new OnDiskRangeTrie<>(new ByteBufferRebufferer(buffer), deserializer, version, root, null);
    }
}
