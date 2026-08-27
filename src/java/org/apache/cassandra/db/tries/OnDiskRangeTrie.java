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
import org.apache.cassandra.io.util.RebuffererFactory;
import org.apache.cassandra.utils.Closeable;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

public abstract class OnDiskRangeTrie<S extends RangeState<S>> extends OnDiskBaseTrie<S, RangeCursor<S>, RangeTrie<S>> implements RangeTrie<S>, Closeable
{
    public OnDiskRangeTrie(RebuffererFactory rebuffererFactory, OnDiskCursor.DataDeserializer<S> deserializer, ByteComparable.Version byteComparableVersion, long root)
    {
        super(rebuffererFactory, deserializer, byteComparableVersion, root);
    }

    @Override
    public RangeCursor<S> makeCursor(Direction direction)
    {
        if (root != 0)
            return new OnDiskCursor.Range<>(deserializer, this, byteComparableVersion, direction, root);
        else
            return new RangeCursor.Empty<>(null, byteComparableVersion, direction);
    }

    static class WithoutChannel<S extends RangeState<S>> extends OnDiskRangeTrie<S> implements OnDiskBaseTrie.WithoutChannel
    {
        public WithoutChannel(RebuffererFactory rebuffererFactory, OnDiskCursor.DataDeserializer<S> deserializer, ByteComparable.Version byteComparableVersion, long root)
        {
            super(rebuffererFactory, deserializer, byteComparableVersion, root);
        }
    }

    static class WithOwnChannel<S extends RangeState<S>> extends OnDiskRangeTrie<S> implements OnDiskBaseTrie.WithOwnChannel
    {
        public WithOwnChannel(RebuffererFactory rebuffererFactory, OnDiskCursor.DataDeserializer<S> deserializer, ByteComparable.Version byteComparableVersion, long root)
        {
            super(rebuffererFactory, deserializer, byteComparableVersion, root);
        }
    }


    public static <S extends RangeState<S>> OnDiskRangeTrie<S> open(File file, OnDiskCursor.DataDeserializer<S> deserializer, ByteComparable.Version version, long root)
    {
        ChannelProxy channel = new ChannelProxy(file);
        try
        {
            RebuffererFactory rebuffererFactory = openChunkReader(channel);
            return new WithOwnChannel(rebuffererFactory, deserializer, version, root >= 0 ? root : rebuffererFactory.fileLength());
        }
        catch (Throwable t)
        {
            channel.close();
            throw t;
        }
    }
}
