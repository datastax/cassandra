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

import org.apache.cassandra.io.util.Rebufferer;
import org.apache.cassandra.utils.Closeable;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

public abstract class OnDiskBaseTrie<T, C extends Cursor<T>, Q extends BaseTrie<T, C, Q>> implements BaseTrie<T, C, Q>, Closeable
{
    final Rebufferer rebufferer;
    final OnDiskCursor.DataDeserializer<T> deserializer;
    final ByteComparable.Version byteComparableVersion;
    final long root;

    public OnDiskBaseTrie(Rebufferer rebufferer, OnDiskCursor.DataDeserializer<T> deserializer, ByteComparable.Version byteComparableVersion, long root)
    {
        this.rebufferer = rebufferer;
        this.deserializer = deserializer;
        this.byteComparableVersion = byteComparableVersion;
        this.root = root;
    }

    public Rebufferer rebufferer()
    {
        return rebufferer;
    }

    interface RebuffererAccess
    {
        Rebufferer rebufferer();
    }

    protected interface WithoutChannel extends RebuffererAccess, Closeable
    {
        @Override
        default void close()
        {
            // Cursors aren't closeable. User must control that they are no longer in use.
            rebufferer().closeReader();
        }
    }

    protected interface WithOwnChannel extends RebuffererAccess, Closeable
    {
        @Override
        default void close()
        {
            Rebufferer rebufferer = rebufferer();
            try
            {
                rebufferer.closeReader();
            }
            finally
            {
                try
                {
                    rebufferer.close();
                }
                finally
                {
                    rebufferer.channel().close();
                }
            }
        }
    }
}
