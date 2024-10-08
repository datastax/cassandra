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

package org.apache.cassandra.db;


import java.nio.ByteBuffer;

import org.apache.cassandra.db.marshal.ByteArrayAccessor;
import org.apache.cassandra.db.marshal.ValueAccessor;

public abstract class AbstractArrayClusteringPrefix extends AbstractOnHeapClusteringPrefix<byte[]>
{
    public static final byte[][] EMPTY_VALUES_ARRAY = new byte[0][];

    public AbstractArrayClusteringPrefix(Kind kind, byte[][] values)
    {
        super(kind, values);
    }

    public ValueAccessor<byte[]> accessor()
    {
        return ByteArrayAccessor.instance;
    }

    public ByteBuffer[] getBufferArray()
    {
        ByteBuffer[] out = new ByteBuffer[values.length];
        for (int i = 0; i < values.length; i++)
        {
            // Compact tables allowed null clustering elements, so take those into account: 
            out[i] = values[i] == null ? null : ByteBuffer.wrap(values[i]);
        }
        return out;
    }

    public ClusteringPrefix<byte[]> retainable()
    {
        return this;
    }
}
