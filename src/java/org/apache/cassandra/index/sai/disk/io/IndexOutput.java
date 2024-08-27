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

package org.apache.cassandra.index.sai.disk.io;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * A subclass of {@link org.apache.lucene.store.IndexOutput} that provides access to the byte order of the underlying data.
 * This is used to select output implementations compatible with varying versions of Lucene.
 */
public abstract class IndexOutput extends org.apache.lucene.store.IndexOutput
{
    protected final ByteOrder order;

    public IndexOutput(String resourceDescription, String name, ByteOrder order)
    {
        super(resourceDescription, name);
        this.order = order;
    }

    public ByteOrder order()
    {
        return order;
    }

    public final void writeBytes(ByteBuffer buf) throws IOException
    {
        byte[] bytes = ByteBufferUtil.getArray(buf);
        writeVInt(bytes.length);
        writeBytes(bytes, 0, bytes.length);
    }
}
