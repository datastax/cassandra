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

package org.apache.cassandra.index.sai.disk.v2;

import java.io.IOException;

import org.apache.cassandra.index.sai.disk.v1.DirectReaders;
import org.apache.cassandra.index.sai.disk.v1.LeafOrderMap;
import org.apache.cassandra.index.sai.utils.SeekingRandomAccessInput;
import org.apache.cassandra.index.sai.utils.SharedIndexInput;
import org.apache.lucene.util.BytesRef;

public class PrefixBytesReader
{
    final SharedIndexInput lengthsInput;
    final SharedIndexInput input;
    final DirectReaders.Reader lengthsReader;
    final int lengthsBits;
    private int idx = 0;

    private final byte[] bytes = new byte[10];
    private final BytesRef bytesRef = new BytesRef();
    {
        bytesRef.bytes = bytes;
    }

    public PrefixBytesReader(long fp, SharedIndexInput input) throws IOException
    {
        this.input = input;
        this.lengthsBits = input.readByte();
        this.lengthsReader = DirectReaders.getReaderForBitsPerValue((byte) lengthsBits);
        this.lengthsInput = input.sharedCopy();
    }

    public BytesRef next() throws IOException
    {
        final long fp2 = input.getFilePointer();
        final SeekingRandomAccessInput lensSeekInput = new SeekingRandomAccessInput(lengthsInput);

        final int len = LeafOrderMap.getValue(lensSeekInput, fp2, idx, lengthsReader);

        idx++;

        input.readBytes(bytes, 0, len);

        return bytesRef;
    }
}
