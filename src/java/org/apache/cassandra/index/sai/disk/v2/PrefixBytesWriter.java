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

import org.agrona.collections.IntArrayList;
import org.apache.cassandra.index.sai.disk.v2.blockindex.BytesUtil;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;

public class PrefixBytesWriter
{
    //public static final int SUB_BLOCK_SIZE = 1;
    private final IndexOutput out;
    private final ByteBuffersDataOutput subBlockTerms = new ByteBuffersDataOutput();
    private final IntArrayList bytesLengths = new IntArrayList();
    private int count = 0;
    private BytesRef lastSubBlockTerm = null;

    public PrefixBytesWriter(IndexOutput out) throws IOException
    {
        this.out = out;
    }

    public void add(BytesRef bytes)
    {
//        if (count % SUB_BLOCK_SIZE == 0)
//        {
        if (lastSubBlockTerm != null)
        {
            final int diff = BytesUtil.bytesDifference(lastSubBlockTerm, bytes);
            subBlockTerms.writeBytes(bytes.bytes, diff, bytes.length);
            bytesLengths.add(bytes.length - diff);
        }
        else
        {
            subBlockTerms.writeBytes(bytes.bytes, 0, bytes.length);
            bytesLengths.add(bytes.length);
        }
        lastSubBlockTerm = BytesRef.deepCopyOf(bytes);
        //}
        count++;
    }

    public static class PrefixResult
    {
        public final long size;
        public final long lengthsFP;

        public PrefixResult(long size, long lengthsFP)
        {
            this.size = size;
            this.lengthsFP = lengthsFP;
        }
    }

    public PrefixResult finish() throws IOException
    {
        final int[] lengths = bytesLengths.toIntArray();
        final long lengthsFP = SimpleFoR.write(lengths, out);
        final DataInput bytesInput = subBlockTerms.toDataInput();
        final long size = subBlockTerms.size();
        for (int x = 0; x < size; x++)
        {
            out.writeByte(bytesInput.readByte());
        }
        return new PrefixResult(size, lengthsFP);
    }
}
