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
import java.util.Arrays;

import org.apache.cassandra.index.sai.disk.v1.DirectReaders;
import org.apache.cassandra.index.sai.disk.v1.LeafOrderMap;
import org.apache.cassandra.index.sai.utils.SeekingRandomAccessInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.packed.DirectWriter;

public class SimpleFoR
{
    public static long write(int[] array, IndexOutput out) throws IOException
    {
        long fp = out.getFilePointer();
        final int max = Arrays.stream(array).max().getAsInt();
        final int bits = DirectWriter.unsignedBitsRequired(max);
        out.writeByte((byte) bits);
        LeafOrderMap.write(array, array.length, max, out);
        return fp;
    }

    public static void read(int[] array, int len, long fp, IndexInput input) throws IOException
    {
        input.seek(fp);
        final int bits = input.readByte();
        final long fp2 = input.getFilePointer();
        final SeekingRandomAccessInput seekInput = new SeekingRandomAccessInput(input);
        final DirectReaders.Reader reader = DirectReaders.getReaderForBitsPerValue((byte) bits);
        for (int x = 0; x < len; x++)
        {
            array[x] = LeafOrderMap.getValue(seekInput, fp2, x, reader);
        }
    }
}
