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
package org.apache.cassandra.index.sai.disk.v1.kdtree;

import java.io.IOException;
import java.nio.ByteOrder;

import org.apache.cassandra.index.sai.disk.oldlucene.DirectWriterAdapter;
import org.apache.cassandra.index.sai.disk.oldlucene.LuceneCompat;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.LongValues;

public class LeafOrderMap
{
    public static int getValue(int index, LongValues reader)
    {
        return Math.toIntExact(reader.get(index));
    }

    public static void write(ByteOrder order, final int[] array, int length, int maxValue, final DataOutput out) throws IOException
    {
        final int bits = LuceneCompat.directWriterUnsignedBitsRequired(order, maxValue);
        final DirectWriterAdapter writer = LuceneCompat.directWriterGetInstance(order, out, length, bits);
        for (int i = 0; i < length; i++)
        {
            assert array[i] <= maxValue;

            writer.add(array[i]);
        }
        writer.finish();
    }
}
