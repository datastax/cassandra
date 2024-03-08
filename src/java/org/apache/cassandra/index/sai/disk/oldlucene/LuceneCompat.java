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

package org.apache.cassandra.index.sai.disk.oldlucene;

import java.nio.ByteOrder;

import org.apache.cassandra.index.sai.utils.SeekingRandomAccessInput;
import org.apache.lucene.backward_codecs.packed.LegacyDirectReader;
import org.apache.lucene.backward_codecs.packed.LegacyDirectWriter;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.packed.DirectReader;
import org.apache.lucene.util.packed.DirectWriter;

/**
 * Compatibility layer for Lucene 7.5 and earlier.
 */
public class LuceneCompat
{
    public static LongValues directReaderGetInstance(SeekingRandomAccessInput slice, int bitsPerValue, long offset)
    {
        if (slice.order() == ByteOrder.LITTLE_ENDIAN)
            return DirectReader.getInstance(slice, bitsPerValue, offset);
        // Lucene 7.5 and earlier used big-endian formatting
        return LegacyDirectReader.getInstance(slice, bitsPerValue, offset);
    }

    public static int directWriterUnsignedBitsRequired(ByteOrder order, long maxValue)
    {
        if (order == ByteOrder.LITTLE_ENDIAN)
            return DirectWriter.unsignedBitsRequired(maxValue);
        // Lucene 7.5 and earlier used big-endian formatting
        return LegacyDirectWriter.unsignedBitsRequired(maxValue);
    }
}
