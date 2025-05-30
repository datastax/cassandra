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
package org.apache.cassandra.utils;

import java.io.DataInput;
import java.io.IOException;
import java.io.InputStream;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.obs.IBitSet;
import org.apache.cassandra.utils.obs.MemoryLimiter;
import org.apache.cassandra.utils.obs.OffHeapBitSet;

import static org.apache.cassandra.utils.FilterFactory.AlwaysPresent;

public final class BloomFilterSerializer implements IFilterSerializer, IFilterDeserializer
{
    private final static Logger logger = LoggerFactory.getLogger(BloomFilterSerializer.class);

    private final MemoryLimiter memoryLimiter;

    public BloomFilterSerializer(MemoryLimiter memoryLimiter)
    {
        this.memoryLimiter = memoryLimiter;
    }

    @Override
    public void serialize(IFilter filter, DataOutputPlus out) throws IOException
    {
        Preconditions.checkArgument(filter instanceof BloomFilter);
        BloomFilter bf = (BloomFilter) filter;
        out.writeInt(bf.hashCount);
        bf.bitset.serialize(out);
    }

    @SuppressWarnings("resource")
    @Override
    public <I extends InputStream & DataInput> IFilter deserialize(I in, boolean oldBfFormat) throws IOException
    {
        int hashes = in.readInt();
        IBitSet bs;
        try
        {
            bs = OffHeapBitSet.deserialize(in, oldBfFormat, memoryLimiter);
        }
        catch (MemoryLimiter.ReachedMemoryLimitException | OutOfMemoryError e)
        {
            logger.error("Failed to create Bloom filter during deserialization: ({}) - " +
                         "continuing but this will have severe performance implications, consider increasing FP chance or" +
                         "lowering number of sstables through compaction", e.getMessage());
            return AlwaysPresent;
        }
        return new BloomFilter(hashes, bs);
    }

    /**
     * Calculates a serialized size of the given Bloom Filter
     *
     * @param bf Bloom filter to calculate serialized size
     * @return serialized size of the given bloom filter
     * @see org.apache.cassandra.io.ISerializer#serialize(Object, org.apache.cassandra.io.util.DataOutputPlus)
     */
    public long serializedSize(BloomFilter bf)
    {
        int size = TypeSizes.sizeof(bf.hashCount); // hash count
        size += bf.bitset.serializedSize();
        return size;
    }
}
