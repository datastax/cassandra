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

package org.apache.cassandra.index.sai.disk.v2.sortedbytes;

import java.io.IOException;
import java.util.Iterator;

import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.lucene.store.ByteArrayIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.packed.DirectMonotonicReader;

import static org.apache.cassandra.index.sai.disk.v2.sortedbytes.SortedBytesWriter.DIRECT_MONOTONIC_BLOCK_SHIFT;
import static org.apache.cassandra.index.sai.disk.v2.sortedbytes.SortedBytesWriter.TERMS_DICT_BLOCK_MASK;
import static org.apache.cassandra.index.sai.disk.v2.sortedbytes.SortedBytesWriter.TERMS_DICT_BLOCK_SHIFT;

/**
 * Lookup a point id by ByteComparable.
 *   Implemented exactly from Lucene version 7.5 SortedDocValues
 *
 * Lookup a ByteComparable by point id.
 *   Implemented using the trie with a long payload for the point id.
 */
public class SortedBytesReader
{
    private final SortedBytesWriter.Meta meta;
    private final FileHandle trieHandle;
    private final LongValues offsets;

    public SortedBytesReader(SortedBytesWriter.Meta meta,
                             FileHandle trieHandle,
                             IndexInput offsetsInput) throws IOException
    {
        this.meta = meta;
        this.trieHandle = trieHandle;

        ByteArrayIndexInput offsestMetaInput = new ByteArrayIndexInput("", meta.offsetMetaBytes);

        DirectMonotonicReader.Meta offsetsMeta = DirectMonotonicReader.loadMeta(offsestMetaInput, meta.offsetBlockCount, DIRECT_MONOTONIC_BLOCK_SHIFT);
        RandomAccessInput offsetSlice = offsetsInput.randomAccessSlice(0, offsetsInput.length());

        offsets = DirectMonotonicReader.getInstance(offsetsMeta, offsetSlice);
    }

    /**
     * Returns the point id / ordinal of the target term, if not matching the next greater.
     * @param term target term to lookup
     * @return point id / ordinal of the target or -1 if not found
     * @throws IOException
     */
    public long seekToBytes(ByteComparable term) throws IOException
    {
        if (term == null)
        {
            throw new IllegalArgumentException("term null");
        }

        try (TrieRangeIterator reader = new TrieRangeIterator(trieHandle.instantiateRebufferer(),
                                                              meta.trieFP,
                                                              term,
                                                              null,
                                                              true,
                                                              true))
        {
            final Iterator<Pair<ByteSource, Long>> iterator = reader.iterator();

            if (iterator.hasNext())
            {
                final Pair<ByteSource, Long> pair = iterator.next();
                return pair.right.longValue();
            }
            return -1;
        }
    }

    /**
     * Reusable context object for seekExact calls
     */
    public class Context
    {
        private final BytesRef term = new BytesRef(meta.maxTermLength);
        private long pointId = -1;
    }

    public Context createContext()
    {
        return new Context();
    }

    /**
     * Look up the ByteComparable of a target point id
     *
     * @param target Point id to lookup
     * @param bytesInput Bytes input stream
     * @param context Reusable object for each call
     * @return ByteComparable at the target point id
     * @throws IOException
     */
    public ByteComparable seekToPointId(long target, IndexInput bytesInput, Context context) throws IOException
    {
        if (target < 0 || target >= meta.count)
        {
            throw new IndexOutOfBoundsException();
        }
        final long blockIndex = target >>> TERMS_DICT_BLOCK_SHIFT;
        final long blockAddress = offsets.get(blockIndex);
        bytesInput.seek(blockAddress);
        context.pointId = (blockIndex << TERMS_DICT_BLOCK_SHIFT) - 1;
        BytesRef term = null;
        do
        {
            term = next(bytesInput, context);
        } while (context.pointId < target);
        return fixedLength(term);
    }

    public static ByteComparable fixedLength(BytesRef bytes)
    {
        return ByteComparable.fixedLength(bytes.bytes, bytes.offset, bytes.length);
    }

    private BytesRef next(IndexInput bytesInput, Context context) throws IOException
    {
        if (++context.pointId >= meta.count)
        {
            return null;
        }
        if ((context.pointId & TERMS_DICT_BLOCK_MASK) == 0L)
        {
            context.term.length = bytesInput.readVInt();
            bytesInput.readBytes(context.term.bytes, 0, context.term.length);
        }
        else
        {
            final int token = Byte.toUnsignedInt(bytesInput.readByte());
            int prefixLength = token & 0x0F;
            int suffixLength = 1 + (token >>> 4);
            if (prefixLength == 15)
            {
                prefixLength += bytesInput.readVInt();
            }
            if (suffixLength == 16)
            {
                suffixLength += bytesInput.readVInt();
            }
            context.term.length = prefixLength + suffixLength;
            bytesInput.readBytes(context.term.bytes, prefixLength, suffixLength);
        }
        return context.term;
    }
}
