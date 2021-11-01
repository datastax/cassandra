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

import org.apache.cassandra.index.sai.disk.io.IndexOutputWriter;
import org.apache.cassandra.io.tries.IncrementalDeepTrieWriterPageAware;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RAMOutputStream;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.packed.DirectMonotonicWriter;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedLongValues;


import static org.apache.cassandra.index.sai.disk.v1.trie.TrieTermsDictionaryReader.trieSerializer;

/**
 * Writes sorted bytes to be used by @see org.apache.cassandra.index.sai.disk.v2.sortedbytes.SortedBytesReader
 *
 * A trie for bytes -> point id lookup.
 *
 * A Lucene version 7.5 sorted doc values copy for point id -> bytes lookup.
 */
public class SortedBytesWriter
{
    static final int TERMS_DICT_BLOCK_SHIFT = 4;
    static final int TERMS_DICT_BLOCK_SIZE = 1 << TERMS_DICT_BLOCK_SHIFT;
    static final int TERMS_DICT_BLOCK_MASK = TERMS_DICT_BLOCK_SIZE - 1;

    private final IncrementalDeepTrieWriterPageAware trieWriter;
    private final IndexOutput bytesOutput;
    private final IndexOutput offsetsOutput;
    private final BytesRefBuilder previous = new BytesRefBuilder();
    private final BytesRefBuilder temp = new BytesRefBuilder();

    private final long bytesStartFP;
    static final int DIRECT_MONOTONIC_BLOCK_SHIFT = 16;
    private final PackedLongValues.Builder offsetsBuilder;

    private int maxLength = -1;
    private long pointId = 0;

    public SortedBytesWriter(IndexOutputWriter trieOutputWriter,
                             IndexOutput bytesOutput,
                             IndexOutput offsetsOutput)
    {
        this.trieWriter = new IncrementalDeepTrieWriterPageAware<>(trieSerializer, trieOutputWriter.asSequentialWriter());
        this.bytesOutput = bytesOutput;
        this.bytesStartFP = bytesOutput.getFilePointer();
        this.offsetsOutput = offsetsOutput;

        offsetsBuilder = PackedLongValues.deltaPackedBuilder(PackedInts.COMPACT);
    }

    /**
     * Add bytes to the trie and the sorted bytes disk structures.
     *
     * Must be in lexicographic order.
     *
     * @param termBC ByteComparable term
     * @throws IOException
     */
    public void add(final ByteComparable termBC) throws IOException
    {
        gatherBytes(termBC, temp);
        final BytesRef term = temp.get();

        if (previous.get().length > 0 && previous.get().compareTo(term) >= 0)
        {
            throw new IllegalArgumentException("Bytes must be added in lexicographic ascending order.");
        }

        trieWriter.add(termBC, new Long(pointId));

        if ((pointId & TERMS_DICT_BLOCK_MASK) == 0)
        {
            offsetsBuilder.add(bytesOutput.getFilePointer() - bytesStartFP);

            bytesOutput.writeVInt(term.length);
            bytesOutput.writeBytes(term.bytes, term.offset, term.length);
        }
        else
        {
            final int prefixLength = StringHelper.bytesDifference(previous.get(), term);
            final int suffixLength = term.length - prefixLength;
            assert suffixLength > 0; // terms are unique

            bytesOutput.writeByte((byte) (Math.min(prefixLength, 15) | (Math.min(15, suffixLength - 1) << 4)));
            if (prefixLength >= 15)
            {
                bytesOutput.writeVInt(prefixLength - 15);
            }
            if (suffixLength >= 16)
            {
                bytesOutput.writeVInt(suffixLength - 16);
            }
            bytesOutput.writeBytes(term.bytes, term.offset + prefixLength, term.length - prefixLength);
        }
        maxLength = Math.max(maxLength, term.length);
        previous.copyBytes(term);

        pointId++;
    }

    public static class Meta
    {
        public final long trieFP;
        public final long count;
        public final int maxTermLength;
        public final byte[] offsetMetaBytes;
        public final long offsetBlockCount;

        public Meta(long trieFP, long count, int maxTermLength, byte[] offsetMetaBytes, long offsetBlockCount)
        {
            this.trieFP = trieFP;
            this.count = count;
            this.maxTermLength = maxTermLength;
            this.offsetMetaBytes = offsetMetaBytes;
            this.offsetBlockCount = offsetBlockCount;
        }
    }

    public Meta finish() throws IOException
    {
        long offsetBlockCount = (pointId + TERMS_DICT_BLOCK_MASK) >>> TERMS_DICT_BLOCK_SHIFT;

        final PackedLongValues offsets = offsetsBuilder.build();

        RAMOutputStream meta = new RAMOutputStream();

        // DirectMonotonicReader is much faster than MonotonicBlockPackedReader
        DirectMonotonicWriter offsetsWriter = DirectMonotonicWriter.getInstance(meta, offsetsOutput, offsetBlockCount, DIRECT_MONOTONIC_BLOCK_SHIFT);
        for (int x = 0; x < offsetBlockCount; x++)
        {
            offsetsWriter.add(offsets.get(x));
        }
        offsetsWriter.finish();

        byte[] metaBytes = new byte[(int)meta.getFilePointer()];
        meta.writeTo(metaBytes, 0);

        final long trieFP = this.trieWriter.complete();
        return new Meta(trieFP, pointId, maxLength, metaBytes, offsetBlockCount);
    }

    public static int gatherBytes(ByteComparable term, BytesRefBuilder builder)
    {
        return gatherBytes(term.asComparableBytes(ByteComparable.Version.OSS41), builder);
    }

    public static int gatherBytes(ByteSource byteSource, BytesRefBuilder builder)
    {
        builder.clear();
        int length = 0;
        // gather the term bytes from the byteSource
        while (true)
        {
            final int val = byteSource.next();
            if (val != ByteSource.END_OF_STREAM)
            {
                ++length;
                builder.append((byte) val);
            }
            else
            {
                break;
            }
        }
        return length;
    }
}
