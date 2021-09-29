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

package org.apache.cassandra.index.sai.disk.v2.blockindex;

import java.io.IOException;
import java.util.BitSet;

import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.disk.v2.postings.PostingsReader;
import org.apache.cassandra.index.sai.disk.v2.postings.PostingsWriter;
import org.apache.cassandra.index.sai.metrics.QueryEventListener;
import org.apache.cassandra.index.sai.utils.SharedIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.FixedBitSet;

public class BitSetSerializer
{
    public static FixedBitSet deserialize(long fp, IndexInput input) throws IOException
    {
        final int size = input.readVInt();
        final FixedBitSet bitSet = new FixedBitSet(size);
        final SharedIndexInput sharedInput = new SharedIndexInput(input);
        final PostingsReader postingsReader = new PostingsReader(sharedInput, fp, QueryEventListener.PostingListEventListener.NO_OP);
        while (true)
        {
            final long val = postingsReader.nextPosting();
            if (val == PostingList.END_OF_STREAM)
            {
                 break;
            }
            bitSet.set((int)val);
        }
        return bitSet;
    }

    public static long serialize(BitSet bitSet, IndexOutput out) throws IOException
    {
        out.writeVInt(bitSet.size());
        PostingsWriter writer = new PostingsWriter(out);
        BitSetPostingList postingList = new BitSetPostingList(bitSet);
        return writer.write(postingList);
    }

    public static class BitSetPostingList implements PostingList
    {
        final BitSet bitSet;
        private int idx = 0;

        public BitSetPostingList(BitSet bitSet)
        {
            this.bitSet = bitSet;
        }

        @Override
        public long nextPosting() throws IOException
        {
            int setBit = bitSet.nextSetBit(idx);
            if (setBit == -1)
            {
                return PostingList.END_OF_STREAM;
            }
            idx = setBit + 1;
            return setBit;
        }

        @Override
        public long size()
        {
            return bitSet.cardinality();
        }

        @Override
        public long advance(long targetRowId) throws IOException
        {
            throw new UnsupportedOperationException();
        }
    }
}
