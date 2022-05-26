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

package org.apache.cassandra.index.sai.disk.v3.postings;

import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.disk.v1.postings.OrdinalPostingList;
import org.apache.cassandra.index.sai.disk.v3.postings.lucene8xpostings.Lucene8xIndexInput;
import org.apache.cassandra.index.sai.disk.v3.postings.lucene8xpostings.PForUtil;

import java.io.IOException;

import static org.apache.cassandra.index.sai.disk.v3.postings.ForBlockPostingsWriter.BLOCK_SIZE;

public class ForBlockPostingsReader implements OrdinalPostingList
{
    private final PForUtil forEncoder;
    private final long[] rowidBuffer = new long[BLOCK_SIZE];
    private final Lucene8xIndexInput input;
    public final long count;
    private final long startFP;
    private long index = 0;
    private int blockIndex = 0;
    private long lastRowID = 0;
    private long prevLastRowID = -1;
    private long block;

    public ForBlockPostingsReader(long fp,
                                  Lucene8xIndexInput input,
                                  PForUtil forEncoder) throws IOException
    {
        input.seek(fp);
        this.input = input;
        this.forEncoder = forEncoder;
        count = input.readInt();
        startFP = input.getFilePointer();
    }

    public long block()
    {
        return block;
    }

    public long previousPosting()
    {
        return prevLastRowID;
    }

    public long currentPosting()
    {
        return lastRowID;
    }

    @Override
    public long getOrdinal()
    {
        return index;
    }

    @Override
    public long size()
    {
        return count;
    }

    @Override
    public long advance(long targetRowID) throws IOException
    {
        throw new UnsupportedOperationException();
    }

    public void seekToBlock(long prevBlockMax, int block, long fp) throws IOException
    {
        this.block = block;
        index = block * BLOCK_SIZE;
        input.seek(startFP + fp);
        lastRowID = prevBlockMax;
    }

    public long nextPosting() throws IOException
    {
        try
        {
            if (index == count)
            {
                prevLastRowID = lastRowID;
                return lastRowID = PostingList.END_OF_STREAM;
            }
            if (index % BLOCK_SIZE == 0)
            {
                forEncoder.decode(input, rowidBuffer);
                blockIndex = 0;
                block++;
            }
            prevLastRowID = lastRowID;
            return lastRowID = rowidBuffer[blockIndex] + lastRowID;
        }
        finally
        {
            index++;
            blockIndex++;
        }
    }
}
