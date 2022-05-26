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
import org.apache.cassandra.index.sai.disk.v3.postings.lucene8xpostings.ForUtil;
import org.apache.cassandra.index.sai.disk.v3.postings.lucene8xpostings.Lucene8xIndexInput;
import org.apache.cassandra.index.sai.disk.v3.postings.lucene8xpostings.PForUtil;

import java.io.IOException;

public class PFoRPostingsReader implements OrdinalPostingList
{
    private final Lucene8xIndexInput input;
    private final PForUtil forEncoder = new PForUtil(new ForUtil());
    private final ForBlockPostingsReader postings;
    private final long startFP;
    private final int blockMaxRowidsBytes, blockFilePointersBytes;
    private long currentPosting = -1;
    private ForBlockPostingsReader blockMaxRowids, blockFilePointers; // lazy init'd
    private int lastBlockIdx = -1;

    public PFoRPostingsReader(long fp, Lucene8xIndexInput input) throws IOException
    {
        input.seek(fp);
        this.input = input;
        blockMaxRowidsBytes = input.readVInt();
        blockFilePointersBytes = input.readVInt();
        int postingsWriterBytes = input.readVInt();
        startFP = input.getFilePointer();
        postings = new ForBlockPostingsReader(startFP + blockMaxRowidsBytes + blockFilePointersBytes, input.clone(), forEncoder);
    }

    @Override
    public void close() throws IOException
    {
        input.close();
    }

    public long block()
    {
        return postings.block() - 1;
    }

    @Override
    public long getOrdinal()
    {
        return postings.getOrdinal();
    }

    public long currentPosting()
    {
        return currentPosting;
    }

    @Override
    public long nextPosting() throws IOException
    {
        return currentPosting = postings.nextPosting();
    }

    @Override
    public long size()
    {
        return postings.count;
    }

    @Override
    public long advance(long targetRowID) throws IOException
    {
        if (blockMaxRowids == null)
        {
            blockMaxRowids = new ForBlockPostingsReader(startFP, input.clone(), forEncoder);
            blockFilePointers = new ForBlockPostingsReader(startFP + blockMaxRowidsBytes, input.clone(), forEncoder);
            // init blockMaxRowids and blockFilePointers
            blockMaxRowids.nextPosting();
            blockFilePointers.nextPosting();
        }
        while (blockMaxRowids.currentPosting() != PostingList.END_OF_STREAM)
        {
            long peekMaxRowID = blockMaxRowids.currentPosting();
            if (targetRowID < peekMaxRowID)
            {
                break;
            }
            else
            {
                blockMaxRowids.nextPosting();
                blockFilePointers.nextPosting();
            }
        }
        long blockFP = blockFilePointers.previousPosting();
        long blockMaxRowID = blockMaxRowids.previousPosting();

        if (blockFilePointers.getOrdinal() != blockMaxRowids.getOrdinal())
        {
            throw new IllegalStateException();
        }

        // use blockMaxRowids ordinal as the block index up to
        int blockIndex = (int)blockMaxRowids.getOrdinal() - 1;

        // not in the same block anymore, seek to the correct block
        if (lastBlockIdx != blockIndex)
        {
            postings.seekToBlock(blockMaxRowID, blockIndex, blockFP);
        }
        lastBlockIdx = blockIndex;
        while (true)
        {
            long rowid = nextPosting();
            if (rowid >= targetRowID)
            {
                return rowid;
            }
        }
    }
}
