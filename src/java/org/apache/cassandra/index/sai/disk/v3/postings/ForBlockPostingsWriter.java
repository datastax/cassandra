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

import org.apache.cassandra.index.sai.disk.io.RAMIndexOutput;
import org.apache.cassandra.index.sai.disk.v3.postings.lucene8xpostings.ForUtil;
import org.apache.cassandra.index.sai.disk.v3.postings.lucene8xpostings.PForUtil;
import org.apache.lucene.store.IndexOutput;

import java.io.IOException;

public class ForBlockPostingsWriter
{
    public static final int BLOCK_SIZE = 128;
    private final PForUtil pforEncoder = new PForUtil(new ForUtil());
    private final long[] rowidBuffer = new long[BLOCK_SIZE];
    private final long[] rowidDeltaBuffer = new long[BLOCK_SIZE];
    private final RAMIndexOutput postingTemp = new RAMIndexOutput("");
    private int rowidBufferIdx = 0;
    private int blockIdx = 0;
    private long lastRowid = 0;
    private long count = 0;

    private boolean reset = true;

    public long add(long rowid) throws IOException
    {
        if (reset)
        {
            postingTemp.reset();
            reset = false;
        }
        rowidDeltaBuffer[rowidBufferIdx] = rowid - lastRowid;
        rowidBuffer[rowidBufferIdx] = rowid;
        rowidBufferIdx++;
        lastRowid = rowid;

        long flushFP = -1;

        if (rowidBufferIdx == BLOCK_SIZE)
        {
            flushFP = flushBlock();
        }
        count++;
        return flushFP;
    }

    public int numBytes()
    {
        return (int)postingTemp.getFilePointer() + 4;
    }

    public long finish() throws IOException
    {
        if (rowidBufferIdx > 0)
        {
            return flushBlock();
        }
        return -1;
    }

    public long complete(IndexOutput out) throws IOException
    {
        long fp = out.getFilePointer();
        out.writeInt((int)count);
        postingTemp.writeTo(out);

        // reset for the next posting list
        rowidBufferIdx = 0;
        count = 0;
        blockIdx = 0;
        reset = true;
        return fp;
    }

    private long flushBlock() throws IOException
    {
        long fp = postingTemp.getFilePointer();
        pforEncoder.encode(rowidDeltaBuffer, postingTemp);
        rowidBufferIdx = 0;
        blockIdx++;
        return fp;
    }
}
