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

import org.apache.lucene.store.IndexOutput;

import java.io.IOException;

public class PFoRPostingsWriter
{
    private final ForBlockPostingsWriter blockMaxRowids = new ForBlockPostingsWriter();
    private final ForBlockPostingsWriter blockFilePointers = new ForBlockPostingsWriter();
    private final ForBlockPostingsWriter postingsWriter = new ForBlockPostingsWriter();

    public PFoRPostingsWriter()
    {
    }

    public void add(long rowid) throws IOException
    {
        long flushFP = postingsWriter.add(rowid);
        // when a new postings block is flushed, record the block max row id and block file pointer
        if (flushFP != -1)
        {
            blockMaxRowids.add(rowid);
            blockFilePointers.add(flushFP);
        }
    }

    public long finish(IndexOutput out) throws IOException
    {
        // call finish so numBytes is accurate
        // flushes the last block if there is one
        blockMaxRowids.finish();
        blockFilePointers.finish();
        postingsWriter.finish();

        long fp = out.getFilePointer();

        out.writeVInt(blockMaxRowids.numBytes());
        out.writeVInt(blockFilePointers.numBytes());
        out.writeVInt(postingsWriter.numBytes());

        blockMaxRowids.complete(out);
        blockFilePointers.complete(out);
        postingsWriter.complete(out);
        return fp;
    }
}
