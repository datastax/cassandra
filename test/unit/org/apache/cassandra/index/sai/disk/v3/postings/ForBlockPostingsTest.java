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

import com.carrotsearch.hppc.LongArrayList;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v3.postings.lucene8xpostings.ForUtil;
import org.apache.cassandra.index.sai.disk.v3.postings.lucene8xpostings.Lucene8xIndexInput;
import org.apache.cassandra.index.sai.disk.v3.postings.lucene8xpostings.LuceneMMap;
import org.apache.cassandra.index.sai.disk.v3.postings.lucene8xpostings.PForUtil;
import org.apache.cassandra.index.sai.utils.SaiRandomizedTest;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.lucene.store.IndexOutput;
import org.junit.Test;

import java.io.IOException;

public class ForBlockPostingsTest extends SaiRandomizedTest
{
    @Test
    public void testSeekToBlock() throws Exception
    {
        final IndexDescriptor indexDescriptor = newIndexDescriptor();
        final String index = newIndex();
        final IndexContext indexContext = SAITester.createIndexContext(index, UTF8Type.instance);

        long postingsFP = -1;
        long[] array = new long[1000];
        for (int x = 0; x < array.length; x++)
        {
            array[x] = x;
        }

        LongArrayList blockFPs = new LongArrayList();

        try (IndexOutput postingsOut = indexDescriptor.openPerIndexOutput(IndexComponent.POSTING_LISTS, indexContext))
        {
            ForBlockPostingsWriter postingsWriter = new ForBlockPostingsWriter();

            for (int x = 0; x < array.length; x++)
            {
                long fp = postingsWriter.add(x);
                if (fp != -1)
                {
                    blockFPs.add(fp);
                }
            }
            long finalBlockFP = postingsWriter.finish();
            if (finalBlockFP != -1)
            {
                blockFPs.add(finalBlockFP);
            }
            postingsFP = postingsWriter.complete(postingsOut);
        }

        try (FileHandle fileHandle = indexDescriptor.createPerIndexFileHandle(IndexComponent.POSTING_LISTS, indexContext);
             Lucene8xIndexInput input = LuceneMMap.openLuceneInput(fileHandle))
        {
            PForUtil forEncoder = new PForUtil(new ForUtil());

            // iterate on all rowids starting from each block
            // assert the row ids match the array row ids
            for (int blockIndex = 0; blockIndex < blockFPs.size(); blockIndex++)
            {
                ForBlockPostingsReader reader = new ForBlockPostingsReader(postingsFP, input, forEncoder);

                long blockFP = blockFPs.get(blockIndex);
                long prevBlockMax = 0;
                if (blockIndex > 0)
                {
                    prevBlockMax = array[blockIndex * 128 - 1];
                }
                reader.seekToBlock(prevBlockMax, blockIndex, blockFP);

                LongArrayList rowids = toList(reader);

                int i = 0;
                for (int x = blockIndex * 128; x < array.length; x++)
                {
                    assertEquals(rowids.get(i++), array[x]);
                }
            }
        }
    }

    public static LongArrayList toList(ForBlockPostingsReader reader) throws IOException
    {
        LongArrayList rowids = new LongArrayList();
        while (true)
        {
            long rowid = reader.nextPosting();
            if (rowid == PostingList.END_OF_STREAM)
            {
                break;
            }
            rowids.add(rowid);
        }
        return rowids;
    }

    @Test
    public void testNext() throws Exception
    {
        final IndexDescriptor indexDescriptor = newIndexDescriptor();
        final String index = newIndex();
        final IndexContext indexContext = SAITester.createIndexContext(index, UTF8Type.instance);

        long postingsFP = -1;
        long[] array = new long[1000];
        for (int x = 0; x < array.length; x++)
        {
            array[x] = x;
        }

        try (IndexOutput postingsOut = indexDescriptor.openPerIndexOutput(IndexComponent.POSTING_LISTS, indexContext))
        {
            ForBlockPostingsWriter postingsWriter = new ForBlockPostingsWriter();

            for (int x = 0; x < array.length; x++)
            {
                postingsWriter.add(x);
            }
            postingsWriter.finish();
            postingsFP = postingsWriter.complete(postingsOut);
        }

        try (FileHandle fileHandle = indexDescriptor.createPerIndexFileHandle(IndexComponent.POSTING_LISTS, indexContext);
             Lucene8xIndexInput input = LuceneMMap.openLuceneInput(fileHandle))
        {
            PForUtil forEncoder = new PForUtil(new ForUtil());
            ForBlockPostingsReader reader = new ForBlockPostingsReader(postingsFP, input, forEncoder);

            LongArrayList rowids = new LongArrayList();
            while (true)
            {
                long rowid = reader.nextPosting();
                if (rowid == PostingList.END_OF_STREAM)
                {
                    break;
                }
                rowids.add(rowid);
            }
            assertArrayEquals(array, rowids.toArray());
        }
    }
}
