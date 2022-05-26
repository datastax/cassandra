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
import org.apache.cassandra.index.sai.disk.v1.postings.PostingsReader;
import org.apache.cassandra.index.sai.disk.v1.postings.PostingsWriter;
import org.apache.cassandra.index.sai.disk.v3.postings.lucene8xpostings.Lucene8xIndexInput;
import org.apache.cassandra.index.sai.disk.v3.postings.lucene8xpostings.LuceneMMap;
import org.apache.cassandra.index.sai.metrics.QueryEventListener;
import org.apache.cassandra.index.sai.utils.ArrayPostingList;
import org.apache.cassandra.index.sai.utils.SaiRandomizedTest;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.junit.Test;

import java.io.IOException;

public class PFoRPostingsTest extends SaiRandomizedTest
{
    @Test
    public void testNext() throws Exception {
        PFoRPostingsWriter writer = new PFoRPostingsWriter();

        long[] array = new long[1000];
        for (int x = 0; x < array.length; x++)
        {
            array[x] = x;
            writer.add(x);
        }

        long fp = -1;
        final IndexDescriptor indexDescriptor = newIndexDescriptor();
        final String index = newIndex();
        final IndexContext indexContext = SAITester.createIndexContext(index, UTF8Type.instance);
        try (IndexOutput postingsOut = indexDescriptor.openPerIndexOutput(IndexComponent.POSTING_LISTS, indexContext))
        {
            fp = writer.finish(postingsOut);
        }

        try (FileHandle fileHandle = indexDescriptor.createPerIndexFileHandle(IndexComponent.POSTING_LISTS, indexContext);
             Lucene8xIndexInput input = LuceneMMap.openLuceneInput(fileHandle))
        {
            PFoRPostingsReader reader = new PFoRPostingsReader(fp, input);
            LongArrayList list = toList(reader);
            // System.out.println("list="+list);
            assertArrayEquals(array, list.toArray());
        }
    }

    @Test
    public void testRandomAdvance() throws Exception
    {
        final IndexDescriptor indexDescriptor = newIndexDescriptor();
        final String index = newIndex();
        final IndexContext indexContext = SAITester.createIndexContext(index, UTF8Type.instance);

        final PFoRPostingsWriter writer = new PFoRPostingsWriter();

        int[] array = new int[nextInt(1, 10_000)];
        int rowid = -1;
        for (int x = 0; x < array.length; x++)
        {
            rowid = nextInt(rowid + 1, rowid + 10);
            array[x] = rowid;
            writer.add(rowid);
        }

        long fp = -1;
        long fp2 = -1;

        try (IndexOutput postingsOut = indexDescriptor.openPerIndexOutput(IndexComponent.KD_TREE_POSTING_LISTS, indexContext))
        {
            fp = writer.finish(postingsOut);
        }

        try (final PostingsWriter postingsWriter = new PostingsWriter(indexDescriptor, indexContext, false))
        {
            fp2 = postingsWriter.write(new ArrayPostingList(array));
        }

        long advRowid = -1;
        try (FileHandle fileHandle = indexDescriptor.createPerIndexFileHandle(IndexComponent.KD_TREE_POSTING_LISTS, indexContext);
             PFoRPostingsReader reader = new PFoRPostingsReader(fp, LuceneMMap.openLuceneInput(fileHandle));
             IndexInput input2 = indexDescriptor.openPerIndexInput(IndexComponent.POSTING_LISTS, indexContext);
             PostingsReader reader2 = new PostingsReader(input2, fp2, QueryEventListener.PostingListEventListener.NO_OP))
        {
            advRowid += nextLong(1, 10);

            if (advRowid > array[array.length - 1])
            {
                long adv = reader.advance(advRowid);
                long adv2 = reader2.advance(advRowid);

                long ordinal = reader.getOrdinal();
                long ordinal2 = reader2.getOrdinal();

                assertEquals(adv2, adv);
                assertEquals(ordinal2, ordinal);
            }
        }
    }

    @Test
    public void testRandomNext() throws Exception
    {
        final PFoRPostingsWriter writer = new PFoRPostingsWriter();

        long[] array = new long[1000];
        long rowid = -1;
        for (int x = 0; x < array.length; x++)
        {
            rowid = nextLong(rowid + 1, rowid + 100);
            array[x] = rowid;
            writer.add(rowid);
        }

        long fp = -1;
        final IndexDescriptor indexDescriptor = newIndexDescriptor();
        final String index = newIndex();
        final IndexContext indexContext = SAITester.createIndexContext(index, UTF8Type.instance);
        try (IndexOutput postingsOut = indexDescriptor.openPerIndexOutput(IndexComponent.POSTING_LISTS, indexContext))
        {
            fp = writer.finish(postingsOut);
        }

        try (FileHandle fileHandle = indexDescriptor.createPerIndexFileHandle(IndexComponent.POSTING_LISTS, indexContext);
             Lucene8xIndexInput input = LuceneMMap.openLuceneInput(fileHandle))
        {
            PFoRPostingsReader reader = new PFoRPostingsReader(fp, input);
            LongArrayList list = toList(reader);
            System.out.println("list=" + list);
            assertArrayEquals(array, list.toArray());
        }
    }

    public static LongArrayList toList(PostingList reader) throws IOException
    {
        LongArrayList rowids = new LongArrayList();
        while (true) {
            long rowid = reader.nextPosting();
            if (rowid == PostingList.END_OF_STREAM)
            {
                break;
            }
            rowids.add(rowid);
        }
        return rowids;
    }
}
