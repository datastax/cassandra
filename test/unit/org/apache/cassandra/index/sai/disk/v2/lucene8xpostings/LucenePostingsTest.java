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

package org.apache.cassandra.index.sai.disk.v2.lucene8xpostings;

import org.junit.Test;

import com.carrotsearch.hppc.IntArrayList;
import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.disk.io.IndexComponents;
import org.apache.cassandra.index.sai.utils.ArrayPostingList;
import org.apache.cassandra.index.sai.utils.NdiRandomizedTest;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.lucene.store.IndexOutput;

public class LucenePostingsTest extends NdiRandomizedTest
{
    @Test
    public void testAdvance() throws Exception
    {
        IndexComponents comps = newIndexComponents();

        int blockSize = 128;

        long postingsFP = -1;
        long postingsFP2 = -1;
        int[] array = new int[2000];
        for (int x = 0; x < array.length; x++)
        {
            array[x] = x;
        }
        try (IndexOutput postingsOut = comps.createOutput(comps.postingLists))
        {
            LucenePostingsWriter postingsWriter = new LucenePostingsWriter(postingsOut, blockSize, array.length);

            postingsFP = postingsWriter.write(new ArrayPostingList(array));
            postingsFP2 = postingsWriter.write(new ArrayPostingList(array));
        }

        try (FileHandle fileHandle = comps.createFileHandle(comps.postingLists);
             Lucene8xIndexInput input = LuceneMMap.openLuceneInput(fileHandle);
             LucenePostingsReader reader = new LucenePostingsReader(input,
                                                                    blockSize,
                                                                    postingsFP))
        {
            long target = 0;

            while (true)
            {
                if (target >= array.length) break;

                long result = reader.advance(target);

                assertEquals(result, target);

                target += nextInt(1, 1000);
            }
        }

        try (FileHandle fileHandle = comps.createFileHandle(comps.postingLists);
             Lucene8xIndexInput input = LuceneMMap.openLuceneInput(fileHandle);
             LucenePostingsReader reader = new LucenePostingsReader(input,
                                                                    blockSize,
                                                                    postingsFP2))
        {
            long target = 0;

            while (true)
            {
                if (target >= array.length) break;

                long result = reader.advance(target);

                assertEquals(result, target);

                target += nextInt(1, 1000);
            }
        }
    }

    @Test
    public void testNext() throws Exception
    {
        IndexComponents comps = newIndexComponents();

        int blockSize = 128;

        long postingsFP = -1;
        long postingsFP2 = -1;
        int[] array = new int[1000];
        for (int x = 0; x < array.length; x++)
        {
            array[x] = x;
        }

        int[] array2 = new int[1000];
        for (int x = 0; x < array2.length; x++)
        {
            array2[x] = x * 1000;
        }
        try (IndexOutput postingsOut = comps.createOutput(comps.postingLists))
        {
            LucenePostingsWriter postingsWriter = new LucenePostingsWriter(postingsOut, blockSize, array.length);

            postingsFP = postingsWriter.write(new ArrayPostingList(array));
            // write 2 postings lists to test writing multiple postings using one writer instance
            postingsFP2 = postingsWriter.write(new ArrayPostingList(array2));
        }

        try (FileHandle fileHandle = comps.createFileHandle(comps.postingLists);
             Lucene8xIndexInput input = LuceneMMap.openLuceneInput(fileHandle);
             LucenePostingsReader reader = new LucenePostingsReader(input,
                                                                    blockSize,
                                                                    postingsFP))
        {
            IntArrayList rowids = new IntArrayList();
            while (true)
            {
                long rowid = reader.nextPosting();
                if (rowid == PostingList.END_OF_STREAM)
                {
                    break;
                }
                rowids.add((int)rowid);
            }

            assertArrayEquals(array, rowids.toArray());
        }

        try (FileHandle fileHandle = comps.createFileHandle(comps.postingLists);
             Lucene8xIndexInput input = LuceneMMap.openLuceneInput(fileHandle);
             LucenePostingsReader reader = new LucenePostingsReader(input,
                                                                    blockSize,
                                                                    postingsFP2))
        {
            IntArrayList rowids = new IntArrayList();
            while (true)
            {
                long rowid = reader.nextPosting();
                if (rowid == PostingList.END_OF_STREAM)
                {
                    break;
                }
                rowids.add((int)rowid);
            }

            assertArrayEquals(array2, rowids.toArray());
        }
    }
}
