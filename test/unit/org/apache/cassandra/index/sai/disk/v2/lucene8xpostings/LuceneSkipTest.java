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

import org.apache.cassandra.index.sai.disk.io.IndexComponents;
import org.apache.cassandra.index.sai.utils.NdiRandomizedTest;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.lucene.store.IndexOutput;

import static org.apache.cassandra.index.sai.disk.v2.lucene8xpostings.LuceneSkipWriter.MAX_SKIP_LEVELS;

public class LuceneSkipTest extends NdiRandomizedTest
{
    // TODO: add postings file pointers
    @Test
    public void test() throws Exception
    {
        IndexComponents comps = newIndexComponents();

        int blockSize = 128;
        int df = (128 * 10);

        long skipFP = -1;
        try (IndexOutput output = comps.createOutput(comps.termsData);
             IndexOutput postingsOut = comps.createOutput(comps.postingLists))
        {
            LuceneSkipWriter skipWriter = new LuceneSkipWriter(blockSize,
                                                               8,
                                                               MAX_SKIP_LEVELS,
                                                               df,
                                                               postingsOut);

            for (int doc = 128; doc < df; doc += 128)
            {
                System.out.println("bufferSkip doc="+doc);
                skipWriter.bufferSkip(doc, doc);
                byte[] fake = new byte[] {99, 100};
                postingsOut.writeBytes(fake, 0, fake.length);
            }
            skipFP = skipWriter.writeSkip(output);
        }

        try (FileHandle fileHandle = comps.createFileHandle(comps.termsData);
             Lucene8xIndexInput input = LuceneMMap.openLuceneInput(fileHandle);
             LuceneSkipReader reader = new LuceneSkipReader(input, MAX_SKIP_LEVELS, blockSize))
        {
            reader.init(skipFP, df);

            long skipDoc = reader.skipTo(500) + 1;

            System.out.println("skipDoc=" + skipDoc+" lastDocPointer="+reader.getDocPointer());
        }
    }
}
