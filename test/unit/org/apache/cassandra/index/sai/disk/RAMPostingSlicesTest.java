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
package org.apache.cassandra.index.sai.disk;


import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import org.apache.cassandra.index.sai.utils.SaiRandomizedTest;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.FixedBitSet;

public class RAMPostingSlicesTest extends SaiRandomizedTest
{
    @Test
    public void testRAMPostingSlices() throws Exception
    {
        RAMPostingSlices slices = new RAMPostingSlices(Counter.newCounter(), false);

        int[] segmentRowIdUpto = new int[1024];
        Arrays.fill(segmentRowIdUpto, -1);

        FixedBitSet[] bitSets = new FixedBitSet[segmentRowIdUpto.length];

        for (int x = 0; x < 1_000_000; x++)
        {
            int termID = nextInt(segmentRowIdUpto.length);

            if (segmentRowIdUpto[termID] == -1)
            {
                slices.createNewSlice(termID);
            }

            segmentRowIdUpto[termID]++;

            if (bitSets[termID] == null)
            {
                bitSets[termID] = new FixedBitSet(1_000_000);
            }

            bitSets[termID].set(segmentRowIdUpto[termID]);

            slices.writePosting(termID, segmentRowIdUpto[termID], 1);
        }

        for (int termID = 0; termID < segmentRowIdUpto.length; termID++)
        {
            ByteSliceReader reader = new ByteSliceReader();
            slices.initReader(reader, termID);

            int segmentRowId = -1;

            while (!reader.eof())
            {
                segmentRowId = reader.readVInt();
                assertTrue("termID=" + termID + " segmentRowId=" + segmentRowId, bitSets[termID].get(segmentRowId));
            }
            assertEquals(segmentRowId, segmentRowIdUpto[termID]);
        }
    }

    @Test
    public void testRAMPostingSlicesWithFrequencies() throws Exception {
        RAMPostingSlices slices = new RAMPostingSlices(Counter.newCounter(), true);

        // Test with just 3 terms and known frequencies
        for (int termId = 0; termId < 3; termId++) {
            slices.createNewSlice(termId);

            // Write a sequence of rows with different frequencies for each term
            slices.writePosting(termId, 5, 1);    // first posting at row 5
            slices.writePosting(termId, 3, 2);    // next at row 8 (delta=3)
            slices.writePosting(termId, 2, 3);    // next at row 10 (delta=2)
        }

        // Verify each term's postings
        for (int termId = 0; termId < 3; termId++) {
            ByteSliceReader reader = new ByteSliceReader();
            PostingList postings = slices.postingList(termId, reader, 10);

            assertEquals(5, postings.nextPosting());
            assertEquals(1, postings.frequency());

            assertEquals(8, postings.nextPosting());
            assertEquals(2, postings.frequency());

            assertEquals(10, postings.nextPosting());
            assertEquals(3, postings.frequency());

            assertEquals(PostingList.END_OF_STREAM, postings.nextPosting());
        }
    }
}
