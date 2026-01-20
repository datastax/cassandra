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

package org.apache.cassandra.index.sai.disk.vector;

import java.util.List;

import org.junit.Test;

import io.github.jbellis.jvector.util.RamUsageEstimator;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class CompactionGraphTest
{

    @Test
    public void test10Entries() throws Exception
    {
        // 10 entries means we hit the limit sooner
        testEntries(10, 1000, 1);
    }

    @Test
    public void test1MEntries() throws Exception
    {
        // more entries means it takes longer, but we hit this bug in prod before, so it is worth testing
        testEntries(1000000, 5000, 100);
    }

    @Test
    public void test50MEntries() throws Exception
    {
        // more entries means it takes longer, but we hit this bug in prod before, so it is worth testing
        testEntries(50000000, 5000, 100);
    }

    // Callers of this method are expected to provide enough iterations and postings added per iteration
    // to hit the entry size limit without exceeding it too much. Note that we add postings one at a time in the
    // compaction graph, so we only ever increment by 4 bytes each time we attempt to re-serialize the entry.
    private void testEntries(int entries, int iterations, int postingsAddedPerIteration) throws Exception
    {
        File postingsFile = FileUtils.createTempFile("testfile", "tmp");
        try(var postingsMap = ChronicleMapBuilder.of(Integer.class, (Class<VectorPostings.CompactionVectorPostings>) (Class) VectorPostings.CompactionVectorPostings.class)
                                         .averageValueSize(VectorPostings.emptyBytesUsed() + RamUsageEstimator.NUM_BYTES_OBJECT_REF + 2 * Integer.BYTES)
                                         .valueMarshaller(new VectorPostings.Marshaller())
                                         .entries(entries)
                                         .createPersistedTo(postingsFile.toJavaIOFile()))
        {
            postingsMap.put(0, new VectorPostings.CompactionVectorPostings(0, List.of()));
            int rowId = 0;
            boolean recoveredFromFailure = false;
            for (int i = 0; i < iterations; i++)
            {
                // Iterate so we can fail sooner
                var existing = postingsMap.get(0);
                for (int j = 0; j < postingsAddedPerIteration; j++)
                    existing.add(rowId++);

                recoveredFromFailure = CompactionGraph.safePut(postingsMap, 0, existing);
                if (recoveredFromFailure)
                    break;
            }

            assertTrue("Failed to hit entry size limit", recoveredFromFailure);
            // Validate that we can read (deserialize) the entry
            var existing = postingsMap.get(0);
            assertNotNull(existing);
            assertEquals(rowId, existing.size());
            // Validate that the row ids are correct
            var rowIds = existing.getRowIds();
            for (int i = 0; i < rowId; i++)
                assertEquals(i, (int) rowIds.get(i));
        }
    }
}
