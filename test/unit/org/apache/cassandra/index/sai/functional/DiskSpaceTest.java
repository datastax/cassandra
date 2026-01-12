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
package org.apache.cassandra.index.sai.functional;

import org.junit.Test;

import org.apache.cassandra.index.sai.SAITester;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DiskSpaceTest extends SAITester
{
    @Test
    public void testTableTotalDiskSpaceUsed() throws Throwable
    {
        createTable(CREATE_TABLE_TEMPLATE);

        int rows = 1000;
        for (int j = 0; j < rows; j++)
        {
            execute("INSERT INTO %s (id1, v1) VALUES (?, 1)", Integer.toString(j));
        }
        flush();

        long sstableSize = totalDiskSpaceUsed();

        // create index, disk space should include index components
        String indexName = createIndex(String.format(CREATE_INDEX_TEMPLATE, "v1"));

        long indexSize = indexDiskSpaceUse();
        long sstableSizeWithIndex = totalDiskSpaceUsed();
        assertEquals(sstableSize + indexSize, sstableSizeWithIndex);
        verifyIndexComponentsIncludedInSSTable();

        // drop index, disk space should not include index, but SSTables still include index components
        dropIndex("DROP INDEX %s." + indexName);
        assertEquals(sstableSize, totalDiskSpaceUsed());
        verifyIndexComponentsNotIncludedInSSTable();
    }

    @Test
    public void testDiskUsageWithCompactionAfterUpdates() throws Throwable
    {
        createTable(CREATE_TABLE_TEMPLATE);

        // Insert initial data and flush to create first SSTable
        int rows = 100;
        for (int j = 0; j < rows; j++)
            execute("INSERT INTO %s (id1, v1) VALUES (?, ?)", Integer.toString(j), j);

        flush();
        
        assertEquals("Should have 1 SSTable after first flush", 1, getCurrentColumnFamilyStore().getLiveSSTables().size());
        long sstableSizeAfterFirstFlush = totalDiskSpaceUsed();

        // Update the same keys with different values to create overlapping data
        for (int j = 0; j < rows; j++)
            execute("INSERT INTO %s (id1, v1) VALUES (?, ?)", Integer.toString(j), j + 1000);

        flush();
        
        assertEquals("Should have 2 SSTables after second flush", 2, getCurrentColumnFamilyStore().getLiveSSTables().size());
        long sstableSizeBeforeCompaction = totalDiskSpaceUsed();
        
        // Verify that we have more disk usage with 2 SSTables containing overlapping data
        assertTrue("Disk usage should increase with overlapping SSTables",
                   sstableSizeBeforeCompaction > sstableSizeAfterFirstFlush);

        // Compact to merge the updates before creating index
        compact();
        waitForCompactionsFinished();
        
        assertEquals("Should have 1 SSTable after compaction", 1, getCurrentColumnFamilyStore().getLiveSSTables().size());
        long sstableSizeAfterCompaction = totalDiskSpaceUsed();
        
        // Compaction should reduce disk usage by merging duplicate keys
        assertTrue("Compaction should reduce disk usage by merging duplicates",
                   sstableSizeAfterCompaction < sstableSizeBeforeCompaction);

        // Create index on the compacted SSTable
        String indexName = createIndex(String.format(CREATE_INDEX_TEMPLATE, "v1"));

        long indexSize = indexDiskSpaceUse();
        long sstableSizeWithIndex = totalDiskSpaceUsed();
        
        assertTrue("Index size should be positive", indexSize > 0);
        
        // Verify that total disk usage equals base table plus index after compaction
        assertEquals("Total disk should equal base table plus index after compaction",
                     sstableSizeAfterCompaction + indexSize, sstableSizeWithIndex);
        verifyIndexComponentsIncludedInSSTable();

        // Drop index and verify disk space accounting
        dropIndex("DROP INDEX %s." + indexName);
        assertEquals("After dropping index, disk usage should equal base table only",
                     sstableSizeAfterCompaction, totalDiskSpaceUsed());
        verifyIndexComponentsNotIncludedInSSTable();
    }
}
