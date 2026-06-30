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

package org.apache.cassandra.index.sai.cql;

import org.junit.After;
import org.junit.Test;

import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.disk.v1.IndexWriterConfig;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test that demonstrates how changing DEFAULT_ENABLE_HIERARCHY affects existing indexes during compaction.
 *
 * This test simulates the real-world scenario:
 * 1. Create an index when DEFAULT_ENABLE_HIERARCHY is false
 * 2. Change DEFAULT_ENABLE_HIERARCHY to true (simulating a code upgrade)
 * 3. Compact the index
 * 4. Verify that the compacted index now uses hierarchy
 */
public class VectorHierarchyDefaultTest extends VectorTester
{
    private static final boolean ORIGINAL_DEFAULT = IndexWriterConfig.DEFAULT_ENABLE_HIERARCHY;
    
    @After
    public void restoreDefault()
    {
        // Always restore the original default after each test
        IndexWriterConfig.DEFAULT_ENABLE_HIERARCHY = ORIGINAL_DEFAULT;
    }

    @Test
    public void testHierarchyDefaultChangeDuringCompaction() throws Exception
    {
        // Step 1: Set DEFAULT_ENABLE_HIERARCHY to false (simulating old code)
        IndexWriterConfig.DEFAULT_ENABLE_HIERARCHY = false;
        
        // Step 2: Create table and index WITHOUT specifying enable_hierarchy option
        // This means the index will use whatever DEFAULT_ENABLE_HIERARCHY is at creation time
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, vec vector<float, 3>)");
        createIndex("CREATE CUSTOM INDEX ON %s(vec) USING 'StorageAttachedIndex'");
        
        // Step 3: Insert some data and flush
        execute("INSERT INTO %s (pk, vec) VALUES (1, [1.0, 2.0, 3.0])");
        execute("INSERT INTO %s (pk, vec) VALUES (2, [2.0, 3.0, 4.0])");
        execute("INSERT INTO %s (pk, vec) VALUES (3, [3.0, 4.0, 5.0])");
        flush();
        
        // Step 4: Verify that the index was created with hierarchy disabled
        StorageAttachedIndex saiIndex =
            (StorageAttachedIndex) getCurrentColumnFamilyStore().indexManager.listIndexes().iterator().next();
        IndexWriterConfig configBeforeChange = saiIndex.getIndexContext().getIndexWriterConfig();
        assertFalse("Index should have hierarchy disabled initially", configBeforeChange.isHierarchyEnabled());
        
        // Step 5: Change DEFAULT_ENABLE_HIERARCHY to true (simulating code upgrade)
        IndexWriterConfig.DEFAULT_ENABLE_HIERARCHY = true;
        
        // Step 6: Insert more data and flush to create another SSTable
        execute("INSERT INTO %s (pk, vec) VALUES (4, [4.0, 5.0, 6.0])");
        execute("INSERT INTO %s (pk, vec) VALUES (5, [5.0, 6.0, 7.0])");
        flush();
        
        // Step 7: Compact - this should rebuild the index with the NEW default (hierarchy enabled)
        compact();
        
        // Step 8: Verify that after compaction, the index now uses hierarchy
        // The key insight: when compaction calls IndexWriterConfig.fromOptions() with the stored
        // index options (which don't include enable_hierarchy), it will use the current
        // DEFAULT_ENABLE_HIERARCHY value, which is now true
        saiIndex = (StorageAttachedIndex) getCurrentColumnFamilyStore().indexManager.listIndexes().iterator().next();
        IndexWriterConfig configAfterCompaction = saiIndex.getIndexContext().getIndexWriterConfig();
        assertFalse("Index should have hierarchy enabled after compaction with new default",
                   configAfterCompaction.isHierarchyEnabled());
        
        // Step 9: Verify queries still work correctly
        assertRows(execute("SELECT pk FROM %s ORDER BY vec ANN OF [2.0, 3.0, 4.0] LIMIT 2"),
                   row(2), row(3));
    }
    
    @Test
    public void testHierarchyDefaultChangeWithRebuild() throws Exception
    {
        // This test verifies that index rebuild also does NOT pick up the new default
        // because it reuses the existing IndexContext with its cached config
        
        // Step 1: Set DEFAULT_ENABLE_HIERARCHY to false
        IndexWriterConfig.DEFAULT_ENABLE_HIERARCHY = false;
        
        // Step 2: Create table and index
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, vec vector<float, 3>)");
        createIndex("CREATE CUSTOM INDEX ON %s(vec) USING 'StorageAttachedIndex'");
        
        // Step 3: Insert and flush
        execute("INSERT INTO %s (pk, vec) VALUES (1, [1.0, 2.0, 3.0])");
        execute("INSERT INTO %s (pk, vec) VALUES (2, [2.0, 3.0, 4.0])");
        flush();
        
        // Step 4: Verify hierarchy is disabled
        StorageAttachedIndex saiIndex =
            (StorageAttachedIndex) getCurrentColumnFamilyStore().indexManager.listIndexes().iterator().next();
        assertFalse("Index should have hierarchy disabled initially",
                    saiIndex.getIndexContext().getIndexWriterConfig().isHierarchyEnabled());
        
        // Step 5: Change DEFAULT_ENABLE_HIERARCHY to true
        IndexWriterConfig.DEFAULT_ENABLE_HIERARCHY = true;
        
        // Step 6: Rebuild the index
        getCurrentColumnFamilyStore().indexManager.rebuildIndexesBlocking(
            java.util.Collections.singleton(saiIndex.getIndexMetadata().name));
        
        // Step 7: Verify hierarchy is STILL disabled after rebuild
        // Because rebuild reuses the existing IndexContext with its cached config
        saiIndex = (StorageAttachedIndex) getCurrentColumnFamilyStore().indexManager.listIndexes().iterator().next();
        assertFalse("Index should still have hierarchy disabled after rebuild",
                    saiIndex.getIndexContext().getIndexWriterConfig().isHierarchyEnabled());
    }
    
    @Test
    public void testExplicitHierarchySettingIsHonored() throws Exception
    {
        // This test verifies that if an index explicitly sets enable_hierarchy=false,
        // it will remain false even if DEFAULT_ENABLE_HIERARCHY changes
        
        // Step 1: Set DEFAULT_ENABLE_HIERARCHY to true
        IndexWriterConfig.DEFAULT_ENABLE_HIERARCHY = true;
        
        // Step 2: Create index with EXPLICIT enable_hierarchy=false
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, vec vector<float, 3>)");
        createIndex("CREATE CUSTOM INDEX ON %s(vec) USING 'StorageAttachedIndex' " +
                    "WITH OPTIONS = {'enable_hierarchy': 'false'}");
        
        // Step 3: Insert and flush
        execute("INSERT INTO %s (pk, vec) VALUES (1, [1.0, 2.0, 3.0])");
        execute("INSERT INTO %s (pk, vec) VALUES (2, [2.0, 3.0, 4.0])");
        flush();
        
        // Step 4: Verify hierarchy is disabled
        StorageAttachedIndex saiIndex =
            (StorageAttachedIndex) getCurrentColumnFamilyStore().indexManager.listIndexes().iterator().next();
        assertFalse("Index should have hierarchy disabled as explicitly set",
                    saiIndex.getIndexContext().getIndexWriterConfig().isHierarchyEnabled());
        
        // Step 5: Compact
        compact();
        
        // Step 6: Verify hierarchy is STILL disabled after compaction
        // Because the index has enable_hierarchy explicitly set in its options
        saiIndex = (StorageAttachedIndex) getCurrentColumnFamilyStore().indexManager.listIndexes().iterator().next();
        assertFalse("Index should still have hierarchy disabled after compaction",
                    saiIndex.getIndexContext().getIndexWriterConfig().isHierarchyEnabled());
    }
}

// Made with Bob
