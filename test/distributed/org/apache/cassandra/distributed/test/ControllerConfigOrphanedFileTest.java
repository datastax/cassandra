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

package org.apache.cassandra.distributed.test;

import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.unified.Controller;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.schema.TableMetadata;

import static org.apache.cassandra.distributed.shared.FutureUtils.waitOn;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test to reproduce the bug where orphaned controller-config.JSON files
 * cause IllegalArgumentException during node restart.
 * 
 * The bug occurs when:
 * 1. A table with UCS compaction strategy is created
 * 2. Data is written and flushed (creates controller-config.JSON file)
 * 3. The UCS config file is saved
 * 4. Table is dropped (file is NOT deleted)
 * 5. Node is restarted (cleanupControllerConfig() throws IllegalArgumentException)
 */
public class ControllerConfigOrphanedFileTest extends TestBaseImpl
{
    @Test
    public void testOrphanedControllerConfigFileOnRestart() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(1).start()))
        {
            // create keyspace and table with UCS compaction strategy
            cluster.schemaChange(withKeyspace("CREATE KEYSPACE test_ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};"));
            cluster.schemaChange(withKeyspace("CREATE TABLE test_ks.test_table (pk int PRIMARY KEY, v int) WITH compaction = " +
                                              "{'class': 'UnifiedCompactionStrategy', 'adaptive': 'true'};"));

            // insert data and flush to trigger controller-config.JSON creation
            for (int i = 0; i < 100; i++)
            {
                cluster.coordinator(1).execute(withKeyspace("INSERT INTO test_ks.test_table (pk, v) VALUES (?, ?)"),
                                               ConsistencyLevel.ONE, i, i);
            }
            
            cluster.get(1).flush(KEYSPACE);


            // store the controller-config.JSON file and verify it exists
            cluster.get(1).runOnInstance(() -> {
                CompactionManager.storeControllerConfig();
                TableMetadata metadata = TableMetadata.minimal("test_ks", "test_table");
                assertTrue("Controller config file should exist after flush",
                           Controller.getControllerConfigPath(metadata).exists());
            });

            // drop the table - this should delete the controller-config.JSON file but currently doesn't
            cluster.schemaChange(withKeyspace("DROP TABLE test_ks.test_table;"));

            // verify the orphaned file still exists
            cluster.get(1).runOnInstance(() -> {
                // This assertion will pass, showing the file is orphaned
                TableMetadata metadata = TableMetadata.minimal("test_ks", "test_table");
                assertTrue("Controller config file is orphaned after table drop",
                           Controller.getControllerConfigPath(metadata).exists());
            });

            // when
            // stopping the node
            waitOn(cluster.get(1).shutdown());

            // then
            // starting the node again should succeed
            cluster.get(1).startup();
            // after restart, the orphaned file should be cleaned up
            cluster.get(1).runOnInstance(() -> {
                TableMetadata metadata = TableMetadata.minimal("test_ks", "test_table");
                assertFalse("Controller config file should be deleted after restart cleanup",
                            Controller.getControllerConfigPath(metadata).exists());
            });
        }
    }
}
