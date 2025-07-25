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

import java.util.Arrays;

import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.UnifiedCompactionContainer;
import org.apache.cassandra.db.compaction.UnifiedCompactionStrategy;
import org.apache.cassandra.db.compaction.unified.AdaptiveController;
import org.apache.cassandra.db.compaction.unified.Controller;
import org.apache.cassandra.db.compaction.unified.StaticController;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.schema.TableMetadata;

import static org.apache.cassandra.SchemaLoader.standardCFMD;
import static org.apache.cassandra.distributed.shared.FutureUtils.waitOn;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CompactionControllerConfigTest extends TestBaseImpl
{

    private static final String quiteLongkeyspaceName = "g38373639353166362d356631322d343864652d393063362d653862616534343165333764_tpch";
    private static final String longTableName = "test_create_k8yq1r75bpzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz";

    @Test
    public void storedAdaptiveCompactionOptionsTest() throws Throwable
    {
        try(Cluster cluster = init(Cluster.build(1).start()))
        {
            cluster.schemaChange(withKeyspace("CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2};"));
            cluster.schemaChange(withKeyspace("CREATE TABLE ks.tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH compaction = " +
                                              "{'class': 'UnifiedCompactionStrategy', " +
                                              "'adaptive': 'true'};"));
            cluster.schemaChange(withKeyspace("CREATE TABLE ks.tbl2 (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH compaction = " +
                                              "{'class': 'UnifiedCompactionStrategy', " +
                                              "'adaptive': 'true'};"));
            cluster.get(1).runOnInstance(() ->
                                             {
                                                 ColumnFamilyStore cfs = Keyspace.open("ks").getColumnFamilyStore("tbl");
                                                 ColumnFamilyStore cfs2 = Keyspace.open("ks").getColumnFamilyStore("tbl2");
                                                 UnifiedCompactionContainer container = (UnifiedCompactionContainer) cfs.getCompactionStrategy();
                                                 UnifiedCompactionStrategy ucs = (UnifiedCompactionStrategy) container.getStrategies().get(0);
                                                 Controller controller = ucs.getController();
                                                 assertTrue(controller instanceof AdaptiveController);
                                                 //scaling parameter on L0 should be 0 to start
                                                 assertEquals(0, controller.getScalingParameter(0));

                                                 //manually write new scaling parameters and flushSizeBytes to see if they are picked up on restart
                                                 int[] scalingParameters = new int[32];
                                                 Arrays.fill(scalingParameters, 5);
                                                 AdaptiveController.storeOptions(cfs.metadata(), scalingParameters, 10 << 20);


                                                 //write different scaling parameters to second table to make sure each table keeps its own configuration
                                                 Arrays.fill(scalingParameters, 8);
                                                 AdaptiveController.storeOptions(cfs2.metadata(), scalingParameters, 10 << 20);
                                             });
            waitOn(cluster.get(1).shutdown());
            cluster.get(1).startup();

            cluster.get(1).runOnInstance(() ->
                                         {
                                             ColumnFamilyStore cfs = Keyspace.open("ks").getColumnFamilyStore("tbl");
                                             UnifiedCompactionContainer container = (UnifiedCompactionContainer) cfs.getCompactionStrategy();
                                             UnifiedCompactionStrategy ucs = (UnifiedCompactionStrategy) container.getStrategies().get(0);
                                             Controller controller = ucs.getController();
                                             assertTrue(controller instanceof AdaptiveController);
                                             //when the node is restarted, it should see the new configuration that was manually written
                                             assertEquals(5, controller.getScalingParameter(0));
                                             assertEquals(10 << 20, controller.getFlushSizeBytes());

                                             ColumnFamilyStore cfs2 = Keyspace.open("ks").getColumnFamilyStore("tbl2");
                                             UnifiedCompactionContainer container2 = (UnifiedCompactionContainer) cfs2.getCompactionStrategy();
                                             UnifiedCompactionStrategy ucs2 = (UnifiedCompactionStrategy) container2.getStrategies().get(0);
                                             Controller controller2 = ucs2.getController();
                                             assertTrue(controller2 instanceof AdaptiveController);
                                             //when the node is restarted, it should see the new configuration that was manually written
                                             assertEquals(8, controller2.getScalingParameter(0));
                                             assertEquals(10 << 20, controller2.getFlushSizeBytes());
                                         });
        }
    }

    @Test
    public void storedStaticCompactionOptionsTest() throws Throwable
    {
        try(Cluster cluster = init(Cluster.build(1).start()))
        {
            cluster.schemaChange(withKeyspace("CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2};"));
            cluster.schemaChange(withKeyspace("CREATE TABLE ks.tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH compaction = " +
                                              "{'class': 'UnifiedCompactionStrategy', " +
                                              "'adaptive': 'false', " +
                                              "'scaling_parameters': '0'};"));
            cluster.get(1).runOnInstance(() ->
                                         {
                                             ColumnFamilyStore cfs = Keyspace.open("ks").getColumnFamilyStore("tbl");
                                             UnifiedCompactionContainer container = (UnifiedCompactionContainer) cfs.getCompactionStrategy();
                                             UnifiedCompactionStrategy ucs = (UnifiedCompactionStrategy) container.getStrategies().get(0);
                                             Controller controller = ucs.getController();
                                             assertTrue(controller instanceof StaticController);
                                             //scaling parameter on L0 should be 0 to start
                                             assertEquals(0, controller.getScalingParameter(0));

                                             //manually write new flushSizeBytes to see if it is picked up on restart
                                             int[] scalingParameters = new int[32];
                                             Arrays.fill(scalingParameters, 0);
                                             AdaptiveController.storeOptions(cfs.metadata(), scalingParameters, 10 << 20);
                                         });
            waitOn(cluster.get(1).shutdown());
            cluster.get(1).startup();

            cluster.get(1).runOnInstance(() ->
                                         {
                                             ColumnFamilyStore cfs = Keyspace.open("ks").getColumnFamilyStore("tbl");
                                             UnifiedCompactionContainer container = (UnifiedCompactionContainer) cfs.getCompactionStrategy();
                                             UnifiedCompactionStrategy ucs = (UnifiedCompactionStrategy) container.getStrategies().get(0);
                                             Controller controller = ucs.getController();
                                             assertTrue(controller instanceof StaticController);
                                             //when the node is restarted, it should see the new configuration that was manually written
                                             assertEquals(10 << 20, controller.getFlushSizeBytes());
                                         });
        }
    }

    @Test
    public void testStoreAndCleanupControllerConfig() throws Throwable
    {
        try(Cluster cluster = init(Cluster.build(1).start()))
        {
            cluster.schemaChange(withKeyspace("CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2};"));
            cluster.schemaChange(withKeyspace("CREATE TABLE ks.tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH compaction = " +
                                              "{'class': 'UnifiedCompactionStrategy', " +
                                              "'adaptive': 'false', " +
                                              "'scaling_parameters': '0'};"));
            cluster.schemaChange(withKeyspace("CREATE KEYSPACE ks2 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2};"));
            cluster.schemaChange(withKeyspace("CREATE TABLE ks2.tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH compaction = " +
                                              "{'class': 'UnifiedCompactionStrategy', " +
                                              "'adaptive': 'false', " +
                                              "'scaling_parameters': '0'};"));
            cluster.schemaChange(withKeyspace("CREATE KEYSPACE "+quiteLongkeyspaceName+" WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2};"));
            cluster.schemaChange(withKeyspace("CREATE TABLE "+quiteLongkeyspaceName+"."+longTableName+" (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH compaction = " +
                                              "{'class': 'UnifiedCompactionStrategy', " +
                                              "'adaptive': 'false', " +
                                              "'scaling_parameters': '0'};"));


            cluster.get(1).runOnInstance(() ->
                                         {
                                             //logs should show that scaling parameters and flush size are written to a file for each table
                                             CompactionManager.storeControllerConfig();
                                             TableMetadata metadata = standardCFMD("does_not", "exist").build();

                                             //store controller config for a table that does not exist to see if it is removed by the cleanup method
                                             int[] scalingParameters = new int[32];
                                             Arrays.fill(scalingParameters, 5);

                                             AdaptiveController.storeOptions(metadata, scalingParameters, 10 << 20);

                                             //verify that the file was created
                                             assert Controller.getControllerConfigPath(metadata).exists();

                                             //cleanup method should remove the file corresponding to the table "does_not.exist"
                                             CompactionManager.cleanupControllerConfig();

                                             //verify that the file was deleted
                                             assert !Controller.getControllerConfigPath(metadata).exists();

                                             assertThat(Controller.getControllerConfigPath(ColumnFamilyStore.getIfExists(quiteLongkeyspaceName, longTableName).metadata()).toJavaIOFile()).exists();

                                         });

        }
    }

    @Test
    public void testStoreLongTableName() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(1).start()))
        {
            cluster.get(1).runOnInstance(() ->
                                         {
                                             CompactionManager.storeControllerConfig();

                                             // try to store controller config for a table with a long name
                                             TableMetadata metadata = standardCFMD(quiteLongkeyspaceName, longTableName).build();
                                             int[] scalingParameters = new int[32];
                                             Arrays.fill(scalingParameters, 5);
                                             AdaptiveController.storeOptions(metadata, scalingParameters, 10 << 20);

                                             // verify that the file WAS created (CNDB-12972)
                                             assert Controller.getControllerConfigPath(metadata).exists();

                                             CompactionManager.cleanupControllerConfig();

                                             assert !Controller.getControllerConfigPath(metadata).exists(); // table not really exists
                                         });
        }
    }

    @Test
    public void testVectorControllerConfig() throws Throwable
    {
        vectorControllerConfig(true);
        vectorControllerConfig(false);
    }


    public void vectorControllerConfig(boolean vectorOverride) throws Throwable
    {
        System.setProperty("unified_compaction.override_ucs_config_for_vector_tables", String.valueOf(vectorOverride));
        try(Cluster cluster = init(Cluster.build(1).start()))
        {
            cluster.schemaChange(withKeyspace("CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2};"));
            cluster.schemaChange(withKeyspace("CREATE TABLE ks.tbl (pk int, ck int, val vector<float, 2>, PRIMARY KEY (pk, ck)) WITH compaction = " +
                                              "{'class': 'UnifiedCompactionStrategy', " +
                                              "'adaptive': 'false', " +
                                              "'scaling_parameters': '0'};"));
            cluster.schemaChange(withKeyspace("CREATE TABLE ks.tbl2 (pk int, ck int, PRIMARY KEY (pk, ck)) WITH compaction = " +
                                              "{'class': 'UnifiedCompactionStrategy', " +
                                              "'adaptive': 'false', " +
                                              "'scaling_parameters': '0'};"));

            cluster.get(1).runOnInstance(() ->
                                         {
                                             ColumnFamilyStore cfs = Keyspace.open("ks").getColumnFamilyStore("tbl");
                                             UnifiedCompactionContainer container = (UnifiedCompactionContainer) cfs.getCompactionStrategy();
                                             UnifiedCompactionStrategy ucs = (UnifiedCompactionStrategy) container.getStrategies().get(0);
                                             Controller controller = ucs.getController();
                                             // ucs config should be set to the vector config
                                             assertEquals(vectorOverride ? Controller.DEFAULT_VECTOR_TARGET_SSTABLE_SIZE
                                                                         : Controller.DEFAULT_TARGET_SSTABLE_SIZE,
                                                          controller.getTargetSSTableSize());
                                             // but any property set in the table compaction config should override the vector config
                                             assertEquals(0, controller.getScalingParameter(0));

                                             ColumnFamilyStore cfs2 = Keyspace.open("ks").getColumnFamilyStore("tbl2");
                                             UnifiedCompactionContainer container2 = (UnifiedCompactionContainer) cfs2.getCompactionStrategy();
                                             UnifiedCompactionStrategy ucs2 = (UnifiedCompactionStrategy) container2.getStrategies().get(0);
                                             Controller controller2 = ucs2.getController();
                                             // since tbl2 does not have a vectorType the ucs config should not be set to the vector config
                                             assertEquals(Controller.DEFAULT_TARGET_SSTABLE_SIZE, controller2.getTargetSSTableSize());
                                             assertEquals(0, controller2.getScalingParameter(0));
                                         });
            cluster.schemaChange(withKeyspace("ALTER TABLE ks.tbl2 ADD val vector<float, 2>;"));
            cluster.get(1).runOnInstance(() ->
                                         {
                                             ColumnFamilyStore cfs2 = Keyspace.open("ks").getColumnFamilyStore("tbl2");
                                             UnifiedCompactionContainer container2 = (UnifiedCompactionContainer) cfs2.getCompactionStrategy();
                                             UnifiedCompactionStrategy ucs2 = (UnifiedCompactionStrategy) container2.getStrategies().get(0);
                                             Controller controller2 = ucs2.getController();
                                             // a vector was added to tbl2 so it should now have the vector config
                                             assertEquals(vectorOverride ? Controller.DEFAULT_VECTOR_TARGET_SSTABLE_SIZE
                                                                         : Controller.DEFAULT_TARGET_SSTABLE_SIZE,
                                                          controller2.getTargetSSTableSize());
                                             assertEquals(0, controller2.getScalingParameter(0));
                                         });
        }
    }
}
