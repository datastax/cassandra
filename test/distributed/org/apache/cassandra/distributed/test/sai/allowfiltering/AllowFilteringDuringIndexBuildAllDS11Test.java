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

package org.apache.cassandra.distributed.test.sai.allowfiltering;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.net.MessagingService;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;

/**
 * {@link AllowFilteringDuringIndexBuildTester} for clusters with all nodes on mesaging version DS11.
 */
public class AllowFilteringDuringIndexBuildAllDS11Test extends AllowFilteringDuringIndexBuildTester
{
    @BeforeClass
    public static void setupCluster() throws Exception
    {
        CassandraRelevantProperties.DS_CURRENT_MESSAGING_VERSION.setInt(MessagingService.VERSION_DS_11);

        cluster = init(Cluster.build(NUM_REPLICAS)
                              .withConfig(config -> config.with(GOSSIP).with(NETWORK))
                              .start(), RF);
    }

    @Test
    public void testAllowFilteringDuringInitialIndexBuildWithAllDS11()
    {
        testSelectWithAllowFilteringDuringIndexBuilding(null, true, false);
    }

    @Test
    public void testAllowFilteringDuringIndexRebuildWithAllDS11NewTable()
    {
        testSelectWithAllowFilteringDuringIndexBuilding(INDEX_NOT_AVAILABLE_MESSAGE, false, true);
    }

    @Test
    public void testAllowFilteringDuringIndexRebuildWithAllDS11ExistingTable()
    {
        testSelectWithAllowFilteringDuringIndexBuilding(INDEX_NOT_AVAILABLE_MESSAGE, false, false);
    }
}
