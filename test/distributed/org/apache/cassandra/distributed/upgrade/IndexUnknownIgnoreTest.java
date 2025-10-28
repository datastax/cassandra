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

package org.apache.cassandra.distributed.upgrade;

import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.shared.DistributedTestBase.KEYSPACE;

public class IndexUnknownIgnoreTest extends UpgradeTestBase
{
    @Test
    public void testIndexUnknownIgnored() throws Throwable
    {
        try
        {
            new TestCase()
                .nodes(2)
                .nodesToUpgrade(1, 2)
                .upgradesToCurrentFrom(OLDEST)
                .withConfig(config -> config.with(GOSSIP, NETWORK).set("enable_sasi_indexes", true))
                .setup((cluster) -> {
                    cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");
                    cluster.schemaChange("CREATE CUSTOM INDEX index_name ON " + KEYSPACE + ".tbl (v) USING 'org.apache.cassandra.index.sasi.SASIIndex'");
                    cluster.get(1).executeInternal("DESCRIBE INDEX " + KEYSPACE + ".index_name");
                })
                .runBeforeClusterUpgrade((cluster) -> System.setProperty("cassandra.index.unknown_custom_class.ignore", "true"))
                .runAfterClusterUpgrade((cluster) -> {
                    cluster.get(1).executeInternal("DESCRIBE INDEX " + KEYSPACE + ".index_name");
                })
                .run();
        }
        finally
        {
            System.clearProperty("cassandra.index.unknown_custom_class.ignore");
        }
    }

    @Test
    public void testIndexUnknownFails() throws Throwable
    {
        try
        {
            new TestCase()
                .nodes(2)
                .nodesToUpgrade(1, 2)
                .upgradesToCurrentFrom(OLDEST)
                .withConfig(config -> config.with(GOSSIP, NETWORK).set("enable_sasi_indexes", true))
                .setup((cluster) -> {
                    cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");
                    cluster.schemaChange("CREATE CUSTOM INDEX index_name ON " + KEYSPACE + ".tbl (v) USING 'org.apache.cassandra.index.sasi.SASIIndex'");
                    cluster.get(1).executeInternal("DESCRIBE INDEX " + KEYSPACE + ".index_name");
                })
                .runAfterClusterUpgrade((cluster) -> {
                        cluster.get(1).executeInternal("DESCRIBE INDEX " + KEYSPACE + ".index_name");
                })
                .run();
            throw new RuntimeException("expected failure, schema with SASI should not be readable in upgraded version");
        }
        catch(AssertionError error)
        {
            assertTrue(error.getCause() instanceof RuntimeException);
            assertTrue("ConfigurationException".equals(error.getCause().getCause().getClass().getSimpleName()));
            assertTrue(error.getCause().getCause().getCause() instanceof ClassNotFoundException);
        }
    }
}
