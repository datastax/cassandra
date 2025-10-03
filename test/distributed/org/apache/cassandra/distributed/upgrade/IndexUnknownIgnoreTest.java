/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.distributed.upgrade;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import static org.apache.cassandra.config.CassandraRelevantProperties.INDEX_UNKNOWN_IGNORE;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;

public class IndexUnknownIgnoreTest extends UpgradeTestBase
{

    private static final String SECONDARY_INDEX_MANAGER_ERROR = "Cannot find index type org.apache.cassandra.index.sasi.SASIIndex, but '"
                                                                + INDEX_UNKNOWN_IGNORE.getKey() + "' is true so creating noop index index_name";

    private static final String NOOP_INDEX_ERROR = "is enabled so using an noop index that will ignore writes";

    @Test
    public void testIndexUnknownIgnored() throws Throwable
    {
        try
        {
            setupClusterWithSasi()
                .runBeforeClusterUpgrade((cluster) -> INDEX_UNKNOWN_IGNORE.setBoolean(true))
                .runAfterClusterUpgrade((cluster) -> {
                    cluster.get(1).executeInternal("DESCRIBE INDEX " + KEYSPACE + ".index_name");
                    assertEquals(1, cluster.get(1).logs().grep(SECONDARY_INDEX_MANAGER_ERROR).getResult().size());
                    assertEquals(1, cluster.get(1).logs().grep(NOOP_INDEX_ERROR).getResult().size());
                    cluster.get(1).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 2, 3)");
                    cluster.get(1).executeInternal("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1 AND ck = 2");
                    cluster.get(1).executeInternal("SELECT * FROM " + KEYSPACE + ".tbl WHERE ck = 2 ALLOW FILTERING");
                })
                .run();
        }
        finally
        {
            INDEX_UNKNOWN_IGNORE.clearValue(); // checkstyle: suppress nearby 'clearValueSystemPropertyUsage'
        }
    }

    @Test
    public void testIndexUnknownNoopNeedsAllowFiltering() throws Throwable
    {
        try
        {
            setupClusterWithSasi()
                .runBeforeClusterUpgrade((cluster) -> INDEX_UNKNOWN_IGNORE.setBoolean(true))
                .runAfterClusterUpgrade((cluster) -> {
                    cluster.get(1).executeInternal("DESCRIBE INDEX " + KEYSPACE + ".index_name");
                    assertEquals(1, cluster.get(1).logs().grep(SECONDARY_INDEX_MANAGER_ERROR).getResult().size());
                    assertEquals(1, cluster.get(1).logs().grep(NOOP_INDEX_ERROR).getResult().size());
                    cluster.get(1).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 2, 3)");
                    cluster.get(1).executeInternal("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1 AND ck = 2");
                    cluster.get(1).executeInternal("SELECT * FROM " + KEYSPACE + ".tbl WHERE ck = 2 ALLOW FILTERING");
                    cluster.get(1).executeInternal("SELECT * FROM " + KEYSPACE + ".tbl WHERE ck = 2");
                })
                .run();
                fail("expected failure, noop index isn't queryable");
        }
        catch(AssertionError error)
        {
            if (!("InvalidRequestException".equals(error.getCause().getClass().getSimpleName())
                    && error.getCause().getMessage().contains(NOOP_INDEX_ERROR)))
                throw error;
        }
        finally
        {
            INDEX_UNKNOWN_IGNORE.clearValue(); // checkstyle: suppress nearby 'clearValueSystemPropertyUsage'
        }
    }

    @Test
    public void testIndexUnknownFails() throws Throwable
    {
        try
        {
            setupClusterWithSasi()
                .runAfterClusterUpgrade((cluster) -> {
                    cluster.get(1).executeInternal("DESCRIBE INDEX " + KEYSPACE + ".index_name");
                })
                .run();
            fail("expected failure, schema with SASI should not be readable in upgraded version");
        }
        catch(AssertionError error)
        {
            // expecting a RuntimeException -> ConfigurationException -> ClassNotFoundException
            if (!(error.getCause() instanceof RuntimeException
                    && "ConfigurationException".equals(error.getCause().getCause().getClass().getSimpleName())
                    && error.getCause().getCause().getCause() instanceof ClassNotFoundException))
                throw error;
        }
    }

    private static TestCase setupClusterWithSasi()
    {
        return new TestCase()
                .nodes(2)
                .nodesToUpgrade(1, 2)
                .upgradesToCurrentFrom(OLDEST)
                .withConfig(config -> config.with(GOSSIP, NETWORK).set("enable_sasi_indexes", true))
                .setup((cluster) -> {
                    cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");
                    cluster.schemaChange("CREATE CUSTOM INDEX index_name ON " + KEYSPACE + ".tbl (ck) USING 'org.apache.cassandra.index.sasi.SASIIndex'");
                    cluster.get(1).executeInternal("DESCRIBE INDEX " + KEYSPACE + ".index_name");
                    assertEquals(0, cluster.get(1).logs().grep(SECONDARY_INDEX_MANAGER_ERROR).getResult().size());
                    assertEquals(0, cluster.get(1).logs().grep(NOOP_INDEX_ERROR).getResult().size());
                });
    }
}
