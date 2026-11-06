/*
 * Copyright IBM Corp.
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

package org.apache.cassandra.concurrent;

import java.util.Set;
import java.util.TreeSet;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.hints.HintsService;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;
import org.apache.cassandra.metrics.ThreadPoolMetrics;

import static org.apache.cassandra.config.CassandraRelevantProperties.EXECUTOR_LIVENESS_WATCHDOG_EXCLUDED_POOLS;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Every pool the watchdog excludes by default must be one it would otherwise watch, so that a renamed pool does not
 * silently become watched while its stale name stays excluded.
 */
public class ExecutorLivenessWatchdogExclusionsTest extends CQLTester
{
    @Test
    public void testDefaultExclusionsAreRegisteredPools()
    {
        // the compaction manager's pools, the hints dispatcher, and, from the first table's index manager, the index
        // management pool
        assertNotNull(CompactionManager.instance);
        assertNotNull(HintsService.instance);
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");

        Set<String> registered = new TreeSet<>();
        for (ThreadPoolMetrics metrics : CassandraMetricsRegistry.Metrics.allThreadPoolMetrics())
            registered.add(metrics.poolName);
        for (ExecutorLivenessWatchdog.Pool pool : ExecutorLivenessWatchdog.registeredPools())
            registered.add(pool.name);

        for (String name : EXECUTOR_LIVENESS_WATCHDOG_EXCLUDED_POOLS.getDefaultValue().split(","))
        {
            if (name.endsWith("*"))
            {
                String prefix = name.substring(0, name.length() - 1);
                assertTrue(name + " matches none of " + registered, registered.stream().anyMatch(n -> n.startsWith(prefix)));
            }
            else
            {
                assertTrue(name + " is not one of " + registered, registered.contains(name));
            }
        }
    }
}
