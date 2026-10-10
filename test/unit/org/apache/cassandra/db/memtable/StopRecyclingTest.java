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

package org.apache.cassandra.db.memtable;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;

import static org.junit.Assert.assertTrue;

/**
 * Test that verifies stopRecycling is called when a memtable is flushed.
 */
@RunWith(BMUnitRunner.class)
public class StopRecyclingTest extends CQLTester
{
    public static AtomicInteger stopRecyclingCalls = new AtomicInteger(0);

    public static void countStopRecycling()
    {
        stopRecyclingCalls.getAndIncrement();
    }

    @Test
    @BMRule(name = "Count stopRecycling calls",
            targetClass = "InMemoryBaseTrie",
            targetMethod = "stopRecycling",
            action = "org.apache.cassandra.db.memtable.StopRecyclingTest.countStopRecycling()")
    public void testStopRecyclingCalledOnFlush() throws Throwable
    {
        // Skip test if not using TrieMemtable
        String keyspace = createKeyspace("CREATE KEYSPACE %s WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }");
        String table = createTable(keyspace, "CREATE TABLE %s (pk int PRIMARY KEY, value int)");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore(keyspace);
        Assume.assumeTrue("Test only applies to TrieMemtable", cfs.getCurrentMemtable() instanceof TrieMemtable);

        // Reset counter
        stopRecyclingCalls.set(0);

        // Insert data to populate the memtable
        for (int i = 0; i < 1000; i++)
        {
            execute("INSERT INTO " + keyspace + "." + table + " (pk, value) VALUES (?, ?)", i, i);
        }

        // Get the initial count
        int initialCount = stopRecyclingCalls.get();

        // Flush the memtable
        flush(keyspace, table);

        // Verify that stopRecycling was called
        // The number of calls depends on the number of shards in the memtable
        int finalCount = stopRecyclingCalls.get();
        assertTrue("stopRecycling should have been called at least once during flush", 
                   finalCount > initialCount);
        
        System.out.println("stopRecycling was called " + (finalCount - initialCount) + " times during flush");
    }
}
