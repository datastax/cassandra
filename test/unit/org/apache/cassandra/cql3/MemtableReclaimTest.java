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

package org.apache.cassandra.cql3;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.memtable.AbstractAllocatorMemtable;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;

import static org.junit.Assert.assertEquals;

@RunWith(BMUnitRunner.class)
@BMRule(name = "count discards",
targetClass = "AbstractAllocatorMemtable",
targetMethod = "discard",
action = "org.apache.cassandra.cql3.MemtableReclaimTest.discards.getAndIncrement()")
public class MemtableReclaimTest extends CQLTester
{
    public static AtomicInteger discards = new AtomicInteger(0);

    @BeforeClass
    public static void setThreads()
    {
        CassandraRelevantProperties.MEMTABLE_RECLAIM_THREADS.setInt(3);
    }

    @Test
    public void testIndependentReclaim() throws InterruptedException
    {
        String keyspace = createKeyspace("CREATE KEYSPACE %s with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 } and durable_writes = false");
        String table1 = createTable(keyspace, "CREATE TABLE %s ( userid bigint, picid bigint, commentid bigint, PRIMARY KEY(userid, picid))");
        String table2 = createTable(keyspace, "CREATE TABLE %s ( userid bigint, picid bigint, commentid bigint, PRIMARY KEY(userid, picid))");
        execute("use " + keyspace + ';');

        for (long i = 0; i < 100; ++i)
        {
            for (long j = 0; j < 100; ++j)
            {
                execute("INSERT INTO " + table1 + "(userid,picid,commentid)VALUES(?,?,?)", i, j, i + j);
                execute("INSERT INTO " + table2 + "(userid,picid,commentid)VALUES(?,?,?)", i, j, i - j);
            }
        }

        Memtable memtable1 = getColumnFamilyStore(keyspace, table1).getCurrentMemtable();
        Memtable.MemoryUsage usage1 = Memtable.getMemoryUsage(memtable1);
        Memtable memtable2 = getColumnFamilyStore(keyspace, table2).getCurrentMemtable();
        Memtable.MemoryUsage usage2 = Memtable.getMemoryUsage(memtable2);
        long usedOnHeap = AbstractAllocatorMemtable.MEMORY_POOL.onHeap.used();
        long usedOffHeap = AbstractAllocatorMemtable.MEMORY_POOL.offHeap.used();
        System.out.println(table1 + "\n" + usage1);
        System.out.println(table2 + "\n" + usage2);
        System.out.println("Total on " + FBUtilities.prettyPrintMemory(usedOnHeap) + " off " + FBUtilities.prettyPrintMemory(usedOffHeap));

        try (OpOrder.Group stopReclaim = memtable1.readOrdering().start())
        {
            flush(keyspace, table1);
            flush(keyspace, table2);
            Thread.sleep(100);
            System.out.println("discards: " + discards.get());

            long usedOnHeapAfter = AbstractAllocatorMemtable.MEMORY_POOL.onHeap.used();
            long usedOffHeapAfter = AbstractAllocatorMemtable.MEMORY_POOL.offHeap.used();
            System.out.println("Total on " + FBUtilities.prettyPrintMemory(usedOnHeapAfter) + " off " + FBUtilities.prettyPrintMemory(usedOffHeapAfter));
            assertEquals(usedOffHeap - usage2.ownsOffHeap, usedOffHeapAfter, 100000.0);
            assertEquals(usedOnHeap - usage2.ownsOnHeap, usedOnHeapAfter, 100000.0);
        }
        Thread.sleep(100);
        System.out.println("discards: " + discards.get());

        long usedOnHeapAfter = AbstractAllocatorMemtable.MEMORY_POOL.onHeap.used();
        long usedOffHeapAfter = AbstractAllocatorMemtable.MEMORY_POOL.offHeap.used();
        System.out.println("Total on " + FBUtilities.prettyPrintMemory(usedOnHeapAfter) + " off " + FBUtilities.prettyPrintMemory(usedOffHeapAfter));
        assertEquals(usedOffHeap - usage2.ownsOffHeap - usage1.ownsOffHeap, usedOffHeapAfter, 100000.0);
        assertEquals(usedOnHeap - usage2.ownsOnHeap - usage1.ownsOnHeap, usedOnHeapAfter, 100000.0);
    }

}
