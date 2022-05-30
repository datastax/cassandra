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
package org.apache.cassandra.metrics;

import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.CounterMutation;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.schema.KeyspaceParams;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CounterMutationMetricsTest {
    private static final String KEYSPACE1 = "CounterMutationTest";
    private static final String CF1 = "Counter1";
    private static final String CF2 = "Counter2";

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.counterCFMD(KEYSPACE1, CF1),
                                    SchemaLoader.counterCFMD(KEYSPACE1, CF2));
    }

    @Test
    public void testLocksDuplicate()
    {
        AssertMetrics metrics = new AssertMetrics();

        ColumnFamilyStore cfsOne = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF1);
        ColumnFamilyStore cfsTwo = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF2);

        cfsOne.truncateBlocking();
        cfsTwo.truncateBlocking();

        Mutation.PartitionUpdateCollector batch = new Mutation.PartitionUpdateCollector(KEYSPACE1, Util.dk("key1"));
        batch.add(new RowUpdateBuilder(cfsOne.metadata(), 5, "key1")
                  .clustering("cc")
                  .add("val", 1L)
                  .add("val2", -1L)
                  .build().getPartitionUpdate(cfsOne.metadata()));

        batch.add(new RowUpdateBuilder(cfsTwo.metadata(), 5, "key1")
                  .clustering("cc")
                  .add("val", 2L)
                  .add("val2", -2L)
                  .build().getPartitionUpdate(cfsTwo.metadata()));

        new CounterMutation(batch.build(), ConsistencyLevel.ONE).apply();

        metrics.assertLockTimeout(0);
        metrics.assertLocksPerUpdate(1);
    }

    @Test
    public void testLockTimeout() throws Throwable
    {
        AssertMetrics metrics = new AssertMetrics(11L);

        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF1);
        cfs.truncateBlocking();

        // Obtain lock
        new CounterMutation(
        new RowUpdateBuilder(cfs.metadata(), 5, "key1").clustering("cc")
                                                       .add("val", 1L)
                                                       .build(),
        ConsistencyLevel.ONE).grabCounterLocks(Keyspace.open(KEYSPACE1), new ArrayList<>());

        metrics.assertLockTimeout(0);

        // Try to mutate
        CompletableFuture.runAsync(() ->
                                   {
                                       try
                                       {
                                           new CounterMutation(
                                           new RowUpdateBuilder(cfs.metadata(), 5, "key1")
                                           .clustering("cc")
                                           .add("val", 1L)
                                           .add("val2", -1L)
                                           .build(),
                                           ConsistencyLevel.ONE).apply();
                                       }
                                       catch (WriteTimeoutException e)
                                       {
                                           // Expected exception.
                                           // If not met, lockTimeout counter will not be increased, which is asserted.
                                       }
                                   }).get(100, TimeUnit.MILLISECONDS);

        metrics.assertLockTimeout(1);
        metrics.assertLocksPerUpdate(2);
    }

    public class AssertMetrics
    {
        public final long timeout;
        public final long prevLockTimeout;
        public final long prevMinLockAcquireTime;
        public final long prevMaxLockAcquireTime;

        public AssertMetrics()
        {
            this(DatabaseDescriptor.getCounterWriteRpcTimeout(TimeUnit.NANOSECONDS));
        }

        public void clear()
        {
            ((ClearableHistogram)CounterMutation.locksPerUpdate).clear();
        }

        public AssertMetrics(long timeout)
        {
            clear();
            prevLockTimeout = CounterMutation.lockTimeout.getCount();
            prevMinLockAcquireTime = CounterMutation.lockAcquireTime.latency.getSnapshot().getMin();
            prevMaxLockAcquireTime = CounterMutation.lockAcquireTime.latency.getSnapshot().getMax();

            this.timeout = timeout;
            DatabaseDescriptor.setCounterWriteRpcTimeout(timeout);
        }

        public void assertLockTimeout(long expected)
        {
            assertEquals(prevLockTimeout + expected, CounterMutation.lockTimeout.getCount());

            long maxLockAcquireTime = CounterMutation.lockAcquireTime.latency.getSnapshot().getMax();
            if (expected > 0)
                assertTrue(timeout <= maxLockAcquireTime);
            else
                assertTrue(timeout > maxLockAcquireTime
                           || prevMaxLockAcquireTime <= maxLockAcquireTime);
        }

        public void assertLocksPerUpdate(long expectedCount)
        {
            assertEquals(expectedCount, CounterMutation.locksPerUpdate.getCount());
        }
    }
}
