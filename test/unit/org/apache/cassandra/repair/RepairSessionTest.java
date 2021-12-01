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

package org.apache.cassandra.repair;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Sets;
import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.utils.WithResources;
import org.apache.cassandra.utils.concurrent.Future;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.config.CassandraRelevantProperties.NODES_DISABLE_PERSISTING_TO_SYSTEM_KEYSPACE;
import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class RepairSessionTest
{
    @BeforeClass
    public static void initDD()
    {
        NODES_DISABLE_PERSISTING_TO_SYSTEM_KEYSPACE.setBoolean(true);
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testConviction() throws Exception
    {
        InetAddressAndPort remote = InetAddressAndPort.getByName("127.0.0.2");
        Gossiper.instance.initializeNodeUnsafe(remote, UUID.randomUUID(), 1);

        // Set up RepairSession
        TimeUUID parentSessionId = nextTimeUUID();
        IPartitioner p = Murmur3Partitioner.instance;
        Range<Token> repairRange = new Range<>(p.getToken(ByteBufferUtil.bytes(0)), p.getToken(ByteBufferUtil.bytes(100)));
        Set<InetAddressAndPort> endpoints = Sets.newHashSet(remote);
        RepairSession session = new RepairSession(SharedContext.Global.instance, new Scheduler.NoopScheduler(), parentSessionId,
                                                  new CommonRange(endpoints, Collections.emptySet(), Arrays.asList(repairRange)),
                                                  "Keyspace1", RepairParallelism.SEQUENTIAL,
                                                  false, false,
                                                  PreviewKind.NONE, false, false, false, "Standard1");

        // perform convict
        session.convict(remote, Double.MAX_VALUE);

        // RepairSession should throw ExecutorException with the cause of IOException when getting its value
        assertSessionFails(session);
    }

    private void assertSessionFails(RepairSession session) throws InterruptedException
    {
        try
        {
            session.get();
            fail();
        }
        catch (ExecutionException ex)
        {
            assertEquals(IOException.class, ex.getCause().getClass());
        }
    }

    private static class NoopExecutorService implements ExecutorPlus
    {
        @Override
        public void shutdown()
        {
        }

        @Override
        public List<Runnable> shutdownNow()
        {
            return null;
        }

        @Override
        public boolean isShutdown()
        {
            return false;
        }

        @Override
        public boolean isTerminated()
        {
            return false;
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> Future<T> submit(Callable<T> task)
        {
            return null;
        }

        @Override
        public <T> Future<T> submit(Runnable task, T result)
        {
            return null;
        }

        @Override
        public Future<?> submit(Runnable task)
        {
            return null;
        }

        @Override
        public void execute(WithResources withResources, Runnable task)
        {
        }

        @Override
        public <T> Future<T> submit(WithResources withResources, Callable<T> task)
        {
            return null;
        }

        @Override
        public Future<?> submit(WithResources withResources, Runnable task)
        {
            return null;
        }

        @Override
        public <T> Future<T> submit(WithResources withResources, Runnable task, T result)
        {
            return null;
        }

        @Override
        public boolean inExecutor()
        {
            return false;
        }

        @Override
        public void execute(Runnable command)
        {
        }

        @Override
        public int getCorePoolSize()
        {
            return 0;
        }

        @Override
        public void setCorePoolSize(int newCorePoolSize)
        {
        }

        @Override
        public int getMaximumPoolSize()
        {
            return 0;
        }

        @Override
        public void setMaximumPoolSize(int newMaximumPoolSize)
        {
        }

        @Override
        public int getActiveTaskCount()
        {
            return 0;
        }

        @Override
        public long getCompletedTaskCount()
        {
            return 0;
        }

        @Override
        public int getPendingTaskCount()
        {
            return 0;
        }
    }

    @Test
    public void testRepairingDeadNodeFails() throws Exception
    {
        InetAddressAndPort remote = InetAddressAndPort.getByName("127.0.0.2");
        Gossiper.instance.initializeNodeUnsafe(remote, UUID.randomUUID(), 1);
        // Mark remote as dead
        Gossiper.instance.convict(remote, Double.MAX_VALUE);

        // Set up RepairSession
        TimeUUID parentSessionId = TimeUUID.Generator.nextTimeUUID();
        IPartitioner p = Murmur3Partitioner.instance;
        Range<Token> repairRange = new Range<>(p.getToken(ByteBufferUtil.bytes(0)), p.getToken(ByteBufferUtil.bytes(100)));
        Set<InetAddressAndPort> endpoints = Sets.newHashSet(remote);
        SharedContext.Global ctx = SharedContext.Global.instance;
        RepairSession session = new RepairSession(ctx, new Scheduler.NoopScheduler(), parentSessionId,
                                                  new CommonRange(endpoints, Collections.emptySet(), Arrays.asList(repairRange)),
                                                  "Keyspace1", RepairParallelism.SEQUENTIAL,
                                                  false, false,
                                                  PreviewKind.NONE, false,
                                                  false, false, "Standard1");

        NoopExecutorService executor = new NoopExecutorService();
        session.start(executor);

        // RepairSession should fail when trying to repair a dead node
        assertSessionFails(session);
    }
}
