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

package org.apache.cassandra.concurrent;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CustomExecutorTest
{
    @BeforeClass
    public static void beforeClass()
    {
        CassandraRelevantProperties.CUSTOM_STAGE_EXECUTOR_FACTORY_PROPERTY.setString(CustomExecutorFactory.class.getName());
        DatabaseDescriptor.daemonInitialization();
    }

    private static final HashMap<String, LocalAwareExecutorService> executors = new HashMap<>();

    @Test
    public void testCustomExecutor()
    {
        Arrays.stream(Stage.values()).forEach(stage -> stage.execute(() -> {}));

        assertTrue(executors.get(Stage.MUTATION.jmxName) instanceof CustomExecutor);
        assertEquals(Stage.MUTATION.jmxName, ((CustomExecutor) executors.get(Stage.MUTATION.jmxName)).name);

        assertTrue(executors.get(Stage.READ.jmxName) instanceof CustomExecutor);
        assertEquals(Stage.READ.jmxName, ((CustomExecutor) executors.get(Stage.READ.jmxName)).name);

        assertTrue(executors.get(Stage.COUNTER_MUTATION.jmxName) instanceof CustomExecutor);
        assertEquals(Stage.COUNTER_MUTATION.jmxName, ((CustomExecutor) executors.get(Stage.COUNTER_MUTATION.jmxName)).name);

        assertTrue(executors.get(Stage.VIEW_MUTATION.jmxName) instanceof CustomExecutor);
        assertEquals(Stage.VIEW_MUTATION.jmxName, ((CustomExecutor) executors.get(Stage.VIEW_MUTATION.jmxName)).name);

        assertTrue(executors.get(Stage.REQUEST_RESPONSE.jmxName) instanceof CustomExecutor);
        assertEquals(Stage.REQUEST_RESPONSE.jmxName, ((CustomExecutor) executors.get(Stage.REQUEST_RESPONSE.jmxName)).name);

        assertTrue(executors.get(Stage.NATIVE_TRANSPORT_REQUESTS.jmxName) instanceof CustomExecutor);
        assertEquals(Stage.NATIVE_TRANSPORT_REQUESTS.jmxName, ((CustomExecutor) executors.get(Stage.NATIVE_TRANSPORT_REQUESTS.jmxName)).name);
    }

    public static class CustomExecutorFactory implements StageExecutorFactory
    {
        @Override
        public LocalAwareExecutorService init(String jmxName, String jmxType, int numThreads, LocalAwareExecutorService.MaximumPoolSizeListener onSetMaximumPoolSize)
        {
            return executors.computeIfAbsent(jmxName, k -> new CustomExecutor(jmxName));
        }
    }

    public static class CustomExecutor implements LocalAwareExecutorService
    {
        private final String name;

        public CustomExecutor(String name)
        {
            this.name = name;
        }

        @Override
        public void execute(Runnable command, ExecutorLocals locals)
        {

        }

        @Override
        public void maybeExecuteImmediately(Runnable command)
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

        @Override
        public void shutdown()
        {

        }

        @Override
        public List<Runnable> shutdownNow()
        {
            return List.of();
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
            return false;
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
        public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException
        {
            return List.of();
        }

        @Override
        public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException
        {
            return List.of();
        }

        @Override
        public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException
        {
            return null;
        }

        @Override
        public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException
        {
            return null;
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
    }
}
