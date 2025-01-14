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
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.awaitility.Awaitility;

import static org.junit.Assert.assertEquals;

public class StageTimeMeasurementTest
{
    private static final Logger logger = LoggerFactory.getLogger(StageTimeMeasurementTest.class);

    public static final Stage TESTED_STAGE = Stage.READ;
    private static final int MAX_CONCURRENCY = 2;
    private static final long TASK_DURATION_NANOS = TimeUnit.MILLISECONDS.toNanos(100);
    static TestTaskExecutionCallback callback;

    @BeforeClass
    public static void setup()
    {
        CassandraRelevantProperties.CUSTOM_TASK_EXECUTION_CALLBACK_CLASS.setString(TestTaskExecutionCallback.class.getName());
        callback = (TestTaskExecutionCallback) TaskExecutionCallback.instance;
        DatabaseDescriptor.daemonInitialization();
        Stage.READ.setMaximumPoolSize(MAX_CONCURRENCY);

        // prime the stage, so that the first task doesn't have to wait for the stage to be initialized
        for (int i = 0; i < MAX_CONCURRENCY; i++)
        {
            TESTED_STAGE.execute(new LongRunnable());
        }
        Awaitility.await().until(() -> callback.executionTimes.size() == MAX_CONCURRENCY);
    }

    @Before
    public void reset()
    {
        callback.executionTimes.clear();
        callback.enqueuedTimes.clear();
    }

    @Test
    public void executionAndQueueTimeAreCountedOnExecute()
    {
        testExecutionAndQueueTimeAreCounted(TESTED_STAGE::execute);
    }

    @Test
    public void executionAndQueueTimeAreCountedOnExecuteWithLocals()
    {
        testExecutionAndQueueTimeAreCounted(r -> TESTED_STAGE.execute(r, null));
    }

    @Test
    public void executionAndQueueTimeAreCountedOnMaybeExecuteImmediately()
    {
        testExecutionAndQueueTimeAreCounted(TESTED_STAGE::maybeExecuteImmediately);
    }

    @Test
    public void executionAndQueueTimeAreCountedOnSubmit()
    {
        testExecutionAndQueueTimeAreCounted(TESTED_STAGE::submit);
    }

    @Test
    public void executionAndQueueTimeAreCountedOnSubmitWithResult()
    {
        testExecutionAndQueueTimeAreCounted(r -> TESTED_STAGE.submit(r, null));
    }

    @Test
    public void executionAndQueueTimeAreCountedOnSubmitCallable()
    {
        testExecutionAndQueueTimeAreCounted(r -> TESTED_STAGE.submit(() -> { r.run(); return null; }));
    }

    public void testExecutionAndQueueTimeAreCounted(Consumer<Runnable> runnableRunner)
    {
        int NUM_TASKS = 10;

        for (int i = 0; i < NUM_TASKS; i++)
        {
            ForkJoinPool.commonPool().execute(() -> runnableRunner.accept(new LongRunnable()));
        }

        Awaitility.await().until(() -> callback.executionTimes.size() == NUM_TASKS);

        logger.info("Completed tasks: {}", TESTED_STAGE.getCompletedTaskCount());
        logger.info("Execution times: {}", callback.executionTimes);
        logger.info("Queue times: {}", callback.enqueuedTimes);

        final double MAX_ACCEPTABLE_MEASUREMENT_ERROR = 0.1 * TASK_DURATION_NANOS;

        for (int i = 0; i < NUM_TASKS; i++)
        {
            // expect each task takes roughly TASK_DURATION_MS
            assertEquals(TASK_DURATION_NANOS, callback.executionTimes.get(i), MAX_ACCEPTABLE_MEASUREMENT_ERROR);
        }
        for (int i = 0; i < NUM_TASKS; i += MAX_CONCURRENCY)
        {
            // expect in each iteration tasks are enqueued for TASK_DURATION_NANOS more
            for (int concurrentTask = 0; concurrentTask < MAX_CONCURRENCY; concurrentTask++)
            {
                assertEquals((double) i / MAX_CONCURRENCY * TASK_DURATION_NANOS, callback.enqueuedTimes.get(i + concurrentTask), MAX_ACCEPTABLE_MEASUREMENT_ERROR);
            }
        }
    }

    public static class TestTaskExecutionCallback implements TaskExecutionCallback
    {
        private final List<Long> executionTimes = new CopyOnWriteArrayList<>();
        private final List<Long> enqueuedTimes = new CopyOnWriteArrayList<>();

        @Override
        public void onCompleted(Stage stage, long executionDurationNanos)
        {
            assertEquals(TESTED_STAGE, stage);
            executionTimes.add(executionDurationNanos);
        }

        @Override
        public void onDequeue(Stage stage, long enqueuedDurationNanos)
        {
            assertEquals(TESTED_STAGE, stage);
            enqueuedTimes.add(enqueuedDurationNanos);
        }
    }

    private static class LongRunnable implements Runnable
    {
        @Override
        public void run()
        {
            Uninterruptibles.sleepUninterruptibly(TASK_DURATION_NANOS, TimeUnit.NANOSECONDS);
        }
    }
}