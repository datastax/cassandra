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

package org.apache.cassandra.transport;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.concurrent.DebuggableTask;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.config.DatabaseDescriptor;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.cassandra.utils.MonotonicClock.Global.preciseTime;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Native-transport queue backpressure ({@link Dispatcher#hasQueueCapacity()}) looks only at submitted requests on the
 * request executor, measured from their creation; other tasks at the head of that queue do not count.
 */
public class DispatcherQueueCapacityTest
{
    private static final long TIMEOUT_MILLIS = 50;

    private int maxThreads;
    private long timeoutMillis;
    private double threshold;
    private Blocker blocker;

    @BeforeClass
    public static void setupDD()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Before
    public void blockRequestExecutor() throws InterruptedException
    {
        maxThreads = Stage.NATIVE_TRANSPORT_REQUESTS.getMaximumPoolSize();
        timeoutMillis = DatabaseDescriptor.getNativeTransportTimeout(MILLISECONDS);
        threshold = DatabaseDescriptor.getNativeTransportQueueMaxItemAgeThreshold();
        DatabaseDescriptor.setNativeTransportTimeout(TIMEOUT_MILLIS, MILLISECONDS);
        DatabaseDescriptor.getRawConfig().native_transport_queue_max_item_age_threshold = 1.0;

        Stage.NATIVE_TRANSPORT_REQUESTS.setMaximumPoolSize(1);
        blocker = new Blocker();
        Dispatcher.requestExecutor.execute(blocker);
        assertTrue(blocker.started.await(10, TimeUnit.SECONDS));
    }

    @After
    public void restore()
    {
        blocker.release.countDown();
        Util.spinAssertEquals(0, Dispatcher.requestExecutor::getPendingTaskCount, 10);
        Stage.NATIVE_TRANSPORT_REQUESTS.setMaximumPoolSize(maxThreads);
        DatabaseDescriptor.setNativeTransportTimeout(timeoutMillis, MILLISECONDS);
        DatabaseDescriptor.getRawConfig().native_transport_queue_max_item_age_threshold = threshold;
    }

    private static final class Blocker implements Runnable
    {
        final CountDownLatch started = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);

        public void run()
        {
            started.countDown();
            try { release.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }
    }

    // a submitted request, as Dispatcher.RequestProcessor
    private static final class Request implements DebuggableTask.RunnableDebuggableTask
    {
        final long createdAtNanos;

        Request(long createdAtNanos)
        {
            this.createdAtNanos = createdAtNanos;
        }

        public void run() {}
        public long creationTimeNanos() { return createdAtNanos; }
        public long startTimeNanos() { return 0; }
        public String description() { return "request"; }
    }

    @Test
    public void testStaleRequestAtHeadAppliesBackpressure()
    {
        Dispatcher dispatcher = new Dispatcher(false);
        Dispatcher.requestExecutor.submit(new Request(preciseTime.now() - TimeUnit.SECONDS.toNanos(10)));
        assertFalse(dispatcher.hasQueueCapacity());
    }

    @Test
    public void testFreshRequestAtHeadHasCapacity()
    {
        Dispatcher dispatcher = new Dispatcher(false);
        Dispatcher.requestExecutor.submit(new Request(preciseTime.now()));
        assertTrue(dispatcher.hasQueueCapacity());
    }

    @Test
    public void testOtherTaskAtHeadHasCapacity() throws InterruptedException
    {
        Dispatcher dispatcher = new Dispatcher(false);
        // queued well past the timeout, but not a request, as submitted to this stage by other code
        Stage.NATIVE_TRANSPORT_REQUESTS.submit(() -> {});
        Thread.sleep(TIMEOUT_MILLIS * 3);
        assertTrue(dispatcher.hasQueueCapacity());
    }
}
