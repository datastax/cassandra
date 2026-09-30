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

import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;

import static org.apache.cassandra.utils.MonotonicClock.approxTime;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class TimedRunnableTest
{
    @Test
    public void runsDelegateAndReportsClassAndStamp()
    {
        AtomicBoolean ran = new AtomicBoolean();
        Runnable delegate = new Runnable() { public void run() { ran.set(true); } };
        long before = approxTime.now();
        TimedRunnable timed = new TimedRunnable(delegate);
        timed.run();
        assertTrue(ran.get());
        assertSame(delegate.getClass(), timed.taskClass());
        assertTrue(timed.enqueuedAtNanos() >= before);
        assertTrue(timed.enqueuedAtNanos() <= approxTime.now());
    }

    @Test
    public void reportsGivenClass()
    {
        TimedRunnable timed = new TimedRunnable(() -> {}, String.class);
        assertSame(String.class, timed.taskClass());
    }

    @Test
    public void ageClampsAtZero()
    {
        assertEquals(0L, TimedTask.ageNanos(100L, 50L));
        assertEquals(25L, TimedTask.ageNanos(25L, 50L));
    }

    @Test
    public void defaultInterfaceMethodsReportIdle()
    {
        ResizableThreadPool pool = new ResizableThreadPool()
        {
            public int getCorePoolSize() { return 0; }
            public void setCorePoolSize(int n) {}
            public int getMaximumPoolSize() { return 0; }
            public void setMaximumPoolSize(int n) {}
        };
        assertEquals(0L, pool.getOldestQueuedTaskAgeMs());
        assertEquals(0L, pool.getLongestRunningTaskAgeMs());
        assertNull(pool.getLongestRunningTaskClass());
    }
}
