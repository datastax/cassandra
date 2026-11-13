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

import java.lang.reflect.Field;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;

/**
 * Constructing an executor must not initialise the WorkerSlots thread-local, which initialises netty's
 * InternalThreadLocalMap and its logger (the in-JVM dtest Instance builds an executor before it sets the node's log
 * identity). Kept apart from WorkerSlotsTest, and to a single test, so no earlier task has initialised it in this JVM.
 */
public class WorkerSlotsLazyInitTest
{
    @Test
    public void testConstructionDoesNotInitialiseThreadLocal() throws Throwable
    {
        Class<?> holder = Class.forName(WorkerSlots.class.getName() + "$Holder", false, WorkerSlots.class.getClassLoader());
        Assert.assertFalse("initialised before the test", isInitialised(holder));

        ExecutorPlus pooled = executorFactory().sequential("lazy-init");
        ScheduledExecutorPlus scheduled = executorFactory().scheduled("lazy-init-scheduled");
        try
        {
            Assert.assertFalse(isInitialised(holder));

            // the check does see the first task initialise it
            pooled.submit(() -> {}).get(10, TimeUnit.SECONDS);
            Assert.assertTrue(isInitialised(holder));
        }
        finally
        {
            pooled.shutdownNow();
            scheduled.shutdownNow();
        }
    }

    // reflectively, as shouldBeInitialized is deprecated on later JDKs; there is no public way to ask
    private static boolean isInitialised(Class<?> c) throws Exception
    {
        Class<?> unsafeClass = Class.forName("sun.misc.Unsafe");
        Field theUnsafe = unsafeClass.getDeclaredField("theUnsafe");
        theUnsafe.setAccessible(true);
        return !(boolean) unsafeClass.getMethod("shouldBeInitialized", Class.class).invoke(theUnsafe.get(null), c);
    }
}
