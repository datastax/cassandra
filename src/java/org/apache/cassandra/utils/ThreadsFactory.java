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

package org.apache.cassandra.utils;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.concurrent.InlinedThreadLocalThread;

public class ThreadsFactory
{
    /**
     * @param name name of the thread for this executor
     * @return a single threaded executor whose threads have names
     */
    public static ExecutorService newSingleThreadedExecutor(String name)
    {
        return Executors.newSingleThreadExecutor(new NamedThreadFactory(name));
    }

    /**
     * @param r runnable task for the thread
     * @param name for the thread
     * @return a new daemon thread which has the given name and task
     */
    public static Thread newDaemonThread(Runnable r, String name)
    {
        return newThread(r, name, true);
    }

    /**
     * @param r runnable task for the thread
     * @param name for the thread
     * @param isDaemon
     * @return a new thread which has the given name and task
     */
    public static Thread newThread(Runnable r, String name, boolean isDaemon)
    {
        Thread t = new InlinedThreadLocalThread(r, name);
        t.setDaemon(isDaemon);
        return t;
    }

    public static void addShutdownHook(Runnable r, String name)
    {
        // shutdown hook threads should not be daemon
        Runtime.getRuntime().addShutdownHook(newThread(r, name, false));
    }
}
