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

public interface ResizableThreadPool
{
    /**
     * Returns maximum pool size of thread pool.
     */
    public int getCorePoolSize();

    /**
     * Allows user to resize maximum size of the thread pool.
     */
    public void setCorePoolSize(int newCorePoolSize);

    /**
     * Returns maximum pool size of thread pool.
     */
    public int getMaximumPoolSize();

    /**
     * Allows user to resize maximum size of the thread pool.
     */
    public void setMaximumPoolSize(int newMaximumPoolSize);

    /**
     * Age in milliseconds of the task at the head of this pool's queue, 0 when the queue is empty
     * or the pool does not track it (scheduled executors).
     */
    default long getOldestQueuedTaskAgeMs()
    {
        return 0;
    }

    /**
     * Age in milliseconds of the oldest task currently executing in this pool, 0 when idle.
     */
    default long getLongestRunningTaskAgeMs()
    {
        return 0;
    }

    /**
     * Fully qualified class name of the oldest task currently executing, null when idle.
     */
    default String getLongestRunningTaskClass()
    {
        return null;
    }
}
