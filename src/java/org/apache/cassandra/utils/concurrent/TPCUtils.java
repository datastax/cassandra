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

package org.apache.cassandra.utils.concurrent;

import java.util.concurrent.*;

import org.apache.cassandra.config.DatabaseDescriptor;

/**
 * Utility class with static methods meant to make working and interacting with TPC easier. Please note this class
 * must never implement methods causing initialization of the TPC state, so please do not call any {@link TPC} static
 * methods here.
 */
public class TPCUtils
{
    private static final int NUM_CORES = DatabaseDescriptor.getTPCCores();

    public static int getCoreId()
    {
        return getCoreId(Thread.currentThread());
    }

    /**
     * @return the core id for TPC threads, otherwise the number of cores. Callers can verify if the returned
     * core is valid via {@link #isValidCoreId(int)}, or alternatively can allocate an
     * array with length num_cores + 1, and use thread safe operations only on the last element.
     */
    public static int getCoreId(Thread t)
    {
        // return t instanceof TPCThread ? ((TPCThread) t).coreId() : NUM_CORES;
        return NUM_CORES;
    }


    /**
     *
     * @return the configured number of TPC cores
     */
    public static int getNumCores()
    {
        return NUM_CORES;
    }

    /**
     * Check if this is a valid core id.
     *
     * @param coreId the core id to check.
     *
     * @return true if the core id is valid, that is it is &gt= 0 and &lt {@link DatabaseDescriptor#getTPCCores()}.
     */
    public static boolean isValidCoreId(int coreId)
    {
        return coreId >= 0 && coreId < NUM_CORES;
    }

    public static void blockingAwait(CompletableFuture<?> completable)
    {
        //ensureNotOnTPCThread("Calling blockingAwait");

        completable.join();
    }

}
