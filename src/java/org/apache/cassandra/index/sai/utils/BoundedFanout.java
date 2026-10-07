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

package org.apache.cassandra.index.sai.utils;

import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;

/**
 * An {@link InsertFanout} whose in-flight weight is bounded by a weighted {@link Semaphore}: at most
 * {@code maxWeight} total weight (e.g. bytes) may be in flight across all sharers of the budget. The
 * producer parks (no busy-wait) on {@link Semaphore#acquire(int)} when the budget is exhausted. The
 * {@code budget} may be shared across many {@code BoundedFanout} instances to enforce a single
 * node-wide bound.
 */
public final class BoundedFanout extends InsertFanout
{
    private final Semaphore budget;
    private final int maxWeight;

    /**
     * @param executor  runs the dispatched tasks; its size bounds thread concurrency
     * @param budget    weighted admission gate bounding total in-flight weight; may be shared
     * @param maxWeight the {@code budget}'s total permits, used to clamp an over-budget unit so a single
     *                  unit heavier than the whole budget is still admitted (one at a time)
     */
    public BoundedFanout(Executor executor, Semaphore budget, int maxWeight)
    {
        super(executor);
        this.budget = budget;
        this.maxWeight = Math.max(1, maxWeight);
    }

    @Override
    protected int admit(int weight)
    {
        int w = Math.max(1, Math.min(maxWeight, weight));
        try
        {
            budget.acquire(w);
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while awaiting fan-out admission", e);
        }
        return w;
    }

    @Override
    protected void release(int admitted)
    {
        budget.release(admitted);
    }
}
