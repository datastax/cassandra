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

import java.util.concurrent.Semaphore;

/**
 * A {@link Semaphore} whose total permit count can be changed at runtime. Growing adds permits
 * ({@link Semaphore#release(int)}); shrinking removes them ({@link Semaphore#reducePermits(int)}),
 * which may leave the available count temporarily negative until in-flight holders release — a safe
 * way to lower a budget without disturbing work already admitted. Resizing is atomic with respect to
 * other resizes.
 */
public final class ResizableSemaphore extends Semaphore
{
    private int totalPermits;

    public ResizableSemaphore(int permits)
    {
        super(Math.max(0, permits));
        this.totalPermits = Math.max(0, permits);
    }

    /** The current total permit count (not the available count). */
    public synchronized int totalPermits()
    {
        return totalPermits;
    }

    /**
     * Change the total permit count to {@code newTotal} (clamped at 0). A larger value releases the
     * difference; a smaller value reduces it (available permits may go negative until releases catch up).
     */
    public synchronized void setTotalPermits(int newTotal)
    {
        newTotal = Math.max(0, newTotal);
        int delta = newTotal - totalPermits;
        if (delta > 0)
            release(delta);
        else if (delta < 0)
            reducePermits(-delta);
        totalPermits = newTotal;
    }
}
