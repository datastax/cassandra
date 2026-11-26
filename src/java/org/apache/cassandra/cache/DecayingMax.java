/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.cache;

import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.base.Preconditions;

/**
 * A watermark that tracks the maximum of recent samples, decaying by a fixed {@code factor} every time
 * {@link #update(long)} is called. Unlike a moving average, this is intentionally a lagging, inflated estimate:
 * it only ever decreases by {@code factor} per call, so between peaks it sits above the true current value.
 * <p>
 */
@ThreadSafe
class DecayingMax
{
    private final double factor;
    private final AtomicLong value = new AtomicLong();

    /**
     * @param factor decay factor applied to the current value on every {@link #update(long)}, in {@code (0, 1)}.
     *               E.g. {@code 0.5} halves the previous value each tick, before comparing to the new sample.
     */
    DecayingMax(double factor)
    {
        Preconditions.checkArgument(factor > 0.0 && factor < 1.0, "factor must be in (0, 1), got %s", factor);
        this.factor = factor;
    }

    /**
     * Record a new sample: the stored value becomes the larger of the new sample and the previous value decayed
     * by {@code factor}. Applied as a single atomic read-modify-write so concurrent callers never clobber each
     * other's sample.
     */
    void update(long sample)
    {
        value.updateAndGet(prev -> Math.max(sample, (long) (prev * factor)));
    }

    long get()
    {
        return value.get();
    }
}
