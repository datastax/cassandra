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

package org.apache.cassandra.metrics;

import com.codahale.metrics.Reservoir;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.UniformSnapshot;

/**
 * A reservoir that stores the last N measurements.
 * Same as com.codahale.metrics.SlidingWindowReservoir but does not require array copy and sort
 * to get mean value (via snapshot).
 */
public class SlidingWindowReservoirWithQuickMean implements Reservoir
{
    private final long[] measurements;
    private long count;

    public SlidingWindowReservoirWithQuickMean(int size) {
        this.measurements = new long[size];
        this.count = 0L;
    }

    public int size() {
        if (this.count >= this.measurements.length)
            return this.measurements.length;
        synchronized (this)
        {
            return (int) Math.min(this.count, this.measurements.length);
        }
    }

    public synchronized void update(long value) {
        this.measurements[(int)(this.count++ % this.measurements.length)] = value;
    }

    public double getMean()
    {
        final int sz = size();

        if (sz == 0)
            return 0.0;

        double sum = 0.0;
        synchronized(this) {
            for(int i = 0; i < sz; ++i) {
                sum += this.measurements[i];
            }
        }
        return sum / sz;
    }

    public Snapshot getSnapshot()
    {
        long[] values = new long[this.size()];

        synchronized(this) {
            System.arraycopy(this.measurements, 0, values, 0, values.length);
        }

        return new UniformSnapshot(values);
    }
}
