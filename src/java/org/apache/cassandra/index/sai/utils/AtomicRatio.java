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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class AtomicRatio
{
    private final AtomicReference<Ratio> ratio = new AtomicReference<>(new Ratio(0, 0));
    private final AtomicInteger updateCount = new AtomicInteger();

    private static class Ratio {
        public final long numerator;
        public final long denominator;

        public Ratio(long numerator, long denominator) {
            this.numerator = numerator;
            this.denominator = denominator;
        }
    }

    public double updateAndGet(long numerator, long denominator) {
        var updated = ratio.updateAndGet((current) -> new Ratio(current.numerator + numerator, current.denominator + denominator));
        updateCount.incrementAndGet();
        return (double) updated.numerator / updated.denominator;
    }

    public double get() {
        var current = ratio.get();
        return (double) current.numerator / current.denominator;
    }

    public int getUpdateCount() {
        return updateCount.get();
    }
}
