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

package org.apache.cassandra.service.paxos;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.Uninterruptibles;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.metrics.CASClientRequestMetrics;

public final class PaxosUtils
{
    private static final Integer minPaxosBackoffMillis = CassandraRelevantProperties.LWT_MIN_BACKOFF_MS.getInt();
    private static final Integer maxPaxosBackoffMillis = CassandraRelevantProperties.LWT_MAX_BACKOFF_MS.getInt();

    private PaxosUtils()
    {
    }

    /**
     * Applies a random sleep time between minPaxosBackoffMillis (inclusive) and maxPaxosBackoffMillis (exclusive)
     * and emits the contentionBackoffLatency metric.
     */
    public static void applyPaxosContentionBackoff(CASClientRequestMetrics casMetrics)
    {
        int sleepInMillis = ThreadLocalRandom.current().nextInt(minPaxosBackoffMillis, maxPaxosBackoffMillis);
        Uninterruptibles.sleepUninterruptibly(sleepInMillis, TimeUnit.MILLISECONDS);
        long sleepInNanos = TimeUnit.MILLISECONDS.toNanos(sleepInMillis);
        casMetrics.contentionBackoffLatency.addNano(sleepInNanos);
    }
}
