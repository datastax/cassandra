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

package org.apache.cassandra.service;

import org.junit.BeforeClass;
import org.junit.Test;

import com.carrotsearch.randomizedtesting.annotations.Timeout;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.metrics.CASClientRequestMetrics;
import org.apache.cassandra.service.paxos.PaxosUtils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class PaxosUtilsTest
{
    private static final int minPaxosBackoffMillis = 1;
    private static final int maxPaxosBackoffMillis = 5;

    private static final long minPaxosBackoffMicros = minPaxosBackoffMillis * 1000;
    private static final long maxPaxosBackoffMicros = maxPaxosBackoffMillis * 1000;

    @BeforeClass
    public static void beforeClass() throws Throwable
    {
        CassandraRelevantProperties.LWT_MIN_BACKOFF_MS.setInt(minPaxosBackoffMillis);
        CassandraRelevantProperties.LWT_MAX_BACKOFF_MS.setInt(maxPaxosBackoffMillis);
    }

    @Timeout(millis = 500)
    @Test
    public void testApplyPaxosContentionBackoff()
    {
        CASClientRequestMetrics casMetrics = new CASClientRequestMetrics("test", "");
        long totalLatencyMicrosFromPreviousIteration = 0;
        for (int i = 0; i < 100; i++)
        {
            PaxosUtils.applyPaxosContentionBackoff(casMetrics);
            assertEquals(i + 1, casMetrics.contentionBackoffLatency.latency.getCount());

            double lastRecordedLatencyMicros = casMetrics.contentionBackoffLatency.totalLatency.getCount() - totalLatencyMicrosFromPreviousIteration;
            totalLatencyMicrosFromPreviousIteration = casMetrics.contentionBackoffLatency.totalLatency.getCount();
            assertTrue(lastRecordedLatencyMicros >= minPaxosBackoffMicros);
            assertTrue(lastRecordedLatencyMicros < maxPaxosBackoffMicros);
        }
    }
}
