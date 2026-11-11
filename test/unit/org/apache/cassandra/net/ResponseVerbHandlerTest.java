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

package org.apache.cassandra.net;

import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MonotonicClock;

import static org.apache.cassandra.utils.MonotonicClock.Global.approxTime;
import static org.apache.cassandra.utils.MonotonicClock.Global.preciseTime;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

public class ResponseVerbHandlerTest
{
    @BeforeClass
    public static void setup()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    /**
     * Requests built from a {@link Dispatcher.RequestTime} are stamped with the precise clock, while {@code approxTime}
     * is a sampled clock that lags behind it. The latency reported to the {@link LatencySubscribers} must never be
     * negative, otherwise histograms with DSE-compatible boundaries reject it (CNDB-15548).
     */
    @Test
    public void latencyIsNotNegativeWhenApproxClockLagsBehindPreciseClock() throws Exception
    {
        assumeTrue("requires the default sampled approximate clock", approxTime instanceof MonotonicClock.SampledClock);
        MonotonicClock.SampledClock sampled = (MonotonicClock.SampledClock) approxTime;

        AtomicLong reportedLatencyNanos = new AtomicLong(Long.MIN_VALUE);
        InetAddressAndPort replica = FBUtilities.getBroadcastAddressAndPort();
        MessagingService.instance().latencySubscribers.subscribe((address, latency, unit) -> {
            if (address.equals(replica))
                reportedLatencyNanos.set(unit.toNanos(latency));
        });

        sampled.pauseNowSampling();
        try
        {
            // approxTime is now frozen, so any later precise timestamp is strictly ahead of it
            TimeUnit.MILLISECONDS.sleep(10);
            Dispatcher.RequestTime requestTime = new Dispatcher.RequestTime(preciseTime.now());
            assertTrue(requestTime.startedAtNanos() > approxTime.now());

            Message<NoPayload> request = Message.outWithFlags(Verb.ECHO_REQ, NoPayload.noPayload, requestTime, Collections.emptyList());
            MessagingService.instance().callbacks.addWithExpiration(new RequestCallback<NoPayload>()
            {
                @Override
                public void onResponse(Message<NoPayload> msg) {}

                @Override
                public boolean trackLatencyForSnitch(Verb responseVerb, boolean isTimeout)
                {
                    return true;
                }
            }, request, replica);

            ResponseVerbHandler.instance.doVerb(request.emptyResponse());
        }
        finally
        {
            sampled.resumeNowSampling();
        }

        long latency = reportedLatencyNanos.get();
        assertTrue("latency was not reported", latency != Long.MIN_VALUE);
        assertTrue("latency must not be negative but was " + latency, latency >= 0);
    }
}
