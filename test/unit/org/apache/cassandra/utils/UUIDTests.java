package org.apache.cassandra.utils;
/*
 *
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
 *
 */


import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.RateLimiter;
import org.junit.Test;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Snapshot;
import org.apache.cassandra.db.marshal.TimeUUIDType;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;


public class UUIDTests
{
    @Test
    public void verifyType1()
    {

        UUID uuid = UUIDGen.getTimeUUID();
        assert uuid.version() == 1;
    }

    @Test
    public void verifyOrdering1()
    {
        UUID one = UUIDGen.getTimeUUID();
        UUID two = UUIDGen.getTimeUUID();
        assert one.timestamp() < two.timestamp();
    }


    @Test
    public void testDecomposeAndRaw()
    {
        UUID a = UUIDGen.getTimeUUID();
        byte[] decomposed = UUIDGen.decompose(a);
        UUID b = UUIDGen.getUUID(ByteBuffer.wrap(decomposed));
        assert a.equals(b);
    }

    @Test
    public void testTimeUUIDType()
    {
        TimeUUIDType comp = TimeUUIDType.instance;
        ByteBuffer first = ByteBuffer.wrap(UUIDGen.getTimeUUIDBytes());
        ByteBuffer second = ByteBuffer.wrap(UUIDGen.getTimeUUIDBytes());
        assert comp.compare(first, second) < 0;
        assert comp.compare(second, first) > 0;
        ByteBuffer sameAsFirst = ByteBuffer.wrap(UUIDGen.decompose(UUIDGen.getUUID(first)));
        assert comp.compare(first, sameAsFirst) == 0;
    }

    @Test
    public void testUUIDTimestamp()
    {
        long now = System.currentTimeMillis();
        UUID uuid = UUIDGen.getTimeUUID();
        long tstamp = UUIDGen.getAdjustedTimestamp(uuid);

        // I'll be damn is the uuid timestamp is more than 10ms after now
        assert now <= tstamp && now >= tstamp - 10 : "now = " + now + ", timestamp = " + tstamp;
    }

//    @Test
//    public void testUUIDDrift() throws Exception
//    {
//        Histogram histogram = Metrics.histogram("foo");
//        Runnable worker = () -> {
//            RateLimiter rateLimiter = RateLimiter.create(50000);
//            for (long i = 0; !Thread.currentThread().isInterrupted(); i++)
//            {
//                rateLimiter.acquire();
//                UUID id = UUIDGen.getTimeUUID();
//
//                long age = UUIDGen.microsAge(id);
//                if (age < -100)
//                    histogram.update(-age);
//            }
//        };
//
//        // 11:45
//
//        int threads = 20;
//        ExecutorService executor = Executors.newFixedThreadPool(threads);
//        for (int i = 0; i < threads; i++)
//            executor.submit(worker);
//
//        long end = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(60);
//        while (System.currentTimeMillis() < end)
//        {
//            Snapshot snap = histogram.getSnapshot();
//            System.out.println(String.format("p50=%d p75=%d p90=%d p95=%d p99=%d p999=%d pMax=%d count=%d",
//                               (int)snap.getMean(),
//                               (int)snap.get75thPercentile(),
//                               (int)snap.getValue(0.9),
//                               (int)snap.get95thPercentile(),
//                               (int)snap.get99thPercentile(),
//                               (int)snap.get999thPercentile(),
//                               snap.getMax(),
//                               histogram.getCount()));
//
//            Thread.sleep(1000L);
//        }
//    }
}
