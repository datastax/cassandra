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

package org.apache.cassandra.locator;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class DynamicEndpointSnitchTest
{

    @BeforeClass
    public static void setupDD()
    {
        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.setDynamicBadnessThreshold(0.1);
        DatabaseDescriptor.setDynamicUpdateInterval(1000000);
        StorageService.instance.unsafeInitialize();
    }

    private static void setScores(DynamicEndpointSnitch dsnitch, int rounds, List<InetAddressAndPort> hosts, Integer... scores) throws InterruptedException
    {
        for (int round = 0; round < rounds; round++)
        {
            for (int i = 0; i < hosts.size(); i++)
                dsnitch.receiveTiming(hosts.get(i), scores[i], MILLISECONDS);
        }
        dsnitch.updateScores();
    }

    private static EndpointsForRange full(InetAddressAndPort... endpoints)
    {
        EndpointsForRange.Builder rlist = EndpointsForRange.builder(ReplicaUtils.FULL_RANGE, endpoints.length);
        for (InetAddressAndPort endpoint: endpoints)
        {
            rlist.add(ReplicaUtils.full(endpoint));
        }
        return rlist.build();
    }

    @Test
    public void testDynamicSnitchSetSeverity()
    {
        // do this because SS needs to be initialized before DES can work properly.
        SimpleSnitch ss = new SimpleSnitch();

        DynamicEndpointSnitch dsnitch = new DynamicEndpointSnitch(ss, String.valueOf(ss.hashCode()));
        double origSeverity = dsnitch.getSeverity();
        double updSeverity = origSeverity + 1.0d;
        dsnitch.setSeverity(updSeverity);
        Assert.assertEquals(updSeverity, dsnitch.getSeverity(), 0.0d);
        dsnitch.setSeverity(origSeverity);
    }

    @Test
    public void testSnitch() throws InterruptedException, IOException, ConfigurationException
    {
        // do this because SS needs to be initialized before DES can work properly.
        DatabaseDescriptor.setDynamicBadnessThreshold(0.1);
        StorageService.instance.unsafeInitialize();
        SimpleSnitch ss = new SimpleSnitch();
        DynamicEndpointSnitch dsnitch = new DynamicEndpointSnitch(ss, String.valueOf(ss.hashCode()));
        InetAddressAndPort self = FBUtilities.getBroadcastAddressAndPort();
        InetAddressAndPort host1 = InetAddressAndPort.getByName("127.0.0.2");
        InetAddressAndPort host2 = InetAddressAndPort.getByName("127.0.0.3");
        InetAddressAndPort host3 = InetAddressAndPort.getByName("127.0.0.4");
        InetAddressAndPort host4 = InetAddressAndPort.getByName("127.0.0.5");
        List<InetAddressAndPort> hosts = Arrays.asList(host1, host2, host3);

        // first, make all hosts equal
        setScores(dsnitch, 1, hosts, 10, 10, 10);
        EndpointsForRange order = full(host1, host2, host3);
        Util.assertRCEquals(order, dsnitch.sortedByProximity(self, full(host1, host2, host3)));

        // make host1 a little worse
        setScores(dsnitch, 1, hosts, 20, 10, 10);
        order = full(host2, host3, host1);
        Util.assertRCEquals(order, dsnitch.sortedByProximity(self, full(host1, host2, host3)));

        // make host2 as bad as host1
        setScores(dsnitch, 2, hosts, 15, 20, 10);
        order = full(host3, host1, host2);
        Util.assertRCEquals(order, dsnitch.sortedByProximity(self, full(host1, host2, host3)));

        // make host3 the worst
        setScores(dsnitch, 3, hosts, 10, 10, 30);
        order = full(host1, host2, host3);
        Util.assertRCEquals(order, dsnitch.sortedByProximity(self, full(host1, host2, host3)));

        // make host3 equal to the others
        setScores(dsnitch, 5, hosts, 10, 10, 10);
        order = full(host1, host2, host3);
        Util.assertRCEquals(order, dsnitch.sortedByProximity(self, full(host1, host2, host3)));

        /// Tests CASSANDRA-6683 improvements
        // make the scores differ enough from the ideal order that we sort by score; under the old
        // dynamic snitch behavior (where we only compared neighbors), these wouldn't get sorted
        setScores(dsnitch, 20, hosts, 10, 70, 20);
        order = full(host1, host3, host2);
        Util.assertRCEquals(order, dsnitch.sortedByProximity(self, full(host1, host2, host3)));

        order = full(host4, host1, host3, host2);
        Util.assertRCEquals(order, dsnitch.sortedByProximity(self, full(host1, host2, host3, host4)));


        setScores(dsnitch, 20, hosts, 10, 10, 10);
        order = full(host4, host1, host2, host3);
        Util.assertRCEquals(order, dsnitch.sortedByProximity(self, full(host1, host2, host3, host4)));
    }

    @Test
    public void testDynamicSnitchSetQuantizationToMillis()
    {
        // do this because SS needs to be initialized before DES can work properly.
        SimpleSnitch ss = new SimpleSnitch();

        DynamicEndpointSnitch dsnitch = new DynamicEndpointSnitch(ss, String.valueOf(ss.hashCode()));
        boolean originalQuantization = dsnitch.getQuantizationToMillis();
        dsnitch.setQuantizationToMillis(!originalQuantization);
        Assert.assertEquals(!originalQuantization, dsnitch.getQuantizationToMillis());
    }

    @Test
    public void testDynamicSnitchSetQuantile()
    {
        // do this because SS needs to be initialized before DES can work properly.
        SimpleSnitch ss = new SimpleSnitch();

        DynamicEndpointSnitch dsnitch = new DynamicEndpointSnitch(ss, String.valueOf(ss.hashCode()));
        double quantile = ThreadLocalRandom.current().nextDouble();
        dsnitch.setQuantile(quantile);
        Assert.assertEquals(quantile, dsnitch.getQuantile(), 0.01);
    }

    @Test
    public void testDynamicSnitchQuantilePicking() throws UnknownHostException
    {
        // do this because SS needs to be initialized before DES can work properly.
        SimpleSnitch ss = new SimpleSnitch();

        DynamicEndpointSnitch dsnitch = new DynamicEndpointSnitch(ss, String.valueOf(ss.hashCode()));
        dsnitch.setQuantile(0.9);

        // add a slow replica, that always reports 100ms
        for (int i = 0; i < 100; i++)
        {
            dsnitch.receiveTiming(InetAddressAndPort.getByName("127.0.0.1"), 100, MILLISECONDS);
        }
        // add a replica that has p90 = 90ms
        for (int i = 1; i <= 100; i++)
        {
            dsnitch.receiveTiming(InetAddressAndPort.getByName("127.0.0.2"), i, MILLISECONDS);
        }

        dsnitch.updateScores();

        // the score for the slow replica should be 1, and the score for the fast replica should be 9/10 = 0.9
        Map<InetAddress, Double> scores = dsnitch.getScores();

        Assert.assertEquals(1.0, scores.get(InetAddress.getByName("127.0.0.1")), 0.01);
        Assert.assertEquals(0.9, scores.get(InetAddress.getByName("127.0.0.2")), 0.01);
    }

    @Test
    public void testQuantizationToMillisEnabled() throws UnknownHostException
    {
        // do this because SS needs to be initialized before DES can work properly.
        SimpleSnitch ss = new SimpleSnitch();

        DynamicEndpointSnitch dsnitch = new DynamicEndpointSnitch(ss, String.valueOf(ss.hashCode()));

        dsnitch.setQuantizationToMillis(true);
        // add a slow replica, that always reports 1999us
        for (int i = 0; i < 100; i++)
        {
            dsnitch.receiveTiming(InetAddressAndPort.getByName("127.0.0.1"), 1999, MICROSECONDS);
        }
        // add a fast replica, that always reports 1001us
        for (int i = 0; i < 100; i++)
        {
            dsnitch.receiveTiming(InetAddressAndPort.getByName("127.0.0.2"), 1001, MICROSECONDS);
        }

        dsnitch.updateScores();

        // the score for both replicas should be 1 because the quantization should round to 1ms
        Map<InetAddress, Double> scores = dsnitch.getScores();

        Assert.assertEquals(1.0, scores.get(InetAddress.getByName("127.0.0.1")), 0.01);
        Assert.assertEquals(1.0, scores.get(InetAddress.getByName("127.0.0.2")), 0.01);
    }

    @Test
    public void testQuantizationToMillisDisabled() throws UnknownHostException
    {
        // do this because SS needs to be initialized before DES can work properly.
        SimpleSnitch ss = new SimpleSnitch();

        DynamicEndpointSnitch dsnitch = new DynamicEndpointSnitch(ss, String.valueOf(ss.hashCode()));

        dsnitch.setQuantizationToMillis(false);
        dsnitch.setQuantile(0.9);
        // add a slow replica, that always reports 100us
        for (int i = 0; i < 100; i++)
        {
            dsnitch.receiveTiming(InetAddressAndPort.getByName("127.0.0.1"), 100, MICROSECONDS);
        }
        // add a fast replica, that always reports 10us
        for (int i = 0; i < 100; i++)
        {
            dsnitch.receiveTiming(InetAddressAndPort.getByName("127.0.0.2"), 10, MICROSECONDS);
        }

        dsnitch.updateScores();

        // the score for the slow replica should be 1, and the score for the fast replica should be 10/100 = 0.1
        // there should be no quantization rounding to 1ms
        Map<InetAddress, Double> scores = dsnitch.getScores();

        Assert.assertEquals(1.0, scores.get(InetAddress.getByName("127.0.0.1")), 0.01);
        Assert.assertEquals(0.1, scores.get(InetAddress.getByName("127.0.0.2")), 0.01);
    }

    @Test
    public void testScoresAreUpdatedPeriodically() throws UnknownHostException, InterruptedException
    {
        // do this because SS needs to be initialized before DES can work properly.
        SimpleSnitch ss = new SimpleSnitch();

        int updateInterval = DatabaseDescriptor.getDynamicUpdateInterval();
        try
        {
            DynamicEndpointSnitch dsnitch = new DynamicEndpointSnitch(ss, String.valueOf(ss.hashCode()));

            // add a slow replica, that always reports 100ms
            for (int i = 0; i < 100; i++)
            {
                dsnitch.receiveTiming(InetAddressAndPort.getByName("127.0.0.1"), 100, MILLISECONDS);
            }
            // add a fast replica, that always reports 1ms
            for (int i = 0; i < 100; i++)
            {
                dsnitch.receiveTiming(InetAddressAndPort.getByName("127.0.0.2"), 1, MILLISECONDS);
            }

            Assert.assertTrue(dsnitch.getScores().isEmpty());

            DatabaseDescriptor.setDynamicUpdateInterval(100);
            dsnitch.applyConfigChanges();

            Thread.sleep(150);

            Assert.assertFalse(dsnitch.getScores().isEmpty());
        }
        finally
        {
            DatabaseDescriptor.setDynamicUpdateInterval(updateInterval);
        }
    }
}
