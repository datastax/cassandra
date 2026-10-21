/*
 * Copyright IBM Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.hints;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.service.StorageService;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class HintsRateLimiterFactoryTest
{
    private final List<InetAddressAndPort> members = new ArrayList<>();

    @BeforeClass
    public static void setUp()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @After
    public void cleanup()
    {
        members.forEach(StorageService.instance.getTokenMetadata()::removeEndpoint);
    }

    @Test
    public void testTransferRateIsNotScaledByNodeCount() throws Exception
    {
        TokenMetadata tokenMetadata = StorageService.instance.getTokenMetadata();
        for (String address : new String[]{ "127.0.0.11", "127.0.0.12", "127.0.0.13" })
        {
            InetAddressAndPort endpoint = InetAddressAndPort.getByName(address);
            tokenMetadata.updateHostId(UUID.randomUUID(), endpoint);
            tokenMetadata.updateNormalToken(DatabaseDescriptor.getPartitioner().getRandomToken(), endpoint);
            members.add(endpoint);
        }
        int nodes = tokenMetadata.getSizeOfAllEndpoints();
        assertTrue(nodes >= 3);
        double throttleInBytes = DatabaseDescriptor.getHintedHandoffThrottleInKiB() * 1024.0;
        assertTrue(throttleInBytes > 0);

        HintsRateLimiterFactory factory = new HintsRateLimiterFactory.DefaultHintsRateLimiterFactory();
        UUID hostId = UUID.randomUUID();

        // a normal dispatch shares the throttle with the other nodes sending to the same host
        assertEquals(throttleInBytes / (nodes - 1), factory.create(hostId).getRate(), 1.0);
        assertEquals(throttleInBytes / (nodes - 1), factory.create(hostId, false).getRate(), 1.0);
        // a transfer is the only sender, so it gets the whole throttle
        assertEquals(throttleInBytes, factory.create(hostId, true).getRate(), 1.0);

        // a throttle smaller than the number of senders is a small rate, not an unlimited one (the integer
        // division in KiB rounded it down to 0, which means unlimited)
        int savedThrottle = DatabaseDescriptor.getHintedHandoffThrottleInKiB();
        DatabaseDescriptor.setHintedHandoffThrottleInKiB(1);
        try
        {
            assertEquals(1024.0 / (nodes - 1), factory.create(hostId).getRate(), 0.01);
            assertEquals(1024.0, factory.create(hostId, true).getRate(), 0.01);
        }
        finally
        {
            DatabaseDescriptor.setHintedHandoffThrottleInKiB(savedThrottle);
        }
    }
}
