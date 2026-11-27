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

package org.apache.cassandra.fuzz.harry.integration;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.harry.soak.SoakRunner;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.junit.Assert.assertEquals;

/**
 * Runs {@link SoakRunner} over the native protocol against a node of an in-JVM cluster, as it runs against an
 * external cluster.
 */
public abstract class DriverSoakRunnerTestBase extends TestBaseImpl
{
    protected static Cluster cluster;

    @BeforeClass
    public static void before() throws Throwable
    {
        cluster = Cluster.build(1)
                         .withConfig(c -> c.with(GOSSIP, NETWORK, NATIVE_PROTOCOL))
                         .start();
    }

    @AfterClass
    public static void afterClass()
    {
        if (cluster != null)
            cluster.close();
    }

    protected static void assertPasses(String... options)
    {
        String[] args = new String[options.length + 8];
        args[0] = "--contact-points";
        args[1] = cluster.get(1).broadcastAddress().getHostString();
        args[2] = "--port";
        args[3] = String.valueOf(cluster.get(1).config().get("native_transport_port"));
        args[4] = "--replication-factor";
        args[5] = "1";
        args[6] = "--progress-interval";
        args[7] = "10s";
        System.arraycopy(options, 0, args, 8, options.length);
        assertEquals(SoakRunner.Outcome.PASSED, new SoakRunner(SoakRunner.Config.parse(args)).run());
    }
}
