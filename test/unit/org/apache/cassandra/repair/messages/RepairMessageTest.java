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

package org.apache.cassandra.repair.messages;

import java.io.File;
import java.util.UUID;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.nodes.Nodes;
import org.apache.cassandra.nodes.PeerInfo;
import org.apache.cassandra.utils.CassandraVersion;

import static org.assertj.core.api.Assertions.assertThat;

public class RepairMessageTest
{
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();
    private File dir;

    @Before
    public void initialize() throws Throwable
    {
        dir = folder.newFolder();
        Nodes.Instance.unsafeSetup(dir.toPath());
    }

    @After
    public void tearDown()
    {
        // Always clean the system property before running a new test
        System.clearProperty(RepairMessage.ALWAYS_CONSIDER_TIMEOUTS_SUPPORTED_PROPERTY);
    }

    @Test
    public void testSupportsTimeoutsIsTrueWithCassandraVersionSupportingTimeouts() throws Exception
    {
        InetAddressAndPort addressAndPort = InetAddressAndPort.getByName("127.0.0.3");
        Nodes.peers().update(addressAndPort, p -> fakePeer(p, RepairMessage.SUPPORTS_TIMEOUTS));
        assertThat(RepairMessage.supportsTimeouts(addressAndPort, UUID.randomUUID())).isTrue();
    }

    @Test
    public void testSupportsTimeoutsIsFalseWithCassandraVersionNotSupportingTimeouts() throws Exception
    {
        InetAddressAndPort addressAndPort = InetAddressAndPort.getByName("127.0.0.3");
        CassandraVersion versionNotSupportingTimeouts = CassandraVersion.CASSANDRA_4_0;
        Nodes.peers().update(addressAndPort, p -> fakePeer(p, versionNotSupportingTimeouts));

        assertThat(versionNotSupportingTimeouts.compareTo(RepairMessage.SUPPORTS_TIMEOUTS)).isLessThan(0);
        assertThat(RepairMessage.supportsTimeouts(addressAndPort, UUID.randomUUID())).isFalse();
    }

    @Test
    public void testSupportsTimeoutsIsAlwaysTrueWhenSystemPropertyIsSetToTrue() throws Exception
    {
        System.setProperty(RepairMessage.ALWAYS_CONSIDER_TIMEOUTS_SUPPORTED_PROPERTY, "true");

        // Check with peer not supporting timeouts
        InetAddressAndPort peerNotSupportingTimeouts = InetAddressAndPort.getByName("127.0.0.3");
        CassandraVersion versionNotSupportingTimeouts = CassandraVersion.CASSANDRA_4_0;
        Nodes.peers().update(peerNotSupportingTimeouts, p -> fakePeer(p, versionNotSupportingTimeouts));
        assertThat(versionNotSupportingTimeouts.compareTo(RepairMessage.SUPPORTS_TIMEOUTS)).isLessThan(0);
        assertThat(RepairMessage.supportsTimeouts(peerNotSupportingTimeouts, UUID.randomUUID())).isTrue();

        // Check with peer supporting timeouts
        InetAddressAndPort peerSupportingTimeouts = InetAddressAndPort.getByName("127.0.0.4");
        Nodes.peers().update(peerSupportingTimeouts, p -> fakePeer(p, RepairMessage.SUPPORTS_TIMEOUTS));
        assertThat(RepairMessage.supportsTimeouts(peerSupportingTimeouts, UUID.randomUUID())).isTrue();
    }

    static void fakePeer(PeerInfo p, CassandraVersion version)
    {
        int nodeId = p.getPeer().address.getAddress()[3];
        p.setPreferred(p.getPeer());
        p.setDataCenter("DC" + nodeId);
        p.setRack("RAC" + nodeId);
        p.setHostId(UUID.randomUUID());
        p.setReleaseVersion(version);
    }
}