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

package org.apache.cassandra.service;

import java.io.IOException;
import java.nio.file.Files;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.InternodeCompatibilityMode;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessagingService;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The drain of a node in DSE 6 compatibility mode writes the record of DSE 6 peers once more, after messaging stopped,
 * so that a change whose asynchronous write the drain dropped reaches the file. A class of its own: the drain shuts the
 * node down for the rest of the JVM.
 */
public class DsePeersRecordDrainTest
{
    private static EmbeddedCassandraService service;
    private static InternodeCompatibilityMode modeBefore;

    @BeforeClass
    public static void startup() throws IOException
    {
        service = ServerTestUtils.startEmbeddedCassandraService();
        modeBefore = DatabaseDescriptor.getInternodeCompatibilityMode();
    }

    @AfterClass
    public static void tearDown() throws IOException
    {
        if (modeBefore != null)
        {
            DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
            DsePeersRecord.clear();
            DatabaseDescriptor.setInternodeCompatibilityMode(modeBefore);
        }
        if (service != null)
            service.stop();
    }

    @Test
    public void theDrainWritesTheLastStateOfTheRecord() throws Exception
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        DsePeersRecord.clear();
        MessagingService.instance().versions.set(InetAddressAndPort.getByName("127.0.0.44:7000"), MessagingService.VERSION_3014);
        DsePeersRecord.awaitPersisted();
        // a write that did not reach the file, as one the drain drops when it shuts the executor of the writes down
        Files.delete(DsePeersRecord.path());

        StorageService.instance.drain();

        assertThat(StorageService.instance.isDrained()).isTrue();
        assertThat(DsePeersRecord.persistedPeers()).containsExactly("127.0.0.44:7000");
    }
}
