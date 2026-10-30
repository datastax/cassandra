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

package org.apache.cassandra.nodes;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dsecompat.DseFixtures;
import org.apache.cassandra.dsecompat.DseFixtures.DseVersion;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@link DseNodesFiles} on the {@code nodes/local} and {@code nodes/peers} files written by DSE 6.8.65 and 6.9.26
 * ({@code test/data/dse-compat/nodes}), read in place from any directory.
 */
public class DseNodesFilesTest
{
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @BeforeClass
    public static void initialise()
    {
        DatabaseDescriptor.clientInitialization();
    }

    @Test
    public void readsTheFilesOfADse6865Node() throws IOException
    {
        Path nodes = DseFixtures.nodesLocal(DseVersion.DSE_6_8_65).getParent();
        DseNodeRecord local = DseNodesFiles.readLocal(nodes);
        assertThat(local).isNotNull();
        assertThat(local.peer).isNull();
        assertThat(local.broadcastAddress.getHostAddress()).isEqualTo("192.168.57.11");
        assertThat(local.listenAddress.getHostAddress()).isEqualTo("192.168.57.11");
        assertThat(local.hostId).isEqualTo(UUID.fromString("0c646ff5-7983-460a-9aad-b0cdd24df296"));
        assertThat(local.clusterName).isEqualTo("dse-lab");
        assertThat(local.dataCenter).isEqualTo("dc1");
        assertThat(local.rack).isEqualTo("rack1");
        assertThat(local.releaseVersion).isEqualTo(DseVersion.DSE_6_8_65.releaseVersion);
        assertThat(local.dseVersion).isEqualTo("6.8.65");
        assertThat(local.schemaVersion).isNotNull();
        assertThat(local.workloads.workloads).containsExactly(DseNodeWorkloads.CASSANDRA);
        assertThat(local.workloads.graph).isFalse();
        assertThat(local.tokenCount).isEqualTo(8);

        List<DseNodeRecord> peers = DseNodesFiles.readPeers(nodes);
        assertThat(peers).extracting(p -> p.peer.getHostAddress()).containsExactlyInAnyOrder("192.168.57.12", "192.168.57.13");
        assertThat(peers).allSatisfy(p -> {
            assertThat(p.releaseVersion).isEqualTo(DseVersion.DSE_6_8_65.releaseVersion);
            assertThat(p.dseVersion).isEqualTo("6.8.65");
            assertThat(p.hostId).isNotNull();
            assertThat(p.broadcastAddress).isNull();
            assertThat(p.workloads.peer).isEqualTo(p.peer);
            assertThat(p.tokenCount).isEqualTo(8);
        });
        assertThat(peers).anyMatch(p -> UUID.fromString("ed39c7b2-7f97-4a0a-91fc-71398afe1d12").equals(p.hostId));
    }

    @Test
    public void readsAPeerAlreadyRunningThisSoftware() throws IOException
    {
        List<DseNodeRecord> peers = DseNodesFiles.readPeers(DseFixtures.nodesDirectoryMixed());
        assertThat(peers).hasSize(2);
        DseNodeRecord upgraded = peers.stream().filter(p -> p.peer.getHostAddress().equals("192.168.57.13")).findFirst().orElseThrow();
        assertThat(upgraded.releaseVersion).isEqualTo("5.0.7.0-SNAPSHOT");
        // the DSE version upgraded nodes gossip to DSE nodes
        assertThat(upgraded.dseVersion).isEqualTo("6.8.1");
        assertThat(DseNodesFiles.readLocal(DseFixtures.nodesDirectoryMixed()).broadcastAddress.getHostAddress()).isEqualTo("192.168.57.12");
    }

    @Test
    public void readsTheFilesOfADse6926Node() throws IOException
    {
        Path nodes = DseFixtures.nodesLocal(DseVersion.DSE_6_9_26).getParent();
        DseNodeRecord local = DseNodesFiles.readLocal(nodes);
        assertThat(local.releaseVersion).isEqualTo(DseVersion.DSE_6_9_26.releaseVersion);
        assertThat(local.dseVersion).isEqualTo("6.9.26");
        assertThat(local.clusterName).isEqualTo("Test Cluster");
        assertThat(local.dataCenter).isEqualTo("Cassandra");
        assertThat(DseNodesFiles.readPeers(nodes)).hasSize(1).allMatch(p -> DseVersion.DSE_6_9_26.releaseVersion.equals(p.releaseVersion));
    }

    @Test
    public void anEmptyOrAbsentFileHoldsNoNode() throws IOException
    {
        Path nodes = temporaryFolder.newFolder().toPath();
        assertThat(DseNodesFiles.readLocal(nodes)).isNull();
        assertThat(DseNodesFiles.readPeers(nodes)).isEmpty();
        Files.copy(DseFixtures.nodesPeersEmpty(), nodes.resolve("peers"));
        assertThat(DseNodesFiles.readPeers(nodes)).isEmpty();
    }

    @Test
    public void aFileWhoseExistenceCannotBeToldIsAnError() throws IOException
    {
        Path nodes = temporaryFolder.newFolder().toPath();
        Files.copy(DseFixtures.nodesLocal(DseVersion.DSE_6_8_65), nodes.resolve("local"));
        Files.setPosixFilePermissions(nodes, PosixFilePermissions.fromString("r--------"));
        try
        {
            Assume.assumeFalse("the directory is still searchable (the tests run as root)", Files.isExecutable(nodes));
            assertThatThrownBy(() -> DseNodesFiles.readLocal(nodes)).hasMessageContaining("cannot tell whether " + nodes.resolve("local") + " exists");
            assertThatThrownBy(() -> DseNodesFiles.readPeers(nodes)).hasMessageContaining("cannot tell whether " + nodes.resolve("peers") + " exists");
        }
        finally
        {
            Files.setPosixFilePermissions(nodes, PosixFilePermissions.fromString("rwx------"));
        }
    }

    @Test
    public void aDamagedFileIsAnError() throws IOException
    {
        Path nodes = temporaryFolder.newFolder().toPath();
        byte[] local = Files.readAllBytes(DseFixtures.nodesLocal(DseVersion.DSE_6_8_65));
        Files.write(nodes.resolve("local"), Arrays.copyOf(local, local.length / 2));
        assertThatThrownBy(() -> DseNodesFiles.readLocal(nodes)).hasMessageContaining("cannot decode " + nodes.resolve("local"));
    }
}
