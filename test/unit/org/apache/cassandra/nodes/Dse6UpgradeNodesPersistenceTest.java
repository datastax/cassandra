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

import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.InternodeCompatibilityMode;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.dsecompat.DseFixtures;
import org.apache.cassandra.dsecompat.DseFixtures.DseVersion;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.nodes.Dse6UpgradeNodesPersistence.ImportState;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Which import {@link Nodes} takes for a {@code nodes} directory ({@link Nodes#nodesDirectoryImport()}), and that the
 * import of {@link Dse6UpgradeNodesPersistence} with the flag dropped flushes nothing and archives the directory
 * inline, as {@link CC4UpgradeNodesPersistence} does. The import of DSE's files in
 * {@code internode_compatibility_mode: DSE_6} is tested by {@link CC4NodesFileReaderDseTest}.
 */
public class Dse6UpgradeNodesPersistenceTest extends CQLTester
{
    private static final UUID HOST_6926 = UUID.fromString("55771b40-b5c1-4123-94e5-8af8ce644811");

    private Path tempDir;
    private File originalMetadataDir;
    private InternodeCompatibilityMode modeBefore;

    @Before
    public void setUp() throws Exception
    {
        tempDir = Files.createTempDirectory("dse6-upgrade-test");
        originalMetadataDir = DatabaseDescriptor.getMetadataDirectory();
        DatabaseDescriptor.setMetadataDirectory(new File(tempDir));
        modeBefore = DatabaseDescriptor.getInternodeCompatibilityMode();
        // the system tables are shared by the tests of this class: start every test without peers
        NodesPersistence tables = new NodesPersistence();
        for (PeerInfo peer : tables.loadPeers().collect(Collectors.toList()))
            tables.deletePeer(peer.getPeerAddressAndPort());
    }

    @After
    public void tearDown()
    {
        try
        {
            Dse6UpgradeNodesPersistence.awaitPendingCompletion();
        }
        finally
        {
            Dse6UpgradeNodesPersistence.resetPendingCompletion();
            DatabaseDescriptor.setMetadataDirectory(originalMetadataDir);
            DatabaseDescriptor.setInternodeCompatibilityMode(modeBefore);
            new File(tempDir).deleteRecursive();
        }
    }

    @Test
    public void withoutTheModeAndWithoutAMarkerTheCC4ImportArchivesInTheLoad() throws Exception
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.NONE);
        Path nodes = DseFixtures.installNodesDirectory(tempDir, DseVersion.DSE_6_9_26, false);
        assertThat(Dse6UpgradeNodesPersistence.handlesNodesDirectory()).isFalse();
        NodesPersistence persistence = Nodes.nodesDirectoryImport();
        assertThat(persistence).isExactlyInstanceOf(CC4UpgradeNodesPersistence.class);

        assertThat(persistence.loadLocal().getHostId()).isEqualTo(HOST_6926);
        assertThat(Files.isDirectory(nodes)).isTrue();
        assertThat(persistence.loadPeers().map(PeerInfo::getPeerAddressAndPort))
            .containsExactly(address("127.0.0.2", 7000));
        // the directory is archived by the load itself
        assertThat(Files.exists(nodes)).isFalse();
        assertThat(Files.exists(tempDir.resolve("nodes.migrated").resolve("peers"))).isTrue();
    }

    @Test
    public void theDseImportInDse6ModeOrWithAMarker() throws Exception
    {
        Path nodes = DseFixtures.installNodesDirectory(tempDir, DseVersion.DSE_6_9_26, false);
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        assertThat(Dse6UpgradeNodesPersistence.handlesNodesDirectory()).isTrue();
        assertThat(Nodes.nodesDirectoryImport()).isExactlyInstanceOf(Dse6UpgradeNodesPersistence.class);

        // the flag dropped: a directory under a marker's name is no marker, a file of any host id is one
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.NONE);
        Path notAMarker = Files.createDirectory(nodes.resolve(Dse6UpgradeNodesPersistence.markerName(HOST_6926)));
        assertThat(Dse6UpgradeNodesPersistence.handlesNodesDirectory()).isFalse();
        assertThat(Nodes.nodesDirectoryImport()).isExactlyInstanceOf(CC4UpgradeNodesPersistence.class);
        Files.delete(notAMarker);
        Files.write(nodes.resolve(Dse6UpgradeNodesPersistence.markerName(UUID.randomUUID())), new byte[0]);
        assertThat(Dse6UpgradeNodesPersistence.handlesNodesDirectory()).isTrue();
        assertThat(Nodes.nodesDirectoryImport()).isExactlyInstanceOf(Dse6UpgradeNodesPersistence.class);
    }

    @Test
    public void aNodesDirectoryThatCannotBeListedFailsTheChoice() throws Exception
    {
        Assume.assumeTrue("POSIX permissions",
                          FileSystems.getDefault().supportedFileAttributeViews().contains("posix"));
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.NONE);
        Path nodes = DseFixtures.installNodesDirectory(tempDir, DseVersion.DSE_6_9_26, false);
        Set<PosixFilePermission> before = Files.getPosixFilePermissions(nodes);
        // searchable but not readable: whether it holds a marker cannot be told
        Files.setPosixFilePermissions(nodes, PosixFilePermissions.fromString("-wx------"));
        try
        {
            Assume.assumeFalse("the directory stays readable (run as root?)", Files.isReadable(nodes));
            assertThatThrownBy(Nodes::nodesDirectoryImport).isInstanceOf(RuntimeException.class)
                                                           .hasMessage("Failed to list " + nodes);
        }
        finally
        {
            Files.setPosixFilePermissions(nodes, before);
        }
    }

    @Test
    public void withTheFlagDroppedTheLoadsNeverFlushAndArchiveInline() throws Exception
    {
        Path nodes = DseFixtures.installNodesDirectory(tempDir, DseVersion.DSE_6_9_26, false);
        // a marker that no longer matches the files (DSE ran after the import), then the flag dropped
        Path marker = nodes.resolve(Dse6UpgradeNodesPersistence.markerName(HOST_6926));
        Files.write(marker, "local=absent\npeers=absent\n".getBytes(StandardCharsets.UTF_8));
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.NONE);
        assertThat(Dse6UpgradeNodesPersistence.importState()).isEqualTo(ImportState.STALE);

        // the loads run while Nodes is initialised, when no flush can end: with the flag dropped they flush nothing
        // at all and archive the directory themselves, as the CC4 import does
        DeferredSyncPersistence persistence = new DeferredSyncPersistence(() -> false);
        try
        {
            assertThat(persistence.loadLocal().getHostId()).isEqualTo(HOST_6926);
            assertThat(Files.isDirectory(nodes)).isTrue();
            assertThat(persistence.loadPeers().map(PeerInfo::getPeerAddressAndPort))
                .containsExactly(address("127.0.0.2", 7000));
            assertThat(Files.exists(nodes)).isFalse();
            assertThat(Files.exists(tempDir.resolve("nodes.migrated").resolve("local"))).isTrue();
        }
        finally
        {
            persistence.loadsReturned();
        }
        // a completion scheduled by mistake with the flag dropped would flush now that the loads returned: none may
        // have run
        Dse6UpgradeNodesPersistence.awaitPendingCompletion();
        assertThat(persistence.flushes).isEmpty();
    }

    private static InetAddressAndPort address(String host, int port) throws Exception
    {
        return InetAddressAndPort.getByNameOverrideDefaults(host, port);
    }
}
