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
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.InternodeCompatibilityMode;
import org.apache.cassandra.config.StartupChecksOptions;
import org.apache.cassandra.db.Dse51SystemTables;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.dsecompat.Dse51Fixtures;
import org.apache.cassandra.dsecompat.DseFixtures;
import org.apache.cassandra.exceptions.StartupException;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.ULIDBasedSSTableId;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.locator.Ec2Snitch;
import org.apache.cassandra.locator.Ec2Snitches;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.DseCompat;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JsonUtils;

import static org.apache.cassandra.config.CassandraRelevantProperties.INTERNODE_COMPATIBILITY_MODE_DSE_VERSION;
import static org.apache.cassandra.config.CassandraRelevantProperties.INTERNODE_COMPATIBILITY_MODE_IGNORE_DSE_PEERS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The startup checks with {@code internode_compatibility_mode: DSE_5_1}: what a start in the mode refuses to find on
 * disk (what the node wrote without the mode), the marker of the upgrade from DSE 5.1 it writes once every check has
 * passed, the refusal of a start with {@code NONE} while that marker or the record of DSE 5.1 peers exists, the warning
 * about the standard EC2 names, and the DSE version the node gossips to DSE 5.1 nodes. The checks of {@code DSE_6} are
 * in {@link Dse6StartupChecksTest}; the cases here that involve {@code DSE_6} show the refusal of a start in that mode
 * while the marker of the upgrade from DSE 5.1 is present or its record lists a peer, and that nothing else of its
 * behaviour depends on those files. Every test works in data, commit log and metadata directories of its own.
 */
public class Dse51StartupChecksTest
{
    private static final StartupChecksOptions OPTIONS = new StartupChecksOptions();
    private static final String REFUSAL = "cannot enter DSE_5_1 compatibility mode: the node already ran without it, " +
                                          "these files could not be read correctly, or at all, by DSE 5.1 after a " +
                                          "rollback: ";

    private File[] dataDirectoriesBefore;
    private File commitLogBefore;
    private File metadataBefore;
    private InternodeCompatibilityMode modeBefore;

    private Path root;
    private Path sstableDir;
    private Path commitLogDir;

    @BeforeClass
    public static void init()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Before
    public void setUp() throws IOException
    {
        modeBefore = DatabaseDescriptor.getInternodeCompatibilityMode();
        dataDirectoriesBefore = DatabaseDescriptor.getAllDataFileLocations();
        commitLogBefore = DatabaseDescriptor.getCommitLogLocation();
        metadataBefore = DatabaseDescriptor.getMetadataDirectory();

        root = Files.createTempDirectory("dse51checks");
        Path dataDir = Files.createDirectories(root.resolve("data"));
        sstableDir = Files.createDirectories(dataDir.resolve("ks").resolve("t-269b3740b8d411f1acb39d12a1a4d70a"));
        commitLogDir = Files.createDirectories(root.resolve("commitlog"));
        DatabaseDescriptor.setDataDirectories(new File[]{ new File(dataDir) });
        DatabaseDescriptor.setCommitLogLocation(new File(commitLogDir));
        DatabaseDescriptor.setMetadataDirectory(new File(Files.createDirectories(root.resolve("metadata"))));
    }

    @After
    public void tearDown()
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(modeBefore);
        DatabaseDescriptor.setDataDirectories(dataDirectoriesBefore);
        DatabaseDescriptor.setCommitLogLocation(commitLogBefore);
        DatabaseDescriptor.setMetadataDirectory(metadataBefore);
        new File(root).deleteRecursive();
    }

    private static void touch(Path dir, String... names) throws IOException
    {
        for (String name : names)
            Files.write(dir.resolve(name), new byte[0]);
    }

    private static void mode(InternodeCompatibilityMode mode)
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(mode);
    }

    private interface CheckedRunnable
    {
        void run() throws Exception;
    }

    /** Runs {@code action} and returns the messages {@link StartupChecks} logged at {@code level} meanwhile. */
    private static List<String> logsOf(Level level, CheckedRunnable action) throws Exception
    {
        return eventsOf(action).stream()
                               .filter(e -> e.getLevel() == level)
                               .map(ILoggingEvent::getFormattedMessage)
                               .collect(Collectors.toList());
    }

    /** Runs {@code action} and returns what {@link StartupChecks} logged meanwhile. */
    private static List<ILoggingEvent> eventsOf(CheckedRunnable action) throws Exception
    {
        Logger logger = (Logger) LoggerFactory.getLogger(StartupChecks.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
        try
        {
            action.run();
        }
        finally
        {
            logger.detachAppender(appender);
        }
        return appender.list;
    }

    /** Runs the artefact check and its post-action the way {@link StartupChecks#verify} does. */
    private static void verifyArtefacts() throws StartupException
    {
        new StartupChecks().withTest(StartupChecks.checkDse6Artefacts).verify(OPTIONS);
    }

    private static void writePeersRecord(InternodeCompatibilityMode source, String... lines) throws IOException
    {
        Files.createDirectories(DsePeersRecord.path(source).getParent());
        Files.write(DsePeersRecord.path(source), String.join("\n", lines).getBytes(StandardCharsets.UTF_8));
    }

    @Test
    public void dse51AcceptsWhatItWritesAndWhatDse51WroteAndWritesItsMarker() throws Exception
    {
        mode(InternodeCompatibilityMode.DSE_5_1);
        // what DSE 5.1 wrote, and what the node and the offline tools write in the mode (big me), and older big
        // versions DSE 5.1 still reads; commit log segments of DSE 5.1 (version 6) and of the node in the mode
        // (version 7)
        touch(sstableDir, "me-1-big-Data.db", "me-1-big-TOC.txt", "mc-2-big-Data.db", "ma-3-big-Data.db",
              "me-4-big-Data.db");
        touch(commitLogDir, "CommitLog-6-1.log", "CommitLog-7-2.log");

        StartupChecks.checkDse6Artefacts.execute(OPTIONS);
        assertThat(DseUpgradeMarker.exists(InternodeCompatibilityMode.DSE_5_1))
        .as("execute() must not write the marker").isFalse();

        Path marker = DseUpgradeMarker.path(InternodeCompatibilityMode.DSE_5_1);
        assertThat(marker.getFileName().toString()).isEqualTo("dse51-upgrade-in-progress");
        assertThat(logsOf(Level.INFO, Dse51StartupChecksTest::verifyArtefacts))
        .containsExactly("Wrote the DSE 5.1 upgrade marker " + marker);
        String content = Files.readString(marker);
        assertThat(content).startsWith("DSE 5.1 in-place upgrade in progress on this node since ")
                           .endsWith("). Delete this file only after every node of the cluster runs this software, " +
                                     "before restarting with internode_compatibility_mode: NONE.\n");
        // the marker of the upgrade from DSE 6 is not written
        assertThat(DseUpgradeMarker.exists()).isFalse();

        // idempotent: a second start keeps the marker as it is, and says nothing about it
        assertThat(logsOf(Level.INFO, Dse51StartupChecksTest::verifyArtefacts)).isEmpty();
        assertThat(Files.readString(marker)).isEqualTo(content);
        assertThat(DseUpgradeMarker.writeIfAbsent(InternodeCompatibilityMode.DSE_5_1)).isFalse();
    }

    @Test
    public void dse51RefusesWhatItDoesNotWrite() throws Exception
    {
        mode(InternodeCompatibilityMode.DSE_5_1);
        String ulid = ULIDBasedSSTableId.Builder.instance.generator(Stream.empty()).get().toString();
        touch(sstableDir, "me-1-big-Data.db", "nb-2-big-Data.db", "oa-7-big-Data.db", "pa-3-big-Data.db",
              "aa-4-bti-Data.db",
              "bb-5-bti-Data.db", "me-6-xyz-Data.db", "nb-3h43_18di_43ocw25niiw1m23p1r-big-Data.db",
              "me-" + ulid + "-big-Data.db");
        touch(commitLogDir, "CommitLog-6-1.log", "CommitLog-7-2.log", "CommitLog-8-3.log", "CommitLog-110-4.log",
              "CommitLog-680-5.log");
        assertThatThrownBy(() -> StartupChecks.checkDse6Artefacts.execute(OPTIONS))
        .isInstanceOf(StartupException.class)
        .hasMessageStartingWith(REFUSAL)
        .hasMessageContaining("nb-2-big-Data.db")
        .hasMessageContaining("oa-7-big-Data.db")
        .hasMessageContaining("pa-3-big-Data.db")
        .hasMessageContaining("aa-4-bti-Data.db")
        .hasMessageContaining("bb-5-bti-Data.db")
        .hasMessageContaining("me-6-xyz-Data.db")
        .hasMessageContaining("nb-3h43_18di_43ocw25niiw1m23p1r-big-Data.db")
        .hasMessageContaining("me-" + ulid + "-big-Data.db")
        .hasMessageContaining("CommitLog-8-3.log")
        .hasMessageContaining("CommitLog-110-4.log")
        .hasMessageContaining("CommitLog-680-5.log")
        .hasMessageNotContaining("me-1-big-Data.db")
        .hasMessageNotContaining("CommitLog-6-1.log")
        .hasMessageNotContaining("CommitLog-7-2.log")
        .hasMessageEndingWith(". Restore the node from its pre-upgrade snapshot. Deleting these files instead loses " +
                              "this node's copy of their data: do it only if that data is replicated on other nodes, " +
                              "and repair the node once the compatibility mode is dropped")
        .satisfies(e -> assertThat(((StartupException) e).returnCode).isEqualTo(StartupException.ERR_WRONG_DISK_STATE));
        assertThat(DseUpgradeMarker.exists(InternodeCompatibilityMode.DSE_5_1)).isFalse();
    }

    @Test
    public void theRulesOfEachModeDiffer()
    {
        // big me, nb and oa, every trie version, numeric and other identifiers, names that do not parse
        assertThat(StartupChecks.isSSTableWrittenWithoutDse51Mode("me-1-big-Data.db")).isFalse();
        assertThat(StartupChecks.isSSTableUnreadableByDse6("me-1-big-Data.db")).isFalse();
        assertThat(StartupChecks.isSSTableWrittenWithoutDse51Mode("nb-1-big-Data.db")).isTrue();
        assertThat(StartupChecks.isSSTableUnreadableByDse6("nb-1-big-Data.db")).isTrue();
        assertThat(StartupChecks.isSSTableWrittenWithoutDse51Mode("bb-1-bti-Data.db")).isTrue();
        assertThat(StartupChecks.isSSTableUnreadableByDse6("bb-1-bti-Data.db")).isFalse();
        assertThat(StartupChecks.isSSTableWrittenWithoutDse51Mode("oa-1-big-Data.db")).isTrue();
        assertThat(StartupChecks.isSSTableWrittenWithoutDse51Mode("pa-1-big-Data.db")).isTrue();
        assertThat(StartupChecks.isSSTableWrittenWithoutDse51Mode("ka-1-big-Data.db")).isFalse();
        assertThat(StartupChecks.isSSTableWrittenWithoutDse51Mode("ks-t-ka-1-Data.db")).isFalse();
        assertThat(StartupChecks.isSSTableWrittenWithoutDse51Mode("B1-1-big-Data.db")).isFalse();
        assertThat(StartupChecks.isSSTableWrittenWithoutDse51Mode("me-not!an!id-big-Data.db")).isFalse();
        // commit log versions: DSE 6's 680 is refused in DSE_5_1 only
        assertThat(StartupChecks.isCommitLogSegmentWrittenWithoutDse51Mode("CommitLog-680-1.log")).isTrue();
        assertThat(StartupChecks.isCommitLogSegmentWrittenWithoutDse6Mode("CommitLog-680-1.log")).isFalse();
        assertThat(StartupChecks.isCommitLogSegmentWrittenWithoutDse51Mode("CommitLog-6-1.log")).isFalse();
        assertThat(StartupChecks.isCommitLogSegmentWrittenWithoutDse51Mode("CommitLog-1234567890123.log")).isFalse();
        assertThat(StartupChecks.isCommitLogSegmentWrittenWithoutDse51Mode("CommitLog-99999999999-1.log")).isFalse();
    }

    /**
     * The names of the files DSE 5.1.51 wrote ({@link Dse51Fixtures}: its sstables of every kind and its commit log
     * segments): none of them is taken for one written without the mode, while each, made a trie sstable or given
     * DSE 6's commit log version, is.
     */
    @Test
    public void theFilesDse51WroteAreNotFlagged() throws IOException
    {
        List<String> sstables;
        List<String> segments;
        try (Stream<Path> files = Files.walk(DseFixtures.ROOT))
        {
            List<String> names = files.filter(Files::isRegularFile)
                                      .filter(Dse51Fixtures::isDse51Fixture)
                                      .map(p -> p.getFileName().toString()).collect(Collectors.toList());
            sstables = names.stream().filter(n -> Descriptor.isValidFile(new File(n))).collect(Collectors.toList());
            // the fixtures keep the segments compressed
            segments = names.stream().filter(n -> n.startsWith("CommitLog-")).map(n -> n.replaceFirst("\\.gz$", ""))
                            .collect(Collectors.toList());
        }
        assertThat(sstables).isNotEmpty().allMatch(n -> n.startsWith(Dse51Fixtures.SSTABLE_VERSION + '-') &&
                                                        n.contains("-big-"));
        assertThat(segments).isNotEmpty().allMatch(n -> n.startsWith("CommitLog-" +
                                                                     Dse51Fixtures.COMMIT_LOG_VERSION + '-'));
        assertThat(sstables).allMatch(n -> n.split("-").length == 4, "four parts");
        assertThat(sstables).noneMatch(StartupChecks::isSSTableWrittenWithoutDse51Mode);
        assertThat(segments).noneMatch(StartupChecks::isCommitLogSegmentWrittenWithoutDse51Mode);
        // the names reach the classification: the same names of another format or version are refused
        assertThat(sstables).allMatch(n -> StartupChecks.isSSTableWrittenWithoutDse51Mode(n.replace("-big-", "-bti-")));
        assertThat(sstables).allMatch(n -> StartupChecks.isSSTableWrittenWithoutDse51Mode("nb" + n.substring(2)));
        assertThat(segments).allMatch(n -> StartupChecks.isCommitLogSegmentWrittenWithoutDse51Mode(
            n.replaceFirst("^CommitLog-" + Dse51Fixtures.COMMIT_LOG_VERSION + '-', "CommitLog-680-")));
    }

    @Test
    public void noneIsRefusedWhileTheDse51MarkerExists() throws Exception
    {
        assertThat(DseUpgradeMarker.writeIfAbsent(InternodeCompatibilityMode.DSE_5_1)).isTrue();
        mode(InternodeCompatibilityMode.NONE);
        Path marker = DseUpgradeMarker.path(InternodeCompatibilityMode.DSE_5_1);
        assertThatThrownBy(() -> StartupChecks.checkDsePeersWithoutFlag.execute(OPTIONS))
        .isInstanceOf(StartupException.class)
        .hasMessage("internode_compatibility_mode is NONE but the DSE 5.1 upgrade marker " + marker + " is present: " +
                    "DSE 5.1 peers may still be in the cluster. Set internode_compatibility_mode: " +
                    "DSE_5_1 with storage_compatibility_mode: DSE_5_1 and sstable.selected_format: big, or once " +
                    "every node runs this software, delete the marker, or start with " +
                    "-Dcassandra.internode_compatibility_mode.ignore_dse_peers=true")
        .satisfies(e -> assertThat(((StartupException) e).returnCode).isEqualTo(StartupException.ERR_WRONG_CONFIG));

        try (WithProperties properties = new WithProperties().set(INTERNODE_COMPATIBILITY_MODE_IGNORE_DSE_PEERS, true))
        {
            assertThat(logsOf(Level.WARN, () -> StartupChecks.checkDsePeersWithoutFlag.execute(OPTIONS)))
            .containsExactly("The DSE 5.1 upgrade marker " + marker + " is present but ignored " +
                             "(cassandra.internode_compatibility_mode.ignore_dse_peers=true); DSE 5.1 peers, if any, " +
                             "will be unreachable");
        }

        assertThat(DseUpgradeMarker.delete(InternodeCompatibilityMode.DSE_5_1)).isTrue();
        assertThat(logsOf(Level.WARN, () -> StartupChecks.checkDsePeersWithoutFlag.execute(OPTIONS))).isEmpty();
    }

    @Test
    public void noneIsRefusedWhileDse51PeersAreOnRecord() throws Exception
    {
        writePeersRecord(InternodeCompatibilityMode.DSE_5_1, "# peers", "10.0.0.2:7000", "10.0.0.3:not-a-port");
        Path record = DsePeersRecord.path(InternodeCompatibilityMode.DSE_5_1);
        assertThat(record.getFileName().toString()).isEqualTo("dse51-peers-seen");
        mode(InternodeCompatibilityMode.NONE);
        assertThatThrownBy(() -> StartupChecks.checkDsePeersWithoutFlag.execute(OPTIONS))
        .isInstanceOf(StartupException.class)
        .hasMessage("internode_compatibility_mode is NONE but the record of DSE 5.1 peers " + record +
                    " listing [10.0.0.2:7000, 10.0.0.3:not-a-port] is present: DSE 5.1 peers may still be in the " +
                    "cluster. Set internode_compatibility_mode: DSE_5_1 with storage_compatibility_mode: DSE_5_1 " +
                    "and sstable.selected_format: big, or the record empties itself once this node has seen every " +
                    "listed peer at messaging version 4.0 or later (run once more with DSE_5_1 after the last " +
                    "upgrade); delete it by hand only after checking that the listed peers have left the cluster; " +
                    "the entries [10.0.0.3:not-a-port] are not addresses with port (<ip>:<port>) and must be " +
                    "removed from it by hand, or start with " +
                    "-Dcassandra.internode_compatibility_mode.ignore_dse_peers=true");

        // a record that cannot be read refuses the start too
        Files.delete(record);
        Files.createDirectories(record);
        assertThatThrownBy(() -> StartupChecks.checkDsePeersWithoutFlag.execute(OPTIONS))
        .isInstanceOf(StartupException.class)
        .hasMessageStartingWith("internode_compatibility_mode is NONE but the record of DSE 5.1 peers " + record +
                                " cannot be read (")
        .hasMessageContaining("): DSE 5.1 peers may still be in the cluster. Set internode_compatibility_mode: " +
                              "DSE_5_1 with storage_compatibility_mode: DSE_5_1 and sstable.selected_format: big, " +
                              "or make the record readable, or start with ");
    }

    @Test
    public void noneStartsDespiteTheDse51RecordWithTheOverride() throws Exception
    {
        writePeersRecord(InternodeCompatibilityMode.DSE_5_1, "10.0.0.2:7000");
        mode(InternodeCompatibilityMode.NONE);
        try (WithProperties properties = new WithProperties().set(INTERNODE_COMPATIBILITY_MODE_IGNORE_DSE_PEERS, true))
        {
            assertThat(logsOf(Level.WARN, () -> StartupChecks.checkDsePeersWithoutFlag.execute(OPTIONS)))
            .containsExactly("The record of DSE 5.1 peers " + DsePeersRecord.path(InternodeCompatibilityMode.DSE_5_1) +
                             " listing [10.0.0.2:7000] is present but ignored " +
                             "(cassandra.internode_compatibility_mode.ignore_dse_peers=true); DSE 5.1 peers, if any, " +
                             "will be unreachable");
        }
    }

    /** Snapshots and backups are not inspected in DSE_5_1 either. */
    @Test
    public void dse51IgnoresSnapshotsAndBackups() throws Exception
    {
        mode(InternodeCompatibilityMode.DSE_5_1);
        Path snapshots = Files.createDirectories(sstableDir.resolve("snapshots").resolve("pre-upgrade"));
        Path backups = Files.createDirectories(sstableDir.resolve("backups"));
        touch(snapshots, "cc-1-bti-Data.db");
        touch(backups, "pa-1-big-Data.db");
        verifyArtefacts();
        assertThat(DseUpgradeMarker.exists(InternodeCompatibilityMode.DSE_5_1)).isTrue();
    }

    @Test
    public void dse51IsNotRefusedByItsOwnMarkerAndRecord() throws Exception
    {
        assertThat(DseUpgradeMarker.writeIfAbsent(InternodeCompatibilityMode.DSE_5_1)).isTrue();
        writePeersRecord(InternodeCompatibilityMode.DSE_5_1, "10.0.0.2:7000");
        mode(InternodeCompatibilityMode.DSE_5_1);
        assertThat(logsOf(Level.WARN, () -> StartupChecks.checkDsePeersWithoutFlag.execute(OPTIONS))).isEmpty();
        assertThat(DsePeersRecord.persistedPeers(InternodeCompatibilityMode.DSE_5_1)).containsExactly("10.0.0.2:7000");

        // the record of DSE 6 peers still refuses it
        writePeersRecord(InternodeCompatibilityMode.DSE_6, "10.0.0.4:7000");
        assertThatThrownBy(() -> StartupChecks.checkDsePeersWithoutFlag.execute(OPTIONS))
        .isInstanceOf(StartupException.class)
        .hasMessageStartingWith("internode_compatibility_mode is DSE_5_1 but the record of DSE 6 peers " +
                                DsePeersRecord.path() + " listing [10.0.0.4:7000] is present: DSE 6 peers may still " +
                                "be in the cluster. Set internode_compatibility_mode: DSE_6, or ");

        // and so does the DSE 6 marker
        Files.delete(DsePeersRecord.path());
        assertThat(DseUpgradeMarker.writeIfAbsent()).isTrue();
        assertThatThrownBy(() -> StartupChecks.checkDsePeersWithoutFlag.execute(OPTIONS))
        .isInstanceOf(StartupException.class)
        .hasMessageStartingWith("internode_compatibility_mode is DSE_5_1 but the DSE 6 upgrade marker " +
                                DseUpgradeMarker.path() + " is present: DSE 6 peers may still be in the cluster.");
    }

    /**
     * DSE_6 is refused while the marker of the upgrade from DSE 5.1 is present: the node took part in that upgrade, and
     * DSE_6 would speak DSE 6's dialect to its DSE 5.1 peers. The text names the file and the remedies; the override
     * logs it at WARN instead, and the start then writes the DSE 6 marker and leaves the DSE 5.1 one as it is.
     */
    @Test
    public void dse6IsRefusedWhileTheDse51MarkerExists() throws Exception
    {
        assertThat(DseUpgradeMarker.writeIfAbsent(InternodeCompatibilityMode.DSE_5_1)).isTrue();
        Path marker = DseUpgradeMarker.path(InternodeCompatibilityMode.DSE_5_1);
        byte[] markerBytes = Files.readAllBytes(marker);
        mode(InternodeCompatibilityMode.DSE_6);
        assertThatThrownBy(() -> StartupChecks.checkDsePeersWithoutFlag.execute(OPTIONS))
        .isInstanceOf(StartupException.class)
        .hasMessage("internode_compatibility_mode is DSE_6 but the DSE 5.1 upgrade marker " + marker + " is present: " +
                    "DSE 5.1 peers may still be in the cluster. Set internode_compatibility_mode: " +
                    "DSE_5_1 with storage_compatibility_mode: DSE_5_1 and sstable.selected_format: big, or once " +
                    "every node runs this software, delete the marker, or start with " +
                    "-Dcassandra.internode_compatibility_mode.ignore_dse_peers=true")
        .satisfies(e -> assertThat(((StartupException) e).returnCode).isEqualTo(StartupException.ERR_WRONG_CONFIG));

        try (WithProperties properties = new WithProperties().set(INTERNODE_COMPATIBILITY_MODE_IGNORE_DSE_PEERS, true))
        {
            assertThat(logsOf(Level.WARN, () -> StartupChecks.checkDsePeersWithoutFlag.execute(OPTIONS)))
            .containsExactly("The DSE 5.1 upgrade marker " + marker + " is present but ignored " +
                             "(cassandra.internode_compatibility_mode.ignore_dse_peers=true); DSE 5.1 peers, if any, " +
                             "will be spoken to as DSE 6 nodes");
            assertThat(logsOf(Level.INFO, Dse51StartupChecksTest::verifyArtefacts))
            .containsExactly("Wrote the DSE 6 upgrade marker " + DseUpgradeMarker.path());
        }
        assertThat(Files.readAllBytes(marker)).isEqualTo(markerBytes);

        // that DSE 6 marker then refuses the start in DSE_5_1
        mode(InternodeCompatibilityMode.DSE_5_1);
        assertThatThrownBy(() -> StartupChecks.checkDsePeersWithoutFlag.execute(OPTIONS))
        .isInstanceOf(StartupException.class)
        .hasMessage("internode_compatibility_mode is DSE_5_1 but the DSE 6 upgrade marker " + DseUpgradeMarker.path() +
                    " is present: DSE 6 peers may still be in the cluster. Set internode_compatibility_mode: " +
                    "DSE_6, or once every node runs this software, delete the marker, or start with " +
                    "-Dcassandra.internode_compatibility_mode.ignore_dse_peers=true")
        .satisfies(e -> assertThat(((StartupException) e).returnCode).isEqualTo(StartupException.ERR_WRONG_CONFIG));
    }

    /**
     * DSE_6 is refused while the record of DSE 5.1 peers lists a peer, or is there and cannot be read; with both files
     * the refusal names both. The override logs the record at WARN instead.
     */
    @Test
    public void dse6IsRefusedWhileDse51PeersAreOnRecord() throws Exception
    {
        writePeersRecord(InternodeCompatibilityMode.DSE_5_1, "# peers", "10.0.0.2:7000");
        Path record = DsePeersRecord.path(InternodeCompatibilityMode.DSE_5_1);
        mode(InternodeCompatibilityMode.DSE_6);
        String recordRemedy = "the record empties itself once this node has seen every listed peer at messaging " +
                              "version 4.0 or later (run once more with DSE_5_1 after the last upgrade); delete it " +
                              "by hand only after checking that the listed peers have left the cluster, or start " +
                              "with -Dcassandra.internode_compatibility_mode.ignore_dse_peers=true";
        assertThatThrownBy(() -> StartupChecks.checkDsePeersWithoutFlag.execute(OPTIONS))
        .isInstanceOf(StartupException.class)
        .hasMessage("internode_compatibility_mode is DSE_6 but the record of DSE 5.1 peers " + record +
                    " listing [10.0.0.2:7000] is present: DSE 5.1 peers may still be in the cluster. Set " +
                    "internode_compatibility_mode: DSE_5_1 with storage_compatibility_mode: DSE_5_1 and " +
                    "sstable.selected_format: big, or " + recordRemedy)
        .satisfies(e -> assertThat(((StartupException) e).returnCode).isEqualTo(StartupException.ERR_WRONG_CONFIG));

        try (WithProperties properties = new WithProperties().set(INTERNODE_COMPATIBILITY_MODE_IGNORE_DSE_PEERS, true))
        {
            assertThat(logsOf(Level.WARN, () -> StartupChecks.checkDsePeersWithoutFlag.execute(OPTIONS)))
            .containsExactly("The record of DSE 5.1 peers " + record + " listing [10.0.0.2:7000] is present but " +
                             "ignored (cassandra.internode_compatibility_mode.ignore_dse_peers=true); DSE 5.1 peers, " +
                             "if any, will be spoken to as DSE 6 nodes");
        }

        assertThat(DseUpgradeMarker.writeIfAbsent(InternodeCompatibilityMode.DSE_5_1)).isTrue();
        Path marker = DseUpgradeMarker.path(InternodeCompatibilityMode.DSE_5_1);
        assertThatThrownBy(() -> StartupChecks.checkDsePeersWithoutFlag.execute(OPTIONS))
        .isInstanceOf(StartupException.class)
        .hasMessage("internode_compatibility_mode is DSE_6 but the DSE 5.1 upgrade marker " + marker + " is present " +
                    "and the record of DSE 5.1 peers " + record + " listing [10.0.0.2:7000] is present: DSE 5.1 " +
                    "peers may still be in the cluster. Set internode_compatibility_mode: DSE_5_1 with " +
                    "storage_compatibility_mode: DSE_5_1 and sstable.selected_format: big, or once every node runs " +
                    "this software, delete the marker; " + recordRemedy);
        assertThat(DseUpgradeMarker.delete(InternodeCompatibilityMode.DSE_5_1)).isTrue();

        Files.delete(record);
        Files.createDirectories(record);
        assertThatThrownBy(() -> StartupChecks.checkDsePeersWithoutFlag.execute(OPTIONS))
        .isInstanceOf(StartupException.class)
        .hasMessageStartingWith("internode_compatibility_mode is DSE_6 but the record of DSE 5.1 peers " + record +
                                " cannot be read (")
        .hasMessageContaining("): DSE 5.1 peers may still be in the cluster. Set internode_compatibility_mode: " +
                              "DSE_5_1 with storage_compatibility_mode: DSE_5_1 and sstable.selected_format: big, " +
                              "or make the record readable, or start with ")
        .satisfies(e -> assertThat(((StartupException) e).returnCode).isEqualTo(StartupException.ERR_WRONG_CONFIG));
        try (WithProperties properties = new WithProperties().set(INTERNODE_COMPATIBILITY_MODE_IGNORE_DSE_PEERS, true))
        {
            assertThat(logsOf(Level.WARN, () -> StartupChecks.checkDsePeersWithoutFlag.execute(OPTIONS)))
            .singleElement().asString()
            .startsWith("The record of DSE 5.1 peers " + record + " cannot be read (")
            .endsWith(") but ignored (cassandra.internode_compatibility_mode.ignore_dse_peers=true); DSE 5.1 peers, " +
                      "if any, will be spoken to as DSE 6 nodes");
        }
    }

    /**
     * DSE_6 starts with neither file of the upgrade from DSE 5.1, or with a record that lists no peer, as a node that
     * never ran in DSE_5_1 does; one whose metadata directory is not a directory too, the presence of the files then
     * unknown. It writes its own marker only, and leaves the record as it is.
     */
    @Test
    public void dse6StartsWithoutTheDse51FilesOrWithAnEmptyRecord() throws Exception
    {
        mode(InternodeCompatibilityMode.DSE_6);
        assertThat(logsOf(Level.WARN, () -> StartupChecks.checkDsePeersWithoutFlag.execute(OPTIONS))).isEmpty();

        writePeersRecord(InternodeCompatibilityMode.DSE_5_1, "# peers");
        byte[] record = Files.readAllBytes(DsePeersRecord.path(InternodeCompatibilityMode.DSE_5_1));
        assertThat(logsOf(Level.WARN, () -> StartupChecks.checkDsePeersWithoutFlag.execute(OPTIONS))).isEmpty();
        assertThat(logsOf(Level.INFO, Dse51StartupChecksTest::verifyArtefacts))
        .containsExactly("Wrote the DSE 6 upgrade marker " + DseUpgradeMarker.path());
        assertThat(DseUpgradeMarker.path(InternodeCompatibilityMode.DSE_5_1)).doesNotExist();
        assertThat(Files.readAllBytes(DsePeersRecord.path(InternodeCompatibilityMode.DSE_5_1))).isEqualTo(record);

        // a metadata directory that is a file: the presence of either file is unknown, which does not refuse DSE_6
        // (Files.exists is false there, as for a directory that cannot be searched)
        DatabaseDescriptor.setMetadataDirectory(new File(Files.createFile(root.resolve("not-a-directory"))));
        assertThat(DseUpgradeMarker.exists(InternodeCompatibilityMode.DSE_5_1)).isTrue();
        assertThat(Files.exists(DseUpgradeMarker.path(InternodeCompatibilityMode.DSE_5_1))).isFalse();
        assertThat(Files.exists(DsePeersRecord.path(InternodeCompatibilityMode.DSE_5_1))).isFalse();
        assertThat(logsOf(Level.WARN, () -> StartupChecks.checkDsePeersWithoutFlag.execute(OPTIONS))).isEmpty();
    }

    /**
     * DSE_6 starts with the marker of the upgrade from DSE 5.1 in a metadata directory that cannot be searched, where
     * {@code Files.exists} is false: the presence of the marker is unknown. Skipped where the permissions do not apply
     * (for root).
     */
    @Test
    public void dse6StartsWhenTheMetadataDirectoryCannotBeSearched() throws Exception
    {
        mode(InternodeCompatibilityMode.DSE_6);
        assertThat(DseUpgradeMarker.writeIfAbsent(InternodeCompatibilityMode.DSE_5_1)).isTrue();
        Path metadata = DatabaseDescriptor.getMetadataDirectory().toPath();
        Set<PosixFilePermission> permissions = Files.getPosixFilePermissions(metadata);
        Files.setPosixFilePermissions(metadata, PosixFilePermissions.fromString("rw-------"));
        try
        {
            Assume.assumeFalse("the permissions of the metadata directory apply",
                               Files.exists(DseUpgradeMarker.path(InternodeCompatibilityMode.DSE_5_1)));
            assertThat(DseUpgradeMarker.exists(InternodeCompatibilityMode.DSE_5_1)).isTrue();
            assertThat(logsOf(Level.WARN, () -> StartupChecks.checkDsePeersWithoutFlag.execute(OPTIONS))).isEmpty();
        }
        finally
        {
            Files.setPosixFilePermissions(metadata, permissions);
        }
    }

    /**
     * A DSE 5.1 marker whose content cannot be written but whose file exists is logged at WARN, naming the two starts
     * it refuses.
     */
    @Test
    public void aDse51MarkerWhoseContentCannotBeWrittenIsLogged() throws Exception
    {
        Path marker = DseUpgradeMarker.path(InternodeCompatibilityMode.DSE_5_1);
        List<ILoggingEvent> events = eventsOf(() -> StartupChecks.writeUpgradeMarker(
            InternodeCompatibilityMode.DSE_5_1, () -> {
                Files.createFile(marker);
                throw new IOException("disk full");
            }));
        assertThat(events).filteredOn(e -> e.getLevel() == Level.ERROR).isEmpty();
        assertThat(events).filteredOn(e -> e.getLevel() == Level.WARN)
                          .singleElement()
                          .satisfies(e -> {
                              assertThat(e.getFormattedMessage())
                              .isEqualTo("Unable to write the content of the DSE 5.1 upgrade marker " + marker +
                                         "; the file exists, which is all that matters: a later start with " +
                                         "internode_compatibility_mode NONE or DSE_6 is refused while it does");
                              assertThat(e.getThrowableProxy().getMessage()).isEqualTo("disk full");
                          });
        assertThat(DseUpgradeMarker.exists(InternodeCompatibilityMode.DSE_5_1)).isTrue();
    }

    /** With the files of both upgrades, NONE is refused for the DSE 6 ones first, and the override logs one WARN. */
    @Test
    public void theFilesOfTheUpgradeFromDse6AreCheckedFirst() throws Exception
    {
        assertThat(DseUpgradeMarker.writeIfAbsent(InternodeCompatibilityMode.DSE_5_1)).isTrue();
        assertThat(DseUpgradeMarker.writeIfAbsent(InternodeCompatibilityMode.DSE_6)).isTrue();
        mode(InternodeCompatibilityMode.NONE);
        assertThatThrownBy(() -> StartupChecks.checkDsePeersWithoutFlag.execute(OPTIONS))
        .isInstanceOf(StartupException.class)
        .hasMessage("internode_compatibility_mode is NONE but the DSE 6 upgrade marker " + DseUpgradeMarker.path() +
                    " is present: DSE 6 peers may still be in the cluster. Set internode_compatibility_mode: DSE_6, " +
                    "or once every node runs this software, delete the marker, or start with " +
                    "-Dcassandra.internode_compatibility_mode.ignore_dse_peers=true");
        try (WithProperties properties = new WithProperties().set(INTERNODE_COMPATIBILITY_MODE_IGNORE_DSE_PEERS, true))
        {
            assertThat(logsOf(Level.WARN, () -> StartupChecks.checkDsePeersWithoutFlag.execute(OPTIONS)))
            .singleElement().asString().startsWith("The DSE 6 upgrade marker ");
        }
        // once the DSE 6 marker is gone, the DSE 5.1 one refuses the start
        assertThat(DseUpgradeMarker.delete()).isTrue();
        assertThatThrownBy(() -> StartupChecks.checkDsePeersWithoutFlag.execute(OPTIONS))
        .isInstanceOf(StartupException.class)
        .hasMessageContaining("but the DSE 5.1 upgrade marker ");
    }

    @Test
    public void aDse51MarkerThatCannotBeWrittenIsLoggedAndDoesNotStopTheStart() throws Exception
    {
        mode(InternodeCompatibilityMode.DSE_5_1);
        // the metadata "directory" is a regular file, so the marker cannot be created under it
        Path notADirectory = Files.write(root.resolve("metadata-file"), new byte[0]);
        DatabaseDescriptor.setMetadataDirectory(new File(notADirectory));
        assertThat(logsOf(Level.ERROR, Dse51StartupChecksTest::verifyArtefacts))
        .singleElement().asString()
        .startsWith("Unable to write the DSE 5.1 upgrade marker " +
                    notADirectory.resolve(DseUpgradeMarker.DSE_51_FILE_NAME) + ": unless the file exists, a later " +
                    "start with internode_compatibility_mode NONE or DSE_6 will not be refused while DSE 5.1 peers " +
                    "may remain; create it by hand. Until it exists, every start also discards the record of the " +
                    "values of DSE 5.1's columns of system.local and system.peers");
    }

    /**
     * The start that writes the DSE 5.1 upgrade marker begins a new upgrade: it deletes the record of the values of
     * DSE 5.1's columns of system.local and system.peers an earlier one left; a later start, the marker present, keeps
     * the record. A start with NONE or DSE_6 does neither.
     */
    @Test
    public void theStartThatWritesTheDse51MarkerDeletesTheRecordOfAnEarlierUpgrade() throws Exception
    {
        Path record = Dse51SystemTables.recordPath();
        Files.write(record, List.of("an earlier upgrade"));
        mode(InternodeCompatibilityMode.NONE);
        verifyArtefacts();
        assertThat(record).exists();
        assertThat(DseUpgradeMarker.exists(InternodeCompatibilityMode.DSE_5_1)).isFalse();
        mode(InternodeCompatibilityMode.DSE_6);
        verifyArtefacts();
        assertThat(record).exists();
        DseUpgradeMarker.delete(InternodeCompatibilityMode.DSE_6);

        mode(InternodeCompatibilityMode.DSE_5_1);
        verifyArtefacts();
        assertThat(DseUpgradeMarker.exists(InternodeCompatibilityMode.DSE_5_1)).isTrue();
        assertThat(record).doesNotExist();

        Files.write(record, List.of("this upgrade"));
        verifyArtefacts();
        assertThat(Files.readAllLines(record)).containsExactly("this upgrade");
    }

    /** When the record of an earlier upgrade cannot be deleted, the DSE 5.1 upgrade marker is not written. */
    @Test
    public void theDse51MarkerIsNotWrittenWhenTheRecordOfAnEarlierUpgradeCannotBeDeleted() throws Exception
    {
        Path record = Files.createDirectories(Dse51SystemTables.recordPath());
        Path inTheWay = Files.write(record.resolve("in-the-way"), new byte[0]);
        try
        {
            mode(InternodeCompatibilityMode.DSE_5_1);
            verifyArtefacts();
            assertThat(DseUpgradeMarker.exists(InternodeCompatibilityMode.DSE_5_1)).isFalse();
        }
        finally
        {
            Files.deleteIfExists(inTheWay);
            Files.deleteIfExists(record);
            // clears the refusal of the start this test left
            Dse51SystemTables.deleteRecordOfAnEarlierUpgrade();
        }
        assertThat(Dse51SystemTables.deleteRecordOfAnEarlierUpgrade()).isTrue();
    }

    /**
     * The record of DSE 5.1 peers is loaded at a start in DSE_5_1, which maintains it, and the DSE 6 record is not read
     * (its texts: {@code DsePeersRecordTest}).
     */
    @Test
    public void theDse51RecordIsLoadedInDse51Mode() throws Exception
    {
        try
        {
            DsePeersRecord.clear();
            writePeersRecord(InternodeCompatibilityMode.DSE_5_1, "10.0.0.2:7000");
            writePeersRecord(InternodeCompatibilityMode.DSE_6, "10.0.0.3:7000");
            mode(InternodeCompatibilityMode.DSE_5_1);
            StartupChecks.loadDsePeersRecord.execute(OPTIONS);
            assertThat(DsePeersRecord.peers()).containsExactly(InetAddressAndPort.getByName("10.0.0.2:7000"));
        }
        finally
        {
            DsePeersRecord.clear();
        }
    }

    @Test
    public void dse51WarnsOfAnEc2SnitchWithTheStandardNames() throws Exception
    {
        IEndpointSnitch snitchBefore = DatabaseDescriptor.getEndpointSnitch();
        InetAddress broadcastBefore = DatabaseDescriptor.getBroadcastAddress();
        InetAddress broadcastRpcBefore = DatabaseDescriptor.getBroadcastRpcAddress();
        try
        {
            Ec2Snitch standard = Ec2Snitches.ec2Snitch("us-east-1a");
            Ec2Snitch legacy = Ec2Snitches.ec2Snitch("us-east-1a", "ec2_naming_scheme", "legacy");
            mode(InternodeCompatibilityMode.DSE_5_1);
            DatabaseDescriptor.setEndpointSnitch(standard);
            assertThat(logsOf(Level.WARN, () -> StartupChecks.checkDse6Ec2NamingScheme.execute(OPTIONS)))
            .containsExactly("DSE 5.1 compatibility mode: Ec2Snitch names this node's data center us-east-1 and rack " +
                             "us-east-1a with ec2_naming_scheme=standard, the default; DSE 5.1 always used the " +
                             "legacy EC2 names (data center us-east and rack 1a for the zone us-east-1a), so the " +
                             "data center and rack checks refuse the start of a node that ran DSE, unless " +
                             "cassandra.ignore_dc and cassandra.ignore_rack are set. Set ec2_naming_scheme=legacy in " +
                             "cassandra-rackdc.properties, with the dc_suffix of DSE's cassandra-rackdc.properties " +
                             "when it sets one");
            DatabaseDescriptor.setEndpointSnitch(legacy);
            assertThat(logsOf(Level.WARN, () -> StartupChecks.checkDse6Ec2NamingScheme.execute(OPTIONS))).isEmpty();
        }
        finally
        {
            DatabaseDescriptor.setEndpointSnitch(snitchBefore);
            DatabaseDescriptor.setBroadcastAddress(broadcastBefore);
            DatabaseDescriptor.setBroadcastRpcAddress(broadcastRpcBefore);
            FBUtilities.reset();
        }
    }

    /**
     * The DSE version gossiped to DSE 5.1 nodes in DSE_5_1 mode: 5.1.51, also with the property unset, with no warning;
     * none and any other value refuse the start, so that the node never gossips a value DSE 5.1 cannot parse, nor none.
     */
    @Test
    public void dse51RefusesAnyDseVersionBut5151() throws Exception
    {
        mode(InternodeCompatibilityMode.DSE_5_1);
        assertThat(new StartupChecks().withDefaultTests().getPreFlightChecks()).contains(StartupChecks.checkDse6GossipedDseVersion);
        try (WithProperties unset = new WithProperties().clear(INTERNODE_COMPATIBILITY_MODE_DSE_VERSION))
        {
            assertThat(logsOf(Level.WARN, () -> StartupChecks.checkDse6GossipedDseVersion.execute(OPTIONS))).isEmpty();
        }
        try (WithProperties set = new WithProperties().set(INTERNODE_COMPATIBILITY_MODE_DSE_VERSION, Dse51Fixtures.DSE_VERSION))
        {
            assertThat(logsOf(Level.WARN, () -> StartupChecks.checkDse6GossipedDseVersion.execute(OPTIONS))).isEmpty();
        }
        for (String refused : new String[]{ DseCompat.NO_DSE_VERSION, "6.8.1", "5.1.50", "5.1.51 ", "" })
        {
            try (WithProperties properties = new WithProperties().set(INTERNODE_COMPATIBILITY_MODE_DSE_VERSION, refused))
            {
                assertThatThrownBy(() -> StartupChecks.checkDse6GossipedDseVersion.execute(OPTIONS))
                .as("'%s'", refused)
                .isInstanceOf(StartupException.class)
                .hasMessage(INTERNODE_COMPATIBILITY_MODE_DSE_VERSION.getKey() + " is '" + refused + "': in internode_compatibility_mode " +
                            "DSE_5_1 it must be '5.1.51', its default in that mode, the DSE version of the DSE 5.1 nodes, which " +
                            "this node gossips to them so that their version gates stay as in a cluster of DSE 5.1.51 nodes; " +
                            "'none' (no DSE version) is not accepted in that mode either: remove the property");
            }
        }
    }

    /**
     * The state a node starts gossiping with in DSE_5_1 mode: its DSE state at the ordinal of X_11_PADDING in DSE 5.1's
     * form, with the DSE version 5.1.51, whatever the DSE 6 default of the property is: the state node 1 of the
     * captured DSE 5.1.51 cluster gossips, its keys in its order and with its types, but for its server id, null here,
     * and its health, left out.
     */
    @Test
    public void dse51StartsGossipingTheDse51FormOfTheDseState() throws IOException
    {
        Dse51Fixtures dse51 = DseFixtures.dse51();
        String captured = dse51.gossipStates(dse51.gossipInfo(1), "/127.0.51.11").stream()
                               .filter(s -> s.name.equals(ApplicationState.X_11_PADDING.name()))
                               .findFirst().orElseThrow().value;
        Map<String, Object> expected = JsonUtils.fromJsonMap(captured);
        assertThat(expected).containsKeys("server_id", "health");
        expected.put("server_id", null);
        expected.remove("health");

        mode(InternodeCompatibilityMode.DSE_5_1);
        VersionedValue.VersionedValueFactory factory = new VersionedValue.VersionedValueFactory(DatabaseDescriptor.getPartitioner());
        try (WithProperties unset = new WithProperties().clear(INTERNODE_COMPATIBILITY_MODE_DSE_VERSION))
        {
            Map<ApplicationState, VersionedValue> states = new EnumMap<>(ApplicationState.class);
            StorageService.addDseState(states, factory);
            assertThat(states).containsOnlyKeys(ApplicationState.X_11_PADDING);
            assertThat(states.get(ApplicationState.X_11_PADDING).value).isEqualTo(factory.dse51State(Dse51Fixtures.DSE_VERSION).value)
                                                                        .isEqualTo(JsonUtils.writeAsJsonString(expected))
                                                                        .isEqualTo("{\"dse_version\":\"5.1.51\",\"workloads\":\"Cassandra\"," +
                                                                                   "\"workload\":\"Cassandra\",\"active\":\"true\"," +
                                                                                   "\"server_id\":null,\"graph\":false}");
        }
    }
}
