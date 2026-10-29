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
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.After;
import org.junit.AfterClass;
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
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.dsecompat.DseFixtures;
import org.apache.cassandra.exceptions.StartupException;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.ULIDBasedSSTableId;
import org.apache.cassandra.io.sstable.format.bti.BtiFormat;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.locator.DynamicEndpointSnitch;
import org.apache.cassandra.locator.Ec2Snitch;
import org.apache.cassandra.locator.Ec2Snitches;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.SimpleSnitch;
import org.apache.cassandra.net.DseCompat;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.config.CassandraRelevantProperties.INTERNODE_COMPATIBILITY_MODE_DSE_VERSION;
import static org.apache.cassandra.config.CassandraRelevantProperties.INTERNODE_COMPATIBILITY_MODE_IGNORE_DSE_PEERS;
import static org.apache.cassandra.config.CassandraRelevantProperties.TRIE_INDEX_FORMAT_VERSION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The startup checks of the DSE 6 compatibility mode: what {@code DSE_6} refuses to find on disk, the upgrade marker
 * it writes once every check has passed, the refusal of {@code NONE} while the marker or the record of DSE 6 peers
 * exists, and the DSE version {@code DSE_6} gossips to DSE nodes (with the state it starts gossiping with).
 * <p>
 * The JVM of this class (unit tests fork per class) initialises the trie format at version {@code bb}, as
 * {@code DSE_6} does on a real node, so that {@link StartupChecks#checkSSTablesFormat} rejects newer trie files the
 * way it does in production. Every test works in data, commit log and metadata directories of its own.
 */
public class Dse6StartupChecksTest
{
    private static final StartupChecksOptions OPTIONS = new StartupChecksOptions();
    private static final String DSE_REFUSAL = "cannot enter DSE_6 compatibility mode: the node already ran without it";
    private static WithProperties trieVersion;

    private File[] dataDirectoriesBefore;
    private File commitLogBefore;
    private File metadataBefore;
    private InternodeCompatibilityMode modeBefore;

    private Path root;
    private Path dataDir1;
    private Path dataDir2;
    private Path sstableDir;
    private Path commitLogDir;

    @BeforeClass
    public static void init()
    {
        trieVersion = new WithProperties().set(TRIE_INDEX_FORMAT_VERSION, InternodeCompatibilityMode.DSE_6_TRIE_INDEX_FORMAT_VERSION);
        DatabaseDescriptor.daemonInitialization();
        assertThat(DatabaseDescriptor.getSSTableFormats().get(BtiFormat.NAME).getLatestVersion().version)
        .isEqualTo(InternodeCompatibilityMode.DSE_6_TRIE_INDEX_FORMAT_VERSION);
    }

    @AfterClass
    public static void tearDownClass()
    {
        trieVersion.close();
    }

    @Before
    public void setUp() throws IOException
    {
        modeBefore = DatabaseDescriptor.getInternodeCompatibilityMode();
        dataDirectoriesBefore = DatabaseDescriptor.getAllDataFileLocations();
        commitLogBefore = DatabaseDescriptor.getCommitLogLocation();
        metadataBefore = DatabaseDescriptor.getMetadataDirectory();

        root = Files.createTempDirectory("dse6checks");
        dataDir1 = Files.createDirectories(root.resolve("data1"));
        dataDir2 = Files.createDirectories(root.resolve("data2"));
        sstableDir = Files.createDirectories(dataDir1.resolve("ks").resolve("t-269b3740b8d411f1acb39d12a1a4d70a"));
        commitLogDir = Files.createDirectories(root.resolve("commitlog"));
        DatabaseDescriptor.setDataDirectories(new File[]{ new File(dataDir1), new File(dataDir2) });
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

    private static void dse6()
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
    }

    private static void none()
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.NONE);
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

    /** Runs {@code action} and returns the events {@link StartupChecks} logged meanwhile. */
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

    /** Runs the check and its post-action the way {@link StartupChecks#verify} does. */
    private static void verifyDse6Artefacts() throws StartupException
    {
        new StartupChecks().withTest(StartupChecks.checkDse6Artefacts).verify(OPTIONS);
    }

    @Test
    public void artefactCheckRunsBeforeTheSSTableFormatCheck()
    {
        List<StartupCheck> checks = new StartupChecks().withDefaultTests().getPreFlightChecks();
        assertThat(checks.indexOf(StartupChecks.checkDse6Artefacts)).isNotNegative()
                                                                     .isLessThan(checks.indexOf(StartupChecks.checkSSTablesFormat));
        assertThat(checks).contains(StartupChecks.checkDsePeersWithoutFlag);
        // the record of DSE peers is loaded after the NONE check and before any check that could connect or write
        assertThat(checks.indexOf(StartupChecks.loadDsePeersRecord)).isGreaterThan(checks.indexOf(StartupChecks.checkDsePeersWithoutFlag))
                                                                     .isLessThan(checks.indexOf(StartupChecks.checkDse6Artefacts));
    }

    @Test
    public void artefactCheckIsSilentInNoneMode() throws Exception
    {
        none();
        touch(sstableDir, "cc-1-bti-Data.db", "cc-1-bti-TOC.txt", "nb-2-big-Data.db");
        touch(commitLogDir, "CommitLog-8-1.log");
        assertThat(logsOf(Level.WARN, Dse6StartupChecksTest::verifyDse6Artefacts)).isEmpty();
        assertThat(logsOf(Level.ERROR, Dse6StartupChecksTest::verifyDse6Artefacts)).isEmpty();
        assertThat(DseUpgradeMarker.exists()).isFalse();
    }

    @Test
    public void dse6AcceptsWhatDseReadsAndWritesTheMarker() throws Exception
    {
        dse6();
        // trie bb and big me with numeric generations, DSE's own commit log version and the 4.0 one HCD_1 writes
        touch(sstableDir, "bb-1-bti-Data.db", "bb-1-bti-TOC.txt", "aa-2-bti-Data.db", "me-3-big-Data.db");
        touch(commitLogDir, "CommitLog-680-1.log", "CommitLog-7-1.log");

        StartupChecks.checkDse6Artefacts.execute(OPTIONS);
        assertThat(DseUpgradeMarker.exists()).as("execute() must not write the marker").isFalse();

        assertThat(logsOf(Level.INFO, Dse6StartupChecksTest::verifyDse6Artefacts))
        .containsExactly("Wrote the DSE 6 upgrade marker " + DseUpgradeMarker.path());
        assertThat(DseUpgradeMarker.exists()).isTrue();
        String content = Files.readString(DseUpgradeMarker.path());
        assertThat(content).contains("DSE 6 in-place upgrade in progress");

        // idempotent: a second start keeps the marker as it is, and says nothing about it
        assertThat(logsOf(Level.INFO, Dse6StartupChecksTest::verifyDse6Artefacts)).isEmpty();
        assertThat(Files.readString(DseUpgradeMarker.path())).isEqualTo(content);
        assertThat(DseUpgradeMarker.writeIfAbsent()).isFalse();
    }

    @Test
    public void dse6RefusesNewerTrieVersion() throws Exception
    {
        dse6();
        touch(sstableDir, "bb-1-bti-Data.db", "cc-2-bti-Data.db", "cc-2-bti-TOC.txt");
        assertThatThrownBy(() -> StartupChecks.checkDse6Artefacts.execute(OPTIONS))
        .isInstanceOf(StartupException.class)
        .hasMessageContaining(DSE_REFUSAL)
        .hasMessageContaining("cc-2-bti-Data.db")
        .hasMessageContaining("Restore the node from its pre-upgrade snapshot")
        .hasMessageContaining("loses this node's copy")
        .hasMessageNotContaining("bb-1-bti-Data.db")
        .satisfies(e -> assertThat(((StartupException) e).returnCode).isEqualTo(StartupException.ERR_WRONG_DISK_STATE));
        assertThat(DseUpgradeMarker.exists()).isFalse();
    }

    @Test
    public void dse6RefusalComesBeforeTheSSTableFormatCheck() throws Exception
    {
        dse6();
        touch(sstableDir, "cc-2-bti-Data.db");
        // in this JVM, as in DSE_6 mode, trie cc is unreadable by the node itself
        assertThatThrownBy(() -> StartupChecks.checkSSTablesFormat.execute(OPTIONS))
        .isInstanceOf(StartupException.class)
        .hasMessageContaining("Detected unreadable sstables");
        // in the daemon order the operator gets the DSE-specific explanation
        StartupChecks daemonOrder = new StartupChecks();
        for (StartupCheck check : new StartupChecks().withDefaultTests().getPreFlightChecks())
        {
            if (check == StartupChecks.checkDse6Artefacts || check == StartupChecks.checkSSTablesFormat)
                daemonOrder.withTest(check);
        }
        assertThatThrownBy(() -> daemonOrder.verify(OPTIONS))
        .isInstanceOf(StartupException.class)
        .hasMessageContaining(DSE_REFUSAL)
        .hasMessageContaining("cc-2-bti-Data.db");
        assertThat(DseUpgradeMarker.exists()).isFalse();
    }

    @Test
    public void dse6RefusesNewerBigVersionOtherFormatsAndUuidIds() throws Exception
    {
        dse6();
        touch(sstableDir, "me-1-big-Data.db", "nb-2-big-Data.db", "oa-3-big-Data.db", "bb-4-xyz-Data.db",
              "bb-3h43_18di_43ocw25niiw1m23p1r-bti-Data.db");
        assertThatThrownBy(() -> StartupChecks.checkDse6Artefacts.execute(OPTIONS))
        .isInstanceOf(StartupException.class)
        .hasMessageContaining(DSE_REFUSAL)
        .hasMessageContaining("nb-2-big-Data.db")
        .hasMessageContaining("oa-3-big-Data.db")
        .hasMessageContaining("bb-4-xyz-Data.db")
        .hasMessageContaining("bb-3h43_18di_43ocw25niiw1m23p1r-bti-Data.db")
        .hasMessageNotContaining("me-1-big-Data.db");
    }

    @Test
    public void dse6RefusesUlidIds() throws Exception
    {
        dse6();
        String ulid = ULIDBasedSSTableId.Builder.instance.generator(Stream.empty()).get().toString();
        touch(sstableDir, "bb-1-bti-Data.db", "bb-" + ulid + "-bti-Data.db");
        assertThatThrownBy(() -> StartupChecks.checkDse6Artefacts.execute(OPTIONS))
        .isInstanceOf(StartupException.class)
        .hasMessageContaining(DSE_REFUSAL)
        .hasMessageContaining("bb-" + ulid + "-bti-Data.db")
        .hasMessageNotContaining("bb-1-bti-Data.db");
    }

    @Test
    public void dse6SkipsUnknownSystemTableDirectories() throws Exception
    {
        dse6();
        // a system table this software does not know holds what DSE wrote; checkSSTablesFormat logs it
        Path unknownSystemTable = Files.createDirectories(dataDir1.resolve("system").resolve("dse_only_table-469b3740b8d411f1acb39d12a1a4d70a"));
        touch(unknownSystemTable, "cc-1-bti-Data.db");
        verifyDse6Artefacts();
        assertThat(DseUpgradeMarker.exists()).isTrue();
    }

    @Test
    public void aMarkerThatCannotBeWrittenIsLoggedAndDoesNotStopTheStart() throws Exception
    {
        dse6();
        // the metadata "directory" is a regular file, so the marker cannot be created under it
        Path notADirectory = Files.write(root.resolve("metadata-file"), new byte[0]);
        DatabaseDescriptor.setMetadataDirectory(new File(notADirectory));
        List<String> errors = logsOf(Level.ERROR, Dse6StartupChecksTest::verifyDse6Artefacts);
        assertThat(errors).hasSize(1);
        assertThat(errors.get(0)).contains("Unable to write the DSE 6 upgrade marker")
                                 .contains(notADirectory.resolve(DseUpgradeMarker.FILE_NAME).toString());
        assertThat(Files.isRegularFile(DseUpgradeMarker.path())).isFalse();
    }

    @Test
    public void namesThatDoNotParseAreLeftToTheSSTableFormatCheck()
    {
        assertThat(StartupChecks.isSSTableUnreadableByDse6("ks-t-ka-1-Data.db")).isFalse();
        assertThat(StartupChecks.isSSTableUnreadableByDse6("KeyCache-g.db")).isFalse();
        assertThat(StartupChecks.isSSTableUnreadableByDse6("B1-1-bti-Data.db")).isFalse();
        assertThat(StartupChecks.isSSTableUnreadableByDse6("bb-not!an!id-bti-Data.db")).isFalse();
        assertThat(StartupChecks.isSSTableUnreadableByDse6("bb-99999999999-bti-Data.db")).isFalse();
        assertThat(StartupChecks.isSSTableUnreadableByDse6("bb-1-bti-Data.db")).isFalse();
        assertThat(StartupChecks.isSSTableUnreadableByDse6("ca-1-bti-Data.db")).isTrue();
    }

    /** The names of the files DSE 6.8.65 and 6.9.26 wrote: none of them is taken for one written without the mode. */
    @Test
    public void theFilesDseWroteAreNotFlagged() throws IOException
    {
        List<String> sstables;
        List<String> segments;
        try (Stream<Path> files = Files.walk(DseFixtures.ROOT))
        {
            List<String> names = files.filter(Files::isRegularFile).map(p -> p.getFileName().toString()).collect(Collectors.toList());
            sstables = names.stream().filter(n -> Descriptor.isValidFile(new File(n))).collect(Collectors.toList());
            // the fixtures keep the segments compressed
            segments = names.stream().filter(n -> n.startsWith("CommitLog-")).map(n -> n.replaceFirst("\\.gz$", "")).collect(Collectors.toList());
        }
        assertThat(sstables).anyMatch(n -> n.startsWith("bb-") && n.contains("-bti-")).anyMatch(n -> n.contains("SAI"));
        assertThat(segments).isNotEmpty().allMatch(n -> n.startsWith("CommitLog-680-"));
        assertThat(sstables).noneMatch(StartupChecks::isSSTableUnreadableByDse6);
        assertThat(segments).noneMatch(StartupChecks::isCommitLogSegmentWrittenWithoutDse6Mode);
    }

    /** NONE: the sstable format check walks the data directories as before, with the same log lines and skips. */
    @Test
    public void theSSTableFormatCheckLogsUnknownSystemTablesAndSkipsWhatIsNotData() throws Exception
    {
        none();
        Path unknownSystemTable = Files.createDirectories(dataDir1.resolve("system").resolve("dropped_table-469b3740b8d411f1acb39d12a1a4d70a"));
        Path snapshotNamed = Files.createDirectories(dataDir2.resolve("system").resolve("snapshot_table-569b3740b8d411f1acb39d12a1a4d70a"));
        // in this JVM trie cc is unreadable: these files would be refused, were they not skipped
        touch(unknownSystemTable, "cc-1-bti-Data.db");
        touch(snapshotNamed, "cc-1-bti-Data.db");
        Path nestedCommitLog = Files.createDirectories(dataDir1.resolve("commitlog"));
        DatabaseDescriptor.setCommitLogLocation(new File(nestedCommitLog));
        touch(nestedCommitLog, "cc-1-bti-Data.db");
        touch(Files.createDirectories(sstableDir.resolve("snapshots").resolve("t1")), "cc-1-bti-Data.db");
        touch(Files.createDirectories(sstableDir.resolve("backups")), "cc-1-bti-Data.db");
        touch(sstableDir, "bb-1-bti-Data.db");

        Logger logger = (Logger) LoggerFactory.getLogger(StartupChecks.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
        try
        {
            StartupChecks.checkSSTablesFormat.execute(OPTIONS);
        }
        finally
        {
            logger.detachAppender(appender);
        }
        assertThat(appender.list).filteredOn(e -> e.getLevel() == Level.WARN)
                                 .extracting(ILoggingEvent::getFormattedMessage)
                                 .containsExactly("Found unknown system directory system.dropped_table at " + unknownSystemTable.toRealPath() +
                                                  " - this is likely left over from a previous version of Cassandra and should be " +
                                                  "removed after inspection.");
        assertThat(appender.list).filteredOn(e -> e.getLevel() == Level.INFO)
                                 .extracting(ILoggingEvent::getFormattedMessage)
                                 .containsExactly("Found unknown system directory system.snapshot_table at " + snapshotNamed.toRealPath() +
                                                  " that contains the word snapshot. This may be left over from a previous version of " +
                                                  "Cassandra or may be normal.  Consider removing after inspection if determined to be unnecessary.");

        // the same file in a table directory is refused
        touch(sstableDir, "cc-2-bti-Data.db");
        assertThatThrownBy(() -> StartupChecks.checkSSTablesFormat.execute(OPTIONS))
        .isInstanceOf(StartupException.class)
        .hasMessageContaining("cc-2-bti-Data.db")
        .hasMessageNotContaining("cc-1-bti-Data.db");
    }

    @Test
    public void dse6RefusesNewerCommitLogSegments() throws Exception
    {
        dse6();
        touch(commitLogDir, "CommitLog-8-1.log", "CommitLog-7-2.log");
        assertThatThrownBy(() -> StartupChecks.checkDse6Artefacts.execute(OPTIONS))
        .isInstanceOf(StartupException.class)
        .hasMessageContaining(DSE_REFUSAL)
        .hasMessageContaining("CommitLog-8-1.log")
        .hasMessageNotContaining("CommitLog-7-2.log");
    }

    @Test
    public void dse6LeavesUnparsableCommitLogNamesToTheReplay() throws Exception
    {
        dse6();
        // a legacy segment without a version, and a version out of the int range
        touch(commitLogDir, "CommitLog-1234567890123.log", "CommitLog-99999999999-1.log", "CommitLog-7-1.log");
        verifyDse6Artefacts();
        assertThat(DseUpgradeMarker.exists()).isTrue();
        assertThat(StartupChecks.isCommitLogSegmentWrittenWithoutDse6Mode("CommitLog-1234567890123.log")).isFalse();
        assertThat(StartupChecks.isCommitLogSegmentWrittenWithoutDse6Mode("CommitLog-99999999999-1.log")).isFalse();
    }

    @Test
    public void dse6FailsOnACommitLogDirectoryItCannotList() throws Exception
    {
        dse6();
        Path notADirectory = Files.write(root.resolve("commitlog-file"), new byte[0]);
        DatabaseDescriptor.setCommitLogLocation(new File(notADirectory));
        assertThatThrownBy(() -> StartupChecks.checkDse6Artefacts.execute(OPTIONS))
        .isInstanceOf(StartupException.class)
        .hasMessageContaining("Unable to verify commit log segments on disk: cannot list " + notADirectory);
    }

    @Test
    public void dse6IgnoresSnapshotsAndBackups() throws Exception
    {
        dse6();
        Path snapshots = Files.createDirectories(sstableDir.resolve("snapshots").resolve("pre-upgrade"));
        Path backups = Files.createDirectories(sstableDir.resolve("backups"));
        touch(snapshots, "cc-1-bti-Data.db");
        touch(backups, "nb-1-big-Data.db");
        verifyDse6Artefacts();
        assertThat(DseUpgradeMarker.exists()).isTrue();
    }

    @Test
    public void dse6InspectsEveryDataDirectory() throws Exception
    {
        dse6();
        Path otherTable = Files.createDirectories(dataDir2.resolve("ks").resolve("u-369b3740b8d411f1acb39d12a1a4d70a"));
        touch(sstableDir, "bb-1-bti-Data.db");
        touch(otherTable, "cc-5-bti-Data.db");
        assertThatThrownBy(() -> StartupChecks.checkDse6Artefacts.execute(OPTIONS))
        .isInstanceOf(StartupException.class)
        .hasMessageContaining(DSE_REFUSAL)
        .hasMessageContaining(otherTable.resolve("cc-5-bti-Data.db").toString())
        .hasMessageNotContaining("bb-1-bti-Data.db");
    }

    @Test
    public void dse6SkipsACommitLogDirectoryNestedInADataDirectory() throws Exception
    {
        dse6();
        Path nested = Files.createDirectories(dataDir1.resolve("commitlog"));
        DatabaseDescriptor.setCommitLogLocation(new File(nested));
        // not an sstable, but named like one: only the segment rules apply in the commit log directory
        touch(nested, "cc-1-bti-Data.db", "CommitLog-7-1.log");
        verifyDse6Artefacts();
        assertThat(DseUpgradeMarker.exists()).isTrue();
    }

    @Test
    public void noneIsRefusedWhileTheMarkerExists() throws Exception
    {
        none();
        assertThat(DseUpgradeMarker.writeIfAbsent()).isTrue();
        assertThatThrownBy(() -> StartupChecks.checkDsePeersWithoutFlag.execute(OPTIONS))
        .isInstanceOf(StartupException.class)
        .hasMessageContaining("internode_compatibility_mode is NONE but the DSE 6 upgrade marker")
        .hasMessageContaining(DseUpgradeMarker.FILE_NAME + " is present: DSE 6 peers may still be in the cluster")
        .hasMessageContaining("once every node runs this software, delete the marker")
        .hasMessageContaining("ignore_dse_peers=true")
        .satisfies(e -> assertThat(((StartupException) e).returnCode).isEqualTo(StartupException.ERR_WRONG_CONFIG));
    }

    @Test
    public void noneIsRefusedWhenTheMarkerCannotBeChecked() throws Exception
    {
        none();
        // the metadata "directory" is a regular file: whether the marker exists cannot be determined
        Path notADirectory = Files.write(root.resolve("metadata-file"), new byte[0]);
        DatabaseDescriptor.setMetadataDirectory(new File(notADirectory));
        assertThat(DseUpgradeMarker.exists()).isTrue();
        assertThatThrownBy(() -> StartupChecks.checkDsePeersWithoutFlag.execute(OPTIONS))
        .isInstanceOf(StartupException.class)
        .hasMessageContaining(DseUpgradeMarker.FILE_NAME + " may be present (its presence cannot be checked)")
        .hasMessageContaining("make the metadata directory readable so that the marker can be checked")
        .hasMessageNotContaining("delete the marker");

        // with the override the warning says the same (the record, under the same "directory", cannot be read either)
        try (WithProperties properties = new WithProperties().set(INTERNODE_COMPATIBILITY_MODE_IGNORE_DSE_PEERS, true))
        {
            List<String> warnings = logsOf(Level.WARN, () -> StartupChecks.checkDsePeersWithoutFlag.execute(OPTIONS));
            assertThat(warnings).hasSize(1);
            assertThat(warnings.get(0)).startsWith("The DSE 6 upgrade marker " + DseUpgradeMarker.path() + " may be present (its presence " +
                                                   "cannot be checked) and the record of DSE 6 peers " + DsePeersRecord.path() + " cannot be read (")
                                       .endsWith(") but ignored (cassandra.internode_compatibility_mode.ignore_dse_peers=true); DSE 6 peers, " +
                                                 "if any, will be unreachable");
        }
    }

    @Test
    public void aMarkerThatFailsAfterCreationIsKept() throws Exception
    {
        IOException failure = new IOException("disk full");
        assertThatThrownBy(() -> DseUpgradeMarker.writeIfAbsent(() -> "partial", (channel, bytes) -> {
            channel.write(bytes);
            throw failure;
        })).isSameAs(failure);
        // only its existence matters: the partial marker still refuses a start with NONE, and is not written again
        assertThat(Files.readString(DseUpgradeMarker.path())).isEqualTo("partial");
        assertThat(DseUpgradeMarker.writeIfAbsent()).isFalse();
        none();
        assertThatThrownBy(() -> StartupChecks.checkDsePeersWithoutFlag.execute(OPTIONS))
        .isInstanceOf(StartupException.class)
        .hasMessageContaining(DseUpgradeMarker.FILE_NAME + " is present");
    }

    @Test
    public void aMarkerWhoseContentCannotBeBuiltIsKept() throws Exception
    {
        IllegalStateException failure = new IllegalStateException("no broadcast address");
        assertThatThrownBy(() -> DseUpgradeMarker.writeIfAbsent(() -> { throw failure; }, (channel, bytes) -> channel.write(bytes)))
        .isSameAs(failure);
        assertThat(Files.size(DseUpgradeMarker.path())).isZero();
        assertThat(DseUpgradeMarker.writeIfAbsent()).isFalse();
        none();
        assertThatThrownBy(() -> StartupChecks.checkDsePeersWithoutFlag.execute(OPTIONS))
        .isInstanceOf(StartupException.class)
        .hasMessageContaining(DseUpgradeMarker.FILE_NAME + " is present");
    }

    @Test
    public void aMarkerKeptAfterAFailedWriteIsAWarning() throws Exception
    {
        // the content is written in part, then the write fails
        StartupChecks.UpgradeMarkerWrite failedWrite = () -> DseUpgradeMarker.writeIfAbsent(() -> "partial", (channel, bytes) -> {
            channel.write(bytes);
            throw new IOException("disk full");
        });
        assertOneWarningWithTheFailure(failedWrite, "disk full");
        assertThat(Files.readString(DseUpgradeMarker.path())).isEqualTo("partial");

        // the content cannot be built
        assertThat(DseUpgradeMarker.delete()).isTrue();
        StartupChecks.UpgradeMarkerWrite failedContent = () -> DseUpgradeMarker.writeIfAbsent(() -> {
            throw new IllegalStateException("no broadcast address");
        }, (channel, bytes) -> channel.write(bytes));
        assertOneWarningWithTheFailure(failedContent, "no broadcast address");
        assertThat(Files.size(DseUpgradeMarker.path())).isZero();
    }

    private static void assertOneWarningWithTheFailure(StartupChecks.UpgradeMarkerWrite write, String failure) throws Exception
    {
        List<ILoggingEvent> events = eventsOf(() -> StartupChecks.writeUpgradeMarker(write));
        assertThat(events).filteredOn(e -> e.getLevel() == Level.ERROR).isEmpty();
        List<ILoggingEvent> warnings = events.stream().filter(e -> e.getLevel() == Level.WARN).collect(Collectors.toList());
        assertThat(warnings).hasSize(1);
        assertThat(warnings.get(0).getFormattedMessage())
        .isEqualTo("Unable to write the content of the DSE 6 upgrade marker " + DseUpgradeMarker.path() + "; the file exists, " +
                   "which is all that matters: a later start with internode_compatibility_mode NONE is refused while it does");
        assertThat(warnings.get(0).getThrowableProxy().getMessage()).isEqualTo(failure);
        assertThat(DseUpgradeMarker.exists()).isTrue();
    }

    @Test
    public void noneIsAcceptedWithoutTheMarkerOrWithTheOverride() throws Exception
    {
        none();
        assertThat(logsOf(Level.WARN, () -> StartupChecks.checkDsePeersWithoutFlag.execute(OPTIONS))).isEmpty();

        assertThat(DseUpgradeMarker.writeIfAbsent()).isTrue();
        try (WithProperties properties = new WithProperties().set(INTERNODE_COMPATIBILITY_MODE_IGNORE_DSE_PEERS, true))
        {
            List<String> warnings = logsOf(Level.WARN, () -> StartupChecks.checkDsePeersWithoutFlag.execute(OPTIONS));
            assertThat(warnings).hasSize(1);
            assertThat(warnings.get(0)).contains("The DSE 6 upgrade marker")
                                       .contains(DseUpgradeMarker.path() + " is present but ignored")
                                       .contains(INTERNODE_COMPATIBILITY_MODE_IGNORE_DSE_PEERS.getKey() + "=true");
        }

        // once the marker is deleted (every node runs this software), NONE starts without the override
        assertThat(DseUpgradeMarker.delete()).isTrue();
        assertThat(DseUpgradeMarker.exists()).isFalse();
        StartupChecks.checkDsePeersWithoutFlag.execute(OPTIONS);
        // and the override set with nothing to ignore says nothing
        try (WithProperties properties = new WithProperties().set(INTERNODE_COMPATIBILITY_MODE_IGNORE_DSE_PEERS, true))
        {
            assertThat(logsOf(Level.WARN, () -> StartupChecks.checkDsePeersWithoutFlag.execute(OPTIONS))).isEmpty();
        }
    }

    @Test
    public void dse6IsNotRefusedByTheMarker() throws Exception
    {
        dse6();
        assertThat(DseUpgradeMarker.writeIfAbsent()).isTrue();
        String content = Files.readString(DseUpgradeMarker.path());
        assertThat(logsOf(Level.WARN, () -> StartupChecks.checkDsePeersWithoutFlag.execute(OPTIONS))).isEmpty();
        assertThat(Files.readString(DseUpgradeMarker.path())).isEqualTo(content);
    }

    private static void writePeersRecord(String... peers) throws IOException
    {
        Files.createDirectories(DsePeersRecord.path().getParent());
        Files.write(DsePeersRecord.path(), String.join("\n", peers).getBytes(StandardCharsets.UTF_8));
    }

    @Test
    public void entriesThatAreNotAddressesMustBeRemovedByHand() throws Exception
    {
        none();
        writePeersRecord("10.0.0.2:7000", "10.0.0.3:not-a-port");
        assertThatThrownBy(() -> StartupChecks.checkDsePeersWithoutFlag.execute(OPTIONS))
        .isInstanceOf(StartupException.class)
        .hasMessageContaining("peers have left the cluster; the entries [10.0.0.3:not-a-port] are not addresses with port (<ip>:<port>) and must be removed from it by hand, " +
                              "or start with -Dcassandra.internode_compatibility_mode.ignore_dse_peers=true");
    }

    @Test
    public void noneIsRefusedWhileDsePeersAreOnRecord() throws Exception
    {
        none();
        writePeersRecord("10.0.0.2:7000", "10.0.0.3:7000");
        assertThatThrownBy(() -> StartupChecks.checkDsePeersWithoutFlag.execute(OPTIONS))
        .isInstanceOf(StartupException.class)
        .hasMessage("internode_compatibility_mode is NONE but the record of DSE 6 peers " + DsePeersRecord.path() + " listing " +
                    "[10.0.0.2:7000, 10.0.0.3:7000] is present: DSE 6 peers may still be in the cluster. Set internode_compatibility_mode: " +
                    "DSE_6, or the record empties itself once this node has seen every listed peer at messaging version 4.0 or later " +
                    "(run once more with DSE_6 after the last upgrade); delete it by hand only after checking that the listed peers " +
                    "have left the cluster, or start with -Dcassandra.internode_compatibility_mode.ignore_dse_peers=true");

        // both sources present: both named, both remedies given
        DseUpgradeMarker.writeIfAbsent();
        assertThatThrownBy(() -> StartupChecks.checkDsePeersWithoutFlag.execute(OPTIONS))
        .isInstanceOf(StartupException.class)
        .hasMessageContaining("but the DSE 6 upgrade marker " + DseUpgradeMarker.path() + " is present and the record of DSE 6 peers")
        .hasMessageContaining("] is present: DSE 6 peers may still be in the cluster")
        .hasMessageContaining("once every node runs this software, delete the marker; the record empties itself");
    }

    @Test
    public void noneIsAcceptedWithAnEmptyRecordOrWithTheOverride() throws Exception
    {
        none();
        writePeersRecord("# nothing on record", "");
        StartupChecks.checkDsePeersWithoutFlag.execute(OPTIONS);

        writePeersRecord("10.0.0.2:7000");
        try (WithProperties properties = new WithProperties().set(INTERNODE_COMPATIBILITY_MODE_IGNORE_DSE_PEERS, true))
        {
            List<String> warnings = logsOf(Level.WARN, () -> StartupChecks.checkDsePeersWithoutFlag.execute(OPTIONS));
            assertThat(warnings).containsExactly("The record of DSE 6 peers " + DsePeersRecord.path() + " listing [10.0.0.2:7000] is present " +
                                                 "but ignored (cassandra.internode_compatibility_mode.ignore_dse_peers=true); DSE 6 peers, " +
                                                 "if any, will be unreachable");
        }
    }

    @Test
    public void anUnreadableRecordRefusesTheStart() throws Exception
    {
        none();
        // the record is a directory: it exists but cannot be read as a file; the NONE check fails closed
        Files.createDirectories(DsePeersRecord.path());
        assertThatThrownBy(() -> StartupChecks.checkDsePeersWithoutFlag.execute(OPTIONS))
        .isInstanceOf(StartupException.class)
        .hasMessageStartingWith("internode_compatibility_mode is NONE but the record of DSE 6 peers " + DsePeersRecord.path() + " cannot be read (")
        .hasMessageContaining("): DSE 6 peers may still be in the cluster. Set internode_compatibility_mode: DSE_6, or make the record readable, or start with ")
        .satisfies(e -> assertThat(((StartupException) e).returnCode).isEqualTo(StartupException.ERR_WRONG_CONFIG));
    }

    @Test
    public void dse6IsNotRefusedByTheRecord() throws Exception
    {
        dse6();
        writePeersRecord("10.0.0.2:7000");
        assertThat(logsOf(Level.WARN, () -> StartupChecks.checkDsePeersWithoutFlag.execute(OPTIONS))).isEmpty();
        assertThat(DsePeersRecord.persistedPeers()).containsExactly("10.0.0.2:7000");
    }

    @Test
    public void anUnreadableRecordRefusesADse6Start() throws Exception
    {
        try
        {
            DsePeersRecord.clear();
            dse6();
            // the record is a directory: it exists but cannot be read as a file
            Files.createDirectories(DsePeersRecord.path());
            assertThatThrownBy(() -> StartupChecks.loadDsePeersRecord.execute(OPTIONS))
            .isInstanceOf(StartupException.class)
            .hasMessage("Unable to read the record of DSE 6 peers " + DsePeersRecord.path() + "; make it readable (it lists the DSE 6 peers of the last run)")
            .satisfies(e -> assertThat(((StartupException) e).returnCode).isEqualTo(StartupException.ERR_WRONG_DISK_STATE));

            // once readable, the next attempt loads it
            Files.delete(DsePeersRecord.path());
            writePeersRecord("10.0.0.2:7000");
            StartupChecks.loadDsePeersRecord.execute(OPTIONS);
            assertThat(DsePeersRecord.peers()).containsExactly(InetAddressAndPort.getByName("10.0.0.2:7000"));
        }
        finally
        {
            Files.deleteIfExists(DsePeersRecord.path());
            DsePeersRecord.clear();
        }
    }

    @Test
    public void theRecordIsLoadedAtADse6StartOnly() throws Exception
    {
        try
        {
            DsePeersRecord.clear();
            none();
            writePeersRecord("10.0.0.2:7000");
            StartupChecks.loadDsePeersRecord.execute(OPTIONS);
            assertThat(DsePeersRecord.peers()).isEmpty();

            dse6();
            StartupChecks.loadDsePeersRecord.execute(OPTIONS);
            assertThat(DsePeersRecord.peers()).containsExactly(InetAddressAndPort.getByName("10.0.0.2:7000"));
        }
        finally
        {
            DsePeersRecord.clear();
        }
    }

    /** The DSE version gossiped to DSE nodes in DSE_6 mode: 6.8.0 or 6.8.1, or none with a warning. */
    @Test
    public void dse6RefusesADseVersionItMayNotGossip() throws Exception
    {
        dse6();
        assertThat(new StartupChecks().withDefaultTests().getPreFlightChecks()).contains(StartupChecks.checkDse6GossipedDseVersion);
        assertThat(logsOf(Level.WARN, () -> StartupChecks.checkDse6GossipedDseVersion.execute(OPTIONS))).isEmpty();
        assertThat(DseCompat.gossipedDseVersion()).isEqualTo("6.8.1");
        for (String accepted : new String[]{ "6.8.0", "6.8.1" })
        {
            try (WithProperties properties = new WithProperties().set(INTERNODE_COMPATIBILITY_MODE_DSE_VERSION, accepted))
            {
                StartupChecks.checkDse6GossipedDseVersion.execute(OPTIONS);
                assertThat(DseCompat.gossipedDseVersion()).isEqualTo(accepted);
            }
        }
        for (String refused : new String[]{ "6.8.2", "6.8.2-SNAPSHOT", "6.9.0", "7.0.0", "6.0.18", "6.8", "06.8.1", "6.8.1 ", "6.8.1+x",
                                            "6.8.1-SNAPSHOT", "6.8.1.5", "6.8.1.99999999999", "", "dse", "NONE" })
        {
            try (WithProperties properties = new WithProperties().set(INTERNODE_COMPATIBILITY_MODE_DSE_VERSION, refused))
            {
                assertThatThrownBy(() -> StartupChecks.checkDse6GossipedDseVersion.execute(OPTIONS))
                .as("'%s'", refused)
                .isInstanceOf(StartupException.class)
                .hasMessage(INTERNODE_COMPATIBILITY_MODE_DSE_VERSION.getKey() + " is '" + refused + "': it must be 'none' (no DSE version) or " +
                            "the DSE version 6.8.0 or 6.8.1, which this node gossips to DSE nodes so that they do not send " +
                            "it the range reads of a storage-attached index query with a verb only DSE nodes understand");
            }
        }
        try (WithProperties properties = new WithProperties().set(INTERNODE_COMPATIBILITY_MODE_DSE_VERSION, DseCompat.NO_DSE_VERSION))
        {
            List<String> warnings = logsOf(Level.WARN, () -> StartupChecks.checkDse6GossipedDseVersion.execute(OPTIONS));
            assertThat(warnings).containsExactly("This node gossips no DSE version to DSE nodes (" + INTERNODE_COMPATIBILITY_MODE_DSE_VERSION.getKey() +
                                                 "=none): DSE nodes that stored one for it when it ran DSE send it the range reads of " +
                                                 "storage-attached index queries with a verb it cannot receive, unless they run with " +
                                                 "-Ddse.disable_endpoint_group_range_reads=true");
            assertThat(DseCompat.gossipedDseVersion()).isNull();
        }
    }

    @Test
    public void noneDoesNotReadTheGossipedDseVersion() throws Exception
    {
        none();
        try (WithProperties properties = new WithProperties().set(INTERNODE_COMPATIBILITY_MODE_DSE_VERSION, "6.9.0"))
        {
            assertThat(logsOf(Level.WARN, () -> StartupChecks.checkDse6GossipedDseVersion.execute(OPTIONS))).isEmpty();
            Map<ApplicationState, VersionedValue> states = new EnumMap<>(ApplicationState.class);
            StorageService.addDseState(states, new VersionedValue.VersionedValueFactory(DatabaseDescriptor.getPartitioner()));
            assertThat(states).isEmpty();
        }
    }

    /** The state a node starts gossiping with in DSE_6 mode: DSE's DSE_GOSSIP_STATE at the ordinal of X_11_PADDING, unless none. */
    @Test
    public void dse6StartsGossipingADseStateUnlessNone()
    {
        dse6();
        VersionedValue.VersionedValueFactory factory = new VersionedValue.VersionedValueFactory(DatabaseDescriptor.getPartitioner());
        Map<ApplicationState, VersionedValue> states = new EnumMap<>(ApplicationState.class);
        StorageService.addDseState(states, factory);
        assertThat(states).containsOnlyKeys(ApplicationState.X_11_PADDING);
        assertThat(states.get(ApplicationState.X_11_PADDING).value).isEqualTo(factory.dseState("6.8.1").value)
                                                                    .contains("\"dse_version\":\"6.8.1\"");

        states.clear();
        try (WithProperties properties = new WithProperties().set(INTERNODE_COMPATIBILITY_MODE_DSE_VERSION, DseCompat.NO_DSE_VERSION))
        {
            StorageService.addDseState(states, factory);
            assertThat(states).isEmpty();
        }
    }

    /**
     * EC2 names: in DSE_6 mode an Ec2Snitch or Ec2MultiRegionSnitch with the standard names (dc_suffix included) gets a
     * warning before the data center and rack checks, also behind the dynamic snitch; the legacy names, another snitch
     * and NONE get none.
     */
    @Test
    public void dse6WarnsOfAnEc2SnitchWithTheStandardNames() throws Exception
    {
        IEndpointSnitch snitchBefore = DatabaseDescriptor.getEndpointSnitch();
        InetAddress broadcastBefore = DatabaseDescriptor.getBroadcastAddress();
        InetAddress broadcastRpcBefore = DatabaseDescriptor.getBroadcastRpcAddress();
        try
        {
            Ec2Snitch standard = Ec2Snitches.ec2Snitch("us-east-1a", "dc_suffix", "_live");
            // its constructor sets the broadcast addresses, restored below
            Ec2Snitch multiRegion = Ec2Snitches.ec2MultiRegionSnitch("eu-west-1c", FBUtilities.getJustLocalAddress().getHostAddress());
            Ec2Snitch legacy = Ec2Snitches.ec2Snitch("us-east-1a", "ec2_naming_scheme", "legacy");
            dse6();
            List<StartupCheck> checks = new StartupChecks().withDefaultTests().getPreFlightChecks();
            assertThat(checks.indexOf(StartupChecks.checkDse6Ec2NamingScheme)).isGreaterThanOrEqualTo(0)
                                                                              .isLessThan(checks.indexOf(StartupChecks.checkDatacenter));
            String expected = " with ec2_naming_scheme=standard, the default; DSE 6 always used the legacy EC2 names (data center us-east and rack " +
                              "1a for the zone us-east-1a), so the data center and rack checks refuse the start of a node that ran DSE, unless " +
                              "cassandra.ignore_dc and cassandra.ignore_rack are set. Set ec2_naming_scheme=legacy in " +
                              "cassandra-rackdc.properties, with the dc_suffix of DSE's cassandra-rackdc.properties when it sets one";
            for (IEndpointSnitch snitch : new IEndpointSnitch[]{ standard, new DynamicEndpointSnitch(standard, "dse6-ec2-test") })
            {
                DatabaseDescriptor.setEndpointSnitch(snitch);
                assertThat(logsOf(Level.WARN, () -> StartupChecks.checkDse6Ec2NamingScheme.execute(OPTIONS)))
                .containsExactly("DSE 6 compatibility mode: Ec2Snitch names this node's data center us-east-1_live and rack us-east-1a" + expected);
            }
            DatabaseDescriptor.setEndpointSnitch(multiRegion);
            assertThat(logsOf(Level.WARN, () -> StartupChecks.checkDse6Ec2NamingScheme.execute(OPTIONS)))
            .containsExactly("DSE 6 compatibility mode: Ec2MultiRegionSnitch names this node's data center eu-west-1 and rack eu-west-1c" + expected);
            for (IEndpointSnitch snitch : new IEndpointSnitch[]{ legacy, new SimpleSnitch() })
            {
                DatabaseDescriptor.setEndpointSnitch(snitch);
                assertThat(logsOf(Level.WARN, () -> StartupChecks.checkDse6Ec2NamingScheme.execute(OPTIONS))).isEmpty();
            }
            none();
            DatabaseDescriptor.setEndpointSnitch(standard);
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
}
