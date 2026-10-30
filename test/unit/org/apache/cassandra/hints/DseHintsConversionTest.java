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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.DirectoryIteratorException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.CRC32;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.google.common.collect.ImmutableMap;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.auth.AuthKeyspace;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.InternodeCompatibilityMode;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.view.DseViewHiddenColumnCatalog;
import org.apache.cassandra.db.view.DseViewHiddenColumnCatalog.HiddenColumn;
import org.apache.cassandra.db.view.DseViewWireFormat;
import org.apache.cassandra.dsecompat.DseFixtures;
import org.apache.cassandra.dsecompat.DseFixtures.DseVersion;
import org.apache.cassandra.exceptions.UnrecoverableIllegalStateException;
import org.apache.cassandra.io.FSError;
import org.apache.cassandra.io.FSErrorHandler;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileInputStreamPlus;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.metrics.HintsServiceMetrics;
import org.apache.cassandra.nodes.Nodes;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaTestUtil;
import org.apache.cassandra.schema.SchemaTransformations;
import org.apache.cassandra.schema.SystemDistributedKeyspace;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.INativeLibrary;
import org.apache.cassandra.utils.JVMKiller;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.KillerForTests;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.vint.VIntCoding;

import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The conversion of the hints files DataStax Enterprise 6 left on a node upgraded in place into hints files of this
 * software ({@link DseHintsConverter}), on the 6.8.65 and 6.9.26 fixtures: which hints are converted, which are
 * dropped and why, what the new file holds, when the DSE file and its companions are deleted, and that a failure
 * keeps them and does not throw. With {@code internode_compatibility_mode: NONE} nothing is converted. The time the
 * TTL of the hints is checked against is set by each test, close after the last hint of the fixture.
 */
public class DseHintsConversionTest extends CQLTester
{
    private static final String FIXTURE_KEYSPACE = DseFixtures.FIXTURE_KEYSPACE;
    private static final TableId ROLE_PERMISSIONS = TableId.fromUUID(DseFixtures.ROLE_PERMISSIONS_TABLE_ID);
    private static final TableId NODESYNC_STATUS = TableId.fromUUID(DseFixtures.NODESYNC_STATUS_TABLE_ID);
    /** DSE's hidden column of a view of the fixture table keyed by {@code v}, as system_schema.hidden_columns lists it. */
    private static final HiddenColumn DSE_V = new HiddenColumn(".v", "regular", true, "text");
    /** DSE's hints file header: version, timestamp, host id, parameters length, CRC32, {@code {}}, CRC32. */
    private static final int HEADER_SIZE = 4 + 8 + 16 + 4 + 4 + 2 + 4;

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private File directory;
    private final List<HintsDescriptor> handedOver = new ArrayList<>();
    private Logger converterLogger;
    private ListAppender<ILoggingEvent> appender;

    /** The creation time of the hints {@link #writeViewHint} writes. */
    private final long viewHintCreationTime = currentTimeMillis();

    /** {@code system_auth}, whose {@code role_permissions} DSE hints with the fixture tables. */
    @BeforeClass
    public static void createAuthKeyspace()
    {
        SchemaTestUtil.addOrUpdateKeyspace(AuthKeyspace.metadata(), true);
    }

    @Before
    public void setUpDirectoryAndLog() throws IOException
    {
        directory = new File(folder.newFolder());
        converterLogger = (Logger) LoggerFactory.getLogger(DseHintsConverter.class);
        appender = new ListAppender<>();
        appender.start();
        converterLogger.addAppender(appender);
    }

    @After
    public void tearDownLogAndKeyspace() throws Throwable
    {
        converterLogger.detachAppender(appender);
        appender.stop();
        execute("DROP KEYSPACE IF EXISTS " + FIXTURE_KEYSPACE);
    }

    @Test
    public void theHintsOf6865AreConvertedAndTheDseFilesDeleted() throws Throwable
    {
        assertConvertedAndDeleted(DseVersion.DSE_6_8_65);
    }

    @Test
    public void theHintsOf6926AreConvertedAndTheDseFilesDeleted() throws Throwable
    {
        assertConvertedAndDeleted(DseVersion.DSE_6_9_26);
    }

    @Test
    public void theHintsOf6865ForAHostNotInTheRingAreDropped() throws Throwable
    {
        assertDroppedForAHostNotInTheRing(DseVersion.DSE_6_8_65);
    }

    @Test
    public void theHintsOf6926ForAHostNotInTheRingAreDropped() throws Throwable
    {
        assertDroppedForAHostNotInTheRing(DseVersion.DSE_6_9_26);
    }

    @Test
    public void theExpiredHintsOf6865AreDropped() throws Throwable
    {
        assertExpiredDropped(DseVersion.DSE_6_8_65);
    }

    @Test
    public void theExpiredHintsOf6926AreDropped() throws Throwable
    {
        assertExpiredDropped(DseVersion.DSE_6_9_26);
    }

    @Test
    public void aCorruptHintOf6865KeepsTheDseFiles() throws Throwable
    {
        assertCorruptLastHintKeepsTheFiles(DseVersion.DSE_6_8_65);
    }

    @Test
    public void aCorruptHintOf6926KeepsTheDseFiles() throws Throwable
    {
        assertCorruptLastHintKeepsTheFiles(DseVersion.DSE_6_9_26);
    }

    @Test
    public void aCutFileKeepsTheDseFiles() throws Throwable
    {
        createFixtureSchema(DseVersion.DSE_6_8_65);
        Path dseFile = install(DseVersion.DSE_6_8_65);
        byte[] bytes = Files.readAllBytes(dseFile);
        byte[] cut = new byte[bytes.length - 3];
        System.arraycopy(bytes, 0, cut, 0, cut.length);
        Files.write(dseFile, cut);
        List<String> before = list();

        assertThat(converter(id -> true).convert(dseFile, now(DseVersion.DSE_6_8_65))).isNull();

        assertThat(list()).containsExactlyInAnyOrderElementsOf(before);
        assertThat(Files.readAllBytes(dseFile)).isEqualTo(cut);
        assertThat(handedOver).isEmpty();
        assertThat(warnings()).singleElement().asString().contains("is cut in its hint");
    }

    /** The DSE file is the commit point: a companion that cannot be deleted afterwards is only logged. */
    @Test
    public void aCompanionThatCannotBeDeletedIsLoggedAndTheConversionStands() throws Throwable
    {
        createFixtureSchema(DseVersion.DSE_6_8_65);
        Path dseFile = install(DseVersion.DSE_6_8_65);
        // a companion that cannot be deleted: a directory that is not empty
        Path crc = companion(dseFile, ".crc32");
        Files.delete(crc);
        Files.createDirectories(crc.resolve("blocker"));

        DseHintsConverter.Counts counts = converter(id -> true).convert(dseFile, now(DseVersion.DSE_6_8_65));

        assertThat(counts).isNotNull();
        assertThat(counts.converted).isPositive();
        assertThat(dseFile).doesNotExist();
        assertThat(companion(dseFile, "-Statistics.hints")).doesNotExist();
        assertThat(crc).isDirectory();
        assertThat(handedOver).hasSize(1);
        List<String> expectedFiles = expectedNewFiles();
        expectedFiles.add(crc.getFileName().toString());
        assertThat(list()).containsExactlyInAnyOrderElementsOf(expectedFiles);
        assertThat(warnings()).singleElement().asString()
                              .startsWith("DSE 6 compatibility mode: unable to delete " + crc + ", a companion file of the DataStax Enterprise 6 hints file " + dseFile.getFileName())
                              .contains("DirectoryNotEmptyException")
                              .endsWith("It is tried again at the next start; it holds no hint.");
        assertThat(messages(Level.INFO)).singleElement().asString().endsWith("expired); deleted it");
    }

    /**
     * A disk error while converting (here from the ring check, before any hint is written) keeps the DSE files, is
     * logged and reaches neither the disk failure handler nor the JVM killer of {@code disk_failure_policy: die}, which
     * would stop the starting node; the controls show both are live.
     */
    @Test
    public void aDiskErrorKeepsTheDseFilesAndSparesTheDiskFailurePolicy() throws Throwable
    {
        createFixtureSchema(DseVersion.DSE_6_8_65);
        Path dseFile = install(DseVersion.DSE_6_8_65);
        List<String> before = list();
        Predicate<UUID> failing = id -> { throw new FSReadError(new IOException("disk read error"), dseFile.toString()); };

        List<Throwable> handled = new ArrayList<>();
        KillerForTests killer = withDiskFailurePolicyDie(handled, () -> {
            assertThat(converter(failing).convert(dseFile, now(DseVersion.DSE_6_8_65))).isNull();
            assertThat(handled).as("disk failure handler calls").isEmpty();
        });
        assertThat(killer.wasKilled()).as("killed by the conversion").isFalse();

        // controls: the same disk error, inspected, reaches both
        KillerForTests control = withDiskFailurePolicyDie(handled, () -> JVMStabilityInspector.inspectThrowable(new FSReadError(new IOException("control"), dseFile.toString())));
        assertThat(handled).hasSize(1);
        assertThat(control.wasKilled()).isTrue();

        assertThat(list()).containsExactlyInAnyOrderElementsOf(before);
        assertThat(handedOver).isEmpty();
        assertThat(warnings()).singleElement().asString()
                              .contains("unable to convert the hints file " + dseFile.getFileName() + " written by DataStax Enterprise 6")
                              .contains("disk read error")
                              .contains("left in place with its companion files");
    }

    /** Disk errors only are spared: an error the stability inspector deems fatal still reaches it. */
    @Test
    public void anUnrecoverableErrorIsStillInspected() throws Throwable
    {
        Path dseFile = install(DseVersion.DSE_6_8_65);
        List<String> before = list();
        Predicate<UUID> failing = id -> { throw new UnrecoverableIllegalStateException("unrecoverable"); };

        KillerForTests killer = withDiskFailurePolicyDie(new ArrayList<>(), () -> assertThat(converter(failing).convert(dseFile, now(DseVersion.DSE_6_8_65))).isNull());

        assertThat(killer.wasKilled()).isTrue();
        assertThat(list()).containsExactlyInAnyOrderElementsOf(before);
        assertThat(warnings()).singleElement().asString().contains("unable to convert the hints file").contains("unrecoverable");
    }

    /** An unrecoverable state is inspected even when a disk error comes with it. */
    @Test
    public void anUnrecoverableErrorWithADiskErrorIsStillInspected() throws Throwable
    {
        Path dseFile = install(DseVersion.DSE_6_8_65);
        Predicate<UUID> failing = id -> {
            UnrecoverableIllegalStateException e = new UnrecoverableIllegalStateException("unrecoverable");
            e.addSuppressed(new FSReadError(new IOException("disk error"), dseFile.toString()));
            throw e;
        };

        List<String> before = list();
        // best_effort: a disk error alone would not kill, only the unrecoverable state can
        KillerForTests killer = withDiskFailurePolicy(Config.DiskFailurePolicy.best_effort, new ArrayList<>(),
                                                      () -> assertThat(converter(failing).convert(dseFile, now(DseVersion.DSE_6_8_65))).isNull());

        assertThat(killer.wasKilled()).isTrue();
        assertThat(list()).containsExactlyInAnyOrderElementsOf(before);
        assertThat(warnings()).singleElement().asString()
                              .contains("UnrecoverableIllegalStateException: unrecoverable, suppressed: [FSReadError in ")
                              .contains(", caused by java.io.IOException: disk error]");
    }

    /** An exhaustion of file handles inside a disk error is fatal to the stability inspector: it is inspected alone. */
    @Test
    public void fileHandleExhaustionInsideADiskErrorIsInspected() throws Throwable
    {
        Path dseFile = install(DseVersion.DSE_6_8_65);
        List<String> before = list();
        Predicate<UUID> failing = id -> { throw new FSWriteError(new FileSystemException(dseFile.toString(), null, "Too many open files"), dseFile.toString()); };
        List<Throwable> handled = new ArrayList<>();

        KillerForTests killer = withDiskFailurePolicy(Config.DiskFailurePolicy.best_effort, handled,
                                                      () -> assertThat(converter(failing).convert(dseFile, now(DseVersion.DSE_6_8_65))).isNull());

        assertThat(killer.wasKilled()).isTrue();
        assertThat(handled).isEmpty();
        assertThat(list()).containsExactlyInAnyOrderElementsOf(before);
        assertThat(warnings()).singleElement().asString().contains("Too many open files");

        // control: the same disk error with an ordinary message is spared
        Predicate<UUID> ordinary = id -> { throw new FSWriteError(new FileSystemException(dseFile.toString(), null, "Input/output error"), dseFile.toString()); };
        KillerForTests control = withDiskFailurePolicy(Config.DiskFailurePolicy.best_effort, handled,
                                                       () -> assertThat(converter(ordinary).convert(dseFile, now(DseVersion.DSE_6_8_65))).isNull());
        assertThat(control.wasKilled()).isFalse();
        assertThat(handled).isEmpty();
    }

    /** A disk error hidden as the cause of another failure is spared too. */
    @Test
    public void aDiskErrorAmongTheCausesIsSpared() throws Throwable
    {
        assertHiddenDiskErrorSpared(dseFile -> new RuntimeException(new FSReadError(new IOException("hidden disk error"), dseFile.toString())));
    }

    /** And as a suppressed exception. */
    @Test
    public void aSuppressedDiskErrorIsSpared() throws Throwable
    {
        assertHiddenDiskErrorSpared(dseFile -> {
            RuntimeException e = new RuntimeException("wrapper");
            e.addSuppressed(new FSWriteError(new IOException("hidden disk error"), dseFile.toString()));
            return e;
        });
    }

    private void assertHiddenDiskErrorSpared(Function<Path, RuntimeException> failure) throws Throwable
    {
        Path dseFile = install(DseVersion.DSE_6_8_65);
        List<String> before = list();
        Predicate<UUID> failing = id -> { throw failure.apply(dseFile); };
        List<Throwable> handled = new ArrayList<>();

        KillerForTests killer = withDiskFailurePolicyDie(handled, () -> assertThat(converter(failing).convert(dseFile, now(DseVersion.DSE_6_8_65))).isNull());

        assertThat(killer.wasKilled()).isFalse();
        assertThat(handled).isEmpty();
        assertThat(list()).containsExactlyInAnyOrderElementsOf(before);
        assertThat(warnings()).singleElement().asString().contains("unable to convert the hints file").contains("hidden disk error");
    }

    /**
     * HintsWriter opened the new file, then failed writing its descriptor: the file is this run's and is deleted, the
     * hints-on-disk count is untouched, the DSE files stay.
     */
    @Test
    public void aNewFileOpenedThenFailingIsDeleted() throws Throwable
    {
        createFixtureSchema(DseVersion.DSE_6_8_65);
        Path dseFile = install(DseVersion.DSE_6_8_65);
        List<String> before = list();
        long onDisk = HintsServiceMetrics.hintsOnDisk.getCount();
        List<String> opened = new ArrayList<>();
        DseHintsConverter converter = new DseHintsConverter(directory, ImmutableMap.of(), id -> true, id -> null, handedOver::add, (dir, descriptor) -> {
            Files.createFile(descriptor.file(dir).toPath());
            opened.add(descriptor.fileName());
            throw new IOException("descriptor write failed");
        }, Files::delete);

        assertThat(converter.convert(dseFile, now(DseVersion.DSE_6_8_65))).isNull();

        assertThat(opened).hasSize(1);
        assertThat(list()).containsExactlyInAnyOrderElementsOf(before);
        assertThat(HintsServiceMetrics.hintsOnDisk.getCount()).isEqualTo(onDisk);
        assertThat(warnings()).singleElement().asString().contains("descriptor write failed");
    }

    /** The name was taken between the check and the opening: that file is not this run's and is left alone. */
    @Test
    public void aNameTakenAtTheOpeningIsLeftAlone() throws Throwable
    {
        createFixtureSchema(DseVersion.DSE_6_8_65);
        Path dseFile = install(DseVersion.DSE_6_8_65);
        List<String> before = list();
        List<Path> taken = new ArrayList<>();
        DseHintsConverter converter = new DseHintsConverter(directory, ImmutableMap.of(), id -> true, id -> null, handedOver::add, (dir, descriptor) -> {
            Path file = descriptor.file(dir).toPath();
            Files.write(file, new byte[]{ 1, 2, 3 });
            taken.add(file);
            throw new FileAlreadyExistsException(file.toString());
        }, Files::delete);

        assertThat(converter.convert(dseFile, now(DseVersion.DSE_6_8_65))).isNull();

        assertThat(taken).singleElement().satisfies(file -> assertThat(Files.readAllBytes(file)).containsExactly(1, 2, 3));
        List<String> expectedFiles = new ArrayList<>(before);
        expectedFiles.add(taken.get(0).getFileName().toString());
        assertThat(list()).containsExactlyInAnyOrderElementsOf(expectedFiles);
        assertThat(warnings()).singleElement().asString().contains("FileAlreadyExistsException");
    }

    /**
     * The DSE file cannot be deleted at the commit point: the new file, complete and counted on disk, is deleted with
     * its companions and uncounted, the DSE file and its companions stay. A disk error there is spared as well.
     */
    @Test
    public void aFailureToDeleteTheDseFileDeletesTheNewOne() throws Throwable
    {
        assertDseFileDeletionFailure(new IOException("busy"));
    }

    @Test
    public void aDiskErrorDeletingTheDseFileDeletesTheNewOne() throws Throwable
    {
        assertDseFileDeletionFailure(new FSWriteError(new IOException("read-only file system"), "the DSE file"));
    }

    private void assertDseFileDeletionFailure(Throwable failure) throws Throwable
    {
        createFixtureSchema(DseVersion.DSE_6_8_65);
        Path dseFile = install(DseVersion.DSE_6_8_65);
        List<String> before = list();
        long onDisk = HintsServiceMetrics.hintsOnDisk.getCount();
        long[] onDiskAtTheCommitPoint = new long[1];
        List<String> atTheCommitPoint = new ArrayList<>();
        DseHintsConverter converter = new DseHintsConverter(directory, ImmutableMap.of(), id -> true, id -> null, handedOver::add, HintsWriter::create, file -> {
            onDiskAtTheCommitPoint[0] = HintsServiceMetrics.hintsOnDisk.getCount();
            atTheCommitPoint.addAll(list());
            if (failure instanceof IOException)
                throw (IOException) failure;
            throw (Error) failure;
        });
        Expected expected = expected(DseVersion.DSE_6_8_65, TableId.fromUUID(DseFixtures.fixtureTableId(DseVersion.DSE_6_8_65)));

        KillerForTests killer = withDiskFailurePolicyDie(new ArrayList<>(), () -> assertThat(converter.convert(dseFile, now(DseVersion.DSE_6_8_65))).isNull());

        assertThat(killer.wasKilled()).isFalse();
        assertThat(onDiskAtTheCommitPoint[0] - onDisk).as("hints counted on disk at the commit point").isEqualTo(expected.converted);
        assertThat(atTheCommitPoint).hasSize(before.size() + 3); // the new file and its two companions
        assertThat(HintsServiceMetrics.hintsOnDisk.getCount()).isEqualTo(onDisk);
        assertThat(list()).containsExactlyInAnyOrderElementsOf(before);
        assertThat(handedOver).isEmpty();
        assertThat(warnings()).singleElement().asString()
                              .contains("unable to convert the hints file " + dseFile.getFileName())
                              .contains(failure.getClass().getSimpleName());
    }

    /** The guard of the dispatch start: a failure outside the conversion of a file is logged and goes no further. */
    @Test
    public void aFailureOutsideTheFilesIsLoggedAndNeverStopsTheStart()
    {
        DseHintsConverter converter = converter(id -> true);
        DseHintsConverter.convertAtDispatchStart(() -> converter.convertAll(dir -> { throw new DirectoryIteratorException(new IOException("listing broke")); }, 0));

        assertThat(warnings()).singleElement().asString()
                              .startsWith("DSE 6 compatibility mode: the conversion of the hints files written by DataStax Enterprise 6 failed: " +
                                          "java.nio.file.DirectoryIteratorException")
                              .contains("listing broke")
                              .endsWith("The files not converted yet are left in place and converted at the next start of this node.");
    }

    /** The DSE file disappeared before the commit point: the new file holds its hints and is handed over. */
    @Test
    public void aDseFileGoneAtTheCommitPointKeepsTheNewFile() throws Throwable
    {
        createFixtureSchema(DseVersion.DSE_6_9_26);
        Path dseFile = install(DseVersion.DSE_6_9_26);
        DseHintsConverter converter = new DseHintsConverter(directory, ImmutableMap.of(), id -> true, id -> null, handedOver::add, HintsWriter::create, file -> {
            Files.delete(file);
            throw new NoSuchFileException(file.toString());
        });

        assertThat(converter.convert(dseFile, now(DseVersion.DSE_6_9_26))).isNotNull();

        assertThat(dseFile).doesNotExist();
        assertThat(handedOver).hasSize(1);
        assertThat(list()).containsExactlyInAnyOrderElementsOf(expectedNewFiles());
        assertThat(warnings()).singleElement().asString()
                              .isEqualTo("DSE 6 compatibility mode: the hints file " + dseFile.getFileName() + " written by DataStax Enterprise 6 " +
                                         "disappeared while it was converted; its hints are kept in " + handedOver.get(0).fileName());
        assertThat(messages(Level.INFO)).singleElement().asString().endsWith("expired); found it gone, deleted its companion files");
    }

    /** Same with nothing to convert: no new file, the WARN says so. */
    @Test
    public void aDseFileWithNothingToConvertGoneAtTheCommitPoint() throws Throwable
    {
        Path dseFile = install(DseVersion.DSE_6_9_26);
        DseHintsConverter converter = new DseHintsConverter(directory, ImmutableMap.of(), id -> false, id -> null, handedOver::add, HintsWriter::create, file -> {
            Files.delete(file);
            throw new NoSuchFileException(file.toString());
        });

        assertThat(converter.convert(dseFile, now(DseVersion.DSE_6_9_26))).isNotNull();

        assertThat(handedOver).isEmpty();
        assertThat(list()).isEmpty();
        // the hints were all for a host not in the ring, which has its own WARN
        assertThat(warnings()).hasSize(2)
                              .contains("DSE 6 compatibility mode: the hints file " + dseFile.getFileName() + " written by DataStax Enterprise 6 " +
                                        "disappeared while it was converted; it had no hint to keep")
                              .anyMatch(w -> w.startsWith("DSE 6 compatibility mode: dropped the "));
        assertThat(messages(Level.INFO)).singleElement().asString()
                                        .contains("0 hint(s) written to no file")
                                        .endsWith("expired); found it gone, deleted its companion files");
    }

    /** A failing endpoint provider, used only for the log line, does not undo the conversion. */
    @Test
    public void aFailingEndpointLookupLeavesTheConversionStanding() throws Throwable
    {
        assertEndpointFailureHarmless(new IllegalStateException("custom provider broke"));
    }

    @Test
    public void anAssertingEndpointLookupLeavesTheConversionStanding() throws Throwable
    {
        assertEndpointFailureHarmless(new AssertionError("custom provider broke"));
    }

    /**
     * An Error of the endpoint provider (which the lookup for the log line does not catch) comes past the commit point
     * and the hand-over: the conversion stands, and the WARN says what was done before the failure.
     */
    @Test
    public void anErrorAfterTheCommitPointLeavesTheConversionStanding() throws Throwable
    {
        createFixtureSchema(DseVersion.DSE_6_9_26);
        Path dseFile = install(DseVersion.DSE_6_9_26);
        HintsDescriptor dse = readDescriptor(dseFile);
        Error failure = new Error("custom provider broke");
        DseHintsConverter converter = new DseHintsConverter(directory, ImmutableMap.of(), id -> true, id -> { throw failure; }, handedOver::add);

        assertThat(converter.convert(dseFile, now(DseVersion.DSE_6_9_26))).isNotNull();

        assertThat(dseFile).doesNotExist();
        assertThat(handedOver).hasSize(1);
        Path newFile = handedOver.get(0).file(directory).toPath();
        assertThat(newFile).exists();
        assertThat(DseFixtures.hintsRecords(newFile)).hasSize(expected(DseVersion.DSE_6_9_26, TableId.fromUUID(DseFixtures.fixtureTableId(DseVersion.DSE_6_9_26))).converted);
        assertThat(messages(Level.INFO)).isEmpty();
        assertThat(warnings()).singleElement().asString()
                              .isEqualTo("DSE 6 compatibility mode: converted the hints file " + dseFile.getFileName() + " written by " +
                                         "DataStax Enterprise 6 for host " + dse.hostId + " into " + handedOver.get(0).fileName() +
                                         " and deleted it, then failed: " + failure);
    }

    private void assertEndpointFailureHarmless(Throwable failure) throws Throwable
    {
        createFixtureSchema(DseVersion.DSE_6_9_26);
        Path dseFile = install(DseVersion.DSE_6_9_26);
        DseHintsConverter converter = new DseHintsConverter(directory, ImmutableMap.of(), id -> true, id -> {
            if (failure instanceof Error)
                throw (Error) failure;
            throw (RuntimeException) failure;
        }, handedOver::add);

        assertThat(converter.convert(dseFile, now(DseVersion.DSE_6_9_26))).isNotNull();

        assertThat(dseFile).doesNotExist();
        assertThat(handedOver).hasSize(1);
        assertThat(warnings()).isEmpty();
        assertThat(messages(Level.INFO)).singleElement().asString()
                                        .contains("(address unknown: " + failure + ')');
    }

    /**
     * Closing the writing session fails (its flush): only the channel is closed, no checksum or statistics file is
     * written, the new file is deleted and was never counted on disk, the DSE files stay.
     */
    @Test
    public void aFailureClosingTheSessionLeavesNothingBehind() throws Throwable
    {
        createFixtureSchema(DseVersion.DSE_6_8_65);
        Path dseFile = install(DseVersion.DSE_6_8_65);
        List<String> before = list();
        long onDisk = HintsServiceMetrics.hintsOnDisk.getCount();
        DseHintsConverter converter = new DseHintsConverter(directory, ImmutableMap.of(), id -> true, id -> null, handedOver::add, (dir, descriptor) -> {
            HintsWriter real = HintsWriter.create(dir, descriptor);
            return new HintsWriter(dir, descriptor, real.getFile(), real.channel, INativeLibrary.instance.getfd(real.channel), real.globalCRC)
            {
                @Override
                protected void writeBuffer(ByteBuffer bb) throws IOException
                {
                    throw new IOException("flush failed");
                }
            };
        }, Files::delete);

        assertThat(converter.convert(dseFile, now(DseVersion.DSE_6_8_65))).isNull();

        assertThat(list()).containsExactlyInAnyOrderElementsOf(before);
        assertThat(HintsServiceMetrics.hintsOnDisk.getCount()).isEqualTo(onDisk);
        assertThat(handedOver).isEmpty();
        assertThat(warnings()).singleElement().asString().contains("flush failed");
    }

    /**
     * Out of memory in the conversion of a file (here when the new file is opened) goes on, after the partial new file
     * is deleted; the stability inspector sees it once, at the dispatch start, alone even with a disk error beside it.
     */
    @Test
    public void outOfMemoryIsInspectedOnceAtTheDispatchStart() throws Throwable
    {
        assertOutOfMemoryInspectedOnce(false);
    }

    @Test
    public void outOfMemoryWithADiskErrorIsInspectedOnceAtTheDispatchStart() throws Throwable
    {
        assertOutOfMemoryInspectedOnce(true);
    }

    private void assertOutOfMemoryInspectedOnce(boolean withDiskError) throws Throwable
    {
        createFixtureSchema(DseVersion.DSE_6_8_65);
        Path dseFile = install(DseVersion.DSE_6_8_65);
        List<String> before = list();
        OutOfMemoryError oom = new OutOfMemoryError("test");
        long now = now(DseVersion.DSE_6_8_65);
        DseHintsConverter converter = new DseHintsConverter(directory, ImmutableMap.of(), id -> true, id -> null, handedOver::add, (dir, descriptor) -> {
            Files.createFile(descriptor.file(dir).toPath());
            RuntimeException e = new RuntimeException(oom);
            if (withDiskError)
                e.addSuppressed(new FSWriteError(new IOException("disk error"), descriptor.fileName()));
            throw e;
        }, Files::delete);
        List<Throwable> inspected = new ArrayList<>();
        BiConsumer<Throwable, Boolean> previousHandler = JVMStabilityInspector.getGlobalErrorHandler();
        List<Throwable> handled = new ArrayList<>();
        try
        {
            JVMStabilityInspector.setGlobalErrorHandler((error, uncaught) -> inspected.add(error));
            KillerForTests killer = withDiskFailurePolicyDie(handled, () -> {
                assertThatThrownBy(() -> converter.convert(dseFile, now)).isInstanceOf(RuntimeException.class).hasCause(oom);
                assertThat(inspected).as("inspected by the conversion of the file").isEmpty();
                assertThatThrownBy(() -> DseHintsConverter.convertAtDispatchStart(() -> converter.convertAll(HintsCatalog::dseHintsFiles, now)))
                .isSameAs(oom);
            });
            // holds only because the recording handler replaces the default one, the only caller of the killer
            assertThat(killer.wasKilled()).isFalse();
        }
        finally
        {
            JVMStabilityInspector.setGlobalErrorHandler(previousHandler);
        }
        assertThat(inspected).containsExactly(oom);
        // one per conversion of the file: the direct call and the one through the dispatch start
        assertThat(warnings()).hasSize(2).allSatisfy(w -> assertThat(w).contains("unable to convert the hints file")
                                                                     .contains("java.lang.OutOfMemoryError: test"));
        assertThat(handled).as("disk failure handler calls").isEmpty();
        assertThat(list()).containsExactlyInAnyOrderElementsOf(before);
        assertThat(handedOver).isEmpty();
    }

    /** The guard of the dispatch start spares a disk error too. */
    @Test
    public void aDiskErrorOutsideTheFilesNeverStopsTheStart() throws Throwable
    {
        List<Throwable> handled = new ArrayList<>();
        KillerForTests killer = withDiskFailurePolicyDie(handled, () -> DseHintsConverter.convertAtDispatchStart(() -> {
            throw new FSReadError(new IOException("listing disk error"), directory.toString());
        }));

        assertThat(killer.wasKilled()).isFalse();
        assertThat(handled).isEmpty();
        assertThat(warnings()).singleElement().asString().contains("listing disk error");
    }

    /**
     * Runs the body with {@code disk_failure_policy: die}, a test killer and a recording disk failure handler, all
     * restored after it; returns the killer.
     */
    private static KillerForTests withDiskFailurePolicyDie(List<Throwable> handled, ThrowingRunnable body) throws Throwable
    {
        return withDiskFailurePolicy(Config.DiskFailurePolicy.die, handled, body);
    }

    /** As {@link #withDiskFailurePolicyDie}, with the given policy. */
    private static KillerForTests withDiskFailurePolicy(Config.DiskFailurePolicy policy, List<Throwable> handled, ThrowingRunnable body) throws Throwable
    {
        Config.DiskFailurePolicy previousPolicy = DatabaseDescriptor.getDiskFailurePolicy();
        KillerForTests killer = new KillerForTests();
        JVMKiller previousKiller = JVMStabilityInspector.replaceKiller(killer);
        FileUtils.setFSErrorHandler(new FSErrorHandler()
        {
            public void handleCorruptSSTable(CorruptSSTableException e)
            {
                handled.add(e);
            }

            public void handleFSError(FSError e)
            {
                handled.add(e);
            }
        });
        try
        {
            DatabaseDescriptor.setDiskFailurePolicy(policy);
            body.run();
            return killer;
        }
        finally
        {
            // FileUtils has no getter for the handler; the unit-test JVM has none set (the daemon sets it at its start)
            FileUtils.setFSErrorHandler(null);
            JVMStabilityInspector.replaceKiller(previousKiller);
            DatabaseDescriptor.setDiskFailurePolicy(previousPolicy);
        }
    }

    private interface ThrowingRunnable
    {
        void run() throws Throwable;
    }

    /** Past the commit point the new file is the only copy of the hints: a failure keeps it for the next start. */
    @Test
    public void aFailureAfterTheDseFileIsDeletedKeepsTheNewFile() throws Throwable
    {
        createFixtureSchema(DseVersion.DSE_6_9_26);
        Path dseFile = install(DseVersion.DSE_6_9_26);
        List<HintsDescriptor> offered = new ArrayList<>();
        DseHintsConverter converter = new DseHintsConverter(directory, ImmutableMap.of(), id -> true, id -> null, d -> {
            offered.add(d);
            throw new FSWriteError(new IOException("no space left"), d.fileName());
        });

        assertThat(converter.convert(dseFile, now(DseVersion.DSE_6_9_26))).as("converted, its dispatch left to the next start").isNotNull();

        assertThat(offered).hasSize(1);
        assertThat(dseFile).doesNotExist();
        handedOver.addAll(offered);
        assertThat(list()).containsExactlyInAnyOrderElementsOf(expectedNewFiles());
        assertThat(warnings()).singleElement().asString()
                              .startsWith("DSE 6 compatibility mode: converted the hints file " + dseFile.getFileName() + " written by DataStax Enterprise 6 for host ")
                              .contains("into " + offered.get(0).fileName() + " and deleted it, but could not hand the new file to the hints dispatch")
                              .contains("no space left")
                              .endsWith("It is dispatched after the next start of this node.");
    }

    @Test
    public void aFileWhoseDescriptorIsNotDsesIsKept() throws Throwable
    {
        Path file = DseFixtures.writeDseHintsFile(DseVersion.DSE_6_8_65, directory.toPath(), UUID.randomUUID(), 1L, 2L, 2, "{}");
        byte[] before = Files.readAllBytes(file);

        assertThat(converter(id -> true).convert(file, now(DseVersion.DSE_6_8_65))).isNull();

        assertThat(Files.readAllBytes(file)).isEqualTo(before);
        assertThat(list()).containsExactly(file.getFileName().toString());
        assertThat(warnings()).singleElement().asString().contains("its descriptor has version 2, not DSE's -1");
    }

    @Test
    public void aCompressedDseFileIsKept() throws Throwable
    {
        Path file = DseFixtures.writeDseHintsFile(DseVersion.DSE_6_8_65, directory.toPath(), UUID.randomUUID(), 1L, 2L, -1,
                                                  "{\"compression\":{\"class_name\":\"LZ4Compressor\"}}");

        assertThat(converter(id -> true).convert(file, now(DseVersion.DSE_6_8_65))).isNull();

        assertThat(list()).containsExactly(file.getFileName().toString());
        assertThat(warnings()).singleElement().asString()
                              .contains("its descriptor has parameters {compression={class_name=LZ4Compressor}} (compression or encryption)");
    }

    @Test
    public void aRecordWhoseSizeFailsItsChecksumKeepsTheFile() throws Throwable
    {
        Path dseFile = install(DseVersion.DSE_6_8_65);
        byte[] bytes = Files.readAllBytes(dseFile);
        bytes[HEADER_SIZE + Integer.BYTES] ^= 0x01; // the CRC32 of the size of the first record
        Files.write(dseFile, bytes);

        assertThat(converter(id -> true).convert(dseFile, now(DseVersion.DSE_6_8_65))).isNull();

        assertThat(Files.readAllBytes(dseFile)).isEqualTo(bytes);
        assertThat(warnings()).singleElement().asString().contains("the record at offset " + HEADER_SIZE + " fails the checksum of its size");
    }

    @Test
    public void aNegativeSizeKeepsTheFile() throws Throwable
    {
        Path dseFile = install(DseVersion.DSE_6_8_65);
        byte[] bytes = Files.readAllBytes(dseFile);
        // the first record's size set to -1, with a valid CRC32 of the size
        CRC32 crc = new CRC32();
        crc.update(new byte[]{ -1, -1, -1, -1 });
        ByteBuffer.wrap(bytes).putInt(HEADER_SIZE, -1).putInt(HEADER_SIZE + Integer.BYTES, (int) crc.getValue());
        Files.write(dseFile, bytes);

        assertThat(converter(id -> true).convert(dseFile, now(DseVersion.DSE_6_8_65))).isNull();

        assertThat(Files.readAllBytes(dseFile)).isEqualTo(bytes);
        assertThat(warnings()).singleElement().asString().contains("the record at offset " + HEADER_SIZE + " is cut in its hint (size -1)");
    }

    /**
     * A record whose checksums hold but whose hint has bytes left after its mutation is damaged content: the file is
     * kept, and the WARN says that trying again at the next start will not help.
     */
    @Test
    public void bytesLeftAfterTheMutationKeepTheFile() throws Throwable
    {
        createFixtureSchema(DseVersion.DSE_6_8_65);
        Path dseFile = install(DseVersion.DSE_6_8_65);
        byte[] original = Files.readAllBytes(dseFile);
        byte[] first = DseFixtures.hintsRecords(DseVersion.DSE_6_8_65).get(0);
        byte[] longer = Arrays.copyOf(first, first.length + 1);
        ByteBuffer file = ByteBuffer.allocate(HEADER_SIZE + 3 * Integer.BYTES + longer.length);
        file.put(original, 0, HEADER_SIZE);
        putRecord(file, longer);
        byte[] bytes = file.array();
        Files.write(dseFile, bytes);

        assertThat(converter(id -> true).convert(dseFile, now(DseVersion.DSE_6_8_65))).isNull();

        assertThat(Files.readAllBytes(dseFile)).isEqualTo(bytes);
        assertThat(handedOver).isEmpty();
        assertThat(warnings()).singleElement().asString()
                              .contains("the hint at offset " + HEADER_SIZE + " has 1 byte(s) left after its mutation")
                              .contains("Its content cannot be converted: the conversion fails the same way at every start of this node, which starts all the same.");
    }

    /** Fewer bytes than a record's size and its checksum after the last whole record: the file is cut in that size. */
    @Test
    public void aFileCutInTheSizeOfARecordKeepsTheFile() throws Throwable
    {
        createFixtureSchema(DseVersion.DSE_6_8_65);
        Path dseFile = install(DseVersion.DSE_6_8_65);
        byte[] original = Files.readAllBytes(dseFile);
        byte[] first = DseFixtures.hintsRecords(DseVersion.DSE_6_8_65).get(0);
        int cutAt = HEADER_SIZE + 3 * Integer.BYTES + first.length;
        ByteBuffer file = ByteBuffer.allocate(cutAt + 5);
        file.put(original, 0, HEADER_SIZE);
        putRecord(file, first);
        file.put(new byte[]{ 0, 0, 0, 1, 0 });
        byte[] bytes = file.array();
        Files.write(dseFile, bytes);

        assertThat(converter(id -> true).convert(dseFile, now(DseVersion.DSE_6_8_65))).isNull();

        assertThat(Files.readAllBytes(dseFile)).isEqualTo(bytes);
        assertThat(handedOver).isEmpty();
        assertThat(list()).containsExactlyInAnyOrder(dseFile.getFileName().toString(), companion(dseFile, ".crc32").getFileName().toString(),
                                                     companion(dseFile, "-Statistics.hints").getFileName().toString());
        assertThat(warnings()).singleElement().asString()
                              .contains("the record at offset " + cutAt + " is cut in its size")
                              .contains("the conversion fails the same way at every start of this node");
    }

    /** A transient failure (here the deletion of the DSE file at the commit point) is tried again at the next start. */
    @Test
    public void aWriteOrDeleteFailureIsTriedAgainAtTheNextStart() throws Throwable
    {
        createFixtureSchema(DseVersion.DSE_6_8_65);
        Path dseFile = install(DseVersion.DSE_6_8_65);
        DseHintsConverter converter = new DseHintsConverter(directory, ImmutableMap.of(), id -> true, id -> null, handedOver::add, HintsWriter::create, file -> {
            throw new IOException("delete refused");
        });

        assertThat(converter.convert(dseFile, now(DseVersion.DSE_6_8_65))).isNull();

        assertThat(dseFile).exists();
        assertThat(warnings()).singleElement().asString()
                              .contains("The conversion is tried again at the next start of this node")
                              .doesNotContain("the conversion fails the same way at every start of this node");
    }

    /**
     * Companion files of a DSE hints file that is gone (a crash between the commit point and their deletion) are
     * deleted at the next conversion run; those of a DSE file still there (here one the lister does not return, so
     * that it is not converted in the same run), of one set aside as {@code .corrupt.hints}, and the files of this
     * software stay.
     */
    @Test
    public void companionsOfAGoneDseFileAreDeletedAtTheNextRun() throws Throwable
    {
        String gone = "ed39c7b2-7f97-4a0a-91fc-71398afe1d12-1790335633207-v1";
        String setAside = "ed39c7b2-7f97-4a0a-91fc-71398afe1d12-1790335633206-v1";
        Path orphanChecksum = Files.write(directory.toPath().resolve(gone + ".crc32"), "0".getBytes());
        Path orphanStatistics = Files.write(directory.toPath().resolve(gone + "-Statistics.hints"), new byte[8]);
        Path corrupt = Files.write(directory.toPath().resolve(setAside + ".corrupt.hints"), new byte[8]);
        Path corruptChecksum = Files.write(directory.toPath().resolve(setAside + ".crc32"), "0".getBytes());
        Path dseFile = install(DseVersion.DSE_6_8_65);
        String own = UUID.randomUUID() + "-1790335633209-" + HintsDescriptor.currentStorageVersion();
        Path ownChecksum = Files.write(directory.toPath().resolve(own + ".crc32"), "0".getBytes());
        Path ownStatistics = Files.write(directory.toPath().resolve(own + "-Statistics.hints"), new byte[8]);
        List<String> kept = Arrays.asList(corrupt.getFileName().toString(), corruptChecksum.getFileName().toString(),
                                          dseFile.getFileName().toString(), companion(dseFile, ".crc32").getFileName().toString(),
                                          companion(dseFile, "-Statistics.hints").getFileName().toString(),
                                          ownChecksum.getFileName().toString(), ownStatistics.getFileName().toString());

        DseHintsConverter.Summary summary = converter(id -> true).convertAll(dir -> Collections.emptyList(), now(DseVersion.DSE_6_8_65));

        assertThat(summary.filesFound).isZero();
        assertThat(list()).containsExactlyInAnyOrderElementsOf(kept);
        assertThat(messages(Level.INFO)).containsExactlyInAnyOrder("DSE 6 compatibility mode: deleted " + orphanChecksum.getFileName() +
                                                                   ", a companion file of a DataStax Enterprise 6 hints file that is gone; it held no hint",
                                                                   "DSE 6 compatibility mode: deleted " + orphanStatistics.getFileName() +
                                                                   ", a companion file of a DataStax Enterprise 6 hints file that is gone; it held no hint");
        assertThat(warnings()).isEmpty();
    }

    /** A DSE file deleted between the listing and its conversion is neither converted nor kept after a failure. */
    @Test
    public void aDseFileGoneBeforeItsConversionIsNotAFailure() throws Throwable
    {
        Path dseFile = directory.toPath().resolve("ed39c7b2-7f97-4a0a-91fc-71398afe1d12-1790335633208-v1.hints");
        Path checksum = Files.write(companion(dseFile, ".crc32"), "0".getBytes());

        DseHintsConverter.Summary summary = converter(id -> true).convertAll(dir -> Collections.singletonList(dseFile), now(DseVersion.DSE_6_8_65));

        assertThat(summary.filesFound).isEqualTo(1);
        assertThat(summary.filesGone).isEqualTo(1);
        assertThat(summary.filesConverted + summary.filesKept).isZero();
        assertThat(checksum).doesNotExist();
        assertThat(warnings()).isEmpty();
        assertThat(messages(Level.INFO)).contains("DSE 6 compatibility mode: the hints file " + dseFile.getFileName() + " written by DataStax " +
                                                  "Enterprise 6 disappeared before it was converted; nothing to convert")
                                        // gone before the run: its companion is an orphan, deleted by the sweep at the start of the run
                                        .contains("DSE 6 compatibility mode: deleted " + checksum.getFileName() + ", a companion file of a " +
                                                  "DataStax Enterprise 6 hints file that is gone; it held no hint")
                                        .anyMatch(m -> m.contains(", 0 converted and deleted, 1 gone before their conversion, 0 left in place after a failure; "));
    }

    /**
     * An empty DSE file (no descriptor, no hint) is deleted with its companions, and counted as converted and deleted,
     * with no hint.
     */
    @Test
    public void anEmptyDseFileIsDeleted() throws Throwable
    {
        Path dseFile = install(DseVersion.DSE_6_8_65);
        Files.write(dseFile, new byte[0]);

        DseHintsConverter.Summary summary = converter(id -> true).convertAll(dir -> Collections.singletonList(dseFile), now(DseVersion.DSE_6_8_65));

        assertThat(summary.filesConverted).isEqualTo(1);
        assertThat(summary.filesGone + summary.filesKept).isZero();
        assertThat(summary.hints.converted + summary.hints.dropped()).isZero();
        assertThat(list()).isEmpty();
        assertThat(handedOver).isEmpty();
        assertThat(warnings()).isEmpty();
        assertThat(messages(Level.INFO)).contains("DSE 6 compatibility mode: the hints file " + dseFile.getFileName() + " written by " +
                                                  "DataStax Enterprise 6 is empty, with no hint to convert; deleted it and its companion files")
                                        .anyMatch(m -> m.contains(", 1 converted and deleted, 0 gone before their conversion, 0 left in place after a failure; " +
                                                                  "0 hint(s) converted, 0 dropped"));
    }

    /** An empty DSE file that cannot be deleted is kept, with its companions, and tried again at the next start. */
    @Test
    public void anEmptyDseFileThatCannotBeDeletedIsKept() throws Throwable
    {
        Path dseFile = install(DseVersion.DSE_6_8_65);
        Files.write(dseFile, new byte[0]);
        List<String> before = list();
        DseHintsConverter converter = new DseHintsConverter(directory, ImmutableMap.of(), id -> true, id -> null, handedOver::add, HintsWriter::create, file -> {
            throw new IOException("delete refused");
        });

        DseHintsConverter.Summary summary = converter.convertAll(dir -> Collections.singletonList(dseFile), now(DseVersion.DSE_6_8_65));

        assertThat(summary.filesKept).isEqualTo(1);
        assertThat(summary.filesConverted + summary.filesGone).isZero();
        assertThat(list()).containsExactlyInAnyOrderElementsOf(before);
        assertThat(handedOver).isEmpty();
        assertThat(messages(Level.INFO)).noneMatch(m -> m.contains(dseFile.getFileName() + " written by DataStax Enterprise 6 is empty"));
        assertThat(warnings()).singleElement().asString()
                              .isEqualTo("DSE 6 compatibility mode: the hints file " + dseFile.getFileName() + " written by DataStax " +
                                         "Enterprise 6 is empty, with no hint to convert, and could not be deleted: " +
                                         "java.io.IOException: delete refused. It is tried again at the next start of this node.");
    }

    /** An empty DSE file deleted by someone else between the opening and the deletion is gone, not a failure. */
    @Test
    public void anEmptyDseFileGoneBeforeItsDeletionIsNotAFailure() throws Throwable
    {
        Path dseFile = install(DseVersion.DSE_6_8_65);
        Files.write(dseFile, new byte[0]);
        DseHintsConverter converter = new DseHintsConverter(directory, ImmutableMap.of(), id -> true, id -> null, handedOver::add, HintsWriter::create, file -> {
            Files.delete(file);
            throw new NoSuchFileException(file.toString());
        });

        DseHintsConverter.Summary summary = converter.convertAll(dir -> Collections.singletonList(dseFile), now(DseVersion.DSE_6_8_65));

        assertThat(summary.filesGone).isEqualTo(1);
        assertThat(summary.filesConverted + summary.filesKept).isZero();
        assertThat(list()).isEmpty();
        assertThat(handedOver).isEmpty();
        assertThat(warnings()).isEmpty();
        // the file was there at the sweep, so its companions are deleted with the result of the conversion
        assertThat(messages(Level.INFO)).contains("DSE 6 compatibility mode: the hints file " + dseFile.getFileName() + " written by DataStax " +
                                                  "Enterprise 6 disappeared before it was converted; nothing to convert, deleted its companion files")
                                        .anyMatch(m -> m.contains(", 0 converted and deleted, 1 gone before their conversion, 0 left in place after a failure; "));
    }

    /** A descriptor that fails its checksum is unconvertible content: the file is kept, and the WARN says why. */
    @Test
    public void aDescriptorFailingItsChecksumKeepsTheFile() throws Throwable
    {
        Path dseFile = install(DseVersion.DSE_6_8_65);
        byte[] bytes = Files.readAllBytes(dseFile);
        bytes[4] ^= 0x01; // the timestamp of the descriptor, covered by its first CRC32
        Files.write(dseFile, bytes);

        List<String> before = list();

        assertThat(converter(id -> true).convert(dseFile, now(DseVersion.DSE_6_8_65))).isNull();

        assertThat(Files.readAllBytes(dseFile)).isEqualTo(bytes);
        assertThat(list()).as("the DSE file and its companions, no new file").containsExactlyInAnyOrderElementsOf(before).hasSize(3);
        assertThat(handedOver).isEmpty();
        assertThat(warnings()).singleElement().asString()
                              .startsWith("DSE 6 compatibility mode: unable to convert the hints file " + dseFile.getFileName() +
                                          " written by DataStax Enterprise 6, left in place with its companion files: its descriptor " +
                                          "does not read, caused by org.apache.cassandra.hints.ChecksumMismatchException: Hints Descriptor CRC Mismatch.")
                              .contains("Its content cannot be converted: the conversion fails the same way at every start of this node, which starts all the same.")
                              .doesNotContain("UnconvertibleFileException");
    }

    /** Appends a record framed as DSE frames it: size, CRC32 of the size, hint, CRC32 of the size and the hint. */
    private static void putRecord(ByteBuffer file, byte[] hint)
    {
        CRC32 crc = new CRC32();
        byte[] size = ByteBuffer.allocate(Integer.BYTES).putInt(hint.length).array();
        crc.update(size);
        file.put(size).putInt((int) crc.getValue());
        crc.update(hint);
        file.put(hint).putInt((int) crc.getValue());
    }

    /**
     * The TTL is the smaller of the gc grace seconds the hint carries and the table's: with the table's raised to 30
     * days, the fixture table hints (10 days in the hint) are expired 11 days after they were created, those of
     * {@code system_auth.role_permissions} (90 days) are not.
     */
    @Test
    public void theGcGraceSecondsOfTheHintBoundItsTtl() throws Throwable
    {
        createFixtureSchema(DseVersion.DSE_6_8_65);
        execute("ALTER TABLE " + FIXTURE_KEYSPACE + ".t WITH gc_grace_seconds = 2592000");
        Path dseFile = install(DseVersion.DSE_6_8_65);
        Expected expected = expected(DseVersion.DSE_6_8_65, TableId.fromUUID(DseFixtures.fixtureTableId(DseVersion.DSE_6_8_65)));

        DseHintsConverter.Counts counts = converter(id -> true).convert(dseFile, lastCreationTime(DseVersion.DSE_6_8_65) + TimeUnit.DAYS.toMillis(11));

        assertThat(counts).isNotNull();
        assertThat(counts.expired).isEqualTo(expected.fixtureHints).isPositive();
        assertThat(counts.converted).isEqualTo(expected.converted - expected.fixtureHints).isPositive();
    }

    /** The default ring check also accepts a host id known only from {@code system.peers}. */
    @Test
    public void dse6ModeConvertsForAHostKnownOnlyFromThePeersTable() throws Throwable
    {
        createFixtureSchema(DseVersion.DSE_6_8_65);
        UUID hostId = UUID.randomUUID();
        InetAddressAndPort peer = InetAddressAndPort.getByName("127.0.0.86");
        long timestamp = currentTimeMillis();
        Path dseFile = DseFixtures.writeDseHintsFile(DseVersion.DSE_6_8_65, directory.toPath(), hostId, timestamp, timestamp);
        HintsCatalog catalog = HintsCatalog.load(directory, ImmutableMap.of());
        assertThat(StorageService.instance.getEndpointForHostId(hostId)).isNull();
        Nodes.peers().update(peer, info -> info.setHostId(hostId), true);
        try
        {
            assertThat(SystemKeyspace.loadHostIds()).containsEntry(peer, hostId);
            DseHintsConverter.Summary summary = withMode(InternodeCompatibilityMode.DSE_6, () -> DseHintsConverter.convertIfDse6(catalog));
            assertThat(summary.hints.unknownHost).isZero();
            assertThat(summary.hints.converted).isPositive();
        }
        finally
        {
            Nodes.peers().remove(peer, true, false);
        }
        assertThat(dseFile).doesNotExist();
        assertThat(catalog.getNullable(hostId)).isNotNull();
    }

    /** A host id neither in the token metadata nor in {@code system.peers}: the default ring check drops its hints. */
    @Test
    public void dse6ModeDropsTheHintsForAnUnknownHost() throws Throwable
    {
        createFixtureSchema(DseVersion.DSE_6_8_65);
        UUID hostId = UUID.randomUUID();
        long timestamp = currentTimeMillis();
        Path dseFile = DseFixtures.writeDseHintsFile(DseVersion.DSE_6_8_65, directory.toPath(), hostId, timestamp, timestamp);
        int records = DseFixtures.hintsRecords(DseVersion.DSE_6_8_65).size();
        HintsCatalog catalog = HintsCatalog.load(directory, ImmutableMap.of());

        DseHintsConverter.Summary summary = withMode(InternodeCompatibilityMode.DSE_6, () -> DseHintsConverter.convertIfDse6(catalog));

        assertThat(summary.hints.unknownHost).isEqualTo(records);
        assertThat(summary.hints.converted).isZero();
        assertThat(dseFile).doesNotExist();
        assertThat(catalog.getNullable(hostId)).isNull();
    }

    @Test
    public void aNameTakenByAnInterruptedConversionMovesTheNewFileToTheNextTimestamp() throws Throwable
    {
        createFixtureSchema(DseVersion.DSE_6_9_26);
        Path dseFile = install(DseVersion.DSE_6_9_26);
        HintsDescriptor dse = readDescriptor(dseFile);
        // a name is taken by any of the three files of a hints file of this software
        List<Path> placeholders = new ArrayList<>();
        placeholders.add(directory.toPath().resolve(new HintsDescriptor(dse.hostId, dse.timestamp, ImmutableMap.of()).fileName()));
        placeholders.add(directory.toPath().resolve(new HintsDescriptor(dse.hostId, dse.timestamp + 1, ImmutableMap.of()).checksumFileName()));
        placeholders.add(directory.toPath().resolve(new HintsDescriptor(dse.hostId, dse.timestamp + 2, ImmutableMap.of()).statisticsFileName()));
        for (Path placeholder : placeholders)
            Files.createFile(placeholder);

        assertThat(converter(id -> true).convert(dseFile, now(DseVersion.DSE_6_9_26))).isNotNull();

        assertThat(handedOver).singleElement().satisfies(d -> {
            assertThat(d.hostId).isEqualTo(dse.hostId);
            assertThat(d.timestamp).isEqualTo(dse.timestamp + 3);
        });
        for (Path placeholder : placeholders)
            assertThat(placeholder).exists().isEmptyFile();
    }

    @Test
    public void bothFilesInOneRunLogOneLineEachAndASummary() throws Throwable
    {
        // only the 6.8.65 fixture table exists: the hints of the 6.9.26 one are for an unknown table
        createFixtureSchema(DseVersion.DSE_6_8_65);
        Path first = install(DseVersion.DSE_6_8_65);
        Path second = install(DseVersion.DSE_6_9_26);
        long now = Math.max(now(DseVersion.DSE_6_8_65), now(DseVersion.DSE_6_9_26));
        Expected one = expected(DseVersion.DSE_6_8_65, TableId.fromUUID(DseFixtures.fixtureTableId(DseVersion.DSE_6_8_65)));
        Expected two = expected(DseVersion.DSE_6_9_26, null);

        DseHintsConverter.Summary summary = converter(id -> true).convertAll(HintsCatalog::dseHintsFiles, now);

        assertThat(summary.filesFound).isEqualTo(2);
        assertThat(summary.filesConverted).isEqualTo(2);
        assertThat(summary.filesKept).isZero();
        assertThat(summary.hints.converted).isEqualTo(one.converted + two.converted);
        assertThat(summary.hints.unknownTable).isEqualTo(one.unknownTable + two.unknownTable);
        assertThat(summary.hints.unknownHost).isZero();
        assertThat(summary.hints.expired).isZero();
        assertThat(handedOver).hasSize(2);

        List<String> infos = messages(Level.INFO);
        assertThat(infos).hasSize(4);
        assertThat(infos.remove(0)).isEqualTo("DSE 6 compatibility mode: converting 2 hints file(s) written by DataStax Enterprise 6 in " + directory +
                                              " (" + (Files.size(DseFixtures.hintsFile(DseVersion.DSE_6_8_65)) + Files.size(DseFixtures.hintsFile(DseVersion.DSE_6_9_26))) +
                                              " bytes) before the hints dispatch starts");
        // the files are converted in the order of their names, the host id first
        assertThat(infos.get(0)).startsWith("DSE 6 compatibility mode: converted the hints file " + second.getFileName() + " written by DataStax Enterprise 6 for host ")
                                .contains(two.converted + " hint(s) written to ")
                                .contains("(0 for a host not in the ring, " + two.unknownTable + " for an unknown table, 0 for an unknown column, 0 expired); deleted it and its companion files");
        assertThat(infos.get(1)).startsWith("DSE 6 compatibility mode: converted the hints file " + first.getFileName() + " written by DataStax Enterprise 6 for host ")
                                .contains(one.converted + " hint(s) written to ")
                                .contains("(0 for a host not in the ring, " + one.unknownTable + " for an unknown table, 0 for an unknown column, 0 expired); deleted it and its companion files");
        assertThat(infos.get(2)).isEqualTo("DSE 6 compatibility mode: 2 hints file(s) written by DataStax Enterprise 6 found in " + directory +
                                           ", 2 converted and deleted, 0 gone before their conversion, 0 left in place after a failure; " + summary.hints.converted +
                                           " hint(s) converted, " + summary.hints.unknownTable + " dropped (0 for a host not in the ring, " +
                                           summary.hints.unknownTable + " for an unknown table, 0 for an unknown column, 0 expired)");
        assertThat(list()).containsExactlyInAnyOrderElementsOf(expectedNewFiles());
    }

    @Test
    public void noDseFileLogsNothing()
    {
        DseHintsConverter.Summary summary = converter(id -> true).convertAll(HintsCatalog::dseHintsFiles, 0);
        assertThat(summary.filesFound).isZero();
        assertThat(appender.list).isEmpty();
    }

    @Test
    public void aListingFailureIsLoggedAndNotFatal() throws IOException
    {
        // a regular file where the directory should be: listing it fails with an IOException
        File notADirectory = new File(folder.newFile());
        assertListingFailureLogged(notADirectory, HintsCatalog::dseHintsFiles);
    }

    @Test
    public void aFailureWhileIteratingTheListingIsLoggedAndNotFatal()
    {
        // Files.list reports an I/O error met while iterating the directory as an UncheckedIOException
        assertListingFailureLogged(directory, dir -> { throw new UncheckedIOException(new IOException("read error")); });
        assertThat(warnings()).containsExactly("DSE 6 compatibility mode: unable to look for hints files written by DataStax Enterprise 6 in " +
                                               directory + ": java.io.UncheckedIOException: java.io.IOException: read error");
        // the WARN names the failure, the trace goes to DEBUG
        assertTraceAtDebugOnly();
    }

    /** The WARNs carry no throwable; with DEBUG on (the test log configuration), one DEBUG line carries the trace. */
    private void assertTraceAtDebugOnly()
    {
        assertThat(appender.list).filteredOn(event -> event.getLevel() == Level.WARN).allMatch(event -> event.getThrowableProxy() == null);
        if (converterLogger.isDebugEnabled())
            assertThat(appender.list).filteredOn(event -> event.getLevel() == Level.DEBUG)
                                     .singleElement().satisfies(event -> assertThat(event.getThrowableProxy()).isNotNull());
    }

    /** A listing failure of the sweep of orphan companions (here a regular file where the directory should be) is logged. */
    @Test
    public void aListingFailureOfTheSweepIsLoggedAndNotFatal() throws IOException
    {
        File notADirectory = new File(folder.newFile());
        DseHintsConverter converter = new DseHintsConverter(notADirectory, ImmutableMap.of(), id -> true, id -> null, handedOver::add);

        DseHintsConverter.Summary summary = converter.convertAll(dir -> Collections.emptyList(), 0);

        assertThat(summary.filesFound).isZero();
        assertThat(warnings()).singleElement().asString()
                              .startsWith("DSE 6 compatibility mode: unable to look in " + notADirectory + " for companion files of " +
                                          "DataStax Enterprise 6 hints files that are gone: java.nio.file.NotDirectoryException");
        assertThat(messages(Level.INFO)).isEmpty();
        assertTraceAtDebugOnly();
    }

    /** An orphan companion that cannot be deleted (here a directory that is not empty) is logged and kept for the next start. */
    @Test
    public void anOrphanCompanionThatCannotBeDeletedIsLogged() throws Throwable
    {
        Path orphan = Files.createDirectories(directory.toPath().resolve("ed39c7b2-7f97-4a0a-91fc-71398afe1d12-1790335633207-v1.crc32"));
        Files.write(orphan.resolve("x"), new byte[1]);

        converter(id -> true).convertAll(dir -> Collections.emptyList(), 0);

        assertThat(orphan).exists();
        assertThat(warnings()).singleElement().asString()
                              .startsWith("DSE 6 compatibility mode: unable to delete " + orphan + ", a companion file of a DataStax " +
                                          "Enterprise 6 hints file that is gone: java.nio.file.DirectoryNotEmptyException")
                              .endsWith(". It is tried again at the next start; it holds no hint.");
        assertThat(messages(Level.INFO)).isEmpty();
        assertTraceAtDebugOnly();
    }

    /** An empty DSE file whose deletion fails with a runtime exception is kept, as for an I/O error. */
    @Test
    public void anEmptyDseFileWhoseDeletionThrowsAtRuntimeIsKept() throws Throwable
    {
        Path dseFile = install(DseVersion.DSE_6_8_65);
        Files.write(dseFile, new byte[0]);
        List<String> before = list();
        DseHintsConverter converter = new DseHintsConverter(directory, ImmutableMap.of(), id -> true, id -> null, handedOver::add, HintsWriter::create, file -> {
            throw new UncheckedIOException(new IOException("delete refused"));
        });

        DseHintsConverter.Summary summary = converter.convertAll(dir -> Collections.singletonList(dseFile), now(DseVersion.DSE_6_8_65));

        assertThat(summary.filesKept).isEqualTo(1);
        assertThat(list()).containsExactlyInAnyOrderElementsOf(before);
        assertThat(warnings()).singleElement().asString()
                              .isEqualTo("DSE 6 compatibility mode: the hints file " + dseFile.getFileName() + " written by DataStax " +
                                         "Enterprise 6 is empty, with no hint to convert, and could not be deleted: " +
                                         "java.io.UncheckedIOException: java.io.IOException: delete refused. It is tried again at the next start of this node.");
    }

    @Test
    public void noneModeLeavesTheDseFilesAlone() throws Throwable
    {
        Path dseFile = install(DseVersion.DSE_6_8_65);
        Path second = install(DseVersion.DSE_6_9_26);
        // companions of a DSE file that is gone: the sweep of DSE_6 mode would delete them
        String gone = "ed39c7b2-7f97-4a0a-91fc-71398afe1d12-1790335633207-v1";
        Files.write(directory.toPath().resolve(gone + ".crc32"), "0".getBytes());
        Files.write(directory.toPath().resolve(gone + "-Statistics.hints"), new byte[8]);
        List<String> before = list();
        byte[] bytes = Files.readAllBytes(dseFile);

        HintsCatalog catalog = HintsCatalog.load(directory, ImmutableMap.of());
        assertThat(withMode(InternodeCompatibilityMode.NONE, () -> DseHintsConverter.convertIfDse6(catalog, now(DseVersion.DSE_6_8_65)))).isNull();
        // and the entry point of the dispatch start
        withMode(InternodeCompatibilityMode.NONE, () -> {
            DseHintsConverter.convertAtDispatchStart(catalog);
            return null;
        });

        assertThat(list()).containsExactlyInAnyOrderElementsOf(before).contains(second.getFileName().toString());
        assertThat(Files.readAllBytes(dseFile)).isEqualTo(bytes);
        assertThat(catalog.stores().count()).isZero();
        assertThat(appender.list).isEmpty();
    }

    /**
     * The start-up path in {@code DSE_6}, on the clock: a DSE file with hints created now for a peer in the token
     * metadata (the 6.8.65 hints, written for a host id of the test) is converted and the new file lands in the
     * catalog's store of that host, ready for dispatch; the default ring check drops nothing.
     */
    @Test
    public void dse6ModeHandsTheConvertedFileToTheCatalog() throws Throwable
    {
        createFixtureSchema(DseVersion.DSE_6_8_65);
        UUID hostId = UUID.randomUUID();
        long timestamp = currentTimeMillis();
        Path dseFile = DseFixtures.writeDseHintsFile(DseVersion.DSE_6_8_65, directory.toPath(), hostId, timestamp, timestamp);
        InetAddressAndPort peer = InetAddressAndPort.getByName("127.0.0.85");
        Expected expected = expected(DseVersion.DSE_6_8_65, TableId.fromUUID(DseFixtures.fixtureTableId(DseVersion.DSE_6_8_65)));
        HintsCatalog catalog = HintsCatalog.load(directory, ImmutableMap.of());
        StorageService.instance.getTokenMetadata().updateHostId(hostId, peer);
        try
        {
            DseHintsConverter.Summary summary = withMode(InternodeCompatibilityMode.DSE_6, () -> DseHintsConverter.convertIfDse6(catalog));
            assertThat(summary.filesConverted).isEqualTo(1);
            assertThat(summary.hints.converted).isEqualTo(expected.converted);
            assertThat(summary.hints.unknownTable).isEqualTo(expected.unknownTable);
            assertThat(summary.hints.unknownHost).isZero();
            assertThat(summary.hints.expired).isZero();
        }
        finally
        {
            StorageService.instance.getTokenMetadata().removeEndpoint(peer);
        }
        assertThat(dseFile).doesNotExist();
        HintsStore store = catalog.getNullable(hostId);
        assertThat(store).isNotNull();
        assertThat(store.hasFiles()).isTrue();
        HintsDescriptor converted = store.poll();
        assertThat(converted.fileName()).isEqualTo(hostId + "-" + timestamp + '-' + HintsDescriptor.currentStorageVersion() + ".hints");
        assertThat(converted.file(directory).exists()).isTrue();
        assertThat(messages(Level.INFO)).anyMatch(m -> m.contains("for host " + hostId + " (" + peer + "): " + expected.converted + " hint(s) written to " + converted.fileName()));
    }

    /**
     * The call site: in {@code DSE_6} mode the start of the hints dispatch of the service converts the DSE files of the
     * hints directory, and the service's catalog holds the new file for the host.
     */
    @Test
    public void dse6ModeConvertsWhenTheHintsDispatchStarts() throws Throwable
    {
        createFixtureSchema(DseVersion.DSE_6_8_65);
        UUID hostId = UUID.randomUUID();
        long timestamp = currentTimeMillis();
        Path dseFile = DseFixtures.writeDseHintsFile(DseVersion.DSE_6_8_65, DatabaseDescriptor.getHintsDirectory().toPath(),
                                                     hostId, timestamp, timestamp);
        InetAddressAndPort peer = InetAddressAndPort.getByName("127.0.0.87");
        StorageService.instance.getTokenMetadata().updateHostId(hostId, peer);
        // one service at a time (their executors register the same MBeans): the test's own replaces the one of the
        // test server, never started, and a fresh one, not started either, replaces it at the end
        if (!HintsService.instance.isShutDown())
            HintsService.instance.shutdownBlocking();
        // not alive: nothing is dispatched
        HintsService service = new HintsService(id -> false);
        HintsService.instance = service;
        try
        {
            assertThat(service.getTotalHintsSize(hostId)).isZero();

            withMode(InternodeCompatibilityMode.DSE_6, () -> {
                service.startDispatch();
                return null;
            });

            assertThat(dseFile).doesNotExist();
            assertThat(service.getTotalHintsSize(hostId)).isPositive();
            assertThat(messages(Level.INFO)).anyMatch(m -> m.startsWith("DSE 6 compatibility mode: converted the hints file " + dseFile.getFileName()));
        }
        finally
        {
            service.shutdownBlocking();
            service.deleteAllHints();
            HintsService.instance = new HintsService(HintsEndpointProvider.instance::isAlive);
            StorageService.instance.getTokenMetadata().removeEndpoint(peer);
            Files.deleteIfExists(dseFile);
        }
    }

    /**
     * A hint naming a column the table does not have here (a DSE-only column) is dropped and counted, and the rest of
     * the file is converted: here the fixture table is created without its column {@code n}, which every insert of the
     * fixture workload writes and none of its three deletions does. Before, such a hint kept the whole file from
     * converting at every start.
     */
    @Test
    public void aHintOf6865WithAColumnUnknownHereIsDropped() throws Throwable
    {
        assertUnknownColumnDropped(DseVersion.DSE_6_8_65);
    }

    @Test
    public void aHintOf6926WithAColumnUnknownHereIsDropped() throws Throwable
    {
        assertUnknownColumnDropped(DseVersion.DSE_6_9_26);
    }

    private void assertUnknownColumnDropped(DseVersion version) throws Throwable
    {
        for (String statement : DseFixtures.fixtureSchema(version))
        {
            String withoutN = statement.replace(" n bigint,", "");
            if (statement.startsWith("CREATE TABLE"))
                assertThat(withoutN).as("the fixture table without its column n").isNotEqualTo(statement);
            execute(withoutN);
        }
        TableId fixtureTable = TableId.fromUUID(DseFixtures.fixtureTableId(version));
        assertThat(Schema.instance.getTableMetadata(fixtureTable).getColumn(ColumnIdentifier.getInterned("n", false))).isNull();
        Path dseFile = install(version);
        HintsDescriptor dse = readDescriptor(dseFile);
        Expected expected = expected(version, fixtureTable);
        assertThat(expected.fixtureHints % DseFixtures.FIXTURE_MUTATIONS_PER_RUN).isZero();
        int deletions = 3 * expected.fixtureHints / DseFixtures.FIXTURE_MUTATIONS_PER_RUN;

        DseHintsConverter.Counts counts = converter(id -> true).convert(dseFile, now(version));

        assertThat(counts).isNotNull();
        assertThat(counts.unknownColumn).isEqualTo(expected.fixtureHints - deletions);
        assertThat(counts.converted).isEqualTo(expected.converted - counts.unknownColumn);
        assertThat(counts.unknownTable).isEqualTo(expected.unknownTable);
        assertThat(counts.unknownHost + counts.expired).isZero();
        assertThat(dseFile).doesNotExist();
        Path newFile = handedOver.get(0).file(directory).toPath();
        assertThat(DseFixtures.hintsRecords(newFile)).hasSize(counts.converted);
        assertThat(messages(Level.INFO)).singleElement().asString()
                                        .contains(counts.dropped() + " dropped (0 for a host not in the ring, " + counts.unknownTable +
                                                  " for an unknown table, " + counts.unknownColumn + " for an unknown column, 0 expired)");
        assertThat(warnings()).singleElement().asString()
                              .isEqualTo("DSE 6 compatibility mode: dropped " + counts.unknownColumn + " hint(s) of the hints file " +
                                         dseFile.getFileName() + " written by DataStax Enterprise 6 for host " + dse.hostId +
                                         " that name a column this node does not know (column n of " + FIXTURE_KEYSPACE + ".t). " +
                                         "A repair of those tables once internode_compatibility_mode is dropped brings their writes to the host.");

        // the hints kept for the fixture table are its deletions: the one of partition 1 shadows an older row
        String t = FIXTURE_KEYSPACE + ".t";
        execute("INSERT INTO " + t + " (id, ck, v) VALUES (1, 'ck1', 'older') USING TIMESTAMP 1");
        int fixtureHints = 0;
        for (byte[] record : DseFixtures.hintsRecords(newFile))
        {
            try (DataInputBuffer in = new DataInputBuffer(record))
            {
                Hint hint = Hint.serializer.deserialize(in, handedOver.get(0).messagingVersion());
                if (hint.mutation.getTableIds().contains(fixtureTable))
                {
                    fixtureHints++;
                    hint.mutation.apply();
                }
            }
        }
        assertThat(fixtureHints).isEqualTo(deletions);
        assertRows(execute("SELECT id, ck FROM " + t + " WHERE id = 1"));
    }

    /**
     * In {@code DSE_6}, a DSE hint holding the update of a materialized view, which names DSE's hidden column of the view,
     * is converted when the catalog of DSE's hidden columns lists that column for the view: the new file holds the update
     * in this software's encoding, whose rows have the liveness of their hidden cells, so the entry DSE wrote dead (a
     * hidden tombstone) stays dead.
     */
    @Test
    public void aDseHintOfAViewIsConvertedInDse6Mode() throws Throwable
    {
        TableMetadata view = createFixtureView("t_by_v");
        Path dseFile = writeViewHint(view);

        DseHintsConverter.Counts counts = withMode(InternodeCompatibilityMode.DSE_6, () -> withCatalog(
            Collections.singletonMap(Pair.create(FIXTURE_KEYSPACE, view.name), Collections.singletonList(DSE_V)),
            () -> converter(id -> true).convert(dseFile, viewHintCreationTime + 1000)));

        assertThat(counts.converted).isEqualTo(1);
        assertThat(counts.dropped()).isZero();
        assertThat(dseFile).doesNotExist();
        assertThat(warnings()).isEmpty();
        HintsDescriptor converted = handedOver.get(0);
        List<byte[]> written = DseFixtures.hintsRecords(converted.file(directory).toPath());
        assertThat(written).hasSize(1);
        Hint hint;
        try (DataInputBuffer in = new DataInputBuffer(written.get(0)))
        {
            hint = Hint.serializer.deserialize(in, converted.messagingVersion());
        }
        assertThat(hint.creationTime).isEqualTo(viewHintCreationTime);
        assertThat(hint.mutation.getPartitionUpdate(view).columns().regulars).isEmpty();
        hint.mutation.apply();
        assertRows(execute("SELECT v, id, ck FROM " + view + " WHERE v = 'x'"), row("x", 1, "a"));
    }

    /**
     * A DSE hint holding the update of a materialized view is dropped and counted among the hints for an unknown column,
     * as before, when the translation of the hidden column is not certain (in {@code DSE_6}, the catalog of DSE's hidden
     * columns lists no hidden column for the view, or cannot be read; a WARN says why) or not made (without the mode, where
     * the catalog is not read).
     */
    @Test
    public void aDseHintOfAViewIsDroppedWhenItsTranslationIsUncertain() throws Throwable
    {
        TableMetadata unlistedView = createFixtureView("unlisted_by_v");
        TableMetadata unreadableView = createFixtureView("unreadable_by_v");
        TableMetadata view = createFixtureView("t_by_v");

        Path unlisted = writeViewHint(unlistedView);
        assertViewHintDroppedWithWarning(unlisted, unlistedView, Collections.emptyMap(),
                                         "DSE's system_schema.hidden_columns on this node lists no hidden column for that view.");

        appender.list.clear();
        Path unreadable = writeViewHint(unreadableView);
        assertViewHintDroppedWithWarning(unreadable, unreadableView, null,
                                         "DSE 6 compatibility mode: cannot read DSE's system_schema.hidden_columns from the data directories of this node");

        appender.list.clear();
        Path withoutTheMode = writeViewHint(view);
        DseHintsConverter.Counts counts = withMode(InternodeCompatibilityMode.NONE, () -> withCatalog(null, () -> {
            DseHintsConverter.Counts noneCounts = converter(id -> true).convert(withoutTheMode, viewHintCreationTime + 1000);
            assertThat(DseViewHiddenColumnCatalog.setInstance(null)).as("the catalog was not read").isNull();
            return noneCounts;
        }));
        assertViewHintDropped(counts, withoutTheMode, view);
        for (TableMetadata dropped : new TableMetadata[]{ unlistedView, unreadableView, view })
            assertRows(execute("SELECT * FROM " + dropped));
    }

    /**
     * In {@code DSE_6}, with the catalog of DSE's hidden columns listing {@code hiddenColumns} (none read yet when null),
     * the hint of {@code dseFile} for {@code view} is dropped, with a WARN of the translation containing {@code reason}.
     */
    private void assertViewHintDroppedWithWarning(Path dseFile, TableMetadata view, Map<Pair<String, String>, List<HiddenColumn>> hiddenColumns,
                                                  String reason) throws Throwable
    {
        withLogAppender(DseViewWireFormat.class, translation -> {
            DseHintsConverter.Counts counts = withMode(InternodeCompatibilityMode.DSE_6, () -> withCatalog(
                hiddenColumns, () -> converter(id -> true).convert(dseFile, viewHintCreationTime + 1000)));
            assertViewHintDropped(counts, dseFile, view);
            assertThat(translation.list).singleElement().satisfies(event -> {
                assertThat(event.getLevel()).isEqualTo(Level.WARN);
                assertThat(event.getFormattedMessage()).contains("names the column .v of materialized view " + view + ", which is not translated: " + reason);
            });
        });
    }

    private void assertViewHintDropped(DseHintsConverter.Counts counts, Path dseFile, TableMetadata view) throws IOException
    {
        assertThat(counts.unknownColumn).isEqualTo(1);
        assertThat(counts.converted).isZero();
        assertThat(dseFile).doesNotExist();
        assertThat(warnings()).singleElement().asString()
                              .contains("that name a column this node does not know (column .v of " + view + ")");
    }

    /**
     * The view {@code name} of the 6.8.65 fixture table, keyed by {@code (v, id, ck)}, with the fixture schema when it is
     * not there yet. The keyspace system_distributed, where the build of a view is recorded, is created here: the tests
     * run without the network, which creates it otherwise.
     */
    private TableMetadata createFixtureView(String name) throws Throwable
    {
        Schema.instance.transform(SchemaTransformations.updateSystemKeyspace(SystemDistributedKeyspace.metadata(), SystemDistributedKeyspace.GENERATION));
        if (Schema.instance.getKeyspaceMetadata(FIXTURE_KEYSPACE) == null)
            createFixtureSchema(DseVersion.DSE_6_8_65);
        execute("CREATE MATERIALIZED VIEW " + FIXTURE_KEYSPACE + '.' + name + " AS SELECT id, ck, v FROM " + FIXTURE_KEYSPACE + ".t " +
                "WHERE v IS NOT NULL AND id IS NOT NULL AND ck IS NOT NULL PRIMARY KEY (v, id, ck)");
        return Schema.instance.getTableMetadata(FIXTURE_KEYSPACE, name);
    }

    /**
     * Writes a hints file as DSE writes one, for a host of its own, holding one hint: the update
     * {@link DseFixtures#dseViewUpdate} of {@code view}.
     */
    private Path writeViewHint(TableMetadata view) throws IOException
    {
        byte[] hint = DseFixtures.dseHint(viewHintCreationTime, view.params.gcGraceSeconds, DseFixtures.dse68Mutation(DseFixtures.dseViewUpdate(view, "x")));
        return DseFixtures.writeDseHintsFile(directory.toPath(), UUID.randomUUID(), viewHintCreationTime, Collections.singletonList(hint));
    }

    /**
     * Runs {@code body} with the catalog of DSE's hidden columns listing {@code hiddenColumns}, or with none read yet when
     * {@code hiddenColumns} is null, then restores the catalog.
     */
    private static <T> T withCatalog(Map<Pair<String, String>, List<HiddenColumn>> hiddenColumns, ThrowingSupplier<T> body) throws Throwable
    {
        DseViewHiddenColumnCatalog before = DseViewHiddenColumnCatalog.setInstance(hiddenColumns == null ? null : new DseViewHiddenColumnCatalog(hiddenColumns));
        try
        {
            return body.get();
        }
        finally
        {
            DseViewHiddenColumnCatalog.setInstance(before);
        }
    }

    /** A DSE file for this node's own host id holds no hint any peer needs: the default ring check drops them all. */
    @Test
    public void dse6ModeDropsTheHintsForThisNode() throws Throwable
    {
        createFixtureSchema(DseVersion.DSE_6_8_65);
        UUID local = SystemKeyspace.getLocalHostId();
        assertThat(local).isNotNull();
        long timestamp = currentTimeMillis();
        Path dseFile = DseFixtures.writeDseHintsFile(DseVersion.DSE_6_8_65, directory.toPath(), local, timestamp, timestamp);
        int records = DseFixtures.hintsRecords(DseVersion.DSE_6_8_65).size();
        HintsCatalog catalog = HintsCatalog.load(directory, ImmutableMap.of());

        DseHintsConverter.Summary summary = withMode(InternodeCompatibilityMode.DSE_6, () -> DseHintsConverter.convertIfDse6(catalog));

        assertThat(summary.filesConverted).isEqualTo(1);
        assertThat(summary.hints.unknownHost).isEqualTo(records);
        assertThat(summary.hints.converted).isZero();
        assertThat(dseFile).doesNotExist();
        assertThat(list()).isEmpty();
        assertThat(catalog.stores().count()).isZero();
    }

    /** Converts the fixture of a version, checks the counts, the new file and its hints, and that the DSE files are gone. */
    private void assertConvertedAndDeleted(DseVersion version) throws Throwable
    {
        createFixtureSchema(version);
        TableId fixtureTable = TableId.fromUUID(DseFixtures.fixtureTableId(version));
        Path dseFile = install(version);
        HintsDescriptor dse = readDescriptor(dseFile);
        Expected expected = expected(version, fixtureTable);
        assertThat(expected.converted).isPositive();

        DseHintsConverter.Counts counts = converter(id -> true).convert(dseFile, now(version));

        assertThat(counts).isNotNull();
        assertThat(counts.converted).isEqualTo(expected.converted);
        assertThat(counts.unknownTable).isEqualTo(expected.unknownTable);
        assertThat(counts.unknownHost).isZero();
        assertThat(counts.expired).isZero();
        assertThat(list()).containsExactlyInAnyOrderElementsOf(expectedNewFiles());
        assertThat(handedOver).singleElement().satisfies(d -> {
            assertThat(d.hostId).isEqualTo(dse.hostId);
            assertThat(d.timestamp).isEqualTo(dse.timestamp);
            assertThat(d.version).isEqualTo(HintsDescriptor.currentStorageVersion());
        });

        // the new file holds the converted hints, with their creation time and gc grace seconds, in this software's layout
        HintsDescriptor converted = handedOver.get(0);
        Path newFile = converted.file(directory).toPath();
        assertThat(readDescriptor(newFile).version).isEqualTo(converted.version);
        List<byte[]> written = DseFixtures.hintsRecords(newFile);
        assertThat(written).hasSize(expected.converted);
        List<Hint> fixtureHints = new ArrayList<>();
        List<String> writtenHeaders = new ArrayList<>();
        for (byte[] record : written)
        {
            try (DataInputBuffer in = new DataInputBuffer(record))
            {
                Hint hint = Hint.serializer.deserialize(in, converted.messagingVersion());
                assertThat(in.available()).isZero();
                writtenHeaders.add(hint.creationTime + "/" + hint.gcgs + '/' + hint.mutation.getTableIds());
                if (hint.mutation.getTableIds().contains(fixtureTable))
                    fixtureHints.add(hint);
            }
        }
        assertThat(writtenHeaders).containsExactlyElementsOf(expected.headers);
        assertThat(fixtureHints.size() % DseFixtures.FIXTURE_MUTATIONS_PER_RUN).isZero();

        for (Hint hint : fixtureHints)
            hint.mutation.apply();
        String t = FIXTURE_KEYSPACE + ".t";
        assertRows(execute("SELECT count(*) FROM " + t), row((long) DseFixtures.COLLECTION_TABLE_COUNT_STAR));
        assertRows(execute("SELECT id, ck, s FROM " + t + " WHERE id = 3"), row(3, null, 30));
        assertRows(execute("SELECT id, ck, v, n, s FROM " + t + " WHERE id = 100"),
                   row(100, "ck1", "val199", 19900L, 1000),
                   row(100, "ck2", "val200", 20000L, 1000));

        assertThat(messages(Level.INFO)).singleElement().asString()
                                        .isEqualTo("DSE 6 compatibility mode: converted the hints file " + dseFile.getFileName() +
                                                   " written by DataStax Enterprise 6 for host " + dse.hostId + " (address unknown): " +
                                                   expected.converted + " hint(s) written to " + converted.fileName() + ", " +
                                                   expected.unknownTable + " dropped (0 for a host not in the ring, " +
                                                   expected.unknownTable + " for an unknown table, 0 for an unknown column, 0 expired); deleted it and its companion files");
        assertThat(warnings()).isEmpty();
    }

    private void assertDroppedForAHostNotInTheRing(DseVersion version) throws Throwable
    {
        createFixtureSchema(version);
        Path dseFile = install(version);
        int records = DseFixtures.hintsRecords(version).size();

        DseHintsConverter.Counts counts = converter(id -> false).convert(dseFile, now(version));

        assertThat(counts).isNotNull();
        assertThat(counts.unknownHost).isEqualTo(records);
        assertThat(counts.converted + counts.unknownTable + counts.expired).isZero();
        assertThat(list()).isEmpty();
        assertThat(handedOver).isEmpty();
        assertThat(messages(Level.INFO)).singleElement().asString()
                                        .contains("0 hint(s) written to no file, " + records + " dropped (" + records + " for a host not in the ring, 0 for an unknown table, 0 for an unknown column, 0 expired)");
        HintsDescriptor dse = readDescriptor(DseFixtures.hintsFile(version));
        assertThat(warnings()).singleElement().asString()
                              .isEqualTo("DSE 6 compatibility mode: dropped the " + records + " hint(s) of the hints file " + dseFile.getFileName() +
                                         " written by DataStax Enterprise 6 for host " + dse.hostId + ", which is not a peer of this node in the " +
                                         "ring (neither in its token metadata nor in system.peers). If that host is part of the cluster, a " +
                                         "repair of the affected tables once internode_compatibility_mode is dropped brings those writes to it.");
    }

    /**
     * With the gc grace seconds of the fixture table lowered to a minute and the clock two minutes past the last hint,
     * every hint of the fixture table is expired; those of {@code system_auth.role_permissions} (90 days) are not.
     */
    private void assertExpiredDropped(DseVersion version) throws Throwable
    {
        createFixtureSchema(version);
        execute("ALTER TABLE " + FIXTURE_KEYSPACE + ".t WITH gc_grace_seconds = 60");
        TableId fixtureTable = TableId.fromUUID(DseFixtures.fixtureTableId(version));
        Path dseFile = install(version);
        Expected expected = expected(version, fixtureTable);

        DseHintsConverter.Counts counts = converter(id -> true).convert(dseFile, lastCreationTime(version) + 120_000);

        assertThat(counts).isNotNull();
        assertThat(counts.expired).isEqualTo(expected.fixtureHints).isPositive();
        assertThat(counts.converted).isEqualTo(expected.converted - expected.fixtureHints).isPositive();
        assertThat(counts.unknownTable).isEqualTo(expected.unknownTable);
        assertThat(counts.unknownHost).isZero();
        assertThat(DseFixtures.hintsRecords(handedOver.get(0).file(directory).toPath())).hasSize(counts.converted);
        assertThat(dseFile).doesNotExist();
    }

    /** Flips a byte of the last hint: the hints before it are written, then the new file is deleted and the DSE files kept. */
    private void assertCorruptLastHintKeepsTheFiles(DseVersion version) throws Throwable
    {
        createFixtureSchema(version);
        Path dseFile = install(version);
        byte[] bytes = Files.readAllBytes(dseFile);
        bytes[bytes.length - 4 - 10] ^= 0x5a; // inside the last hint, before its CRC32
        Files.write(dseFile, bytes);
        List<String> before = list();

        assertThat(converter(id -> true).convert(dseFile, now(version))).isNull();

        assertThat(list()).containsExactlyInAnyOrderElementsOf(before);
        assertThat(Files.readAllBytes(dseFile)).isEqualTo(bytes);
        assertThat(handedOver).isEmpty();
        assertThat(messages(Level.INFO)).isEmpty();
        assertThat(warnings()).singleElement().asString()
                              .contains("unable to convert the hints file " + dseFile.getFileName() + " written by DataStax Enterprise 6")
                              .contains("fails the checksum of its hint");
    }

    private void assertListingFailureLogged(File dir, DseHintsConverter.DseHintsFileLister lister)
    {
        DseHintsConverter converter = new DseHintsConverter(dir, ImmutableMap.of(), id -> true, id -> null, handedOver::add);
        DseHintsConverter.Summary summary = converter.convertAll(lister, 0);
        assertThat(summary.filesFound).isZero();
        assertThat(warnings()).singleElement().asString()
                              .startsWith("DSE 6 compatibility mode: unable to look for hints files written by DataStax Enterprise 6 in " + dir + ": ");
        assertThat(messages(Level.INFO)).isEmpty();
    }

    /** What the conversion of a fixture should give, read ahead of it from the records. */
    private static final class Expected
    {
        int converted;
        int unknownTable;
        int fixtureHints;
        final List<String> headers = new ArrayList<>();
    }

    /**
     * Classifies the hints of a fixture by table: the fixture table (when created here) and {@code system_auth.role_permissions}
     * are known to this test server, DSE's {@code nodesync_status} is not.
     */
    private static Expected expected(DseVersion version, TableId fixtureTable) throws IOException
    {
        assertThat(Schema.instance.getTableMetadata(ROLE_PERMISSIONS)).as("system_auth.role_permissions").isNotNull();
        assertThat(Schema.instance.getTableMetadata(NODESYNC_STATUS)).as("DSE's nodesync_status").isNull();
        Expected expected = new Expected();
        for (byte[] record : DseFixtures.hintsRecords(version))
        {
            TableId table = TableId.fromUUID(DseFixtures.hintTableId(record));
            boolean known = table.equals(ROLE_PERMISSIONS) || table.equals(fixtureTable);
            if (!known)
            {
                expected.unknownTable++;
                continue;
            }
            expected.converted++;
            if (table.equals(fixtureTable))
                expected.fixtureHints++;
            ByteBuffer raw = ByteBuffer.wrap(record);
            expected.headers.add(raw.getLong(0) + "/" + VIntCoding.getUnsignedVInt(raw, 8) + "/[" + table + ']');
        }
        return expected;
    }

    private DseHintsConverter converter(Predicate<UUID> isPeerInRing)
    {
        return new DseHintsConverter(directory, ImmutableMap.of(), isPeerInRing, id -> null, handedOver::add);
    }

    private void createFixtureSchema(DseVersion version) throws Throwable
    {
        for (String statement : DseFixtures.fixtureSchema(version))
            execute(statement);
    }

    /** Copies the hints file of a version into the test directory, with the two companion files DSE writes next to it. */
    private Path install(DseVersion version) throws IOException
    {
        Path source = DseFixtures.hintsFile(version);
        Path target = Files.copy(source, directory.toPath().resolve(source.getFileName().toString()));
        Files.write(companion(target, ".crc32"), "0".getBytes());
        Files.write(companion(target, "-Statistics.hints"), new byte[8]);
        return target;
    }

    private static Path companion(Path dseFile, String suffix)
    {
        String name = dseFile.getFileName().toString();
        return dseFile.resolveSibling(name.substring(0, name.length() - ".hints".length()) + suffix);
    }

    /** The files a successful conversion leaves: the new hints file of each handed-over descriptor and its companions. */
    private List<String> expectedNewFiles()
    {
        List<String> names = new ArrayList<>();
        for (HintsDescriptor descriptor : handedOver)
        {
            names.add(descriptor.fileName());
            names.add(descriptor.checksumFileName());
            names.add(descriptor.statisticsFileName());
        }
        return names;
    }

    private List<String> list() throws IOException
    {
        try (Stream<Path> files = Files.list(directory.toPath()))
        {
            return files.map(p -> p.getFileName().toString()).collect(Collectors.toList());
        }
    }

    private static HintsDescriptor readDescriptor(Path file) throws IOException
    {
        try (FileInputStreamPlus in = new FileInputStreamPlus(file))
        {
            return HintsDescriptor.deserialize(in);
        }
    }

    /** One second after the last hint of the fixture was created: every hint is live. */
    private static long now(DseVersion version) throws IOException
    {
        return lastCreationTime(version) + 1000;
    }

    private static long lastCreationTime(DseVersion version) throws IOException
    {
        long last = Long.MIN_VALUE;
        for (byte[] record : DseFixtures.hintsRecords(version))
            last = Math.max(last, ByteBuffer.wrap(record).getLong(0));
        return last;
    }

    private List<String> messages(Level level)
    {
        return appender.list.stream()
                            .filter(event -> event.getLevel() == level)
                            .map(ILoggingEvent::getFormattedMessage)
                            .collect(Collectors.toList());
    }

    private List<String> warnings()
    {
        return messages(Level.WARN);
    }

    private interface ThrowingSupplier<T>
    {
        T get() throws Throwable;
    }

    private static <T> T withMode(InternodeCompatibilityMode mode, ThrowingSupplier<T> body) throws Throwable
    {
        InternodeCompatibilityMode previous = DatabaseDescriptor.getInternodeCompatibilityMode();
        try
        {
            DatabaseDescriptor.setInternodeCompatibilityMode(mode);
            return body.get();
        }
        finally
        {
            DatabaseDescriptor.setInternodeCompatibilityMode(previous);
        }
    }
}
