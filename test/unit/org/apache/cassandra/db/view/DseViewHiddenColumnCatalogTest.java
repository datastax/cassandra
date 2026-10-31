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

package org.apache.cassandra.db.view;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.InternodeCompatibilityMode;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.view.DseViewHiddenColumnCatalog.HiddenColumn;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.dsecompat.DseFixtures;
import org.apache.cassandra.exceptions.StartupException;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.net.DseCompatRefusals;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.DefaultFSErrorHandler;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.JVMKiller;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.KillerForTests;
import org.apache.cassandra.utils.Pair;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The catalog of the hidden columns DSE lists for the materialized views of this node in its
 * {@code system_schema.hidden_columns} ({@link DseViewHiddenColumnCatalog}): how it is read from the data directories,
 * what happens when it cannot be, and the views a node in DSE 6 compatibility mode refuses to start with.
 */
public class DseViewHiddenColumnCatalogTest extends CQLTester
{
    /** These tests create tables in DSE_6 mode, whose schema freeze is not what they test. */
    private static WithProperties allowDdl;

    @BeforeClass
    public static void allowDdlInDse6Mode()
    {
        allowDdl = new WithProperties().set(CassandraRelevantProperties.INTERNODE_COMPATIBILITY_MODE_ALLOW_DDL, true);
    }

    @AfterClass
    public static void restoreDdlFreeze()
    {
        allowDdl.close();
    }

    private static final String VIEW_QUERY = "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                                             "WHERE v IS NOT NULL AND pk IS NOT NULL AND ck IS NOT NULL PRIMARY KEY (v, pk, ck)";
    private static final String REMEDY = "Drop the view on DSE before the upgrade and create it again once no node runs DSE; " +
                                         "a node already running this software must first be rolled back to DSE";
    /** The directory of DSE's table in a data directory. */
    private static final String TABLE_DIRECTORY = "system_schema/hidden_columns-2461e8b28f6a31a1ac7230b9cd9c6ca5";
    /** DSE's hidden column of a regular base column in the view primary key, as system_schema.hidden_columns lists it. */
    private static final HiddenColumn DSE_V = new HiddenColumn(".v", "regular", true, "text");

    private InternodeCompatibilityMode modeBefore;
    private DseViewHiddenColumnCatalog catalogBefore;
    private final Map<Pair<String, String>, List<HiddenColumn>> dseHiddenColumns = new HashMap<>();
    private TableMetadata view;

    @BeforeClass
    public static void startNetwork()
    {
        // view builds need the node to have joined its ring
        requireNetwork();
    }

    @Before
    public void dse6ModeAndView()
    {
        modeBefore = DatabaseDescriptor.getInternodeCompatibilityMode();
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        catalogBefore = DseViewHiddenColumnCatalog.setInstance(new DseViewHiddenColumnCatalog(dseHiddenColumns));
        createTable("CREATE TABLE %s (pk int, ck int, v text, w int, PRIMARY KEY (pk, ck))");
        String viewName = createView(VIEW_QUERY);
        dseHiddenColumns.put(Pair.create(KEYSPACE, viewName), List.of(DSE_V));
        ColumnFamilyStore viewStore = getColumnFamilyStore(KEYSPACE, viewName);
        viewStore.disableAutoCompaction();
        view = viewStore.metadata();
    }

    @After
    public void restoreModeAndCatalog()
    {
        DseViewHiddenColumnCatalog.setInstance(catalogBefore);
        DatabaseDescriptor.setInternodeCompatibilityMode(modeBefore);
    }

    /** The sstable of system_schema.hidden_columns DSE 6.8.65 wrote, from the test fixtures: the row of the view lab.wide_by_v. */
    @Test
    public void theCatalogIsReadFromTheSSTableDseWrote()
    {
        DseViewHiddenColumnCatalog catalog = DseViewHiddenColumnCatalog.load(List.of(new File(DseFixtures.schemaDataDirectory())));
        assertThat(catalog.hiddenColumns("lab", "wide_by_v")).extracting(HiddenColumn::toString)
                                                            .containsExactly(".v text (regular, required for liveness)");
        assertThat(catalog.hiddenColumns("lab", "wide")).isEmpty();
    }

    /**
     * The catalog is read from the sstables of DSE's system_schema.hidden_columns in a data directory; here sstables of a
     * table with the columns DSE gives it, written by this software, with rows of the three kinds of hidden columns, a
     * deleted row and the failures of the read (the sstable DSE wrote is read by {@link #theCatalogIsReadFromTheSSTableDseWrote}).
     */
    @Test
    public void theCatalogIsReadFromTheHiddenColumnsSSTables() throws Throwable
    {
        Path dataDirectory = Files.createTempDirectory("dse-hidden-columns");
        try
        {
            String table = createTable("CREATE TABLE %s (keyspace_name text, table_name text, column_name text, clustering_order text, " +
                                       "column_name_bytes blob, kind text, position int, required_for_liveness boolean, type text, " +
                                       "PRIMARY KEY ((keyspace_name), table_name, column_name))");
            execute("INSERT INTO %s (keyspace_name, table_name, column_name, clustering_order, column_name_bytes, kind, position, required_for_liveness, type) " +
                    "VALUES ('lab', 'wide_by_v', '.v', 'none', 0x2e76, 'regular', -1, true, 'text')");
            execute("INSERT INTO %s (keyspace_name, table_name, column_name, clustering_order, column_name_bytes, kind, position, required_for_liveness, type) " +
                    "VALUES ('lab', 'filtered', '.x', 'none', 0x2e78, 'regular', -1, true, 'int')");
            execute("INSERT INTO %s (keyspace_name, table_name, column_name, clustering_order, column_name_bytes, kind, position, required_for_liveness, type) " +
                    "VALUES ('other', 'unselected', '.u', 'none', 0x2e75, 'regular', -1, false, 'int')");
            flush();
            // a view dropped on DSE: its row is deleted, in a second sstable
            execute("DELETE FROM %s WHERE keyspace_name = 'lab' AND table_name = 'filtered'");
            flush();
            ColumnFamilyStore store = getColumnFamilyStore(KEYSPACE, table);
            assertThat(store.getLiveSSTables()).hasSize(2);

            Path hiddenColumnsDirectory = Files.createDirectories(dataDirectory.resolve("system_schema")
                                                                               .resolve(DseViewHiddenColumnCatalog.HIDDEN_COLUMNS + '-' + DseViewHiddenColumnCatalog.HIDDEN_COLUMNS_ID.toHexString()));
            for (SSTableReader sstable : store.getLiveSSTables())
                for (Component component : sstable.components())
                    Files.copy(sstable.descriptor.fileFor(component).toPath(), hiddenColumnsDirectory.resolve(sstable.descriptor.fileFor(component).name()));

            DseViewHiddenColumnCatalog catalog = DseViewHiddenColumnCatalog.load(List.of(new File(dataDirectory)));
            List<HiddenColumn> wideByV = catalog.hiddenColumns("lab", "wide_by_v");
            assertThat(wideByV).hasSize(1);
            assertThat(wideByV.get(0).name).isEqualTo(".v");
            assertThat(wideByV.get(0).kind).isEqualTo("regular");
            assertThat(wideByV.get(0).requiredForLiveness).isTrue();
            assertThat(wideByV.get(0).type).isEqualTo("text");
            assertThat(catalog.hiddenColumns("lab", "filtered")).isEmpty();
            List<HiddenColumn> unselected = catalog.hiddenColumns("other", "unselected");
            assertThat(unselected).hasSize(1);
            assertThat(unselected.get(0).name).isEqualTo(".u");
            assertThat(unselected.get(0).requiredForLiveness).isFalse();

            // the directory of DSE's table without sstables (a DSE node that never had a view of the new format): nothing
            Path noSSTables = Files.createDirectories(dataDirectory.resolve("no-sstables").resolve("system_schema").resolve(DseViewHiddenColumnCatalog.HIDDEN_COLUMNS + '-' + DseViewHiddenColumnCatalog.HIDDEN_COLUMNS_ID.toHexString()));
            assertThat(DseViewHiddenColumnCatalog.load(List.of(new File(noSSTables.getParent().getParent()))).hiddenColumns("lab", "wide_by_v")).isEmpty();

            // data directories without the directory of DSE's table: not the ones of a node that ran DSE
            File empty = new File(Files.createDirectories(dataDirectory.resolve("empty")));
            assertThatThrownBy(() -> DseViewHiddenColumnCatalog.load(List.of(empty)))
            .isInstanceOf(IllegalStateException.class)
            .hasMessage("no data directory holds " + TABLE_DIRECTORY + ": [" + empty + "] are not the data directories of a node that ran DSE 6");

            // an sstable whose metadata cannot be read
            Path corrupt = Files.createDirectories(dataDirectory.resolve("corrupt").resolve("system_schema").resolve(hiddenColumnsDirectory.getFileName()));
            for (SSTableReader sstable : store.getLiveSSTables())
            {
                for (Component component : sstable.components())
                {
                    Path copy = corrupt.resolve(sstable.descriptor.fileFor(component).name());
                    Files.copy(sstable.descriptor.fileFor(component).toPath(), copy);
                    if (component.type == SSTableFormat.Components.Types.STATS)
                        Files.write(copy, new byte[]{ 1, 2, 3 });
                }
            }
            assertThatThrownBy(() -> DseViewHiddenColumnCatalog.load(List.of(new File(corrupt.getParent().getParent())))).isInstanceOf(CorruptSSTableException.class);

            // a directory that is not under DSE's table id is not DSE's table
            Path notATable = Files.createDirectories(dataDirectory.resolve("not-a-table").resolve("system_schema").resolve(DseViewHiddenColumnCatalog.HIDDEN_COLUMNS + "-backup"));
            File notATableRoot = new File(notATable.getParent().getParent());
            assertThatThrownBy(() -> DseViewHiddenColumnCatalog.load(List.of(notATableRoot)))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageStartingWith("no data directory holds " + TABLE_DIRECTORY + ": ");

            // a transaction log that cannot be read: which sstables are committed is unknown
            Path badLog = Files.createDirectories(dataDirectory.resolve("bad-log").resolve("system_schema").resolve(hiddenColumnsDirectory.getFileName()));
            for (SSTableReader sstable : store.getLiveSSTables())
                for (Component component : sstable.components())
                    Files.copy(sstable.descriptor.fileFor(component).toPath(), badLog.resolve(sstable.descriptor.fileFor(component).name()));
            Files.write(badLog.resolve("bb_txn_compaction_" + UUID.randomUUID() + ".log"), "not a record".getBytes(StandardCharsets.UTF_8));
            assertThatThrownBy(() -> DseViewHiddenColumnCatalog.load(List.of(new File(badLog.getParent().getParent()))))
            .isInstanceOf(RuntimeException.class)
            .hasMessage("Failed to list files in " + badLog);
        }
        finally
        {
            FileUtils.deleteRecursive(new File(dataDirectory));
        }
    }

    /** A system_schema directory that cannot be listed fails the read, rather than pass for one without DSE's table. */
    @Test
    public void aSchemaDirectoryThatCannotBeListedFailsTheRead() throws Throwable
    {
        Path dataDirectory = Files.createTempDirectory("dse-hidden-columns-unreadable");
        Path unreadable = Files.createDirectories(dataDirectory.resolve("system_schema"));
        Files.setPosixFilePermissions(unreadable, PosixFilePermissions.fromString("-wx------"));
        try
        {
            // a user that reads it anyway (root) cannot run this test
            Assume.assumeFalse(Files.isReadable(unreadable));
            assertThatThrownBy(() -> DseViewHiddenColumnCatalog.load(List.of(new File(dataDirectory))))
            .isInstanceOf(UncheckedIOException.class);
        }
        finally
        {
            Files.setPosixFilePermissions(unreadable, PosixFilePermissions.fromString("rwx------"));
            FileUtils.deleteRecursive(new File(dataDirectory));
        }
    }

    /**
     * A directory of the table under another table id than DSE's (a stale copy, for instance) is not DSE's table: it is
     * skipped with a WARN, and its rows are not read.
     */
    @Test
    public void theCatalogSkipsOtherTableIds() throws Throwable
    {
        Path dataDirectory = Files.createTempDirectory("dse-hidden-columns-two-ids");
        try
        {
            String table = createTable("CREATE TABLE %s (keyspace_name text, table_name text, column_name text, clustering_order text, " +
                                       "column_name_bytes blob, kind text, position int, required_for_liveness boolean, type text, " +
                                       "PRIMARY KEY ((keyspace_name), table_name, column_name))");
            ColumnFamilyStore store = getColumnFamilyStore(KEYSPACE, table);
            execute("INSERT INTO %s (keyspace_name, table_name, column_name, kind, required_for_liveness, type) VALUES ('ks', 'first', '.v', 'regular', true, 'text')");
            flush();
            copySSTables(store, dataDirectory.resolve("system_schema").resolve(DseViewHiddenColumnCatalog.HIDDEN_COLUMNS + '-' + DseViewHiddenColumnCatalog.HIDDEN_COLUMNS_ID.toHexString()));
            store.truncateBlocking();
            execute("INSERT INTO %s (keyspace_name, table_name, column_name, kind, required_for_liveness, type) VALUES ('ks', 'second', '.w', 'regular', true, 'int')");
            flush();
            copySSTables(store, dataDirectory.resolve("system_schema").resolve(DseViewHiddenColumnCatalog.HIDDEN_COLUMNS + "-fedcba9876543210fedcba9876543210"));

            Logger catalogLogger = (Logger) LoggerFactory.getLogger(DseViewHiddenColumnCatalog.class);
            ListAppender<ILoggingEvent> appender = new ListAppender<>();
            appender.start();
            catalogLogger.addAppender(appender);
            DseViewHiddenColumnCatalog catalog;
            try
            {
                catalog = DseViewHiddenColumnCatalog.load(List.of(new File(dataDirectory)));
            }
            finally
            {
                catalogLogger.detachAppender(appender);
            }
            File skipped = new File(dataDirectory.resolve("system_schema").resolve(DseViewHiddenColumnCatalog.HIDDEN_COLUMNS + "-fedcba9876543210fedcba9876543210"));
            assertThat(appender.list).anyMatch(e -> e.getLevel() == Level.WARN &&
                                                    e.getFormattedMessage().equals("DSE 6 compatibility mode: " + skipped + " is not the directory of DSE's " +
                                                                                   "system_schema.hidden_columns, whose table id is " +
                                                                                   "2461e8b28f6a31a1ac7230b9cd9c6ca5, skipped"));
            assertThat(catalog.hiddenColumns("ks", "first")).extracting(HiddenColumn::toString).containsExactly(".v text (regular, required for liveness)");
            assertThat(catalog.hiddenColumns("ks", "second")).isEmpty();

            // only a copy under another id: not the data directories of a node that ran DSE 6
            File copyOnly = new File(Files.createDirectories(dataDirectory.resolve("copy-only").resolve("system_schema")).getParent());
            copySSTables(store, copyOnly.toPath().resolve("system_schema").resolve(DseViewHiddenColumnCatalog.HIDDEN_COLUMNS + "-fedcba9876543210fedcba9876543210"));
            assertThatThrownBy(() -> DseViewHiddenColumnCatalog.load(List.of(copyOnly)))
            .isInstanceOf(IllegalStateException.class)
            .hasMessage("no data directory holds " + TABLE_DIRECTORY + ": [" + copyOnly + "] are not the data directories of a node that ran DSE 6");
        }
        finally
        {
            FileUtils.deleteRecursive(new File(dataDirectory));
        }
    }

    /** Copies the files of the live sstables of {@code store} to {@code directory}. */
    private static void copySSTables(ColumnFamilyStore store, Path directory) throws IOException
    {
        Files.createDirectories(directory);
        for (SSTableReader sstable : store.getLiveSSTables())
            for (Component component : sstable.components())
                Files.copy(sstable.descriptor.fileFor(component).toPath(), directory.resolve(sstable.descriptor.fileFor(component).name()));
    }

    /**
     * The catalog of the node is read from its data directories when it is first needed; when they do not hold DSE's
     * table (as those of this test), the node in DSE 6 compatibility mode does not start while it has views; the failure
     * is kept, and counted in compatstatus.
     */
    @Test
    public void aCatalogThatCannotBeReadRefusesTheViews() throws Throwable
    {
        DseViewHiddenColumnCatalog.setInstance(null);
        long refusedBefore = DseCompatRefusals.count(DseViewHiddenColumnCatalog.REFUSED_VIEW);
        String failure = "DSE 6 compatibility mode: cannot read DSE's system_schema.hidden_columns from the data directories of this node " +
                         "(no data directory holds " + TABLE_DIRECTORY + ": " + List.of(DatabaseDescriptor.getAllDataFileLocations()) +
                         " are not the data directories of a node that ran DSE 6); a node in DSE 6 compatibility mode with materialized views does not start";
        assertThatThrownBy(DseViewHiddenColumnCatalog::instance).isInstanceOf(IllegalStateException.class).hasMessage(failure);
        // the failure is kept, and each call that meets it is counted
        assertThatThrownBy(DseViewHiddenColumnCatalog::instance).isInstanceOf(IllegalStateException.class).hasMessage(failure);
        assertThat(DseCompatRefusals.count(DseViewHiddenColumnCatalog.REFUSED_VIEW)).isEqualTo(refusedBefore + 2);
        assertThatThrownBy(DseViewHiddenColumnCatalog::checkViewsAtStartup)
        .isInstanceOf(StartupException.class)
        .hasMessage(failure)
        .satisfies(e -> assertThat(((StartupException) e).returnCode).isEqualTo(StartupException.ERR_WRONG_DISK_STATE));
        assertThat(DseCompatRefusals.count(DseViewHiddenColumnCatalog.REFUSED_VIEW)).isEqualTo(refusedBefore + 3);

        // nothing is read, and nothing refused, outside of the mode
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.NONE);
        DseViewHiddenColumnCatalog.checkViewsAtStartup();
        assertThat(DseCompatRefusals.count(DseViewHiddenColumnCatalog.REFUSED_VIEW)).isEqualTo(refusedBefore + 3);
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);

        // another catalog replaces the failed one, and the node starts with its view
        DseViewHiddenColumnCatalog.setInstance(new DseViewHiddenColumnCatalog(dseHiddenColumns));
        ColumnMetadata hidden = ColumnMetadata.regularColumn(view, ByteBufferUtil.bytes(".v"), UTF8Type.instance);
        assertThat(DseViewHiddenColumnCatalog.instance().translatedColumn(view)).isEqualTo(hidden);
        DseViewHiddenColumnCatalog.checkViewsAtStartup();
        assertThat(DseCompatRefusals.count(DseViewHiddenColumnCatalog.REFUSED_VIEW)).isEqualTo(refusedBefore + 3);
    }

    /**
     * A sstable of DSE's table that cannot be read at startup, whether a component the reader opens (the metadata) or
     * the data the catalog scans: the disk failure policy applies, as to any sstable (with {@code stop}, the node stops;
     * here the killer of the test records it), and the views are refused.
     */
    @Test
    public void aCorruptCatalogMeetsTheDiskFailurePolicy() throws Throwable
    {
        corruptCatalogMeetsTheDiskFailurePolicy(SSTableFormat.Components.Types.STATS, false, true);
        corruptCatalogMeetsTheDiskFailurePolicy(SSTableFormat.Components.Types.DATA, false, true);
        // a truncated copy fails an assertion of the reader, which the policy does not cover; the views are refused
        corruptCatalogMeetsTheDiskFailurePolicy(SSTableFormat.Components.Types.DATA, true, false);
    }

    private void corruptCatalogMeetsTheDiskFailurePolicy(Component.Type corrupted, boolean truncate, boolean killed) throws Throwable
    {
        Path tableDirectory = Path.of(DatabaseDescriptor.getAllDataFileLocations()[0].toString(), "system_schema",
                                      DseViewHiddenColumnCatalog.HIDDEN_COLUMNS + '-' + DseViewHiddenColumnCatalog.HIDDEN_COLUMNS_ID.toHexString());
        Config.DiskFailurePolicy policyBefore = DatabaseDescriptor.getDiskFailurePolicy();
        KillerForTests killer = new KillerForTests();
        JVMKiller killerBefore = JVMStabilityInspector.replaceKiller(killer);
        try
        {
            String table = createTable("CREATE TABLE %s (keyspace_name text, table_name text, column_name text, clustering_order text, " +
                                       "column_name_bytes blob, kind text, position int, required_for_liveness boolean, type text, " +
                                       "PRIMARY KEY ((keyspace_name), table_name, column_name))");
            execute("INSERT INTO %s (keyspace_name, table_name, column_name, kind, required_for_liveness, type) VALUES ('ks', 'view', '.v', 'regular', true, 'text')");
            flush();
            Files.createDirectories(tableDirectory);
            for (SSTableReader sstable : getColumnFamilyStore(KEYSPACE, table).getLiveSSTables())
            {
                for (Component component : sstable.components())
                {
                    Path copy = tableDirectory.resolve(sstable.descriptor.fileFor(component).name());
                    Files.copy(sstable.descriptor.fileFor(component).toPath(), copy);
                    if (component.type == corrupted && truncate)
                    {
                        Files.write(copy, new byte[]{ 1, 2, 3 });
                    }
                    else if (component.type == corrupted)
                    {
                        // the same length, other bytes
                        byte[] bytes = Files.readAllBytes(copy);
                        for (int i = 0; i < bytes.length; i++)
                            bytes[i] ^= 0x5a;
                        Files.write(copy, bytes);
                    }
                }
            }
            DatabaseDescriptor.setDiskFailurePolicy(Config.DiskFailurePolicy.stop);
            FileUtils.setFSErrorHandler(new DefaultFSErrorHandler());
            DseViewHiddenColumnCatalog.setInstance(null);
            long refusedBefore = DseCompatRefusals.count(DseViewHiddenColumnCatalog.REFUSED_VIEW);

            assertThatThrownBy(DseViewHiddenColumnCatalog::instance)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageStartingWith("DSE 6 compatibility mode: cannot read DSE's system_schema.hidden_columns from the data directories of this node (")
            .hasMessageContaining(truncate ? "(" + AssertionError.class.getName() : tableDirectory.toString());
            assertThat(killer.wasKilled()).isEqualTo(killed);
            assertThat(DseCompatRefusals.count(DseViewHiddenColumnCatalog.REFUSED_VIEW)).isEqualTo(refusedBefore + 1);
            // the failure is kept: the sstable is not read again
            killer.reset();
            assertThatThrownBy(DseViewHiddenColumnCatalog::instance).isInstanceOf(IllegalStateException.class);
            assertThat(killer.wasKilled()).isFalse();
            assertThat(DseCompatRefusals.count(DseViewHiddenColumnCatalog.REFUSED_VIEW)).isEqualTo(refusedBefore + 2);
        }
        finally
        {
            // the unit tests run without a handler, which only the daemon sets
            FileUtils.setFSErrorHandler(null);
            DatabaseDescriptor.setDiskFailurePolicy(policyBefore);
            JVMStabilityInspector.replaceKiller(killerBefore);
            FileUtils.deleteRecursive(new File(tableDirectory));
        }
    }

    /**
     * At startup in DSE 6 compatibility mode, the node refuses the views whose DSE data it cannot read as DSE does: one
     * with another hidden column, and one whose DSE sstables name a hidden column DSE's table does not list here.
     */
    @Test
    public void startupRefusesTheViewsWhoseDataCannotBeRead() throws Throwable
    {
        Path importRoot = Files.createTempDirectory("dse-view-startup");
        try
        {
            DseViewHiddenColumnCatalog catalog = new DseViewHiddenColumnCatalog(dseHiddenColumns);
            assertThat(catalog.startupProblems(List.of(view))).isEmpty();

            // a regular base column of type int in the view primary key, which DSE lists as int
            String byW = createView("CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE w IS NOT NULL AND pk IS NOT NULL AND ck IS NOT NULL PRIMARY KEY (w, pk, ck)");
            dseHiddenColumns.put(Pair.create(KEYSPACE, byW), List.of(new HiddenColumn(".w", "regular", true, "int")));
            TableMetadata byWView = getColumnFamilyStore(KEYSPACE, byW).metadata();
            assertThat(catalog.startupProblems(List.of(byWView))).isEmpty();
            assertThat(catalog.translatedColumn(byWView)).isEqualTo(ColumnMetadata.regularColumn(byWView, ByteBufferUtil.bytes(".w"), Int32Type.instance));

            String keyOnly = createView("CREATE MATERIALIZED VIEW %s AS SELECT pk, ck FROM %s WHERE pk IS NOT NULL AND ck IS NOT NULL PRIMARY KEY (ck, pk)");
            dseHiddenColumns.put(Pair.create(KEYSPACE, keyOnly), List.of(new HiddenColumn(".v", "regular", false, "text")));
            TableMetadata keyOnlyView = getColumnFamilyStore(KEYSPACE, keyOnly).metadata();
            // a view DSE keeps no hidden column for, and no DSE sstable: read as it is
            TableMetadata legacy = getColumnFamilyStore(KEYSPACE, createView(VIEW_QUERY)).metadata();
            // a view DSE keeps no hidden column for according to the table, but whose DSE sstable names .v
            createTable("CREATE TABLE %s (pk int, ck int, v text, PRIMARY KEY (pk, ck))");
            String fixtureView = createView("CREATE MATERIALIZED VIEW %s AS SELECT pk, ck, v FROM %s " +
                                            "WHERE v IS NOT NULL AND pk IS NOT NULL AND ck IS NOT NULL PRIMARY KEY (v, pk, ck)");
            importInto(importRoot, fixtureView, DseFixtures.viewSSTableComponents());
            ColumnFamilyStore fixtureStore = getColumnFamilyStore(KEYSPACE, fixtureView);

            List<DseViewHiddenColumnCatalog.StartupProblem> problems = catalog.startupProblems(List.of(view, keyOnlyView, legacy, fixtureStore.metadata()));
            assertThat(problems).extracting(problem -> problem.diskState).containsExactly(false, true);
            assertThat(problems).extracting(problem -> problem.message).containsExactly("DSE 6 compatibility mode: DSE keeps the hidden columns [.v text (regular)] for materialized view " + KEYSPACE + '.' + keyOnly +
                                                 "; this software only reproduces the hidden column of a regular base column in the view primary key, and " +
                                                 "refuses the view's data rather than return wrong view rows. " + REMEDY,
                                                 "DSE 6 compatibility mode: the sstable " + fixtureStore.getLiveSSTables().iterator().next().descriptor +
                                                 " of materialized view " + KEYSPACE + '.' + fixtureView + ", written by DSE, holds the hidden column .v, but " +
                                                 "DSE's system_schema.hidden_columns on this node lists no hidden column for the view; the data directories of " +
                                                 "this node do not hold the table DSE wrote, and this software cannot tell which hidden columns DSE keeps in " +
                                                 "the view's data");
            DseViewHiddenColumnCatalog.setInstance(catalog);
            // the problems of every view of the schema, whichever the order of the schema, with the exit code of the
            // data directories at fault
            assertThatThrownBy(DseViewHiddenColumnCatalog::checkViewsAtStartup)
            .isInstanceOf(StartupException.class)
            .satisfies(e -> assertThat(((StartupException) e).returnCode).isEqualTo(StartupException.ERR_WRONG_DISK_STATE))
            .satisfies(e -> assertThat(e.getMessage().split("\n")).containsExactlyInAnyOrderElementsOf(problems.stream().map(problem -> problem.message)
                                                                                                                  .collect(Collectors.toList())));
        }
        finally
        {
            FileUtils.deleteRecursive(new File(importRoot));
        }
    }

    /** Imports the given sstable files into the table or view {@code table}, verifying them first. */
    private void importInto(Path importRoot, String table, Collection<Path> files) throws IOException
    {
        ColumnFamilyStore store = getColumnFamilyStore(KEYSPACE, table);
        Path dir = Files.createDirectories(importRoot.resolve(KEYSPACE).resolve(table + '-' + store.metadata.id.toHexString()));
        for (Path file : files)
            Files.copy(file, dir.resolve(file.getFileName()));
        // import only places the files, which an upgraded node finds on disk at startup; the upgrade guards of the
        // mode refuse sstable import in DSE_6 mode, so it runs with the mode off and the mode of the test is restored
        // afterwards
        InternodeCompatibilityMode mode = DatabaseDescriptor.getInternodeCompatibilityMode();
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.NONE);
        List<String> failed;
        try
        {
            failed = store.importNewSSTables(Collections.singleton(dir.toString()), false, false, true, false, false, true, true);
        }
        finally
        {
            DatabaseDescriptor.setInternodeCompatibilityMode(mode);
        }
        assertThat(failed).isEmpty();
    }
}
