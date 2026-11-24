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
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.fasterxml.jackson.databind.JsonNode;
import org.awaitility.Awaitility;
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

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.InternodeCompatibilityMode;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.view.DseViewHiddenColumnCatalog.HiddenColumn;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.dsecompat.DseFixtures;
import org.apache.cassandra.dsecompat.DseFixtures.DseVersion;
import org.apache.cassandra.io.compress.LZ4Compressor;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JsonUtils;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The materialized views DSE 6.8.65 and 6.9.26 wrote for the fixtures {@code sstables/dse-<version>-mv-updates} of
 * {@code test/data/dse-compat} (see its README): a view of DSE's new format over a base table with other regular
 * columns, written by inserts, partial updates, deletions of cells, rows, ranges and partitions, inserts without the
 * view key column, TTLs, changes of the view key column, and a base row deleted and inserted again without the view key
 * column in one update, one flush each, and a view of DSE's old format. Reading DSE's sstables gives the rows DSE
 * answered, and the sstables show the facts about DSE that {@link DseViewHiddenColumns} relies on.
 */
public class DseViewUpdatesFixtureTest extends CQLTester
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
        // null when the class setup failed before it set the property: nothing to restore, and the real error stays the only one
        if (allowDdl != null)
            allowDdl.close();
    }

    @BeforeClass
    public static void startNetwork()
    {
        // view builds need the node to have joined its ring
        requireNetwork();
    }

    /** The directory of DSE's system_schema.hidden_columns in a data directory. */
    private static final String TABLE_DIRECTORY = "hidden_columns-2461e8b28f6a31a1ac7230b9cd9c6ca5";
    /** The keyspace of the fixtures, which DSE's system_schema.hidden_columns names. */
    private static final String DSE_KEYSPACE = "test_ks";
    /** The view of DSE's new format and its base table. */
    private static final String VIEW = "base_by_v";
    private static final String BASE = "base";
    /** The view of DSE's old format and its base table. */
    private static final String LEGACY_VIEW = "legacy_by_v";
    private static final String LEGACY_BASE = "legacy_base";
    /** The flushes of each table of the new format, one for each step of the writes. */
    private static final int FLUSHES = 8;
    /** The regular columns of the view, the hidden one included, in the order of this software. */
    private static final List<String> VIEW_REGULAR_COLUMNS = List.of("-x", ".v", "a", "b", "m", "s");
    /**
     * The rows of the view sstables without a hidden cell: the six view rows deleted with their base rows (one sstable)
     * and the view row of a base row deleted and inserted again without the view key column (another one).
     */
    private static final int ROWS_WITHOUT_HIDDEN_CELL = 7;
    /** The rows of the view sstables, as DSE's sstablemetadata counts them (totalRows). */
    private static final int DUMPED_ROWS = 49;
    /** The regular columns of the base table, in the order of this software. */
    private static final List<String> BASE_REGULAR_COLUMNS = List.of("-x", "a", "b", "v", "m", "s");
    /**
     * When the rows written with the long TTL of step 7 expire, 2036-05-01T00:26:00Z (6.8.65; a few minutes later for
     * 6.9.26): from then on, the answers DSE recorded are no longer what the sstables give.
     */
    private static final long LONG_TTL_EXPIRES_AT = 2093214360L;

    private InternodeCompatibilityMode modeBefore;
    private Path importRoot;

    @Before
    public void setUpImportRoot() throws IOException
    {
        modeBefore = DatabaseDescriptor.getInternodeCompatibilityMode();
        importRoot = Files.createTempDirectory("dse-view-updates");
    }

    @After
    public void tearDownImportRoot()
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(modeBefore);
        FileUtils.deleteRecursive(new File(importRoot));
    }

    @Test
    public void dse6865ViewSSTablesGiveTheRowsDseAnswered() throws Throwable
    {
        viewSSTablesGiveTheRowsDseAnswered(DseVersion.DSE_6_8_65, InternodeCompatibilityMode.DSE_6);
    }

    @Test
    public void dse6926ViewSSTablesGiveTheRowsDseAnswered() throws Throwable
    {
        viewSSTablesGiveTheRowsDseAnswered(DseVersion.DSE_6_9_26, InternodeCompatibilityMode.DSE_6);
    }

    @Test
    public void dse6865ViewSSTablesGiveTheRowsDseAnsweredInNoneMode() throws Throwable
    {
        viewSSTablesGiveTheRowsDseAnswered(DseVersion.DSE_6_8_65, InternodeCompatibilityMode.NONE);
    }

    @Test
    public void dse6926ViewSSTablesGiveTheRowsDseAnsweredInNoneMode() throws Throwable
    {
        viewSSTablesGiveTheRowsDseAnswered(DseVersion.DSE_6_9_26, InternodeCompatibilityMode.NONE);
    }

    @Test
    public void dse6865OldFormatViewHasNoHiddenColumn() throws Throwable
    {
        oldFormatViewHasNoHiddenColumn(DseVersion.DSE_6_8_65);
    }

    @Test
    public void dse6926OldFormatViewHasNoHiddenColumn() throws Throwable
    {
        oldFormatViewHasNoHiddenColumn(DseVersion.DSE_6_9_26);
    }

    private void viewSSTablesGiveTheRowsDseAnswered(DseVersion version, InternodeCompatibilityMode mode) throws Throwable
    {
        assertThat(FBUtilities.nowInSeconds()).as("the rows of the fixture with the long TTL have expired: record the fixture again")
                                              .isLessThan(LONG_TTL_EXPIRES_AT);
        DatabaseDescriptor.setInternodeCompatibilityMode(mode);
        createDseSchema();
        ColumnFamilyStore viewStore = importDseSSTables(version, BASE, VIEW);
        assertThat(viewStore.getLiveSSTables()).hasSize(FLUSHES);
        ColumnFamilyStore baseStore = getColumnFamilyStore(DSE_KEYSPACE, BASE);
        assertThat(baseStore.getLiveSSTables()).hasSize(FLUSHES);
        for (SSTableReader sstable : baseStore.getLiveSSTables())
        {
            assertThat(names(sstable.header.columns().regulars)).isEqualTo(BASE_REGULAR_COLUMNS);
            assertDseSSTable(sstable);
        }

        // DSE's dump of the view sstables: DSE writes the hidden cell in every view row of a live view entry, whichever
        // base columns its update writes, so the rows without it are those with a row deletion; and DSE orders the cells
        // of a row as this software orders the columns of the header, simple columns before complex ones, each by name,
        // so "-x" before the hidden column
        List<DumpedRow> dumped = dseDump(version, baseViewKeyCells(baseStore));
        assertThat(dumped).hasSize(DUMPED_ROWS);
        assertThat(dumped).filteredOn(row -> !row.cells.contains(".v")).hasSize(ROWS_WITHOUT_HIDDEN_CELL).allMatch(row -> row.deleted);
        // one of them, the base row of step 8 deleted and inserted again, has a primary key liveness newer than the
        // deletion, which this software reads as dead
        assertThat(dumped).filteredOn(row -> row.deleted && row.hasLiveness).extracting(row -> row.key).containsExactly("v1|1|1");
        for (DumpedRow row : dumped)
            assertThat(row.cells).isSortedAccordingTo(Comparator.comparingInt(VIEW_REGULAR_COLUMNS::indexOf));
        assertThat(dumped.stream().flatMap(row -> row.cells.stream()).collect(Collectors.toSet())).containsExactlyInAnyOrderElementsOf(VIEW_REGULAR_COLUMNS);

        // DSE names every regular column of the view, the hidden one included, in the header of each view sstable,
        // even in the one holding only row deletions, which has no cell at all; each row read without a row deletion
        // has the liveness of its hidden cell, as DSE's dump shows the cell
        int withoutCells = 0;
        long totalRows = 0;
        List<String> livenesses = new ArrayList<>();
        int deletedRows = 0;
        for (SSTableReader sstable : viewStore.getLiveSSTables())
        {
            assertThat(sstable.header.dseViewLivenessColumn()).isNotNull();
            assertThat(sstable.header.dseViewLivenessColumn().name.toString()).isEqualTo(".v");
            assertThat(names(sstable.header.columns().regulars)).isEqualTo(VIEW_REGULAR_COLUMNS);
            assertDseSSTable(sstable);
            if (sstable.getSSTableMetadata().totalColumnsSet == 0)
                withoutCells++;
            totalRows += sstable.getSSTableMetadata().totalRows;
            deletedRows += livenessOfTheRows(sstable, livenesses);
        }
        assertThat(withoutCells).isEqualTo(1);
        assertThat(totalRows).isEqualTo(DUMPED_ROWS);
        assertThat(deletedRows + livenesses.size()).isEqualTo(DUMPED_ROWS);
        assertThat(dumped).filteredOn(row -> row.deleted).hasSize(deletedRows);
        assertThat(livenesses).containsExactlyInAnyOrderElementsOf(dumped.stream()
                                                                          .filter(row -> !row.deleted)
                                                                          .map(row -> row.key + ' ' + row.hiddenCell)
                                                                          .collect(Collectors.toList()));

        assertTheRowsDseAnswered(version, "select-star-base_by_v.txt", VIEW, 12);
        assertTheRowsDseAnswered(version, "select-star-base.txt", BASE, 16);

        // the same rows once compaction has merged the DSE sstables into one this software wrote, without the hidden column
        viewStore.forceMajorCompaction();
        assertThat(viewStore.getLiveSSTables()).hasSize(1);
        SSTableReader compacted = viewStore.getLiveSSTables().iterator().next();
        assertThat(compacted.header.dseViewLivenessColumn()).isNull();
        assertThat(names(compacted.header.columns().regulars)).doesNotContain(".v");
        assertTheRowsDseAnswered(version, "select-star-base_by_v.txt", VIEW, 12);

        // and once a compaction has purged every tombstone and expired liveness, whatever the date (with the default
        // gc_grace_seconds the fixture's are purgeable from ten days after they were written; a view refuses 0)
        CompactionManager.instance.submitUserDefined(viewStore, Collections.singleton(compacted.descriptor), FBUtilities.nowInSeconds()).get();
        assertThat(viewStore.getLiveSSTables()).hasSize(1);
        SSTableReader purged = viewStore.getLiveSSTables().iterator().next();
        assertThat(purged.descriptor).isNotEqualTo(compacted.descriptor);
        assertThat(purged.getSSTableMetadata().totalRows).isEqualTo(12);
        assertTheRowsDseAnswered(version, "select-star-base_by_v.txt", VIEW, 12);
    }

    /**
     * A view DSE created in its old format (a node started with {@code -Dcassandra.mv.create_legacy_schema=true}) has no
     * row in DSE's system_schema.hidden_columns, which lists the hidden column of the view of the new format; its DSE
     * sstable names no hidden column, and gives the rows DSE answered. At startup in DSE 6 compatibility mode the node
     * accepts both views, warning about the one DSE lists nothing for.
     */
    private void oldFormatViewHasNoHiddenColumn(DseVersion version) throws Throwable
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        // DSE's schema: the view of the new format has version 1, the other 0, and DSE's system_schema.hidden_columns
        // lists the hidden column of the first alone
        List<List<String>> schema = dseAnswers(version, "system-schema-legacy.txt");
        assertThat(schema).hasSize(3);
        assertThat(column(schema.get(0), "view_name")).containsExactly(VIEW, LEGACY_VIEW);
        assertThat(column(schema.get(0), "version")).containsExactly("1", "0");
        assertThat(column(schema.get(1), "table_name")).containsExactly(VIEW);
        assertThat(column(schema.get(1), "column_name")).containsExactly(".v");
        try (Stream<Path> files = Files.list(fixture(version).resolve("sstables").resolve("system_schema").resolve(TABLE_DIRECTORY)))
        {
            assertThat(files.filter(f -> f.getFileName().toString().endsWith("-Data.db"))).hasSize(3);
        }
        DseViewHiddenColumnCatalog catalog = DseViewHiddenColumnCatalog.load(List.of(new File(fixture(version).resolve("sstables"))));
        assertThat(catalog.hiddenColumns(DSE_KEYSPACE, VIEW)).extracting(HiddenColumn::toString).containsExactly(".v text (regular, required for liveness)");
        assertThat(catalog.hiddenColumns(DSE_KEYSPACE, LEGACY_VIEW)).isEmpty();

        createDseSchema();
        ColumnFamilyStore legacyStore = importDseSSTables(version, LEGACY_BASE, LEGACY_VIEW);
        assertThat(legacyStore.getLiveSSTables()).hasSize(1);
        SSTableReader legacySSTable = legacyStore.getLiveSSTables().iterator().next();
        assertDseSSTable(legacySSTable);
        assertThat(legacySSTable.header.dseViewLivenessColumn()).isNull();
        assertThat(names(legacySSTable.header.columns().regulars)).containsExactly("a");
        List<List<String>> expected = dseAnswers(version, "select-star-legacy.txt");
        assertThat(expected).hasSize(2);
        assertThat(rowsOf(LEGACY_VIEW, expected.get(0))).isEqualTo(expected.get(0));
        assertThat(rowsOf(LEGACY_BASE, expected.get(1))).isEqualTo(expected.get(1));

        TableMetadata view = getColumnFamilyStore(DSE_KEYSPACE, VIEW).metadata();
        TableMetadata legacyView = getColumnFamilyStore(DSE_KEYSPACE, LEGACY_VIEW).metadata();
        Logger catalogLogger = (Logger) LoggerFactory.getLogger(DseViewHiddenColumnCatalog.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        catalogLogger.addAppender(appender);
        try
        {
            assertThat(catalog.startupProblems(List.of(view, legacyView))).isEmpty();
            assertThat(catalog.translatedColumn(view)).isEqualTo(DseViewHiddenColumns.requiredHiddenColumn(view));
            assertThat(catalog.translatedColumn(legacyView)).isNull();
        }
        finally
        {
            catalogLogger.detachAppender(appender);
        }
        assertThat(appender.list).filteredOn(e -> e.getLevel() == Level.WARN)
                                 .extracting(ILoggingEvent::getFormattedMessage)
                                 .containsExactly("DSE 6 compatibility mode: DSE's system_schema.hidden_columns on this node lists no hidden column for " +
                                                  "materialized view " + DSE_KEYSPACE + '.' + LEGACY_VIEW + ", whose data is exchanged with DSE 6 nodes as " +
                                                  "it is, as for a view of DSE's old format or one created by this software; if DSE created the view in " +
                                                  "its new format, the data directories of this node do not hold the table DSE wrote, and DSE reads the " +
                                                  "view rows of this node as dead");
    }

    /** The tables and views of the fixtures, as DSE created them, in the keyspace DSE's system_schema.hidden_columns names. */
    private void createDseSchema()
    {
        createKeyspace("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}", DSE_KEYSPACE);
        schemaChange("CREATE TABLE " + DSE_KEYSPACE + '.' + BASE + " (pk int, ck int, v text, a int, b text, \"-x\" int, s set<int>, m map<text, int>, PRIMARY KEY (pk, ck))");
        schemaChange("CREATE MATERIALIZED VIEW " + DSE_KEYSPACE + '.' + VIEW + " AS SELECT * FROM " + DSE_KEYSPACE + '.' + BASE +
                     " WHERE v IS NOT NULL AND pk IS NOT NULL AND ck IS NOT NULL PRIMARY KEY (v, pk, ck)");
        schemaChange("CREATE TABLE " + DSE_KEYSPACE + '.' + LEGACY_BASE + " (pk int, ck int, v text, a int, PRIMARY KEY (pk, ck))");
        schemaChange("CREATE MATERIALIZED VIEW " + DSE_KEYSPACE + '.' + LEGACY_VIEW + " AS SELECT * FROM " + DSE_KEYSPACE + '.' + LEGACY_BASE +
                     " WHERE v IS NOT NULL AND pk IS NOT NULL AND ck IS NOT NULL PRIMARY KEY (v, pk, ck)");
        // the builds of the empty views end before the DSE sstables of their base tables are imported
        for (String view : List.of(VIEW, LEGACY_VIEW))
            Awaitility.await().atMost(1, TimeUnit.MINUTES).until(() -> SystemKeyspace.isViewBuilt(DSE_KEYSPACE, view));
        for (String table : List.of(BASE, VIEW, LEGACY_BASE, LEGACY_VIEW))
            getColumnFamilyStore(DSE_KEYSPACE, table).disableAutoCompaction();
    }

    /** Imports the DSE sstables of {@code base} and of its view {@code view}, and returns the store of the view. */
    private ColumnFamilyStore importDseSSTables(DseVersion version, String base, String view) throws IOException
    {
        importInto(base, dseTableDirectory(version, base));
        importInto(view, dseTableDirectory(version, view));
        return getColumnFamilyStore(DSE_KEYSPACE, view);
    }

    /** Asserts that {@code sstable} is a DSE sstable as the README of the fixtures describes it: its 8 components, LZ4 compression. */
    private static void assertDseSSTable(SSTableReader sstable)
    {
        assertThat(sstable.components()).hasSize(8);
        assertThat(sstable.getCompressionMetadata().compressor()).isInstanceOf(LZ4Compressor.class);
    }

    /**
     * Adds to {@code livenesses}, for each row of the view sstable {@code sstable} without a row deletion, its key and its
     * primary key liveness in the form of {@link DumpedRow#hiddenCell}. Asserts that no row has the hidden cell itself,
     * that no row without a row deletion has {@link DseViewHiddenColumns#LIVENESS_WITHOUT_HIDDEN_CELL}, and that the rows
     * with one have no liveness.
     *
     * @return the rows with a row deletion
     */
    private static int livenessOfTheRows(SSTableReader sstable, List<String> livenesses)
    {
        int deleted = 0;
        try (ISSTableScanner scanner = sstable.getScanner())
        {
            while (scanner.hasNext())
            {
                try (UnfilteredRowIterator partition = scanner.next())
                {
                    String v = UTF8Type.instance.compose(partition.partitionKey().getKey());
                    while (partition.hasNext())
                    {
                        Row row = (Row) partition.next();
                        assertThat(row.getCell(sstable.header.dseViewLivenessColumn())).isNull();
                        if (!row.deletion().isLive())
                        {
                            // the row deletion shadows the liveness of the row, LIVENESS_WITHOUT_HIDDEN_CELL included
                            assertThat(row.primaryKeyLivenessInfo().isEmpty()).isTrue();
                            deleted++;
                            continue;
                        }
                        String key = v + '|' + Int32Type.instance.compose(row.clustering().bufferAt(0)) + '|' + Int32Type.instance.compose(row.clustering().bufferAt(1));
                        LivenessInfo liveness = row.primaryKeyLivenessInfo();
                        assertThat(liveness).isNotEqualTo(DseViewHiddenColumns.LIVENESS_WITHOUT_HIDDEN_CELL);
                        // an expired liveness is expiring too, with the TTL EXPIRED_LIVENESS_TTL
                        livenesses.add(key + ' ' + (liveness.isExpiring() ? hiddenCell(liveness.timestamp(), liveness.ttl(), liveness.localExpirationTime())
                                                                          : hiddenCell(liveness.timestamp(), LivenessInfo.NO_TTL, LivenessInfo.NO_EXPIRATION_TIME)));
                    }
                }
            }
        }
        return deleted;
    }

    /** A hidden cell, or the liveness this software gives the row of the cell, as {@link DumpedRow#hiddenCell} gives it. */
    private static String hiddenCell(long timestamp, int ttl, long localExpirationTime)
    {
        return timestamp + "/" + ttl + '/' + localExpirationTime;
    }

    /**
     * Asserts that {@code SELECT} of the columns of DSE's answer {@code file} from {@code table} gives, in the same order,
     * the {@code rows} rows DSE answered.
     */
    private void assertTheRowsDseAnswered(DseVersion version, String file, String table, int rows) throws Throwable
    {
        List<List<String>> answers = dseAnswers(version, file);
        assertThat(answers).hasSize(1);
        List<String> expected = answers.get(0);
        assertThat(expected).hasSize(1 + rows);
        assertThat(rowsOf(table, expected)).isEqualTo(expected);
    }

    /**
     * The rows of {@code table}, in the format of cqlsh: the column names of {@code expected}'s first line, then one line
     * for each row, the values separated by {@code " | "}.
     */
    private List<String> rowsOf(String table, List<String> expected) throws Throwable
    {
        List<String> columns = Arrays.asList(expected.get(0).split(" \\| "));
        String select = columns.stream().map(c -> c.matches("[a-z_][a-z0-9_]*") ? c : '"' + c + '"').collect(Collectors.joining(", "));
        UntypedResultSet result = execute("SELECT " + select + " FROM " + DSE_KEYSPACE + '.' + table);
        List<String> lines = new ArrayList<>();
        lines.add(String.join(" | ", columns));
        for (UntypedResultSet.Row row : result)
        {
            List<String> values = new ArrayList<>();
            for (ColumnSpecification column : result.metadata())
            {
                ByteBuffer value = row.getBytes(column.name.toString());
                if (value == null)
                    values.add("null");
                else if (column.type instanceof UTF8Type)
                    values.add(UTF8Type.instance.compose(value));
                else
                    values.add(column.type.toCQLString(value));
            }
            lines.add(String.join(" | ", values));
        }
        return lines;
    }

    /**
     * What DSE answered in the cqlsh output {@code file} of the fixture: for each result, the line of its column names
     * then one line for each row, the values trimmed and separated by {@code " | "}; each result checked against the row
     * count cqlsh printed.
     */
    private static List<List<String>> dseAnswers(DseVersion version, String file) throws IOException
    {
        List<List<String>> answers = new ArrayList<>();
        List<String> current = null;
        Pattern count = Pattern.compile("^\\((\\d+) rows\\)$");
        for (String line : Files.readAllLines(fixture(version).resolve(file)))
        {
            Matcher m = count.matcher(line.trim());
            if (m.matches())
            {
                assertThat(current).isNotNull();
                assertThat(current).hasSize(1 + Integer.parseInt(m.group(1)));
                answers.add(current);
                current = null;
            }
            else if (line.contains(" | "))
            {
                if (current == null)
                    current = new ArrayList<>();
                current.add(Stream.of(line.split("\\|")).map(String::trim).collect(Collectors.joining(" | ")));
            }
        }
        assertThat(current).isNull();
        return answers;
    }

    /**
     * A row of DSE's sstabledump of the view sstables: its key ({@code v|pk|ck}), the names of its cells, in the order
     * DSE wrote them, whether it has a row deletion and a primary key liveness, and its hidden cell, null when it has
     * none: the timestamp, TTL and local expiration time of the cell ({@link #hiddenCell}; for a tombstone, the TTL
     * {@link LivenessInfo#EXPIRED_LIVENESS_TTL} and its local deletion time, for a cell that does not expire,
     * {@link LivenessInfo#NO_TTL} and {@link LivenessInfo#NO_EXPIRATION_TIME}), which is the liveness this software
     * gives the row.
     */
    private static final class DumpedRow
    {
        private final String key;
        private final List<String> cells;
        private final boolean deleted;
        private final boolean hasLiveness;
        private final String hiddenCell;

        private DumpedRow(String key, List<String> cells, boolean deleted, boolean hasLiveness, String hiddenCell)
        {
            this.key = key;
            this.cells = cells;
            this.deleted = deleted;
            this.hasLiveness = hasLiveness;
            this.hiddenCell = hiddenCell;
        }
    }

    /** The rows of DSE's sstabledump of the view sstables of the fixture, one JSON array for each sstable. */
    private static List<DumpedRow> dseDump(DseVersion version, Map<String, String> baseViewKeyCells) throws IOException
    {
        List<StringBuilder> dumps = new ArrayList<>();
        for (String line : Files.readAllLines(fixture(version).resolve("sstabledump-base_by_v.txt")))
        {
            if (line.startsWith("=== "))
                dumps.add(new StringBuilder());
            else
                dumps.get(dumps.size() - 1).append(line).append('\n');
        }
        assertThat(dumps).hasSize(FLUSHES);
        List<DumpedRow> rows = new ArrayList<>();
        for (StringBuilder dump : dumps)
        {
            for (JsonNode partition : JsonUtils.JSON_OBJECT_MAPPER.readTree(dump.toString()))
            {
                String v = partition.get("partition").get("key").get(0).asText();
                for (JsonNode row : partition.get("rows"))
                {
                    assertThat(row.get("type").asText()).isEqualTo("row");
                    String key = v + '|' + row.get("clustering").get(0).asInt() + '|' + row.get("clustering").get(1).asInt();
                    // sstabledump leaves out the timestamp of a cell that has the one of the row's liveness, and the
                    // TTL and expiration of a cell that does not expire, and of an expiring cell whose TTL is the one
                    // of the row's liveness, whatever its expiration; so for a cell without them in a row whose
                    // liveness expires, the base cell of the view key column, from the base sstables, gives them
                    JsonNode liveness = row.get("liveness_info");
                    List<String> cells = new ArrayList<>();
                    String hiddenCell = null;
                    for (JsonNode cell : row.get("cells"))
                    {
                        // a quoted identifier is dumped with its quotes
                        String name = cell.get("name").asText().replaceAll("^\"(.*)\"$", "$1");
                        if (cells.isEmpty() || !cells.get(cells.size() - 1).equals(name))
                            cells.add(name);
                        if (!name.equals(".v"))
                            continue;
                        assertThat(cell.has("tstamp") || liveness != null).as("the timestamp of the hidden cell of the row %s", key).isTrue();
                        long timestamp = micros((cell.has("tstamp") ? cell : liveness).get("tstamp").asText());
                        if (cell.has("deletion_info"))
                            hiddenCell = hiddenCell(timestamp, LivenessInfo.EXPIRED_LIVENESS_TTL, seconds(cell.get("deletion_info").get("local_delete_time").asText()));
                        else if (cell.has("ttl"))
                            hiddenCell = hiddenCell(timestamp, cell.get("ttl").asInt(), seconds(cell.get("expires_at").asText()));
                        else if (liveness != null && liveness.has("ttl"))
                            hiddenCell = baseViewKeyCells.get(key + '|' + timestamp);
                        else
                            hiddenCell = hiddenCell(timestamp, LivenessInfo.NO_TTL, LivenessInfo.NO_EXPIRATION_TIME);
                    }
                    if (cells.contains(".v"))
                        assertThat(hiddenCell).as("the hidden cell of the row %s", key).isNotNull();
                    rows.add(new DumpedRow(key, cells, row.has("deletion_info"), liveness != null, hiddenCell));
                }
            }
        }
        return rows;
    }

    /**
     * The cells of the view key column {@code v} in the base sstables of {@code baseStore}, by the key of their view row
     * and their timestamp ({@code v|pk|ck|timestamp}), in the form of {@link DumpedRow#hiddenCell}.
     */
    private static Map<String, String> baseViewKeyCells(ColumnFamilyStore baseStore)
    {
        ColumnMetadata column = baseStore.metadata().getColumn(ByteBufferUtil.bytes("v"));
        Map<String, String> cells = new HashMap<>();
        for (SSTableReader sstable : baseStore.getLiveSSTables())
        {
            try (ISSTableScanner scanner = sstable.getScanner())
            {
                while (scanner.hasNext())
                {
                    try (UnfilteredRowIterator partition = scanner.next())
                    {
                        int pk = Int32Type.instance.compose(partition.partitionKey().getKey());
                        while (partition.hasNext())
                        {
                            Unfiltered unfiltered = partition.next();
                            if (!unfiltered.isRow())
                                continue;
                            Row row = (Row) unfiltered;
                            Cell<?> cell = row.getCell(column);
                            if (cell == null || cell.isTombstone())
                                continue;
                            String key = UTF8Type.instance.compose(cell.buffer()) + '|' + pk + '|' + Int32Type.instance.compose(row.clustering().bufferAt(0));
                            cells.put(key + '|' + cell.timestamp(), cell.isExpiring() ? hiddenCell(cell.timestamp(), cell.ttl(), cell.localDeletionTime())
                                                                                      : hiddenCell(cell.timestamp(), LivenessInfo.NO_TTL, LivenessInfo.NO_EXPIRATION_TIME));
                        }
                    }
                }
            }
        }
        return cells;
    }

    /** The microseconds since the epoch of a timestamp as sstabledump prints it ({@code 2026-10-28T19:05:42.999999Z}). */
    private static long micros(String timestamp)
    {
        Instant instant = Instant.parse(timestamp);
        return TimeUnit.SECONDS.toMicros(instant.getEpochSecond()) + TimeUnit.NANOSECONDS.toMicros(instant.getNano());
    }

    /** The seconds since the epoch of a local deletion or expiration time as sstabledump prints it ({@code 2026-10-28T19:05:48Z}). */
    private static long seconds(String time)
    {
        Instant instant = Instant.parse(time);
        assertThat(instant.getNano()).isZero();
        return instant.getEpochSecond();
    }

    /** The values of the column {@code name} in the result {@code answer} of {@link #dseAnswers}. */
    private static List<String> column(List<String> answer, String name)
    {
        int index = Arrays.asList(answer.get(0).split(" \\| ")).indexOf(name);
        assertThat(index).isNotNegative();
        return answer.subList(1, answer.size()).stream().map(line -> line.split(" \\| ")[index]).collect(Collectors.toList());
    }

    private static List<String> names(Iterable<ColumnMetadata> columns)
    {
        List<String> names = new ArrayList<>();
        for (ColumnMetadata column : columns)
            names.add(column.name.toString());
        return names;
    }

    /** The fixture of {@code version}: what DSE wrote and answered for the views of {@link #DSE_KEYSPACE}. */
    private static Path fixture(DseVersion version)
    {
        Path fixture = DseFixtures.ROOT.resolve("sstables").resolve(version.dir + "-mv-updates");
        assertThat(fixture).isDirectory();
        return fixture;
    }

    /** The directory of DSE's sstables of {@code table} in the fixture of {@code version}. */
    private static Path dseTableDirectory(DseVersion version, String table) throws IOException
    {
        try (Stream<Path> dirs = Files.list(fixture(version).resolve("sstables").resolve(DSE_KEYSPACE)))
        {
            List<Path> found = dirs.filter(d -> d.getFileName().toString().startsWith(table + '-')).collect(Collectors.toList());
            assertThat(found).hasSize(1);
            return found.get(0);
        }
    }

    /** Imports the sstables of the DSE table directory {@code dseDirectory} into {@code table}, verifying them first. */
    private void importInto(String table, Path dseDirectory) throws IOException
    {
        ColumnFamilyStore store = getColumnFamilyStore(DSE_KEYSPACE, table);
        Path dir = Files.createDirectories(importRoot.resolve(DSE_KEYSPACE).resolve(table + '-' + store.metadata.id.toHexString()));
        try (Stream<Path> files = Files.list(dseDirectory))
        {
            for (Path file : files.collect(Collectors.toList()))
                Files.copy(file, dir.resolve(file.getFileName()));
        }
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
