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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Uninterruptibles;
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
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.streaming.CassandraOutgoingFile;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.dsecompat.DseFixtures;
import org.apache.cassandra.exceptions.UnknownColumnException;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.IScrubber;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.streaming.OutgoingStream;
import org.apache.cassandra.streaming.StreamOperation;
import org.apache.cassandra.streaming.StreamPlan;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.Refs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Materialized view sstables written by DataStax Enterprise, whose header names the hidden column DSE keeps
 * for a regular base column in the view primary key ({@code .v}): the real sstable of a DSE 6.8.65 view gives
 * the rows DSE answers, in both compatibility modes and after compaction, and sstables carrying such a column
 * with each kind of cell give the rows DSE would give (see {@link DseViewHiddenColumns}).
 */
public class DseViewHiddenColumnsTest extends CQLTester
{
    /**
     * These tests create tables in DSE_6 mode, with the property reserved for schema changes in the mode set, as the
     * tests of the mode do.
     */
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

    private static final String VIEW_QUERY = "CREATE MATERIALIZED VIEW %s AS SELECT pk, ck, v FROM %s " +
                                             "WHERE v IS NOT NULL AND pk IS NOT NULL AND ck IS NOT NULL PRIMARY KEY (v, pk, ck)";

    private InternodeCompatibilityMode modeBefore;
    private Path importRoot;

    @BeforeClass
    public static void startNetwork()
    {
        // view builds need the node to have joined its ring
        requireNetwork();
    }

    @Before
    public void setUpImportRoot() throws IOException
    {
        modeBefore = DatabaseDescriptor.getInternodeCompatibilityMode();
        importRoot = Files.createTempDirectory("dse-view-hidden-columns");
    }

    @After
    public void tearDownImportRoot()
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(modeBefore);
        FileUtils.deleteRecursive(new File(importRoot));
    }

    @Test
    public void dseViewSSTableGivesTheRowsDseAnswersInDse6Mode() throws Throwable
    {
        dseViewSSTableGivesTheRowsDseAnswers(InternodeCompatibilityMode.DSE_6);
    }

    @Test
    public void dseViewSSTableGivesTheRowsDseAnswersInNoneMode() throws Throwable
    {
        dseViewSSTableGivesTheRowsDseAnswers(InternodeCompatibilityMode.NONE);
    }

    private void dseViewSSTableGivesTheRowsDseAnswers(InternodeCompatibilityMode mode) throws Throwable
    {
        Logger resolveLogger = (Logger) LoggerFactory.getLogger(DseViewHiddenColumns.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        resolveLogger.addAppender(appender);
        try
        {
            dseViewSSTableGivesTheRowsDseAnswers(mode, appender);
        }
        finally
        {
            resolveLogger.detachAppender(appender);
        }
    }

    private void dseViewSSTableGivesTheRowsDseAnswers(InternodeCompatibilityMode mode, ListAppender<ILoggingEvent> appender) throws Throwable
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(mode);
        createTable("CREATE TABLE %s (pk int, ck int, v text, PRIMARY KEY (pk, ck))");
        String view = createView(VIEW_QUERY);
        ColumnFamilyStore viewStore = getColumnFamilyStore(KEYSPACE, view);
        viewStore.disableAutoCompaction();

        importInto(view, DseFixtures.viewSSTableComponents());

        SSTableReader dseSSTable = viewStore.getLiveSSTables().iterator().next();
        ColumnMetadata hidden = dseSSTable.header.dseViewLivenessColumn();
        assertThat(hidden).isNotNull();
        assertThat(hidden.name.toString()).isEqualTo(".v");
        assertThat(dseSSTable.getSSTableMetadata().totalColumnsSet).isEqualTo(5000);

        List<Object[]> expected = DseFixtures.viewSelectStar();
        assertThat(expected).hasSize(5000);
        assertRows(execute("SELECT * FROM " + KEYSPACE + '.' + view), expected.toArray(new Object[0][]));
        assertRows(execute("SELECT count(*) FROM " + KEYSPACE + '.' + view), row(5000L));
        // one INFO line for the view, whatever the number of times its header is read
        assertThat(appender.list).filteredOn(e -> e.getLevel() == Level.INFO)
                                 .extracting(ILoggingEvent::getFormattedMessage)
                                 .singleElement()
                                 .satisfies(m -> assertThat(m).startsWith("Materialized view " + KEYSPACE + '.' + view + " has sstables written by " +
                                                                          "DSE with the hidden column .v (first seen in ")
                                                              .endsWith("): its cells give those view rows their liveness, as DSE reads them"));
        assertRows(execute("SELECT pk, ck FROM " + KEYSPACE + '.' + view + " WHERE v = 'v3' AND pk = 0 LIMIT 3"),
                   row(0, 3), row(0, 10), row(0, 17));
        // a reversed read of the same partition
        assertRows(execute("SELECT pk, ck FROM " + KEYSPACE + '.' + view + " WHERE v = 'v3' ORDER BY pk DESC, ck DESC LIMIT 3"),
                   row(9, 493), row(9, 486), row(9, 479));

        // Compaction writes the rows with the liveness their hidden cells gave them, without the hidden cells and
        // without the hidden column in the header: the rows stay the same.
        viewStore.forceMajorCompaction();
        assertThat(viewStore.getLiveSSTables()).hasSize(1);
        SSTableReader compacted = viewStore.getLiveSSTables().iterator().next();
        assertThat(compacted.descriptor).isNotEqualTo(dseSSTable.descriptor);
        assertThat(compacted.getSSTableMetadata().totalColumnsSet).isZero();
        assertThat(compacted.header.columns().regulars.isEmpty()).isTrue();
        assertThat(compacted.header.dseViewLivenessColumn()).isNull();
        assertRows(execute("SELECT * FROM " + KEYSPACE + '.' + view), expected.toArray(new Object[0][]));

        // Opened again from disk, the compacted sstable is one of this software: its header does not name the hidden
        // column and its rows have the liveness compaction wrote.
        SSTableReader reopened = SSTableReader.open(viewStore, compacted.descriptor);
        try (ISSTableScanner scanner = reopened.getScanner())
        {
            assertThat(reopened.header.dseViewLivenessColumn()).isNull();
            assertThat(reopened.header.columns().regulars.isEmpty()).isTrue();
            long nowInSec = FBUtilities.nowInSeconds();
            int liveRows = 0;
            while (scanner.hasNext())
            {
                try (UnfilteredRowIterator partition = scanner.next())
                {
                    while (partition.hasNext())
                    {
                        Row row = (Row) partition.next();
                        assertThat(row.primaryKeyLivenessInfo().isLive(nowInSec)).isTrue();
                        assertThat(row.hasLiveData(nowInSec, true)).isTrue();
                        liveRows++;
                    }
                }
            }
            assertThat(liveRows).isEqualTo(5000);
        }
        finally
        {
            reopened.selfRef().release();
        }
    }

    /**
     * The DSE sstables of the base table and of the view, then base updates made by this software: the view entries it
     * writes (with its own encoding) merge with the DSE ones, before and after compaction.
     */
    @Test
    public void viewUpdatesOfThisSoftwareMergeWithDseViewRows() throws Throwable
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        String base = createTable("CREATE TABLE %s (pk int, ck int, v text, PRIMARY KEY (pk, ck))");
        String view = createView(VIEW_QUERY);
        getColumnFamilyStore(KEYSPACE, base).disableAutoCompaction();
        ColumnFamilyStore viewStore = getColumnFamilyStore(KEYSPACE, view);
        viewStore.disableAutoCompaction();
        importInto(base, DseFixtures.viewBaseSSTableComponents());
        importInto(view, DseFixtures.viewSSTableComponents());
        String viewTable = KEYSPACE + '.' + view;
        assertRows(execute("SELECT count(*) FROM %s"), row(5000L));
        assertRows(execute("SELECT count(*) FROM " + viewTable), row(5000L));

        // the base cell changes value: the DSE entry goes, a new one comes
        execute("UPDATE %s SET v = 'v9' WHERE pk = 0 AND ck = 3");
        // the base cell is deleted, then the whole base row: their DSE entries go
        execute("DELETE v FROM %s WHERE pk = 0 AND ck = 10");
        execute("DELETE FROM %s WHERE pk = 1 AND ck = 3");
        // the base cell is written again with the same value: the entry stays
        execute("UPDATE %s SET v = 'v3' WHERE pk = 2 AND ck = 3");

        for (int round = 0; round < 2; round++)
        {
            assertEmpty(execute("SELECT * FROM " + viewTable + " WHERE v = 'v3' AND pk = 0 AND ck = 3"));
            assertRows(execute("SELECT * FROM " + viewTable + " WHERE v = 'v9'"), row("v9", 0, 3));
            assertEmpty(execute("SELECT * FROM " + viewTable + " WHERE v = 'v3' AND pk = 0 AND ck = 10"));
            assertEmpty(execute("SELECT * FROM " + viewTable + " WHERE v = 'v3' AND pk = 1 AND ck = 3"));
            assertRows(execute("SELECT * FROM " + viewTable + " WHERE v = 'v3' AND pk = 2 AND ck = 3"), row("v3", 2, 3));
            assertRows(execute("SELECT count(*) FROM " + viewTable), row(4998L));
            assertRows(execute("SELECT count(*) FROM " + viewTable + " WHERE v = 'v3'"), row(707L));

            // the same once the entries of this software are flushed and compacted with the DSE ones
            flush(KEYSPACE, view);
            viewStore.forceMajorCompaction();
            assertThat(viewStore.getLiveSSTables()).hasSize(1);
            assertThat(viewStore.getLiveSSTables().iterator().next().header.columns().regulars.isEmpty()).isTrue();
        }
    }

    @Test
    public void scrubRewritesTheDseViewSSTableWithoutTheHiddenColumn() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, v text, PRIMARY KEY (pk, ck))");
        String view = createView(VIEW_QUERY);
        ColumnFamilyStore viewStore = getColumnFamilyStore(KEYSPACE, view);
        viewStore.disableAutoCompaction();
        importInto(view, DseFixtures.viewSSTableComponents());
        SSTableReader dseSSTable = viewStore.getLiveSSTables().iterator().next();

        // scrub writes with the header of the sstable it reads
        viewStore.scrub(true, IScrubber.options().checkData().build(), 1);
        assertThat(viewStore.getLiveSSTables()).hasSize(1);
        SSTableReader scrubbed = viewStore.getLiveSSTables().iterator().next();
        assertThat(scrubbed.descriptor).isNotEqualTo(dseSSTable.descriptor);
        assertThat(scrubbed.header.columns().regulars.isEmpty()).isTrue();
        assertThat(scrubbed.getSSTableMetadata().totalColumnsSet).isZero();
        assertRows(execute("SELECT * FROM " + KEYSPACE + '.' + view), DseFixtures.viewSelectStar().toArray(new Object[0][]));
    }

    /**
     * The same rule for a view with regular columns besides the hidden one, a simple and a complex one: the cells of
     * the other columns are read as they are, whether a row has all the columns of the header or only some of them,
     * and the hidden cell is dropped from every row.
     */
    @Test
    public void hiddenCellAmongOtherRegularColumns() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, v text, w int, s set<int>, PRIMARY KEY (pk, ck))");
        String view = createView("CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                                 "WHERE v IS NOT NULL AND pk IS NOT NULL AND ck IS NOT NULL PRIMARY KEY (v, pk, ck)");
        ColumnFamilyStore viewStore = getColumnFamilyStore(KEYSPACE, view);
        viewStore.disableAutoCompaction();
        String dse = createTable("CREATE TABLE %s (v text, pk int, ck int, \".v\" text, w int, s set<int>, PRIMARY KEY (v, pk, ck))");
        getColumnFamilyStore(KEYSPACE, dse).disableAutoCompaction();

        // every column of the header, the set with the complex deletion of its assignment, the hidden cell newer than
        // the primary key liveness: live, with the liveness of the hidden cell
        execute("INSERT INTO %s (v, pk, ck, w, s) VALUES ('a', 1, 1, 1, {1, 2}) USING TIMESTAMP 5");
        execute("UPDATE %s USING TIMESTAMP 10 SET \".v\" = '' WHERE v = 'a' AND pk = 1 AND ck = 1");
        // the hidden cell and the simple column only: live
        execute("UPDATE %s USING TIMESTAMP 10 SET \".v\" = '', w = 2 WHERE v = 'b' AND pk = 1 AND ck = 2");
        // the hidden cell and the complex column only: live
        execute("UPDATE %s USING TIMESTAMP 10 SET \".v\" = '', s = {3} WHERE v = 'c' AND pk = 1 AND ck = 3");
        // a hidden tombstone with live cells of the other columns: not live, the view has the strict liveness of a view
        // with a regular base column in its primary key
        execute("UPDATE %s USING TIMESTAMP 10 SET w = 4, s = {4} WHERE v = 'd' AND pk = 1 AND ck = 4");
        execute("DELETE \".v\" FROM %s USING TIMESTAMP 10 WHERE v = 'd' AND pk = 1 AND ck = 4");
        // no hidden cell, a primary key liveness and live cells (what DSE's compaction may leave of an old view entry
        // once it purges its hidden tombstone): not live, as for DSE
        execute("INSERT INTO %s (v, pk, ck, w, s) VALUES ('e', 1, 5, 5, {5}) USING TIMESTAMP 10");
        // a partition of many rows, larger than the column index size (asserted below), where every third row has no
        // hidden cell
        for (int ck = 0; ck < WIDE_ROWS; ck++)
        {
            if (ck % 3 == 1)
                execute("INSERT INTO %s (v, pk, ck, w) VALUES ('m', 1, ?, ?) USING TIMESTAMP 10", ck, ck);
            else
                execute("INSERT INTO %s (v, pk, ck, \".v\", w, s) VALUES ('m', 1, ?, '', ?, {" + ck + "}) USING TIMESTAMP 10", ck, ck);
            execute("UPDATE %s USING TIMESTAMP 10 SET s = s + {-1} WHERE v = 'm' AND pk = 1 AND ck = ?", ck);
        }
        flush(KEYSPACE, dse);

        List<Path> files = new ArrayList<>();
        for (SSTableReader sstable : getColumnFamilyStore(KEYSPACE, dse).getLiveSSTables())
            for (Component component : sstable.components())
                files.add(sstable.descriptor.fileFor(component).toPath());
        importInto(view, files);

        SSTableReader dseSSTable = viewStore.getLiveSSTables().iterator().next();
        ColumnMetadata hidden = dseSSTable.header.dseViewLivenessColumn();
        assertThat(hidden.name.toString()).isEqualTo(".v");
        assertThat(dseSSTable.header.columns().regulars.size()).isEqualTo(3);
        assertThat(DatabaseDescriptor.getColumnIndexSize(0)).isPositive();
        assertThat(dseSSTable.getEstimatedPartitionSize().max()).isGreaterThan(DatabaseDescriptor.getColumnIndexSize(0));
        long nowInSec = FBUtilities.nowInSeconds();
        int rows = 0;
        try (ISSTableScanner scanner = dseSSTable.getScanner())
        {
            while (scanner.hasNext())
            {
                try (UnfilteredRowIterator partition = scanner.next())
                {
                    while (partition.hasNext())
                    {
                        Row row = (Row) partition.next();
                        assertThat(row.getCell(hidden)).isNull();
                        String v = UTF8Type.instance.compose(partition.partitionKey().getKey());
                        int ck = Int32Type.instance.compose(row.clustering().bufferAt(1));
                        boolean withoutHiddenCell = v.equals("e") || (v.equals("m") && ck % 3 == 1);
                        assertThat(row.primaryKeyLivenessInfo().isLive(nowInSec)).isEqualTo(!withoutHiddenCell && !v.equals("d"));
                        if (withoutHiddenCell)
                            assertThat(row.primaryKeyLivenessInfo()).isEqualTo(DseViewHiddenColumns.LIVENESS_WITHOUT_HIDDEN_CELL);
                        else
                            assertThat(row.primaryKeyLivenessInfo().timestamp()).isEqualTo(10);
                        rows++;
                    }
                }
            }
        }
        assertThat(rows).isEqualTo(5 + WIDE_ROWS);

        String select = "SELECT v, pk, ck, s, w FROM " + KEYSPACE + '.' + view;
        Object[][] live = { row("a", 1, 1, set(1, 2), 1), row("b", 1, 2, null, 2), row("c", 1, 3, set(3), null) };
        assertRowsIgnoringOrder(execute(select + " WHERE v IN ('a', 'b', 'c', 'd', 'e')"), live);
        assertRows(execute(select + " WHERE v = 'b' AND pk = 1 AND ck = 2"), row("b", 1, 2, null, 2));
        assertEmpty(execute(select + " WHERE v = 'd' AND pk = 1 AND ck = 4"));
        assertEmpty(execute(select + " WHERE v = 'e' AND pk = 1 AND ck = 5"));
        assertWidePartition(select);

        // a partial stream of the sstable writes the row without the hidden cell with its liveness, and reads the same
        boolean entireBefore = DatabaseDescriptor.streamEntireSSTables();
        try
        {
            DatabaseDescriptor.setStreamEntireSSTables(false);
            SSTableReader partial = streamToSelf(viewStore, dseSSTable);
            assertThat(partial.header.dseViewLivenessColumn()).isNull();
            assertThat(partial.getMinTimestamp()).isEqualTo(LivenessInfo.NO_TIMESTAMP + 1);
            int withoutHiddenCell = 0;
            try (ISSTableScanner scanner = partial.getScanner())
            {
                while (scanner.hasNext())
                {
                    try (UnfilteredRowIterator partition = scanner.next())
                    {
                        while (partition.hasNext())
                        {
                            Row row = (Row) partition.next();
                            String v = UTF8Type.instance.compose(partition.partitionKey().getKey());
                            int ck = Int32Type.instance.compose(row.clustering().bufferAt(1));
                            if (v.equals("e") || (v.equals("m") && ck % 3 == 1))
                            {
                                assertThat(row.primaryKeyLivenessInfo()).isEqualTo(DseViewHiddenColumns.LIVENESS_WITHOUT_HIDDEN_CELL);
                                assertThat(row.primaryKeyLivenessInfo().localExpirationTime()).isEqualTo(EncodingStats.NO_STATS.minLocalDeletionTime);
                                withoutHiddenCell++;
                            }
                        }
                    }
                }
            }
            assertThat(withoutHiddenCell).isEqualTo(1 + WIDE_ROWS / 3);
        }
        finally
        {
            DatabaseDescriptor.setStreamEntireSSTables(entireBefore);
        }
        assertRowsIgnoringOrder(execute(select + " WHERE v IN ('a', 'b', 'c', 'd', 'e')"), live);
        assertWidePartition(select);

        // compaction writes the other columns and the liveness the hidden cells gave, without the hidden column
        viewStore.forceMajorCompaction();
        assertThat(viewStore.getLiveSSTables()).hasSize(1);
        SSTableReader compacted = viewStore.getLiveSSTables().iterator().next();
        assertThat(compacted.descriptor).isNotEqualTo(dseSSTable.descriptor);
        assertThat(compacted.header.dseViewLivenessColumn()).isNull();
        assertThat(compacted.header.columns().regulars.size()).isEqualTo(2);
        assertThat(compacted.header.columns().regulars.contains(hidden)).isFalse();
        // the liveness of the row without the hidden cell is purged, and the row with it
        assertThat(compacted.getMinTimestamp()).isPositive();
        assertRowsIgnoringOrder(execute(select + " WHERE v IN ('a', 'b', 'c', 'd', 'e')"), live);
        assertWidePartition(select);
    }

    private static final int WIDE_ROWS = 300;

    /**
     * A row without the hidden cell in one sstable and the live hidden cell of the same row in another: the merged row
     * is live, with the cells of both, as DSE reads it. A compaction of the sstable without the hidden cell of row x
     * alone purges the row (which DSE does not write: it writes the hidden cell in every update of a live view entry, as
     * {@link DseViewUpdatesFixtureTest} shows): its cells are gone, and the row stays live through the other sstable.
     */
    @Test
    public void aRowWithoutTheHiddenCellIsLiveThroughAnotherSource() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, v text, w int, PRIMARY KEY (pk, ck))");
        String view = createView("CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                                 "WHERE v IS NOT NULL AND pk IS NOT NULL AND ck IS NOT NULL PRIMARY KEY (v, pk, ck)");
        ColumnFamilyStore viewStore = getColumnFamilyStore(KEYSPACE, view);
        viewStore.disableAutoCompaction();
        String dse = createTable("CREATE TABLE %s (v text, pk int, ck int, \".v\" text, w int, PRIMARY KEY (v, pk, ck))");
        ColumnFamilyStore dseStore = getColumnFamilyStore(KEYSPACE, dse);
        dseStore.disableAutoCompaction();

        execute("UPDATE %s USING TIMESTAMP 10 SET \".v\" = '' WHERE v = 'x' AND pk = 1 AND ck = 1");
        flush(KEYSPACE, dse);
        execute("INSERT INTO %s (v, pk, ck, w) VALUES ('x', 1, 1, 7) USING TIMESTAMP 20");
        // another row of the same sstable has a hidden cell, so that its header names the column
        execute("UPDATE %s USING TIMESTAMP 20 SET \".v\" = '' WHERE v = 'y' AND pk = 1 AND ck = 1");
        flush(KEYSPACE, dse);
        List<Path> files = new ArrayList<>();
        for (SSTableReader sstable : dseStore.getLiveSSTables())
            for (Component component : sstable.components())
                files.add(sstable.descriptor.fileFor(component).toPath());
        importInto(view, files);

        String select = "SELECT v, pk, ck, w FROM " + KEYSPACE + '.' + view;
        assertRows(execute(select + " WHERE v = 'x'"), row("x", 1, 1, 7));

        SSTableReader withoutHiddenCell = null;
        for (SSTableReader sstable : viewStore.getLiveSSTables())
            if (sstable.getMaxTimestamp() == 20)
                withoutHiddenCell = sstable;
        assertThat(withoutHiddenCell).isNotNull();
        CompactionManager.instance.submitUserDefined(viewStore, Collections.singleton(withoutHiddenCell.descriptor), FBUtilities.nowInSeconds()).get();
        assertThat(viewStore.getLiveSSTables()).doesNotContain(withoutHiddenCell);
        assertRows(execute(select + " WHERE v = 'x'"), row("x", 1, 1, null));
    }

    /**
     * The rows of partition 'm' of {@link #hiddenCellAmongOtherRegularColumns}: those with a hidden cell, read forwards,
     * backwards, by slice and by name.
     */
    private void assertWidePartition(String select) throws Throwable
    {
        List<Object[]> forward = new ArrayList<>();
        for (int ck = 0; ck < WIDE_ROWS; ck++)
            if (ck % 3 != 1)
                forward.add(row("m", 1, ck, set(-1, ck), ck));
        assertRows(execute(select + " WHERE v = 'm'"), forward.toArray(new Object[0][]));
        List<Object[]> backward = new ArrayList<>(forward);
        Collections.reverse(backward);
        assertRows(execute(select + " WHERE v = 'm' ORDER BY pk DESC, ck DESC"), backward.toArray(new Object[0][]));
        assertRows(execute(select + " WHERE v = 'm' AND pk = 1 AND ck >= 99 AND ck <= 103"),
                   row("m", 1, 99, set(-1, 99), 99), row("m", 1, 101, set(-1, 101), 101), row("m", 1, 102, set(-1, 102), 102));
        assertRows(execute(select + " WHERE v = 'm' AND pk = 1 AND ck IN (150, 151, 152)"),
                   row("m", 1, 150, set(-1, 150), 150), row("m", 1, 152, set(-1, 152), 152));
        assertRows(execute("SELECT count(*) FROM " + select.substring(select.indexOf(" FROM ") + 6) + " WHERE v = 'm'"), row((long) forward.size()));
    }

    /**
     * Rows as DSE writes them for a view whose primary key holds a regular base column: the hidden cell decides
     * the liveness, the primary key liveness does not. The sstables are written here through a table with the
     * view's primary key and a regular column named {@code .v}, then imported into the view.
     */
    @Test
    public void hiddenCellDecidesTheLivenessOfTheRow() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, v text, PRIMARY KEY (pk, ck))");
        String view = createView(VIEW_QUERY);
        getColumnFamilyStore(KEYSPACE, view).disableAutoCompaction();
        String dse = createTable("CREATE TABLE %s (v text, pk int, ck int, \".v\" text, PRIMARY KEY (v, pk, ck))");
        getColumnFamilyStore(KEYSPACE, dse).disableAutoCompaction();

        // live hidden cell, no primary key liveness (DSE's entry for a base row written by UPDATE): live
        execute("UPDATE %s USING TIMESTAMP 10 SET \".v\" = '' WHERE v = 'a' AND pk = 1 AND ck = 1");
        // primary key liveness and a hidden tombstone (DSE's old entry after the base cell changed): not live
        execute("INSERT INTO %s (v, pk, ck) VALUES ('b', 1, 2) USING TIMESTAMP 10");
        execute("DELETE \".v\" FROM %s USING TIMESTAMP 10 WHERE v = 'b' AND pk = 1 AND ck = 2");
        // primary key liveness and a live hidden cell (DSE's entry for a base row written by INSERT): live
        execute("INSERT INTO %s (v, pk, ck, \".v\") VALUES ('c', 1, 3, '') USING TIMESTAMP 10");
        // primary key liveness and no hidden cell (DSE's entry when one base update deletes the base row and inserts
        // it again without v): not live, DSE ignores the primary key liveness
        execute("INSERT INTO %s (v, pk, ck) VALUES ('d', 1, 4) USING TIMESTAMP 10");
        // a row deletion older than the hidden cell, which shadows the primary key liveness: live
        execute("INSERT INTO %s (v, pk, ck) VALUES ('f', 1, 6) USING TIMESTAMP 3");
        execute("DELETE FROM %s USING TIMESTAMP 5 WHERE v = 'f' AND pk = 1 AND ck = 6");
        execute("UPDATE %s USING TIMESTAMP 10 SET \".v\" = '' WHERE v = 'f' AND pk = 1 AND ck = 6");
        // a row deletion newer than the hidden cell (DSE's entry when the base row is deleted): not live
        execute("INSERT INTO %s (v, pk, ck, \".v\") VALUES ('j', 1, 10, '') USING TIMESTAMP 10");
        execute("DELETE FROM %s USING TIMESTAMP 20 WHERE v = 'j' AND pk = 1 AND ck = 10");
        // primary key liveness without TTL and an expired hidden cell (base cell written with a TTL, expired): not live
        execute("INSERT INTO %s (v, pk, ck) VALUES ('e', 1, 5) USING TIMESTAMP 10");
        execute("UPDATE %s USING TIMESTAMP 10 AND TTL 1 SET \".v\" = '' WHERE v = 'e' AND pk = 1 AND ck = 5");
        // the cell was written by this second, so it has expired once the second after the next one starts
        long expiringWrittenBy = FBUtilities.nowInSeconds();
        // the same with a hidden cell that has not expired yet: live
        execute("INSERT INTO %s (v, pk, ck) VALUES ('i', 1, 9) USING TIMESTAMP 10");
        execute("UPDATE %s USING TIMESTAMP 10 AND TTL 3600 SET \".v\" = '' WHERE v = 'i' AND pk = 1 AND ck = 9");
        // two sstables: a live hidden cell then a newer hidden tombstone (not live), and a hidden tombstone then a
        // newer live hidden cell (live)
        execute("UPDATE %s USING TIMESTAMP 10 SET \".v\" = '' WHERE v = 'g' AND pk = 1 AND ck = 7");
        execute("INSERT INTO %s (v, pk, ck) VALUES ('h', 1, 8) USING TIMESTAMP 10");
        execute("DELETE \".v\" FROM %s USING TIMESTAMP 10 WHERE v = 'h' AND pk = 1 AND ck = 8");
        execute("INSERT INTO %s (v, pk, ck, \".v\") VALUES ('k', 1, 11, '') USING TIMESTAMP 10");
        flush(KEYSPACE, dse);
        execute("DELETE \".v\" FROM %s USING TIMESTAMP 20 WHERE v = 'g' AND pk = 1 AND ck = 7");
        execute("UPDATE %s USING TIMESTAMP 20 SET \".v\" = '' WHERE v = 'h' AND pk = 1 AND ck = 8");
        // and a live hidden cell with its primary key liveness, then a newer row deletion in the other sstable: not live
        execute("DELETE FROM %s USING TIMESTAMP 20 WHERE v = 'k' AND pk = 1 AND ck = 11");
        flush(KEYSPACE, dse);
        while (FBUtilities.nowInSeconds() < expiringWrittenBy + 2)
            Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);

        ColumnFamilyStore dseStore = getColumnFamilyStore(KEYSPACE, dse);
        assertThat(dseStore.getLiveSSTables()).hasSize(2);
        List<Path> files = new ArrayList<>();
        for (SSTableReader sstable : dseStore.getLiveSSTables())
            for (Component component : sstable.components())
                files.add(sstable.descriptor.fileFor(component).toPath());
        importInto(view, files);

        String select = "SELECT v, pk, ck FROM " + KEYSPACE + '.' + view;
        Object[][] live = { row("a", 1, 1), row("c", 1, 3), row("f", 1, 6), row("h", 1, 8), row("i", 1, 9) };
        assertRowsIgnoringOrder(execute(select), live);
        // single-partition reads, also merging the two sstables
        assertEmpty(execute(select + " WHERE v = 'b' AND pk = 1 AND ck = 2"));
        assertEmpty(execute(select + " WHERE v = 'd' AND pk = 1 AND ck = 4"));
        assertEmpty(execute(select + " WHERE v = 'g' AND pk = 1 AND ck = 7"));
        assertEmpty(execute(select + " WHERE v = 'j' AND pk = 1 AND ck = 10"));
        assertEmpty(execute(select + " WHERE v = 'k' AND pk = 1 AND ck = 11"));
        assertRows(execute(select + " WHERE v = 'h' AND pk = 1 AND ck = 8"), row("h", 1, 8));
        assertRows(execute(select + " WHERE v = 'h' ORDER BY pk DESC, ck DESC"), row("h", 1, 8));

        // the same answer once compaction has merged the two sstables into one without hidden cells (scrub is covered
        // by scrubRewritesTheDseViewSSTableWithoutTheHiddenColumn)
        ColumnFamilyStore viewStore = getColumnFamilyStore(KEYSPACE, view);
        viewStore.forceMajorCompaction();
        assertThat(viewStore.getLiveSSTables()).hasSize(1);
        assertThat(viewStore.getLiveSSTables().iterator().next().getSSTableMetadata().totalColumnsSet).isZero();
        assertRowsIgnoringOrder(execute(select), live);
    }

    /**
     * Streaming the DSE view sstable: a partial stream is written with a header without the hidden column and rows
     * with the liveness their hidden cells gave them; an entire-sstable stream copies the DSE sstable as it is.
     */
    @Test
    public void streamingTheDseViewSSTable() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, v text, PRIMARY KEY (pk, ck))");
        String view = createView(VIEW_QUERY);
        ColumnFamilyStore viewStore = getColumnFamilyStore(KEYSPACE, view);
        viewStore.disableAutoCompaction();
        importInto(view, DseFixtures.viewSSTableComponents());
        SSTableReader dseSSTable = viewStore.getLiveSSTables().iterator().next();
        Object[][] expected = DseFixtures.viewSelectStar().toArray(new Object[0][]);

        boolean entireBefore = DatabaseDescriptor.streamEntireSSTables();
        try
        {
            DatabaseDescriptor.setStreamEntireSSTables(false);
            SSTableReader partial = streamToSelf(viewStore, dseSSTable);
            assertThat(partial.header.dseViewLivenessColumn()).isNull();
            assertThat(partial.header.columns().regulars.isEmpty()).isTrue();
            assertThat(partial.getSSTableMetadata().totalColumnsSet).isZero();
            assertThat(countLiveRows(partial)).isEqualTo(5000);

            DatabaseDescriptor.setStreamEntireSSTables(true);
            SSTableReader entire = streamToSelf(viewStore, dseSSTable);
            assertThat(entire.header.dseViewLivenessColumn()).isNotNull();
            assertThat(entire.getSSTableMetadata().totalColumnsSet).isEqualTo(5000);
            assertThat(countLiveRows(entire)).isEqualTo(5000);
        }
        finally
        {
            DatabaseDescriptor.setStreamEntireSSTables(entireBefore);
        }

        assertThat(viewStore.getLiveSSTables()).hasSize(3);
        assertRows(execute("SELECT * FROM " + KEYSPACE + '.' + view), expected);
    }

    /** Streams the whole token range of {@code sstable} to this node and returns the sstable it received. */
    private static SSTableReader streamToSelf(ColumnFamilyStore store, SSTableReader sstable) throws Exception
    {
        Set<SSTableReader> before = new HashSet<>(store.getLiveSSTables());
        IPartitioner partitioner = sstable.getPartitioner();
        List<Range<Token>> ranges = Collections.singletonList(new Range<>(partitioner.getMinimumToken(), partitioner.getMinimumToken()));
        // the stream releases the reference once it is sent
        Refs<SSTableReader> refs = Refs.tryRef(Collections.singletonList(sstable));
        assertThat(refs).isNotNull();
        OutgoingStream stream = new CassandraOutgoingFile(StreamOperation.OTHER, refs.get(sstable), sstable.getPositionsForRanges(ranges),
                                                          ranges, sstable.estimatedKeysForRanges(ranges));
        new StreamPlan(StreamOperation.OTHER).transferStreams(FBUtilities.getBroadcastAddressAndPort(), Collections.singletonList(stream))
                                             .execute().get();
        Set<SSTableReader> received = new HashSet<>(store.getLiveSSTables());
        received.removeAll(before);
        assertThat(received).hasSize(1);
        return received.iterator().next();
    }

    /** The rows of {@code sstable} that are live on their own, with the strict liveness of the view. */
    private static int countLiveRows(SSTableReader sstable)
    {
        long nowInSec = FBUtilities.nowInSeconds();
        int liveRows = 0;
        try (ISSTableScanner scanner = sstable.getScanner())
        {
            while (scanner.hasNext())
            {
                try (UnfilteredRowIterator partition = scanner.next())
                {
                    while (partition.hasNext())
                    {
                        if (((Row) partition.next()).hasLiveData(nowInSec, true))
                            liveRows++;
                    }
                }
            }
        }
        return liveRows;
    }

    @Test
    public void livenessOfTheHiddenCellIsTheOneThisSoftwareWritesForTheBaseCell()
    {
        ColumnMetadata hidden = ColumnMetadata.regularColumn("ks", "view", ".v", UTF8Type.instance);

        LivenessInfo live = DseViewHiddenColumns.livenessOf(BufferCell.live(hidden, 10, ByteBufferUtil.EMPTY_BYTE_BUFFER));
        assertThat(live).isEqualTo(LivenessInfo.withExpirationTime(10, LivenessInfo.NO_TTL, LivenessInfo.NO_EXPIRATION_TIME));
        assertThat(live.isLive(FBUtilities.nowInSeconds())).isTrue();

        long expiresAt = FBUtilities.nowInSeconds() + 100;
        Cell<?> expiring = BufferCell.expiring(hidden, 10, 100, expiresAt - 100, ByteBufferUtil.EMPTY_BYTE_BUFFER);
        LivenessInfo expiringLiveness = DseViewHiddenColumns.livenessOf(expiring);
        assertThat(expiringLiveness).isEqualTo(LivenessInfo.withExpirationTime(10, 100, expiresAt));
        assertThat(expiringLiveness.isLive(expiresAt - 1)).isTrue();
        assertThat(expiringLiveness.isLive(expiresAt)).isFalse();

        long deletedAt = FBUtilities.nowInSeconds();
        LivenessInfo deleted = DseViewHiddenColumns.livenessOf(BufferCell.tombstone(hidden, 10, deletedAt));
        assertThat(deleted).isEqualTo(LivenessInfo.withExpirationTime(10, LivenessInfo.EXPIRED_LIVENESS_TTL, deletedAt));
        assertThat(deleted.isLive(deletedAt - 1)).isFalse();
        // it wins over a live liveness of the same timestamp, as the hidden tombstone wins over the live hidden cell
        assertThat(deleted.supersedes(live)).isTrue();
    }

    @Test
    public void otherHiddenColumnsAreRefusedWithTheRemedy() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, v text, w int, PRIMARY KEY (pk, ck))");
        String view = createView(VIEW_QUERY);
        TableMetadata viewMetadata = getColumnFamilyStore(KEYSPACE, view).metadata();
        String viewName = KEYSPACE + '.' + view;
        String remedy = "Drop the view on DSE before the upgrade and create it again once no node runs DSE; " +
                        "a node already running this software must first be rolled back to DSE";

        // a hidden column for a base column the view does not have in its primary key (unselected or filtered)
        assertThatThrownBy(() -> headerFor(viewMetadata, ImmutableMap.of(".w", Int32Type.instance)).toHeader("sstable-w", viewMetadata, sstableVersion(), false))
        .isInstanceOf(UnknownColumnException.class)
        .hasMessage("The sstable header of sstable-w, for materialized view " + viewName + ", holds the DSE hidden column .w for the base " +
                    "column w, which is not a regular base column in the view primary key; this software cannot reproduce the liveness " +
                    "DSE gives such columns and refuses the sstable rather than return wrong view rows. " + remedy);

        // a hidden column for a view primary key column that is a primary key column of the base table: DSE never writes it
        assertThatThrownBy(() -> headerFor(viewMetadata, ImmutableMap.of(".pk", Int32Type.instance)).toHeader("sstable-pk", viewMetadata, sstableVersion(), false))
        .isInstanceOf(UnknownColumnException.class)
        .hasMessage("The sstable header of sstable-pk, for materialized view " + viewName + ", holds the DSE hidden column .pk for the base " +
                    "column pk, which is not a regular base column in the view primary key; this software cannot reproduce the liveness " +
                    "DSE gives such columns and refuses the sstable rather than return wrong view rows. " + remedy);

        // a hidden column whose type is not the one of the view column
        assertThatThrownBy(() -> headerFor(viewMetadata, ImmutableMap.of(".v", Int32Type.instance)).toHeader("sstable-v-int", viewMetadata, sstableVersion(), false))
        .isInstanceOf(UnknownColumnException.class)
        .hasMessage("The sstable header of sstable-v-int, for materialized view " + viewName + ", holds the DSE hidden column .v of type int, " +
                    "but the view column v has type text: the sstable does not belong to this view");

        // two hidden columns for view primary key columns (a DSE view with two regular base columns in its primary key,
        // seen by an offline tool, where the base table is unknown and DSE's rule decides)
        TableMetadata offlineView = TableMetadata.builder(KEYSPACE, "offline_" + view, TableId.generate())
                                                 .kind(TableMetadata.Kind.VIEW)
                                                 .addPartitionKeyColumn("v", UTF8Type.instance)
                                                 .addClusteringColumn("pk", Int32Type.instance)
                                                 .addClusteringColumn("ck", Int32Type.instance)
                                                 .build();
        String offlineViewName = KEYSPACE + ".offline_" + view;
        assertThatThrownBy(() -> headerFor(offlineView, ImmutableMap.of(".pk", Int32Type.instance, ".v", UTF8Type.instance))
                                 .toHeader("sstable-pk-v", offlineView, sstableVersion(), false))
        .isInstanceOf(UnknownColumnException.class)
        .hasMessage("The sstable header of sstable-pk-v, for materialized view " + offlineViewName + ", holds the DSE hidden columns .pk and .v: " +
                    "the view has more than one regular base column in its primary key, which this software does not support. " + remedy);

        // in a view, an unknown column without the leading '.', a '.' alone and a static '.v' stay unknown columns
        assertThatThrownBy(() -> headerFor(viewMetadata, ImmutableMap.of("x", UTF8Type.instance)).toHeader("sstable-x", viewMetadata, sstableVersion(), false))
        .isInstanceOf(UnknownColumnException.class)
        .hasMessage("Unknown column x during deserialization");
        assertThatThrownBy(() -> headerFor(viewMetadata, ImmutableMap.of(".", UTF8Type.instance)).toHeader("sstable-dot", viewMetadata, sstableVersion(), false))
        .isInstanceOf(UnknownColumnException.class)
        .hasMessage("Unknown column . during deserialization");
        SerializationHeader.Component withStatic = SerializationHeader.makeWithoutStats(viewMetadata.unbuild().addStaticColumn(".v", UTF8Type.instance).build()).toComponent();
        assertThatThrownBy(() -> withStatic.toHeader("sstable-static", viewMetadata, sstableVersion(), false))
        .isInstanceOf(UnknownColumnException.class)
        .hasMessage("Unknown column .v during deserialization");

        // outside of a view, a column named like a hidden one stays an unknown column, as before
        TableMetadata base = getCurrentColumnFamilyStore().metadata();
        assertThatThrownBy(() -> headerFor(base, ImmutableMap.of(".v", UTF8Type.instance)).toHeader("sstable-base", base, sstableVersion(), false))
        .isInstanceOf(UnknownColumnException.class)
        .hasMessage("Unknown column .v during deserialization");

        // the hidden column of the regular base column in the view primary key is read, and left out of the headers
        // this software writes
        SerializationHeader header = headerFor(viewMetadata, ImmutableMap.of(".v", UTF8Type.instance)).toHeader("sstable-v", viewMetadata, sstableVersion(), false);
        assertThat(header.dseViewLivenessColumn().name.toString()).isEqualTo(".v");
        assertThat(header.columns().regulars.contains(header.dseViewLivenessColumn())).isTrue();
        SerializationHeader written = header.withoutDseViewLivenessColumn();
        assertThat(written.dseViewLivenessColumn()).isNull();
        assertThat(written.columns().regulars.isEmpty()).isTrue();
        assertThat(written.withoutDseViewLivenessColumn()).isSameAs(written);
    }

    /** The serialization header component of an sstable of {@code metadata} with the given extra regular columns. */
    private static SerializationHeader.Component headerFor(TableMetadata metadata, Map<String, AbstractType<?>> extraColumns)
    {
        TableMetadata.Builder builder = metadata.unbuild();
        extraColumns.forEach(builder::addRegularColumn);
        return SerializationHeader.makeWithoutStats(builder.build()).toComponent();
    }

    private static Version sstableVersion()
    {
        return DatabaseDescriptor.getSelectedSSTableFormat().getLatestVersion();
    }

    /** Imports the given sstable files into the table or view {@code table}, verifying them first. */
    private void importInto(String table, Collection<Path> files) throws IOException
    {
        ColumnFamilyStore store = getColumnFamilyStore(KEYSPACE, table);
        Path dir = Files.createDirectories(importRoot.resolve(KEYSPACE).resolve(table + '-' + store.metadata.id.toHexString()));
        for (Path file : files)
            Files.copy(file, dir.resolve(file.getFileName()));
        // import only places the files, which an upgraded node finds on disk at startup
        List<String> failed = store.importNewSSTables(Collections.singleton(dir.toString()), false, false, true, false, false, true, true);
        assertThat(failed).isEmpty();
    }
}
