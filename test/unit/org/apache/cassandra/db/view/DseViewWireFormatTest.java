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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;

import ch.qos.logback.classic.Level;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.InternodeCompatibilityMode;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Columns;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.Digest;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.SingletonUnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.DeserializationHelper;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.view.DseViewHiddenColumnCatalog.HiddenColumn;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.dsecompat.DseFixtures;
import org.apache.cassandra.exceptions.UnknownColumnException;
import org.apache.cassandra.hints.Hint;
import org.apache.cassandra.hints.HintMessage;
import org.apache.cassandra.hints.HintMessages;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.net.DseCompatRefusals;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.net.MessagingService.VERSION_3014;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The data of a materialized view exchanged with DSE 6 peers at messaging version 11 ({@link DseViewWireFormat}).
 * <p>
 * The HCD 1.2 stand-in of the in-JVM tests has no hidden view columns, so it cannot tell whether this software speaks
 * DSE's encoding of view rows. The DSE side is built here instead: the view's metadata with the hidden column {@code .v}
 * added as a regular column is the view as DSE knows it (DSE lists its hidden columns among the view's regular
 * columns), and the serializers of this software run on it with a catalog listing no hidden column ({@code asDse})
 * write and read exactly the bytes DSE does. Those DSE-shaped messages are then read and answered by this software with
 * a catalog listing the hidden column of the view, as DSE's table lists it.
 * The encoding DSE writes is taken from the DSE 6.8.65 view sstable of the fixture ({@code test/data/dse-compat}), and
 * which views DSE keeps a hidden column for from a {@link DseViewHiddenColumnCatalog} naming it.
 */
public class DseViewWireFormatTest extends CQLTester
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

    private static final String VIEW_QUERY = "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                                             "WHERE v IS NOT NULL AND pk IS NOT NULL AND ck IS NOT NULL PRIMARY KEY (v, pk, ck)";
    private static final String REMEDY = "Drop the view on DSE before the upgrade and create it again once no node runs DSE; " +
                                         "a node already running this software must first be rolled back to DSE";
    /** DSE's hidden column of a regular base column in the view primary key, as system_schema.hidden_columns lists it. */
    private static final HiddenColumn DSE_V = new HiddenColumn(".v", "regular", true, "text");

    private InternodeCompatibilityMode modeBefore;
    private DseViewHiddenColumnCatalog catalogBefore;
    private final Map<Pair<String, String>, List<HiddenColumn>> dseHiddenColumns = new HashMap<>();
    private TableMetadata view;
    /** The view as DSE knows it: {@link #view} with DSE's hidden column {@code .v}. */
    private TableMetadata dseView;
    private ColumnMetadata dseHidden;
    private ColumnMetadata w;
    private ColumnMetadata dseW;

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
        dseView = view.unbuild().addRegularColumn(".v", UTF8Type.instance).build();
        dseHidden = dseView.getColumn(ByteBufferUtil.bytes(".v"));
        w = view.getColumn(ByteBufferUtil.bytes("w"));
        dseW = dseView.getColumn(ByteBufferUtil.bytes("w"));
    }

    @After
    public void restoreModeAndCatalog()
    {
        DseViewHiddenColumnCatalog.setInstance(catalogBefore);
        DatabaseDescriptor.setInternodeCompatibilityMode(modeBefore);
    }

    @Test
    public void theHiddenColumnIsTheOneDseListsForTheView() throws Throwable
    {
        ColumnMetadata hidden = DseViewWireFormat.hiddenColumn(view, VERSION_3014);
        assertThat(hidden).isEqualTo(dseHidden);
        assertThat(hidden.isRegular()).isTrue();
        assertThat(hidden.type).isEqualTo(UTF8Type.instance);

        // not at other versions, not for a table, not outside of the mode
        assertThat(DseViewWireFormat.hiddenColumn(view, MessagingService.current_version)).isNull();
        assertThat(DseViewWireFormat.hiddenColumn(view, MessagingService.VERSION_40)).isNull();
        assertThat(DseViewWireFormat.hiddenColumn(view, MessagingService.VERSION_30)).isNull();
        assertThat(DseViewWireFormat.translates(currentTableMetadata(), VERSION_3014)).isFalse();
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.NONE);
        assertThat(DseViewWireFormat.translates(view, VERSION_3014)).isFalse();
        assertThat(DseViewWireFormat.hiddenColumn(view, VERSION_3014)).isNull();
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);

        // a view DSE's table lists no hidden column for (taken to be of DSE's old format): exchanged as it is
        TableMetadata legacy = getColumnFamilyStore(KEYSPACE, createView(VIEW_QUERY)).metadata();
        assertThat(DseViewWireFormat.translates(legacy, VERSION_3014)).isTrue();
        assertThat(DseViewWireFormat.hiddenColumn(legacy, VERSION_3014)).isNull();

        // a regular base column of another type in the view primary key: the type DSE lists must be the one of the column
        String byW = createView("CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE w IS NOT NULL AND pk IS NOT NULL AND ck IS NOT NULL PRIMARY KEY (w, pk, ck)");
        TableMetadata byWView = getColumnFamilyStore(KEYSPACE, byW).metadata();
        dseHiddenColumns.put(Pair.create(KEYSPACE, byW), List.of(new HiddenColumn(".w", "regular", true, "int")));
        ColumnMetadata hiddenW = DseViewWireFormat.hiddenColumn(byWView, VERSION_3014);
        assertThat(hiddenW.name.toString()).isEqualTo(".w");
        assertThat(hiddenW.type).isEqualTo(Int32Type.instance);
        DseViewHiddenColumnCatalog wrongType = new DseViewHiddenColumnCatalog(Map.of(Pair.create(KEYSPACE, byW), List.of(new HiddenColumn(".w", "regular", true, "text"))));
        assertThatThrownBy(() -> wrongType.translatedColumn(byWView))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageStartingWith("DSE 6 compatibility mode: DSE keeps the hidden columns [.w text (regular, required for liveness)] for materialized view " + KEYSPACE + '.' + byW);

        // a view no longer in the schema: nothing to translate, and nothing kept for a view created again with its name
        TableMetadata dropped = legacy;
        execute("DROP MATERIALIZED VIEW " + KEYSPACE + '.' + dropped.name);
        dseHiddenColumns.put(Pair.create(KEYSPACE, dropped.name), List.of(DSE_V));
        assertThat(DseViewHiddenColumnCatalog.instance().translatedColumn(dropped)).isNull();
        execute(String.format("CREATE MATERIALIZED VIEW %s.%s AS SELECT * FROM %s.%s WHERE v IS NOT NULL AND pk IS NOT NULL AND ck IS NOT NULL PRIMARY KEY (v, pk, ck)",
                              KEYSPACE, dropped.name, KEYSPACE, currentTable()));
        TableMetadata createdAgain = getColumnFamilyStore(KEYSPACE, dropped.name).metadata();
        assertThat(createdAgain.id).isNotEqualTo(dropped.id);
        assertThat(DseViewWireFormat.hiddenColumn(createdAgain, VERSION_3014).name.toString()).isEqualTo(".v");

        // the column carries the type of the base column, whatever the clustering order of the view
        String descending = createView("CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE v IS NOT NULL AND pk IS NOT NULL AND ck IS NOT NULL " +
                                       "PRIMARY KEY (pk, v, ck) WITH CLUSTERING ORDER BY (v DESC, ck ASC)");
        dseHiddenColumns.put(Pair.create(KEYSPACE, descending), List.of(DSE_V));
        ColumnMetadata descendingHidden = DseViewWireFormat.hiddenColumn(getColumnFamilyStore(KEYSPACE, descending).metadata(), VERSION_3014);
        assertThat(descendingHidden.name.toString()).isEqualTo(".v");
        assertThat(descendingHidden.type).isEqualTo(UTF8Type.instance);
    }

    /**
     * A read of the view coordinated by DSE (as a {@code SELECT count(*)} of the view at ALL run on a DSE node): the column filter
     * names {@code .v}, the read runs without it, and the rows of the response carry the {@code .v} cells DSE writes for
     * the rows of this software, with both a wildcard filter and a filter fetching all columns and querying one.
     */
    @Test
    public void readFromDseCoordinatorGetsTheHiddenCells() throws Throwable
    {
        long writtenFrom = FBUtilities.nowInSeconds();
        writeBaseRows();
        long writtenBy = FBUtilities.nowInSeconds();
        List<Row> local = localRows("a");
        assertThat(local).hasSize(4);

        for (ColumnFilter dseFilter : dseColumnFilters())
        {
            SinglePartitionReadCommand dseCommand = dseRead("a", dseFilter, writtenBy);
            ByteBuffer sent = asDse(() -> serialize(ReadCommand.serializer, dseCommand, VERSION_3014));
            if (dseFilter.isWildcard())
            {
                // the read this software sends for the same query, byte for byte
                SinglePartitionReadCommand ours = SinglePartitionReadCommand.create(view, writtenBy, ColumnFilter.all(view), RowFilter.none(), DataLimits.NONE,
                                                                                    viewKey("a"), new ClusteringIndexSliceFilter(Slices.ALL, false));
                assertThat(serialize(ReadCommand.serializer, ours, VERSION_3014)).isEqualTo(sent);
            }
            else
            {
                // this software also queries .v in such a read, which DSE may not do (not compared with DSE's own filter here)
                assertThat(ByteBufferUtil.bytesToHex(sent)).contains(ByteBufferUtil.bytesToHex(ByteBufferUtil.bytes(".v")));
            }

            ReadCommand received = deserialize(ReadCommand.serializer, sent, VERSION_3014);
            assertThat(received.columnFilter().fetchedColumns().regulars).containsExactly(w);
            assertThat(received.columnFilter().queriedColumns().regulars.contains(dseHidden)).isFalse();
            assertThat(received.columnFilter().dseColumnFilter()).isEqualTo(dseFilter);
            // every copy of the command keeps the filter DSE sent
            assertThat(received.withUpdatedLimit(DataLimits.cqlLimits(2)).columnFilter().dseColumnFilter()).isEqualTo(dseFilter);

            ByteBuffer answer = serialize(ReadResponse.serializer, respond(received), VERSION_3014);
            List<Row> dseRows = asDse(() -> rows(deserialize(ReadResponse.serializer, answer, VERSION_3014).makeIterator(dseCommand)));
            assertThat(dseRows).hasSize(local.size());
            for (int i = 0; i < local.size(); i++)
                assertDseRowOf(local.get(i), dseRows.get(i));
        }

        // concretely: the rows written by INSERT, INSERT with a TTL, INSERT then UPDATE of v, and UPDATE
        List<Row> dseRows = dseRowsOf("a", Slices.ALL, false, writtenBy);
        assertHiddenCell(dseRows.get(0), 10, Cell.NO_TTL, Cell.NO_DELETION_TIME, false);
        long expiresAt = dseRows.get(1).getCell(dseHidden).localDeletionTime();
        assertThat(expiresAt).isBetween(writtenFrom + 3600, writtenBy + 3600);
        assertHiddenCell(dseRows.get(1), 11, 3600, expiresAt, false);
        assertHiddenCell(dseRows.get(2), 13, Cell.NO_TTL, Cell.NO_DELETION_TIME, false);
        assertHiddenCell(dseRows.get(3), 14, Cell.NO_TTL, Cell.NO_DELETION_TIME, false);
        assertThat(dseRows.get(0).primaryKeyLivenessInfo()).isEqualTo(LivenessInfo.create(10, writtenBy));
        // the entry the UPDATE of v left behind: DSE's hidden tombstone at the timestamp of the old base cell, deleted when
        // the UPDATE ran, next to a live liveness at that timestamp, as DSE holds the old entry of a base row written by INSERT
        List<Row> moved = dseRowsOf("b", Slices.ALL, false, writtenBy);
        assertThat(moved).hasSize(1);
        long deletedAt = moved.get(0).getCell(dseHidden).localDeletionTime();
        assertThat(deletedAt).isBetween(writtenFrom, writtenBy);
        assertHiddenCell(moved.get(0), 12, Cell.NO_TTL, deletedAt, true);
        assertThat(moved.get(0).primaryKeyLivenessInfo()).isEqualTo(LivenessInfo.create(12, writtenBy));

        // a reversed read of a slice with a limit (a page of DSE's paging)
        Slices fromSecondRow = Slices.with(view.comparator, Slice.make(ClusteringBound.inclusiveStartOf(clustering(1, 2)), ClusteringBound.TOP));
        List<Row> reversed = dseRowsOf("a", fromSecondRow, true, writtenBy);
        assertThat(reversed).hasSize(3);
        assertHiddenCell(reversed.get(0), 14, Cell.NO_TTL, Cell.NO_DELETION_TIME, false);
        assertHiddenCell(reversed.get(2), 11, 3600, expiresAt, false);
    }

    /** A range read of the view coordinated by DSE, and the one this software sends to a DSE replica. */
    @Test
    public void rangeReadsCarryTheHiddenColumn() throws Throwable
    {
        writeBaseRows();
        long nowInSec = FBUtilities.nowInSeconds();
        PartitionRangeReadCommand dseCommand = PartitionRangeReadCommand.create(dseView, nowInSec, ColumnFilter.all(dseView), RowFilter.none(), DataLimits.NONE,
                                                                                DataRange.allData(dseView.partitioner));
        ByteBuffer sent = asDse(() -> serialize(ReadCommand.serializer, dseCommand, VERSION_3014));
        ReadCommand received = deserialize(ReadCommand.serializer, sent, VERSION_3014);
        assertThat(received.columnFilter().dseColumnFilter()).isEqualTo(ColumnFilter.all(dseView));
        ByteBuffer answer = serialize(ReadResponse.serializer, respond(received), VERSION_3014);
        List<Row> dseRows = asDse(() -> rows(deserialize(ReadResponse.serializer, answer, VERSION_3014).makeIterator(dseCommand)));
        List<Row> local = new ArrayList<>(localRows("a"));
        local.addAll(localRows("b"));
        assertThat(dseRows).hasSameSizeAs(local);
        for (Row dseRow : dseRows)
            assertThat(dseRow.getCell(dseHidden)).isNotNull();

        // this software's range read is the one DSE sends for the same range
        PartitionRangeReadCommand ours = PartitionRangeReadCommand.create(view, nowInSec, ColumnFilter.all(view), RowFilter.none(), DataLimits.NONE,
                                                                          DataRange.allData(view.partitioner));
        assertThat(serialize(ReadCommand.serializer, ours, VERSION_3014)).isEqualTo(sent);
    }

    /** What this software sends for the rows of the DSE 6.8.65 view sstable is what that sstable holds, cell for cell. */
    @Test
    public void rowsOfTheDseFixtureGoBackToDseAsDseWroteThem() throws Throwable
    {
        Path importRoot = Files.createTempDirectory("dse-view-wire-format");
        try
        {
            createTable("CREATE TABLE %s (pk int, ck int, v text, PRIMARY KEY (pk, ck))");
            String fixtureView = createView("CREATE MATERIALIZED VIEW %s AS SELECT pk, ck, v FROM %s " +
                                            "WHERE v IS NOT NULL AND pk IS NOT NULL AND ck IS NOT NULL PRIMARY KEY (v, pk, ck)");
            dseHiddenColumns.put(Pair.create(KEYSPACE, fixtureView), List.of(DSE_V));
            String raw = createTable("CREATE TABLE %s (v text, pk int, ck int, \".v\" text, PRIMARY KEY (v, pk, ck))");
            importInto(importRoot, fixtureView, DseFixtures.viewSSTableComponents());
            importInto(importRoot, raw, DseFixtures.viewSSTableComponents());

            TableMetadata fixtureViewMetadata = getColumnFamilyStore(KEYSPACE, fixtureView).metadata();
            TableMetadata dseFixtureView = fixtureViewMetadata.unbuild().addRegularColumn(".v", UTF8Type.instance).build();
            long nowInSec = FBUtilities.nowInSeconds();
            DecoratedKey key = fixtureViewMetadata.partitioner.decorateKey(ByteBufferUtil.bytes("v3"));

            SinglePartitionReadCommand dseCommand = SinglePartitionReadCommand.create(dseFixtureView, nowInSec, key, ColumnFilter.all(dseFixtureView),
                                                                                      new ClusteringIndexSliceFilter(Slices.ALL, false));
            ByteBuffer sent = asDse(() -> serialize(ReadCommand.serializer, dseCommand, VERSION_3014));
            ByteBuffer answer = serialize(ReadResponse.serializer, respond(deserialize(ReadCommand.serializer, sent, VERSION_3014)), VERSION_3014);
            List<Row> sentBack = asDse(() -> rows(deserialize(ReadResponse.serializer, answer, VERSION_3014).makeIterator(dseCommand)));

            TableMetadata rawMetadata = getColumnFamilyStore(KEYSPACE, raw).metadata();
            SinglePartitionReadCommand rawRead = SinglePartitionReadCommand.create(rawMetadata, nowInSec, rawMetadata.partitioner.decorateKey(ByteBufferUtil.bytes("v3")),
                                                                                   ColumnFilter.all(rawMetadata), new ClusteringIndexSliceFilter(Slices.ALL, false));
            List<Row> asWritten = rows(respond(rawRead).makeIterator(rawRead));
            // the rows of v3 in DSE's own answer to SELECT * on the view
            long v3Rows = DseFixtures.viewSelectStar().stream().filter(row -> "v3".equals(row[0])).count();
            assertThat(asWritten).hasSize((int) v3Rows);
            assertThat(sentBack).hasSameSizeAs(asWritten);
            ColumnMetadata rawHidden = rawMetadata.getColumn(ByteBufferUtil.bytes(".v"));
            ColumnMetadata sentHidden = dseFixtureView.getColumn(ByteBufferUtil.bytes(".v"));
            for (int i = 0; i < asWritten.size(); i++)
            {
                Row expected = asWritten.get(i);
                Row actual = sentBack.get(i);
                assertThat(actual.clustering().bufferAt(0)).isEqualTo(expected.clustering().bufferAt(0));
                assertThat(actual.clustering().bufferAt(1)).isEqualTo(expected.clustering().bufferAt(1));
                assertThat(actual.primaryKeyLivenessInfo()).isEqualTo(expected.primaryKeyLivenessInfo());
                assertThat(actual.deletion()).isEqualTo(expected.deletion());
                assertThat(actual.columnCount()).isEqualTo(expected.columnCount());
                Cell<?> expectedCell = expected.getCell(rawHidden);
                Cell<?> actualCell = actual.getCell(sentHidden);
                assertThat(actualCell.buffer()).isEqualTo(expectedCell.buffer());
                assertThat(actualCell.timestamp()).isEqualTo(expectedCell.timestamp());
                assertThat(actualCell.ttl()).isEqualTo(expectedCell.ttl());
                assertThat(actualCell.localDeletionTime()).isEqualTo(expectedCell.localDeletionTime());
            }
        }
        finally
        {
            FileUtils.deleteRecursive(new File(importRoot));
        }
    }

    /**
     * View updates from a DSE base replica (and hints, batched mutations and read-repair writes, which carry the same
     * mutation): each row gets the liveness of its {@code .v} cell, as for DSE's view sstables, whatever its primary key
     * liveness, and the column never reaches the view.
     */
    @Test
    public void mutationFromDseGivesRowsTheLivenessOfTheirHiddenCells() throws Throwable
    {
        long nowInSec = FBUtilities.nowInSeconds();
        PartitionUpdate.Builder builder = PartitionUpdate.builder(dseView, viewKey("a"), dseView.regularAndStaticColumns(), 5);
        // a live hidden cell and no primary key liveness (the entry of a base row written by UPDATE): live
        builder.add(dseRow(1, null, BufferCell.live(dseHidden, 10, ByteBufferUtil.EMPTY_BYTE_BUFFER), null));
        // primary key liveness and a hidden tombstone (the old entry after the base cell changed): not live
        builder.add(dseRow(2, LivenessInfo.create(10, nowInSec), BufferCell.tombstone(dseHidden, 10, nowInSec), null));
        // primary key liveness, a live hidden cell and a regular cell (the entry of a base row written by INSERT): live
        builder.add(dseRow(3, LivenessInfo.create(10, nowInSec), BufferCell.live(dseHidden, 10, ByteBufferUtil.EMPTY_BYTE_BUFFER),
                           BufferCell.live(dseW, 10, Int32Type.instance.decompose(7))));
        // primary key liveness and no hidden cell: not live, DSE ignores the primary key liveness
        builder.add(dseRow(4, LivenessInfo.create(10, nowInSec), null, null));
        // an expiring hidden cell (a base cell written with a TTL): live until it expires
        builder.add(dseRow(5, LivenessInfo.create(10, nowInSec), BufferCell.expiring(dseHidden, 10, 3600, nowInSec, ByteBufferUtil.EMPTY_BYTE_BUFFER), null));
        Mutation dseMutation = new Mutation(builder.build());

        ByteBuffer sent = asDse(() -> serialize(Mutation.serializer, dseMutation, VERSION_3014));
        Mutation received = deserialize(Mutation.serializer, sent, VERSION_3014);
        PartitionUpdate update = received.getPartitionUpdate(view);
        assertThat(update.columns().regulars).containsExactly(w);
        List<Row> rows = rowsOf(update);
        // the row without hidden cell has nothing left and is not received
        assertThat(rows).hasSize(4);
        assertThat(rows.get(0).primaryKeyLivenessInfo()).isEqualTo(LivenessInfo.create(10, nowInSec));
        assertThat(rows.get(1).primaryKeyLivenessInfo()).isEqualTo(LivenessInfo.withExpirationTime(10, LivenessInfo.EXPIRED_LIVENESS_TTL, nowInSec));
        assertThat(rows.get(2).primaryKeyLivenessInfo()).isEqualTo(LivenessInfo.create(10, nowInSec));
        assertThat(rows.get(2).columnCount()).isEqualTo(1);
        assertThat(rows.get(3).primaryKeyLivenessInfo()).isEqualTo(LivenessInfo.expiring(10, 3600, nowInSec));
        for (Row row : rows)
            assertThat(row.getCell(dseHidden)).isNull();

        received.apply();
        assertRows(execute("SELECT v, pk, ck, w FROM " + KEYSPACE + '.' + view.name + " WHERE v = 'a'"),
                   row("a", 1, 1, null), row("a", 1, 3, 7), row("a", 1, 5, null));
        flush(KEYSPACE, view.name);
        assertThat(getColumnFamilyStore(KEYSPACE, view.name).getLiveSSTables().iterator().next().header.columns().regulars).containsExactly(w);
    }

    /**
     * A DSE view update whose header does not name the hidden column at all: its rows have no hidden cell, and DSE does
     * not consider them live whatever their primary key liveness, so they get none here. Their cells are kept for the
     * entry they update, whose own hidden cell decides whether it is live.
     */
    @Test
    public void mutationFromDseWithoutTheHiddenColumnGivesNoLiveness() throws Throwable
    {
        long nowInSec = FBUtilities.nowInSeconds();
        // the entry, as DSE creates it
        PartitionUpdate.Builder created = PartitionUpdate.builder(dseView, viewKey("c"), dseView.regularAndStaticColumns(), 1);
        created.add(dseRow(1, LivenessInfo.create(10, nowInSec), BufferCell.live(dseHidden, 10, ByteBufferUtil.EMPTY_BYTE_BUFFER), null));
        deserialize(Mutation.serializer, asDse(() -> serialize(Mutation.serializer, new Mutation(created.build()), VERSION_3014)), VERSION_3014).apply();

        RegularAndStaticColumns onlyW = new RegularAndStaticColumns(Columns.NONE, Columns.of(dseW));
        PartitionUpdate.Builder builder = PartitionUpdate.builder(dseView, viewKey("c"), onlyW, 2);
        builder.add(dseRow(1, LivenessInfo.create(20, nowInSec), null, BufferCell.live(dseW, 20, Int32Type.instance.decompose(3))));
        builder.add(dseRow(2, LivenessInfo.create(20, nowInSec), null, null));
        ByteBuffer sent = asDse(() -> serialize(Mutation.serializer, new Mutation(builder.build()), VERSION_3014));
        assertThat(ByteBufferUtil.bytesToHex(sent)).doesNotContain(ByteBufferUtil.bytesToHex(ByteBufferUtil.bytes(".v")));

        Mutation received = deserialize(Mutation.serializer, sent, VERSION_3014);
        List<Row> rows = rowsOf(received.getPartitionUpdate(view));
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).primaryKeyLivenessInfo().isEmpty()).isTrue();
        assertThat(rows.get(0).getCell(w).buffer()).isEqualTo(Int32Type.instance.decompose(3));
        received.apply();
        // the entry keeps the liveness of its hidden cell, at 10, and the row with only a primary key liveness is not there
        assertRows(execute("SELECT v, pk, ck, w, writetime(w) FROM " + KEYSPACE + '.' + view.name + " WHERE v = 'c'"), row("c", 1, 1, 3, 20L));
        assertThat(localRows("c").get(0).primaryKeyLivenessInfo()).isEqualTo(LivenessInfo.create(10, nowInSec));
    }

    /**
     * View updates of this software to a DSE paired view replica, as mutations and as hints (and batched mutations and
     * read-repair writes, which carry the same mutation): every row carries the {@code .v} cell DSE writes.
     */
    @Test
    public void mutationToDseCarriesTheHiddenCells() throws Throwable
    {
        writeBaseRows();
        PartitionUpdate.PartitionUpdateSerializer dseSerializer = new PartitionUpdate.PartitionUpdateSerializer(id -> dseView);
        for (String v : new String[]{ "a", "b" })
        {
            PartitionUpdate.Builder builder = PartitionUpdate.builder(view, viewKey(v), view.regularAndStaticColumns(), 4);
            List<Row> local = localRows(v);
            local.forEach(builder::add);
            Mutation mutation = new Mutation(builder.build());

            ByteBuffer update = serialize(mutation.getPartitionUpdate(view), VERSION_3014);
            PartitionUpdate atDse = asDse(() -> {
                try (DataInputBuffer in = new DataInputBuffer(update, true))
                {
                    return dseSerializer.deserialize(in, VERSION_3014, DeserializationHelper.Flag.FROM_REMOTE);
                }
            });
            assertThat(atDse.columns().regulars).containsExactly(dseHidden, dseW);
            List<Row> dseRows = rowsOf(atDse);
            assertThat(dseRows).hasSameSizeAs(local);
            for (int i = 0; i < local.size(); i++)
                assertDseRowOf(local.get(i), dseRows.get(i));

            // the mutation, and the hint of the mutation for a DSE node, carry the partition update with the hidden cells
            ByteBuffer sent = serialize(Mutation.serializer, mutation, VERSION_3014);
            assertThat(ByteBufferUtil.bytesToHex(sent)).contains(ByteBufferUtil.bytesToHex(update));
            HintMessage hint = HintMessages.create(UUID.randomUUID(), Hint.create(mutation, FBUtilities.timestampMicros() / 1000));
            try (DataOutputBuffer out = new DataOutputBuffer())
            {
                HintMessage.serializer.serialize(hint, out, VERSION_3014);
                assertThat(out.getLength()).isEqualTo(HintMessage.serializer.serializedSize(hint, VERSION_3014));
                assertThat(ByteBufferUtil.bytesToHex(out.asNewBuffer())).contains(ByteBufferUtil.bytesToHex(update));
            }
        }

        // at 4.0 and later the update is sent as it is
        PartitionUpdate.Builder builder = PartitionUpdate.builder(view, viewKey("a"), view.regularAndStaticColumns(), 4);
        localRows("a").forEach(builder::add);
        PartitionUpdate ours = builder.build();
        ByteBuffer atCurrent = serialize(ours, MessagingService.current_version);
        assertThat(atCurrent).isEqualTo(asDse(() -> serialize(ours, MessagingService.current_version)));
        try (DataInputBuffer in = new DataInputBuffer(atCurrent, true))
        {
            PartitionUpdate received = PartitionUpdate.serializer.deserialize(in, MessagingService.current_version, DeserializationHelper.Flag.FROM_REMOTE);
            assertThat(received.columns()).isEqualTo(ours.columns());
            assertThat(rowsOf(received)).isEqualTo(rowsOf(ours));
        }
    }

    /**
     * A read of the view coordinated by this software with a DSE replica: the column filter it sends is the one DSE
     * sends for the same read, and the rows of DSE's response get the liveness of their {@code .v} cells.
     */
    @Test
    public void readSentToDseReplicaFetchesTheHiddenColumnAndReadsItsCells() throws Throwable
    {
        long nowInSec = FBUtilities.nowInSeconds();
        SinglePartitionReadCommand ours = SinglePartitionReadCommand.create(view, nowInSec, viewKey("a"), ColumnFilter.all(view), new ClusteringIndexSliceFilter(Slices.ALL, false));
        SinglePartitionReadCommand dseCommand = dseRead("a", ColumnFilter.all(dseView), nowInSec);
        assertThat(serialize(ReadCommand.serializer, ours, VERSION_3014)).isEqualTo(asDse(() -> serialize(ReadCommand.serializer, dseCommand, VERSION_3014)));
        assertThat(DseViewWireFormat.columnFilterFor(ours, VERSION_3014)).isEqualTo(ColumnFilter.all(dseView));
        // and at 4.0 and later the filter of the command, unchanged
        assertThat(serialize(ReadCommand.serializer, ours, MessagingService.current_version))
        .isEqualTo(asDse(() -> serialize(ReadCommand.serializer, ours, MessagingService.current_version)));

        ReadResponse dseResponse = dseDataResponse(dseCommand, dseRowsForResponse(nowInSec));
        ByteBuffer answer = asDse(() -> serialize(ReadResponse.serializer, dseResponse, VERSION_3014));
        List<Unfiltered> received = new ArrayList<>();
        try (UnfilteredPartitionIterator partitions = deserialize(ReadResponse.serializer, answer, VERSION_3014).makeIterator(ours);
             UnfilteredRowIterator partition = partitions.next())
        {
            assertThat(partition.columns().regulars).containsExactly(w);
            partition.forEachRemaining(received::add);
        }
        assertThat(received).hasSize(2);
        Row live = (Row) received.get(0);
        assertThat(live.primaryKeyLivenessInfo()).isEqualTo(LivenessInfo.create(10, nowInSec));
        assertThat(live.getCell(dseHidden)).isNull();
        Row moved = (Row) received.get(1);
        assertThat(moved.primaryKeyLivenessInfo()).isEqualTo(LivenessInfo.withExpirationTime(12, LivenessInfo.EXPIRED_LIVENESS_TTL, nowInSec));
    }

    /**
     * The read digests of the view at version 11 are DSE's: the replica's digest for a DSE coordinator, the digest this
     * software computes for its own data and for the data of a DSE replica agree with the digest DSE computes. The
     * expected value is fed by hand following DSE 6's digest code on rows built by hand as DSE holds them: the partition,
     * row and cell parts as in Apache Cassandra 3.0 and in this software, and for the cells of DSE's hidden column their
     * value, timestamp, TTL, local deletion time as a 32-bit integer ({@code Integer.MAX_VALUE} when live) and the counter
     * flag. Only that hidden-cell part is fed independently: the liveness (with the TTL and local expiration time of an
     * expiring one), the deletions, the other cells and the column names are fed by this software's own digest methods,
     * taken to be those of Apache Cassandra 3.0, on which DSE 6 is built. They are not compared here with the digests of
     * real DSE nodes.
     */
    @Test
    public void digestsAtVersion11AreDses() throws Throwable
    {
        long nowInSec = FBUtilities.nowInSeconds();
        execute("INSERT INTO %s (pk, ck, v, w) VALUES (1, 1, 'd', 5) USING TIMESTAMP 10");
        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 2, 'd') USING TIMESTAMP 11 AND TTL 3600");
        List<Row> local = localRows("d");
        assertThat(local).hasSize(2);
        long expiresAt = local.get(1).primaryKeyLivenessInfo().localExpirationTime();
        assertThat(expiresAt).isBetween(nowInSec + 3600, FBUtilities.nowInSeconds() + 3600);
        // the same rows as DSE writes them: the hidden cell of the base cell, and the primary key liveness of the base row
        List<Row> asDseWritesThem = List.of(dseRow(1, LivenessInfo.create(10, nowInSec), BufferCell.live(dseHidden, 10, ByteBufferUtil.EMPTY_BYTE_BUFFER),
                                                   BufferCell.live(dseW, 10, Int32Type.instance.decompose(5))),
                                            dseRow(2, LivenessInfo.withExpirationTime(11, 3600, expiresAt),
                                                   new BufferCell(dseHidden, 11, 3600, expiresAt, ByteBufferUtil.EMPTY_BYTE_BUFFER, null), null));
        byte[] expected = expectedDseDigest(viewKey("d"), asDseWritesThem);

        // the digest a DSE coordinator asks this replica for
        SinglePartitionReadCommand dseDigestCommand = (SinglePartitionReadCommand) dseRead("d", ColumnFilter.all(dseView), nowInSec)
                                                                                    .copyAsDigestQuery(Collections.emptyList())
                                                                                    .setDigestVersion(VERSION_3014);
        ByteBuffer sent = asDse(() -> serialize(ReadCommand.serializer, dseDigestCommand, VERSION_3014));
        ReadCommand received = deserialize(ReadCommand.serializer, sent, VERSION_3014);
        ByteBuffer answered = deserialize(ReadResponse.serializer, serialize(ReadResponse.serializer, respond(received), VERSION_3014), VERSION_3014).digest(received);
        assertThat(ByteBufferUtil.getArray(answered)).isEqualTo(expected);

        // the digest this software computes as coordinator of a read with a DSE replica, for its own data
        SinglePartitionReadCommand ours = SinglePartitionReadCommand.create(view, nowInSec, viewKey("d"), ColumnFilter.all(view), new ClusteringIndexSliceFilter(Slices.ALL, false));
        ours.setDigestVersion(VERSION_3014);
        assertThat(ByteBufferUtil.getArray(respond(ours).digest(ours))).isEqualTo(expected);
        assertThat(ByteBufferUtil.getArray(respond(ours.copyAsDigestQuery(Collections.emptyList())).digest(ours))).isEqualTo(expected);

        // and for the same data from a DSE replica
        PartitionUpdate.Builder builder = PartitionUpdate.builder(dseView, viewKey("d"), dseView.regularAndStaticColumns(), 2);
        asDseWritesThem.forEach(builder::add);
        SinglePartitionReadCommand dseCommand = dseRead("d", ColumnFilter.all(dseView), nowInSec);
        ByteBuffer dseAnswer = asDse(() -> serialize(ReadResponse.serializer, dseDataResponse(dseCommand, builder.build()), VERSION_3014));
        assertThat(ByteBufferUtil.getArray(deserialize(ReadResponse.serializer, dseAnswer, VERSION_3014).digest(ours))).isEqualTo(expected);

        // A DSE row whose primary key liveness is not the one of its hidden cell (a base row written by UPDATE has none):
        // its digest is computed on the row as DSE holds it, not on the row as this software reads it.
        List<Row> updatedByDse = List.of(dseRow(1, null, BufferCell.live(dseHidden, 10, ByteBufferUtil.EMPTY_BYTE_BUFFER),
                                                BufferCell.live(dseW, 10, Int32Type.instance.decompose(5))),
                                         dseRow(2, null, new BufferCell(dseHidden, 11, 3600, expiresAt, ByteBufferUtil.EMPTY_BYTE_BUFFER, null), null));
        PartitionUpdate.Builder updated = PartitionUpdate.builder(dseView, viewKey("d"), dseView.regularAndStaticColumns(), 2);
        updatedByDse.forEach(updated::add);
        ByteBuffer updatedAnswer = asDse(() -> serialize(ReadResponse.serializer, dseDataResponse(dseCommand, updated.build()), VERSION_3014));
        byte[] dseDigest = expectedDseDigest(viewKey("d"), updatedByDse);
        assertThat(dseDigest).isNotEqualTo(expected);
        assertThat(ByteBufferUtil.getArray(deserialize(ReadResponse.serializer, updatedAnswer, VERSION_3014).digest(ours))).isEqualTo(dseDigest);

        // at the digest version of 4.0 and later, the digest of this software, without the hidden cells
        SinglePartitionReadCommand current = SinglePartitionReadCommand.create(view, nowInSec, viewKey("d"), ColumnFilter.all(view), new ClusteringIndexSliceFilter(Slices.ALL, false));
        Digest plain = Digest.forReadResponse();
        try (ReadExecutionController controller = current.executionController();
             UnfilteredPartitionIterator iterator = current.executeLocally(controller))
        {
            UnfilteredPartitionIterators.digest(iterator, plain, MessagingService.current_version);
        }
        byte[] plainDigest = plain.digest();
        assertThat(plainDigest).isNotEqualTo(expected);
        // a command whose digest version was never set, and one at the current version
        assertThat(current.digestVersion()).isZero();
        assertThat(ByteBufferUtil.getArray(respond(current).digest(current))).isEqualTo(plainDigest);
        current.setDigestVersion(MessagingService.current_version);
        assertThat(ByteBufferUtil.getArray(respond(current).digest(current))).isEqualTo(plainDigest);
    }

    /**
     * When the catalog of the node cannot be read (its data directories, as those of this test, do not hold DSE's table),
     * no view data is exchanged with DSE 6 nodes, and each refusal is counted in compatstatus; outside of the mode nothing
     * is read. The catalog itself is covered by {@code DseViewHiddenColumnCatalogTest}.
     */
    @Test
    public void aCatalogThatCannotBeReadRefusesTheViewData() throws Throwable
    {
        DseViewHiddenColumnCatalog.setInstance(null);
        long refusedBefore = DseCompatRefusals.count(DseViewHiddenColumnCatalog.REFUSED_VIEW);
        String failure = "DSE 6 compatibility mode: cannot read DSE's system_schema.hidden_columns from the data directories of this node " +
                         "(no data directory holds system_schema/hidden_columns-2461e8b28f6a31a1ac7230b9cd9c6ca5: " + List.of(DatabaseDescriptor.getAllDataFileLocations()) +
                         " are not the data directories of a node that ran DSE 6); the data of materialized views is not exchanged with DSE 6 nodes";
        assertThatThrownBy(() -> DseViewWireFormat.hiddenColumn(view, VERSION_3014)).isInstanceOf(IllegalStateException.class).hasMessage(failure);
        assertThat(DseCompatRefusals.count(DseViewHiddenColumnCatalog.REFUSED_VIEW)).isEqualTo(refusedBefore + 1);

        // nothing is read, and nothing refused, outside of the mode
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.NONE);
        assertThat(DseViewWireFormat.hiddenColumn(view, VERSION_3014)).isNull();
        assertThat(DseCompatRefusals.count(DseViewHiddenColumnCatalog.REFUSED_VIEW)).isEqualTo(refusedBefore + 1);
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);

        // another catalog replaces the failed one
        DseViewHiddenColumnCatalog.setInstance(new DseViewHiddenColumnCatalog(dseHiddenColumns));
        assertThat(DseViewWireFormat.hiddenColumn(view, VERSION_3014)).isEqualTo(dseHidden);
    }

    /** A hint DSE stored for this node while it was down, with a view update: its rows get the liveness of their hidden cells. */
    @Test
    public void hintFromDseGivesRowsTheLivenessOfTheirHiddenCells() throws Throwable
    {
        long nowInSec = FBUtilities.nowInSeconds();
        PartitionUpdate.Builder builder = PartitionUpdate.builder(dseView, viewKey("e"), dseView.regularAndStaticColumns(), 1);
        builder.add(dseRow(1, null, BufferCell.live(dseHidden, 10, ByteBufferUtil.EMPTY_BYTE_BUFFER), BufferCell.live(dseW, 10, Int32Type.instance.decompose(9))));
        HintMessage dseHint = HintMessages.create(UUID.randomUUID(), Hint.create(new Mutation(builder.build()), FBUtilities.timestampMicros() / 1000));
        ByteBuffer sent = asDse(() -> {
            try (DataOutputBuffer out = new DataOutputBuffer())
            {
                HintMessage.serializer.serialize(dseHint, out, VERSION_3014);
                return out.asNewBuffer();
            }
        });
        HintMessage received;
        try (DataInputBuffer in = new DataInputBuffer(sent, true))
        {
            received = HintMessage.serializer.deserialize(in, VERSION_3014);
        }
        Mutation mutation = HintMessages.mutationOf(received);
        List<Row> rows = rowsOf(mutation.getPartitionUpdate(view));
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).primaryKeyLivenessInfo()).isEqualTo(LivenessInfo.create(10, nowInSec));
        assertThat(rows.get(0).getCell(dseHidden)).isNull();
        mutation.apply();
        assertRows(execute("SELECT v, pk, ck, w FROM " + KEYSPACE + '.' + view.name + " WHERE v = 'e'"), row("e", 1, 1, 9));
    }

    /**
     * A view update DSE 6.8 or 6.9 stored on its own disk before the upgrade (a batch of its batchlog, a hint, a mutation of
     * its commit log), in DSE's own layout, decoded at {@link MessagingService#VERSION_DSE_68}: its rows are those of the
     * same update sent at version 11, with the liveness of their hidden cells, and the stale entry (a hidden tombstone next
     * to a primary key liveness) is not live. Replayed or delivered to a DSE 6 peer, the update carries the hidden cells
     * DSE stored again.
     */
    @Test
    public void viewUpdateStoredByDseIsTranslatedAsOnTheWire() throws Throwable
    {
        long nowInSec = FBUtilities.nowInSeconds();
        PartitionUpdate.Builder builder = PartitionUpdate.builder(dseView, viewKey("s"), dseView.regularAndStaticColumns(), 4);
        // a live hidden cell and no primary key liveness (the entry of a base row written by UPDATE): live
        builder.add(dseRow(1, null, BufferCell.live(dseHidden, 10, ByteBufferUtil.EMPTY_BYTE_BUFFER), null));
        // primary key liveness and a hidden tombstone (the old entry after the base cell changed): not live
        builder.add(dseRow(2, LivenessInfo.create(10, nowInSec), BufferCell.tombstone(dseHidden, 10, nowInSec), null));
        // primary key liveness, a live hidden cell and a regular cell (the entry of a base row written by INSERT): live
        builder.add(dseRow(3, LivenessInfo.create(10, nowInSec), BufferCell.live(dseHidden, 10, ByteBufferUtil.EMPTY_BYTE_BUFFER),
                           BufferCell.live(dseW, 10, Int32Type.instance.decompose(7))));
        // primary key liveness and no hidden cell: not live, DSE ignores the primary key liveness
        builder.add(dseRow(4, LivenessInfo.create(10, nowInSec), null, null));
        Mutation dseMutation = new Mutation(builder.build());
        ByteBuffer stored = DseFixtures.dse68Mutation(dseMutation);
        assertThat(ByteBufferUtil.bytesToHex(stored)).contains(ByteBufferUtil.bytesToHex(ByteBufferUtil.bytes(".v")));

        Mutation decoded = deserialize(Mutation.serializer, stored, MessagingService.VERSION_DSE_68);
        PartitionUpdate update = decoded.getPartitionUpdate(view);
        Mutation sentAt11 = deserialize(Mutation.serializer, asDse(() -> serialize(Mutation.serializer, dseMutation, VERSION_3014)), VERSION_3014);
        assertThat(rowsOf(update)).isEqualTo(rowsOf(sentAt11.getPartitionUpdate(view)));
        assertThat(update.columns().regulars).containsExactly(w);
        List<Row> rows = rowsOf(update);
        // the row without hidden cell has nothing left and is not decoded
        assertThat(rows).hasSize(3);
        assertThat(rows.get(0).primaryKeyLivenessInfo()).isEqualTo(LivenessInfo.create(10, nowInSec));
        assertThat(rows.get(1).primaryKeyLivenessInfo()).isEqualTo(LivenessInfo.withExpirationTime(10, LivenessInfo.EXPIRED_LIVENESS_TTL, nowInSec));
        assertThat(rows.get(2).primaryKeyLivenessInfo()).isEqualTo(LivenessInfo.create(10, nowInSec));
        for (Row row : rows)
            assertThat(row.getCell(dseHidden)).isNull();

        // to a DSE 6 peer, at version 11: the hidden cells DSE stored, the live ones and the tombstone
        ByteBuffer toDse = serialize(decoded.getPartitionUpdate(view), VERSION_3014);
        PartitionUpdate.PartitionUpdateSerializer dseSerializer = new PartitionUpdate.PartitionUpdateSerializer(id -> dseView);
        List<Row> dseRows = rowsOf(asDse(() -> {
            try (DataInputBuffer in = new DataInputBuffer(toDse, true))
            {
                return dseSerializer.deserialize(in, VERSION_3014, DeserializationHelper.Flag.FROM_REMOTE);
            }
        }));
        assertThat(dseRows).hasSize(3);
        assertHiddenCell(dseRows.get(0), 10, Cell.NO_TTL, Cell.NO_DELETION_TIME, false);
        assertHiddenCell(dseRows.get(1), 10, Cell.NO_TTL, nowInSec, true);
        assertHiddenCell(dseRows.get(2), 10, Cell.NO_TTL, Cell.NO_DELETION_TIME, false);

        decoded.apply();
        assertRows(execute("SELECT v, pk, ck, w FROM " + KEYSPACE + '.' + view.name + " WHERE v = 's'"),
                   row("s", 1, 1, null), row("s", 1, 3, 7));
    }

    /**
     * A view update DSE stored is translated only when the translation is certain: when DSE's table lists no hidden column
     * for the view, when the catalog refuses the view or cannot be read, the update fails to decode as one naming a column
     * unknown here, as it did before, with a WARN once per view saying why. DSE 6.0's layout, decoded at
     * {@link MessagingService#VERSION_40}, is not translated, and outside of the mode nothing is translated and the catalog
     * is not read.
     */
    @Test
    public void viewUpdateStoredByDseIsNotTranslatedWhenUncertain() throws Throwable
    {
        // a view DSE's table lists no hidden column for
        TableMetadata unlisted = getColumnFamilyStore(KEYSPACE, createView(VIEW_QUERY)).metadata();
        assertStoredUpdateNotTranslated(unlisted, ".v", "DSE's system_schema.hidden_columns on this node lists no hidden column for that view");

        // a view DSE's table lists another hidden column for than the one the update names
        TableMetadata other = getColumnFamilyStore(KEYSPACE, createView(VIEW_QUERY)).metadata();
        dseHiddenColumns.put(Pair.create(KEYSPACE, other.name), List.of(DSE_V));
        assertStoredUpdateNotTranslated(other, ".x", "DSE's system_schema.hidden_columns on this node lists the hidden column .v for that view");

        // a view DSE's table lists another hidden column for too: the catalog refuses it
        TableMetadata refused = getColumnFamilyStore(KEYSPACE, createView(VIEW_QUERY)).metadata();
        dseHiddenColumns.put(Pair.create(KEYSPACE, refused.name), List.of(DSE_V, new HiddenColumn(".w", "regular", false, "int")));
        assertStoredUpdateNotTranslated(refused, ".v", "DSE 6 compatibility mode: DSE keeps the hidden columns [.v text (regular, required for liveness), " +
                                                 ".w int (regular)] for materialized view " + KEYSPACE + '.' + refused.name);

        // a catalog that cannot be read (the data directories of this test do not hold DSE's table)
        DseViewHiddenColumnCatalog.setInstance(null);
        assertStoredUpdateNotTranslated(view, ".v", "DSE 6 compatibility mode: cannot read DSE's system_schema.hidden_columns from the data directories of this node");
        DseViewHiddenColumnCatalog.setInstance(new DseViewHiddenColumnCatalog(dseHiddenColumns));

        // DSE 6.0's layout, decoded at VERSION_40: the views of the new format came with DSE 6.7
        ByteBuffer dse60 = serialize(Mutation.serializer, storedDseUpdate(view, ".v"), MessagingService.VERSION_40);
        assertThatThrownBy(() -> deserialize(Mutation.serializer, dse60, MessagingService.VERSION_40))
        .isInstanceOf(RuntimeException.class)
        .hasMessage("Unknown column .v during deserialization of " + KEYSPACE + '.' + view.name);

        // outside of the mode: as before, and the catalog is not read
        ByteBuffer stored = DseFixtures.dse68Mutation(storedDseUpdate(view, ".v"));
        DseViewHiddenColumnCatalog.setInstance(null);
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.NONE);
        assertThat(DseViewWireFormat.translatesStored(view, MessagingService.VERSION_DSE_68)).isFalse();
        assertThatThrownBy(() -> deserialize(Mutation.serializer, stored, MessagingService.VERSION_DSE_68))
        .isInstanceOf(RuntimeException.class)
        .hasMessage("Unknown column .v during deserialization of " + KEYSPACE + '.' + view.name);
        assertThat(DseViewHiddenColumnCatalog.setInstance(null)).isNull();
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);

        // in the mode, only at DSE's stored version and only for views
        assertThat(DseViewWireFormat.translatesStored(view, MessagingService.VERSION_DSE_68)).isTrue();
        assertThat(DseViewWireFormat.translatesStored(view, MessagingService.VERSION_40)).isFalse();
        assertThat(DseViewWireFormat.translatesStored(view, VERSION_3014)).isFalse();
        assertThat(DseViewWireFormat.translatesStored(view, MessagingService.current_version)).isFalse();
        assertThat(DseViewWireFormat.translatesStored(currentTableMetadata(), MessagingService.VERSION_DSE_68)).isFalse();
    }

    /**
     * The view update DSE stores for the entry {@code (pk 1, ck 1)} of the partition {@code 'u'} of {@code viewMetadata}: a
     * live hidden cell and a regular cell, on the view as DSE knows it, with the hidden column {@code hiddenName}.
     */
    private static Mutation storedDseUpdate(TableMetadata viewMetadata, String hiddenName)
    {
        TableMetadata dse = viewMetadata.unbuild().addRegularColumn(hiddenName, UTF8Type.instance).build();
        PartitionUpdate.Builder builder = PartitionUpdate.builder(dse, dse.partitioner.decorateKey(ByteBufferUtil.bytes("u")), dse.regularAndStaticColumns(), 1);
        builder.add(dseRow(1, LivenessInfo.create(10, FBUtilities.nowInSeconds()),
                           BufferCell.live(dse.getColumn(ByteBufferUtil.bytes(hiddenName)), 10, ByteBufferUtil.EMPTY_BYTE_BUFFER),
                           BufferCell.live(dse.getColumn(ByteBufferUtil.bytes("w")), 10, Int32Type.instance.decompose(1))));
        return new Mutation(builder.build());
    }

    /**
     * The view update DSE stored for {@code viewMetadata}, naming the hidden column {@code hiddenName}, fails to decode as
     * one naming a column unknown here, twice, with one WARN whose reason starts with {@code reason}, and no refusal of
     * view data counted. The WARN is logged once per view for the life of the node: each call needs a view no earlier
     * call or test met.
     */
    private void assertStoredUpdateNotTranslated(TableMetadata viewMetadata, String hiddenName, String reason) throws Throwable
    {
        ByteBuffer stored = DseFixtures.dse68Mutation(storedDseUpdate(viewMetadata, hiddenName));
        long refusedBefore = DseCompatRefusals.count(DseViewHiddenColumnCatalog.REFUSED_VIEW);
        withLogAppender(DseViewWireFormat.class, appender -> {
            for (int i = 0; i < 2; i++)
            {
                assertThatThrownBy(() -> deserialize(Mutation.serializer, stored, MessagingService.VERSION_DSE_68))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("Unknown column " + hiddenName + " during deserialization of " + viewMetadata);
            }
            assertThat(appender.list).singleElement().satisfies(event -> {
                assertThat(event.getLevel()).isEqualTo(Level.WARN);
                assertThat(event.getFormattedMessage())
                .startsWith("DSE 6 compatibility mode: a view update DSE 6 stored on the disk of this node before the upgrade (in a batch of " +
                            "its batchlog, a hint of its hints files or a mutation of its commit log) names the column " + hiddenName + " of materialized view " +
                            viewMetadata + ", which is not translated: " + reason)
                .endsWith(". The update is read as one naming a column unknown here (a batch is kept in the batchlog, a hint is dropped, " +
                          "a mutation of the commit log fails its replay), rather than written as view rows that may be wrong; this is logged " +
                          "once per view");
            });
        });
        assertThat(DseCompatRefusals.count(DseViewHiddenColumnCatalog.REFUSED_VIEW)).isEqualTo(refusedBefore);
    }

    /**
     * Outside of DSE 6 compatibility mode nothing is translated and nothing is read: a message naming a hidden column
     * fails as it did before the mode existed.
     */
    @Test
    public void outsideOfTheModeHiddenColumnsAreUnknownAsBefore() throws Throwable
    {
        long nowInSec = FBUtilities.nowInSeconds();
        PartitionUpdate.Builder builder = PartitionUpdate.builder(dseView, viewKey("a"), dseView.regularAndStaticColumns(), 1);
        builder.add(dseRow(1, LivenessInfo.create(10, nowInSec), BufferCell.live(dseHidden, 10, ByteBufferUtil.EMPTY_BYTE_BUFFER), null));
        PartitionUpdate dseUpdate = builder.build();
        ByteBuffer atCurrent = asDse(() -> serialize(dseUpdate, MessagingService.current_version));

        DseViewHiddenColumnCatalog.setInstance(null);
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.NONE);
        try (DataInputBuffer in = new DataInputBuffer(atCurrent, true))
        {
            assertThatThrownBy(() -> PartitionUpdate.serializer.deserialize(in, MessagingService.current_version, DeserializationHelper.Flag.FROM_REMOTE))
            .isInstanceOf(RuntimeException.class)
            .hasMessage("Unknown column .v during deserialization of " + KEYSPACE + '.' + view.name);
        }
        SinglePartitionReadCommand dseCommand = dseRead("a", ColumnFilter.all(dseView), nowInSec);
        ByteBuffer read = asDse(() -> serialize(ReadCommand.serializer, dseCommand, MessagingService.current_version));
        DseViewHiddenColumnCatalog.setInstance(null);
        assertThatThrownBy(() -> deserialize(ReadCommand.serializer, read, MessagingService.current_version))
        .isInstanceOf(RuntimeException.class)
        .hasMessage("Unknown column .v during deserialization of " + KEYSPACE + '.' + view.name);
        // the catalog was never read
        assertThat(DseViewHiddenColumnCatalog.setInstance(null)).isNull();
    }

    /**
     * A view DSE's table lists no hidden column for (taken to be of DSE's old format) is exchanged as it is in both directions, and a message
     * naming a hidden column DSE does not list for it is refused.
     */
    @Test
    public void viewsWithoutHiddenColumnAreExchangedAsTheyAre() throws Throwable
    {
        String legacyName = createView(VIEW_QUERY);
        TableMetadata legacy = getColumnFamilyStore(KEYSPACE, legacyName).metadata();
        long nowInSec = FBUtilities.nowInSeconds();
        DecoratedKey key = legacy.partitioner.decorateKey(ByteBufferUtil.bytes("a"));

        SinglePartitionReadCommand read = SinglePartitionReadCommand.create(legacy, nowInSec, key, ColumnFilter.all(legacy), new ClusteringIndexSliceFilter(Slices.ALL, false));
        assertThat(serialize(ReadCommand.serializer, read, VERSION_3014)).isEqualTo(asDse(() -> serialize(ReadCommand.serializer, read, VERSION_3014)));
        PartitionUpdate.Builder builder = PartitionUpdate.builder(legacy, key, legacy.regularAndStaticColumns(), 1);
        builder.add(dseRow(1, LivenessInfo.create(10, nowInSec), null, BufferCell.live(legacy.getColumn(ByteBufferUtil.bytes("w")), 10, Int32Type.instance.decompose(1))));
        PartitionUpdate update = builder.build();
        ByteBuffer sent = serialize(update, VERSION_3014);
        assertThat(sent).isEqualTo(asDse(() -> serialize(update, VERSION_3014)));
        // inbound, the rows keep their primary key liveness
        try (DataInputBuffer in = new DataInputBuffer(sent, true))
        {
            List<Row> rows = rowsOf(PartitionUpdate.serializer.deserialize(in, VERSION_3014, DeserializationHelper.Flag.FROM_REMOTE));
            assertThat(rows.get(0).primaryKeyLivenessInfo()).isEqualTo(LivenessInfo.create(10, nowInSec));
        }

        TableMetadata dseLegacy = legacy.unbuild().addRegularColumn(".v", UTF8Type.instance).build();
        SinglePartitionReadCommand withHidden = SinglePartitionReadCommand.create(dseLegacy, nowInSec, key, ColumnFilter.all(dseLegacy), new ClusteringIndexSliceFilter(Slices.ALL, false));
        ByteBuffer dseRead = asDse(() -> serialize(ReadCommand.serializer, withHidden, VERSION_3014));
        assertThatThrownBy(() -> deserialize(ReadCommand.serializer, dseRead, VERSION_3014))
        .isInstanceOf(UnknownColumnException.class)
        .hasMessage("A message from a DSE 6 peer names the column .v of materialized view " + KEYSPACE + '.' + legacyName + ", which DSE's " +
                    "system_schema.hidden_columns on this node does not list as a hidden column of that view; this software refuses the message " +
                    "rather than return or store wrong view rows. Check that the data directories of this node hold the sstables of " +
                    "system_schema.hidden_columns that DSE wrote");
    }

    /**
     * A view with any other DSE hidden column is refused in both directions with the remedy, and so is a filter on a
     * hidden column.
     */
    @Test
    public void otherHiddenColumnsAndFiltersOnHiddenColumnsAreRefused() throws Throwable
    {
        long nowInSec = FBUtilities.nowInSeconds();

        // a view without a regular base column in its primary key, whose DSE twin keeps "virtual" hidden cells for v and w
        String keyOnly = createView("CREATE MATERIALIZED VIEW %s AS SELECT pk, ck FROM %s WHERE pk IS NOT NULL AND ck IS NOT NULL PRIMARY KEY (ck, pk)");
        dseHiddenColumns.put(Pair.create(KEYSPACE, keyOnly), List.of(new HiddenColumn(".v", "regular", false, "text"), new HiddenColumn(".w", "regular", false, "int")));
        TableMetadata keyOnlyView = getColumnFamilyStore(KEYSPACE, keyOnly).metadata();
        String refusal = "DSE 6 compatibility mode: DSE keeps the hidden columns [.v text (regular), .w int (regular)] for materialized view " + KEYSPACE + '.' + keyOnly +
                         "; this software only reproduces the hidden column of a regular base column in the view primary key, and refuses to exchange " +
                         "the view's data with DSE 6 nodes rather than return or store wrong view rows. " + REMEDY;
        TableMetadata dseKeyOnlyView = keyOnlyView.unbuild().addRegularColumn(".v", UTF8Type.instance).addRegularColumn(".w", Int32Type.instance).build();
        DecoratedKey key = keyOnlyView.partitioner.decorateKey(Int32Type.instance.decompose(1));
        Row row = BTreeRow.singleCellRow(Clustering.make(Int32Type.instance.decompose(1)),
                                         BufferCell.live(dseKeyOnlyView.getColumn(ByteBufferUtil.bytes(".v")), 10, ByteBufferUtil.EMPTY_BYTE_BUFFER));
        ByteBuffer mutation = asDse(() -> serialize(Mutation.serializer, new Mutation(PartitionUpdate.singleRowUpdate(dseKeyOnlyView, key, row)), VERSION_3014));
        assertThatThrownBy(() -> deserialize(Mutation.serializer, mutation, VERSION_3014))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(refusal);
        // and the other way: neither a view update nor a read of that view is sent to a DSE node
        Row ourRow = BTreeRow.emptyDeletedRow(Clustering.make(Int32Type.instance.decompose(1)), Row.Deletion.regular(DeletionTime.build(10, nowInSec)));
        PartitionUpdate ourUpdate = PartitionUpdate.singleRowUpdate(keyOnlyView, key, ourRow);
        assertThatThrownBy(() -> serialize(ourUpdate, VERSION_3014)).isInstanceOf(IllegalStateException.class).hasMessage(refusal);
        SinglePartitionReadCommand keyOnlyRead = SinglePartitionReadCommand.create(keyOnlyView, nowInSec, key, ColumnFilter.all(keyOnlyView),
                                                                                   new ClusteringIndexSliceFilter(Slices.ALL, false));
        assertThatThrownBy(() -> serialize(ReadCommand.serializer, keyOnlyRead, VERSION_3014)).isInstanceOf(IllegalStateException.class).hasMessage(refusal);
        // at 4.0 and later, between nodes of this software, as before
        assertThat(serialize(ourUpdate, MessagingService.current_version)).isEqualTo(asDse(() -> serialize(ourUpdate, MessagingService.current_version)));

        // a hidden column of the view of the other tests that DSE does not list for it
        TableMetadata dseViewWithOther = view.unbuild().addRegularColumn(".x", Int32Type.instance).build();
        SinglePartitionReadCommand otherHidden = SinglePartitionReadCommand.create(dseViewWithOther, nowInSec, viewKey("a"), ColumnFilter.all(dseViewWithOther),
                                                                                   new ClusteringIndexSliceFilter(Slices.ALL, false));
        ByteBuffer read = asDse(() -> serialize(ReadCommand.serializer, otherHidden, VERSION_3014));
        assertThatThrownBy(() -> deserialize(ReadCommand.serializer, read, VERSION_3014))
        .isInstanceOf(UnknownColumnException.class)
        .hasMessageStartingWith("A message from a DSE 6 peer names the column .x of materialized view " + KEYSPACE + '.' + view.name);

        // a filter on the hidden column: DSE never sends one, and it cannot be evaluated here
        SinglePartitionReadCommand filtered = SinglePartitionReadCommand.create(dseView, nowInSec, ColumnFilter.all(dseView), rowFilterOnHiddenColumn(),
                                                                                DataLimits.NONE, viewKey("a"), new ClusteringIndexSliceFilter(Slices.ALL, false));
        ByteBuffer filteredRead = asDse(() -> serialize(ReadCommand.serializer, filtered, VERSION_3014));
        assertThatThrownBy(() -> deserialize(ReadCommand.serializer, filteredRead, VERSION_3014))
        .isInstanceOf(UnknownColumnException.class)
        .hasMessage("A read of materialized view " + KEYSPACE + '.' + view.name + " from a DSE 6 peer filters on the DSE hidden column .v, " +
                    "which this software does not keep; it refuses the message rather than return wrong view rows");

        // outside of a view, a column named like a hidden one stays an unknown column, as before
        TableMetadata base = currentTableMetadata();
        TableMetadata baseWithDot = base.unbuild().addRegularColumn(".v", UTF8Type.instance).build();
        Row baseRow = BTreeRow.singleCellRow(Clustering.make(Int32Type.instance.decompose(1)),
                                             BufferCell.live(baseWithDot.getColumn(ByteBufferUtil.bytes(".v")), 10, ByteBufferUtil.EMPTY_BYTE_BUFFER));
        DecoratedKey baseKey = base.partitioner.decorateKey(Int32Type.instance.decompose(1));
        ByteBuffer baseMutation = asDse(() -> serialize(Mutation.serializer, new Mutation(PartitionUpdate.singleRowUpdate(baseWithDot, baseKey, baseRow)), VERSION_3014));
        assertThatThrownBy(() -> deserialize(Mutation.serializer, baseMutation, VERSION_3014))
        .isInstanceOf(RuntimeException.class)
        .hasMessage("Unknown column .v during deserialization of " + KEYSPACE + '.' + base.name);
    }

    private RowFilter rowFilterOnHiddenColumn()
    {
        RowFilter.Builder builder = RowFilter.builder(true, null);
        builder.add(dseHidden, Operator.EQ, ByteBufferUtil.EMPTY_BYTE_BUFFER);
        return builder.build();
    }

    /**
     * Base writes whose view rows cover each liveness this software writes: partition {@code a} gets the rows of an INSERT
     * (1, 1), an INSERT with a TTL (1, 2), an UPDATE of v moving (1, 3) from {@code b} and an UPDATE (1, 4); partition
     * {@code b} keeps the expired entry of (1, 3).
     */
    private void writeBaseRows() throws Throwable
    {
        execute("INSERT INTO %s (pk, ck, v, w) VALUES (1, 1, 'a', 1) USING TIMESTAMP 10");
        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 2, 'a') USING TIMESTAMP 11 AND TTL 3600");
        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 3, 'b') USING TIMESTAMP 12");
        execute("UPDATE %s USING TIMESTAMP 13 SET v = 'a' WHERE pk = 1 AND ck = 3");
        execute("UPDATE %s USING TIMESTAMP 14 SET v = 'a', w = 4 WHERE pk = 1 AND ck = 4");
    }

    /** The column filters DSE sends for a read of the view: a wildcard one, and one fetching every column and querying w. */
    private List<ColumnFilter> dseColumnFilters()
    {
        List<ColumnFilter> filters = new ArrayList<>();
        filters.add(ColumnFilter.all(dseView));
        // the pre-4.0 layout has no FETCH_ALL_STATICS bit: fetching all regular columns fetches all static ones too
        filters.add(ColumnFilter.allRegularColumnsBuilder(dseView, true).add(dseW).build());
        return filters;
    }

    private SinglePartitionReadCommand dseRead(String v, ColumnFilter filter, long nowInSec)
    {
        return SinglePartitionReadCommand.create(dseView, nowInSec, filter, RowFilter.none(), DataLimits.NONE, viewKey(v),
                                                 new ClusteringIndexSliceFilter(Slices.ALL, false));
    }

    /**
     * The rows of the given slices of partition {@code v} of the view, as this software sends them to a DSE coordinator
     * and DSE reads them; a reversed read is limited to 3 rows.
     */
    private List<Row> dseRowsOf(String v, Slices slices, boolean reversed, long nowInSec) throws Exception
    {
        SinglePartitionReadCommand dseCommand = SinglePartitionReadCommand.create(dseView, nowInSec, ColumnFilter.all(dseView), RowFilter.none(),
                                                                                  reversed ? DataLimits.cqlLimits(3) : DataLimits.NONE, viewKey(v),
                                                                                  new ClusteringIndexSliceFilter(slices, reversed));
        ByteBuffer sent = asDse(() -> serialize(ReadCommand.serializer, dseCommand, VERSION_3014));
        ByteBuffer answer = serialize(ReadResponse.serializer, respond(deserialize(ReadCommand.serializer, sent, VERSION_3014)), VERSION_3014);
        return asDse(() -> rows(deserialize(ReadResponse.serializer, answer, VERSION_3014).makeIterator(dseCommand)));
    }

    /** A live row with liveness at 10 and a moved entry, as DSE holds them, and a row without hidden cell, which DSE does not show. */
    private PartitionUpdate dseRowsForResponse(long nowInSec)
    {
        PartitionUpdate.Builder builder = PartitionUpdate.builder(dseView, viewKey("a"), dseView.regularAndStaticColumns(), 3);
        builder.add(dseRow(1, LivenessInfo.create(10, nowInSec), BufferCell.live(dseHidden, 10, ByteBufferUtil.EMPTY_BYTE_BUFFER), null));
        builder.add(dseRow(2, LivenessInfo.create(20, nowInSec), null, null));
        builder.add(dseRow(3, null, BufferCell.tombstone(dseHidden, 12, nowInSec), null));
        return builder.build();
    }

    private static ReadResponse dseDataResponse(SinglePartitionReadCommand dseCommand, PartitionUpdate rows)
    {
        return ReadResponse.createRemoteDataResponse(new SingletonUnfilteredPartitionIterator(rows.unfilteredIterator()),
                                                     ByteBufferUtil.EMPTY_BYTE_BUFFER, true, dseCommand, VERSION_3014);
    }

    private static Clustering<?> clustering(int pk, int ck)
    {
        return Clustering.make(Int32Type.instance.decompose(pk), Int32Type.instance.decompose(ck));
    }

    private static Row dseRow(int ck, LivenessInfo liveness, Cell<?> hidden, Cell<?> regular)
    {
        Row.Builder builder = BTreeRow.unsortedBuilder();
        builder.newRow(clustering(1, ck));
        if (liveness != null)
            builder.addPrimaryKeyLivenessInfo(liveness);
        if (hidden != null)
            builder.addCell(hidden);
        if (regular != null)
            builder.addCell(regular);
        return builder.build();
    }

    private DecoratedKey viewKey(String v)
    {
        return view.partitioner.decorateKey(ByteBufferUtil.bytes(v));
    }

    /** The rows of partition {@code v} of the view, as this software holds them. */
    private List<Row> localRows(String v)
    {
        SinglePartitionReadCommand command = SinglePartitionReadCommand.create(view, FBUtilities.nowInSeconds(), viewKey(v), ColumnFilter.all(view),
                                                                               new ClusteringIndexSliceFilter(Slices.ALL, false));
        try (ReadExecutionController controller = command.executionController())
        {
            return rows(command.executeLocally(controller));
        }
    }

    /**
     * {@code dseRow} is what DSE reads for {@code ours}: same clustering, cells and deletion, the hidden cell of its
     * liveness, and the liveness itself (a live one at the same timestamp for an expired one).
     */
    private void assertDseRowOf(Row ours, Row dseRow)
    {
        assertThat(dseRow.clustering().bufferAt(0)).isEqualTo(ours.clustering().bufferAt(0));
        assertThat(dseRow.clustering().bufferAt(1)).isEqualTo(ours.clustering().bufferAt(1));
        assertThat(dseRow.deletion()).isEqualTo(ours.deletion());
        LivenessInfo liveness = ours.primaryKeyLivenessInfo();
        boolean expired = liveness.ttl() == LivenessInfo.EXPIRED_LIVENESS_TTL;
        Cell<?> hidden = dseRow.getCell(dseHidden);
        assertThat(hidden.buffer()).isEqualTo(ByteBufferUtil.EMPTY_BYTE_BUFFER);
        assertThat(hidden.timestamp()).isEqualTo(liveness.timestamp());
        assertThat(hidden.isTombstone()).isEqualTo(expired);
        assertThat(hidden.ttl()).isEqualTo(expired ? Cell.NO_TTL : liveness.ttl());
        assertThat(hidden.localDeletionTime()).isEqualTo(liveness.isExpiring() ? liveness.localExpirationTime() : Cell.NO_DELETION_TIME);
        assertThat(dseRow.primaryKeyLivenessInfo()).isEqualTo(expired ? LivenessInfo.create(liveness.timestamp(), 0) : liveness);
        Cell<?> ourW = ours.getCell(w);
        Cell<?> theirW = dseRow.getCell(dseW);
        if (ourW == null)
        {
            assertThat(theirW).isNull();
        }
        else
        {
            assertThat(theirW.buffer()).isEqualTo(ourW.buffer());
            assertThat(theirW.timestamp()).isEqualTo(ourW.timestamp());
        }
    }

    private void assertHiddenCell(Row dseRow, long timestamp, int ttl, long localDeletionTime, boolean tombstone)
    {
        Cell<?> hidden = dseRow.getCell(dseHidden);
        assertThat(hidden.timestamp()).isEqualTo(timestamp);
        assertThat(hidden.ttl()).isEqualTo(ttl);
        assertThat(hidden.localDeletionTime()).isEqualTo(localDeletionTime);
        assertThat(hidden.isTombstone()).isEqualTo(tombstone);
        assertThat(hidden.buffer()).isEqualTo(ByteBufferUtil.EMPTY_BYTE_BUFFER);
    }

    /**
     * The digest DSE computes for partition {@code key} holding {@code dseRows} (rows of {@link #dseView}, each with its
     * hidden cell), fed by hand following DSE 6's digest code for the hidden cells, by this software's digest methods for
     * the rest.
     */
    private byte[] expectedDseDigest(DecoratedKey key, List<Row> dseRows)
    {
        Digest digest = Digest.forReadResponse();
        digest.update(key.getKey());
        DeletionTime.LIVE.digest(digest);
        Columns.from(List.of(dseHidden, dseW)).digest(digest);
        digest.updateWithBoolean(false);
        Rows.EMPTY_STATIC_ROW.digest(digest);
        for (Row row : dseRows)
        {
            digest.updateWithByte(row.kind().ordinal());
            row.clustering().digest(digest);
            row.deletion().digest(digest);
            row.primaryKeyLivenessInfo().digest(digest);
            // DSE's hidden cell comes first of the regular columns ('.' sorts before 'w'), with its local deletion time
            Cell<?> hidden = row.getCell(dseHidden);
            digest.update(ByteBufferUtil.EMPTY_BYTE_BUFFER);
            digest.updateWithLong(hidden.timestamp());
            digest.updateWithInt(hidden.ttl());
            digest.updateWithInt(hidden.isExpiring() || hidden.isTombstone() ? (int) hidden.localDeletionTime() : Integer.MAX_VALUE);
            digest.updateWithBoolean(false);
            Cell<?> regular = row.getCell(dseW);
            if (regular != null)
                regular.digest(digest);
        }
        return digest.digest();
    }

    private static List<Row> rowsOf(PartitionUpdate update)
    {
        List<Row> rows = new ArrayList<>();
        try (UnfilteredRowIterator partition = update.unfilteredIterator())
        {
            while (partition.hasNext())
                rows.add((Row) partition.next());
        }
        return rows;
    }

    private static ByteBuffer serialize(PartitionUpdate update, int version) throws IOException
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            PartitionUpdate.serializer.serialize(update, out, version);
            assertThat(out.getLength()).isEqualTo(PartitionUpdate.serializer.serializedSize(update, version));
            return out.asNewBuffer();
        }
    }

    private static List<Row> rows(UnfilteredPartitionIterator partitions)
    {
        List<Row> rows = new ArrayList<>();
        try (UnfilteredPartitionIterator iterator = partitions)
        {
            while (iterator.hasNext())
            {
                try (UnfilteredRowIterator partition = iterator.next())
                {
                    while (partition.hasNext())
                        rows.add((Row) partition.next());
                }
            }
        }
        return rows;
    }

    private static ReadResponse respond(ReadCommand command)
    {
        try (ReadExecutionController controller = command.executionController();
             UnfilteredPartitionIterator iterator = command.executeLocally(controller))
        {
            return command.createResponse(iterator, controller.getRepairedDataInfo());
        }
    }

    /**
     * Runs {@code action} as the DSE side: with a catalog listing no hidden column, the serializers of this software
     * translate nothing, and on the metadata of {@link #dseView} they write and read DSE's own bytes.
     */
    private static <T> T asDse(Callable<T> action) throws Exception
    {
        DseViewHiddenColumnCatalog ours = DseViewHiddenColumnCatalog.setInstance(new DseViewHiddenColumnCatalog(Collections.emptyMap()));
        try
        {
            return action.call();
        }
        finally
        {
            DseViewHiddenColumnCatalog.setInstance(ours);
        }
    }

    private static <T> ByteBuffer serialize(IVersionedSerializer<T> serializer, T value, int version) throws IOException
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            serializer.serialize(value, out, version);
            assertThat(out.getLength()).isEqualTo(serializer.serializedSize(value, version));
            return out.asNewBuffer();
        }
    }

    private static <T> T deserialize(IVersionedSerializer<T> serializer, ByteBuffer bytes, int version) throws IOException
    {
        try (DataInputBuffer in = new DataInputBuffer(bytes, true))
        {
            return serializer.deserialize(in, version);
        }
    }

    /** Imports the given sstable files into the table or view {@code table}, verifying them first. */
    private void importInto(Path importRoot, String table, Collection<Path> files) throws IOException
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
