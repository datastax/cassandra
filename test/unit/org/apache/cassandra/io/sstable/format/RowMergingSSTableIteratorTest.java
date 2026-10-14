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

package org.apache.cassandra.io.sstable.format;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.rows.AbstractUnfilteredRowIterator;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellData;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.RangeTombstoneBoundMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.sstable.format.bti.BtiFormat;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.OutputHandler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests of {@link SortedTableScrubber.RowMergingSSTableIterator}, the iterator that merges duplicate rows and
 * handles the rows with an overflowed local expiration time while scrubbing.
 */
public class RowMergingSSTableIteratorTest
{
    private static final int TTL = 3600;
    // a negative signed int local expiration time decoded as an unsigned one
    private static final long UNSIGNED_OVERFLOW = (1L << 31) + 1000;

    private static TableMetadata metadata;
    private static ColumnMetadata column;
    private static ColumnMetadata setColumn;
    private static DecoratedKey key;
    private static Version currentVersion;
    private static Version legacyVersion;
    private static Version legacyBtiVersion;

    @BeforeClass
    public static void setUpClass()
    {
        DatabaseDescriptor.daemonInitialization();
        metadata = TableMetadata.builder("ks", "tbl")
                                .addPartitionKeyColumn("pk", Int32Type.instance)
                                .addClusteringColumn("ck", Int32Type.instance)
                                .addRegularColumn("v", Int32Type.instance)
                                .addRegularColumn("s", SetType.getInstance(Int32Type.instance, true))
                                .build();
        column = metadata.getColumn(ByteBufferUtil.bytes("v"));
        setColumn = metadata.getColumn(ByteBufferUtil.bytes("s"));
        key = metadata.partitioner.decorateKey(ByteBufferUtil.bytes(1));
        currentVersion = BigFormat.getInstance().getVersion("oa");
        legacyVersion = BigFormat.getInstance().getVersion("nb");
        assertThat(currentVersion.hasUIntDeletionTime()).isTrue();
        assertThat(legacyVersion.hasUIntDeletionTime()).isFalse();
        // legacy BTI sstables can hold expiration times after 2038
        legacyBtiVersion = BtiFormat.getInstance().getVersion("cc");
        assertThat(legacyBtiVersion.hasUIntDeletionTime()).isFalse();
    }

    @Test
    public void testRangeTombstoneMarkersPassThrough()
    {
        // a marker right after a row is the element the iterator peeks while looking for duplicates
        List<Unfiltered> input = Arrays.asList(row(1, 1, 10),
                                               open(2, 20),
                                               close(4, 20),
                                               row(5, 5, 10),
                                               open(6, 20),
                                               close(7, 20));
        assertThat(drain(iterator(input, currentVersion, false))).containsExactlyElementsOf(input);
    }

    @Test
    public void testOnlyRangeTombstoneMarkers()
    {
        List<Unfiltered> input = Arrays.asList(open(2, 20), close(4, 20));
        assertThat(drain(iterator(input, currentVersion, false))).containsExactlyElementsOf(input);
    }

    @Test
    public void testDuplicateRowsAreMerged()
    {
        CapturingOutput output = new CapturingOutput();
        Row older = row(1, 1, 10);
        Row newer = row(1, 2, 20);
        Unfiltered open = open(2, 30);
        Unfiltered close = close(3, 30);
        Row last = row(4, 4, 10);
        Row duplicateOfLast = row(4, 5, 20);

        List<Unfiltered> result = drain(iterator(Arrays.asList(older, newer, open, close, last, duplicateOfLast),
                                                 currentVersion, false, output));

        assertThat(result).containsExactly(Rows.merge(older, newer), open, close, Rows.merge(last, duplicateOfLast));
        assertThat(((Row) result.get(0)).getCell(column).buffer()).isEqualTo(ByteBufferUtil.bytes(2));
        assertThat(((Row) result.get(3)).getCell(column).buffer()).isEqualTo(ByteBufferUtil.bytes(5));
        assertThat(output.warnings).hasSize(2).allMatch(w -> w.startsWith("Duplicate row detected in ks.tbl"));
    }

    @Test
    public void testOverflowedRowsAreDropped()
    {
        Row live = row(1, 1, 10);
        Unfiltered open = open(3, 20);
        Unfiltered close = close(4, 20);
        List<Unfiltered> input = Arrays.asList(overflowedRow(0), live, overflowedRow(2), open, close, overflowedRow(5));
        CapturingOutput output = new CapturingOutput();

        SortedTableScrubber.RowMergingSSTableIterator iterator = iterator(input, legacyVersion, false, output);
        assertThat(drain(iterator)).containsExactly(live, open, close);
        assertThat(iterator.droppedRows()).isEqualTo(3);
        assertThat(output.warnings).isEmpty();
    }

    @Test
    public void testTrailingOverflowedRowIsDropped()
    {
        Row live = row(1, 1, 10);
        UnfilteredRowIterator iterator = iterator(Arrays.asList(live, overflowedRow(2)), legacyVersion, false);

        assertThat(iterator.hasNext()).isTrue();
        assertThat(iterator.next()).isEqualTo(live);
        // the dropped trailing row must not make hasNext() return true and next() return null
        assertThat(iterator.hasNext()).isFalse();
        assertThat(iterator.hasNext()).isFalse();
        assertThatThrownBy(iterator::next).isInstanceOf(NoSuchElementException.class);
    }

    @Test
    public void testOnlyOverflowedRows()
    {
        UnfilteredRowIterator iterator = iterator(Arrays.asList(overflowedRow(1), overflowedRow(2)), legacyVersion, false);
        assertThat(iterator.hasNext()).isFalse();
    }

    @Test
    public void testOverflowedRowsAreKeptWhenReinserting()
    {
        Unfiltered open = open(3, 20);
        Unfiltered close = close(4, 20);
        List<Unfiltered> input = Arrays.asList(overflowedRow(1), open, close, overflowedRow(5));

        SortedTableScrubber.RowMergingSSTableIterator iterator = iterator(input, legacyVersion, true);
        List<Unfiltered> result = drain(iterator);

        assertThat(result).hasSize(4);
        assertThat(result.get(0).clustering()).isEqualTo(clustering(1));
        assertThat(result.get(1)).isEqualTo(open);
        assertThat(result.get(2)).isEqualTo(close);
        assertThat(result.get(3).clustering()).isEqualTo(clustering(5));
        assertThat(iterator.droppedRows()).isZero();
    }

    @Test
    public void testOverflowedExpirationTimesOfLegacyBigSSTables()
    {
        // these sstables were always written with the 2038 cap
        long cap = CellData.MAX_DELETION_TIME_2038_LEGACY_CAP;
        Row atCap = rowWithLiveness(1, LivenessInfo.withExpirationTime(10, TTL, cap));
        Row aboveCap = rowWithLiveness(2, LivenessInfo.withExpirationTime(10, TTL, cap + 1));
        Row unsigned = rowWithLiveness(3, LivenessInfo.withExpirationTime(10, TTL, UNSIGNED_OVERFLOW));
        Row invalid = overflowedRow(4);
        Row simpleCell = BTreeRow.singleCellRow(clustering(5), expiringCell(column, null, CellData.INVALID_DELETION_TIME));
        Row complexCell = complexRow(6, UNSIGNED_OVERFLOW);

        SortedTableScrubber.RowMergingSSTableIterator iterator = iterator(Arrays.asList(atCap, aboveCap, unsigned, invalid, simpleCell, complexCell),
                                                                          legacyVersion, false);
        assertThat(drain(iterator)).containsExactly(atCap);
        assertThat(iterator.droppedRows()).isEqualTo(5);
    }

    @Test
    public void testOverflowedExpirationTimesOfLegacyBtiSSTables()
    {
        // these sstables can hold expiration times after 2038, only an invalid one has overflowed
        long after2038 = CellData.MAX_DELETION_TIME_2038_LEGACY_CAP + 365L * 24 * 3600;
        Row row = rowWithLiveness(1, LivenessInfo.withExpirationTime(10, TTL, after2038));
        Row cell = BTreeRow.singleCellRow(clustering(2), expiringCell(column, null, after2038));
        Row unsigned = complexRow(3, UNSIGNED_OVERFLOW);
        Row invalid = overflowedRow(4);
        Row simpleCell = BTreeRow.singleCellRow(clustering(5), expiringCell(column, null, CellData.INVALID_DELETION_TIME));
        Row complexCell = complexRow(6, CellData.INVALID_DELETION_TIME);

        SortedTableScrubber.RowMergingSSTableIterator iterator = iterator(Arrays.asList(row, cell, unsigned, invalid, simpleCell, complexCell),
                                                                          legacyBtiVersion, false);
        assertThat(drain(iterator)).containsExactly(row, cell, unsigned);
        assertThat(iterator.droppedRows()).isEqualTo(3);
    }

    @Test
    public void testExpiringRowsOfLegacySSTablesAreKept()
    {
        // a row with a TTL that has not overflowed must survive the scrub of a legacy sstable
        long localExpirationTime = FBUtilities.nowInSeconds() + TTL;
        Row expiring = rowWithLiveness(1, LivenessInfo.withExpirationTime(10, TTL, localExpirationTime));
        Row expiringCell = BTreeRow.singleCellRow(clustering(2), BufferCell.expiring(column, 10, TTL, FBUtilities.nowInSeconds(), ByteBufferUtil.bytes(2)));

        assertThat(drain(iterator(Arrays.asList(expiring, expiringCell), legacyVersion, false))).containsExactly(expiring, expiringCell);
        assertThat(drain(iterator(Arrays.asList(expiring, expiringCell), legacyBtiVersion, false))).containsExactly(expiring, expiringCell);
    }

    @Test
    public void testOverflowedRowsOfCurrentSSTablesAreNotDropped()
    {
        Row row = overflowedRow(1);
        assertThat(drain(iterator(Arrays.asList(row), currentVersion, false))).containsExactly(row);
    }

    private static List<Unfiltered> drain(UnfilteredRowIterator iterator)
    {
        List<Unfiltered> result = new ArrayList<>();
        while (iterator.hasNext())
        {
            Unfiltered next = iterator.next();
            assertThat(next).isNotNull();
            result.add(next);
        }
        assertThatThrownBy(iterator::next).isInstanceOf(NoSuchElementException.class);
        return result;
    }

    private static SortedTableScrubber.RowMergingSSTableIterator iterator(List<Unfiltered> content, Version version, boolean reinsertOverflowedTTLRows)
    {
        return iterator(content, version, reinsertOverflowedTTLRows, new CapturingOutput());
    }

    private static SortedTableScrubber.RowMergingSSTableIterator iterator(List<Unfiltered> content, Version version, boolean reinsertOverflowedTTLRows, OutputHandler output)
    {
        Iterator<Unfiltered> source = content.iterator();
        UnfilteredRowIterator wrapped = new AbstractUnfilteredRowIterator(metadata,
                                                                          key,
                                                                          DeletionTime.LIVE,
                                                                          RegularAndStaticColumns.of(column),
                                                                          Rows.EMPTY_STATIC_ROW,
                                                                          false,
                                                                          EncodingStats.NO_STATS)
        {
            @Override
            protected Unfiltered computeNext()
            {
                return source.hasNext() ? source.next() : endOfData();
            }
        };
        return new SortedTableScrubber.RowMergingSSTableIterator(wrapped, output, version, reinsertOverflowedTTLRows);
    }

    private static Clustering<?> clustering(int ck)
    {
        return Clustering.make(ByteBufferUtil.bytes(ck));
    }

    private static Row row(int ck, int value, long timestamp)
    {
        Row.Builder builder = BTreeRow.unsortedBuilder();
        builder.newRow(clustering(ck));
        builder.addPrimaryKeyLivenessInfo(LivenessInfo.create(timestamp, FBUtilities.nowInSeconds()));
        builder.addCell(BufferCell.live(column, timestamp, ByteBufferUtil.bytes(value)));
        return builder.build();
    }

    private static Row rowWithLiveness(int ck, LivenessInfo liveness)
    {
        Row.Builder builder = BTreeRow.unsortedBuilder();
        builder.newRow(clustering(ck));
        builder.addPrimaryKeyLivenessInfo(liveness);
        return builder.build();
    }

    /**
     * A row as read from a legacy sstable holding a local expiration time that overflowed the signed 32-bit integer
     * (CASSANDRA-14092), see {@link org.apache.cassandra.db.rows.Cell#decodeLocalDeletionTime}.
     */
    private static Row overflowedRow(int ck)
    {
        return rowWithLiveness(ck, LivenessInfo.withExpirationTime(10, TTL, CellData.INVALID_DELETION_TIME));
    }

    private static Cell<?> expiringCell(ColumnMetadata c, CellPath path, long localExpirationTime)
    {
        return new BufferCell(c, 10, TTL, localExpirationTime, path == null ? ByteBufferUtil.bytes(1) : ByteBufferUtil.EMPTY_BYTE_BUFFER, path);
    }

    /** A row whose only overflowed expiration time is the one of the second element of a set. */
    private static Row complexRow(int ck, long localExpirationTime)
    {
        Row.Builder builder = BTreeRow.unsortedBuilder();
        builder.newRow(clustering(ck));
        builder.addCell(expiringCell(setColumn, CellPath.create(ByteBufferUtil.bytes(1)), FBUtilities.nowInSeconds() + TTL));
        builder.addCell(expiringCell(setColumn, CellPath.create(ByteBufferUtil.bytes(2)), localExpirationTime));
        return builder.build();
    }

    private static Unfiltered open(int ck, long timestamp)
    {
        return RangeTombstoneBoundMarker.inclusiveOpen(false, clustering(ck), DeletionTime.build(timestamp, FBUtilities.nowInSeconds()));
    }

    private static Unfiltered close(int ck, long timestamp)
    {
        return RangeTombstoneBoundMarker.inclusiveClose(false, clustering(ck), DeletionTime.build(timestamp, FBUtilities.nowInSeconds()));
    }

    private static class CapturingOutput extends OutputHandler.LogOutput
    {
        final List<String> warnings = new ArrayList<>();

        @Override
        public void warn(String msg)
        {
            warnings.add(msg);
            super.warn(msg);
        }
    }
}
