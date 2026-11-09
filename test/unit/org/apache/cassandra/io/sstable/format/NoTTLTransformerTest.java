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

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.OutputHandler;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link SortedTableScrubber.NoTTLTransformer}: expiring liveness info and cells become permanent.
 * By default they keep their timestamp; when timestamp bumping is requested they get the original timestamp
 * incremented by one (so that they supersede un-scrubbed expiring copies and their expiration tombstones).
 * Everything that never had a TTL is left untouched.
 */
public class NoTTLTransformerTest
{
    private static TableMetadata metadata;
    private static ColumnMetadata mapCol;
    private static ColumnMetadata valCol;
    private static ColumnMetadata permanentCol;

    @BeforeClass
    public static void setUp()
    {
        DatabaseDescriptor.daemonInitialization();
        metadata = TableMetadata.builder("ks", "no_ttl_transformer")
                                .addPartitionKeyColumn("k", Int32Type.instance)
                                .addRegularColumn("m", MapType.getInstance(UTF8Type.instance, UTF8Type.instance, true))
                                .addRegularColumn("v", UTF8Type.instance)
                                .addRegularColumn("w", UTF8Type.instance)
                                .build();
        mapCol = metadata.getColumn(ByteBufferUtil.bytes("m"));
        valCol = metadata.getColumn(ByteBufferUtil.bytes("v"));
        permanentCol = metadata.getColumn(ByteBufferUtil.bytes("w"));
    }

    private static SortedTableScrubber.NoTTLTransformer transformer(SortedTableScrubber.OverwrittenTTLInfoMetrics metrics)
    {
        return new SortedTableScrubber.NoTTLTransformer(new OutputHandler.LogOutput(), metadata, metrics);
    }

    private static SortedTableScrubber.NoTTLTransformer bumpingTransformer(SortedTableScrubber.OverwrittenTTLInfoMetrics metrics)
    {
        return new SortedTableScrubber.NoTTLTransformer(new OutputHandler.LogOutput(), metadata, metrics, true);
    }

    private static ComplexColumnData complexColumn(Row row, ColumnMetadata column)
    {
        for (ColumnData cd : row)
            if (cd.column().equals(column))
                return (ComplexColumnData) cd;
        return null;
    }

    @Test
    public void testExpiringLivenessAndCellsKeepTheirTimestampByDefault()
    {
        long nowInSec = FBUtilities.nowInSeconds();
        int ttl = 3600;

        Row.Builder builder = BTreeRow.sortedBuilder();
        builder.newRow(Clustering.EMPTY);
        builder.addPrimaryKeyLivenessInfo(LivenessInfo.create(1000, ttl, nowInSec));
        builder.addCell(BufferCell.expiring(mapCol, 3000, ttl, nowInSec, ByteBufferUtil.bytes("va"), CellPath.create(ByteBufferUtil.bytes("a"))));
        builder.addCell(BufferCell.live(mapCol, 4000, ByteBufferUtil.bytes("vb"), CellPath.create(ByteBufferUtil.bytes("b"))));
        builder.addCell(BufferCell.expiring(valCol, 1000, ttl, nowInSec, ByteBufferUtil.bytes("val")));
        builder.addCell(BufferCell.live(permanentCol, 2000, ByteBufferUtil.bytes("perm")));
        Row row = builder.build();

        SortedTableScrubber.OverwrittenTTLInfoMetrics metrics = new SortedTableScrubber.OverwrittenTTLInfoMetrics();
        Row result = transformer(metrics).applyToStatic(row);

        assertEquals(1, metrics.getNoTTLOverwrittenRows());
        assertFalse(SortedTableScrubber.NoTTLTransformer.hasAnyTTL(result));

        assertFalse(result.primaryKeyLivenessInfo().isExpiring());
        assertEquals(1000, result.primaryKeyLivenessInfo().timestamp());

        Cell<?> val = result.getCell(valCol);
        assertFalse(val.isExpiring());
        assertEquals(1000, val.timestamp());
        assertEquals(ByteBufferUtil.bytes("val"), val.buffer());

        Cell<?> a = complexColumn(result, mapCol).getCell(CellPath.create(ByteBufferUtil.bytes("a")));
        assertFalse(a.isExpiring());
        assertEquals(3000, a.timestamp());
    }

    @Test
    public void testExpiringLivenessAndCellsAreRewrittenWithTimestampPlusOneWhenBumping()
    {
        long nowInSec = FBUtilities.nowInSeconds();
        int ttl = 3600;

        Row.Builder builder = BTreeRow.sortedBuilder();
        builder.newRow(Clustering.EMPTY);
        builder.addPrimaryKeyLivenessInfo(LivenessInfo.create(1000, ttl, nowInSec));
        builder.addCell(BufferCell.expiring(mapCol, 3000, ttl, nowInSec, ByteBufferUtil.bytes("va"), CellPath.create(ByteBufferUtil.bytes("a"))));
        builder.addCell(BufferCell.live(mapCol, 4000, ByteBufferUtil.bytes("vb"), CellPath.create(ByteBufferUtil.bytes("b"))));
        builder.addCell(BufferCell.expiring(valCol, 1000, ttl, nowInSec, ByteBufferUtil.bytes("val")));
        builder.addCell(BufferCell.live(permanentCol, 2000, ByteBufferUtil.bytes("perm")));
        Row row = builder.build();

        SortedTableScrubber.OverwrittenTTLInfoMetrics metrics = new SortedTableScrubber.OverwrittenTTLInfoMetrics();
        Row result = bumpingTransformer(metrics).applyToStatic(row);

        assertEquals(1, metrics.getNoTTLOverwrittenRows());
        assertEquals(row.clustering(), result.clustering());

        LivenessInfo liveness = result.primaryKeyLivenessInfo();
        assertFalse(liveness.isExpiring());
        assertEquals(1001, liveness.timestamp());

        Cell<?> val = result.getCell(valCol);
        assertFalse(val.isExpiring());
        assertEquals(1001, val.timestamp());
        assertEquals(ByteBufferUtil.bytes("val"), val.buffer());

        Cell<?> perm = result.getCell(permanentCol);
        assertFalse(perm.isExpiring());
        assertEquals("cells without TTL keep their timestamp", 2000, perm.timestamp());

        ComplexColumnData map = complexColumn(result, mapCol);
        assertNotNull(map);
        Cell<?> a = map.getCell(CellPath.create(ByteBufferUtil.bytes("a")));
        assertFalse(a.isExpiring());
        assertEquals(3001, a.timestamp());
        assertEquals(ByteBufferUtil.bytes("va"), a.buffer());
        Cell<?> b = map.getCell(CellPath.create(ByteBufferUtil.bytes("b")));
        assertEquals("complex cells without TTL keep their timestamp", 4000, b.timestamp());

        assertFalse(SortedTableScrubber.NoTTLTransformer.hasAnyTTL(result));
    }

    @Test
    public void testAlreadyExpiredCellIsResurrectedWithTimestampPlusOneWhenBumping()
    {
        long nowInSec = FBUtilities.nowInSeconds();
        Row.Builder builder = BTreeRow.sortedBuilder();
        builder.newRow(Clustering.EMPTY);
        builder.addPrimaryKeyLivenessInfo(LivenessInfo.create(1000, nowInSec));
        // expired 10 seconds ago, not yet purged
        builder.addCell(BufferCell.expiring(valCol, 1000, 1, nowInSec - 10, ByteBufferUtil.bytes("val")));
        Row row = builder.build();
        assertFalse(row.getCell(valCol).isLive(nowInSec));

        SortedTableScrubber.OverwrittenTTLInfoMetrics metrics = new SortedTableScrubber.OverwrittenTTLInfoMetrics();
        Row result = bumpingTransformer(metrics).applyToStatic(row);

        Cell<?> val = result.getCell(valCol);
        assertTrue(val.isLive(nowInSec));
        assertFalse(val.isExpiring());
        assertEquals(1001, val.timestamp());
        assertEquals("row liveness without TTL keeps its timestamp", 1000, result.primaryKeyLivenessInfo().timestamp());
        assertEquals(1, metrics.getNoTTLOverwrittenRows());
    }

    @Test
    public void testRowWithoutTTLIsPassedThroughUntouched()
    {
        long nowInSec = FBUtilities.nowInSeconds();
        Row.Builder builder = BTreeRow.sortedBuilder();
        builder.newRow(Clustering.EMPTY);
        builder.addPrimaryKeyLivenessInfo(LivenessInfo.create(1000, nowInSec));
        builder.addCell(BufferCell.live(valCol, 1000, ByteBufferUtil.bytes("val")));
        builder.addCell(BufferCell.tombstone(permanentCol, 5000, nowInSec));
        Row row = builder.build();

        SortedTableScrubber.OverwrittenTTLInfoMetrics metrics = new SortedTableScrubber.OverwrittenTTLInfoMetrics();
        Row result = transformer(metrics).applyToStatic(row);

        assertSame(row, result);
        assertEquals(0, metrics.getNoTTLOverwrittenRows());
    }

    @Test
    public void testTransformIsIdempotent()
    {
        long nowInSec = FBUtilities.nowInSeconds();
        Row.Builder builder = BTreeRow.sortedBuilder();
        builder.newRow(Clustering.EMPTY);
        builder.addPrimaryKeyLivenessInfo(LivenessInfo.create(1000, 3600, nowInSec));
        builder.addCell(BufferCell.expiring(valCol, 1000, 3600, nowInSec, ByteBufferUtil.bytes("val")));
        Row row = builder.build();

        SortedTableScrubber.OverwrittenTTLInfoMetrics metrics = new SortedTableScrubber.OverwrittenTTLInfoMetrics();
        Row once = bumpingTransformer(metrics).applyToStatic(row);
        Row twice = bumpingTransformer(metrics).applyToStatic(once);

        // a second NO_TTL scrub sees no TTL and must not bump the timestamp again
        assertSame(once, twice);
        assertEquals(1001, twice.primaryKeyLivenessInfo().timestamp());
        assertEquals(1001, twice.getCell(valCol).timestamp());
        assertEquals(1, metrics.getNoTTLOverwrittenRows());
    }
}
