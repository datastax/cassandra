/*
 * Copyright DataStax, Inc.
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

package org.apache.cassandra.index.sai.utils;

import java.util.function.Consumer;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PartitionInfoTest
{
    private static DecoratedKey partitionKey;
    private static Row staticRow;
    private static RegularAndStaticColumns columns;
    private static DeletionTime partitionDeletion;
    private static EncodingStats encodingStats;

    @BeforeClass
    public static void setup()
    {
        DatabaseDescriptor.setPartitionerUnsafe(new Murmur3Partitioner());
        TableMetadata tableMetadata = TableMetadata.builder("test", "test")
                                                   .partitioner(Murmur3Partitioner.instance)
                                                   .addPartitionKeyColumn("pk", Int32Type.instance)
                                                   .addRegularColumn("regular_col", Int32Type.instance)
                                                   .addStaticColumn("static_col", Int32Type.instance)
                                                   .build();

        partitionKey = Util.dk("test_key");
        staticRow = BTreeRow.emptyRow(Clustering.STATIC_CLUSTERING);
        columns = tableMetadata.regularAndStaticColumns();
        partitionDeletion = new DeletionTime(1000L, 10);
        encodingStats = new EncodingStats(500L, LivenessInfo.NO_EXPIRATION_TIME, 0);
    }

    @Test
    public void testCreateFromUnfilteredIterator()
    {
        UnfilteredRowIterator unfilteredIterator = unfilteredIterator();
        PartitionInfo partitionInfo = PartitionInfo.create(unfilteredIterator);

        assertNotNull(partitionInfo);
        assertSame(partitionKey, partitionInfo.key);
        assertSame(staticRow, partitionInfo.staticRow);
        assertSame(columns, partitionInfo.columns);
        assertSame(partitionDeletion, partitionInfo.partitionDeletion);
        assertSame(encodingStats, partitionInfo.encodingStats);
    }

    @Test
    public void testCreateFromRowIterator()
    {
        RowIterator rowIterator = rowIterator();
        PartitionInfo partitionInfo = PartitionInfo.create(rowIterator);

        assertNotNull(partitionInfo);
        assertSame(partitionKey, partitionInfo.key);
        assertSame(staticRow, partitionInfo.staticRow);
        assertSame(columns, partitionInfo.columns);
        assertNull(partitionInfo.partitionDeletion);
        assertNull(partitionInfo.encodingStats);
    }

    @Test
    public void testEqualsAndHashCode()
    {
        PartitionInfo info1 = PartitionInfo.create(unfilteredIterator());
        PartitionInfo info2 = PartitionInfo.create(unfilteredIterator());

        assertEquals(info1, info2);
        assertEquals(info1.hashCode(), info2.hashCode());
    }

    @Test
    public void testEqualsWithDifferentKeys()
    {
        testEqualsWithDifferentAttribute(iterator -> {
            DecoratedKey differentKey = Util.dk("different_key");
            when(iterator.partitionKey()).thenReturn(differentKey);
        });
    }

    @Test
    public void testEqualsWithDifferentStaticRow()
    {
        testEqualsWithDifferentAttribute(iterator -> {
            BufferCell cell = BufferCell.live(columns.statics.getSimple(0), 0, Int32Type.instance.decompose(0));
            Row differentStaticRow = BTreeRow.singleCellRow(Clustering.STATIC_CLUSTERING, cell);
            when(iterator.staticRow()).thenReturn(differentStaticRow);
        });
    }

    @Test
    public void testEqualsWithDifferentColumns()
    {
        testEqualsWithDifferentAttribute(iterator -> {
            ColumnMetadata differentColumn = ColumnMetadata.regularColumn("test", "test", "different_col", Int32Type.instance);
            RegularAndStaticColumns differentColumns = RegularAndStaticColumns.of(differentColumn);
            when(iterator.columns()).thenReturn(differentColumns);
        });
    }

    @Test
    public void testEqualsWithDifferentDeletionTime()
    {
        testEqualsWithDifferentAttribute(iterator -> {
            DeletionTime differentDeletion = new DeletionTime(1001L, 11);
            when(iterator.partitionLevelDeletion()).thenReturn(differentDeletion);
        });
    }

    @Test
    public void testEqualsWithDifferentEncodingStats()
    {
        testEqualsWithDifferentAttribute(iterator -> {
            EncodingStats differentStats = new EncodingStats(501L, LivenessInfo.NO_EXPIRATION_TIME, 0);
            when(iterator.stats()).thenReturn(differentStats);
        });
    }

    private static void testEqualsWithDifferentAttribute(Consumer<UnfilteredRowIterator> attributeChanger)
    {
        UnfilteredRowIterator iterator1 = unfilteredIterator();
        UnfilteredRowIterator iterator2 = unfilteredIterator();

        attributeChanger.accept(iterator2);

        assertNotEquals(PartitionInfo.create(iterator1),
                        PartitionInfo.create(iterator2));
    }

    @Test
    public void testEqualsWithNullValues()
    {
        PartitionInfo info1 = PartitionInfo.create(rowIterator());
        PartitionInfo info2 = PartitionInfo.create(rowIterator());

        assertEquals(info1, info2);
        assertEquals(info1.hashCode(), info2.hashCode());
    }

    @Test
    public void testEqualsSameInstance()
    {
        UnfilteredRowIterator iterator = unfilteredIterator();
        PartitionInfo partitionInfo = PartitionInfo.create(iterator);
        assertEquals(partitionInfo, partitionInfo);
    }

    @Test
    public void testEqualsWithNull()
    {
        PartitionInfo partitionInfo = PartitionInfo.create(rowIterator());
        assertNotEquals(null, partitionInfo);
    }

    @Test
    public void testEqualsWithDifferentClass()
    {
        PartitionInfo partitionInfo = PartitionInfo.create(rowIterator());
        assertNotEquals("not a PartitionInfo", partitionInfo);
    }

    private static UnfilteredRowIterator unfilteredIterator()
    {
        UnfilteredRowIterator iterator = mock(UnfilteredRowIterator.class);
        when(iterator.partitionKey()).thenReturn(partitionKey);
        when(iterator.staticRow()).thenReturn(staticRow);
        when(iterator.columns()).thenReturn(columns);
        when(iterator.partitionLevelDeletion()).thenReturn(partitionDeletion);
        when(iterator.stats()).thenReturn(encodingStats);
        return iterator;
    }

    private static RowIterator rowIterator()
    {
        RowIterator iterator = mock(RowIterator.class);
        when(iterator.partitionKey()).thenReturn(partitionKey);
        when(iterator.staticRow()).thenReturn(staticRow);
        when(iterator.columns()).thenReturn(columns);
        return iterator;
    }
}
