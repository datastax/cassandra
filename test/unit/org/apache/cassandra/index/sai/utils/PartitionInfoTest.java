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
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.schema.TableMetadata;

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
        UnfilteredRowIterator iterator = mock(UnfilteredRowIterator.class);
        when(iterator.partitionKey()).thenReturn(partitionKey);
        when(iterator.staticRow()).thenReturn(staticRow);
        when(iterator.columns()).thenReturn(columns);
        when(iterator.partitionLevelDeletion()).thenReturn(partitionDeletion);
        when(iterator.stats()).thenReturn(encodingStats);

        PartitionInfo info = PartitionInfo.create(iterator);

        assertNotNull(info);
        assertSame(partitionKey, info.key);
        assertSame(staticRow, info.staticRow);
        assertSame(columns, info.columns);
        assertSame(partitionDeletion, info.partitionDeletion);
        assertSame(encodingStats, info.encodingStats);
    }

    @Test
    public void testCreateFromRowIterator()
    {
        RowIterator iterator = mock(RowIterator.class);
        when(iterator.partitionKey()).thenReturn(partitionKey);
        when(iterator.staticRow()).thenReturn(staticRow);
        when(iterator.columns()).thenReturn(columns);

        PartitionInfo info = PartitionInfo.create(iterator);

        assertNotNull(info);
        assertSame(partitionKey, info.key);
        assertSame(staticRow, info.staticRow);
        assertSame(columns, info.columns);
        assertNull(info.partitionDeletion);
        assertNull(info.encodingStats);
    }
}
