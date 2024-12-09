/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.db.filter;

import com.google.common.collect.Iterables;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class DseColumnFilterTest
{
    private final static DseColumnFilter.Serializer SERIALIZER_30 = new DseColumnFilter.Serializer(MessagingService.VERSION_30);
    private final static DseColumnFilter.Serializer SERIALIZER_3014 = new DseColumnFilter.Serializer(MessagingService.VERSION_3014);
    private final static DseColumnFilter.Serializer SERIALIZER_40 = new DseColumnFilter.Serializer(MessagingService.VERSION_40);
    private final static DseColumnFilter.Serializer SERIALIZER_DSE60 = new DseColumnFilter.Serializer(MessagingService.VERSION_DSE_68);

    @BeforeClass
    public static void beforeClass()
    {
        DatabaseDescriptor.clientInitialization();
    }

    @Test
    public void testDseColumnFilterSerialisationRoundTrip() throws Exception
    {
        TableMetadata metadata = TableMetadata.builder("ks", "table")
                                              .partitioner(Murmur3Partitioner.instance)
                                              .addPartitionKeyColumn("pk", Int32Type.instance)
                                              .addClusteringColumn("ck", Int32Type.instance)
                                              .addRegularColumn("v1", Int32Type.instance)
                                              .addRegularColumn("v2", Int32Type.instance)
                                              .addRegularColumn("v3", Int32Type.instance)
                                              .build();

        ColumnMetadata v1 = metadata.getColumn(ByteBufferUtil.bytes("v1"));

        DseColumnFilter columnFilter;

        columnFilter = DseColumnFilter.all(metadata);

        testRoundTrip(columnFilter, SERIALIZER_30.maybeUpdatedForBackwardCompatibility(columnFilter), metadata, SERIALIZER_30);
        testRoundTrip(columnFilter, SERIALIZER_3014.maybeUpdatedForBackwardCompatibility(columnFilter), metadata, SERIALIZER_3014);
        testRoundTrip(columnFilter, metadata, SERIALIZER_40);
        testRoundTrip(columnFilter, metadata, SERIALIZER_DSE60);

        testRoundTrip(DseColumnFilter.selection(metadata.regularAndStaticColumns().without(v1)), metadata, SERIALIZER_30);
        testRoundTrip(DseColumnFilter.selection(metadata.regularAndStaticColumns().without(v1)), metadata, SERIALIZER_3014);
        testRoundTrip(DseColumnFilter.selection(metadata.regularAndStaticColumns().without(v1)), metadata, SERIALIZER_40);
        testRoundTrip(DseColumnFilter.selection(metadata.regularAndStaticColumns().without(v1)), metadata, SERIALIZER_DSE60);

        columnFilter = DseColumnFilter.selection(metadata, metadata.regularAndStaticColumns().without(v1));
        testRoundTrip(columnFilter, SERIALIZER_30.maybeUpdatedForBackwardCompatibility(columnFilter), metadata, SERIALIZER_30);
        testRoundTrip(columnFilter, SERIALIZER_3014.maybeUpdatedForBackwardCompatibility(columnFilter), metadata, SERIALIZER_3014);
        testRoundTrip(columnFilter, metadata, SERIALIZER_40);
        testRoundTrip(columnFilter, metadata, SERIALIZER_DSE60);

        // Table with static column
        metadata = TableMetadata.builder("ks", "table")
                                .partitioner(Murmur3Partitioner.instance)
                                .addPartitionKeyColumn("pk", Int32Type.instance)
                                .addClusteringColumn("ck", Int32Type.instance)
                                .addStaticColumn("s1", Int32Type.instance)
                                .addRegularColumn("v1", Int32Type.instance)
                                .addRegularColumn("v2", Int32Type.instance)
                                .addRegularColumn("v3", Int32Type.instance)
                                .build();

        v1 = metadata.getColumn(ByteBufferUtil.bytes("v1"));
        ColumnMetadata s1 = metadata.getColumn(ByteBufferUtil.bytes("s1"));

        columnFilter = DseColumnFilter.all(metadata);
        testRoundTrip(columnFilter, SERIALIZER_30.maybeUpdatedForBackwardCompatibility(columnFilter), metadata, SERIALIZER_30);
        testRoundTrip(columnFilter, SERIALIZER_3014.maybeUpdatedForBackwardCompatibility(columnFilter), metadata, SERIALIZER_3014);
        testRoundTrip(columnFilter, metadata, SERIALIZER_40);
        testRoundTrip(columnFilter, metadata, SERIALIZER_DSE60);


        testRoundTrip(DseColumnFilter.selection(metadata.regularAndStaticColumns().without(v1)), metadata, SERIALIZER_30);
        testRoundTrip(DseColumnFilter.selection(metadata.regularAndStaticColumns().without(v1)), metadata, SERIALIZER_3014);
        testRoundTrip(DseColumnFilter.selection(metadata.regularAndStaticColumns().without(v1)), metadata, SERIALIZER_40);
        testRoundTrip(DseColumnFilter.selection(metadata.regularAndStaticColumns().without(v1)), metadata, SERIALIZER_DSE60);

        testRoundTrip(DseColumnFilter.selection(metadata.regularAndStaticColumns().without(v1).without(s1)), metadata, SERIALIZER_30);
        testRoundTrip(DseColumnFilter.selection(metadata.regularAndStaticColumns().without(v1).without(s1)), metadata, SERIALIZER_3014);
        testRoundTrip(DseColumnFilter.selection(metadata.regularAndStaticColumns().without(v1).without(s1)), metadata, SERIALIZER_40);
        testRoundTrip(DseColumnFilter.selection(metadata.regularAndStaticColumns().without(v1).without(s1)), metadata, SERIALIZER_DSE60);

        columnFilter = DseColumnFilter.selection(metadata, metadata.regularAndStaticColumns().without(v1));

        testRoundTrip(columnFilter, SERIALIZER_30.maybeUpdatedForBackwardCompatibility(columnFilter), metadata, SERIALIZER_30);
        testRoundTrip(columnFilter, SERIALIZER_3014.maybeUpdatedForBackwardCompatibility(columnFilter), metadata, SERIALIZER_3014);
        testRoundTrip(columnFilter, metadata, SERIALIZER_40);
        testRoundTrip(columnFilter, metadata, SERIALIZER_DSE60);

        columnFilter = DseColumnFilter.selection(metadata, metadata.regularAndStaticColumns().without(v1).without(s1));
        testRoundTrip(columnFilter, SERIALIZER_30.maybeUpdatedForBackwardCompatibility(columnFilter), metadata, SERIALIZER_30);
        testRoundTrip(columnFilter, SERIALIZER_3014.maybeUpdatedForBackwardCompatibility(columnFilter), metadata, SERIALIZER_3014);
        testRoundTrip(columnFilter, metadata, SERIALIZER_40);
        testRoundTrip(columnFilter, metadata, SERIALIZER_DSE60);
    }

    @Test
    public void testDseColumnFilterConstruction()
    {
        // all regular column
        TableMetadata metadata = TableMetadata.builder("ks", "table")
                                              .partitioner(Murmur3Partitioner.instance)
                                              .addPartitionKeyColumn("pk", Int32Type.instance)
                                              .addClusteringColumn("ck", Int32Type.instance)
                                              .addRegularColumn("v1", Int32Type.instance)
                                              .addRegularColumn("v2", Int32Type.instance)
                                              .addRegularColumn("v3", Int32Type.instance)
                                              .build();
        DseColumnFilter columnFilter = DseColumnFilter.all(metadata);
        assertTrue(columnFilter.fetchType == DseColumnFilter.FetchType.ALL_COLUMNS);
        assertEquals(metadata.regularAndStaticColumns(), columnFilter.fetched);
        assertNull(columnFilter.queried);
        assertEquals("*", columnFilter.toString());

        RegularAndStaticColumns queried = RegularAndStaticColumns.builder()
                                                                 .add(metadata.getColumn(ByteBufferUtil.bytes("v1"))).build();
        columnFilter = DseColumnFilter.selection(queried);
        assertFalse(columnFilter.fetchType == DseColumnFilter.FetchType.ALL_COLUMNS);
        assertEquals(queried, columnFilter.fetched);
        assertEquals(queried, columnFilter.queried);
        assertEquals("v1", columnFilter.toString());

        // with static column
        metadata = TableMetadata.builder("ks", "table")
                                .partitioner(Murmur3Partitioner.instance)
                                .addPartitionKeyColumn("pk", Int32Type.instance)
                                .addClusteringColumn("ck", Int32Type.instance)
                                .addStaticColumn("sc1", Int32Type.instance)
                                .addStaticColumn("sc2", Int32Type.instance)
                                .addRegularColumn("v1", Int32Type.instance)
                                .build();

        columnFilter = DseColumnFilter.all(metadata);
        assertTrue(columnFilter.fetchType == DseColumnFilter.FetchType.ALL_COLUMNS);
        assertEquals(metadata.regularAndStaticColumns(), columnFilter.fetched);
        assertNull(columnFilter.queried);
        assertEquals("*", columnFilter.toString());

        queried = RegularAndStaticColumns.builder()
                                         .add(metadata.getColumn(ByteBufferUtil.bytes("v1"))).build();
        columnFilter = DseColumnFilter.selection(metadata, queried);
        assertEquals("v1", columnFilter.toString());

        // only static
        metadata = TableMetadata.builder("ks", "table")
                                .partitioner(Murmur3Partitioner.instance)
                                .addPartitionKeyColumn("pk", Int32Type.instance)
                                .addClusteringColumn("ck", Int32Type.instance)
                                .addStaticColumn("sc", Int32Type.instance)
                                .build();

        columnFilter = DseColumnFilter.all(metadata);
        assertTrue(columnFilter.fetchType == DseColumnFilter.FetchType.ALL_COLUMNS);
        assertEquals(metadata.regularAndStaticColumns(), columnFilter.fetched);
        assertNull(columnFilter.queried);
        assertEquals("*", columnFilter.toString());

        // with collection type
        metadata = TableMetadata.builder("ks", "table")
                                .partitioner(Murmur3Partitioner.instance)
                                .addPartitionKeyColumn("pk", Int32Type.instance)
                                .addClusteringColumn("ck", Int32Type.instance)
                                .addRegularColumn("v1", Int32Type.instance)
                                .addRegularColumn("set", SetType.getInstance(Int32Type.instance, true))
                                .build();

        columnFilter = DseColumnFilter.all(metadata);
        assertTrue(columnFilter.fetchType == DseColumnFilter.FetchType.ALL_COLUMNS);
        assertEquals(metadata.regularAndStaticColumns(), columnFilter.fetched);
        assertNull(columnFilter.queried);
        assertEquals("*", columnFilter.toString());

        columnFilter = DseColumnFilter.selectionBuilder().add(metadata.getColumn(ByteBufferUtil.bytes("v1")))
                                   .select(metadata.getColumn(ByteBufferUtil.bytes("set")), CellPath.create(ByteBufferUtil.bytes(1)))
                                   .build();
        assertEquals("set[1], v1", columnFilter.toString());
    }

    @Test
    public void testDseColumnFilterSelectionTypes()
    {
        TableMetadata metadata = TableMetadata.builder("ks", "table")
                                              .partitioner(Murmur3Partitioner.instance)
                                              .addPartitionKeyColumn("pk", Int32Type.instance)
                                              .addStaticColumn("s1", Int32Type.instance)
                                              .addClusteringColumn("c1", Int32Type.instance)
                                              .addClusteringColumn("c2", Int32Type.instance)
                                              .addRegularColumn("complex1", SetType.getInstance(Int32Type.instance, true))
                                              .addRegularColumn("v1", Int32Type.instance)
                                              .addRegularColumn("v2", Int32Type.instance)
                                              .addRegularColumn("v3", Int32Type.instance)
                                              .build();

        ColumnMetadata v1 = metadata.getColumn(ByteBufferUtil.bytes("v1"));
        ColumnMetadata s1 = metadata.getColumn(ByteBufferUtil.bytes("s1"));
        ColumnMetadata complex1 = metadata.getColumn(ByteBufferUtil.bytes("complex1"));

        // filter fetches all
        DseColumnFilter filter = DseColumnFilter.all(metadata);
        assertEquals(metadata.regularAndStaticColumns(), filter.fetchedColumns());
        assertEquals(metadata.regularAndStaticColumns(), filter.queriedColumns());
        assertEquals("*", filter.toString());
        assertTrue(filter.allFetchedColumnsAreQueried());
        assertTrue(filter.fetchesAllColumns(false));
        assertTrue(filter.fetchesAllColumns(true));
        assertTrue(filter.fetchedCellIsQueried(complex1, CellPath.create(ByteBufferUtil.bytes("val1"))));
        for (ColumnMetadata c : Iterables.concat(metadata.regularAndStaticColumns().regulars,
                                                 metadata.regularAndStaticColumns().statics))
        {
            assertTrue(filter.fetches(c));
            assertTrue(filter.fetchedColumnIsQueried(c));
        }

        // filter fetches a selection only
        RegularAndStaticColumns selectedColumns = RegularAndStaticColumns.of(v1);
        filter = DseColumnFilter.selection(selectedColumns);
        assertEquals(selectedColumns, filter.fetchedColumns());
        assertEquals(selectedColumns, filter.queriedColumns());
        assertEquals("v1", filter.toString());
        assertTrue(filter.allFetchedColumnsAreQueried());
        assertFalse(filter.fetchesAllColumns(false));
        assertFalse(filter.fetchesAllColumns(true));
        for (ColumnMetadata c : Iterables.concat(metadata.regularAndStaticColumns().regulars,
                                                 metadata.regularAndStaticColumns().statics))
        {
            if (c.equals(v1))
            {
                assertTrue(filter.fetches(c));
                assertTrue(filter.fetchedColumnIsQueried(c));
            }
            else
            {
                assertFalse(filter.fetches(c));
            }
        }

        // filter fetches all but queries a selection only
        filter = DseColumnFilter.selection(metadata, selectedColumns);
        assertEquals(metadata.regularAndStaticColumns().without(s1), filter.fetchedColumns());
        assertEquals(selectedColumns, filter.queriedColumns());
        assertEquals("v1", filter.toString());
        assertFalse(filter.allFetchedColumnsAreQueried());
        assertTrue(filter.fetchesAllColumns(false));
        assertFalse(filter.fetchesAllColumns(true)); // queried != null
        assertFalse(filter.fetches(s1));
        for (ColumnMetadata c : metadata.regularAndStaticColumns().regulars)
        {
            assertTrue(filter.fetches(c));
            if (c.equals(v1))
            {
                assertTrue(filter.fetchedColumnIsQueried(c));
            }
            else
            {
                assertFalse(filter.fetchedColumnIsQueried(c));
            }
        }

        // filter queries static columns
        selectedColumns = RegularAndStaticColumns.of(s1);
        filter = DseColumnFilter.selection(selectedColumns);
        assertEquals(selectedColumns, filter.fetchedColumns());
        assertEquals(selectedColumns, filter.queriedColumns());
        assertEquals("s1", filter.toString());
        assertTrue(filter.allFetchedColumnsAreQueried());
        assertFalse(filter.fetchesAllColumns(false));
        assertFalse(filter.fetchesAllColumns(true));
        for (ColumnMetadata c : Iterables.concat(metadata.regularAndStaticColumns().regulars,
                                                 metadata.regularAndStaticColumns().statics))
        {
            if (c.equals(s1))
            {
                assertTrue(filter.fetches(c));
                assertTrue(filter.fetchedColumnIsQueried(c));
            }
            else
            {
                assertFalse(filter.fetches(c));
            }
        }

        // filter queries complex columns
        selectedColumns = RegularAndStaticColumns.of(complex1);
        filter = DseColumnFilter.selection(selectedColumns);
        assertEquals(selectedColumns, filter.fetchedColumns());
        assertEquals(selectedColumns, filter.queriedColumns());
        assertEquals("complex1", filter.toString());
        assertTrue(filter.allFetchedColumnsAreQueried());
        assertFalse(filter.fetchesAllColumns(false));
        assertFalse(filter.fetchesAllColumns(true));
        for (ColumnMetadata c : Iterables.concat(metadata.regularAndStaticColumns().regulars,
                                                 metadata.regularAndStaticColumns().statics))
        {
            if (c.equals(complex1))
            {
                assertTrue(filter.fetches(c));
                assertTrue(filter.fetchedColumnIsQueried(c));
                assertTrue(filter.fetchedCellIsQueried(c, CellPath.create(ByteBufferUtil.bytes("val1"))));
                assertNull(filter.newTester(c));
            }
            else
            {
                assertFalse(filter.fetches(c));
            }
        }

        // filter queries only a regular column and a sub-selection of a complex column
        selectedColumns = RegularAndStaticColumns.of(v1).mergeTo(RegularAndStaticColumns.of(complex1));
        filter = DseColumnFilter.selectionBuilder()
                             .add(metadata.getColumn(ByteBufferUtil.bytes("v1")))
                             .select(metadata.getColumn(ByteBufferUtil.bytes("complex1")), CellPath.create(ByteBufferUtil.bytes(1)))
                             .build();
        assertEquals(selectedColumns, filter.fetchedColumns());
        assertEquals(selectedColumns, filter.queriedColumns());
        assertEquals("complex1[1], v1", filter.toString());
        assertTrue(filter.allFetchedColumnsAreQueried());
        assertFalse(filter.fetchesAllColumns(false));
        assertFalse(filter.fetchesAllColumns(true));
        for (ColumnMetadata c : Iterables.concat(metadata.regularAndStaticColumns().regulars,
                                                 metadata.regularAndStaticColumns().statics))
        {
            if (c.equals(v1))
            {
                assertTrue(filter.fetches(c));
                assertTrue(filter.fetchedColumnIsQueried(c));
            }
            else if (c.equals(complex1))
            {
                CellPath b1 = CellPath.create(ByteBufferUtil.bytes(1)); // fetched and queried
                CellPath b2 = CellPath.create(ByteBufferUtil.bytes(2)); // not fetched, not queried

                assertTrue(filter.fetches(c));
                assertTrue(filter.fetchedColumnIsQueried(c));
                assertTrue(filter.fetchedCellIsQueried(c, b1));
                DseColumnFilter.Tester tester = filter.newTester(c);
                assertNotNull(tester);
                assertTrue(tester.fetches(b1));
                assertTrue(tester.fetchedCellIsQueried(b1));
                assertFalse(tester.fetches(b2));
            }
            else
            {
                assertFalse(filter.fetches(c));
            }
        }

        // filter fetches all columns but queries only a regular column and a sub-selection of a complex column
        selectedColumns = RegularAndStaticColumns.of(v1).mergeTo(RegularAndStaticColumns.of(complex1));
        filter = DseColumnFilter.allRegularColumnsBuilder(metadata)
                             .add(metadata.getColumn(ByteBufferUtil.bytes("v1")))
                             .select(metadata.getColumn(ByteBufferUtil.bytes("complex1")), CellPath.create(ByteBufferUtil.bytes(1)))
                             .build();
        assertEquals(metadata.regularAndStaticColumns().without(s1), filter.fetchedColumns());
        assertEquals(selectedColumns, filter.queriedColumns());
        assertEquals("complex1[1], v1", filter.toString());
        assertFalse(filter.allFetchedColumnsAreQueried());
        assertTrue(filter.fetchesAllColumns(false));
        assertFalse(filter.fetchesAllColumns(true));
        for (ColumnMetadata c : Iterables.concat(metadata.regularAndStaticColumns().regulars,
                                                 metadata.regularAndStaticColumns().statics))
        {
            if (c.equals(v1))
            {
                assertTrue(filter.fetches(c));
                assertTrue(filter.fetchedColumnIsQueried(c));
            }
            else if (c.equals(complex1))
            {
                CellPath b1 = CellPath.create(ByteBufferUtil.bytes(1)); // fetched and queried
                CellPath b2 = CellPath.create(ByteBufferUtil.bytes(2)); // fetched but not queried

                assertTrue(filter.fetches(c));
                assertTrue(filter.fetchedColumnIsQueried(c));
                assertTrue(filter.fetchedCellIsQueried(c, b1));
                assertFalse(filter.fetchedCellIsQueried(c, b2));

                DseColumnFilter.Tester tester = filter.newTester(c);
                assertNotNull(tester);
                assertTrue(tester.fetches(b1));
                assertTrue(tester.fetchedCellIsQueried(b1));
                assertTrue(tester.fetches(b2));
                assertFalse(tester.fetchedCellIsQueried(b2));
            }
            else if (c.equals(s1))
            {
                assertFalse(filter.fetches(c));
            }
            else
            {
                assertTrue(filter.fetches(c));
                assertFalse(filter.fetchedColumnIsQueried(c));
            }
        }
    }

    @Test
    public void testDseColumnFilterWithColumnsAdded()
    {
        TableMetadata metadata = TableMetadata.builder("ks", "table")
                                              .partitioner(Murmur3Partitioner.instance)
                                              .addPartitionKeyColumn("pk", Int32Type.instance)
                                              .addStaticColumn("s1", Int32Type.instance)
                                              .addClusteringColumn("c1", Int32Type.instance)
                                              .addClusteringColumn("c2", Int32Type.instance)
                                              .addRegularColumn("complex1", SetType.getInstance(Int32Type.instance, true))
                                              .addRegularColumn("v1", Int32Type.instance)
                                              .build();

        ColumnMetadata v1 = metadata.getColumn(ByteBufferUtil.bytes("v1"));
        ColumnMetadata s1 = metadata.getColumn(ByteBufferUtil.bytes("s1"));
        ColumnMetadata complex1 = metadata.getColumn(ByteBufferUtil.bytes("complex1"));

        TableMetadata augmentedMetadata = metadata.unbuild()
                                                  .addStaticColumn("s2", Int32Type.instance)
                                                  .addRegularColumn("v2", Int32Type.instance)
                                                  .addRegularColumn("complex2", SetType.getInstance(Int32Type.instance, true))
                                                  .build();

        ColumnMetadata v2 = augmentedMetadata.getColumn(ByteBufferUtil.bytes("v2"));
        ColumnMetadata s2 = augmentedMetadata.getColumn(ByteBufferUtil.bytes("s2"));
        ColumnMetadata complex2 = augmentedMetadata.getColumn(ByteBufferUtil.bytes("complex2"));

        // filter fetches all
        DseColumnFilter filter = DseColumnFilter.all(metadata);
        assertTrue(filter.fetchesAllColumns(true));
        assertTrue(filter.fetchesAllColumns(false));
        assertTrue(filter.fetches(v1));
        assertTrue(filter.fetches(s1));
        assertTrue(filter.fetches(complex1));
        assertTrue(filter.fetches(v2)); // poor filter doesn't know any better
        assertTrue(filter.fetches(s2));
        assertTrue(filter.fetches(complex2));

        // verify columns but using the same set of columns, shouldn't change anything
        filter = filter.withPartitionColumnsVerified(metadata.regularAndStaticColumns());
        assertTrue(filter.fetchesAllColumns(true));
        assertTrue(filter.fetchesAllColumns(false));
        assertTrue(filter.fetches(v1));
        assertTrue(filter.fetches(s1));
        assertTrue(filter.fetches(complex1));
        assertTrue(filter.fetches(v2)); // poor filter doesn't know any better
        assertTrue(filter.fetches(s2));
        assertTrue(filter.fetches(complex2));

        // not verify with the augmented columns, filter should conclude that it must check the fetched set in order to exclude v3
        filter = filter.withPartitionColumnsVerified(augmentedMetadata.regularAndStaticColumns());
        assertFalse(filter.fetchesAllColumns(true));
        assertFalse(filter.fetchesAllColumns(false)); // the filter cannot any longer be sure that all columns are fetched
        assertTrue(filter.fetches(v1));
        assertTrue(filter.fetches(s1));
        assertTrue(filter.fetches(complex1));
        assertFalse(filter.fetches(v2)); // the filter now gets it that these new columns should be excluded
        assertFalse(filter.fetches(s2));
        assertFalse(filter.fetches(complex2));
    }

    @Test
    public void testDseColumnFilterWithColumnsRemoved()
    {
        TableMetadata metadata = TableMetadata.builder("ks", "table")
                                              .partitioner(Murmur3Partitioner.instance)
                                              .addPartitionKeyColumn("pk", Int32Type.instance)
                                              .addStaticColumn("s1", Int32Type.instance)
                                              .addStaticColumn("s2", Int32Type.instance)
                                              .addClusteringColumn("c1", Int32Type.instance)
                                              .addClusteringColumn("c2", Int32Type.instance)
                                              .addRegularColumn("complex1", SetType.getInstance(Int32Type.instance, true))
                                              .addRegularColumn("complex2", SetType.getInstance(Int32Type.instance, true))
                                              .addRegularColumn("v1", Int32Type.instance)
                                              .addRegularColumn("v2", Int32Type.instance)
                                              .build();

        ColumnMetadata v1 = metadata.getColumn(ByteBufferUtil.bytes("v1"));
        ColumnMetadata s1 = metadata.getColumn(ByteBufferUtil.bytes("s1"));
        ColumnMetadata complex1 = metadata.getColumn(ByteBufferUtil.bytes("complex1"));

        ColumnMetadata v2 = metadata.getColumn(ByteBufferUtil.bytes("v2"));
        ColumnMetadata s2 = metadata.getColumn(ByteBufferUtil.bytes("s2"));
        ColumnMetadata complex2 = metadata.getColumn(ByteBufferUtil.bytes("complex2"));

        RegularAndStaticColumns reducedColumns = metadata.regularAndStaticColumns()
                                                         .without(v2)
                                                         .without(s2)
                                                         .without(complex2);

        // filter fetches all
        DseColumnFilter filter = DseColumnFilter.all(metadata);
        assertTrue(filter.fetchesAllColumns(true));
        assertTrue(filter.fetchesAllColumns(false));
        assertTrue(filter.fetches(v1));
        assertTrue(filter.fetches(s1));
        assertTrue(filter.fetches(complex1));
        assertTrue(filter.fetches(v2));
        assertTrue(filter.fetches(s2));
        assertTrue(filter.fetches(complex2));

        // verify with the reduced columns, filter should conclude that it can still safely fetch all columns because
        // reducing columns simply means that the iterators will not ask the filter for some columns that were dropped
        filter = filter.withPartitionColumnsVerified(reducedColumns);
        assertTrue(filter.fetchesAllColumns(true));
        assertTrue(filter.fetchesAllColumns(false)); // the filter cannot any longer be sure that all columns are fetched
        assertTrue(filter.fetches(v1));
        assertTrue(filter.fetches(s1));
        assertTrue(filter.fetches(complex1));
        assertTrue(filter.fetches(v2)); // the filter should not fetch the columns that were removed
        assertTrue(filter.fetches(s2));
        assertTrue(filter.fetches(complex2));
    }

    static void testRoundTrip(DseColumnFilter columnFilter, TableMetadata metadata, DseColumnFilter.Serializer serializer) throws Exception
    {
        testRoundTrip(columnFilter, columnFilter.withPartitionColumnsVerified(metadata.regularAndStaticColumns()), metadata, serializer);
    }

    @SuppressWarnings("resource")
    static void testRoundTrip(DseColumnFilter columnFilter, DseColumnFilter expected, TableMetadata metadata, DseColumnFilter.Serializer serializer) throws Exception
    {
        DataOutputBuffer output = new DataOutputBuffer();
        serializer.serialize(columnFilter, output);
        assertEquals(serializer.serializedSize(columnFilter), output.position());
        DataInputPlus input = new DataInputBuffer(output.buffer(), false);
        DseColumnFilter deserialized = serializer.deserialize(input, metadata);
        assertEquals(deserialized, expected);
    }
}
