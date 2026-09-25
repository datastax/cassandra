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

package org.apache.cassandra.db.filter;

import java.io.IOException;
import java.util.NavigableSet;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIteratorSerializer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.btree.BTreeSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests {@link ClusteringIndexFilter#filterNotIndexed(ColumnFilter, UnfilteredRowIterator)} on both
 * {@link ClusteringIndexSliceFilter} and {@link ClusteringIndexNamesFilter}: the returned iterator must advertise
 * the fetched columns of the column filter rather than the columns of the source iterator, otherwise a source
 * with more columns than the query fetches (for instance the full partition read that populates the row cache)
 * produces an iterator that cannot be serialized for that column filter.
 * <p>
 * The row cache only ever calls this method with a slice filter (see {@code SinglePartitionReadCommand#getThroughCache}
 * and {@link ClusteringIndexNamesFilter#isHeadFilter()}), so the names filter is covered here directly.
 */
public class ClusteringIndexFilterNotIndexedTest extends CQLTester
{
    private static final int KEY = 1;

    @Test
    public void testSliceFilter() throws IOException
    {
        createAndPopulate();
        checkFilter(new ClusteringIndexSliceFilter(Slices.ALL, false), 2);
    }

    @Test
    public void testReversedSliceFilter() throws IOException
    {
        createAndPopulate();
        checkFilter(new ClusteringIndexSliceFilter(Slices.ALL, true), 2);
    }

    @Test
    public void testNamesFilter() throws IOException
    {
        TableMetadata metadata = createAndPopulate();
        NavigableSet<Clustering<?>> clusterings = BTreeSet.of(metadata.comparator, metadata.comparator.make(2));
        checkFilter(new ClusteringIndexNamesFilter(clusterings, false), 1);
    }

    private TableMetadata createAndPopulate()
    {
        createTable("CREATE TABLE %s (k int, c int, s1 int static, s2 int static, s3 int static, r1 int, r2 int, " +
                    "PRIMARY KEY (k, c))");
        execute("INSERT INTO %s (k, c, s1, s2, s3, r1, r2) VALUES (?, 1, 10, 20, 30, 100, 200)", KEY);
        execute("INSERT INTO %s (k, c, r1, r2) VALUES (?, 2, 101, 201)", KEY);
        return currentTableMetadata();
    }

    private void checkFilter(ClusteringIndexFilter filter, int expectedRows) throws IOException
    {
        TableMetadata metadata = currentTableMetadata();
        ColumnFilter subset = ColumnFilter.selectionBuilder()
                                          .add(column(metadata, "s1"))
                                          .add(column(metadata, "r1"))
                                          .build();

        check(filter, subset, expectedRows);
        flush();
        check(filter, subset, expectedRows);

        // A column filter fetching every column must keep working as before
        check(filter, ColumnFilter.all(metadata), expectedRows);
    }

    private void check(ClusteringIndexFilter filter, ColumnFilter columnFilter, int expectedRows) throws IOException
    {
        RegularAndStaticColumns fetched = columnFilter.fetchedColumns();

        SinglePartitionReadCommand command = fullPartitionRead();
        try (ReadExecutionController controller = command.executionController();
             UnfilteredRowIterator filtered = filterNotIndexed(filter, columnFilter, command, controller))
        {
            assertEquals(fetched, filtered.columns());
            assertColumnsFetched(fetched, filtered.staticRow());
            assertEquals(fetched.statics.isEmpty(), filtered.staticRow().isEmpty());

            int rows = 0;
            while (filtered.hasNext())
            {
                Unfiltered unfiltered = filtered.next();
                assertTrue(unfiltered.isRow());
                assertColumnsFetched(fetched, (Row) unfiltered);
                rows++;
            }
            assertEquals(expectedRows, rows);
        }

        // This is what fails with "... is not a subset of ..." when the iterator advertises columns that the column
        // filter does not fetch
        command = fullPartitionRead();
        try (ReadExecutionController controller = command.executionController();
             UnfilteredRowIterator filtered = filterNotIndexed(filter, columnFilter, command, controller);
             DataOutputBuffer out = new DataOutputBuffer())
        {
            UnfilteredRowIteratorSerializer.serializer.serialize(filtered, columnFilter, out, MessagingService.current_version);
        }
    }

    /**
     * A full partition read, as done by {@code SinglePartitionReadCommand#getThroughCache} to populate the row cache.
     */
    private SinglePartitionReadCommand fullPartitionRead()
    {
        DecoratedKey key = getCurrentColumnFamilyStore().decorateKey(Int32Type.instance.decompose(KEY));
        return SinglePartitionReadCommand.fullPartitionRead(currentTableMetadata(), FBUtilities.nowInSeconds(), key);
    }

    private UnfilteredRowIterator filterNotIndexed(ClusteringIndexFilter filter,
                                                   ColumnFilter columnFilter,
                                                   SinglePartitionReadCommand command,
                                                   ReadExecutionController controller)
    {
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        UnfilteredRowIterator full = command.queryMemtableAndDisk(cfs, controller);
        // The source iterator advertises all the columns of the table
        assertEquals(cfs.metadata().regularAndStaticColumns(), full.columns());
        return filter.filterNotIndexed(columnFilter, full);
    }

    private static void assertColumnsFetched(RegularAndStaticColumns fetched, Row row)
    {
        for (ColumnMetadata column : row.columns())
            assertTrue(column + " is not fetched", fetched.contains(column));
    }

    private static ColumnMetadata column(TableMetadata metadata, String name)
    {
        return metadata.getColumn(ByteBufferUtil.bytes(name));
    }
}
