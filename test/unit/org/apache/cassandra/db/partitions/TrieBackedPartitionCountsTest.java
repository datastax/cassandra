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

package org.apache.cassandra.db.partitions;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.google.common.collect.Iterators;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.memtable.TrieMemtable;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.TrieTombstoneMarker;
import org.apache.cassandra.db.rows.TrieTombstoneMarker.Covering;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.db.partitions.TriePartitionUpdate.startsRowDeletion;
import static org.apache.cassandra.db.rows.TrieTombstoneMarker.Kind.COLUMN;
import static org.apache.cassandra.db.rows.TrieTombstoneMarker.Kind.RANGE;
import static org.apache.cassandra.db.rows.TrieTombstoneMarker.LevelMarker.ROW;
import static org.apache.cassandra.db.rows.TrieTombstoneMarker.make;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/// Tests the row counts, tombstone counts and static rows of [TrieBackedPartition]s.
///
/// Trie-backed partitions count only the rows with live data (see [TrieBackedPartition#rowCount]), however they are
/// built. A static row may hold only a deletion, e.g. of a whole non-frozen collection. Such a row is not counted in
/// `rowCountIncludingStatic`, so [TrieBackedPartition#rowCount] must not subtract it.
///
/// Each test checks the memtable partition and the partitions built from its content by the other construction paths.
public class TrieBackedPartitionCountsTest extends CQLTester
{
    private static final String TABLE = "CREATE TABLE %s (k int, c int, v int, s set<text>, st set<text> static, " +
                                        "PRIMARY KEY (k, c)) WITH memtable = 'trie'";

    private enum StaticRow
    {
        NONE,
        LIVE,
        DELETION_ONLY
    }

    @Test
    public void testStaticColumnDeletionWithLiveRow() throws Throwable
    {
        createTable(TABLE);
        execute("INSERT INTO %s (k, c, v) VALUES (0, 1, 10)");
        execute("DELETE st FROM %s WHERE k = 0");

        assertPartition(0, 1, 1, StaticRow.DELETION_ONLY);
    }

    @Test
    public void testStaticColumnDeletionOverLiveStaticWithLiveRow() throws Throwable
    {
        createTable(TABLE);
        execute("UPDATE %s SET st = {'a', 'b'} WHERE k = 0");
        execute("INSERT INTO %s (k, c, v) VALUES (0, 1, 10)");
        execute("DELETE st FROM %s WHERE k = 0");

        assertPartition(0, 1, 1, StaticRow.DELETION_ONLY);
    }

    @Test
    public void testStaticColumnDeletionOnly() throws Throwable
    {
        createTable(TABLE);
        execute("DELETE st FROM %s WHERE k = 0");

        assertPartition(0, 0, 0, StaticRow.DELETION_ONLY);
    }

    @Test
    public void testLiveStaticRowOnly() throws Throwable
    {
        createTable(TABLE);
        execute("UPDATE %s SET st = {'a'} WHERE k = 0");

        assertPartition(0, 0, 0, StaticRow.LIVE);
    }

    @Test
    public void testLiveStaticRowWithLiveRows() throws Throwable
    {
        createTable(TABLE);
        execute("UPDATE %s SET st = {'a'} WHERE k = 0");
        execute("INSERT INTO %s (k, c, v) VALUES (0, 1, 10)");
        execute("INSERT INTO %s (k, c, v) VALUES (0, 2, 20)");

        assertPartition(0, 2, 2, StaticRow.LIVE);
    }

    @Test
    public void testNoStaticRow() throws Throwable
    {
        createTable(TABLE);
        execute("INSERT INTO %s (k, c, v) VALUES (0, 1, 10)");
        execute("INSERT INTO %s (k, c, v) VALUES (0, 2, 20)");
        // A row that only holds a deletion is not counted.
        execute("DELETE s FROM %s WHERE k = 0 AND c = 3");

        assertPartition(0, 2, 3, StaticRow.NONE);
    }

    @Test
    public void testDeletedRows() throws Throwable
    {
        createTable(TABLE);
        execute("INSERT INTO %s (k, c, v) VALUES (0, 1, 10)");
        execute("DELETE FROM %s WHERE k = 0 AND c = 2");
        execute("INSERT INTO %s (k, c, v) VALUES (0, 3, 30) USING TIMESTAMP 10");
        execute("DELETE FROM %s USING TIMESTAMP 20 WHERE k = 0 AND c = 3");
        // A row with a deleted cell holds a cell, and is counted like a row with live data
        execute("DELETE v FROM %s WHERE k = 0 AND c = 4");

        assertPartition(0, 2, 4, StaticRow.NONE);
    }

    @Test
    public void testPartitionDeletionWithoutStaticRow() throws Throwable
    {
        createTable(TABLE);
        execute("DELETE FROM %s WHERE k = 0");

        assertPartition(0, 0, 0, StaticRow.NONE);
    }

    @Test
    public void testPartitionDeletionWithLiveStaticRow() throws Throwable
    {
        createTable(TABLE);
        execute("DELETE FROM %s USING TIMESTAMP 10 WHERE k = 0");
        execute("UPDATE %s USING TIMESTAMP 20 SET st = {'a'} WHERE k = 0");
        execute("INSERT INTO %s (k, c, v) VALUES (0, 1, 10) USING TIMESTAMP 20");

        assertPartition(0, 1, 1, StaticRow.LIVE);
    }

    @Test
    public void testTableWithoutStaticColumns() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, v int, s set<text>, PRIMARY KEY (k, c)) WITH memtable = 'trie'");
        execute("INSERT INTO %s (k, c, v) VALUES (0, 1, 10)");
        execute("UPDATE %s SET s = {'a'} WHERE k = 0 AND c = 2");
        execute("DELETE s FROM %s WHERE k = 0 AND c = 3");

        assertPartition(0, 2, 3, StaticRow.NONE);
    }

    /// The memtable stores tombstone markers off-heap and reads each side of a marker back as a separate object. A row
    /// level marker under a covering range deletion then has equal, but not identical, sides. It must not be counted
    /// as a tombstone when an update merges with it.
    @Test
    public void testTombstoneCountWithRowMarkerUnderRangeDeletion() throws Throwable
    {
        createTable(TABLE);
        execute("DELETE FROM %s USING TIMESTAMP 10 WHERE k = 0 AND c >= 1 AND c <= 10");
        // A row level marker inside the range deletion
        execute("DELETE s FROM %s USING TIMESTAMP 20 WHERE k = 0 AND c = 5");
        assertEquals(1, memtablePartition(0).tombstoneCount);

        // A newer range deletion that covers the old one, together with the row level marker and its column deletion
        execute("DELETE FROM %s USING TIMESTAMP 30 WHERE k = 0 AND c >= 0 AND c <= 20");
        assertEquals(1, memtablePartition(0).tombstoneCount);
        assertEmpty(execute("SELECT * FROM %s WHERE k = 0"));
    }

    @Test
    public void testTombstoneCountWithSeparateRangeDeletions() throws Throwable
    {
        createTable(TABLE);
        execute("DELETE FROM %s USING TIMESTAMP 10 WHERE k = 0 AND c >= 1 AND c <= 10");
        execute("DELETE s FROM %s USING TIMESTAMP 20 WHERE k = 0 AND c = 5");
        execute("DELETE FROM %s USING TIMESTAMP 30 WHERE k = 0 AND c >= 20 AND c <= 30");
        assertEquals(2, memtablePartition(0).tombstoneCount);
    }

    @Test
    public void testStartsRowDeletion()
    {
        Covering range = TrieTombstoneMarker.covering(DeletionTime.build(10, 1000), RANGE);
        // Equal, but not identical, as when the two sides of a marker are read from the memtable's off-heap storage
        Covering sameRange = TrieTombstoneMarker.covering(DeletionTime.build(10, 1000), RANGE);
        assertNotSame(range, sameRange);
        Covering newerRange = TrieTombstoneMarker.covering(DeletionTime.build(20, 1000), RANGE);
        Covering column = TrieTombstoneMarker.covering(DeletionTime.build(20, 1000), COLUMN);

        // Row level markers under a covering deletion do not start a new deletion
        assertFalse(startsRowDeletion(make(range, range, ROW)));
        assertFalse(startsRowDeletion(make(range, sameRange, ROW)));
        assertFalse(startsRowDeletion(range));

        // Each row-level or higher deletion is counted once, on the boundary that switches to it
        assertTrue(startsRowDeletion(make(null, range, null)));
        assertFalse(startsRowDeletion(make(range, null, null)));
        assertTrue(startsRowDeletion(make(range, newerRange, null)));
        assertTrue(startsRowDeletion(make(newerRange, range, null)));

        // Column deletions are not counted
        assertFalse(startsRowDeletion(make(null, column, null)));
        assertFalse(startsRowDeletion(make(column, null, null)));
        assertFalse(startsRowDeletion(make(range, column, null)));
        assertFalse(startsRowDeletion(make(column, range, null)));
    }

    /// Checks the memtable partition with key `k`, and the partitions built from its content by the other
    /// construction paths of trie-backed partitions.
    private void assertPartition(int k, int liveRows, int allRows, StaticRow staticRow)
    {
        TrieBackedPartition partition = memtablePartition(k);
        TableMetadata metadata = partition.metadata();
        List<String> failures = new ArrayList<>();
        check(failures, "memtable partition", partition, liveRows, allRows, staticRow);

        try (UnfilteredRowIterator iterator = partition.unfilteredIterator())
        {
            check(failures, "TrieBackedPartition.fromIterator", TrieBackedPartition.fromIterator(iterator), liveRows, allRows, staticRow);
        }
        try (UnfilteredRowIterator iterator = partition.unfilteredIterator())
        {
            check(failures, "TriePartitionUpdate.fromIterator", TriePartitionUpdate.fromIterator(iterator), liveRows, allRows, staticRow);
        }

        PartitionUpdate.Builder builder = TriePartitionUpdate.FACTORY.builder(metadata, partition.partitionKey(), partition.columns(), allRows);
        builder.addPartitionDeletion(partition.partitionLevelDeletion());
        if (!partition.staticRow().isEmpty())
            builder.add(partition.staticRow());
        try (UnfilteredRowIterator iterator = partition.unfilteredIterator())
        {
            while (iterator.hasNext())
            {
                Unfiltered unfiltered = iterator.next();
                assertTrue("Range deletions are not supported by this test", unfiltered.isRow());
                builder.add((Row) unfiltered);
            }
        }
        check(failures, "TriePartitionUpdate.Builder", (TrieBackedPartition) builder.build(), liveRows, allRows, staticRow);

        if (staticRow != StaticRow.NONE)
            check(failures, "TriePartitionUpdate.singleRowUpdate of the static row",
                  TriePartitionUpdate.singleRowUpdate(metadata, partition.partitionKey(), partition.staticRow()),
                  0, 0, staticRow);

        // Each row with live data counts as one row on its own.
        int singleRowCounts = 0;
        for (Iterator<Row> rows = partition.rowIterator(); rows.hasNext(); )
        {
            Row row = rows.next();
            TriePartitionUpdate update = TriePartitionUpdate.singleRowUpdate(metadata, partition.partitionKey(), row);
            if (update.rowCount() < 0 || update.rowCount() > 1 || update.hasRows() != (update.rowCount() > 0))
                failures.add("TriePartitionUpdate.singleRowUpdate of " + row.toString(metadata) + ": rowCount " + update.rowCount() + ", hasRows " + update.hasRows());
            singleRowCounts += update.rowCount();
        }
        if (singleRowCounts != liveRows)
            failures.add("TriePartitionUpdate.singleRowUpdate of each row: rowCounts add up to " + singleRowCounts + ", expected " + liveRows);

        if (!failures.isEmpty())
            fail(String.join("\n", failures));
    }

    private static void check(List<String> failures, String source, TrieBackedPartition partition, int liveRows, int allRows, StaticRow staticRow)
    {
        try
        {
            assertPartition(source, partition, liveRows, allRows, staticRow);
        }
        catch (AssertionError e)
        {
            failures.add(e.getMessage());
        }
    }

    private static void assertPartition(String source, TrieBackedPartition partition, int liveRows, int allRows, StaticRow staticRow)
    {
        String message = source + ": ";
        assertEquals(message + "rows", allRows, Iterators.size(partition.rowIterator()));

        assertEquals(message + "rowCount", liveRows, partition.rowCount());
        assertEquals(message + "hasRows", liveRows > 0, partition.hasRows());
        assertFalse(message + "isEmpty", partition.isEmpty());

        ColumnMetadata st = partition.metadata().getColumn(ByteBufferUtil.bytes("st"));
        Row row = partition.staticRow();
        switch (staticRow)
        {
            case NONE:
                assertTrue(message + "static row must be empty, got " + row, row.isEmpty());
                break;
            case LIVE:
            {
                assertFalse(message + "static row must not be empty", row.isEmpty());
                ComplexColumnData data = row.getComplexColumnData(st);
                assertNotNull(message + "static row must have st", data);
                assertTrue(message + "static row must have cells in st", data.cellsCount() > 0);
                break;
            }
            case DELETION_ONLY:
            {
                assertFalse(message + "static row must not be empty", row.isEmpty());
                ComplexColumnData data = row.getComplexColumnData(st);
                assertNotNull(message + "static row must have st", data);
                assertFalse(message + "static row must have a deletion of st", data.complexDeletion().isLive());
                assertEquals(message + "static row must have no cells in st", 0, data.cellsCount());
                break;
            }
        }
    }

    private TrieBackedPartition memtablePartition(int k)
    {
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        Memtable memtable = cfs.getCurrentMemtable();
        assertTrue(memtable instanceof TrieMemtable);
        DecoratedKey key = cfs.decorateKey(Int32Type.instance.decompose(k));
        Partition partition = memtable.getPartition(key);
        assertNotNull(partition);
        return (TrieBackedPartition) partition;
    }
}
