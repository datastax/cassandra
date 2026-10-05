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

package org.apache.cassandra.db.memtable;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.TriePartitionUpdate;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellData;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.TrieTombstoneMarker;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.io.sstable.SSTableReadsListener;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.utils.concurrent.OpOrder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/// Checks that the live data size of a trie memtable, which [Memtable#estimateRowCount] divides by the average row
/// size, accounts for exactly the content of the memtable as it is modified by deletions and overwrites.
public class TrieMemtableLiveDataSizeTest extends CQLTester
{
    private static final String TABLE = "CREATE TABLE %s (pk int, ck int, s int static, v int, c set<int>, m map<int, int>, f frozen<list<int>>, b blob, PRIMARY KEY (pk, ck)) WITH memtable = 'trie'";
    private static final String REVERSED_TABLE = "CREATE TABLE %s (pk int, ck1 int, ck2 text, s int static, v int, c set<int>, m map<int, int>, f frozen<list<int>>, b blob, PRIMARY KEY (pk, ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 DESC, ck2 ASC) AND memtable = 'trie'";

    @Test
    public void testPartitionDeletionOverCollections()
    {
        createTable(TABLE);
        execute("INSERT INTO %s (pk, ck, c) VALUES (0, 1, {1}) USING TIMESTAMP 10");
        execute("INSERT INTO %s (pk, ck, c) VALUES (0, 2, {2}) USING TIMESTAMP 10");
        execute("DELETE FROM %s USING TIMESTAMP 20 WHERE pk = 0");
        assertLiveDataSizeMatchesContent();
    }

    @Test
    public void testPartitionDeletionsAroundNewerRangeDeletion()
    {
        createTable(TABLE);
        execute("DELETE FROM %s USING TIMESTAMP 20 WHERE pk = 0 AND ck > 1");
        execute("DELETE FROM %s USING TIMESTAMP 10 WHERE pk = 0");
        assertLiveDataSizeMatchesContent();
        execute("DELETE FROM %s USING TIMESTAMP 30 WHERE pk = 0");
        assertLiveDataSizeMatchesContent();
    }

    @Test
    public void testRowMarkerReinsertedOverRowDeletion()
    {
        createTable(TABLE);
        execute("INSERT INTO %s (pk, ck, v) VALUES (0, 1, 1) USING TIMESTAMP 10");
        execute("UPDATE %s USING TIMESTAMP 30 SET v = 2 WHERE pk = 0 AND ck = 1");
        execute("DELETE FROM %s USING TIMESTAMP 20 WHERE pk = 0 AND ck = 1");
        assertLiveDataSizeMatchesContent();
        execute("INSERT INTO %s (pk, ck) VALUES (0, 1) USING TIMESTAMP 40");
        assertLiveDataSizeMatchesContent();
    }

    @Test
    public void testSAIQueryAfterDeletions()
    {
        createTable(TABLE);
        createIndex("CREATE INDEX ON %s(v) USING 'sai'");
        execute("INSERT INTO %s (pk, ck, v) VALUES (0, 1, 1) USING TIMESTAMP 10");
        // Planning this query computes the average row size of the memtable from the row above.
        assertRows(execute("SELECT pk, ck FROM %s WHERE v = 1"), row(0, 1));

        for (int pk = 1; pk <= 10; ++pk)
        {
            execute("DELETE FROM %s USING TIMESTAMP 20 WHERE pk = ? AND ck > 1", pk);
            execute("DELETE FROM %s USING TIMESTAMP 10 WHERE pk = ?", pk);
            execute("DELETE FROM %s USING TIMESTAMP 30 WHERE pk = ?", pk);
        }
        assertRows(execute("SELECT pk, ck FROM %s WHERE v = 1"), row(0, 1));
        assertLiveDataSizeMatchesContent();
    }

    @Test
    public void testFailedMergeLeavesLiveDataSizeUnchanged()
    {
        createTable(TABLE);
        execute("INSERT INTO %s (pk, ck, v, c) VALUES (0, 1, 1, {1, 2}) USING TIMESTAMP 10");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        long liveDataSize = cfs.getCurrentMemtable().getLiveDataSize();

        // A new row, failing after its row marker and two of its cells have been merged.
        PartitionUpdate.SimpleBuilder insertBuilder = PartitionUpdate.simpleBuilder(cfs.metadata(), 0).timestamp(20);
        insertBuilder.row(2).add("v", 2).add("c", set(3, 4));
        PartitionUpdate insert = insertBuilder.build();
        putFailing(insert, new FailingUpdateTransaction(false, 1));
        assertEquals(liveDataSize, cfs.getCurrentMemtable().getLiveDataSize());
        assertLiveDataSizeMatchesContent();

        // A partition deletion, failing after the first cell has been deleted.
        PartitionUpdate deletion = PartitionUpdate.simpleBuilder(cfs.metadata(), 0).timestamp(30).delete().build();
        putFailing(deletion, new FailingUpdateTransaction(false, 0));
        assertEquals(liveDataSize, cfs.getCurrentMemtable().getLiveDataSize());
        assertLiveDataSizeMatchesContent();

        put(insert, new FailingUpdateTransaction(false, Integer.MAX_VALUE));
        assertLiveDataSizeMatchesContent();
        put(deletion, new FailingUpdateTransaction(false, Integer.MAX_VALUE));
        assertLiveDataSizeMatchesContent();
    }

    @Test
    public void testFailedIndexTransactionStartLeavesLiveDataSizeUnchanged()
    {
        createTable(TABLE);
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        PartitionUpdate.SimpleBuilder builder = PartitionUpdate.simpleBuilder(cfs.metadata(), 0).timestamp(10);
        builder.row(1).add("v", 1);
        put(builder.build(), new FailingUpdateTransaction(false, Integer.MAX_VALUE));
        long liveDataSize = cfs.getCurrentMemtable().getLiveDataSize();
        assertLiveDataSizeMatchesContent();

        // Nothing is merged, but the updater still holds the size change of the write above.
        builder = PartitionUpdate.simpleBuilder(cfs.metadata(), 0).timestamp(20);
        builder.row(2).add("v", 2);
        putFailing(builder.build(), new FailingUpdateTransaction(true, Integer.MAX_VALUE));
        assertEquals(liveDataSize, cfs.getCurrentMemtable().getLiveDataSize());
        assertLiveDataSizeMatchesContent();
    }

    @Test
    public void testRandomUpdatesAndDeletions()
    {
        for (long seed = 1; seed <= 4; ++seed)
        {
            createTable(TABLE);
            executeRandomStatements(seed, 300, false, false);
        }
    }

    @Test
    public void testRandomUpdatesAndDeletionsWithReversedClustering()
    {
        for (long seed = 11; seed <= 12; ++seed)
        {
            createTable(REVERSED_TABLE);
            executeRandomStatements(seed, 300, true, false);
        }
    }

    @Test
    public void testRandomUpdatesAndDeletionsWithTimestampTies()
    {
        for (long seed = 21; seed <= 24; ++seed)
        {
            createTable(TABLE);
            executeRandomStatements(seed, 300, false, true);
        }
    }

    @Test
    public void testRandomCounterUpdatesAndDeletions()
    {
        for (long seed = 31; seed <= 32; ++seed)
        {
            createTable("CREATE TABLE %s (pk int, ck int, a counter, b counter, PRIMARY KEY (pk, ck)) WITH memtable = 'trie'");
            Random random = new Random(seed);
            for (int i = 0; i < 200; ++i)
            {
                String row = "pk = " + random.nextInt(2) + " AND ck = " + random.nextInt(4);
                String statement;
                switch (random.nextInt(4))
                {
                    case 0:
                        statement = "UPDATE %s SET a = a + " + random.nextInt(1000000) + " WHERE " + row;
                        break;
                    case 1:
                        statement = "UPDATE %s SET a = a - 1, b = b + 1 WHERE " + row;
                        break;
                    case 2:
                        statement = "DELETE a FROM %s WHERE " + row;
                        break;
                    default:
                        statement = "DELETE FROM %s WHERE " + row;
                        break;
                }
                execute(statement);
                assertLiveDataSizeMatchesContent("seed " + seed + ", statement " + i + ": " + statement);
            }
        }
    }

    /// Executes a random mix of writes and deletions on [#TABLE] or [#REVERSED_TABLE], checking the live data size
    /// after each one. Unless `timestampTies` is set, timestamps are distinct and applied out of order.
    private void executeRandomStatements(long seed, int count, boolean reversed, boolean timestampTies)
    {
        Random random = new Random(seed);
        // Distinct timestamps keep deletions of different kinds from tying with each other or with the collection
        // deletions made one below the timestamp of a collection overwrite.
        List<Integer> timestamps = new ArrayList<>();
        for (int i = 1; i <= count; ++i)
            timestamps.add(timestampTies ? (1 + random.nextInt(8)) * 10 : i * 10);
        Collections.shuffle(timestamps, random);
        String ckColumns = reversed ? "ck1, ck2" : "ck";
        String rangeColumn = reversed ? "ck1" : "ck";
        for (int i = 0; i < count; ++i)
        {
            int pk = random.nextInt(2);
            int ck = random.nextInt(5);
            int value = random.nextInt(5);
            int timestamp = timestamps.get(i);
            String ckValues = reversed ? ck + ", 'a" + ck % 2 + "'" : String.valueOf(ck);
            String row = "pk = " + pk + " AND " + (reversed ? "ck1 = " + ck + " AND ck2 = 'a" + ck % 2 + "'" : "ck = " + ck);
            String using = " USING TIMESTAMP " + timestamp;
            Object[] values = {};
            String statement;
            switch (random.nextInt(22))
            {
                case 0:
                    statement = "INSERT INTO %s (pk, " + ckColumns + ", v, c) VALUES (" + pk + ", " + ckValues + ", " + value + ", {" + value + "})" + using;
                    break;
                case 1:
                    statement = "INSERT INTO %s (pk, " + ckColumns + ", s) VALUES (" + pk + ", " + ckValues + ", " + value + ")" + using;
                    break;
                case 2:
                    statement = "UPDATE %s" + using + " SET v = " + value + " WHERE " + row;
                    break;
                case 3:
                    statement = "UPDATE %s" + using + " SET c = c + {" + value + "} WHERE " + row;
                    break;
                case 4:
                    statement = "UPDATE %s" + using + " SET c = {" + value + "} WHERE " + row;
                    break;
                case 5:
                    statement = "DELETE v FROM %s" + using + " WHERE " + row;
                    break;
                case 6:
                    statement = "DELETE c FROM %s" + using + " WHERE " + row;
                    break;
                case 7:
                    statement = "DELETE FROM %s" + using + " WHERE " + row;
                    break;
                case 8:
                    statement = "DELETE FROM %s" + using + " WHERE pk = " + pk + " AND " + rangeColumn + " >= " + ck + " AND " + rangeColumn + " < " + (ck + value);
                    break;
                case 9:
                    statement = "DELETE FROM %s" + using + " WHERE pk = " + pk;
                    break;
                case 10:
                    statement = "INSERT INTO %s (pk, " + ckColumns + ", v) VALUES (" + pk + ", " + ckValues + ", " + value + ")" + using + " AND TTL " + (1000 + value);
                    break;
                case 11:
                    statement = "UPDATE %s" + using + " AND TTL 1000 SET v = " + value + " WHERE " + row;
                    break;
                case 12:
                    statement = "DELETE s FROM %s" + using + " WHERE pk = " + pk;
                    break;
                case 13:
                    statement = "UPDATE %s" + using + " AND TTL 1000 SET m[" + value + "] = " + value + " WHERE " + row;
                    break;
                case 14:
                    statement = "DELETE m[" + value + "] FROM %s" + using + " WHERE " + row;
                    break;
                case 15:
                    statement = "UPDATE %s" + using + " SET c = c - {" + value + "} WHERE " + row;
                    break;
                case 16:
                    statement = "UPDATE %s" + using + " SET f = [" + value + ", " + value + "] WHERE " + row;
                    break;
                case 17:
                    statement = "DELETE FROM %s" + using + " WHERE pk = " + pk + " AND " + rangeColumn + " > " + ck;
                    break;
                case 18:
                    statement = "DELETE FROM %s" + using + " WHERE pk = " + pk + " AND " + rangeColumn + " <= " + ck;
                    break;
                case 19:
                    statement = "INSERT INTO %s (pk, " + ckColumns + ") VALUES (" + pk + ", " + ckValues + ")" + using;
                    break;
                case 20:
                    // A single mutation with a row deletion, a newer insert into the same row, and a range deletion.
                    statement = "BEGIN UNLOGGED BATCH " +
                                "DELETE FROM %1$s" + using + " WHERE " + row + "; " +
                                "INSERT INTO %1$s (pk, " + ckColumns + ", v, c) VALUES (" + pk + ", " + ckValues + ", " + value + ", {" + value + "}) USING TIMESTAMP " + (timestamp + 5) + "; " +
                                "DELETE FROM %1$s USING TIMESTAMP " + (timestamp + 1) + " WHERE pk = " + pk + " AND " + rangeColumn + " > " + (ck + 1) + "; " +
                                "APPLY BATCH";
                    break;
                default:
                    statement = "UPDATE %s" + using + " SET b = ? WHERE " + row;
                    values = new Object[] { ByteBuffer.allocate(random.nextBoolean() ? value : 60000 + random.nextInt(20000)) };
                    break;
            }
            execute(statement, values);
            String message = "seed " + seed + ", statement " + i + ": " + statement;
            // Equal timestamps can leave a range tombstone unclosed in the reload, so only stored content is compared.
            if (timestampTies)
                assertLiveDataSizeMatchesStoredContent(message);
            else
                assertLiveDataSizeMatchesContent(message);
        }
    }

    private void put(PartitionUpdate update, UpdateTransaction indexer)
    {
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        try (OpOrder.Group writeGroup = cfs.keyspace.writeOrder.start())
        {
            cfs.getCurrentMemtable().put(update, indexer, writeGroup);
        }
    }

    private void putFailing(PartitionUpdate update, FailingUpdateTransaction indexer)
    {
        try
        {
            put(update, indexer);
            fail("Expected the write to fail");
        }
        catch (InjectedFailure e)
        {
            // expected
        }
    }

    private void assertLiveDataSizeMatchesContent()
    {
        assertLiveDataSizeMatchesContent("");
    }

    /// Checks that the live data size of the current memtable is equal to the summed weight of its content, and to the
    /// live data size of a new memtable into which the current content of the memtable is written with one update per
    /// partition.
    private void assertLiveDataSizeMatchesContent(String message)
    {
        assertLiveDataSizeMatchesStoredContent(message);

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        Memtable memtable = cfs.getCurrentMemtable();
        Memtable rewritten = cfs.metadata().params.memtable.factory().create(null, cfs.metadata, cfs);
        try
        {
            ColumnFilter columnFilter = ColumnFilter.all(cfs.metadata());
            try (OpOrder.Group readGroup = memtable.readOrdering().start();
                 UnfilteredPartitionIterator partitions = memtable.partitionIterator(columnFilter,
                                                                                     DataRange.allData(cfs.getPartitioner()),
                                                                                     SSTableReadsListener.NOOP_LISTENER);
                 OpOrder.Group writeGroup = cfs.keyspace.writeOrder.start())
            {
                while (partitions.hasNext())
                {
                    try (UnfilteredRowIterator partition = partitions.next())
                    {
                        rewritten.put(TriePartitionUpdate.fromIterator(partition), UpdateTransaction.NO_OP, writeGroup);
                    }
                }
            }
            assertEquals(message, rewritten.getLiveDataSize(), memtable.getLiveDataSize());
        }
        finally
        {
            OpOrder.Barrier barrier = cfs.keyspace.writeOrder.newBarrier();
            barrier.issue();
            rewritten.switchOut(barrier, new AtomicReference<>(CommitLogPosition.NONE));
            rewritten.discard();
        }
    }

    /// Checks that the live data size of the current memtable is not negative and is equal to the summed weight of
    /// the content stored in its trie: cells, row markers, and the deletion opened by each tombstone boundary that
    /// changes the deletion.
    private void assertLiveDataSizeMatchesStoredContent(String message)
    {
        Memtable memtable = getCurrentColumnFamilyStore().getCurrentMemtable();
        assertTrue(memtable instanceof TrieMemtable);
        assertTrue(message + " live data size " + memtable.getLiveDataSize(), memtable.getLiveDataSize() >= 0);
        assertTrue(message, Memtable.estimateRowCount(memtable) >= 0);

        long storedSize = 0;
        try (OpOrder.Group readGroup = memtable.readOrdering().start())
        {
            for (Object content : ((TrieMemtable) memtable).mergedTrie.contentOnlyTrie().values())
            {
                if (content instanceof CellData)
                    storedSize += ((CellData<?, ?>) content).dataSizeWithoutPath();
                else if (content instanceof LivenessInfo)
                    storedSize += ((LivenessInfo) content).dataSize();
            }
            for (TrieTombstoneMarker marker : ((TrieMemtable) memtable).mergedTrie.deletionOnlyTrie().values())
            {
                DeletionTime rightSide = marker.rightDeletion();
                if (marker.isBoundary() && rightSide != null && !rightSide.equals(marker.leftDeletion()))
                    storedSize += rightSide.dataSize();
            }
        }
        assertEquals(message, storedSize, memtable.getLiveDataSize());
    }

    private static class InjectedFailure extends RuntimeException
    {
        InjectedFailure()
        {
            super("Injected failure");
        }
    }

    /// Index transaction that fails the write in [#start], or in the call to [#onCellUpdate] that follows the given
    /// number of successful ones. The trie memtable makes that call while merging, after the cell has been applied.
    private static class FailingUpdateTransaction implements UpdateTransaction
    {
        private final boolean failInStart;
        private int cellUpdatesBeforeFailure;

        FailingUpdateTransaction(boolean failInStart, int cellUpdatesBeforeFailure)
        {
            this.failInStart = failInStart;
            this.cellUpdatesBeforeFailure = cellUpdatesBeforeFailure;
        }

        public void start()
        {
            if (failInStart)
                throw new InjectedFailure();
        }

        public void onCellUpdate(Cell<?> original, Cell<?> merged)
        {
            if (cellUpdatesBeforeFailure-- == 0)
                throw new InjectedFailure();
        }

        public void onPartitionDeletion(DeletionTime deletionTime) {}
        public void onRangeTombstone(RangeTombstone rangeTombstone) {}
        public void onInserted(Row row) {}
        public void onUpdated(Row existing, Row updated) {}
        public void startRow(Clustering clustering, LivenessInfo existingLiveness, Row.Deletion existingDeletion, LivenessInfo updatedLiveness, Row.Deletion updatedDeletion) {}
        public void onComplexColumnDeletion(ColumnMetadata column, DeletionTime deletionTime) {}
        public void commit() {}
    }
}
