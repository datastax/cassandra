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

import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.concurrent.OpOrder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/// A read that runs concurrently with a write to a [TrieMemtable] must see the columns of all the data it sees.
/// Otherwise the read may e.g. miss the static row of a partition, or present rows with columns that are not in the
/// partition's column header.
///
/// To test this deterministically, the test reads the partition from inside the write: the index transaction's
/// `commit` is called after the data has been applied to the memtable shard, but before the write completes.
public class TrieMemtableColumnsVisibilityTest extends CQLTester
{
    @Test
    public void testReadDuringWriteSeesNewColumns() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, v int, w int, st int static, PRIMARY KEY (k, c)) WITH memtable = 'trie'");
        disableCompaction();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        Memtable memtable = cfs.getCurrentMemtable();
        assertTrue(memtable instanceof TrieMemtable);
        TableMetadata metadata = cfs.metadata();

        // Give the memtable some columns, but not the ones of the update below.
        execute("INSERT INTO %s (k, c, v) VALUES (0, 0, 0)");

        PartitionUpdate.SimpleBuilder builder = PartitionUpdate.simpleBuilder(metadata, 1).timestamp(1);
        builder.row().add("st", 100);
        builder.row(1).add("w", 10);
        PartitionUpdate update = builder.build();
        DecoratedKey key = update.partitionKey();

        ColumnMetadata st = metadata.getColumn(ByteBufferUtil.bytes("st"));
        ColumnMetadata w = metadata.getColumn(ByteBufferUtil.bytes("w"));
        AtomicReference<Throwable> readError = new AtomicReference<>();
        UpdateTransaction readOnCommit = new NoOpUpdateTransaction()
        {
            @Override
            public void commit()
            {
                try
                {
                    // The update's data is in the memtable at this point.
                    Partition partition = memtable.getPartition(key);
                    assertNotNull("partition must be visible", partition);
                    Row row = partition.getRow(Clustering.make(Int32Type.instance.decompose(1)));
                    assertNotNull("row must be visible", row);
                    assertNotNull("row must have w", row.getCell(w));

                    assertTrue("columns must include w, got " + partition.columns(), partition.columns().regulars.contains(w));
                    assertTrue("columns must include st, got " + partition.columns(), partition.columns().statics.contains(st));
                    Cell<?> staticCell = partition.staticRow().getCell(st);
                    assertNotNull("static row must have st, got " + partition.staticRow(), staticCell);
                    assertEquals(100, (int) Int32Type.instance.compose(staticCell.buffer()));
                }
                catch (Throwable t)
                {
                    readError.set(t);
                }
            }
        };

        try (OpOrder.Group group = Keyspace.writeOrder.start())
        {
            memtable.put(update, readOnCommit, group);
        }
        if (readError.get() != null)
            throw readError.get();

        assertRows(execute("SELECT k, c, st, v, w FROM %s WHERE k = 1"), row(1, 1, 100, null, 10));
    }

    private static class NoOpUpdateTransaction implements UpdateTransaction
    {
        public void start() {}
        public void onPartitionDeletion(DeletionTime deletionTime) {}
        public void onRangeTombstone(RangeTombstone rangeTombstone) {}
        public void onInserted(Row row) {}
        public void onUpdated(Row existing, Row updated) {}
        public void startRow(Clustering clustering, LivenessInfo existingLiveness, Row.Deletion existingDeletion, LivenessInfo updatedLiveness, Row.Deletion updatedDeletion) {}
        public void onCellUpdate(Cell<?> original, Cell<?> merged) {}
        public void onComplexColumnDeletion(ColumnMetadata column, DeletionTime deletionTime) {}
        public void commit() {}
    }
}
