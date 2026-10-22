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

package org.apache.cassandra.index.sai.memory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.utils.PrimaryKey;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class MemtableKeyRangeIteratorSkipToTest extends SAITester
{
    private static final int NUM_KEYS = 20;

    /**
     * Two consecutive skipTo calls, the second one past the last partition of the memtable,
     * must leave the iterator exhausted.
     */
    @Test
    public void testConsecutiveSkipToPastTheEnd() throws IOException
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, a int, b int)");
        List<DecoratedKey> keys = sortedKeys();

        // the memtable only has the partition with the lowest token
        execute("INSERT INTO %s (k, a, b) VALUES (?, 0, 0)", Int32Type.instance.compose(keys.get(0).getKey()));

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        Memtable memtable = cfs.getTracker().getView().getCurrentMemtable();
        PrimaryKey.Factory factory = PrimaryKey.factory(cfs.metadata().comparator,
                                                        Version.current(KEYSPACE).onDiskFormat().indexFeatureSet());
        PartitionPosition min = cfs.getPartitioner().getMinimumToken().minKeyBound();
        AbstractBounds<PartitionPosition> all = new Range<>(min, min);

        try (MemtableKeyRangeIterator iterator = new MemtableKeyRangeIterator(memtable, factory, all))
        {
            // what ResultRetriever does before the first key is fetched
            iterator.skipTo(factory.createTokenOnly(cfs.getPartitioner().getMinimumToken()));
            // what KeyRangeIntersectionIterator does when another range is at a higher key
            PrimaryKey target = factory.create(keys.get(1), Clustering.EMPTY);
            iterator.skipTo(target);

            if (iterator.hasNext())
                assertTrue("skipTo(" + target + ") returned a smaller key: " + iterator.peek(),
                           iterator.peek().compareTo(target) >= 0);
            assertFalse(iterator.hasNext());
        }
    }

    /**
     * End-to-end: an intersection between an EQ clause matching only flushed rows with high tokens
     * and negated clauses that scan the memtable, which only holds a row with a lower token.
     */
    @Test
    public void testIntersectionWithNegationOverMemtable() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, a int, b varint, c varint, s set<int>)");
        createIndex("CREATE CUSTOM INDEX ON %s(a) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(b) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(c) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(s) USING 'StorageAttachedIndex'");
        disableQueryOptimization();
        List<DecoratedKey> keys = sortedKeys();

        int first = Int32Type.instance.compose(keys.get(0).getKey());
        int last = Int32Type.instance.compose(keys.get(NUM_KEYS - 1).getKey());

        for (int i = 1; i < NUM_KEYS - 1; i++)
            execute("INSERT INTO %s (k, a, b, c, s) VALUES (?, 1, 1, 1, {1})", Int32Type.instance.compose(keys.get(i).getKey()));
        flush();

        // The memtable has the partitions with the lowest and the highest tokens, and they don't match the EQ clause.
        // The last partition is out of the queried range, it is there to make the memtable overlap the sstable.
        execute("INSERT INTO %s (k, a, b, c, s) VALUES (?, 0, 1, 1, {1})", first);
        execute("INSERT INTO %s (k, a, b, c, s) VALUES (?, 0, 1, 1, {1})", last);

        // only the unflushed case exercises the memtable scan, the flushed one is there for completeness
        beforeAndAfterFlush(() -> {
            assertRowCount(execute("SELECT k FROM %s WHERE a = 1 AND b != 0 AND token(k) < token(?)", last), NUM_KEYS - 2);
            assertRowCount(execute("SELECT k FROM %s WHERE a = 1 AND s NOT CONTAINS 0 AND token(k) < token(?)", last), NUM_KEYS - 2);
            assertRowCount(execute("SELECT k FROM %s WHERE a = 1 AND (b != 0 OR c != 0) AND token(k) < token(?)", last), NUM_KEYS - 2);
        });
    }

    private List<DecoratedKey> sortedKeys()
    {
        List<DecoratedKey> keys = new ArrayList<>();
        for (int k = 0; k < NUM_KEYS; k++)
            keys.add(getCurrentColumnFamilyStore().decorateKey(Int32Type.instance.decompose(k)));
        keys.sort(Comparator.naturalOrder());
        return keys;
    }
}
