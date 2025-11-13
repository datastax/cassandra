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

package org.apache.cassandra.index.sai.disk.v2;

import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.format.IndexComponents;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.io.sstable.format.SSTableReader;

import static org.junit.Assert.assertEquals;

/**
 * Skinny-table tests (no clustering columns) for
 * {@link SkinnyPrimaryKeyMap} using the row-aware on-disk format.
 */
public class RowAwareSkinnyPrimaryKeyMapTest extends SAITester
{
    private final IndexContext intContext = SAITester.createIndexContext("int_index", Int32Type.instance);
    private final IndexContext textContext = SAITester.createIndexContext("text_index", UTF8Type.instance);

    private IndexComponents.ForRead perSSTableComponents;
    private SSTableReader sstable;
    private PrimaryKey.Factory pkFactory;
    private IPartitioner partitioner;

    @Before
    public void setup() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, int_value int, text_value text)");
        execute("CREATE CUSTOM INDEX int_index ON %s(int_value) USING 'StorageAttachedIndex'");
        execute("CREATE CUSTOM INDEX text_index ON %s(text_value) USING 'StorageAttachedIndex'");

        // Insert a few rows to have first/middle/last and token gaps
        execute("INSERT INTO %s (pk, int_value, text_value) VALUES (?, ?, ?)", 1, 10, "a");
        execute("INSERT INTO %s (pk, int_value, text_value) VALUES (?, ?, ?)", 1000, 20, "b");
        execute("INSERT INTO %s (pk, int_value, text_value) VALUES (?, ?, ?)", 2, 30, "c");
        execute("INSERT INTO %s (pk, int_value, text_value) VALUES (?, ?, ?)", 50000, 40, "d");

        // Flush to generate SSTable and SAI components
        flush();

        // Obtain the just-flushed SSTable
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        this.sstable = cfs.getLiveSSTables().iterator().next();

        IndexDescriptor indexDescriptor = IndexDescriptor.load(sstable, Set.of(intContext, textContext));
        this.perSSTableComponents = indexDescriptor.perSSTableComponents();

        this.partitioner = sstable.metadata().partitioner;
        this.pkFactory = indexDescriptor.perSSTableComponents().version().onDiskFormat().newPrimaryKeyFactory(cfs.metadata.get().comparator);
    }

    @Test
    public void testExactRowIdOrInvertedCeiling() throws Throwable
    {
        try (PrimaryKeyMap.Factory factory = perSSTableComponents.onDiskFormat().newPrimaryKeyMapFactory(perSSTableComponents, pkFactory, sstable);
             PrimaryKeyMap map = factory.newPerSSTablePrimaryKeyMap())
        {
            MapWalker mapWalker = new MapWalker(map, map::exactRowIdOrInvertedCeiling);

            mapWalker.assertResult(mapWalker.beforeFirst(), -1, "before first expects the inverted first");
            mapWalker.assertResult(mapWalker.exactFirst(), 0, "exact first");
            mapWalker.assertResult(mapWalker.betweenFirstAndSecond(), -2, "between first and second expects the inverted second");
            mapWalker.assertResult(mapWalker.exactLast(), mapWalker.count - 1, "exact last");
            mapWalker.assertResult(mapWalker.afterLast(), Long.MIN_VALUE, "after last expects out of range");
        }
    }

    @Test
    public void testCeiling() throws Throwable
    {
        try (PrimaryKeyMap.Factory factory = perSSTableComponents.onDiskFormat().newPrimaryKeyMapFactory(perSSTableComponents, pkFactory, sstable);
             PrimaryKeyMap map = factory.newPerSSTablePrimaryKeyMap())
        {
            MapWalker mapWalker = new MapWalker(map, map::ceiling);

            mapWalker.assertResult(mapWalker.beforeFirst(), 0, "before first expects the first");
            mapWalker.assertResult(mapWalker.exactFirst(), 0, "exact first");
            mapWalker.assertResult(mapWalker.betweenFirstAndSecond(), 1, "between first and second expects the second");
            mapWalker.assertResult(mapWalker.exactLast(), mapWalker.count - 1, "exact last");
            mapWalker.assertResult(mapWalker.afterLast(), -1, "after last expects out of range");
        }
    }

    @Test
    public void testFloorSkinny() throws Throwable
    {
        try (PrimaryKeyMap.Factory factory = perSSTableComponents.onDiskFormat().newPrimaryKeyMapFactory(perSSTableComponents, pkFactory, sstable);
             PrimaryKeyMap map = factory.newPerSSTablePrimaryKeyMap())
        {
            MapWalker mapWalker = new MapWalker(map, map::floor);

            mapWalker.assertResult(mapWalker.beforeFirst(), -1, "before first expects out of range");
            mapWalker.assertResult(mapWalker.exactFirst(), 0, "exact first");
            mapWalker.assertResult(mapWalker.betweenFirstAndSecond(), 0, "between first and second expects the first");
            mapWalker.assertResult(mapWalker.exactLast(), mapWalker.count - 1, "exact last");
            mapWalker.assertResult(mapWalker.afterLast(), mapWalker.count - 1, "after last expects the last");
        }
    }

    /**
     * Functional interface for PrimaryKeyMap API methods that take a PrimaryKey and return a row ID.
     */
    @FunctionalInterface
    private interface PrimaryKeyMapFunction
    {
        long apply(PrimaryKey pk);
    }

    private class MapWalker
    {
        protected final long count;
        private final PrimaryKeyMapFunction rowIdFromPKMethod;
        private final PrimaryKey firstPk;
        private final PrimaryKey lastPk;
        private final long firstToken;
        private final long secondToken;
        private final long lastToken;

        MapWalker(PrimaryKeyMap map, PrimaryKeyMapFunction rowIdFromPKMethod)
        {
            this.rowIdFromPKMethod = rowIdFromPKMethod;
            this.count = map.count();
            this.firstPk = map.primaryKeyFromRowId(0);
            PrimaryKey secondPk = map.primaryKeyFromRowId(1);
            this.lastPk = map.primaryKeyFromRowId(count - 1);
            this.firstToken = firstPk.token().getLongValue();
            this.secondToken = secondPk.token().getLongValue();
            this.lastToken = lastPk.token().getLongValue();
        }

        PrimaryKey beforeFirst()
        {
            return pkFactory.createTokenOnly(partitioner.getTokenFactory().fromLongValue(firstToken - 1));
        }

        PrimaryKey exactFirst()
        {
            return pkFactory.createTokenOnly(firstPk.token());
        }

        PrimaryKey betweenFirstAndSecond()
        {
            long midToken = firstToken + ((secondToken - firstToken) / 2);
            return pkFactory.createTokenOnly(partitioner.getTokenFactory().fromLongValue(midToken));
        }

        PrimaryKey exactLast()
        {
            return pkFactory.createTokenOnly(lastPk.token());
        }

        PrimaryKey afterLast()
        {
            return pkFactory.createTokenOnly(partitioner.getTokenFactory().fromLongValue(lastToken + 1));
        }

        void assertResult(PrimaryKey pk, long expected, String expectationMessage)
        {
            long actual = rowIdFromPKMethod.apply(pk);
            assertEquals(expectationMessage, expected, actual);
        }
    }
}
