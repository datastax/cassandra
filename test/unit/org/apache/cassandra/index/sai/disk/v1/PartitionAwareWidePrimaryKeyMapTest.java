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

package org.apache.cassandra.index.sai.disk.v1;

import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.SAIUtil;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.format.IndexComponents;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.io.sstable.format.SSTableReader;

import static org.junit.Assert.assertEquals;

/**
 * Wide-table tests (with a clustering column) for
 * {@link PartitionAwarePrimaryKeyMap} API using the V1 on-disk format (AA).
 */
public class PartitionAwareWidePrimaryKeyMapTest extends SAITester
{
    private final IndexContext intContext = SAITester.createIndexContext("int_index", Int32Type.instance);
    private final IndexContext textContext = SAITester.createIndexContext("text_index", UTF8Type.instance);

    private SSTableReader sstable;
    private PrimaryKey.Factory pkFactory;
    private IndexComponents.ForRead perSSTableComponents;
    private IPartitioner partitioner;

    @Before
    public void setup() throws Throwable
    {
        // Set the version to AA (legacy V1 format) before creating indexes
        SAIUtil.setCurrentVersion(Version.AA);

        // Create a wide table (with clustering column), and two SAI indexes to ensure primary key components exist
        createTable("CREATE TABLE %s (pk int, ck int, int_value int, text_value text, PRIMARY KEY (pk, ck))");
        execute("CREATE CUSTOM INDEX int_index ON %s(int_value) USING 'StorageAttachedIndex'");
        execute("CREATE CUSTOM INDEX text_index ON %s(text_value) USING 'StorageAttachedIndex'");

        // Insert rows with multiple clustering values per partition to create wide partitions
        // Partition 1: multiple rows
        execute("INSERT INTO %s (pk, ck, int_value, text_value) VALUES (?, ?, ?, ?)", 1, 1, 10, "a");
        execute("INSERT INTO %s (pk, ck, int_value, text_value) VALUES (?, ?, ?, ?)", 1, 2, 11, "b");
        execute("INSERT INTO %s (pk, ck, int_value, text_value) VALUES (?, ?, ?, ?)", 1, 3, 12, "c");

        // Partition 1000: multiple rows
        execute("INSERT INTO %s (pk, ck, int_value, text_value) VALUES (?, ?, ?, ?)", 1000, 1, 20, "d");
        execute("INSERT INTO %s (pk, ck, int_value, text_value) VALUES (?, ?, ?, ?)", 1000, 2, 21, "e");

        // Partition 2: single row
        execute("INSERT INTO %s (pk, ck, int_value, text_value) VALUES (?, ?, ?, ?)", 2, 1, 30, "f");

        // Partition 50000: multiple rows
        execute("INSERT INTO %s (pk, ck, int_value, text_value) VALUES (?, ?, ?, ?)", 50000, 1, 40, "g");
        execute("INSERT INTO %s (pk, ck, int_value, text_value) VALUES (?, ?, ?, ?)", 50000, 2, 41, "h");
        execute("INSERT INTO %s (pk, ck, int_value, text_value) VALUES (?, ?, ?, ?)", 50000, 3, 42, "i");

        // Flush to generate SSTable and SAI components
        flush();

        // Obtain the just-flushed SSTable
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        this.sstable = cfs.getLiveSSTables().iterator().next();

        // Build IndexDescriptor from the live SSTable using the matching index contexts
        IndexDescriptor indexDescriptor = IndexDescriptor.load(sstable, Set.of(intContext, textContext));
        perSSTableComponents = indexDescriptor.perSSTableComponents();

        this.pkFactory = indexDescriptor.perSSTableComponents().version().onDiskFormat().newPrimaryKeyFactory(cfs.metadata.get().comparator);
        this.partitioner = sstable.metadata().partitioner;
    }

    @After
    public void teardown()
    {
        SAIUtil.setCurrentVersion(Version.LATEST);
    }

    @Test
    public void testExactRowIdOrInvertedCeiling() throws Throwable
    {
        try (PrimaryKeyMap.Factory factory = perSSTableComponents.onDiskFormat().newPrimaryKeyMapFactory(perSSTableComponents, pkFactory, sstable);
             PrimaryKeyMap map = factory.newPerSSTablePrimaryKeyMap())
        {
            MapWalker mapWalker = new MapWalker(map, map::exactRowIdOrInvertedCeiling);

            mapWalker.assertResult(mapWalker.beforeFirst(), -1, "before first expects the inverted first");
            mapWalker.assertResult(mapWalker.exactFirstToken(), 0, "exact first token");
            mapWalker.assertResult(mapWalker.betweenFirstAndSecondToken(), -mapWalker.secondTokenRowId - 1, "between first and second token expects inverted second token first row");
            mapWalker.assertResult(mapWalker.exactLastToken(), mapWalker.expectedLastTokenRowId, "last token expects first row id of the last token/partition");
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
            mapWalker.assertResult(mapWalker.exactFirstToken(), 0, "exact first token");
            mapWalker.assertResult(mapWalker.betweenFirstAndSecondToken(), mapWalker.secondTokenRowId, "between first and second token expects second token first row");
            mapWalker.assertResult(mapWalker.exactLastToken(), mapWalker.expectedLastTokenRowId, "last token expects first row id of the last token/partition");
            mapWalker.assertResult(mapWalker.afterLast(), -1, "after last expects out of range");
        }
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testFloorUnsupported() throws Throwable
    {
        try (PrimaryKeyMap.Factory factory = perSSTableComponents.onDiskFormat().newPrimaryKeyMapFactory(perSSTableComponents, pkFactory, sstable);
             PrimaryKeyMap map = factory.newPerSSTablePrimaryKeyMap())
        {
            // AA format does not support floor operation; it should throw UnsupportedOperationException
            PrimaryKey firstPk = map.primaryKeyFromRowId(0);
            map.floor(pkFactory.createTokenOnly(firstPk.token()));
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
        protected final long secondTokenRowId;
        protected final long expectedLastTokenRowId;
        private final PrimaryKeyMapFunction rowIdFromPKMethod;
        private final PrimaryKey firstPk;
        private final PrimaryKey lastPk;
        private final long firstToken;
        private final long lastToken;
        private final PrimaryKey secondTokenPk;

        MapWalker(PrimaryKeyMap map, PrimaryKeyMapFunction rowIdFromPKMethod)
        {
            this.rowIdFromPKMethod = rowIdFromPKMethod;
            this.count = map.count();
            this.firstPk = map.primaryKeyFromRowId(0);
            this.lastPk = map.primaryKeyFromRowId(count - 1);
            this.firstToken = firstPk.token().getLongValue();
            this.lastToken = lastPk.token().getLongValue();

            // Find the first row of the second partition (next token)
            this.secondTokenRowId = getNextTokenRowId(count, map, firstPk);
            this.secondTokenPk = map.primaryKeyFromRowId(secondTokenRowId);

            // Find the first row of the last partition
            this.expectedLastTokenRowId = getExpectedLastTokenFirstRowId(count, map, lastPk);
        }

        private long getNextTokenRowId(long count, PrimaryKeyMap map, PrimaryKey pk)
        {
            long i = 0;
            while (map.primaryKeyFromRowId(i).token().equals(pk.token()))
            {
                assert i < count;
                i++;
            }
            return i;
        }

        private long getExpectedLastTokenFirstRowId(long count, PrimaryKeyMap map, PrimaryKey lastPk)
        {
            long i = 0;
            while (!map.primaryKeyFromRowId(i).token().equals(lastPk.token()))
            {
                assert i < count;
                i++;
            }
            return i;
        }

        PrimaryKey beforeFirst()
        {
            return pkFactory.createTokenOnly(partitioner.getTokenFactory().fromLongValue(firstToken - 1));
        }

        PrimaryKey exactFirstToken()
        {
            return pkFactory.createTokenOnly(firstPk.token());
        }

        PrimaryKey betweenFirstAndSecondToken()
        {
            long t1 = secondTokenPk.token().getLongValue();
            long midToken = firstToken + ((t1 - firstToken) / 2);
            return pkFactory.createTokenOnly(partitioner.getTokenFactory().fromLongValue(midToken));
        }

        PrimaryKey exactLastToken()
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
