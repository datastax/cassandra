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

package org.apache.cassandra.index.sai.disk.v2;

import java.nio.ByteBuffer;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.format.IndexComponents;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.TableMetadata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Wide-table tests (with clustering columns) for
 * {@link RowAwarePrimaryKeyMap#exactRowIdOrInvertedCeiling(PrimaryKey)} using the row-aware on-disk format.
 * <p>
 * This test generates fresh SSTables and SAI components at runtime via SAITester.
 */
public class RowAwareWidePrimaryKeyMapTest extends SAITester
{
    private final IndexContext intContext = SAITester.createIndexContext("int_index", Int32Type.instance);
    private final IndexContext textContext = SAITester.createIndexContext("text_index", UTF8Type.instance);
    private IndexDescriptor indexDescriptor;
    private SSTableReader sstable;
    private PrimaryKey.Factory pkFactory;

    @Before
    public void setup() throws Throwable
    {
        // Create a wide table (with clustering), and two SAI indexes to ensure primary key components exist
        createTable("CREATE TABLE %s (pk int, ck int, int_value int, text_value text, PRIMARY KEY (pk, ck)) WITH CLUSTERING ORDER BY (ck ASC)");
        execute("CREATE CUSTOM INDEX int_index ON %s(int_value) USING 'StorageAttachedIndex'");
        execute("CREATE CUSTOM INDEX text_index ON %s(text_value) USING 'StorageAttachedIndex'");

        // Insert multiple rows per partition to exercise clustering ordering
        // Partition pk=1
        execute("INSERT INTO %s (pk, ck, int_value, text_value) VALUES (?, ?, ?, ?)", 1, 1, 11, "a1");
        execute("INSERT INTO %s (pk, ck, int_value, text_value) VALUES (?, ?, ?, ?)", 1, 2, 12, "a2");
        execute("INSERT INTO %s (pk, ck, int_value, text_value) VALUES (?, ?, ?, ?)", 1, 3, 13, "a3");
        execute("INSERT INTO %s (pk, ck, int_value, text_value) VALUES (?, ?, ?, ?)", 1, 10, 110, "a10");

        // Partition pk=2
        execute("INSERT INTO %s (pk, ck, int_value, text_value) VALUES (?, ?, ?, ?)", 2, 1, 21, "b1");
        execute("INSERT INTO %s (pk, ck, int_value, text_value) VALUES (?, ?, ?, ?)", 2, 5, 25, "b5");

        // Partition pk=1000
        execute("INSERT INTO %s (pk, ck, int_value, text_value) VALUES (?, ?, ?, ?)", 1000, 1, 1001, "c1");
        execute("INSERT INTO %s (pk, ck, int_value, text_value) VALUES (?, ?, ?, ?)", 1000, 2, 1002, "c2");

        // Flush to generate SSTable and SAI components
        flush();

        // Obtain the just-flushed SSTable
        var cfs = getCurrentColumnFamilyStore();
        sstable = cfs.getLiveSSTables().iterator().next();

        // Build IndexDescriptor from the live SSTable using the matching index contexts
        indexDescriptor = IndexDescriptor.load(sstable, Set.of(intContext, textContext));
        TableMetadata tableMetadata = cfs.metadata.get();
        pkFactory = indexDescriptor.perSSTableComponents().version().onDiskFormat().newPrimaryKeyFactory(tableMetadata.comparator);
    }

    private PrimaryKey buildPk(IPartitioner partitioner, int pk, int ck)
    {
        ByteBuffer pkBuf = Int32Type.instance.decompose(pk);
        Token token = partitioner.getToken(pkBuf);
        DecoratedKey key = new BufferDecoratedKey(token, pkBuf);
        Clustering<ByteBuffer> clustering = Clustering.make(Int32Type.instance.decompose(ck));
        return pkFactory.create(key, clustering);
    }

    @Test
    public void testExactRowIdOrInvertedCeiling() throws Throwable
    {
        IndexComponents.ForRead perSSTableComponents = indexDescriptor.perSSTableComponents();
        try (PrimaryKeyMap.Factory factory = perSSTableComponents.onDiskFormat().newPrimaryKeyMapFactory(perSSTableComponents, pkFactory, sstable);
             PrimaryKeyMap map = factory.newPerSSTablePrimaryKeyMap())
        {
            long count = map.count();

            IPartitioner partitioner = sstable.metadata().partitioner;

            // Get boundary tokens for before/after tests
            PrimaryKey firstPk = map.primaryKeyFromRowId(0);
            long t0 = firstPk.token().getLongValue();

            // Before first: token-only with token less than first should yield a negative inverted ceiling
            long invBeforeFirst = map.exactRowIdOrInvertedCeiling(pkFactory.createTokenOnly(partitioner.getTokenFactory().fromLongValue(t0 - 1)));
            assertEquals("Expected inverted ceiling before start to be negative", -1, invBeforeFirst);

            // Exact matches within a single partition should resolve and be ordered by clustering
            long id11 = map.exactRowIdOrInvertedCeiling(buildPk(partitioner, 1, 1));
            long id12 = map.exactRowIdOrInvertedCeiling(buildPk(partitioner, 1, 2));
            long id13 = map.exactRowIdOrInvertedCeiling(buildPk(partitioner, 1, 3));
            long id110 = map.exactRowIdOrInvertedCeiling(buildPk(partitioner, 1, 10));

            assertTrue("RowIds within a partition should be increasing by clustering",
                       0 <= id11 && id11 < id12 && id12 < id13 && id13 < id110);

            // Between clustering values inside the same partition (1,4) -> next is (1,10)
            long between14 = map.exactRowIdOrInvertedCeiling(buildPk(partitioner, 1, 4));
            assertEquals(-id110 - 1, between14);

            // Cross-partition boundary: after last row of pk=1, the next global row id should be id110 + 1
            long afterLastPk1 = map.exactRowIdOrInvertedCeiling(buildPk(partitioner, 1, Integer.MAX_VALUE));
            // If (1, Integer.MAX_VALUE) does not exist (it doesn't), expect inverted ceiling to be -nextId - 1
            long expectedNextAfterPk1 = id110 + 1;
            assertEquals(-expectedNextAfterPk1 - 1, afterLastPk1);

            // After last: token-only with token greater than last should give Long.MIN_VALUE
            PrimaryKey lastPk = map.primaryKeyFromRowId(count - 1);
            long tLast = lastPk.token().getLongValue();

            long invAfterLast = map.exactRowIdOrInvertedCeiling(pkFactory.createTokenOnly(partitioner.getTokenFactory().fromLongValue(tLast + 1)));
            assertEquals("Expected inverted ceiling beyond end to be Long.MIN_VALUE", Long.MIN_VALUE, invAfterLast);
        }
    }

    @Test
    public void testCeiling() throws Throwable
    {
        IndexComponents.ForRead perSSTableComponents = indexDescriptor.perSSTableComponents();
        try (PrimaryKeyMap.Factory factory = perSSTableComponents.onDiskFormat().newPrimaryKeyMapFactory(perSSTableComponents, pkFactory, sstable);
             PrimaryKeyMap map = factory.newPerSSTablePrimaryKeyMap())
        {
            long count = map.count();

            IPartitioner partitioner = sstable.metadata().partitioner;

            // Get boundary tokens for before/after tests
            PrimaryKey firstPk = map.primaryKeyFromRowId(0);
            long t0 = firstPk.token().getLongValue();

            // Before first: token-only with token less than first should yield a negative inverted ceiling
            long ceilBeforeFirst = map.ceiling(pkFactory.createTokenOnly(partitioner.getTokenFactory().fromLongValue(t0 - 1)));
            assertEquals("Expected inverted ceiling before start to be negative", 0, ceilBeforeFirst);

            // Test exact matches - ceiling should return the exact row id
            long id11 = map.ceiling(buildPk(partitioner, 1, 1));
            long id12 = map.ceiling(buildPk(partitioner, 1, 2));
            long id13 = map.ceiling(buildPk(partitioner, 1, 3));
            long id110 = map.ceiling(buildPk(partitioner, 1, 10));

            assertTrue("Ceiling for exact matches should return valid row ids", id11 >= 0);
            assertTrue("Row ids should be ordered by clustering", id11 < id12 && id12 < id13 && id13 < id110);

            // Test between clustering values - ceiling should return next greater row id
            long ceiling14 = map.ceiling(buildPk(partitioner, 1, 4));
            assertEquals("Ceiling for (1,4) should be row id of (1,10)", id110, ceiling14);

            // Test ceiling between existing clustering values
            long ceiling0 = map.ceiling(buildPk(partitioner, 1, 0));
            assertEquals("Ceiling for (1,0) should be row id of (1,1)", id11, ceiling0);

            // Test ceiling after last row in partition pk=1
            long ceilingAfterPk1 = map.ceiling(buildPk(partitioner, 1, Integer.MAX_VALUE));
            // Should either find next partition or return negative
            assertTrue("Ceiling after last row of pk=1 should be valid or negative", ceilingAfterPk1 >= id110 || ceilingAfterPk1 < 0);

            // Test ceiling for partition pk=2
            long id21 = map.ceiling(buildPk(partitioner, 2, 1));
            long id25 = map.ceiling(buildPk(partitioner, 2, 5));
            assertTrue("Ceiling for pk=2 rows should return valid row ids", id21 >= 0 && id25 >= 0);
            assertTrue("Row ids in pk=2 should be ordered", id21 < id25);

            // Test ceiling between rows in pk=2
            long ceiling23 = map.ceiling(buildPk(partitioner, 2, 3));
            assertEquals("Ceiling for (2,3) should be row id of (2,5)", id25, ceiling23);

            // Test ceiling for partition pk=1000
            long id1000_1 = map.ceiling(buildPk(partitioner, 1000, 1));
            long id1000_2 = map.ceiling(buildPk(partitioner, 1000, 2));
            assertTrue("Ceiling for pk=1000 rows should return valid row ids", id1000_1 >= 0 && id1000_2 >= 0);
            assertTrue("Row ids in pk=1000 should be ordered", id1000_1 < id1000_2);

            // Test floor with token-only key matching a partition
            Token token1 = partitioner.getToken(Int32Type.instance.decompose(1));
            long floorToken1 = map.ceiling(pkFactory.createTokenOnly(token1));
            assertEquals("Floor for token-only matching partition should return last row of that partition", id11, floorToken1);

            // After last: token-only with token greater than last should give -1
            PrimaryKey lastPk = map.primaryKeyFromRowId(count - 1);
            long tLast = lastPk.token().getLongValue();

            long ceilAfterLast = map.ceiling(pkFactory.createTokenOnly(partitioner.getTokenFactory().fromLongValue(tLast + 1)));
            assertEquals("Expected inverted ceiling beyond end to be -1", -1, ceilAfterLast);
        }
    }

    @Test
    public void testFloor() throws Throwable
    {
        IndexComponents.ForRead perSSTableComponents = indexDescriptor.perSSTableComponents();
        try (PrimaryKeyMap.Factory factory = perSSTableComponents.onDiskFormat().newPrimaryKeyMapFactory(perSSTableComponents, pkFactory, sstable);
             PrimaryKeyMap map = factory.newPerSSTablePrimaryKeyMap())
        {
            long count = map.count();
            IPartitioner partitioner = sstable.metadata().partitioner;

            PrimaryKey firstPk = map.primaryKeyFromRowId(0);
            long t0 = firstPk.token().getLongValue();

            // Before first: token-only with token less than first should yield a negative inverted ceiling
            long floorBeforeFirst = map.floor(pkFactory.createTokenOnly(partitioner.getTokenFactory().fromLongValue(t0 - 1)));
            assertEquals("Expected inverted ceiling before start to be negative", -1, floorBeforeFirst);

            // Test exact matches - floor should return the exact row id
            long id11 = map.floor(buildPk(partitioner, 1, 1));
            long id12 = map.floor(buildPk(partitioner, 1, 2));
            long id13 = map.floor(buildPk(partitioner, 1, 3));
            long id110 = map.floor(buildPk(partitioner, 1, 10));

            assertTrue("Row ids should be ordered by clustering", 0 <= id11 && id11 < id12 && id12 < id13 && id13 < id110);

            // Test floor before first row in partition
            long floorBeforePk1 = map.floor(buildPk(partitioner, 1, 0));
            assertTrue("Floor before first row of pk=1 should be negative or from previous partition", floorBeforePk1 < id11);

            // Test between clustering values - floor should return previous lesser row id
            long floor14 = map.floor(buildPk(partitioner, 1, 4));
            assertEquals("Floor for (1,4) should be row id of (1,3)", id13, floor14);

            // Test floor after last row in partition
            long floorAfterPk1 = map.floor(buildPk(partitioner, 1, Integer.MAX_VALUE));
            assertEquals("Floor after last row of pk=1 should be row id of (1,10)", id110, floorAfterPk1);

            // Test floor with token-only key after last partition
            PrimaryKey lastPk = map.primaryKeyFromRowId(count - 1);
            long tLast = lastPk.token().getLongValue();

            // Test floor with token-only key matching a partition
            Token token1 = partitioner.getToken(Int32Type.instance.decompose(1));
            long floorToken1 = map.floor(pkFactory.createTokenOnly(token1));
            assertEquals("Floor for token-only matching partition should return last row of that partition", id110, floorToken1);

            long floorAfterLast = map.floor(pkFactory.createTokenOnly(partitioner.getTokenFactory().fromLongValue(tLast + 1)));
            assertEquals("Floor after last should return last row id", count - 1, floorAfterLast);
        }
    }
}
