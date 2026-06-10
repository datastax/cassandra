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

package org.apache.cassandra.index.sai.disk;

import java.nio.ByteBuffer;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.disk.format.IndexComponents;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.io.sstable.format.SSTableReader;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link PrimaryKeyMap} with tables that have static columns,
 * using the row-aware on-disk format. Static columns create special rows
 * with STATIC_CLUSTERING that need to be handled correctly.
 */
public class RowAwareStaticClusteringPrimaryKeyMapTest extends SAITester.Versioned.RawAware
{
    private final IndexContext intContext = SAITester.createIndexContext("int_index", Int32Type.instance);
    private final IndexContext textContext = SAITester.createIndexContext("text_index", UTF8Type.instance);
    private final IndexContext staticContext = SAITester.createIndexContext("static_index", UTF8Type.instance);

    private PrimaryKey.Factory pkFactory;
    private IPartitioner partitioner;
    private PrimaryKeyMap.Factory factory;

    private long idPk1Static;
    private long idPk2Static;
    private long idPk1Ck1;

    @Before
    public void setup() throws Throwable
    {
        // Create a table with static columns and clustering columns
        createTable("CREATE TABLE %s (pk int, ck int, static_value text static, int_value int, text_value text, PRIMARY KEY (pk, ck)) WITH CLUSTERING ORDER BY (ck ASC)");
        execute("CREATE CUSTOM INDEX int_index ON %s(int_value) USING 'StorageAttachedIndex'");
        execute("CREATE CUSTOM INDEX text_index ON %s(text_value) USING 'StorageAttachedIndex'");
        execute("CREATE CUSTOM INDEX static_index ON %s(static_value) USING 'StorageAttachedIndex'");

        // Insert data with static columns
        // Partition pk=1: static row + multiple regular rows
        execute("INSERT INTO %s (pk, static_value) VALUES (?, ?)", 1, "static1");
        execute("INSERT INTO %s (pk, ck, int_value, text_value) VALUES (?, ?, ?, ?)", 1, 1, 11, "a1");
        execute("INSERT INTO %s (pk, ck, int_value, text_value) VALUES (?, ?, ?, ?)", 1, 2, 12, "a2");
        execute("INSERT INTO %s (pk, ck, int_value, text_value) VALUES (?, ?, ?, ?)", 1, 3, 13, "a3");

        // Partition pk=2: static row + single regular row
        execute("INSERT INTO %s (pk, ck, int_value, text_value) VALUES (?, ?, ?, ?)", 2, 1, 21, "b1");
        execute("INSERT INTO %s (pk, static_value) VALUES (?, ?)", 2, "static2");

        // Partition pk=1000: static row + multiple regular rows
        execute("INSERT INTO %s (pk, static_value) VALUES (?, ?)", 1000, "static1000");
        execute("INSERT INTO %s (pk, ck, int_value, text_value) VALUES (?, ?, ?, ?)", 1000, 1, 1001, "c1");
        execute("INSERT INTO %s (pk, ck, int_value, text_value) VALUES (?, ?, ?, ?)", 1000, 2, 1002, "c2");
        execute("INSERT INTO %s (pk, ck, int_value, text_value) VALUES (?, ?, ?, ?)", 1000, 3, 1003, "c3");

        // Flush to generate SSTable and SAI components
        flush();

        // Obtain the just-flushed SSTable
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();

        // Build IndexDescriptor from the live SSTable using the matching index contexts
        IndexDescriptor indexDescriptor = IndexDescriptor.load(sstable, Set.of(intContext, textContext, staticContext));
        this.pkFactory = indexDescriptor.perSSTableComponents().version().onDiskFormat().newPrimaryKeyFactory(cfs.metadata.get().comparator);

        IndexComponents.ForRead perSSTableComponents = indexDescriptor.perSSTableComponents();
        this.partitioner = sstable.metadata().partitioner;
        this.factory = perSSTableComponents.onDiskFormat().newPrimaryKeyMapFactory(perSSTableComponents, pkFactory, sstable);

        try (PrimaryKeyMap map = factory.newPerSSTablePrimaryKeyMap())
        {
            idPk1Static = map.ceiling(buildStaticPk(1));
            idPk1Ck1 = map.ceiling(buildPk(1, 1));
            idPk2Static = map.ceiling(buildStaticPk(2));
        }
    }

    @After
    public void tearDown() throws Exception
    {
        if (factory != null)
            factory.close();
    }

    @Test
    public void testExactRowIdOrInvertedCeiling() throws Throwable
    {
        try (PrimaryKeyMap map = factory.newPerSSTablePrimaryKeyMap())
        {
            MapWalker mapWalker = new MapWalker(map::exactRowIdOrInvertedCeiling);

            mapWalker.assertRowIdForPK(beforeFirst(map), invert(0), "before first expects the inverted first");
            mapWalker.assertRowIdForPK(exactFirstRow(map), 0, "exact first row");

            // Test static row lookup
            mapWalker.assertRowIdForPK(buildStaticPk(1), idPk1Static, "exact pk=1 static row");
            // Test between static and first clustering row
            mapWalker.assertRowIdForPK(buildPk(1, 0), invert(idPk1Static + 1), "between static and ck=1 expects inverted ck=1");

            // Test regular clustering rows
            mapWalker.assertRowIdForPK(buildPk(1, 1), idPk1Static + 1, "exact pk=1, ck=1, which is next after the static row");
            mapWalker.assertRowIdForPK(buildPk(1, 2), idPk1Static + 2, "pk=1, ck=2 expects next after pk=1, ck=1");
            mapWalker.assertRowIdForPK(buildPk(1, 3), idPk1Static + 3, "exact pk=1, ck=3 expects next after pk=1, ck=2");

            // Test after last clustering in partition
            mapWalker.assertRowIdForPK(buildPk(1, Integer.MAX_VALUE),
                                       idPk1Static < map.count() ? invert(idPk1Static + 4) : Long.MIN_VALUE,
                                       "after pk=1 ck=3 expects inverted next partition first row or out of range if the last partition");

            mapWalker.assertRowIdForPK(buildStaticPk(2), idPk2Static, "exact pk=2 static row");
            mapWalker.assertRowIdForPK(buildPk(2, 1), idPk2Static + 1, "exact pk=2 ck=1");

            mapWalker.assertRowIdForPK(exactLastRow(map), map.count() - 1, "exact last row");
            mapWalker.assertRowIdForPK(afterLastToken(map), Long.MIN_VALUE, "after last expects out of range");
        }
    }

    @Test
    public void testCeiling() throws Throwable
    {
        try (PrimaryKeyMap map = factory.newPerSSTablePrimaryKeyMap())
        {
            MapWalker mapWalker = new MapWalker(map::ceiling);

            mapWalker.assertRowIdForPK(beforeFirst(map), 0, "before first expects the first");
            mapWalker.assertRowIdForPK(exactFirstRow(map), 0, "exact first row");

            // Test static row lookup
            mapWalker.assertRowIdForPK(buildStaticPk(1), idPk1Static, "exact pk=1 static row");

            // Test between static and first clustering - ceiling should return first clustering
            mapWalker.assertRowIdForPK(buildPk(1, 0),  idPk1Static + 1, "between static and ck=1 expects ck=1 (ceiling)");

            // Test regular clustering rows
            mapWalker.assertRowIdForPK(buildPk(1, 1), idPk1Static + 1, "exact pk=1, ck=1");
            mapWalker.assertRowIdForPK(buildPk(1, 2), idPk1Static + 2, "pk=1, ck=2 expects next after pk=1, ck=1");
            mapWalker.assertRowIdForPK(buildPk(1, 3), idPk1Static + 3, "exact pk=1, ck=3 expects next after pk=1, ck=2");

            // Test after last clustering in partition - should go to next partition first row
            mapWalker.assertRowIdForPK(buildPk(1, Integer.MAX_VALUE),
                                       idPk1Static < map.count() ? idPk1Static + 4 : -1,
                                       "after pk=1 ck=3 expects next partition first row or out of range if the last partition");

            mapWalker.assertRowIdForPK(exactLastRow(map), map.count() - 1, "exact last row");
            mapWalker.assertRowIdForPK(afterLastToken(map), -1, "after last expects out of range");
        }
    }

    @Test
    public void testFloor() throws Throwable
    {
        try (PrimaryKeyMap map = factory.newPerSSTablePrimaryKeyMap())
        {
            MapWalker mapWalker = new MapWalker(map::floor);

            mapWalker.assertRowIdForPK(beforeFirst(map), -1, "before first expects out of range");
            mapWalker.assertRowIdForPK(buildPk(1, 0), idPk1Ck1 - 1, "before ck=1 expects row before the first in pk 1 (floor) or out of range if the first partition");

            // Test regular clustering rows
            mapWalker.assertRowIdForPK(buildPk(1, 1), idPk1Ck1, "exact pk=1, ck=1");
            mapWalker.assertRowIdForPK(buildPk(1, 2), idPk1Ck1 + 1, "pk=1, ck=2 expects next after pk=1, ck=1");
            mapWalker.assertRowIdForPK(buildPk(1, 3), idPk1Ck1 + 2, "exact pk=1, ck=3 expects next after pk=1, ck=2");

            // Test static row lookup
            mapWalker.assertRowIdForPK(buildStaticPk(1), idPk1Ck1 + 2, "exact pk=1 static row");


            // Test after last clustering in partition - floor should return last clustering
            mapWalker.assertRowIdForPK(buildPk(1, Integer.MAX_VALUE), idPk1Ck1 + 2, "after pk=1 ck=3 expects ck=3 (floor)");

            mapWalker.assertRowIdForPK(exactLastRow(map), map.count() - 1, "exact last row");
            mapWalker.assertRowIdForPK(afterLastToken(map), map.count() - 1, "after last expects the last row");
        }
    }

    private PrimaryKey buildPk(int partitionKey, int clusteringKey)
    {
        ByteBuffer pkBuf = Int32Type.instance.decompose(partitionKey);
        Token token = partitioner.getToken(pkBuf);
        DecoratedKey key = new BufferDecoratedKey(token, pkBuf);
        Clustering<ByteBuffer> clustering = Clustering.make(Int32Type.instance.decompose(clusteringKey));
        return pkFactory.create(key, clustering);
    }

    private PrimaryKey buildStaticPk(int partitionKey)
    {
        ByteBuffer pkBuf = Int32Type.instance.decompose(partitionKey);
        Token token = partitioner.getToken(pkBuf);
        DecoratedKey key = new BufferDecoratedKey(token, pkBuf);
        return pkFactory.create(key, Clustering.STATIC_CLUSTERING);
    }

    private PrimaryKey beforeFirst(PrimaryKeyMap map)
    {
        PrimaryKey firstPk = map.primaryKeyFromRowId(0);
        long firstToken = firstPk.token().getLongValue();
        return pkFactory.createTokenOnly(partitioner.getTokenFactory().fromLongValue(firstToken - 1));
    }

    private PrimaryKey exactFirstRow(PrimaryKeyMap map)
    {
        return map.primaryKeyFromRowId(0);
    }

    private PrimaryKey exactLastRow(PrimaryKeyMap map)
    {
        return map.primaryKeyFromRowId(map.count() - 1);
    }

    private PrimaryKey afterLastToken(PrimaryKeyMap map)
    {
        PrimaryKey lastPk = map.primaryKeyFromRowId(map.count() - 1);
        long lastToken = lastPk.token().getLongValue();
        return pkFactory.createTokenOnly(partitioner.getTokenFactory().fromLongValue(lastToken + 1));
    }

    private long invert(long rowId)
    {
        return -rowId - 1;
    }

    /**
     * Functional interface for PrimaryKeyMap API methods that take a PrimaryKey and return a row ID.
     */
    @FunctionalInterface
    private interface PrimaryKeyMapFunction
    {
        long apply(PrimaryKey pk);
    }

     /**
      * Helper class for testing static clustering columns.
      * Provides position generators and assertion methods for testing PrimaryKeyMap operations
      * with static rows.
      */
     private static class MapWalker
     {
         private final PrimaryKeyMapFunction rowIdFromPKMethod;

         MapWalker(PrimaryKeyMapFunction rowIdFromPKMethod)
         {
             this.rowIdFromPKMethod = rowIdFromPKMethod;
         }

         void assertRowIdForPK(PrimaryKey pk, long expected, String expectationMessage)
         {
             long actual = rowIdFromPKMethod.apply(pk);
             assertEquals(expectationMessage, expected, actual);
         }
     }
}
