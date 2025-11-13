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
import org.apache.cassandra.db.ColumnFamilyStore;
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

import static org.junit.Assert.assertEquals;

/**
 * Wide-table tests (with clustering columns) for
 * {@link WidePrimaryKeyMap} using the row-aware on-disk format.
 */
public class RowAwareWidePrimaryKeyMapTest extends SAITester
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
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        this.sstable = cfs.getLiveSSTables().iterator().next();

        // Build IndexDescriptor from the live SSTable using the matching index contexts
        IndexDescriptor indexDescriptor = IndexDescriptor.load(sstable, Set.of(intContext, textContext));
        this.pkFactory = indexDescriptor.perSSTableComponents().version().onDiskFormat().newPrimaryKeyFactory(cfs.metadata.get().comparator);

        this.perSSTableComponents = indexDescriptor.perSSTableComponents();
        this.partitioner = sstable.metadata().partitioner;
    }

    @Test
    public void testExactRowIdOrInvertedCeiling() throws Throwable
    {
        try (PrimaryKeyMap.Factory factory = perSSTableComponents.onDiskFormat().newPrimaryKeyMapFactory(perSSTableComponents, pkFactory, sstable);
             PrimaryKeyMap map = factory.newPerSSTablePrimaryKeyMap())
        {
            MapWalker mapWalker = new MapWalker(map, map::exactRowIdOrInvertedCeiling);

            mapWalker.assertResult(mapWalker.beforeFirst(), -1, "before first expects the inverted first");
            mapWalker.assertResult(mapWalker.exactFirstRow(), 0, "exact first row");

            mapWalker.assertResult(mapWalker.exactPk1Ck1(), mapWalker.getId11(), "exact pk=1, ck=1");
            mapWalker.assertResult(mapWalker.exactPk1Ck2(), mapWalker.getId11() + 1, "pk=1, ck=2 expects next after pk=1, ck=1");
            mapWalker.assertResult(mapWalker.exactPk1Ck3(), mapWalker.getId12() + 1, "exact pk=1, ck=3 expects next after pk=1, ck=2");
            mapWalker.assertResult(mapWalker.exactPk1Ck10(), mapWalker.getId13() + 1, "exact pk=1, ck=10 expects next after pk=1, ck=3");
            mapWalker.assertResult(mapWalker.betweenPk1Ck3AndCk10(), -mapWalker.getId110() - 1, "between pk=1 ck=3 and ck=10 expects inverted ck=10");
            mapWalker.assertResult(mapWalker.afterLastCkInPk1(), -(mapWalker.getId110() + 1) - 1, "after last ck in pk=1 expects inverted next partition first row");

            mapWalker.assertResult(mapWalker.exactLastRow(), mapWalker.count - 1, "exact last row");
            mapWalker.assertResult(mapWalker.afterLastToken(), Long.MIN_VALUE, "after last expects out of range");
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
            mapWalker.assertResult(mapWalker.exactFirstRow(), 0, "exact first row");

            mapWalker.assertResult(mapWalker.exactPk1Ck1(), mapWalker.getId11(), "exact pk=1, ck=1");
            mapWalker.assertResult(mapWalker.exactPk1Ck2(), mapWalker.getId11() + 1, "pk=1, ck=2 expects next after pk=1, ck=1");
            mapWalker.assertResult(mapWalker.exactPk1Ck3(), mapWalker.getId12() + 1, "exact pk=1, ck=3 expects next after pk=1, ck=2");
            mapWalker.assertResult(mapWalker.exactPk1Ck10(), mapWalker.getId13() + 1, "exact pk=1, ck=10 expects next after pk=1, ck=3");
            mapWalker.assertResult(mapWalker.betweenPk1Ck3AndCk10(), mapWalker.getId110(), "between pk=1 ck=3 and ck=10 expects ck=10");
            mapWalker.assertResult(mapWalker.afterLastCkInPk1(), mapWalker.getId110() + 1, "after last ck in pk=1 expects next partition first row");

            mapWalker.assertResult(mapWalker.exactLastRow(), mapWalker.count - 1, "exact last row");
            mapWalker.assertResult(mapWalker.afterLastToken(), -1, "after last expects out of range");
        }
    }

    @Test
    public void testFloor() throws Throwable
    {
        try (PrimaryKeyMap.Factory factory = perSSTableComponents.onDiskFormat().newPrimaryKeyMapFactory(perSSTableComponents, pkFactory, sstable);
             PrimaryKeyMap map = factory.newPerSSTablePrimaryKeyMap())
        {
            MapWalker mapWalker = new MapWalker(map, map::floor);

            mapWalker.assertResult(mapWalker.beforeFirst(), -1, "before first expects out of range");
            mapWalker.assertResult(mapWalker.exactFirstRow(), 0, "exact first row");

            mapWalker.assertResult(mapWalker.exactPk1Ck1(), mapWalker.getId11(), "exact pk=1, ck=1");
            mapWalker.assertResult(mapWalker.exactPk1Ck2(), mapWalker.getId11() + 1, "pk=1, ck=2 expects next after pk=1, ck=1");
            mapWalker.assertResult(mapWalker.exactPk1Ck3(), mapWalker.getId12() + 1, "exact pk=1, ck=3 expects next after pk=1, ck=2");
            mapWalker.assertResult(mapWalker.exactPk1Ck10(), mapWalker.getId13() + 1, "exact pk=1, ck=10 expects next after pk=1, ck=3");
            mapWalker.assertResult(mapWalker.betweenPk1Ck3AndCk10(), mapWalker.getId13(), "between pk=1 ck=3 and ck=10 expects ck=3");
            mapWalker.assertResult(mapWalker.afterLastCkInPk1(), mapWalker.getId110(), "after last ck in pk=1 expects last ck in pk=1");

            mapWalker.assertResult(mapWalker.exactLastRow(), mapWalker.count - 1, "exact last row");
            mapWalker.assertResult(mapWalker.afterLastToken(), mapWalker.count - 1, "after last expects the last");
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

    /**
     * Helper class for wide partition tests with clustering columns.
     * Provides position generators and assertion methods for testing PrimaryKeyMap operations.
     */
    private class MapWalker
    {
        protected final long count;
        private final PrimaryKey pk11;
        private final PrimaryKey pk12;
        private final PrimaryKey pk13;
        private final PrimaryKey pk110;
        private final PrimaryKeyMapFunction rowIdFromPKMethod;
        private final PrimaryKey firstPk;
        private final PrimaryKey lastPk;
        private final long firstToken;
        private final long lastToken;
        private long id11 = -1;
        private long id12 = -1;
        private long id13 = -1;
        private long id110 = -1;

        MapWalker(PrimaryKeyMap map, PrimaryKeyMapFunction rowIdFromPKMethod)
        {
            this.rowIdFromPKMethod = rowIdFromPKMethod;
            this.count = map.count();
            this.firstPk = map.primaryKeyFromRowId(0);
            this.lastPk = map.primaryKeyFromRowId(count - 1);
            this.firstToken = firstPk.token().getLongValue();
            this.lastToken = lastPk.token().getLongValue();

            // Pre-compute row IDs for clustering tests
            this.pk11 = buildPk(partitioner, 1, 1);
            this.pk12 = buildPk(partitioner, 1, 2);
            this.pk13 = buildPk(partitioner, 1, 3);
            this.pk110 = buildPk(partitioner, 1, 10);
        }


        private PrimaryKey buildPk(IPartitioner partitioner, int pk, int ck)
        {
            ByteBuffer pkBuf = Int32Type.instance.decompose(pk);
            Token token = partitioner.getToken(pkBuf);
            DecoratedKey key = new BufferDecoratedKey(token, pkBuf);
            Clustering<ByteBuffer> clustering = Clustering.make(Int32Type.instance.decompose(ck));
            return pkFactory.create(key, clustering);
        }

        PrimaryKey beforeFirst()
        {
            return pkFactory.createTokenOnly(partitioner.getTokenFactory().fromLongValue(firstToken - 1));
        }

        PrimaryKey exactFirstRow()
        {
            return firstPk;
        }

        PrimaryKey exactLastRow()
        {
            return lastPk;
        }

        PrimaryKey afterLastToken()
        {
            return pkFactory.createTokenOnly(partitioner.getTokenFactory().fromLongValue(lastToken + 1));
        }

        PrimaryKey exactPk1Ck1()
        {
            return pk11;
        }

        PrimaryKey exactPk1Ck2()
        {
            return pk12;
        }

        PrimaryKey exactPk1Ck3()
        {
            return pk13;
        }

        PrimaryKey exactPk1Ck10()
        {
            return pk110;
        }

        PrimaryKey betweenPk1Ck3AndCk10()
        {
            return buildPk(partitioner, 1, 4);
        }

        PrimaryKey afterLastCkInPk1()
        {
            return buildPk(partitioner, 1, Integer.MAX_VALUE);
        }

        void assertResult(PrimaryKey pk, long expected, String expectationMessage)
        {
            long actual = rowIdFromPKMethod.apply(pk);
            assertEquals(expectationMessage, expected, actual);
        }

        public long getId11()
        {
            if (id11 == -1)
                id11 = rowIdFromPKMethod.apply(buildPk(partitioner, 1, 1));
            return id11;
        }

        public long getId12()
        {
            if (id12 == -1)
                id12 = rowIdFromPKMethod.apply(buildPk(partitioner, 1, 2));
            return id12;
        }

        public long getId13()
        {
            if (id13 == -1)
                id13 = rowIdFromPKMethod.apply(buildPk(partitioner, 1, 3));
            return id13;
        }

        public long getId110()
        {
            if (id110 == -1)
                id110 = rowIdFromPKMethod.apply(buildPk(partitioner, 1, 10));
            return id110;
        }
    }
}
