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
import static org.junit.Assert.assertTrue;

/**
 * Wide-table tests (with clustering columns) for
 * {@link RowAwarePrimaryKeyMap} using the row-aware on-disk format.
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
        try (PrimaryKeyMap.Factory factory = perSSTableComponents.onDiskFormat().newPrimaryKeyMapFactory(perSSTableComponents, pkFactory, sstable);
             PrimaryKeyMap map = factory.newPerSSTablePrimaryKeyMap())
        {
            long count = map.count();

            PrimaryKey firstPk = map.primaryKeyFromRowId(0);
            PrimaryKey lastPk = map.primaryKeyFromRowId(count - 1);
            long t0 = firstPk.token().getLongValue();
            long tLast = lastPk.token().getLongValue();

            // 1_ Before first: inverted first RowId (0), -1
            long beforeFirst = map.exactRowIdOrInvertedCeiling(pkFactory.createTokenOnly(partitioner.getTokenFactory().fromLongValue(t0 - 1)));
            assertEquals(-1, beforeFirst);

            // 2) Exact first token (should match first row of that partition)
            long firstRowId = map.exactRowIdOrInvertedCeiling(firstPk);
            assertEquals(0, firstRowId);

            // 3) Exact matches within a single partition should resolve and be ordered by clustering
            long id11 = map.exactRowIdOrInvertedCeiling(buildPk(partitioner, 1, 1));
            long id12 = map.exactRowIdOrInvertedCeiling(buildPk(partitioner, 1, 2));
            long id13 = map.exactRowIdOrInvertedCeiling(buildPk(partitioner, 1, 3));
            long id110 = map.exactRowIdOrInvertedCeiling(buildPk(partitioner, 1, 10));
            assertTrue(0 <= id11);
            assertEquals(id11 + 1, id12);
            assertEquals(id12 + 1, id13);
            assertEquals(id13 + 1, id110);

            // 4) Between clustering values inside the same partition (1,4) -> next is (1,10)
            long between14 = map.exactRowIdOrInvertedCeiling(buildPk(partitioner, 1, 4));
            assertEquals(-id110 - 1, between14);

            // 5) Cross-partition boundary: after last row of pk=1, the next global row id should be id110 + 1, inverted ceiling to be -nextId - 1
            long afterLastPk1 = map.exactRowIdOrInvertedCeiling(buildPk(partitioner, 1, Integer.MAX_VALUE));
            long expectedNextIdAfterPk1 = id110 + 1;
            assertEquals(-expectedNextIdAfterPk1 - 1, afterLastPk1);

            // 6) Exact last
            long invLast = map.exactRowIdOrInvertedCeiling(lastPk);
            assertEquals(count - 1, invLast);

            // 7) After last: Expected inverted ceiling beyond end to be Long.MIN_VALUE
            long afterLast = map.exactRowIdOrInvertedCeiling(pkFactory.createTokenOnly(partitioner.getTokenFactory().fromLongValue(tLast + 1)));
            assertEquals(Long.MIN_VALUE, afterLast);
        }
    }

    @Test
    public void testCeiling() throws Throwable
    {
        try (PrimaryKeyMap.Factory factory = perSSTableComponents.onDiskFormat().newPrimaryKeyMapFactory(perSSTableComponents, pkFactory, sstable);
             PrimaryKeyMap map = factory.newPerSSTablePrimaryKeyMap())
        {
            long count = map.count();

            PrimaryKey firstPk = map.primaryKeyFromRowId(0);
            PrimaryKey lastPk = map.primaryKeyFromRowId(count - 1);
            long t0 = firstPk.token().getLongValue();
            long tLast = lastPk.token().getLongValue();

            // 1_ Before first: first RowId 0
            long beforeFirst = map.ceiling(pkFactory.createTokenOnly(partitioner.getTokenFactory().fromLongValue(t0 - 1)));
            assertEquals(0, beforeFirst);

            // 2) Exact first token (should match first row of that partition)
            long firstRowId = map.ceiling(firstPk);
            assertEquals(0, firstRowId);

            // 3) Exact matches within a single partition should resolve and be ordered by clustering
            long id11 = map.ceiling(buildPk(partitioner, 1, 1));
            long id12 = map.ceiling(buildPk(partitioner, 1, 2));
            long id13 = map.ceiling(buildPk(partitioner, 1, 3));
            long id110 = map.ceiling(buildPk(partitioner, 1, 10));
            assertTrue(0 <= id11);
            assertEquals(id11 + 1, id12);
            assertEquals(id12 + 1, id13);
            assertEquals(id13 + 1, id110);

            // 4) Between clustering values inside the same partition (1,4) -> next is (1,10)
            long between14 = map.ceiling(buildPk(partitioner, 1, 4));
            assertEquals(id110, between14);

            // 5) Cross-partition boundary: after last row of pk=1, the next global row id should be id110 + 1
            long afterLastPk1 = map.ceiling(buildPk(partitioner, 1, Integer.MAX_VALUE));
            assertEquals(id110 + 1, afterLastPk1);

            // 6) Exact last
            long invLast = map.ceiling(lastPk);
            assertEquals(count - 1, invLast);

            // 7) After last: Expected ceiling beyond end to be -1
            long afterLast = map.ceiling(pkFactory.createTokenOnly(partitioner.getTokenFactory().fromLongValue(tLast + 1)));
            assertEquals(-1, afterLast);
        }
    }

    @Test
    public void testFloor() throws Throwable
    {
        try (PrimaryKeyMap.Factory factory = perSSTableComponents.onDiskFormat().newPrimaryKeyMapFactory(perSSTableComponents, pkFactory, sstable);
             PrimaryKeyMap map = factory.newPerSSTablePrimaryKeyMap())
        {
            long count = map.count();

            PrimaryKey firstPk = map.primaryKeyFromRowId(0);
            PrimaryKey lastPk = map.primaryKeyFromRowId(count - 1);
            long t0 = firstPk.token().getLongValue();
            long tLast = lastPk.token().getLongValue();

            // 1_ Before first: expect -1
            long beforeFirst = map.floor(pkFactory.createTokenOnly(partitioner.getTokenFactory().fromLongValue(t0 - 1)));
            assertEquals(-1, beforeFirst);

            // 2) Exact first token (should match first row of that partition)
            long firstRowId = map.floor(firstPk);
            assertEquals(0, firstRowId);

            // 3) Exact matches within a single partition should resolve and be ordered by clustering
            long id11 = map.floor(buildPk(partitioner, 1, 1));
            long id12 = map.floor(buildPk(partitioner, 1, 2));
            long id13 = map.floor(buildPk(partitioner, 1, 3));
            long id110 = map.floor(buildPk(partitioner, 1, 10));
            assertTrue(0 <= id11);
            assertEquals(id11 + 1, id12);
            assertEquals(id12 + 1, id13);
            assertEquals(id13 + 1, id110);

            // 4) Between clustering values inside the same partition (1,4) -> previous is (1,3)
            long between14 = map.floor(buildPk(partitioner, 1, 4));
            assertEquals(id13, between14);

            // 5) Cross-partition boundary: after last row of pk=1, the previous row id in the same partition should be id110
            long afterLastPk1 = map.floor(buildPk(partitioner, 1, Integer.MAX_VALUE));
            assertEquals(id110, afterLastPk1);

            // 6) Exact last
            long invLast = map.floor(lastPk);
            assertEquals(count - 1, invLast);

            // 7) After last: Expect last
            long afterLast = map.floor(pkFactory.createTokenOnly(partitioner.getTokenFactory().fromLongValue(tLast + 1)));
            assertEquals(count - 1, afterLast);
        }
    }
}
