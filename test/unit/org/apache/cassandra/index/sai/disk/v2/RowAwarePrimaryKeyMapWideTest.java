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
 *
 * This test generates fresh SSTables and SAI components at runtime via SAITester.
 */
public class RowAwarePrimaryKeyMapWideTest extends SAITester
{
    private IndexDescriptor indexDescriptor;
    private SSTableReader sstable;
    private PrimaryKey.Factory pkFactory;

    private final IndexContext intContext = SAITester.createIndexContext("int_index", Int32Type.instance);
    private final IndexContext textContext = SAITester.createIndexContext("text_index", UTF8Type.instance);

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
        indexDescriptor = IndexDescriptor.empty(sstable.descriptor).reload(sstable, Set.of(intContext, textContext));
        TableMetadata tableMetadata = cfs.metadata.get().unbuild().build();
        pkFactory = indexDescriptor.perSSTableComponents().version().onDiskFormat().newPrimaryKeyFactory(tableMetadata.comparator);
    }

    private PrimaryKey buildPk(IPartitioner partitioner, int pk, int ck)
    {
        ByteBuffer pkBuf = Int32Type.instance.decompose(pk);
        Token token = partitioner.getToken(pkBuf);
        DecoratedKey key = new BufferDecoratedKey(token, pkBuf);
        Clustering clustering = Clustering.make(Int32Type.instance.decompose(ck));
        return pkFactory.create(key, clustering);
    }

    @Test
    public void testExactRowIdOrInvertedCeiling_wide() throws Throwable
    {
        IndexComponents.ForRead perSSTableComponents = indexDescriptor.perSSTableComponents();
        try (PrimaryKeyMap.Factory factory = perSSTableComponents.onDiskFormat().newPrimaryKeyMapFactory(perSSTableComponents, pkFactory, sstable);
             PrimaryKeyMap map = factory.newPerSSTablePrimaryKeyMap())
        {
            long count = map.count();
            assertTrue("Expected some rows in test sstable", count >= 6);

            IPartitioner partitioner = sstable.metadata().partitioner;

            // Get boundary tokens for before/after tests
            PrimaryKey firstPk = map.primaryKeyFromRowId(0);
            PrimaryKey lastPk = map.primaryKeyFromRowId(count - 1);
            long t0 = firstPk.token().getLongValue();
            long tLast = lastPk.token().getLongValue();

            // Exact matches within a single partition should resolve and be ordered by clustering
            long id_1_1 = map.exactRowIdOrInvertedCeiling(buildPk(partitioner, 1, 1));
            long id_1_2 = map.exactRowIdOrInvertedCeiling(buildPk(partitioner, 1, 2));
            long id_1_3 = map.exactRowIdOrInvertedCeiling(buildPk(partitioner, 1, 3));
            long id_1_10 = map.exactRowIdOrInvertedCeiling(buildPk(partitioner, 1, 10));

            assertTrue(id_1_1 >= 0);
            assertTrue(id_1_2 >= 0);
            assertTrue(id_1_3 >= 0);
            assertTrue(id_1_10 >= 0);

            assertTrue("RowIds within a partition should be increasing by clustering",
                       id_1_1 < id_1_2 && id_1_2 < id_1_3 && id_1_3 < id_1_10);

            // Between clustering values inside the same partition (1,4) -> next is (1,10)
            long between_1_4 = map.exactRowIdOrInvertedCeiling(buildPk(partitioner, 1, 4));
            assertEquals(-(id_1_10) - 1, between_1_4);

            // Cross-partition boundary: after last row of pk=1, the next global row id should be id_1_10 + 1
            long after_last_pk1 = map.exactRowIdOrInvertedCeiling(buildPk(partitioner, 1, Integer.MAX_VALUE));
            // If (1, Integer.MAX_VALUE) does not exist (it doesn't), expect inverted ceiling to be -(nextId) - 1
            long expectedNextAfterPk1 = id_1_10 + 1;
            if (expectedNextAfterPk1 < count)
                assertEquals(-(expectedNextAfterPk1) - 1, after_last_pk1);
            else
                assertTrue("After last row overall should return Long.MIN_VALUE or -(count)-1",
                           after_last_pk1 == Long.MIN_VALUE || after_last_pk1 == -(count) - 1);

            // Before first: token-only with token less than first should yield a negative inverted ceiling
            long invBeforeFirst = map.exactRowIdOrInvertedCeiling(pkFactory.createTokenOnly(partitioner.getTokenFactory().fromLongValue(t0 - 1)));
            assertTrue("Expected inverted ceiling before start to be negative",
                       invBeforeFirst < 0);

            // After last: token-only with token greater than last should give Long.MIN_VALUE or -(count)-1
            long invAfterLast = map.exactRowIdOrInvertedCeiling(pkFactory.createTokenOnly(partitioner.getTokenFactory().fromLongValue(tLast == Long.MAX_VALUE ? tLast : tLast + 1)));
            long expectedStandardAfterLast = -(count) - 1;
            assertTrue("Expected inverted ceiling beyond end to be either Long.MIN_VALUE or -(count)-1",
                       invAfterLast == Long.MIN_VALUE || invAfterLast == expectedStandardAfterLast);

            // Sanity: exact on another partition
            long id_2_1 = map.exactRowIdOrInvertedCeiling(buildPk(partitioner, 2, 1));
            long id_2_5 = map.exactRowIdOrInvertedCeiling(buildPk(partitioner, 2, 5));
            assertTrue(id_2_1 >= 0);
            assertTrue(id_2_5 >= 0);
            assertTrue(id_2_1 < id_2_5);
        }
    }
}
