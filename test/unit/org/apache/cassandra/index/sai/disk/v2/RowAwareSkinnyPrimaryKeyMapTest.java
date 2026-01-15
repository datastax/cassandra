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
 * {@link RowAwarePrimaryKeyMap} using the row-aware on-disk format.
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
            long count = map.count();

            PrimaryKey firstPk = map.primaryKeyFromRowId(0);
            PrimaryKey secondPk = map.primaryKeyFromRowId(1);
            PrimaryKey lastPk = map.primaryKeyFromRowId(count - 1);

            long t0 = firstPk.token().getLongValue();
            long tLast = lastPk.token().getLongValue();

            // 1) Before first: expect -1 (next id 0)
            long invCeilBeforeFirst = map.exactRowIdOrInvertedCeiling(pkFactory.createTokenOnly(partitioner.getTokenFactory().fromLongValue(t0 - 1)));
            assertEquals(-1, invCeilBeforeFirst);

            // 2) Exact first
            long firstRowId = map.exactRowIdOrInvertedCeiling(pkFactory.createTokenOnly(firstPk.token()));
            assertEquals(0, firstRowId);

            // 3) Between first and second: expect next id 1 -> -1-1 == -2
            long t1 = secondPk.token().getLongValue();
            long midTokenValue = t0 + ((t1 - t0) / 2);
            long invCeilBetween01 = map.exactRowIdOrInvertedCeiling(pkFactory.createTokenOnly(partitioner.getTokenFactory().fromLongValue(midTokenValue)));
            assertEquals(-2, invCeilBetween01);

            // 4) Exact last
            long lastRowId = map.exactRowIdOrInvertedCeiling(pkFactory.createTokenOnly(lastPk.token()));
            assertEquals(count - 1, lastRowId);

            // 5) After last: expect inverted ceiling to be Long.MIN_VALUE
            long invCeilAfterLast = map.exactRowIdOrInvertedCeiling(pkFactory.createTokenOnly(partitioner.getTokenFactory().fromLongValue(tLast + 1)));
            assertEquals(Long.MIN_VALUE, invCeilAfterLast);
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
            PrimaryKey secondPk = map.primaryKeyFromRowId(1);
            PrimaryKey lastPk = map.primaryKeyFromRowId(count - 1);

            long t0 = firstPk.token().getLongValue();
            long tLast = lastPk.token().getLongValue();

            // 1) Before first: expect ceiling to be 0 (first row)
            long ceilingBeforeFirst = map.ceiling(pkFactory.createTokenOnly(partitioner.getTokenFactory().fromLongValue(t0 - 1)));
            assertEquals(0, ceilingBeforeFirst);

            // 2) Exact first: expect 0
            long ceilingFirst = map.ceiling(pkFactory.createTokenOnly(firstPk.token()));
            assertEquals(0, ceilingFirst);

            // 3) Between first and second: expect ceiling to be 1 (second row)
            long t1 = secondPk.token().getLongValue();
            long midTokenValue = t0 + ((t1 - t0) / 2);
            long ceilingBetween01 = map.ceiling(pkFactory.createTokenOnly(partitioner.getTokenFactory().fromLongValue(midTokenValue)));
            assertEquals(1, ceilingBetween01);

            // 4) Exact last: expect last row id
            long ceilingLast = map.ceiling(pkFactory.createTokenOnly(lastPk.token()));
            assertEquals(count - 1, ceilingLast);

            // 5) After last: expect -1 for ceiling
            long ceilingAfterLast = map.ceiling(pkFactory.createTokenOnly(partitioner.getTokenFactory().fromLongValue(tLast + 1)));
            assertEquals(-1, ceilingAfterLast);
        }
    }

    @Test
    public void testFloorSkinny() throws Throwable
    {
        try (PrimaryKeyMap.Factory factory = perSSTableComponents.onDiskFormat().newPrimaryKeyMapFactory(perSSTableComponents, pkFactory, sstable);
             PrimaryKeyMap map = factory.newPerSSTablePrimaryKeyMap())
        {
            long count = map.count();

            PrimaryKey firstPk = map.primaryKeyFromRowId(0);
            PrimaryKey secondPk = map.primaryKeyFromRowId(1);
            PrimaryKey lastPk = map.primaryKeyFromRowId(count - 1);

            long t0 = firstPk.token().getLongValue();
            long tLast = lastPk.token().getLongValue();

            // 1) Before first: expect -1
            long floorBeforeFirst = map.floor(pkFactory.createTokenOnly(partitioner.getTokenFactory().fromLongValue(t0 - 1)));
            assertEquals(-1, floorBeforeFirst);

            // 2) Exact first: expect 0
            long floorFirst = map.floor(pkFactory.createTokenOnly(firstPk.token()));
            assertEquals(0, floorFirst);

            // 3) Between first and second: expect floor to be 0 (first row)
            long t1 = secondPk.token().getLongValue();
            long midTokenValue = t0 + ((t1 - t0) / 2);
            long floorBetween01 = map.floor(pkFactory.createTokenOnly(partitioner.getTokenFactory().fromLongValue(midTokenValue)));
            assertEquals(0, floorBetween01);

            // 4) Exact last: expect last row id
            long floorLast = map.floor(pkFactory.createTokenOnly(lastPk.token()));
            assertEquals(count - 1, floorLast);

            // 5) After last: expect last row id (floor is the last row)
            long floorAfterLast = map.floor(pkFactory.createTokenOnly(partitioner.getTokenFactory().fromLongValue(tLast + 1)));
            assertEquals(count - 1, floorAfterLast);
        }
    }
}
