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
import org.apache.cassandra.schema.TableMetadata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Skinny-table tests (no clustering columns) for
 * {@link RowAwarePrimaryKeyMap#exactRowIdOrInvertedCeiling(PrimaryKey)} using the row-aware on-disk format.
 */
public class RowAwareSkinnyPrimaryKeyMapTest extends SAITester
{
    private final IndexContext intContext = SAITester.createIndexContext("int_index", Int32Type.instance);
    private final IndexContext textContext = SAITester.createIndexContext("text_index", UTF8Type.instance);
    private IndexDescriptor indexDescriptor;
    private SSTableReader sstable;
    private PrimaryKey.Factory pkFactory;

    @Before
    public void setup() throws Throwable
    {
        // Create a skinny table (no clustering), and two SAI indexes to ensure primary key components exist
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
        var cfs = getCurrentColumnFamilyStore();
        sstable = cfs.getLiveSSTables().iterator().next();

        // Build IndexDescriptor from the live SSTable using the matching index contexts
        indexDescriptor = IndexDescriptor.load(sstable, Set.of(intContext, textContext));
        TableMetadata tableMetadata = cfs.metadata.get();
        pkFactory = indexDescriptor.perSSTableComponents().version().onDiskFormat().newPrimaryKeyFactory(tableMetadata.comparator);
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

            // Prepare tokens in non-decreasing order to satisfy block-packed reader expectations
            PrimaryKey firstPk = map.primaryKeyFromRowId(0);
            PrimaryKey secondPk = map.primaryKeyFromRowId(1);
            PrimaryKey lastPk = map.primaryKeyFromRowId(count - 1);

            long t0 = firstPk.token().getLongValue();
            long t1 = secondPk.token().getLongValue();
            long tLast = lastPk.token().getLongValue();

            // 1) Before first: expect -1 (next id 0)
            long invCeilBeforeFirst = map.exactRowIdOrInvertedCeiling(pkFactory.createTokenOnly(partitioner.getTokenFactory().fromLongValue(t0 - 1)));
            assertEquals(-1, invCeilBeforeFirst);

            // 2) Exact first
            long firstRowId = map.exactRowIdOrInvertedCeiling(pkFactory.createTokenOnly(firstPk.token()));
            assertEquals(0, firstRowId);

            // 3) Between first and second (or equal if collision): expect next id 1 -> -1-1 == -2
            long midTokenValue = t0 + ((t1 - t0) / 2);
            long invCeilBetween01 = map.exactRowIdOrInvertedCeiling(pkFactory.createTokenOnly(partitioner.getTokenFactory().fromLongValue(midTokenValue)));
            assertEquals(-2, invCeilBetween01);

            // 4) Exact last
            long lastRowId = map.exactRowIdOrInvertedCeiling(pkFactory.createTokenOnly(lastPk.token()));
            assertEquals(count - 1, lastRowId);

            // 5) After last: expect inverted ceiling to be -count - 1 or Long.MIN_VALUE
            long tAfter = tLast + 1;
            long invCeilAfterLast = map.exactRowIdOrInvertedCeiling(pkFactory.createTokenOnly(partitioner.getTokenFactory().fromLongValue(tAfter)));
            long expectedStandardAfterLast = -count - 1;
            assertEquals("Expected inverted ceiling beyond end to be either Long.MIN_VALUE or -(count)-1", invCeilAfterLast, expectedStandardAfterLast);
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

            // Prepare tokens in non-decreasing order to satisfy block-packed reader expectations
            PrimaryKey firstPk = map.primaryKeyFromRowId(0);
            PrimaryKey secondPk = map.primaryKeyFromRowId(1);
            PrimaryKey lastPk = map.primaryKeyFromRowId(count - 1);

            long t0 = firstPk.token().getLongValue();
            long t1 = secondPk.token().getLongValue();
            long tLast = lastPk.token().getLongValue();

            // 1) Before first: expect ceiling to be 0 (first row)
            long ceilingBeforeFirst = map.ceiling(pkFactory.createTokenOnly(partitioner.getTokenFactory().fromLongValue(t0 - 1)));
            assertEquals(0, ceilingBeforeFirst);

            // 2) Exact first: expect 0
            long ceilingFirst = map.ceiling(pkFactory.createTokenOnly(firstPk.token()));
            assertEquals(0, ceilingFirst);

            // 3) Between first and second: expect ceiling to be 1 (second row)
            long midTokenValue = t0 + ((t1 - t0) / 2);
            long ceilingBetween01 = map.ceiling(pkFactory.createTokenOnly(partitioner.getTokenFactory().fromLongValue(midTokenValue)));
            assertEquals(1, ceilingBetween01);

            // 4) Exact last: expect last row id
            long ceilingLast = map.ceiling(pkFactory.createTokenOnly(lastPk.token()));
            assertEquals(count - 1, ceilingLast);

            // 5) After last: expect count (ceiling wraps around to count when beyond last)
            long ceilingAfterLast = map.ceiling(pkFactory.createTokenOnly(partitioner.getTokenFactory().fromLongValue(tLast + 1)));
            // When token is after the last, skinnyExactRowIdOrInvertedCeiling returns -(count)-1,
            // and ceiling converts it back to count
            assertEquals(count, ceilingAfterLast);
        }
    }

    @Test
    public void testFloorSkinny() throws Throwable
    {
        IndexComponents.ForRead perSSTableComponents = indexDescriptor.perSSTableComponents();
        try (PrimaryKeyMap.Factory factory = perSSTableComponents.onDiskFormat().newPrimaryKeyMapFactory(perSSTableComponents, pkFactory, sstable);
             PrimaryKeyMap map = factory.newPerSSTablePrimaryKeyMap())
        {
            long count = map.count();

            IPartitioner partitioner = sstable.metadata().partitioner;

            // Prepare tokens in non-decreasing order to satisfy block-packed reader expectations
            PrimaryKey firstPk = map.primaryKeyFromRowId(0);
            PrimaryKey secondPk = map.primaryKeyFromRowId(1);
            PrimaryKey lastPk = map.primaryKeyFromRowId(count - 1);

            long t0 = firstPk.token().getLongValue();
            long t1 = secondPk.token().getLongValue();
            long tLast = lastPk.token().getLongValue();

            // 1) Before first: expect negative value (no floor exists)
            long floorBeforeFirst = map.floor(pkFactory.createTokenOnly(partitioner.getTokenFactory().fromLongValue(t0 - 1)));
            assertTrue("Expected negative value for floor before first", floorBeforeFirst < 0);

            // 2) Exact first: expect 0
            long floorFirst = map.floor(pkFactory.createTokenOnly(firstPk.token()));
            assertEquals(0, floorFirst);

            // 3) Between first and second: expect floor to be 0 (first row)
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
