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
import org.apache.cassandra.schema.TableMetadata;

import static org.junit.Assert.assertEquals;

/**
 * Skinny-table tests (no clustering columns) for
 * {@link PartitionAwarePrimaryKeyMap#exactRowIdOrInvertedCeiling(PrimaryKey)} using the legacy V1 on-disk format (AA).
 */
public class PartitionAwareSkinnyPrimaryKeyMapTest extends SAITester
{
    private final IndexContext intContext = SAITester.createIndexContext("int_index", Int32Type.instance);
    private final IndexContext textContext = SAITester.createIndexContext("text_index", UTF8Type.instance);
    private IndexDescriptor indexDescriptor;
    private SSTableReader sstable;
    private PrimaryKey.Factory pkFactory;

    @Before
    public void setup() throws Throwable
    {
        // Set the version to AA (legacy V1 format) before creating indexes
        SAIUtil.setCurrentVersion(Version.AA);

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
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        sstable = cfs.getLiveSSTables().iterator().next();

        // Build IndexDescriptor from the live SSTable using the matching index contexts
        indexDescriptor = IndexDescriptor.load(sstable, Set.of(intContext, textContext));
        TableMetadata tableMetadata = cfs.metadata.get();
        pkFactory = indexDescriptor.perSSTableComponents().version().onDiskFormat().newPrimaryKeyFactory(tableMetadata.comparator);
    }

    @After
    public void teardown()
    {
        SAIUtil.setCurrentVersion(Version.LATEST);
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

            // 3) Between first and second (or equal if collision): expect next id 1 -> -(1)-1 == -2
            long midTokenValue = t0 + ((t1 - t0) / 2);
            long invCeilBetween01 = map.exactRowIdOrInvertedCeiling(pkFactory.createTokenOnly(partitioner.getTokenFactory().fromLongValue(midTokenValue)));
            assertEquals(-2, invCeilBetween01);

            // 4) Exact last
            long lastRowId = map.exactRowIdOrInvertedCeiling(pkFactory.createTokenOnly(lastPk.token()));
            assertEquals(count - 1, lastRowId);

            // 5) After last: expect inverted ceiling to be -(count) - 1
            long invCeilAfterLast = map.exactRowIdOrInvertedCeiling(pkFactory.createTokenOnly(partitioner.getTokenFactory().fromLongValue(tLast + 1)));
            assertEquals("Expected inverted ceiling beyond end to be -count-1", invCeilAfterLast, -count - 1);
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

            // 5) After last: AA format returns -1 for ceiling when beyond last token
            long ceilingAfterLast = map.ceiling(pkFactory.createTokenOnly(partitioner.getTokenFactory().fromLongValue(tLast + 1)));
            assertEquals(-1, ceilingAfterLast);
        }
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testFloorUnsupported() throws Throwable
    {
        IndexComponents.ForRead perSSTableComponents = indexDescriptor.perSSTableComponents();
        try (PrimaryKeyMap.Factory factory = perSSTableComponents.onDiskFormat().newPrimaryKeyMapFactory(perSSTableComponents, pkFactory, sstable);
             PrimaryKeyMap map = factory.newPerSSTablePrimaryKeyMap())
        {
            // AA format does not support floor operation; it should throw UnsupportedOperationException
            PrimaryKey firstPk = map.primaryKeyFromRowId(0);
            map.floor(pkFactory.createTokenOnly(firstPk.token()));
        }
    }
}
