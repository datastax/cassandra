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
 * Wide-table tests (with clustering columns) for
 * {@link PartitionAwarePrimaryKeyMap#exactRowIdOrInvertedCeiling(PrimaryKey)} using the legacy V1 on-disk format (AA).
 */
public class PartitionAwareWidePrimaryKeyMapTest extends SAITester
{
    private final IndexContext intContext = SAITester.createIndexContext("int_index", Int32Type.instance);
    private final IndexContext textContext = SAITester.createIndexContext("text_index", UTF8Type.instance);
    private IndexDescriptor indexDescriptor;
    private SSTableReader sstable;
    private PrimaryKey.Factory pkFactory;

    private static long getNextTokenRowId(long count, PrimaryKeyMap map, PrimaryKey pk)
    {
        long i = 0;
        while (map.primaryKeyFromRowId(i).token().equals(pk.token()))
        {
            assert i < count;
            i++;
        }
        return i;
    }

    private static long getExpectedLastTokenFirstRowId(long count, PrimaryKeyMap map, PrimaryKey lastPk)
    {
        long i = 0;
        while (!map.primaryKeyFromRowId(i).token().equals(lastPk.token()))
        {
            assert i < count;
            i++;
        }
        return i;
    }

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

            // Find rows with different tokens (different partitions)
            // In wide partitions, V1 format only distinguishes by token, not clustering
            final PrimaryKey firstPk = map.primaryKeyFromRowId(0);
            final PrimaryKey lastPk = map.primaryKeyFromRowId(count - 1);

            // A row with next token than the first
            long secondTokenRowId = getNextTokenRowId(count, map, firstPk);
            PrimaryKey secondTokenPk = map.primaryKeyFromRowId(secondTokenRowId);
            assert secondTokenPk.token() != firstPk.token();

            long t0 = firstPk.token().getLongValue();
            long tLast = lastPk.token().getLongValue();

            // 1) Before first: expect -1 (next id 0)
            long invCeilBeforeFirst = map.exactRowIdOrInvertedCeiling(pkFactory.createTokenOnly(partitioner.getTokenFactory().fromLongValue(t0 - 1)));
            assertEquals(-1, invCeilBeforeFirst);

            // 2) Exact first token (should match first row of that partition)
            long firstRowId = map.exactRowIdOrInvertedCeiling(pkFactory.createTokenOnly(firstPk.token()));
            assertEquals(0, firstRowId);

            // 3) If we have different tokens, test between them
            long t1 = secondTokenPk.token().getLongValue();
            long midTokenValue = t0 + ((t1 - t0) / 2);
            long invCeilBetween = map.exactRowIdOrInvertedCeiling(pkFactory.createTokenOnly(partitioner.getTokenFactory().fromLongValue(midTokenValue)));
            // Should point to the next row after the first partition
            assertEquals(-secondTokenRowId - 1, invCeilBetween);

            // 4) Exact last token (should match first row of last partition)
            long lastTokenRowId = map.exactRowIdOrInvertedCeiling(pkFactory.createTokenOnly(lastPk.token()));
            // Find the first row with the last token
            long expectedLastTokenRowId = getExpectedLastTokenFirstRowId(count, map, lastPk);
            assertEquals(expectedLastTokenRowId, lastTokenRowId);

            // 5) After last: expect inverted ceiling to be Long.MIN_VALUE
            long invCeilAfterLast = map.exactRowIdOrInvertedCeiling(pkFactory.createTokenOnly(partitioner.getTokenFactory().fromLongValue(tLast + 1)));
            assertEquals("Expected inverted ceiling beyond end to be Long.MIN_VALUE", Long.MIN_VALUE, invCeilAfterLast);
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

            // Find rows with different tokens
            final PrimaryKey firstPk = map.primaryKeyFromRowId(0);
            final PrimaryKey lastPk = map.primaryKeyFromRowId(count - 1);

            // A row with next token than the first
            long secondTokenRowId = getNextTokenRowId(count, map, firstPk);
            PrimaryKey secondTokenPk = map.primaryKeyFromRowId(secondTokenRowId);
            assert secondTokenPk.token() != firstPk.token();

            long t0 = firstPk.token().getLongValue();
            long tLast = lastPk.token().getLongValue();

            // 1) Before first: expect ceiling to be 0 (first row)
            long ceilingBeforeFirst = map.ceiling(pkFactory.createTokenOnly(partitioner.getTokenFactory().fromLongValue(t0 - 1)));
            assertEquals(0, ceilingBeforeFirst);

            // 2) Exact first token: expect 0 (first row of that partition)
            long ceilingFirst = map.ceiling(pkFactory.createTokenOnly(firstPk.token()));
            assertEquals(0, ceilingFirst);

            // 3) If we have different tokens, test between them
            long t1 = secondTokenPk.token().getLongValue();
            long midTokenValue = t0 + ((t1 - t0) / 2);
            long ceilingBetween = map.ceiling(pkFactory.createTokenOnly(partitioner.getTokenFactory().fromLongValue(midTokenValue)));
            assertEquals(secondTokenRowId, ceilingBetween);

            // 4) Exact last token: expect first row of last partition
            long ceilingLast = map.ceiling(pkFactory.createTokenOnly(lastPk.token()));
            long expectedLastTokenRowId = getExpectedLastTokenFirstRowId(count, map, lastPk);
            assertEquals(expectedLastTokenRowId, ceilingLast);

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
