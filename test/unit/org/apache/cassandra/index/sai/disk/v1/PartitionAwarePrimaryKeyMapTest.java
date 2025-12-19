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

package org.apache.cassandra.index.sai.disk.v1;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.disk.FileUtils;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.format.IndexComponents;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.trieindex.TrieIndexFormat;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.service.StorageService;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link PartitionAwarePrimaryKeyMap#exactRowIdOrInvertedCeiling(PrimaryKey)} using
 * the legacy V1 on-disk format (AA).
 */
public class PartitionAwarePrimaryKeyMapTest
{
    private final TemporaryFolder temporaryFolder = new TemporaryFolder();
    private IndexDescriptor indexDescriptor;
    private SSTableReader sstable;
    private PrimaryKey.Factory pkFactory;

    private final IndexContext intContext = SAITester.createIndexContext("int_index", Int32Type.instance);
    private final IndexContext textContext = SAITester.createIndexContext("text_index", UTF8Type.instance);

    @BeforeClass
    public static void initialise()
    {
        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
        StorageService.instance.setPartitionerUnsafe(Murmur3Partitioner.instance);
    }

    @Before
    public void setup() throws Throwable
    {
        temporaryFolder.create();
        Descriptor descriptor = Descriptor.fromFilename(temporaryFolder.newFolder().getAbsolutePath() + "/bb-1-bti-Data.db");
        FileUtils.copySSTablesAndIndexes(descriptor, "aa");
        TableMetadata tableMetadata = TableMetadata.builder("test", "test")
                                                   .addPartitionKeyColumn("pk", Int32Type.instance)
                                                   .addRegularColumn("int_value", Int32Type.instance)
                                                   .addRegularColumn("text_value", UTF8Type.instance)
                                                   .build();
        sstable = TrieIndexFormat.instance.getReaderFactory().openNoValidation(descriptor, TableMetadataRef.forOfflineTools(tableMetadata));
        indexDescriptor = IndexDescriptor.empty(sstable.descriptor).reload(sstable, java.util.Set.of(intContext, textContext));
        pkFactory = indexDescriptor.perSSTableComponents().version().onDiskFormat().newPrimaryKeyFactory(tableMetadata.comparator);
    }

    @After
    public void teardown()
    {
        temporaryFolder.delete();
    }

    @Test
    public void testExactRowIdOrInvertedCeiling() throws Throwable
    {
        IndexComponents.ForRead perSSTableComponents = indexDescriptor.perSSTableComponents();
        PrimaryKeyMap.Factory factory = perSSTableComponents.onDiskFormat().newPrimaryKeyMapFactory(perSSTableComponents, pkFactory, sstable);
        try (PrimaryKeyMap map = factory.newPerSSTablePrimaryKeyMap())
        {
            long count = map.count();
            assertTrue("Expected some rows in test sstable", count > 2);

            IPartitioner partitioner = sstable.metadata().partitioner;

            // Exact match for first rowId
            PrimaryKey firstPk = map.primaryKeyFromRowId(0);
            long firstRowId = map.exactRowIdOrInvertedCeiling(pkFactory.createTokenOnly(firstPk.token()));
            assertEquals(0L, firstRowId);

            // Find a neighboring pair with a gap in token values to test non-exact (inverted ceiling)
            long gapIndex = -1;
            long gapMidValue = Long.MIN_VALUE;
            for (int i = 0; i < Math.min(count - 1, 20); i++) // scan a small prefix to find a gap quickly
            {
                long t0 = map.primaryKeyFromRowId(i).token().getLongValue();
                long t1 = map.primaryKeyFromRowId(i + 1).token().getLongValue();
                if (t1 - t0 >= 2) // ensure there is a missing token value between
                {
                    gapIndex = i;
                    gapMidValue = t0 + 1; // pick a value strictly between t0 and t1
                    break;
                }
            }

            if (gapIndex >= 0)
            {
                Token midToken = partitioner.getTokenFactory().fromLongValue(gapMidValue);
                PrimaryKey midKey = pkFactory.createTokenOnly(midToken);
                long res = map.exactRowIdOrInvertedCeiling(midKey);
                long expected = -(gapIndex + 1) - 1; // insertion point = gapIndex + 1
                assertEquals(expected, res);

                // Now exact for the next real key (monotonic progression preserved)
                PrimaryKey nextPk = map.primaryKeyFromRowId(gapIndex + 1);
                long exactNext = map.exactRowIdOrInvertedCeiling(pkFactory.createTokenOnly(nextPk.token()));
                assertEquals(gapIndex + 1, exactNext);
            }

            // Above last token should return negative with insertion point = count
            long lastTokenValue = map.primaryKeyFromRowId(count - 1).token().getLongValue();
            // choose a value strictly greater than last
            long aboveLastValue = lastTokenValue == Long.MAX_VALUE ? lastTokenValue : lastTokenValue + 1;
            Token aboveLastToken = partitioner.getTokenFactory().fromLongValue(aboveLastValue);
            long aboveLast = map.exactRowIdOrInvertedCeiling(pkFactory.createTokenOnly(aboveLastToken));
            long expectedAboveLast = -(count) - 1;
            assertEquals(expectedAboveLast, aboveLast);
        }
    }

    @Test
    public void testCeiling() throws Throwable
    {
        IndexComponents.ForRead perSSTableComponents = indexDescriptor.perSSTableComponents();
        PrimaryKeyMap.Factory factory = perSSTableComponents.onDiskFormat().newPrimaryKeyMapFactory(perSSTableComponents, pkFactory, sstable);
        try (PrimaryKeyMap map = factory.newPerSSTablePrimaryKeyMap())
        {
            long count = map.count();
            assertTrue("Expected some rows in test sstable", count > 2);

            IPartitioner partitioner = sstable.metadata().partitioner;

            // Exact match for first rowId
            PrimaryKey firstPk = map.primaryKeyFromRowId(0);
            long firstRowId = map.ceiling(pkFactory.createTokenOnly(firstPk.token()));
            assertEquals(0L, firstRowId);

            // Find a neighboring pair with a gap in token values to test non-exact ceiling
            long gapIndex = -1;
            long gapMidValue = Long.MIN_VALUE;
            for (int i = 0; i < Math.min(count - 1, 20); i++)
            {
                long t0 = map.primaryKeyFromRowId(i).token().getLongValue();
                long t1 = map.primaryKeyFromRowId(i + 1).token().getLongValue();
                if (t1 - t0 >= 2)
                {
                    gapIndex = i;
                    gapMidValue = t0 + 1; // value strictly between t0 and t1
                    break;
                }
            }

            if (gapIndex >= 0)
            {
                // Ceiling for a value between two tokens should be the next real key's rowId
                Token midToken = partitioner.getTokenFactory().fromLongValue(gapMidValue);
                long res = map.ceiling(pkFactory.createTokenOnly(midToken));
                assertEquals(gapIndex + 1, res);

                // Now exact for the next real key (monotonic progression preserved)
                PrimaryKey nextPk = map.primaryKeyFromRowId(gapIndex + 1);
                long exactNext = map.ceiling(pkFactory.createTokenOnly(nextPk.token()));
                assertEquals(gapIndex + 1, exactNext);
            }

            // Above last token should return -1 for ceiling
            long lastTokenValue = map.primaryKeyFromRowId(count - 1).token().getLongValue();
            long aboveLastValue = lastTokenValue == Long.MAX_VALUE ? lastTokenValue : lastTokenValue + 1;
            Token aboveLastToken = partitioner.getTokenFactory().fromLongValue(aboveLastValue);
            long aboveLast = map.ceiling(pkFactory.createTokenOnly(aboveLastToken));
            assertEquals(-1L, aboveLast);
        }
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testFloorUnsupported() throws Throwable
    {
        IndexComponents.ForRead perSSTableComponents = indexDescriptor.perSSTableComponents();
        PrimaryKeyMap.Factory factory = perSSTableComponents.onDiskFormat().newPrimaryKeyMapFactory(perSSTableComponents, pkFactory, sstable);
        try (PrimaryKeyMap map = factory.newPerSSTablePrimaryKeyMap())
        {
            // Any key is fine; floor is not supported in V1 implementation and should throw
            PrimaryKey firstPk = map.primaryKeyFromRowId(0);
            map.floor(pkFactory.createTokenOnly(firstPk.token()));
        }
    }
}
