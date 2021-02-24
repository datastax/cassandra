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

package org.apache.cassandra.index.sai.disk;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.tries.MemtableTrie;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.disk.io.IndexComponents;
import org.apache.cassandra.index.sai.disk.v1.PrimaryKeyMap;
import org.apache.cassandra.index.sai.utils.NamedMemoryLimiter;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.inject.Injections;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.schema.TableMetadata;

import static org.apache.cassandra.inject.InvokePointBuilder.newInvokePoint;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SSTableComponentsTest extends SAITester
{
    protected static final Injections.Counter FLUSH_COUNTER = Injections.newCounter("FlushCounter")
                                                                        .add(newInvokePoint().onClass(SSTableComponentsWriter.class).onMethod("flush"))
                                                                        .build();

    private Descriptor descriptor;

    @Before
    public void createDescriptor() throws Throwable
    {
        Path tmpDir = Files.createTempDirectory("SegmentFlushTest");
        descriptor = new Descriptor(tmpDir.toFile(), "test", "test", 1);
    }

    @Test
    public void testEmptyKeys() throws Throwable
    {
        setMemtableTrieAllocatedSizeThreshold(1);

        SSTableComponentsWriter writer = new SSTableComponentsWriter.OnDiskSSTableComponentsWriter(descriptor, null);
        writer.complete();

        TableMetadata tableMetadata = TableMetadata.builder("test", "test")
                                                   .partitioner(Murmur3Partitioner.instance)
                                                   .addPartitionKeyColumn("pk", Int32Type.instance)
                                                   .addClusteringColumn("a", UTF8Type.instance)
                                                   .addClusteringColumn("b", UTF8Type.instance)
                                                   .build();

        IndexComponents indexComponents = IndexComponents.perSSTable(descriptor, null);

        PrimaryKeyMap primaryKeyMap = new PrimaryKeyMap.DefaultPrimaryKeyMap(indexComponents, tableMetadata);

        assertEquals(0, primaryKeyMap.size());

        primaryKeyMap.close();
    }

    @Test
    public void testSegmented() throws Throwable
    {
        testWithMemtableSizeThreshold(1);
        assertTrue(FLUSH_COUNTER.get() >= 1);
    }

    @Test
    public void testUnsegmented() throws Throwable
    {
        testWithMemtableSizeThreshold(2047);
        assertEquals(1, FLUSH_COUNTER.get());
    }

    private void testWithMemtableSizeThreshold(int sizeThresholdMB) throws Throwable
    {
        setMemtableTrieAllocatedSizeThreshold(sizeThresholdMB);
        Injections.inject(FLUSH_COUNTER);
        FLUSH_COUNTER.reset();
        SSTableComponentsWriter writer = new SSTableComponentsWriter.OnDiskSSTableComponentsWriter(descriptor, null);

        TableMetadata tableMetadata = TableMetadata.builder("test", "test")
                                                   .partitioner(Murmur3Partitioner.instance)
                                                   .addPartitionKeyColumn("pk", Int32Type.instance)
                                                   .addClusteringColumn("a", UTF8Type.instance)
                                                   .addClusteringColumn("b", UTF8Type.instance)
                                                   .build();

        PrimaryKey.PrimaryKeyFactory factory = PrimaryKey.factory(tableMetadata);

        int numRows = CQLTester.getRandom().nextIntBetween(2000, 10000);
        int width = CQLTester.getRandom().nextIntBetween(3, 8);
        numRows = (numRows / width) * width;

        List<PrimaryKey> expected = new ArrayList<>(numRows);

        for (int partitionKey = 0; partitionKey < numRows / width; partitionKey++)
        {
            for (int clustering = 0; clustering < width; clustering++)
                expected.add(factory.createKey(makeKey(tableMetadata, Integer.toString(partitionKey)),
                                               makeClustering(tableMetadata, CQLTester.getRandom().nextAsciiString(2, 200), CQLTester.getRandom().nextAsciiString(2, 200))));
        }

        expected.sort(PrimaryKey::compareTo);

        int sstableRowId = 0;
        for (PrimaryKey key : expected)
            writer.nextRow(factory.createKey(key.partitionKey(), key.clustering(), sstableRowId++));

        writer.complete();

        IndexComponents indexComponents = IndexComponents.perSSTable(descriptor, null);

        PrimaryKeyMap primaryKeyMap = new PrimaryKeyMap.DefaultPrimaryKeyMap(indexComponents, tableMetadata);

        assertEquals(numRows, primaryKeyMap.size());

        for (int rowId = 0; rowId < numRows; rowId++)
        {
            assertTrue(primaryKeyMap.primaryKeyFromRowId(rowId).compareTo(expected.get(rowId)) == 0);
        }

        for (int rowId = numRows - 1; rowId >= 0; rowId--)
        {
            assertTrue(primaryKeyMap.primaryKeyFromRowId(rowId).compareTo(expected.get(rowId)) == 0);
        }

        for (int rowId = 0; rowId < numRows; rowId++)
        {
            int randomRowId = CQLTester.getRandom().nextIntBetween(0, numRows - 1);
            assertTrue(primaryKeyMap.primaryKeyFromRowId(randomRowId).compareTo(expected.get(randomRowId)) == 0);
        }

        primaryKeyMap.close();
    }

    private DecoratedKey makeKey(TableMetadata table, String...partitionKeys)
    {
        ByteBuffer key;
        if (TypeUtil.isComposite(table.partitionKeyType))
            key = ((CompositeType)table.partitionKeyType).decompose(partitionKeys);
        else
            key = table.partitionKeyType.fromString(partitionKeys[0]);
        return table.partitioner.decorateKey(key);
    }

    private Clustering makeClustering(TableMetadata table, String...clusteringKeys)
    {
        Clustering clustering;
        if (table.comparator.size() == 0)
            clustering = Clustering.EMPTY;
        else
        {
            ByteBuffer[] values = new ByteBuffer[clusteringKeys.length];
            for (int index = 0; index < table.comparator.size(); index++)
                values[index] = table.comparator.subtype(index).fromString(clusteringKeys[index]);
            clustering = Clustering.make(values);
        }
        return clustering;
    }

    protected static void setMemtableTrieAllocatedSizeThreshold(final int sizeThresholdMB) throws Exception
    {
        Field threshold = MemtableTrie.class.getDeclaredField("ALLOCATED_SIZE_THRESHOLD");
        threshold.setAccessible(true);
        Field modifiersField = Field.class.getDeclaredField("modifiers");
        modifiersField.setAccessible(true);
        modifiersField.setInt(threshold, threshold.getModifiers() & ~Modifier.FINAL);
        threshold.set(MemtableTrie.class, 1024 * 1024 * sizeThresholdMB);
    }
}