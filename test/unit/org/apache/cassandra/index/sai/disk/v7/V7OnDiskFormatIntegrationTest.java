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

package org.apache.cassandra.index.sai.disk.v7;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.Collections;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.SAIUtil;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.StorageAttachedIndexGroup;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexComponentType;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.io.sstable.format.SSTableReader;

import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
public class V7OnDiskFormatIntegrationTest extends SAITester
{
    @Parameterized.Parameter
    public Version version;

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data()
    {
        // Test with versions EC and later which support checksum validation
        return Version.ALL.stream()
                         .filter(v -> v.onOrAfter(Version.EC))
                         .map(v -> new Object[]{ v })
                         .collect(Collectors.toList());
    }

    @Before
    public void setVersion()
    {
        SAIUtil.setCurrentVersion(version);
    }

    @Before
    public void setup()
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, value vector<float, 3>)");
        createIndex("CREATE CUSTOM INDEX value_idx ON %s(value) USING 'StorageAttachedIndex'");
        CompactionManager.instance.disableAutoCompaction();
    }

    @Test
    public void testValidateComponentChecksumForVectorIndex() throws IOException
    {
        execute("INSERT INTO %s (pk, value) VALUES (1, [1.0, 2.0, 3.0])");
        execute("INSERT INTO %s (pk, value) VALUES (2, [4.0, 5.0, 6.0])");
        execute("INSERT INTO %s (pk, value) VALUES (3, [7.0, 8.0, 9.0])");
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();

        StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(cfs);
        StorageAttachedIndex index = group.getIndexes().iterator().next();
        IndexContext indexContext = getIndexContext(index);

        IndexDescriptor indexDescriptor = IndexDescriptor.load(sstable, Collections.singleton(indexContext));

        IndexComponent.ForRead component = indexDescriptor.perIndexComponents(indexContext)
                                                         .get(IndexComponentType.META);

        V7OnDiskFormat.instance.validateIndexComponent(component, true);
    }

    @Test
    public void testValidateComponentDetectsCorruption() throws IOException
    {
        execute("INSERT INTO %s (pk, value) VALUES (1, [1.0, 2.0, 3.0])");
        execute("INSERT INTO %s (pk, value) VALUES (2, [4.0, 5.0, 6.0])");
        execute("INSERT INTO %s (pk, value) VALUES (3, [7.0, 8.0, 9.0])");
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();

        StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(cfs);
        StorageAttachedIndex index = group.getIndexes().iterator().next();
        IndexContext indexContext = getIndexContext(index);

        IndexDescriptor indexDescriptor = IndexDescriptor.load(sstable, Collections.singleton(indexContext));

        // For vector indexes, use PQ component instead of POSTING_LISTS
        IndexComponentType componentType = IndexComponentType.PQ;
        IndexComponent.ForRead component = indexDescriptor.perIndexComponents(indexContext).get(componentType);
        java.io.File fileToCorrupt = component.file().toJavaIOFile();

        long originalSize = fileToCorrupt.length();

        try (RandomAccessFile file = new RandomAccessFile(fileToCorrupt, "rw"))
        {
            // Corrupt the file more severely to ensure checksum failure
            if (originalSize > 20)
            {
                file.seek(10);
                file.write(new byte[]{(byte)0xFF, (byte)0xFF, (byte)0xFF, (byte)0xFF});
                
                file.seek(originalSize / 2);
                file.write(new byte[]{(byte)0xDE, (byte)0xAD, (byte)0xBE, (byte)0xEF});
                
                file.seek(originalSize - 10);
                file.write(new byte[]{(byte)0xBA, (byte)0xAD, (byte)0xCA, (byte)0xFE});
            }
        }

        try
        {
            V7OnDiskFormat.instance.validateIndexComponent(component, true);
            fail("Expected validation to fail due to corruption");
        }
        catch (UncheckedIOException e)
        {
            // Expected - checksum validation should detect corruption
        }
    }

    @Test
    public void testSkipChecksumValidationForTermsData() throws IOException
    {
        execute("INSERT INTO %s (pk, value) VALUES (1, [1.0, 2.0, 3.0])");
        execute("INSERT INTO %s (pk, value) VALUES (2, [4.0, 5.0, 6.0])");
        execute("INSERT INTO %s (pk, value) VALUES (3, [7.0, 8.0, 9.0])");
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();

        StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(cfs);
        StorageAttachedIndex index = group.getIndexes().iterator().next();
        IndexContext indexContext = getIndexContext(index);

        IndexDescriptor indexDescriptor = IndexDescriptor.load(sstable, Collections.singleton(indexContext));

        IndexComponent.ForRead component = indexDescriptor.perIndexComponents(indexContext)
                                                         .get(IndexComponentType.TERMS_DATA);

        java.io.File fileToCorrupt = component.file().toJavaIOFile();
        long originalSize = fileToCorrupt.length();

        // Corrupt the TERMS_DATA file
        try (RandomAccessFile file = new RandomAccessFile(fileToCorrupt, "rw"))
        {
            // Corrupt the TERMS_DATA file
            if (originalSize > 20)
            {
                file.seek(originalSize / 2);
                file.write(new byte[]{(byte)0xDE, (byte)0xAD, (byte)0xBE, (byte)0xEF});
            }
        }

        // For TERMS_DATA, checksum validation is skipped due to known issue
        // So this should not throw even though file is corrupted
        V7OnDiskFormat.instance.validateIndexComponent(component, true);
    }

    @Test
    public void testValidateWithoutChecksumFlag() throws IOException
    {
        execute("INSERT INTO %s (pk, value) VALUES (1, [1.0, 2.0, 3.0])");
        execute("INSERT INTO %s (pk, value) VALUES (2, [4.0, 5.0, 6.0])");
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();

        StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(cfs);
        StorageAttachedIndex index = group.getIndexes().iterator().next();
        IndexContext indexContext = getIndexContext(index);

        IndexDescriptor indexDescriptor = IndexDescriptor.load(sstable, Collections.singleton(indexContext));

        IndexComponent.ForRead component = indexDescriptor.perIndexComponents(indexContext)
                                                         .get(IndexComponentType.META);

        // Should validate successfully without checksum
        V7OnDiskFormat.instance.validateIndexComponent(component, false);
    }

    // Helper method to get the IndexContext from a StorageAttachedIndex
    private IndexContext getIndexContext(StorageAttachedIndex index) throws RuntimeException
    {
        try
        {
            java.lang.reflect.Field field = StorageAttachedIndex.class.getDeclaredField("indexContext");
            field.setAccessible(true);
            return (IndexContext) field.get(index);
        }
        catch (Exception e)
        {
            throw new RuntimeException("Failed to get IndexContext", e);
        }
    }
}
