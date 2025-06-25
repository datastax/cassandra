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

package org.apache.cassandra.index.sai.functional;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexComponents;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.StorageAttachedIndexGroup;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.lucene.index.CorruptIndexException;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ValidateComponentsTest extends SAITester
{
    private static final String CREATE_TABLE_TEMPLATE = "CREATE TABLE %s (id1 TEXT PRIMARY KEY, v1 INT, v2 TEXT) WITH compaction = " +
                                                        "{'class' : 'SizeTieredCompactionStrategy', 'enabled' : false }";

    @Before
    public void setup() throws Exception
    {
        requireNetwork();
    }

    @Test
    public void testValidateComponentsWithValidIndex() throws Throwable
    {
        createTable(CREATE_TABLE_TEMPLATE);
        createIndex("CREATE INDEX ON %s(v1) USING 'sai'");
        createIndex("CREATE INDEX ON %s(v2) USING 'sai'");

        execute("INSERT INTO %s (id1, v1, v2) VALUES ('0', 0, '0')");
        execute("INSERT INTO %s (id1, v1, v2) VALUES ('1', 1, '1')");
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        
        StorageAttachedIndexGroup group = getIndexGroup(cfs);
        
        // Should not throw since all components are valid
        group.validateComponents(sstable, true);
    }

    @Test
    public void testValidateComponentsWithCorruptedIndex() throws Throwable
    {
        createTable(CREATE_TABLE_TEMPLATE);
        createIndex("CREATE INDEX ON %s(v1) USING 'sai'");

        execute("INSERT INTO %s (id1, v1) VALUES ('0', 0)");
        execute("INSERT INTO %s (id1, v1) VALUES ('1', 1)");
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        
        // Corrupt an index component
        StorageAttachedIndexGroup group = getIndexGroup(cfs);
        IndexDescriptor descriptor = group.descriptorFor(sstable);
        IndexComponents.ForRead components = descriptor.perSSTableComponents();
        
        // Find a component to corrupt
        IndexComponent.ForRead componentToCorrupt = components.all().stream()
                                                            .filter(c -> c.file().exists())
                                                            .findFirst()
                                                            .orElseThrow();
        
        // Corrupt the file by writing garbage at the beginning
        try (FileChannel channel = FileChannel.open(componentToCorrupt.file().toPath(), StandardOpenOption.WRITE))
        {
            channel.write(ByteBuffer.wrap("CORRUPTED".getBytes()));
        }
        
        // Now validation should fail
        assertThatThrownBy(() -> group.validateComponents(sstable, true))
            .isInstanceOf(CorruptIndexException.class)
            .hasMessageContaining("Failed validation of per-SSTable components");
    }

    @Test
    public void testValidateComponentsWithMissingComponent() throws Throwable
    {
        createTable(CREATE_TABLE_TEMPLATE);
        createIndex("CREATE INDEX ON %s(v1) USING 'sai'");

        execute("INSERT INTO %s (id1, v1) VALUES ('0', 0)");
        execute("INSERT INTO %s (id1, v1) VALUES ('1', 1)");
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        
        StorageAttachedIndexGroup group = getIndexGroup(cfs);
        IndexDescriptor descriptor = group.descriptorFor(sstable);
        IndexComponents.ForRead components = descriptor.perSSTableComponents();
        
        // Delete a component file
        IndexComponent.ForRead componentToDelete = components.all().stream()
                                                            .filter(c -> c.file().exists())
                                                            .findFirst()
                                                            .orElseThrow();
        
        componentToDelete.file().delete();
        
        // Validation should fail due to missing component
        assertThatThrownBy(() -> group.validateComponents(sstable, true))
            .isInstanceOf(CorruptIndexException.class)
            .hasMessageContaining("Failed validation of per-SSTable components");
    }

    @Test
    public void testValidateComponentsQuickMode() throws Throwable
    {
        createTable(CREATE_TABLE_TEMPLATE);
        createIndex("CREATE INDEX ON %s(v1) USING 'sai'");

        execute("INSERT INTO %s (id1, v1) VALUES ('0', 0)");
        execute("INSERT INTO %s (id1, v1) VALUES ('1', 1)");
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        
        StorageAttachedIndexGroup group = getIndexGroup(cfs);
        
        // Quick mode should skip checksum validation but still check headers/footers
        group.validateComponents(sstable, false);
    }

    private StorageAttachedIndexGroup getIndexGroup(ColumnFamilyStore cfs)
    {
        return (StorageAttachedIndexGroup) cfs.indexManager.listIndexGroups().iterator().next();
    }
}