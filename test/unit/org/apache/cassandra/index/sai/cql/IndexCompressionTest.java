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

package org.apache.cassandra.index.sai.cql;

import java.util.Set;

import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexComponentType;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class IndexCompressionTest extends SAITester
{
    @Test
    public void testKeyCompression()
    {
        createTable("CREATE TABLE %s (pk int, c text, val text, PRIMARY KEY(pk, c))");
        String indexName = createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex' WITH options = { 'key_compression': '{\"class\": \"LZ4Compressor\"}' };");
        for (int i = 0; i < 1000; i++)
            execute("INSERT INTO %s(pk, c, val) VALUES (?, ?, ?)", i, "key", "value" + i);

        flush();

        assertRowCount(execute("SELECT * FROM %s WHERE val = 'value0'"), 1);
        assertRowCount(execute("SELECT * FROM %s WHERE val = 'value5'"), 1);
        assertRowCount(execute("SELECT * FROM %s WHERE val = 'value999'"), 1);

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        StorageAttachedIndex sai = (StorageAttachedIndex) cfs.indexManager.getIndexByName(indexName);
        IndexContext context = sai.getIndexContext();

        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            IndexDescriptor descriptor = IndexDescriptor.load(sstable, Set.of(context));
            assertCompressed(descriptor.perSSTableComponents().get(IndexComponentType.PRIMARY_KEY_TRIE));
            assertCompressed(descriptor.perSSTableComponents().get(IndexComponentType.PRIMARY_KEY_BLOCKS));
        }
    }

    @Test
    public void testKeyCompressionMustMatchOnAllIndexes()
    {
        // Check if we reject creating an index with a different key compression than the one already created.
        // This is beacuse both indexes share the same primary key map, and it can be compressed in one way only.
        createTable("CREATE TABLE %s (pk int, c text, val1 text, val2 text, PRIMARY KEY(pk, c))");
        createIndex("CREATE CUSTOM INDEX ON %s(val1) USING 'StorageAttachedIndex' WITH options = { 'key_compression': '{\"class\": \"LZ4Compressor\"}' }");

        InvalidRequestException ex = assertThrows(InvalidRequestException.class,
                     () -> createIndex("CREATE CUSTOM INDEX ON %s(val2) USING 'StorageAttachedIndex' WITH options = { 'key_compression': '{\"class\": \"ZstdCompressor\"}' }"));
        assertTrue(ex.getMessage().contains("Cannot create storage-attached index"));
        assertTrue(ex.getMessage().contains("val2"));
        assertTrue(ex.getMessage().contains("val1"));
        assertTrue(ex.getMessage().contains("key_compression"));
        assertTrue(ex.getMessage().contains("LZ4Compressor"));
    }

    @Test
    public void testLiteralValueCompression()
    {
        createTable("CREATE TABLE %s (pk int, c text, val text, PRIMARY KEY(pk, c))");
        String indexName = createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex' WITH options = { 'value_compression': '{\"class\": \"LZ4Compressor\"}' };");
        for (int i = 0; i < 1000; i++)
            execute("INSERT INTO %s(pk, c, val) VALUES (?, ?, ?)", i, "key", "value" + i);

        flush();

        assertRowCount(execute("SELECT * FROM %s WHERE val = 'value0'"), 1);
        assertRowCount(execute("SELECT * FROM %s WHERE val = 'value5'"), 1);
        assertRowCount(execute("SELECT * FROM %s WHERE val = 'value999'"), 1);


        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        StorageAttachedIndex sai = (StorageAttachedIndex) cfs.indexManager.getIndexByName(indexName);
        IndexContext context = sai.getIndexContext();

        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            IndexDescriptor descriptor = IndexDescriptor.load(sstable, Set.of(context));
            assertCompressed(descriptor.perIndexComponents(context).get(IndexComponentType.TERMS_DATA));
            assertCompressed(descriptor.perIndexComponents(context).get(IndexComponentType.POSTING_LISTS));
        }
    }

    @Test
    public void testNumericValueCompression()
    {
        createTable("CREATE TABLE %s (pk int, c text, val int, PRIMARY KEY(pk, c))");
        String indexName = createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex' WITH options = { 'value_compression': '{\"class\": \"LZ4Compressor\"}' };");
        for (int i = 0; i < 1000; i++)
            execute("INSERT INTO %s(pk, c, val) VALUES (?, ?, ?)", i, "key", i);

        flush();

        assertRowCount(execute("SELECT * FROM %s WHERE val = 0"), 1);
        assertRowCount(execute("SELECT * FROM %s WHERE val = 5"), 1);
        assertRowCount(execute("SELECT * FROM %s WHERE val = 999"), 1);
        assertRowCount(execute("SELECT * FROM %s WHERE val >= 0"), 1000);
        assertRowCount(execute("SELECT * FROM %s WHERE val < 100"), 100);

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        StorageAttachedIndex sai = (StorageAttachedIndex) cfs.indexManager.getIndexByName(indexName);
        IndexContext context = sai.getIndexContext();

        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            IndexDescriptor descriptor = IndexDescriptor.load(sstable, Set.of(context));
            assertCompressed(descriptor.perIndexComponents(context).get(IndexComponentType.KD_TREE));
            assertCompressed(descriptor.perIndexComponents(context).get(IndexComponentType.KD_TREE_POSTING_LISTS));
        }
    }


    private void assertCompressed(IndexComponent component)
    {
        File compressionMetaFile = component.compressionMetaFile();
        assertTrue(compressionMetaFile.exists());
        try (CompressionMetadata metadata = new CompressionMetadata(compressionMetaFile, component.file().length(), true))
        {
            assertEquals(component.file().length(), metadata.compressedFileLength);
            assertTrue(metadata.compressedFileLength < metadata.dataLength);
        }
    }
}
