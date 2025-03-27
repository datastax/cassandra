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

import java.io.IOException;
import java.util.Set;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.SAIUtil;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexComponentType;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class IndexCompressionTest extends SAITester
{
    @Before
    public void setup()
    {
        if (!Version.current().onOrAfter(Version.EC))
            SAIUtil.setCurrentVersion(Version.EC);
    }

    @Test
    public void testCannotUseCompressionInOlderVersions()
    {
        SAIUtil.setCurrentVersion(Version.EB);
        createTable("CREATE TABLE %s (pk int, c text, val text, PRIMARY KEY(pk, c))");
        var exception = assertThrows(InvalidRequestException.class,
                                     () -> createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex' WITH options = { 'key_compression': '{\"class\": \"LZ4Compressor\"}' };"));
        assertTrue(exception.getMessage().contains("Cannot create compressed storage-attached index"));
        assertTrue(exception.getMessage().contains("ec"));
        assertTrue(exception.getMessage().contains("eb"));
    }

    @Test
    public void testKeyCompression() throws IOException
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
            IndexComponent.ForRead primaryKeyTrieCompressionInfo = descriptor.perSSTableComponents().get(IndexComponentType.PRIMARY_KEY_TRIE_COMPRESSION_INFO);
            IndexComponent.ForRead primaryKeyBlocksCompressionInfo = descriptor.perSSTableComponents().get(IndexComponentType.PRIMARY_KEY_BLOCKS_COMPRESSION_INFO);
            assertTrue(primaryKeyTrieCompressionInfo.file().exists());
            assertTrue(primaryKeyBlocksCompressionInfo.file().exists());
            assertCompressed(descriptor.perSSTableComponents().get(IndexComponentType.PRIMARY_KEY_TRIE));
            assertCompressed(descriptor.perSSTableComponents().get(IndexComponentType.PRIMARY_KEY_BLOCKS));

            var toc = SSTableReader.readTOC(descriptor.descriptor);
            assertTrue(toc.contains(primaryKeyTrieCompressionInfo.asCustomComponent()));
            assertTrue(toc.contains(primaryKeyBlocksCompressionInfo.asCustomComponent()));
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
    public void testLiteralValueCompression() throws IOException
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
            IndexComponent.ForRead termsDataCompressionInfo = descriptor.perIndexComponents(context).get(IndexComponentType.TERMS_DATA_COMPRESSION_INFO);
            IndexComponent.ForRead postingListsCompressionInfo = descriptor.perIndexComponents(context).get(IndexComponentType.POSTING_LISTS_COMPRESSION_INFO);
            assertTrue(termsDataCompressionInfo.file().exists());
            assertTrue(postingListsCompressionInfo.file().exists());
            assertCompressed(descriptor.perIndexComponents(context).get(IndexComponentType.TERMS_DATA));
            assertCompressed(descriptor.perIndexComponents(context).get(IndexComponentType.POSTING_LISTS));

            var toc = SSTableReader.readTOC(descriptor.descriptor);
            assertTrue(toc.contains(termsDataCompressionInfo.asCustomComponent()));
            assertTrue(toc.contains(postingListsCompressionInfo.asCustomComponent()));
        }
    }

    @Test
    public void testNumericValueCompression() throws IOException
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
        checkKdTreeComponentsCompressed(cfs, context);
        compact();

        assertRowCount(execute("SELECT * FROM %s WHERE val = 0"), 1);
        assertRowCount(execute("SELECT * FROM %s WHERE val = 5"), 1);
        assertRowCount(execute("SELECT * FROM %s WHERE val = 999"), 1);
        assertRowCount(execute("SELECT * FROM %s WHERE val >= 0"), 1000);
        assertRowCount(execute("SELECT * FROM %s WHERE val < 100"), 100);
        checkKdTreeComponentsCompressed(cfs, context);
    }

    private void checkKdTreeComponentsCompressed(ColumnFamilyStore cfs, IndexContext context) throws IOException {
        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            IndexDescriptor descriptor = IndexDescriptor.load(sstable, Set.of(context));
            IndexComponent.ForRead kdTreeCompressionInfo = descriptor.perIndexComponents(context).get(IndexComponentType.KD_TREE_COMPRESSION_INFO);
            assertTrue(kdTreeCompressionInfo.file().exists());
            IndexComponent.ForRead kdTreePostingListsCompressionInfo = descriptor.perIndexComponents(context).get(IndexComponentType.KD_TREE_POSTING_LISTS_COMPRESSION_INFO);
            assertTrue(kdTreePostingListsCompressionInfo.file().exists());
            assertCompressed(descriptor.perIndexComponents(context).get(IndexComponentType.KD_TREE));
            assertCompressed(descriptor.perIndexComponents(context).get(IndexComponentType.KD_TREE_POSTING_LISTS));

            var toc = SSTableReader.readTOC(descriptor.descriptor);
            assertTrue(toc.contains(kdTreeCompressionInfo.asCustomComponent()));
            assertTrue(toc.contains(kdTreePostingListsCompressionInfo.asCustomComponent()));
        }
    }


    private void assertCompressed(IndexComponent component)
    {
        assertNotNull(component.compressionMetadataComponent());
        File compressionMetaFile = component.compressionMetadataComponent().file();
        assertTrue(compressionMetaFile.exists());
        try (CompressionMetadata metadata = new CompressionMetadata(compressionMetaFile, component.file().length(), true))
        {
            assertEquals(component.file().length(), metadata.compressedFileLength);
            assertTrue(metadata.compressedFileLength < metadata.dataLength);
        }
    }


}
