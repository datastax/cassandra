/*
 * Copyright IBM Corp.
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

package org.apache.cassandra.io.sstable.format.bti;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.compress.CompressionMetadataReaderType;
import org.apache.cassandra.io.compress.CorruptBlockException;
import org.apache.cassandra.io.compress.EncryptionConfig;
import org.apache.cassandra.io.compress.Encryptor;
import org.apache.cassandra.io.compress.EncryptorTest;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.CompressionInfoComponent;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.StatsComponent;
import org.apache.cassandra.io.util.EncryptedChunkReaderCacheTest;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.schema.DefaultCompressionSelector;
import org.apache.cassandra.utils.Pair;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The data file of an sstable is written with the compression parameters chosen by the
 * {@link CompressionParams.Selector} ({@link CompressionParams#forFlush}/{@link CompressionParams#forCompaction}),
 * which are the ones stored in its compression info file and which readers take the encryptor of the indexes and of
 * the metadata from. A selector returning encryption parameters that differ from the table's (here: another key
 * provider) must thus make the indexes and the metadata be encrypted with the selected parameters too, otherwise the
 * sstable cannot be read.
 */
public class IndexEncryptionCompressionSelectorTest extends CQLTester
{
    private static final int NUM_PARTITIONS = 50;

    private static final String TABLE_OPTIONS = " WITH compression = {'class' : 'Encryptor', " +
                                                "'cipher_algorithm' : 'AES/CBC/PKCS5Padding', " +
                                                "'secret_key_strength' : 128, " +
                                                "'key_provider' : '" + EncryptorTest.KeyProviderFactoryStub.class.getName() + "'}";

    private CompressionParams.Selector previousSelector;

    /**
     * Selects, for flushes and compactions of tables encrypted with the key of
     * {@link EncryptorTest.KeyProviderFactoryStub}, the same encryption with the (different) key of
     * {@link EncryptedChunkReaderCacheTest.OtherKeyProviderFactoryStub}.
     */
    public static class OtherKeySelector implements CompressionParams.Selector
    {
        private final DefaultCompressionSelector defaultSelector = new DefaultCompressionSelector();

        @Override
        public CompressionParams newTableCompression(String keyspace)
        {
            return defaultSelector.newTableCompression(keyspace);
        }

        @Override
        public CompressionParams flushCompression(String keyspace, CompressionParams tableParams)
        {
            return otherKey(tableParams);
        }

        @Override
        public CompressionParams compactionCompression(String keyspace, CompressionParams tableParams)
        {
            return otherKey(tableParams);
        }

        private static CompressionParams otherKey(CompressionParams tableParams)
        {
            if (!EncryptorTest.KeyProviderFactoryStub.class.getName().equals(tableParams.getOtherOptions().get(EncryptionConfig.KEY_PROVIDER)))
                return tableParams;
            Map<String, String> options = new HashMap<>(tableParams.asMap());
            options.put(EncryptionConfig.KEY_PROVIDER, EncryptedChunkReaderCacheTest.OtherKeyProviderFactoryStub.class.getName());
            return CompressionParams.fromMap(options);
        }
    }

    private static Field selectorField() throws NoSuchFieldException
    {
        Field field = CompressionParams.class.getDeclaredField("SELECTOR");
        field.setAccessible(true);
        return field;
    }

    @Before
    public void setSelector() throws Exception
    {
        Assume.assumeTrue(BtiFormat.isSelected());
        previousSelector = (CompressionParams.Selector) selectorField().get(null);
        selectorField().set(null, new OtherKeySelector());
    }

    @After
    public void resetSelector() throws Exception
    {
        if (previousSelector != null)
            selectorField().set(null, previousSelector);
    }

    private static FileHandle.Builder partitionIndexBuilder(Descriptor descriptor, CompressionMetadata encryptionMetadata)
    {
        return new FileHandle.Builder(descriptor.fileFor(BtiFormat.Components.PARTITION_INDEX)).withCompressionMetadata(encryptionMetadata)
                                                                                              .encryptionOnly();
    }

    @Test
    public void testIndexesAreEncryptedWithTheSelectedParameters() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, v text, PRIMARY KEY (pk, ck))" + TABLE_OPTIONS);
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        disableCompaction();
        for (int pk = 0; pk < NUM_PARTITIONS; pk++)
            for (int ck = 0; ck < 3; ck++)
                execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, ck, "value-" + pk + '-' + ck);
        flush();

        assertThat(cfs.getLiveSSTables()).hasSize(1);
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        Descriptor descriptor = sstable.descriptor;
        assertThat(descriptor.version.indicesAreEncrypted()).isTrue();

        // the data file is written with the selected parameters, which differ from the table's
        CompressionParams tableParams = cfs.metadata().params.compression;
        CompressionParams dataParams = sstable.getCompressionMetadata().parameters;
        assertThat(dataParams.getSstableCompressor()).isInstanceOf(Encryptor.class);
        assertThat(dataParams.getOtherOptions().get(EncryptionConfig.KEY_PROVIDER)).isEqualTo(EncryptedChunkReaderCacheTest.OtherKeyProviderFactoryStub.class.getName());
        assertThat(dataParams).isNotEqualTo(tableParams);

        // the partition index can be decrypted with the selected parameters...
        try (CompressionMetadata dataEncryption = CompressionMetadata.encryptedOnly(dataParams);
             PartitionIndex index = PartitionIndex.load(partitionIndexBuilder(descriptor, dataEncryption), cfs.getPartitioner(), false, descriptor.version.getByteComparableVersion()))
        {
            assertThat(index.firstKey()).isEqualTo(sstable.getFirst());
            assertThat(index.lastKey()).isEqualTo(sstable.getLast());
        }
        // ...but not with the table's: the same load then fails decrypting the index (a corruption, not a
        // misconfiguration of the handle)
        try (CompressionMetadata tableEncryption = CompressionMetadata.encryptedOnly(tableParams))
        {
            FileHandle.Builder builder = partitionIndexBuilder(descriptor, tableEncryption);
            assertThatThrownBy(() -> PartitionIndex.load(builder, cfs.getPartitioner(), false, descriptor.version.getByteComparableVersion()).close())
                .isInstanceOf(CorruptSSTableException.class)
                .hasCauseInstanceOf(CorruptBlockException.class);
        }

        // the sstable is readable, through the running reader...
        assertRowCount(execute("SELECT * FROM %s"), NUM_PARTITIONS * 3);
        for (int pk = 0; pk < NUM_PARTITIONS; pk++)
            assertRowCount(execute("SELECT * FROM %s WHERE pk = ?", pk), 3);

        // ...by the offline readers of the key range and of the metadata...
        Pair<DecoratedKey, DecoratedKey> keyRange = descriptor.getFormat().getReaderFactory().readKeyRange(descriptor, cfs.getPartitioner());
        assertThat(keyRange.left).isEqualTo(sstable.getFirst());
        assertThat(keyRange.right).isEqualTo(sstable.getLast());
        assertThat(StatsComponent.load(descriptor).statsMetadata().totalRows).isEqualTo(NUM_PARTITIONS * 3);
        assertThat(CompressionInfoComponent.readCompressionParamsIfExists(descriptor, CompressionMetadataReaderType.READ_TIME))
            .isEqualTo(dataParams);

        // ...and by a reader opened from scratch
        SSTableReader reopened = descriptor.getFormat().getReaderFactory().loadingBuilder(descriptor, cfs.metadata, descriptor.discoverComponents()).build(cfs, true, false);
        try (ISSTableScanner scanner = reopened.getScanner())
        {
            int partitions = 0;
            while (scanner.hasNext())
            {
                scanner.next().close();
                partitions++;
            }
            assertThat(partitions).isEqualTo(NUM_PARTITIONS);
        }
        finally
        {
            reopened.selfRef().release();
        }
    }
}
