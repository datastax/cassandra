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

import java.nio.file.Files;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.compress.EncryptorTest;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.utils.Pair;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Checks that {@link SSTableFormat.SSTableReaderFactory#readKeyRange} - the entry point used by offline tools such as
 * {@code sstablemetadata} to read the first and last key of an sstable without opening it - can read the partition
 * index of BTI sstables whose indexes are encrypted, as well as of compressed and uncompressed (not encrypted) ones;
 * and that the index handles of a loaded reader do not hold the data file's compression metadata.
 */
public class BtiFormatReadKeyRangeTest extends CQLTester
{
    private static final int NUM_PARTITIONS = 100;

    private enum TableCompression
    {
        ENCRYPTED(" WITH compression = {'class' : 'Encryptor', " +
                  "'cipher_algorithm' : 'AES/ECB/PKCS5Padding', " +
                  "'secret_key_strength' : 128, " +
                  "'key_provider' : '" + EncryptorTest.KeyProviderFactoryStub.class.getName() + "'}"),
        LZ4(" WITH compression = {'class' : 'LZ4Compressor'}"),
        UNCOMPRESSED(" WITH compression = {'enabled' : false}");

        final String tableOptions;

        TableCompression(String tableOptions)
        {
            this.tableOptions = tableOptions;
        }
    }

    @Before
    public void assumeBti()
    {
        Assume.assumeTrue(BtiFormat.isSelected());
    }

    @Test
    public void testEncryptedTable() throws Throwable
    {
        testReadKeyRange(TableCompression.ENCRYPTED);
    }

    @Test
    public void testCompressedTable() throws Throwable
    {
        testReadKeyRange(TableCompression.LZ4);
    }

    @Test
    public void testUncompressedTable() throws Throwable
    {
        testReadKeyRange(TableCompression.UNCOMPRESSED);
    }

    /**
     * An encrypted sstable whose TOC lists a compression info file that is missing must not have its encrypted
     * indexes read as plaintext.
     */
    @Test
    public void testEncryptedTableWithMissingCompressionInfo() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, v text, PRIMARY KEY (pk, ck))" + TableCompression.ENCRYPTED.tableOptions);
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        disableCompaction();
        insertPartitions(0, NUM_PARTITIONS);
        flush();

        assertThat(cfs.getLiveSSTables()).hasSize(1);
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        File copyDirectory = new File(Files.createTempDirectory("BtiFormatReadKeyRangeTest"));
        try
        {
            // copy the sstable, all its components but the compression info file
            Descriptor copy = new Descriptor(sstable.descriptor.version, copyDirectory, sstable.descriptor.ksname, sstable.descriptor.cfname, sstable.descriptor.id);
            for (Component component : sstable.descriptor.discoverComponents())
            {
                if (!component.equals(SSTableFormat.Components.COMPRESSION_INFO))
                    Files.copy(sstable.descriptor.fileFor(component).toPath(), copy.fileFor(component).toPath());
            }
            assertThat(copy.fileFor(SSTableFormat.Components.TOC).exists()).isTrue();
            assertThat(copy.fileFor(SSTableFormat.Components.COMPRESSION_INFO).exists()).isFalse();

            assertThatThrownBy(() -> BtiTableReaderLoadingBuilder.maybeLoadIndexEncryptionMetadata(copy))
                .isInstanceOf(CorruptSSTableException.class);
            assertThatThrownBy(() -> copy.getFormat().getReaderFactory().readKeyRange(copy, cfs.getPartitioner()))
                .isInstanceOf(CorruptSSTableException.class);
        }
        finally
        {
            copyDirectory.deleteRecursive();
        }
    }

    private void testReadKeyRange(TableCompression compression) throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, v text, PRIMARY KEY (pk, ck))" + compression.tableOptions);
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        IPartitioner partitioner = cfs.getPartitioner();
        disableCompaction();

        // two sstables, so that more than one key range is checked
        Set<SSTableReader> checked = new HashSet<>();
        for (int flush = 0; flush < 2; flush++)
        {
            int firstPk = flush * NUM_PARTITIONS;
            int endPk = firstPk + NUM_PARTITIONS;
            insertPartitions(firstPk, endPk);
            flush();

            // the expected key range, computed independently of the sstable
            DecoratedKey expectedFirst = null;
            DecoratedKey expectedLast = null;
            for (int pk = firstPk; pk < endPk; pk++)
            {
                DecoratedKey key = partitioner.decorateKey(Int32Type.instance.decompose(pk));
                if (expectedFirst == null || key.compareTo(expectedFirst) < 0)
                    expectedFirst = key;
                if (expectedLast == null || key.compareTo(expectedLast) > 0)
                    expectedLast = key;
            }

            Set<SSTableReader> added = new HashSet<>(cfs.getLiveSSTables());
            added.removeAll(checked);
            assertThat(added).hasSize(1);
            SSTableReader sstable = added.iterator().next();
            checked.add(sstable);
            Descriptor descriptor = sstable.descriptor;

            assertThat(descriptor.fileFor(SSTableFormat.Components.COMPRESSION_INFO).exists())
                .isEqualTo(compression != TableCompression.UNCOMPRESSED);
            try (CompressionMetadata encryptionMetadata = BtiTableReaderLoadingBuilder.maybeLoadIndexEncryptionMetadata(descriptor))
            {
                if (compression == TableCompression.ENCRYPTED)
                {
                    assertThat(descriptor.version.indicesAreEncrypted()).isTrue();
                    assertThat(encryptionMetadata).isNotNull();
                }
                else
                {
                    assertThat(encryptionMetadata).isNull();
                }
            }

            if (compression == TableCompression.ENCRYPTED)
            {
                // reading the encrypted partition index without decrypting it - what readKeyRange used to do - fails
                FileHandle.Builder bareBuilder = new FileHandle.Builder(descriptor.fileFor(BtiFormat.Components.PARTITION_INDEX));
                assertThatThrownBy(() -> PartitionIndex.load(bareBuilder, partitioner, false, descriptor.version.getByteComparableVersion()).close())
                    .isInstanceOf(IllegalArgumentException.class);
            }

            // the index handles of a reader opened from disk (the flushed one was opened by the writer) get only the
            // encryption parameters of the data file, not its chunk offsets (and none at all if the indexes are not
            // encrypted)
            BtiTableReader loaded = (BtiTableReader) descriptor.getFormat()
                                                              .getReaderFactory()
                                                              .loadingBuilder(descriptor, cfs.metadata, descriptor.discoverComponents())
                                                              .build(cfs, false, false);
            try
            {
                BtiTableReader.Builder unbuilt = loaded.unbuildTo(new BtiTableReader.Builder(descriptor), false);
                for (FileHandle indexFile : new FileHandle[]{ unbuilt.getRowIndexFile(), unbuilt.getPartitionIndex().fileHandle() })
                {
                    Optional<CompressionMetadata> indexMetadata = indexFile.compressionMetadata();
                    if (compression == TableCompression.ENCRYPTED)
                    {
                        assertThat(loaded.getCompressionMetadata().hasOffsets()).isTrue();
                        assertThat(indexMetadata).describedAs(indexFile.path()).isPresent();
                        assertThat(indexMetadata.get().isEncryptionOnly()).describedAs(indexFile.path()).isTrue();
                        assertThat(indexMetadata.get().hasOffsets()).describedAs(indexFile.path()).isFalse();
                        assertThat(indexMetadata.get().parameters).isEqualTo(loaded.getCompressionMetadata().parameters);
                    }
                    else
                    {
                        assertThat(indexMetadata).describedAs(indexFile.path()).isEmpty();
                    }
                }
                // the index handles keep working after the loader has closed the metadata it created
                assertThat(unbuilt.getPartitionIndex().firstKey()).isEqualTo(expectedFirst);
                for (int pk = firstPk; pk < endPk; pk++)
                {
                    DecoratedKey key = partitioner.decorateKey(Int32Type.instance.decompose(pk));
                    assertThat(loaded.getPosition(key, SSTableReader.Operator.EQ)).isGreaterThanOrEqualTo(0);
                }
            }
            finally
            {
                loaded.selfRef().release();
            }

            Pair<DecoratedKey, DecoratedKey> keyRange = descriptor.getFormat()
                                                                  .getReaderFactory()
                                                                  .readKeyRange(descriptor, partitioner);
            assertThat(keyRange.left).isEqualTo(expectedFirst).isEqualTo(sstable.getFirst());
            assertThat(keyRange.right).isEqualTo(expectedLast).isEqualTo(sstable.getLast());
        }
    }

    private void insertPartitions(int firstPk, int endPk) throws Throwable
    {
        for (int pk = firstPk; pk < endPk; pk++)
            for (int ck = 0; ck < 3; ck++)
                execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, ck, "value-" + pk + '-' + ck);
    }
}
