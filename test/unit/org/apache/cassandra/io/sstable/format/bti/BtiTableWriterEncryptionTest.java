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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.UpdateBuilder;
import org.apache.cassandra.Util;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.compress.EncryptionConfig;
import org.apache.cassandra.io.compress.Encryptor;
import org.apache.cassandra.io.compress.EncryptorTest;
import org.apache.cassandra.io.sstable.SSTableWriterTestBase;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.schema.KeyspaceParams;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Verifies that the ref-counted {@link CompressionMetadata} which {@link BtiTableWriter.IndexWriter}
 * creates for encrypted ROW_INDEX / PARTITION_INDEX components is released when the writer is
 * closed, on both the commit and the abort path. The instance holds the chunk-offset index in
 * off-heap memory, so failing to release it leaks native memory and triggers
 * "LEAK DETECTED ... ChunkOffsetMemory" reports for every flushed or compacted SSTable of a
 * TDE-enabled table.
 */
public class BtiTableWriterEncryptionTest
{
    private static final String KEYSPACE = "BtiTableWriterEncryptionTest";
    private static final String ENCRYPTED_TABLE = "encrypted_table";
    private static final String PLAIN_TABLE = "plain_table";

    @BeforeClass
    public static void defineSchema()
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE, ENCRYPTED_TABLE).compression(encryptionParams()),
                                    SchemaLoader.standardCFMD(KEYSPACE, PLAIN_TABLE));
    }

    private static CompressionParams encryptionParams()
    {
        Map<String, String> opts = new HashMap<>();
        opts.put(CompressionParams.CLASS, Encryptor.class.getName());
        opts.put(EncryptionConfig.CIPHER_ALGORITHM, "AES/CBC/PKCS5Padding");
        opts.put(EncryptionConfig.SECRET_KEY_STRENGTH, Integer.toString(128));
        opts.put(EncryptionConfig.KEY_PROVIDER, EncryptorTest.KeyProviderFactoryStub.class.getName());
        return CompressionParams.fromMap(opts);
    }

    @Test
    public void testIndexEncryptionMetadataReleasedOnCommit()
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(ENCRYPTED_TABLE);
        cfs.truncateBlocking();
        File dir = cfs.getDirectories().getDirectoryForNewSSTables();
        CompressionMetadata indexMetadata;
        try (LifecycleTransaction txn = LifecycleTransaction.offline(OperationType.WRITE, cfs.metadata))
        {
            try (SSTableWriter writer = SSTableWriterTestBase.getWriter(BtiFormat.getInstance(), cfs, dir, txn))
            {
                indexMetadata = indexEncryptionMetadata(writer);
                assertNotNull("an encrypted table must use encryption metadata for its index components", indexMetadata);
                assertFalse(indexMetadata.isCleanedUp());

                appendRows(cfs, writer);
                writer.finish(false, null);
            }
            assertTrue("the IndexWriter must release its CompressionMetadata when the writer is closed",
                       indexMetadata.isCleanedUp());
        }
        LifecycleTransaction.waitForDeletions();
    }

    @Test
    public void testIndexEncryptionMetadataReleasedOnAbort()
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(ENCRYPTED_TABLE);
        cfs.truncateBlocking();
        File dir = cfs.getDirectories().getDirectoryForNewSSTables();
        CompressionMetadata indexMetadata;
        try (LifecycleTransaction txn = LifecycleTransaction.offline(OperationType.WRITE, cfs.metadata))
        {
            try (SSTableWriter writer = SSTableWriterTestBase.getWriter(BtiFormat.getInstance(), cfs, dir, txn))
            {
                indexMetadata = indexEncryptionMetadata(writer);
                assertNotNull(indexMetadata);

                appendRows(cfs, writer);
                writer.abort();
            }
            assertTrue("the IndexWriter must release its CompressionMetadata when the writer is aborted",
                       indexMetadata.isCleanedUp());
        }
        LifecycleTransaction.waitForDeletions();
    }

    @Test
    public void testIndexEncryptionMetadataReleasedAfterReaderIsClosed()
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(ENCRYPTED_TABLE);
        cfs.truncateBlocking();
        File dir = cfs.getDirectories().getDirectoryForNewSSTables();
        CompressionMetadata indexMetadata;
        try (LifecycleTransaction txn = LifecycleTransaction.offline(OperationType.WRITE, cfs.metadata))
        {
            try (SSTableWriter writer = SSTableWriterTestBase.getWriter(BtiFormat.getInstance(), cfs, dir, txn))
            {
                indexMetadata = indexEncryptionMetadata(writer);
                assertNotNull(indexMetadata);

                appendRows(cfs, writer);

                // opening the result makes the reader's index FileHandles take shared copies of the metadata,
                // so the last reference is released only once the reader is closed as well
                SSTableReader reader = writer.finish(true, null);
                try
                {
                    assertFalse(indexMetadata.isCleanedUp());
                }
                finally
                {
                    reader.selfRef().release();
                }
            }
        }
        LifecycleTransaction.waitForDeletions();
        // the reader tidier runs asynchronously
        Util.spinAssertEquals(true, indexMetadata::isCleanedUp, 10);
    }

    @Test
    public void testNoIndexEncryptionMetadataWithoutEncryption()
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(PLAIN_TABLE);
        cfs.truncateBlocking();
        File dir = cfs.getDirectories().getDirectoryForNewSSTables();
        try (LifecycleTransaction txn = LifecycleTransaction.offline(OperationType.WRITE, cfs.metadata))
        {
            try (SSTableWriter writer = SSTableWriterTestBase.getWriter(BtiFormat.getInstance(), cfs, dir, txn))
            {
                assertNull("index components are not encrypted, so no encryption metadata is expected",
                           indexEncryptionMetadata(writer));
                writer.abort();
            }
        }
        LifecycleTransaction.waitForDeletions();
    }

    private static CompressionMetadata indexEncryptionMetadata(SSTableWriter writer)
    {
        assertTrue(writer instanceof BtiTableWriter);
        return ((BtiTableWriter) writer).indexWriter().indexEncryptionMetadata;
    }

    private static void appendRows(ColumnFamilyStore cfs, SSTableWriter writer)
    {
        List<ByteBuffer> keys = new ArrayList<>();
        for (int i = 0; i < 100; i++)
            keys.add(SSTableWriterTestBase.random(i, 10));
        keys.sort(Comparator.comparing((ByteBuffer key) -> cfs.getPartitioner().decorateKey(key)));

        for (ByteBuffer key : keys)
        {
            UpdateBuilder builder = UpdateBuilder.create(cfs.metadata(), key).withTimestamp(1);
            for (int j = 0; j < 10; j++)
                builder.newRow("" + j).add("val", ByteBuffer.allocate(100));
            writer.append(builder.build().unfilteredIterator());
        }
    }
}
