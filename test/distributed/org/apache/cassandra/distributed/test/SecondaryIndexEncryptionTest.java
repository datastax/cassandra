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

package org.apache.cassandra.distributed.test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.crypto.LocalSystemKey;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.index.internal.CassandraIndex;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.compress.Encryptor;
import org.apache.cassandra.io.sstable.SSTableIdFactory;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.service.StorageService;

import static org.assertj.core.api.Assertions.assertThat;

public class SecondaryIndexEncryptionTest extends TestBaseImpl
{
    private static final String TABLE = "tbl";
    private static final String INDEX = "idx";
    private static final String INDEXED_VAL = "testing_val1";

    @BeforeClass
    public static void beforeAll() throws IOException
    {
        Path systemKeyDirectory = Files.createTempDirectory("system_key_directory");
        CassandraRelevantProperties.SYSTEM_KEY_DIRECTORY.setString(systemKeyDirectory.toString());
    }

    @AfterClass
    public static void tearDown()
    {
        CassandraRelevantProperties.SYSTEM_KEY_DIRECTORY.reset();
    }

    @Test
    public void upgradeSSTablesEncryptsIndexSSTables() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(1).start()))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s." + TABLE + " (id int PRIMARY KEY, value text)"));
            cluster.schemaChange(withKeyspace("CREATE INDEX " + INDEX + " ON %s." + TABLE + " (value) USING 'legacy_local_table'"));
            cluster.get(1).runOnInstance(() -> Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE).disableAutoCompaction());

            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s." + TABLE + " (id, value) VALUES (1, '" + INDEXED_VAL + "')"), ConsistencyLevel.ALL);
            cluster.get(1).flush(KEYSPACE);

            cluster.get(1).runOnInstance(() ->
            {
                assertThat(latestIndexSSTable(KEYSPACE, TABLE, INDEX)
                        .getCompressionMetadata()
                        .parameters
                        .getSstableCompressor())
                        .isNotInstanceOf(Encryptor.class);
            });

            String secretKeyPath = LocalSystemKey.createKey("test_key_" + RandomStringUtils.random(8, true, true), "AES", 128)
                    .toAbsolutePath().toString();

            // enable TDE
            cluster.schemaChange(withKeyspace(encryptionAlterCql("%s." + TABLE, secretKeyPath)));

            cluster.get(1).runOnInstance(() ->
            {
                try
                {
                    StorageService.instance.upgradeSSTables(KEYSPACE, false, TABLE);
                }
                catch (Exception e)
                {
                    throw new RuntimeException(e);
                }
            });

            // assert that encryption works
            cluster.get(1).runOnInstance(() ->
            {
                CompressionMetadata meta = latestIndexSSTable(KEYSPACE, TABLE, INDEX).getCompressionMetadata();
                assertThat(meta.parameters.getSstableCompressor())
                        .isInstanceOf(Encryptor.class);
                assertThat(meta.parameters.getOtherOptions().get("secret_key_file"))
                        .isEqualTo(secretKeyPath);
            });

            // assert that the inserted data is queryable after adding enryption.
            Object[][] rows = cluster.coordinator(1).execute(withKeyspace("SELECT id, value FROM %s." + TABLE + " WHERE id = 1"), ConsistencyLevel.ALL);
            assertThat((String) rows[0][1]).isEqualTo(INDEXED_VAL);
        }
    }

    @Test
    public void rebuildIndexUsesNewKeyAfterKeyRotation() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(1).start()))
        {
            String secretKeyPath1 = LocalSystemKey.createKey("test_key_" + RandomStringUtils.random(8, true, true), "AES", 128)
                    .toAbsolutePath().toString();

            cluster.schemaChange(withKeyspace(String.format("CREATE TABLE %%s.%s (id int PRIMARY KEY, value text) %s",
                                                            TABLE, encryptionCompressionClause(secretKeyPath1))));
            cluster.schemaChange(withKeyspace("CREATE INDEX " + INDEX + " ON %s." + TABLE + " (value) USING 'legacy_local_table'"));
            cluster.get(1).runOnInstance(() -> Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE).disableAutoCompaction());

            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s." + TABLE + " (id, value) VALUES (1, '" + INDEXED_VAL + "')"), ConsistencyLevel.ALL);
            cluster.get(1).flush(KEYSPACE);

            cluster.get(1).runOnInstance(() ->
            {
                assertThat(latestIndexSSTable(KEYSPACE, TABLE, INDEX).getCompressionMetadata()
                        .parameters
                        .getOtherOptions()
                        .get("secret_key_file"))
                        .isEqualTo(secretKeyPath1);
            });

            // create another new keyPath
            String secretKeyPath2 = LocalSystemKey.createKey("test_key_" + RandomStringUtils.random(8, true, true), "AES", 128)
                    .toAbsolutePath().toString();

            // Update with new keypath
            cluster.schemaChange(withKeyspace(encryptionAlterCql("%s." + TABLE, secretKeyPath2)));

            cluster.get(1).runOnInstance(() -> StorageService.instance.rebuildSecondaryIndex(KEYSPACE, TABLE, INDEX));

            // SSTable written by the rebuild uses the new key on disk
            cluster.get(1).runOnInstance(() ->
            {
                assertThat(latestIndexSSTable(KEYSPACE, TABLE, INDEX).getCompressionMetadata()
                        .parameters
                        .getOtherOptions().get("secret_key_file"))
                        .isEqualTo(secretKeyPath2)
                        .isNotEqualTo(secretKeyPath1);
            });
        }
    }

    @Test
    public void flushAfterKeyRotationUsesNewKeyInIndexSSTables() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(1).start()))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s." + TABLE + " (id int PRIMARY KEY, value text)"));
            cluster.schemaChange(withKeyspace("CREATE INDEX " + INDEX + " ON %s." + TABLE + " (value) USING 'legacy_local_table'"));
            cluster.get(1).runOnInstance(() -> Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE).disableAutoCompaction());

            final String valPlain = "val_plain_" + RandomStringUtils.randomNumeric(6);
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s." + TABLE + " (id, value) VALUES (1, '" + valPlain + "')"), ConsistencyLevel.ALL);
            cluster.get(1).flush(KEYSPACE);

            // given Table with TDE disabled
            cluster.get(1).runOnInstance(() ->
            {
                // on-disk: SSTable written without encryption
                assertThat(latestIndexSSTable(KEYSPACE, TABLE, INDEX)
                        .getCompressionMetadata()
                        .parameters
                        .getSstableCompressor())
                        .isNotInstanceOf(Encryptor.class);
            });

            String secretKeyPath1 = LocalSystemKey.createKey("test_key_" + RandomStringUtils.random(8, true, true), "AES", 128)
                    .toAbsolutePath().toString();

            // Enable TDE
            cluster.schemaChange(withKeyspace(encryptionAlterCql("%s." + TABLE, secretKeyPath1)));

            final String valKey1 = "val_key1_" + RandomStringUtils.randomNumeric(6);
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s." + TABLE + " (id, value) VALUES (2, '" + valKey1 + "')"), ConsistencyLevel.ALL);
            cluster.get(1).flush(KEYSPACE);

            // assert encrypted index SSTable.
            cluster.get(1).runOnInstance(() ->
            {
                // on-disk: SSTable written with key1
                assertThat(latestIndexSSTable(KEYSPACE, TABLE, INDEX)
                        .getCompressionMetadata()
                        .parameters
                        .getOtherOptions()
                        .get("secret_key_file"))
                        .isEqualTo(secretKeyPath1);
            });

            String secretKeyPath2 = LocalSystemKey.createKey("test_key_" + RandomStringUtils.random(8, true, true), "AES", 128)
                    .toAbsolutePath().toString();

            //enable TDE with another key
            cluster.schemaChange(withKeyspace(encryptionAlterCql("%s." + TABLE, secretKeyPath2)));

            final String valKey2 = "val_key2_" + RandomStringUtils.randomNumeric(6);
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s." + TABLE + " (id, value) VALUES (3, '" + valKey2 + "')"), ConsistencyLevel.ALL);
            cluster.get(1).flush(KEYSPACE);

            cluster.get(1).runOnInstance(() -> {
                // on-disk: SSTable written with key2
                assertThat(latestIndexSSTable(KEYSPACE, TABLE, INDEX)
                        .getCompressionMetadata()
                        .parameters
                        .getOtherOptions()
                        .get("secret_key_file"))
                        .isEqualTo(secretKeyPath2)
                        .isNotEqualTo(secretKeyPath1);
            });
        }
    }

    private static ColumnFamilyStore getIndexCfs(String keyspace, String table, String index)
    {
        return ((CassandraIndex) Keyspace.open(keyspace).getColumnFamilyStore(table).indexManager.getIndexByName(index))
                .getIndexCfs();
    }

    private static SSTableReader latestIndexSSTable(String keyspace, String table, String index)
    {
        return getIndexCfs(keyspace, table, index)
                .getLiveSSTables()
                .stream()
                .max(Comparator.comparing(s -> s.descriptor.id, SSTableIdFactory.COMPARATOR))
                .orElseThrow(() -> new IllegalStateException("No live SSTables for index " + index));
    }

    private static String encryptionCompressionClause(String secretKeyPath)
    {
        return String.format("WITH compression = {'class': 'Encryptor', 'cipher_algorithm': 'AES/ECB/PKCS5Padding', " +
                             "'secret_key_strength': 128, 'key_provider': 'LocalFileSystemKeyProviderFactory', " +
                             "'secret_key_file': '%s'}", secretKeyPath);
    }

    private static String encryptionAlterCql(String qualifiedTable, String secretKeyPath)
    {
        return String.format("ALTER TABLE %s %s", qualifiedTable, encryptionCompressionClause(secretKeyPath));
    }
}
