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

package org.apache.cassandra.distributed.test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.stream.Collectors;
import javax.crypto.NoSuchPaddingException;

import com.google.common.primitives.Bytes;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.crypto.LocalSystemKey;
import org.apache.cassandra.crypto.TDEConfigurationProvider;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.bti.BtiFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.ChecksumType;

import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.shared.FutureUtils.waitOn;
import static org.apache.cassandra.io.compress.EncryptedSequentialWriter.CHUNK_SIZE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.ThrowableAssert.catchThrowable;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SSTableEncryptionTest extends TestBaseImpl
{
    private static final String KEYSPACE_PREFIX = "ks";
    private static final String TABLE_PREFIX = "tbl";
    private static final String SENSITIVE_KEY = "Key with sensitive information";
    private static final int ROWS_COUNT = 10000;

    private static String defaultSystemKeyDirectory;

    @BeforeClass
    public static void beforeAll() throws IOException
    {
        defaultSystemKeyDirectory = TDEConfigurationProvider.getConfiguration().systemKeyDirectory;
        Path systemKeyDirectory = Files.createTempDirectory("system_key_directory");
        TDEConfigurationProvider.setSystemKeyDirectoryProperty(systemKeyDirectory.toString());
    }

    @AfterClass
    public static void tearDown()
    {
        TDEConfigurationProvider.setSystemKeyDirectoryProperty(defaultSystemKeyDirectory);
    }

    @Test
    public void shouldCreateQueryableEncryptedSSTables() throws Throwable
    {
        try (Cluster cluster = builder().withNodes(2)
                                        .withConfig(config -> config.with(GOSSIP).with(NETWORK))
                                        .start())
        {
            // given a table with data encrypted using local key
            String keyspace = createKeyspace(cluster);
            Path secretKey = createLocalSecretKey();
            String table = createEncryptedTable(cluster, keyspace, secretKey);
            int numberOfRows = 10;

            for (int i = 0; i < numberOfRows; i++)
            {
                for (int j = 0; j < numberOfRows; j++)
                {
                    cluster.coordinator(1).execute(String.format("INSERT INTO %s.%s (id, cc, value) VALUES ('%s', '%s', '%s')", keyspace, table, i, j, j), ConsistencyLevel.ALL);
                }
            }
            // flush to make sure we have sstables
            cluster.get(1).flush(keyspace);

            insertAndFlush(cluster, keyspace, table, numberOfRows);

            // when querying all
            Object[][] rows = cluster.coordinator(1).execute(String.format("SELECT * FROM %s.%s ", keyspace, table), ALL);

            // then read should succeed
            assertThat(rows.length).isEqualTo(100);

            // when querying by id
            for (int i = 0; i < 10; i++)
            {
                Object[][] byIdRows = cluster.coordinator(1).execute(String.format(String.format("SELECT * FROM %%s.%%s WHERE id = '%s';", i), keyspace, table), ALL);

                // then read should succeed
                assertThat(byIdRows.length).isEqualTo(10);
                assertThat(byIdRows[0][0]).isEqualTo(String.valueOf(i));
                assertThat(byIdRows[0][1]).isEqualTo("0");
            }

            // when querying via a range
            Object[][] byIdRows = cluster.coordinator(1).execute(String.format(String.format("SELECT * FROM %%s.%%s WHERE id = '%s' and cc >= '%s' and cc <= '%s';", 5, 2, 8), keyspace, table), ALL);

            // then read should succeed
            assertThat(byIdRows.length).isEqualTo(7);
            assertThat(byIdRows[0][0]).isEqualTo(String.valueOf(5));
            assertThat(byIdRows[0][1]).isEqualTo(String.valueOf(2));
            assertThat(byIdRows[0][2]).isEqualTo(String.valueOf(2));
        }
    }

    @Test
    public void shouldEncryptSensitiveData() throws Exception
    {
        try (Cluster cluster = builder().withNodes(1)
                                        .withConfig(config -> config.with(GOSSIP).with(NETWORK))
                                        .start())
        {
            // given tables with and without encryption
            String keyspace = createKeyspace(cluster);
            TestTable nonEncryptedTable = createTableWithSampleData(cluster, keyspace, "");
            Path secretKey = createLocalSecretKey();
            TestTable encryptedTable = createTableWithSampleData(cluster, keyspace, localSystemKeyEncryptionCompressionSuffix("Encryptor", secretKey.toAbsolutePath().toString()));

            // then
            // sensitive key should not be present in encrypted data
            byte[] sensitiveBytes = SENSITIVE_KEY.getBytes(StandardCharsets.UTF_8);
            assertThat(Bytes.indexOf(nonEncryptedTable.sstableBytes, sensitiveBytes)).isNotEqualTo(-1);
            assertThat(Bytes.indexOf(encryptedTable.sstableBytes, sensitiveBytes)).isEqualTo(-1);
            // sensitive key should not be present in encrypted partition index
            assertThat(Bytes.indexOf(nonEncryptedTable.partitionIndexBytes, sensitiveBytes)).isNotEqualTo(-1);
            assertThat(Bytes.indexOf(encryptedTable.partitionIndexBytes, sensitiveBytes)).isEqualTo(-1);


            // indexes with encryption should pass the checksum check
            assertThat(checkEncryptionCrc(encryptedTable.partitionIndexBytes)).isTrue();
            assertThat(checkEncryptionCrc(encryptedTable.rowIndexBytes)).isTrue();
            // indexes without encryption should fail the checksum check
            assertThat(checkEncryptionCrc(nonEncryptedTable.partitionIndexBytes)).isFalse();
            assertThat(checkEncryptionCrc(nonEncryptedTable.rowIndexBytes)).isFalse();
        }
    }

    private boolean checkEncryptionCrc(byte[] bytes)
    {
        try
        {
            ByteBuffer buffer = ByteBuffer.wrap(bytes);
            buffer.position(0).limit(CHUNK_SIZE - 4);
            int calculatedChecksum = (int) ChecksumType.CRC32.of(buffer);
            //Change the limit to include the checksum
            buffer.limit(CHUNK_SIZE);
            int readChecksum = buffer.getInt();
            return calculatedChecksum == readChecksum;
        }
        catch (Exception e)
        {
            return false;
        }
    }

    @Test
    public void shouldNotReadRowsFromEncryptedTableWithoutTheSecretKey() throws Exception
    {
        try (Cluster cluster = builder().withNodes(1)
                                        .withConfig(config -> config.with(GOSSIP).with(NETWORK))
                                        .start())
        {
            // ignore throwing an exception when closing the cluster as missing key will result in exceptions in logs
            cluster.setUncaughtExceptionsFilter(t -> true);

            // given a table with data encrypted using local key
            String keyspace = createKeyspace(cluster);
            Path secretKey = createLocalSecretKey();
            String encryptedTableName = createEncryptedTable(cluster, keyspace, secretKey);
            String nonEncryptedTableName = createTable(cluster, keyspace);
            int numberOfRows = 10;
            insertAndFlush(cluster, keyspace, encryptedTableName, numberOfRows);
            insertAndFlush(cluster, keyspace, nonEncryptedTableName, numberOfRows);

            // delete secret key file
            assertTrue("secret key should be deleted", Files.deleteIfExists(secretKey));

            // restart to clear in memory secret key cache
            waitOn(cluster.get(1).shutdown());
            cluster.get(1).startup();

            // when
            Object[][] rows = cluster. get(1).executeInternal(String.format("SELECT * FROM %s.%s", keyspace, nonEncryptedTableName));
            Throwable throwable = catchThrowable(() -> cluster.get(0).executeInternal(String.format("SELECT * FROM %s.%s ", keyspace, encryptedTableName)));

            // then it should be possible to read the table without encryption
            assertThat(rows.length).isEqualTo(numberOfRows);
            // then it should not be possible to read the encrypted table
            assertThat(throwable).isInstanceOf(IndexOutOfBoundsException.class);
        }
    }

    @Test
    public void shouldFailWhenReadingWithDifferentKey() throws Exception
    {
        try (Cluster cluster = builder().withNodes(1)
                                        .withConfig(config -> config.with(GOSSIP).with(NETWORK))
                                        .start())
        {
            // ignore throwing an exception when closing the cluster as missing key will result in exceptions in logs
            cluster.setUncaughtExceptionsFilter(t -> true);

            // given a table with data encrypted using local key
            String keyspace = createKeyspace(cluster);
            Path secretKey = createLocalSecretKey();
            String encryptedTableName = createEncryptedTable(cluster, keyspace, secretKey);
            String nonEncryptedTableName = createTable(cluster, keyspace);
            int numberOfRows = 10;
            insertAndFlush(cluster, keyspace, encryptedTableName, numberOfRows);
            insertAndFlush(cluster, keyspace, nonEncryptedTableName, numberOfRows);

            // delete secret key file
            assertTrue("secret key should be deleted", Files.deleteIfExists(secretKey));

            Path secretKey2 = createLocalSecretKey(secretKey.toString());

            assertEquals(secretKey, secretKey2);

            // restart to clear in memory secret key cache
            waitOn(cluster.get(1).shutdown());
            cluster.get(1).startup();

            // when
            Object[][] rows = cluster. get(1).executeInternal(String.format("SELECT * FROM %s.%s", keyspace, nonEncryptedTableName));
            Throwable throwable = catchThrowable(() -> cluster.get(0).executeInternal(String.format("SELECT * FROM %s.%s ", keyspace, encryptedTableName)));

            // then it should be possible to read the table without encryption
            assertThat(rows.length).isEqualTo(numberOfRows);
            // then it should not be possible to read the encrypted table
            assertThat(throwable).isInstanceOf(IndexOutOfBoundsException.class);
        }
    }

    private TestTable createTableWithSampleData(Cluster cluster, String keyspace, String tableDefSuffix) throws IOException
    {
        String tableName = randomTableName();
        String createTableCql = "CREATE TABLE %s.%s (id text, cc text, value text, PRIMARY KEY ((id), cc))" + tableDefSuffix;
        cluster.schemaChange(String.format(createTableCql, keyspace, tableName));

        int k = 0;
        for (int i = 0; i < 10; i++)
        {
            for (int j = 0; j < ROWS_COUNT; j++)
            {
                cluster.coordinator(1).execute(String.format("INSERT INTO %s.%s (id, cc, value) VALUES ('%s', '%s', '%s')", keyspace, tableName, i, j, k), ALL);
                k++;
            }
        }

        cluster.coordinator(1).execute(String.format("INSERT INTO %s.%s (id, cc, value) VALUES ('%s', '%s', '%s')", keyspace, tableName, SENSITIVE_KEY, SENSITIVE_KEY, SENSITIVE_KEY), ALL);

        // flush to make sure we have sstable
        cluster.get(1).flush(keyspace);

        List<String> sstablePaths = getPathsFor(cluster, keyspace, tableName, SSTableFormat.Components.DATA);
        List<String> partitionIndexPaths = getPathsFor(cluster, keyspace, tableName, BtiFormat.Components.PARTITION_INDEX);
        List<String> rowIndexPaths = getPathsFor(cluster, keyspace, tableName, BtiFormat.Components.ROW_INDEX);

        String sstablePath = sstablePaths.get(0);
        byte[] sstableBytes = Files.readAllBytes(Path.of(sstablePath));

        String partitionIndexPath = partitionIndexPaths.get(0);
        byte[] partitionIndexBytes = Files.readAllBytes(Path.of(partitionIndexPath));

        String rowIndexPath = rowIndexPaths.get(0);
        byte[] rowIndexBytes = Files.readAllBytes(Path.of(rowIndexPath));

        return new TestTable(tableName, sstableBytes, sstablePath, partitionIndexBytes, partitionIndexPath, rowIndexBytes, rowIndexPath);
    }

    private enum ComponentType { DATA, PARTITION_INDEX, ROW_INDEX }
    
    private List<String> getPathsFor(Cluster cluster, String keyspace, String tableName, Component component)
    {
        // Determine component type before passing to lambda
        ComponentType componentType;
        if (component == SSTableFormat.Components.DATA) {
            componentType = ComponentType.DATA;
        } else if (component == BtiFormat.Components.PARTITION_INDEX) {
            componentType = ComponentType.PARTITION_INDEX;
        } else if (component == BtiFormat.Components.ROW_INDEX) {
            componentType = ComponentType.ROW_INDEX;
        } else {
            throw new IllegalArgumentException("Unsupported component: " + component);
        }
        
        return cluster.get(1).callOnInstance(() -> {
            Component comp;
            switch (componentType) {
                case DATA:
                    comp = SSTableFormat.Components.DATA;
                    break;
                case PARTITION_INDEX:
                    comp = BtiFormat.Components.PARTITION_INDEX;
                    break;
                case ROW_INDEX:
                    comp = BtiFormat.Components.ROW_INDEX;
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported component type");
            }
            return Keyspace.open(keyspace).getColumnFamilyStore(tableName).getLiveSSTables()
                           .stream()
                           .map(SSTableReader::getDescriptor)
                           .map(d -> d.pathFor(comp).toString())
                           .collect(Collectors.toList());
        });
    }

    private String createKeyspace(Cluster cluster)
    {
        String randomKeyspaceName = KEYSPACE_PREFIX + "_" + RandomStringUtils.randomNumeric(5);
        cluster.schemaChange(String.format("CREATE KEYSPACE IF NOT EXISTS %s WITH REPLICATION = {'class':'SimpleStrategy','replication_factor':'1'}", randomKeyspaceName));
        return randomKeyspaceName;
    }

    private Path createLocalSecretKey() throws IOException, NoSuchAlgorithmException, NoSuchPaddingException
    {
        String keyPath = "system_key_" + RandomStringUtils.random(10, true, true);
        return createLocalSecretKey(keyPath);
    }

    private Path createLocalSecretKey(String keyPath) throws IOException, NoSuchAlgorithmException, NoSuchPaddingException
    {
        return LocalSystemKey.createKey(keyPath, "AES", 256);
    }

    private String createEncryptedTable(Cluster cluster, String keyspace, Path secretKey)
    {
        String table = randomTableName();
        cluster.schemaChange(String.format("CREATE TABLE %s.%s (id text, cc text, value text, PRIMARY KEY ((id), cc)) WITH compression = " +
                          "{'class' : 'Encryptor', " +
                          "'cipher_algorithm' : 'AES/ECB/PKCS5Padding', " +
                          "'secret_key_strength' : 128, " +
                          "'key_provider' : 'LocalFileSystemKeyProviderFactory', " +
                          "'secret_key_file': '%s' };", keyspace, table, secretKey.toAbsolutePath()));
        return table;
    }

    private String createTable(Cluster cluster, String keyspace)
    {
        String table = randomTableName();
        cluster.schemaChange(String.format("CREATE TABLE %s.%s (id text, cc text, value text, PRIMARY KEY ((id), cc))", keyspace, table));
        return table;
    }

    private String randomTableName()
    {
        return TABLE_PREFIX + "_" + RandomStringUtils.randomNumeric(5);
    }

    private void insertAndFlush(Cluster cluster, String keyspace, String table, int rows)
    {
        for (int i = 0; i < rows; i++)
        {
            cluster.coordinator(1).execute(String.format("INSERT INTO %s.%s (id, cc, value) VALUES ('%s', '%s', '%s')", keyspace, table, i, i, i), ALL);
        }
        // flush to make sure we have sstables
         cluster.get(1).flush(keyspace);
    }

    private String localSystemKeyEncryptionCompressionSuffix(String className, String secretKeyPath)
    {
        return String.format(" WITH compression = " +
                             "{'class' : '%s', " +
                             "'cipher_algorithm' : 'AES/ECB/PKCS5Padding', " +
                             "'secret_key_strength' : 128, " +
                             "'key_provider' : 'LocalFileSystemKeyProviderFactory', " +
                             "'secret_key_file': '%s' };", className, secretKeyPath);
    }

    private static class TestTable
    {
        public final String tableName;
        public final byte[] sstableBytes;
        public final String sstablePath;
        public final String partitionIndexPath;
        public final byte[] partitionIndexBytes;
        public final String rowIndexPath;
        public final byte[] rowIndexBytes;

        public TestTable(String tableName, byte[] tableBytes, String sstablePath,  byte[] partitionIndexBytes, String partitionIndexPath, byte[] rowIndexBytes, String rowIndexPath)
        {
            this.tableName = tableName;
            this.sstableBytes = tableBytes;
            this.sstablePath = sstablePath;
            this.partitionIndexPath = partitionIndexPath;
            this.partitionIndexBytes = partitionIndexBytes;
            this.rowIndexPath = rowIndexPath;
            this.rowIndexBytes = rowIndexBytes;
        }
    }
}
