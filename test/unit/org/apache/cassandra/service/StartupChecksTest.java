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
package org.apache.cassandra.service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.slf4j.LoggerFactory;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DataStorageSpec;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.StartupChecksOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.exceptions.StartupException;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.service.DataResurrectionCheck.Heartbeat;
import org.apache.cassandra.utils.Clock;

import static java.util.Collections.singletonList;

import static org.apache.cassandra.config.CassandraRelevantProperties.TEST_INVALID_LEGACY_SSTABLE_ROOT;
import static org.apache.cassandra.config.CassandraRelevantProperties.TEST_CASSANDRA_TESTTAG;
import static org.apache.cassandra.io.util.FileUtils.createTempFile;
import static org.apache.cassandra.service.DataResurrectionCheck.HEARTBEAT_FILE_CONFIG_PROPERTY;
import static org.apache.cassandra.service.StartupChecks.StartupCheckType.check_data_resurrection;

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class StartupChecksTest
{
    StartupChecks startupChecks;
    Path sstableDir;
    static File heartbeatFile;
    private ListAppender<ILoggingEvent> testAppender;

    StartupChecksOptions options = new StartupChecksOptions();

    @BeforeClass
    public static void setupServer()
    {
        heartbeatFile = createTempFile("cassandra-heartbeat-", "");
        SchemaLoader.prepareServer();
    }

    @Before
    public void setup() throws IOException
    {
        for (ColumnFamilyStore cfs : Keyspace.open(SchemaConstants.SYSTEM_KEYSPACE_NAME).getColumnFamilyStores())
            cfs.clearUnsafe();
        for (File dataDir : Directories.getKSChildDirectories(SchemaConstants.SYSTEM_KEYSPACE_NAME))
            dataDir.deleteRecursive();

        File dataDir = DatabaseDescriptor.getAllDataFileLocations()[0];
        sstableDir = Paths.get(dataDir.absolutePath(), "Keyspace1", "Standard1");
        Files.createDirectories(sstableDir);

        options.enable(check_data_resurrection);
        options.getConfig(check_data_resurrection)
               .put(HEARTBEAT_FILE_CONFIG_PROPERTY, heartbeatFile.absolutePath());

        startupChecks = new StartupChecks();

        // Create and attach an in-memory ListAppender to capture warnings logged
        testAppender = new ListAppender<>();
        testAppender.start();
        ((Logger) LoggerFactory.getLogger(StartupChecks.class)).addAppender(testAppender);
    }

    @After
    public void tearDown() throws IOException
    {
        new File(sstableDir).deleteRecursive();
        ((Logger) LoggerFactory.getLogger(StartupChecks.class)).detachAppender(testAppender);
        testAppender.stop();
    }

    @AfterClass
    public static void tearDownClass()
    {
        heartbeatFile.delete();
    }

    @Test
    public void failStartupIfInvalidSSTablesFound() throws Exception
    {
        startupChecks = startupChecks.withTest(StartupChecks.checkSSTablesFormat);

        copyInvalidLegacySSTables(sstableDir);

        verifyFailure(startupChecks, "Detected unreadable sstables");

        // we should ignore invalid sstables in a snapshots directory
        new File(sstableDir).deleteRecursive();
        Path snapshotDir = sstableDir.resolve("snapshots");
        Files.createDirectories(snapshotDir);
        copyInvalidLegacySSTables(snapshotDir); startupChecks.verify(options);

        // and in a backups directory
        new File(sstableDir).deleteRecursive();
        Path backupDir = sstableDir.resolve("backups");
        Files.createDirectories(backupDir);
        copyInvalidLegacySSTables(backupDir);
        startupChecks.verify(options);

        // and in the system directory as of CASSANDRA-17777
        new File(backupDir).deleteRecursive();
        File dataDir = DatabaseDescriptor.getAllDataFileLocations()[0];
        Path systemDir = Paths.get(dataDir.absolutePath(), "system", "InvalidSystemDirectory");
        Files.createDirectories(systemDir);
        copyInvalidLegacySSTables(systemDir);
        startupChecks.verify(options);
    }

    @Test
    public void compatibilityCheckIgnoresNonDbFiles() throws Exception
    {
        startupChecks = startupChecks.withTest(StartupChecks.checkSSTablesFormat);

        copyLegacyNonSSTableFiles(sstableDir);
        assertNotEquals(0, new File(sstableDir).tryList().length);

        startupChecks.verify(options);
    }

    @Test
    public void checkReadAheadKbSettingCheck() throws Exception
    {
        // This test just validates if the verify function
        // doesn't throw any exceptions
        startupChecks = startupChecks.withTest(StartupChecks.checkReadAheadKbSetting);
        startupChecks.verify(options);
    }

    @Test
    public void testGetReadAheadKBPath()
    {
        Path sdaDirectory = StartupChecks.getReadAheadKBPath("/dev/sda12");
        Assert.assertEquals(Paths.get("/sys/block/sda/queue/read_ahead_kb"), sdaDirectory);

        Path scsiDirectory = StartupChecks.getReadAheadKBPath("/dev/scsi1");
        Assert.assertEquals(Paths.get("/sys/block/scsi/queue/read_ahead_kb"), scsiDirectory);

        Path dirWithoutNumbers = StartupChecks.getReadAheadKBPath("/dev/sca");
        Assert.assertEquals(Paths.get("/sys/block/sca/queue/read_ahead_kb"), dirWithoutNumbers);

        Path invalidDir = StartupChecks.getReadAheadKBPath("/invaliddir/xpto");
        Assert.assertNull(invalidDir);
    }

    @Test
    public void maxMapCountCheck() throws Exception
    {
        startupChecks = startupChecks.withTest(StartupChecks.checkMaxMapCount);
        startupChecks.verify(options);
    }

    private void copyLegacyNonSSTableFiles(Path targetDir) throws IOException
    {

        Path legacySSTableRoot = Paths.get(TEST_INVALID_LEGACY_SSTABLE_ROOT.getString(),
                                           "Keyspace1",
                                           "Standard1");
        for (String filename : new String[]{"Keyspace1-Standard1-ic-0-TOC.txt",
                                            "Keyspace1-Standard1-ic-0-Digest.sha1",
                                            "legacyleveled.json"})
            Files.copy(Paths.get(legacySSTableRoot.toString(), filename), targetDir.resolve(filename));
    }

    @Test
    public void testDataResurrectionCheck() throws Exception
    {
        DataResurrectionCheck check = new DataResurrectionCheck() {
            @Override
            List<String> getKeyspaces()
            {
                return singletonList("abc");
            }

            @Override
            List<TableGCPeriod> getTablesGcPeriods(String userKeyspace)
            {
                return singletonList(new TableGCPeriod("def", 10));
            }
        };

        Heartbeat heartbeat = new Heartbeat(Instant.ofEpochMilli(Clock.Global.currentTimeMillis()));
        heartbeat.serializeToJsonFile(heartbeatFile);

        Thread.sleep(15 * 1000);

        startupChecks.withTest(check);

        verifyFailure(startupChecks, "Invalid tables: abc.def");
    }

    private void copyInvalidLegacySSTables(Path targetDir) throws IOException
    {
        File legacySSTableRoot = new File(Paths.get(TEST_INVALID_LEGACY_SSTABLE_ROOT.getString(),
                                                    "Keyspace1",
                                                    "Standard1"));
        for (File f : legacySSTableRoot.tryList())
            Files.copy(f.toPath(), targetDir.resolve(f.name()));

    }

    private void verifyFailure(StartupChecks tests, String message)
    {
        try
        {
            tests.verify(options);
            fail("Expected a startup exception but none was thrown");
        }
        catch (StartupException e)
        {
            assertTrue(e.getMessage().contains(message));
        }
    }

    @Test(expected = StartupException.class)
    public void yamlConfigFail() throws Exception
    {
        // this is expected to fail because of ServerTestUtils.prepare() enabling enable_transient_replication
        // hack unset `cassandra.test`
        try (WithProperties properties = new WithProperties().clear(TEST_CASSANDRA_TESTTAG))
        {
            startupChecks = startupChecks.withTest(StartupChecks.checkYamlConfig);
            startupChecks.verify(options);
        }
    }

    @Test(expected = StartupException.class)
    public void yamlConfigFailOnSASI() throws Exception
    {
        try
        {
            DatabaseDescriptor.setTransientReplicationEnabledUnsafe(false);
            DatabaseDescriptor.getRawConfig().sasi_indexes_enabled = true;
            try (WithProperties properties = new WithProperties().clear(TEST_CASSANDRA_TESTTAG))
            {
                startupChecks = startupChecks.withTest(StartupChecks.checkYamlConfig);
                startupChecks.verify(options);
            }
        }
        finally
        {
            DatabaseDescriptor.setTransientReplicationEnabledUnsafe(true);
            DatabaseDescriptor.getRawConfig().sasi_indexes_enabled = false;
        }
    }

    @Test
    public void yamlConfig() throws Exception
    {
        // this only works because checkYamlConfig detects this is a test and skips the check
        startupChecks = startupChecks.withTest(StartupChecks.checkYamlConfig);
        startupChecks.verify(options);
    }

    @Test
    public void yamlConfigExplicit() throws Exception
    {
        boolean previous = DatabaseDescriptor.isTransientReplicationEnabled();
        try
        {
            DatabaseDescriptor.setTransientReplicationEnabledUnsafe(false);
            startupChecks = startupChecks.withTest(StartupChecks.checkYamlConfig);
            startupChecks.verify(options);
        }
        finally
        {
            DatabaseDescriptor.setTransientReplicationEnabledUnsafe(previous);
        }
    }

    @Test
    public void yamlConfigWarn() throws Exception
    {
        SSTableFormat<?, ?> selectedSSTableType = DatabaseDescriptor.getSelectedSSTableFormat();
        int tokens = DatabaseDescriptor.getNumTokens();
        boolean hcdDefaults = DatabaseDescriptor.isHcdGuardrailsDefaults();
        int tombstone_failure_threshold = DatabaseDescriptor.getGuardrailsConfig().getTombstoneFailThreshold();
        int batch_size_fail_threshold_in_kb = DatabaseDescriptor.getBatchSizeFailThresholdInKiB();
        int columns_per_table_failure_threshold = DatabaseDescriptor.getGuardrailsConfig().getColumnsPerTableFailThreshold();
        int fields_per_udt_failure_threshold = DatabaseDescriptor.getGuardrailsConfig().getFieldsPerUDTFailThreshold();
        DataStorageSpec.LongBytesBound collection_size_warn_threshold_in_kb = DatabaseDescriptor.getGuardrailsConfig().getCollectionSizeWarnThreshold();
        int items_per_collection_warn_threshold = DatabaseDescriptor.getGuardrailsConfig().getItemsPerCollectionWarnThreshold();
        int tables_warn_threshold = DatabaseDescriptor.getGuardrailsConfig().getTablesWarnThreshold();
        int tables_failure_threshold = DatabaseDescriptor.getGuardrailsConfig().getTablesFailThreshold();
        int in_select_cartesian_product_failure_threshold = DatabaseDescriptor.getGuardrailsConfig().getInSelectCartesianProductFailThreshold();
        int partition_keys_in_select_failure_threshold = DatabaseDescriptor.getGuardrailsConfig().getPartitionKeysInSelectFailThreshold();
        Set<ConsistencyLevel> write_consistency_levels_disallowed = DatabaseDescriptor.getGuardrailsConfig().getWriteConsistencyLevelsDisallowed();
        try
        {
            DatabaseDescriptor.setHcdGuardrailsDefaults(true);
            DatabaseDescriptor.getGuardrailsConfig().applyConfig();
            DatabaseDescriptor.setSelectedSSTableFormat(BigFormat.getInstance());
            DatabaseDescriptor.getRawConfig().num_tokens = 17;
            DatabaseDescriptor.getGuardrailsConfig().setTombstonesThreshold(DatabaseDescriptor.getGuardrailsConfig().getTombstoneWarnThreshold(), 100001);
            DatabaseDescriptor.setBatchSizeFailThresholdInKiB(641);
            DatabaseDescriptor.getGuardrailsConfig().setColumnsPerTableThreshold(DatabaseDescriptor.getGuardrailsConfig().getColumnsPerTableWarnThreshold(), 201);;
            DatabaseDescriptor.getGuardrailsConfig().setFieldsPerUDTThreshold(DatabaseDescriptor.getGuardrailsConfig().getFieldsPerUDTWarnThreshold(), 101);
            DatabaseDescriptor.getGuardrailsConfig().setCollectionSizeThreshold(new DataStorageSpec.LongBytesBound.LongBytesBound(10481, DataStorageSpec.DataStorageUnit.KIBIBYTES), DatabaseDescriptor.getGuardrailsConfig().getCollectionSizeFailThreshold());
            DatabaseDescriptor.getGuardrailsConfig().setItemsPerCollectionThreshold(201, DatabaseDescriptor.getGuardrailsConfig().getItemsPerCollectionFailThreshold());
            DatabaseDescriptor.getGuardrailsConfig().setTablesThreshold(101, 201);
            DatabaseDescriptor.getGuardrailsConfig().setInSelectCartesianProductThreshold(DatabaseDescriptor.getGuardrailsConfig().getInSelectCartesianProductWarnThreshold(), 26);
            DatabaseDescriptor.getGuardrailsConfig().setPartitionKeysInSelectThreshold(DatabaseDescriptor.getGuardrailsConfig().getPartitionKeysInSelectWarnThreshold(), 21);
            DatabaseDescriptor.getGuardrailsConfig().setWriteConsistencyLevelsDisallowed(Collections.emptySet());
            startupChecks = startupChecks.withTest(StartupChecks.checkYamlConfig);
            startupChecks.verify(options);
            assertWarningLogged("Not using murmur3 partitioner (org.apache.cassandra.dht.ByteOrderedPartitioner).");
            assertWarningLogged("num_tokens 17 too high. Values over 16 poorly impact repairs and node bootstrapping/decommissioning.");
            assertWarningLogged("Materialised Views should not be enabled.");
            assertWarningLogged("Using `DROP COMPACT STORAGE` on tables should not be enabled.");
            assertWarningLogged("Guardrails value 100001 for tombstone_failure_threshold is too high (>100000).");
            assertWarningLogged("Guardrails value 641 for batch_size_fail_threshold_in_kb is too high (>640).");
            assertWarningLogged("Guardrails value 201 for columns_per_table_fail_threshold is too high (>200).");
            assertWarningLogged("Guardrails value 101 for fields_per_udt_fail_threshold is too high (>100).");
            assertWarningLogged("Guardrails value 10481KiB for collection_size_warn_threshold is too high (>10480).");
            assertWarningLogged("Guardrails value 201 for items_per_collection_warn_threshold is too high (>200).");
            assertWarningLogged("Guardrails value 101 for tables_warn_threshold is too high (>100).");
            assertWarningLogged("Guardrails value 201 for tables_fail_threshold is too high (>200).");
            assertWarningLogged("Guardrails value 26 for in_select_cartesian_product_fail_threshold is too high (>25).");
            assertWarningLogged("Guardrails value 21 for partition_keys_in_select_fail_threshold is too high (>20).");
            assertWarningLogged("Guardrails value \"\" for write_consistency_levels_disallowed does not contain \"ANY\".");
            assertWarningLogged("Trie-based SSTables (bti) should always be the default (current is big).");
        }
        finally
        {
            DatabaseDescriptor.setHcdGuardrailsDefaults(hcdDefaults);
            DatabaseDescriptor.getGuardrailsConfig().applyConfig();
            DatabaseDescriptor.setSelectedSSTableFormat(selectedSSTableType);

            DatabaseDescriptor.getRawConfig().num_tokens = tokens;
            DatabaseDescriptor.getGuardrailsConfig().setTombstonesThreshold(DatabaseDescriptor.getGuardrailsConfig().getTombstoneWarnThreshold(), tombstone_failure_threshold);
            DatabaseDescriptor.setBatchSizeFailThresholdInKiB(batch_size_fail_threshold_in_kb);
            DatabaseDescriptor.getGuardrailsConfig().setColumnsPerTableThreshold(DatabaseDescriptor.getGuardrailsConfig().getColumnsPerTableWarnThreshold(), columns_per_table_failure_threshold);
            DatabaseDescriptor.getGuardrailsConfig().setFieldsPerUDTThreshold(DatabaseDescriptor.getGuardrailsConfig().getFieldsPerUDTWarnThreshold(), fields_per_udt_failure_threshold);
            DatabaseDescriptor.getGuardrailsConfig().setCollectionSizeThreshold(collection_size_warn_threshold_in_kb, DatabaseDescriptor.getGuardrailsConfig().getCollectionSizeWarnThreshold());
            DatabaseDescriptor.getGuardrailsConfig().setItemsPerCollectionThreshold(items_per_collection_warn_threshold, DatabaseDescriptor.getGuardrailsConfig().getItemsPerCollectionFailThreshold());
            DatabaseDescriptor.getGuardrailsConfig().setTablesThreshold(tables_warn_threshold, tables_failure_threshold);
            DatabaseDescriptor.getGuardrailsConfig().setInSelectCartesianProductThreshold(DatabaseDescriptor.getGuardrailsConfig().getInSelectCartesianProductWarnThreshold(), in_select_cartesian_product_failure_threshold);
            DatabaseDescriptor.getGuardrailsConfig().setPartitionKeysInSelectThreshold(DatabaseDescriptor.getGuardrailsConfig().getPartitionKeysInSelectWarnThreshold(), partition_keys_in_select_failure_threshold);
            DatabaseDescriptor.getGuardrailsConfig().setWriteConsistencyLevelsDisallowed(write_consistency_levels_disallowed);
        }
    }

    @Test
    public void checkDataDirs() throws Exception
    {
        startupChecks = startupChecks.withTest(StartupChecks.checkDataDirs);
        startupChecks.verify(options);
    }

    @Test
    public void checkDataDirsWarn() throws Exception
    {
        startupChecks = startupChecks.withTest(StartupChecks.checkDataDirs);
        startupChecks.verify(options);
        File[] previous = DatabaseDescriptor.getNonLocalSystemKeyspacesDataFileLocations();
        try
        {
            DatabaseDescriptor.setDataDirectories(new File[]{previous[0], new File(Files.createTempDirectory("StartupChecksTest.checkDataDirsFail"))});
            startupChecks = startupChecks.withTest(StartupChecks.checkDataDirs);
            startupChecks.verify(options);
            assertWarningLogged("Multiple 2 data_dir configured.  Best practice is to use a (striped) LVM.");
        }
        finally
        {
            DatabaseDescriptor.setDataDirectories(previous);
        }
    }
    @Test
    public void checkTableSettings() throws Exception
    {
        QueryProcessor.executeInternal("CREATE KEYSPACE IF NOT EXISTS startupcheckstest WITH replication={ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }");
        QueryProcessor.executeInternal("CREATE TABLE startupcheckstest.c1(pk int, ck int, v int, PRIMARY KEY(pk, ck))");
        startupChecks = startupChecks.withTest(StartupChecks.checkTableSettings);
        startupChecks.verify(options);
    }

    @Test
    public void checkTableSettingsWarn() throws Exception
    {
        IPartitioner previous = DatabaseDescriptor.getPartitioner();
        try
        {
            DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
            QueryProcessor.executeInternal("CREATE KEYSPACE IF NOT EXISTS startupcheckstest WITH replication={ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }");
            QueryProcessor.executeInternal("CREATE TABLE startupcheckstest.c2(pk int, ck int, v int, PRIMARY KEY(pk, ck)) WITH compaction={'class': 'SizeTieredCompactionStrategy'}");
            QueryProcessor.executeInternal("CREATE TABLE startupcheckstest.c3(pk int PRIMARY KEY, v int) WITH COMPACT STORAGE");
            startupChecks = startupChecks.withTest(StartupChecks.checkTableSettings);
            startupChecks.verify(options);
            assertWarningLogged("The following tables using STCS and LCS should be altered to use UnifiedCompactionStrategy (UCS): startupcheckstest.c2");
            assertWarningLogged("The following tables are `WITH COMPACT STORAGE` and need to be manually migrated to normal tables (Avoid using `DROP COMPACT STORAGE`): startupcheckstest.c3");
        }
        finally
        {
            DatabaseDescriptor.setPartitionerUnsafe(previous);
        }
    }

    private void assertWarningLogged(String expectedMessage)
    {
        boolean found = testAppender.list.stream()
                .anyMatch(event -> Level.WARN == event.getLevel() && event.getFormattedMessage().contains(expectedMessage));

        assertTrue("Expected warning not logged: " + expectedMessage, found);
    }
}
