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
import java.util.Collections;
import java.util.Set;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.StartupException;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.SchemaConstants;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class StartupChecksTest
{
    public static final String INVALID_LEGACY_SSTABLE_ROOT_PROP = "invalid-legacy-sstable-root";
    StartupChecks startupChecks;
    Path sstableDir;
    private ListAppender<ILoggingEvent> testAppender;

    @BeforeClass
    public static void setupServer()
    {
        SchemaLoader.prepareServer();
    }

    @Before
    public void setup() throws IOException
    {
        for (ColumnFamilyStore cfs : Keyspace.open(SchemaConstants.SYSTEM_KEYSPACE_NAME).getColumnFamilyStores())
            cfs.clearUnsafe();
        for (File dataDir : Directories.getKSChildDirectories(SchemaConstants.SYSTEM_KEYSPACE_NAME))
            FileUtils.deleteRecursive(dataDir);

        File dataDir = DatabaseDescriptor.getAllDataFileLocations()[0];
        sstableDir = Paths.get(dataDir.absolutePath(), "Keyspace1", "Standard1");
        Files.createDirectories(sstableDir);

        startupChecks = new StartupChecks();

        // Create and attach an in-memory ListAppender to capture warnings logged
        testAppender = new ListAppender<>();
        testAppender.start();
        ((Logger) LoggerFactory.getLogger(StartupChecks.class)).addAppender(testAppender);
    }

    @After
    public void tearDown() throws IOException
    {
        FileUtils.deleteRecursive(new File(sstableDir));
        ((Logger) LoggerFactory.getLogger(StartupChecks.class)).detachAppender(testAppender);
        testAppender.stop();
    }

    @Test
    public void failStartupIfInvalidSSTablesFound() throws Exception
    {
        startupChecks = startupChecks.withTest(StartupChecks.checkSSTablesFormat);

        copyInvalidLegacySSTables(sstableDir);

        verifyFailure(startupChecks, "Detected unreadable sstables");

        // we should ignore invalid sstables in a snapshots directory
        FileUtils.deleteRecursive(new File(sstableDir));
        Path snapshotDir = sstableDir.resolve("snapshots");
        Files.createDirectories(snapshotDir);
        copyInvalidLegacySSTables(snapshotDir); startupChecks.verify();

        // and in a backups directory
        FileUtils.deleteRecursive(new File(sstableDir));
        Path backupDir = sstableDir.resolve("backups");
        Files.createDirectories(backupDir);
        copyInvalidLegacySSTables(backupDir);
        startupChecks.verify();
    }

    @Test
    public void compatibilityCheckIgnoresNonDbFiles() throws Exception
    {
        startupChecks = startupChecks.withTest(StartupChecks.checkSSTablesFormat);

        copyLegacyNonSSTableFiles(sstableDir);
        assertFalse(new File(sstableDir).tryList().length == 0);

        startupChecks.verify();
    }

    @Test
    public void maxMapCountCheck() throws Exception
    {
        startupChecks = startupChecks.withTest(StartupChecks.checkMaxMapCount);
        startupChecks.verify();
    }

    @Test(expected = StartupException.class)
    public void yamlConfigFail() throws Exception
    {
        // this is expected to fail because of ServerTestUtils.prepare() enabling enable_transient_replication
        // hack unset `cassandra.test`
        String cassandraTestTag = System.getProperty("cassandra.testtag");
        try
        {
            System.clearProperty("cassandra.testtag");
            startupChecks = startupChecks.withTest(StartupChecks.checkYamlConfig);
            startupChecks.verify();
        }
        finally
        {
            System.setProperty("cassandra.testtag", cassandraTestTag);
        }
    }

    @Test
    public void yamlConfig() throws Exception
    {
        // this only works because checkYamlConfig detects this is a test and skips the check
        startupChecks = startupChecks.withTest(StartupChecks.checkYamlConfig);
        startupChecks.verify();
    }

    @Test
    public void yamlConfigExplicit() throws Exception
    {
        boolean previous = DatabaseDescriptor.isTransientReplicationEnabled();
        try
        {
            DatabaseDescriptor.setTransientReplicationEnabledUnsafe(false);
            startupChecks = startupChecks.withTest(StartupChecks.checkYamlConfig);
            startupChecks.verify();
        }
        finally
        {
            DatabaseDescriptor.setTransientReplicationEnabledUnsafe(previous);
        }
    }

    @Test
    public void yamlConfigWarn() throws Exception
    {
        String sstableTypeProp = System.getProperty(SSTableFormat.FORMAT_DEFAULT_PROP, SSTableFormat.Type.BTI.name).toUpperCase();
        int tokens = DatabaseDescriptor.getNumTokens();
        int tombstone_failure_threshold = DatabaseDescriptor.getGuardrailsConfig().tombstone_failure_threshold;
        int batch_size_fail_threshold_in_kb = DatabaseDescriptor.getGuardrailsConfig().batch_size_fail_threshold_in_kb;
        long columns_per_table_failure_threshold = DatabaseDescriptor.getGuardrailsConfig().columns_per_table_failure_threshold;
        long fields_per_udt_failure_threshold = DatabaseDescriptor.getGuardrailsConfig().fields_per_udt_failure_threshold;
        long collection_size_warn_threshold_in_kb = DatabaseDescriptor.getGuardrailsConfig().collection_size_warn_threshold_in_kb;
        long items_per_collection_warn_threshold = DatabaseDescriptor.getGuardrailsConfig().items_per_collection_warn_threshold;
        long tables_warn_threshold = DatabaseDescriptor.getGuardrailsConfig().tables_warn_threshold;
        long tables_failure_threshold = DatabaseDescriptor.getGuardrailsConfig().tables_failure_threshold;
        int in_select_cartesian_product_failure_threshold = DatabaseDescriptor.getGuardrailsConfig().in_select_cartesian_product_failure_threshold;
        int partition_keys_in_select_failure_threshold = DatabaseDescriptor.getGuardrailsConfig().partition_keys_in_select_failure_threshold;
        Set<String> write_consistency_levels_disallowed = DatabaseDescriptor.getGuardrailsConfig().write_consistency_levels_disallowed;
        try
        {
            System.setProperty(SSTableFormat.FORMAT_DEFAULT_PROP, SSTableFormat.Type.BIG.name);
            DatabaseDescriptor.getRawConfig().num_tokens = 17;
            DatabaseDescriptor.getGuardrailsConfig().tombstone_failure_threshold = 100001;
            DatabaseDescriptor.getGuardrailsConfig().batch_size_fail_threshold_in_kb = 641;
            DatabaseDescriptor.getGuardrailsConfig().columns_per_table_failure_threshold = 201L;
            DatabaseDescriptor.getGuardrailsConfig().fields_per_udt_failure_threshold = 101L;
            DatabaseDescriptor.getGuardrailsConfig().collection_size_warn_threshold_in_kb = 10481L;
            DatabaseDescriptor.getGuardrailsConfig().items_per_collection_warn_threshold = 201L;
            DatabaseDescriptor.getGuardrailsConfig().tables_warn_threshold = 101L;
            DatabaseDescriptor.getGuardrailsConfig().tables_failure_threshold = 201L;
            DatabaseDescriptor.getGuardrailsConfig().in_select_cartesian_product_failure_threshold = 26;
            DatabaseDescriptor.getGuardrailsConfig().partition_keys_in_select_failure_threshold = 21;
            DatabaseDescriptor.getGuardrailsConfig().write_consistency_levels_disallowed = Collections.emptySet();
            startupChecks = startupChecks.withTest(StartupChecks.checkYamlConfig);
            startupChecks.verify();
            assertWarningLogged("Not using murmur3 partitioner (org.apache.cassandra.dht.ByteOrderedPartitioner).");
            assertWarningLogged("num_tokens 17 too high. Values over 16 poorly impact repairs and node bootstrapping/decommissioning.");
            assertWarningLogged("Materialised Views should not be enabled.");
            assertWarningLogged("SASI should not be enabled, use SAI instead.");
            assertWarningLogged("Using `DROP COMPACT STORAGE` on tables should not be enabled.");
            assertWarningLogged("Guardrails value 100001 for tombstone_failure_threshold is too high (>100000).");
            assertWarningLogged("Guardrails value 641 for batch_size_fail_threshold_in_kb is too high (>640).");
            assertWarningLogged("Guardrails value 201 for columns_per_table_failure_threshold is too high (>200).");
            assertWarningLogged("Guardrails value 101 for fields_per_udt_failure_threshold is too high (>100).");
            assertWarningLogged("Guardrails value 10481 for collection_size_warn_threshold_in_kb is too high (>10480).");
            assertWarningLogged("Guardrails value 201 for items_per_collection_warn_threshold is too high (>200).");
            assertWarningLogged("Guardrails value 101 for tables_warn_threshold is too high (>100).");
            assertWarningLogged("Guardrails value 201 for tables_failure_threshold is too high (>200).");
            assertWarningLogged("Guardrails value 26 for in_select_cartesian_product_failure_threshold is too high (>25).");
            assertWarningLogged("Guardrails value 21 for partition_keys_in_select_failure_threshold is too high (>20).");
            assertWarningLogged("Guardrails value \"\" for write_consistency_levels_disallowed does not contain \"ANY\".");
            assertWarningLogged("Trie-based SSTables (bti) should always be the default (current is BIG).");
        }
        finally
        {
            if (null == sstableTypeProp)
                System.clearProperty(SSTableFormat.FORMAT_DEFAULT_PROP);
            else
                System.setProperty(SSTableFormat.FORMAT_DEFAULT_PROP, SSTableFormat.Type.BIG.name);

            DatabaseDescriptor.getRawConfig().num_tokens = tokens;
            DatabaseDescriptor.getGuardrailsConfig().tombstone_failure_threshold = tombstone_failure_threshold;
            DatabaseDescriptor.getGuardrailsConfig().batch_size_fail_threshold_in_kb = batch_size_fail_threshold_in_kb;
            DatabaseDescriptor.getGuardrailsConfig().columns_per_table_failure_threshold = columns_per_table_failure_threshold;
            DatabaseDescriptor.getGuardrailsConfig().fields_per_udt_failure_threshold = fields_per_udt_failure_threshold;
            DatabaseDescriptor.getGuardrailsConfig().collection_size_warn_threshold_in_kb = collection_size_warn_threshold_in_kb;
            DatabaseDescriptor.getGuardrailsConfig().items_per_collection_warn_threshold = items_per_collection_warn_threshold;
            DatabaseDescriptor.getGuardrailsConfig().tables_warn_threshold = tables_warn_threshold;
            DatabaseDescriptor.getGuardrailsConfig().tables_failure_threshold = tables_failure_threshold;
            DatabaseDescriptor.getGuardrailsConfig().in_select_cartesian_product_failure_threshold = in_select_cartesian_product_failure_threshold;
            DatabaseDescriptor.getGuardrailsConfig().partition_keys_in_select_failure_threshold = partition_keys_in_select_failure_threshold;
            DatabaseDescriptor.getGuardrailsConfig().write_consistency_levels_disallowed = write_consistency_levels_disallowed;
        }
    }

    @Test
    public void checkDataDirs() throws Exception
    {
        startupChecks = startupChecks.withTest(StartupChecks.checkDataDirs);
        startupChecks.verify();
    }

    @Test
    public void checkDataDirsWarn() throws Exception
    {
        startupChecks = startupChecks.withTest(StartupChecks.checkDataDirs);
        startupChecks.verify();
        File[] previous = DatabaseDescriptor.getNonLocalSystemKeyspacesDataFileLocations();
        try
        {
            DatabaseDescriptor.setDataDirectories(new File[]{previous[0], new File(Files.createTempDirectory("StartupChecksTest.checkDataDirsFail"))});
            startupChecks = startupChecks.withTest(StartupChecks.checkDataDirs);
            startupChecks.verify();
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
        startupChecks.verify();
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
            QueryProcessor.executeInternal("CREATE CUSTOM INDEX ON startupcheckstest.c2(v) USING 'org.apache.cassandra.index.sasi.SASIIndex'");
            QueryProcessor.executeInternal("CREATE TABLE startupcheckstest.c3(pk int PRIMARY KEY, v int) WITH COMPACT STORAGE");
            startupChecks = startupChecks.withTest(StartupChecks.checkTableSettings);
            startupChecks.verify();
            assertWarningLogged("The following tables using STCS and LCS should be altered to use UnifiedCompactionStrategy (UCS): startupcheckstest.c2");
            assertWarningLogged("The following tables with non-SAI indexes should be altered to use SAI: startupcheckstest.c2");
            assertWarningLogged("The following tables are `WITH COMPACT STORAGE` and need to be manually migrated to normal tables (Avoid using `DROP COMPACT STORAGE`): startupcheckstest.c3");
        }
        finally
        {
            DatabaseDescriptor.setPartitionerUnsafe(previous);
        }
    }

    private void copyLegacyNonSSTableFiles(Path targetDir) throws IOException
    {

        Path legacySSTableRoot = Paths.get(System.getProperty(INVALID_LEGACY_SSTABLE_ROOT_PROP),
                                          "Keyspace1",
                                          "Standard1");
        for (String filename : new String[]{"Keyspace1-Standard1-ic-0-TOC.txt",
                                            "Keyspace1-Standard1-ic-0-Digest.sha1",
                                            "legacyleveled.json"})
            Files.copy(Paths.get(legacySSTableRoot.toString(), filename), targetDir.resolve(filename));
    }

    private void copyInvalidLegacySSTables(Path targetDir) throws IOException
    {
        File legacySSTableRoot = new File(Paths.get(System.getProperty(INVALID_LEGACY_SSTABLE_ROOT_PROP),
                                           "Keyspace1",
                                           "Standard1"));
        for (File f : legacySSTableRoot.tryList())
            Files.copy(f.toPath(), targetDir.resolve(f.name()));

    }

    private void verifyFailure(StartupChecks tests, String message)
    {
        try
        {
            tests.verify();
            fail("Expected a startup exception but none was thrown");
        }
        catch (StartupException e)
        {
            assertTrue(e.getMessage().contains(message));
        }
    }


    private void assertWarningLogged(String expectedMessage)
    {
        boolean found = testAppender.list.stream()
                .anyMatch(event -> Level.WARN == event.getLevel() && event.getFormattedMessage().contains(expectedMessage));

        assertTrue("Expected warning not logged: " + expectedMessage, found);
    }

}
