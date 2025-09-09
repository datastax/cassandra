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
package org.apache.cassandra.io.sstable;


import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SinglePartitionSliceCommandTest;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.repair.PendingAntiCompaction;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.streaming.CassandraOutgoingFile;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.sstable.keycache.KeyCacheSupport;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileInputStreamPlus;
import org.apache.cassandra.io.util.FileOutputStreamPlus;
import org.apache.cassandra.service.CacheService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.OutgoingStream;
import org.apache.cassandra.streaming.StreamOperation;
import org.apache.cassandra.streaming.StreamPlan;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.OutputHandler;
import org.apache.cassandra.utils.TimeUUID;
import org.assertj.core.api.SoftAssertions;

import static java.util.Collections.singleton;
import static org.apache.cassandra.config.CassandraRelevantProperties.TEST_LEGACY_SSTABLE_ROOT;
import static org.apache.cassandra.io.sstable.format.AbstractTestVersionSupportedFeatures.ALL_VERSIONS;
import static org.apache.cassandra.service.ActiveRepairService.NO_PENDING_REPAIR;
import static org.apache.cassandra.service.ActiveRepairService.UNREPAIRED_SSTABLE;
import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests backwards compatibility for SSTables
 */
public class LegacySSTableTest
{
    private static final Logger logger = LoggerFactory.getLogger(LegacySSTableTest.class);

    public static File LEGACY_SSTABLE_ROOT;

    private static final String LEGACY_TABLES_KEYSPACE = "legacy_tables";

    /**
     * When adding a new sstable version, add that one here.
     * See {@link #testGenerateSstables()} to generate sstables.
     * Take care on commit as you need to add the sstable files using {@code git add -f}
     *
     * There are two me sstables, where the sequence number indicates the C* version they come from.
     * For example:
     *     me-3025-big-* sstables are generated from 3.0.25
     *     me-31111-big-* sstables are generated from 3.11.11
     * Both exist because of differences introduced in 3.6 (and 3.11) in how frozen multi-cell headers are serialised
     *  without the sstable format `me` being bumped, ref CASSANDRA-15035
     *
     * Sequence numbers represent the C* version used when creating the SSTable, i.e. with #testGenerateSstables()
     */
    public static String[] legacyVersions = null;

    // Get all versions up to the current one. Useful for testing in compatibility mode C18301
    private static String[] getValidLegacyVersions()
    {
        return ALL_VERSIONS.stream()
                           .filter(v -> DatabaseDescriptor.getSelectedSSTableFormat().getVersion(v).isCompatible())
                           .filter(v -> new File(LEGACY_SSTABLE_ROOT, v + "/" + LEGACY_TABLES_KEYSPACE).isDirectory())
                           .toArray(String[]::new);
    }

    // 1200 chars
    static final String longString = StringUtils.repeat("0123456789", 120);

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        String scp = TEST_LEGACY_SSTABLE_ROOT.getString();
        Assert.assertNotNull("System property " + TEST_LEGACY_SSTABLE_ROOT.getKey() + " not set", scp);

        LEGACY_SSTABLE_ROOT = new File(scp).toAbsolute();
        Assert.assertTrue("System property " + LEGACY_SSTABLE_ROOT + " does not specify a directory", LEGACY_SSTABLE_ROOT.isDirectory());

        SchemaLoader.prepareServer();
        StorageService.instance.initServer();
        Keyspace.setInitialized();
        createKeyspace();

        legacyVersions = getValidLegacyVersions();
        for (String legacyVersion : legacyVersions)
        {
            createTables(legacyVersion);
        }
    }

    @After
    public void tearDown()
    {
        for (String legacyVersion : legacyVersions)
        {
            truncateLegacyTables(legacyVersion);
        }
    }

    /**
     * Get a descriptor for the legacy sstable at the given version.
     */
    protected Descriptor getDescriptor(String legacyVersion, String table) throws IOException
    {
        Path file = Files.list(getTableDir(legacyVersion, table).toPath())
                .findFirst()
                .orElseThrow(() -> new RuntimeException(String.format("No files for verion=%s and table=%s", legacyVersion, table)));

        // ignore intentionally empty directory .keep files
        return ".keep".equals(file.toFile().getName()) ? null : Descriptor.fromFilename(new File(file));
    }

    @Test
    public void testLoadLegacyCqlTables()
    {
        DatabaseDescriptor.setColumnIndexCacheSizeInKiB(99999);
        CacheService.instance.invalidateKeyCache();
        doTestLegacyCqlTables();
    }

    @Test
    public void testLoadLegacyCqlTablesShallow()
    {
        DatabaseDescriptor.setColumnIndexCacheSizeInKiB(0);
        CacheService.instance.invalidateKeyCache();
        doTestLegacyCqlTables();
    }

    @Test
    public void testMutateMetadata()
    {
        SoftAssertions assertions = new SoftAssertions();
        // we need to make sure we write old version metadata in the format for that version
        for (String legacyVersion : legacyVersions)
            assertions.assertThatCode(() -> {
                logger.info("Loading legacy version: {}", legacyVersion);
                truncateLegacyTables(legacyVersion);
                loadLegacyTables(legacyVersion);
                CacheService.instance.invalidateKeyCache();

                for (ColumnFamilyStore cfs : Keyspace.open(LEGACY_TABLES_KEYSPACE).getColumnFamilyStores())
                {
                    for (SSTableReader sstable : cfs.getLiveSSTables())
                    {
                        sstable.descriptor.getMetadataSerializer().mutateRepairMetadata(sstable.descriptor, 1234, NO_PENDING_REPAIR, false);
                        sstable.reloadSSTableMetadata();
                        assertEquals(1234, sstable.getRepairedAt());
                        if (sstable.descriptor.version.hasPendingRepair())
                            assertEquals(NO_PENDING_REPAIR, sstable.getPendingRepair());
                    }

                    boolean isTransient = false;
                    for (SSTableReader sstable : cfs.getLiveSSTables())
                    {
                        TimeUUID random = nextTimeUUID();
                        sstable.descriptor.getMetadataSerializer().mutateRepairMetadata(sstable.descriptor, UNREPAIRED_SSTABLE, random, isTransient);
                        sstable.reloadSSTableMetadata();
                        assertEquals(UNREPAIRED_SSTABLE, sstable.getRepairedAt());
                        if (sstable.descriptor.version.hasPendingRepair())
                            assertEquals(random, sstable.getPendingRepair());
                        if (sstable.descriptor.version.hasIsTransient())
                            assertEquals(isTransient, sstable.isTransient());

                        isTransient = !isTransient;
                    }
                }
            }).describedAs(legacyVersion).doesNotThrowAnyException();
        assertions.assertAll();
    }

    @Test
    public void testMutateMetadataCSM()
    {
        SoftAssertions assertions = new SoftAssertions();
        // we need to make sure we write old version metadata in the format for that version
        for (String legacyVersion : legacyVersions)
            assertions.assertThatCode(() -> {
                truncateLegacyTables(legacyVersion);
                loadLegacyTables(legacyVersion);

                for (ColumnFamilyStore cfs : Keyspace.open(LEGACY_TABLES_KEYSPACE).getColumnFamilyStores())
                {
                    // set pending
                    for (SSTableReader sstable : cfs.getLiveSSTables())
                    {
                        TimeUUID random = nextTimeUUID();
                        try
                        {
                            cfs.mutateRepaired(Collections.singleton(sstable), UNREPAIRED_SSTABLE, random, false);
                            if (!sstable.descriptor.version.hasPendingRepair())
                                fail("We should fail setting pending repair on unsupported sstables " + sstable);
                        }
                        catch (IllegalStateException e)
                        {
                            if (sstable.descriptor.version.hasPendingRepair())
                                fail("We should succeed setting pending repair on " + legacyVersion + " sstables, failed on " + sstable);
                        }
                    }
                    // set transient
                    for (SSTableReader sstable : cfs.getLiveSSTables())
                    {
                        try
                        {
                            cfs.mutateRepaired(Collections.singleton(sstable), UNREPAIRED_SSTABLE, nextTimeUUID(), true);
                            if (!sstable.descriptor.version.hasIsTransient())
                                fail("We should fail setting pending repair on unsupported sstables " + sstable);
                        }
                        catch (IllegalStateException e)
                        {
                            if (sstable.descriptor.version.hasIsTransient())
                                fail("We should succeed setting pending repair on " + legacyVersion + " sstables, failed on " + sstable);
                        }
                    }
                }
            }).describedAs(legacyVersion).doesNotThrowAnyException();
        assertions.assertAll();
    }

    @Test
    public void testMutateLevel()
    {
        // we need to make sure we write old version metadata in the format for that version
        SoftAssertions assertions = new SoftAssertions();
        for (String legacyVersion : legacyVersions)
            assertions.assertThatCode(() -> {
                logger.info("Loading legacy version: {}", legacyVersion);
                truncateLegacyTables(legacyVersion);
                loadLegacyTables(legacyVersion);
                CacheService.instance.invalidateKeyCache();

                for (ColumnFamilyStore cfs : Keyspace.open(LEGACY_TABLES_KEYSPACE).getColumnFamilyStores())
                {
                    for (SSTableReader sstable : cfs.getLiveSSTables())
                    {
                        sstable.descriptor.getMetadataSerializer().mutateLevel(sstable.descriptor, 1234);
                        sstable.reloadSSTableMetadata();
                        assertEquals(1234, sstable.getSSTableLevel());
                    }
                }
            }).describedAs(legacyVersion).doesNotThrowAnyException();
        assertions.assertAll();
    }

    private void doTestLegacyCqlTables()
    {
        for (String legacyVersion : legacyVersions)
        {
            if ('m' <= legacyVersion.charAt(0))
            {
                logger.info("Loading legacy version: {}", legacyVersion);
                truncateLegacyTables(legacyVersion);
                loadLegacyTables(legacyVersion);
                CacheService.instance.invalidateKeyCache();
                long startCount = CacheService.instance.keyCache.size();
                verifyReads(legacyVersion);
                if (Keyspace.open(LEGACY_TABLES_KEYSPACE).getColumnFamilyStore(String.format("legacy_%s_simple", legacyVersion)).getLiveSSTables().stream().anyMatch(sstr -> BigFormat.is(sstr.descriptor.getFormat())))
                    verifyCache(legacyVersion, startCount);
                compactLegacyTables(legacyVersion);
            }
        }
    }

    @Test
    public void testStreamLegacyCqlTables()
    {
        SoftAssertions assertions = new SoftAssertions();
        for (String legacyVersion : legacyVersions)
            assertions.assertThatCode(() -> {
                streamLegacyTables(legacyVersion);
                verifyReads(legacyVersion);
            }).describedAs(legacyVersion).doesNotThrowAnyException();
        assertions.assertAll();
    }

    @Test
    public void testInaccurateSSTableMinMax()
    {
        QueryProcessor.executeInternal("CREATE TABLE legacy_tables.legacy_mc_inaccurate_min_max (k int, c1 int, c2 int, c3 int, v int, primary key (k, c1, c2, c3))");
        loadLegacyTable("mc", "inaccurate_min_max");

        /*
         sstable has the following mutations:
            INSERT INTO legacy_tables.legacy_mc_inaccurate_min_max (k, c1, c2, c3, v) VALUES (100, 4, 4, 4, 4)
            DELETE FROM legacy_tables.legacy_mc_inaccurate_min_max WHERE k=100 AND c1<3
         */

        String query = "SELECT * FROM legacy_tables.legacy_mc_inaccurate_min_max WHERE k=100 AND c1=1 AND c2=1";
        List<Unfiltered> unfiltereds = SinglePartitionSliceCommandTest.getUnfilteredsFromSinglePartition(query);
        Assert.assertEquals(2, unfiltereds.size());
        Assert.assertTrue(unfiltereds.get(0).isRangeTombstoneMarker());
        Assert.assertTrue(((RangeTombstoneMarker) unfiltereds.get(0)).isOpen(false));
        Assert.assertTrue(unfiltereds.get(1).isRangeTombstoneMarker());
        Assert.assertTrue(((RangeTombstoneMarker) unfiltereds.get(1)).isClose(false));
    }

    @Test
    public void testVerifyOldSimpleSSTables()
    {
        verifyOldSSTables("simple");
    }

    @Test
    public void testVerifyOldTupleSSTables()
    {
        verifyOldSSTables("tuple");
    }

    @Test
    public void testVerifyOldDroppedTupleSSTables()
    {
        try {
            for (String legacyVersion : legacyVersions)
            {
                QueryProcessor.executeInternal(String.format("ALTER TABLE legacy_tables.legacy_%s_tuple DROP val", legacyVersion));
                QueryProcessor.executeInternal(String.format("ALTER TABLE legacy_tables.legacy_%s_tuple DROP val2", legacyVersion));
                QueryProcessor.executeInternal(String.format("ALTER TABLE legacy_tables.legacy_%s_tuple DROP val3", legacyVersion));
                // dropping non-frozen UDTs disabled, see AlterTableStatement.DropColumns.dropColumn(..)
                //QueryProcessor.executeInternal(String.format("ALTER TABLE legacy_tables.legacy_%s_tuple DROP val4", legacyVersion));
            }

            verifyOldSSTables("tuple");
        }
        finally
        {
            for (String legacyVersion : legacyVersions)
            {
                alterTableAddColumn(legacyVersion, "val frozen<tuple<set<int>,set<text>>>");
                alterTableAddColumn(legacyVersion, "val2 tuple<set<int>,set<text>>");
                alterTableAddColumn(legacyVersion, String.format("val3 frozen<legacy_%s_tuple_udt>", legacyVersion));
                // dropping non-frozen UDTs disabled, see AlterTableStatement.DropColumns.dropColumn(..)
                //alterTableAddColumn(legacyVersion, String.format("val4 legacy_%s_tuple_udt", legacyVersion));
            }
        }
    }

    private static void alterTableAddColumn(String legacyVersion, String column_definition)
    {
        QueryProcessor.executeInternal(String.format("ALTER TABLE legacy_tables.legacy_%s_tuple ADD %s", legacyVersion, column_definition));
    }

    private void verifyOldSSTables(String tableSuffix)
    {
        SoftAssertions assertions = new SoftAssertions();
        for (String legacyVersion : legacyVersions)
            assertions.assertThatCode(() -> {
                ColumnFamilyStore cfs = Keyspace.open(LEGACY_TABLES_KEYSPACE).getColumnFamilyStore(String.format("legacy_%s_%s", legacyVersion, tableSuffix));
                loadLegacyTable(legacyVersion, tableSuffix);

                for (SSTableReader sstable : cfs.getLiveSSTables())
                {
                    try (IVerifier verifier = sstable.getVerifier(cfs, new OutputHandler.LogOutput(), false, IVerifier.options().checkVersion(true).build()))
                    {
                        verifier.verify();
                        if (!sstable.descriptor.version.isLatestVersion())
                            fail("Verify should throw RuntimeException for old sstables " + sstable);
                    }
                    catch (RuntimeException e)
                    {
                    }
                }
                // make sure we don't throw any exception if not checking version:
                for (SSTableReader sstable : cfs.getLiveSSTables())
                {
                    try (IVerifier verifier = sstable.getVerifier(cfs, new OutputHandler.LogOutput(), false, IVerifier.options().checkVersion(false).build()))
                    {
                        verifier.verify();
                    }
                    catch (Throwable e)
                    {
                        fail("Verify should throw RuntimeException for old sstables " + sstable);
                    }
                }
            }).describedAs(legacyVersion).doesNotThrowAnyException();
        assertions.assertAll();
    }

    @Test
    public void testPendingAntiCompactionOldSSTables()
    {
        SoftAssertions assertions = new SoftAssertions();
        for (String legacyVersion : legacyVersions)
            assertions.assertThatCode(() -> {
                ColumnFamilyStore cfs = Keyspace.open(LEGACY_TABLES_KEYSPACE).getColumnFamilyStore(String.format("legacy_%s_simple", legacyVersion));
                loadLegacyTable(legacyVersion, "simple");

                boolean shouldFail = !cfs.getLiveSSTables().stream().allMatch(sstable -> sstable.descriptor.version.hasPendingRepair());
                IPartitioner p = Iterables.getFirst(cfs.getLiveSSTables(), null).getPartitioner();
                Range<Token> r = new Range<>(p.getMinimumToken(), p.getMinimumToken());
                PendingAntiCompaction.AcquisitionCallable acquisitionCallable = new PendingAntiCompaction.AcquisitionCallable(cfs, singleton(r), nextTimeUUID(), 0, 0);
                PendingAntiCompaction.AcquireResult res = acquisitionCallable.call();
                assertEquals(shouldFail, res == null);
                if (res != null)
                    res.abort();
            }).describedAs(legacyVersion).doesNotThrowAnyException();
        assertions.assertAll();
    }

    @Test
    public void testAutomaticUpgrade()
    {
        SoftAssertions assertions = new SoftAssertions();
        for (String legacyVersion : legacyVersions)
            assertions.assertThatCode(() -> {
                logger.info("Loading legacy version: {}", legacyVersion);
                truncateLegacyTables(legacyVersion);
                loadLegacyTables(legacyVersion);
                ColumnFamilyStore cfs = Keyspace.open(LEGACY_TABLES_KEYSPACE).getColumnFamilyStore(String.format("legacy_%s_simple", legacyVersion));
                // there should be no compactions to run with auto upgrades disabled:
                assertTrue(cfs.getCompactionStrategy().getNextBackgroundTasks(0).isEmpty());
            }).describedAs(legacyVersion).doesNotThrowAnyException();
        assertions.assertAll();

        DatabaseDescriptor.setAutomaticSSTableUpgradeEnabled(true);
        for (String legacyVersion : legacyVersions)
            assertions.assertThatCode(() -> {
                logger.info("Loading legacy version: {}", legacyVersion);
                truncateLegacyTables(legacyVersion);
                loadLegacyTables(legacyVersion);
                ColumnFamilyStore cfs = Keyspace.open(LEGACY_TABLES_KEYSPACE).getColumnFamilyStore(String.format("legacy_%s_simple", legacyVersion));
                if (cfs.getLiveSSTables().stream().anyMatch(s -> !s.descriptor.version.isLatestVersion()))
                    assertTrue(cfs.metric.oldVersionSSTableCount.getValue() > 0);
                while (cfs.getLiveSSTables().stream().anyMatch(s -> !s.descriptor.version.isLatestVersion()))
                {
                    CompactionManager.instance.submitBackground(cfs);
                    Thread.sleep(100);
                }
                assertEquals(0, (int) cfs.metric.oldVersionSSTableCount.getValue());
            }).describedAs(legacyVersion).doesNotThrowAnyException();
        DatabaseDescriptor.setAutomaticSSTableUpgradeEnabled(false);
        assertions.assertAll();
    }

    private void streamLegacyTables(String legacyVersion) throws Exception
    {
        logger.info("Streaming legacy version {}", legacyVersion);
        streamLegacyTable("legacy_%s_simple", legacyVersion);
        streamLegacyTable("legacy_%s_simple_counter", legacyVersion);
        streamLegacyTable("legacy_%s_clust", legacyVersion);
        streamLegacyTable("legacy_%s_clust_counter", legacyVersion);
        streamLegacyTable("legacy_%s_tuple", legacyVersion);
    }

    private void streamLegacyTable(String tablePattern, String legacyVersion) throws Exception
    {
        String table = String.format(tablePattern, legacyVersion);
        Descriptor descriptor = getDescriptor(legacyVersion, table);
        if (null != descriptor)
        {
            SSTableReader sstable = SSTableReader.open(null, descriptor);
            IPartitioner p = sstable.getPartitioner();
            List<Range<Token>> ranges = new ArrayList<>();
            ranges.add(new Range<>(p.getMinimumToken(), p.getToken(ByteBufferUtil.bytes("100"))));
            ranges.add(new Range<>(p.getToken(ByteBufferUtil.bytes("100")), p.getMinimumToken()));

            List<OutgoingStream> streams = Lists.newArrayList(new CassandraOutgoingFile(StreamOperation.OTHER,
                    sstable.ref(),
                    sstable.getPositionsForRanges(ranges),
                    ranges,
                    sstable.estimatedKeysForRanges(ranges)));

            new StreamPlan(StreamOperation.OTHER).transferStreams(FBUtilities.getBroadcastAddressAndPort(), streams).execute().get();
        }
    }

    public static void truncateLegacyTables(String legacyVersion)
    {
        logger.info("Truncating legacy version {}", legacyVersion);
        Keyspace.open(LEGACY_TABLES_KEYSPACE).getColumnFamilyStore(String.format("legacy_%s_simple", legacyVersion)).truncateBlocking();
        Keyspace.open(LEGACY_TABLES_KEYSPACE).getColumnFamilyStore(String.format("legacy_%s_simple_counter", legacyVersion)).truncateBlocking();
        Keyspace.open(LEGACY_TABLES_KEYSPACE).getColumnFamilyStore(String.format("legacy_%s_clust", legacyVersion)).truncateBlocking();
        Keyspace.open(LEGACY_TABLES_KEYSPACE).getColumnFamilyStore(String.format("legacy_%s_clust_counter", legacyVersion)).truncateBlocking();
        Keyspace.open(LEGACY_TABLES_KEYSPACE).getColumnFamilyStore(String.format("legacy_%s_tuple", legacyVersion)).truncateBlocking();
        CacheService.instance.invalidateCounterCache();
        CacheService.instance.invalidateKeyCache();
    }

    private static void compactLegacyTables(String legacyVersion)
    {
        logger.info("Compacting legacy version {}", legacyVersion);
        Keyspace.open(LEGACY_TABLES_KEYSPACE).getColumnFamilyStore(String.format("legacy_%s_simple", legacyVersion)).forceMajorCompaction();
        Keyspace.open(LEGACY_TABLES_KEYSPACE).getColumnFamilyStore(String.format("legacy_%s_simple_counter", legacyVersion)).forceMajorCompaction();
        Keyspace.open(LEGACY_TABLES_KEYSPACE).getColumnFamilyStore(String.format("legacy_%s_clust", legacyVersion)).forceMajorCompaction();
        Keyspace.open(LEGACY_TABLES_KEYSPACE).getColumnFamilyStore(String.format("legacy_%s_clust_counter", legacyVersion)).forceMajorCompaction();
        Keyspace.open(LEGACY_TABLES_KEYSPACE).getColumnFamilyStore(String.format("legacy_%s_tuple", legacyVersion)).forceMajorCompaction();
    }

    public static void loadLegacyTables(String legacyVersion)
    {
        logger.info("Preparing legacy version {}", legacyVersion);
        loadLegacyTable(legacyVersion, "simple");
        loadLegacyTable(legacyVersion, "simple_counter");
        loadLegacyTable(legacyVersion, "clust");
        loadLegacyTable(legacyVersion, "clust_counter");
        loadLegacyTable(legacyVersion, "tuple");
    }

    private static void verifyCache(String legacyVersion, long startCount)
    {
        // Only perform test if format uses cache.
        SSTableReader sstable = Iterables.getFirst(Keyspace.open("legacy_tables").getColumnFamilyStore(String.format("legacy_%s_simple", legacyVersion)).getLiveSSTables(), null);
        if (!(sstable instanceof KeyCacheSupport) || DatabaseDescriptor.getKeyCacheSizeInMiB() == 0)
            return;

        //For https://issues.apache.org/jira/browse/CASSANDRA-10778
        //Validate whether the key cache successfully saves in the presence of old keys as
        //well as loads the correct number of keys
        long endCount = CacheService.instance.keyCache.size();
        Assert.assertTrue(endCount > startCount);
        try
        {
            CacheService.instance.keyCache.submitWrite(Integer.MAX_VALUE).get(60, TimeUnit.MINUTES);
        }
        catch (Exception e)
        {
            throw new AssertionError(e);
        }
        CacheService.instance.invalidateKeyCache();
        Assert.assertEquals(startCount, CacheService.instance.keyCache.size());
        CacheService.instance.keyCache.loadSaved();
        Assert.assertEquals(endCount, CacheService.instance.keyCache.size());
    }

    private static void verifyReads(String legacyVersion)
    {
        for (int ck = 0; ck < 50; ck++)
        {
            String ckValue = ck + longString;
            for (int pk = 0; pk < 5; pk++)
            {
                logger.debug("for pk={} ck={}", pk, ck);

                String pkValue = Integer.toString(pk);
                if (ck == 0)
                {
                    readSimpleTable(legacyVersion, pkValue);
                    readSimpleCounterTable(legacyVersion, pkValue);
                }

                readClusteringTable(legacyVersion, ck, ckValue, pkValue);
                readClusteringCounterTable(legacyVersion, ckValue, pkValue);
            }
        }
    }

    private static void readClusteringCounterTable(String legacyVersion, String ckValue, String pkValue)
    {
        logger.debug("Read legacy_{}_clust_counter", legacyVersion);
        UntypedResultSet rs;
        rs = QueryProcessor.executeInternal(String.format("SELECT val FROM legacy_tables.legacy_%s_clust_counter WHERE pk=? AND ck=?", legacyVersion), pkValue, ckValue);
        Assert.assertNotNull(rs);
        Assert.assertEquals(String.format("Read legacy_%s_clust_counter", legacyVersion), 1, rs.size());
        Assert.assertEquals(String.format("Read legacy_%s_clust_counter", legacyVersion), 1L, rs.one().getLong("val"));
    }

    private static void readClusteringTable(String legacyVersion, int ck, String ckValue, String pkValue)
    {
        logger.debug("Read legacy_{}_clust", legacyVersion);
        UntypedResultSet rs;
        rs = QueryProcessor.executeInternal(String.format("SELECT val FROM legacy_tables.legacy_%s_clust WHERE pk=? AND ck=?", legacyVersion), pkValue, ckValue);
        assertLegacyClustRows(1, rs);

        String ckValue2 = (ck < 10 ? 40 : ck - 1) + longString;
        String ckValue3 = (ck > 39 ? 10 : ck + 1) + longString;
        rs = QueryProcessor.executeInternal(String.format("SELECT val FROM legacy_tables.legacy_%s_clust WHERE pk=? AND ck IN (?, ?, ?)", legacyVersion), pkValue, ckValue, ckValue2, ckValue3);
        assertLegacyClustRows(3, rs);
    }

    private static void readSimpleCounterTable(String legacyVersion, String pkValue)
    {
        logger.debug("Read legacy_{}_simple_counter", legacyVersion);
        UntypedResultSet rs;
        rs = QueryProcessor.executeInternal(String.format("SELECT val FROM legacy_tables.legacy_%s_simple_counter WHERE pk=?", legacyVersion), pkValue);
        Assert.assertNotNull(rs);
        Assert.assertEquals(1, rs.size());
        Assert.assertEquals(1L, rs.one().getLong("val"));
    }

    private static void readSimpleTable(String legacyVersion, String pkValue)
    {
        logger.debug("Read simple: legacy_{}_simple", legacyVersion);
        UntypedResultSet rs;
        rs = QueryProcessor.executeInternal(String.format("SELECT val FROM legacy_tables.legacy_%s_simple WHERE pk=?", legacyVersion), pkValue);
        Assert.assertNotNull(rs);
        Assert.assertEquals(1, rs.size());
        Assert.assertEquals("foo bar baz", rs.one().getString("val"));
    }

    private static void createKeyspace()
    {
        QueryProcessor.executeInternal("CREATE KEYSPACE legacy_tables WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}");
    }

    private static void createTables(String legacyVersion)
    {
        QueryProcessor.executeInternal(String.format("CREATE TABLE legacy_tables.legacy_%s_simple (pk text PRIMARY KEY, val text)", legacyVersion));
        QueryProcessor.executeInternal(String.format("CREATE TABLE legacy_tables.legacy_%s_simple_counter (pk text PRIMARY KEY, val counter)", legacyVersion));
        QueryProcessor.executeInternal(String.format("CREATE TABLE legacy_tables.legacy_%s_clust (pk text, ck text, val text, PRIMARY KEY (pk, ck))", legacyVersion));
        QueryProcessor.executeInternal(String.format("CREATE TABLE legacy_tables.legacy_%s_clust_counter (pk text, ck text, val counter, PRIMARY KEY (pk, ck))", legacyVersion));


        QueryProcessor.executeInternal(String.format("CREATE TYPE legacy_tables.legacy_%s_tuple_udt (name tuple<text,text>)", legacyVersion));

        if (legacyVersion.startsWith("m"))
        {
            // sstable formats possibly from 3.0.x would have had a schema with everything frozen
            QueryProcessor.executeInternal(String.format("CREATE TABLE legacy_tables.legacy_%1$s_tuple (pk text PRIMARY KEY, " +
                    "val frozen<tuple<set<int>,set<text>>>, val2 frozen<tuple<set<int>,set<text>>>, val3 frozen<legacy_%1$s_tuple_udt>, val4 frozen<legacy_%1$s_tuple_udt>, extra text)", legacyVersion));
        }
        else
        {
            QueryProcessor.executeInternal(String.format("CREATE TABLE legacy_tables.legacy_%1$s_tuple (pk text PRIMARY KEY, " +
                "val frozen<tuple<set<int>,set<text>>>, val2 tuple<set<int>,set<text>>, val3 frozen<legacy_%1$s_tuple_udt>, val4 legacy_%1$s_tuple_udt, extra text)", legacyVersion));
        }
    }

    private static void assertLegacyClustRows(int count, UntypedResultSet rs)
    {
        Assert.assertNotNull(rs);
        Assert.assertEquals(count, rs.size());
        for (int i = 0; i < count; i++)
        {
            for (UntypedResultSet.Row r : rs)
            {
                Assert.assertEquals(128, r.getString("val").length());
            }
        }
    }

    private static void loadLegacyTable(String legacyVersion, String tableSuffix)
    {
        String table = String.format("legacy_%s_%s", legacyVersion, tableSuffix);

        // ignore if no sstables are in this legacyVersion directory
        getTableDir(legacyVersion, table).forEach(f -> logger.info(f.toString()));
        if (0 == getTableDir(legacyVersion, table).tryList(f -> f.name().endsWith(".db")).length)
            return;

        logger.info("Loading legacy table {}", table);

        ColumnFamilyStore cfs = Keyspace.open(LEGACY_TABLES_KEYSPACE).getColumnFamilyStore(table);

        for (File cfDir : cfs.getDirectories().getCFDirectories())
        {
            try
            {
                copySstablesToTestData(legacyVersion, table, cfDir);
            }
            catch (IOException e)
            {
                throw new AssertionError(e);
            }
        }

        if (legacyVersion.startsWith("m") && legacyVersion.compareTo("me") <= 0)
        {
            // sstables <= me are potentially broken, pretend offline upgrade where the user ran the scrub's header fix
            FBUtilities.setPreviousReleaseVersionString("3.0.25");
            SSTableHeaderFix.fixNonFrozenUDTIfUpgradeFrom30();
        }

        int s0 = cfs.getLiveSSTables().size();
        cfs.loadNewSSTables();
        int s1 = cfs.getLiveSSTables().size();
        assertThat(s1).isGreaterThan(s0);
    }

    /**
     * Generates sstables for CQL tables (see {@link #createTables(String)}) in <i>current</i>
     * sstable format (version) into {@code test/data/legacy-sstables/VERSION}, where
     * {@code VERSION} matches {@link Version#version BigFormat.latestVersion.getVersion()}.
     *
     * Sequence numbers are changed to represent the C* version used when creating the SSTable.
     * <p>
     * Run this test alone (e.g. from your IDE) when a new version is introduced or format changed
     * during development. I.e. remove the {@code @Ignore} annotation temporarily.
     * </p>
     */
    @Ignore // TODO: Currently this test needs to be ran alone to avoid unwanted compactions, flushes, etc to interfere
    @Test
    public void testGenerateSstables() throws Throwable
    {
        SSTableFormat<?, ?> format = DatabaseDescriptor.getSelectedSSTableFormat();
        Random rand = new Random();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 128; i++)
        {
            sb.append((char)('a' + rand.nextInt(26)));
        }
        String randomString = sb.toString();

        for (int pk = 0; pk < 5; pk++)
        {
            String valPk = Integer.toString(pk);
            QueryProcessor.executeInternal(String.format("INSERT INTO legacy_tables.legacy_%s_simple (pk, val) VALUES ('%s', '%s')",
                                                         format.getLatestVersion(), valPk, "foo bar baz"));

            QueryProcessor.executeInternal(String.format("UPDATE legacy_tables.legacy_%s_simple_counter SET val = val + 1 WHERE pk = '%s'",
                                                         format.getLatestVersion(), valPk));

            QueryProcessor.executeInternal(
                    String.format("INSERT INTO legacy_tables.legacy_%s_tuple (pk, val, val2, val3, val4, extra)"
                                    + " VALUES ('%s', ({1,2,3},{'a','b','c'}), ({1,2,3},{'a','b','c'}), {name: ('abc','def')}, {name: ('abc','def')}, '%s')",
                                  format.getLatestVersion(), valPk, randomString));

            for (int ck = 0; ck < 50; ck++)
            {
                String valCk = Integer.toString(ck);

                QueryProcessor.executeInternal(String.format("INSERT INTO legacy_tables.legacy_%s_clust (pk, ck, val) VALUES ('%s', '%s', '%s')",
                                                             format.getLatestVersion(), valPk, valCk + longString, randomString));

                QueryProcessor.executeInternal(String.format("UPDATE legacy_tables.legacy_%s_clust_counter SET val = val + 1 WHERE pk = '%s' AND ck='%s'",
                                                             format.getLatestVersion(), valPk, valCk + longString));
            }
        }

        StorageService.instance.forceKeyspaceFlush(LEGACY_TABLES_KEYSPACE, ColumnFamilyStore.FlushReason.UNIT_TESTS);

        File ksDir = new File(LEGACY_SSTABLE_ROOT, String.format("%s/legacy_tables", format.getLatestVersion()));
        ksDir.tryCreateDirectories();
        copySstablesFromTestData(format.getLatestVersion(), "legacy_%s_simple", ksDir);
        copySstablesFromTestData(format.getLatestVersion(), "legacy_%s_simple_counter", ksDir);
        copySstablesFromTestData(format.getLatestVersion(), "legacy_%s_clust", ksDir);
        copySstablesFromTestData(format.getLatestVersion(), "legacy_%s_clust_counter", ksDir);
        copySstablesFromTestData(format.getLatestVersion(), "legacy_%s_tuple", ksDir);
    }

    public static void copySstablesFromTestData(Version legacyVersion, String tablePattern, File ksDir) throws IOException
    {
        copySstablesFromTestData(legacyVersion, tablePattern, ksDir, LEGACY_TABLES_KEYSPACE);
    }

    public static void copySstablesFromTestData(Version legacyVersion, String tablePattern, File ksDir, String ks) throws IOException
    {
        String table = String.format(tablePattern, legacyVersion);
        File cfDir = new File(ksDir, table);
        cfDir.tryCreateDirectory();

        for (File srcDir : Keyspace.open(ks).getColumnFamilyStore(table).getDirectories().getCFDirectories())
        {
            for (File file : srcDir.tryList())
            {
                // Sequence IDs represent the C* version used when creating the SSTable, i.e. with #testGenerateSstables() (if not uuid based)
                String newSeqId = FBUtilities.getReleaseVersionString().split("-")[0].replaceAll("[^0-9]", "");
                File target = new File(cfDir, file.name().replace(legacyVersion + "-1-", legacyVersion + "-" + newSeqId + "-"));
                copyFile(cfDir, file, target);
            }
        }
    }

    private static void copySstablesToTestData(String legacyVersion, String table, File cfDir) throws IOException
    {
        File tableDir = getTableDir(legacyVersion, table);
        Assert.assertTrue("The table directory " + tableDir + " was not found", tableDir.isDirectory());
        for (File file : tableDir.tryList())
            copyFile(cfDir, file);
    }

    private static File getTableDir(String legacyVersion, String table)
    {
        return new File(LEGACY_SSTABLE_ROOT, String.format("%s/legacy_tables/%s", legacyVersion, table));
    }

    public static void copyFile(File cfDir, File file) throws IOException
    {
        copyFile(cfDir, file,  new File(cfDir, file.name()));
    }

    public static void copyFile(File cfDir, File file, File target) throws IOException
    {
        byte[] buf = new byte[65536];
        if (file.isFile())
        {
            int rd;
            try (FileInputStreamPlus is = new FileInputStreamPlus(file);
                 FileOutputStreamPlus os = new FileOutputStreamPlus(target);)
            {
                while ((rd = is.read(buf)) >= 0)
                    os.write(buf, 0, rd);
            }
        }
    }
}
