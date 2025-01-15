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
import java.util.UUID;
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
import org.apache.cassandra.db.compaction.Verifier;
import org.apache.cassandra.db.repair.PendingAntiCompaction;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.streaming.CassandraOutgoingFile;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.sstable.format.trieindex.TrieIndexFormat;
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
import org.apache.cassandra.utils.UUIDGen;
import org.quicktheories.generators.SourceDSL;

import static org.apache.cassandra.service.ActiveRepairService.NO_PENDING_REPAIR;
import static org.apache.cassandra.service.ActiveRepairService.UNREPAIRED_SSTABLE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.quicktheories.QuickTheory.qt;

/**
 * Tests backwards compatibility for SSTables
 */
public class LegacySSTableTest
{
    private static final Logger logger = LoggerFactory.getLogger(LegacySSTableTest.class);

    public static final String LEGACY_SSTABLE_PROP = "legacy-sstable-root";

    public static File LEGACY_SSTABLE_ROOT;

    /**
     * When adding a new sstable version, add that one here.
     * See {@link #testGenerateSstables()} to generate sstables.
     * Take care on commit as you need to add the sstable files using {@code git add -f}
     */
    public static final String[] legacyVersions = {"me", "na", "nb", "nc", "aa", "ac", "ad", "ba", "bb", "ca", "cb", "cc"};

    // 1200 chars
    static final String longString = StringUtils.repeat("0123456789", 120);

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        String scp = System.getProperty(LEGACY_SSTABLE_PROP);
        Assert.assertNotNull("System property " + LEGACY_SSTABLE_PROP + " not set", scp);

        LEGACY_SSTABLE_ROOT = new File(scp).toAbsolute();
        Assert.assertTrue("System property " + LEGACY_SSTABLE_ROOT + " does not specify a directory", LEGACY_SSTABLE_ROOT.isDirectory());

        SchemaLoader.prepareServer();
        StorageService.instance.initServer();
        Keyspace.setInitialized();
        createKeyspace();
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
            truncateTables(legacyVersion);
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
    public void testLoadLegacyCqlTables() throws Exception
    {
        DatabaseDescriptor.setColumnIndexCacheSizeInKB(99999);
        CacheService.instance.invalidateKeyCache();
        doTestLegacyCqlTables();
    }

    @Test
    public void testLoadLegacyCqlTablesShallow() throws Exception
    {
        DatabaseDescriptor.setColumnIndexCacheSizeInKB(0);
        CacheService.instance.invalidateKeyCache();
        doTestLegacyCqlTables();
    }

    @Test
    public void testMutateMetadata() throws Exception
    {
        // we need to make sure we write old version metadata in the format for that version
        for (String legacyVersion : legacyVersions)
        {
            logger.info("Loading legacy version: {}", legacyVersion);
            truncateLegacyTables(legacyVersion);
            loadLegacyTables(legacyVersion);
            CacheService.instance.invalidateKeyCache();

            for (ColumnFamilyStore cfs : Keyspace.open("legacy_tables").getColumnFamilyStores())
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
                    UUID random = UUID.randomUUID();
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
        }
    }

    @Test
    public void testMutateMetadataCSM() throws Exception
    {
        // we need to make sure we write old version metadata in the format for that version
        for (String legacyVersion : legacyVersions)
        {
            // Skip 2.0.1 sstables as it doesn't have repaired information
            if (legacyVersion.equals("jb"))
                continue;
            truncateTables(legacyVersion);
            loadLegacyTables(legacyVersion);

            for (ColumnFamilyStore cfs : Keyspace.open("legacy_tables").getColumnFamilyStores())
            {
                // set pending
                for (SSTableReader sstable : cfs.getLiveSSTables())
                {
                    UUID random = UUID.randomUUID();
                    try
                    {
                        cfs.mutateRepaired(Collections.singleton(sstable), UNREPAIRED_SSTABLE, random, false);
                        if (!sstable.descriptor.version.hasPendingRepair())
                            fail("We should fail setting pending repair on unsupported sstables "+sstable);
                    }
                    catch (IllegalStateException e)
                    {
                        if (sstable.descriptor.version.hasPendingRepair())
                            fail("We should succeed setting pending repair on "+legacyVersion + " sstables, failed on "+sstable);
                    }
                }
                // set transient
                for (SSTableReader sstable : cfs.getLiveSSTables())
                {
                    try
                    {
                        cfs.mutateRepaired(Collections.singleton(sstable), UNREPAIRED_SSTABLE, UUID.randomUUID(), true);
                        if (!sstable.descriptor.version.hasIsTransient())
                            fail("We should fail setting pending repair on unsupported sstables "+sstable);
                    }
                    catch (IllegalStateException e)
                    {
                        if (sstable.descriptor.version.hasIsTransient())
                            fail("We should succeed setting pending repair on "+legacyVersion + " sstables, failed on "+sstable);
                    }
                }
            }
        }
    }

    @Test
    public void testMutateLevel() throws Exception
    {
        // we need to make sure we write old version metadata in the format for that version
        for (String legacyVersion : legacyVersions)
        {
            logger.info("Loading legacy version: {}", legacyVersion);
            truncateLegacyTables(legacyVersion);
            loadLegacyTables(legacyVersion);
            CacheService.instance.invalidateKeyCache();

            for (ColumnFamilyStore cfs : Keyspace.open("legacy_tables").getColumnFamilyStores())
            {
                for (SSTableReader sstable : cfs.getLiveSSTables())
                {
                    sstable.descriptor.getMetadataSerializer().mutateLevel(sstable.descriptor, 1234);
                    sstable.reloadSSTableMetadata();
                    assertEquals(1234, sstable.getSSTableLevel());
                }
            }
        }
    }

    private void doTestLegacyCqlTables()
    {
        qt().forAll(SourceDSL.arbitrary().pick(legacyVersions)).checkAssert(legacyVersion -> {
            logger.info("Loading legacy version: {}", legacyVersion);
            truncateLegacyTables(legacyVersion);
            loadLegacyTables(legacyVersion);
            CacheService.instance.invalidateKeyCache();
            long startCount = CacheService.instance.keyCache.size();
            verifyReads(legacyVersion);
            if (Keyspace.open("legacy_tables").getColumnFamilyStore(String.format("legacy_%s_simple", legacyVersion)).getLiveSSTables().stream().anyMatch(sstr -> sstr.descriptor.formatType.info.getType() == SSTableFormat.Type.BIG))
                verifyCache(legacyVersion, startCount);
            compactLegacyTables(legacyVersion);
        });
    }

    @Test
    public void testStreamLegacyCqlTables() throws Exception
    {
        for (String legacyVersion : legacyVersions)
        {
            streamLegacyTables(legacyVersion);
            verifyReads(legacyVersion);
        }
    }

    @Test
    public void testVerifyOldSimpleSSTables() throws IOException
    {
        verifyOldSSTables("simple");
    }

    @Test
    public void testVerifyOldTupleSSTables() throws IOException
    {
        verifyOldSSTables("tuple");
    }

    @Test
    public void testVerifyOldDroppedTupleSSTables() throws IOException
    {
        try {
            for (String legacyVersion : legacyVersions) {
                QueryProcessor.executeInternal(String.format("ALTER TABLE legacy_tables.legacy_%s_tuple DROP val", legacyVersion));
                QueryProcessor.executeInternal(String.format("ALTER TABLE legacy_tables.legacy_%s_tuple DROP val2", legacyVersion));
                QueryProcessor.executeInternal(String.format("ALTER TABLE legacy_tables.legacy_%s_tuple DROP val3", legacyVersion));
                QueryProcessor.executeInternal(String.format("ALTER TABLE legacy_tables.legacy_%s_tuple DROP val4", legacyVersion));
            }

            verifyOldSSTables("tuple");
        }
        finally
        {
            for (String legacyVersion : legacyVersions) {
                alterTableAddColumn(legacyVersion, "val frozen<tuple<set<int>,set<text>>>");
                alterTableAddColumn(legacyVersion, "val2 tuple<set<int>,set<text>>");
                alterTableAddColumn(legacyVersion, String.format("val3 frozen<legacy_%s_tuple_udt>", legacyVersion));
                alterTableAddColumn(legacyVersion, String.format("val4 legacy_%s_tuple_udt", legacyVersion));
            }
        }
    }

    private static void alterTableAddColumn(String legacyVersion, String column_definition) {
        // main-5.0 can use "IF NOT EXISTS" (and can also do all columns in one ALTER TABLE…
        //QueryProcessor.executeInternal(String.format("ALTER TABLE legacy_tables.legacy_%s_tuple ADD IF NOT EXISTS %s", legacyVersion, column_definition));
        try
        {
            QueryProcessor.executeInternal(String.format("ALTER TABLE legacy_tables.legacy_%s_tuple ADD %s", legacyVersion, column_definition));
        }
        catch (Throwable e) {}
    }

    private void verifyOldSSTables(String tableSuffix) throws IOException
    {
        for (String legacyVersion : legacyVersions)
        {
            ColumnFamilyStore cfs = Keyspace.open("legacy_tables").getColumnFamilyStore(String.format("legacy_%s_%s", legacyVersion, tableSuffix));
            loadLegacyTable(legacyVersion, tableSuffix);

            for (SSTableReader sstable : cfs.getLiveSSTables())
            {
                try (Verifier verifier = new Verifier(cfs, sstable, false, Verifier.options().checkVersion(true).build()))
                {
                    verifier.verify();
                    if (!sstable.descriptor.version.isLatestVersion())
                        fail("Verify should throw RuntimeException for old sstables "+sstable);
                }
                catch (RuntimeException e)
                {}
            }
            // make sure we don't throw any exception if not checking version:
            for (SSTableReader sstable : cfs.getLiveSSTables())
            {
                try (Verifier verifier = new Verifier(cfs, sstable, false, Verifier.options().checkVersion(false).build()))
                {
                    verifier.verify();
                }
                catch (Throwable e)
                {
                    fail("Verify should throw RuntimeException for old sstables "+sstable);
                }
            }
        }
    }

    @Test
    public void testPendingAntiCompactionOldSSTables() throws Exception
    {
        for (String legacyVersion : legacyVersions)
        {
            ColumnFamilyStore cfs = Keyspace.open("legacy_tables").getColumnFamilyStore(String.format("legacy_%s_simple", legacyVersion));
            loadLegacyTable(legacyVersion, "simple");

            boolean shouldFail = !cfs.getLiveSSTables().stream().allMatch(sstable -> sstable.descriptor.version.hasPendingRepair());
            IPartitioner p = Iterables.getFirst(cfs.getLiveSSTables(), null).getPartitioner();
            Range<Token> r = new Range<>(p.getMinimumToken(), p.getMinimumToken());
            PendingAntiCompaction.AcquisitionCallable acquisitionCallable = new PendingAntiCompaction.AcquisitionCallable(cfs, Collections.singleton(r), UUIDGen.getTimeUUID(), 0, 0);
            PendingAntiCompaction.AcquireResult res = acquisitionCallable.call();
            assertEquals(shouldFail, res == null);
            if (res != null)
                res.abort();
        }
    }

    @Test
    public void testAutomaticUpgrade() throws Exception
    {
        for (String legacyVersion : legacyVersions)
        {
            logger.info("Loading legacy version: {}", legacyVersion);
            truncateLegacyTables(legacyVersion);
            loadLegacyTables(legacyVersion);
            ColumnFamilyStore cfs = Keyspace.open("legacy_tables").getColumnFamilyStore(String.format("legacy_%s_simple", legacyVersion));
            // there should be no compactions to run with auto upgrades disabled:
            assertTrue(cfs.getCompactionStrategy().getNextBackgroundTasks(0).isEmpty());
        }

        DatabaseDescriptor.setAutomaticSSTableUpgradeEnabled(true);
        for (String legacyVersion : legacyVersions)
        {
            logger.info("Loading legacy version: {}", legacyVersion);
            truncateLegacyTables(legacyVersion);
            loadLegacyTables(legacyVersion);
            ColumnFamilyStore cfs = Keyspace.open("legacy_tables").getColumnFamilyStore(String.format("legacy_%s_simple", legacyVersion));
            if (cfs.getLiveSSTables().stream().anyMatch(s -> !s.descriptor.version.isLatestVersion()))
                assertTrue(cfs.metric.oldVersionSSTableCount.getValue() > 0);
            while (cfs.getLiveSSTables().stream().anyMatch(s -> !s.descriptor.version.isLatestVersion()))
            {
                CompactionManager.instance.submitBackground(cfs);
                Thread.sleep(100);
            }
            assertTrue(cfs.metric.oldVersionSSTableCount.getValue() == 0);
        }
        DatabaseDescriptor.setAutomaticSSTableUpgradeEnabled(false);
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
        if (null != descriptor) {
            SSTableReader sstable = descriptor.formatType.info.getReaderFactory().open(descriptor);
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
        Keyspace.open("legacy_tables").getColumnFamilyStore(String.format("legacy_%s_simple", legacyVersion)).truncateBlocking();
        Keyspace.open("legacy_tables").getColumnFamilyStore(String.format("legacy_%s_simple_counter", legacyVersion)).truncateBlocking();
        Keyspace.open("legacy_tables").getColumnFamilyStore(String.format("legacy_%s_clust", legacyVersion)).truncateBlocking();
        Keyspace.open("legacy_tables").getColumnFamilyStore(String.format("legacy_%s_clust_counter", legacyVersion)).truncateBlocking();
        Keyspace.open("legacy_tables").getColumnFamilyStore(String.format("legacy_%s_tuple", legacyVersion)).truncateBlocking();
    }

    private static void compactLegacyTables(String legacyVersion)
    {
        logger.info("Compacting legacy version {}", legacyVersion);
        Keyspace.open("legacy_tables").getColumnFamilyStore(String.format("legacy_%s_simple", legacyVersion)).forceMajorCompaction();
        Keyspace.open("legacy_tables").getColumnFamilyStore(String.format("legacy_%s_simple_counter", legacyVersion)).forceMajorCompaction();
        Keyspace.open("legacy_tables").getColumnFamilyStore(String.format("legacy_%s_clust", legacyVersion)).forceMajorCompaction();
        Keyspace.open("legacy_tables").getColumnFamilyStore(String.format("legacy_%s_clust_counter", legacyVersion)).forceMajorCompaction();
        Keyspace.open("legacy_tables").getColumnFamilyStore(String.format("legacy_%s_tuple", legacyVersion)).forceMajorCompaction();
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
            String ckValue = Integer.toString(ck) + longString;
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
        Assert.assertEquals(1, rs.size());
        Assert.assertEquals(1L, rs.one().getLong("val"));
    }

    private static void readClusteringTable(String legacyVersion, int ck, String ckValue, String pkValue)
    {
        logger.debug("Read legacy_{}_clust", legacyVersion);
        UntypedResultSet rs;
        rs = QueryProcessor.executeInternal(String.format("SELECT val FROM legacy_tables.legacy_%s_clust WHERE pk=? AND ck=?", legacyVersion), pkValue, ckValue);
        assertLegacyClustRows(1, rs);

        String ckValue2 = Integer.toString(ck < 10 ? 40 : ck - 1) + longString;
        String ckValue3 = Integer.toString(ck > 39 ? 10 : ck + 1) + longString;
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

        if (legacyVersion.startsWith("m") && legacyVersion.compareTo("me") <= 0)
        {
            // sstable formats (we know are) from 3.0.x must have had a schema with all collections frozen
            QueryProcessor.executeInternal(String.format("CREATE TABLE legacy_tables.legacy_%1$s_tuple (pk text PRIMARY KEY, " +
                    "val frozen<tuple<set<int>,set<text>>>, val2 frozen<tuple<set<int>,set<text>>>, val3 frozen<legacy_%1$s_tuple_udt>, val4 frozen<legacy_%1$s_tuple_udt>, extra text)", legacyVersion));
        }
        else
        {
            QueryProcessor.executeInternal(String.format("CREATE TABLE legacy_tables.legacy_%1$s_tuple (pk text PRIMARY KEY, " +
                "val frozen<tuple<set<int>,set<text>>>, val2 tuple<set<int>,set<text>>, val3 frozen<legacy_%1$s_tuple_udt>, val4 legacy_%1$s_tuple_udt, extra text)", legacyVersion));
        }
    }

    private static void truncateTables(String legacyVersion)
    {
        QueryProcessor.executeInternal(String.format("TRUNCATE legacy_tables.legacy_%s_simple", legacyVersion));
        QueryProcessor.executeInternal(String.format("TRUNCATE legacy_tables.legacy_%s_simple_counter", legacyVersion));
        QueryProcessor.executeInternal(String.format("TRUNCATE legacy_tables.legacy_%s_clust", legacyVersion));
        QueryProcessor.executeInternal(String.format("TRUNCATE legacy_tables.legacy_%s_clust_counter", legacyVersion));
        QueryProcessor.executeInternal(String.format("TRUNCATE legacy_tables.legacy_%s_tuple", legacyVersion));
        CacheService.instance.invalidateCounterCache();
        CacheService.instance.invalidateKeyCache();
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
        if (0 == getTableDir(legacyVersion, table).tryList(f -> f.name().endsWith(".db")).length)
            return;

        logger.info("Loading legacy table {}", table);

        ColumnFamilyStore cfs = Keyspace.open("legacy_tables").getColumnFamilyStore(table);

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

        // sstables before me are broken, treat this like an offline upgrade where the user ran the scrub's header fix
        //if (legacyVersion.compareTo("mc") <= 0)
        if (legacyVersion.startsWith("m") && legacyVersion.compareTo("me") <= 0)
        {
            FBUtilities.setPreviousReleaseVersionString("3.0.8");
            SSTableHeaderFix.fixNonFrozenUDTIfUpgradeFrom30();
        }

        int s0 = cfs.getLiveSSTables().size();
        cfs.loadNewSSTables();
        int s1 = cfs.getLiveSSTables().size();
        assertThat(s1).isGreaterThan(s0);
    }

    /**
     * Generates sstables for 8 CQL tables (see {@link #createTables(String)}) in <i>current</i>
     * sstable format (version) into {@code test/data/legacy-sstables/VERSION}, where
     * {@code VERSION} matches {@link Version#getVersion() BigFormat.latestVersion.getVersion()}.
     * <p>
     * Run this test alone (e.g. from your IDE) when a new version is introduced or format changed
     * during development. I.e. remove the {@code @Ignore} annotation temporarily.
     * </p>
     */
    @Ignore
    @Test
    public void testGenerateSstables() throws Throwable
    {
        Version version = TrieIndexFormat.latestVersion;
        System.setProperty(SSTableFormat.FORMAT_DEFAULT_PROP, version.getSSTableFormat().getType().name);
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
                                                         version, valPk, "foo bar baz"));

            QueryProcessor.executeInternal(String.format("UPDATE legacy_tables.legacy_%s_simple_counter SET val = val + 1 WHERE pk = '%s'",
                                                         version, valPk));

            QueryProcessor.executeInternal(
                    String.format("INSERT INTO legacy_tables.legacy_%s_tuple (pk, val, val2, val3, val4, extra)"
                                    + " VALUES ('%s', ({1,2,3},{'a','b','c'}), ({1,2,3},{'a','b','c'}), {name: ('abc','def')}, {name: ('abc','def')}, '%s')",
                                  version, valPk, randomString));

            for (int ck = 0; ck < 50; ck++)
            {
                String valCk = Integer.toString(ck);

                QueryProcessor.executeInternal(String.format("INSERT INTO legacy_tables.legacy_%s_clust (pk, ck, val) VALUES ('%s', '%s', '%s')",
                                                             version, valPk, valCk + longString, randomString));

                QueryProcessor.executeInternal(String.format("UPDATE legacy_tables.legacy_%s_clust_counter SET val = val + 1 WHERE pk = '%s' AND ck='%s'",
                                                             version, valPk, valCk + longString));
            }
        }

        StorageService.instance.forceKeyspaceFlush("legacy_tables");

        File ksDir = new File(LEGACY_SSTABLE_ROOT, String.format("%s/legacy_tables", version));
        ksDir.tryCreateDirectories();
        copySstablesFromTestData(String.format("legacy_%s_simple", version), ksDir);
        copySstablesFromTestData(String.format("legacy_%s_simple_counter", version), ksDir);
        copySstablesFromTestData(String.format("legacy_%s_clust", version), ksDir);
        copySstablesFromTestData(String.format("legacy_%s_clust_counter", version), ksDir);
        copySstablesFromTestData(String.format("legacy_%s_tuple", version), ksDir);
    }

    public static void copySstablesFromTestData(String table, File ksDir) throws IOException
    {
        File cfDir = new File(ksDir, table);
        cfDir.tryCreateDirectory();

        for (File srcDir : Keyspace.open("legacy_tables").getColumnFamilyStore(table).getDirectories().getCFDirectories())
        {
            for (File file : srcDir.tryList())
            {
                copyFile(cfDir, file);
            }
        }
    }

    private static void copySstablesToTestData(String legacyVersion, String table, File cfDir) throws IOException
    {
        File tableDir = getTableDir(legacyVersion, table);
        Assert.assertTrue("The table directory " + tableDir + " was not found", tableDir.isDirectory());
        for (File file : tableDir.tryList())
        {
            copyFile(cfDir, file);
        }
    }

    private static File getTableDir(String legacyVersion, String table)
    {
        return new File(LEGACY_SSTABLE_ROOT, String.format("%s/legacy_tables/%s", legacyVersion, table));
    }

    private static void copyFile(File cfDir, File file) throws IOException
    {
        byte[] buf = new byte[65536];
        if (file.isFile())
        {
            File target = new File(cfDir, file.name());
            int rd;
            try (FileInputStreamPlus is = new FileInputStreamPlus(file);
                 FileOutputStreamPlus os = new FileOutputStreamPlus(target);) {
                while ((rd = is.read(buf)) >= 0)
                    os.write(buf, 0, rd);
                }
        }
    }
}
