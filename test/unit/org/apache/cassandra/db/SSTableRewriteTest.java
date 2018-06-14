package org.apache.cassandra.db;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Directories.DataDirectory;
import org.apache.cassandra.db.compaction.AbstractCompactionTask;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.CompactionTask;
import org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy;
import org.apache.cassandra.db.compaction.writers.CompactionAwareWriter;
import org.apache.cassandra.db.compaction.writers.DefaultCompactionWriter;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.junit.BeforeClass;
import org.junit.Test;

public class SSTableRewriteTest
{
    private static final int LOOPS = 200;
    private static final String KEYSPACE1 = "RewriteTest1";
    private static final String CF_STANDARD1 = "Standard1";

    private static final ByteBuffer COLUMN = ByteBufferUtil.bytes("birthdate");
    private static final ByteBuffer VALUE = ByteBuffer.allocate(8);
    static
    {
        VALUE.putLong(20101229);
        VALUE.flip();
    }

    private static Directories tieredDirectories;

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        DatabaseDescriptor.setPartitionerUnsafe(ByteOrderedPartitioner.instance);
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD1));
    }

    @Test
    public void testRewriteWithTier() throws Exception
    {
        StorageService.instance.getTokenMetadata().clearUnsafe();

        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF_STANDARD1);
        setTieredCompactionStrategy(cfs);

        // setup mock tier directories
        DataDirectory[] mockDataDirectory = new DataDirectory[1];
        String mockTier = "mock_tier";
        File mockDir = new File(DatabaseDescriptor.getAllDataFileLocations()[0] + File.separator + mockTier);
        mockDir.deleteOnExit();
        mockDataDirectory[0] = new DataDirectory(mockDir);
        tieredDirectories = new Directories(cfs.metadata, mockDataDirectory);

        // insert data and verify we get it back w/ range query
        fillCF(cfs, "val", LOOPS);
        assertEquals(LOOPS, Util.getAll(Util.cmd(cfs).build()).size());

        cfs.sstablesRewrite(false, 1); // rewrite to the same version

        // verify sstable is written to the mock tier
        assertEquals(LOOPS, Util.getAll(Util.cmd(cfs).build()).size());
        assertFalse(cfs.getLiveSSTables().isEmpty());
        for (SSTableReader sstable : cfs.getLiveSSTables())
            assertTrue("Expected sstable being written to mock tier, but got: " + sstable.getFilename(),
                       sstable.getFilename().contains(mockTier));
    }

    protected void fillCF(ColumnFamilyStore cfs, String colName, int rowsPerSSTable)
    {
        CompactionManager.instance.disableAutoCompaction();

        for (int i = 0; i < rowsPerSSTable; i++)
        {
            String key = String.valueOf(i);
            // create a row and update the birthdate value, test that the index query fetches the new version
            new RowUpdateBuilder(cfs.metadata, System.currentTimeMillis(), ByteBufferUtil.bytes(key))
                                                                                                     .clustering(COLUMN)
                                                                                                     .add(colName,
                                                                                                          VALUE)
                                                                                                     .build()
                                                                                                     .applyUnsafe();
        }

        cfs.forceBlockingFlush();
    }

    private void setTieredCompactionStrategy(ColumnFamilyStore cfs)
    {
        Map<String, String> localOptions = new HashMap<>();
        localOptions.put("class", "org.apache.cassandra.db.SSTableRewriteTest$MockedTieredCompactionStrategy");
        cfs.setCompactionParameters(localOptions);
    }

    public static class MockedTieredCompactionStrategy extends SizeTieredCompactionStrategy
    {
        public MockedTieredCompactionStrategy(ColumnFamilyStore cfs, Map<String, String> options)
        {
            super(cfs, options);
        }

        @Override
        public AbstractCompactionTask getCompactionTask(LifecycleTransaction txn, int gcBefore, long maxSSTableBytes)
        {
            return new CompactionTask(cfs, txn, gcBefore)
            {
                @Override
                public CompactionAwareWriter getCompactionAwareWriter(ColumnFamilyStore cfs,
                                                                      Directories directories, // not used
                                                                      LifecycleTransaction transaction,
                                                                      Set<SSTableReader> nonExpiredSSTables)
                {
                    return new DefaultCompactionWriter(cfs,
                                                       tieredDirectories,
                                                       transaction,
                                                       nonExpiredSSTables,
                                                       offline,
                                                       keepOriginals);
                }
            };
        }
    }
}
