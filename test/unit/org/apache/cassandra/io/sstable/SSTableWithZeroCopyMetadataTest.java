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
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.compaction.Scrubber;
import org.apache.cassandra.db.compaction.Verifier;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.sstable.format.PartitionIndexIterator;
import org.apache.cassandra.io.sstable.format.RowIndexEntry;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReadsListener;
import org.apache.cassandra.io.sstable.format.ScrubPartitionIterator;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.OutputHandler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

@RunWith(Parameterized.class)
public class SSTableWithZeroCopyMetadataTest
{
    private final static Logger logger = LoggerFactory.getLogger(SSTableWithZeroCopyMetadataTest.class);

    private static final String KEYSPACE = "ZeroCopyStreamingTest";
    private static final String TABLE = "Standard1";

    private static final Path UNCOMPRESSED_DATA_DIR = Paths.get("test/data/zcs/uncompressed");
    private static final Path COMPRESSED_DATA_DIR = Paths.get("test/data/zcs/compressed");

    @Parameterized.Parameters(name = "compressed={0}, diskAccessMode={1}")
    public static Object[] parameters()
    {
        return new Object[][]{ { false, Config.DiskAccessMode.standard },
                               { false, Config.DiskAccessMode.mmap },
                               { true, Config.DiskAccessMode.standard },
                               { true, Config.DiskAccessMode.mmap } };
    }

    @Parameterized.Parameter(0)
    public boolean compressed;

    @Parameterized.Parameter(1)
    public Config.DiskAccessMode diskAccessMode;

    private Path dataDir;
    private Path tableDataDir;
    private TableMetadataRef metadataRef;
    private ColumnFamilyStore realm;

    @BeforeClass
    public static void beforeClass()
    {
        DatabaseDescriptor.daemonInitialization(() -> {
            Config config = DatabaseDescriptor.loadConfig();
            config.partitioner = Murmur3Partitioner.class.getName();
            return config;
        });
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE, TABLE));
        StorageService.instance.initServer();
    }

    @Before
    public void beforeTest() throws IOException
    {
        DatabaseDescriptor.setDiskAccessMode(diskAccessMode);
        dataDir = Files.createTempDirectory("zcs-" + (compressed ? "compressed" : "uncompressed"));
        FileUtils.copyDirectory((compressed ? COMPRESSED_DATA_DIR : UNCOMPRESSED_DATA_DIR).toFile(), dataDir.toFile());
        tableDataDir = dataDir.resolve(KEYSPACE).resolve(TABLE);
        metadataRef = Schema.instance.getTableMetadataRef(KEYSPACE, TABLE);
        realm = ColumnFamilyStore.getIfExists(metadataRef.keyspace, metadataRef.name);
        realm.disableAutoCompaction();
    }

    @After
    public void afterTest() throws IOException
    {
        realm.truncateBlockingWithoutSnapshot();
    }

    @Test
    public void testStreamingEntireSSTables() throws ExecutionException, InterruptedException
    {
        DatabaseDescriptor.getRawConfig().stream_entire_sstables = true;
        testStreamingSSTables();
    }

    @Test
    public void testStreamingPartialSSTables() throws ExecutionException, InterruptedException
    {
        DatabaseDescriptor.getRawConfig().stream_entire_sstables = false;
        testStreamingSSTables();
    }

    private void testStreamingSSTables() throws ExecutionException, InterruptedException
    {
        SSTableLoader.Client client = new SSTableLoader.Client()
        {
            @Override
            public void init(String keyspace)
            {
                for (Replica replica : StorageService.instance.getLocalReplicas(keyspace))
                    addRangeForEndpoint(replica.range(), FBUtilities.getBroadcastAddressAndPort());
            }

            @Override
            public TableMetadataRef getTableMetadata(String tableName)
            {
                return metadataRef;
            }
        };
        try
        {
            SSTableLoader loader = new SSTableLoader(new File(tableDataDir), client, new OutputHandler.LogOutput(logger));
            loader.stream().get();
        }
        finally
        {
            client.stop();
        }
        assertThat(realm.getLiveSSTables()).hasSize(4);

        realm.forceMajorCompaction();

        assertThat(realm.getLiveSSTables()).hasSizeLessThan(4);
    }


    @Test
    public void testIterations()
    {
        LifecycleTransaction.getFiles(tableDataDir, (file, fileType) -> Descriptor.fromFilenameWithComponent(file).right == Component.DATA, Directories.OnTxnErr.THROW).forEach(file -> {
            Descriptor desc = Descriptor.fromFilename(file);
            SSTableReader sstable = desc.formatType.info.getReaderFactory().open(desc, SSTable.discoverComponentsFor(desc), metadataRef, true, true);
            assertThatCode(() -> {
                assertThat(sstable.getSSTableMetadata().zeroCopyMetadata.exists()).isTrue();
                try
                {
                    checkSSTableReader(sstable);
                    checkVerifier(sstable);
                    checkScrubber(sstable);
                }
                catch (IOException e)
                {
                    throw new RuntimeException(e);
                }
                finally
                {
                    if (sstable.selfRef().globalCount() > 0) sstable.selfRef().release();
                }
            }).describedAs(sstable.toString()).doesNotThrowAnyException();
        });
    }

    private void checkScrubber(SSTableReader sstable)
    {
        try (Scrubber scrubber = new Scrubber(realm, LifecycleTransaction.offline(OperationType.SCRUB, sstable), false, true, false))
        {
            Scrubber.ScrubResult result = scrubber.scrubWithResult();
            assertThat(result.badPartitions).isZero();
        }
    }

    private void checkVerifier(SSTableReader sstable)
    {
        Verifier.Options options = Verifier.options().extendedVerification(true)
                                           .checkVersion(false)
                                           .quick(false)
                                           .build();

        try (Verifier verifier = new Verifier(realm, sstable, false, options))
        {
            verifier.verify();
        }
    }

    private void checkSSTableReader(SSTableReader sstable) throws IOException
    {
        List<DecoratedKey> keys = new ArrayList<>();
        try (RandomAccessReader dataReader = sstable.openDataReader())
        {
            checkAllKeysIterator(sstable, dataReader, keys);
            checkSrubPartitionsIterator(sstable, dataReader, keys);

            // test getting and iterating over a single partition
            for (DecoratedKey key : keys)
            {
                int idx = keys.indexOf(key);

                // EQ position
                long eqPos = sstable.getPosition(key, SSTableReader.Operator.EQ).position;
                dataReader.seek(eqPos);
                ByteBuffer keyFromDataFile = ByteBufferUtil.readWithShortLength(dataReader);
                assertThat(sstable.getPartitioner().decorateKey(keyFromDataFile)).isEqualTo(key);

                // GE position
                long gePos = sstable.getPosition(key, SSTableReader.Operator.GE).position;
                assertThat(gePos).isEqualTo(eqPos); // because the key exists

                // GT position
                RowIndexEntry gtPos = sstable.getPosition(key, SSTableReader.Operator.GT);
                if (idx != keys.size() - 1)
                {
                    DecoratedKey nextKey = keys.get(idx + 1);
                    long nextEqPos = sstable.getPosition(nextKey, SSTableReader.Operator.EQ).position;
                    assertThat(gtPos.position).isGreaterThan(eqPos);
                    assertThat(gtPos.position).isEqualTo(nextEqPos);
                }

                if (idx == 0)
                    assertThat(key).isEqualTo(sstable.first);
                if (idx == keys.size() - 1)
                    assertThat(key).isEqualTo(sstable.last);

                try (UnfilteredRowIterator it = sstable.simpleIterator(dataReader, key, false))
                {
                    while (it.hasNext())
                    {
                        Unfiltered next = it.next();
                        next.validateData(sstable.metadata());
                    }
                }

                try (UnfilteredRowIterator it = sstable.iterator(key, Slices.ALL, ColumnFilter.NONE, false, SSTableReadsListener.NOOP_LISTENER))
                {
                    while (it.hasNext())
                    {
                        Unfiltered next = it.next();
                        next.validateData(sstable.metadata());
                    }
                }
            }
        }
    }

    private static void checkSrubPartitionsIterator(SSTableReader sstable, RandomAccessReader dataReader, List<DecoratedKey> keys) throws IOException
    {
        try (ScrubPartitionIterator it = sstable.scrubPartitionsIterator())
        {
            while (!it.isExhausted())
            {
                dataReader.seek(it.dataPosition());
                ByteBuffer keyFromDataFile = ByteBufferUtil.readWithShortLength(dataReader);
                DecoratedKey key = sstable.getPartitioner().decorateKey(keyFromDataFile);
                assertThat(keys).contains(key);
                it.advance();
            }
        }
    }

    private static void checkAllKeysIterator(SSTableReader sstable, RandomAccessReader dataReader, List<DecoratedKey> keys) throws IOException
    {
        try (PartitionIndexIterator it = sstable.allKeysIterator())
        {
            while (!it.isExhausted())
            {
                ByteBuffer keyFromIterator = it.key();
                dataReader.seek(it.dataPosition());
                ByteBuffer keyFromDataFile = ByteBufferUtil.readWithShortLength(dataReader);
                DecoratedKey key = sstable.getPartitioner().decorateKey(keyFromDataFile);
                assertThat(keyFromIterator).isEqualTo(keyFromDataFile);
                assertThat(key).isGreaterThanOrEqualTo(sstable.first);
                assertThat(key).isLessThanOrEqualTo(sstable.last);
                keys.add(key);
                it.advance();
            }
        }
    }
}
