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
package org.apache.cassandra.io.sstable.format;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.metrics.RestorableMeter;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.utils.AlwaysPresentFilter;
import org.apache.cassandra.utils.BloomFilter;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.FilterFactory;
import org.apache.cassandra.utils.IFilter;
import org.awaitility.Awaitility;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class LazyBloomFilterTest
{
    private static final String KEYSPACE1 = "SSTableReaderTest";
    private static final String CF_STANDARD = "Standard1";

    private static ColumnFamilyStore store;

    @BeforeClass
    public static void defineSchema()
    {
        CassandraRelevantProperties.BLOOM_FILTER_LAZY_LOADING.setBoolean(true);

        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD).bloomFilterFpChance(0.1));

        store = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD);
    }

    @AfterClass
    public static void tearDown()
    {
        CassandraRelevantProperties.BLOOM_FILTER_LAZY_LOADING_WINDOW.reset();
        CassandraRelevantProperties.BLOOM_FILTER_LAZY_LOADING_THRESHOLD.reset();
        CassandraRelevantProperties.BLOOM_FILTER_LAZY_LOADING.reset();
    }

    @After
    public void cleanup()
    {
        store.loadNewSSTables();
        store.truncateBlocking();
    }

    @Test
    public void testDeserializeOnFirstRead()
    {
        CassandraRelevantProperties.BLOOM_FILTER_LAZY_LOADING_THRESHOLD.setInt(0);

        SSTableReader sstable = reopenFlushedSSTable();

        // first read will trigger bloom filter deserialization
        assertTrue(sstable.couldContain(Util.dk(String.valueOf(10))));
        waitFor("Async BF deserialization", () -> sstable.getBloomFilter() != FilterFactory.AlwaysPresentForLazyLoading);

        IFilter deserializedBloomFilter = sstable.getBloomFilter();
        assertThat(deserializedBloomFilter).isNotInstanceOf(AlwaysPresentFilter.class);
        assertThat(deserializedBloomFilter).isInstanceOf(BloomFilter.class);
        assertThat(deserializedBloomFilter.offHeapSize()).isGreaterThan(0);

        // second read will NOT trigger bloom filter deserialization
        assertTrue(sstable.couldContain(Util.dk(String.valueOf(20))));
        assertSame(deserializedBloomFilter, sstable.getBloomFilter());
        assertThat(deserializedBloomFilter.offHeapSize()).isGreaterThan(0);

        releaseSSTables(sstable);
    }

    @Test
    public void testConcurrentReads() throws InterruptedException
    {
        CassandraRelevantProperties.BLOOM_FILTER_LAZY_LOADING_THRESHOLD.setInt(0);

        SSTableReader sstable = reopenFlushedSSTable();

        int threads = 32;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch latch = new CountDownLatch(1);

        List<Future<Boolean>> futures = new ArrayList<>();
        for (int i = 0; i < 100; i++)
        {
            futures.add(executor.submit(() -> {
                Uninterruptibles.awaitUninterruptibly(latch, 1, TimeUnit.MINUTES);
                return sstable.maybeDeserializeLazyBloomFilter();
            }));
        }

        latch.countDown();
        waitFor("Async BF deserialization", () -> sstable.getBloomFilter() != FilterFactory.AlwaysPresentForLazyLoading);

        executor.shutdown();
        Assert.assertTrue(executor.awaitTermination(1, TimeUnit.MINUTES));

        // only one thread can deserialize BF
        assertThat(FBUtilities.waitOnFutures(futures).stream().filter(b -> b).count()).isEqualTo(1);

        Awaitility.await("Wait for async BF deserialization")
                  .atMost(10, TimeUnit.SECONDS)
                  .untilAsserted(() -> {
                      assertThat(sstable.getBloomFilter()).isNotInstanceOf(AlwaysPresentFilter.class);
                      assertThat(sstable.getBloomFilter().offHeapSize()).isGreaterThan(0);
                  });

        releaseSSTables(sstable);
    }

    @Test
    public void testLazyLoadingCountThreshold()
    {
        testLazyLoadingThreshold(-1, 10);
    }

    @Test
    public void testLazyLoadingCountThresholdBadPartition()
    {
        testLazyLoadingThreshold(-1, 7);
    }

    @Test
    public void testLazyLoading1MThreshold()
    {
        testLazyLoadingThreshold(1, 10);
    }

    @Test
    public void testLazyLoading5MThreshold()
    {
        testLazyLoadingThreshold(5, 10);
    }

    @Test
    public void testLazyLoading15MThreshold()
    {
        testLazyLoadingThreshold(15, 10);
    }

    public void testLazyLoadingThreshold(int window, int keyInt)
    {
        int threshold = 1;
        CassandraRelevantProperties.BLOOM_FILTER_LAZY_LOADING_WINDOW.setInt(window);
        CassandraRelevantProperties.BLOOM_FILTER_LAZY_LOADING_THRESHOLD.setInt(threshold);

        SSTableReader sstable = reopenFlushedSSTable();
        DecoratedKey key = Util.dk(String.valueOf(keyInt));

        // first read will NOT trigger bloom filter deserialization because of threshold not reached
        sstable.couldContain(key);
        assertSame(FilterFactory.AlwaysPresentForLazyLoading, sstable.getBloomFilter());
        assertThat(sstable.getBloomFilter().offHeapSize()).isEqualTo(0);

        // make the sstable access the index
        SinglePartitionReadCommand command = createCommand(key);

        long start = System.nanoTime();
        while (System.nanoTime() - start <= 3 * RestorableMeter.TICK_INTERVAL)
        {
            FBUtilities.sleepQuietly(10);
            Util.getAllUnfiltered(command);

            if (sstable.getBloomFilter() != FilterFactory.AlwaysPresentForLazyLoading)
                break;
        }

        assertThat(sstable.getPartitionIndexReadMeter().count()).isGreaterThan(0);
        if (window > 0)
            assertThat(sstable.getPartitionIndexReadMeter().rate(window)).isGreaterThan(threshold);

        assertThat(sstable.getBloomFilter()).isNotInstanceOf(AlwaysPresentFilter.class);
        assertThat(sstable.getBloomFilter().offHeapSize()).isGreaterThan(0);

        releaseSSTables(sstable);
    }

    @Test
    public void testDeserializationOnReleasedSSTable()
    {
        CassandraRelevantProperties.BLOOM_FILTER_LAZY_LOADING_THRESHOLD.setInt(0);

        SSTableReader sstable = reopenFlushedSSTable();

        // release sstable
        store.getLiveSSTables().forEach(s -> s.selfRef().release()); // ColumnFamilyStore#clearUnsafe won't release sstable reference
        store.clearUnsafe();
        assertThat(sstable.selfRef().globalCount()).isEqualTo(0);

        // it will try to deserialize but skip
        assertThat(sstable.maybeDeserializeLazyBloomFilter()).isTrue();
        Awaitility.await("Async deserialization skipped")
                  .atMost(10, TimeUnit.SECONDS)
                  .until(() -> sstable.bf == FilterFactory.AlwaysPresent);

        assertThat(sstable.getBloomFilter()).isInstanceOf(AlwaysPresentFilter.class);
    }

    private void releaseSSTables(SSTableReader sstable)
    {
        store.getLiveSSTables().forEach(s -> s.selfRef().release()); // ColumnFamilyStore#clearUnsafe won't release sstable reference
        store.clearUnsafe();

        // close sstable, bf should be closed when sstable tidier runs
        waitFor("sstable tidier", ((BloomFilter) sstable.getBloomFilter())::isCleanedUp);
    }

    private void waitFor(String alias, Callable<Boolean> condition)
    {
        Awaitility.await(alias)
                .pollInterval(1, TimeUnit.SECONDS)
                .atMost(30, TimeUnit.SECONDS)
                .until(condition);
    }

    private SinglePartitionReadCommand createCommand(DecoratedKey key)
    {
        return SinglePartitionReadCommand.create(store.metadata(), FBUtilities.nowInSeconds(), key, Slices.ALL);
    }

    private SSTableReader reopenFlushedSSTable()
    {
        SSTableReader sstable = flushSSTable(store, 100, 10);

        // sstable flush writer generates bloom filter and loads it
        assertThat(sstable.getBloomFilter()).isNotInstanceOf(AlwaysPresentFilter.class);
        assertThat(sstable.getBloomFilter().offHeapSize()).isGreaterThan(0);

        // unlink sstables and reopen them
        store.getLiveSSTables().forEach(s -> s.selfRef().release()); // ColumnFamilyStore#clearUnsafe won't release sstable reference
        store.clearUnsafe();
        store.loadNewSSTables();

        // newly opened sstable delays bloom filter deserialization
        sstable = Iterables.getOnlyElement(store.getLiveSSTables());
        assertSame(FilterFactory.AlwaysPresentForLazyLoading, sstable.getBloomFilter());
        assertThat(sstable.getBloomFilter().offHeapSize()).isEqualTo(0);

        return sstable;
    }

    private SSTableReader flushSSTable(ColumnFamilyStore cfs, int numKeys, int step)
    {
        Set<SSTableReader> before = cfs.getLiveSSTables();
        for (int j = 0; j < numKeys; j += step)
        {
            new RowUpdateBuilder(cfs.metadata(), j, String.valueOf(j))
            .clustering("0")
            .add("val", ByteBufferUtil.EMPTY_BYTE_BUFFER)
            .build()
            .applyUnsafe();
        }
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);
        return Sets.difference(cfs.getLiveSSTables(), before).iterator().next();
    }
}
