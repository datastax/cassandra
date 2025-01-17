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

package org.apache.cassandra.cache;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Function;

import com.google.common.collect.ImmutableMap;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.CounterMutation;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.marshal.CounterColumnType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.compress.ICompressor;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.Rebufferer;
import org.apache.cassandra.io.util.RebuffererFactory;
import org.apache.cassandra.io.util.WrappingRebufferer;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.CacheService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMUnitConfig;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;

import static org.apache.cassandra.utils.ByteBufferUtil.bytes;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeNotNull;

/**
 * Exercises loading and storing chunks in the chunk cache, as well as error conditions around that.
 */
@RunWith(BMUnitRunner.class)
@BMUnitConfig(debug = true)
public class ChunkCacheLoadingTest
{
    private static final String KEYSPACE = "db3050";
    private static final String COUNTER = "counter";
    private static final String NORMAL = "normal";

    private static volatile RebufferInterceptingRebufferer rebufferInterceptor;

    @BeforeClass
    public static void setup()
    {
        DatabaseDescriptor.daemonInitialization();

        assumeNotNull(ChunkCache.instance);

        SchemaLoader.prepareServer();

        // Set up chunk cache interception, so that rebuffering results can be inspected and consumed.
        ChunkCache.instance.intercept(rf ->
                                      {
                                          rebufferInterceptor = new RebufferInterceptingRebufferer(rf.instantiateRebufferer(false));
                                          return rebufferInterceptor;
                                      });


        // A counter table, used for setting up and triggering reads from the counter cache.
        // With table params for a custom compressor that issues a read on each uncompression.
        TableMetadata counterTable = TableMetadata.builder(KEYSPACE, COUNTER)
                                                  .isCounter(true)
                                                  .addPartitionKeyColumn("key", Int32Type.instance)
                                                  .addRegularColumn("c", CounterColumnType.instance)
                                                  .compression(CompressionParams.fromMap(ImmutableMap.of(CompressionParams.CLASS,
                                                                                                         ReadingNopCompressor.class.getName())))
                                                  .build();
        // A regular table, used for setting up chunk cache collisions with different chunks.
        TableMetadata normalTable = TableMetadata.builder(KEYSPACE, NORMAL)
                                                 .addPartitionKeyColumn("key", Int32Type.instance)
                                                 .addRegularColumn("val", Int32Type.instance)
                                                 .build();

        SchemaLoader.createKeyspace(KEYSPACE, KeyspaceParams.simple(1), counterTable, normalTable);
    }

    @Before
    public void setUp()
    {
        assumeNotNull(ChunkCache.instance);
    }

    @AfterClass
    public static void cleanup()
    {
        SchemaLoader.cleanupSavedCaches();
    }

    // See DB-3050
    @BMRule(name = "Ensure chunk cache collisions",
            targetClass = "org.apache.cassandra.cache.ChunkCache$Key",
            targetMethod = "hashCode()",
            // This is needed in order to ensure that loads of different chunks will result in colliding loads in the
            // underlying Caffeine cache. We want to ensure that because in 6.0 and above (unlike in 5.1) doing a nested read
            // of the same chunk is much harder to handle without resulting in a stall, mostly because of the changes related
            // to the chunk cache now storing chunk futures instead of the chunks themselves.
            condition = "$0.internedPath.contains(\"db3050\") && $0.internedPath.contains(\"Data.db\")",
            action = "return 1")
    @Test(timeout=5000)
    public void testUncompressionReadCollision() throws Exception
    {
        // Write a single row into the counter table, then flush it, so that a read from it will trigger a chunk cache load.
        ColumnFamilyStore counterCfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(COUNTER);
        new CounterMutation(new RowUpdateBuilder(counterCfs.metadata(), 0, bytes(1)).add("c", 12L).build(), ConsistencyLevel.ONE).apply();
        counterCfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
        // Write a single row into the normal table, then flush it, so that it can be used for the nested, colliding reads from
        // within the custom compressor.
        ColumnFamilyStore normalCfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(NORMAL);
        new RowUpdateBuilder(normalCfs.metadata(), 0, bytes(1)).add("val", 34).build().apply();
        normalCfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);

        // Invalidate everything from the chunk cache but the SSTable partition index files, as invalidating these files will
        // cause an async read for the partition index during the counter read on cache reload. This will cause a jump from the
        // BackgroundIoStage thread to a TPC thread, and we want the BackgroundIoStage to keep going until it blocks due to a
        // blocking secondary read done in some of the ReadingNopCompressor methods.
        Set<File> sstableDataFilePaths = new HashSet<>();
        for (var dataDirectory : Directories.dataDirectories)
            addNonPartitionFiles(sstableDataFilePaths, dataDirectory.location);
        sstableDataFilePaths.forEach(ChunkCache.instance::invalidateFile);

        // Trigger a counter cache load by triggering a flush and then a load (without flushing the load will be a NOP). We
        // don't even need to invalidate the cache after the flush. The load then is guaranteed to trigger the DB-3050 error
        // condition - before the DB-3050 fix, this means recursively calling a chunk cache load (i.e. recursively calling a
        // ConcurrentHashMap.computeIfAbsent) and hitting a collision with the reserved bucket from the first call in the second
        // call. The guarantee comes from issuing a read for the exact same partition during the uncompression part of the
        // chunk cache load for the first read.
        CacheService.instance.counterCache.submitWrite(Integer.MAX_VALUE).get();
        CacheService.instance.counterCache.loadSaved();

        ColumnMetadata cm = counterCfs.metadata().getColumn(ByteBufferUtil.bytes("c"));
        assertEquals(12L, counterCfs.getCachedCounter(CounterCacheKey.create(counterCfs.metadata(), bytes(1), Clustering.EMPTY, cm, null)).count);
    }

    private static void addNonPartitionFiles(Set<File> sstableDataFilePaths, File... roots)
    {
        if (roots == null)
            return;

        for (File file : roots)
        {
            if (file.isDirectory())
            {
                addNonPartitionFiles(sstableDataFilePaths, file.tryList());
                continue;
            }
            assert file.isFile();
            String absolutePath = file.path();
            if (!absolutePath.contains("Partitions.db"))
                sstableDataFilePaths.add(file);
        }
    }

    /**
     * A custom compressor that issues a read on each uncompression and each compressed buffer length check.
     * Used to trigger a nested read during chunk cache loading.
     */
    public static class ReadingNopCompressor implements ICompressor
    {
        private static final AtomicInteger nestedReadsCounter = new AtomicInteger();

        public static ReadingNopCompressor create(Map<String, String> options)
        {
            return new ReadingNopCompressor();
        }

        @Override
        public int initialCompressedBufferLength(int chunkLength)
        {
            // Limit the number of nested reads to avoid stack overflow. It's important that the NORMAL table doesn't use
            // the ReadingNopCompressor, otherwise it can issue nested reads for the same chunk which could cause stalling.
            if (nestedReadsCounter.incrementAndGet() < 10)
                // Using the COUNTER table instead of the NORMAL one will actually result in stalling the test.
                QueryProcessor.executeInternal(String.format("SELECT * FROM %s.%s WHERE key=1;", KEYSPACE, NORMAL));
            return chunkLength;
        }

        @Override
        public int uncompress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset)
        {
            System.arraycopy(input, inputOffset, output, outputOffset, inputLength);
            return inputLength;
        }

        @Override
        public void compress(ByteBuffer input, ByteBuffer output)
        {
            ByteBufferUtil.put(input, output);
        }

        @Override
        public void uncompress(ByteBuffer input, ByteBuffer output)
        {
            // Limit the number of nested reads to avoid stack overflow. It's important that the NORMAL table doesn't use
            // the ReadingNopCompressor, otherwise it can issue nested reads for the same chunk which could cause stalling.
            if (nestedReadsCounter.incrementAndGet() < 10)
                // Using the COUNTER table instead of the NORMAL one will actually result in stalling the test.
                QueryProcessor.executeInternal(String.format("SELECT * FROM %s.%s WHERE key=1;", KEYSPACE, NORMAL));
            ByteBufferUtil.put(input, output);
        }

        @Override
        public Set<String> supportedOptions()
        {
            return Collections.emptySet();
        }

        @Override
        public BufferType preferredBufferType()
        {
            return BufferType.OFF_HEAP;
        }

        @Override
        public boolean supports(BufferType bufferType)
        {
            return true;
        }
    }

    @BMRule(name = "Throw once during async load of compressed chunk",
            targetClass = "org.apache.cassandra.io.util.CompressedChunkReader$Mmap",
            targetMethod = "readChunk(long, java.nio.ByteBuffer)",
            condition = "not flagged(\"throw\")" +
                    "&& $0.toString().contains(\"db3050\")" +
                    "&& $0.toString().contains(\"normal\")" +
                    "&& $0.toString().contains(\"Data.db\")",
            action = "flag(\"throw\");" +
                    "throw new RuntimeException(\"Nope, no chunk for you!\")")
    @Test
    public void testChunkAsyncLoadThrows()
    {
        prepareChunkCache();

        try
        {
            // A single disk read from the NORMAL table - chunk loading should be triggered, and it should throw a
            // RuntimeException.
            QueryProcessor.executeInternal(String.format("SELECT * FROM %s.%s WHERE key=1;", KEYSPACE, NORMAL));
            fail("Expected read to throw due to RuntimeException during load");
        }
        catch (Throwable t)
        {
            // Expect the RuntimeException set up with Byteman to be thrown.
            if (!(t instanceof RuntimeException) || !t.getMessage().contains("no chunk for you"))
                fail(String.format("Expected cause to be RuntimeException, but got %s with message %s",
                                   t.getClass().getSimpleName(),
                                   t.getMessage() == null ? "" : t.getMessage()));
        }

        AtomicBoolean rebufferCalled = new AtomicBoolean();
        rebufferInterceptor.setRebufferExecutionHandler((bufferHolder, throwable) ->
                                                        {
                                                            rebufferInterceptor.setRebufferExecutionHandler(null);
                                                            Assert.assertTrue(rebufferCalled.compareAndSet(false, true));
                                                            Assert.assertTrue(bufferHolder != null);
                                                        });
        // The same single disk read from the NORMAL table - chunk loading should be triggered again as the cache
        // shouldn't contain a future for the previous unsuccessful read, and the read should be successful.
        UntypedResultSet result = QueryProcessor.executeInternal(String.format("SELECT * FROM %s.%s WHERE key=1;", KEYSPACE, NORMAL));
        Assert.assertEquals(34, result.one().getInt("val"));
        Assert.assertTrue(rebufferCalled.get());

        // TODO dimitar.dimitrov Add a test about concurrent reads, where one throws, and the other tries to obtain the same
        //      chunk/chunk future.
    }

    /**
     * Creates a single-row SSTable for the NORMAL table and invalidates chunk cache entries about that SSTable.
     */
    private static void prepareChunkCache()
    {
        // Write a single row into the normal table, then flush it, so that follow-up reads for that table and partition will
        // have to read from disk.
        ColumnFamilyStore normalCfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(NORMAL);
        new RowUpdateBuilder(normalCfs.metadata(), 0, bytes(1)).add("val", 34).build().apply();
        normalCfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);

        // Invalidate everything from the chunk cache (except the SSTable partition index files) to make sure that follow-up
        // reads for that table trigger chunk cache loading.
        Set<File> sstableDataFilePaths = new HashSet<>();
        for (var dataDirectory : Directories.dataDirectories)
            addNonPartitionFiles(sstableDataFilePaths, dataDirectory.location);
        sstableDataFilePaths.forEach(ChunkCache.instance::invalidateFile);
    }

    /**
     * A wrapping rebufferer that allows customizable inspection and consumption of the results of the
     * {@link Rebufferer#rebuffer(long)} calls triggered by the inspected chunk cache.
     * Also implements {@link RebuffererFactory} in order to be used with the {@link ChunkCache#intercept(Function)} API,
     * even though it always returns itself as a {@link Rebufferer}.
     */
    private static class RebufferInterceptingRebufferer extends WrappingRebufferer implements RebuffererFactory
    {
        private volatile BiConsumer<BufferHolder, Throwable> rebufferExecutionHandler;

        RebufferInterceptingRebufferer(Rebufferer source)
        {
            super(source);
        }

        @Override
        public BufferHolder rebuffer(long position)
        {
            if (rebufferExecutionHandler == null)
                return wrapped.rebuffer(position);

            BufferHolder bufferHolder;
            try
            {
                bufferHolder = wrapped.rebuffer(position);
            }
            catch (Throwable t)
            {
                rebufferExecutionHandler.accept(null, t);
                throw t;
            }
            rebufferExecutionHandler.accept(bufferHolder, null);
            return bufferHolder;
        }

        /**
         * Allows the injection of a custom consumer for the results of the {@link #rebuffer(long)} calls
         * to the wrapped rebufferer.
         *
         * @param rebufferExecutionHandler The consumer of the results of the {@link #rebuffer(long)} calls
         *                                 to the wrapped rebufferer. If null, {@link #rebuffer(long)} calls
         *                                 to {@code RebufferInterceptingRebufferer} will just call the wrapped rebufferer.
         */
        void setRebufferExecutionHandler(BiConsumer<BufferHolder, Throwable> rebufferExecutionHandler)
        {
            this.rebufferExecutionHandler = rebufferExecutionHandler;
        }

        @Override
        public Rebufferer instantiateRebufferer(boolean isScan)
        {
            return this;
        }

        public void invalidateIfCached(long position)
        {
            // do nothing
        }
    }
}
