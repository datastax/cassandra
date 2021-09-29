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

package org.apache.cassandra.test.microbench.index.sai.v2;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;

import com.carrotsearch.randomizedtesting.generators.RandomStrings;
import org.apache.cassandra.cache.ChunkCache;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.tries.MemtableTrie;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.disk.PerSSTableWriter;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.disk.v1.PostingsReader;
import org.apache.cassandra.index.sai.disk.v1.PostingsWriter;
import org.apache.cassandra.index.sai.metrics.QueryEventListener;
import org.apache.cassandra.index.sai.utils.ArrayPostingList;
import org.apache.cassandra.index.sai.utils.IndexFileUtils;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.lucene.store.IndexInput;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.TearDown;

public abstract class AbstractOnDiskBenchmark
{
    private static Random random = new Random();

    private Descriptor descriptor;

    TableMetadata metadata;
    IndexDescriptor indexDescriptor;
    private PrimaryKey.PrimaryKeyFactory keyFactory;
    private FileHandle primaryKeys;
    PrimaryKeyMap primaryKeyMap;
    String index;
    IndexContext indexContext;

    private FileHandle postings;
    private long summaryPosition;


    /**
     * @return num of rows to be stored in per-sstable components
     */
    public abstract int numRows();

    /**
     * @return num of postings to be written in posting file
     */
    public abstract int numPostings();

    /**
     * To be called before executing each @Benchmark method
     */
    public abstract void beforeInvocation() throws Throwable;

    /**
     * To be called after executing each @Benchmark method
     */
    public abstract void afterInvocation() throws Throwable;

    protected int toPosting(int id)
    {
        return id;
    }

    @Setup(Level.Trial)
    public void perTrialSetup() throws IOException, MemtableTrie.SpaceExhaustedException
    {
        DatabaseDescriptor.daemonInitialization(); // required to use ChunkCache
        assert ChunkCache.instance != null;

        String keyspaceName = "ks";
        String tableName = this.getClass().getSimpleName();
        metadata = TableMetadata
                .builder(keyspaceName, tableName)
                .partitioner(Murmur3Partitioner.instance)
                .addPartitionKeyColumn("pk", UTF8Type.instance)
                .addRegularColumn("col", IntegerType.instance)
                .build();

        descriptor = new Descriptor(Files.createTempDirectory("jmh").toFile(), metadata.keyspace, metadata.name, 1);
        indexDescriptor = IndexDescriptor.create(descriptor, metadata);
        keyFactory = PrimaryKey.factory(metadata, Version.LATEST.onDiskFormat().indexFeatureSet());
        index = "test";
        indexContext = SAITester.createIndexContext(index, IntegerType.instance);

        writePrimaryKeysComponent(numRows());
        primaryKeys = indexDescriptor.createPerSSTableFileHandle(IndexComponent.PRIMARY_KEYS);
        primaryKeyMap = indexDescriptor.newPrimaryKeyMapFactory(null).newPerSSTablePrimaryKeyMap(null);

        // write postings
        summaryPosition = writePostings(numPostings());
        postings = indexDescriptor.createPerIndexFileHandle(IndexComponent.POSTING_LISTS, SAITester.createIndexContext(index, UTF8Type.instance));
    }

    @TearDown(Level.Trial)
    public void perTrialTearDown() throws IOException
    {
        primaryKeys.close();
        postings.close();
        FileUtils.deleteRecursive(descriptor.directory);
    }

    @Setup(Level.Invocation)
    public void perInvocationSetup() throws Throwable
    {
        beforeInvocation();
    }

    @TearDown(Level.Invocation)
    public void perInvocationTearDown() throws Throwable
    {
        afterInvocation();
    }

    private long writePostings(int rows) throws IOException
    {
        final int[] postings = IntStream.range(0, rows).map(this::toPosting).toArray();
        final ArrayPostingList postingList = new ArrayPostingList(postings);

        try (PostingsWriter writer = new PostingsWriter(indexDescriptor, indexContext, false))
        {
            long summaryPosition = writer.write(postingList);
            writer.complete();

            return summaryPosition;
        }
    }

    protected final PostingsReader openPostingsReader() throws IOException
    {
        IndexInput input = IndexFileUtils.instance.openInput(postings);
        IndexInput summaryInput = IndexFileUtils.instance.openInput(postings);
        PostingsReader.BlocksSummary summary = new PostingsReader.BlocksSummary(summaryInput, summaryPosition);
        return new PostingsReader(input, summary, QueryEventListener.PostingListEventListener.NO_OP);
    }

    /**
     * Generates a number of unique, random primary keys and writes them to the PRIMARY_KEYS component of the sstable
     */
    private void writePrimaryKeysComponent(int rowCount) throws IOException, MemtableTrie.SpaceExhaustedException
    {
        Random rng = ThreadLocalRandom.current();
        IPartitioner partitioner = Murmur3Partitioner.instance;
        PrimaryKey.PrimaryKeyFactory factory = PrimaryKey.factory(metadata, Version.LATEST.onDiskFormat().indexFeatureSet());
        SortedSet<DecoratedKey> primaryKeys = new TreeSet<>();
        while (primaryKeys.size() < rowCount)
        {
            String keyStr = RandomStrings.randomAsciiOfLength(rng, 8);
            DecoratedKey dk = partitioner.decorateKey(UTF8Type.instance.decompose(keyStr));
            primaryKeys.add(dk);
        }

        PerSSTableWriter writer = indexDescriptor.newPerSSTableWriter();
        long rowId = 0;
        for (DecoratedKey dk: primaryKeys)
        {
            PrimaryKey pk = factory.createKey(dk, Clustering.EMPTY, rowId);
            writer.nextRow(pk);
            rowId++;
        }
        writer.complete();
    }

}
