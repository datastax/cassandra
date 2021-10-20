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
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.disk.v2.PerSSTableFileProvider;
import org.apache.cassandra.index.sai.disk.v2.blockindex.BlockIndexFileProvider;
import org.apache.cassandra.index.sai.disk.v2.blockindex.BlockIndexMeta;
import org.apache.cassandra.index.sai.disk.v2.blockindex.BlockIndexReader;
import org.apache.cassandra.index.sai.disk.v2.blockindex.BlockIndexWriter;
import org.apache.cassandra.index.sai.disk.v2.blockindex.BytesUtil;
import org.apache.cassandra.index.sai.disk.v2.blockindex.MergeIndexIterators;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

@Fork(value = 1, jvmArgsAppend = {"-Dcassandra.sai.block.index.leaf.size=1024"})
@Warmup(iterations = 3)
@Measurement(iterations = 5, timeUnit = TimeUnit.MILLISECONDS)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
//@BenchmarkMode(Mode.Throughput)
@State(Scope.Thread)
public class BlockIndexBenchmark
{
    Descriptor descriptor;
    TableMetadata metadata;
    IndexDescriptor indexDescriptor;
    private PrimaryKey.PrimaryKeyFactory keyFactory;
    String index;
    IndexContext indexContext;

    int numRows = 1000000;
    int width = 10;
    int segments = 2;

    BlockIndexFileProvider fileProvider;

    List<BlockIndexMeta> metadatas = new ArrayList<>();
    List<BlockIndexReader> readers = new ArrayList<>();
    List<BlockIndexReader.IndexIterator> iterators = new ArrayList<>();

    MergeIndexIterators mergeIndexIterator;

    BlockIndexWriter writer;

    @Setup(Level.Trial)
    public void perTrialSetup() throws Throwable
    {
        DatabaseDescriptor.daemonInitialization();

        String keyspaceName = "ks";
        String tableName = this.getClass().getSimpleName();

        metadata = TableMetadata.builder(keyspaceName, tableName)
                                .partitioner(Murmur3Partitioner.instance)
                                .addPartitionKeyColumn("pk", Int32Type.instance)
                                .addClusteringColumn("a", UTF8Type.instance)
                                .addClusteringColumn("b", UTF8Type.instance)
                                .build();

        descriptor = new Descriptor(Files.createTempDirectory("jmh").toFile(), metadata.keyspace, metadata.name, 1);
        indexDescriptor = IndexDescriptor.create(descriptor, metadata);
        keyFactory = PrimaryKey.factory(metadata, Version.LATEST.onDiskFormat().indexFeatureSet());
        index = "test";
        indexContext = SAITester.createIndexContext(index, IntegerType.instance);

//        int numRows = CQLTester.getRandom().nextIntBetween(2000, 10000);
//        int width = CQLTester.getRandom().nextIntBetween(3, 8);
        numRows = (numRows / width) * width;

        List<PrimaryKey> expected = new ArrayList<>(numRows);

        for (int partitionKey = 0; partitionKey < numRows / width; partitionKey++)
        {
            for (int clustering = 0; clustering < width; clustering++)
                expected.add(keyFactory.createKey(makeKey(metadata, Integer.toString(partitionKey)),
                                               makeClustering(metadata, CQLTester.getRandom().nextAsciiString(2, 200), CQLTester.getRandom().nextAsciiString(2, 200))));
        }

        expected.sort(PrimaryKey::compareTo);

        int index = 0;


        for (int segment = 0; segment < segments; segment++)
        {
            try (BlockIndexFileProvider fileProvider = new PerSSTableFileProvider(indexDescriptor))
            {
                BlockIndexWriter writer = new BlockIndexWriter(fileProvider, true);

                for (int termId = index; termId < index + numRows / segments; termId++)
                {
                    PrimaryKey primaryKey = expected.get(termId);
                    writer.add(v -> primaryKey.asComparableBytes(v), termId);
                }
                index += numRows / segments;

                metadatas.add(writer.finish());
            }
        }

    }

    @TearDown
    public void perTrialTearDown() throws IOException
    {
    }

    @Setup(Level.Invocation)
    public void perInvocationSetup() throws IOException
    {
        fileProvider = new PerSSTableFileProvider(indexDescriptor);

        for (BlockIndexMeta metadata : metadatas)
        {
            BlockIndexReader reader = new BlockIndexReader(fileProvider, true, metadata);
            readers.add(reader);
            iterators.add(reader.iterator());
        }
        mergeIndexIterator = new MergeIndexIterators(iterators);

        writer = new BlockIndexWriter(fileProvider, false);
    }

    @Benchmark
//    @OperationsPerInvocation(100)
    @BenchmarkMode(Mode.AverageTime)
    public void iterateTerms(Blackhole bh) throws IOException
    {
        while (true)
        {
            BlockIndexReader.IndexState state = mergeIndexIterator.next();
            if (state == null)
            {
                break;
            }
            writer.add(BytesUtil.fixedLength(state.term), state.rowid);
//            bh.consume(state);
        }
    }

    private DecoratedKey makeKey(TableMetadata table, String...partitionKeys)
    {
        ByteBuffer key;
        if (TypeUtil.instance.isComposite(table.partitionKeyType))
            key = ((CompositeType)table.partitionKeyType).decompose(partitionKeys);
        else
            key = table.partitionKeyType.fromString(partitionKeys[0]);
        return table.partitioner.decorateKey(key);
    }

    private Clustering makeClustering(TableMetadata table, String...clusteringKeys)
    {
        Clustering clustering;
        if (table.comparator.size() == 0)
            clustering = Clustering.EMPTY;
        else
        {
            ByteBuffer[] values = new ByteBuffer[clusteringKeys.length];
            for (int index = 0; index < table.comparator.size(); index++)
                values[index] = table.comparator.subtype(index).fromString(clusteringKeys[index]);
            clustering = Clustering.make(values);
        }
        return clustering;
    }

}
