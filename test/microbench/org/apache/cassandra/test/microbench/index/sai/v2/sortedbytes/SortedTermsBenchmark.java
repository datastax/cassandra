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

package org.apache.cassandra.test.microbench.index.sai.v2.sortedbytes;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexComponentType;
import org.apache.cassandra.index.sai.disk.format.IndexComponents;
import org.apache.cassandra.index.sai.disk.v1.LongArray;
import org.apache.cassandra.index.sai.disk.v1.MetadataSource;
import org.apache.cassandra.index.sai.disk.v1.MetadataWriter;
import org.apache.cassandra.index.sai.disk.v1.bitpack.NumericValuesMeta;
import org.apache.cassandra.index.sai.disk.v1.bitpack.NumericValuesWriter;
import org.apache.cassandra.index.sai.disk.v2.sortedterms.SortedTermsMeta;
import org.apache.cassandra.index.sai.disk.v2.sortedterms.SortedTermsReader;
import org.apache.cassandra.index.sai.disk.v2.sortedterms.SortedTermsWriter;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.test.microbench.index.sai.v1.AbstractOnDiskBenchmark;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

@Fork(value = 1, jvmArgsAppend = {
        //        "-XX:+UnlockCommercialFeatures", "-XX:+FlightRecorder","-XX:+UnlockDiagnosticVMOptions", "-XX:+DebugNonSafepoints",
        //        "-XX:StartFlightRecording=duration=60s,filename=./BlockPackedReaderBenchmark.jfr,name=profile,settings=profile",
        //                            "-XX:FlightRecorderOptions=settings=/home/jake/workspace/cassandra/profiling-advanced.jfc,samplethreads=true"
})
@Warmup(iterations = 1)
@Measurement(iterations = 1, timeUnit = TimeUnit.MICROSECONDS)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Thread)
public class SortedTermsBenchmark extends AbstractOnDiskBenchmark
{
    private static final int NUM_ROWS = 1_000_000;
    private static final int NUM_INVOCATIONS = 1_000; // must be <= (NUM_ROWS / max(skippingDistance))

    @Param({ "1", "10", "100", "1000"})
    public int skippingDistance;

    protected LongArray rowIdToToken;
    private int[] rowIds;
    private long[] tokenValues;
    FileHandle trieFile;
    FileHandle termsData;
    FileHandle blockOffsets;
    SortedTermsReader sortedTermsReader;
    Path luceneDir;
    Directory directory;
    DirectoryReader luceneReader;
    SortedDocValues columnASortedDocValues;

    @Override
    public int numRows()
    {
        return NUM_ROWS;
    }

    @Override
    public int numPostings()
    {
        return NUM_ROWS;
    }

    byte[][] bcIntBytes = new byte[NUM_ROWS][];

    @Setup(Level.Trial)
    public void perTrialSetup2() throws IOException
    {
        IndexComponents.ForWrite components = indexDescriptor.newPerSSTableComponentsForWrite();
        try (MetadataWriter metadataWriter = new MetadataWriter(components);
             NumericValuesWriter blockFPWriter = new NumericValuesWriter(components.addOrGet(IndexComponentType.PRIMARY_KEY_BLOCK_OFFSETS),
                                                                         metadataWriter, true);
             SortedTermsWriter writer = new SortedTermsWriter(components.addOrGet(IndexComponentType.PRIMARY_KEY_BLOCKS),
                                                              metadataWriter,
                                                              blockFPWriter,
                                                              components.addOrGet(IndexComponentType.PRIMARY_KEY_TRIE)))
        {
            for (int x = 0; x < NUM_ROWS; x++)
            {
                ByteBuffer buffer = Int32Type.instance.decompose(x);
                ByteSource byteSource = Int32Type.instance.asComparableBytes(buffer, TypeUtil.BYTE_COMPARABLE_VERSION);
                byte[] bytes = ByteSourceInverse.readBytes(byteSource);
                bcIntBytes[x] = bytes;
                writer.add(ByteComparable.preencoded(TypeUtil.BYTE_COMPARABLE_VERSION, bytes));
            }
        }

        // create the lucene index
        luceneDir = Files.createTempDirectory("jmh_lucene_test");
        directory = FSDirectory.open(luceneDir);
        IndexWriterConfig config = new IndexWriterConfig(new WhitespaceAnalyzer());
        IndexWriter indexWriter = new IndexWriter(directory, config);

        Document document = new Document();

        int i = 0;
        for (int x = 0; x < NUM_ROWS; x++)
        {
            document.clear();
            byte[] bytes = new byte[4];
            NumericUtils.intToSortableBytes(x, bytes, 0);
            document.add(new SortedDocValuesField("columnA", new BytesRef(bytes)));
            indexWriter.addDocument(document);
            luceneBytes[x] = bytes;
        }
        indexWriter.forceMerge(1);
        indexWriter.close();
    }

    byte[][] luceneBytes = new byte[NUM_ROWS][];

    @Override
    public void beforeInvocation() throws Throwable
    {
        // rowIdToToken.findTokenRowID keeps track of last position, so it must be per-benchmark-method-invocation.
        rowIdToToken = openRowIdToTokenReader();

        rowIds = new int[NUM_ROWS];
        tokenValues = new long[NUM_ROWS];

        IndexComponents.ForRead components = indexDescriptor.perSSTableComponents();
        MetadataSource metadataSource = MetadataSource.loadMetadata(components);
        IndexComponent.ForRead blocksComponent = components.get(IndexComponentType.PRIMARY_KEY_BLOCKS);
        IndexComponent.ForRead blockOffsetsComponent = components.get(IndexComponentType.PRIMARY_KEY_BLOCK_OFFSETS);
        NumericValuesMeta blockOffsetMeta = new NumericValuesMeta(metadataSource.get(blockOffsetsComponent));
        SortedTermsMeta sortedTermsMeta = new SortedTermsMeta(metadataSource.get(blocksComponent));
        trieFile = components.get(IndexComponentType.PRIMARY_KEY_TRIE).createFileHandle();
        termsData = blocksComponent.createFileHandle();
        blockOffsets = blockOffsetsComponent.createFileHandle();

        sortedTermsReader = new SortedTermsReader(termsData,blockOffsets, trieFile, sortedTermsMeta, blockOffsetMeta);

        luceneReader = DirectoryReader.open(directory);
        LeafReaderContext context = luceneReader.leaves().get(0);

        columnASortedDocValues = context.reader().getSortedDocValues("columnA");
    }

    @Override
    public void afterInvocation() throws Throwable
    {
        luceneReader.close();
        termsData.close();
        blockOffsets.close();
        rowIdToToken.close();
        trieFile.close();
    }

    @Benchmark
    @OperationsPerInvocation(NUM_INVOCATIONS)
    @BenchmarkMode({ Mode.Throughput})
    public void luceneSeekToPointID(Blackhole bh) throws IOException
    {
        for (int i = 0; i < NUM_INVOCATIONS;)
        {
            bh.consume(columnASortedDocValues.lookupOrd(i));
            i += skippingDistance;
        }
    }

    @Benchmark
    @OperationsPerInvocation(NUM_INVOCATIONS)
    @BenchmarkMode({ Mode.Throughput})
    public void luceneSeekToTerm(Blackhole bh) throws IOException
    {
        for (int i = 0; i < NUM_INVOCATIONS; i++)
        {
            bh.consume(columnASortedDocValues.lookupTerm(new BytesRef(luceneBytes[i * skippingDistance])));
        }
    }

    @Benchmark
    @OperationsPerInvocation(NUM_INVOCATIONS)
    @BenchmarkMode({ Mode.Throughput})
    public void advance(Blackhole bh) throws IOException
    {
        try (SortedTermsReader.Cursor cursor = sortedTermsReader.openCursor())
        {
            for (int i = 0; i < NUM_INVOCATIONS; i++)
            {
                cursor.advance();
                bh.consume(cursor.term());
            }
        }
    }

    @Benchmark
    @OperationsPerInvocation(NUM_INVOCATIONS)
    @BenchmarkMode({ Mode.Throughput})
    public void seekToPointID(Blackhole bh) throws IOException
    {
        try (SortedTermsReader.Cursor cursor = sortedTermsReader.openCursor())
        {
            for (int i = 0; i < NUM_INVOCATIONS; i++)
            {
                cursor.seekToPointId((long) i * skippingDistance);
                bh.consume(cursor.term());
            }
        }
    }

    @Benchmark
    @OperationsPerInvocation(NUM_INVOCATIONS)
    @BenchmarkMode({ Mode.Throughput})
    public void seekToTerm(Blackhole bh) throws IOException
    {
        try (SortedTermsReader.Cursor cursor = sortedTermsReader.openCursor())
        {
            for (int i = 0; i < NUM_INVOCATIONS; i++)
            {
                int iFinal = i;
                bh.consume(cursor.ceiling(v -> ByteSource.preencoded(this.bcIntBytes[iFinal * skippingDistance])));
            }
        }
    }

    @Benchmark
    @OperationsPerInvocation(NUM_INVOCATIONS)
    @BenchmarkMode({ Mode.Throughput})
    public void get(Blackhole bh)
    {
        for (int i = 0; i < NUM_INVOCATIONS; i++)
        {
            bh.consume(rowIdToToken.get(rowIds[i * skippingDistance]));
        }
    }

    @Benchmark
    @OperationsPerInvocation(NUM_INVOCATIONS)
    @BenchmarkMode({ Mode.Throughput})
    public void longArrayFindTokenRowID(Blackhole bh)
    {
        for (int i = 0; i < NUM_INVOCATIONS; i++)
        {
            bh.consume(rowIdToToken.ceilingRowId(tokenValues[i * skippingDistance]));
        }
    }
}
