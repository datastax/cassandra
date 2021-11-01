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
import org.apache.cassandra.index.sai.disk.io.IndexOutputWriter;
import org.apache.cassandra.index.sai.disk.v1.LongArray;
import org.apache.cassandra.index.sai.disk.v2.sortedbytes.SortedBytesReader;
import org.apache.cassandra.index.sai.disk.v2.sortedbytes.SortedBytesWriter;
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
import org.apache.lucene.store.IndexInput;
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
public class SortedBytesBenchmark extends AbstractOnDiskBenchmark
{
    private static final int NUM_INVOCATIONS = 1_000_000;

    @Param({ "1", "10", "100", "1000"})
    public int skippingDistance;

    protected LongArray rowIdToToken;
    private int[] rowIds;
    private long[] tokenValues;
    FileHandle trieFile;
    IndexInput bytesInput, blockFPInput;
    SortedBytesReader sortedBytesReader;
    Path luceneDir;
    Directory directory;
    DirectoryReader luceneReader;
    SortedDocValues columnASortedDocValues;

    @Override
    public int numRows()
    {
        return NUM_INVOCATIONS;
    }

    @Override
    public int numPostings()
    {
        return NUM_INVOCATIONS;
    }

    SortedBytesWriter.Meta meta = null;
    byte[][] bcIntBytes = new byte[NUM_INVOCATIONS][];

    @Setup(Level.Trial)
    public void perTrialSetup2() throws IOException
    {

        try (IndexOutputWriter trieWriter = indexDescriptor.openPerSSTableOutput(IndexComponent.KD_TREE);
             IndexOutputWriter bytesWriter = indexDescriptor.openPerSSTableOutput(IndexComponent.TERMS_DATA);
             IndexOutputWriter blockFPWriter = indexDescriptor.openPerSSTableOutput(IndexComponent.KD_TREE_POSTING_LISTS))
        {
            SortedBytesWriter writer = new SortedBytesWriter(trieWriter,
                                                             bytesWriter,
                                                             blockFPWriter);

            for (int x = 0; x < NUM_INVOCATIONS; x++)
            {
                ByteBuffer buffer = Int32Type.instance.decompose(x);
                ByteSource byteSource = Int32Type.instance.asComparableBytes(buffer, ByteComparable.Version.OSS41);
                byte[] bytes = ByteSourceInverse.readBytes(byteSource);
                bcIntBytes[x] = bytes;
                writer.add(ByteComparable.fixedLength(bytes));
            }

            meta = writer.finish();
        }

        // create the lucene index
        luceneDir = Files.createTempDirectory("jmh_lucene_test");
        directory = FSDirectory.open(luceneDir);
        IndexWriterConfig config = new IndexWriterConfig(new WhitespaceAnalyzer());
        IndexWriter indexWriter = new IndexWriter(directory, config);

        Document document = new Document();

        int i = 0;
        for (int x = 0; x < NUM_INVOCATIONS; x++)
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

    byte[][] luceneBytes = new byte[NUM_INVOCATIONS][];

    @Override
    public void beforeInvocation() throws Throwable
    {
        // rowIdToToken.findTokenRowID keeps track of last position, so it must be per-benchmark-method-invocation.
        rowIdToToken = openRowIdToTokenReader();

        rowIds = new int[NUM_INVOCATIONS];
        tokenValues = new long[NUM_INVOCATIONS];

        trieFile = indexDescriptor.createPerSSTableFileHandle(IndexComponent.KD_TREE);
        bytesInput = indexDescriptor.openPerSSTableInput(IndexComponent.TERMS_DATA);
        blockFPInput = indexDescriptor.openPerSSTableInput(IndexComponent.KD_TREE_POSTING_LISTS);

        sortedBytesReader = new SortedBytesReader(meta,
                                                  trieFile,
                                                  blockFPInput);

        luceneReader = DirectoryReader.open(directory);
        LeafReaderContext context = luceneReader.leaves().get(0);

        columnASortedDocValues = context.reader().getSortedDocValues("columnA");
    }

    @Override
    public void afterInvocation() throws Throwable
    {
        luceneReader.close();
        bytesInput.close();
        blockFPInput.close();
        rowIdToToken.close();
        trieFile.close();
    }

    @Benchmark
    @OperationsPerInvocation(NUM_INVOCATIONS)
    @BenchmarkMode({ Mode.Throughput})
    public void luceneBytesSeekToPointID(Blackhole bh) throws IOException
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
    public void luceneBytesSeekToTerm(Blackhole bh) throws IOException
    {
        for (int i = 0; i < NUM_INVOCATIONS;)
        {
            bh.consume(columnASortedDocValues.lookupTerm(new BytesRef(luceneBytes[i])));
            i += skippingDistance;
        }
    }

    @Benchmark
    @OperationsPerInvocation(NUM_INVOCATIONS)
    @BenchmarkMode({ Mode.Throughput})
    public void bytesSeekToPointID(Blackhole bh) throws IOException
    {
        for (int i = 0; i < NUM_INVOCATIONS;)
        {
            bh.consume(sortedBytesReader.seekExact(i, bytesInput));
            i += skippingDistance;
        }
    }

    @Benchmark
    @OperationsPerInvocation(NUM_INVOCATIONS)
    @BenchmarkMode({ Mode.Throughput})
    public void bytesSeekToTerm(Blackhole bh) throws IOException
    {
        for (int i = 0; i < NUM_INVOCATIONS;)
        {
            bh.consume(sortedBytesReader.seekToBytes(ByteComparable.fixedLength(this.bcIntBytes[i])));
            i += skippingDistance;
        }
    }

    @Benchmark
    @OperationsPerInvocation(NUM_INVOCATIONS)
    @BenchmarkMode({ Mode.Throughput})
    public void get(Blackhole bh)
    {
        for (int i = 0; i < NUM_INVOCATIONS;)
        {
            bh.consume(rowIdToToken.get(rowIds[i]));
            i += skippingDistance;
        }
    }

    @Benchmark
    @OperationsPerInvocation(NUM_INVOCATIONS)
    @BenchmarkMode({ Mode.Throughput})
    public void findTokenRowID(Blackhole bh)
    {
        for (int i = 0; i < NUM_INVOCATIONS;)
        {
            bh.consume(rowIdToToken.findTokenRowID(tokenValues[i]));
            i += skippingDistance;
        }
    }
}
