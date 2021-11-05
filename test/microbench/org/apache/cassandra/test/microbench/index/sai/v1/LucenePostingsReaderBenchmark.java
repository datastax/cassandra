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

package org.apache.cassandra.test.microbench.index.sai.v1;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.disk.v1.PostingsReader;
import org.apache.cassandra.index.sai.disk.v2.lucene8xpostings.Lucene8xIndexInput;
import org.apache.cassandra.index.sai.disk.v2.lucene8xpostings.LuceneMMap;
import org.apache.cassandra.index.sai.disk.v2.lucene8xpostings.LucenePostingsReader;
import org.apache.cassandra.index.sai.disk.v2.lucene8xpostings.LucenePostingsWriter;
import org.apache.cassandra.index.sai.utils.ArrayPostingList;
import org.apache.cassandra.index.sai.utils.LongArray;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
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

@Fork(1)
@Warmup(iterations = 5)
@Measurement(iterations = 10, timeUnit = TimeUnit.MILLISECONDS)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Thread)
public class LucenePostingsReaderBenchmark extends AbstractOnDiskBenchmark
{
    private static final int NUM_INVOCATIONS = 1_000_000;

    @Param({ "1", "10", "100", "1000"})
    public int skippingDistance;

    protected LongArray rowIdToToken;
    protected PostingsReader reader;
    private int[] rowIds;

    protected PostingsEnum lucenePostings;
    protected DirectoryReader luceneReader;
    protected Directory directory;

    private long lucene8xPostingsFP = -1;

    private FileHandle lucene8xHandle;
    private Lucene8xIndexInput lucene8xInput;
    private LucenePostingsReader lucene8xPostingsReader;

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

    @Setup(Level.Trial)
    public void perTrialSetup2() throws IOException
    {
        rowIds = new int[NUM_INVOCATIONS];
        for (int i = 0; i <NUM_INVOCATIONS; i++)
        {
            rowIds[i] = i;
        }

        // write lucene 8x postings
        try (IndexOutput postingsOut = indexComponents.createOutput(indexComponents.kdTreePostingLists))
        {
            LucenePostingsWriter postingsWriter = new LucenePostingsWriter(postingsOut, 128, rowIds.length);

            lucene8xPostingsFP = postingsWriter.write(new ArrayPostingList(rowIds));
        }

        // write lucene 7.5 index
        Path luceneDir = Files.createTempDirectory("jmh_lucene_test");
        directory = FSDirectory.open(luceneDir);
        IndexWriterConfig config = new IndexWriterConfig(new WhitespaceAnalyzer());
        IndexWriter indexWriter = new IndexWriter(directory, config);

        StringField field = new StringField("columnA", "value", Field.Store.NO);
        Document document = new Document();
        document.add(field);
        for (int x = 0; x < NUM_INVOCATIONS; x++)
        {
            indexWriter.addDocument(document);
        }
        indexWriter.forceMerge(1);
        indexWriter.close();
    }

    @Override
    public void beforeInvocation() throws Throwable
    {
        // rowIdToToken.findTokenRowID keeps track of last position, so it must be per-benchmark-method-invocation.
        rowIdToToken = openRowIdToTokenReader();
        reader = openPostingsReader();

        // open lucene 8x postings
        lucene8xHandle = indexComponents.createFileHandle(indexComponents.kdTreePostingLists);
        lucene8xInput = LuceneMMap.openLuceneInput(lucene8xHandle);
        lucene8xPostingsReader = new LucenePostingsReader(lucene8xInput,
                                                          128,
                                                          this.lucene8xPostingsFP);

        // open lucene 7.5 postings from the lucene index
        luceneReader = DirectoryReader.open(directory);
        LeafReaderContext context = luceneReader.leaves().get(0);
        lucenePostings = context.reader().postings(new Term("columnA", new BytesRef("value")));
    }

    @Override
    public void afterInvocation() throws Throwable
    {
        lucene8xInput.close();
        lucene8xHandle.close();
        rowIdToToken.close();
        reader.close();
        luceneReader.close();
    }

    @Benchmark
    @OperationsPerInvocation(NUM_INVOCATIONS)
    @BenchmarkMode({ Mode.Throughput})
    public void advanceLucene8x(Blackhole bh) throws Throwable
    {
        int i = 0;
        while (true)
        {
            if (i > rowIds.length - 1) break;

            int rowId = rowIds[i];
            long advanceRowId = lucene8xPostingsReader.advance(rowId);
            if (advanceRowId == PostingList.END_OF_STREAM)
            {
                break;
            }
            bh.consume(advanceRowId);
            i += skippingDistance;
        }
    }

    @Benchmark
    @OperationsPerInvocation(NUM_INVOCATIONS)
    @BenchmarkMode({ Mode.Throughput})
    public void advanceLucene(Blackhole bh) throws Throwable
    {
        int i = 0;
        while (true)
        {
            if (i > rowIds.length - 1) break;

            int rowId = rowIds[i];
            int docid = lucenePostings.advance(rowId);
            if (docid == PostingsEnum.NO_MORE_DOCS)
            {
                break;
            }
            bh.consume(docid);
            i += skippingDistance;
        }
    }

    @Benchmark
    @OperationsPerInvocation(NUM_INVOCATIONS)
    @BenchmarkMode({ Mode.Throughput})
    public void advance(Blackhole bh) throws Throwable
    {
        int i = 0;
        while (true)
        {
            if (i > rowIds.length - 1) break;

            int rowId = rowIds[i];
            long advanceRowId = reader.advance(rowId);
            if (advanceRowId == PostingList.END_OF_STREAM)
            {
                break;
            }
            bh.consume(advanceRowId);
            i += skippingDistance;
        }
    }
}
