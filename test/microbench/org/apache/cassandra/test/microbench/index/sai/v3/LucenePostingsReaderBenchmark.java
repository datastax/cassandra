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

package org.apache.cassandra.test.microbench.index.sai.v3;

import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v1.postings.PostingsReader;
import org.apache.cassandra.index.sai.disk.v3.postings.PFoRPostingsReader;
import org.apache.cassandra.index.sai.disk.v3.postings.PFoRPostingsWriter;
import org.apache.cassandra.index.sai.disk.v3.postings.lucene8xpostings.Lucene8xIndexInput;
import org.apache.cassandra.index.sai.disk.v3.postings.lucene8xpostings.LuceneMMap;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableIdFactory;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.test.microbench.index.sai.v1.AbstractOnDiskBenchmark;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

@Fork(1)
@Warmup(iterations = 2)
@Measurement(iterations = 4, timeUnit = TimeUnit.MILLISECONDS)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Thread)
public class LucenePostingsReaderBenchmark extends AbstractOnDiskBenchmark
{
    private static final int NUM_INVOCATIONS = 100_000;

    @Param({ "1", "10", "100", "1000", "3000"})
    public int skippingDistance;

    protected PostingsReader reader;
    private int[] rowIds;

    protected PostingsEnum lucenePostings;
    protected DirectoryReader luceneReader;
    protected Directory directory;

    //private long lucene8xPostingsFP = -1;

    protected long newPostingsFP = -1;
    private Lucene8xIndexInput newPostingsInput;
    private FileHandle newPostingsHandle;

//    private FileHandle lucene8xHandle;
//    private Lucene8xIndexInput lucene8xInput;
//    private LucenePostingsReader lucene8xPostingsReader;

    protected PFoRPostingsReader newPostingsReader;

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
        String keyspaceName = "ks";
        String tableName = this.getClass().getSimpleName();
        metadata = TableMetadata
                .builder(keyspaceName, tableName)
                .partitioner(Murmur3Partitioner.instance)
                .addPartitionKeyColumn("pk", UTF8Type.instance)
                .addRegularColumn("col", IntegerType.instance)
                .build();

        Descriptor descriptor = new Descriptor(new File(Files.createTempDirectory("jmh").toFile()),
                metadata.keyspace,
                metadata.name,
                SSTableIdFactory.instance.defaultBuilder().generator(Stream.empty()).get());
        indexDescriptor = IndexDescriptor.create(descriptor, metadata.partitioner, metadata.comparator);
        String index = "test";
        indexContext = SAITester.createIndexContext(index, IntegerType.instance);

        rowIds = new int[NUM_INVOCATIONS];
        for (int i = 0; i < NUM_INVOCATIONS; i++)
        {
            rowIds[i] = i;
        }

//        // write lucene 8x postings
//        try (IndexOutput postingsOut = indexComponents.createOutput(indexComponents.kdTreePostingLists))
//        {
//            LucenePostingsWriter postingsWriter = new LucenePostingsWriter(postingsOut, 128, rowIds.length);
//
//            lucene8xPostingsFP = postingsWriter.write(new ArrayPostingList(rowIds));
//        }

        try (IndexOutput newPostingsOut = indexDescriptor.openPerIndexOutput(IndexComponent.META, indexContext))
        {
            PFoRPostingsWriter newPostingsWriter = new PFoRPostingsWriter();
            for (int x = 0; x < rowIds.length; x++)
            {
                newPostingsWriter.add(rowIds[x]);
            }
            newPostingsFP = newPostingsWriter.finish(newPostingsOut);
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

    IndexContext indexContext;

    @Override
    public void beforeInvocation() throws Throwable
    {
        reader = openPostingsReader();

        // open new postings
        newPostingsHandle = indexDescriptor.createPerIndexFileHandle(IndexComponent.META, indexContext);
        newPostingsInput = LuceneMMap.openLuceneInput(newPostingsHandle);
        newPostingsReader = new PFoRPostingsReader(newPostingsFP, newPostingsInput);

//        // open lucene 8x postings
//        lucene8xHandle = indexComponents.createFileHandle(indexComponents.kdTreePostingLists);
//        lucene8xInput = LuceneMMap.openLuceneInput(lucene8xHandle);
//        lucene8xPostingsReader = new LucenePostingsReader(lucene8xInput, 128);
//        lucene8xPostingsReader.reset(this.lucene8xPostingsFP);

        // open lucene 7.5 postings from the lucene index
        luceneReader = DirectoryReader.open(directory);
        LeafReaderContext context = luceneReader.leaves().get(0);
        lucenePostings = context.reader().postings(new Term("columnA", new BytesRef("value")));
    }

    @Override
    public void afterInvocation() throws Throwable
    {
//        lucene8xInput.close();
//        lucene8xHandle.close();
        reader.close();
        luceneReader.close();
        newPostingsInput.close();
        newPostingsHandle.close();
    }

    @Benchmark
    @OperationsPerInvocation(NUM_INVOCATIONS)
    @BenchmarkMode({ Mode.Throughput})
    public void advanceNewPostings(Blackhole bh) throws Throwable
    {
        int i = 0;
        while (true)
        {
            if (i > rowIds.length - 1) break;

            int rowId = rowIds[i];
            long advanceRowId = newPostingsReader.advance(rowId);
            if (advanceRowId == PostingList.END_OF_STREAM)
            {
                break;
            }
            bh.consume(advanceRowId);
            i += skippingDistance;
        }
    }

//    @Benchmark
//    @OperationsPerInvocation(NUM_INVOCATIONS)
//    @BenchmarkMode({ Mode.Throughput})
//    public void advanceLucene8x(Blackhole bh) throws Throwable
//    {
//        int i = 0;
//        while (true)
//        {
//            if (i > rowIds.length - 1) break;
//
//            int rowId = rowIds[i];
//            long advanceRowId = lucene8xPostingsReader.advance(rowId);
//            if (advanceRowId == PostingList.END_OF_STREAM)
//            {
//                break;
//            }
//            bh.consume(advanceRowId);
//            i += skippingDistance;
//        }
//    }

    @Benchmark
    @OperationsPerInvocation(NUM_INVOCATIONS)
    @BenchmarkMode({ Mode.Throughput})
    public void advanceLucene75(Blackhole bh) throws Throwable
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
    public void advanceSAI(Blackhole bh) throws Throwable
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
