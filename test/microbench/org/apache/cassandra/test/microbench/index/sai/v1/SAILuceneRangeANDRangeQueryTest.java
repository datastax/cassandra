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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import io.netty.util.internal.ThreadLocalRandom;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
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
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
//@Warmup(iterations = 100000, time = 1, timeUnit = TimeUnit.SECONDS)
//@Measurement(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
@Warmup(iterations = 2, time = 5, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 3, time = 20, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1)
@Threads(1)
@State(Scope.Benchmark)
public class SAILuceneRangeANDRangeQueryTest extends CQLTester
{
    public static int ROWS = 1_000_000;

    static String keyspace;
    String table;
    String writeStatement;
    String readStatement;
    ColumnFamilyStore cfs;
    FSDirectory directory;
    DirectoryReader luceneReader;
    IndexSearcher indexSearcher;
    int luceneQueryCount = 0;
    int luceneQueriesWithResults = 0;
    int saiQueriesWithResults = 0;
    int saiQueryCount = 0;

    @Setup(Level.Trial)
    public void setup() throws Throwable
    {
        CQLTester.setUpClass();
        keyspace = createKeyspace("CREATE KEYSPACE %s with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 } and durable_writes = false");
        table = createTable(keyspace, "CREATE TABLE %s (key int, columnA int, columnB int, PRIMARY KEY(key))");
        createIndex("CREATE CUSTOM INDEX ON "+keyspace+"."+table+"(columnA) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON "+keyspace+"."+table+"(columnB) USING 'StorageAttachedIndex'");

        execute("USE "+keyspace+";");
        writeStatement = "INSERT INTO "+table+" (key, columnA, columnB) VALUES (?, ?, ?)";
        readStatement = "SELECT * FROM "+table+" WHERE columnA >= ? AND columnA <= ? AND columnB >= ? AND columnB <= ? LIMIT 10";

        cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
        cfs.disableAutoCompaction();

        Path luceneDir = Files.createTempDirectory("jmh_lucene_test");
        directory = FSDirectory.open(luceneDir);
        IndexWriterConfig config = new IndexWriterConfig(new KeywordAnalyzer());
        config.setRAMBufferSizeMB(1000);
        IndexWriter indexWriter = new IndexWriter(directory, config);

        Document document = new Document();

        int[] colAValues = new int[ROWS];
        int[] colBValues = new int[ROWS];

        System.err.println("Writing "+ROWS+" rows");

        long saiIndexStart = System.currentTimeMillis();

        for (int i = 0; i < ROWS; i++)
        {
            int columnAValue = genColumnA();
            int columnBValue = genColumnB();

            colAValues[i] = columnAValue;
            colBValues[i] = columnBValue;

            execute(writeStatement, i, columnAValue, columnBValue);

//            if (i % (ROWS / 10) == 0)
//            {
//                flush(keyspace);
//            }
        }

        long saiIndexingDuration = System.currentTimeMillis() - saiIndexStart;

        long luceneIndexingStart = System.currentTimeMillis();

        for (int i = 0; i < ROWS; i++)
        {
            int columnAValue = colAValues[i];
            int columnBValue = colBValues[i];

            document.clear();
            document.add(new StringField("key", Integer.toString(i), Field.Store.YES));
            document.add(new IntPoint("columnA", columnAValue));
            document.add(new IntPoint("columnB", columnBValue));
            document.add(new StoredField("columnA", columnAValue));
            document.add(new StoredField("columnB", columnBValue));
            indexWriter.addDocument(document);

            // create 100 segments
            // create 100 sstable indexes

//            if (i % (ROWS / 10) == 0)
//            {
//                indexWriter.flush();
//            }
        }

        long luceneIndexingDuration = System.currentTimeMillis() - luceneIndexingStart;

        long luceneMergeStart = System.currentTimeMillis();
        indexWriter.forceMerge(1);
        long luceneMergeDuration = System.currentTimeMillis() - luceneMergeStart;

        indexWriter.close();

        long dbCompactStart = System.currentTimeMillis();
        compact(keyspace);
        long dbCompactDuration = System.currentTimeMillis() - dbCompactStart;

        System.out.println("saiIndexingDuration="+saiIndexingDuration+" luceneIndexingDuration="+luceneIndexingDuration+" luceneMergeDuration="+luceneMergeDuration+" dbCompactDuration="+dbCompactDuration);

        luceneReader = DirectoryReader.open(directory);

        if (luceneReader.maxDoc() != ROWS)
        {
            throw new IllegalStateException("luceneReader.maxDoc="+luceneReader.maxDoc()+" rows="+ROWS);
        }

        indexSearcher = new IndexSearcher(luceneReader);
        indexSearcher.setQueryCache(null); // disable the default query cache
    }

    @TearDown(Level.Trial)
    public void teardown() throws IOException, ExecutionException, InterruptedException
    {
        System.out.println("luceneQueriesWithResults="+luceneQueriesWithResults+" luceneQueryCount="+luceneQueryCount);
        System.out.println("saiQueriesWithResults="+saiQueriesWithResults+" saiQueryCount="+saiQueryCount);

        luceneReader.close();

        CommitLog.instance.shutdownBlocking();
        CQLTester.cleanup();
    }

    // A * B < ROWS otherwise not all queries will have matches
    public int genColumnA()
    {
        return ThreadLocalRandom.current().nextInt(0, 100);
    }

    public int genColumnB()
    {
        return ThreadLocalRandom.current().nextInt(0, 500);
    }

    @Benchmark
    @OperationsPerInvocation(1)
    @BenchmarkMode({ Mode.Throughput})
    public Object queryRangeANDRangeLucene() throws Throwable
    {
        int columnA = genColumnA();
        int columnA2 = genColumnA();
        int columnB = genColumnB();
        int columnB2 = genColumnB();

        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        builder.add(new BooleanClause(IntPoint.newRangeQuery("columnA", Math.min(columnA, columnA2), Math.max(columnA, columnA2)), BooleanClause.Occur.MUST));
        builder.add(new BooleanClause(IntPoint.newRangeQuery("columnB", Math.min(columnB, columnB2), Math.max(columnB, columnB2)), BooleanClause.Occur.MUST));
        BooleanQuery query = builder.build();

        luceneQueryCount++;

        TopFieldCollector collector = TopFieldCollector.create(Sort.INDEXORDER, 10, null, false, false, false, false);

        indexSearcher.search(query, collector);

        TopFieldDocs topDocs = collector.topDocs();

        if (topDocs.totalHits > 0)
        {
            luceneQueriesWithResults++;

            // iterate on each result
            for (ScoreDoc doc : topDocs.scoreDocs)
            {
                Document document = indexSearcher.doc(doc.doc);
            }
        }
        return topDocs;
    }

    @Benchmark
    @OperationsPerInvocation(1)
    @BenchmarkMode({ Mode.Throughput})
    public Object queryRangeANDRangeSAI() throws Throwable
    {
        saiQueryCount++;
        int columnA = genColumnA();
        int columnA2 = genColumnA();
        int columnB = genColumnB();
        int columnB2 = genColumnB();

        UntypedResultSet results = execute(readStatement,
                                           Math.min(columnA, columnA2),
                                           Math.max(columnA, columnA2),
                                           Math.min(columnB, columnB2),
                                           Math.max(columnB, columnB2));
        if (results.size() > 0)
        {
            // iterate on each result
            for (UntypedResultSet.Row row : results)
            {

            }
            saiQueriesWithResults++;
        }
        return results;
    }
}
