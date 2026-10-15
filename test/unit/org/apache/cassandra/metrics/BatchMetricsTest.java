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

package org.apache.cassandra.metrics;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.service.EmbeddedCassandraService;

import static org.apache.cassandra.cql3.statements.BatchStatement.metrics;
import static org.apache.cassandra.metrics.DecayingEstimatedHistogramReservoir.EstimatedHistogramReservoirSnapshot;
import static org.apache.cassandra.metrics.DecayingEstimatedHistogramReservoir.Range;
import static org.junit.Assert.assertEquals;
import static org.quicktheories.QuickTheory.qt;
import static org.quicktheories.generators.Generate.intArrays;
import static org.quicktheories.generators.SourceDSL.integers;

public class BatchMetricsTest
{
    private static final int MAX_ROUNDS_TO_PERFORM = 3;
    private static final int MAX_DISTINCT_PARTITIONS = 128;
    private static final int MAX_STATEMENTS_PER_ROUND = 32;

    private static EmbeddedCassandraService cassandra;

    private static Cluster cluster;
    private static Session session;

    private static String KEYSPACE = "junit";
    private static final String LOGGER_TABLE = "loggerbatchmetricstest";
    private static final String COUNTER_TABLE = "counterbatchmetricstest";
    private static final String CLUSTERING_TABLE = "clusteringbatchmetricstest";
    private static final String CLUSTERING_COUNTER_TABLE = "clusteringcounterbatchmetricstest";

    private static PreparedStatement psLogger;
    private static PreparedStatement psCounter;
    private static PreparedStatement psClustering;
    private static PreparedStatement psClusteringCounter;

    @BeforeClass()
    public static void setup() throws ConfigurationException, IOException
    {
        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.setWriteRpcTimeout(TimeUnit.SECONDS.toMillis(10));

        cassandra = ServerTestUtils.startEmbeddedCassandraService();

        cluster = Cluster.builder().addContactPoint("127.0.0.1").withPort(DatabaseDescriptor.getNativeTransportPort()).build();
        session = cluster.connect();

        session.execute("CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE + " WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");
        session.execute("USE " + KEYSPACE);
        session.execute("CREATE TABLE IF NOT EXISTS " + LOGGER_TABLE + " (id int PRIMARY KEY, val text);");
        session.execute("CREATE TABLE IF NOT EXISTS " + COUNTER_TABLE + " (id int PRIMARY KEY, val counter);");
        session.execute("CREATE TABLE IF NOT EXISTS " + CLUSTERING_TABLE + " (id int, ck int, val text, PRIMARY KEY (id, ck));");
        session.execute("CREATE TABLE IF NOT EXISTS " + CLUSTERING_COUNTER_TABLE + " (id int, ck int, val counter, PRIMARY KEY (id, ck));");

        psLogger = session.prepare("INSERT INTO " + KEYSPACE + '.' + LOGGER_TABLE + " (id, val) VALUES (?, ?);");
        psCounter = session.prepare("UPDATE " + KEYSPACE + '.' + COUNTER_TABLE + " SET val = val + 1 WHERE id = ?;");
        psClustering = session.prepare("INSERT INTO " + KEYSPACE + '.' + CLUSTERING_TABLE + " (id, ck, val) VALUES (?, ?, ?);");
        psClusteringCounter = session.prepare("UPDATE " + KEYSPACE + '.' + CLUSTERING_COUNTER_TABLE + " SET val = val + 1 WHERE id = ? AND ck = ?;");
    }

    @AfterClass
    public static void tearDown()
    {
        if (cluster != null)
            cluster.close();
        if (cassandra != null)
            cassandra.stop();
    }

    private void executeLoggerBatch(BatchStatement.Type batchStatementType, int distinctPartitions, int statementsPerPartition)
    {
        BatchStatement batch = new BatchStatement(batchStatementType);

        for (int i = 0; i < distinctPartitions; i++)
        {
            for (int j = 0; j < statementsPerPartition; j++)
            {
                if (batchStatementType == BatchStatement.Type.UNLOGGED || batchStatementType == BatchStatement.Type.LOGGED)
                    batch.add(psLogger.bind(i, "aaaaaaaa"));
                else if (batchStatementType == BatchStatement.Type.COUNTER)
                    batch.add(psCounter.bind(i));
                else
                    throw new IllegalStateException("There is no a case for BatchStatement.Type." + batchStatementType.name());
            }
        }

        session.execute(batch);
    }

    @Test
    public void testLoggedPartitionsPerBatch()
    {
        qt().withExamples(25)
            .forAll(intArrays(integers().between(1, MAX_ROUNDS_TO_PERFORM),
                              integers().between(1, MAX_STATEMENTS_PER_ROUND)),
                    integers().between(1, MAX_DISTINCT_PARTITIONS))
            .checkAssert((rounds, distinctPartitions) ->
                         assertMetrics(BatchStatement.Type.LOGGED, rounds, distinctPartitions));
    }

    @Test
    public void testUnloggedPartitionsPerBatch()
    {
        qt().withExamples(25)
            .forAll(intArrays(integers().between(1, MAX_ROUNDS_TO_PERFORM),
                              integers().between(1, MAX_STATEMENTS_PER_ROUND)),
                    integers().between(1, MAX_DISTINCT_PARTITIONS))
            .checkAssert((rounds, distinctPartitions) ->
                         assertMetrics(BatchStatement.Type.UNLOGGED, rounds, distinctPartitions));
    }

    @Test
    public void testCounterPartitionsPerBatch()
    {
        qt().withExamples(10)
            .forAll(intArrays(integers().between(1, MAX_ROUNDS_TO_PERFORM),
                              integers().between(1, MAX_STATEMENTS_PER_ROUND)),
                    integers().between(1, MAX_DISTINCT_PARTITIONS))
            .checkAssert((rounds, distinctPartitions) ->
                         assertMetrics(BatchStatement.Type.COUNTER, rounds, distinctPartitions));
    }

    /**
     * Verifies that rowsPerBatch and partitionsPerBatch diverge when multiple clustering rows share
     * the same partition key. A single-partition batch with {@code rowsPerPartition} distinct clustering
     * values produces partitions=1 but rows={@code rowsPerPartition}.
     */
    @Test
    public void testRowsAndPartitionsDifferWithClusteringColumns()
    {
        clearHistogram();

        // One partition key (id=0), several distinct clustering values → 1 partition, N rows
        int rowsPerPartition = 5;
        BatchStatement batch = new BatchStatement(BatchStatement.Type.LOGGED);
        for (int ck = 0; ck < rowsPerPartition; ck++)
            batch.add(psClustering.bind(0, ck, "val" + ck));

        session.execute(batch);

        // Exactly one sample recorded in the logged histograms
        assertEquals(1, metrics.partitionsPerLoggedBatch.getCount());
        assertEquals(1, metrics.rowsPerLoggedBatch.getCount());

        // The recorded partition count must be 1 (one partition key)
        EstimatedHistogramReservoirSnapshot partitionsSnapshot =
            (EstimatedHistogramReservoirSnapshot) metrics.partitionsPerLoggedBatch.getSnapshot();
        Range expectedPartitionsRange = partitionsSnapshot.getBucketingRangeForValue(1);
        assertEquals(expectedPartitionsRange, new Range(partitionsSnapshot.getMin(), partitionsSnapshot.getMax()));

        // The recorded row count must be rowsPerPartition (distinct clustering values)
        EstimatedHistogramReservoirSnapshot rowsSnapshot =
            (EstimatedHistogramReservoirSnapshot) metrics.rowsPerLoggedBatch.getSnapshot();
        Range expectedRowsRange = rowsSnapshot.getBucketingRangeForValue(rowsPerPartition);
        assertEquals(expectedRowsRange, new Range(rowsSnapshot.getMin(), rowsSnapshot.getMax()));

        // Partition count and row count must differ: rows > partitions
        assertEquals(1L, partitionsSnapshot.getMax());
        assertEquals(rowsPerPartition, rowsSnapshot.getMax());
    }

    /**
     * Verifies that rowsPerUnloggedBatch and partitionsPerUnloggedBatch diverge when multiple clustering
     * rows share the same partition key in an UNLOGGED batch.
     */
    @Test
    public void testUnloggedRowsAndPartitionsDifferWithClusteringColumns()
    {
        clearHistogram();

        int rowsPerPartition = 5;
        BatchStatement batch = new BatchStatement(BatchStatement.Type.UNLOGGED);
        for (int ck = 0; ck < rowsPerPartition; ck++)
            batch.add(psClustering.bind(0, ck, "val" + ck));

        session.execute(batch);

        assertEquals(1, metrics.partitionsPerUnloggedBatch.getCount());
        assertEquals(1, metrics.rowsPerUnloggedBatch.getCount());

        EstimatedHistogramReservoirSnapshot partitionsSnapshot =
            (EstimatedHistogramReservoirSnapshot) metrics.partitionsPerUnloggedBatch.getSnapshot();
        Range expectedPartitionsRange = partitionsSnapshot.getBucketingRangeForValue(1);
        assertEquals(expectedPartitionsRange, new Range(partitionsSnapshot.getMin(), partitionsSnapshot.getMax()));

        EstimatedHistogramReservoirSnapshot rowsSnapshot =
            (EstimatedHistogramReservoirSnapshot) metrics.rowsPerUnloggedBatch.getSnapshot();
        Range expectedRowsRange = rowsSnapshot.getBucketingRangeForValue(rowsPerPartition);
        assertEquals(expectedRowsRange, new Range(rowsSnapshot.getMin(), rowsSnapshot.getMax()));

        assertEquals(1L, partitionsSnapshot.getMax());
        assertEquals(rowsPerPartition, rowsSnapshot.getMax());
    }

    /**
     * Verifies that rowsPerCounterBatch and partitionsPerCounterBatch diverge when multiple clustering
     * rows share the same partition key in a COUNTER batch.
     */
    @Test
    public void testCounterRowsAndPartitionsDifferWithClusteringColumns()
    {
        clearHistogram();

        int rowsPerPartition = 5;
        BatchStatement batch = new BatchStatement(BatchStatement.Type.COUNTER);
        for (int ck = 0; ck < rowsPerPartition; ck++)
            batch.add(psClusteringCounter.bind(0, ck));

        session.execute(batch);

        assertEquals(1, metrics.partitionsPerCounterBatch.getCount());
        assertEquals(1, metrics.rowsPerCounterBatch.getCount());

        EstimatedHistogramReservoirSnapshot partitionsSnapshot =
            (EstimatedHistogramReservoirSnapshot) metrics.partitionsPerCounterBatch.getSnapshot();
        Range expectedPartitionsRange = partitionsSnapshot.getBucketingRangeForValue(1);
        assertEquals(expectedPartitionsRange, new Range(partitionsSnapshot.getMin(), partitionsSnapshot.getMax()));

        EstimatedHistogramReservoirSnapshot rowsSnapshot =
            (EstimatedHistogramReservoirSnapshot) metrics.rowsPerCounterBatch.getSnapshot();
        Range expectedRowsRange = rowsSnapshot.getBucketingRangeForValue(rowsPerPartition);
        assertEquals(expectedRowsRange, new Range(rowsSnapshot.getMin(), rowsSnapshot.getMax()));

        assertEquals(1L, partitionsSnapshot.getMax());
        assertEquals(rowsPerPartition, rowsSnapshot.getMax());
    }

    private void assertMetrics(BatchStatement.Type batchTypeTested, int[] rounds, int distinctPartitions)
    {
        // reset the histogram between runs
        clearHistogram();

        // roundsOfStatementsPerPartition - array length is the number of rounds to executeLoggerBatch() and each
        // value in the array represents the number of statements to execute per partition on that round
        for (int ix = 0; ix < rounds.length; ix++)
        {
            long partitionsPerLoggedBatchCountPre = metrics.partitionsPerLoggedBatch.getCount();
            long expectedPartitionsPerLoggedBatchCount = partitionsPerLoggedBatchCountPre + (batchTypeTested == BatchStatement.Type.LOGGED ? 1 : 0);
            long partitionsPerUnloggedBatchCountPre = metrics.partitionsPerUnloggedBatch.getCount();
            long expectedPartitionsPerUnloggedBatchCount = partitionsPerUnloggedBatchCountPre + (batchTypeTested == BatchStatement.Type.UNLOGGED ? 1 : 0);
            long partitionsPerCounterBatchCountPre = metrics.partitionsPerCounterBatch.getCount();
            long expectedPartitionsPerCounterBatchCount = partitionsPerCounterBatchCountPre + (batchTypeTested == BatchStatement.Type.COUNTER ? 1 : 0);

            long rowsPerLoggedBatchCountPre = metrics.rowsPerLoggedBatch.getCount();
            long expectedRowsPerLoggedBatchCount = rowsPerLoggedBatchCountPre + (batchTypeTested == BatchStatement.Type.LOGGED ? 1 : 0);
            long rowsPerUnloggedBatchCountPre = metrics.rowsPerUnloggedBatch.getCount();
            long expectedRowsPerUnloggedBatchCount = rowsPerUnloggedBatchCountPre + (batchTypeTested == BatchStatement.Type.UNLOGGED ? 1 : 0);
            long rowsPerCounterBatchCountPre = metrics.rowsPerCounterBatch.getCount();
            long expectedRowsPerCounterBatchCount = rowsPerCounterBatchCountPre + (batchTypeTested == BatchStatement.Type.COUNTER ? 1 : 0);

            long columnsPerLoggedBatchCountPre = metrics.columnsPerLoggedBatch.getCount();
            long expectedColumnsPerLoggedBatchCount = columnsPerLoggedBatchCountPre + (batchTypeTested == BatchStatement.Type.LOGGED ? 1 : 0);
            long columnsPerUnloggedBatchCountPre = metrics.columnsPerUnloggedBatch.getCount();
            long expectedColumnsPerUnloggedBatchCount = columnsPerUnloggedBatchCountPre + (batchTypeTested == BatchStatement.Type.UNLOGGED ? 1 : 0);
            long columnsPerCounterBatchCountPre = metrics.columnsPerCounterBatch.getCount();
            long expectedColumnsPerCounterBatchCount = columnsPerCounterBatchCountPre + (batchTypeTested == BatchStatement.Type.COUNTER ? 1 : 0);

            executeLoggerBatch(batchTypeTested, distinctPartitions, rounds[ix]);

            assertEquals(expectedPartitionsPerUnloggedBatchCount, metrics.partitionsPerUnloggedBatch.getCount());
            assertEquals(expectedPartitionsPerLoggedBatchCount, metrics.partitionsPerLoggedBatch.getCount());
            assertEquals(expectedPartitionsPerCounterBatchCount, metrics.partitionsPerCounterBatch.getCount());
            assertEquals(expectedRowsPerUnloggedBatchCount, metrics.rowsPerUnloggedBatch.getCount());
            assertEquals(expectedRowsPerLoggedBatchCount, metrics.rowsPerLoggedBatch.getCount());
            assertEquals(expectedRowsPerCounterBatchCount, metrics.rowsPerCounterBatch.getCount());
            assertEquals(expectedColumnsPerUnloggedBatchCount, metrics.columnsPerUnloggedBatch.getCount());
            assertEquals(expectedColumnsPerLoggedBatchCount, metrics.columnsPerLoggedBatch.getCount());
            assertEquals(expectedColumnsPerCounterBatchCount, metrics.columnsPerCounterBatch.getCount());

            EstimatedHistogramReservoirSnapshot partitionsPerLoggedBatchSnapshot = (EstimatedHistogramReservoirSnapshot) metrics.partitionsPerLoggedBatch.getSnapshot();
            EstimatedHistogramReservoirSnapshot partitionsPerUnloggedBatchSnapshot = (EstimatedHistogramReservoirSnapshot) metrics.partitionsPerUnloggedBatch.getSnapshot();
            EstimatedHistogramReservoirSnapshot partitionsPerCounterBatchSnapshot = (EstimatedHistogramReservoirSnapshot) metrics.partitionsPerCounterBatch.getSnapshot();
            EstimatedHistogramReservoirSnapshot rowsPerLoggedBatchSnapshot = (EstimatedHistogramReservoirSnapshot) metrics.rowsPerLoggedBatch.getSnapshot();
            EstimatedHistogramReservoirSnapshot rowsPerUnloggedBatchSnapshot = (EstimatedHistogramReservoirSnapshot) metrics.rowsPerUnloggedBatch.getSnapshot();
            EstimatedHistogramReservoirSnapshot rowsPerCounterBatchSnapshot = (EstimatedHistogramReservoirSnapshot) metrics.rowsPerCounterBatch.getSnapshot();
            EstimatedHistogramReservoirSnapshot columnsPerLoggedBatchSnapshot = (EstimatedHistogramReservoirSnapshot) metrics.columnsPerLoggedBatch.getSnapshot();
            EstimatedHistogramReservoirSnapshot columnsPerUnloggedBatchSnapshot = (EstimatedHistogramReservoirSnapshot) metrics.columnsPerUnloggedBatch.getSnapshot();
            EstimatedHistogramReservoirSnapshot columnsPerCounterBatchSnapshot = (EstimatedHistogramReservoirSnapshot) metrics.columnsPerCounterBatch.getSnapshot();

            // BatchMetrics uses DecayingEstimatedHistogramReservoir which notes that the return of getMax()
            // may be more than the actual max value recorded in the reservoir with similar but reverse properties
            // for getMin(). uses getBucketingForValue() on the snapshot to identify the exact max. since the
            // distinctPartitions doesn't change per test round these values shouldn't change.
            Range expectedPartitionsPerLoggedBatchMinMax = batchTypeTested == BatchStatement.Type.LOGGED ?
                                                           determineExpectedMinMax(partitionsPerLoggedBatchSnapshot, distinctPartitions) :
                                                           new Range(0L, 0L);
            Range expectedPartitionsPerUnloggedBatchMinMax = batchTypeTested == BatchStatement.Type.UNLOGGED ?
                                                             determineExpectedMinMax(partitionsPerUnloggedBatchSnapshot, distinctPartitions) :
                                                             new Range(0L, 0L);
            Range expectedPartitionsPerCounterBatchMinMax = batchTypeTested == BatchStatement.Type.COUNTER ?
                                                            determineExpectedMinMax(partitionsPerCounterBatchSnapshot, distinctPartitions) :
                                                            new Range(0L, 0L);
            Range expectedRowsPerLoggedBatchMinMax = batchTypeTested == BatchStatement.Type.LOGGED ?
                                                     determineExpectedMinMax(rowsPerLoggedBatchSnapshot, distinctPartitions) :
                                                     new Range(0L, 0L);
            Range expectedRowsPerUnloggedBatchMinMax = batchTypeTested == BatchStatement.Type.UNLOGGED ?
                                                       determineExpectedMinMax(rowsPerUnloggedBatchSnapshot, distinctPartitions) :
                                                       new Range(0L, 0L);
            Range expectedRowsPerCounterBatchMinMax = batchTypeTested == BatchStatement.Type.COUNTER ?
                                                      determineExpectedMinMax(rowsPerCounterBatchSnapshot, distinctPartitions) :
                                                      new Range(0L, 0L);
            Range expectedColumnsPerLoggedBatchMinMax = batchTypeTested == BatchStatement.Type.LOGGED ?
                                                        determineExpectedMinMax(columnsPerLoggedBatchSnapshot, distinctPartitions) :
                                                        new Range(0L, 0L);
            Range expectedColumnsPerUnloggedBatchMinMax = batchTypeTested == BatchStatement.Type.UNLOGGED ?
                                                          determineExpectedMinMax(columnsPerUnloggedBatchSnapshot, distinctPartitions) :
                                                          new Range(0L, 0L);
            Range expectedColumnsPerCounterBatchMinMax = batchTypeTested == BatchStatement.Type.COUNTER ?
                                                         determineExpectedMinMax(columnsPerCounterBatchSnapshot, distinctPartitions) :
                                                         new Range(0L, 0L);

            assertEquals(expectedPartitionsPerLoggedBatchMinMax, new Range(partitionsPerLoggedBatchSnapshot.getMin(), partitionsPerLoggedBatchSnapshot.getMax()));
            assertEquals(expectedPartitionsPerUnloggedBatchMinMax, new Range(partitionsPerUnloggedBatchSnapshot.getMin(), partitionsPerUnloggedBatchSnapshot.getMax()));
            assertEquals(expectedPartitionsPerCounterBatchMinMax, new Range(partitionsPerCounterBatchSnapshot.getMin(), partitionsPerCounterBatchSnapshot.getMax()));
            assertEquals(expectedRowsPerLoggedBatchMinMax, new Range(rowsPerLoggedBatchSnapshot.getMin(), rowsPerLoggedBatchSnapshot.getMax()));
            assertEquals(expectedRowsPerUnloggedBatchMinMax, new Range(rowsPerUnloggedBatchSnapshot.getMin(), rowsPerUnloggedBatchSnapshot.getMax()));
            assertEquals(expectedRowsPerCounterBatchMinMax, new Range(rowsPerCounterBatchSnapshot.getMin(), rowsPerCounterBatchSnapshot.getMax()));
            assertEquals(expectedColumnsPerLoggedBatchMinMax, new Range(columnsPerLoggedBatchSnapshot.getMin(), columnsPerLoggedBatchSnapshot.getMax()));
            assertEquals(expectedColumnsPerUnloggedBatchMinMax, new Range(columnsPerUnloggedBatchSnapshot.getMin(), columnsPerUnloggedBatchSnapshot.getMax()));
            assertEquals(expectedColumnsPerCounterBatchMinMax, new Range(columnsPerCounterBatchSnapshot.getMin(), columnsPerCounterBatchSnapshot.getMax()));
        }
    }

    private void clearHistogram()
    {
        ((ClearableHistogram) metrics.partitionsPerLoggedBatch).clear();
        ((ClearableHistogram) metrics.partitionsPerUnloggedBatch).clear();
        ((ClearableHistogram) metrics.partitionsPerCounterBatch).clear();
        ((ClearableHistogram) metrics.rowsPerLoggedBatch).clear();
        ((ClearableHistogram) metrics.rowsPerUnloggedBatch).clear();
        ((ClearableHistogram) metrics.rowsPerCounterBatch).clear();
        ((ClearableHistogram) metrics.columnsPerLoggedBatch).clear();
        ((ClearableHistogram) metrics.columnsPerUnloggedBatch).clear();
        ((ClearableHistogram) metrics.columnsPerCounterBatch).clear();
    }

    private Range determineExpectedMinMax(EstimatedHistogramReservoirSnapshot snapshot, long value)
    {
        return snapshot.getBucketingRangeForValue(value);
    }
}
