/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.distributed.test.sai;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Test;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.monitoring.MonitoringTask;
import org.apache.cassandra.db.monitoring.MonitoringTaskTest;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.index.SecondaryIndexManager;
import org.apache.cassandra.index.sai.plan.QueryMonitorableExecutionInfo;
import org.assertj.core.api.AbstractIterableAssert;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.ListAssert;
import org.awaitility.Awaitility;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.apache.cassandra.utils.MonotonicClock.Global.approxTime;

/**
 * Tests {@link QueryMonitorableExecutionInfo} combined with the {@link MonitoringTask} mechanism,
 * the core functionality testing of that feature should is in {@link MonitoringTaskTest}.
 */
public class SlowSAIQueryLoggerTest extends TestBaseImpl
{
    private static final int SLOW_QUERY_LOG_TIMEOUT_IN_MS = 100;

    @Test
    public void testSlowSAIQueryLogger() throws Throwable
    {
        // effectively disable the scheduled monitoring task so we control it manually for better test stability
        CassandraRelevantProperties.MONITORING_REPORT_INTERVAL_MS.setInt((int) TimeUnit.HOURS.toMillis(1));

        try (Cluster cluster = init(Cluster.build(1)
                                           .withConfig(c -> c.set("slow_query_log_timeout_in_ms", SLOW_QUERY_LOG_TIMEOUT_IN_MS))
                                           .withInstanceInitializer(BB::install)
                                           .start()))
        {
            // create a table with numeric, text and vector indexes
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.t (k int, c int, n int, s text, v vector<float, 2>, l int, PRIMARY KEY(k, c))"));
            cluster.schemaChange(withKeyspace("CREATE CUSTOM INDEX ON %s.t (n) USING 'StorageAttachedIndex'"));
            cluster.schemaChange(withKeyspace("CREATE CUSTOM INDEX ON %s.t (s) USING 'StorageAttachedIndex'"));
            cluster.schemaChange(withKeyspace("CREATE CUSTOM INDEX ON %s.t (v) USING 'StorageAttachedIndex'"));

            // insert some data
            ICoordinator coordinator = cluster.coordinator(1);
            IInvokableInstance node = cluster.get(1);
            coordinator.execute(withKeyspace("INSERT INTO %s.t (k, c, n, s, v, l) VALUES (1, 1, 1, 's_1', [1, 1], 1)"), ConsistencyLevel.ONE);
            coordinator.execute(withKeyspace("INSERT INTO %s.t (k, c, n, s, v, l) VALUES (1, 2, 2, 's_2', [1, 2], 2)"), ConsistencyLevel.ONE);
            coordinator.execute(withKeyspace("INSERT INTO %s.t (k, c, n, s, v, l) VALUES (2, 1, 3, 's_3', [1, 3], 3)"), ConsistencyLevel.ONE);
            coordinator.execute(withKeyspace("INSERT INTO %s.t (k, c, n, s, v, l) VALUES (2, 2, 4, 's_4', [1, 4], 4)"), ConsistencyLevel.ONE);
            node.flush(KEYSPACE);

            // test single numeric query
            long mark = node.logs().mark();
            String numericQuery = withKeyspace("SELECT * FROM %s.t WHERE n > 1");
            coordinator.execute(numericQuery, ConsistencyLevel.ONE);
            assertLogsContain(mark, node,
                              "SAI slow query metrics:",
                              "sstablesHit: 1",
                              "segmentsHit: 1",
                              "partitionsRead: 2",
                              "rowsFiltered: 3",
                              "rowsPreFiltered: 0",
                              "trieSegmentsHit: 0",
                              "bkdPostingListsHit: 1",
                              "bkdSegmentsHit: 1",
                              "bkdPostingsSkips: 0",
                              "bkdPostingsDecodes: 4",
                              "triePostingsSkips: 0",
                              "triePostingsDecodes: 0",
                              "annGraphSearchLatencyNanos: ",
                              "shadowedPrimaryKeyCount",
                              "SAI slow query plan:",
                              "NumericIndexScan");

            // test aggregated numeric query
            mark = node.logs().mark();
            for (int i = 0; i < 2; i++)
                coordinator.execute(numericQuery, ConsistencyLevel.ONE);
            assertLogsContain(mark, node,
                              "SAI slowest query metrics:",
                              "sstablesHit: 1",
                              "segmentsHit: 1",
                              "partitionsRead: 2",
                              "rowsFiltered: 3",
                              "rowsPreFiltered: 0",
                              "trieSegmentsHit: 0",
                              "bkdPostingListsHit: 1",
                              "bkdSegmentsHit: 1",
                              "bkdPostingsSkips: 0",
                              "bkdPostingsDecodes: 4",
                              "triePostingsSkips: 0",
                              "triePostingsDecodes: 0",
                              "annGraphSearchLatencyNanos: ",
                              "shadowedPrimaryKeyCount",
                              "SAI slowest query plan:",
                              "NumericIndexScan");

            // test single text query
            mark = node.logs().mark();
            String textQuery = withKeyspace("SELECT * FROM %s.t WHERE s = 's_2' OR s = 's_3'");
            coordinator.execute(textQuery, ConsistencyLevel.ONE);
            assertLogsContain(mark, node,
                              "SAI slow query metrics:",
                              "sstablesHit: 2",
                              "segmentsHit: 2",
                              "partitionsRead: 2",
                              "rowsFiltered: 2",
                              "rowsPreFiltered: 0",
                              "trieSegmentsHit: 2",
                              "bkdPostingListsHit: 0",
                              "bkdSegmentsHit: 0",
                              "bkdPostingsSkips: 0",
                              "bkdPostingsDecodes: 0",
                              "triePostingsSkips: 0",
                              "triePostingsDecodes: 2",
                              "annGraphSearchLatencyNanos: 0",
                              "shadowedPrimaryKeyCount: 0",
                              "SAI slow query plan:",
                              "LiteralIndexScan");

            // test aggregated text query
            mark = node.logs().mark();
            for (int i = 0; i < 2; i++)
                coordinator.execute(textQuery, ConsistencyLevel.ONE);
            assertLogsContain(mark, node,
                              "SAI slowest query metrics:",
                              "sstablesHit: 2",
                              "segmentsHit: 2",
                              "partitionsRead: 2",
                              "rowsFiltered: 2",
                              "rowsPreFiltered: 0",
                              "trieSegmentsHit: 2",
                              "bkdPostingListsHit: 0",
                              "bkdSegmentsHit: 0",
                              "bkdPostingsSkips: 0",
                              "bkdPostingsDecodes: 0",
                              "triePostingsSkips: 0",
                              "triePostingsDecodes: 2",
                              "annGraphSearchLatencyNanos: 0",
                              "shadowedPrimaryKeyCount: 0",
                              "SAI slowest query plan:",
                              "LiteralIndexScan");

            // test single ANN query
            mark = node.logs().mark();
            String annQuery = withKeyspace("SELECT * FROM %s.t ORDER BY v ANN OF [1, 1] LIMIT 10");
            coordinator.execute(annQuery, ConsistencyLevel.ONE);
            assertLogsContain(mark, node,
                              "SAI slow query metrics:",
                              "sstablesHit: 0", // TODO: should be fixed by CNDB-15620
                              "segmentsHit: 0", // TODO: should be fixed by CNDB-15620
                              "partitionsRead: 4",
                              "rowsFiltered: 4",
                              "rowsPreFiltered: 0",
                              "trieSegmentsHit: 0",
                              "bkdPostingListsHit: 0",
                              "bkdSegmentsHit: 0",
                              "bkdPostingsSkips: 0",
                              "bkdPostingsDecodes: 0",
                              "triePostingsSkips: 0",
                              "triePostingsDecodes: 0",
                              "annGraphSearchLatencyNanos: [1-9][0-9]*", // unknown, but greater than zero
                              "shadowedPrimaryKeyCount: 0",
                              "SAI slow query plan:",
                              "AnnIndexScan");

            // test aggregated ANN query
            mark = node.logs().mark();
            for (int i = 0; i < 2; i++)
                coordinator.execute(annQuery, ConsistencyLevel.ONE);
            assertLogsContain(mark, node,
                              "SAI slowest query metrics:",
                              "sstablesHit: 0", // TODO: should be fixed by CNDB-15620
                              "segmentsHit: 0", // TODO: should be fixed by CNDB-15620
                              "partitionsRead: 4",
                              "rowsFiltered: 4",
                              "rowsPreFiltered: 0",
                              "trieSegmentsHit: 0",
                              "bkdPostingListsHit: 0",
                              "bkdSegmentsHit: 0",
                              "bkdPostingsSkips: 0",
                              "bkdPostingsDecodes: 0",
                              "triePostingsSkips: 0",
                              "triePostingsDecodes: 0",
                              "annGraphSearchLatencyNanos: [1-9][0-9]*", // unknown, but greater than zero
                              "shadowedPrimaryKeyCount: 0",
                              "SAI slowest query plan:",
                              "AnnIndexScan");

            // test single hybrid query
            mark = node.logs().mark();
            String hybridQuery = withKeyspace("SELECT * FROM %s.t WHERE n > 1 ORDER BY s LIMIT 10");
            coordinator.execute(hybridQuery, ConsistencyLevel.ONE);
            assertLogsContain(mark, node,
                              "SAI slow query metrics:",
                              "sstablesHit: 0", // TODO: should be fixed by CNDB-15620
                              "segmentsHit: 0", // TODO: should be fixed by CNDB-15620
                              "partitionsRead: 4",
                              "rowsFiltered: 5",
                              "rowsPreFiltered: 0",
                              "trieSegmentsHit: 0",
                              "bkdPostingListsHit: 0",
                              "bkdSegmentsHit: 0",
                              "bkdPostingsSkips: 0",
                              "bkdPostingsDecodes: 0",
                              "triePostingsSkips: 0",
                              "triePostingsDecodes: 0",
                              "annGraphSearchLatencyNanos: 0",
                              "shadowedPrimaryKeyCount: 1",
                              "SAI slow query plan:",
                              "LiteralIndexScan");

            // test aggregated hybrid query
            mark = node.logs().mark();
            for (int i = 0; i < 2; i++)
                coordinator.execute(hybridQuery, ConsistencyLevel.ONE);
            assertLogsContain(mark, node,
                              "SAI slowest query metrics:",
                              "sstablesHit: 0", // TODO: should be fixed by CNDB-15620
                              "segmentsHit: 0", // TODO: should be fixed by CNDB-15620
                              "partitionsRead: 4",
                              "rowsFiltered: 5",
                              "rowsPreFiltered: 0",
                              "trieSegmentsHit: 0",
                              "bkdPostingListsHit: 0",
                              "bkdSegmentsHit: 0",
                              "bkdPostingsSkips: 0",
                              "bkdPostingsDecodes: 0",
                              "triePostingsSkips: 0",
                              "triePostingsDecodes: 0",
                              "annGraphSearchLatencyNanos: 0",
                              "shadowedPrimaryKeyCount: 1",
                              "SAI slowest query plan:",
                              "LiteralIndexScan");

            // test changing data between identical queries, making one of them slower than the other,
            // so we can check that only the execution info of the slowest query are reported
            mark = node.logs().mark();
            coordinator.execute(numericQuery, ConsistencyLevel.ONE);
            node.runOnInstance(() -> BB.queryDelay.updateAndGet(x -> x * 4)); // make queries 4x slower
            coordinator.execute(withKeyspace("INSERT INTO %s.t (k, c, n) VALUES (1, 3, 5)"), ConsistencyLevel.ONE);
            coordinator.execute(withKeyspace("INSERT INTO %s.t (k, c, n) VALUES (2, 3, 6)"), ConsistencyLevel.ONE);
            coordinator.execute(withKeyspace("INSERT INTO %s.t (k, c, n) VALUES (3, 1, 7)"), ConsistencyLevel.ONE);
            node.flush(KEYSPACE);
            coordinator.execute(numericQuery, ConsistencyLevel.ONE);
            assertLogsContain(mark, node,
                              "SAI slowest query metrics:",
                              "sstablesHit: 3",
                              "segmentsHit: 3",
                              "partitionsRead: 3",
                              "rowsFiltered: 6");
            node.runOnInstance(() -> BB.queryDelay.updateAndGet(x -> x / 4)); // restore the query delay

            // disable execution info logging and verify they are not logged
            CassandraRelevantProperties.SAI_SLOW_QUERY_LOG_EXECUTION_INFO_ENABLED.setBoolean(false);
            mark = node.logs().mark();
            coordinator.execute(numericQuery, ConsistencyLevel.ONE);
            coordinator.execute(textQuery, ConsistencyLevel.ONE);
            coordinator.execute(annQuery, ConsistencyLevel.ONE);
            coordinator.execute(hybridQuery, ConsistencyLevel.ONE);
            assertLogsContain(mark, node, "4 operations were slow");
            assertLogsDoNotContainSAIExecutionInfo(mark, node);
            CassandraRelevantProperties.SAI_SLOW_QUERY_LOG_EXECUTION_INFO_ENABLED.setBoolean(true);

            // test with a legacy index, there should be no SAI execution info
            cluster.schemaChange(withKeyspace("CREATE INDEX legacy_idx ON %s.t (l)"));
            Awaitility.waitAtMost(1, TimeUnit.MINUTES).until(() -> cluster.get(1).callOnInstance(() -> {
                SecondaryIndexManager sim = Keyspace.open(KEYSPACE).getColumnFamilyStore("t").indexManager;
                return sim.isIndexQueryable("legacy_idx");
            }));
            mark = node.logs().mark();
            String legacyIndexQuery = withKeyspace("SELECT * FROM %s.t WHERE l = 1");
            coordinator.execute(legacyIndexQuery, ConsistencyLevel.ONE);
            assertLogsContain(mark, node, "1 operations were slow", "WHERE l = ?");
            assertLogsDoNotContainSAIExecutionInfo(mark, node);

            // test with a regular, non-indexed query, there should be no SAI execution info
            mark = node.logs().mark();
            String regularQuery = withKeyspace("SELECT * FROM %s.t WHERE k = 1");
            coordinator.execute(regularQuery, ConsistencyLevel.ONE);
            assertLogsContain(mark, node, "1 operations were slow", "WHERE k = ?");
            assertLogsDoNotContainSAIExecutionInfo(mark, node);

            // test that queries with the same relations and different values get grouped due to redaction
            mark = node.logs().mark();
            coordinator.execute(withKeyspace("SELECT * FROM %s.t WHERE n = 1"), ConsistencyLevel.ONE);
            coordinator.execute(withKeyspace("SELECT * FROM %s.t WHERE n = 2"), ConsistencyLevel.ONE);
            coordinator.execute(withKeyspace("SELECT * FROM %s.t WHERE n > 1"), ConsistencyLevel.ONE);
            coordinator.execute(withKeyspace("SELECT * FROM %s.t WHERE n > 2"), ConsistencyLevel.ONE);
            coordinator.execute(withKeyspace("SELECT * FROM %s.t WHERE n > 3"), ConsistencyLevel.ONE);
            assertLogsContain(mark, node, "was slow 2 times", "WHERE n = ?", "SAI slowest query metrics:");
            assertLogsContain(mark, node, "was slow 3 times", "WHERE n > ?", "SAI slowest query metrics:");
            assertLogsDoNotContain(mark, node, "WHERE n = 1", "WHERE n = 2", "WHERE n > 1", "WHERE n > 2", "WHERE n > 3");
        }
    }

    private static void assertLogsContain(long mark, IInvokableInstance node, String... lines)
    {
        assertLogs(mark, node, AbstractIterableAssert::isNotEmpty, lines);
    }

    private static void assertLogsDoNotContain(long mark, IInvokableInstance node, String... lines)
    {
        assertLogs(mark, node, AbstractIterableAssert::isEmpty, lines);
    }

    private static void assertLogsDoNotContainSAIExecutionInfo(long mark, IInvokableInstance node)
    {
        assertLogsDoNotContain(mark, node,
                               "SAI slow query metrics:",
                               "SAI slow query plan:",
                               "SAI slowest query metrics:",
                               "SAI slowest query plan:");
    }

    private static void assertLogs(long mark, IInvokableInstance node, Consumer<ListAssert<String>> listAssert, String... lines)
    {
        // manually trigger the monitoring task to log the slow operations
        node.runOnInstance(() -> MonitoringTask.instance.logOperations(approxTime.now()));

        for (String line : lines)
        {
            List<String> matchingLines = node.logs().grep(mark, line).getResult();
            listAssert.accept(Assertions.assertThat(matchingLines));
        }
    }

    /**
     * ByteBuddy interceptor to slow down SAI queries so they are logged as slow.
     */
    public static class BB
    {
        static AtomicInteger queryDelay = new AtomicInteger(SLOW_QUERY_LOG_TIMEOUT_IN_MS * 2);

        @SuppressWarnings("resource")
        public static void install(ClassLoader classLoader, int node)
        {
            new ByteBuddy().rebase(ReadCommand.class)
                           .method(named("executeLocally"))
                           .intercept(MethodDelegation.to(SlowSAIQueryLoggerTest.BB.class))
                           .make()
                           .load(classLoader, ClassLoadingStrategy.Default.INJECTION);
        }

        @SuppressWarnings("unused")
        public static UnfilteredPartitionIterator executeLocally(ReadExecutionController executionController,
                                                                 @SuperCall Callable<UnfilteredPartitionIterator> zuper)
        {
            if (executionController.metadata().keyspace.equals(KEYSPACE))
                Uninterruptibles.sleepUninterruptibly(queryDelay.get(), TimeUnit.MILLISECONDS);
            try
            {
                return zuper.call();
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }
    }
}
