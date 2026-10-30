/*
 * Copyright IBM Corp.
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
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.monitoring.Monitorable;
import org.apache.cassandra.db.monitoring.MonitoringTask;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.test.AbortedQueryLoggerTest;
import org.apache.cassandra.distributed.test.SlowQueryLoggerTest;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.plan.QueryMonitorableExecutionInfo;
import org.apache.cassandra.index.sai.plan.StorageAttachedIndexSearcher;
import org.apache.cassandra.index.sai.utils.AbortedOperationException;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.PrimaryKeyWithSortKey;
import org.apache.cassandra.utils.Shared;
import org.apache.cassandra.utils.Throwables;
import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;

import static java.util.regex.Pattern.quote;
import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;
import static org.apache.cassandra.utils.MonotonicClock.approxTime;

/**
 * Tests the management of aborted queries, as done by {@link MonitoringTask} and {@link QueryContext#checkpoint()},
 * ensuring that the queries are aborted on the replica side and the right {@link Monitorable.ExecutionInfo} is used.
 * </p>
 * More detailed tests for the {@link Monitorable.ExecutionInfo} specifics can be found in the tests for slow but not
 * aborted queries, {@link SlowQueryLoggerTest} and {@link SlowSAIQueryLoggerTest}, and in {@link AbortedQueryLoggerTest}.
 */
public class AbortedSAIQueriesTest extends TestBaseImpl
{
    private static final long QUERY_TIMEOUT_MS = 100;
    private static final String TABLE = "t";

    private static Cluster cluster;
    private static ICoordinator coordinator;
    private static IInvokableInstance node;

    @BeforeClass
    public static void setupCluster() throws Exception
    {
        // effectively disable the scheduled monitoring task so we control it manually for better test stability
        CassandraRelevantProperties.MONITORING_REPORT_INTERVAL_MS.setLong(TimeUnit.HOURS.toMillis(1));

        cluster = init(Cluster.build(2).withInstanceInitializer(AbortedSAIQueriesTest.BBHelper::install).start(), 1);

        // Set a short read timeout in the coordinator. The replica will use the same timeout since it's sent as part of
        // the message in Verb.expiration.
        cluster.get(1).runOnInstance(() -> {
            DatabaseDescriptor.setReadRpcTimeout(QUERY_TIMEOUT_MS);
            DatabaseDescriptor.setRangeRpcTimeout(QUERY_TIMEOUT_MS);
        });

        coordinator = cluster.coordinator(1);
        node = cluster.get(2);

        // create a table with numeric, text and vector indexes
        cluster.schemaChange(format("CREATE TABLE %s.%s (k int PRIMARY KEY, n int, s text, a text)"));
        cluster.schemaChange(format("CREATE CUSTOM INDEX ON %s.%s (n) USING 'StorageAttachedIndex'"));
        cluster.schemaChange(format("CREATE CUSTOM INDEX ON %s.%s (s) USING 'StorageAttachedIndex'"));
        cluster.schemaChange(format("CREATE CUSTOM INDEX ON %s.%s (a) USING 'StorageAttachedIndex' WITH OPTIONS = { 'index_analyzer': 'standard' }"));

        // insert some data
        int numRows = 10;
        for (int i = 0; i < numRows; i++)
            coordinator.execute(format("INSERT INTO %s.%s (k, n, s, a) VALUES (?, ?, ?, ?)"),
                                ConsistencyLevel.ONE,
                                i, i % 2, String.valueOf(i % 2), String.valueOf(i % 2), String.valueOf(i % 2));
        cluster.forEach(n -> n.flush(KEYSPACE));
    }

    @AfterClass
    public static void closeCluster()
    {
        cluster.schemaChange(format("DROP TABLE IF EXISTS %s.%s"));

        if (cluster != null)
            cluster.close();
    }

    @Before
    public void before()
    {
        delaySearch(false);
        delayFilterResultRetriever(false);
        delayScoredResultRetriever(false);

        // trigger the monitoring task to flush any pending slow operations before the test starts
        node.runOnInstance(() -> MonitoringTask.instance.logOperations(approxTime.now()));
    }

    @Test
    public void testNumericFilteringQuery()
    {
        delayFilterResultRetriever(true);
        assertAborted("SELECT * FROM %s.%s WHERE n > 0",
                      "1 operations timed out in the last ",
                      quote("n > ?"),
                      "SAI slow query metrics:",
                      "NumericIndexScan");
    }

    @Test
    public void testLiteralFilteringQuery()
    {
        delayFilterResultRetriever(true);
        assertAborted("SELECT * FROM %s.%s WHERE s = '1'",
                      "1 operations timed out in the last ",
                      quote("s = ?"),
                      "SAI slow query metrics:",
                      "LiteralIndexScan");
    }

    @Test
    public void testNumericOrderingQuery()
    {
        delayScoredResultRetriever(true);
        assertAborted("SELECT * FROM %s.%s ORDER BY n LIMIT 100",
                      "1 operations timed out in the last ",
                      quote("ORDER BY n ASC LIMIT 100"),
                      "SAI slow query metrics:",
                      "NumericIndexScan");
    }

    @Test
    public void testLiteralOrderingQuery()
    {
        delayScoredResultRetriever(true);
        assertAborted("SELECT * FROM %s.%s ORDER BY s DESC LIMIT 100",
                      "1 operations timed out in the last ",
                      quote("ORDER BY s DESC LIMIT 100"),
                      "SAI slow query metrics:",
                      "LiteralIndexScan");
    }

    @Test
    public void tesBM25OrderingQuery()
    {
        delayScoredResultRetriever(true);
        assertAborted("SELECT * FROM %s.%s ORDER BY a BM25 OF '1' LIMIT 100",
                      "1 operations timed out in the last ",
                      quote("WHERE a BM25 ? LIMIT 100"),
                      "SAI slow query metrics:",
                      "Bm25IndexScan");
    }

    @Test
    public void testHybridQuery()
    {
        delayScoredResultRetriever(true);

        // filtering on numeric and ordering on literal
        assertAborted("SELECT * FROM %s.%s WHERE n > 0 ORDER BY s DESC LIMIT 100",
                      "1 operations timed out in the last ",
                      quote("n > ? ORDER BY s DESC LIMIT 100"),
                      "SAI slow query metrics:",
                      "LiteralIndexScan");

        // filtering on literal and ordering on numeric
        assertAborted("SELECT * FROM %s.%s WHERE s = '1' ORDER BY n DESC LIMIT 100",
                      "1 operations timed out in the last ",
                      quote("s = ? ORDER BY n DESC LIMIT 100"),
                      "SAI slow query metrics:",
                      "NumericIndexScan");
    }

    /**
     * Test SAI query that times out before its plan is built.
     */
    @Test
    public void testBeforePlanning()
    {
        delaySearch(true);
        assertAborted("SELECT * FROM %s.%s WHERE s = '1' ORDER BY n DESC LIMIT 100",
                      "1 operations timed out in the last ",
                      quote("s = ? ORDER BY n DESC LIMIT 100"),
                      "SAI slow query metrics:",
                      QueryMonitorableExecutionInfo.UNKNOWN_PLAN);
    }

    private static String format(String query)
    {
        return String.format(query, KEYSPACE, TABLE);
    }

    private void assertAborted(String query, String... lines)
    {
        SharedState.saiTimedOut.set(false);

        long mark = node.logs().mark();

        // Run the query and verify that it times out.
        // This time out can come from either the coordinator or the non-coordinator node, or both.
        String formattedQuery = format(query);
        Assertions.assertThatThrownBy(() -> coordinator.execute(formattedQuery, ALL))
                  .matches(e -> e.getClass().getName().equals(ReadTimeoutException.class.getName()))
                  .hasMessageContaining("Operation timed out");

        // Verify that the query has timed out in the replica, not only in the coordinator.
        Awaitility.await()
                  .atMost(30, TimeUnit.SECONDS)
                  .pollDelay(10, TimeUnit.MILLISECONDS)
                  .untilAsserted(() -> Assertions.assertThat(SharedState.saiTimedOut.get()).isTrue());

        // Verify that monitoring prints the expected log lines
        Awaitility.waitAtMost(30, TimeUnit.SECONDS)
                  .pollDelay(10, TimeUnit.MILLISECONDS)
                  .untilAsserted(() -> {
                      logOperations();
                      for (String line : lines)
                      {
                          List<String> matchingLines = node.logs().grep(mark, line).getResult();
                          Assertions.assertThat(matchingLines).isNotEmpty();
                      }
                  });
    }

    private static void logOperations()
    {
        node.runOnInstance(() -> MonitoringTask.instance.logOperations(approxTime.now()));
    }

    private static void delaySearch(boolean delay)
    {
        node.runOnInstance(() -> SharedState.delaySearch.set(delay));
    }

    private static void delayFilterResultRetriever(boolean delay)
    {
        node.runOnInstance(() -> SharedState.delayFilterResultRetriever.set(delay));
    }

    private static void delayScoredResultRetriever(boolean delay)
    {
        node.runOnInstance(() -> SharedState.delayScoredResultRetriever.set(delay));
    }

    /**
     * Shared state between the test classloader and the node classloaders.
     * Must be @Shared so that all classloaders resolve it to the same Class object.
     */
    @Shared
    public static class SharedState
    {
        public static final AtomicBoolean saiTimedOut = new AtomicBoolean(false);
        public static final AtomicBoolean delaySearch = new AtomicBoolean(false);
        public static final AtomicBoolean delayFilterResultRetriever = new AtomicBoolean(false);
        public static final AtomicBoolean delayScoredResultRetriever = new AtomicBoolean(false);
    }

    /**
     * ByteBuddy interceptor to delay SAI reads.
     */
    public static class BBHelper
    {
        @SuppressWarnings("resource")
        public static void install(ClassLoader classLoader, Integer node)
        {
            if (node != 2)
                return;

            // injection to track replica-side query abortion
            new ByteBuddy().rebase(QueryContext.class)
                           .method(named("checkpoint"))
                           .intercept(MethodDelegation.to(BBHelper.class))
                           .make()
                           .load(classLoader, ClassLoadingStrategy.Default.INJECTION);

            // injection to delay filtering queries
            new ByteBuddy().rebase(StorageAttachedIndexSearcher.ResultRetriever.class)
                           .method(named("nextKey"))
                           .intercept(MethodDelegation.to(BBHelper.class))
                           .make()
                           .load(classLoader, ClassLoadingStrategy.Default.INJECTION);

            // injection to delay top-k queries
            new ByteBuddy().rebase(StorageAttachedIndexSearcher.ScoreOrderedResultRetriever.class)
                           .method(named("nextSelectedKeyInRange"))
                           .intercept(MethodDelegation.to(BBHelper.class))
                           .make()
                           .load(classLoader, ClassLoadingStrategy.Default.INJECTION);

            // injection to queries during planning
            new ByteBuddy().rebase(StorageAttachedIndexSearcher.class)
                           .method(named("search"))
                           .intercept(MethodDelegation.to(BBHelper.class))
                           .make()
                           .load(classLoader, ClassLoadingStrategy.Default.INJECTION);
        }

        @SuppressWarnings("unused")
        public static void checkpoint(@SuperCall Callable<Void> zuperCall)
        {
            try
            {
                zuperCall.call();
            }
            catch (AbortedOperationException e)
            {
                SharedState.saiTimedOut.set(true);
                throw e;
            }
            catch (Exception e)
            {
                throw Throwables.unchecked(e);
            }
        }

        @SuppressWarnings("unused")
        public static PrimaryKey nextKey(@SuperCall Callable<PrimaryKey> zuper)
        {

            return delay(zuper, SharedState.delayFilterResultRetriever.get());
        }

        @SuppressWarnings("unused")
        public static PrimaryKeyWithSortKey nextSelectedKeyInRange(@SuperCall Callable<PrimaryKeyWithSortKey> zuper)
        {
            return delay(zuper, SharedState.delayScoredResultRetriever.get());
        }

        @SuppressWarnings("unused")
        public static UnfilteredPartitionIterator search(ReadExecutionController executionController, @SuperCall Callable<UnfilteredPartitionIterator> zuper)
        {
            return delay(zuper, SharedState.delaySearch.get());
        }

        private static <T> T delay(Callable<T> zuper, boolean condition)
        {
            if (condition)
                Uninterruptibles.sleepUninterruptibly(QUERY_TIMEOUT_MS * 2, TimeUnit.MILLISECONDS);

            try
            {
                return zuper.call();
            }
            catch (Exception e)
            {
                throw Throwables.unchecked(e);
            }
        }
    }
}
