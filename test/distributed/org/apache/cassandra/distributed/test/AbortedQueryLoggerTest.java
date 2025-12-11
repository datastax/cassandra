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

package org.apache.cassandra.distributed.test;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.After;
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
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.monitoring.Monitorable;
import org.apache.cassandra.db.monitoring.MonitoringTask;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.transform.Transformation;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.test.sai.SlowSAIQueryLoggerTest;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Throwables;
import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;
import static org.apache.cassandra.utils.MonotonicClock.approxTime;

/**
 * Tests the logging of aborted queries, as done by {@link MonitoringTask},
 * ensuring that the right {@link Monitorable.ExecutionInfo} is used.
 * </p>
 * More detailed tests for the {@link Monitorable.ExecutionInfo} specifics can be found in the tests for slow but not
 * aborted queries, {@link SlowQueryLoggerTest} and {@link SlowSAIQueryLoggerTest}.
 */
public class AbortedQueryLoggerTest extends TestBaseImpl
{
    private static final int QUERY_TIMEOUT_MS = 100;
    private static final AtomicInteger SEQ = new AtomicInteger();

    private static Cluster cluster;
    private static String table;
    private static ICoordinator coordinator;
    private static IInvokableInstance node;

    @BeforeClass
    public static void setupCluster() throws Exception
    {
        cluster = init(Cluster.build(2).withInstanceInitializer(AbortedQueryLoggerTest.BBHelper::install).start());

        // Set a short read timeout in the coordinator. The replica will use the same timeout since it's sent as part of
        // the message in Verb.expiration.
        cluster.get(1).runOnInstance(() -> {
            DatabaseDescriptor.setReadRpcTimeout(QUERY_TIMEOUT_MS);
            DatabaseDescriptor.setRangeRpcTimeout(QUERY_TIMEOUT_MS);
        });

        coordinator = cluster.coordinator(1);
        node = cluster.get(2);
    }

    @AfterClass
    public static void closeCluster()
    {
        if (cluster != null)
            cluster.close();
    }

    @Before
    public void before()
    {
        CassandraRelevantProperties.MONITORING_EXECUTION_INFO_ENABLED.setBoolean(true);
        table = "t_" + SEQ.getAndIncrement();

        // trigger the monitoring task to flush any pending slow operations before the test starts
        node.runOnInstance(() -> MonitoringTask.instance.logOperations(approxTime.now()));
    }

    @After
    public void after()
    {
        cluster.schemaChange(format("DROP TABLE IF EXISTS %s.%s"));
    }

    /**
     * Test that log reports about aborted queries do not log sensitive data.
     */
    @Test
    public void testDoesNotLogSensitiveData()
    {
        setReadRowsBeforeDelay(0);

        cluster.schemaChange(format("CREATE TABLE %s.%s (k text, c text, v text, b blob, PRIMARY KEY (k, c))"));
        coordinator.execute(format("INSERT INTO %s.%s (k, c, v) VALUES ('secret_k', 'secret_c', 'secret_v')"), ALL);

        // verify that slow queries are logged with redacted values
        long mark = node.logs().mark();
        assertTimesOut("SELECT * FROM %s.%s WHERE k = 'secret_k' AND c = 'secret_c' AND v = 'secret_v' ALLOW FILTERING");
        assertLogsContain(mark, node, "operations timed out", format("<SELECT \\* FROM %s\\.%s WHERE k = \\? AND c = \\? AND v = \\? ALLOW FILTERING>"));
        assertLogsDoNotContain(mark, node, "secret_k", "secret_c", "secret_v");

        // verify that large values include size hints
        mark = node.logs().mark();
        String query = format("SELECT * FROM %s.%s WHERE b = ? ALLOW FILTERING");
        assertTimesOut(query, ByteBuffer.allocate(100 + 1));
        assertTimesOut(query, ByteBuffer.allocate(1024 + 1));
        assertTimesOut(query, ByteBuffer.allocate(10 * 1024 + 1));
        assertLogsContain(mark, node,
                          format("<SELECT \\* FROM %s\\.%s WHERE b = \\?\\[>100B\\] ALLOW FILTERING>"),
                          format("<SELECT \\* FROM %s\\.%s WHERE b = \\?\\[>1KiB\\] ALLOW FILTERING>"),
                          format("<SELECT \\* FROM %s\\.%s WHERE b = \\?\\[>10KiB\\] ALLOW FILTERING>"));
    }

    /**
     * Test that the slow query logger outputs the correct metrics for number of returned partitions, rows, etc.
     */
    @Test
    public void testLogsReadMetrics()
    {
        setReadRowsBeforeDelay(2);

        cluster.schemaChange(format("CREATE TABLE %s.%s (k int, c int, v int, l int, s int, PRIMARY KEY (k, c))"));
        cluster.schemaChange(format("CREATE INDEX legacy_idx ON %s.%s (l)"));
        cluster.schemaChange(format("CREATE CUSTOM INDEX sai_idx ON %s.%s (s) USING 'StorageAttachedIndex'"));
        int numPartitions = 10;
        int numClusterings = 10;
        int numRows = 0;
        for (int k = 0; k < numPartitions; k++)
            for (int c = 0; c < numClusterings; c++)
                coordinator.execute(format("INSERT INTO %s.%s (k, c, v, l, s) VALUES (?, ?, ?, ?, ?)"),
                                    ALL, k, c, numRows++, numRows % 2, numRows % 2);
        cluster.forEach(i -> i.flush(KEYSPACE));

        // unrestricted query
        long mark = node.logs().mark();
        assertTimesOut("SELECT * FROM %s.%s");
        assertLogsContain(mark, node,
                          format("<SELECT \\* FROM %s\\.%s ALLOW FILTERING>"),
                          "  Fetched/returned/tombstones:",
                          "    partitions: 1/1/0",
                          "    rows: 2/2/0");

        // partition query
        mark = node.logs().mark();
        assertTimesOut("SELECT * FROM %s.%s WHERE k = 2");
        assertLogsContain(mark, node,
                          format("<SELECT \\* FROM %s\\.%s WHERE k = \\? ALLOW FILTERING>"),
                          "  Fetched/returned/tombstones:",
                          "    partitions: 1/1/0",
                          "    rows: 2/2/0");

        // clustering query
        setReadRowsBeforeDelay(0);
        mark = node.logs().mark();
        assertTimesOut("SELECT * FROM %s.%s WHERE k = 2 AND c = 2");
        assertLogsContain(mark, node,
                          format("<SELECT \\* FROM %s\\.%s WHERE k = \\? AND c = \\? ALLOW FILTERING>"),
                          "  Fetched/returned/tombstones:",
                          "    partitions: 0/0/0",
                          "    rows: 0/0/0");

        // filtered range query
        setReadRowsBeforeDelay(2);
        mark = node.logs().mark();
        assertTimesOut("SELECT * FROM %s.%s WHERE v < 25 ALLOW FILTERING");
        assertLogsContain(mark, node,
                          format("<SELECT \\* FROM %s\\.%s WHERE v < \\? ALLOW FILTERING>"),
                          "  Fetched/returned/tombstones:",
                          "    partitions: 2/1/0",
                          "    rows: 12/2/0");

        // test multiple slow runs of different queries with the same redacted form, it should log the slowest one
        mark = node.logs().mark();
        for (int i = 0; i < numPartitions; i++)
        {
            assertTimesOut("SELECT * FROM %s.%s WHERE k = " + i);
        }
        assertLogsContain(mark, node,
                          format("<SELECT \\* FROM %s\\.%s WHERE k = \\? ALLOW FILTERING>"),
                          "  Slowest fetched/returned/tombstones:",
                          "    partitions: 1/1/0",
                          "    rows: 2/2/0");

        // delete the first partition and query again, to see partition tombstone metrics
        coordinator.execute(format("DELETE FROM %s.%s WHERE k = 5"), ALL);
        mark = node.logs().mark();
        assertTimesOut("SELECT * FROM %s.%s");
        assertLogsContain(mark, node,
                          format("<SELECT \\* FROM %s\\.%s ALLOW FILTERING>"),
                          "  Fetched/returned/tombstones:",
                          "    partitions: 1/1/1",
                          "    rows: 2/2/0");

        // delete a row and query again, to see row tombstone metrics
        coordinator.execute(format("DELETE FROM %s.%s WHERE k = 1 AND c = 1"), ALL);
        mark = node.logs().mark();
        assertTimesOut("SELECT * FROM %s.%s");
        assertLogsContain(mark, node,
                          format("<SELECT \\* FROM %s\\.%s ALLOW FILTERING>"),
                          "  Fetched/returned/tombstones:",
                          "    partitions: 1/1/1",
                          "    rows: 2/2/1");

        // delete a range of rows and query again, to see more row tombstone metrics
        coordinator.execute(format("DELETE FROM %s.%s WHERE k = 1 AND c < 5"), ALL);
        mark = node.logs().mark();
        assertTimesOut("SELECT * FROM %s.%s");
        assertLogsContain(mark, node,
                          format("<SELECT \\* FROM %s\\.%s ALLOW FILTERING>"),
                          "  Fetched/returned/tombstones:",
                          "    partitions: 1/1/1",
                          "    rows: 2/2/2");

        // query with a legacy index, which doesn't provide its own execution info, so generic execution info should be used
        mark = node.logs().mark();
        assertTimesOut("SELECT * FROM %s.%s WHERE l = 0");
        assertLogsContain(mark, node,
                          format("<SELECT \\* FROM %s\\.%s WHERE l = \\? ALLOW FILTERING>"),
                          "  Fetched/returned/tombstones:",
                          "    partitions: 1/1/1",
                          "    rows: 2/2/4");

        // query with a SAI index, which provides its own execution info, so generic execution info shouldn't be used
        mark = node.logs().mark();
        assertTimesOut("SELECT * FROM %s.%s WHERE s = 0");
        assertLogsContain(mark, node,
                          format("<SELECT \\* FROM %s\\.%s WHERE s = \\? ALLOW FILTERING>"),
                          "  SAI slow query metrics:",
                          "    sstablesHit: 3",
                          "    segmentsHit: 3",
                          "    keysFetched: 10",
                          "    partitionsFetched: 1",
                          "    partitionsReturned: 1",
                          "    partitionTombstonesFetched: 1",
                          "    rowsFetched: 3",
                          "    rowsReturned: 3",
                          "    rowTombstonesFetched: 4",
                          "    trieSegmentsHit: 0",
                          "    bkdPostingListsHit: 3",
                          "    bkdSegmentsHit: 3",
                          "    bkdPostingsSkips: 0",
                          "    bkdPostingsDecodes: 0",
                          "    triePostingsSkips: 0",
                          "    triePostingsDecodes: 0",
                          "    annGraphSearchLatencyNanos: 0",
                          "  SAI slow query plan:");
        assertLogsDoNotContain(mark, node, "  Fetched/returned/tombstones:");

        // disable generic execution info logging and verify that info is not logged anymore
        CassandraRelevantProperties.MONITORING_EXECUTION_INFO_ENABLED.setBoolean(false);
        logOperations();
        mark = node.logs().mark();
        assertTimesOut("SELECT * FROM %s.%s");
        assertLogsContain(mark, node, format("<SELECT \\* FROM %s\\.%s ALLOW FILTERING>"));
        assertLogsDoNotContain(mark, node, "  Fetched/returned/tombstones:", "  SAI slow query metrics:");
        CassandraRelevantProperties.MONITORING_EXECUTION_INFO_ENABLED.setBoolean(true);

        // disable SAI execution info logging and verify that info is not logged anymore
        CassandraRelevantProperties.SAI_MONITORING_EXECUTION_INFO_ENABLED.setBoolean(false);
        logOperations();
        mark = node.logs().mark();
        assertTimesOut("SELECT * FROM %s.%s WHERE s = 0");
        assertLogsContain(mark, node, format("<SELECT \\* FROM %s\\.%s WHERE s = \\? ALLOW FILTERING>"));
        assertLogsDoNotContain(mark, node, "  Fetched/returned/tombstones:", "  SAI slow query metrics:");
        CassandraRelevantProperties.SAI_MONITORING_EXECUTION_INFO_ENABLED.setBoolean(true);
    }

    private static String format(String query)
    {
        return String.format(query, KEYSPACE, table);
    }

    private void setReadRowsBeforeDelay(int maxRows)
    {
        cluster.get(2).runOnInstance(() -> BBHelper.readRowsBeforeDelay.set(maxRows));
    }

    private void assertTimesOut(String query, Object... boundValues)
    {
        String formattedQuery = format(query);
        Assertions.assertThatThrownBy(() -> coordinator.execute(formattedQuery, ALL, boundValues))
                  .hasMessageContaining("Operation");
    }

    public static void assertLogsContain(long mark, IInvokableInstance node, String... lines)
    {
        Awaitility.waitAtMost(30, TimeUnit.SECONDS)
                  .pollDelay(QUERY_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                  .untilAsserted(() -> {
                      logOperations();
                      for (String line : lines)
                      {
                          List<String> matchingLines = node.logs().grep(mark, line).getResult();
                          Assertions.assertThat(matchingLines).isNotEmpty();
                      }
                  });
    }

    public static void assertLogsDoNotContain(long mark, IInvokableInstance node, String... lines)
    {
        for (String line : lines)
        {
            List<String> matchingLines = node.logs().grep(mark, line).getResult();
            Assertions.assertThat(matchingLines).isEmpty();
        }
    }

    private static void logOperations()
    {
        node.runOnInstance(() -> MonitoringTask.instance.logOperations(approxTime.now()));
    }

    /**
     * ByteBuddy interceptor to delay reads on node 2 after having returned {@link BBHelper#readRowsBeforeDelay} rows.
     */
    public static class BBHelper
    {
        private static final AtomicInteger readRowsBeforeDelay = new AtomicInteger();

        @SuppressWarnings("resource")
        public static void install(ClassLoader classLoader, Integer node)
        {
            if (node != 2)
                return;

            new ByteBuddy().rebase(ReadCommand.class)
                           .method(named("executeLocally"))
                           .intercept(MethodDelegation.to(AbortedQueryLoggerTest.BBHelper.class))
                           .make()
                           .load(classLoader, ClassLoadingStrategy.Default.INJECTION);
        }

        @SuppressWarnings("unused")
        public static UnfilteredPartitionIterator executeLocally(ReadExecutionController executionController,
                                                                 @SuperCall Callable<UnfilteredPartitionIterator> zuperCall)
        {
            UnfilteredPartitionIterator result;
            try
            {
                result = zuperCall.call();
            }
            catch (Exception e)
            {
                throw Throwables.unchecked(e);
            }

            if (!executionController.metadata().keyspace.equals(KEYSPACE))
                return result;

            if (readRowsBeforeDelay.get() == 0)
            {
                delay();
                return result;
            }

            int nowInSec = FBUtilities.nowInSeconds();
            return Transformation.apply(result, new Transformation<>()
            {
                private int liveRows = 0;

                @Override
                protected UnfilteredRowIterator applyToPartition(UnfilteredRowIterator partition)
                {
                    return Transformation.apply(partition, new Transformation<>()
                    {
                        @Override
                        protected Row applyToRow(Row row)
                        {
                            if (row.hasLiveData(nowInSec, false) && ++liveRows >= readRowsBeforeDelay.get())
                                delay();

                            return row;
                        }
                    });
                }
            });
        }

        private static void delay()
        {
            Uninterruptibles.sleepUninterruptibly(QUERY_TIMEOUT_MS * 2, TimeUnit.MILLISECONDS);
        }
    }
}
