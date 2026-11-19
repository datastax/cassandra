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

package org.apache.cassandra.distributed.test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Assert;
import org.junit.Test;

import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.exceptions.OperationTimedOutException;
import com.datastax.driver.core.exceptions.OverloadedException;
import com.datastax.driver.core.exceptions.ReadTimeoutException;
import com.datastax.driver.core.policies.FallthroughRetryPolicy;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.metrics.ClientMetrics;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.transport.messages.ResultMessage;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;
import static org.apache.cassandra.config.CassandraRelevantProperties.TEST_READ_ITERATION_DELAY_MS;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;

public class OverloadTest extends TestBaseImpl
{
    @Test
    public void clientBackpressureDisabled() throws Throwable
    {
        testApplyClientBackpressure(0);
    }

    @Test
    public void applyClientBackpressure() throws Throwable
    {
        testApplyClientBackpressure(0.5);
    }

    private void testApplyClientBackpressure(double threshold) throws Throwable
    {
        try (Cluster control = init(Cluster.build().withNodes(1)
                                           .withInstanceInitializer(SlowSelect::install)
                                           .withConfig(config -> config.with(GOSSIP, NETWORK, NATIVE_PROTOCOL)
                                                                       .set("native_transport_queue_max_item_age_threshold", threshold)
                                                                       .set("concurrent_reads", 2)
                                                                       .set("native_transport_max_threads", 2)
                                                                       .set("read_request_timeout_in_ms", 1000)
                                                                       .set("write_request_timeout_in_ms", 1000)
                                                                       .set("range_request_timeout_in_ms", 1000)
                                                                       .set("counter_write_request_timeout_in_ms", 1000)
                                                                       .set("native_transport_timeout", "1000ms")
                                           )
                                           .start()))
        {
            control.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int PRIMARY KEY, v int)"));

            for (ProtocolVersion protocolVersion : new ProtocolVersion[]{ ProtocolVersion.V4, ProtocolVersion.V5 })
            {
                ExecutorService executor = Executors.newCachedThreadPool();
                try (com.datastax.driver.core.Cluster cluster = driver(control, protocolVersion);
                     Session session = cluster.connect(KEYSPACE))
                {
                    int pausedBefore = control.get(1).callsOnInstance(() -> {
                        return ClientMetrics.instance.getNumberOfPausedConnections();
                    }).call();

                    session.execute("select * from tbl").one();
                    control.get(1).runOnInstance(() -> {
                        Assert.assertTrue(SlowSelect.enabled.compareAndSet(false, true));
                    });
                    List<Future<?>> futures = new ArrayList<>();
                    int count = 128; // 128 at most because we do not want to exceed client-side queue size
                    for (int i = 0; i < count; i++)
                        futures.add(CompletableFuture.supplyAsync(() -> session.execute("select * from tbl").one(), executor));
                    // Wait and let requests in the head of the line queue up before the ones already submitted (they all got admitted
                    // to the queue since it was completely empty)
                    Thread.sleep(700);
                    for (int i = 0; i < count; i++)
                        futures.add(CompletableFuture.supplyAsync(() -> session.execute("select * from tbl").one(), executor));
                    int success = 0;
                    int timedOut = 0;
                    int overloaded = 0;
                    for (Future<?> future : futures)
                    {
                        try
                        {
                            future.get();
                            success++;
                        }
                        catch (ExecutionException e)
                        {
                            if (e.getCause() instanceof OperationTimedOutException)
                                timedOut++;
                            else if (e.getCause() instanceof ReadTimeoutException)
                                timedOut++;
                            else if (e.getCause() instanceof OverloadedException)
                                overloaded++;
                            else throw e;
                        }
                    }

                    Assert.assertEquals(count * 2, success + timedOut + overloaded);
                    int pausedAfter = control.get(1).callsOnInstance(() -> {
                        return ClientMetrics.instance.getNumberOfPausedConnections();
                    }).call();
                    if (threshold > 0)
                    {
                        Assert.assertTrue(String.format("Number of pauses after the test (%d) should have been larger than %s", pausedAfter, pausedBefore),
                                          pausedAfter > pausedBefore);
                    }
                    else
                    {
                        Assert.assertEquals(String.format("Number of pauses after the test (%d) should have been the same as before (%s)", pausedAfter, pausedBefore),
                                            pausedAfter, pausedBefore);

                    }
                }
                finally
                {
                    executor.shutdown();
                    control.get(1).runOnInstance(() -> SlowSelect.enabled.set(false));
                }
            }
        }
    }

    @Test
    public void testFinishInProgressQueries() throws Throwable
    {
        ExecutorService executor = Executors.newCachedThreadPool();
        try (Cluster control = init(Cluster.build().withNodes(1)
                                           .withInstanceInitializer(SlowSelect::install)
                                           .withConfig(config -> config.with(GOSSIP, NETWORK, NATIVE_PROTOCOL)
                                                                       .set("read_request_timeout_in_ms", 1000)
                                                                       .set("write_request_timeout_in_ms", 1000)
                                                                       .set("range_request_timeout_in_ms", 1000)
                                                                       .set("counter_write_request_timeout_in_ms", 1000)
                                           )
                                           .start());
             com.datastax.driver.core.Cluster cluster = driver(control, ProtocolVersion.V5);
             Session session = cluster.connect(KEYSPACE))
        {
            control.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int PRIMARY KEY, v int)"));
            session.execute("select * from tbl").one();
            control.get(1).runOnInstance(() -> {
                Assert.assertTrue(SlowSelect.enabled.compareAndSet(false, true));
            });
            List<Future<?>> futures = new ArrayList<>();
            for (int i = 0; i < 1000; i++)
                futures.add(CompletableFuture.supplyAsync(() -> session.execute("select * from tbl").one(), executor));

            int success = 0;
            int timedOut = 0;
            int overloaded = 0;
            for (Future<?> future : futures)
            {
                try
                {
                    future.get();
                    success++;
                }
                catch (ExecutionException e)
                {
                    if (e.getCause() instanceof OperationTimedOutException)
                        timedOut++;
                    else if (e.getCause() instanceof ReadTimeoutException)
                        timedOut++;
                    else if (e.getCause() instanceof OverloadedException)
                        overloaded++;
                    else throw e;
                }
            }

            Assert.assertEquals(1000, success + timedOut + overloaded);
        }
        finally
        {
            executor.shutdown();
        }
    }

    private static final int AGGREGATION_NATIVE_TRANSPORT_TIMEOUT_MILLIS = 2000;
    private static final int AGGREGATION_READ_TIMEOUT_MILLIS = 1000;
    private static final int AGGREGATION_ROWS = 100;
    /**
     * Delay injected by {@code TEST_READ_ITERATION_DELAY_MS} for every partition and every row a replica reads.
     */
    private static final int AGGREGATION_ROW_DELAY_MILLIS = 50;

    /**
     * An aggregation is made of many sub-pages, and its total duration is bounded by aggregation timeout,
     * not by native transport timeout or the verb timeout, both of which are expected to apply to each
     * individual sub-page only.
     * <p>
     * Setup (see {@link #loadUniformRows}): 100 rows of about 500 bytes, read in 1KiB sub-pages of two rows each.
     * <ul>
     *   <li>Every sub-page takes at least {@code 100ms}, which is a fraction of the 1s read timeout.</li>
     *   <li>The aggregation takes at least {@code 100 * 50ms = 5s}, which is more than the native transport timeout and
     *       the read timeout. If these two timeouts applied to the aggregation as a whole, it could not succeed.</li>
     *   <li>The 60s aggregation timeout is far above that, so the aggregation is expected to succeed.</li>
     * </ul>
     */
    @Test
    public void aggregationOutlivesNativeTransportAndReadTimeouts() throws Throwable
    {
        AggregationResult result = runAggregation("60000ms", AGGREGATION_NATIVE_TRANSPORT_TIMEOUT_MILLIS, AGGREGATION_READ_TIMEOUT_MILLIS,
                                                  OverloadTest::loadUniformRows);
        Assert.assertTrue("Aggregation was expected to succeed, but timed out", result.succeeded);
        Assert.assertTrue("Aggregation should have taken longer than native transport timeout, but took " + result.elapsedMillis + "ms",
                          result.elapsedMillis > AGGREGATION_NATIVE_TRANSPORT_TIMEOUT_MILLIS);
    }

    /**
     * Aggregation timeout still bounds the total duration of an aggregation.
     * <p>
     * Setup: the same data as above (100 rows, two rows per 1KiB sub-page, every sub-page taking at least 100ms), with
     * a 4s aggregation timeout, which is deliberately between the 2s native transport timeout and the time the
     * aggregation needs. The aggregation takes at least {@code 100 * 50ms = 5s}, which is more than the 4s aggregation timeout, so
     * it cannot complete in time and is guaranteed to time out.
     */
    @Test
    public void aggregationTimeoutIsStillEnforced() throws Throwable
    {
        long aggregationTimeoutMillis = 4000;
        Assert.assertTrue(aggregationTimeoutMillis > AGGREGATION_NATIVE_TRANSPORT_TIMEOUT_MILLIS);

        AggregationResult result = runAggregation(aggregationTimeoutMillis + "ms", AGGREGATION_NATIVE_TRANSPORT_TIMEOUT_MILLIS,
                                                  AGGREGATION_READ_TIMEOUT_MILLIS, OverloadTest::loadUniformRows);

        Assert.assertFalse("Aggregation was expected to time out", result.succeeded);
        Assert.assertTrue("Timeout was not triggered by the aggregation timeout", result.aggregationTimeoutLogged);
    }

    /**
     * Verb timeout is still applied to every sub-page, the first few sub-pages are fast,
     * but a later one takes longer than the read timeout and has to fail the aggregation, even though neither the
     * aggregation timeout nor the native transport timeout is reached.
     * <p>
     * Setup: 1s read timeout, 60s native transport and aggregation timeouts.
     * <ul>
     *   <li>Sub-pages 1 to 5 have two large rows each and take at least 100ms.
     *       Each is a fraction of the read timeout and should succeed.</li>
     *   <li>Sub-page 6 has all 30 tiny rows and takes at least 1.5s, which is more than the
     *       1s read timeout, so it is guaranteed to exceed it.</li>
     *   <li>Neither of the other two timeouts (60s) can have fired, which the missing aggregation timeout log line
     *       confirms.</li>
     * </ul>
     * Run the same data with a 60s read timeout later, which shows that the failure is caused by the read timeout.
     */
    @Test
    public void readTimeoutIsStillEnforcedForLaterSubPages() throws Throwable
    {
        AggregationResult result = runAggregation("60000ms", 60000, AGGREGATION_READ_TIMEOUT_MILLIS, OverloadTest::loadFastThenSlowSubPage);

        Assert.assertFalse("Aggregation was expected to time out, because one of its sub-pages exceeds the read timeout", result.succeeded);
        Assert.assertFalse("Timeout should not have been triggered by the aggregation timeout", result.aggregationTimeoutLogged);

        AggregationResult control = runAggregation("60000ms", 60000, 60000, OverloadTest::loadFastThenSlowSubPage);
        Assert.assertTrue("Aggregation was expected to succeed with a larger read timeout", control.succeeded);
    }

    private static class AggregationResult
    {
        final boolean succeeded;
        final long elapsedMillis;
        final boolean aggregationTimeoutLogged;

        AggregationResult(boolean succeeded, long elapsedMillis, boolean aggregationTimeoutLogged)
        {
            this.succeeded = succeeded;
            this.elapsedMillis = elapsedMillis;
            this.aggregationTimeoutLogged = aggregationTimeoutLogged;
        }
    }


    private interface AggregationData
    {
        int load(Cluster cluster);
    }

    /**
     * 500 bytes each
     */
    private static int loadUniformRows(Cluster cluster)
    {
        for (int i = 0; i < AGGREGATION_ROWS; i++)
            insertRow(cluster, i, String.join("", Collections.nCopies(500, "x")));
        return AGGREGATION_ROWS;
    }

    /**
     * 10 large rows (500 bytes each), which make 5 sub-pages of two rows
     * followed by 30 tiny rows (1 byte each) that all fit in a single 6th sub-page,
     * which takes at least {@code 1.5s}.
     */
    private static int loadFastThenSlowSubPage(Cluster cluster)
    {
        for (int i = 0; i < 10; i++)
            insertRow(cluster, i, String.join("", Collections.nCopies(500, "x")));
        for (int i = 10; i < 40; i++)
            insertRow(cluster, i, "x");
        return 40;
    }

    private static void insertRow(Cluster cluster, int ck, String value)
    {
        cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.tbl (pk, ck, v) VALUES (1, ?, ?)"), ConsistencyLevel.ONE, ck, value);
    }

    private AggregationResult runAggregation(String aggregationTimeout,
                                             int nativeTransportTimeoutMillis,
                                             int readTimeoutMillis,
                                             AggregationData data) throws Throwable
    {
        // Each row read on a replica takes at least AGGREGATION_ROW_DELAY_MILLIS.
        try (WithProperties ignored = new WithProperties().set(TEST_READ_ITERATION_DELAY_MS, AGGREGATION_ROW_DELAY_MILLIS);
             Cluster control = init(Cluster.build().withNodes(1)
                                           .withConfig(config -> config.with(GOSSIP, NETWORK, NATIVE_PROTOCOL)
                                                                       .set("read_request_timeout", readTimeoutMillis + "ms")
                                                                       .set("range_request_timeout", readTimeoutMillis + "ms")
                                                                       .set("native_transport_timeout", nativeTransportTimeoutMillis + "ms")
                                                                       .set("aggregation_request_timeout", aggregationTimeout)
                                                                       .set("aggregation_subpage_size", "1KiB"))
                                           .start()))
        {
            control.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int, ck int, v text, PRIMARY KEY (pk, ck))"));
            int rows = data.load(control);

            // No retries, so that the elapsed time is the time it took for the first attempt to fail
            try (com.datastax.driver.core.Cluster cluster = com.datastax.driver.core.Cluster.builder()
                                                                                            .addContactPoint(control.get(1).broadcastAddress().getHostString())
                                                                                            .withSocketOptions(new SocketOptions().setReadTimeoutMillis(120000))
                                                                                            .withPoolingOptions(new PoolingOptions().setHeartbeatIntervalSeconds(5000))
                                                                                            .withProtocolVersion(ProtocolVersion.V5)
                                                                                            .withRetryPolicy(FallthroughRetryPolicy.INSTANCE)
                                                                                            .build();
                 Session session = cluster.connect(KEYSPACE))
            {
                long start = System.nanoTime();
                boolean succeeded;
                try
                {
                    Assert.assertEquals(rows, session.execute("SELECT count(*) FROM tbl WHERE pk = 1").one().getLong(0));
                    succeeded = true;
                }
                catch (ReadTimeoutException e)
                {
                    succeeded = false;
                }
                long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
                // checkTimeout() in AggregationQueryPager logs this when it is the aggregation timeout that fired
                boolean aggregationTimeoutLogged = !control.get(1).logs().grep("Aggregation query timeout triggered").getResult().isEmpty();
                return new AggregationResult(succeeded, elapsedMillis, aggregationTimeoutLogged);
            }
        }
    }

    public static com.datastax.driver.core.Cluster driver(Cluster sut, ProtocolVersion protocolVersion)
    {
        return driver(sut, protocolVersion, 2000);
    }

    public static com.datastax.driver.core.Cluster driver(Cluster sut, ProtocolVersion protocolVersion, int readTimeoutMillis)
    {
        return com.datastax.driver.core.Cluster.builder().addContactPoint(sut.get(1).broadcastAddress().getHostString())
                                               .withSocketOptions(new SocketOptions().setReadTimeoutMillis(readTimeoutMillis))
                                               .withPoolingOptions(new PoolingOptions().setHeartbeatIntervalSeconds(5000))
                                               .withProtocolVersion(protocolVersion)
                                               .build();
    }

    public static class SlowSelect
    {
        static AtomicBoolean enabled = new AtomicBoolean(false);

        static void install(ClassLoader cl, int nodeNumber)
        {
            new ByteBuddy().rebase(SelectStatement.class)
                           .method(named("execute").and(takesArguments(QueryState.class, QueryOptions.class, Dispatcher.RequestTime.class)))
                           .intercept(MethodDelegation.to(SlowSelect.class))
                           .make()
                           .load(cl, ClassLoadingStrategy.Default.INJECTION);
        }

        public static ResultMessage.Rows execute(QueryState state, QueryOptions options, Dispatcher.RequestTime requestTime, @SuperCall Callable<ResultMessage.Rows> r) throws Exception
        {
            if (enabled.get() && !state.getClientState().isInternal)
            {
                Thread.sleep(1100);
            }
            return r.call();
        }
    }
}