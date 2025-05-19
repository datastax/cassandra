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

package org.apache.cassandra.cql3.validation.operations;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.exceptions.ReadFailureException;
import com.datastax.driver.core.exceptions.ReadTimeoutException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.metrics.ClientRequestsMetrics;
import org.apache.cassandra.metrics.ClientRequestsMetricsProvider;
import org.apache.cassandra.metrics.ClientRequestsMetricsUtil;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

@RunWith(BMUnitRunner.class)
@BMRules(rules = {
    @BMRule(
        name = "delay page read",
        targetClass = "org.apache.cassandra.db.ReadCommandVerbHandler",
        targetMethod = "doVerb",
        targetLocation = "AT ENTRY",
        action = "org.apache.cassandra.cql3.validation.operations.AggregationQueriesTest.delayPageRead();"),
    @BMRule(
        name = "dont issue local reads",
        targetClass = "org.apache.cassandra.service.reads.range.NonGroupingRangeCommandIterator",
        targetMethod = "isLocalRead",
        targetLocation = "AT ENTRY",
        action = "return false;")
})
public class AggregationQueriesTest extends CQLTester
{
    private static final AtomicLong pageReadDelayMillis = new AtomicLong();
    private static final AtomicInteger pageReadsCounter = new AtomicInteger();

    @Before
    public void setup()
    {
        pageReadDelayMillis.set(0);
    }

    @Test
    public void testAggregationQueryShouldFailWhenSinglePageReadExceedesReadTimeout() throws Throwable
    {
        logger.info("Creating table");
        createTable("CREATE TABLE %s (a int, b int, c double, primary key (a, b))");

        logger.info("Inserting data");
        for (int i = 0; i < 4; i++)
            for (int j = 0; j < 200; j++)
                execute("INSERT INTO %s (a, b, c) VALUES (?, ?, 1)", i, j);

        // connect net session
        sessionNet();

        logger.info("Setting timeouts");
        var oldTimeouts = getDBTimeouts();
        try
        {
            int rangeTimeoutMs = 50;
            DatabaseDescriptor.setRangeRpcTimeout(rangeTimeoutMs);
            pageReadDelayMillis.set(1000);

            ClientRequestsMetricsUtil.clearMetrics(ClientRequestsMetricsProvider.instance.metrics(KEYSPACE));
            pageReadsCounter.set(0);
            logger.info("Running aggregate, multi-page query");

            long queryStartTime = System.nanoTime();
            // please note that we get ReadFailureException, not timeout
            // the timeout for the whole operation is not exceeded, but a single page read timed out internally
            ReadFailureException exception = assertThrows("expected read timeout",
                                                          ReadFailureException.class,
                                                          () -> executeNet("SELECT a, count(c) FROM %s group by a").all());
            long queryDuration = System.nanoTime() - queryStartTime;
            assertTrue("Query duration " + queryDuration + " should be greater than range read timeout " + rangeTimeoutMs + "ms",
                       queryDuration > MILLISECONDS.toNanos(rangeTimeoutMs));
            // add assertion about metrics; there should be 0 client range reads and 1 aggregate read
            ClientRequestsMetrics metrics = ClientRequestsMetricsProvider.instance.metrics(KEYSPACE);
            assertEquals(0, metrics.rangeMetrics.serviceTimeMetrics.latency.getCount());
            assertEquals(1, metrics.aggregationMetrics.serviceTimeMetrics.latency.getCount());
            assertEquals(0, metrics.aggregationMetrics.timeouts.getCount());
            assertEquals(pageReadsCounter.get(), metrics.aggregationMetrics.roundTrips.getSnapshot().getMax());
            assertEquals(1, metrics.aggregationMetrics.failures.getCount());
            assertEquals(0, metrics.aggregationMetrics.unavailables.getCount());
            assertTrue("aggregate query should have taken longer than range read timeout " + rangeTimeoutMs + ", but it was " +
                       metrics.aggregationMetrics.serviceTimeMetrics.latency.getSnapshot().getMax(),
                       metrics.aggregationMetrics.serviceTimeMetrics.latency.getSnapshot().getMax() > rangeTimeoutMs);
            logger.info("Query failed after {} ms as expected with ", NANOSECONDS.toMillis(queryDuration), exception);
        }
        finally
        {
            setDBTimeouts(oldTimeouts);
        }
    }

    @Test
    public void testAggregationQueryShouldNotTimeoutWhenItExceedesReadTimeoutButNotAggregateTimeout() throws Throwable
    {
        logger.info("Creating table");
        createTable("CREATE TABLE %s (a int, b int, c double, primary key (a, b))");

        logger.info("Inserting data");
        for (int i = 0; i < 4; i++)
            for (int j = 0; j < 40000; j++)
                execute("INSERT INTO %s (a, b, c) VALUES (?, ?, 1)", i, j);

        // connect net session
        sessionNet();

        logger.info("Setting timeouts");
        var oldTimeouts = getDBTimeouts();
        try
        {
            // single page read should fit in the range timeout, but multiple pages should not;
            // the query should complete nevertheless because aggregate timeout is large
            int rangeTimeoutMs = 1000;
            pageReadDelayMillis.set(200);
            DatabaseDescriptor.setRangeRpcTimeout(rangeTimeoutMs);
            DatabaseDescriptor.setAggregationRpcTimeout(120000);

            ClientRequestsMetricsUtil.clearMetrics(ClientRequestsMetricsProvider.instance.metrics(KEYSPACE));
            pageReadsCounter.set(0);
            logger.info("Running aggregate, multi-page query");

            long queryStartTime = System.nanoTime();
            List<Row> result = executeNet("SELECT a, count(c) FROM %s group by a").all();
            long queryDuration = System.nanoTime() - queryStartTime;
            assertTrue("Query duration " + queryDuration + " should be greater than range read timeout " + rangeTimeoutMs + "ms",
                       queryDuration > MILLISECONDS.toNanos(rangeTimeoutMs));
            ClientRequestsMetrics metrics = ClientRequestsMetricsProvider.instance.metrics(KEYSPACE);
            assertEquals(0, metrics.rangeMetrics.serviceTimeMetrics.latency.getCount());
            assertEquals(0, metrics.rangeMetrics.timeouts.getCount());
            assertEquals(0, metrics.aggregationMetrics.timeouts.getCount());
            assertEquals(1, metrics.aggregationMetrics.serviceTimeMetrics.latency.getCount());
            assertEquals(pageReadsCounter.get(), metrics.aggregationMetrics.roundTrips.getSnapshot().getMax());
            assertTrue("aggregate query should have read multiple sub-pages",
                       metrics.aggregationMetrics.roundTrips.getSnapshot().getMax() > 1);
            assertTrue("aggregate query should have taken longer than range read timeout " + rangeTimeoutMs + ", but it was " +
                       metrics.aggregationMetrics.serviceTimeMetrics.latency.getSnapshot().getMax(),
                       metrics.aggregationMetrics.serviceTimeMetrics.latency.getSnapshot().getMax() > rangeTimeoutMs);

            logger.info("Query succeeded in {} ms as expected; result={}", NANOSECONDS.toMillis(queryDuration), result);
        }
        finally
        {
            setDBTimeouts(oldTimeouts);
        }
    }

    @Test
    public void testAggregationQueryShouldTimeoutWhenSinglePageReadIsFastButAggregationExceedesTimeout() throws Throwable
    {
        logger.info("Creating table");
        createTable("CREATE TABLE %s (a int, b int, c double, primary key (a, b))");

        logger.info("Inserting data");
        for (int i = 0; i < 4; i++)
            for (int j = 0; j < 40000; j++)
                execute("INSERT INTO %s (a, b, c) VALUES (?, ?, 1)", i, j);

        // connect net session
        sessionNet();

        logger.info("Setting timeouts");
        var oldTimeouts = getDBTimeouts();
        try
        {
            // page reads should fit in the timeout, but the query should time out on aggregate timeout
            // the query should complete nevertheless
            int aggregationTimeoutMs = 2000;
            pageReadDelayMillis.set(500);
            DatabaseDescriptor.setRangeRpcTimeout(10000);
            DatabaseDescriptor.setAggregationRpcTimeout(aggregationTimeoutMs);

            ClientRequestsMetricsUtil.clearMetrics(ClientRequestsMetricsProvider.instance.metrics(KEYSPACE));
            pageReadsCounter.set(0);
            logger.info("Running aggregate, multi-page query");

            long queryStartTime = System.nanoTime();
            ReadTimeoutException exception = assertThrows("expected read timeout",
                                                          ReadTimeoutException.class,
                                                          () -> executeNet(new SimpleStatement(formatQuery("SELECT a, count(c) FROM %s GROUP BY a")).setReadTimeoutMillis(60000)).all());
            long queryDuration = System.nanoTime() - queryStartTime;
            assertTrue("Query duration " + queryDuration + " should be greater than aggregate timeout " + aggregationTimeoutMs + "ms",
                       queryDuration > MILLISECONDS.toNanos(aggregationTimeoutMs));
            ClientRequestsMetrics metrics = ClientRequestsMetricsProvider.instance.metrics(KEYSPACE);
            assertEquals(0, metrics.rangeMetrics.serviceTimeMetrics.latency.getCount());
            assertEquals(1, metrics.aggregationMetrics.serviceTimeMetrics.latency.getCount());
            assertEquals(0, metrics.rangeMetrics.timeouts.getCount());
            assertEquals(1, metrics.aggregationMetrics.timeouts.getCount());
            assertEquals(pageReadsCounter.get(), metrics.aggregationMetrics.roundTrips.getSnapshot().getMax());
            assertTrue("aggregate query should have read multiple sub-pages",
                       metrics.aggregationMetrics.roundTrips.getSnapshot().getMax() > 1);
            assertTrue("aggregate query should have taken longer than aggregation read timeout " + aggregationTimeoutMs + ", but it was " +
                       metrics.aggregationMetrics.serviceTimeMetrics.latency.getSnapshot().getMax(),
                       metrics.aggregationMetrics.serviceTimeMetrics.latency.getSnapshot().getMax() > aggregationTimeoutMs);

            logger.info("Query failed after {} ms as expected with ", NANOSECONDS.toMillis(queryDuration), exception);
        }
        catch (Exception e)
        {
            logger.error("Query failed", e);
            logger.info("let's wait for a while and see what happens");
            Uninterruptibles.sleepUninterruptibly(10, TimeUnit.SECONDS);
            throw e;
        }
        finally
        {
            setDBTimeouts(oldTimeouts);
        }
    }

    @Test
    public void testSinglePartitionAggregationShouldBumpAggregationMetrics() throws Throwable
    {
        logger.info("Creating table");
        createTable("CREATE TABLE %s (a int, b int, c double, primary key (a, b))");

        logger.info("Inserting data");
        for (int i = 0; i < 4; i++)
            for (int j = 0; j < 40000; j++)
                execute("INSERT INTO %s (a, b, c) VALUES (?, ?, 1)", i, j);

        // connect net session
        sessionNet();

        logger.info("Setting timeouts");
        var oldTimeouts = getDBTimeouts();
        try
        {
            // page reads should fit in the timeout, but the query should time out on aggregate timeout
            // the query should complete nevertheless
            int aggregationTimeoutMs = 2000;
            pageReadDelayMillis.set(300);
            DatabaseDescriptor.setRangeRpcTimeout(10000);
            DatabaseDescriptor.setAggregationRpcTimeout(aggregationTimeoutMs);

            ClientRequestsMetricsUtil.clearMetrics(ClientRequestsMetricsProvider.instance.metrics(KEYSPACE));
            pageReadsCounter.set(0);
            logger.info("Running aggregate, multi-page query");

            long queryStartTime = System.nanoTime();
            ReadTimeoutException exception = assertThrows("expected read timeout",
                    ReadTimeoutException.class,
                    () -> executeNet(new SimpleStatement(formatQuery("SELECT a, b, count(c) FROM %s WHERE a=1 GROUP BY b")).setReadTimeoutMillis(60000)).all());
            long queryDuration = System.nanoTime() - queryStartTime;
            assertTrue("Query duration " + queryDuration + " should be greater than aggregate timeout " + aggregationTimeoutMs + "ms",
                    queryDuration > MILLISECONDS.toNanos(aggregationTimeoutMs));
            ClientRequestsMetrics metrics = ClientRequestsMetricsProvider.instance.metrics(KEYSPACE);
            assertEquals(0, metrics.rangeMetrics.serviceTimeMetrics.latency.getCount());
            assertEquals(1, metrics.aggregationMetrics.serviceTimeMetrics.latency.getCount());
            assertEquals(0, metrics.rangeMetrics.timeouts.getCount());
            assertEquals(1, metrics.aggregationMetrics.timeouts.getCount());
            assertEquals(1, metrics.aggregationMetrics.roundTrips.getSnapshot().getMax());
            assertTrue("aggregate query should have read multiple sub-pages",
                    metrics.aggregationMetrics.roundTrips.getSnapshot().getMax() > 1);
            assertTrue("aggregate query should have taken longer than aggregation read timeout " + aggregationTimeoutMs + ", but it was " +
                            metrics.aggregationMetrics.serviceTimeMetrics.latency.getSnapshot().getMax(),
                    metrics.aggregationMetrics.serviceTimeMetrics.latency.getSnapshot().getMax() > aggregationTimeoutMs);

            logger.info("Query failed after {} ms as expected with ", NANOSECONDS.toMillis(queryDuration), exception);
        }
        catch (Exception e)
        {
            logger.error("Query failed", e);
            logger.info("let's wait for a while and see what happens");
            Uninterruptibles.sleepUninterruptibly(10, TimeUnit.SECONDS);
            throw e;
        }
        finally
        {
            setDBTimeouts(oldTimeouts);
        }
    }

    private long[] getDBTimeouts()
    {
        return new long[]{
        DatabaseDescriptor.getReadRpcTimeout(MILLISECONDS),
        DatabaseDescriptor.getRangeRpcTimeout(MILLISECONDS),
        DatabaseDescriptor.getAggregationRpcTimeout(MILLISECONDS)
        };
    }

    private void setDBTimeouts(long[] timeouts)
    {
        DatabaseDescriptor.setReadRpcTimeout(timeouts[0]);
        DatabaseDescriptor.setRangeRpcTimeout(timeouts[1]);
        DatabaseDescriptor.setAggregationRpcTimeout(timeouts[2]);
    }

    private static void delayPageRead()
    {
        pageReadsCounter.incrementAndGet();
        long delay = pageReadDelayMillis.get();
        if (delay == 0)
            return;
        logger.info("Delaying page read for {} ms", delay);
        Uninterruptibles.sleepUninterruptibly(delay, MILLISECONDS);
        logger.info("Resuming page read");
    }
}