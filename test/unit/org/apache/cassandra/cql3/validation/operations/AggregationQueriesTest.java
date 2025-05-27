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
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.exceptions.ReadTimeoutException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

@RunWith(BMUnitRunner.class)
@BMRule(
name = "delay page read",
targetClass = "org.apache.cassandra.service.StorageProxy",
targetMethod = "getRangeSlice",
targetLocation = "AT ENTRY",
action = "org.apache.cassandra.cql3.validation.operations.AggregationQueriesTest.delayPageRead();")
public class AggregationQueriesTest extends CQLTester
{
    private static final AtomicLong pageReadDelayMillis = new AtomicLong();

    @Before
    public void setup()
    {
        pageReadDelayMillis.set(0);
    }

    @Test
    public void testAggregationQueryShouldTimeoutWhenSinglePageReadExceedesReadTimeout() throws Throwable
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
            // 3rd and subsequent page reads should be delayed enough to time out the query
            int rangeTimeoutMs = 50;
            DatabaseDescriptor.setRangeRpcTimeout(rangeTimeoutMs);
            pageReadDelayMillis.set(100);

            logger.info("Running aggregate, multi-page query");

            long queryStartTime = System.nanoTime();
            Exception exception = assertThrows("expected read timeout",
                                                          Exception.class,
                                                          () -> executeNet("SELECT a, count(c) FROM %s group by a").all());
            assertTrue("Expected ReadTimeoutException or ReadFailureException but got " + exception.getClass().getName(),
                       exception instanceof ReadTimeoutException || exception instanceof com.datastax.driver.core.exceptions.ReadFailureException);
            long queryDuration = System.nanoTime() - queryStartTime;
            assertTrue("Query duration " + queryDuration + " should be greater than range read timeout " + rangeTimeoutMs + "ms",
                       queryDuration > MILLISECONDS.toNanos(rangeTimeoutMs));
            logger.info("Query failed after {} ms as expected with ", NANOSECONDS.toMillis(queryDuration), exception);
        }
        finally
        {
            setDBTimeouts(oldTimeouts);
        }
    }

    @Test
    public void testAggregationQueryShouldNotTimeoutWhenItExceedesReadTimeout() throws Throwable
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
            int rangeTimeoutMs = 2000;
            pageReadDelayMillis.set(400);
            DatabaseDescriptor.setRangeRpcTimeout(rangeTimeoutMs);
            DatabaseDescriptor.setAggregationRpcTimeout(120000);

            logger.info("Running aggregate, multi-page query");

            long queryStartTime = System.nanoTime();
            List<Row> result = executeNet("SELECT a, count(c) FROM %s group by a").all();
            long queryDuration = System.nanoTime() - queryStartTime;
            assertTrue("Query duration " + queryDuration + " should be greater than range read timeout " + rangeTimeoutMs + "ms",
                       queryDuration > MILLISECONDS.toNanos(rangeTimeoutMs));
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
            int aggregateTimeoutMs = 1000;
            pageReadDelayMillis.set(400);
            DatabaseDescriptor.setRangeRpcTimeout(10000);
            DatabaseDescriptor.setAggregationRpcTimeout(aggregateTimeoutMs);

            logger.info("Running aggregate, multi-page query");

            long queryStartTime = System.nanoTime();
            Exception exception = assertThrows("expected read timeout",
                                                          Exception.class,
                                                          () -> executeNet("SELECT a, count(c) FROM %s group by a").all());
            assertTrue("Expected ReadTimeoutException or ReadFailureException but got " + exception.getClass().getName(),
                       exception instanceof ReadTimeoutException || exception instanceof com.datastax.driver.core.exceptions.ReadFailureException);
            long queryDuration = System.nanoTime() - queryStartTime;
            assertTrue("Query duration " + queryDuration + " should be greater than aggregate timeout " + aggregateTimeoutMs + "ms",
                       queryDuration > MILLISECONDS.toNanos(aggregateTimeoutMs));
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
        long delay = pageReadDelayMillis.get();
        if (delay == 0)
            return;
        logger.info("Delaying page read for {} ms", delay);
        Uninterruptibles.sleepUninterruptibly(delay, MILLISECONDS);
        logger.info("Resuming page read");
    }
}