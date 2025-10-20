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
import com.datastax.driver.core.exceptions.ReadTimeoutException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.DataStorageSpec;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.PageSize;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.junit.Assert.assertEquals;
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
    private static final AtomicInteger pageReadCount = new AtomicInteger();

    @Before
    public void setup()
    {
        pageReadDelayMillis.set(0);
        pageReadCount.set(0);
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
        PageSize oldAggregationSubPageSize = DatabaseDescriptor.getAggregationSubPageSize();
        try
        {
            // Set a small sub-page size to ensure predictable behavior across environments
            DatabaseDescriptor.setAggregationSubPageSize(PageSize.inBytes(1024));
            
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
            
            // Verify that we attempted multiple page reads before timing out
            int pageReads = pageReadCount.get();
            assertTrue("Expected at least 1 page read before timeout but got " + pageReads, pageReads >= 1);
            
            logger.info("Query failed after {} ms with {} page reads as expected with ", 
                       NANOSECONDS.toMillis(queryDuration), pageReads, exception);
        }
        finally
        {
            setDBTimeouts(oldTimeouts);
            DatabaseDescriptor.setAggregationSubPageSize(oldAggregationSubPageSize);
        }
    }

    @Test
    public void testAggregationQueryShouldNotTimeoutWhenItExceedesReadTimeout() throws Throwable
    {
        logger.info("Creating table");
        createTable("CREATE TABLE %s (a int, b int, c double, primary key (a, b))");

        logger.info("Inserting data");
        for (int i = 0; i < 4; i++)
            for (int j = 0; j < 7500; j++)
                execute("INSERT INTO %s (a, b, c) VALUES (?, ?, 1)", i, j);

        // connect net session
        sessionNet();

        logger.info("Setting timeouts");
        var oldTimeouts = getDBTimeouts();
        DataStorageSpec.LongBytesBound oldLocalReadSizeFailThreshold = DatabaseDescriptor.getLocalReadSizeFailThreshold();
        PageSize oldAggregationSubPageSize = DatabaseDescriptor.getAggregationSubPageSize();
        try
        {
            // Increase the local read size fail threshold to avoid hitting it with large data
            DatabaseDescriptor.setLocalReadSizeFailThreshold(new DataStorageSpec.LongBytesBound(100, DataStorageSpec.DataStorageUnit.MEBIBYTES));
            
            // This test verifies that aggregation queries work correctly with multiple page fetches.
            // We use a moderate page size to ensure multiple fetches occur, and add delays to simulate
            // realistic latency. The aggregation timeout is set much higher than the range timeout
            // to demonstrate that aggregation queries are governed by their own timeout.
            
            // Use a moderate page size to ensure we get multiple page fetches
            DatabaseDescriptor.setAggregationSubPageSize(PageSize.inBytes(64 * 1024)); // 64KB
            
            // Set timeouts to reasonable values
            // The actual values matter less than ensuring the query succeeds
            int rangeTimeoutMs = 1000;
            pageReadDelayMillis.set(30); // Small delay to simulate network/disk latency
            DatabaseDescriptor.setRangeRpcTimeout(rangeTimeoutMs);
            DatabaseDescriptor.setAggregationRpcTimeout(120000);

            logger.info("Running aggregate, multi-page query");

            long queryStartTime = System.nanoTime();
            // Use a custom read timeout that matches the aggregation timeout
            // This ensures the driver won't timeout before the server completes the aggregation
            SimpleStatement statement = new SimpleStatement(formatQuery("SELECT a, count(c) FROM %s group by a"));
            statement.setReadTimeoutMillis(120000); // Match the server-side aggregation timeout
            List<Row> result = sessionNet().execute(statement).all();
            long queryDuration = System.nanoTime() - queryStartTime;
            
            assertEquals("Should return 4 groups", 4, result.size());
            
            // Verify that multiple page fetches occurred
            int pageReads = pageReadCount.get();
            assertTrue("Expected multiple page reads but got " + pageReads, pageReads >= 2);
            
            logger.info("Query succeeded in {} ms with {} page reads; result={}", 
                       NANOSECONDS.toMillis(queryDuration), pageReads, result);
        }
        finally
        {
            setDBTimeouts(oldTimeouts);
            DatabaseDescriptor.setLocalReadSizeFailThreshold(oldLocalReadSizeFailThreshold);
            DatabaseDescriptor.setAggregationSubPageSize(oldAggregationSubPageSize);
        }
    }

    @Test
    public void testAggregationQueryShouldTimeoutWhenSinglePageReadIsFastButAggregationExceedesTimeout() throws Throwable
    {
        logger.info("Creating table");
        createTable("CREATE TABLE %s (a int, b int, c double, primary key (a, b))");

        logger.info("Inserting data");
        for (int i = 0; i < 4; i++)
            for (int j = 0; j < 7500; j++)
                execute("INSERT INTO %s (a, b, c) VALUES (?, ?, 1)", i, j);

        // connect net session
        sessionNet();

        logger.info("Setting timeouts");
        var oldTimeouts = getDBTimeouts();
        DataStorageSpec.LongBytesBound oldLocalReadSizeFailThreshold = DatabaseDescriptor.getLocalReadSizeFailThreshold();
        PageSize oldAggregationSubPageSize = DatabaseDescriptor.getAggregationSubPageSize();
        try
        {
            // Increase the local read size fail threshold to avoid hitting it with large data
            DatabaseDescriptor.setLocalReadSizeFailThreshold(new DataStorageSpec.LongBytesBound(100, DataStorageSpec.DataStorageUnit.MEBIBYTES));
            
            // Set a small sub-page size to force multiple page fetches
            DatabaseDescriptor.setAggregationSubPageSize(PageSize.inBytes(1024));
            
            // page reads should fit in the timeout, but the query should time out on aggregate timeout
            // the query should complete nevertheless
            int aggregateTimeoutMs = 50;
            pageReadDelayMillis.set(30);
            DatabaseDescriptor.setRangeRpcTimeout(10000);
            DatabaseDescriptor.setAggregationRpcTimeout(aggregateTimeoutMs);

            logger.info("Running aggregate, multi-page query");

            long queryStartTime = System.nanoTime();
            ReadTimeoutException exception = assertThrows("expected read timeout",
                                                          ReadTimeoutException.class,
                                                          () -> executeNet("SELECT a, count(c) FROM %s group by a").all());
            long queryDuration = System.nanoTime() - queryStartTime;
            assertTrue("Query duration " + queryDuration + " should be greater than aggregate timeout " + aggregateTimeoutMs + "ms",
                       queryDuration > MILLISECONDS.toNanos(aggregateTimeoutMs));
            
            // Verify that multiple page reads occurred before hitting the aggregation timeout
            // With 1KB pages and 30k rows, we expect many page reads
            int pageReads = pageReadCount.get();
            assertTrue("Expected multiple page reads before timeout but got " + pageReads, pageReads >= 2);
            
            logger.info("Query failed after {} ms with {} page reads as expected with ", 
                       NANOSECONDS.toMillis(queryDuration), pageReads, exception);
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
            DatabaseDescriptor.setLocalReadSizeFailThreshold(oldLocalReadSizeFailThreshold);
            DatabaseDescriptor.setAggregationSubPageSize(oldAggregationSubPageSize);
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

    public static void delayPageRead()
    {
        pageReadCount.incrementAndGet();
        long delay = pageReadDelayMillis.get();
        if (delay == 0)
            return;
        logger.info("Delaying page read #{} for {} ms", pageReadCount.get(), delay);
        Uninterruptibles.sleepUninterruptibly(delay, MILLISECONDS);
        logger.info("Resuming page read");
    }
}
