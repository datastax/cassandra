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

package org.apache.cassandra.transport;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.OverloadedException;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.metrics.ClientMetrics;
import org.assertj.core.api.Assertions;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;

import static org.apache.cassandra.utils.MonotonicClock.approxTime;

@RunWith(BMUnitRunner.class)
public class NativeTransportTimeoutTest extends CQLTester
{
    static Semaphore EXECUTE_BARRIER;
    static Semaphore WAIT_BARRIER;

    @Test
    @BMRules(rules = { @BMRule(name = "Delay Message execution on NTR stage",
                       targetClass = "org.apache.cassandra.transport.Message$Request",
                       targetMethod = "execute",
                       targetLocation = "AT ENTRY",
                       condition = "$this.getCustomPayload() != null",
                       action = "org.apache.cassandra.transport.NativeTransportTimeoutTest.WAIT_BARRIER.release(); " +
                                "org.apache.cassandra.transport.NativeTransportTimeoutTest.EXECUTE_BARRIER.acquire(); " +
                                "flag(Thread.currentThread());"),
                       @BMRule(name = "Mock NTR timeout from Request.execute",
                       targetClass = "org.apache.cassandra.config.DatabaseDescriptor",
                       targetMethod = "getNativeTransportTimeout",
                       targetLocation = "AT ENTRY",
                       condition = "flagged(Thread.currentThread()) && callerEquals(\"Message$Request.execute\", true)",
                       action = "clear(Thread.currentThread()); " +
                                "return 10000000;") })
    public void testNativeTransportLoadShedding() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v text)");
        Statement statement = new SimpleStatement("SELECT * FROM " + KEYSPACE + '.' + currentTable());
        doTestLoadShedding(false, statement);
    }

    @Test
    @BMRules(rules = { @BMRule(name = "Delay elapsedTimeSinceCreationCheck from async stage",
                       targetClass = "org.apache.cassandra.transport.Message$Request",
                       targetMethod = "elapsedTimeSinceCreation",
                       targetLocation = "AT ENTRY",
                       condition = "$this.getCustomPayload() != null && !callerEquals(\"Message$Request.execute\", true)",
                       action = "org.apache.cassandra.transport.NativeTransportTimeoutTest.WAIT_BARRIER.release(); " +
                                "org.apache.cassandra.transport.NativeTransportTimeoutTest.EXECUTE_BARRIER.acquire(); " +
                                "flag(Thread.currentThread());"),
                       @BMRule(name = "Mock native transport timeout from async stage",
                       targetClass = "org.apache.cassandra.config.DatabaseDescriptor",
                       targetMethod = "getNativeTransportTimeout",
                       targetLocation = "AT ENTRY",
                       condition = "flagged(Thread.currentThread()) && callerMatches(\".*maybeExecuteAsync.*\", true)",
                       action = "clear(Thread.currentThread()); " +
                                "return 10000000;") })
    public void testAsyncStageLoadShedding() throws Throwable
    {
        CassandraRelevantProperties.NATIVE_TRANSPORT_ASYNC_READ_WRITE_ENABLED.setBoolean(true);

        try
        {
            createTable("CREATE TABLE %s (pk int PRIMARY KEY, v text)");

            Statement statement = new SimpleStatement("SELECT * FROM " + KEYSPACE + '.' + currentTable());
            doTestLoadShedding(true, statement);

            Statement insert1 = new SimpleStatement("INSERT INTO " + KEYSPACE + '.' + currentTable() + " (pk, v) VALUES (1, 'foo')");
            Statement insert2 = new SimpleStatement("INSERT INTO " + KEYSPACE + '.' + currentTable() + " (pk, v) VALUES (2, 'bar')");
            statement = new BatchStatement().add(insert1).add(insert2);
            doTestLoadShedding(true, statement);

            PreparedStatement ps = sessionNet().prepare("SELECT * FROM " + KEYSPACE + '.' + currentTable());
            doTestLoadShedding(true, ps.bind());
        }
        finally
        {
            CassandraRelevantProperties.NATIVE_TRANSPORT_ASYNC_READ_WRITE_ENABLED.setBoolean(false);
        }
    }

    private void doTestLoadShedding(boolean useAsyncStages, Statement statement) throws InterruptedException
    {
        EXECUTE_BARRIER = new Semaphore(0);
        WAIT_BARRIER = new Semaphore(0);

        Meter timedOutMeter;
        Timer queueTimer;

        Session session = sessionNet();

        // custom payload used to make detection of this statement easy early in byteman rules
        statement.setOutgoingPayload(Collections.singletonMap("sentinel", ByteBuffer.wrap(new byte[0])));

        if (useAsyncStages)
        {
            timedOutMeter = ClientMetrics.instance.timedOutBeforeAsyncProcessing;
            queueTimer = ClientMetrics.instance.asyncQueueTime;
        }
        else
        {
            timedOutMeter = ClientMetrics.instance.timedOutBeforeProcessing;
            queueTimer = ClientMetrics.instance.queueTime;
        }

        long initialTimedOut = timedOutMeter.getCount();

        ResultSetFuture rsf = session.executeAsync(statement);

        // once WAIT_BARRIER is acquired, the Stage we want an OverloadedException from is executing the statement,
        // but it hasn't yet retrieved the elapsed time. It will not proceed until the EXECUTE_BARRIER is released.
        // The Byteman rules in the tests will override the native transport timeout to 10 milliseconds from that
        // callsite. Therefore, to ensure an OverloadedException by exceeding the timeout, we need to sleep for 10
        // milliseconds plus 2x the error of approxTime (creation timestamp error + error when getting current time).
        WAIT_BARRIER.acquire();
        Thread.sleep(10 + TimeUnit.MILLISECONDS.convert(approxTime.error(), TimeUnit.NANOSECONDS) * 2);
        EXECUTE_BARRIER.release();

        Assertions.assertThatThrownBy(rsf::get).hasCauseInstanceOf(OverloadedException.class);
        Assert.assertEquals(initialTimedOut + 1, timedOutMeter.getCount());
        Assert.assertTrue(queueTimer.getSnapshot().get999thPercentile() > TimeUnit.NANOSECONDS.convert(10, TimeUnit.MILLISECONDS));
    }
}
