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

import java.util.Collections;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.codahale.metrics.Meter;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryEvents;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.exceptions.ReadFailureException;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.service.QueryState;

import static org.junit.Assert.assertEquals;

public class ClientRequestsMetricsAllRequestsTest extends CQLTester
{
    AllRequestsMetrics metrics = ClientRequestsMetricsProvider.instance.metrics(KEYSPACE).allRequestsMetrics;

    @Before
    public void setup()
    {
        createTable("CREATE TABLE %s (id INT PRIMARY KEY, v TEXT)");
    }

    @After
    public void teardown()
    {
        dropTable("DROP TABLE %s");
    }

    @Test
    public void testInvalidRequest()
    {
        long before = metrics.invalid.getCount();
        try
        {
            executeNet("INSERT INTO %s (id, v) VALUES (1, ?)");
        }
        catch (Throwable t)
        {
            // expected
        }
        assertEquals(1, metrics.invalid.getCount() - before);
    }

    @Test
    public void testInvalidPreparedRequest()
    {
        try(Session session = sessionNet())
        {
            PreparedStatement prepare1 = session.prepare(formatQuery("DELETE FROM %s WHERE id = ?"));
            BoundStatement bind = prepare1.bind();
            long before = metrics.invalid.getCount();
            try
            {
                session.execute(bind);
            }
            catch (Throwable t)
            {
                // expected
            }
            assertEquals(1, metrics.invalid.getCount() - before);
        }
    }

    @Test
    public void testTimeoutRequest()
    {
        testNotifyQueryFailure(metrics.timeouts, new ReadTimeoutException(ConsistencyLevel.ONE));
    }

    @Test
    public void testUnavailableRequest()
    {
        testNotifyQueryFailure(metrics.unavailables, UnavailableException.create(ConsistencyLevel.ONE, 1, 0));
    }

    @Test
    public void testFailureRequest()
    {
        testNotifyQueryFailure(metrics.failures, new ReadFailureException(ConsistencyLevel.ONE, 0, 0, false, Collections.emptyMap()));
    }

    @Test
    public void testOtherErrorRequest()
    {
        testNotifyQueryFailure(metrics.otherErrors, new RuntimeException());
    }

    public void testNotifyQueryFailure(Meter meter, Exception exception)
    {
        CQLStatement cqlStatement = parseStatement("CREATE TABLE %s (id INT PRIMARY KEY, v TEXT)");
        long before = meter.getCount();

        QueryEvents.instance.notifyQueryFailure(cqlStatement, cqlStatement.getRawCQLStatement(), QueryOptions.DEFAULT,
                                                QueryState.forInternalCalls(), exception);

        assertEquals(1, meter.getCount() - before);
    }


}
