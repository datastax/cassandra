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

package org.apache.cassandra.cql3;

import java.util.Collections;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.guardrails.GuardrailTester;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.transport.messages.ResultMessage;

import static org.junit.Assert.assertTrue;

public class GuardrailPagingTest extends GuardrailTester
{
    private static int defaultPageSizeThreshold;
    private static final int pageSizeThresholdInKB = 14;

    private static final int partitionCount = 10;
    private static final int rowsPerPartition = 100;

    @BeforeClass
    public static void setup()
    {
        defaultPageSizeThreshold = DatabaseDescriptor.getGuardrailsConfig().page_size_failure_threshold_in_kb;
    }

    @AfterClass
    public static void tearDown()
    {
        DatabaseDescriptor.getGuardrailsConfig().page_size_failure_threshold_in_kb = defaultPageSizeThreshold;
    }

    @Before
    public void setUp() throws Throwable
    {
        DatabaseDescriptor.getGuardrailsConfig().page_size_failure_threshold_in_kb = pageSizeThresholdInKB;
        createTable("CREATE TABLE IF NOT EXISTS %s (k INT, c INT, v TEXT, PRIMARY KEY(k, c))");

        for (int i = 0; i < partitionCount; i++)
            for (int j = 0; j < rowsPerPartition; j++)
                execute("INSERT INTO %s (k, c, v) VALUES (?, ?, ?)", i, j, "test message");
    }


    private ResultMessage.Rows selectWithPaging(int size, PageSize.PageUnit unit) throws InvalidRequestException
    {
        PageSize pageSize = new PageSize(size, unit);
        QueryOptions options = QueryOptions.create(ConsistencyLevel.LOCAL_QUORUM,
                                                   Collections.emptyList(),
                                                   false,
                                                   pageSize,
                                                   null,
                                                   ConsistencyLevel.LOCAL_SERIAL,
                                                   ProtocolVersion.CURRENT,
                                                   KEYSPACE);

        ClientState clientState = ClientState.forExternalCalls(AuthenticatedUser.ANONYMOUS_USER);
        clientState.setKeyspace(KEYSPACE);
        QueryState queryState = new QueryState(clientState);

        QueryHandler.Prepared prepared = QueryProcessor.prepareInternal(String.format("SELECT * FROM %s.%s", KEYSPACE, currentTable()));
        return (ResultMessage.Rows) prepared.statement.execute(queryState, options, System.nanoTime());
    }

    /**
     * Test that the number of returned rows per page is silently limited to fit into the guardrail hard limit
     */
    @Test
    public void testQueryWithPagedByRows() throws Throwable
    {
        // ask for more rows per page than can fit with the current guardrail
        int rowLimit = partitionCount * rowsPerPartition;
        ResultMessage.Rows result = selectWithPaging(rowLimit, PageSize.PageUnit.ROWS);
        assertTrue(result.result.rows.size() < rowLimit);
    }

    /**
     * Test that a query throws with page size that is bigger than the guardrail hard limit
     */
    @Test(expected = InvalidRequestException.class)
    public void testQueryWithLargeBytePagesThrows() throws Throwable
    {
        selectWithPaging(140 * 1024, PageSize.PageUnit.BYTES);
    }

    /**
     * Test that a query does not throw with page size that is smaller than the guardrail hard limit
     */
    @Test
    public void testQueryWithSmallBytePagesWorks() throws Throwable
    {
        int maxPageSize = 100 * 128;
        ResultMessage.Rows result = selectWithPaging(maxPageSize, PageSize.PageUnit.BYTES);
        // technically incorrect as we compare a size of encoded message to be sent to a client to the page size,
        // but we can't know the page at this point.
        assertTrue(ResultMessage.codec.encodedSize(result, ProtocolVersion.CURRENT) < maxPageSize);
    }
}
