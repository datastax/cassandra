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

package org.apache.cassandra.db.guardrails;

import java.util.Collections;

import org.apache.cassandra.config.DataStorageSpec;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.service.ClientWarn;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.PageSize;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.ProtocolVersion;

import static java.lang.String.format;
import static org.apache.cassandra.config.DataStorageSpec.DataStorageUnit.BYTES;
import static org.apache.cassandra.config.DataStorageSpec.DataStorageUnit.KIBIBYTES;

/**
 * Tests the guardrail for the page size, {@link Guardrails#pageSize}.
 */
public class GuardrailPageWeightTest extends ThresholdTester
{
    private static final int PAGE_WEIGHT_WARN_THRESHOLD_KIB = 4;
    private static final int PAGE_WEIGHT_FAIL_THRESHOLD_KIB = 7;

    public GuardrailPageWeightTest()
    {
        super(Math.toIntExact(KIBIBYTES.toBytes(PAGE_WEIGHT_WARN_THRESHOLD_KIB)) + "B",
                Math.toIntExact(KIBIBYTES.toBytes(PAGE_WEIGHT_FAIL_THRESHOLD_KIB)) + "B",
              Guardrails.pageWeight,
              Guardrails::setPageWeightThreshold,
              Guardrails::getPageWeightWarnThreshold,
              Guardrails::getPageWeightFailThreshold,
                bytes -> new DataStorageSpec.LongBytesBound(bytes, BYTES).toString(),
                size -> new DataStorageSpec.LongBytesBound(size).toBytes(),
                Integer.MAX_VALUE - 1);
    }

    @Before
    public void setupTest()
    {
        createTable("CREATE TABLE IF NOT EXISTS %s (k INT, c INT, v TEXT, PRIMARY KEY(k, c))");
    }

    @Test
    public void testSelectStatementAgainstPageWeight() throws Throwable
    {
        // regular query
        String query = "SELECT * FROM %s";
        assertPagingValid(query, 3);
        assertPagingValid(query, PAGE_WEIGHT_WARN_THRESHOLD_KIB);
        assertPagingWarns(query, 6);
        assertPagingWarns(query, PAGE_WEIGHT_FAIL_THRESHOLD_KIB);
        assertPagingFails(query, 11);

        // aggregation query
        query = "SELECT COUNT(*) FROM %s WHERE k=0";
        assertPagingNotSupported(query, 3);
        assertPagingNotSupported(query, PAGE_WEIGHT_WARN_THRESHOLD_KIB);
        assertPagingNotSupported(query, 6);
        assertPagingNotSupported(query, PAGE_WEIGHT_FAIL_THRESHOLD_KIB);
        assertPagingFails(query, 11);

        // query with limit over thresholds does not affect page weight guardrail
        query = "SELECT * FROM %s LIMIT 100";
        assertPagingValid(query, 3);
        assertPagingValid(query, PAGE_WEIGHT_WARN_THRESHOLD_KIB);
        assertPagingWarns(query, 6);
        assertPagingWarns(query, PAGE_WEIGHT_FAIL_THRESHOLD_KIB);
        assertPagingFails(query, 11);

        // query with limit under thresholds does not affect page weight guardrail
        query = "SELECT * FROM %s LIMIT 1";
        assertPagingValid(query, 3);
        assertPagingValid(query, PAGE_WEIGHT_WARN_THRESHOLD_KIB);
        assertPagingWarns(query, 6);
        assertPagingWarns(query, PAGE_WEIGHT_FAIL_THRESHOLD_KIB);
        assertPagingFails(query, 11);
    }

    @Test
    public void testExcludedUsers() throws Throwable
    {
        assertPagingIgnored("SELECT * FROM %s", PAGE_WEIGHT_WARN_THRESHOLD_KIB + 1);
        assertPagingIgnored("SELECT * FROM %s", PAGE_WEIGHT_FAIL_THRESHOLD_KIB + 1);
    }

    private void assertPagingValid(String query, int pageWeight) throws Throwable
    {
        assertValid(() -> executeWithPaging(userClientState, query, pageWeight));
    }

    private void assertPagingIgnored(String query, int pageWeight) throws Throwable
    {
        assertValid(() -> executeWithPaging(superClientState, query, pageWeight));
        assertValid(() -> executeWithPaging(systemClientState, query, pageWeight));
    }

    private void assertPagingWarns(String query, int pageWeight) throws Throwable
    {
        assertWarns(() -> executeWithPaging(userClientState, query, pageWeight),
                    format("Query for table %s with page weight %s bytes exceeds warning threshold of %s bytes.",
                           currentTable(), toBytes(pageWeight), toBytes(PAGE_WEIGHT_WARN_THRESHOLD_KIB)));
    }

    private void assertPagingFails(String query, int pageWeight) throws Throwable
    {
        assertFails(() -> executeWithPaging(userClientState, query, pageWeight),
                    format("Aborting query for table %s, page weight %s bytes exceeds fail threshold of %s bytes.",
                           currentTable(), toBytes(pageWeight), toBytes(PAGE_WEIGHT_FAIL_THRESHOLD_KIB)));
    }

    private void assertPagingNotSupported(String query, int pageWeight) throws Throwable
    {
        assertThrows(() -> executeWithPaging(userClientState, query, pageWeight), InvalidRequestException.class,
                    format("Paging in bytes is not supported for aggregation queries. Please specify the page size in rows."));
        ClientWarn.instance.resetWarnings();
        listener.clear();
    }

    private void executeWithPaging(ClientState state, String query, int pageWeightBytes)
    {
        QueryState queryState = new QueryState(state);

        String formattedQuery = formatQuery(query);
        CQLStatement statement = QueryProcessor.parseStatement(formattedQuery, queryState.getClientState());
        statement.validate(state);

        QueryOptions options = QueryOptions.create(ConsistencyLevel.ONE,
                                                   Collections.emptyList(),
                                                   false,
                                                   PageSize.inBytes(toBytes(pageWeightBytes)),
                                                   null,
                                                   null,
                                                   ProtocolVersion.CURRENT,
                                                   KEYSPACE);

        statement.executeLocally(queryState, options);
    }

    private int toBytes(int kib)
    {
        return Math.toIntExact(KIBIBYTES.toBytes(kib));
    }

}
