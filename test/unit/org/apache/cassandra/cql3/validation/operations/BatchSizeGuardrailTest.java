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
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang.RandomStringUtils;
import org.junit.Test;

import com.datastax.driver.core.exceptions.InvalidQueryException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.Assert.assertEquals;

public class BatchSizeGuardrailTest extends CQLTester
{
    private static final int KEY_SIZE = 1024 * 10;
    private static final int ROWS_COUNT = 3;

    @Test
    public void shouldRejectTooLargeBatchWithDeletesForPrimaryKeyWithClusteringColumns() throws Throwable
    {
        // given
        String table = createTable("CREATE TABLE %s (pk int, value1 text, value2 int, PRIMARY KEY (pk, value1))");
        List<String> randomKeys = generateNRandomKeysOfSize(ROWS_COUNT, KEY_SIZE);
        for (int i = 0; i < ROWS_COUNT; i++)
        {
            execute("INSERT INTO %s (pk, value1, value2) VALUES (?, ?, ?) IF NOT EXISTS", i, randomKeys.get(i), i);
        }

        // rows were inserted
        assertEquals(ROWS_COUNT, execute("SELECT * FROM %s").size());

        // fail threshold is 10KB
        givenBatchSizeFailThresholdInKB(10);

        String batch = buildBatch(String.format("DELETE FROM %s WHERE pk = %%s AND value1 = '%%s';", KEYSPACE + "." + table),
                (delete, i) -> String.format(delete, i, randomKeys.get(i)), ROWS_COUNT);

        // when executing batch
        Throwable thrown = catchThrowable(() -> executeNet(batch));

        // then it should be rejected
        assertBatchIsRejected(thrown, table);
        // no rows were deleted
        assertEquals("should not delete any rows", ROWS_COUNT, execute("SELECT * FROM %s").size());
    }

    @Test
    public void shouldRejectTooLargeBatchWithDeletesForSimplePrimaryKey() throws Throwable
    {
        // given
        String table = createTable("CREATE TABLE %s (pk text, value1 int, PRIMARY KEY (pk))");
        List<String> randomKeys = generateNRandomKeysOfSize(ROWS_COUNT, KEY_SIZE);
        for (int i = 0; i < ROWS_COUNT; i++)
        {
            execute("INSERT INTO %s (pk, value1) VALUES (?, ?) IF NOT EXISTS", randomKeys.get(i), i);
        }

        // rows were inserted
        assertEquals(ROWS_COUNT, execute("SELECT * FROM %s").size());

        // fail threshold is 10KB
        givenBatchSizeFailThresholdInKB(10);

        String batch = buildBatch(String.format("DELETE FROM %s WHERE pk = '%%s';", KEYSPACE + "." + table),
                (delete, i) -> String.format(delete, randomKeys.get(i)), ROWS_COUNT);

        // when executing batch
        Throwable thrown = catchThrowable(() -> executeNet(batch));

        // then it should be rejected
        assertBatchIsRejected(thrown, table);
        // no rows were deleted
        assertEquals("should not delete any rows", ROWS_COUNT, execute("SELECT * FROM %s").size());
    }

    @Test
    public void shouldAcceptBatchWithDeletesForPrimaryKeyWithClusteringColumns() throws Throwable
    {
        // given
        String table = createTable("CREATE TABLE %s (pk int, value1 text, value2 int, PRIMARY KEY (pk, value1))");
        List<String> randomKeys = generateNRandomKeysOfSize(ROWS_COUNT, KEY_SIZE);
        for (int i = 0; i < ROWS_COUNT; i++)
        {
            execute("INSERT INTO %s (pk, value1, value2) VALUES (?, ?, ?) IF NOT EXISTS", i, randomKeys.get(i), i);
        }

        // rows were inserted
        assertEquals(ROWS_COUNT, execute("SELECT * FROM %s").size());

        // fail threshold is large enough to accept batch for of 3 rows
        givenBatchSizeFailThresholdInKB(10 * (ROWS_COUNT + 1));

        String batch = buildBatch(String.format("DELETE FROM %s WHERE pk = %%s AND value1 = '%%s';", KEYSPACE + "." + table),
                (delete, i) -> String.format(delete, i, randomKeys.get(i)), ROWS_COUNT);

        // when executing batch
        executeNet(batch);

        // then rows were deleted
        assertEquals("should delete all the rows", 0, execute("SELECT * FROM %s").size());
    }

    @Test
    public void shouldAcceptBatchWithDeletesForSimplePrimaryKey() throws Throwable
    {
        // given
        String table = createTable("CREATE TABLE %s (pk text, value1 int, PRIMARY KEY (pk))");
        List<String> randomKeys = generateNRandomKeysOfSize(ROWS_COUNT, KEY_SIZE);
        for (int i = 0; i < ROWS_COUNT; i++)
        {
            execute("INSERT INTO %s (pk, value1) VALUES (?, ?) IF NOT EXISTS", randomKeys.get(i), i);
        }

        // rows were inserted
        assertEquals(ROWS_COUNT, execute("SELECT * FROM %s").size());

        // fail threshold is large enough to accept batch for of 3 rows
        givenBatchSizeFailThresholdInKB(10 * (ROWS_COUNT + 1));

        String batch = buildBatch(String.format("DELETE FROM %s WHERE pk = '%%s';", KEYSPACE + "." + table),
                (delete, i) -> String.format(delete, randomKeys.get(i)), ROWS_COUNT);

        // when executing batch
        executeNet(batch);

        // then rows were deleted
        assertEquals("should delete all the rows", 0, execute("SELECT * FROM %s").size());
    }

    private static String buildBatch(String statementTemplate, BiFunction<String, Integer, String> statementFormatter, int rowsCount)
    {
        StringBuilder batchBuilder = new StringBuilder("BEGIN BATCH\n");
        for (int i = 0; i < rowsCount; i++)
        {
            batchBuilder.append(statementFormatter.apply(statementTemplate, i));
        }
        return batchBuilder.append("APPLY BATCH").toString();
    }

    private static void givenBatchSizeFailThresholdInKB(int thresholdInKB)
    {
        DatabaseDescriptor.getGuardrailsConfig().setBatchSizeWarnThresholdInKB(1);  //we are intentionally setting warn threshold to 1KB as we are not testing it here
        DatabaseDescriptor.getGuardrailsConfig().setBatchSizeFailThresholdInKB(thresholdInKB);
    }

    private static void assertBatchIsRejected(Throwable thrown, String table)
    {
        assertThat(thrown)
                .isNotNull()
                .describedAs("batch of size abothe the thresbold should be rejected");
        assertThat(thrown)
                .isInstanceOf(RuntimeException.class)
                .hasRootCauseInstanceOf(InvalidQueryException.class)
                .hasRootCauseMessage(String.format("Batch for [cql_test_keyspace.%s] is of size 30kB, exceeding specified failure threshold 10kB", table));
    }

    private static List<String> generateNRandomKeysOfSize(int n, int size)
    {
        return Stream.generate(() -> RandomStringUtils.randomAlphanumeric(size))
                .limit(n).collect(Collectors.toList());
    }
}
