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

package org.apache.cassandra.guardrails;

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

public class GuardrailBatchSizeTest extends CQLTester
{
    private static final int KEY_SIZE_IN_BYTES = 1024 * 10;
    private static final int ROWS_COUNT = 3;

    @Test
    public void batchSizeWithPKGuardrailShouldRejectTooLargeBatchWithInsertsForSimplePrimaryKey() throws Throwable
    {
        // given
        String table = createTable("CREATE TABLE %s (pk text, value1 int, PRIMARY KEY (pk))");
        // there are no rows
        assertEquals(0, execute("SELECT * FROM %s").size());

        // fail threshold is 10KB
        givenBatchSizeWithPKFailThresholdInKB(10);

        List<String> randomKeys = generateNRandomKeysOfSize(ROWS_COUNT, KEY_SIZE_IN_BYTES);
        String batch = buildBatch(String.format("INSERT INTO %s (pk, value1) VALUES ('%%s', %%s);", KEYSPACE + "." + table),
                (insert, i) -> String.format(insert, randomKeys.get(i), i), ROWS_COUNT);

        // when executing batch
        Throwable thrown = catchThrowable(() -> executeNet(batch));

        // then it should be rejected
        assertBatchIsRejected(thrown, String.format("Batch for [cql_test_keyspace.%s] is of size 30kB including the primary key, exceeding specified failure threshold 10kB", table));
        // no rows were inserted
        assertEquals("should not insert any rows", 0, execute("SELECT * FROM %s").size());
    }

    @Test
    public void batchSizeWithPKGuardrailShouldAcceptBatchWithInsertsForSimplePrimaryKey() throws Throwable
    {
        // given
        String table = createTable("CREATE TABLE %s (pk text, value1 int, PRIMARY KEY (pk))");
        // there are no rows
        assertEquals(0, execute("SELECT * FROM %s").size());

        // batch size with pk fail threshold is large enough to accept batch for of ROW_COUNT rows
        givenBatchSizeWithPKFailThresholdInKB((ROWS_COUNT + 1) * 10);

        List<String> randomKeys = generateNRandomKeysOfSize(ROWS_COUNT, KEY_SIZE_IN_BYTES);
        String batch = buildBatch(String.format("INSERT INTO %s (pk, value1) VALUES ('%%s', %%s);", KEYSPACE + "." + table),
                (insert, i) -> String.format(insert, randomKeys.get(i), i), ROWS_COUNT);

        // when executing batch
        executeNet(batch);

        // then it should insert rows via the batch
        assertEquals("should insert rows via the batch", ROWS_COUNT, execute("SELECT * FROM %s").size());
    }

    /**
     * The primary motivation for this test is to ensure that the old batch size guardrail still works ignoring the PK size.
     */
    @Test
    public void batchSizeGuardrailShouldAcceptBatchWithInsertsForSimplePrimaryKey() throws Throwable
    {
        // given
        String table = createTable("CREATE TABLE %s (pk text, value1 int, PRIMARY KEY (pk))");
        // there are no rows
        assertEquals(0, execute("SELECT * FROM %s").size());

        // batch size fail threshold is 10KB
        givenBatchSizeFailThresholdInKB(10);

        List<String> randomKeys = generateNRandomKeysOfSize(ROWS_COUNT, KEY_SIZE_IN_BYTES);
        String batch = buildBatch(String.format("INSERT INTO %s (pk, value1) VALUES ('%%s', %%s);", KEYSPACE + "." + table),
                (insert, i) -> String.format(insert, randomKeys.get(i), i), ROWS_COUNT);

        // when executing batch
        executeNet(batch);

        // then it should insert rows via the batch even if the size of the batch witk PK is larger than the fail threshold
        assertEquals("should insert rows via the batch", ROWS_COUNT, execute("SELECT * FROM %s").size());
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

    private static void givenBatchSizeWithPKFailThresholdInKB(int thresholdInKB)
    {
        DatabaseDescriptor.getGuardrailsConfig().setBatchSizeWithPKWarnThresholdInKB(1);  //we are intentionally setting warn threshold to 1KB as we are not testing it here
        DatabaseDescriptor.getGuardrailsConfig().setBatchSizeWithPKFailThresholdInKB(thresholdInKB);
    }

    private static void assertBatchIsRejected(Throwable thrown, String message)
    {
        assertThat(thrown)
                .isNotNull()
                .describedAs("batch of size above the thresbold should be rejected");
        assertThat(thrown)
                .isInstanceOf(RuntimeException.class)
                .hasRootCauseInstanceOf(InvalidQueryException.class)
                .hasRootCauseMessage(message);
    }

    private static List<String> generateNRandomKeysOfSize(int n, int size)
    {
        return Stream.generate(() -> RandomStringUtils.randomAlphanumeric(size))
                .limit(n).collect(Collectors.toList());
    }
}
