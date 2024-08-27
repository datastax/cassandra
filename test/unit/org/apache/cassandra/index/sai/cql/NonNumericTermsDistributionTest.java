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

package org.apache.cassandra.index.sai.cql;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.memory.TrieMemtableIndex;
import org.apache.cassandra.index.sai.plan.Expression;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * This test aims to cover the different encodings of terms distribution to ensure expected results.
 */
public class NonNumericTermsDistributionTest extends SAITester
{

    private StorageAttachedIndex getIndex()
    {
        // A bit brittle, but this is the most realistic way to test encodings.
        return (StorageAttachedIndex) Keyspace.open(KEYSPACE).getColumnFamilyStore(currentTable()).getIndexManager()
                                              .getIndexByName(currentIndex());
    }

    @Test
    public void testBooleanIndexEstimates()
    {
        createTable("CREATE TABLE %s (pk int primary key, a boolean)");
        String indexName = createIndex("CREATE CUSTOM INDEX ON %s(a) USING 'StorageAttachedIndex'");
        waitForIndexQueryable(indexName);

        execute("INSERT INTO %s (pk, a) VALUES (1, true)");
        execute("INSERT INTO %s (pk, a) VALUES (2, true)");
        execute("INSERT INTO %s (pk, a) VALUES (3, true)");
        execute("INSERT INTO %s (pk, a) VALUES (4, false)");
        execute("INSERT INTO %s (pk, a) VALUES (5, false)");
        execute("INSERT INTO %s (pk, a) VALUES (6, true)");

        var sai = getIndex();

        assertInMemoryEstimateCount(sai, Operator.EQ, "true", 4);
        assertInMemoryEstimateCount(sai, Operator.EQ, "false", 2);

        flush();

        assertSSTableEstimateCount(sai, Operator.EQ, "true", 4);
        assertSSTableEstimateCount(sai, Operator.EQ, "false", 2);
    }

    @Test
    public void testUtf8IndexEstimates()
    {
        createTable("CREATE TABLE %s (pk int primary key, a text)");
        String indexName = createIndex("CREATE CUSTOM INDEX ON %s(a) USING 'StorageAttachedIndex'");
        waitForIndexQueryable(indexName);
        execute("INSERT INTO %s (pk, a) VALUES (1, 'a')");
        execute("INSERT INTO %s (pk, a) VALUES (2, 'aa')");
        execute("INSERT INTO %s (pk, a) VALUES (3, 'ab')");
        execute("INSERT INTO %s (pk, a) VALUES (4, 'c')");
        execute("INSERT INTO %s (pk, a) VALUES (5, 'z')");
        execute("INSERT INTO %s (pk, a) VALUES (6, '•')");

        var sai = getIndex();

        assertInMemoryEstimateCount(sai, Operator.EQ, "ab", 1);
        assertInMemoryEstimateCount(sai, Operator.LT, "ab", 2);
        assertInMemoryEstimateCount(sai, Operator.GT, "ab", 3);
        assertInMemoryEstimateCount(sai, Operator.LTE, "ab", 3);
        assertInMemoryEstimateCount(sai, Operator.GTE, "ab", 4);
        assertInMemoryEstimateCount(sai, Operator.EQ, "•", 1);
        assertInMemoryEstimateCount(sai, Operator.EQ, "x", 0);

        flush();

        assertSSTableEstimateCount(sai, Operator.EQ, "ab", 1);
        assertSSTableEstimateCount(sai, Operator.LT, "ab", 2);
        assertSSTableEstimateCount(sai, Operator.GT, "ab", 3);
        assertSSTableEstimateCount(sai, Operator.LTE, "ab", 3);
        assertSSTableEstimateCount(sai, Operator.GTE, "ab", 4);
        assertSSTableEstimateCount(sai, Operator.EQ, "•", 1);
        assertSSTableEstimateCount(sai, Operator.EQ, "x", 0);
    }

    @Test
    public void testUtf8IndexEstimatesForManyRows()
    {
        createTable("CREATE TABLE %s (pk int primary key, a text)");
        String indexName = createIndex("CREATE CUSTOM INDEX ON %s(a) USING 'StorageAttachedIndex'");
        waitForIndexQueryable(indexName);

        final int ROW_COUNT = 10000;
        final int ROWS_PER_POINT = 10;

        List<String> values = new ArrayList<>();
        String minValue = null;
        String maxValue = null;

        int rowNum = 0;
        while (rowNum < ROW_COUNT)
        {
            String value = randomString(getRandom().nextIntBetween(12, 16));
            values.add(value);
            if (minValue == null || value.compareTo(minValue) < 0)
                minValue = value;
            if (maxValue == null || value.compareTo(maxValue) > 0)
                maxValue = value;

            for (int j = 0; j < ROWS_PER_POINT; j++)
            {
                execute("INSERT INTO %s (pk, a) VALUES (?, ?)", rowNum, value);
                rowNum++;
            }
        }

        var sai = getIndex();

        for (int i = 0; i < 100; i++)
            assertSSTableEstimateCount(sai, Operator.EQ, randomString(16), 0);
        for (String value : values)
            assertInMemoryEstimateCount(sai, Operator.EQ, value, ROWS_PER_POINT, ROWS_PER_POINT / 10);

        flush();

        for (int i = 0; i < 100; i++)
        {
            // For non-existing rows, we should always estimate 0 if out of range, but there is a bit of uncertainty if
            // we are asked about the value within the range of the index. In that case, the actual estimate may be
            // either 0 or ROWS_PER_POINT depending on whether we hit a value in the histogram bucket which
            // already contains one of known most frequent values.
            String randomValue = randomString(getRandom().nextIntBetween(12, 16));
            long maxRows = (randomValue.compareTo(minValue) >= 0 && randomValue.compareTo(maxValue) <= 0) ? ROWS_PER_POINT : 0;
            assertTrue(estimateSSTableRowCount(sai, Operator.EQ, randomValue) <= maxRows);
        }

        for (String value : values)
            assertSSTableEstimateCount(sai, Operator.EQ, value, ROWS_PER_POINT, ROWS_PER_POINT / 10);
    }

    @Test
    public void testAsciiIndexEstimates()
    {
        createTable("CREATE TABLE %s (pk int primary key, a ascii)");
        String indexName = createIndex("CREATE CUSTOM INDEX ON %s(a) USING 'StorageAttachedIndex'");
        waitForIndexQueryable(indexName);

        execute("INSERT INTO %s (pk, a) VALUES (1, 'a')");
        execute("INSERT INTO %s (pk, a) VALUES (2, 'aa')");
        execute("INSERT INTO %s (pk, a) VALUES (3, 'ab')");
        execute("INSERT INTO %s (pk, a) VALUES (4, 'c')");
        execute("INSERT INTO %s (pk, a) VALUES (5, 'z')");
        execute("INSERT INTO %s (pk, a) VALUES (6, 'zz')");

        var sai = getIndex();

        assertInMemoryEstimateCount(sai, Operator.EQ, "ab", 1);
        assertInMemoryEstimateCount(sai, Operator.LT, "ab", 2);
        assertInMemoryEstimateCount(sai, Operator.GT, "ab", 3);
        assertInMemoryEstimateCount(sai, Operator.LTE, "ab", 3);
        assertInMemoryEstimateCount(sai, Operator.GTE, "ab", 4);
        assertInMemoryEstimateCount(sai, Operator.EQ, "x", 0);

        flush();

        assertSSTableEstimateCount(sai, Operator.EQ, "ab", 1);
        assertSSTableEstimateCount(sai, Operator.LT, "ab", 2);
        assertSSTableEstimateCount(sai, Operator.GT, "ab", 3);
        assertSSTableEstimateCount(sai, Operator.LTE, "ab", 3);
        assertSSTableEstimateCount(sai, Operator.GTE, "ab", 4);
        assertSSTableEstimateCount(sai, Operator.EQ, "x", 0);
    }

    @Test
    public void testInetAddressIndexEstimates()
    {
        createTable("CREATE TABLE %s (pk int primary key, a inet)");
        String indexName = createIndex("CREATE CUSTOM INDEX ON %s(a) USING 'StorageAttachedIndex'");
        waitForIndexQueryable(indexName);

        execute("INSERT INTO %s (pk, a) VALUES (1, '127.0.0.1')");
        execute("INSERT INTO %s (pk, a) VALUES (2, '192.168.1.1')");
        execute("INSERT INTO %s (pk, a) VALUES (3, '192.168.1.2')");
        execute("INSERT INTO %s (pk, a) VALUES (4, '192.168.1.3')");
        execute("INSERT INTO %s (pk, a) VALUES (5, 'ff06::fa')");
        execute("INSERT INTO %s (pk, a) VALUES (6, 'ff06::ff')");

        var sai = getIndex();

        assertInMemoryEstimateCount(sai, Operator.EQ, "192.168.1.2", 1);
        assertInMemoryEstimateCount(sai, Operator.LT, "192.168.1.2", 2);
        assertInMemoryEstimateCount(sai, Operator.GT, "192.168.1.2", 3);
        assertInMemoryEstimateCount(sai, Operator.LTE, "192.168.1.2", 3);
        assertInMemoryEstimateCount(sai, Operator.GTE, "192.168.1.2", 4);
        assertInMemoryEstimateCount(sai, Operator.EQ, "ff06::ff", 1);
        assertInMemoryEstimateCount(sai, Operator.EQ, "ff07::ff", 0);
        assertInMemoryEstimateCount(sai, Operator.EQ, "192.168.11.11", 0);

        flush();

        assertSSTableEstimateCount(sai, Operator.EQ, "192.168.1.2", 1);
        assertSSTableEstimateCount(sai, Operator.LT, "192.168.1.2", 2);
        assertSSTableEstimateCount(sai, Operator.GT, "192.168.1.2", 3);
        assertSSTableEstimateCount(sai, Operator.LTE, "192.168.1.2", 3);
        assertSSTableEstimateCount(sai, Operator.GTE, "192.168.1.2", 4);
        assertSSTableEstimateCount(sai, Operator.EQ, "ff06::ff", 1);
        assertSSTableEstimateCount(sai, Operator.EQ, "ff07::ff", 0);
        assertSSTableEstimateCount(sai, Operator.EQ, "192.168.11.11", 0);
    }

    @Test
    public void testDateIndexEstimates()
    {
        createTable("CREATE TABLE %s (pk int primary key, a date)");
        String indexName = createIndex("CREATE CUSTOM INDEX ON %s(a) USING 'StorageAttachedIndex'");
        waitForIndexQueryable(indexName);

        execute("INSERT INTO %s (pk, a) VALUES (1, '1810-12-31')");
        execute("INSERT INTO %s (pk, a) VALUES (2, '2024-01-01')");
        execute("INSERT INTO %s (pk, a) VALUES (3, '2024-01-01')");
        execute("INSERT INTO %s (pk, a) VALUES (4, '2024-01-01')");
        execute("INSERT INTO %s (pk, a) VALUES (5, '2024-01-02')");
        execute("INSERT INTO %s (pk, a) VALUES (6, '2550-01-01')");

        var sai = getIndex();

        assertInMemoryEstimateCount(sai, Operator.EQ, "2024-01-01", 3);
        assertInMemoryEstimateCount(sai, Operator.LT, "2024-01-01", 1);
        assertInMemoryEstimateCount(sai, Operator.GT, "2024-01-01", 2);
        assertInMemoryEstimateCount(sai, Operator.LTE, "2024-01-01", 4);
        assertInMemoryEstimateCount(sai, Operator.GTE, "2024-01-01", 5);
        assertInMemoryEstimateCount(sai, Operator.EQ, "2550-01-01", 1);
        assertInMemoryEstimateCount(sai, Operator.EQ, "2550-01-02", 0);
        assertInMemoryEstimateCount(sai, Operator.EQ, "1810-12-31", 1);
        assertInMemoryEstimateCount(sai, Operator.EQ, "1810-12-30", 0);

        flush();

        assertSSTableEstimateCount(sai, Operator.EQ, "2024-01-01", 3);
        assertSSTableEstimateCount(sai, Operator.LT, "2024-01-01", 1);
        assertSSTableEstimateCount(sai, Operator.GT, "2024-01-01", 2);
        assertSSTableEstimateCount(sai, Operator.LTE, "2024-01-01", 4);
        assertSSTableEstimateCount(sai, Operator.GTE, "2024-01-01", 5);
        assertSSTableEstimateCount(sai, Operator.EQ, "2550-01-01", 1);
        assertSSTableEstimateCount(sai, Operator.EQ, "2550-01-02", 0);
        assertSSTableEstimateCount(sai, Operator.EQ, "1810-12-31", 1);
        assertSSTableEstimateCount(sai, Operator.EQ, "1810-12-30", 0);
    }

    @Test
    public void testTimeIndexEstimates()
    {
        createTable("CREATE TABLE %s (pk int primary key, a time)");
        String indexName = createIndex("CREATE CUSTOM INDEX ON %s(a) USING 'StorageAttachedIndex'");
        waitForIndexQueryable(indexName);

        execute("INSERT INTO %s (pk, a) VALUES (1, '4:01:23.000000000')");
        execute("INSERT INTO %s (pk, a) VALUES (2, '11:59:59.999999999')");
        execute("INSERT INTO %s (pk, a) VALUES (3, '12:00:00.000000000')");
        execute("INSERT INTO %s (pk, a) VALUES (4, '12:00:00.000000001')");
        execute("INSERT INTO %s (pk, a) VALUES (5, '15:00:23.000000000')");
        execute("INSERT INTO %s (pk, a) VALUES (6, '23:59:59.999999999')");

        var sai = getIndex();

        assertInMemoryEstimateCount(sai, Operator.EQ, "12:00:00.000000000", 1);
        assertInMemoryEstimateCount(sai, Operator.LT, "12:00:00.000000000", 2);
        assertInMemoryEstimateCount(sai, Operator.GT, "12:00:00.000000000", 3);
        assertInMemoryEstimateCount(sai, Operator.LTE, "12:00:00.000000000", 3);
        assertInMemoryEstimateCount(sai, Operator.GTE, "12:00:00.000000000", 4);
        assertInMemoryEstimateCount(sai, Operator.EQ, "23:59:59.999999999", 1);
        assertInMemoryEstimateCount(sai, Operator.EQ, "23:59:59.999999998", 0);

        flush();

        assertSSTableEstimateCount(sai, Operator.EQ, "12:00:00.000000000", 1);
        assertSSTableEstimateCount(sai, Operator.LT, "12:00:00.000000000", 2);
        assertSSTableEstimateCount(sai, Operator.GT, "12:00:00.000000000", 3);
        assertSSTableEstimateCount(sai, Operator.LTE, "12:00:00.000000000", 3);
        assertSSTableEstimateCount(sai, Operator.GTE, "12:00:00.000000000", 4);
        assertSSTableEstimateCount(sai, Operator.EQ, "23:59:59.999999999", 1);
        assertSSTableEstimateCount(sai, Operator.EQ, "23:59:59.999999998", 0);
    }

    @Test
    public void testTimestampIndexEstimates()
    {
        createTable("CREATE TABLE %s (pk int primary key, a timestamp)");
        String indexName = createIndex("CREATE CUSTOM INDEX ON %s(a) USING 'StorageAttachedIndex'");
        waitForIndexQueryable(indexName);

        execute("INSERT INTO %s (pk, a) VALUES (1, '1810-12-31 16:00:00')");
        execute("INSERT INTO %s (pk, a) VALUES (2, '2024-01-01 11:00:59.999')");
        execute("INSERT INTO %s (pk, a) VALUES (3, '2024-01-01 12:00:00.000')");
        execute("INSERT INTO %s (pk, a) VALUES (4, '2024-01-01 12:00:00.001')");
        execute("INSERT INTO %s (pk, a) VALUES (5, '2024-01-02 12:00:00')");
        execute("INSERT INTO %s (pk, a) VALUES (6, '2550-01-01 12:00:00')");

        var sai = getIndex();

        assertInMemoryEstimateCount(sai, Operator.EQ, "2024-01-01 12:00:00.000", 1);
        assertInMemoryEstimateCount(sai, Operator.LT, "2024-01-01 12:00:00.000", 2);
        assertInMemoryEstimateCount(sai, Operator.GT, "2024-01-01 12:00:00.000", 3);
        assertInMemoryEstimateCount(sai, Operator.LTE, "2024-01-01 12:00:00.000", 3);
        assertInMemoryEstimateCount(sai, Operator.GTE, "2024-01-01 12:00:00.000", 4);
        assertInMemoryEstimateCount(sai, Operator.EQ, "2550-01-01 12:00:00", 1);
        assertInMemoryEstimateCount(sai, Operator.EQ, "2550-01-01 12:00:01", 0);
        assertInMemoryEstimateCount(sai, Operator.EQ, "1810-12-31 16:00:00", 1);
        assertInMemoryEstimateCount(sai, Operator.EQ, "1810-12-30 16:00:00", 0);

        flush();

        assertSSTableEstimateCount(sai, Operator.EQ, "2024-01-01 12:00:00.000", 1);
        assertSSTableEstimateCount(sai, Operator.LT, "2024-01-01 12:00:00.000", 2);
        assertSSTableEstimateCount(sai, Operator.GT, "2024-01-01 12:00:00.000", 3);
        assertSSTableEstimateCount(sai, Operator.LTE, "2024-01-01 12:00:00.000", 3);
        assertSSTableEstimateCount(sai, Operator.GTE, "2024-01-01 12:00:00.000", 4);
        assertSSTableEstimateCount(sai, Operator.EQ, "2550-01-01 12:00:00", 1);
        assertSSTableEstimateCount(sai, Operator.EQ, "2550-01-01 12:00:01", 0);
        assertSSTableEstimateCount(sai, Operator.EQ, "1810-12-31 16:00:00", 1);
        assertSSTableEstimateCount(sai, Operator.EQ, "1810-12-30 16:00:00", 0);
    }

    @Test
    public void testUuidIndexEstimates()
    {
        createTable("CREATE TABLE %s (pk int primary key, a uuid)");
        String indexName = createIndex("CREATE CUSTOM INDEX ON %s(a) USING 'StorageAttachedIndex'");
        waitForIndexQueryable(indexName);

        execute("INSERT INTO %s (pk, a) VALUES (1, 53aeb60c-8ac9-11ef-b864-0242ac120055)");
        execute("INSERT INTO %s (pk, a) VALUES (2, 53aeb60c-8ac9-11ef-b864-0242ac120055)");
        execute("INSERT INTO %s (pk, a) VALUES (3, 53aeb7ad-8ac9-11ef-b864-0242ac120055)");
        execute("INSERT INTO %s (pk, a) VALUES (4, 53aeb7ad-8ac9-11ef-b864-0242ac120055)");
        execute("INSERT INTO %s (pk, a) VALUES (5, 53aeb7ad-8ac9-11ef-b864-0242ac120055)");
        execute("INSERT INTO %s (pk, a) VALUES (6, 53aeb7ad-8ac9-11ef-b864-0242ac120055)");

        var sai = getIndex();

        assertInMemoryEstimateCount(sai, Operator.EQ, "53aeb60c-8ac9-11ef-b864-0242ac120055", 2);
        assertInMemoryEstimateCount(sai, Operator.EQ, "53aeb7ad-8ac9-11ef-b864-0242ac120055", 4);
        assertInMemoryEstimateCount(sai, Operator.EQ, "53aeb7ad-8ac9-11ef-b864-0242ac120099", 0);

        flush();

        assertSSTableEstimateCount(sai, Operator.EQ, "53aeb60c-8ac9-11ef-b864-0242ac120055", 2);
        assertSSTableEstimateCount(sai, Operator.EQ, "53aeb7ad-8ac9-11ef-b864-0242ac120055", 4);
        assertSSTableEstimateCount(sai, Operator.EQ, "53aeb7ad-8ac9-11ef-b864-0242ac120099", 0);
    }

    @Test
    public void testTimeUuidIndexEstimates()
    {
        createTable("CREATE TABLE %s (pk int primary key, a timeuuid)");
        String indexName = createIndex("CREATE CUSTOM INDEX ON %s(a) USING 'StorageAttachedIndex'");
        waitForIndexQueryable(indexName);

        execute("INSERT INTO %s (pk, a) VALUES (1, 53aeb60c-8ac9-11ef-b864-0242ac120055)");
        execute("INSERT INTO %s (pk, a) VALUES (2, 53aeb7ad-8ac9-11ef-b864-0242ac120054)");
        execute("INSERT INTO %s (pk, a) VALUES (3, 53aeb7ad-8ac9-11ef-b864-0242ac120055)");
        execute("INSERT INTO %s (pk, a) VALUES (4, 53aeb7ad-8ac9-11ef-b864-0242ac120056)");
        execute("INSERT INTO %s (pk, a) VALUES (5, 53aeba94-8ac9-11ef-b864-0242ac120040)");
        execute("INSERT INTO %s (pk, a) VALUES (6, 53aebb34-8ac9-11ef-b864-0242ac120030)");

        var sai = getIndex();

        assertInMemoryEstimateCount(sai, Operator.EQ, "53aeb7ad-8ac9-11ef-b864-0242ac120055", 1);
        assertInMemoryEstimateCount(sai, Operator.LT, "53aeb7ad-8ac9-11ef-b864-0242ac120055", 2);
        assertInMemoryEstimateCount(sai, Operator.GT, "53aeb7ad-8ac9-11ef-b864-0242ac120055", 3);
        assertInMemoryEstimateCount(sai, Operator.LTE, "53aeb7ad-8ac9-11ef-b864-0242ac120055", 3);
        assertInMemoryEstimateCount(sai, Operator.GTE, "53aeb7ad-8ac9-11ef-b864-0242ac120055", 4);
        assertInMemoryEstimateCount(sai, Operator.EQ, "53aeb7ad-8ac9-11ef-b864-0242ac120077", 0);

        flush();

        assertSSTableEstimateCount(sai, Operator.EQ, "53aeb7ad-8ac9-11ef-b864-0242ac120055", 1);
        assertSSTableEstimateCount(sai, Operator.LT, "53aeb7ad-8ac9-11ef-b864-0242ac120055", 2);
        assertSSTableEstimateCount(sai, Operator.GT, "53aeb7ad-8ac9-11ef-b864-0242ac120055", 3);
        assertSSTableEstimateCount(sai, Operator.LTE, "53aeb7ad-8ac9-11ef-b864-0242ac120055", 3);
        assertSSTableEstimateCount(sai, Operator.GTE, "53aeb7ad-8ac9-11ef-b864-0242ac120055", 4);
        assertSSTableEstimateCount(sai, Operator.EQ, "53aeb7ad-8ac9-11ef-b864-0242ac120077", 0);
    }

    private void assertInMemoryEstimateCount(StorageAttachedIndex index, Operator op, String value, long expectedCount)
    {
        assertInMemoryEstimateCount(index, op, value, expectedCount, 0);
    }

    private void assertInMemoryEstimateCount(StorageAttachedIndex index, Operator op, String value, long expectedCount, long uncertainty)
    {
        var memtableIndexes = index.getIndexContext()
                                   .getLiveMemtables()
                                   .values();
        var expression = buildExpression(index, op, value);
        var memoryCount = 0L;
        var wholeRange = DataRange.allData(index.getIndexContext().getPartitioner()).keyRange();
        for (var memtableIndex : memtableIndexes)
            for (var memoryIndex : ((TrieMemtableIndex) memtableIndex).getRangeIndexes())
                memoryCount += memoryIndex.estimateMatchingRowsCount(expression, wholeRange);

        assertEstimateCorrect(expectedCount, uncertainty, memoryCount);
    }


    private void assertSSTableEstimateCount(StorageAttachedIndex index, Operator op, String value, long expectedCount)
    {
        assertSSTableEstimateCount(index, op, value, expectedCount, 0);
    }

    private void assertSSTableEstimateCount(StorageAttachedIndex index, Operator op, String value, long expectedCount, long uncertainty)
    {
        var onDiskCount = estimateSSTableRowCount(index, op, value);
        assertEstimateCorrect(expectedCount, uncertainty, onDiskCount);
    }

    private long estimateSSTableRowCount(StorageAttachedIndex index, Operator op, String value)
    {
        var expression = buildExpression(index, op, value);
        var wholeRange = DataRange.allData(index.getIndexContext().getPartitioner()).keyRange();
        var view = index.getIndexContext().getView();
        var onDiskCount = 0L;
        for (var sstableIndex : view.getIndexes())
            onDiskCount += sstableIndex.estimateMatchingRowsCount(expression, wholeRange);
        return onDiskCount;
    }

    private void assertEstimateCorrect(long expectedCount, long uncertainty, long actualCount)
    {
        if (uncertainty == 0)
        {
            assertEquals(expectedCount, actualCount);
        }
        else
        {
            assertTrue("Expected min: " + (expectedCount - uncertainty) + " Actual: " + actualCount,
                       actualCount >= expectedCount - uncertainty);
            assertTrue("Expected max: " + (expectedCount + uncertainty) + " Actual: " + actualCount,
                       actualCount <= expectedCount + uncertainty);
        }
    }

    private Expression buildExpression(StorageAttachedIndex index, Operator op, String value)
    {
        var expression = new Expression(index.getIndexContext());
        expression.add(op, index.getIndexContext().getValidator().fromString(value));
        return expression;
    }

    private static String randomString(int length) {
        String alphanumericChars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                                   + "abcdefghijklmnopqrstuvwxyz"
                                   + "0123456789";
        StringBuilder sb = new StringBuilder(length);
        var random = getRandom();

        for (int i = 0; i < length; i++) {
            int index = random.nextIntBetween(0, alphanumericChars.length() - 1);
            sb.append(alphanumericChars.charAt(index));
        }
        return sb.toString();
    }
}
