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

import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.marshal.NumberType;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.memory.TrieMemtableIndex;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.TypeUtil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * This test aims to cover the different encodings of terms distribution to ensure expected results.
 */
@RunWith(Parameterized.class)
public class NumericTermsDistributionTest extends SAITester
{
    @Parameterized.Parameter
    public CQL3Type.Native testType;

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data()
    {
        return Stream.of(CQL3Type.Native.values())
                     .filter(type -> {
                         if (!(type.getType() instanceof NumberType))
                             return false;
                         // Counters cannot have indexes.
                         return !type.getType().isCounter();
                     })
                     .map(type -> new Object[]{ type })
                     .collect(Collectors.toList());
    }

    private StorageAttachedIndex getIndex()
    {
        // A bit brittle, but this is the most realistic way to test encodings.
        return (StorageAttachedIndex) Keyspace.open(KEYSPACE).getColumnFamilyStore(currentTable()).getIndexManager()
                                              .getIndexByName(currentIndex());
    }

    @Test
    public void testNumericIndexEstimates() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int primary key, a " + testType + ')');
        String indexName = createIndex("CREATE CUSTOM INDEX ON %s(a) USING 'StorageAttachedIndex'");
        waitForIndexQueryable(indexName);

        execute("INSERT INTO %s (pk, a) VALUES (1, -10)");
        execute("INSERT INTO %s (pk, a) VALUES (2, -1)");
        execute("INSERT INTO %s (pk, a) VALUES (3, 0)");
        execute("INSERT INTO %s (pk, a) VALUES (4, 1)");
        execute("INSERT INTO %s (pk, a) VALUES (5, 10)");
        execute("INSERT INTO %s (pk, a) VALUES (6, 15)");

        var sai = getIndex();

        assertInMemoryEstimateCount(sai, Operator.EQ, "0", 1);
        assertInMemoryEstimateCount(sai, Operator.LT, "0", 2, 1, 0);
        assertInMemoryEstimateCount(sai, Operator.GT, "0", 3, 1, 0);
        assertInMemoryEstimateCount(sai, Operator.LTE, "0", 3);
        assertInMemoryEstimateCount(sai, Operator.GTE, "0", 4);
        assertInMemoryEstimateCount(sai, Operator.EQ, "5", 0);

        flush();

        assertSSTableEstimateCount(sai, Operator.EQ, "0", 1);
        assertSSTableEstimateCount(sai, Operator.LT, "0", 2, 1, 0);
        assertSSTableEstimateCount(sai, Operator.GT, "0", 3, 1, 0);
        assertSSTableEstimateCount(sai, Operator.LTE, "0", 3);
        assertSSTableEstimateCount(sai, Operator.GTE, "0", 4);
        assertSSTableEstimateCount(sai, Operator.EQ, "5", 0);
    }

    @Test
    public void testLossyNumericTypes() throws Throwable
    {
        // Only test types that support rounding.
        if (!TypeUtil.supportsRounding(testType.getType()))
            return;

        createTable("CREATE TABLE %s (pk text PRIMARY KEY, x " + testType + ')');
        createIndex("CREATE CUSTOM INDEX ON %s(x) USING 'StorageAttachedIndex'");

        // We truncate decimals to 24 bytes in the numeric index. These numbers are chosen to ensure
        // that we get the expected results.
        var a = "1111111111111111111111111111111111111111111111111111111111";
        var b = "1111111111111111111111111111111111111111111111111111111112";
        var c = "1111111111111111111111111111111111111111111111111111111113";

        execute("INSERT INTO %s (pk, x) VALUES ('a', " + a + ')');
        execute("INSERT INTO %s (pk, x) VALUES ('b', " + b + ')');
        execute("INSERT INTO %s (pk, x) VALUES ('c', " + c + ')');

        var sai = getIndex();

        // Because a, b, and c all truncate to the same value, we expect to get all 3 rows from the index.
        assertInMemoryEstimateCount(sai, Operator.EQ, b, 3);
        assertInMemoryEstimateCount(sai, Operator.LT, b, 3);
        assertInMemoryEstimateCount(sai, Operator.LTE, b, 3);
        assertInMemoryEstimateCount(sai, Operator.GT, b, 3);
        assertInMemoryEstimateCount(sai, Operator.GTE, b, 3);

        flush();

        assertSSTableEstimateCount(sai, Operator.EQ, b, 3);
        assertSSTableEstimateCount(sai, Operator.LT, b, 3);
        assertSSTableEstimateCount(sai, Operator.LTE, b, 3);
        assertSSTableEstimateCount(sai, Operator.GT, b, 3);
        assertSSTableEstimateCount(sai, Operator.GTE, b, 3);
    }

    @Test
    public void testNumericIndexEstimatesOnManyRows() throws Throwable
    {
        // This tests inserts thousands of rows and covers different paths of code than a small-scale tests
        // inserting a few rows only.
        // For example, for estimating the number of rows in the memory tries we walk only a small portion
        // of the trie, and then we estimate the total number by extrapolation. That extrapolation wouldn't
        // be invoked if the trie contained insufficient number of rows. Aditionally, it is good to test
        // with row counts significantly larger than the number of histogram buckets in the terms distribution,
        // so it also checks the computations of bucket fractions.

        createTable("CREATE TABLE %s (pk int primary key, a " + testType + ')');
        String indexName = createIndex("CREATE CUSTOM INDEX ON %s(a) USING 'StorageAttachedIndex'");
        waitForIndexQueryable(indexName);

        final int COUNT = (testType == CQL3Type.Native.TINYINT) ? 127 : 10000;
        for (int i = 0; i < COUNT; i++)
            execute("INSERT INTO %s (pk, a) VALUES (1, "  + i + ')');

        var sai = getIndex();

        final String MID_POINT = "" + COUNT / 2;
        assertInMemoryEstimateCount(sai, Operator.EQ, MID_POINT, 1);
        assertInMemoryEstimateCount(sai, Operator.LT, MID_POINT, COUNT / 2, 0, 2);
        assertInMemoryEstimateCount(sai, Operator.GT, MID_POINT, COUNT / 2, 0, 2);
        assertInMemoryEstimateCount(sai, Operator.LTE, MID_POINT, COUNT / 2, 1, 2);
        assertInMemoryEstimateCount(sai, Operator.GTE, MID_POINT, COUNT / 2, 1, 2);
        assertInMemoryEstimateCount(sai, Operator.EQ, "-1", 0);

        flush();

        assertSSTableEstimateCount(sai, Operator.EQ, MID_POINT, 1);
        assertSSTableEstimateCount(sai, Operator.LT, MID_POINT, COUNT / 2, 0, 2);
        assertSSTableEstimateCount(sai, Operator.GT, MID_POINT, COUNT / 2, 0, 2);
        assertSSTableEstimateCount(sai, Operator.LTE, MID_POINT, COUNT / 2, 1, 2);
        assertSSTableEstimateCount(sai, Operator.GTE, MID_POINT, COUNT / 2, 1, 2);
        assertSSTableEstimateCount(sai, Operator.EQ, "-1", 0);
    }


    private void assertInMemoryEstimateCount(StorageAttachedIndex index, Operator op, String value, long expectedCount)
    {
        assertInMemoryEstimateCount(index, op, value, expectedCount, 0, 0);
    }

    private void assertInMemoryEstimateCount(StorageAttachedIndex index, Operator op, String value, long expectedCount, long roundingValue, long uncertainty)
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

        assertEstimateCorrect(expectedCount, roundingValue, uncertainty, memoryCount);
    }

    private void assertSSTableEstimateCount(StorageAttachedIndex index, Operator op, String value, long expectedCount)
    {
        assertSSTableEstimateCount(index, op, value, expectedCount, 0, 0);
    }

    private void assertSSTableEstimateCount(StorageAttachedIndex index, Operator op, String value, long expectedCount, long roundingValue, long uncertainty)
    {
        var expression = buildExpression(index, op, value);
        var wholeRange = DataRange.allData(index.getIndexContext().getPartitioner()).keyRange();
        var view = index.getIndexContext().getView();
        var onDiskCount = 0L;
        for (var sstableIndex : view.getIndexes())
            onDiskCount += sstableIndex.estimateMatchingRowsCount(expression, wholeRange);

        assertEstimateCorrect(expectedCount, roundingValue, uncertainty, onDiskCount);
    }

    private void assertEstimateCorrect(long expectedCount, long roundingValue, long uncertainty, long actualCount)
    {
        // This is a special case because Decimal and varint types are lossy, so when the index builds the expression
        // it is automatically inclusive to prevent missed results.
        if (TypeUtil.supportsRounding(testType.getType()))
            expectedCount += roundingValue;

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
}
