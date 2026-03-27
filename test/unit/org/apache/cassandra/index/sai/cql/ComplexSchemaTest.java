/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.index.sai.cql;

import org.junit.Test;

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.index.sai.SAITester;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A test with a complex wide partition schema and SAI indexes.
 * Tests with such a complex schema and indexes do not exist.
 * This test allowed catching a bug while implementing CNDB-15608.
 */
public class ComplexSchemaTest extends SAITester
{
    private static final int ROW_COUNT = 1000;
    private static final int PAR_COUNT = 100;
    private static final int LC = 10;  // Low cardinality
    private static final int MC = 100; // Medium cardinality
    private static final int HC = 1000; // High cardinality
    private static final long BASE_TIMESTAMP = 1700000000000L;
    private static final int DAYS_TO_SPREAD = 30;

    @Test
    public void testWideTimeseriesWithMultipleIndexes()
    {
        createTable("CREATE TABLE %s (" +
                    "pk_int int, " +
                    "ck1_bigint bigint, " +
                    "ck2_bigint bigint, " +
                    "ck3_text text, " +
                    "ck4_bigint bigint, " +
                    "ck5_bigint bigint, " +
                    "col1_text text, " +
                    "col2_timestamp timestamp, " +
                    "col3_bigint bigint, " +
                    "col4_int int, " +
                    "col5_int int, " +
                    "col6_int int, " +
                    "col7_text text, " +
                    "col8_int int, " +
                    "col9_timestamp timestamp, " +
                    "PRIMARY KEY (pk_int, ck1_bigint, ck2_bigint, ck3_text, ck4_bigint, ck5_bigint)" +
                    ") WITH CLUSTERING ORDER BY (ck1_bigint DESC, ck2_bigint DESC, ck3_text ASC, ck4_bigint ASC, ck5_bigint ASC)");

        createIndex("CREATE CUSTOM INDEX ON %s(col1_text) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(ck3_text) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(ck2_bigint) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(ck1_bigint) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(ck4_bigint) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(col4_int) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(col8_int) USING 'StorageAttachedIndex'");

        insertData();

        flush();

        int testIndex = ROW_COUNT / 2;
        int pkInt = hashRange(testIndex, PAR_COUNT);
        long ck2Bigint = BASE_TIMESTAMP + (testIndex * DAYS_TO_SPREAD * 86400000L / ROW_COUNT);
        long ck2BigintUpper = ck2Bigint + 86400000L;
        long ck2BigintLower = ck2Bigint - 86400000L;
        long ck1BigintLower = (ck2BigintLower / 3600000L) * 3600000L;
        long ck1BigintUpper = (ck2BigintUpper / 3600000L) * 3600000L;
        long ck4Bigint = hash2(testIndex, 1) % HC;

        UntypedResultSet result = execute("SELECT * FROM %s WHERE pk_int=? AND ck1_bigint>=? AND ck1_bigint<=? " +
                                          "AND ck2_bigint<=? AND ck2_bigint>=? AND ck4_bigint=? LIMIT 10 ALLOW FILTERING",
                                          pkInt, ck1BigintLower, ck1BigintUpper, ck2BigintUpper, ck2BigintLower, ck4Bigint);
        assertThat(result).as("Query should return at least one result").isNotEmpty();

        result = execute("SELECT * FROM %s WHERE pk_int=? AND col5_int=? LIMIT 10 ALLOW FILTERING", pkInt, 1);
        assertThat(result).as("Query with pk_int and col5_int should return at least one result").isNotEmpty();

        result = execute("SELECT * FROM %s WHERE pk_int=? AND ck2_bigint<? AND ck3_text>? AND ck4_bigint>? LIMIT 10 ALLOW FILTERING",
                         pkInt, ck2Bigint, "value_10", ck4Bigint / 2);
        assertThat(result).as("Complex multi-column query should execute successfully").isNotEmpty();
    }

    private void insertData()
    {
        for (int i = 0; i < ROW_COUNT; i++)
        {
            int pkInt = hashRange(i, PAR_COUNT);
            long ck2Bigint = BASE_TIMESTAMP + (i * DAYS_TO_SPREAD * 86400000L / ROW_COUNT);
            long col2Timestamp = ck2Bigint + (hash2(i, 8) % 86400) * 1000L;
            long ck1Bigint = (col2Timestamp / 3600000L) * 3600000L;
            String ck3Text = "value_" + i;
            long ck4Bigint = hash2(i, 1) % HC;
            long ck5Bigint = hash2(i, 2) % MC;
            String col1Text = "text_" + (i + 1);
            long col3Bigint = (i + 1000) * 1000L;
            int col4Int = hash2(i, 3) % LC;
            int col5Int = hash2(i, 4) % 2;
            int col6Int = hash2(i, 5) % 100;
            String col7Text = "data_" + i;
            int col8Int = hash2(i, 6) % LC;
            long col9Timestamp = col2Timestamp + (hash2(i, 7) % 1000) * 1000L;

            execute("INSERT INTO %s (pk_int, ck1_bigint, ck2_bigint, ck3_text, ck4_bigint, ck5_bigint, " +
                    "col1_text, col2_timestamp, col3_bigint, col4_int, col5_int, col6_int, " +
                    "col7_text, col8_int, col9_timestamp) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    pkInt, ck1Bigint, ck2Bigint, ck3Text, ck4Bigint, ck5Bigint,
                    col1Text, col2Timestamp, col3Bigint, col4Int, col5Int, col6Int,
                    col7Text, col8Int, col9Timestamp);
        }
    }

    private int hashRange(int i, int range)
    {
        return Math.abs(hash(i)) % range;
    }

    private int hash(int i)
    {
        // Simple hash function
        int h = i;
        h ^= (h >>> 16);
        h *= 0x85ebca6b;
        h ^= (h >>> 13);
        h *= 0xc2b2ae35;
        h ^= (h >>> 16);
        return h;
    }

    private int hash2(int i, int seed)
    {
        // Hash with seed
        return Math.abs(hash(i + seed * 31));
    }
}
