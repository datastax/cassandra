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
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.Collection;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.memtable.TrieMemtable;
import org.apache.cassandra.index.sai.SAITester;

/// This is a regression test for CNDB-16363.
/// It verifies that estimated row counts returned by the query planner are not zero even when the number
/// of shards is large and shards contain little data.
/// To some degree this test duplicates SingleRestrictionEstimatedRowCountTest, however this one is parameterized
/// differently and explores different ranges of shard/partition counts, so both tests are needed.
@RunWith(Parameterized.class)
public class EstimatedRowCountTest extends SAITester
{
    @Parameterized.Parameter(0)
    public int numShards;

    @Parameterized.Parameter(1)
    public int numPartitions;

    @Parameters(name = "numShards={0}, numPartitions={1}")
    public static Collection<Object[]> data()
    {
        return Arrays.asList(new Object[][] {
            { 1, 0 },
            { 8, 0 },
            { 64, 0 },
            { 1, 11 },
            { 2, 11 },
            { 4, 11 },
            { 8, 11 },
            { 16, 11 },
            { 64, 11 },
            { 1, 1000 },
            { 8, 1000 },
            { 64, 10000 }
        });
    }

    @Test
    public void testReturnedRowsEstimates() throws Throwable
    {
        TrieMemtable.SHARD_COUNT = numShards;
        createTable("CREATE TABLE %s (k int, c int, n int, PRIMARY KEY(k, c))");
        createIndex("CREATE CUSTOM INDEX n_idx ON %s (n) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX c_idx ON %s (c) USING 'StorageAttachedIndex'");

        int numRowsPerPartition = 13;
        int numRows = numPartitions * numRowsPerPartition;

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.unsafeRunWithoutFlushing(() -> {
             for (int k = 0; k < numPartitions; k++)
                 for (int c = 0; c < numRowsPerPartition; c++)
                     execute("INSERT INTO %s (k, c, n) VALUES (?, ?, 1)", k, c);
         });

        beforeAndAfterFlush(() -> {
            assertThatPlanFor("SELECT k, c FROM %s WHERE n = 0", 0)
                .hasEstimatedRowsCountBetween(0.0, 0.0);
            assertThatPlanFor("SELECT k, c FROM %s WHERE n = 1", numRows)
                .hasEstimatedRowsCountBetween((double) numRows / 2, numRows * 2);
            assertThatPlanFor("SELECT k, c FROM %s WHERE n < 5", numRows)
                .hasEstimatedRowsCountBetween((double) numRows / 2, numRows * 2);
            assertThatPlanFor("SELECT k, c FROM %s WHERE c = 1", numPartitions)
                .hasEstimatedRowsCountBetween((double) numPartitions / 2, numPartitions * 2);
            assertThatPlanFor("SELECT k, c FROM %s WHERE c >= 1 AND c <= 10", numPartitions * 10)
                .hasEstimatedRowsCountBetween((double) numPartitions * 5, numPartitions * 20);
            assertThatPlanFor("SELECT k, c FROM %s WHERE n = 1 AND c = 1", numPartitions)
                .hasEstimatedRowsCountBetween((double) numPartitions / 2, numPartitions * 2);;
        });
    }
}