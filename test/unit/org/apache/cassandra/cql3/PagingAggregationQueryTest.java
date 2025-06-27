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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.assertj.core.api.Assertions;

@RunWith(Parameterized.class)
public class PagingAggregationQueryTest extends CQLTester
{
    public static final int NUM_PARTITIONS = 100;

    @Parameterized.Parameters(name = "aggregation_sub_page_size={0} data_size={1} flush={2}")
    public static Collection<Object[]> generateParameters()
    {
        List<Object[]> params = new ArrayList<>();
        for (PageSize pageSize : Arrays.asList(PageSize.inBytes(1024), PageSize.NONE))
            for (DataSize dataSize : DataSize.values())
                for (boolean flush : new boolean[]{ true, false })
                    params.add(new Object[]{ pageSize, dataSize, flush });
        return params;
    }

    /**
     * The regular column value to be inserted.
     * It will use different sizes to change the number of rows that fit in a page.
     */
    private final ByteBuffer value;

    /**
     * Whether to flush after each type of mutation.
     */
    private final boolean flush;

    public enum DataSize
    {
        NULL(-1),
        SMALL(10), // multiple rows per page
        LARGE(2000); // one row per page

        private final int size;

        DataSize(int size)
        {
            this.size = size;
        }

        public ByteBuffer value()
        {
            return size == -1 ? null : ByteBuffer.allocate(size);
        }
    }

    public PagingAggregationQueryTest(PageSize subPageSize, DataSize dataSize, boolean flush)
    {
        DatabaseDescriptor.setAggregationSubPageSize(subPageSize);
        this.value = dataSize.value();
        this.flush = flush;
        enableCoordinatorExecution();
    }

    /**
     * DSP-22813, DBPE-16245, DBPE-16378 and CNDB-13978: Test that count(*) aggregation queries return the correct
     * number of rows, even if there are tombstones and paging is required.
     * </p>
     * Before the DSP-22813/DBPE-16245/DBPE-16378/CNDB-13978 fix these queries would stop counting after hitting the
     * {@code aggregation_sub_page_size} page size, returning only the count of a single page.
     */
    @Test
    public void testAggregationWithCellTomsbstones()
    {
        // we are only interested in the NULL value case, which produces cell tombstones
        Assume.assumeTrue(value == null && !flush);

        createTable("CREATE TABLE %s (k int, c1 int, c2 int, v blob, PRIMARY KEY (k, c1, c2))");

        int ks = 13;
        int c1s = 17;
        int c2s = 19;

        // insert some data
        for (int k = 0; k < ks; k++)
        {
            for (int c1 = 0; c1 < c1s; c1++)
            {
                for (int c2 = 0; c2 < c2s; c2++)
                {
                    execute("INSERT INTO %s (k, c1, c2, v) VALUES (?, ?, ?, null)", k, c1, c2);
                }
            }

            // test aggregation on single partition query
            int numRows = execute("SELECT * FROM %s WHERE k=?", k).size();
            long count = execute("SELECT COUNT(*) FROM %s WHERE k=?", k).one().getLong("count");
            Assertions.assertThat(count).isEqualTo(numRows).isEqualTo(c1s * c2s);
        }

        // test aggregation on range query
        int numRows = execute("SELECT * FROM %s").size();
        long count = execute("SELECT COUNT(*) FROM %s").one().getLong("count");
        Assertions.assertThat(count).isEqualTo(numRows).isEqualTo(ks * c1s * c2s);

        // test aggregation with group by
        Assertions.assertThat(execute("SELECT COUNT(*) FROM %s GROUP BY k").size()).isEqualTo(ks);
        Assertions.assertThat(execute("SELECT COUNT(*) FROM %s GROUP BY k, c1").size()).isEqualTo(ks * c1s);
        Assertions.assertThat(execute("SELECT COUNT(*) FROM %s GROUP BY k, c1, c2").size()).isEqualTo(ks * c1s * c2s);
    }

    @Test
    public void testAggregationWithRowDeletions()
    {
        createTable("CREATE TABLE %s (k bigint, c int, v blob, PRIMARY KEY(k, c))");

        int numClusterings = 7;

        // insert some clusterings, and flush
        for (long k = 1; k <= NUM_PARTITIONS; k++)
        {
            // insert some clusterings
            for (int c = 1; c <= numClusterings; c++)
            {
                execute("INSERT INTO %s (k, c, v) VALUES (?, ?, ?)", k, c, value);
            }
        }
        maybeFlush();

        // for each partition, delete the first and last clustering
        for (long k = 1; k <= NUM_PARTITIONS; k++)
        {
            execute("DELETE FROM %s WHERE k = ? AND c = ?", k, 1);
            execute("DELETE FROM %s WHERE k = ? AND c = ?", k, numClusterings);

            // test aggregation on single partition query
            int numRows = execute("SELECT * FROM %s WHERE k=?", k).size();
            long count = execute("SELECT COUNT(*) FROM %s WHERE k=?", k).one().getLong("count");
            Assertions.assertThat(count)
                      .isEqualTo(numRows)
                      .isEqualTo(numClusterings - 2);
            long maxK = execute("SELECT max(k) FROM %s WHERE k=?", k).one().getLong("system.max(k)");
            Assertions.assertThat(maxK).isEqualTo(k);
            int maxC = execute("SELECT max(c) FROM %s WHERE k=?", k).one().getInt("system.max(c)");
            Assertions.assertThat(maxC).isEqualTo(numClusterings - 1);
        }

        // test aggregation on range query
        int selectRows = execute("SELECT * FROM %s").size();
        long selectCountRows = execute("SELECT COUNT(*) FROM %s").one().getLong("count");
        Assertions.assertThat(selectCountRows)
                  .isEqualTo(selectRows)
                  .isEqualTo(NUM_PARTITIONS * (numClusterings - 2));
        long maxK = execute("SELECT max(k) FROM %s").one().getLong("system.max(k)");
        Assertions.assertThat(maxK).isEqualTo(NUM_PARTITIONS);
        int maxC = execute("SELECT max(c) FROM %s").one().getInt("system.max(c)");
        Assertions.assertThat(maxC).isEqualTo(numClusterings - 1);

        // test aggregation with group by
        Assertions.assertThat(execute("SELECT COUNT(*) FROM %s GROUP BY k").size()).isEqualTo(NUM_PARTITIONS);
        Assertions.assertThat(execute("SELECT COUNT(*) FROM %s GROUP BY k, c").size()).isEqualTo(NUM_PARTITIONS * (numClusterings - 2));
    }

    @Test
    public void testAggregationWithPartitionDeletionWithoutClustering()
    {
        createTable("CREATE TABLE %s (k bigint PRIMARY KEY, v blob)");

        // insert some partitions, and flush
        for (long k = 1; k <= NUM_PARTITIONS; k++)
        {
            execute("INSERT INTO %s (k, v) VALUES (?, ?)", k, value);
        }
        maybeFlush();

        // delete all the partitions, and flush
        for (long k = 1; k <= NUM_PARTITIONS; k++)
        {
            execute("DELETE FROM %s WHERE k = ?", k);
        }
        maybeFlush();

        // re-insert part of the data, using the same partitions
        for (long k = 1; k <= NUM_PARTITIONS; k++)
        {
            execute("INSERT INTO %s (k) VALUES (?)", k);

            // test aggregation on single partition query
            int numRows = execute("SELECT * FROM %s WHERE k=?", k).size();
            long count = execute("SELECT COUNT(*) FROM %s WHERE k=?", k).one().getLong("count");
            Assertions.assertThat(count)
                      .isEqualTo(numRows)
                      .isEqualTo(1);
            long max = execute("SELECT max(k) FROM %s WHERE k=?", k).one().getLong("system.max(k)");
            Assertions.assertThat(max).isEqualTo(k);
        }

        // test aggregation on range query
        int selectRows = execute("SELECT * FROM %s").size();
        long selectCountRows = execute("SELECT COUNT(*) FROM %s").one().getLong("count");
        Assertions.assertThat(selectCountRows)
                  .isEqualTo(selectRows)
                  .isEqualTo(NUM_PARTITIONS);
        long max = execute("SELECT max(k) FROM %s").one().getLong("system.max(k)");
        Assertions.assertThat(max).isEqualTo(NUM_PARTITIONS);
    }

    @Test
    public void testAggregationWithPartitionDeletionWithClustering()
    {
        createTable("CREATE TABLE %s (k bigint, c int, v blob, PRIMARY KEY(k, c))");

        int numClusteringsBeforeDeletion = 10;
        int numClusteringsAfterDeletion = 7;

        // insert some partitions, and flush
        for (long k = 1; k <= NUM_PARTITIONS; k++)
        {
            for (int c = 1; c <= numClusteringsBeforeDeletion; c++)
            {
                execute("INSERT INTO %s (k, c, v) VALUES (?, ?, ?)", k, c, value);
            }
        }
        maybeFlush();

        // delete all the partitions, and flush
        for (long k = 1; k <= NUM_PARTITIONS; k++)
        {
            execute("DELETE FROM %s WHERE k = ?", k);
        }
        maybeFlush();

        // re-insert part of the data, using the same partitions
        for (long k = 1; k <= NUM_PARTITIONS; k++)
        {
            for (int c = 1; c <= numClusteringsAfterDeletion; c++)
            {
                execute("INSERT INTO %s (k, c) VALUES (?, ?)", k, c);
            }

            // test aggregation on single partition query
            int numRows = execute("SELECT * FROM %s WHERE k=?", k).size();
            long count = execute("SELECT COUNT(*) FROM %s WHERE k=?", k).one().getLong("count");
            Assertions.assertThat(count)
                      .isEqualTo(numRows)
                      .isEqualTo(numClusteringsAfterDeletion);
            long max = execute("SELECT max(k) FROM %s WHERE k=?", k).one().getLong("system.max(k)");
            Assertions.assertThat(max).isEqualTo(k);
        }

        // test aggregation on range query
        int selectRows = execute("SELECT * FROM %s").size();
        long selectCountRows = execute("SELECT COUNT(*) FROM %s").one().getLong("count");
        Assertions.assertThat(selectCountRows)
                  .isEqualTo(selectRows)
                  .isEqualTo(NUM_PARTITIONS * numClusteringsAfterDeletion);
        long max = execute("SELECT max(k) FROM %s").one().getLong("system.max(k)");
        Assertions.assertThat(max).isEqualTo(NUM_PARTITIONS);
    }

    private void maybeFlush()
    {
        if (flush)
            flush();
    }
}