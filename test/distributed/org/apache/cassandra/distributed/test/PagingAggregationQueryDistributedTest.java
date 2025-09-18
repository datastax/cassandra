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

package org.apache.cassandra.distributed.test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.cql3.PageSize;
import org.apache.cassandra.cql3.PagingAggregationQueryTest;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.assertj.core.api.Assertions;

import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;

@RunWith(Parameterized.class)
public class PagingAggregationQueryDistributedTest extends TestBaseImpl
{
    public static final int NUM_PARTITIONS = 100;

    private static Cluster CLUSTER;
    private static ICoordinator COORDINATOR;

    private static final AtomicInteger seq = new AtomicInteger();
    private String table;

    @Parameterized.Parameters(name = "aggregation_sub_page_size={0} data_size={1} flush={2}")
    public static Collection<Object[]> generateParameters()
    {
        List<Object[]> params = new ArrayList<>();
        for (PageSize pageSize : Arrays.asList(PageSize.inBytes(1024), PageSize.NONE))
            for (PagingAggregationQueryTest.DataSize dataSize : PagingAggregationQueryTest.DataSize.values())
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

    @Before
    public void beforeTest()
    {
        table = "table_" + seq.getAndIncrement();
    }

    @After
    public void afterTest()
    {
        CLUSTER.schemaChange(format("DROP TABLE IF EXISTS %s"));
    }

    private String format(String query)
    {
        return String.format(query, KEYSPACE + '.' + table);
    }

    @AfterClass
    public static void after()
    {
        if (CLUSTER != null)
            CLUSTER.close();
    }

    public PagingAggregationQueryDistributedTest(PageSize subPageSize, PagingAggregationQueryTest.DataSize dataSize, boolean flush) throws IOException
    {
        CLUSTER = init(Cluster.build().withNodes(3).withConfig(c -> c.set("aggregation_subpage_size_in_kb", subPageSize.getSize())).start());
        COORDINATOR = CLUSTER.coordinator(1);
        this.value = dataSize.value();
        this.flush = flush;
    }

    @Test
    public void testAggregationWithCellTomsbstones()
    {
        CLUSTER.schemaChange(format("CREATE TABLE %s (k int, c1 int, c2 int, v blob, PRIMARY KEY (k, c1, c2))"));

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
                    write(1, "INSERT INTO %s (k, c1, c2, v) VALUES (?, ?, ?, null)", k, c1, c2);
                }
            }

            // test aggregation on single partition query
            long selectRows = selectRows("SELECT * FROM %s WHERE k=?", k);
            long selectCountRows = selectCountRows("SELECT COUNT(*) FROM %s WHERE k=?", k);
            Assertions.assertThat(selectCountRows).isEqualTo(selectRows).isEqualTo(c1s * c2s);
        }
        maybeFlush();

        // test aggregation on range query
        long selectRows = selectRows("SELECT * FROM %s");
        long selectCountRows = selectCountRows("SELECT COUNT(*) FROM %s");
        Assertions.assertThat(selectCountRows).isEqualTo(selectRows).isEqualTo(ks * c1s * c2s);
    }

    @Test
    public void testAggregationWithPartialRowDeletions()
    {
        CLUSTER.schemaChange(format("CREATE TABLE %s (k bigint, c int, v blob, PRIMARY KEY(k, c))"));

        int numClusterings = 7;

        // insert some clusterings, and flush
        for (long k = 1; k <= NUM_PARTITIONS; k++)
        {
            // insert some clusterings
            for (int c = 1; c <= numClusterings; c++)
            {
                write(1, "INSERT INTO %s (k, c, v) VALUES (?, ?, ?)", k, c, value);
            }
        }
        maybeFlush();

        // for each partition, delete the first and last clustering
        for (long k = 1; k <= NUM_PARTITIONS; k++)
        {
            write(2, "DELETE FROM %s WHERE k = ? AND c = ?", k, 1);
            write(2, "DELETE FROM %s WHERE k = ? AND c = ?", k, numClusterings);

            // test aggregation on single partition query
            long selectRows = selectRows("SELECT * FROM %s WHERE k=?", k);
            long selectCountRows = selectCountRows("SELECT COUNT(*) FROM %s WHERE k=?", k);
            Assertions.assertThat(selectRows)
                      .isEqualTo(selectCountRows)
                      .isEqualTo(numClusterings - 2);
        }

        // test aggregation on range query
        long selectRows = selectRows("SELECT * FROM %s");
        long selectCountRows = selectCountRows("SELECT COUNT(*) FROM %s");
        Assertions.assertThat(selectCountRows)
                  .isEqualTo(selectRows)
                  .isEqualTo(NUM_PARTITIONS * (numClusterings - 2));
    }

    @Test
    public void testAggregationWithCompleteRowDeletions()
    {
        CLUSTER.schemaChange(format("CREATE TABLE %s (k bigint, c int, v blob, PRIMARY KEY(k, c))"));

        int numClusterings = 7;

        // insert some clusterings, and flush
        for (long k = 1; k <= NUM_PARTITIONS; k++)
        {
            // insert some clusterings
            for (int c = 1; c <= numClusterings; c++)
            {
                write(1, "INSERT INTO %s (k, c, v) VALUES (?, ?, ?)", k, c, value);
            }
        }
        maybeFlush();

        // for each partition, delete the first and last clustering
        for (long k = 1; k <= NUM_PARTITIONS; k++)
        {
            for (int c = 1; c <= numClusterings; c++)
            {
                write(2, "DELETE FROM %s WHERE k = ? AND c = ?", k, c);

                // test aggregation on single partition query
                long selectRows = selectRows("SELECT * FROM %s WHERE k=?", k);
                long selectCountRows = selectCountRows("SELECT COUNT(*) FROM %s WHERE k=?", k);
                Assertions.assertThat(selectRows)
                          .isEqualTo(selectCountRows)
                          .isEqualTo(numClusterings - c);
            }
        }

        // test aggregation on range query
        long selectRows = selectRows("SELECT * FROM %s");
        long selectCountRows = selectCountRows("SELECT COUNT(*) FROM %s");
        Assertions.assertThat(selectCountRows)
                  .isEqualTo(selectRows)
                  .isEqualTo(0);
    }

    @Test
    public void testAggregationWithPartitionDeletionWithoutClustering()
    {
        CLUSTER.schemaChange(format("CREATE TABLE %s (k bigint PRIMARY KEY, v blob)"));

        // insert some partitions, and flush
        for (long k = 1; k <= NUM_PARTITIONS; k++)
        {
            write(1, "INSERT INTO %s (k, v) VALUES (?, ?) USING TIMESTAMP 1", k, value);
        }
        maybeFlush();

        // delete all the partitions, and flush
        for (long k = 1; k <= NUM_PARTITIONS; k++)
        {
            write(2, "DELETE FROM %s USING TIMESTAMP 2 WHERE k = ?", k);
        }
        maybeFlush();

        // re-insert part of the data, using the same partitions
        for (long k = 1; k <= NUM_PARTITIONS; k++)
        {
            write(3, "INSERT INTO %s (k) VALUES (?) USING TIMESTAMP 3", k);

            // test aggregation on single partition query
            long selectRows = selectRows("SELECT * FROM %s WHERE k=?", k);
            long selectCountRows = selectCountRows("SELECT COUNT(*) FROM %s WHERE k=?", k);
            Assertions.assertThat(selectRows).isEqualTo(selectCountRows);
            Assertions.assertThat(selectRows).isEqualTo(1);
        }

        // test aggregation on range query
        long selectRows = selectRows("SELECT * FROM %s");
        long selectCountRows = selectCountRows("SELECT COUNT(*) FROM %s");
        Assertions.assertThat(selectRows).isEqualTo(selectCountRows);
        Assertions.assertThat(selectRows).isEqualTo(NUM_PARTITIONS);
    }

    @Test
    public void testAggregationWithPartitionDeletionWithClustering()
    {
        CLUSTER.schemaChange(format("CREATE TABLE %s (k bigint, c int, v blob, PRIMARY KEY(k, c))"));

        int numClusteringsBeforeDeletion = 10;
        int numClusteringsAfterDeletion = 7;

        // insert some partitions, and flush
        for (long k = 1; k <= NUM_PARTITIONS; k++)
        {
            for (int c = 1; c <= numClusteringsBeforeDeletion; c++)
            {
                write(1, "INSERT INTO %s (k, c, v) VALUES (?, ?, ?) USING TIMESTAMP 1", k, c, value);
            }
        }
        maybeFlush();

        // delete all the partitions, and flush
        for (long k = 1; k <= NUM_PARTITIONS; k++)
        {
            write(2, "DELETE FROM %s USING TIMESTAMP 2 WHERE k = ?", k);
        }
        maybeFlush();

        // re-insert part of the data, using the same partitions
        for (long k = 1; k <= NUM_PARTITIONS; k++)
        {
            for (int c = 1; c <= numClusteringsAfterDeletion; c++)
            {
                write(3, "INSERT INTO %s (k, c) VALUES (?, ?) USING TIMESTAMP 3", k, c);
            }

            // test aggregation on single partition query
            long selectRows = selectRows("SELECT * FROM %s WHERE k=?", k);
            long selectCountRows = selectCountRows("SELECT COUNT(*) FROM %s WHERE k=?", k);
            Assertions.assertThat(selectRows).isEqualTo(selectCountRows);
            Assertions.assertThat(selectRows).isEqualTo(numClusteringsAfterDeletion);
        }

        // test aggregation on range query
        long selectRows = selectRows("SELECT * FROM %s");
        long selectCountRows = selectCountRows("SELECT COUNT(*) FROM %s");
        Assertions.assertThat(selectRows).isEqualTo(selectCountRows);
        Assertions.assertThat(selectRows).isEqualTo(NUM_PARTITIONS * numClusteringsAfterDeletion);
    }

    private void write(int node, String query, Object... args)
    {
        CLUSTER.get(node).executeInternal(format(query), args);
    }

    private long selectRows(String query, Object... args)
    {
        return COORDINATOR.execute(format(query), ALL, args).length;
    }

    private long selectCountRows(String query, Object... args)
    {
        return (long) COORDINATOR.execute(format(query), ALL, args)[0][0];
    }

    private void maybeFlush()
    {
        if (flush)
            CLUSTER.forEach(i -> i.flush(KEYSPACE));
    }
}
