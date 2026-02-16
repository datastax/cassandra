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

package org.apache.cassandra.index.sai;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.plan.StorageAttachedIndexQueryPlan;
import org.apache.cassandra.index.sai.plan.StorageAttachedIndexSearcher;

import static org.junit.Assert.assertEquals;

public class QueryContextTest extends SAITester.Versioned
{
    @Test
    public void testSkinnyTable()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, a int, b int)");
        createIndex("CREATE CUSTOM INDEX ON %s(a) USING 'StorageAttachedIndex'");
        execute("INSERT INTO %s (k, a, b) VALUES (0, 0, 0)");
        execute("INSERT INTO %s (k, a, b) VALUES (1, 1, 1)");
        execute("INSERT INTO %s (k, a, b) VALUES (2, 0, 0)");
        execute("INSERT INTO %s (k, a, b) VALUES (3, 1, 1)");
        flush();
        QueryContext.Snapshot snapshot;

        // index filtering that accepts all rows
        snapshot = queryContext("SELECT * FROM %s WHERE a >= 0 ALLOW FILTERING",
                                row(0, 0, 0),
                                row(1, 1, 1),
                                row(2, 0, 0),
                                row(3, 1, 1));
        assertEquals(4, snapshot.keysFetched);
        assertEquals(4, snapshot.partitionsFetched);
        assertEquals(4, snapshot.partitionsReturned);
        assertEquals(0, snapshot.partitionTombstonesFetched);
        assertEquals(4, snapshot.rowsFetched);
        assertEquals(4, snapshot.rowsReturned);
        assertEquals(0, snapshot.rowTombstonesFetched);

        // index filtering that accepts no rows
        snapshot = queryContext("SELECT * FROM %s WHERE a < 0 ALLOW FILTERING");
        assertEquals(0, snapshot.keysFetched);
        assertEquals(0, snapshot.partitionsFetched);
        assertEquals(0, snapshot.partitionsReturned);
        assertEquals(0, snapshot.partitionTombstonesFetched);
        assertEquals(0, snapshot.rowsFetched);
        assertEquals(0, snapshot.rowsReturned);
        assertEquals(0, snapshot.rowTombstonesFetched);

        // index filtering that accepts some rows
        snapshot = queryContext("SELECT * FROM %s WHERE a = 0 ALLOW FILTERING",
                                row(0, 0, 0),
                                row(2, 0, 0));
        assertEquals(2, snapshot.keysFetched);
        assertEquals(2, snapshot.partitionsFetched);
        assertEquals(2, snapshot.partitionsReturned);
        assertEquals(0, snapshot.partitionTombstonesFetched);
        assertEquals(2, snapshot.rowsFetched);
        assertEquals(2, snapshot.rowsReturned);
        assertEquals(0, snapshot.rowTombstonesFetched);

        // index filtering that accepts some rows, different value
        snapshot = queryContext("SELECT * FROM %s WHERE a = 1 ALLOW FILTERING",
                                row(1, 1, 1),
                                row(3, 1, 1));
        assertEquals(2, snapshot.keysFetched);
        assertEquals(2, snapshot.partitionsFetched);
        assertEquals(2, snapshot.partitionsReturned);
        assertEquals(0, snapshot.partitionTombstonesFetched);
        assertEquals(2, snapshot.rowsFetched);
        assertEquals(2, snapshot.rowsReturned);
        assertEquals(0, snapshot.rowTombstonesFetched);

        // not-indexed column filtering that accepts all rows
        snapshot = queryContext("SELECT * FROM %s WHERE a = 0 AND b = 0 ALLOW FILTERING",
                                row(0, 0, 0),
                                row(2, 0, 0));
        assertEquals(2, snapshot.keysFetched);
        assertEquals(2, snapshot.partitionsFetched);
        assertEquals(2, snapshot.partitionsReturned);
        assertEquals(0, snapshot.partitionTombstonesFetched);
        assertEquals(2, snapshot.rowsFetched);
        assertEquals(2, snapshot.rowsReturned);
        assertEquals(0, snapshot.rowTombstonesFetched);

        // not-indexed column filtering that accepts no rows
        snapshot = queryContext("SELECT * FROM %s WHERE a = 0 AND b = 1 ALLOW FILTERING");
        assertEquals(2, snapshot.keysFetched);
        assertEquals(2, snapshot.partitionsFetched);
        assertEquals(0, snapshot.partitionsReturned);
        assertEquals(0, snapshot.partitionTombstonesFetched);
        assertEquals(2, snapshot.rowsFetched);
        assertEquals(0, snapshot.rowsReturned);
        assertEquals(0, snapshot.rowTombstonesFetched);

        // not-indexed column filtering that accepts some rows
        snapshot = queryContext("SELECT * FROM %s WHERE a >= 0 AND b = 0 ALLOW FILTERING",
                                row(0, 0, 0),
                                row(2, 0, 0));
        assertEquals(4, snapshot.keysFetched);
        assertEquals(4, snapshot.partitionsFetched);
        assertEquals(2, snapshot.partitionsReturned);
        assertEquals(0, snapshot.partitionTombstonesFetched);
        assertEquals(4, snapshot.rowsFetched);
        assertEquals(2, snapshot.rowsReturned);
        assertEquals(0, snapshot.rowTombstonesFetched);

        // partition/primary key query
        snapshot = queryContext("SELECT * FROM %s WHERE a >= 0 AND k = 0 ALLOW FILTERING",
                                row(0, 0, 0));
        assertEquals(1, snapshot.keysFetched);
        assertEquals(1, snapshot.partitionsFetched);
        assertEquals(1, snapshot.partitionsReturned);
        assertEquals(0, snapshot.partitionTombstonesFetched);
        assertEquals(1, snapshot.rowsFetched);
        assertEquals(1, snapshot.rowsReturned);
        assertEquals(0, snapshot.rowTombstonesFetched);

        // partition/primary key filtering
        snapshot = queryContext("SELECT * FROM %s WHERE a >= 0 AND k != 1 ALLOW FILTERING",
                                row(0, 0, 0),
                                row(2, 0, 0),
                                row(3, 1, 1));
        assertEquals(4, snapshot.keysFetched);
        assertEquals(4, snapshot.partitionsFetched);
        assertEquals(3, snapshot.partitionsReturned);
        assertEquals(0, snapshot.partitionTombstonesFetched);
        assertEquals(4, snapshot.rowsFetched);
        assertEquals(3, snapshot.rowsReturned);
        assertEquals(0, snapshot.rowTombstonesFetched);

        // delete a partition/row
        execute("DELETE FROM %s WHERE k = 1");
        snapshot = queryContext("SELECT * FROM %s WHERE a >= 0 ALLOW FILTERING",
                                row(0, 0, 0),
                                row(2, 0, 0),
                                row(3, 1, 1));
        assertEquals(4, snapshot.keysFetched);
        assertEquals(3, snapshot.partitionsFetched);
        assertEquals(3, snapshot.partitionsReturned);
        assertEquals(1, snapshot.partitionTombstonesFetched);
        assertEquals(3, snapshot.rowsFetched);
        assertEquals(3, snapshot.rowsReturned);
        assertEquals(0, snapshot.rowTombstonesFetched);

        // delete an indexed cell
        execute("DELETE a FROM %s WHERE k = 2");
        snapshot = queryContext("SELECT * FROM %s WHERE a >= 0 ALLOW FILTERING",
                                row(0, 0, 0),
                                row(3, 1, 1));
        assertEquals(4, snapshot.keysFetched);
        assertEquals(3, snapshot.partitionsFetched);
        assertEquals(2, snapshot.partitionsReturned);
        assertEquals(1, snapshot.partitionTombstonesFetched);
        assertEquals(3, snapshot.rowsFetched);
        assertEquals(2, snapshot.rowsReturned);
        assertEquals(0, snapshot.rowTombstonesFetched);

        // compact to rebuild the index, and verify that tombstones are gone
        flush();
        compact();
        snapshot = queryContext("SELECT * FROM %s WHERE a >= 0 ALLOW FILTERING",
                                row(0, 0, 0),
                                row(3, 1, 1));
        assertEquals(2, snapshot.keysFetched);
        assertEquals(2, snapshot.partitionsFetched);
        assertEquals(2, snapshot.partitionsReturned);
        assertEquals(0, snapshot.partitionTombstonesFetched);
        assertEquals(2, snapshot.rowsFetched);
        assertEquals(2, snapshot.rowsReturned);
        assertEquals(0, snapshot.rowTombstonesFetched);

        // truncate the table
        truncate(false);
        snapshot = queryContext("SELECT * FROM %s WHERE a >= 0 ALLOW FILTERING");
        assertEquals(0, snapshot.keysFetched);
        assertEquals(0, snapshot.partitionsFetched);
        assertEquals(0, snapshot.partitionsReturned);
        assertEquals(0, snapshot.partitionTombstonesFetched);
        assertEquals(0, snapshot.rowsFetched);
        assertEquals(0, snapshot.rowsReturned);
        assertEquals(0, snapshot.rowTombstonesFetched);

        // insert some data using TTLs
        execute("INSERT INTO %s (k, a, b) VALUES (0, 0, 0)");
        execute("INSERT INTO %s (k, a, b) VALUES (1, 1, 1) USING TTL 1");
        execute("INSERT INTO %s (k, a, b) VALUES (2, 0, 0)");
        execute("INSERT INTO %s (k, a, b) VALUES (3, 1, 1) USING TTL 1");
        flush();
        Uninterruptibles.sleepUninterruptibly(2, TimeUnit.SECONDS);
        snapshot = queryContext("SELECT * FROM %s WHERE a >= 0 ALLOW FILTERING",
                                row(0, 0, 0),
                                row(2, 0, 0));
        assertEquals(4, snapshot.keysFetched);
        assertEquals(4, snapshot.partitionsFetched);
        assertEquals(2, snapshot.partitionsReturned);
        assertEquals(0, snapshot.partitionTombstonesFetched);
        assertEquals(2, snapshot.rowsFetched);
        assertEquals(2, snapshot.rowsReturned);
        assertEquals(2, snapshot.rowTombstonesFetched);
    }

    @Test
    public void testWideTableWithoutStatics()
    {
        createTable("CREATE TABLE %s (k int, c int, a int, b int, PRIMARY KEY(k, c))");
        createIndex("CREATE CUSTOM INDEX ON %s(a) USING 'StorageAttachedIndex'");
        execute("INSERT INTO %s (k, c, a, b) VALUES (0, 0, 0, 0)");
        execute("INSERT INTO %s (k, c, a, b) VALUES (0, 1, 1, 1)");
        execute("INSERT INTO %s (k, c, a, b) VALUES (0, 2, 0, 0)");
        execute("INSERT INTO %s (k, c, a, b) VALUES (0, 3, 1, 1)");
        execute("INSERT INTO %s (k, c, a, b) VALUES (1, 0, 0, 0)");
        execute("INSERT INTO %s (k, c, a, b) VALUES (1, 1, 1, 1)");
        execute("INSERT INTO %s (k, c, a, b) VALUES (1, 2, 0, 0)");
        execute("INSERT INTO %s (k, c, a, b) VALUES (1, 3, 1, 1)");
        flush();
        QueryContext.Snapshot snapshot;

        // index filtering that accepts all rows
        snapshot = queryContext("SELECT * FROM %s WHERE a >= 0",
                                row(0, 0, 0, 0),
                                row(0, 1, 1, 1),
                                row(0, 2, 0, 0),
                                row(0, 3, 1, 1),
                                row(1, 0, 0, 0),
                                row(1, 1, 1, 1),
                                row(1, 2, 0, 0),
                                row(1, 3, 1, 1));
        assertEquals(isRowAware() ? 8 : 2, snapshot.keysFetched);
        assertEquals(2, snapshot.partitionsFetched);
        assertEquals(2, snapshot.partitionsReturned);
        assertEquals(0, snapshot.partitionTombstonesFetched);
        assertEquals(0, snapshot.partitionTombstonesFetched);
        assertEquals(8, snapshot.rowsFetched);
        assertEquals(8, snapshot.rowsReturned);
        assertEquals(0, snapshot.rowTombstonesFetched);

        // index filtering that accepts no rows
        snapshot = queryContext("SELECT * FROM %s WHERE a < 0");
        assertEquals(0, snapshot.keysFetched);
        assertEquals(0, snapshot.partitionsFetched);
        assertEquals(0, snapshot.partitionsReturned);
        assertEquals(0, snapshot.partitionTombstonesFetched);
        assertEquals(0, snapshot.rowsFetched);
        assertEquals(0, snapshot.rowsReturned);
        assertEquals(0, snapshot.rowTombstonesFetched);

        // index filtering that accepts some rows
        snapshot = queryContext("SELECT * FROM %s WHERE a = 0",
                                row(0, 0, 0, 0),
                                row(0, 2, 0, 0),
                                row(1, 0, 0, 0),
                                row(1, 2, 0, 0));
        assertEquals(isRowAware() ? 4 : 2, snapshot.keysFetched);
        assertEquals(2, snapshot.partitionsFetched);
        assertEquals(2, snapshot.partitionsReturned);
        assertEquals(0, snapshot.partitionTombstonesFetched);
        assertEquals(isRowAware() ? 4 : 8, snapshot.rowsFetched);
        assertEquals(4, snapshot.rowsReturned);
        assertEquals(0, snapshot.rowTombstonesFetched);

        // index filtering that accepts some rows, different value
        assertEquals(isRowAware() ? 4 : 2, snapshot.keysFetched);
        snapshot = queryContext("SELECT * FROM %s WHERE a = 1",
                                row(0, 1, 1, 1),
                                row(0, 3, 1, 1),
                                row(1, 1, 1, 1),
                                row(1, 3, 1, 1));
        assertEquals(2, snapshot.partitionsFetched);
        assertEquals(2, snapshot.partitionsReturned);
        assertEquals(0, snapshot.partitionTombstonesFetched);
        assertEquals(isRowAware() ? 4 : 8, snapshot.rowsFetched);
        assertEquals(4, snapshot.rowsReturned);
        assertEquals(0, snapshot.rowTombstonesFetched);

        // not-indexed column filtering that accepts all rows
        snapshot = queryContext("SELECT * FROM %s WHERE a = 0 AND b >= 0 ALLOW FILTERING",
                                row(0, 0, 0, 0),
                                row(0, 2, 0, 0),
                                row(1, 0, 0, 0),
                                row(1, 2, 0, 0));
        assertEquals(isRowAware() ? 4 : 2, snapshot.keysFetched);
        assertEquals(2, snapshot.partitionsFetched);
        assertEquals(2, snapshot.partitionsReturned);
        assertEquals(0, snapshot.partitionTombstonesFetched);
        assertEquals(isRowAware() ? 4 : 8, snapshot.rowsFetched);
        assertEquals(4, snapshot.rowsReturned);
        assertEquals(0, snapshot.rowTombstonesFetched);

        // not-indexed column filtering that accepts no rows
        snapshot = queryContext("SELECT * FROM %s WHERE a = 0 AND b < 0 ALLOW FILTERING");
        assertEquals(isRowAware() ? 4 : 2, snapshot.keysFetched);
        assertEquals(2, snapshot.partitionsFetched);
        assertEquals(0, snapshot.partitionsReturned);
        assertEquals(0, snapshot.partitionTombstonesFetched);
        assertEquals(isRowAware() ? 4 : 8, snapshot.rowsFetched);
        assertEquals(0, snapshot.rowsReturned);
        assertEquals(0, snapshot.rowTombstonesFetched);

        // not-indexed column filtering that accepts some rows
        snapshot = queryContext("SELECT * FROM %s WHERE a >= 0 AND b = 0 ALLOW FILTERING",
                                row(0, 0, 0, 0),
                                row(0, 2, 0, 0),
                                row(1, 0, 0, 0),
                                row(1, 2, 0, 0));
        assertEquals(isRowAware() ? 8 : 2, snapshot.keysFetched);
        assertEquals(2, snapshot.partitionsFetched);
        assertEquals(2, snapshot.partitionsReturned);
        assertEquals(0, snapshot.partitionTombstonesFetched);
        assertEquals(8, snapshot.rowsFetched);
        assertEquals(4, snapshot.rowsReturned);
        assertEquals(0, snapshot.rowTombstonesFetched);

        // partition key query
        snapshot = queryContext("SELECT * FROM %s WHERE a >= 0 AND k = 0 ALLOW FILTERING",
                                row(0, 0, 0, 0),
                                row(0, 1, 1, 1),
                                row(0, 2, 0, 0),
                                row(0, 3, 1, 1));
        assertEquals(isRowAware() ? 4 : 1, snapshot.keysFetched);
        assertEquals(1, snapshot.partitionsFetched);
        assertEquals(1, snapshot.partitionsReturned);
        assertEquals(0, snapshot.partitionTombstonesFetched);
        assertEquals(4, snapshot.rowsFetched);
        assertEquals(4, snapshot.rowsReturned);
        assertEquals(0, snapshot.rowTombstonesFetched);

        // primary key query
        snapshot = queryContext("SELECT * FROM %s WHERE a >= 0 AND k = 0 AND c IN (0, 2) ALLOW FILTERING",
                                row(0, 0, 0, 0),
                                row(0, 2, 0, 0));
        assertEquals(isRowAware() ? 2 : 1, snapshot.keysFetched);
        assertEquals(1, snapshot.partitionsFetched);
        assertEquals(1, snapshot.partitionsReturned);
        assertEquals(0, snapshot.partitionTombstonesFetched);
        assertEquals(2, snapshot.rowsFetched);
        assertEquals(2, snapshot.rowsReturned);
        assertEquals(0, snapshot.rowTombstonesFetched);

        // partition key filtering
        snapshot = queryContext("SELECT * FROM %s WHERE a >= 0 AND k != 1 ALLOW FILTERING",
                                row(0, 0, 0, 0),
                                row(0, 1, 1, 1),
                                row(0, 2, 0, 0),
                                row(0, 3, 1, 1));
        assertEquals(isRowAware() ? 8 : 2, snapshot.keysFetched);
        assertEquals(2, snapshot.partitionsFetched);
        assertEquals(1, snapshot.partitionsReturned);
        assertEquals(0, snapshot.partitionTombstonesFetched);
        assertEquals(8, snapshot.rowsFetched);
        assertEquals(4, snapshot.rowsReturned);
        assertEquals(0, snapshot.rowTombstonesFetched);

        // clustering key filtering
        snapshot = queryContext("SELECT * FROM %s WHERE a >= 0 AND c != 1 ALLOW FILTERING",
                                row(0, 0, 0, 0),
                                row(0, 2, 0, 0),
                                row(0, 3, 1, 1),
                                row(1, 0, 0, 0),
                                row(1, 2, 0, 0),
                                row(1, 3, 1, 1));
        assertEquals(isRowAware() ? 6 : 2, snapshot.keysFetched); // the clustering key filter is applied to indexed keys
        assertEquals(2, snapshot.partitionsFetched);
        assertEquals(2, snapshot.partitionsReturned);
        assertEquals(0, snapshot.partitionTombstonesFetched);
        assertEquals(6, snapshot.rowsFetched);
        assertEquals(6, snapshot.rowsReturned);
        assertEquals(0, snapshot.rowTombstonesFetched);

        // delete a row
        execute("DELETE FROM %s WHERE k = 0 AND c = 0");
        snapshot = queryContext("SELECT * FROM %s WHERE a = 0",
                                row(0, 2, 0, 0),
                                row(1, 0, 0, 0),
                                row(1, 2, 0, 0));
        assertEquals(isRowAware() ? 4 : 2, snapshot.keysFetched);
        assertEquals(2, snapshot.partitionsFetched);
        assertEquals(2, snapshot.partitionsReturned);
        assertEquals(0, snapshot.partitionTombstonesFetched);
        assertEquals(isRowAware() ? 3 : 7, snapshot.rowsFetched);
        assertEquals(3, snapshot.rowsReturned);
        assertEquals(1, snapshot.rowTombstonesFetched);

        // delete a cell
        execute("DELETE a FROM %s WHERE k = 1 AND c = 0");
        snapshot = queryContext("SELECT * FROM %s WHERE a = 0",
                                row(0, 2, 0, 0),
                                row(1, 2, 0, 0));
        assertEquals(isRowAware() ? 4 : 2, snapshot.keysFetched);
        assertEquals(2, snapshot.partitionsFetched);
        assertEquals(2, snapshot.partitionsReturned);
        assertEquals(0, snapshot.partitionTombstonesFetched);
        assertEquals(isRowAware() ? 3 : 7, snapshot.rowsFetched);
        assertEquals(2, snapshot.rowsReturned);
        assertEquals(1, snapshot.rowTombstonesFetched);

        // delete a partition
        execute("DELETE FROM %s WHERE k = 0");
        snapshot = queryContext("SELECT * FROM %s WHERE a = 0",
                                row(1, 2, 0, 0));
        assertEquals(isRowAware() ? 4 : 2, snapshot.keysFetched);
        assertEquals(1, snapshot.partitionsFetched);
        assertEquals(1, snapshot.partitionsReturned);
        assertEquals(1, snapshot.partitionTombstonesFetched);
        assertEquals(isRowAware() ? 2 : 4, snapshot.rowsFetched);
        assertEquals(1, snapshot.rowsReturned);
        assertEquals(0, snapshot.rowTombstonesFetched);

        // delete all the rows in a partition
        execute("DELETE FROM %s WHERE k = 1 AND c = 0");
        execute("DELETE FROM %s WHERE k = 1 AND c = 2");
        snapshot = queryContext("SELECT * FROM %s WHERE a = 0");
        assertEquals(isRowAware() ? 4 : 2, snapshot.keysFetched);
        assertEquals(1, snapshot.partitionsFetched);
        assertEquals(0, snapshot.partitionsReturned);
        assertEquals(1, snapshot.partitionTombstonesFetched);
        assertEquals(isRowAware() ? 0 : 2, snapshot.rowsFetched);
        assertEquals(0, snapshot.rowsReturned);
        assertEquals(2, snapshot.rowTombstonesFetched);

        // compact to rebuild the index, and verify that tombstones are gone
        flush();
        compact();
        snapshot = queryContext("SELECT * FROM %s WHERE a >= 0",
                                row(1, 1, 1, 1),
                                row(1, 3, 1, 1));
        assertEquals(isRowAware() ? 2 : 1, snapshot.keysFetched);
        assertEquals(1, snapshot.partitionsFetched);
        assertEquals(1, snapshot.partitionsReturned);
        assertEquals(0, snapshot.partitionTombstonesFetched);
        assertEquals(2, snapshot.rowsFetched);
        assertEquals(2, snapshot.rowsReturned);
        assertEquals(isRowAware() ? 0 : 2, snapshot.rowTombstonesFetched);

        // truncate the table
        truncate(false);
        snapshot = queryContext("SELECT * FROM %s WHERE a >= 0 ALLOW FILTERING");
        assertEquals(0, snapshot.keysFetched);
        assertEquals(0, snapshot.partitionsFetched);
        assertEquals(0, snapshot.partitionsReturned);
        assertEquals(0, snapshot.partitionTombstonesFetched);
        assertEquals(0, snapshot.rowsFetched);
        assertEquals(0, snapshot.rowsReturned);
        assertEquals(0, snapshot.rowTombstonesFetched);

        // insert some data using TTLs
        execute("INSERT INTO %s (k, c, a, b) VALUES (0, 0, 0, 0)");
        execute("INSERT INTO %s (k, c, a, b) VALUES (0, 1, 1, 1) USING TTL 1");
        execute("INSERT INTO %s (k, c, a, b) VALUES (0, 2, 0, 0)");
        execute("INSERT INTO %s (k, c, a, b) VALUES (1, 0, 0, 0)");
        execute("INSERT INTO %s (k, c, a, b) VALUES (1, 1, 1, 1)");
        execute("INSERT INTO %s (k, c, a, b) VALUES (1, 2, 0, 0) USING TTL 1");
        flush();
        Uninterruptibles.sleepUninterruptibly(2, TimeUnit.SECONDS);
        snapshot = queryContext("SELECT * FROM %s WHERE a >= 0 ALLOW FILTERING",
                                row(0, 0, 0, 0),
                                row(0, 2, 0, 0),
                                row(1, 0, 0, 0),
                                row(1, 1, 1, 1));
        assertEquals(isRowAware() ? 6 : 2, snapshot.keysFetched);
        assertEquals(2, snapshot.partitionsFetched);
        assertEquals(2, snapshot.partitionsReturned);
        assertEquals(0, snapshot.partitionTombstonesFetched);
        assertEquals(4, snapshot.rowsFetched);
        assertEquals(4, snapshot.rowsReturned);
        assertEquals(2, snapshot.rowTombstonesFetched);
    }

    @Test
    public void testWideTableScoreOrdered()
    {
        Assume.assumeTrue(version.onOrAfter(Version.JVECTOR_EARLIEST));

        createTable("CREATE TABLE %s (k int, c int, n int, v vector<float, 2>, PRIMARY KEY(k, c))");
        createIndex("CREATE CUSTOM INDEX ON %s(n) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function' : 'euclidean'}");
        execute("INSERT INTO %s (k, c, n, v) VALUES (0, 0, 0, [0, 0])");
        execute("INSERT INTO %s (k, c, n, v) VALUES (0, 1, 1, [1, 1])");
        execute("INSERT INTO %s (k, c, n, v) VALUES (0, 2, 0, [0, 0])");
        execute("INSERT INTO %s (k, c, n, v) VALUES (0, 3, 1, [1, 1])");
        execute("INSERT INTO %s (k, c, n, v) VALUES (1, 0, 0, [0, 0])");
        execute("INSERT INTO %s (k, c, n, v) VALUES (1, 1, 1, [1, 1])");
        execute("INSERT INTO %s (k, c, n, v) VALUES (1, 2, 0, [0, 0])");
        execute("INSERT INTO %s (k, c, n, v) VALUES (1, 3, 1, [1, 1])");
        flush();
        QueryContext.Snapshot snapshot;

        // index filtering that accepts all rows
        snapshot = queryContext("SELECT * FROM %s ORDER BY v ANN OF [0, 0] LIMIT 10",
                                row(1, 0, 0, vector(0, 0)),
                                row(1, 2, 0, vector(0, 0)),
                                row(0, 0, 0, vector(0, 0)),
                                row(0, 2, 0, vector(0, 0)),
                                row(1, 1, 1, vector(1, 1)),
                                row(1, 3, 1, vector(1, 1)),
                                row(0, 1, 1, vector(1, 1)),
                                row(0, 3, 1, vector(1, 1)));
        assertEquals(8, snapshot.keysFetched);
        assertEquals(8, snapshot.partitionsFetched);
        assertEquals(2, snapshot.partitionsReturned);
        assertEquals(0, snapshot.partitionTombstonesFetched);
        assertEquals(8, snapshot.rowsFetched);
        assertEquals(8, snapshot.rowsReturned);
        assertEquals(0, snapshot.rowTombstonesFetched);

        // index filtering that accepts limited rows
        snapshot = queryContext("SELECT * FROM %s ORDER BY v ANN OF [0, 0] LIMIT 4",
                                row(1, 0, 0, vector(0, 0)),
                                row(1, 2, 0, vector(0, 0)),
                                row(0, 0, 0, vector(0, 0)),
                                row(0, 2, 0, vector(0, 0)));
        assertEquals(4, snapshot.keysFetched);
        assertEquals(4, snapshot.partitionsFetched);
        assertEquals(2, snapshot.partitionsReturned);
        assertEquals(0, snapshot.partitionTombstonesFetched);
        assertEquals(4, snapshot.rowsFetched);
        assertEquals(4, snapshot.rowsReturned);
        assertEquals(0, snapshot.rowTombstonesFetched);

        // index filtering that accepts some rows, different value
        snapshot = queryContext("SELECT * FROM %s WHERE n = 1 ORDER BY v ANN OF [0, 0] LIMIT 10 ALLOW FILTERING",
                                row(1, 1, 1, vector(1, 1)),
                                row(1, 3, 1, vector(1, 1)),
                                row(0, 1, 1, vector(1, 1)),
                                row(0, 3, 1, vector(1, 1)));
        assertEquals(8, snapshot.keysFetched);
        assertEquals(8, snapshot.partitionsFetched);
        assertEquals(2, snapshot.partitionsReturned);
        assertEquals(0, snapshot.partitionTombstonesFetched);
        assertEquals(8, snapshot.rowsFetched);
        assertEquals(4, snapshot.rowsReturned);
        assertEquals(0, snapshot.rowTombstonesFetched);

        // partition key query
        snapshot = queryContext("SELECT * FROM %s WHERE k = 1 ORDER BY v ANN OF [0, 0] LIMIT 10",
                                row(1, 0, 0, vector(0, 0)),
                                row(1, 2, 0, vector(0, 0)),
                                row(1, 1, 1, vector(1, 1)),
                                row(1, 3, 1, vector(1, 1)));
        assertEquals(4, snapshot.keysFetched);
        assertEquals(4, snapshot.partitionsFetched);
        assertEquals(1, snapshot.partitionsReturned);
        assertEquals(0, snapshot.partitionTombstonesFetched);
        assertEquals(4, snapshot.rowsFetched);
        assertEquals(4, snapshot.rowsReturned);
        assertEquals(0, snapshot.rowTombstonesFetched);

        // delete a row
        execute("DELETE FROM %s WHERE k = 0 AND c = 0");
        snapshot = queryContext("SELECT * FROM %s ORDER BY v ANN OF [0, 0] LIMIT 10",
                                row(1, 0, 0, vector(0, 0)),
                                row(1, 2, 0, vector(0, 0)),
                                row(0, 2, 0, vector(0, 0)),
                                row(1, 1, 1, vector(1, 1)),
                                row(1, 3, 1, vector(1, 1)),
                                row(0, 1, 1, vector(1, 1)),
                                row(0, 3, 1, vector(1, 1)));
        assertEquals(8, snapshot.keysFetched);
        assertEquals(8, snapshot.partitionsFetched);
        assertEquals(2, snapshot.partitionsReturned);
        assertEquals(0, snapshot.partitionTombstonesFetched);
        assertEquals(7, snapshot.rowsFetched);
        assertEquals(7, snapshot.rowsReturned);
        assertEquals(1, snapshot.rowTombstonesFetched);

        // delete a cell
        execute("DELETE v FROM %s WHERE k = 1 AND c = 0");
        snapshot = queryContext("SELECT * FROM %s ORDER BY v ANN OF [0, 0] LIMIT 10",
                                row(1, 2, 0, vector(0, 0)),
                                row(0, 2, 0, vector(0, 0)),
                                row(1, 1, 1, vector(1, 1)),
                                row(1, 3, 1, vector(1, 1)),
                                row(0, 1, 1, vector(1, 1)),
                                row(0, 3, 1, vector(1, 1)));
        assertEquals(8, snapshot.keysFetched);
        assertEquals(8, snapshot.partitionsFetched);
        assertEquals(2, snapshot.partitionsReturned);
        assertEquals(0, snapshot.partitionTombstonesFetched);
        assertEquals(7, snapshot.rowsFetched);
        assertEquals(6, snapshot.rowsReturned);
        assertEquals(1, snapshot.rowTombstonesFetched);

        // delete a partition
        execute("DELETE FROM %s WHERE k = 0");
        flush();
        snapshot = queryContext("SELECT * FROM %s ORDER BY v ANN OF [0, 0] LIMIT 10",
                                row(1, 2, 0, vector(0, 0)),
                                row(1, 1, 1, vector(1, 1)),
                                row(1, 3, 1, vector(1, 1)));
        assertEquals(8, snapshot.keysFetched);
        assertEquals(4, snapshot.partitionsFetched);
        assertEquals(1, snapshot.partitionsReturned);
        assertEquals(4, snapshot.partitionTombstonesFetched);
        assertEquals(4, snapshot.rowsFetched);
        assertEquals(3, snapshot.rowsReturned);
        assertEquals(0, snapshot.rowTombstonesFetched);

        // delete all the rows in a partition with a range tombstone
        execute("DELETE FROM %s WHERE k = 1 AND c >= 1");
        snapshot = queryContext("SELECT * FROM %s ORDER BY v ANN OF [0, 0] LIMIT 10");
        assertEquals(8, snapshot.keysFetched);
        assertEquals(4, snapshot.partitionsFetched);
        assertEquals(0, snapshot.partitionsReturned);
        assertEquals(4, snapshot.partitionTombstonesFetched);
        assertEquals(1, snapshot.rowsFetched);
        assertEquals(0, snapshot.rowsReturned);
        assertEquals(6, snapshot.rowTombstonesFetched); // 3 index entries with start/end bounds each

        // compact to rebuild the index, and verify that tombstones are gone
        flush();
        compact();
        snapshot = queryContext("SELECT * FROM %s ORDER BY v ANN OF [0, 0] LIMIT 10");
        assertEquals(0, snapshot.keysFetched);
        assertEquals(0, snapshot.partitionsFetched);
        assertEquals(0, snapshot.partitionsReturned);
        assertEquals(0, snapshot.partitionTombstonesFetched);
        assertEquals(0, snapshot.rowsFetched);
        assertEquals(0, snapshot.rowsReturned);
        assertEquals(0, snapshot.rowTombstonesFetched);

        // truncate the table
        truncate(false);
        snapshot = queryContext("SELECT * FROM %s ORDER BY v ANN OF [0, 0] LIMIT 10");
        assertEquals(0, snapshot.keysFetched);
        assertEquals(0, snapshot.partitionsFetched);
        assertEquals(0, snapshot.partitionsReturned);
        assertEquals(0, snapshot.partitionTombstonesFetched);
        assertEquals(0, snapshot.rowsFetched);
        assertEquals(0, snapshot.rowsReturned);
        assertEquals(0, snapshot.rowTombstonesFetched);

        // insert some data using TTLs
        execute("INSERT INTO %s (k, c, n, v) VALUES (0, 0, 0, [0, 0]) USING TTL 1");
        execute("INSERT INTO %s (k, c, n, v) VALUES (0, 1, 1, [1, 1])");
        execute("INSERT INTO %s (k, c, n, v) VALUES (0, 2, 0, [0, 0])");
        execute("INSERT INTO %s (k, c, n, v) VALUES (0, 3, 1, [1, 1])");
        execute("INSERT INTO %s (k, c, n, v) VALUES (1, 0, 0, [0, 0])");
        execute("INSERT INTO %s (k, c, n, v) VALUES (1, 1, 1, [1, 1]) USING TTL 1");
        execute("INSERT INTO %s (k, c, n, v) VALUES (1, 2, 0, [0, 0])");
        execute("INSERT INTO %s (k, c, n, v) VALUES (1, 3, 1, [1, 1])");
        flush();
        Uninterruptibles.sleepUninterruptibly(2, TimeUnit.SECONDS);
        snapshot = queryContext("SELECT * FROM %s ORDER BY v ANN OF [0, 0] LIMIT 10",
                                row(1, 0, 0, vector(0, 0)),
                                row(1, 2, 0, vector(0, 0)),
                                row(0, 2, 0, vector(0, 0)),
                                row(1, 3, 1, vector(1, 1)),
                                row(0, 1, 1, vector(1, 1)),
                                row(0, 3, 1, vector(1, 1)));
        assertEquals(8, snapshot.keysFetched);
        assertEquals(8, snapshot.partitionsFetched);
        assertEquals(2, snapshot.partitionsReturned);
        assertEquals(0, snapshot.partitionTombstonesFetched);
        assertEquals(6, snapshot.rowsFetched);
        assertEquals(6, snapshot.rowsReturned);
        assertEquals(2, snapshot.rowTombstonesFetched);
    }

    @Test
    public void testWideTableWithStatics()
    {
        createTable("CREATE TABLE %s (k int, c int, a int, b int, s int static, PRIMARY KEY(k, c))");
        createIndex("CREATE CUSTOM INDEX ON %s(a) USING 'StorageAttachedIndex'");
        execute("INSERT INTO %s (k, c, a, b, s) VALUES (0, 0, 0, 0, 0)");
        execute("INSERT INTO %s (k, c, a, b) VALUES (0, 1, 1, 1)");
        execute("INSERT INTO %s (k, c, a, b) VALUES (0, 2, 0, 0)");
        execute("INSERT INTO %s (k, c, a, b) VALUES (0, 3, 1, 1)");
        execute("INSERT INTO %s (k, c, a, b, s) VALUES (1, 0, 0, 0, 1)");
        execute("INSERT INTO %s (k, c, a, b) VALUES (1, 1, 1, 1)");
        execute("INSERT INTO %s (k, c, a, b) VALUES (1, 2, 0, 0)");
        execute("INSERT INTO %s (k, c, a, b) VALUES (1, 3, 1, 1)");
        flush();
        QueryContext.Snapshot snapshot;

        // index filtering that accepts all rows
        snapshot = queryContext("SELECT k,c,a,b,s FROM %s WHERE a >= 0",
                                row(0, 0, 0, 0, 0),
                                row(0, 1, 1, 1, 0),
                                row(0, 2, 0, 0, 0),
                                row(0, 3, 1, 1, 0),
                                row(1, 0, 0, 0, 1),
                                row(1, 1, 1, 1, 1),
                                row(1, 2, 0, 0, 1),
                                row(1, 3, 1, 1, 1));
        assertEquals(isRowAware() ? 8 : 2, snapshot.keysFetched);
        assertEquals(2, snapshot.partitionsFetched);
        assertEquals(2, snapshot.partitionsReturned);
        assertEquals(0, snapshot.partitionTombstonesFetched);
        assertEquals(8, snapshot.rowsFetched);
        assertEquals(8, snapshot.rowsReturned);
        assertEquals(0, snapshot.rowTombstonesFetched);

        // index filtering that accepts no rows
        snapshot = queryContext("SELECT k,c,a,b,s FROM %s WHERE a < 0");
        assertEquals(0, snapshot.keysFetched);
        assertEquals(0, snapshot.partitionsFetched);
        assertEquals(0, snapshot.partitionsReturned);
        assertEquals(0, snapshot.partitionTombstonesFetched);
        assertEquals(0, snapshot.rowsFetched);
        assertEquals(0, snapshot.rowsReturned);
        assertEquals(0, snapshot.rowTombstonesFetched);

        // index filtering that accepts some rows
        snapshot = queryContext("SELECT k,c,a,b,s FROM %s WHERE a = 0",
                                row(0, 0, 0, 0, 0),
                                row(0, 2, 0, 0, 0),
                                row(1, 0, 0, 0, 1),
                                row(1, 2, 0, 0, 1));
        assertEquals(isRowAware() ? 4 : 2, snapshot.keysFetched);
        assertEquals(2, snapshot.partitionsFetched);
        assertEquals(2, snapshot.partitionsReturned);
        assertEquals(0, snapshot.partitionTombstonesFetched);
        assertEquals(isRowAware() ? 4 : 8, snapshot.rowsFetched);
        assertEquals(4, snapshot.rowsReturned);
        assertEquals(0, snapshot.rowTombstonesFetched);

        // index filtering that accepts some, different value
        snapshot = queryContext("SELECT k,c,a,b,s FROM %s WHERE a = 1",
                                row(0, 1, 1, 1, 0),
                                row(0, 3, 1, 1, 0),
                                row(1, 1, 1, 1, 1),
                                row(1, 3, 1, 1, 1));
        assertEquals(isRowAware() ? 4 : 2, snapshot.keysFetched);
        assertEquals(2, snapshot.partitionsFetched);
        assertEquals(2, snapshot.partitionsReturned);
        assertEquals(0, snapshot.partitionTombstonesFetched);
        assertEquals(isRowAware() ? 4 : 8, snapshot.rowsFetched);
        assertEquals(4, snapshot.rowsReturned);
        assertEquals(0, snapshot.rowTombstonesFetched);

        // not-indexed column filtering that accepts all rows
        snapshot = queryContext("SELECT k,c,a,b,s FROM %s WHERE a = 0 AND b >= 0 ALLOW FILTERING",
                                row(0, 0, 0, 0, 0),
                                row(0, 2, 0, 0, 0),
                                row(1, 0, 0, 0, 1),
                                row(1, 2, 0, 0, 1));
        assertEquals(isRowAware() ? 4 : 2, snapshot.keysFetched);
        assertEquals(2, snapshot.partitionsFetched);
        assertEquals(2, snapshot.partitionsReturned);
        assertEquals(0, snapshot.partitionTombstonesFetched);
        assertEquals(isRowAware() ? 4 : 8, snapshot.rowsFetched);
        assertEquals(4, snapshot.rowsReturned);
        assertEquals(0, snapshot.rowTombstonesFetched);

        // not-indexed column filtering that accepts no rows
        snapshot = queryContext("SELECT k,c,a,b,s FROM %s WHERE a = 0 AND b < 0 ALLOW FILTERING");
        assertEquals(isRowAware() ? 4 : 2, snapshot.keysFetched);
        assertEquals(2, snapshot.partitionsFetched);
        assertEquals(0, snapshot.partitionsReturned);
        assertEquals(0, snapshot.partitionTombstonesFetched);
        assertEquals(isRowAware() ? 4 : 8, snapshot.rowsFetched);
        assertEquals(0, snapshot.rowsReturned);
        assertEquals(0, snapshot.rowTombstonesFetched);

        // not-indexed column filtering that accepts some rows
        snapshot = queryContext("SELECT k,c,a,b,s FROM %s WHERE a >= 0 AND b = 0 ALLOW FILTERING",
                                row(0, 0, 0, 0, 0),
                                row(0, 2, 0, 0, 0),
                                row(1, 0, 0, 0, 1),
                                row(1, 2, 0, 0, 1));
        assertEquals(isRowAware() ? 8 : 2, snapshot.keysFetched);
        assertEquals(2, snapshot.partitionsFetched);
        assertEquals(2, snapshot.partitionsReturned);
        assertEquals(0, snapshot.partitionTombstonesFetched);
        assertEquals(8, snapshot.rowsFetched);
        assertEquals(4, snapshot.rowsReturned);
        assertEquals(0, snapshot.rowTombstonesFetched);

        // partition key query
        snapshot = queryContext("SELECT k,c,a,b,s FROM %s WHERE a >= 0 AND k = 0 ALLOW FILTERING",
                                row(0, 0, 0, 0, 0),
                                row(0, 1, 1, 1, 0),
                                row(0, 2, 0, 0, 0),
                                row(0, 3, 1, 1, 0));
        assertEquals(isRowAware() ? 4 : 1, snapshot.keysFetched);
        assertEquals(1, snapshot.partitionsFetched);
        assertEquals(1, snapshot.partitionsReturned);
        assertEquals(0, snapshot.partitionTombstonesFetched);
        assertEquals(4, snapshot.rowsFetched);
        assertEquals(4, snapshot.rowsReturned);
        assertEquals(0, snapshot.rowTombstonesFetched);

        // primary key query
        snapshot = queryContext("SELECT k,c,a,b,s FROM %s WHERE a >= 0 AND k = 0 AND c IN (0, 2) ALLOW FILTERING",
                                row(0, 0, 0, 0, 0),
                                row(0, 2, 0, 0, 0));
        assertEquals(isRowAware() ? 2 : 1, snapshot.keysFetched);
        assertEquals(1, snapshot.partitionsFetched);
        assertEquals(1, snapshot.partitionsReturned);
        assertEquals(0, snapshot.partitionTombstonesFetched);
        assertEquals(2, snapshot.rowsFetched);
        assertEquals(2, snapshot.rowsReturned);
        assertEquals(0, snapshot.rowTombstonesFetched);

        // partition key filtering
        snapshot = queryContext("SELECT k,c,a,b,s FROM %s WHERE a >= 0 AND k != 1 ALLOW FILTERING",
                                row(0, 0, 0, 0, 0),
                                row(0, 1, 1, 1, 0),
                                row(0, 2, 0, 0, 0),
                                row(0, 3, 1, 1, 0));
        assertEquals(isRowAware() ? 8 : 2, snapshot.keysFetched);
        assertEquals(2, snapshot.partitionsFetched);
        assertEquals(1, snapshot.partitionsReturned);
        assertEquals(0, snapshot.partitionTombstonesFetched);
        assertEquals(8, snapshot.rowsFetched);
        assertEquals(4, snapshot.rowsReturned);
        assertEquals(0, snapshot.rowTombstonesFetched);

        // clustering key filtering
        snapshot = queryContext("SELECT k,c,a,b,s FROM %s WHERE a >= 0 AND c != 1 ALLOW FILTERING",
                                row(0, 0, 0, 0, 0),
                                row(0, 2, 0, 0, 0),
                                row(0, 3, 1, 1, 0),
                                row(1, 0, 0, 0, 1),
                                row(1, 2, 0, 0, 1),
                                row(1, 3, 1, 1, 1));
        assertEquals(isRowAware() ? 6 : 2, snapshot.keysFetched);
        assertEquals(2, snapshot.partitionsFetched);
        assertEquals(2, snapshot.partitionsReturned);
        assertEquals(0, snapshot.partitionTombstonesFetched);
        assertEquals(6, snapshot.rowsFetched);
        assertEquals(6, snapshot.rowsReturned);
        assertEquals(0, snapshot.rowTombstonesFetched);

        // static row filtering
        snapshot = queryContext("SELECT k,c,a,b,s FROM %s WHERE a >= 0 AND s != 1 ALLOW FILTERING",
                                row(0, 0, 0, 0, 0),
                                row(0, 1, 1, 1, 0),
                                row(0, 2, 0, 0, 0),
                                row(0, 3, 1, 1, 0));
        assertEquals(isRowAware() ? 8 : 2, snapshot.keysFetched);
        assertEquals(2, snapshot.partitionsFetched);
        assertEquals(1, snapshot.partitionsReturned);
        assertEquals(0, snapshot.partitionTombstonesFetched);
        assertEquals(8, snapshot.rowsFetched);
        assertEquals(4, snapshot.rowsReturned);
        assertEquals(0, snapshot.rowTombstonesFetched);

        // static row cell deletion
        execute("DELETE s FROM %s WHERE k = 1");
        snapshot = queryContext("SELECT k,c,a,b,s FROM %s WHERE a >= 0",
                                row(0, 0, 0, 0, 0),
                                row(0, 1, 1, 1, 0),
                                row(0, 2, 0, 0, 0),
                                row(0, 3, 1, 1, 0),
                                row(1, 0, 0, 0, null),
                                row(1, 1, 1, 1, null),
                                row(1, 2, 0, 0, null),
                                row(1, 3, 1, 1, null));
        assertEquals(isRowAware() ? 8 : 2, snapshot.keysFetched);
        assertEquals(2, snapshot.partitionsFetched);
        assertEquals(2, snapshot.partitionsReturned);
        assertEquals(0, snapshot.partitionTombstonesFetched);
        assertEquals(8, snapshot.rowsFetched);
        assertEquals(8, snapshot.rowsReturned);
        assertEquals(0, snapshot.rowTombstonesFetched);

        // static row cell filtering using the deleted value
        snapshot = queryContext("SELECT k,c,a,b,s FROM %s WHERE a >= 0 AND s = 1 ALLOW FILTERING");
        assertEquals(isRowAware() ? 8 : 2, snapshot.keysFetched);
        assertEquals(2, snapshot.partitionsFetched);
        assertEquals(0, snapshot.partitionsReturned);
        assertEquals(0, snapshot.partitionTombstonesFetched);
        assertEquals(8, snapshot.rowsFetched);
        assertEquals(0, snapshot.rowsReturned);
        assertEquals(0, snapshot.rowTombstonesFetched);

        // delete a row
        execute("DELETE FROM %s WHERE k = 0 AND c = 0");
        snapshot = queryContext("SELECT k,c,a,b,s FROM %s WHERE a >= 0",
                                row(0, 1, 1, 1, 0),
                                row(0, 2, 0, 0, 0),
                                row(0, 3, 1, 1, 0),
                                row(1, 0, 0, 0, null),
                                row(1, 1, 1, 1, null),
                                row(1, 2, 0, 0, null),
                                row(1, 3, 1, 1, null));
        assertEquals(isRowAware() ? 8 : 2, snapshot.keysFetched);
        assertEquals(2, snapshot.partitionsFetched);
        assertEquals(2, snapshot.partitionsReturned);
        assertEquals(0, snapshot.partitionTombstonesFetched);
        assertEquals(7, snapshot.rowsFetched);
        assertEquals(7, snapshot.rowsReturned);
        assertEquals(1, snapshot.rowTombstonesFetched);

        // delete a partition
        execute("DELETE FROM %s WHERE k = 0");
        snapshot = queryContext("SELECT k,c,a,b,s FROM %s WHERE a >= 0",
                                row(1, 0, 0, 0, null),
                                row(1, 1, 1, 1, null),
                                row(1, 2, 0, 0, null),
                                row(1, 3, 1, 1, null));
        assertEquals(isRowAware() ? 8 : 2, snapshot.keysFetched);
        assertEquals(1, snapshot.partitionsFetched);
        assertEquals(1, snapshot.partitionsReturned);
        assertEquals(1, snapshot.partitionTombstonesFetched);
        assertEquals(4, snapshot.rowsFetched);
        assertEquals(4, snapshot.rowsReturned);
        assertEquals(0, snapshot.rowTombstonesFetched);

        // delete all the rows in a partition with a range tombstone
        execute("DELETE FROM %s WHERE k = 1 AND c >= 0");
        snapshot = queryContext("SELECT k,c,a,b,s FROM %s WHERE a >= 0");
        assertEquals(isRowAware() ? 8 : 2, snapshot.keysFetched);
        assertEquals(1, snapshot.partitionsFetched);
        assertEquals(0, snapshot.partitionsReturned);
        assertEquals(1, snapshot.partitionTombstonesFetched);
        assertEquals(0, snapshot.rowsFetched);
        assertEquals(0, snapshot.rowsReturned);
        assertEquals(isRowAware() ? 8 : 2, snapshot.rowTombstonesFetched);

        // compact to rebuild the index, and verify that tombstones are gone
        flush();
        compact();
        snapshot = queryContext("SELECT k,c,a,b,s FROM %s WHERE a >= 0");
        assertEquals(0, snapshot.keysFetched);
        assertEquals(0, snapshot.partitionsFetched);
        assertEquals(0, snapshot.partitionsReturned);
        assertEquals(0, snapshot.partitionTombstonesFetched);
        assertEquals(0, snapshot.rowsFetched);
        assertEquals(0, snapshot.rowsReturned);
        assertEquals(0, snapshot.rowTombstonesFetched);

        // truncate the table
        truncate(false);
        snapshot = queryContext("SELECT * FROM %s WHERE a >= 0 ALLOW FILTERING");
        assertEquals(0, snapshot.keysFetched);
        assertEquals(0, snapshot.partitionsFetched);
        assertEquals(0, snapshot.partitionsReturned);
        assertEquals(0, snapshot.partitionTombstonesFetched);
        assertEquals(0, snapshot.rowsFetched);
        assertEquals(0, snapshot.rowsReturned);
        assertEquals(0, snapshot.rowTombstonesFetched);

        // insert some data using TTLs
        execute("INSERT INTO %s (k, c, a, b, s) VALUES (0, 0, 0, 0, 0) USING TTL 1");
        execute("INSERT INTO %s (k, c, a, b) VALUES (0, 1, 1, 1)");
        execute("INSERT INTO %s (k, c, a, b) VALUES (0, 2, 0, 0)");
        execute("INSERT INTO %s (k, c, a, b) VALUES (0, 3, 1, 1)");
        execute("INSERT INTO %s (k, c, a, b, s) VALUES (1, 0, 0, 0, 1)");
        execute("INSERT INTO %s (k, c, a, b) VALUES (1, 1, 1, 1)");
        execute("INSERT INTO %s (k, c, a, b) VALUES (1, 2, 0, 0) USING TTL 1");
        execute("INSERT INTO %s (k, c, a, b) VALUES (1, 3, 1, 1)");
        flush();
        Uninterruptibles.sleepUninterruptibly(2, TimeUnit.SECONDS);
        snapshot = queryContext("SELECT k,c,a,b,s FROM %s WHERE a >= 0",
                                row(0, 1, 1, 1, null),
                                row(0, 2, 0, 0, null),
                                row(0, 3, 1, 1, null),
                                row(1, 0, 0, 0, 1),
                                row(1, 1, 1, 1, 1),
                                row(1, 3, 1, 1, 1));
        assertEquals(isRowAware() ? 8 : 2, snapshot.keysFetched);
        assertEquals(2, snapshot.partitionsFetched);
        assertEquals(2, snapshot.partitionsReturned);
        assertEquals(0, snapshot.partitionTombstonesFetched);
        assertEquals(6, snapshot.rowsFetched);
        assertEquals(6, snapshot.rowsReturned);
        assertEquals(2, snapshot.rowTombstonesFetched);
    }

    @Test
    public void testCollections()
    {
        createTable("CREATE TABLE %s (k int, c int, l list<int>, s set<int>, m map<int, int>,  PRIMARY KEY(k, c))");
        createIndex("CREATE CUSTOM INDEX ON %s(l) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(s) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(keys(m)) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(values(m)) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(entries(m)) USING 'StorageAttachedIndex'");
        execute("INSERT INTO %s (k, c, l, s, m) VALUES (1, 1, [1, 2, 3], {1, 2, 3}, {1:10, 2:20, 3:30})");
        execute("INSERT INTO %s (k, c, l, s, m) VALUES (1, 2, [2, 3, 4], {2, 3, 4}, {2:20, 3:30, 4:40})");
        execute("INSERT INTO %s (k, c, l, s, m) VALUES (2, 1, [3, 4, 5], {3, 4, 5}, {3:30, 4:40, 5:50})");
        execute("INSERT INTO %s (k, c, l, s, m) VALUES (2, 2, [5 ,6, 7], {5 ,6, 7}, {5:50, 6:60, 7:70})");
        flush();
        QueryContext.Snapshot snapshot;

        List<String> queries = Arrays.asList("SELECT k, c FROM %s WHERE l CONTAINS 3",
                                             "SELECT k, c FROM %s WHERE s CONTAINS 3",
                                             "SELECT k, c FROM %s WHERE m CONTAINS KEY 3",
                                             "SELECT k, c FROM %s WHERE m CONTAINS 30",
                                             "SELECT k, c FROM %s WHERE m[3] = 30");

        for (String query : queries)
        {
            snapshot = queryContext(query, row(1, 1), row(1, 2), row(2, 1));
            assertEquals(isRowAware() ? 3 : 2, snapshot.keysFetched);
            assertEquals(2, snapshot.partitionsFetched);
            assertEquals(2, snapshot.partitionsReturned);
            assertEquals(0, snapshot.partitionTombstonesFetched);
            assertEquals(isRowAware() ? 3 : 4, snapshot.rowsFetched);
            assertEquals(3, snapshot.rowsReturned);
            assertEquals(0, snapshot.rowTombstonesFetched);
        }

        // delete a cell
        execute("UPDATE %s SET l = l - [3], s = s - {3}, m = m - {3} WHERE k = 1 AND c = 1");
        for (String query : queries)
        {
            snapshot = queryContext(query, row(1, 2), row(2, 1));
            assertEquals(isRowAware() ? 3 : 2, snapshot.keysFetched);
            assertEquals(2, snapshot.partitionsFetched);
            assertEquals(2, snapshot.partitionsReturned);
            assertEquals(0, snapshot.partitionTombstonesFetched);
            assertEquals(isRowAware() ? 3 : 4, snapshot.rowsFetched);
            assertEquals(2, snapshot.rowsReturned);
            assertEquals(0, snapshot.rowTombstonesFetched);
        }

        // delete a row
        execute("DELETE FROM %s WHERE k = 1 AND c = 2");
        for (String query : queries)
        {
            snapshot = queryContext(query, row(2, 1));
            assertEquals(isRowAware() ? 3 : 2, snapshot.keysFetched);
            assertEquals(2, snapshot.partitionsFetched);
            assertEquals(1, snapshot.partitionsReturned);
            assertEquals(0, snapshot.partitionTombstonesFetched);
            assertEquals(isRowAware() ? 2 : 3, snapshot.rowsFetched);
            assertEquals(1, snapshot.rowsReturned);
            assertEquals(1, snapshot.rowTombstonesFetched);
        }

        // delete a partition
        execute("DELETE FROM %s WHERE k = 2");
        for (String query : queries)
        {
            snapshot = queryContext(query);
            assertEquals(isRowAware() ? 3 : 2, snapshot.keysFetched);
            assertEquals(1, snapshot.partitionsFetched);
            assertEquals(0, snapshot.partitionsReturned);
            assertEquals(1, snapshot.partitionTombstonesFetched);
            assertEquals(1, snapshot.rowsFetched);
            assertEquals(0, snapshot.rowsReturned);
            assertEquals(1, snapshot.rowTombstonesFetched);
        }
    }

    private boolean isRowAware()
    {
        return version.after(Version.AA);
    }

    private QueryContext.Snapshot queryContext(String query, Object[]... rows)
    {
        // First execute the query with the normal test path to validate the results
        assertRowsIgnoringOrder(execute(query), rows);

        // Get an index searcher for the query
        PartitionRangeReadCommand command = (PartitionRangeReadCommand) parseReadCommand(query);
        StorageAttachedIndexQueryPlan plan = (StorageAttachedIndexQueryPlan) command.indexQueryPlan();
        Assert.assertNotNull(plan);
        StorageAttachedIndexSearcher searcher = plan.searcherFor(command);

        // Execute the search for the query and consume the results to popupate the query context
        try (ReadExecutionController executionController = command.executionController();
             UnfilteredPartitionIterator partitions = searcher.search(executionController))
        {
            while (partitions.hasNext())
            {
                try (UnfilteredRowIterator partition = partitions.next())
                {
                    while (partition.hasNext())
                    {
                        partition.next();
                    }
                }
            }
        }

        // Return the query context snapshot, which should be populated
        return searcher.queryContext().snapshot();
    }
}
