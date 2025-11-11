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

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.index.sai.plan.StorageAttachedIndexQueryPlan;
import org.apache.cassandra.index.sai.plan.StorageAttachedIndexSearcher;

import static org.junit.Assert.assertEquals;

public class QueryContextTest extends SAITester
{
    @Test
    public void testRowsPostFiltered()
    {
        createTable("CREATE TABLE %s (k int, c int, a int, b int, PRIMARY KEY(k, c))");
        createIndex("CREATE CUSTOM INDEX ON %s(a) USING 'StorageAttachedIndex'");

        execute("INSERT INTO %s (k, c, a, b) VALUES (0, 0, 0, 0)");
        execute("INSERT INTO %s (k, c, a, b) VALUES (0, 1, 0, 1)");
        execute("INSERT INTO %s (k, c, a, b) VALUES (1, 0, 0, 0)");
        execute("INSERT INTO %s (k, c, a, b) VALUES (1, 1, 0, 1)");

        QueryContext.Snapshot snapshot = snapshot("SELECT * FROM %s WHERE a = 0 AND b = 0 ALLOW FILTERING",
                                                  row(0, 0, 0, 0),
                                                  row(1, 0, 0, 0));

        assertEquals(2, snapshot.partitionsRead);
        assertEquals(4, snapshot.rowsFiltered); // should be 2?
        assertEquals(0, snapshot.rowsPreFiltered); // should be 4?
        assertEquals(0, snapshot.shadowedPrimaryKeyCount); // should be 2?
    }

    @Test
    public void testRowsDeleted()
    {
        createTable("CREATE TABLE %s (k int, c int, a int, b int, PRIMARY KEY(k, c))");
        createIndex("CREATE CUSTOM INDEX ON %s(a) USING 'StorageAttachedIndex'");

        // insert some data
        execute("INSERT INTO %s (k, c, a, b) VALUES (0, 0, 0, 0)");
        execute("INSERT INTO %s (k, c, a, b) VALUES (0, 1, 0, 1)");
        execute("INSERT INTO %s (k, c, a, b) VALUES (1, 0, 0, 0)");
        execute("INSERT INTO %s (k, c, a, b) VALUES (1, 1, 0, 1)");
        flush();
        execute("DELETE FROM %s WHERE k = 0 AND c = 0");
        execute("DELETE FROM %s WHERE k = 1 AND c = 0");

        QueryContext.Snapshot snapshot = snapshot("SELECT * FROM %s WHERE a = 0",
                                                  row(0, 1, 0, 1),
                                                  row(1, 1, 0, 1));

        assertEquals(2, snapshot.partitionsRead);
        assertEquals(4, snapshot.rowsFiltered); // should be 2?
        assertEquals(0, snapshot.rowsPreFiltered); // should be 4?
        assertEquals(0, snapshot.shadowedPrimaryKeyCount); // should be 2?
    }

    @Test
    public void testPartitionsPostFiltered()
    {
        createTable("CREATE TABLE %s (k int, c int, a int, b int, PRIMARY KEY(k, c))");
        createIndex("CREATE CUSTOM INDEX ON %s(a) USING 'StorageAttachedIndex'");

        execute("INSERT INTO %s (k, c, a, b) VALUES (0, 0, 0, 0)");
        execute("INSERT INTO %s (k, c, a, b) VALUES (0, 1, 0, 1)");
        execute("INSERT INTO %s (k, c, a, b) VALUES (1, 0, 0, 0)");
        execute("INSERT INTO %s (k, c, a, b) VALUES (1, 1, 0, 1)");

        QueryContext.Snapshot snapshot = snapshot("SELECT * FROM %s WHERE a = 0 AND k != 0 ALLOW FILTERING",
                                                  row(1, 0, 0, 0),
                                                  row(1, 1, 0, 1));

        assertEquals(2, snapshot.partitionsRead); // should be 1?
        assertEquals(5, snapshot.rowsFiltered); // should be 2?
        assertEquals(0, snapshot.rowsPreFiltered); // should be 4?
        assertEquals(1, snapshot.shadowedPrimaryKeyCount); // should be 2?
    }

    @Test
    public void testPartitionsDeleted()
    {
        createTable("CREATE TABLE %s (k int, c int, a int, b int, PRIMARY KEY(k, c))");
        createIndex("CREATE CUSTOM INDEX ON %s(a) USING 'StorageAttachedIndex'");

        execute("INSERT INTO %s (k, c, a, b) VALUES (0, 0, 0, 0)");
        execute("INSERT INTO %s (k, c, a, b) VALUES (0, 1, 0, 1)");
        execute("INSERT INTO %s (k, c, a, b) VALUES (1, 0, 0, 0)");
        execute("INSERT INTO %s (k, c, a, b) VALUES (1, 1, 0, 1)");
        flush();
        execute("DELETE FROM %s WHERE k = 0");

        QueryContext.Snapshot snapshot = snapshot("SELECT * FROM %s WHERE a = 0",
                                                  row(1, 0, 0, 0),
                                                  row(1, 1, 0, 1));

        assertEquals(2, snapshot.partitionsRead); // should be 1?
        assertEquals(3, snapshot.rowsFiltered); // should be 2?
        assertEquals(0, snapshot.rowsPreFiltered); // should be 4?
        assertEquals(1, snapshot.shadowedPrimaryKeyCount); // should be 2?
    }

    private QueryContext.Snapshot snapshot(String query, Object[]... rows)
    {
        // First execute the query with the normal test path to validate the results
        assertRowsIgnoringOrder(execute(query), rows);

        // Get an index searcher for the query
        PartitionRangeReadCommand command = (PartitionRangeReadCommand) parseReadCommand(query);
        StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(getCurrentColumnFamilyStore());
        Assert.assertNotNull(group);
        StorageAttachedIndexQueryPlan plan = group.queryPlanFor(command.rowFilter());
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
