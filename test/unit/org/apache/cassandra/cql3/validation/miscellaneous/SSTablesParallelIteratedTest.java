/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.cql3.validation.miscellaneous;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.metrics.ClearableHistogram;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for checking how many sstables we access during cql queries.
 */
public class SSTablesParallelIteratedTest extends CQLTester
{
    @BeforeClass
    public static void setUp()
    {
        // compared to SSTablesIteratedTest where parallel read is disabled,
        // this may result in more sstables read with numbers varying.
        System.setProperty(CassandraRelevantProperties.USE_PARALLEL_SSTABLES_READ.getKey(), "true");
    }

    @AfterClass
    public static void tearDown()
    {
        System.clearProperty(CassandraRelevantProperties.USE_PARALLEL_SSTABLES_READ.getKey());
    }

    private void executeAndCheck(String query, int numSSTables, Object[]... rows) throws Throwable
    {
        executeAndCheck(query, numSSTables, numSSTables, rows);
    }

    private void executeAndCheck(String query, int minNumSSTables, int maxNumSSTables, Object[]... rows) throws Throwable
    {
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore(KEYSPACE_PER_TEST);

        ((ClearableHistogram) cfs.metric.sstablesPerReadHistogram.tableOrKeyspaceHistogram()).clear(); // resets counts

        assertRows(execute(query), rows);

        long numSSTablesIterated = cfs.metric.sstablesPerReadHistogram.tableOrKeyspaceHistogram().getSnapshot().getMax(); // max sstables read
        assertTrue(String.format("Expected at least %d sstables iterated but got %d instead, with %d live sstables",
                                  minNumSSTables, numSSTablesIterated, cfs.getLiveSSTables().size()),
                   numSSTablesIterated >= minNumSSTables);
        assertTrue(String.format("Expected at most %d sstables iterated but got %d instead, with %d live sstables",
                                  maxNumSSTables, numSSTablesIterated, cfs.getLiveSSTables().size()),
                   numSSTablesIterated <= maxNumSSTables);
    }

    @Override
    public String createTable(String query)
    {
        String ret = super.createTable(KEYSPACE_PER_TEST, query);
        disableCompaction(KEYSPACE_PER_TEST);
        return ret;
    }

    @Override
    public UntypedResultSet execute(String query, Object... values)
    {
        return executeFormattedQuery(formatQuery(KEYSPACE_PER_TEST, query), values);
    }

    @Override
    public void flush()
    {
        super.flush(KEYSPACE_PER_TEST);
    }

    @Test
    public void testCompactAndNonCompactTableWithPartitionTombstones() throws Throwable
    {
        for (Boolean compact  : new Boolean[] {Boolean.FALSE, Boolean.TRUE})
        {
            String with = compact ? " WITH COMPACT STORAGE" : "";
            createTable("CREATE TABLE %s (pk int PRIMARY KEY, v1 int, v2 int)" + with);

            execute("INSERT INTO %s (pk, v1, v2) VALUES (?, ?, ?) USING TIMESTAMP 1000", 1, 1, 1);
            execute("INSERT INTO %s (pk, v1, v2) VALUES (?, ?, ?) USING TIMESTAMP 1001", 2, 1, 1);
            execute("INSERT INTO %s (pk, v1, v2) VALUES (?, ?, ?) USING TIMESTAMP 1002", 3, 1, 1);
            execute("INSERT INTO %s (pk, v1, v2) VALUES (?, ?, ?) USING TIMESTAMP 1003", 4, 1, 1);
            flush();
            execute("INSERT INTO %s (pk, v1) VALUES (?, ?) USING TIMESTAMP 2000", 1, 2);
            execute("DELETE FROM %s USING TIMESTAMP 2001 WHERE pk = ?", 2);
            execute("UPDATE %s USING TIMESTAMP 2002 SET v1 = ? WHERE pk = ?", 2, 3);
            execute("DELETE FROM %s USING TIMESTAMP 2003 WHERE pk = ?", 4);
            flush();
            execute("DELETE FROM %s USING TIMESTAMP 3000 WHERE pk = ?", 1);
            execute("INSERT INTO %s (pk, v1) VALUES (?, ?) USING TIMESTAMP 3001", 2, 3);
            execute("DELETE FROM %s USING TIMESTAMP 3002 WHERE pk = ?", 3);
            execute("UPDATE %s USING TIMESTAMP 3003 SET v1 = ? WHERE pk = ?", 3, 4);
            flush();

            executeAndCheck("SELECT * FROM %s WHERE pk = 1", 1, 3);
            executeAndCheck("SELECT pk, v1 FROM %s WHERE pk = 1", 1, 3);
            executeAndCheck("SELECT v1, v2 FROM %s WHERE pk = 1", 1, 3);

            executeAndCheck("SELECT * FROM %s WHERE pk = 2", 2, 3, row(2, 3, null));
            executeAndCheck("SELECT v1, v2 FROM %s WHERE pk = 2", 2, 3, row(3, null));
            executeAndCheck("SELECT v2 FROM %s WHERE pk = 2", 2, 3, row((Integer) null));

            executeAndCheck("SELECT * FROM %s WHERE pk = 3", 1, 3);
            executeAndCheck("SELECT pk, v1 FROM %s WHERE pk = 3", 1, 3);
            executeAndCheck("SELECT v1, v2 FROM %s WHERE pk = 3", 1, 3);

            executeAndCheck("SELECT * FROM %s WHERE pk = 4", 2, 3, row(4, 3, null));
            executeAndCheck("SELECT v1, v2 FROM %s WHERE pk = 4", 2, 3, row(3, null));
            executeAndCheck("SELECT v2 FROM %s WHERE pk = 4", 2, 3, row((Integer) null));

            if (compact)
            {
                execute("ALTER TABLE %s DROP COMPACT STORAGE");
                executeAndCheck("SELECT * FROM %s WHERE pk = 1", 1, 3);
                executeAndCheck("SELECT pk, v1 FROM %s WHERE pk = 1", 1, 3);
                executeAndCheck("SELECT v1, v2 FROM %s WHERE pk = 1", 1, 3);

                assertColumnNames(execute("SELECT * FROM %s WHERE pk = 1"), "pk", "column1", "v1", "v2", "value");
                executeAndCheck("SELECT * FROM %s WHERE pk = 2", 2, 3, row(2, null, 3, null, null));
                executeAndCheck("SELECT v1, v2 FROM %s WHERE pk = 2", 2, 3, row(3, null));
                executeAndCheck("SELECT v2 FROM %s WHERE pk = 2", 2, 3, row((Integer) null));

                executeAndCheck("SELECT * FROM %s WHERE pk = 3", 1, 3);
                executeAndCheck("SELECT pk, v1 FROM %s WHERE pk = 3", 1, 3);
                executeAndCheck("SELECT v1, v2 FROM %s WHERE pk = 3", 1, 3);

                executeAndCheck("SELECT * FROM %s WHERE pk = 4", 2, 3, row(4, null, 3, null, null));
                executeAndCheck("SELECT v1, v2 FROM %s WHERE pk = 4", 2, 3, row(3, null));
                executeAndCheck("SELECT v2 FROM %s WHERE pk = 4", 2, 3, row((Integer) null));
            }
        }
    }

    @Test
    public void testNonCompactTableWithStaticColumnAndPartitionTombstones() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, c int, s int static, v int, PRIMARY KEY(pk, c))");

        execute("INSERT INTO %s (pk, c, s, v) VALUES (?, ?, ?, ?) USING TIMESTAMP 1000", 1, 1, 1, 1);
        execute("INSERT INTO %s (pk, c, s, v) VALUES (?, ?, ?, ?) USING TIMESTAMP 1001", 2, 1, 1, 1);
        execute("INSERT INTO %s (pk, c, s, v) VALUES (?, ?, ?, ?) USING TIMESTAMP 1002", 3, 1, 1, 1);
        execute("INSERT INTO %s (pk, c, s, v) VALUES (?, ?, ?, ?) USING TIMESTAMP 1003", 4, 1, 1, 1);
        flush();
        execute("INSERT INTO %s (pk, c, s) VALUES (?, ?, ?) USING TIMESTAMP 2000", 1, 1, 2);
        execute("DELETE FROM %s USING TIMESTAMP 2001 WHERE pk = ?", 2);
        execute("UPDATE %s USING TIMESTAMP 2002 SET s = ? WHERE pk = ?", 2, 3);
        execute("DELETE FROM %s USING TIMESTAMP 2003 WHERE pk = ?", 4);
        flush();
        execute("DELETE FROM %s USING TIMESTAMP 3000 WHERE pk = ?", 1);
        execute("INSERT INTO %s (pk, c, s) VALUES (?, ?, ?) USING TIMESTAMP 3001", 2, 1, 3);
        execute("DELETE FROM %s USING TIMESTAMP 3002 WHERE pk = ?", 3);
        execute("UPDATE %s USING TIMESTAMP 3003 SET s = ? WHERE pk = ?", 3, 4);
        flush();

        executeAndCheck("SELECT * FROM %s WHERE pk = 1 AND c = 1", 1, 3);
        executeAndCheck("SELECT s, v FROM %s WHERE pk = 1 AND c = 1", 1, 3);
        executeAndCheck("SELECT DISTINCT s FROM %s WHERE pk = 1", 1, 3);
        executeAndCheck("SELECT v FROM %s WHERE pk = 1 AND c = 1", 1, 3);

        executeAndCheck("SELECT * FROM %s WHERE pk = 2 AND c = 1", 2, 3, row(2, 1, 3, null));
        executeAndCheck("SELECT s, v FROM %s WHERE pk = 2 AND c = 1", 2, 3, row(3, null));
        executeAndCheck("SELECT DISTINCT s FROM %s WHERE pk = 2", 2, 3, row(3));
        executeAndCheck("SELECT v FROM %s WHERE pk = 2 AND c = 1", 2, 3, row((Integer) null));

        executeAndCheck("SELECT * FROM %s WHERE pk = 3 AND c = 1", 1, 3);
        executeAndCheck("SELECT s, v FROM %s WHERE pk = 3 AND c = 1", 1, 3);
        executeAndCheck("SELECT DISTINCT s FROM %s WHERE pk = 3", 1, 3);
        executeAndCheck("SELECT v FROM %s WHERE pk = 3 AND c = 1", 1, 3);

        executeAndCheck("SELECT * FROM %s WHERE pk = 4 AND c = 1", 2, 3);
        executeAndCheck("SELECT s, v FROM %s WHERE pk = 4 AND c = 1", 2, 3);
        executeAndCheck("SELECT DISTINCT s FROM %s WHERE pk = 4", 2, 3, row(3));
        executeAndCheck("SELECT v FROM %s WHERE pk = 4 AND c = 1", 2, 3);
    }
}
