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

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.disk.vector.VectorMemtableIndex;
import org.apache.cassandra.index.sai.plan.QueryController;

import static org.apache.cassandra.index.sai.cql.VectorTypeTest.assertContainsInt;
import static org.apache.cassandra.index.sai.disk.vector.CassandraOnHeapGraph.MIN_PQ_ROWS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

public class VectorUpdateDeleteTest extends VectorTester.VersionedWithChecksums
{
    @Before
    public void setup() throws Throwable
    {
        super.setup();

        // Enable the optimizer by default. If there are any tests that need to disable it, they can do so explicitly.
        QueryController.QUERY_OPT_LEVEL = 1;
    }

    @Test
    public void testVectorRowWhereUpdateMakesRowMatchNonOrderingPredicates()
    {
        createTable(KEYSPACE, "CREATE TABLE %s (pk int, val text, vec vector<float, 2>, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(vec) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");

        // Split the row across 1 sstable and the memtable.
        execute("INSERT INTO %s (pk, vec) VALUES (1, [1,1])");
        flush();
        execute("INSERT INTO %s (pk, val) VALUES (1, 'match me')");

        assertRows(execute("SELECT pk FROM %s WHERE val = 'match me' ORDER BY vec ANN OF [1,1] LIMIT 2"), row(1));
        // Push memtable to sstable. we should get same result
        flush();
        assertRows(execute("SELECT pk FROM %s WHERE val = 'match me' ORDER BY vec ANN OF [1,1] LIMIT 2"), row(1));

        // Run the test again but instead inserting a full row and then overwrite val to match the predicate.
        // This covers a different case because when there is no data for a column, it doesn't get an index file.

        execute("INSERT INTO %s (pk, val, vec) VALUES (1, 'no match', [1,1])");
        flush();
        execute("INSERT INTO %s (pk, val) VALUES (1, 'match me')");

        assertRows(execute("SELECT pk FROM %s WHERE val = 'match me' ORDER BY vec ANN OF [1,1] LIMIT 2"), row(1));
        // Push memtable to sstable. we should get same result
        flush();
        assertRows(execute("SELECT pk FROM %s WHERE val = 'match me' ORDER BY vec ANN OF [1,1] LIMIT 2"), row(1));
    }

}
