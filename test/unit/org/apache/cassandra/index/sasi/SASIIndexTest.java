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

package org.apache.cassandra.index.sasi;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.restrictions.StatementRestrictions;

import static java.lang.String.format;
import static org.junit.Assert.assertFalse;

public class SASIIndexTest extends CQLTester
{
    @Test
    public void testDummyImplementation() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");

        // put some data in sstables and memtables
        execute("INSERT INTO %s (k, v) VALUES (1, 1)");
        flush();
        execute("INSERT INTO %s (k, v) VALUES (2, 2)");

        // create an index verifying that it warns about being unsupported
        String indexName = "sasi_idx";
        String createIndex = format("CREATE CUSTOM INDEX %s ON %%s(v) USING '%s'", indexName, SASIIndex.class.getName());
        String unsupportedMessage = SASIIndex.getUnsupportedMessage(KEYSPACE, indexName);
        assertClientWarning(unsupportedMessage, createIndex);

        // check that the index still allows to write and flush
        execute("INSERT INTO %s (k, v) VALUES (3, 3)");
        flush();

        // check that the index is not selected for any operation, but we can still use allow filtering
        String errorMessage = format(StatementRestrictions.HAS_UNSUPPORTED_SASI_INDEX_RESTRICTION_MESSAGE, 'v');
        String[] operators = new String[]{ "=", ">", "<", ">=", "<=" };
        for (String operator : operators)
        {
            String select = format("SELECT * FROM %%s WHERE v %s 2", operator);
            assertInvalidMessage(errorMessage, select);
            assertFalse(execute(select + " ALLOW FILTERING").isEmpty());
        }

        // drop the index and verify that we can still write, flush and query
        dropIndex("DROP INDEX %s." + indexName);
        execute("INSERT INTO %s (k, v) VALUES (4, 4)");
        flush();
        for (String operator : operators)
        {
            String select = format("SELECT * FROM %%s WHERE v %s 2", operator);
            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE, select);
            assertFalse(execute(select + " ALLOW FILTERING").isEmpty());
        }
    }
}
