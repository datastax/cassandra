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

package org.apache.cassandra.db.monitoring;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.index.sai.SAITester;

import static org.junit.Assert.assertEquals;

public class SaiSlowLogTest extends SAITester
{
    @Before
    public void setup() throws Throwable
    {
        requireNetwork();

        createTable(CREATE_TABLE_TEMPLATE);
        createIndex(String.format(CREATE_INDEX_TEMPLATE, "v1"));
        createIndex(String.format(CREATE_INDEX_TEMPLATE, "v2"));

        if (!execute("SELECT * FROM %s").isEmpty())
        {
            return;
        }

        for (int i = 0; i < 100; ++i)
        {
            execute("INSERT INTO %s(id1,v1,v2) VALUES (?, ?, ?)", i, i, Integer.toString(i % 5));
        }
        flush();

        execute("SELECT * FROM %s WHERE v1 >= 0 AND v1 < 10000");
        execute("SELECT * FROM %s WHERE v2 = '0'");

        SaiSlowLog.instance = new SaiSlowLog();
    }

    @After
    public void cleanUp()
    {
        SaiSlowLog.instance.retrieveRecentSlowestCqlQueries();
    }

    // very basic test to verify that the slowest queries are returned
    // Need to add more tests for:
    // verification of the log format - toString()
    // logLongRunningSelect
    // logLongRunningSelectWithTracingEnabled
    // verify slowest queries are returned, correctly ordered
    // test things are handled right when we try invalid num slowest queries
    // verify when we decrease/increase the number of slowest queries the results
    // overall test all edge cases for all new configurations work as expected

    @Test
    public void verifySlowestQueriesAreReturned() throws Throwable
    {
        performQueries(10);
        // KATE - this is a bit of a hack to ensure the slowest queries are processed; We should use something more
        // robust than Thread.sleep; some polling mechanism maybe
        Thread.sleep(2000);

        assertEquals(5, SaiSlowLog.instance.retrieveRecentSlowestCqlQueries().size());
    }

    private void performQueries(int queriesToWrite)
    {
        // we need to reset the most recent slowest queries
        SaiSlowLog.instance.retrieveRecentSlowestCqlQueries();

        for (int i = 0; i < queriesToWrite; i++)
            executeNet("SELECT * FROM %s WHERE v1 >= 0 AND v1 < 10000");
    }
}
