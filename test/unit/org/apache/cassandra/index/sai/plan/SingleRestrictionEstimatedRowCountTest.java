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

package org.apache.cassandra.index.sai.plan;

import java.math.BigDecimal;
import java.math.BigInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.SAIUtil;
import org.apache.cassandra.index.sai.disk.format.Version;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class SingleRestrictionEstimatedRowCountTest extends SAITester
{
    private int queryOptLevel;

    @Before
    public void setup()
    {
        queryOptLevel = QueryController.QUERY_OPT_LEVEL;
        QueryController.QUERY_OPT_LEVEL = 0;
    }

    @After
    public void teardown()
    {
        QueryController.QUERY_OPT_LEVEL = queryOptLevel;
    }

    static protected Object getFilterValue(String type, int value)
    {
        switch (type)
        {
            case "int":
                return value;
            case "decimal":
                return BigDecimal.valueOf(value);
            case "varint":
                return BigInteger.valueOf(value);
        }
        fail("Must be known type");
        return null;
    }

    protected ColumnFamilyStore prepareTable(String type)
    {
        createTable("CREATE TABLE %s (pk text PRIMARY KEY, age " + type + ')');
        createIndex("CREATE CUSTOM INDEX ON %s(age) USING 'StorageAttachedIndex'");
        for (int i = 0; i < 100; i++)
        {
            execute("INSERT INTO %s (pk, age) VALUES (?," + i + ')', "key" + i);
        }
        return getCurrentColumnFamilyStore();
    }

    class RowCountTest
    {
        final Operator op;
        final int filterValue;

        RowCountTest(Operator op, int filterValue)
        {
            this.op = op;
            this.filterValue = filterValue;
        }

        void doTest(Version version, String type, double expectedRows)
        {
            Version latest = Version.latest();
            SAIUtil.setLatestVersion(version);

            ColumnFamilyStore cfs = prepareTable(type);
            Object filter = getFilterValue(type, filterValue);

            ReadCommand rc = Util.cmd(cfs)
                                 .columns("age")
                                 .filterOn("age", op, filter)
                                 .build();
            QueryController controller = new QueryController(cfs,
                                                             rc,
                                                             version.onDiskFormat().indexFeatureSet(),
                                                             new QueryContext(),
                                                             null);
            long totalRows = controller.planFactory.tableMetrics.rows;
            assertEquals(97, totalRows);

            Plan plan = controller.buildPlan();
            assert plan instanceof Plan.RowsIteration;
            Plan.RowsIteration root = (Plan.RowsIteration) plan;
            Plan.KeysIteration planNode = root.firstNodeOfType(Plan.KeysIteration.class);
            assertNotNull(planNode);

            assertEquals(expectedRows, root.expectedRows(), 0.1);
            assertEquals(expectedRows, planNode.expectedKeys(), 0.1);
            assertEquals(expectedRows / totalRows, planNode.selectivity(), 0.001);

            SAIUtil.setLatestVersion(latest);
        }
    }

    @Test
    public void testInequality()
    {
        var test = new RowCountTest(Operator.NEQ, 25);
        test.doTest(Version.DB, "int", 97.0);
        test.doTest(Version.EB, "int", 97.0);
        // Truncated numeric types planned differently
        test.doTest(Version.DB, "decimal", 97.0);
        test.doTest(Version.EB, "decimal", 97.0);
        test.doTest(Version.EB, "varint", 97.0);
    }

    @Test
    public void testHalfRangeMiddle()
    {
        var test = new RowCountTest(Operator.LT, 50);
        test.doTest(Version.DB, "int", 48);
        test.doTest(Version.EB, "int", 48);
        test.doTest(Version.DB, "decimal", 48);
        test.doTest(Version.EB, "decimal", 48);
    }

    @Test
    public void testHalfRangeEverything()
    {
        var test = new RowCountTest(Operator.LT, 150);
        test.doTest(Version.DB, "int", 97);
        test.doTest(Version.EB, "int", 97);
        test.doTest(Version.DB, "decimal", 97);
        test.doTest(Version.EB, "decimal", 97);
    }

    @Test
    public void testEquality()
    {
        var test = new RowCountTest(Operator.EQ, 31);
        test.doTest(Version.DB, "int", 15);
        test.doTest(Version.EB, "int", 0);
        test.doTest(Version.DB, "decimal", 15);
        test.doTest(Version.EB, "decimal", 0);
    }
}
