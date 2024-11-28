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

import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

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

@RunWith(Parameterized.class)
public class EstimatedRowCountTest extends SAITester
{
    @Parameterized.Parameter
    public Version version;
    @Parameterized.Parameter(1)
    public String numType;
    @Parameterized.Parameter(2)
    public Object filter;
    @Parameterized.Parameter(3)
    public Double expectedRows;
    @Parameterized.Parameter(4)
    public Class<?> nodeType;

    private Version latest;
    private int queryOptLevel;

    @Before
    public void setup() throws Throwable
    {
        latest = Version.latest();
        SAIUtil.setLatestVersion(version);

        System.setProperty("cassandra.sai.test.disable.timeout", "true");

        queryOptLevel = QueryController.QUERY_OPT_LEVEL;
        QueryController.QUERY_OPT_LEVEL = 0;
    }

    @After
    public void teardown() throws Throwable
    {
        QueryController.QUERY_OPT_LEVEL = queryOptLevel;
        System.setProperty("cassandra.sai.test.disable.timeout", "false");
        SAIUtil.setLatestVersion(latest);
    }

    @Test
    public void testIneqNrRowsWithHistograms()
    {
        createTable("CREATE TABLE %s (pk text PRIMARY KEY, age " +
                                    numType + ')');
        createIndex("CREATE CUSTOM INDEX ON %s(age) USING 'StorageAttachedIndex'");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        for (int i = 0; i < 100; i++)
        {
            execute("INSERT INTO %s (pk, age) VALUES (?," + i + ')', "key" + i);
        }
        double totalRows = 97.0;
        ReadCommand rc = Util.cmd(cfs)
                             .columns("age")
                             .filterOn("age", Operator.NEQ, filter)
                             .build();
        QueryController controller = new QueryController(cfs, rc,
                                                         version.onDiskFormat().indexFeatureSet(),
                                                         new QueryContext(), null);
        Plan plan = controller.buildPlan();

        assert plan instanceof Plan.RowsIteration;
        Plan.RowsIteration root = (Plan.RowsIteration) plan;
        assertEquals(expectedRows, root.expectedRows(), 0.1);
        Plan.KeysIteration planNode = root.firstNodeOfType(Plan.KeysIteration.class);
        assert planNode != null;
        nodeType.isAssignableFrom(planNode.getClass());
        assertNotNull(planNode);
        assertEquals(expectedRows, planNode.expectedKeys(), 0.1);
        assertEquals(expectedRows / totalRows, planNode.selectivity(), 0.001);
    }

    @Parameterized.Parameters
    public static List<Object[]> data() throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException
    {
        return List.of(new Object[]{ Version.DB, "int", 25, 82.0, Plan.AntiJoin.class },
                       new Object[]{ Version.EB, "int", 25, 97.0, Plan.AntiJoin.class },
                       // The truncated types cannot use AntiJoin and Everything is applied
                       new Object[]{ Version.DB, "decimal", BigDecimal.valueOf(25), 97.0, Plan.Everything.class },
                       new Object[]{ Version.EB, "decimal", BigDecimal.valueOf(25), 97.0, Plan.Everything.class },
                       new Object[]{ Version.DB, "varint", BigInteger.valueOf(25), 97.0, Plan.Everything.class },
                       new Object[]{ Version.EB, "varint", BigInteger.valueOf(25), 97.0, Plan.Everything.class });
    }
}
