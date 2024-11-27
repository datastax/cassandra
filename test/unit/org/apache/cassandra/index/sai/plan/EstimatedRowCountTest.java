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

import java.util.List;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.Util;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.SAIUtil;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(Parameterized.class)
public class EstimatedRowCountTest extends SAITester
{
    @Parameterized.Parameter
    public Version version;
    @Parameterized.Parameter(1)
    public String numType;

    private Version latest;

    @Before
    public void setup() throws Throwable
    {
        latest = Version.latest();
        SAIUtil.setLatestVersion(version);
    }

    @After
    public void teardown() throws Throwable
    {
        SAIUtil.setLatestVersion(latest);
    }

    @Test
    public void testIneqNrRowsWithHistograms()
    {
        var tableName = createTable("CREATE TABLE %s (pk text PRIMARY KEY, age int)");
        createIndex("CREATE CUSTOM INDEX ON %s(age) USING 'StorageAttachedIndex'");
        ColumnFamilyStore cfs = Keyspace.open(CQLTester.KEYSPACE).getColumnFamilyStore(tableName);
        for (int i = 0; i < 100; i++)
        {
            new RowUpdateBuilder(cfs.metadata(), FBUtilities.timestampMicros(), "key" + i)
            .add("age", i)
            .build()
            .applyUnsafe();
        }
        ReadCommand rc = Util.cmd(cfs)
                             .columns("age")
                             .filterOn("age", Operator.NEQ, 25)
                             .build();
        QueryController controller = new QueryController(cfs, rc,
                                         version.onDiskFormat().indexFeatureSet(),
                                         new QueryContext(), null);
        Plan plan = controller.buildPlan();

        assert plan instanceof Plan.RowsIteration;
        Plan.RowsIteration root = (Plan.RowsIteration) plan;
        assertEquals(97, root.expectedRows(), 0.1);
        Plan.KeysIteration planNode =root.firstNodeOfType(Plan.KeysIteration.class);
        assert planNode instanceof Plan.IndexScan;
        assertNotNull(planNode);
        assertEquals(97, planNode.expectedKeys(), 0.1);
        assertEquals(1.0, planNode.selectivity(), 0.001);
    }

    @Parameterized.Parameters
    public static List<Object[]> data()
    {
        var indexVersions = List.of(Version.DB, Version.EB);
        var truncatedTypes = List.of("int", "decimal", "varint");
        return Lists.cartesianProduct(indexVersions, truncatedTypes)
                    .stream().map(List::toArray).collect(Collectors.toList());
    }
}
