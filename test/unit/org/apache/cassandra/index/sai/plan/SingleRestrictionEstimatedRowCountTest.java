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
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.SAIUtil;
import org.apache.cassandra.index.sai.disk.format.Version;

import static org.apache.cassandra.cql3.CQL3Type.Native.DECIMAL;
import static org.apache.cassandra.cql3.CQL3Type.Native.INT;
import static org.apache.cassandra.cql3.CQL3Type.Native.VARINT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class SingleRestrictionEstimatedRowCountTest extends SAITester
{
    static protected Map<Map.Entry<Version, CQL3Type.Native>, ColumnFamilyStore> tables = new HashMap<>();
    static Version[] versions = new Version[]{ Version.DB, Version.EB };
    static CQL3Type.Native[] types = new CQL3Type.Native[]{ INT, DECIMAL, VARINT };

    static protected Object getFilterValue(CQL3Type.Native type, int value)
    {
        switch (type)
        {
            case INT:
                return value;
            case DECIMAL:
                return BigDecimal.valueOf(value);
            case VARINT:
                return BigInteger.valueOf(value);
        }
        fail("Must be known type");
        return null;
    }

    static Map.Entry<Version, CQL3Type.Native> tablesEntryKey(Version version, CQL3Type.Native type)
    {
        return new AbstractMap.SimpleEntry<>(version, type);
    }

    @Test
    public void testMemtablesSAI()
    {
        createTables();

        RowCountTest test = new RowCountTest(Operator.NEQ, 25);
        test.doTest(Version.DB, INT, 97.0);
        test.doTest(Version.EB, INT, 97.0);
        // Truncated numeric types planned differently
        test.doTest(Version.DB, DECIMAL, 97.0);
        test.doTest(Version.EB, DECIMAL, 97.0);
        test.doTest(Version.EB, VARINT, 97.0);

        test = new RowCountTest(Operator.LT, 50);
        test.doTest(Version.DB, INT, 48);
        test.doTest(Version.EB, INT, 48);
        test.doTest(Version.DB, DECIMAL, 48);
        test.doTest(Version.EB, DECIMAL, 48);

        test = new RowCountTest(Operator.LT, 150);
        test.doTest(Version.DB, INT, 97);
        test.doTest(Version.EB, INT, 97);
        test.doTest(Version.DB, DECIMAL, 97);
        test.doTest(Version.EB, DECIMAL, 97);

        test = new RowCountTest(Operator.EQ, 31);
        test.doTest(Version.DB, INT, 15);
        test.doTest(Version.EB, INT, 0);
        test.doTest(Version.DB, DECIMAL, 15);
        test.doTest(Version.EB, DECIMAL, 0);
    }


    void createTables()
    {
        for (Version version : versions)
        {
            SAIUtil.setCurrentVersion(version);
            for (CQL3Type.Native type : types)
            {
                createTable("CREATE TABLE %s (pk text PRIMARY KEY, age " + type + ')');
                createIndex("CREATE CUSTOM INDEX ON %s(age) USING 'StorageAttachedIndex'");
                tables.put(tablesEntryKey(version, type), getCurrentColumnFamilyStore());
            }
        }
        flush();
        for (ColumnFamilyStore cfs : tables.values())
            populateTable(cfs);
    }

    void populateTable(ColumnFamilyStore cfs)
    {
        // Avoid race condition of starting before flushing completed
        cfs.unsafeRunWithoutFlushing(() -> {
            for (int i = 0; i < 100; i++)
            {
                String query = String.format("INSERT INTO %s (pk, age) VALUES (?, " + i + ')',
                                             cfs.keyspace.getName() + '.' + cfs.name);
                executeFormattedQuery(query, "key" + i);
            }
        });
    }

    static class RowCountTest
    {
        final Operator op;
        final int filterValue;

        RowCountTest(Operator op, int filterValue)
        {
            this.op = op;
            this.filterValue = filterValue;
        }

        void doTest(Version version, CQL3Type.Native type, double expectedRows)
        {
            ColumnFamilyStore cfs = tables.get(new AbstractMap.SimpleEntry<>(version, type));
            Object filter = getFilterValue(type, filterValue);
            ReadCommand rc = Util.cmd(cfs)
                                 .columns("age")
                                 .filterOn("age", op, filter)
                                 .build();
            QueryController controller = new QueryController(cfs,
                                                             rc,
                                                             version.onDiskFormat().indexFeatureSet(),
                                                             new QueryContext());

            long totalRows = controller.planFactory.tableMetrics.rows;
            assertEquals(0, cfs.metrics().liveSSTableCount.getValue().intValue());

            Plan plan = controller.buildPlan();
            assert plan instanceof Plan.RowsIteration;
            Plan.RowsIteration root = (Plan.RowsIteration) plan;
            Plan.KeysIteration planNode = root.firstNodeOfType(Plan.KeysIteration.class);
            assertNotNull(planNode);

            assertEquals(expectedRows, root.expectedRows(), 0.1);
            assertEquals(expectedRows, planNode.expectedKeys(), 0.1);
            assertEquals(expectedRows / totalRows, planNode.selectivity(), 0.001);
        }
    }
}
