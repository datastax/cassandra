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

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.index.sai.IndexingSchemaLoader;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.disk.v1.V1OnDiskFormat;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class EstimatedRowCountTest extends IndexingSchemaLoader
{
    private static final String KS_NAME = "sai";
    private static final String CF_NAME = "test_cf";
    private static final String CLUSTERING_CF_NAME = "clustering_test_cf";
    private static final String STATIC_CF_NAME = "static_ndi_test_cf";

    private static ColumnFamilyStore BACKEND;

    @BeforeClass
    public static void loadSchema() throws ConfigurationException
    {
        System.setProperty("cassandra.config", "cassandra-murmur.yaml");
        IndexingSchemaLoader.loadSchema();
        IndexingSchemaLoader.createKeyspace(KS_NAME,
                                            KeyspaceParams.simpleTransient(1),
                                            IndexingSchemaLoader.ndiCFMD(KS_NAME, CF_NAME),
                                            IndexingSchemaLoader.clusteringNDICFMD(KS_NAME, CLUSTERING_CF_NAME),
                                            IndexingSchemaLoader.staticNDICFMD(KS_NAME, STATIC_CF_NAME));
        BACKEND = Keyspace.open(KS_NAME).getColumnFamilyStore(CF_NAME);
    }

    @Test
    public void testIneqRowEstimation()
    {
        ColumnFamilyStore cfs = Keyspace.open(KS_NAME).getColumnFamilyStore(CF_NAME);
        for (int i = 0; i < 100; i++)
        {
            new RowUpdateBuilder(cfs.metadata(), FBUtilities.timestampMicros(), "key" + i)
            .add("age", i)
            .build()
            .applyUnsafe();
        }

        ReadCommand rc = Util.cmd(BACKEND)
                             .columns("age")
                             .filterOn("age", Operator.NEQ, 25)
                             .build();
        QueryController controller = new QueryController(BACKEND, rc,
                                         V1OnDiskFormat.instance.indexFeatureSet(),
                                         new QueryContext(), null);

        Plan plan = controller.buildPlan();
        assert plan instanceof Plan.RowsIteration;
        Plan.RowsIteration root = (Plan.RowsIteration) plan;
        assertEquals(97, root.expectedRows(), 0.1);
        Plan.KeysIteration planNode =root.firstNodeOfType(Plan.KeysIteration.class);
        assertNotNull(planNode);
        assertEquals(97, planNode.expectedKeys(), 0.1);
        assertEquals(1.0, planNode.selectivity(), 0.001);
    }
}
