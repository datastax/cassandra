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

package org.apache.cassandra.distributed.test.sensors;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.cql3.PageSize;
import org.apache.cassandra.cql3.QueryHandler;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IIsolatedExecutor;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.sensors.ActiveSensorsFactory;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.assertj.core.api.Assertions;

/**
 * Test to verify that the sensors are propagated via the native protocol in the custom payload respecting
 * the configuration set in {@link CassandraRelevantProperties#SENSORS_VIA_NATIVE_PROTOCOL}
 */
@RunWith(Parameterized.class)
public class SensorsTest extends TestBaseImpl
{
    // Table names — shared by setupCluster(), truncateTables(), and data()
    private static final String TBL = "tbl";
    private static final String TBL_COUNTER = "tbl_counter";
    private static final String TBL_2I = "tbl_2i";
    private static final String TBL_SAI = "tbl_sai_idx";
    private static final String TBL_COL = "tbl_col";

    // Sensor header constants per table
    private static final String WRITE_TBL = "WRITE_BYTES_REQUEST." + KEYSPACE + "." + TBL;
    private static final String READ_TBL = "READ_BYTES_REQUEST." + KEYSPACE + "." + TBL;
    private static final String WRITE_COUNTER = "WRITE_BYTES_REQUEST." + KEYSPACE + "." + TBL_COUNTER;
    private static final String WRITE_2I = "WRITE_BYTES_REQUEST." + KEYSPACE + "." + TBL_2I;
    private static final String READ_2I = "READ_BYTES_REQUEST." + KEYSPACE + "." + TBL_2I;
    private static final String INDEX_WRITE_2I = "INDEX_WRITE_BYTES_REQUEST." + KEYSPACE + "." + TBL_2I;
    private static final String WRITE_SAI = "WRITE_BYTES_REQUEST." + KEYSPACE + "." + TBL_SAI;
    private static final String READ_SAI = "READ_BYTES_REQUEST." + KEYSPACE + "." + TBL_SAI;
    private static final String INDEX_WRITE_SAI = "INDEX_WRITE_BYTES_REQUEST." + KEYSPACE + "." + TBL_SAI;
    private static final String WRITE_COL = "WRITE_BYTES_REQUEST." + KEYSPACE + "." + TBL_COL;
    private static final String INDEX_WRITE_COL = "INDEX_WRITE_BYTES_REQUEST." + KEYSPACE + "." + TBL_COL;

    /**
     * Using a combination of 2 nodes with ALL consistency level to ensure internode communication code paths are exercised in the test
     */
    private static final int NODES_COUNT = 2;
    private static final ConsistencyLevel CONSISTENCY_LEVEL = ConsistencyLevel.ALL;

    /**
     * Single shared cluster for all parameterized scenarios — avoids the metaspace exhaustion that results from
     * spinning up a new in-process dtest cluster (with its own isolated classloader) for every scenario.
     * All tables are created once in {@link #setupCluster()}; each scenario truncates them in {@link #truncateTables()}.
     */
    private static Cluster cluster;

    /**
     * Table name for the scenario — kept for test name readability in parameterized output only.
     */
    @Parameterized.Parameter(0)
    public String schema;

    /**
     * Queries to be executed to prepare the table, for example insert some data before read to populate read sensors.
     * Will be run before the {@link #testQuery}.
     */
    @Parameterized.Parameter(1)
    public String[] prepQueries;

    /**
     * Query to be executed to test the sensors, will be run after the {@link #prepQueries}.
     */
    @Parameterized.Parameter(2)
    public String testQuery;

    /**
     * Expected headers in the custom payload for the test queries.
     */
    @Parameterized.Parameter(3)
    public String[] expectedHeaders;

    @BeforeClass
    public static void setupCluster() throws IOException
    {
        CassandraRelevantProperties.SENSORS_FACTORY.setString(ActiveSensorsFactory.class.getName());

        cluster = init(Cluster.build(NODES_COUNT).start());

        // Create all table variants upfront so the cluster is reused across every parameterized scenario.
        // Each scenario truncates the relevant tables in @Before rather than recreating the cluster.
        cluster.schemaChange(withKeyspace("CREATE TABLE %s." + TBL + " (pk int PRIMARY KEY, v1 text)"));
        cluster.schemaChange(withKeyspace("CREATE TABLE %s." + TBL_COUNTER + " (pk int PRIMARY KEY, total counter)"));
        cluster.schemaChange(withKeyspace("CREATE TABLE %s." + TBL_2I + " (pk int PRIMARY KEY, v1 text)"));
        cluster.schemaChange(withKeyspace("CREATE INDEX ON %s." + TBL_2I + " (v1)"));
        cluster.schemaChange(withKeyspace("CREATE TABLE %s." + TBL_SAI + " (pk int PRIMARY KEY, v1 text)"));
        cluster.schemaChange(withKeyspace("CREATE CUSTOM INDEX ON %s." + TBL_SAI + " (v1) USING '" + StorageAttachedIndex.class.getName() + "'"));
        cluster.schemaChange(withKeyspace("CREATE TABLE %s." + TBL_COL + " (pk int PRIMARY KEY, tags set<text>)"));
        cluster.schemaChange(withKeyspace("CREATE CUSTOM INDEX ON %s." + TBL_COL + " (tags) USING '" + StorageAttachedIndex.class.getName() + "'"));
    }

    @AfterClass
    public static void teardownCluster()
    {
        if (cluster != null)
            cluster.close();
    }

    @Before
    public void truncateTables()
    {
        cluster.schemaChange(withKeyspace("TRUNCATE %s." + TBL));
        cluster.schemaChange(withKeyspace("TRUNCATE %s." + TBL_COUNTER));
        cluster.schemaChange(withKeyspace("TRUNCATE %s." + TBL_2I));
        cluster.schemaChange(withKeyspace("TRUNCATE %s." + TBL_SAI));
        cluster.schemaChange(withKeyspace("TRUNCATE %s." + TBL_COL));
    }

    @Parameterized.Parameters(name = "schema={0}, prepQueries={1}, testQuery={2}, expectedHeaders={3}")
    public static Collection<Object[]> data()
    {
        List<Object[]> result = new ArrayList<>();
        result.addAll(baselineScenarios());
        result.addAll(secondaryIndexScenarios());
        result.addAll(saiScalarScenarios());
        result.addAll(saiCollectionScenarios());
        result.addAll(conditionalBatchScenarios());
        return result;
    }

    /**
     * Baseline scenarios: non-indexed writes, reads and CAS on {@value TBL} and {@value TBL_COUNTER}.
     */
    private static List<Object[]> baselineScenarios()
    {
        String[] noPrep = new String[0];
        String write = withKeyspace("INSERT INTO %s." + TBL + "(pk, v1) VALUES (1, 'read me')");
        String counter = withKeyspace("UPDATE %s." + TBL_COUNTER + " SET total = total + 1 WHERE pk = 1");
        String read = withKeyspace("SELECT * FROM %s." + TBL + " WHERE pk=1");
        String range = withKeyspace("SELECT * FROM %s." + TBL);
        String cas = withKeyspace("UPDATE %s." + TBL + " SET v1 = 'cas update' WHERE pk = 1 IF v1 = 'read me'");
        String loggedBatch = String.format("BEGIN BATCH\n" +
                                           "INSERT INTO %s." + TBL + "(pk, v1) VALUES (2, 'read me 2');\n" +
                                           "INSERT INTO %s." + TBL + "(pk, v1) VALUES (3, 'read me 3');\n" +
                                           "APPLY BATCH;", KEYSPACE, KEYSPACE);
        String unloggedBatch = String.format("BEGIN UNLOGGED BATCH\n" +
                                             "INSERT INTO %s." + TBL + "(pk, v1) VALUES (4, 'read me 2');\n" +
                                             "INSERT INTO %s." + TBL + "(pk, v1) VALUES (4, 'read me 3');\n" +
                                             "APPLY BATCH;", KEYSPACE, KEYSPACE);

        List<Object[]> result = new ArrayList<>();
        result.add(new Object[]{ TBL, noPrep, write, new String[]{ WRITE_TBL } });
        result.add(new Object[]{ TBL_COUNTER, noPrep, counter, new String[]{ WRITE_COUNTER } });
        result.add(new Object[]{ TBL, new String[]{ write }, read, new String[]{ READ_TBL } });
        result.add(new Object[]{ TBL, noPrep, cas, new String[]{ WRITE_TBL, READ_TBL } });
        result.add(new Object[]{ TBL, noPrep, loggedBatch, new String[]{ WRITE_TBL } });
        result.add(new Object[]{ TBL, noPrep, unloggedBatch, new String[]{ WRITE_TBL } });
        result.add(new Object[]{ TBL, new String[]{ write }, range, new String[]{ READ_TBL } });
        return result;
    }

    /**
     * Secondary index (2i) scenarios on {@value TBL_2I}: inserts (insertRow path), updates (updateRow path),
     * CAS (insert via IF NOT EXISTS / update via IF condition), and multi-table batches mixing
     * {@value TBL_2I} and {@value TBL_SAI} (exercises the per-table sensor loop for two distinct tables).
     */
    private static List<Object[]> secondaryIndexScenarios()
    {
        String[] noPrep = new String[0];
        String write = withKeyspace("INSERT INTO %s." + TBL_2I + "(pk, v1) VALUES (1, '2i read me')");
        String writeUpdate = withKeyspace("INSERT INTO %s." + TBL_2I + "(pk, v1) VALUES (1, '2i updated')");
        String cas = withKeyspace("UPDATE %s." + TBL_2I + " SET v1 = '2i cas update' WHERE pk = 1 IF v1 = '2i read me'");
        String casInsert = withKeyspace("INSERT INTO %s." + TBL_2I + "(pk, v1) VALUES (5, '2i cas insert') IF NOT EXISTS");
        // single-table batches (same table, multiple rows)
        String loggedBatch = String.format("BEGIN BATCH\n" +
                                           "INSERT INTO %s." + TBL_2I + "(pk, v1) VALUES (2, '2i read me 2');\n" +
                                           "INSERT INTO %s." + TBL_2I + "(pk, v1) VALUES (3, '2i read me 3');\n" +
                                           "APPLY BATCH;", KEYSPACE, KEYSPACE);
        String unloggedBatch = String.format("BEGIN UNLOGGED BATCH\n" +
                                             "INSERT INTO %s." + TBL_2I + "(pk, v1) VALUES (4, '2i read me 2');\n" +
                                             "INSERT INTO %s." + TBL_2I + "(pk, v1) VALUES (4, '2i read me 3');\n" +
                                             "APPLY BATCH;", KEYSPACE, KEYSPACE);
        String loggedBatchUpdate = String.format("BEGIN BATCH\n" +
                                                 "INSERT INTO %s." + TBL_2I + "(pk, v1) VALUES (2, '2i updated 2');\n" +
                                                 "INSERT INTO %s." + TBL_2I + "(pk, v1) VALUES (3, '2i updated 3');\n" +
                                                 "APPLY BATCH;", KEYSPACE, KEYSPACE);
        String unloggedBatchUpdate = String.format("BEGIN UNLOGGED BATCH\n" +
                                                   "INSERT INTO %s." + TBL_2I + "(pk, v1) VALUES (4, '2i updated 2');\n" +
                                                   "INSERT INTO %s." + TBL_2I + "(pk, v1) VALUES (4, '2i updated 3');\n" +
                                                   "APPLY BATCH;", KEYSPACE, KEYSPACE);
        // multi-table batches: tbl_2i + tbl_sai in the same batch — sensors must appear for both tables
        String multiTableLoggedBatch = String.format("BEGIN BATCH\n" +
                                                     "INSERT INTO %s." + TBL_2I + "(pk, v1) VALUES (20, 'mt 2i a');\n" +
                                                     "INSERT INTO %s." + TBL_2I + "(pk, v1) VALUES (21, 'mt 2i b');\n" +
                                                     "INSERT INTO %s." + TBL_SAI + "(pk, v1) VALUES (20, 'mt sai a');\n" +
                                                     "INSERT INTO %s." + TBL_SAI + "(pk, v1) VALUES (21, 'mt sai b');\n" +
                                                     "APPLY BATCH;", KEYSPACE, KEYSPACE, KEYSPACE, KEYSPACE);
        String multiTableUnloggedBatch = String.format("BEGIN UNLOGGED BATCH\n" +
                                                       "INSERT INTO %s." + TBL_2I + "(pk, v1) VALUES (22, 'mt 2i a');\n" +
                                                       "INSERT INTO %s." + TBL_2I + "(pk, v1) VALUES (23, 'mt 2i b');\n" +
                                                       "INSERT INTO %s." + TBL_SAI + "(pk, v1) VALUES (22, 'mt sai a');\n" +
                                                       "INSERT INTO %s." + TBL_SAI + "(pk, v1) VALUES (23, 'mt sai b');\n" +
                                                       "APPLY BATCH;", KEYSPACE, KEYSPACE, KEYSPACE, KEYSPACE);

        List<Object[]> result = new ArrayList<>();
        // inserts: insertRow path
        result.add(new Object[]{ TBL_2I, noPrep, write, new String[]{ WRITE_2I, INDEX_WRITE_2I } });
        result.add(new Object[]{ TBL_2I, noPrep, loggedBatch, new String[]{ WRITE_2I, INDEX_WRITE_2I } });
        result.add(new Object[]{ TBL_2I, noPrep, unloggedBatch, new String[]{ WRITE_2I, INDEX_WRITE_2I } });
        // updates: updateRow path (row pre-exists, new value differs to ensure index allocation)
        result.add(new Object[]{ TBL_2I, new String[]{ write }, writeUpdate, new String[]{ WRITE_2I, INDEX_WRITE_2I } });
        result.add(new Object[]{ TBL_2I, new String[]{ loggedBatch }, loggedBatchUpdate, new String[]{ WRITE_2I, INDEX_WRITE_2I } });
        result.add(new Object[]{ TBL_2I, new String[]{ unloggedBatch }, unloggedBatchUpdate, new String[]{ WRITE_2I, INDEX_WRITE_2I } });
        // CAS: IF NOT EXISTS (insertRow path) and IF condition (updateRow path)
        result.add(new Object[]{ TBL_2I, noPrep, casInsert, new String[]{ WRITE_2I, READ_2I, INDEX_WRITE_2I } });
        result.add(new Object[]{ TBL_2I, new String[]{ write }, cas, new String[]{ WRITE_2I, READ_2I, INDEX_WRITE_2I } });
        // multi-table: sensors from both tbl_2i and tbl_sai must be present in the response
        result.add(new Object[]{ TBL_2I + "+" + TBL_SAI, noPrep, multiTableLoggedBatch, new String[]{ WRITE_2I, INDEX_WRITE_2I, WRITE_SAI, INDEX_WRITE_SAI } });
        result.add(new Object[]{ TBL_2I + "+" + TBL_SAI, noPrep, multiTableUnloggedBatch, new String[]{ WRITE_2I, INDEX_WRITE_2I, WRITE_SAI, INDEX_WRITE_SAI } });
        return result;
    }

    /**
     * SAI scenarios on a scalar column ({@value TBL_SAI}): inserts (insertRow path), updates (updateRow path),
     * and CAS (insert via IF NOT EXISTS / update via IF condition).
     * The update path exercises {@code TrieMemtableIndex.update(ByteBuffer, ByteBuffer)}.
     */
    private static List<Object[]> saiScalarScenarios()
    {
        String[] noPrep = new String[0];
        String write = withKeyspace("INSERT INTO %s." + TBL_SAI + "(pk, v1) VALUES (1, 'sai read me')");
        String writeUpdate = withKeyspace("INSERT INTO %s." + TBL_SAI + "(pk, v1) VALUES (1, 'sai updated')");
        String cas = withKeyspace("UPDATE %s." + TBL_SAI + " SET v1 = 'sai cas update' WHERE pk = 1 IF v1 = 'sai read me'");
        String casInsert = withKeyspace("INSERT INTO %s." + TBL_SAI + "(pk, v1) VALUES (5, 'sai cas insert') IF NOT EXISTS");
        String loggedBatch = String.format("BEGIN BATCH\n" +
                                           "INSERT INTO %s." + TBL_SAI + "(pk, v1) VALUES (2, 'sai read me 2');\n" +
                                           "INSERT INTO %s." + TBL_SAI + "(pk, v1) VALUES (3, 'sai read me 3');\n" +
                                           "APPLY BATCH;", KEYSPACE, KEYSPACE);
        String unloggedBatch = String.format("BEGIN UNLOGGED BATCH\n" +
                                             "INSERT INTO %s." + TBL_SAI + "(pk, v1) VALUES (4, 'sai read me 2');\n" +
                                             "INSERT INTO %s." + TBL_SAI + "(pk, v1) VALUES (4, 'sai read me 3');\n" +
                                             "APPLY BATCH;", KEYSPACE, KEYSPACE);
        String loggedBatchUpdate = String.format("BEGIN BATCH\n" +
                                                 "INSERT INTO %s." + TBL_SAI + "(pk, v1) VALUES (2, 'sai updated 2');\n" +
                                                 "INSERT INTO %s." + TBL_SAI + "(pk, v1) VALUES (3, 'sai updated 3');\n" +
                                                 "APPLY BATCH;", KEYSPACE, KEYSPACE);
        String unloggedBatchUpdate = String.format("BEGIN UNLOGGED BATCH\n" +
                                                   "INSERT INTO %s." + TBL_SAI + "(pk, v1) VALUES (4, 'sai updated 2');\n" +
                                                   "INSERT INTO %s." + TBL_SAI + "(pk, v1) VALUES (4, 'sai updated 3');\n" +
                                                   "APPLY BATCH;", KEYSPACE, KEYSPACE);

        List<Object[]> result = new ArrayList<>();
        // inserts: insertRow path
        result.add(new Object[]{ TBL_SAI, noPrep, write, new String[]{ WRITE_SAI, INDEX_WRITE_SAI } });
        result.add(new Object[]{ TBL_SAI, noPrep, loggedBatch, new String[]{ WRITE_SAI, INDEX_WRITE_SAI } });
        result.add(new Object[]{ TBL_SAI, noPrep, unloggedBatch, new String[]{ WRITE_SAI, INDEX_WRITE_SAI } });
        // updates: updateRow path (row pre-exists, new value differs to ensure index allocation)
        result.add(new Object[]{ TBL_SAI, new String[]{ write }, writeUpdate, new String[]{ WRITE_SAI, INDEX_WRITE_SAI } });
        result.add(new Object[]{ TBL_SAI, new String[]{ loggedBatch }, loggedBatchUpdate, new String[]{ WRITE_SAI, INDEX_WRITE_SAI } });
        result.add(new Object[]{ TBL_SAI, new String[]{ unloggedBatch }, unloggedBatchUpdate, new String[]{ WRITE_SAI, INDEX_WRITE_SAI } });
        // CAS: IF NOT EXISTS (insertRow path) and IF condition (updateRow path)
        result.add(new Object[]{ TBL_SAI, noPrep, casInsert, new String[]{ WRITE_SAI, READ_SAI, INDEX_WRITE_SAI } });
        result.add(new Object[]{ TBL_SAI, new String[]{ write }, cas, new String[]{ WRITE_SAI, READ_SAI, INDEX_WRITE_SAI } });
        return result;
    }

    /**
     * SAI scenarios on a non-frozen collection column ({@value TBL_COL}): inserts and updates.
     * The update path exercises {@code TrieMemtableIndex.update(Iterator, Iterator)}.
     */
    private static List<Object[]> saiCollectionScenarios()
    {
        String collectionWrite = withKeyspace("INSERT INTO %s." + TBL_COL + "(pk, tags) VALUES (1, {'a', 'b'})");
        String collectionUpdate = withKeyspace("INSERT INTO %s." + TBL_COL + "(pk, tags) VALUES (1, {'c', 'd'})");

        List<Object[]> result = new ArrayList<>();
        result.add(new Object[]{ TBL_COL, new String[0], collectionWrite, new String[]{ WRITE_COL, INDEX_WRITE_COL } });
        result.add(new Object[]{ TBL_COL, new String[]{ collectionWrite }, collectionUpdate, new String[]{ WRITE_COL, INDEX_WRITE_COL } });
        return result;
    }

    /**
     * Conditional batch scenarios: BEGIN BATCH statements with IF conditions, routed through
     * {@code BatchStatement.executeWithConditions}. These exercise the {@code ResultMessage.Rows}
     * return path, which must also carry INDEX_WRITE_BYTES sensors.
     *
     * Cassandra requires all statements in a conditional batch to target the same partition key and table.
     * Multi-statement scenarios below use multiple statements on the same partition to exercise the
     * repeated-same-TableMetadata path through {@code .distinct()} in the sensor loop.
     */
    private static List<Object[]> conditionalBatchScenarios()
    {
        String[] noPrep = new String[0];

        // 2i: single conditional statement — IF NOT EXISTS (insert path)
        String prep2i = withKeyspace("INSERT INTO %s." + TBL_2I + "(pk, v1) VALUES (1, '2i read me')");
        String conditionalBatch2iInsert = String.format("BEGIN BATCH\n" +
                                                        "INSERT INTO %s." + TBL_2I + "(pk, v1) VALUES (10, '2i cond batch insert') IF NOT EXISTS;\n" +
                                                        "APPLY BATCH;", KEYSPACE);
        // 2i: single conditional statement — IF condition (update path)
        String conditionalBatch2iUpdate = String.format("BEGIN BATCH\n" +
                                                        "UPDATE %s." + TBL_2I + " SET v1 = '2i cond batch update' WHERE pk = 1 IF v1 = '2i read me';\n" +
                                                        "APPLY BATCH;", KEYSPACE);
        // 2i: multiple statements on the same partition — conditional insert + unconditional insert on pk=10,
        // exercises the duplicate-TableMetadata path (same metadata object appears twice in statements list)
        String conditionalBatch2iMultiStmt = String.format("BEGIN BATCH\n" +
                                                           "INSERT INTO %s." + TBL_2I + "(pk, v1) VALUES (10, '2i multi a') IF NOT EXISTS;\n" +
                                                           "INSERT INTO %s." + TBL_2I + "(pk, v1) VALUES (10, '2i multi b');\n" +
                                                           "APPLY BATCH;", KEYSPACE, KEYSPACE);

        // SAI: single conditional statement — IF NOT EXISTS (insert path)
        String prepSai = withKeyspace("INSERT INTO %s." + TBL_SAI + "(pk, v1) VALUES (1, 'sai read me')");
        String conditionalBatchSaiInsert = String.format("BEGIN BATCH\n" +
                                                         "INSERT INTO %s." + TBL_SAI + "(pk, v1) VALUES (10, 'sai cond batch insert') IF NOT EXISTS;\n" +
                                                         "APPLY BATCH;", KEYSPACE);
        // SAI: single conditional statement — IF condition (update path)
        String conditionalBatchSaiUpdate = String.format("BEGIN BATCH\n" +
                                                         "UPDATE %s." + TBL_SAI + " SET v1 = 'sai cond batch update' WHERE pk = 1 IF v1 = 'sai read me';\n" +
                                                         "APPLY BATCH;", KEYSPACE);
        // SAI: multiple statements on the same partition — conditional insert + unconditional insert on pk=10,
        // exercises the duplicate-TableMetadata path (same metadata object appears twice in statements list)
        String conditionalBatchSaiMultiStmt = String.format("BEGIN BATCH\n" +
                                                            "INSERT INTO %s." + TBL_SAI + "(pk, v1) VALUES (10, 'sai multi a') IF NOT EXISTS;\n" +
                                                            "INSERT INTO %s." + TBL_SAI + "(pk, v1) VALUES (10, 'sai multi b');\n" +
                                                            "APPLY BATCH;", KEYSPACE, KEYSPACE);

        List<Object[]> result = new ArrayList<>();
        // 2i conditional batch: IF NOT EXISTS (insert path) and IF condition (update path)
        result.add(new Object[]{ TBL_2I, noPrep, conditionalBatch2iInsert, new String[]{ WRITE_2I, READ_2I, INDEX_WRITE_2I } });
        result.add(new Object[]{ TBL_2I, new String[]{ prep2i }, conditionalBatch2iUpdate, new String[]{ WRITE_2I, READ_2I, INDEX_WRITE_2I } });
        // 2i conditional batch: multiple statements on same partition (duplicate TableMetadata in statements list)
        result.add(new Object[]{ TBL_2I, noPrep, conditionalBatch2iMultiStmt, new String[]{ WRITE_2I, READ_2I, INDEX_WRITE_2I } });
        // SAI conditional batch: IF NOT EXISTS (insert path) and IF condition (update path)
        result.add(new Object[]{ TBL_SAI, noPrep, conditionalBatchSaiInsert, new String[]{ WRITE_SAI, READ_SAI, INDEX_WRITE_SAI } });
        result.add(new Object[]{ TBL_SAI, new String[]{ prepSai }, conditionalBatchSaiUpdate, new String[]{ WRITE_SAI, READ_SAI, INDEX_WRITE_SAI } });
        // SAI conditional batch: multiple statements on same partition (duplicate TableMetadata in statements list)
        result.add(new Object[]{ TBL_SAI, noPrep, conditionalBatchSaiMultiStmt, new String[]{ WRITE_SAI, READ_SAI, INDEX_WRITE_SAI } });
        return result;
    }

    @Test
    public void testSensorsInCQLResponseEnabled() throws Throwable
    {
        Map<String, ByteBuffer> customPayload = executeTest(true);
        for (String header : expectedHeaders)
        {
            double requestBytes = getBytesForHeader(customPayload, header);
            Assertions.assertThat(requestBytes).isGreaterThan(0D);
        }
    }

    @Test
    public void testSensorsInCQLResponseDisabled() throws Throwable
    {
        Map<String, ByteBuffer> customPayload = executeTest(false);
        // customPayload will be null if it has no headers. However, non-sensor headers could've been added. So here we check for nullability or non-existence of sensor headers
        if (customPayload != null)
        {
            for (String header : expectedHeaders)
            {
                Assertions.assertThat(customPayload).doesNotContainKey(header);
            }
        } // else do nothing as null customPayload means no sensors were added
    }

    /**
     * Execute the test with the given {@code propagateViaNativeProtocol} flag and return the custom payload.
     */
    private Map<String, ByteBuffer> executeTest(boolean propagateViaNativeProtocol) throws Throwable
    {
        AtomicReference<Map<String, ByteBuffer>> customPayload = new AtomicReference<>();
        for (String prepQuery : this.prepQueries)
            cluster.coordinator(1).execute(prepQuery, ConsistencyLevel.ALL);
        // work around serializability of @Parameterized.Parameter by providing a locally scoped variable
        String query = this.testQuery;
        // The cluster is shared across scenarios, so SENSORS_VIA_NATIVE_PROTOCOL must be set inside the node's
        // classloader via runOnInstance rather than on the outer test JVM — the node won't see outer JVM property changes.
        // Any methods used inside the runOnInstance() block should be static, otherwise java.io.NotSerializableException will be thrown
        cluster.get(1).acceptsOnInstance(
               (IIsolatedExecutor.SerializableConsumer<AtomicReference<Map<String, ByteBuffer>>>)
               (reference) -> {
                   CassandraRelevantProperties.SENSORS_VIA_NATIVE_PROTOCOL.setBoolean(propagateViaNativeProtocol);
                   reference.set(executeWithResult(query).getCustomPayload());
               })
               .accept(customPayload);
        return customPayload.get();
    }

    private double getBytesForHeader(Map<String, ByteBuffer> customPayload, String expectedHeader)
    {
        Assertions.assertThat(customPayload).describedAs("Expected header %s not found in custom payload", expectedHeader).containsKey(expectedHeader);
        return ByteBufferUtil.toDouble(customPayload.get(expectedHeader));
    }

    /**
     * TODO: update SimpleQueryResult in the dtest-api project to expose custom payload and use Coordinator##executeWithResult instead
     */
    private static ResultMessage<?> executeWithResult(String query)
    {
        long nanoTime = System.nanoTime();
        QueryHandler.Prepared prepared = QueryProcessor.prepareInternal(query);
        ConsistencyLevel consistencyLevel = ConsistencyLevel.valueOf(CONSISTENCY_LEVEL.name());
        org.apache.cassandra.db.ConsistencyLevel cl = org.apache.cassandra.db.ConsistencyLevel.fromCode(consistencyLevel.ordinal());
        QueryOptions initialOptions = QueryOptions.create(cl,
                                                          null,
                                                          false,
                                                          PageSize.inRows(512),
                                                          null,
                                                          null,
                                                          ProtocolVersion.CURRENT,
                                                          prepared.keyspace);
        return prepared.statement.execute(QueryProcessor.internalQueryState(), initialOptions, nanoTime);
    }
}
