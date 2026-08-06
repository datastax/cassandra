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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
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
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.sensors.ActiveSensorsFactory;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.tracing.TraceKeyspace;
import org.apache.cassandra.tracing.TraceStateImpl;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.UUIDGen;
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
    private static final String READ_LATENCY_TIER_TBL = "READ_LATENCY_TIER_REQUEST." + KEYSPACE + "." + TBL;
    private static final String WRITE_COUNTER = "WRITE_BYTES_REQUEST." + KEYSPACE + "." + TBL_COUNTER;
    private static final String WRITE_2I = "WRITE_BYTES_REQUEST." + KEYSPACE + "." + TBL_2I;
    private static final String READ_2I = "READ_BYTES_REQUEST." + KEYSPACE + "." + TBL_2I;
    private static final String READ_LATENCY_TIER_2I = "READ_LATENCY_TIER_REQUEST." + KEYSPACE + "." + TBL_2I;
    private static final String INDEX_WRITE_2I = "INDEX_WRITE_BYTES_REQUEST." + KEYSPACE + "." + TBL_2I;
    private static final String WRITE_SAI = "WRITE_BYTES_REQUEST." + KEYSPACE + "." + TBL_SAI;
    private static final String READ_SAI = "READ_BYTES_REQUEST." + KEYSPACE + "." + TBL_SAI;
    private static final String READ_LATENCY_TIER_SAI = "READ_LATENCY_TIER_REQUEST." + KEYSPACE + "." + TBL_SAI;
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
     * Human-readable scenario name used as the JUnit test display name.
     */
    @Parameterized.Parameter(0)
    public String scenarioName;

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

    /**
     * When {@code true}, the query is executed with a page size, which always takes the paging
     * path ({@code execute(Pager,...)}). When {@code false}, no page size is supplied
     * ({@link PageSize#NONE}), causing {@code canSkipPaging} to return {@code true} and routing
     * execution through the distributed non-paging path ({@code execute(ReadQuery,...)}).
     */
    @Parameterized.Parameter(4)
    public boolean paging;

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
        cluster.coordinator(1).execute(withKeyspace("TRUNCATE %s." + TBL), ConsistencyLevel.ALL);
        cluster.coordinator(1).execute(withKeyspace("TRUNCATE %s." + TBL_COUNTER), ConsistencyLevel.ALL);
        cluster.coordinator(1).execute(withKeyspace("TRUNCATE %s." + TBL_2I), ConsistencyLevel.ALL);
        cluster.coordinator(1).execute(withKeyspace("TRUNCATE %s." + TBL_SAI), ConsistencyLevel.ALL);
        cluster.coordinator(1).execute(withKeyspace("TRUNCATE %s." + TBL_COL), ConsistencyLevel.ALL);
    }

    @Parameterized.Parameters(name = "{0}")
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
        result.add(new Object[]{ "tbl: insert", noPrep, write, new String[]{ WRITE_TBL }, true });
        result.add(new Object[]{ "tbl_counter: counter update", noPrep, counter, new String[]{ WRITE_COUNTER }, true });
        result.add(new Object[]{ "tbl: point read (paging)", new String[]{ write }, read, new String[]{ READ_TBL, READ_LATENCY_TIER_TBL }, true });
        result.add(new Object[]{ "tbl: point read (no paging)", new String[]{ write }, read, new String[]{ READ_TBL, READ_LATENCY_TIER_TBL }, false });
        result.add(new Object[]{ "tbl: CAS update", noPrep, cas, new String[]{ WRITE_TBL, READ_TBL, READ_LATENCY_TIER_TBL }, true });
        result.add(new Object[]{ "tbl: logged batch insert", noPrep, loggedBatch, new String[]{ WRITE_TBL }, true });
        result.add(new Object[]{ "tbl: unlogged batch insert", noPrep, unloggedBatch, new String[]{ WRITE_TBL }, true });
        result.add(new Object[]{ "tbl: range read (paging)", new String[]{ write }, range, new String[]{ READ_TBL, READ_LATENCY_TIER_TBL }, true });
        result.add(new Object[]{ "tbl: range read (no paging)", new String[]{ write }, range, new String[]{ READ_TBL, READ_LATENCY_TIER_TBL }, false });
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
        result.add(new Object[]{ "2i: insert (insertRow path)", noPrep, write, new String[]{ WRITE_2I, INDEX_WRITE_2I }, true });
        result.add(new Object[]{ "2i: logged batch insert", noPrep, loggedBatch, new String[]{ WRITE_2I, INDEX_WRITE_2I }, true });
        result.add(new Object[]{ "2i: unlogged batch insert", noPrep, unloggedBatch, new String[]{ WRITE_2I, INDEX_WRITE_2I }, true });
        result.add(new Object[]{ "2i: update (updateRow path)", new String[]{ write }, writeUpdate, new String[]{ WRITE_2I, INDEX_WRITE_2I }, true });
        result.add(new Object[]{ "2i: logged batch update", new String[]{ loggedBatch }, loggedBatchUpdate, new String[]{ WRITE_2I, INDEX_WRITE_2I }, true });
        result.add(new Object[]{ "2i: unlogged batch update", new String[]{ unloggedBatch }, unloggedBatchUpdate, new String[]{ WRITE_2I, INDEX_WRITE_2I }, true });
        result.add(new Object[]{ "2i: CAS IF NOT EXISTS (insertRow path)", noPrep, casInsert, new String[]{ WRITE_2I, READ_2I, READ_LATENCY_TIER_2I, INDEX_WRITE_2I }, true });
        result.add(new Object[]{ "2i: CAS IF condition (updateRow path)", new String[]{ write }, cas, new String[]{ WRITE_2I, READ_2I, READ_LATENCY_TIER_2I, INDEX_WRITE_2I }, true });
        result.add(new Object[]{ "2i+sai: multi-table logged batch", noPrep, multiTableLoggedBatch, new String[]{ WRITE_2I, INDEX_WRITE_2I, WRITE_SAI, INDEX_WRITE_SAI }, true });
        result.add(new Object[]{ "2i+sai: multi-table unlogged batch", noPrep, multiTableUnloggedBatch, new String[]{ WRITE_2I, INDEX_WRITE_2I, WRITE_SAI, INDEX_WRITE_SAI }, true });
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
        result.add(new Object[]{ "sai: insert (insertRow path)", noPrep, write, new String[]{ WRITE_SAI, INDEX_WRITE_SAI }, true });
        result.add(new Object[]{ "sai: logged batch insert", noPrep, loggedBatch, new String[]{ WRITE_SAI, INDEX_WRITE_SAI }, true });
        result.add(new Object[]{ "sai: unlogged batch insert", noPrep, unloggedBatch, new String[]{ WRITE_SAI, INDEX_WRITE_SAI }, true });
        result.add(new Object[]{ "sai: update (updateRow path)", new String[]{ write }, writeUpdate, new String[]{ WRITE_SAI, INDEX_WRITE_SAI }, true });
        result.add(new Object[]{ "sai: logged batch update", new String[]{ loggedBatch }, loggedBatchUpdate, new String[]{ WRITE_SAI, INDEX_WRITE_SAI }, true });
        result.add(new Object[]{ "sai: unlogged batch update", new String[]{ unloggedBatch }, unloggedBatchUpdate, new String[]{ WRITE_SAI, INDEX_WRITE_SAI }, true });
        result.add(new Object[]{ "sai: CAS IF NOT EXISTS (insertRow path)", noPrep, casInsert, new String[]{ WRITE_SAI, READ_SAI, READ_LATENCY_TIER_SAI, INDEX_WRITE_SAI }, true });
        result.add(new Object[]{ "sai: CAS IF condition (updateRow path)", new String[]{ write }, cas, new String[]{ WRITE_SAI, READ_SAI, READ_LATENCY_TIER_SAI, INDEX_WRITE_SAI }, true });
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
        result.add(new Object[]{ "sai collection: insert",  new String[0],                   collectionWrite,  new String[]{ WRITE_COL, INDEX_WRITE_COL }, true  });
        result.add(new Object[]{ "sai collection: update",  new String[]{ collectionWrite },  collectionUpdate, new String[]{ WRITE_COL, INDEX_WRITE_COL }, true  });
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
        result.add(new Object[]{ "2i cond batch: IF NOT EXISTS (insertRow)", noPrep, conditionalBatch2iInsert, new String[]{ WRITE_2I, READ_2I, READ_LATENCY_TIER_2I, INDEX_WRITE_2I }, true });
        result.add(new Object[]{ "2i cond batch: IF condition (updateRow)", new String[]{ prep2i }, conditionalBatch2iUpdate, new String[]{ WRITE_2I, READ_2I, READ_LATENCY_TIER_2I, INDEX_WRITE_2I }, true });
        result.add(new Object[]{ "2i cond batch: multi-stmt same partition", noPrep, conditionalBatch2iMultiStmt, new String[]{ WRITE_2I, READ_2I, READ_LATENCY_TIER_2I, INDEX_WRITE_2I }, true });
        result.add(new Object[]{ "sai cond batch: IF NOT EXISTS (insertRow)", noPrep, conditionalBatchSaiInsert, new String[]{ WRITE_SAI, READ_SAI, READ_LATENCY_TIER_SAI, INDEX_WRITE_SAI }, true });
        result.add(new Object[]{ "sai cond batch: IF condition (updateRow)", new String[]{ prepSai }, conditionalBatchSaiUpdate, new String[]{ WRITE_SAI, READ_SAI, READ_LATENCY_TIER_SAI, INDEX_WRITE_SAI }, true });
        result.add(new Object[]{ "sai cond batch: multi-stmt same partition", noPrep, conditionalBatchSaiMultiStmt, new String[]{ WRITE_SAI, READ_SAI, READ_LATENCY_TIER_SAI, INDEX_WRITE_SAI }, true });
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
     * Routes through {@link #executeWithResultNoPaging} when {@link #paging} is {@code false},
     * otherwise through {@link #executeWithResult}.
     */
    private Map<String, ByteBuffer> executeTest(boolean propagateViaNativeProtocol) throws Throwable
    {
        AtomicReference<Map<String, ByteBuffer>> customPayload = new AtomicReference<>();
        for (String prepQuery : this.prepQueries)
            cluster.coordinator(1).execute(prepQuery, ConsistencyLevel.ALL);
        // work around serializability of @Parameterized.Parameter by providing a locally scoped variable
        String query = this.testQuery;
        boolean paging = this.paging;
        // The cluster is shared across scenarios, so SENSORS_VIA_NATIVE_PROTOCOL must be set inside the node's
        // classloader via runOnInstance rather than on the outer test JVM — the node won't see outer JVM property changes.
        // Any methods used inside the runOnInstance() block should be static, otherwise java.io.NotSerializableException will be thrown
        cluster.get(1).acceptsOnInstance(
               (IIsolatedExecutor.SerializableConsumer<AtomicReference<Map<String, ByteBuffer>>>)
               (reference) -> {
                   CassandraRelevantProperties.SENSORS_VIA_NATIVE_PROTOCOL.setBoolean(propagateViaNativeProtocol);
                   ResultMessage<?> result = paging ? executeWithResult(query) : executeWithResultNoPaging(query);
                   reference.set(result.getCustomPayload());
               })
               .accept(customPayload);
        return customPayload.get();
    }

    private double getBytesForHeader(Map<String, ByteBuffer> customPayload, String expectedHeader)
    {
        Assertions.assertThat(customPayload).describedAs("Expected header %s not found in custom payload", expectedHeader).containsKey(expectedHeader);
        return ByteBufferUtil.toDouble(customPayload.get(expectedHeader));
    }

    /** TODO: update SimpleQueryResult in the dtest-api project to expose custom payload and use Coordinator##executeWithResult instead */
    private static ResultMessage<?> executeWithResult(String query)
    {
        return executeWithResult(query, PageSize.inRows(512));
    }

    /**
     * Like {@link #executeWithResult(String)} but passes {@link PageSize#NONE} so that
     * {@code canSkipPaging} returns {@code true} and the distributed non-paging path
     * ({@code execute(ReadQuery,...)}) is taken instead of the paging path.
     */
    private static ResultMessage<?> executeWithResultNoPaging(String query)
    {
        return executeWithResult(query, PageSize.NONE);
    }

    private static ResultMessage<?> executeWithResult(String query, PageSize pageSize)
    {
        long nanoTime = System.nanoTime();
        QueryHandler.Prepared prepared = QueryProcessor.prepareInternal(query);
        org.apache.cassandra.db.ConsistencyLevel cl = org.apache.cassandra.db.ConsistencyLevel.fromCode(ConsistencyLevel.valueOf(CONSISTENCY_LEVEL.name()).ordinal());
        QueryOptions options = QueryOptions.create(cl, null, false, pageSize, null, null, ProtocolVersion.CURRENT, prepared.keyspace);
        return prepared.statement.execute(QueryProcessor.internalQueryState(), options, nanoTime);
    }

    /**
     * Verifies that enabling tracing on a CQL INSERT does not inflate the user table's {@code WRITE_BYTES_REQUEST}
     * sensor in the response custom payload.
     *
     * <p>Trace writes are submitted to {@code Stage.TRACING} with an isolated {@link org.apache.cassandra.sensors.RequestSensors}
     * instance. The {@code StorageProxy.mutate()} call on that thread creates its own {@code system_traces} sensor set
     * and never touches the user table's sensor set on the coordinator thread. Therefore the sensor payload for a traced
     * INSERT must be byte-for-byte identical to that of an untraced INSERT against the same table.
     */
    @Test
    public void testTraceWriteDoesNotInflateSensors() throws Throwable
    {
        cluster.schemaChange(withKeyspace("CREATE TABLE IF NOT EXISTS %s.tbl_trace_isolation (pk int PRIMARY KEY, v1 text)"));

        String insert = withKeyspace("INSERT INTO %s.tbl_trace_isolation (pk, v1) VALUES (1, 'hello')");
        String expectedHeader = "WRITE_BYTES_REQUEST." + KEYSPACE + ".tbl_trace_isolation";

        // Untraced INSERT — baseline WRITE_BYTES for the user table.
        AtomicReference<Map<String, ByteBuffer>> refNoTrace = new AtomicReference<>();
        cluster.get(1).acceptsOnInstance(
               (IIsolatedExecutor.SerializableConsumer<AtomicReference<Map<String, ByteBuffer>>>)
               (reference) -> {
                   CassandraRelevantProperties.SENSORS_VIA_NATIVE_PROTOCOL.setBoolean(true);
                   reference.set(executeWithResult(insert).getCustomPayload());
               })
               .accept(refNoTrace);
        Map<String, ByteBuffer> payloadNoTrace = refNoTrace.get();
        Assertions.assertThat(payloadNoTrace)
                  .describedAs("Untraced INSERT must carry sensor payload")
                  .isNotNull()
                  .containsKey(expectedHeader);
        double writeBytesNoTrace = ByteBufferUtil.toDouble(payloadNoTrace.get(expectedHeader));
        Assertions.assertThat(writeBytesNoTrace)
                  .describedAs("Untraced INSERT WRITE_BYTES must be > 0")
                  .isGreaterThan(0D);

        // Traced INSERT — Tracing.instance.newSession() is called inside the node's classloader so the
        // TRACING thread-local is set correctly on the node side. The trace writes to system_traces must
        // not appear in the user table's sensor payload.
        // Use Object[] to carry both the custom payload map and the session UUID out of the node's
        // isolated classloader in a single acceptsOnInstance call. Two-element array: [0] = payload
        // map, [1] = session UUID. AtomicReference set inside the lambda only updates the node-side
        // copy, so we use the consumer argument itself to ferry data back to the outer JVM.
        Object[] tracedResult = new Object[2];
        cluster.get(1).acceptsOnInstance(
               (IIsolatedExecutor.SerializableConsumer<Object[]>)
               (result) -> {
                   CassandraRelevantProperties.SENSORS_VIA_NATIVE_PROTOCOL.setBoolean(true);
                   // Ensure stopSession() waits for all Stage.TRACING futures so that
                   // system_traces.sessions is durable before we query it after the call.
                   int prevTimeout = TraceStateImpl.WAIT_FOR_PENDING_EVENTS_TIMEOUT_SECS;
                   TraceStateImpl.WAIT_FOR_PENDING_EVENTS_TIMEOUT_SECS = 60;
                   UUID sessionId = UUIDGen.getTimeUUID();
                   result[1] = sessionId;
                   Tracing.instance.newSession(ClientState.forInternalCalls(), sessionId, Collections.emptyMap());
                   // begin() writes the system_traces.sessions row — mirrors what QueryMessage does
                   // before executing the CQL statement in the native protocol handler.
                   Tracing.instance.begin("Execute CQL3 query", null, Collections.emptyMap());
                   try
                   {
                       result[0] = executeWithResult(insert).getCustomPayload();
                   }
                   finally
                   {
                       // stopSession() calls waitForPendingEvents(), which (with the timeout set above)
                       // blocks until all Stage.TRACING futures complete — guaranteeing the sessions
                       // row is written before we query system_traces below.
                       Tracing.instance.stopSession();
                       TraceStateImpl.WAIT_FOR_PENDING_EVENTS_TIMEOUT_SECS = prevTimeout;
                   }
               })
               .accept(tracedResult);
        @SuppressWarnings("unchecked")
        Map<String, ByteBuffer> payloadTraced = (Map<String, ByteBuffer>) tracedResult[0];
        UUID sessionId = (UUID) tracedResult[1];
        Assertions.assertThat(payloadTraced)
                  .describedAs("Traced INSERT must carry sensor payload")
                  .isNotNull()
                  .containsKey(expectedHeader);
        double writeBytesTraced = ByteBufferUtil.toDouble(payloadTraced.get(expectedHeader));

        // Verify tracing was genuinely active: system_traces.sessions must contain a row for
        // our session_id, written by TraceStateImpl when the query started.
        AtomicBoolean traceSessionExists = new AtomicBoolean(false);
        cluster.get(1).acceptsOnInstance(
               (IIsolatedExecutor.SerializableConsumer<AtomicBoolean>)
               (flag) -> {
                   UntypedResultSet rows = QueryProcessor.executeInternal(
                           "SELECT session_id FROM " + SchemaConstants.TRACE_KEYSPACE_NAME + '.' + TraceKeyspace.SESSIONS + " WHERE session_id = ?",
                           sessionId);
                   flag.set(rows != null && !rows.isEmpty());
               })
               .accept(traceSessionExists);
        Assertions.assertThat(traceSessionExists.get())
                  .describedAs("system_traces.sessions must contain a row for session %s — confirms tracing was active", sessionId)
                  .isTrue();

        // Trace writes go to system_traces on a separate TRACING thread with an isolated sensor set.
        // The user table's WRITE_BYTES must be unchanged — not inflated by trace writes.
        Assertions.assertThat(writeBytesTraced)
                  .describedAs("Enabling tracing must not inflate WRITE_BYTES for the user table")
                  .isEqualTo(writeBytesNoTrace);
    }
}
