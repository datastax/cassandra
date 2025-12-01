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

package org.apache.cassandra.distributed.test.sai;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.index.sai.SAITester;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

public class AnalyzerDistributedTest extends TestBaseImpl
{
    @Rule
    public SAITester.FailureWatcher failureRule = new SAITester.FailureWatcher();

    private static final String CREATE_KEYSPACE = "CREATE KEYSPACE IF NOT EXISTS %%s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': %d}";
    private static final int NUM_REPLICAS = 3;
    private static final int RF = 2;

    private static final AtomicInteger seq = new AtomicInteger();
    private static String table;

    private static Cluster cluster;

    @BeforeClass
    public static void setupCluster() throws Exception
    {
        cluster = Cluster.build(NUM_REPLICAS)
                         .withConfig(config -> config.with(GOSSIP).with(NETWORK))
                         .start();

        cluster.schemaChange(withKeyspace(String.format(CREATE_KEYSPACE, RF)));
    }

    @AfterClass
    public static void closeCluster()
    {
        if (cluster != null)
            cluster.close();
    }

    @Before
    public void before()
    {
        table = "table_" + seq.getAndIncrement();
    }

    @After
    public void after()
    {
        cluster.schemaChange(formatQuery("DROP TABLE IF EXISTS %s"));
    }

    @Test
    public void testAnalyzerSearch()
    {
        cluster.schemaChange(formatQuery("CREATE TABLE %s (pk int PRIMARY KEY, not_analyzed int, val text)"));
        cluster.schemaChange(formatQuery("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex' WITH OPTIONS = {'index_analyzer': 'standard'}"));
        SAIUtil.waitForIndexQueryableOnFirstNode(cluster, KEYSPACE);

        var iterations = 15000;
        for (int i = 0; i < iterations; i++)
        {
            var x = i % 100;
            if (i % 100 == 0)
            {
                execute(String.format(
                "INSERT INTO %s (pk, not_analyzed, val) VALUES (%s, %s, '%s')",
                KEYSPACE + '.' + table, i, x, "this will be tokenized"));
            }
            else if (i % 2 == 0)
            {
                execute(String.format(
                "INSERT INTO %s (pk, not_analyzed, val) VALUES (%s, %s, '%s')",
                KEYSPACE + '.' + table, i, x, "this is different"));
            }
            else
            {
                execute(String.format(
                "INSERT INTO %s (pk, not_analyzed, val) VALUES (%s, %s, '%s')",
                KEYSPACE + '.' + table, i, x, "basic test"));
            }
        }
        // We match the first inserted statement here, and that one is just written 1/100 times
        var result = execute("SELECT * FROM %s WHERE val : 'tokenized'");
        assertThat(result).hasNumberOfRows(iterations / 100);
        // We match the first and second inserted statements here, and those account for 1/2 the inserts
        result = execute("SELECT * FROM %s WHERE val : 'this'");
        assertThat(result).hasNumberOfRows(iterations / 2);
        // We match the last write here, and that accounts for the other 1/2 of the inserts
        result = execute("SELECT * FROM %s WHERE val : 'test'");
        assertThat(result).hasNumberOfRows(iterations / 2);
    }

    /**
     * See CNDB-12739 for more details.
     */
    @Test
    public void testIndexAndQueryAnalyzerSearch()
    {
        cluster.schemaChange(formatQuery("CREATE TABLE %s (c1 int PRIMARY KEY , c2 text)"));
        cluster.schemaChange(formatQuery("CREATE CUSTOM INDEX ON %s(c2) USING 'StorageAttachedIndex' WITH OPTIONS = {" +
                                         "'index_analyzer': '{" +
                                         "  \"tokenizer\" : { \"name\" : \"whitespace\", \"args\" : {} }," +
                                         "  \"filters\" : [ { \"name\" : \"lowercase\", \"args\": {} }, " +
                                         "                  { \"name\" : \"edgengram\", \"args\": { \"minGramSize\":\"1\", \"maxGramSize\":\"30\" } }]," +
                                         "  \"charFilters\" : []}', " +
                                         "'query_analyzer': '{" +
                                         "  \"tokenizer\" : { \"name\" : \"whitespace\", \"args\" : {} }," +
                                         "  \"filters\" : [ {\"name\" : \"lowercase\",\"args\": {}} ]}'}"));
        SAIUtil.waitForIndexQueryableOnFirstNode(cluster, KEYSPACE);

        execute("INSERT INTO %s(c1,c2) VALUES (1, 'astra quick fox')");
        execute("INSERT INTO %s(c1,c2) VALUES (2, 'astra quick foxes')");
        execute("INSERT INTO %s(c1,c2) VALUES (3, 'astra1')");
        execute("INSERT INTO %s(c1,c2) VALUES (4, 'astra4 -1@a#')");

        Object[][] result = execute("SELECT * FROM %s WHERE c2 :'ast' ");
        assertThat(result).hasNumberOfRows(4);
    }

    @Test
    public void testEdgeNgramFilterWithOR() throws Throwable
    {
        cluster.schemaChange(formatQuery("CREATE TABLE %s (id text PRIMARY KEY, val text)"));
        cluster.schemaChange(formatQuery("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex' WITH OPTIONS = {" +
                                         "'index_analyzer': '{\n" +
                                         "\t\"tokenizer\":{\"name\":\"standard\", \"args\":{}}," +
                                         "\t\"filters\":[{\"name\":\"lowercase\", \"args\":{}}, " +
                                         "{\"name\":\"edgengram\", \"args\":{\"minGramSize\":\"2\", \"maxGramSize\":\"30\"}}],\n" +
                                         "\t\"charFilters\":[]" +
                                         "}'};"));
        SAIUtil.waitForIndexQueryableOnFirstNode(cluster, KEYSPACE);

        execute("INSERT INTO %s (id, val) VALUES ('1', 'MAL0133AU')");
        execute("INSERT INTO %s (id, val) VALUES ('2', 'WFS2684AU')");
        execute("INSERT INTO %s (id, val) VALUES ('3', 'FPWMCR005 some other word')");
        execute("INSERT INTO %s (id, val) VALUES ('4', 'WFS7093AU')");
        execute("INSERT INTO %s (id, val) VALUES ('5', 'WFS0565AU')");

        beforeAndAfterFlush(cluster, KEYSPACE, () -> {

            // match (:)
            assertEquals(1, execute("SELECT val FROM %s WHERE val : 'MAL0133AU'").length);
            assertEquals(1, execute("SELECT val FROM %s WHERE val : 'WFS2684AU'").length);
            assertEquals(0, execute("SELECT val FROM %s WHERE val : ''").length);
            assertEquals(2, execute("SELECT val FROM %s WHERE val : 'MAL0133AU' OR val : 'WFS2684AU'").length);
            assertEquals(1, execute("SELECT val FROM %s WHERE val : '' OR val : 'WFS2684AU'").length);
            assertEquals(0, execute("SELECT val FROM %s WHERE val : '' AND val : 'WFS2684AU'").length);

            // equals (=)
            assertEquals(1, execute("SELECT val FROM %s WHERE val = 'MAL0133AU'").length);
            assertEquals(1, execute("SELECT val FROM %s WHERE val = 'WFS2684AU'").length);
            assertEquals(0, execute("SELECT val FROM %s WHERE val = ''").length);
            assertEquals(2, execute("SELECT val FROM %s WHERE val = 'MAL0133AU' OR val = 'WFS2684AU'").length);
            assertEquals(1, execute("SELECT val FROM %s WHERE val = '' OR val = 'WFS2684AU'").length);
            assertEquals(0, execute("SELECT val FROM %s WHERE val = '' AND val = 'WFS2684AU'").length);

            // mixed match (:) and equals (=)
            assertEquals(2, execute("SELECT val FROM %s WHERE val = 'MAL0133AU' OR val : 'WFS2684AU'").length);
            assertEquals(1, execute("SELECT val FROM %s WHERE val = '' OR val : 'WFS2684AU'").length);
            assertEquals(0, execute("SELECT val FROM %s WHERE val = '' AND val : 'WFS2684AU'").length);
            assertEquals(2, execute("SELECT val FROM %s WHERE val : 'MAL0133AU' OR val = 'WFS2684AU'").length);
            assertEquals(1, execute("SELECT val FROM %s WHERE val : '' OR val = 'WFS2684AU'").length);
            assertEquals(0, execute("SELECT val FROM %s WHERE val : '' AND val = 'WFS2684AU'").length);
        });
    }

    @Test
    public void testNgramFilterWithOR() throws Throwable
    {
        cluster.schemaChange(formatQuery("CREATE TABLE %s (id text PRIMARY KEY, val text)"));
        cluster.schemaChange(formatQuery("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex' WITH OPTIONS = {" +
                    "'index_analyzer': '{\n" +
                    "\t\"tokenizer\":{\"name\":\"standard\", \"args\":{}}," +
                    "\t\"filters\":[{\"name\":\"lowercase\", \"args\":{}}, " +
                    "{\"name\":\"ngram\", \"args\":{\"minGramSize\":\"2\", \"maxGramSize\":\"30\"}}],\n" +
                    "\t\"charFilters\":[]" +
                    "}'};"));
        SAIUtil.waitForIndexQueryableOnFirstNode(cluster, KEYSPACE);

        execute("INSERT INTO %s (id, val) VALUES ('1', 'MAL0133AU')");
        execute("INSERT INTO %s (id, val) VALUES ('2', 'WFS2684AU')");
        execute("INSERT INTO %s (id, val) VALUES ('3', 'FPWMCR005 some other words')");
        execute("INSERT INTO %s (id, val) VALUES ('4', 'WFS7093AU')");
        execute("INSERT INTO %s (id, val) VALUES ('5', 'WFS0565AU')");

        beforeAndAfterFlush(cluster, KEYSPACE, () -> {

            // match (:)
            assertEquals(1, execute("SELECT val FROM %s WHERE val : 'MAL0133AU'").length);
            assertEquals(1, execute("SELECT val FROM %s WHERE val : 'WFS2684AU'").length);
            assertEquals(1, execute("SELECT val FROM %s WHERE val : '268'").length);
            assertEquals(2, execute("SELECT val FROM %s WHERE val : 'MAL0133AU' OR val : 'WFS2684AU'").length);
            assertEquals(2, execute("SELECT val FROM %s WHERE val : '133' OR val : 'WFS2684AU'").length);
            assertEquals(1, execute("SELECT val FROM %s WHERE val : 'MAL' AND val : 'AU'").length);
            assertEquals(0, execute("SELECT val FROM %s WHERE val : 'XYZ' AND val : 'AU'").length);

            // equals (=)
            assertEquals(1, execute("SELECT val FROM %s WHERE val = 'MAL0133AU'").length);
            assertEquals(1, execute("SELECT val FROM %s WHERE val = 'WFS2684AU'").length);
            assertEquals(1, execute("SELECT val FROM %s WHERE val = '268'").length);
            assertEquals(2, execute("SELECT val FROM %s WHERE val = 'MAL0133AU' OR val = 'WFS2684AU'").length);
            assertEquals(2, execute("SELECT val FROM %s WHERE val = '133' OR val = 'WFS2684AU'").length);
            assertEquals(1, execute("SELECT val FROM %s WHERE val = 'MAL' AND val = 'AU'").length);
            assertEquals(0, execute("SELECT val FROM %s WHERE val = 'XYZ' AND val = 'AU'").length);

            // mixed match (:) and equals (=)
            assertEquals(2, execute("SELECT val FROM %s WHERE val : 'MAL0133AU' OR val = 'WFS2684AU'").length);
            assertEquals(2, execute("SELECT val FROM %s WHERE val : '133' OR val = 'WFS2684AU'").length);
            assertEquals(1, execute("SELECT val FROM %s WHERE val : 'MAL' AND val = 'AU'").length);
            assertEquals(0, execute("SELECT val FROM %s WHERE val : 'XYZ' AND val = 'AU'").length);
            assertEquals(2, execute("SELECT val FROM %s WHERE val = 'MAL0133AU' OR val : 'WFS2684AU'").length);
            assertEquals(2, execute("SELECT val FROM %s WHERE val = '133' OR val : 'WFS2684AU'").length);
            assertEquals(1, execute("SELECT val FROM %s WHERE val = 'MAL' AND val : 'AU'").length);
            assertEquals(0, execute("SELECT val FROM %s WHERE val = 'XYZ' AND val : 'AU'").length);
        });
    }

    private static Object[][] execute(String query)
    {
        return cluster.coordinator(1).execute(formatQuery(query), ConsistencyLevel.QUORUM);
    }

    private static String formatQuery(String query)
    {
        return String.format(query, KEYSPACE + '.' + table);
    }
}
