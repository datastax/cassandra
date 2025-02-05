/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.test.TestBaseImpl;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for {@code query_analyzer} in distributed mode. See CNDB-12739 for more details.
 */
public class QueryAnalyzerDistributedTest extends TestBaseImpl
{
    private static final String CREATE_KEYSPACE = "CREATE KEYSPACE %%s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': %d}";
    private static final String CREATE_TABLE = "CREATE TABLE %s (c1 int PRIMARY KEY , c2 text)";
    private static final String CREATE_INDEX = "CREATE CUSTOM INDEX ON %%s(%s) USING 'StorageAttachedIndex' WITH OPTIONS = {" +
                                               "'index_analyzer': '{" +
                                               "  \"tokenizer\" : { \"name\" : \"whitespace\", \"args\" : {} }," +
                                               "  \"filters\" : [ { \"name\" : \"lowercase\", \"args\": {} }, " +
                                               "                  { \"name\" : \"edgengram\", \"args\": { \"minGramSize\":\"1\", \"maxGramSize\":\"30\" } }]," +
                                               "  \"charFilters\" : []}', " +
                                               "'query_analyzer': '{" +
                                               "  \"tokenizer\" : { \"name\" : \"whitespace\", \"args\" : {} }," +
                                               "  \"filters\" : [ {\"name\" : \"lowercase\",\"args\": {}} ]}'}";
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
        cluster.schemaChange(formatQuery(CREATE_TABLE));
        cluster.schemaChange(formatQuery(String.format(CREATE_INDEX, "c2")));
        SAIUtil.waitForIndexQueryable(cluster, KEYSPACE);

        execute("insert into %s(c1,c2) values(1, 'astra quick fox')");
        execute("insert into %s(c1,c2) values(2, 'astra quick foxes')");
        execute("insert into %s(c1,c2) values(3, 'astra1')");
        execute("insert into %s(c1,c2) values(4, 'astra4 -1@a#')");

        Object[][] result = execute("select * from %s where c2 :'ast' ");
        assertThat(result).hasNumberOfRows(4);
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
