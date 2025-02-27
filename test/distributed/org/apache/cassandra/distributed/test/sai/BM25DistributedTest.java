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

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.index.sai.disk.format.Version;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.assertj.core.api.Assertions.assertThat;

public class BM25DistributedTest extends TestBaseImpl
{
    private static final String CREATE_KEYSPACE = "CREATE KEYSPACE %%s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': %d}";
    private static final String CREATE_TABLE = "CREATE TABLE %s (k int PRIMARY KEY, v text)";
    private static final String CREATE_INDEX = "CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex' WITH OPTIONS = {'index_analyzer': '{\"tokenizer\" : {\"name\" : \"standard\"}, \"filters\" : [{\"name\" : \"porterstem\"}]}'}";

    // To get consistent results from BM25 we need to know which docs are evaluated, the easiest way
    // to do that is to put all the docs on every replica
    private static final int NUM_NODES = 3;
    private static final int RF = 3;

    private static Cluster cluster;
    private static String table;

    private static final AtomicInteger seq = new AtomicInteger();

    @BeforeClass
    public static void setupCluster() throws Exception
    {
        cluster = Cluster.build(NUM_NODES)
                        .withTokenCount(1)
                        .withDataDirCount(1)
                        .withConfig(config -> config.with(GOSSIP).with(NETWORK))
                        .start();

        cluster.schemaChange(withKeyspace(String.format(CREATE_KEYSPACE, RF)));
        cluster.forEach(i -> i.runOnInstance(() -> org.apache.cassandra.index.sai.SAIUtil.setLatestVersion(Version.EC)));
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
        cluster.schemaChange(formatQuery(CREATE_TABLE));
        cluster.schemaChange(formatQuery(CREATE_INDEX));
        SAIUtil.waitForIndexQueryable(cluster, KEYSPACE);
    }

    @Test
    public void testTermFrequencyOrdering()
    {
        // Insert documents with varying frequencies of the term "apple"
        execute("INSERT INTO %s (k, v) VALUES (1, 'apple')");
        execute("INSERT INTO %s (k, v) VALUES (2, 'apple apple')");
        execute("INSERT INTO %s (k, v) VALUES (3, 'apple apple apple')");

        // Query memtable index
        assertBM25Ordering();

        // Flush and query on-disk index
        cluster.forEach(n -> n.flush(KEYSPACE));
        assertBM25Ordering();
    }

    private void assertBM25Ordering()
    {
        Object[][] result = execute("SELECT k FROM %s ORDER BY v BM25 OF 'apple' LIMIT 3");
        assertThat(result).hasNumberOfRows(3);
        
        // Results should be ordered by term frequency (highest to lowest)
        assertThat((Integer) result[0][0]).isEqualTo(3); // 3 occurrences
        assertThat((Integer) result[1][0]).isEqualTo(2); // 2 occurrences
        assertThat((Integer) result[2][0]).isEqualTo(1); // 1 occurrence
    }

    private static Object[][] execute(String query)
    {
        return execute(query, ConsistencyLevel.QUORUM);
    }

    private static Object[][] execute(String query, ConsistencyLevel consistencyLevel)
    {
        return cluster.coordinator(1).execute(formatQuery(query), consistencyLevel);
    }

    private static String formatQuery(String query)
    {
        return String.format(query, KEYSPACE + '.' + table);
    }
}
