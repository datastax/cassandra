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

package org.apache.cassandra.distributed.test.sai.features;

import java.io.IOException;

import org.junit.AfterClass;
import org.junit.Test;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.FixedValue;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.distributed.test.sai.SAIUtil;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.index.FeatureNeedsIndexRebuildException;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.assertj.core.api.Assertions;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.ONE;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;

/**
 * Tests the availabilty of features in different versions of the SAI on-disk format in a multinode, multiversion cluster.
 * </p>
 * This is done with a 2-node cluster where the first node has the newer version of the SAI on-disk format and the
 * second node has the older version being tested.
 */
public abstract class FeaturesVersionSupportTester extends TestBaseImpl
{
    private static Version version;
    private static Cluster cluster;

    // To be called by inheritors in a method annotated with @BeforeClass
    protected static void initCluster(Version version) throws IOException
    {
        FeaturesVersionSupportTester.version = version;
        cluster = init(Cluster.build(2)
                              .withInstanceInitializer((cl, n) -> BB.install(cl, n, version))
                              .withConfig(config -> config.with(GOSSIP).with(NETWORK))
                              .start(), 1);
        
        // Set a comprehensive exception filter that covers all expected exceptions
        // When testing with older SAI versions, index build failures can occur on either node
        cluster.setUncaughtExceptionsFilter((i, t) -> {
            String className = t.getClass().getName();
            String message = t.getMessage() != null ? t.getMessage() : "";
            
            // Also check the cause if the exception is wrapped
            Throwable cause = t.getCause();
            String causeMessage = cause != null && cause.getMessage() != null ? cause.getMessage() : "";
            
            // Filter index build failures wrapped in RuntimeException or ExecutionException
            if ((t instanceof RuntimeException || t instanceof java.util.concurrent.ExecutionException) && 
                (message.contains("Failed to update views on column indexes") ||
                 causeMessage.contains("Failed to update views on column indexes")))
                return true;
                
            // Filter FeatureNeedsIndexRebuildException for both vector and BM25
            if (className.contains("FeatureNeedsIndexRebuildException") &&
                (message.contains("does not support vector indexes") ||
                 message.contains("does not support BM25 scoring")))
                return true;
                
            return false;
        });
    }

    @AfterClass
    public static void cleanup()
    {
        cluster.close();
    }
    
    /**
     * Test that vector indexes are supported with on-disk format versions from {@link Version#CA}.
     * Nodes using older versions should fail their index build, although the index will still exist.
     */
    @Test
    public void testANN()
    {
        cluster.schemaChange(withKeyspace("CREATE TABLE %s.ann (k int PRIMARY KEY, v vector<float, 2>)"));

        ICoordinator coordinator = cluster.coordinator(1);
        coordinator.execute(withKeyspace("INSERT INTO %s.ann (k, v) VALUES (0, [1.0, 2.0])"), ALL);
        coordinator.execute(withKeyspace("INSERT INTO %s.ann (k, v) VALUES (1, [2.0, 3.0])"), ALL);
        coordinator.execute(withKeyspace("INSERT INTO %s.ann (k, v) VALUES (2, [3.0, 4.0])"), ALL);
        coordinator.execute(withKeyspace("INSERT INTO %s.ann (k, v) VALUES (3, [4.0, 5.0])"), ALL);
        cluster.forEach(node -> node.flush(KEYSPACE));

        String createIndexQuery = withKeyspace("CREATE CUSTOM INDEX ann_idx ON %s.ann(v) USING 'StorageAttachedIndex'" +
                                               " WITH OPTIONS = {'similarity_function' : 'euclidean'}");
        String annSelectQuery = withKeyspace("SELECT k FROM %s.ann ORDER BY v ANN OF [2.5, 3.5] LIMIT 3");
        String geoSelectQuery = withKeyspace("SELECT k FROM %s.ann WHERE GEO_DISTANCE(v, [2.5, 3.5]) < 157000");

        if (version.onOrAfter(Version.JVECTOR_EARLIEST))
        {
            cluster.schemaChange(createIndexQuery);
            SAIUtil.waitForIndexQueryable(cluster, KEYSPACE, "ann_idx");
            Assertions.assertThat(coordinator.execute(annSelectQuery, ONE)).hasNumberOfRows(3);
            Assertions.assertThat(coordinator.execute(geoSelectQuery, ONE)).hasNumberOfRows(2);
        }
        else
        {
            // The on-disk format version in node 2 is too old to support indexes on vector columns.
            cluster.schemaChange(createIndexQuery);
            SAIUtil.assertIndexBuildFailed(cluster.get(1), cluster.get(2), KEYSPACE, "ann_idx");
        }
    }

    /**
     * Test that BM25 queries are supported with on-disk format versions from {@link Version#EC}.
     * Older versions should reject BM25 queries with {@link RequestFailureReason#FEATURE_NEEDS_INDEX_REBUILD} errors.
     */
    @Test
    public void testBM25()
    {
        cluster.schemaChange(withKeyspace("CREATE TABLE %s.bm25 (k int PRIMARY KEY, v text)"));

        ICoordinator coordinator = cluster.coordinator(1);
        coordinator.execute(withKeyspace("INSERT INTO %s.bm25 (k, v) VALUES (1, 'apple')"), ALL);
        coordinator.execute(withKeyspace("INSERT INTO %s.bm25 (k, v) VALUES (2, 'orange')"), ALL);
        coordinator.execute(withKeyspace("INSERT INTO %s.bm25 (k, v) VALUES (3, 'banana')"), ALL);
        coordinator.execute(withKeyspace("INSERT INTO %s.bm25 (k, v) VALUES (4, 'kiwi')"), ALL);
        cluster.forEach(node -> node.flush(KEYSPACE));

        cluster.schemaChange(withKeyspace("CREATE CUSTOM INDEX bm25_idx ON %s.bm25(v) " +
                                          "USING 'org.apache.cassandra.index.sai.StorageAttachedIndex' " +
                                          "WITH OPTIONS = {" +
                                          "'index_analyzer': '{" +
                                          "\"tokenizer\" : {\"name\" : \"standard\"}, " +
                                          "\"filters\" : [{\"name\" : \"porterstem\"}]}'}"));
        SAIUtil.waitForIndexQueryable(cluster, KEYSPACE, "bm25_idx");

        String query = withKeyspace("SELECT k FROM %s.bm25 ORDER BY v BM25 OF 'apple' LIMIT 3");

        if (version.onOrAfter(Version.BM25_EARLIEST))
        {
            Assertions.assertThat(coordinator.execute(query, ONE)).hasNumberOfRows(1);
        }
        else
        {
            Assertions.assertThatThrownBy(() -> coordinator.execute(query, ONE))
                      .hasMessageContaining(RequestFailureReason.FEATURE_NEEDS_INDEX_REBUILD.name());
        }
    }

    /**
     * Test that index-time analyzers are supported with all on-disk format versions.
     */
    @Test
    public void testIndexAnalyzer()
    {
        cluster.schemaChange(withKeyspace("CREATE TABLE %s.analyzer (k int PRIMARY KEY, v text)"));

        ICoordinator coordinator = cluster.coordinator(1);
        coordinator.execute(withKeyspace("INSERT INTO %s.analyzer (k, v) VALUES (0, 'Quick fox')"), ALL);
        coordinator.execute(withKeyspace("INSERT INTO %s.analyzer (k, v) VALUES (1, 'Lazy dogs')"), ALL);
        cluster.forEach(node -> node.flush(KEYSPACE));

        cluster.schemaChange(withKeyspace("CREATE CUSTOM INDEX analyzer_idx ON %s.analyzer(v)" +
                                          " USING 'org.apache.cassandra.index.sai.StorageAttachedIndex'" +
                                          "WITH OPTIONS = { 'index_analyzer': 'standard' }"));
        SAIUtil.waitForIndexQueryable(cluster, KEYSPACE, "analyzer_idx");

        Assertions.assertThat(coordinator.execute(withKeyspace("SELECT * FROM %s.analyzer WHERE v = 'dogs'"), ONE))
                  .hasNumberOfRows(1);
    }

    /**
     * Test that query-time analyzers are supported with all on-disk format versions.
     */
    @Test
    public void testQueryAnalyzer()
    {
        cluster.schemaChange(withKeyspace("CREATE TABLE %s.q_analyzer (k int PRIMARY KEY, v text)"));

        ICoordinator coordinator = cluster.coordinator(1);
        coordinator.execute(withKeyspace("INSERT INTO %s.q_analyzer (k, v) VALUES (1, 'astra quick fox')"), ALL);
        coordinator.execute(withKeyspace("INSERT INTO %s.q_analyzer (k, v) VALUES (2, 'astra2 quick fox')"), ALL);
        coordinator.execute(withKeyspace("INSERT INTO %s.q_analyzer (k, v) VALUES (3, 'astra3 quick foxes')"), ALL);
        cluster.forEach(node -> node.flush(KEYSPACE));

        cluster.schemaChange(withKeyspace("CREATE CUSTOM INDEX q_analyzer_idx ON %s.q_analyzer(v) USING 'StorageAttachedIndex' WITH OPTIONS = {" +
                                          "'index_analyzer': '{" +
                                          "  \"tokenizer\" : { \"name\" : \"whitespace\", \"args\" : {} }," +
                                          "  \"filters\" : [ { \"name\" : \"lowercase\", \"args\": {} }, " +
                                          "                  { \"name\" : \"edgengram\", \"args\": { \"minGramSize\":\"1\", \"maxGramSize\":\"30\" } }]," +
                                          "  \"charFilters\" : []}', " +
                                          "'query_analyzer': '{" +
                                          "  \"tokenizer\" : { \"name\" : \"whitespace\", \"args\" : {} }," +
                                          "  \"filters\" : [ {\"name\" : \"lowercase\",\"args\": {}} ]}'}"));
        SAIUtil.waitForIndexQueryable(cluster, KEYSPACE, "q_analyzer_idx");

        Assertions.assertThat(coordinator.execute(withKeyspace("SELECT k FROM %s.q_analyzer WHERE v : 'ast'"), ONE))
                  .hasNumberOfRows(3);
        Assertions.assertThat(coordinator.execute(withKeyspace("SELECT k FROM %s.q_analyzer WHERE v : 'astra'"), ONE))
                  .hasNumberOfRows(3);
        Assertions.assertThat(coordinator.execute(withKeyspace("SELECT k FROM %s.q_analyzer WHERE v : 'astra2'"), ONE))
                  .hasNumberOfRows(1);
        Assertions.assertThat(coordinator.execute(withKeyspace("SELECT k FROM %s.q_analyzer WHERE v : 'fox'"), ONE))
                  .hasNumberOfRows(3);
        Assertions.assertThat(coordinator.execute(withKeyspace("SELECT k FROM %s.q_analyzer WHERE v : 'foxes'"), ONE))
                  .hasNumberOfRows(1);
    }

    /**
     * Injection to set the SAI on-disk version on the first one to the most recent one, and the tested version to the second node.
     */
    public static class BB
    {
        @SuppressWarnings("resource")
        public static void install(ClassLoader classLoader, int node, Version version)
        {
            new ByteBuddy().rebase(Version.class)
                           .method(named("currentVersionProperty"))
                           .intercept(FixedValue.value(node == 1 ? Version.LATEST.toString() : version.toString()))
                           .make()
                           .load(classLoader, ClassLoadingStrategy.Default.INJECTION);
        }
    }
}
