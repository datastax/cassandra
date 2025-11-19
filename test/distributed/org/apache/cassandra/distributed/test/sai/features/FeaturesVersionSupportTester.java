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
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import org.junit.AfterClass;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.distributed.test.sai.SAIUtil;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.assertj.core.api.Assertions;

import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.ONE;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;

/**
 * Tests the availabilty of features in different versions of the SAI on-disk format in a multinode, multiversion cluster.
 * </p>
 * This is done with a 2-node cluster where the first node has the newer version of the SAI on-disk format and the
 * second node has the older version being tested configured as the default current version.
 * </p>
 * Additionally, there will be a keyspace per version configured to use that version on the second node instead of the
 * default one.
 */
public abstract class FeaturesVersionSupportTester extends TestBaseImpl
{
    private static final Map<String, Version> VERSIONS_PER_KEYSPACE = Version.ALL.stream().collect(Collectors.toMap(v -> "ks_" + v.toString(), v -> v));

    private static Version version;
    private static Cluster cluster;

    // To be called by inheritors in a method annotated with @BeforeClass
    protected static void initCluster(Version version) throws IOException
    {
        // Clean up any existing cluster from a previous test
        if (cluster != null)
        {
            cluster.close();
            cluster = null;
        }
        
        FeaturesVersionSupportTester.version = version;

        cluster = init(Cluster.build(2)
                              .withInstanceInitializer((cl, n) -> {
                                  // Disable synthetic score to ensure compatibility between ED and older versions.
                                  // ED enables it by default but older versions don't support it.
                                  // Note: In 5.0, Ordering.useSyntheticScore() uses CassandraRelevantProperties
                                  // which caches values at class loading time, so we must set this property before
                                  // the classes are loaded. In the main branch, Ordering uses System.getProperty() directly,
                                  // which is why this workaround isn't needed there.
                                  // checkstyle: suppress below 'blockSystemPropertyUsage'
                                  System.setProperty("cassandra.sai.ann_use_synthetic_score", "false");
                              })
                              .withConfig(config -> config.with(GOSSIP).with(NETWORK))
                              .start(), 1);

        // The first node, which will be the coordinator in all tests, uses the latest version for all keyspaces
        cluster.get(1).runOnInstance(() -> org.apache.cassandra.index.sai.SAIUtil.setCurrentVersion(Version.LATEST));

        // The second node will use the tested version as the default, and specific versions for known keyspaces
        String versionString = version.toString();
        cluster.get(2).runOnInstance(() -> org.apache.cassandra.index.sai.SAIUtil.setCurrentVersion(Version.parse(versionString), VERSIONS_PER_KEYSPACE));

        for (String keyspace : VERSIONS_PER_KEYSPACE.keySet())
            cluster.schemaChange("CREATE KEYSPACE " + keyspace + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");

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
        if (cluster != null)
        {
            cluster.close();
            cluster = null;
        }
        version = null;
    }
    
    /**
     * Test that vector indexes are supported with on-disk format versions from {@link Version#CA}.
     * Nodes using older versions should fail their index build, although the index will still exist.
     */
    @Test
    public void testANN()
    {
        test(this::testANN);
    }

    private void testANN(String keyspace, Version version)
    {
        cluster.schemaChange("CREATE TABLE " + keyspace + ".ann (k int PRIMARY KEY, v vector<float, 2>)");

        ICoordinator coordinator = cluster.coordinator(1);
        coordinator.execute("INSERT INTO " + keyspace + ".ann (k, v) VALUES (0, [1.0, 2.0])", ALL);
        coordinator.execute("INSERT INTO " + keyspace + ".ann (k, v) VALUES (1, [2.0, 3.0])", ALL);
        coordinator.execute("INSERT INTO " + keyspace + ".ann (k, v) VALUES (2, [3.0, 4.0])", ALL);
        coordinator.execute("INSERT INTO " + keyspace + ".ann (k, v) VALUES (3, [4.0, 5.0])", ALL);
        cluster.forEach(node -> node.flush(keyspace));

        String createIndexQuery = "CREATE CUSTOM INDEX ann_idx ON " + keyspace + ".ann(v) USING 'StorageAttachedIndex' " +
                                  "WITH OPTIONS = {'similarity_function' : 'euclidean'}";
        String annSelectQuery = "SELECT k FROM " + keyspace + ".ann ORDER BY v ANN OF [2.5, 3.5] LIMIT 3";
        String geoSelectQuery = "SELECT k FROM " + keyspace + ".ann WHERE GEO_DISTANCE(v, [2.5, 3.5]) < 157000";

        if (version.onOrAfter(Version.JVECTOR_EARLIEST))
        {
            cluster.schemaChange(createIndexQuery);
            SAIUtil.waitForIndexQueryableOnFirstNode(cluster, keyspace, "ann_idx");
            Assertions.assertThat(coordinator.execute(annSelectQuery, ONE)).hasNumberOfRows(3);
            Assertions.assertThat(coordinator.execute(geoSelectQuery, ONE)).hasNumberOfRows(2);
        }
        else
        {
            // The on-disk format version in node 2 is too old to support indexes on vector columns.
            cluster.schemaChange(createIndexQuery);
            SAIUtil.assertIndexBuildFailed(cluster.get(1), cluster.get(2), keyspace, "ann_idx");
        }
    }

    /**
     * Test that BM25 queries are supported with on-disk format versions from {@link Version#EC}.
     * Older versions should reject BM25 queries with {@link RequestFailureReason#FEATURE_NEEDS_INDEX_REBUILD} errors.
     */
    @Test
    public void testBM25()
    {
        test(this::testBM25);
    }

    private void testBM25(String keyspace, Version version)
    {
        cluster.schemaChange("CREATE TABLE " + keyspace + ".bm25 (k int PRIMARY KEY, v text)");

        ICoordinator coordinator = cluster.coordinator(1);
        coordinator.execute("INSERT INTO " + keyspace + ".bm25 (k, v) VALUES (1, 'apple')", ALL);
        coordinator.execute("INSERT INTO " + keyspace + ".bm25 (k, v) VALUES (2, 'orange')", ALL);
        coordinator.execute("INSERT INTO " + keyspace + ".bm25 (k, v) VALUES (3, 'banana')", ALL);
        coordinator.execute("INSERT INTO " + keyspace + ".bm25 (k, v) VALUES (4, 'kiwi')", ALL);
        cluster.forEach(node -> node.flush(keyspace));

        cluster.schemaChange("CREATE CUSTOM INDEX bm25_idx ON " + keyspace + ".bm25(v) " +
                             "USING 'org.apache.cassandra.index.sai.StorageAttachedIndex' " +
                             "WITH OPTIONS = {" +
                             "'index_analyzer': '{" +
                             "\"tokenizer\" : {\"name\" : \"standard\"}, " +
                             "\"filters\" : [{\"name\" : \"porterstem\"}]}'}");
        SAIUtil.waitForIndexQueryableOnFirstNode(cluster, keyspace, "bm25_idx");

        String query = "SELECT k FROM " + keyspace + ".bm25 ORDER BY v BM25 OF 'apple' LIMIT 3";

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
        test(this::testIndexAnalyzer);
    }

    private void testIndexAnalyzer(String keyspace, Version version)
    {
        cluster.schemaChange("CREATE TABLE " + keyspace + ".analyzer (k int PRIMARY KEY, v text)");

        ICoordinator coordinator = cluster.coordinator(1);
        coordinator.execute("INSERT INTO " + keyspace + ".analyzer (k, v) VALUES (0, 'Quick fox')", ALL);
        coordinator.execute("INSERT INTO " + keyspace + ".analyzer (k, v) VALUES (1, 'Lazy dogs')", ALL);
        cluster.forEach(node -> node.flush(keyspace));

        cluster.schemaChange("CREATE CUSTOM INDEX analyzer_idx ON " + keyspace + ".analyzer(v)" +
                             " USING 'org.apache.cassandra.index.sai.StorageAttachedIndex'" +
                             "WITH OPTIONS = { 'index_analyzer': 'standard' }");
        SAIUtil.waitForIndexQueryableOnFirstNode(cluster, keyspace, "analyzer_idx");

        Assertions.assertThat(coordinator.execute("SELECT * FROM " + keyspace + ".analyzer WHERE v = 'dogs'", ONE))
                  .hasNumberOfRows(1);
    }

    /**
     * Test that query-time analyzers are supported with all on-disk format versions.
     */
    @Test
    public void testQueryAnalyzer()
    {
        test(this::testQueryAnalyzer);
    }

    private void testQueryAnalyzer(String keyspace, Version version)
    {
        cluster.schemaChange("CREATE TABLE " + keyspace + ".q_analyzer (k int PRIMARY KEY, v text)");

        ICoordinator coordinator = cluster.coordinator(1);
        coordinator.execute("INSERT INTO " + keyspace + ".q_analyzer (k, v) VALUES (1, 'astra quick fox')", ALL);
        coordinator.execute("INSERT INTO " + keyspace + ".q_analyzer (k, v) VALUES (2, 'astra2 quick fox')", ALL);
        coordinator.execute("INSERT INTO " + keyspace + ".q_analyzer (k, v) VALUES (3, 'astra3 quick foxes')", ALL);
        cluster.forEach(node -> node.flush(keyspace));

        cluster.schemaChange("CREATE CUSTOM INDEX q_analyzer_idx ON " + keyspace + ".q_analyzer(v) " +
                             "USING 'StorageAttachedIndex' WITH OPTIONS = {" +
                             "'index_analyzer': '{" +
                             "  \"tokenizer\" : { \"name\" : \"whitespace\", \"args\" : {} }," +
                             "  \"filters\" : [ { \"name\" : \"lowercase\", \"args\": {} }, " +
                             "                  { \"name\" : \"edgengram\", \"args\": { \"minGramSize\":\"1\", \"maxGramSize\":\"30\" } }]," +
                             "  \"charFilters\" : []}', " +
                             "'query_analyzer': '{" +
                             "  \"tokenizer\" : { \"name\" : \"whitespace\", \"args\" : {} }," +
                             "  \"filters\" : [ {\"name\" : \"lowercase\",\"args\": {}} ]}'}");
        SAIUtil.waitForIndexQueryableOnFirstNode(cluster, keyspace, "q_analyzer_idx");

        String query = "SELECT k FROM " + keyspace + ".q_analyzer WHERE v : ";
        Assertions.assertThat(coordinator.execute(query + "'ast'", ONE)).hasNumberOfRows(3);
        Assertions.assertThat(coordinator.execute(query + "'astra'", ONE)).hasNumberOfRows(3);
        Assertions.assertThat(coordinator.execute(query + "'astra2'", ONE)).hasNumberOfRows(1);
        Assertions.assertThat(coordinator.execute(query + "'fox'", ONE)).hasNumberOfRows(3);
        Assertions.assertThat(coordinator.execute(query + "'foxes'", ONE)).hasNumberOfRows(1);
    }

    private static void test(BiConsumer<String, Version> testMethod)
    {
        testMethod.accept(KEYSPACE, version);
        VERSIONS_PER_KEYSPACE.forEach(testMethod);
    }
}
