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

package org.apache.cassandra.index.sai.cql;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.SAIUtil;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.assertj.core.api.Assertions;

import static java.lang.String.format;

/**
 * Tests the {@link Version.Selector} used by {@link Version#current(String)} to verify that each keyspace can
 * independently use a different current version, and that the current version of a keyspace can be changed so future
 * index builds will use the new version.
 */
@RunWith(Parameterized.class)
public class VersionSelectorTest extends SAITester
{
    public static final String TABLE = "tbl";
    private static final Logger logger = LoggerFactory.getLogger(VersionSelectorTest.class);

    @Parameterized.Parameter
    public Version globalCurrentVersion;

    @Parameterized.Parameters(name = "globalCurrentVersion={0}")
    public static Collection<Object[]> data()
    {
        return Version.ALL.stream().map(v -> new Object[]{ v }).collect(Collectors.toList());
    }

    @Test
    public void testNumericOnSkinnyTable()
    {
        test(keyspace -> {
            createTable(keyspace, "CREATE TABLE %s (k int PRIMARY KEY, v int)");
            createIndex(keyspace, "CREATE CUSTOM INDEX IF NOT EXISTS ON %s(v) USING 'StorageAttachedIndex'");
            execute(keyspace, "INSERT INTO %s (k, v) VALUES (0, 0)");
            execute(keyspace, "INSERT INTO %s (k, v) VALUES (1, 1)");
        }).withQueries(keyspace -> {
            assertRowCount(keyspace, "SELECT * FROM %s WHERE v>=0", 2);
            assertRowCount(keyspace, "SELECT * FROM %s WHERE v>0", 1);
        });
    }

    @Test
    public void testNumericOnWideTable()
    {
        test(keyspace -> {
            createTable(keyspace, "CREATE TABLE %s (k int, c int, v int, PRIMARY KEY (k, c))");
            createIndex(keyspace, "CREATE CUSTOM INDEX IF NOT EXISTS ON %s(v) USING 'StorageAttachedIndex'");
            execute(keyspace, "INSERT INTO %s (k, c, v) VALUES (0, 0, 0)");
            execute(keyspace, "INSERT INTO %s (k, c, v) VALUES (0, 1, 1)");
            execute(keyspace, "INSERT INTO %s (k, c, v) VALUES (1, 0, 0)");
            execute(keyspace, "INSERT INTO %s (k, c, v) VALUES (1, 1, 2)");
        }).withQueries(keyspace -> {
            assertRowCount(keyspace, "SELECT * FROM %s WHERE v>=0", 4);
            assertRowCount(keyspace, "SELECT * FROM %s WHERE v>0", 2);
            assertRowCount(keyspace, "SELECT * FROM %s WHERE k = 0 AND v>=0", 2);
            assertRowCount(keyspace, "SELECT * FROM %s WHERE k = 0 AND v>0", 1);
        });
    }

    @Test
    public void testLiteralOnSkinnyTable()
    {
        test(keyspace -> {
            createTable(keyspace, "CREATE TABLE %s (k int PRIMARY KEY, v text)");
            createIndex(keyspace, "CREATE CUSTOM INDEX IF NOT EXISTS ON %s(v) USING 'StorageAttachedIndex'");
            execute(keyspace, "INSERT INTO %s (k, v) VALUES (0, '0')");
            execute(keyspace, "INSERT INTO %s (k, v) VALUES (1, '0')");
            execute(keyspace, "INSERT INTO %s (k, v) VALUES (2, '1')");
        }).withQueries(keyspace -> {
            assertRowCount(keyspace, "SELECT * FROM %s WHERE v='0'", 2);
            assertRowCount(keyspace, "SELECT * FROM %s WHERE v='1'", 1);
            assertRowCount(keyspace, "SELECT * FROM %s WHERE v='2'", 0);
        });
    }

    @Test
    public void testLiteralOnWideTable()
    {
        test(keyspace -> {
            createTable(keyspace, "CREATE TABLE %s (k int, c int, v text, PRIMARY KEY (k, c))");
            createIndex(keyspace, "CREATE CUSTOM INDEX IF NOT EXISTS ON %s(v) USING 'StorageAttachedIndex'");
            execute(keyspace, "INSERT INTO %s (k, c, v) VALUES (0, 0, '0')");
            execute(keyspace, "INSERT INTO %s (k, c, v) VALUES (0, 1, '0')");
            execute(keyspace, "INSERT INTO %s (k, c, v) VALUES (0, 2, '1')");
            execute(keyspace, "INSERT INTO %s (k, c, v) VALUES (1, 0, '0')");
            execute(keyspace, "INSERT INTO %s (k, c, v) VALUES (1, 1, '0')");
            execute(keyspace, "INSERT INTO %s (k, c, v) VALUES (1, 2, '1')");
        }).withQueries(keyspace -> {
            assertRowCount(keyspace, "SELECT * FROM %s WHERE v='0'", 4);
            assertRowCount(keyspace, "SELECT * FROM %s WHERE v='1'", 2);
            assertRowCount(keyspace, "SELECT * FROM %s WHERE v='2'", 0);
            assertRowCount(keyspace, "SELECT * FROM %s WHERE k = 0 AND v='0'", 2);
            assertRowCount(keyspace, "SELECT * FROM %s WHERE k = 0 AND v='1'", 1);
            assertRowCount(keyspace, "SELECT * FROM %s WHERE k = 0 AND v='2'", 0);
        });
    }

    @Test
    public void testQueryAnalyzersOnSkinnyTable()
    {
        test(keyspace -> {
            createTable(keyspace, "CREATE TABLE %s (k int PRIMARY KEY, v text)");
            createIndex(keyspace, "CREATE CUSTOM INDEX IF NOT EXISTS ON %s(v) USING 'StorageAttachedIndex' " +
                               "WITH OPTIONS = {" +
                               "'index_analyzer': '{" +
                               "  \"tokenizer\" : { \"name\" : \"whitespace\", \"args\" : {} }," +
                               "  \"filters\" : [ { \"name\" : \"lowercase\", \"args\": {} }, " +
                               "                  { \"name\" : \"edgengram\", \"args\": { \"minGramSize\":\"1\", \"maxGramSize\":\"30\" } }]," +
                               "  \"charFilters\" : []}', " +
                               "'query_analyzer': '{" +
                               "  \"tokenizer\" : { \"name\" : \"whitespace\", \"args\" : {} }," +
                               "  \"filters\" : [ {\"name\" : \"lowercase\",\"args\": {}} ]}'}");
            execute(keyspace, "INSERT INTO %s (k, v) VALUES (1, 'astra quick fox')");
            execute(keyspace, "INSERT INTO %s (k, v) VALUES (2, 'astra2 quick fox')");
            execute(keyspace, "INSERT INTO %s (k, v) VALUES (3, 'astra3 quick foxes')");
        }).withQueries(keyspace -> {
            assertRowCount(keyspace, "SELECT * FROM %s WHERE v='ast'", 3);
            assertRowCount(keyspace, "SELECT * FROM %s WHERE v='astra'", 3);
            assertRowCount(keyspace, "SELECT * FROM %s WHERE v='astra2'", 1);
            assertRowCount(keyspace, "SELECT * FROM %s WHERE v='fox'", 3);
            assertRowCount(keyspace, "SELECT * FROM %s WHERE v='foxes'", 1);
        });
    }

    @Test
    public void testQueryAnalyzersOnWideTable()
    {
        test(keyspace -> {
            createTable(keyspace, "CREATE TABLE %s (k int, c int, v text, PRIMARY KEY (k, c))");
            createIndex(keyspace, "CREATE CUSTOM INDEX IF NOT EXISTS ON %s(v) USING 'StorageAttachedIndex' " +
                               "WITH OPTIONS = {" +
                               "'index_analyzer': '{" +
                               "  \"tokenizer\" : { \"name\" : \"whitespace\", \"args\" : {} }," +
                               "  \"filters\" : [ { \"name\" : \"lowercase\", \"args\": {} }, " +
                               "                  { \"name\" : \"edgengram\", \"args\": { \"minGramSize\":\"1\", \"maxGramSize\":\"30\" } }]," +
                               "  \"charFilters\" : []}', " +
                               "'query_analyzer': '{" +
                               "  \"tokenizer\" : { \"name\" : \"whitespace\", \"args\" : {} }," +
                               "  \"filters\" : [ {\"name\" : \"lowercase\",\"args\": {}} ]}'}");
            execute(keyspace, "INSERT INTO %s (k, c, v) VALUES (0, 1, 'astra quick fox')");
            execute(keyspace, "INSERT INTO %s (k, c, v) VALUES (0, 2, 'astra2 quick fox')");
            execute(keyspace, "INSERT INTO %s (k, c, v) VALUES (0, 3, 'astra3 quick foxes')");
            execute(keyspace, "INSERT INTO %s (k, c, v) VALUES (1, 1, 'astra quick fox')");
            execute(keyspace, "INSERT INTO %s (k, c, v) VALUES (1, 2, 'astra2 quick fox')");
            execute(keyspace, "INSERT INTO %s (k, c, v) VALUES (1, 3, 'astra3 quick foxes')");
        }).withQueries(keyspace -> {
            assertRowCount(keyspace, "SELECT * FROM %s WHERE v='ast'", 6);
            assertRowCount(keyspace, "SELECT * FROM %s WHERE v='astra'", 6);
            assertRowCount(keyspace, "SELECT * FROM %s WHERE v='astra2'", 2);
            assertRowCount(keyspace, "SELECT * FROM %s WHERE v='fox'", 6);
            assertRowCount(keyspace, "SELECT * FROM %s WHERE v='foxes'", 2);
            assertRowCount(keyspace, "SELECT * FROM %s WHERE k = 0 AND v='ast'", 3);
            assertRowCount(keyspace, "SELECT * FROM %s WHERE k = 0 AND v='astra'", 3);
            assertRowCount(keyspace, "SELECT * FROM %s WHERE k = 0 AND v='astra2'", 1);
            assertRowCount(keyspace, "SELECT * FROM %s WHERE k = 0 AND v='fox'", 3);
            assertRowCount(keyspace, "SELECT * FROM %s WHERE k = 0 AND v='foxes'", 1);
        });
    }

    @Test
    public void testANNOnSkinnyTable()
    {
        test(keyspace -> {
            createTable(keyspace, "CREATE TABLE %s (k int PRIMARY KEY, v vector<float, 2>)");
            createIndex(keyspace, "CREATE CUSTOM INDEX IF NOT EXISTS ON %s(v) USING 'StorageAttachedIndex' " +
                               "WITH OPTIONS = {'similarity_function' : 'euclidean'}");
            execute(keyspace, "INSERT INTO %s (k, v) VALUES (0, [1.0, 2.0])");
            execute(keyspace, "INSERT INTO %s (k, v) VALUES (1, [2.0, 3.0])");
            execute(keyspace, "INSERT INTO %s (k, v) VALUES (2, [3.0, 4.0])");
            execute(keyspace, "INSERT INTO %s (k, v) VALUES (3, [4.0, 5.0])");
        }, Version.JVECTOR_EARLIEST, "JVector is not supported in V2OnDiskFormat").withQueries(keyspace -> {
            assertRowCount(keyspace, "SELECT k FROM %s ORDER BY v ANN OF [2.5, 3.5] LIMIT 3", 3);
            assertRowCount(keyspace, "SELECT k FROM %s WHERE GEO_DISTANCE(v, [2.5, 3.5]) < 157000", 2);
        }, Version.JVECTOR_EARLIEST, "INDEX_NOT_AVAILABLE");
    }

    @Test
    public void testANNOnWideTable()
    {
        test(keyspace -> {
            createTable(keyspace, "CREATE TABLE %s (k int, c int, v vector<float, 2>, PRIMARY KEY (k, c))");
            createIndex(keyspace, "CREATE CUSTOM INDEX IF NOT EXISTS ON %s(v) USING 'StorageAttachedIndex' " +
                               "WITH OPTIONS = {'similarity_function' : 'euclidean'}");
            execute(keyspace, "INSERT INTO %s (k, c, v) VALUES (0, 0, [1.0, 2.0])");
            execute(keyspace, "INSERT INTO %s (k, c, v) VALUES (0, 1, [2.0, 3.0])");
            execute(keyspace, "INSERT INTO %s (k, c, v) VALUES (0, 2, [3.0, 4.0])");
            execute(keyspace, "INSERT INTO %s (k, c, v) VALUES (0, 3, [4.0, 5.0])");
            execute(keyspace, "INSERT INTO %s (k, c, v) VALUES (1, 0, [1.0, 2.0])");
            execute(keyspace, "INSERT INTO %s (k, c, v) VALUES (1, 1, [2.0, 3.0])");
            execute(keyspace, "INSERT INTO %s (k, c, v) VALUES (1, 2, [3.0, 4.0])");
            execute(keyspace, "INSERT INTO %s (k, c, v) VALUES (1, 3, [4.0, 5.0])");
        }, Version.JVECTOR_EARLIEST, "JVector is not supported in V2OnDiskFormat").withQueries(keyspace -> {
            assertRowCount(keyspace, "SELECT k FROM %s ORDER BY v ANN OF [2.5, 3.5] LIMIT 3", 3);
            assertRowCount(keyspace, "SELECT k FROM %s WHERE GEO_DISTANCE(v, [2.5, 3.5]) < 157000", 4);
            assertRowCount(keyspace, "SELECT k FROM %s WHERE k=0 ORDER BY v ANN OF [2.5, 3.5] LIMIT 3", 3);
            assertRowCount(keyspace, "SELECT k FROM %s WHERE k=0 AND GEO_DISTANCE(v, [2.5, 3.5]) < 157000", 2);
        }, Version.JVECTOR_EARLIEST, "INDEX_NOT_AVAILABLE");
    }

    @Test
    public void testBM25OnSkinnyTable()
    {
        test(keyspace -> {
            createTable(keyspace, "CREATE TABLE %s (k int PRIMARY KEY, v text)");
            createIndex(keyspace, "CREATE CUSTOM INDEX IF NOT EXISTS ON %s(v) USING 'StorageAttachedIndex' " +
                               "WITH OPTIONS = {" +
                               "'index_analyzer': '{" +
                               "\"tokenizer\" : {\"name\" : \"standard\"}, " +
                               "\"filters\" : [{\"name\" : \"porterstem\"}]}'}");
            execute(keyspace, "INSERT INTO %s (k, v) VALUES (1, 'apple')");
            execute(keyspace, "INSERT INTO %s (k, v) VALUES (2, 'orange')");
            execute(keyspace, "INSERT INTO %s (k, v) VALUES (3, 'banana')");
            execute(keyspace, "INSERT INTO %s (k, v) VALUES (4, 'apple')");
        }).withQueries(keyspace -> {
            assertRowCount(keyspace, "SELECT k FROM %s ORDER BY v BM25 OF 'apple' LIMIT 3", 2);
            assertRowCount(keyspace, "SELECT k FROM %s ORDER BY v BM25 OF 'orange' LIMIT 3", 1);
            assertRowCount(keyspace, "SELECT k FROM %s ORDER BY v BM25 OF 'kiwi' LIMIT 3", 0);
        }, Version.BM25_EARLIEST, "FEATURE_NEEDS_INDEX_REBUILD");
    }

    @Test
    public void testBM25OnWideTable()
    {
        test(keyspace -> {
            createTable(keyspace, "CREATE TABLE %s (k int, c int, v text, PRIMARY KEY (k, c))");
            createIndex(keyspace, "CREATE CUSTOM INDEX IF NOT EXISTS ON %s(v) USING 'StorageAttachedIndex' " +
                               "WITH OPTIONS = {" +
                               "'index_analyzer': '{" +
                               "\"tokenizer\" : {\"name\" : \"standard\"}, " +
                               "\"filters\" : [{\"name\" : \"porterstem\"}]}'}");
            execute(keyspace, "INSERT INTO %s (k, c, v) VALUES (0, 1, 'apple')");
            execute(keyspace, "INSERT INTO %s (k, c, v) VALUES (0, 2, 'orange')");
            execute(keyspace, "INSERT INTO %s (k, c, v) VALUES (0, 3, 'banana')");
            execute(keyspace, "INSERT INTO %s (k, c, v) VALUES (0, 4, 'apple')");
            execute(keyspace, "INSERT INTO %s (k, c, v) VALUES (1, 1, 'apple')");
            execute(keyspace, "INSERT INTO %s (k, c, v) VALUES (1, 2, 'orange')");
            execute(keyspace, "INSERT INTO %s (k, c, v) VALUES (1, 3, 'banana')");
            execute(keyspace, "INSERT INTO %s (k, c, v) VALUES (1, 4, 'apple')");
        }).withQueries(keyspace -> {
            assertRowCount(keyspace, "SELECT k FROM %s ORDER BY v BM25 OF 'apple' LIMIT 3", 3);
            assertRowCount(keyspace, "SELECT k FROM %s ORDER BY v BM25 OF 'orange' LIMIT 3", 2);
            assertRowCount(keyspace, "SELECT k FROM %s ORDER BY v BM25 OF 'kiwi' LIMIT 3", 0);
            assertRowCount(keyspace, "SELECT k FROM %s WHERE k=0 ORDER BY v BM25 OF 'apple' LIMIT 3", 2);
            assertRowCount(keyspace, "SELECT k FROM %s WHERE k=0 ORDER BY v BM25 OF 'orange' LIMIT 3", 1);
            assertRowCount(keyspace, "SELECT k FROM %s WHERE k=0 ORDER BY v BM25 OF 'kiwi' LIMIT 3", 0);
        }, Version.BM25_EARLIEST, "FEATURE_NEEDS_INDEX_REBUILD");
    }

    @Override
    protected String createTable(String keyspace, String query)
    {
        return super.createTable(keyspace, query, TABLE);
    }

    @Override
    protected String createIndex(String keyspace, String query)
    {
        return super.createIndex(keyspace, format(query, keyspace + '.' + TABLE));
    }

    private UntypedResultSet execute(String keyspace, String query)
    {
        return execute(format(query, keyspace + '.' + TABLE));
    }

    private void assertRowCount(String keyspace, String query, int expected)
    {
        assertRowCount(execute(keyspace, query), expected);
    }

    /**
     * @param schemaCreator the initial schema and data
     */
    private Tester test(Consumer<String> schemaCreator)
    {
        return test(schemaCreator, Version.AA, null);
    }

    /**
     * @param schemaCreator the initial schema and data
     * @param minVersionForIndexing the minimum index version required to support creating an index with the tested features
     * @param indexingErrorIfUnsupported the error message expected during indexing if the index version does not support the tested features
     */
    private Tester test(Consumer<String> schemaCreator, Version minVersionForIndexing, String indexingErrorIfUnsupported)
    {
        return new Tester(schemaCreator, minVersionForIndexing, indexingErrorIfUnsupported);
    }

    private class Tester
    {
        private final Consumer<String> schemaCreator;
        private final Version minVersionForIndexing;
        private final String indexingErrorIfUnsupported;

        /**
         * @param schemaCreator the initial schema and data
         * @param minVersionForIndexing the minimum index version required to support creating an index with the tested features
         * @param indexingErrorIfUnsupported the error message expected during indexing if the index version does not support the tested features
         */
        Tester(Consumer<String> schemaCreator,
               Version minVersionForIndexing,
               String indexingErrorIfUnsupported)
        {
            // we can skip the test case if the starting version does not support the tested index
            Assume.assumeTrue(globalCurrentVersion.onOrAfter(minVersionForIndexing));

            this.minVersionForIndexing = minVersionForIndexing;
            this.indexingErrorIfUnsupported = indexingErrorIfUnsupported;
            this.schemaCreator = schemaCreator;
        }

        /**
         * @param queryResultsVerifier the queries to run and verify the results
         */
        void withQueries(Consumer<String> queryResultsVerifier)
        {
            withQueries(queryResultsVerifier, Version.AA, null);
        }

        /**
         * @param queryResultsVerifier the queries to run and verify the results
         * @param minVersionForQuerying the minimum index version required to support the tested queries
         * @param queryingErrorIfUnsupported the error message expected if the index version does not support the tested queries
         */
        void withQueries(Consumer<String> queryResultsVerifier,
                         Version minVersionForQuerying,
                         String queryingErrorIfUnsupported)
        {
            // we can skip the test case if the starting version does not support the tested queries
            Assume.assumeTrue(globalCurrentVersion.onOrAfter(minVersionForQuerying));

            // set the default current version, without any per-keyspace overrides
            SAIUtil.setCurrentVersion(globalCurrentVersion);

            // create a series of keyspaces, all of them using the initial default current version,
            // and verify the queries and the version of the index
            Map<String, Version> versionsPerKeyspace = new HashMap<>();
            for (Version version : Version.ALL)
            {
                // create and track the keyspace for the index version we will later upgrade to
                String keyspace = "ks_" + version;
                createKeyspace("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}", keyspace);
                versionsPerKeyspace.put(keyspace, version);

                // create the schema and data, still using the initial version, and verify the queries
                schemaCreator.accept(keyspace);
                disableCompaction(keyspace, TABLE);
                flush(keyspace, TABLE);
                verifySAIVersionInUse(globalCurrentVersion, keyspace, TABLE);
                queryResultsVerifier.accept(keyspace);
            }

            // assign a new version to each tested keyspace, so we end up with a different version for each keyspace,
            // although their index files are still in the initial version
            SAIUtil.setCurrentVersion(globalCurrentVersion, versionsPerKeyspace);

            // try to rebuild the indexes of each keyspace, which should leave them in their new version,
            // and verify the queries and the version of the index
            versionsPerKeyspace.forEach((keyspace, version) -> {
                logger.debug("Testing index rebuild from {} to {} in keyspace {}", globalCurrentVersion, version, keyspace);

                verifySAIVersionInUse(globalCurrentVersion, keyspace, TABLE);

                // Verify if we can rebuild the index, depending on whether the new version supports it
                passOrFail(() -> {
                    rebuildTableIndexes(keyspace, TABLE);
                    reloadSSTableIndexInPlace(keyspace, TABLE);
                    verifySAIVersionInUse(version, keyspace, TABLE);
                }, version.onOrAfter(minVersionForIndexing), indexingErrorIfUnsupported);

                // Verify if we can query the index, depending on whether the new version supports the queries
                passOrFail(() -> queryResultsVerifier.accept(keyspace),
                           version.onOrAfter(minVersionForQuerying),
                           queryingErrorIfUnsupported);
            });
        }

        private void passOrFail(Runnable r, boolean shouldPass, String message)
        {
            if (shouldPass)
                r.run();
            else
                Assertions.assertThatThrownBy(r::run).hasMessageContaining(message);
        }
    }
}
