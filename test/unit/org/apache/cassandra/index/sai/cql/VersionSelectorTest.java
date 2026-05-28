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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.SAIUtil;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
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

    @Parameterized.Parameter
    public Version globalCurrentVersion;

    @Parameterized.Parameter(1)
    public Operation operation;

    public enum Operation
    {
        CREATE,
        REBUILD
    }

    @Parameterized.Parameters(name = "globalCurrentVersion={0} operation={1}")
    public static Collection<Object[]> data()
    {
        Collection<Object[]> params = new ArrayList<>();
        for (Operation operation : Operation.values())
        {
            for (Version version : Version.ALL)
            {
                params.add(new Object[]{ version, operation });
            }
        }
        return params;
    }

    @Before
    public void before()
    {
        SAIUtil.setCurrentVersion(globalCurrentVersion);
    }

    @Test
    public void testNumericOnSkinnyTable()
    {
        test(keyspace -> {
            createTable(keyspace, "CREATE TABLE %s (k int PRIMARY KEY, v int)");
            execute(keyspace, "INSERT INTO %s (k, v) VALUES (0, 0)");
            execute(keyspace, "INSERT INTO %s (k, v) VALUES (1, 1)");
        }).withIndex(keyspace -> createIndex(keyspace, "CREATE CUSTOM INDEX IF NOT EXISTS ON %s(v) USING 'StorageAttachedIndex'"))
          .withQueries(keyspace -> {
              assertRowCount(keyspace, "SELECT * FROM %s WHERE v>=0", 2);
              assertRowCount(keyspace, "SELECT * FROM %s WHERE v>0", 1);
          });
    }

    @Test
    public void testNumericOnWideTable()
    {
        test(keyspace -> {
            createTable(keyspace, "CREATE TABLE %s (k int, c int, v int, PRIMARY KEY (k, c))");
            execute(keyspace, "INSERT INTO %s (k, c, v) VALUES (0, 0, 0)");
            execute(keyspace, "INSERT INTO %s (k, c, v) VALUES (0, 1, 1)");
            execute(keyspace, "INSERT INTO %s (k, c, v) VALUES (1, 0, 0)");
            execute(keyspace, "INSERT INTO %s (k, c, v) VALUES (1, 1, 2)");
        }).withIndex(keyspace -> createIndex(keyspace, "CREATE CUSTOM INDEX IF NOT EXISTS ON %s(v) USING 'StorageAttachedIndex'"))
          .withQueries(keyspace -> {
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
            execute(keyspace, "INSERT INTO %s (k, v) VALUES (0, '0')");
            execute(keyspace, "INSERT INTO %s (k, v) VALUES (1, '0')");
            execute(keyspace, "INSERT INTO %s (k, v) VALUES (2, '1')");
        }).withIndex(keyspace -> createIndex(keyspace, "CREATE CUSTOM INDEX IF NOT EXISTS ON %s(v) USING 'StorageAttachedIndex'"))
          .withQueries(keyspace -> {
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
            execute(keyspace, "INSERT INTO %s (k, c, v) VALUES (0, 0, '0')");
            execute(keyspace, "INSERT INTO %s (k, c, v) VALUES (0, 1, '0')");
            execute(keyspace, "INSERT INTO %s (k, c, v) VALUES (0, 2, '1')");
            execute(keyspace, "INSERT INTO %s (k, c, v) VALUES (1, 0, '0')");
            execute(keyspace, "INSERT INTO %s (k, c, v) VALUES (1, 1, '0')");
            execute(keyspace, "INSERT INTO %s (k, c, v) VALUES (1, 2, '1')");
        }).withIndex(keyspace -> createIndex(keyspace, "CREATE CUSTOM INDEX IF NOT EXISTS ON %s(v) USING 'StorageAttachedIndex'"))
          .withQueries(keyspace -> {
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
            execute(keyspace, "INSERT INTO %s (k, v) VALUES (1, 'astra quick fox')");
            execute(keyspace, "INSERT INTO %s (k, v) VALUES (2, 'astra2 quick fox')");
            execute(keyspace, "INSERT INTO %s (k, v) VALUES (3, 'astra3 quick foxes')");
        }).withIndex(keyspace -> createIndex(keyspace,
                                             "CREATE CUSTOM INDEX IF NOT EXISTS ON %s(v) USING 'StorageAttachedIndex' " +
                                             "WITH OPTIONS = {" +
                                             "'index_analyzer': '{" +
                                             "  \"tokenizer\" : { \"name\" : \"whitespace\", \"args\" : {} }," +
                                             "  \"filters\" : [ { \"name\" : \"lowercase\", \"args\": {} }, " +
                                             "                  { \"name\" : \"edgengram\", \"args\": { \"minGramSize\":\"1\", \"maxGramSize\":\"30\" } }]," +
                                             "  \"charFilters\" : []}', " +
                                             "'query_analyzer': '{" +
                                             "  \"tokenizer\" : { \"name\" : \"whitespace\", \"args\" : {} }," +
                                             "  \"filters\" : [ {\"name\" : \"lowercase\",\"args\": {}} ]}'}"))
          .withQueries(keyspace -> {
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
            execute(keyspace, "INSERT INTO %s (k, c, v) VALUES (0, 1, 'astra quick fox')");
            execute(keyspace, "INSERT INTO %s (k, c, v) VALUES (0, 2, 'astra2 quick fox')");
            execute(keyspace, "INSERT INTO %s (k, c, v) VALUES (0, 3, 'astra3 quick foxes')");
            execute(keyspace, "INSERT INTO %s (k, c, v) VALUES (1, 1, 'astra quick fox')");
            execute(keyspace, "INSERT INTO %s (k, c, v) VALUES (1, 2, 'astra2 quick fox')");
            execute(keyspace, "INSERT INTO %s (k, c, v) VALUES (1, 3, 'astra3 quick foxes')");
        }).withIndex(keyspace -> createIndex(keyspace,
                                             "CREATE CUSTOM INDEX IF NOT EXISTS ON %s(v) USING 'StorageAttachedIndex' " +
                                             "WITH OPTIONS = {" +
                                             "'index_analyzer': '{" +
                                             "  \"tokenizer\" : { \"name\" : \"whitespace\", \"args\" : {} }," +
                                             "  \"filters\" : [ { \"name\" : \"lowercase\", \"args\": {} }, " +
                                             "                  { \"name\" : \"edgengram\", \"args\": { \"minGramSize\":\"1\", \"maxGramSize\":\"30\" } }]," +
                                             "  \"charFilters\" : []}', " +
                                             "'query_analyzer': '{" +
                                             "  \"tokenizer\" : { \"name\" : \"whitespace\", \"args\" : {} }," +
                                             "  \"filters\" : [ {\"name\" : \"lowercase\",\"args\": {}} ]}'}"))
          .withQueries(keyspace -> {
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
            execute(keyspace, "INSERT INTO %s (k, v) VALUES (0, [1.0, 2.0])");
            execute(keyspace, "INSERT INTO %s (k, v) VALUES (1, [2.0, 3.0])");
            execute(keyspace, "INSERT INTO %s (k, v) VALUES (2, [3.0, 4.0])");
            execute(keyspace, "INSERT INTO %s (k, v) VALUES (3, [4.0, 5.0])");
        }).withIndex(keyspace -> createIndex(keyspace, "CREATE CUSTOM INDEX IF NOT EXISTS ON %s(v) USING 'StorageAttachedIndex' " +
                                                       "WITH OPTIONS = {'similarity_function' : 'euclidean'}"),
                     Version.JVECTOR_EARLIEST,
                     StorageAttachedIndex::vectorUnsupportedByCurrentVersionError,
                     "JVector is not supported in V2OnDiskFormat")
          .withQueries(keyspace -> {
              assertRowCount(keyspace, "SELECT k FROM %s ORDER BY v ANN OF [2.5, 3.5] LIMIT 3", 3);
              assertRowCount(keyspace, "SELECT k FROM %s WHERE GEO_DISTANCE(v, [2.5, 3.5]) < 157000", 2);
          }, Version.JVECTOR_EARLIEST, "INDEX_NOT_AVAILABLE");
    }

    @Test
    public void testANNOnWideTable()
    {
        test(keyspace -> {
            createTable(keyspace, "CREATE TABLE %s (k int, c int, v vector<float, 2>, PRIMARY KEY (k, c))");
            execute(keyspace, "INSERT INTO %s (k, c, v) VALUES (0, 0, [1.0, 2.0])");
            execute(keyspace, "INSERT INTO %s (k, c, v) VALUES (0, 1, [2.0, 3.0])");
            execute(keyspace, "INSERT INTO %s (k, c, v) VALUES (0, 2, [3.0, 4.0])");
            execute(keyspace, "INSERT INTO %s (k, c, v) VALUES (0, 3, [4.0, 5.0])");
            execute(keyspace, "INSERT INTO %s (k, c, v) VALUES (1, 0, [1.0, 2.0])");
            execute(keyspace, "INSERT INTO %s (k, c, v) VALUES (1, 1, [2.0, 3.0])");
            execute(keyspace, "INSERT INTO %s (k, c, v) VALUES (1, 2, [3.0, 4.0])");
            execute(keyspace, "INSERT INTO %s (k, c, v) VALUES (1, 3, [4.0, 5.0])");
        }).withIndex(keyspace -> createIndex(keyspace, "CREATE CUSTOM INDEX IF NOT EXISTS ON %s(v) USING 'StorageAttachedIndex' " +
                                                       "WITH OPTIONS = {'similarity_function' : 'euclidean'}"),
                     Version.JVECTOR_EARLIEST,
                     StorageAttachedIndex::vectorUnsupportedByCurrentVersionError,
                     "JVector is not supported in V2OnDiskFormat")
          .withQueries(keyspace -> {
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
            execute(keyspace, "INSERT INTO %s (k, v) VALUES (1, 'apple')");
            execute(keyspace, "INSERT INTO %s (k, v) VALUES (2, 'orange')");
            execute(keyspace, "INSERT INTO %s (k, v) VALUES (3, 'banana')");
            execute(keyspace, "INSERT INTO %s (k, v) VALUES (4, 'apple')");
        }).withIndex(keyspace -> createIndex(keyspace, "CREATE CUSTOM INDEX IF NOT EXISTS ON %s(v) USING 'StorageAttachedIndex' " +
                                                       "WITH OPTIONS = {" +
                                                       "'index_analyzer': '{" +
                                                       "\"tokenizer\" : {\"name\" : \"standard\"}, " +
                                                       "\"filters\" : [{\"name\" : \"porterstem\"}]}'}"))
          .withQueries(keyspace -> {
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
            execute(keyspace, "INSERT INTO %s (k, c, v) VALUES (0, 1, 'apple')");
            execute(keyspace, "INSERT INTO %s (k, c, v) VALUES (0, 2, 'orange')");
            execute(keyspace, "INSERT INTO %s (k, c, v) VALUES (0, 3, 'banana')");
            execute(keyspace, "INSERT INTO %s (k, c, v) VALUES (0, 4, 'apple')");
            execute(keyspace, "INSERT INTO %s (k, c, v) VALUES (1, 1, 'apple')");
            execute(keyspace, "INSERT INTO %s (k, c, v) VALUES (1, 2, 'orange')");
            execute(keyspace, "INSERT INTO %s (k, c, v) VALUES (1, 3, 'banana')");
            execute(keyspace, "INSERT INTO %s (k, c, v) VALUES (1, 4, 'apple')");
        }).withIndex(keyspace -> createIndex(keyspace, "CREATE CUSTOM INDEX IF NOT EXISTS ON %s(v) USING 'StorageAttachedIndex' " +
                                                       "WITH OPTIONS = {" +
                                                       "'index_analyzer': '{" +
                                                       "\"tokenizer\" : {\"name\" : \"standard\"}, " +
                                                       "\"filters\" : [{\"name\" : \"porterstem\"}]}'}"))
          .withQueries(keyspace -> {
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

    private TestCase test(Consumer<String> tableCreator)
    {
        return Operation.CREATE == operation
               ? new CreateIndexTestCase(tableCreator)
               : new RebuildIndexTestCase(tableCreator);
    }

    private abstract class TestCase
    {
        protected final Map<String, Version> versionsPerKeyspace;
        protected Consumer<String> indexCreator;
        protected Version minVersionForIndexing;
        protected Function<Version, String> createIndexErrorIfUnsupported;
        protected String rebuildIndexErrorIfUnsupported;

        /**
         * @param tableCreator the initial schema and data
         */
        TestCase(Consumer<String> tableCreator)
        {
            tableCreator.accept(KEYSPACE);

            // create one keyspace, table and dataset per index version
            versionsPerKeyspace = new HashMap<>();
            for (Version version : Version.ALL)
            {
                String keyspace = "ks_" + version;
                createKeyspace("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}", keyspace);
                tableCreator.accept(keyspace);
                disableCompaction(keyspace, TABLE);
                flush(keyspace, TABLE);
                versionsPerKeyspace.put(keyspace, version);
            }
        }

        TestCase withIndex(Consumer<String> indexCreator)
        {
            return withIndex(indexCreator, Version.AA, null, null);
        }

        /**
         * @param indexCreator the index creation statement
         * @param minVersionForIndexing the minimum index version required to create or rebuild the index
         * @param createIndexErrorIfUnsupported the error message expected if the index version does not support the tested index
         * @param rebuildIndexErrorIfUnsupported the error message expected if the index version does not support index rebuild
         */
        TestCase withIndex(Consumer<String> indexCreator,
                           Version minVersionForIndexing,
                           Function<Version, String> createIndexErrorIfUnsupported,
                           String rebuildIndexErrorIfUnsupported)
        {
            this.indexCreator = indexCreator;
            this.minVersionForIndexing = minVersionForIndexing;
            this.createIndexErrorIfUnsupported = createIndexErrorIfUnsupported;
            this.rebuildIndexErrorIfUnsupported = rebuildIndexErrorIfUnsupported;
            return this;
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
        abstract void withQueries(Consumer<String> queryResultsVerifier,
                                  Version minVersionForQuerying,
                                  String queryingErrorIfUnsupported);

        protected void verifyQueries(Consumer<String> queryResultsVerifier,
                                     String keyspace,
                                     Version currentVersion,
                                     Version minVersion,
                                     String errorMessage)
        {
            if (currentVersion.onOrAfter(minVersion))
                queryResultsVerifier.accept(keyspace);
            else
                Assertions.assertThatThrownBy(() -> queryResultsVerifier.accept(keyspace))
                          .hasMessageContaining(errorMessage);
        }
    }

    /**
     * Test case for index creation.
     */
    private class CreateIndexTestCase extends TestCase
    {
        CreateIndexTestCase(Consumer<String> tableCreator)
        {
            super(tableCreator);
        }

        @Override
        void withQueries(Consumer<String> queryResultsVerifier,
                         Version minVersionForQuerying,
                         String queryingErrorIfUnsupported)
        {
            // set the per-keyspace current versions
            SAIUtil.setCurrentVersion(globalCurrentVersion, versionsPerKeyspace);

            // verify whether we can create the index in each keyspace,
            // depending on whether the tested version supports it,
            // and whether we can run the tested queries in case the index creation succeeds
            versionsPerKeyspace.forEach((keyspace, version) -> {
                if (version.onOrAfter(minVersionForIndexing))
                {
                    indexCreator.accept(keyspace);
                    verifySAIVersionInUse(keyspace, TABLE, version);
                    verifyQueries(queryResultsVerifier,
                                  keyspace,
                                  version,
                                  minVersionForQuerying,
                                  queryingErrorIfUnsupported);
                }
                else
                {
                    Assertions.assertThatThrownBy(() -> indexCreator.accept(keyspace))
                              .hasMessageContaining(createIndexErrorIfUnsupported.apply(version));
                    verifyNoIndexFiles(keyspace, TABLE);
                }
            });
        }
    }

    /**
     * Test case for index rebuild.
     */
    private class RebuildIndexTestCase extends TestCase
    {
        RebuildIndexTestCase(Consumer<String> tableCreator)
        {
            super(tableCreator);
        }

        @Override
        void withQueries(Consumer<String> queryResultsVerifier,
                         Version minVersionForQuerying,
                         String queryingErrorIfUnsupported)
        {
            // if the starting version does not support the tested queries then we cannot even create the index
            if (!globalCurrentVersion.onOrAfter(minVersionForIndexing))
            {
                Assertions.assertThatThrownBy(() -> indexCreator.accept(KEYSPACE))
                          .hasMessageContaining(createIndexErrorIfUnsupported.apply(globalCurrentVersion));
                verifyNoIndexFiles(KEYSPACE, TABLE);
                return;
            }

            // set the default current version, without any per-keyspace overrides
            SAIUtil.setCurrentVersion(globalCurrentVersion);

            // create the index for each keyspace, which should use the default current version
            versionsPerKeyspace.keySet().forEach(keyspace -> {
                indexCreator.accept(keyspace);
                verifySAIVersionInUse(keyspace, TABLE, globalCurrentVersion);
                verifyQueries(queryResultsVerifier,
                              keyspace,
                              globalCurrentVersion,
                              minVersionForQuerying,
                              queryingErrorIfUnsupported);
            });

            // assign a new version to each tested keyspace,
            // so we end up with a different version for each keyspace,
            // although their index files are still in the initial version
            SAIUtil.setCurrentVersion(globalCurrentVersion, versionsPerKeyspace);

            // try to rebuild the indexes of each keyspace,
            // which should leave them in their new version if it's supported,
            // and verify the queries and the version of the index
            versionsPerKeyspace.forEach((keyspace, version) -> {

                verifySAIVersionInUse(keyspace, TABLE, globalCurrentVersion);

                // Verify if we can rebuild the index, depending on whether the new version supports it
                if (version.onOrAfter(minVersionForIndexing))
                {
                    // If the new version per-sstable components is different from the old version,
                    // we need to upgrade the sstables first. Otherwise, the index rebuild would ignore the new version.
                    if (!version.equals(globalCurrentVersion))
                    {
                        rebuildTableIndexes(keyspace, TABLE);
                        verifySAIVersionInUse(keyspace, TABLE, globalCurrentVersion);
                        upgradeSSTables(keyspace, TABLE);
                    }

                    rebuildTableIndexes(keyspace, TABLE);
                    reloadSSTableIndexInPlace(keyspace, TABLE);
                    verifySAIVersionInUse(keyspace, TABLE, version);
                }
                else
                {
                    Assertions.assertThatThrownBy(() -> rebuildTableIndexes(keyspace, TABLE))
                              .hasMessageContaining(rebuildIndexErrorIfUnsupported);
                }

                // Verify if we can query the index, depending on whether the new version supports the queries
                verifyQueries(queryResultsVerifier,
                              keyspace,
                              version,
                              minVersionForQuerying,
                              queryingErrorIfUnsupported);
            });
        }
    }
}
