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
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.exceptions.ReadFailureException;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.index.sai.SAIUtil;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.assertj.core.api.Assertions;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests the availabilty of features in different versions of the SAI on-disk format.
 */
@RunWith(Parameterized.class)
public class FeaturesVersionSupportTest extends VectorTester
{
    @Parameterized.Parameter
    public Version version;

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data()
    {
        return Version.ALL.stream().map(v -> new Object[]{ v }).collect(Collectors.toList());
    }

    @Before
    @Override
    public void setup() throws Throwable
    {
        super.setup();
        requireNetwork();
        SAIUtil.setCurrentVersion(version);
    }

    /**
     * Test that ANN queries are supported with on-disk format versions from {@link Version#CA}.
     * Nodes using older versions should fail their index build, although the index will still exist.
     */
    @Test
    public void testANNSupport()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, x int, v vector<float, 3>)");

        execute("INSERT INTO %s (k, x, v) VALUES (0, 0, [1.0, 2.0, 3.0])");
        execute("INSERT INTO %s (k, x, v) VALUES (1, 0, [2.0, 3.0, 4.0])");
        execute("INSERT INTO %s (k, x, v) VALUES (2, 0, [3.0, 4.0, 5.0])");
        execute("INSERT INTO %s (k, x, v) VALUES (3, 0, [4.0, 5.0, 6.0])");
        flush();

        // create a non-vector index with the old version, so there are some per-sstable components around
        createIndex("CREATE CUSTOM INDEX ON %s(x) USING 'StorageAttachedIndex'");

        // vector index creation will be rejected in older versions
        String idx = createIndexAsync("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
        waitForIndexBuilds(idx);
        boolean isIndexQueryable = isIndexQueryable(keyspace(), idx);
        if (version.onOrAfter(Version.JVECTOR_EARLIEST))
        {
            assertThat(isIndexQueryable).isTrue();

            // the index is marked as queryable, so we should be able to query it
            assertRows(execute("SELECT k FROM %s ORDER BY v ANN OF [2.5, 3.5, 4.5] LIMIT 3"), row(2), row(1), row(3));
            assertRows(execute("SELECT k FROM %s ORDER BY v ANN OF [2.5, 3.5, 4.5] LIMIT 3 WITH ann_options = {'rerank_k':3}"), row(2), row(1), row(3));
        }
        else
        {
            assertThat(isIndexQueryable).isFalse();
        }

        // vector index creation will be accepted on newer versions, even if there is still another index in the older version
        dropIndex("DROP INDEX %s." + idx);
        SAIUtil.setCurrentVersion(Version.LATEST);
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");

        // once the index has been created, we can query it
        assertRows(execute("SELECT k FROM %s ORDER BY v ANN OF [2.5, 3.5, 4.5] LIMIT 3"), row(2), row(1), row(3));
        assertRows(execute("SELECT k FROM %s ORDER BY v ANN OF [2.5, 3.5, 4.5] LIMIT 3 WITH ann_options = {'rerank_k':3}"), row(2), row(1), row(3));
    }

    /**
     * Test that geo distance queries are supported with on-disk format versions from {@link Version#CA}.
     * Nodes using older versions should fail their index build, although the index will still exist.
     */
    @Test
    public void testGeoDistance()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, x int, v vector<float, 2>)");

        // Distances computed using GeoDistanceAccuracyTest#strictHaversineDistance
        execute("INSERT INTO %s (k, x, v) VALUES (0, 0, [1, 2])"); // distance is 555661 m from [5,5]
        execute("INSERT INTO %s (k, x, v) VALUES (1, 0, [4, 4])"); // distance is 157010 m from [5,5]
        execute("INSERT INTO %s (k, x, v) VALUES (2, 0, [5, 5])"); // distance is 0 m from [5,5]
        execute("INSERT INTO %s (k, x, v) VALUES (3, 0, [6, 6])"); // distance is 156891 m from [5,5]
        execute("INSERT INTO %s (k, x, v) VALUES (4, 0, [8, 9])"); // distance is 553647 m from [5,5]
        execute("INSERT INTO %s (k, x, v) VALUES (5, 0, [10, 10])"); // distance is 782780 m from [5,5]
        flush();

        // create a non-vector index with the old version, so there are some per-sstable components around
        createIndex("CREATE CUSTOM INDEX ON %s(x) USING 'StorageAttachedIndex'");

        // vector index creation will be rejected in older versions
        String idx = createIndexAsync("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function' : 'euclidean'}");
        waitForIndexBuilds(idx);
        boolean isIndexQueryable = isIndexQueryable(keyspace(), idx);
        if (version.onOrAfter(Version.JVECTOR_EARLIEST))
        {
            assertThat(isIndexQueryable).isTrue();

            // the index is marked as queryable, so we should be able to query it
            assertRowsIgnoringOrder(execute("SELECT k FROM %s WHERE GEO_DISTANCE(v, [5,5]) < 157000"), row(2), row(3));
            assertRowsIgnoringOrder(execute("SELECT k FROM %s WHERE GEO_DISTANCE(v, [5,5]) < 157011"), row(1), row(2), row(3));
        }
        else
        {
            assertThat(isIndexQueryable).isFalse();
        }

        // vector index creation will be accepted on newer versions, even if there is still an index in the older version
        dropIndex("DROP INDEX %s." + idx);
        SAIUtil.setCurrentVersion(Version.LATEST);
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function' : 'euclidean'}");

        // once the index has been created, we can query it
        assertRowsIgnoringOrder(execute("SELECT k FROM %s WHERE GEO_DISTANCE(v, [5,5]) < 157000"), row(2), row(3));
        assertRowsIgnoringOrder(execute("SELECT k FROM %s WHERE GEO_DISTANCE(v, [5,5]) < 157011"), row(1), row(2), row(3));
    }

    /**
     * Test that BM25 queries are supported with on-disk format versions from {@link Version#EC}.
     * Older versions should reject BM25 queries with {@link RequestFailureReason#FEATURE_NEEDS_INDEX_REBUILD} errors.
     */
    @Test
    public void testBM25() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v text)");
        createIndex("CREATE CUSTOM INDEX ON %s(v) " +
                    "USING 'org.apache.cassandra.index.sai.StorageAttachedIndex' " +
                    "WITH OPTIONS = {" +
                    "'index_analyzer': '{" +
                    "\"tokenizer\" : {\"name\" : \"standard\"}, " +
                    "\"filters\" : [{\"name\" : \"porterstem\"}]}'}");
        execute("INSERT INTO %s (k, v) VALUES (1, 'apple')");
        String query = "SELECT k FROM %s WHERE v : 'apple' ORDER BY v BM25 OF 'apple' LIMIT 3";
        beforeAndAfterFlush(() -> {
            if (version.onOrAfter(Version.BM25_EARLIEST))
            {
                assertRows(execute(query), row(1));
            }
            else
            {
                Assertions.assertThatThrownBy(() -> execute(query))
                          .isInstanceOf(ReadFailureException.class)
                          .hasMessageContaining(RequestFailureReason.FEATURE_NEEDS_INDEX_REBUILD.name());
            }
        });
    }

    /**
     * Test that index-time analyzers are supported with all on-disk format versions.
     */
    @Test
    public void testIndexAnalyzer()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v text)");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'org.apache.cassandra.index.sai.StorageAttachedIndex' " +
                    "WITH OPTIONS = { 'index_analyzer': 'standard' }");

        execute("INSERT INTO %s (k, v) VALUES (0, 'Quick fox')");
        execute("INSERT INTO %s (k, v) VALUES (1, 'Lazy dogs')");
        flush();

        UntypedResultSet result = execute("SELECT * FROM %s WHERE v = 'dogs'");
        assertThat(result).hasSize(1);
    }

    /**
     * Test that query-time analyzers are supported with all on-disk format versions.
     */
    @Test
    public void testQueryAnalyzer()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v text)");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex' WITH OPTIONS = {" +
                    "'index_analyzer': '{" +
                    "  \"tokenizer\" : { \"name\" : \"whitespace\", \"args\" : {} }," +
                    "  \"filters\" : [ { \"name\" : \"lowercase\", \"args\": {} }, " +
                    "                  { \"name\" : \"edgengram\", \"args\": { \"minGramSize\":\"1\", \"maxGramSize\":\"30\" } }]," +
                    "  \"charFilters\" : []}', " +
                    "'query_analyzer': '{" +
                    "  \"tokenizer\" : { \"name\" : \"whitespace\", \"args\" : {} }," +
                    "  \"filters\" : [ {\"name\" : \"lowercase\",\"args\": {}} ]}'}");

        execute("INSERT INTO %s (k, v) VALUES (1, 'astra quick fox')");
        execute("INSERT INTO %s (k, v) VALUES (2, 'astra2 quick fox')");
        execute("INSERT INTO %s (k, v) VALUES (3, 'astra3 quick foxes')");
        flush();

        assertRows(execute("SELECT k FROM %s WHERE v : 'ast'"), row(1), row(2), row(3));
        assertRows(execute("SELECT k FROM %s WHERE v : 'astra'"), row(1), row(2), row(3));
        assertRows(execute("SELECT k FROM %s WHERE v : 'astra2'"), row(2));
        assertRows(execute("SELECT k FROM %s WHERE v : 'fox'"), row(1), row(2), row(3));
        assertRows(execute("SELECT k FROM %s WHERE v : 'foxes'"), row(3));
    }
}
