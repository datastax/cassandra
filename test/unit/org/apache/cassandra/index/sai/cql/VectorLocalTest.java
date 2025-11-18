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

package org.apache.cassandra.index.sai.cql;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.VectorType;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.index.sai.disk.v1.SegmentBuilder;
import org.apache.cassandra.index.sai.utils.Glove;
import org.assertj.core.data.Percentage;

import static org.assertj.core.api.Assertions.assertThat;

public class VectorLocalTest extends VectorTester.VersionedWithChecksums
{
    private static final VectorTypeSupport vts = VectorizationProvider.getInstance().getVectorTypeSupport();

    private static Glove.WordVector word2vec;

    @BeforeClass
    public static void loadModel() throws Throwable
    {
        word2vec = Glove.parse(VectorLocalTest.class.getClassLoader().getResourceAsStream("glove.3K.50d.txt"));
    }

    @After
    public void cleanupConfigs()
    {
        CassandraRelevantProperties.SAI_VECTOR_FLUSH_PERIOD_IN_MILLIS.reset();
        CassandraRelevantProperties.SAI_VECTOR_FLUSH_THRESHOLD_MAX_ROWS.reset();
    }

    @Test
    public void skipToAssertRegressionTest()
    {
        createTable("CREATE TABLE %s (\n" +
                  "    lat_low_precision smallint,\n" +
                  "    lon_low_precision smallint,\n" +
                  "    lat_high_precision tinyint,\n" +
                  "    lon_high_precision tinyint,\n" +
                  "    id int,\n" +
                  "    lat_exact decimal,\n" +
                  "    lat_lon_embedding vector<float, 2>,\n" +
                  "    lon_exact decimal,\n" +
                  "    name text,\n" +
                  "    proximity_edge tinyint,\n" +
                  "    PRIMARY KEY ((lat_low_precision, lon_low_precision), lat_high_precision, lon_high_precision, id)\n" +
                  ") WITH CLUSTERING ORDER BY (lat_high_precision ASC, lon_high_precision ASC, id ASC);");
        createIndex("CREATE CUSTOM INDEX lat_high_precision_index ON %s (lat_high_precision) USING 'org.apache.cassandra.index.sai.StorageAttachedIndex';");
        createIndex("CREATE CUSTOM INDEX lat_lon_embedding_index ON %s (lat_lon_embedding) USING 'org.apache.cassandra.index.sai.StorageAttachedIndex' WITH OPTIONS = {'similarity_function': 'EUCLIDEAN'};");
        createIndex("CREATE CUSTOM INDEX lon_high_precision_index ON %s (lon_high_precision) USING 'org.apache.cassandra.index.sai.StorageAttachedIndex';");

        int vectorCount = getRandom().nextIntBetween(500, 1000);
        List<Vector<Float>> vectors = IntStream.range(0, vectorCount).mapToObj(s -> randomVectorBoxed(2)).collect(Collectors.toList());

        int pk = 0;
        for (Vector<Float> vector : vectors)
            execute("INSERT INTO %s (lat_low_precision, lon_low_precision, lat_high_precision, lon_high_precision, id, lat_exact, lat_lon_embedding, lon_exact, name, proximity_edge) "
                    + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (short)(pk % 3), (short)(pk % 5), (byte)(pk % 7), (byte)(pk % 11), pk++, 1.2 * pk, vector, 1.3 * pk, Integer.toString(pk), (byte)(pk % 2));

        flush();

        var queryVector = randomVectorBoxed(2);

        final int limit = 10;
        UntypedResultSet result;

        result = execute("SELECT name, id, lat_exact, lon_exact, proximity_edge "
                      + "FROM %s WHERE lat_low_precision = ? AND lon_low_precision = ? AND lat_high_precision = ? AND lon_high_precision = ? "
                      + "ORDER BY lat_lon_embedding ANN OF ? LIMIT ?",
                      (short)0, (short)3, (byte)3, (byte)3,
                      queryVector, limit);

        assertThat(result).hasSize(1);

        result = execute("SELECT name, id, lat_exact, lon_exact, proximity_edge "
                      + "FROM %s WHERE lat_low_precision = ? AND lon_low_precision = ? AND lat_high_precision = ? AND lon_high_precision = ? "
                      + "ORDER BY lat_lon_embedding ANN OF ? LIMIT ?",
                      (short)0, (short)0, (byte)0, (byte)0,
                      queryVector, limit);

        assertThat(result).hasSize(1);
    }

    @Test
    public void randomizedTest()
    {
        randomizedTest(word2vec.dimension());
    }

    // We do not test BQ because it is no longer implemented as of https://github.com/datastax/cassandra/pull/1580.
    // Technically, the test was covering the case where we have large vectors, but it was taking a significant
    // amount of time, so until we re-implement BQ, we can ignore this test. See for details
    // https://github.com/riptano/cndb/issues/15985.
    @Ignore
    public void randomizedBqCompressedTest()
    {
        randomizedTest(2048);
    }

    private void randomizedTest(int dimension)
    {
        createTable(String.format("CREATE TABLE %%s (pk int, str_val text, val vector<float, %d>, PRIMARY KEY(pk))", dimension));
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");

        int vectorCount = getRandom().nextIntBetween(500, 1000);
        List<Vector<Float>> vectors = IntStream.range(0, vectorCount).mapToObj(s -> randomVectorBoxed(dimension)).collect(Collectors.toList());

        int pk = 0;
        for (Vector<Float> vector : vectors)
            execute("INSERT INTO %s (pk, str_val, val) VALUES (?, 'A', ?)", pk++, vector);

        // query memtable index
        int limit = Math.min(getRandom().nextIntBetween(30, 50), vectors.size());
        var queryVector = randomVectorBoxed(dimension);
        UntypedResultSet resultSet = search(queryVector, limit);
        assertDescendingScore(queryVector, getVectorsFromResult(resultSet, dimension));

        flush();

        // query on-disk index
        queryVector = randomVectorBoxed(dimension);
        resultSet = search(queryVector, limit);
        assertDescendingScore(queryVector, getVectorsFromResult(resultSet, dimension));

        // populate some more vectors
        int additionalVectorCount = getRandom().nextIntBetween(500, 1000);
        List<Vector<Float>> additionalVectors = IntStream.range(0, additionalVectorCount).mapToObj(s -> randomVectorBoxed(dimension)).collect(Collectors.toList());
        for (Vector<Float> vector : additionalVectors)
            execute("INSERT INTO %s (pk, str_val, val) VALUES (?, 'A', ?)", pk++, vector);

        vectors.addAll(additionalVectors);

        // query both memtable index and on-disk index
        queryVector = randomVectorBoxed(dimension);
        resultSet = search(queryVector, limit);
        assertDescendingScore(queryVector, getVectorsFromResult(resultSet, dimension));

        flush();

        // query multiple on-disk indexes
        queryVector = randomVectorBoxed(dimension);
        resultSet = search(queryVector, limit);
        assertDescendingScore(queryVector, getVectorsFromResult(resultSet, dimension));

        compact();

        // query compacted on-disk index
        queryVector = randomVectorBoxed(dimension);
        resultSet = search(queryVector, limit);
        assertDescendingScore(queryVector, getVectorsFromResult(resultSet, dimension));
    }

    @Test
    public void multiSSTablesTest()
    {
        createTable(String.format("CREATE TABLE %%s (pk int, str_val text, val vector<float, %d>, PRIMARY KEY(pk))", word2vec.dimension()));
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");
        disableCompaction(keyspace());

        int sstableCount = getRandom().nextIntBetween(3, 6);
        int vectorCountPerSSTable = getRandom().nextIntBetween(200, 400);
        int pk = 0;

        int vectorCount = 0;
        // create multiple sstables
        List<float[]> allVectors = new ArrayList<>(sstableCount * vectorCountPerSSTable);
        for (int sstable = 0; sstable < sstableCount; sstable++)
        {
            for (int row = 0; row < vectorCountPerSSTable; row++)
            {
                String word = word2vec.word(vectorCount++);
                float[] vector = word2vec.vector(word);
                execute("INSERT INTO %s (pk, str_val, val) VALUES (?, ?, " + vectorString(vector) + " )", pk++, word);
                allVectors.add(vector);
            }
            flush();
        }
        assertThat(getCurrentColumnFamilyStore(keyspace()).getLiveSSTables()).hasSize(sstableCount);

        // query multiple on-disk indexes
        int limit = Math.min(getRandom().nextIntBetween(30, 50), vectorCountPerSSTable);
        float[] queryVector = allVectors.get(getRandom().nextIntBetween(0, allVectors.size() - 1));
        UntypedResultSet resultSet = search(queryVector, limit);

        // expect recall to be at least 0.8
        List<float[]> resultVectors = getVectorsFromResult(resultSet);
        assertDescendingScore(queryVector, resultVectors);

        double recall = rawIndexedRecall(allVectors, queryVector, resultVectors, limit);
        assertThat(recall).isGreaterThanOrEqualTo(0.8);
    }

    @Test
    public void rangeRestrictedTest() throws Throwable
    {
        createTable(String.format("CREATE TABLE %%s (pk int, str_val text, val vector<float, %d>, PRIMARY KEY(pk))", word2vec.dimension()));
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");

        int vectorCount = getRandom().nextIntBetween(500, 1000);

        int pk = 0;
        Multimap<Long, float[]> vectorsByToken = ArrayListMultimap.create();
        for (int index = 0; index < vectorCount; index++)
        {
            String word = word2vec.word(index);
            float[] vector = word2vec.vector(word);
            vectorsByToken.put(Murmur3Partitioner.instance.getToken(Int32Type.instance.decompose(pk)).getLongValue(), vector);
            execute("INSERT INTO %s (pk, str_val, val) VALUES (?, ?, " + vectorString(vector) + " )", pk++, word);
        }

        // query memtable index first and then on-disk index
        beforeAndAfterFlush(() -> {
            for (int executionCount = 0; executionCount < 50; executionCount++)
            {
                int key1 = getRandom().nextIntBetween(1, vectorCount * 2);
                long token1 = Murmur3Partitioner.instance.getToken(Int32Type.instance.decompose(key1)).getLongValue();
                int key2 = getRandom().nextIntBetween(1, vectorCount * 2);
                long token2 = Murmur3Partitioner.instance.getToken(Int32Type.instance.decompose(key2)).getLongValue();

                long minToken = Math.min(token1, token2);
                long maxToken = Math.max(token1, token2);
                List<float[]> expected = vectorsByToken.entries().stream()
                                                       .filter(e -> e.getKey() >= minToken && e.getKey() <= maxToken)
                                                       .map(Map.Entry::getValue)
                                                       .collect(Collectors.toList());

                float[] queryVector = word2vec.vector(word2vec.word(getRandom().nextIntBetween(0, vectorCount - 1)));

                UntypedResultSet result = execute("SELECT * FROM %s" +
                                                  " WHERE token(pk) <= " + maxToken +
                                                  " AND token(pk) >= " + minToken +
                                                  " ORDER BY val ANN OF " + Arrays.toString(queryVector) +
                                                  " LIMIT 1000");
                assertThat(result.size()).isCloseTo(expected.size(), Percentage.withPercentage(20))
                                         .isLessThanOrEqualTo(1000);

                List<float[]> resultVectors = getVectorsFromResult(result);
                assertDescendingScore(queryVector, resultVectors);

                if (expected.isEmpty())
                {
                    assertThat(resultVectors).isEmpty();
                }
                else
                {
                    double recall = recallMatch(expected, resultVectors, expected.size());
                    assertThat(recall).isGreaterThanOrEqualTo(0.8);
                }
            }
        });
    }

    // test retrieval of multiple rows that have the same vector value
    @Test
    public void multipleSegmentsMultiplePostingsTest()
    {
        createTable(String.format("CREATE TABLE %%s (pk int, val vector<float, %d>, PRIMARY KEY(pk))", word2vec.dimension()));
        disableCompaction(KEYSPACE);

        int sstableCount = getRandom().nextIntBetween(3, 6);
        int vectorCountPerSSTable = getRandom().nextIntBetween(100, 200);
        int pk = 0;

        // create multiple sstables
        int vectorCount = 0;
        var population = new ArrayList<float[]>();
        for (int i = 0; i < sstableCount; i++)
        {
            for (int row = 0; row < vectorCountPerSSTable; row++)
            {
                float[] v = word2vec.vector(word2vec.word(vectorCount++));
                for (int j = 0; j < getRandom().nextIntBetween(1, 4); j++)
                {
                    execute("INSERT INTO %s (pk, val) VALUES (?, ?)", pk++, vector(v));
                    population.add(v);
                }
            }
            flush();
        }
        assertThat(getCurrentColumnFamilyStore(keyspace()).getLiveSSTables()).hasSize(sstableCount);

        // create index on existing sstable to produce multiple segments
        SegmentBuilder.updateLastValidSegmentRowId(50); // 50 rows per segment
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");
        verifyChecksum();

        // query multiple on-disk indexes
        var testCount = 200;
        var start = getRandom().nextIntBetween(vectorCount, word2vec.size() - testCount);
        for (int i = start; i < start + testCount; i++)
        {
            var q = word2vec.vector(word2vec.word(i));
            int limit = Math.min(getRandom().nextIntBetween(10, 50), vectorCountPerSSTable);
            UntypedResultSet result = execute("SELECT * FROM %s ORDER BY val ann of ? LIMIT ?", vector(q), limit);
            assertThat(result).hasSize(limit);

            List<float[]> resultVectors = getVectorsFromResult(result);
            assertDescendingScore(q, resultVectors);

            double recall = bruteForceRecall(q, resultVectors, population, limit);
            assertThat(recall).isGreaterThanOrEqualTo(0.8);
        }
    }

    private double bruteForceRecall(float[] q, List<float[]> resultVectors, List<float[]> population, int limit)
    {
        List<float[]> expected = population
                                 .stream()
                                 .sorted(Comparator.comparingDouble(v -> -compareFloatArrays(v, q)))
                                 .limit(limit)
                                 .collect(Collectors.toList());
        return recallMatch(expected, resultVectors, limit);
    }

    // search across multiple segments, combined with a non-ann predicate
    @Test
    public void multipleNonAnnSegmentsTest()
    {
        createTable(String.format("CREATE TABLE %%s (pk int, str_val text, val vector<float, %d>, PRIMARY KEY(pk))", word2vec.dimension()));
        disableCompaction(KEYSPACE);

        int sstableCount = getRandom().nextIntBetween(3, 6);
        int vectorCountPerSSTable = getRandom().nextIntBetween(200, 400);
        int pk = 0;

        // create multiple sstables
        Multimap<String, float[]> vectorsByStringValue = ArrayListMultimap.create();
        int vectorCount = 0;
        for (int i = 0; i < sstableCount; i++)
        {
            for (int row = 0; row < vectorCountPerSSTable; row++)
            {
                String stringValue = String.valueOf(pk % 10); // 10 different string values
                float[] vector = word2vec.vector(word2vec.word(vectorCount++));
                execute("INSERT INTO %s (pk, str_val, val) VALUES (?, ?, " + vectorString(vector) + " )", pk++, stringValue);
                vectorsByStringValue.put(stringValue, vector);
            }
            flush();
        }
        assertThat(getCurrentColumnFamilyStore(keyspace()).getLiveSSTables()).hasSize(sstableCount);

        // create indexes on existing sstable to produce multiple segments
        SegmentBuilder.updateLastValidSegmentRowId(50); // 50 rows per segment
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(str_val) USING 'StorageAttachedIndex'");
        flush();
        verifyChecksum();

        // query multiple on-disk indexes
        for (String stringValue : vectorsByStringValue.keySet())
        {
            int limit = Math.min(getRandom().nextIntBetween(30, 50), vectorCountPerSSTable);
            float[] queryVector = vectorsByStringValue.get(stringValue).stream().findAny().get();
            UntypedResultSet resultSet = search(stringValue, queryVector, limit);

            // expect recall to be at least 0.8
            List<float[]> resultVectors = getVectorsFromResult(resultSet);
            assertDescendingScore(queryVector, resultVectors);

            double recall = rawIndexedRecall(vectorsByStringValue.get(stringValue), queryVector, resultVectors, limit);
            assertThat(recall).isGreaterThanOrEqualTo(0.8);
        }
    }

    private UntypedResultSet search(String stringValue, float[] queryVector, int limit)
    {
        UntypedResultSet result = execute("SELECT * FROM %s WHERE str_val = '" + stringValue + "' ORDER BY val ann of " + Arrays.toString(queryVector) + " LIMIT " + limit);
        assertThat(result).hasSize(limit);
        return result;
    }

    private UntypedResultSet search(float[] queryVector, int limit)
    {
        UntypedResultSet result = execute("SELECT * FROM %s ORDER BY val ann of " + Arrays.toString(queryVector) + " LIMIT " + limit);
        assertThat(result.size()).isCloseTo(limit, Percentage.withPercentage(5));
        return result;
    }

    private UntypedResultSet search(Vector<Float> queryVector, int limit)
    {
        UntypedResultSet result = execute("SELECT * FROM %s ORDER BY val ann of ? LIMIT " + limit, queryVector);
        assertThat(result.size()).isCloseTo(limit, Percentage.withPercentage(5));
        return result;
    }

    private void assertDescendingScore(Vector<Float> queryVector, List<float[]> resultVectors)
    {
        float[] arr = new float[queryVector.size()];
        for (int i = 0; i < queryVector.size(); i++)
            arr[i] = queryVector.get(i);

        assertDescendingScore(arr, resultVectors);
    }

    private void assertDescendingScore(float[] queryVector, List<float[]> resultVectors)
    {
        float prevScore = -1;
        for (float[] current : resultVectors)
        {
            float score = compareFloatArrays(queryVector, current);
            if (prevScore >= 0)
                assertThat(score).isLessThanOrEqualTo(prevScore);

            prevScore = score;
        }
    }

    private static float compareFloatArrays(float[] queryVector, float[] current)
    {
        return VectorSimilarityFunction.COSINE.compare(vts.createFloatVector(current), vts.createFloatVector(queryVector));
    }

    private List<float[]> getVectorsFromResult(UntypedResultSet result)
    {
        return getVectorsFromResult(result, word2vec.dimension());
    }

    private List<float[]> getVectorsFromResult(UntypedResultSet result, int dimension)
    {
        List<float[]> vectors = new ArrayList<>();
        VectorType<?> vectorType = VectorType.getInstance(FloatType.instance, dimension);

        // verify results are part of inserted vectors
        for (UntypedResultSet.Row row: result)
        {
            vectors.add(vectorType.composeAsFloat(row.getBytes("val")));
        }

        return vectors;
    }
}
