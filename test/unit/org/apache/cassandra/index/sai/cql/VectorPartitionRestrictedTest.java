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

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.index.sai.utils.Glove;
import org.assertj.core.data.Percentage;

import static org.assertj.core.api.Assertions.assertThat;

public class VectorPartitionRestrictedTest extends VectorTester.VersionedWithChecksums
{
    private static Glove.WordVector word2vec;

    @BeforeClass
    public static void loadModel() throws Throwable
    {
        word2vec = Glove.parse(VectorPartitionRestrictedTest.class.getClassLoader().getResourceAsStream("glove.3K.50d.txt"));
    }

    @After
    public void cleanupConfigs()
    {
        CassandraRelevantProperties.SAI_VECTOR_FLUSH_PERIOD_IN_MILLIS.reset();
        CassandraRelevantProperties.SAI_VECTOR_FLUSH_THRESHOLD_MAX_ROWS.reset();
    }

    @Test
    public void partitionRestrictedTest()
    {
        createTable(String.format("CREATE TABLE %%s (pk int, str_val text, val vector<float, %d>, PRIMARY KEY(pk))", word2vec.dimension()));
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");

        int vectorCount = getRandom().nextIntBetween(500, 1000);

        for (int pk = 0; pk < vectorCount; pk++)
            execute("INSERT INTO %s (pk, str_val, val) VALUES (?, 'A', " + vectorString(word2vec.vector(word2vec.word(pk))) + " )", pk);

        // query memtable index

        for (int executionCount = 0; executionCount < 50; executionCount++)
        {
            int key = getRandom().nextIntBetween(1, vectorCount - 1);
            float[] queryVector = word2vec.vector(word2vec.word(getRandom().nextIntBetween(0, vectorCount - 1)));
            searchWithKey(queryVector, key, 1);
        }

        flush();

        // query on-disk index with existing key:
        for (int executionCount = 0; executionCount < 50; executionCount++)
        {
            int key = getRandom().nextIntBetween(1, vectorCount - 1);
            float[] queryVector = word2vec.vector(word2vec.word(getRandom().nextIntBetween(0, vectorCount - 1)));
            searchWithKey(queryVector, key, 1);
        }

        // query on-disk index with non-existing key:
        for (int executionCount = 0; executionCount < 50; executionCount++)
        {
            int nonExistingKey = getRandom().nextIntBetween(1, vectorCount) + vectorCount;
            float[] queryVector = word2vec.vector(word2vec.word(getRandom().nextIntBetween(0, vectorCount - 1)));
            searchWithNonExistingKey(queryVector, nonExistingKey);
        }
    }

    @Test
    public void partitionRestrictedWidePartitionTest()
    {
        partitionRestrictedWidePartitionTest(word2vec.dimension(), 0, 1000);
    }

    @Test
    public void partitionRestrictedWidePartitionBqCompressedTest()
    {
        partitionRestrictedWidePartitionTest(2048, 0, Integer.MAX_VALUE);
    }

    @Test
    public void partitionRestrictedWidePartitionPqCompressedTest()
    {
        partitionRestrictedWidePartitionTest(word2vec.dimension(), 2000, Integer.MAX_VALUE);
    }

    public void partitionRestrictedWidePartitionTest(int dimension, int minvectorCount, int maxvectorCount)
    {
        createTable(String.format("CREATE TABLE %%s (pk int, ck int, val vector<float, %d>, PRIMARY KEY(pk, ck))", dimension));
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");

        int partitions = getRandom().nextIntBetween(20, 40);
        int vectorCountPerPartition = getRandom().nextIntBetween(50, 100);
        int vectorCount = partitions * vectorCountPerPartition;

        if (vectorCount > maxvectorCount)
        {
            vectorCountPerPartition = maxvectorCount / partitions;
            vectorCount = partitions * vectorCountPerPartition;
        }
        else if (vectorCount < minvectorCount)
        {
            vectorCountPerPartition = minvectorCount / partitions;
            vectorCount = partitions * vectorCountPerPartition;
        }

        List<Vector<Float>> vectors = IntStream.range(0, vectorCount).mapToObj(s -> randomVectorBoxed(dimension)).collect(Collectors.toList());

        int i = 0;
        for (int pk = 1; pk <= partitions; pk++)
        {
            for (int ck = 1; ck <= vectorCountPerPartition; ck++)
            {
                var vector = vectors.get(i++);
                execute("INSERT INTO %s (pk, ck, val) VALUES (?, ?, ?)", pk, ck, vector);
            }
        }

        // query memtable index
        for (int executionCount = 0; executionCount < 50; executionCount++)
        {
            int key = getRandom().nextIntBetween(1, partitions);
            var queryVector = randomVectorBoxed(dimension);
            searchWithKey(queryVector, key, vectorCountPerPartition, 1000);
            searchWithKey(queryVector, key, 1, 1);
        }

        flush();

        // query on-disk index with existing key:
        for (int executionCount = 0; executionCount < 50; executionCount++)
        {
            int key = getRandom().nextIntBetween(1, partitions);
            var queryVector = randomVectorBoxed(dimension);
            searchWithKey(queryVector, key, vectorCountPerPartition, 1000);
            searchWithKey(queryVector, key, 1, 1);
        }

        // query on-disk index with non-existing key:
        for (int executionCount = 0; executionCount < 50; executionCount++)
        {
            int nonExistingKey = getRandom().nextIntBetween(1, partitions) + partitions;
            var queryVector = randomVectorBoxed(dimension);
            searchWithNonExistingKey(queryVector, nonExistingKey);
        }
    }

    private void searchWithNonExistingKey(float[] queryVector, int key)
    {
        searchWithKey(queryVector, key, 0);
    }

    private void searchWithNonExistingKey(Vector<Float> queryVector, int key)
    {
        searchWithKey(queryVector, key, 0);
    }

    private void searchWithKey(float[] queryVector, int key, int expectedSize)
    {
        UntypedResultSet result = execute("SELECT * FROM %s WHERE pk = " + key + " ORDER BY val ann of " + Arrays.toString(queryVector) + " LIMIT 1000");

        // VSTODO maybe we should have different methods for these cases
        if (expectedSize < 10)
            assertThat(result).hasSize(expectedSize);
        else
            assertThat(result.size()).isCloseTo(expectedSize, Percentage.withPercentage(5));
        result.stream().forEach(row -> assertThat(row.getInt("pk")).isEqualTo(key));
    }

    private void searchWithKey(Vector<Float> queryVector, int key, int expectedSize)
    {
        searchWithKey(queryVector, key, expectedSize, 1000);
    }

    private void searchWithKey(Vector<Float> queryVector, int key, int expectedSize, int limit)
    {
        UntypedResultSet result = execute("SELECT * FROM %s WHERE pk = ? ORDER BY val ann of ? LIMIT " + limit, key, queryVector);

        // VSTODO maybe we should have different methods for these cases
        if (expectedSize < 10)
            assertThat(result).hasSize(expectedSize);
        else
            assertThat(result.size()).isCloseTo(expectedSize, Percentage.withPercentage(10));
        result.stream().forEach(row -> assertThat(row.getInt("pk")).isEqualTo(key));
    }
}
