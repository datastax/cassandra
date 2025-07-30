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
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.index.sai.plan.QueryController;
import org.apache.cassandra.index.sai.utils.Glove;

import static org.assertj.core.api.Assertions.assertThat;

public class VectorKeyRestrictedOnPartitionTest extends VectorKeyRestrictedTester
{
    private static Glove.WordVector word2vec;

    @BeforeClass
    public static void loadModel() throws Throwable
    {
        word2vec = Glove.parse(VectorKeyRestrictedOnPartitionTest.class.getClassLoader().getResourceAsStream("glove.3K.50d.txt"));
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

    @Test
    public void testPartitionKeyRestrictionCombinedWithSearchPredicate() throws Throwable
    {
        // Need to test the search then order path
        QueryController.QUERY_OPT_LEVEL = 0;

        // We use a clustered primary key to simplify the mental model for this test.
        // The bug this test exposed happens when the last row(s) in a segment, based on PK order, are present
        // in a peer index for an sstable's search index but not its vector index.
        createTable("CREATE TABLE %s (partition int, i int, v vector<float, 2>, c int, PRIMARY KEY(partition, i))");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function': 'euclidean'}");
        createIndex("CREATE CUSTOM INDEX ON %s(c) USING 'StorageAttachedIndex'");

        var partitionKeys = new ArrayList<Integer>();
        // Insert many rows
        for (int i = 1; i < 1000; i++)
        {
            execute("INSERT INTO %s (partition, i, v, c) VALUES (?, ?, ?, ?)", i, i, vector(i, i), i);
            partitionKeys.add(i);
        }

        beforeAndAfterFlush(() -> {
            // Restricted by partition key and with low as well as high cardinality of results for column c
            assertRows(execute("SELECT i FROM %s WHERE partition = 1 AND c > 0 ORDER BY v ANN OF [1,1] LIMIT 1"), row(1));
            assertRows(execute("SELECT i FROM %s WHERE partition = 1 AND c < 10 ORDER BY v ANN OF [1,1] LIMIT 1"), row(1));

            // Do some partition key range queries, the restriction on c is meaningless, but forces the search then
            // order path
            var r1 = execute("SELECT partition FROM %s WHERE token(partition) < token(11) AND c > 0 ORDER BY v ANN OF [1,1] LIMIT 1000");
            var e1 = keysWithUpperBound(partitionKeys, 11,false);
            assertThat(keys(r1)).containsExactlyInAnyOrderElementsOf(e1);

            var r2 = execute("SELECT partition FROM %s WHERE token(partition) >= token(11) AND token(partition) <= token(20) AND c <= 1000 ORDER BY v ANN OF [1,1] LIMIT 1000");
            var e2 = keysInBounds(partitionKeys, 11, true, 20, true);
            assertThat(keys(r2)).containsExactlyInAnyOrderElementsOf(e2);
        });
    }
}
