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
import org.junit.BeforeClass;
import org.junit.Test;

import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.VectorType;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.disk.v1.SegmentBuilder;
import org.apache.cassandra.index.sai.utils.Glove;
import org.assertj.core.data.Percentage;

import static org.assertj.core.api.Assertions.assertThat;

public class VectorLocalRandPerfTest extends VectorTester
{
    private static Glove.WordVector word2vec;
    final int vectorCount = 5000;
    final int limit = 200;
    List<float[]> vectors;

    @Override
    public void beforeTest() throws Throwable
    {
        super.beforeTest();

        createTable(String.format("CREATE TABLE %%s (pk int, str_val text, val vector<float, %d>, PRIMARY KEY(pk))", word2vec.dimension()));
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();


        vectors = IntStream.range(0, vectorCount)
                           .mapToObj(s -> randomVector())
                           .collect(Collectors.toList());
        int pk = 0;
        for (float[] vector : vectors)
            execute("INSERT INTO %s (pk, str_val, val) VALUES (?, 'A', " + vectorString(vector) + " )", pk++);
        flush();
    }

    @BeforeClass
    public static void loadModel() throws Throwable
    {
        word2vec = Glove.parse(VectorLocalRandPerfTest.class.getClassLoader().getResourceAsStream("glove.3K.50d.txt"));
    }

    @Test
    public void randomizedPerfTest() throws Throwable
    {
        long start = System.nanoTime();
        // query on-disk index
        for (var q : vectors)
        {
            UntypedResultSet resultSet = search(q, limit);
            assertDescendingScore(q, getVectorsFromResult(resultSet));
        }
        long end = System.nanoTime();
        logger.info("Querying vectors took {} millisecond", (float)(end - start) / 1_000_000.0);

    }


    private UntypedResultSet search(float[] queryVector, int limit) throws Throwable
    {
        UntypedResultSet result = execute("SELECT * FROM %s ORDER BY val ann of " + Arrays.toString(queryVector) + " LIMIT " + limit);
        assertThat(result.size()).isCloseTo(limit, Percentage.withPercentage(5));
        return result;
    }

    private String vectorString(float[] vector)
    {
        return Arrays.toString(vector);
    }

    private float[] randomVector()
    {
        float[] rawVector = new float[word2vec.dimension()];
        for (int i = 0; i < word2vec.dimension(); i++)
        {
            rawVector[i] = getRandom().nextFloat();
        }
        return rawVector;
    }

    private void assertDescendingScore(float[] queryVector, List<float[]> resultVectors)
    {
        float prevScore = -1;
        for (float[] current : resultVectors)
        {
            float score = VectorSimilarityFunction.COSINE.compare(current, queryVector);
            if (prevScore >= 0)
                assertThat(score).isLessThanOrEqualTo(prevScore);

            prevScore = score;
        }
    }

    private List<float[]> getVectorsFromResult(UntypedResultSet result)
    {
        List<float[]> vectors = new ArrayList<>();
        VectorType<?> vectorType = VectorType.getInstance(FloatType.instance, word2vec.dimension());

        // verify results are part of inserted vectors
        for (UntypedResultSet.Row row: result)
        {
            vectors.add(vectorType.composeAsFloat(row.getBytes("val")));
        }

        return vectors;
    }

}
