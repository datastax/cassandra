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
import java.util.stream.Collectors;

import org.junit.Test;

import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import org.apache.cassandra.db.marshal.FloatType;

public class VectorDotProductWithLengthTest extends VectorTester
{
    @Test
    public void testTrueDotproduct()
    {
        // setup
        createTable("CREATE TABLE %s (pk int, v vector<float, 2>, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function' : 'dot_product'}");
        var vectors = new ArrayList<float[]>();
        for (int i = 0; i < 2000; i++) { // 2000 is enough for PQ to run
            var v = create2DVector();
            vectors.add(v);
            execute("INSERT INTO %s (pk, v) VALUES (?, ?)", i, vector(v));
        }
        flush();

        // check that results are consistent with dot product similarity knn
        for (int i = 0; i < 10; i++) {
            var q = create2DVector();
            var result = execute("SELECT pk, v FROM %s ORDER BY v ANN OF ? LIMIT 3", vector(q));
            var ann = result.stream().map(row -> {
                var vList = row.getVector("v", FloatType.instance, 2);
                return new float[] { vList.get(0), vList.get(1)};
            }).collect(Collectors.toList());
            assert computeRecall(vectors, q, ann, VectorSimilarityFunction.DOT_PRODUCT) > 0.9;
        }
    }

    private static float[] create2DVector() {
        var R = getRandom();
        // these need to NOT be unit vectors to test the difference between DP and cosine
        return new float[] { R.nextFloatBetween(-100, 100), R.nextFloatBetween(-100, 100) };
    }
}
