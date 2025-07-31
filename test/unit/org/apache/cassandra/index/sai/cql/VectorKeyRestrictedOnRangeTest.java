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

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import org.apache.cassandra.cql3.UntypedResultSet;

import static org.assertj.core.api.Assertions.assertThat;

public class VectorKeyRestrictedOnRangeTest extends VectorKeyRestrictedTester
{
    @Test
    public void rangeSearchTest() throws Throwable
    {
        createTable("CREATE TABLE %s (partition int, val vector<float, 2>, PRIMARY KEY(partition))");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function' : 'euclidean'}");

        var nPartitions = 100;
        Map<Integer, float[]> vectorsByKey = new HashMap<>();

        for (int i = 1; i <= nPartitions; i++)
        {
            float[] vector = {(float) i, (float) i};
            execute("INSERT INTO %s (partition, val) VALUES (?, ?)", i, vector(vector));
            vectorsByKey.put(i, vector);
        }

        var queryVector = vector(1.5f, 1.5f);
        beforeAndAfterFlush(() -> {
            for (int i = 1; i <= nPartitions; i++)
            {
                UntypedResultSet result = execute("SELECT partition FROM %s WHERE token(partition) > token(?) ORDER BY val ann of ? LIMIT 1000", i, queryVector);
                assertThat(keys(result)).containsExactlyInAnyOrderElementsOf(keysWithLowerBound(vectorsByKey.keySet(), i, false));

                result = execute("SELECT partition FROM %s WHERE token(partition) >= token(?) ORDER BY val ann of ? LIMIT 1000", i, queryVector);
                assertThat(keys(result)).containsExactlyInAnyOrderElementsOf(keysWithLowerBound(vectorsByKey.keySet(), i, true));

                result = execute("SELECT partition FROM %s WHERE token(partition) < token(?) ORDER BY val ann of ? LIMIT 1000", i, queryVector);
                assertThat(keys(result)).containsExactlyInAnyOrderElementsOf(keysWithUpperBound(vectorsByKey.keySet(), i, false));

                result = execute("SELECT partition FROM %s WHERE token(partition) <= token(?) ORDER BY val ann of ? LIMIT 1000", i, queryVector);
                assertThat(keys(result)).containsExactlyInAnyOrderElementsOf(keysWithUpperBound(vectorsByKey.keySet(), i, true));

                for (int j = 1; j <= nPartitions; j++)
                {
                    result = execute("SELECT partition FROM %s WHERE token(partition) >= token(?) AND token(partition) <= token(?) ORDER BY val ann of ? LIMIT 1000", i, j, queryVector);
                    assertThat(keys(result)).containsExactlyInAnyOrderElementsOf(keysInBounds(vectorsByKey.keySet(), i, true, j, true));

                    result = execute("SELECT partition FROM %s WHERE token(partition) > token(?) AND token(partition) <= token(?) ORDER BY val ann of ? LIMIT 1000", i, j, queryVector);
                    assertThat(keys(result)).containsExactlyInAnyOrderElementsOf(keysInBounds(vectorsByKey.keySet(), i, false, j, true));

                    result = execute("SELECT partition FROM %s WHERE token(partition) >= token(?) AND token(partition) < token(?) ORDER BY val ann of ? LIMIT 1000", i, j, queryVector);
                    assertThat(keys(result)).containsExactlyInAnyOrderElementsOf(keysInBounds(vectorsByKey.keySet(), i, true, j, false));

                    result = execute("SELECT partition FROM %s WHERE token(partition) > token(?) AND token(partition) < token(?) ORDER BY val ann of ? LIMIT 1000", i, j, queryVector);
                    assertThat(keys(result)).containsExactlyInAnyOrderElementsOf(keysInBounds(vectorsByKey.keySet(), i, false, j, false));
                }
            }
        });
    }
}
