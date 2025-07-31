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
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.assertj.core.data.Percentage;

import static org.assertj.core.api.Assertions.assertThat;

public abstract class VectorKeyRestrictedTester extends VectorTester.VersionedWithChecksums
{
    protected static final IPartitioner partitioner = Murmur3Partitioner.instance;

    protected Collection<Integer> keys(UntypedResultSet result)
    {
        List<Integer> keys = new ArrayList<>(result.size());
        for (UntypedResultSet.Row row : result)
            keys.add(row.getInt("partition"));
        return keys;
    }

    protected Collection<Integer> keysWithUpperBound(Collection<Integer> keys, int rightKey, boolean rightInclusive)
    {
        return keysInTokenRange(keys, partitioner.getMinimumToken().getToken(), true,
                                partitioner.getToken(Int32Type.instance.decompose(rightKey)), rightInclusive);
    }

    protected Collection<Integer> keysWithLowerBound(Collection<Integer> keys, int leftKey, boolean leftInclusive)
    {
        return keysInTokenRange(keys, partitioner.getToken(Int32Type.instance.decompose(leftKey)), leftInclusive,
                                partitioner.getMaximumToken().getToken(), true);
    }

    protected Collection<Integer> keysInBounds(Collection<Integer> keys, int leftKey, boolean leftInclusive, int rightKey, boolean rightInclusive)
    {
        return keysInTokenRange(keys, partitioner.getToken(Int32Type.instance.decompose(leftKey)), leftInclusive,
                                partitioner.getToken(Int32Type.instance.decompose(rightKey)), rightInclusive);
    }

    protected Collection<Integer> keysInTokenRange(Collection<Integer> keys, Token leftToken, boolean leftInclusive, Token rightToken, boolean rightInclusive)
    {
        long left = leftToken.getLongValue();
        long right = rightToken.getLongValue();
        return keys.stream()
                   .filter(k -> {
                       long t = partitioner.getToken(Int32Type.instance.decompose(k)).getLongValue();
                       return (left < t || left == t && leftInclusive) && (t < right || t == right && rightInclusive);
                   }).collect(Collectors.toSet());
    }

    protected void searchWithNonExistingKey(float[] queryVector, int key)
    {
        searchWithKey(queryVector, key, 0);
    }

    protected void searchWithNonExistingKey(Vector<Float> queryVector, int key)
    {
        searchWithKey(queryVector, key, 0);
    }

    protected void searchWithKey(float[] queryVector, int key, int expectedSize)
    {
        UntypedResultSet result = execute("SELECT * FROM %s WHERE pk = " + key + " ORDER BY val ann of " + Arrays.toString(queryVector) + " LIMIT 1000");

        // VSTODO maybe we should have different methods for these cases
        if (expectedSize < 10)
            assertThat(result).hasSize(expectedSize);
        else
            assertThat(result.size()).isCloseTo(expectedSize, Percentage.withPercentage(5));
        result.stream().forEach(row -> assertThat(row.getInt("pk")).isEqualTo(key));
    }

    protected void searchWithKey(Vector<Float> queryVector, int key, int expectedSize)
    {
        searchWithKey(queryVector, key, expectedSize, 1000);
    }

    protected void searchWithKey(Vector<Float> queryVector, int key, int expectedSize, int limit)
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
