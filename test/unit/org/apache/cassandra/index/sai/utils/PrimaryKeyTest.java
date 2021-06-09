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
package org.apache.cassandra.index.sai.utils;

import java.nio.ByteBuffer;

import org.junit.Test;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;

import static org.junit.Assert.assertTrue;

public class PrimaryKeyTest
{
    @Test
    public void primaryKeyFromBytesTest() throws Exception
    {
        PrimaryKey.PrimaryKeyFactory keyFactory = PrimaryKey.factory(Murmur3Partitioner.instance, new ClusteringComparator());
        DecoratedKey decoratedKey = Murmur3Partitioner.instance.decorateKey(UTF8Type.instance.decompose("A"));
        PrimaryKey expected = keyFactory.createKey(decoratedKey);
        byte[] bytes = expected.asBytes();

        PrimaryKey result = keyFactory.createKey(bytes);

        assertTrue(expected.compareTo(result) == 0);
    }

    @Test
    public void staticClusteringFromBytesTest() throws Exception
    {
        PrimaryKey.PrimaryKeyFactory keyFactory = PrimaryKey.factory(Murmur3Partitioner.instance, new ClusteringComparator(UTF8Type.instance));

        DecoratedKey decoratedKey = Murmur3Partitioner.instance.decorateKey(UTF8Type.instance.decompose("A"));

        PrimaryKey expected = keyFactory.createKey(decoratedKey, Clustering.STATIC_CLUSTERING);

        byte[] bytes = expected.asBytes();

        PrimaryKey result = keyFactory.createKey(bytes);

        assertTrue(expected.compareTo(result) == 0);
    }

    @Test
    public void skinnyRowTest() throws Exception
    {
        PrimaryKey.PrimaryKeyFactory keyFactory = PrimaryKey.factory(Murmur3Partitioner.instance, new ClusteringComparator());
        DecoratedKey decoratedKey = Murmur3Partitioner.instance.decorateKey(UTF8Type.instance.decompose("A"));
        PrimaryKey expected = keyFactory.createKey(decoratedKey);

        ByteComparable comparable = v -> expected.asComparableBytes(v);

        PrimaryKey result = keyFactory.createKey(comparable, 1);

        assertTrue(expected.compareTo(result) == 0);
    }

    @Test
    public void test() throws Exception
    {
        DecoratedKey decoratedKey = Murmur3Partitioner.instance.decorateKey(UTF8Type.instance.decompose("A"));
        ByteBuffer[] values = new ByteBuffer[1];
        values[0] = UTF8Type.instance.decompose("B");
        Clustering clustering = Clustering.make(values);

        PrimaryKey.PrimaryKeyFactory keyFactory = PrimaryKey.factory(Murmur3Partitioner.instance, new ClusteringComparator(UTF8Type.instance));

        PrimaryKey expected = keyFactory.createKey(decoratedKey, clustering);

        ByteComparable comparable = v -> expected.asComparableBytes(v);

        PrimaryKey result = keyFactory.createKey(comparable, 1);

        assertTrue(expected.compareTo(result) == 0);
    }
}
