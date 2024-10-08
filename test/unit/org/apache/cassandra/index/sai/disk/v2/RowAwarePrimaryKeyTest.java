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

package org.apache.cassandra.index.sai.disk.v2;

import java.nio.ByteBuffer;
import java.util.function.Supplier;

import org.junit.Test;

import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.utils.PrimaryKey;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RowAwarePrimaryKeyTest extends SAITester
{
    @Test
    public void testHashCodeForDeferredPrimaryKey()
    {
        var factory = Version.BA.onDiskFormat().newPrimaryKeyFactory(EMPTY_COMPARATOR);

        // Test relies on this implementation detail
        assertTrue(factory instanceof RowAwarePrimaryKeyFactory);

        // Set up the primary key
        Token token = new Murmur3Partitioner.LongToken(1);
        DecoratedKey key = new BufferDecoratedKey(token, ByteBuffer.allocate(1));
        Supplier<PrimaryKey> supplier = () -> factory.create(key, Clustering.EMPTY);
        PrimaryKey primaryKey1 = factory.createDeferred(token, supplier);

        // Verify the results
        int hash1 = primaryKey1.hashCode();
        // Equals triggers loading the primary key
        assertEquals(primaryKey1, primaryKey1);
        assertEquals(hash1, primaryKey1.hashCode());

        // Do again with explicit loading
        PrimaryKey primaryKey2 = factory.createDeferred(token, supplier);
        int hash2 = primaryKey2.hashCode();
        primaryKey2.loadDeferred();
        assertEquals(hash2, primaryKey2.hashCode());
    }

    @Test
    public void testHashCodeForLoadedPrimaryKey()
    {
        var factory = Version.BA.onDiskFormat().newPrimaryKeyFactory(EMPTY_COMPARATOR);

        // Test relies on this implementation detail
        assertTrue(factory instanceof RowAwarePrimaryKeyFactory);

        // Set up the primary key
        Token token1 = new Murmur3Partitioner.LongToken(1);
        DecoratedKey key1 = new BufferDecoratedKey(token1, ByteBuffer.allocate(1));
        PrimaryKey primaryKey1 = factory.create(key1, Clustering.EMPTY);

        // Create equivalent PK
        Token token2 = new Murmur3Partitioner.LongToken(1);
        DecoratedKey key2 = new BufferDecoratedKey(token2, ByteBuffer.allocate(1));
        PrimaryKey primaryKey2 = factory.create(key2, Clustering.EMPTY);

        assertEquals(primaryKey1.hashCode(), primaryKey2.hashCode());
    }

    @Test
    public void testHashCodeForDeferedPrimaryKeyWithClusteringColumns()
    {
        var comparator = new ClusteringComparator(Int32Type.instance);
        var factory = Version.BA.onDiskFormat().newPrimaryKeyFactory(comparator);

        // Test relies on this implementation detail
        assertTrue(factory instanceof RowAwarePrimaryKeyFactory);

        // Set up the primary key
        Token token1 = new Murmur3Partitioner.LongToken(1);
        DecoratedKey key1 = new BufferDecoratedKey(token1, ByteBuffer.allocate(1));
        PrimaryKey primaryKey1 = factory.create(key1, Clustering.make(ByteBuffer.allocate(1)));

        // Create equivalent PK
        Token token2 = new Murmur3Partitioner.LongToken(1);
        DecoratedKey key2 = new BufferDecoratedKey(token2, ByteBuffer.allocate(1));
        PrimaryKey primaryKey2 = factory.create(key2, Clustering.make(ByteBuffer.allocate(1)));

        assertEquals(primaryKey1.hashCode(), primaryKey2.hashCode());
    }
}
