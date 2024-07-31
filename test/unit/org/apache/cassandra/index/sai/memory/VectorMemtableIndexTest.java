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

package org.apache.cassandra.index.sai.memory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.VectorType;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.BootStrapper;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.ExcludingBounds;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.IncludingExcludingBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.cql.VectorTester;
import org.apache.cassandra.index.sai.disk.vector.VectorMemtableIndex;
import org.apache.cassandra.index.sai.plan.Orderer;
import org.apache.cassandra.index.sai.utils.PrimaryKeyWithScore;
import org.apache.cassandra.inject.Injections;
import org.apache.cassandra.inject.InvokePointBuilder;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.schema.MockSchema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.OpOrder;

import static org.apache.cassandra.config.CassandraRelevantProperties.MEMTABLE_SHARD_COUNT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class VectorMemtableIndexTest extends SAITester
{
    private static final Injections.Counter indexSearchCounter = Injections.newCounter("IndexSearchCounter")
                                                                           .add(InvokePointBuilder.newInvokePoint()
                                                                                                  .onClass(TrieMemoryIndex.class)
                                                                                                  .onMethod("search"))
                                                                           .build();

    private ColumnFamilyStore cfs;
    private IndexContext indexContext;
    private VectorMemtableIndex memtableIndex;
    private IPartitioner partitioner;
    private Map<DecoratedKey, Integer> keyMap;
    private Map<Integer, ByteBuffer> rowMap;
    private int dimensionCount;

    @BeforeClass
    public static void setShardCount()
    {
        MEMTABLE_SHARD_COUNT.setInt(8);
    }

    @Before
    public void setup() throws Throwable
    {
        TokenMetadata metadata = StorageService.instance.getTokenMetadata();
        metadata.updateNormalTokens(BootStrapper.getRandomTokens(metadata, 10), FBUtilities.getBroadcastAddressAndPort());

        TableMetadata tableMetadata = TableMetadata.builder("ks", "tb")
                                                   .addPartitionKeyColumn("pk", Int32Type.instance)
                                                   .addRegularColumn("val", Int32Type.instance)
                                                   .build();
        cfs = MockSchema.newCFS(tableMetadata);
        partitioner = cfs.getPartitioner();
        dimensionCount = getRandom().nextIntBetween(2, 2048);
        indexContext = SAITester.createIndexContext("index", VectorType.getInstance(FloatType.instance, dimensionCount), cfs);
        indexSearchCounter.reset();
        keyMap = new ConcurrentSkipListMap<>();
        rowMap = new ConcurrentHashMap<>();

        Injections.inject(indexSearchCounter);
    }

    @Test
    public void randomQueryTest()
    {
        var memtable = cfs.getCurrentMemtable();
        memtableIndex = new VectorMemtableIndex(indexContext, memtable);

        // insert rows
        int rowCount = ThreadLocalRandom.current().nextInt(1000, 5000);
        IntStream.range(0, rowCount).parallel().forEach(i ->
        {
            var value = randomVectorSerialized();
            while (true)
            {
                var pk = ThreadLocalRandom.current().nextInt(0, 10000);
                if (rowMap.putIfAbsent(pk, value) == null)
                {
                    addRow(pk, value);
                    break;
                }
            }
        });
        memtableIndex.cleanup();
        // master list of (random) keys inserted
        List<DecoratedKey> keys = new ArrayList<>(keyMap.keySet());

        // execute queries both with and without brute force enabled
        validate(keys);
        VectorTester.setMaxBruteForceRows(0);
        validate(keys);
    }

    private void validate(List<DecoratedKey> keys)
    {
        IntStream.range(0, 1_000).parallel().forEach(i ->
        {
            var orderer = generateRandomOrderer();
            AbstractBounds<PartitionPosition> keyRange = generateRandomBounds(keys);
            // compute keys in range of the bounds
            Set<Integer> keysInRange = keys.stream().filter(keyRange::contains)
                                           .map(k -> Int32Type.instance.compose(k.getKey()))
                                           .collect(Collectors.toSet());

            Set<Integer> foundKeys = new HashSet<>();
            int limit = getRandom().nextIntBetween(1, 100);

            long expectedResults = Math.min(limit, keysInRange.size());

            // execute the random ANN expression, and check that we get back as many keys as we asked for
            try (var iterator = memtableIndex.orderBy(new QueryContext(), orderer, keyRange, limit))
            {
                PrimaryKeyWithScore lastKey = null;
                while (iterator.hasNext() && foundKeys.size() < expectedResults)
                {
                    PrimaryKeyWithScore primaryKeyWithScore = (PrimaryKeyWithScore) iterator.next();
                    if (lastKey != null)
                        // This assertion only holds true as long as we query at most the expectedNumResults.
                        // Once we query deeper, we might get a key with a higher score than the last key.
                        // This is a direct consequence of the approximate part of ANN.
                        // Note that PrimaryKeyWithScore is flipped to descending order, so we use >= here.
                        assertTrue("Returned keys are not ordered by score", primaryKeyWithScore.compareTo(lastKey) >= 0);
                    lastKey = primaryKeyWithScore;
                    int key = Int32Type.instance.compose(primaryKeyWithScore.partitionKey().getKey());
                    assertFalse(foundKeys.contains(key));

                    assertTrue(keyRange.contains(primaryKeyWithScore.partitionKey()));
                    assertTrue(rowMap.containsKey(key));
                    foundKeys.add(key);
                }
                if (foundKeys.size() < expectedResults)
                    assertEquals("Expected " + expectedResults + " results but got " + foundKeys.size(), foundKeys.size(), expectedResults);
                if (limit < keysInRange.size())
                    assertTrue("Iterator should not be exhausted since it can resume search", iterator.hasNext());
            }
        });
    }

    @Test
    public void indexIteratorTest()
    {
        // VSTODO
    }

    private Orderer generateRandomOrderer()
    {
        return new Orderer(indexContext, Operator.ANN, randomVectorSerialized());
    }

    private ByteBuffer randomVectorSerialized() {
        return CQLTester.randomVectorSerialized(dimensionCount);
    }

    private AbstractBounds<PartitionPosition> generateRandomBounds(List<DecoratedKey> keys)
    {
        PartitionPosition leftBound = getRandom().nextBoolean() ? partitioner.getMinimumToken().minKeyBound()
                                                                : keys.get(getRandom().nextIntBetween(0, keys.size() - 1)).getToken().minKeyBound();

        PartitionPosition rightBound = getRandom().nextBoolean() ? partitioner.getMinimumToken().minKeyBound()
                                                                 : keys.get(getRandom().nextIntBetween(0, keys.size() - 1)).getToken().maxKeyBound();

        AbstractBounds<PartitionPosition> keyRange;

        if (leftBound.isMinimum() && rightBound.isMinimum())
            keyRange = new Range<>(leftBound, rightBound);
        else
        {
            if (AbstractBounds.strictlyWrapsAround(leftBound, rightBound))
            {
                PartitionPosition temp = leftBound;
                leftBound = rightBound;
                rightBound = temp;
            }
            if (getRandom().nextBoolean())
                keyRange = new Bounds<>(leftBound, rightBound);
            else if (getRandom().nextBoolean())
                keyRange = new ExcludingBounds<>(leftBound, rightBound);
            else
                keyRange = new IncludingExcludingBounds<>(leftBound, rightBound);
        }
        return keyRange;
    }

    private void addRow(int pk, ByteBuffer value)
    {
        DecoratedKey key = makeKey(cfs.metadata(), pk);
        memtableIndex.index(key,
                            Clustering.EMPTY,
                            value,
                            cfs.getCurrentMemtable(),
                            new OpOrder().start());
        keyMap.put(key, pk);
    }

    private DecoratedKey makeKey(TableMetadata table, Integer partitionKey)
    {
        ByteBuffer key = table.partitionKeyType.fromString(partitionKey.toString());
        return table.partitioner.decorateKey(key);
    }
}
