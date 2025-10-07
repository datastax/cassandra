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
package org.apache.cassandra.index.sai.iterators;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import org.junit.Assert;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.SaiRandomizedTest;
import org.apache.cassandra.utils.Pair;

public class AbstractKeyRangeIteratorTest extends SaiRandomizedTest
{
    protected long[] arr(long... longArray)
    {
        return longArray;
    }

    protected long[] arr(int... intArray)
    {
        return Arrays.stream(intArray).mapToLong(i -> i).toArray();
    }

    final KeyRangeIterator buildIntersection(KeyRangeIterator... ranges)
    {
        return KeyRangeIntersectionIterator.<PrimaryKey>builder().add(Arrays.asList(ranges)).build();
    }

    final KeyRangeIterator buildSelectiveIntersection(int limit, KeyRangeIterator... ranges)
    {
        return KeyRangeIntersectionIterator.<PrimaryKey>builder(limit).add(Arrays.asList(ranges)).build();
    }

    final KeyRangeIterator buildIntersection(long[]... ranges)
    {
        return buildIntersection(toRangeIterator(ranges));
    }

    final KeyRangeIterator buildSelectiveIntersection(int limit, long[]... ranges)
    {
        return buildSelectiveIntersection(limit, toRangeIterator(ranges));
    }

    static KeyRangeIterator buildUnion(KeyRangeIterator... ranges)
    {
        return KeyRangeUnionIterator.<PrimaryKey>builder().add(Arrays.asList(ranges)).build();
    }

    static KeyRangeIterator buildUnion(long[]... ranges)
    {
        return buildUnion(toRangeIterator(ranges));
    }

    static KeyRangeIterator buildConcat(KeyRangeIterator... ranges)
    {
        return KeyRangeConcatIterator.builder(ranges.length).add(Arrays.asList(ranges)).build();
    }

    static KeyRangeIterator buildConcat(long[]... ranges)
    {
        return buildConcat(toRangeIterator(ranges));
    }

    private static KeyRangeIterator[] toRangeIterator(long[]... ranges)
    {
        return Arrays.stream(ranges).map(AbstractKeyRangeIteratorTest::build).toArray(KeyRangeIterator[]::new);
    }

    protected static LongIterator build(long... tokens)
    {
        return new LongIterator(tokens);
    }

    protected KeyRangeIterator build(KeyRangeIterator.Builder.IteratorType type, long[] tokensA, long[] tokensB)
    {
        KeyRangeIterator rangeA = new LongIterator(tokensA);
        KeyRangeIterator rangeB = new LongIterator(tokensB);

        switch (type)
        {
            case INTERSECTION:
                return buildIntersection(rangeA, rangeB);
            case UNION:
                return buildUnion(rangeA, rangeB);
            case CONCAT:
                return buildConcat(rangeA, rangeB);
            default:
                throw new IllegalArgumentException("unknown type: " + type);
        }
    }

    static void validateWithSkipping(KeyRangeIterator ri, long[] totalOrdering)
    {
        int count = 0;
        while (ri.hasNext())
        {
            // make sure hasNext plays nice with skipTo
            if (randomBoolean())
                ri.hasNext();

            // skipping to the same element should also be a no-op
            if (randomBoolean())
                ri.skipTo(LongIterator.fromToken(totalOrdering[count]));

            // skip a few elements
            if (nextDouble() < 0.1)
            {
                int n = nextInt(1, 3);
                if (count + n < totalOrdering.length)
                {
                    count += n;
                    ri.skipTo(LongIterator.fromToken(totalOrdering[count]));
                }
            }
            Assert.assertEquals(totalOrdering[count++], ri.next().token().getLongValue());
        }
        Assert.assertEquals(totalOrdering.length, count);
    }

    static Set<Long> toSet(long[] tokens)
    {
        return Arrays.stream(tokens).boxed().collect(Collectors.toSet());
    }

    /**
     * @return a random {Concat,Intersection, Union} iterator, and a long[] of the elements in the iterator.
     *         elements will range from 0..1024.
     */
    static Pair<KeyRangeIterator, long[]> createRandomIterator()
    {
        var n = randomIntBetween(0, 3);
        switch (n)
        {
            case 0:
                return KeyRangeConcatIteratorTest.createRandom();
            case 1:
                return KeyRangeIntersectionIteratorTest.createRandom(nextInt(1, 16));
            case 2:
                return KeyRangeUnionIteratorTest.createRandom(nextInt(1, 16));
            default:
                throw new AssertionError();
        }
    }


    private static final PrimaryKey.Factory TEST_PRIMARY_KEY_FACTORY = Version.current().onDiskFormat()
                                                                              .newPrimaryKeyFactory(new ClusteringComparator(LongType.instance));

    /**
     * Generates a random list of primary keys with the given average number of partitions and rows per partition.
     * Partition keys and clusterings are generated in such a way that when combining two such lists generated with
     * same parameters (but different random), there is a high chance both sets would contain many common keys, as well
     * as each would contain some keys not present in the other set.
     *
     * @return list of primary keys in (token, clustering) order.
     */
    static List<PrimaryKey> randomPrimaryKeys(int avgPartitions, int avgRowsPerPartition)
    {
        List<PrimaryKey> keys = new ArrayList<>((int)(avgPartitions * avgRowsPerPartition * 1.5));

        for (int p = 0; p < avgPartitions * 2; p++)
        {
            if (randomBoolean())   // skip 50% of partitions
                continue;

            if (randomBoolean())
            {
                keys.add(makeKey(p, null)); // add partition key only
            }
            else
            {
                for (int r = 0; r < avgRowsPerPartition * 2; r++)
                {
                    if (randomBoolean())   // skip 50% of rows
                        keys.add(makeKey(p, (long) r));
                }
            }
        }

        // We must sort the keys to recover proper token order
        Collections.sort(keys);
        return keys;
    }


    /**
      * Helper to create PrimaryKey with/without clustering.
      * Pass null clustering to create a key with Clustering.EMPTY.
      */
    static PrimaryKey makeKey(long partitionKey, @Nullable Long clustering)
    {
        ByteBuffer pkValue = LongType.instance.getSerializer().serialize(partitionKey);
        ByteBuffer clusteringValue = LongType.instance.getSerializer().serialize(clustering);
        DecoratedKey pk = Murmur3Partitioner.instance.decorateKey(pkValue);
        Clustering<ByteBuffer> c = clustering == null ? Clustering.EMPTY : Clustering.make(clusteringValue);
        return TEST_PRIMARY_KEY_FACTORY.create(pk, c);
    }

    /**
     * Convenience method for comparing arrays of PrimaryKey; we don't use assertEquals to compare arrays
     * because its output is one huge line of text that is hard to read when the test fails.
     */
    void assertKeysEqual(List<PrimaryKey> expected, List<PrimaryKey> result)
    {
        int matchesUntil = 0;
        try
        {
            for (int i = 0; i < expected.size() && i < result.size(); i++)
            {
                PrimaryKey e = expected.get(i);
                PrimaryKey r = result.get(i);
                assertEquals(e, r);
                matchesUntil = i;
            }

            if (result.size() < expected.size())
                throw new AssertionError("Missing " + (expected.size() - result.size()) + " key(s) at the end");
            if (result.size() > expected.size())
                throw new AssertionError("Got extra keys at the end: " + result.get(expected.size()));
        }
        catch (AssertionError e)
        {
            // Print out all the keys that matched properly before the failure to help debugging
            for (int i = 0; i < matchesUntil; i++)
                System.err.println("Keys match correctly: " + expected.get(i));

            throw e;
        }
    }

    /**
     * Checks if the given keys are in increasing order and contain no duplicates.
     */
    static void assertIncreasing(Collection<PrimaryKey> keys)
    {
        PrimaryKey lastPrimaryKey = null;
        DecoratedKey lastPartitionKey = null;
        Clustering<?> lastClustering = Clustering.EMPTY;
        for (PrimaryKey key : keys)
        {
            if (key.hasEmptyClustering() && key.partitionKey().equals(lastPartitionKey))
                throw new AssertionError("A primary key with empty clustering follows a key in the same partition:\n" + key + "\nafter:\n" + lastPrimaryKey);

            if (!key.hasEmptyClustering() && lastClustering.isEmpty() && key.partitionKey().equals(lastPartitionKey))
                throw new AssertionError("A primary key with non-empty clustering follows a key with empty clustering in the same partition:\n" + key + "\nafter:\n" + lastPrimaryKey);

            if (Objects.equals(key, lastPrimaryKey))
                throw new AssertionError("Duplicate key:\n" + key + " = " + lastPrimaryKey);

            if (lastPrimaryKey != null && key.compareTo(lastPrimaryKey) < 0)
                throw new AssertionError("Out of order key:\n" + key + " < " + lastPrimaryKey);

            lastPrimaryKey = key;
            lastPartitionKey = key.partitionKey();
            lastClustering = key.clustering();
        }
    }

    /**
     * Helper class to quickly find if a key exists in the set or not.
     * We cannot just use a hashmap for that, because keys with no clustering match full partitions.
     */
    static class PrimaryKeySet
    {
        Set<DecoratedKey> partitions = new HashSet<>();
        Set<Pair<DecoratedKey, Clustering<?>>> rows = new HashSet<>();

        public PrimaryKeySet(Collection<PrimaryKey> keys)
        {
            for (PrimaryKey pk : keys)
            {
                if (pk.hasEmptyClustering())
                    partitions.add(pk.partitionKey());
                else
                    rows.add(Pair.create(pk.partitionKey(), pk.clustering()));
            }
        }

        public boolean contains(PrimaryKey key)
        {
            return partitions.contains(key.partitionKey()) ||
                   rows.contains(Pair.create(key.partitionKey(), key.clustering()));
        }
    }


    static class PrimaryKeyListIterator extends KeyRangeIterator
    {
        private final List<PrimaryKey> keys;
        private int currentIdx = 0;

        private PrimaryKeyListIterator(List<PrimaryKey> keys)
        {
            super(keys.isEmpty() ? null : keys.get(0), keys.isEmpty() ? null : keys.get(keys.size() - 1), keys.size());
            this.keys = new ArrayList<>(keys);

        }

        public static PrimaryKeyListIterator create(PrimaryKey... keys)
        {
            List<PrimaryKey> list = Arrays.asList(keys);
            Collections.sort(list);
            return new PrimaryKeyListIterator(list);
        }

        public static PrimaryKeyListIterator create(List<PrimaryKey> keys)
        {
            Collections.sort(keys);
            return new PrimaryKeyListIterator(keys);
        }

        @Override
        protected PrimaryKey computeNext()
        {
            if (currentIdx >= keys.size())
                return endOfData();

            return keys.get(currentIdx++);
        }

        @Override
        protected void performSkipTo(PrimaryKey nextToken)
        {
            while (currentIdx < keys.size() && keys.get(currentIdx).compareTo(nextToken) < 0)
                currentIdx++;
        }

        @Override
        public void close()
        {}
    }


    /**
     * Prints each key in a separate line to help debugging.
     * Useful because those printed keys are very long.
     */
    void printKeys(Collection<PrimaryKey> keys)
    {
        for (PrimaryKey key : keys)
            System.err.println(key);
    }

    /**
     * Generates all permutations of array of integers from 0 to n - 1.
     * E.g. for n = 3, generates: [[0, 1, 2], [0, 2, 1], [1, 0, 2], [1, 2, 0], [2, 1, 0], [2, 0, 1]]
     */
    static List<int[]> permutations(int n) {
        int[] indices = new int[n];
        for (int i = 0; i < n; i++) {
            indices[i] = i;
        }

        List<int[]> result = new ArrayList<>();
        generatePermutations(indices, 0, result);
        return result;
    }

    // Recursive function to find all possible permutations
    private static void generatePermutations(int[] arr, int idx, List<int[]> res) {
        if (idx == arr.length)
        {
            res.add(Arrays.copyOf(arr, arr.length));
            return;
        }

        for (int i = idx; i < arr.length; i++) {
            int temp = arr[idx];
            arr[idx] = arr[i];
            arr[i] = temp;

            generatePermutations(arr, idx + 1, res);

            temp = arr[idx];
            arr[idx] = arr[i];
            arr[i] = temp;
        }
    }


    /**
     * Performs a merge operation on the primary key lists and validates the result.
     * If the validation fails, it will try to first minimize the input lists needed to crash the operation
     * or fail the validation. If the operation succeeds, returns normally, otherwise throws the final exception.
     *
     * @param inputs some arbitrary lists of primary keys
     * @param operation operation under test that merges multiple lists into one
     * @param validator validation function that checks if the result of the operation is correct, expected to throw if not
     */
    void testMergeAndValidate(List<List<PrimaryKey>> inputs,
                              Function<List<List<PrimaryKey>>, List<PrimaryKey>> operation,
                              BiConsumer<List<List<PrimaryKey>>, List<PrimaryKey>> validator) throws Throwable
    {
        List<PrimaryKey> result = null;       // last result we obtained from operation (may be valid)
        List<PrimaryKey> failedResult = null; // last result that failed validation
        Throwable exception = null;    // last exception we got from operation or validation

        try
        {
            result = operation.apply(inputs);
            validator.accept(inputs, result);
            return;  // test passes, nothing to do
        }
        catch (Throwable e)
        {
            failedResult = result;
            exception = e;
        }

        // Run the test with smaller inputs until the test doesn't fail anymore
        // or we reach the max number of attempts
        int attempt = 0;

        while (attempt < 10 && inputs.stream().anyMatch(l -> !l.isEmpty()))
        {
            // make a copy of each input with some keys removed
            boolean removed; // tracks if we actually removed something
            List<List<PrimaryKey>> minimizedInputs;
            do
            {
                minimizedInputs = new ArrayList<>();
                removed = false;
                int totalKeys = inputs.stream().mapToInt(List::size).sum();

                for (List<PrimaryKey> input : inputs)
                {
                    ArrayList<PrimaryKey> minimized = new ArrayList<>();
                    minimizedInputs.add(minimized);

                    // We want to remove a constant fraction of keys (~10%) to make sure we converge quickly,
                    // but we must be carefult when the number of keys gets small, so we don't end up leaving all keys
                    // unmodified.
                    for (PrimaryKey key : input)
                        if (nextInt(Math.min(10, totalKeys)) != 0)
                            minimized.add(key);

                    removed |= minimized.size() < input.size();
                }
            } while (!removed);

            try
            {
                result = null;  // must clean result in case operation.apply fails in the next line;
                                // we don't want to keep a result from a previous run
                result = operation.apply(minimizedInputs);
                validator.accept(minimizedInputs, result);
                attempt++;
            }
            catch (Throwable e)
            {
                // if we're still failing, then it's a success! we managed to get a smaller input
                attempt = 0;
                inputs = minimizedInputs;
                failedResult = result;
                exception = e;
            }
        }

        System.err.println("Validation failed");
        for (int i = 0; i < inputs.size(); i++)
        {
            System.err.println("\nInput " + i + ':');
            printKeys(inputs.get(i));
        }


        if (failedResult != null)
        {
            System.err.println("\nResult:");
            printKeys(failedResult);
        }

        throw exception;
    }

}
